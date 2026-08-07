use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time::timeout,
};

pub const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExternalCommand {
    program: PathBuf,
    args: Vec<OsString>,
    timeout: Duration,
    max_output_bytes: usize,
}

impl ExternalCommand {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            timeout: DEFAULT_COMMAND_TIMEOUT,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }

    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = max_output_bytes;
        self
    }

    pub fn program(&self) -> &Path {
        &self.program
    }

    pub fn args(&self) -> &[OsString] {
        &self.args
    }

    pub fn timeout_duration(&self) -> Duration {
        self.timeout
    }

    pub fn max_output_bytes_limit(&self) -> usize {
        self.max_output_bytes
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExternalCommandOutput {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[async_trait]
pub trait ExternalCommandRunner: Send + Sync {
    async fn run(&self, command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessCommandRunner;

#[async_trait]
impl ExternalCommandRunner for ProcessCommandRunner {
    async fn run(&self, command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError> {
        let program = command.program().to_owned();
        let mut child = Command::new(command.program())
            .args(command.args())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|source| CommandError::Spawn { program: program.clone(), source })?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CommandError::Pipe { program: program.clone(), stream: "stdout" })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CommandError::Pipe { program: program.clone(), stream: "stderr" })?;

        let max_output_bytes = command.max_output_bytes_limit();
        let execution = async {
            let stdout_read = read_bounded(stdout, max_output_bytes);
            let stderr_read = read_bounded(stderr, max_output_bytes);
            let wait = child.wait();
            let (stdout, stderr, status) = tokio::join!(stdout_read, stderr_read, wait);
            let stdout = stdout.map_err(|source| CommandError::Read {
                program: program.clone(),
                stream: "stdout",
                source,
            })?;
            let stderr = stderr.map_err(|source| CommandError::Read {
                program: program.clone(),
                stream: "stderr",
                source,
            })?;
            let status =
                status.map_err(|source| CommandError::Wait { program: program.clone(), source })?;
            Ok::<_, CommandError>((stdout, stderr, status))
        };
        let (stdout, stderr, status) = match timeout(command.timeout_duration(), execution).await {
            Ok(result) => result?,
            Err(_) => {
                let _ = child.kill().await;
                let _ = timeout(Duration::from_secs(1), child.wait()).await;
                return Err(CommandError::TimedOut {
                    program,
                    timeout: command.timeout_duration(),
                });
            }
        };

        Ok(ExternalCommandOutput {
            success: status.success(),
            exit_code: status.code(),
            stdout: stdout.bytes,
            stderr: stderr.bytes,
            stdout_truncated: stdout.truncated,
            stderr_truncated: stderr.truncated,
        })
    }
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("could not start external command {program}: {source}")]
    Spawn { program: PathBuf, source: std::io::Error },
    #[error("could not capture {stream} from external command {program}")]
    Pipe { program: PathBuf, stream: &'static str },
    #[error("could not read {stream} from external command {program}: {source}")]
    Read { program: PathBuf, stream: &'static str, source: std::io::Error },
    #[error("external command {program} timed out after {timeout:?}")]
    TimedOut { program: PathBuf, timeout: Duration },
    #[error("could not wait for external command {program}: {source}")]
    Wait { program: PathBuf, source: std::io::Error },
}

impl CommandError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::TimedOut { .. })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct BoundedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn read_bounded(
    mut stream: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<BoundedOutput, std::io::Error> {
    let mut bytes = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;

    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }

        let remaining = limit.saturating_sub(bytes.len());
        if remaining > 0 {
            let copied = read.min(remaining);
            bytes.extend_from_slice(&buffer[..copied]);
            truncated |= copied < read;
        } else {
            truncated = true;
        }
    }

    Ok(BoundedOutput { bytes, truncated })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_keeps_program_and_arguments_separate() {
        let command =
            ExternalCommand::new("ffprobe").arg("-v").arg("error").arg("file with spaces.mp4");

        assert_eq!(command.program(), Path::new("ffprobe"));
        assert_eq!(
            command.args(),
            [OsString::from("-v"), OsString::from("error"), OsString::from("file with spaces.mp4")]
        );
    }

    #[tokio::test]
    async fn process_runner_bounds_captured_output() {
        let command = if cfg!(windows) {
            ExternalCommand::new("cmd").arg("/C").arg("echo output")
        } else {
            ExternalCommand::new("printf").arg("123456789")
        }
        .max_output_bytes(4);

        let output = ProcessCommandRunner.run(command).await.expect("command should run");
        assert_eq!(output.stdout, b"1234");
        assert!(output.stdout_truncated);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_runner_terminates_a_timed_out_command() {
        let command = ExternalCommand::new("sleep").arg("5").timeout(Duration::from_millis(20));

        let error = ProcessCommandRunner.run(command).await.expect_err("command should time out");
        assert!(error.is_timeout());
    }
}
