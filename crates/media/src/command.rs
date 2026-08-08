use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use async_trait::async_trait;
#[cfg(unix)]
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
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
        let mut process = Command::new(command.program());
        process
            .args(command.args())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        process.process_group(0);
        let mut child = process
            .kill_on_drop(true)
            .spawn()
            .map_err(|source| CommandError::Spawn { program: program.clone(), source })?;
        let mut group_cleanup = ProcessGroupCleanup::new(child.id());

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
                terminate_process_group(&mut child).await;
                group_cleanup.disarm();
                return Err(CommandError::TimedOut {
                    program,
                    timeout: command.timeout_duration(),
                });
            }
        };
        group_cleanup.disarm();

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

#[cfg(unix)]
struct ProcessGroupCleanup {
    process_group_id: Option<u32>,
}

#[cfg(not(unix))]
struct ProcessGroupCleanup;

impl ProcessGroupCleanup {
    fn new(process_id: Option<u32>) -> Self {
        #[cfg(unix)]
        {
            Self { process_group_id: process_id }
        }
        #[cfg(not(unix))]
        {
            let _ = process_id;
            Self
        }
    }

    fn disarm(&mut self) {
        #[cfg(unix)]
        {
            self.process_group_id = None;
        }
    }
}

#[cfg(unix)]
impl Drop for ProcessGroupCleanup {
    fn drop(&mut self) {
        if let Some(process_group_id) = self.process_group_id.take() {
            kill_process_group_sync(process_group_id, "-KILL");
        }
    }
}

#[cfg(not(unix))]
impl Drop for ProcessGroupCleanup {
    fn drop(&mut self) {}
}

#[cfg(unix)]
async fn terminate_process_group(child: &mut tokio::process::Child) {
    if let Some(process_group_id) = child.id() {
        let _ = signal_process_group(process_group_id, "-TERM").await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = signal_process_group(process_group_id, "-KILL").await;
    }
    let _ = child.kill().await;
    let _ = timeout(Duration::from_secs(1), child.wait()).await;
}

#[cfg(not(unix))]
async fn terminate_process_group(child: &mut tokio::process::Child) {
    let _ = child.kill().await;
    let _ = timeout(Duration::from_secs(1), child.wait()).await;
}

#[cfg(unix)]
async fn signal_process_group(process_group_id: u32, signal: &str) -> std::io::Result<()> {
    kill_group(process_group_id, signal)
}

#[cfg(unix)]
fn kill_process_group_sync(process_group_id: u32, signal: &str) {
    let _ = kill_group(process_group_id, signal);
}

#[cfg(unix)]
fn kill_group(process_group_id: u32, signal: &str) -> std::io::Result<()> {
    let process_group_id = i32::try_from(process_group_id)
        .map_err(|_| std::io::Error::other("process ID does not fit in a POSIX PID"))?;
    let signal = match signal {
        "-TERM" => Signal::SIGTERM,
        "-KILL" => Signal::SIGKILL,
        _ => return Err(std::io::Error::other("unsupported process-group signal")),
    };
    kill(Pid::from_raw(-process_group_id), signal)
        .map_err(|error| std::io::Error::from_raw_os_error(error as i32))
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

    #[cfg(unix)]
    #[tokio::test]
    async fn process_runner_terminates_descendants_in_the_owned_group() {
        use std::process::Stdio;

        let pid_file =
            std::env::temp_dir().join(format!("sooqa-child-pid-{}", uuid::Uuid::new_v4()));
        let command = ExternalCommand::new("/bin/sh")
            // The shell is only a test fixture: production commands still use
            // argument arrays and never invoke a shell.
            .arg("-c")
            .arg("sleep 5 & echo $! > \"$1\"; wait")
            .arg("sooqa-process-group-test")
            .arg(pid_file.to_string_lossy().into_owned())
            .timeout(Duration::from_millis(200));

        let error = ProcessCommandRunner.run(command).await.expect_err("command should time out");
        assert!(error.is_timeout());
        let child_pid =
            tokio::fs::read_to_string(&pid_file).await.expect("child should have written its PID");
        let status = std::process::Command::new("/bin/kill")
            .arg("-0")
            .arg(child_pid.trim())
            .stderr(Stdio::null())
            .status()
            .expect("kill probe should run");
        assert!(!status.success(), "descendant process should have been terminated");
        tokio::fs::remove_file(pid_file).await.expect("PID fixture should be removed");
    }
}
