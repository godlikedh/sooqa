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
pub const DEFAULT_COMMAND_PATH: &str = "/usr/local/bin:/usr/bin:/bin";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExternalCommand {
    program: PathBuf,
    args: Vec<OsString>,
    current_dir: Option<PathBuf>,
    clear_environment: bool,
    environment: Vec<(OsString, OsString)>,
    timeout: Duration,
    max_output_bytes: usize,
}

impl ExternalCommand {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            current_dir: None,
            clear_environment: false,
            environment: Vec::new(),
            timeout: DEFAULT_COMMAND_TIMEOUT,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }

    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn current_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(path.into());
        self
    }

    pub fn clear_environment(mut self) -> Self {
        self.clear_environment = true;
        self
    }

    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.environment.push((key.into(), value.into()));
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

    pub fn current_directory(&self) -> Option<&Path> {
        self.current_dir.as_deref()
    }

    pub fn clears_environment(&self) -> bool {
        self.clear_environment
    }

    pub fn environment(&self) -> &[(OsString, OsString)] {
        &self.environment
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

    /// Run a command that produces one bounded output file.
    ///
    /// The default keeps test and adapter runners compatible. The production
    /// process runner overrides this to monitor the file while the child is
    /// still running.
    async fn run_file(
        &self,
        command: ExternalCommand,
        _output_file: &Path,
        _max_bytes: u64,
    ) -> Result<ExternalCommandOutput, CommandError> {
        self.run(command).await
    }

    /// Run a command that produces a bounded sequence of files.
    ///
    /// The default keeps test and adapter runners compatible. The production
    /// process runner overrides this to monitor the output directory while
    /// the child is still running.
    async fn run_sequence(
        &self,
        command: ExternalCommand,
        _output_directory: &Path,
        _max_bytes: u64,
    ) -> Result<ExternalCommandOutput, CommandError> {
        self.run(command).await
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessCommandRunner;

#[async_trait]
impl ExternalCommandRunner for ProcessCommandRunner {
    async fn run(&self, command: ExternalCommand) -> Result<ExternalCommandOutput, CommandError> {
        run_process_command(command, None).await
    }

    async fn run_file(
        &self,
        command: ExternalCommand,
        output_file: &Path,
        max_bytes: u64,
    ) -> Result<ExternalCommandOutput, CommandError> {
        run_process_command(
            command,
            Some(OutputLimitTarget::File { path: output_file.to_owned(), max_bytes }),
        )
        .await
    }

    async fn run_sequence(
        &self,
        command: ExternalCommand,
        output_directory: &Path,
        max_bytes: u64,
    ) -> Result<ExternalCommandOutput, CommandError> {
        run_process_command(
            command,
            Some(OutputLimitTarget::Directory { path: output_directory.to_owned(), max_bytes }),
        )
        .await
    }
}

#[derive(Debug, Clone)]
enum OutputLimitTarget {
    File { path: PathBuf, max_bytes: u64 },
    Directory { path: PathBuf, max_bytes: u64 },
}

async fn run_process_command(
    command: ExternalCommand,
    output_limit: Option<OutputLimitTarget>,
) -> Result<ExternalCommandOutput, CommandError> {
    let program = command.program().to_owned();
    let mut process = Command::new(command.program());
    process.args(command.args()).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    if command.clears_environment() {
        process.env_clear();
        for (key, value) in command.environment() {
            process.env(key, value);
        }
    }
    if let Some(current_dir) = command.current_directory() {
        process.current_dir(current_dir);
    }
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
    let monitor_config = output_limit.clone();
    let monitor = async move {
        match monitor_config {
            Some(target) => monitor_output_limit(target).await,
            None => std::future::pending::<Result<(), std::io::Error>>().await,
        }
    };
    let (stdout, stderr, status) = tokio::select! {
        result = timeout(command.timeout_duration(), execution) => match result {
            Ok(result) => result?,
            Err(_) => {
                terminate_process_group(&mut child).await;
                group_cleanup.disarm();
                return Err(CommandError::TimedOut {
                    program,
                    timeout: command.timeout_duration(),
                });
            }
        },
        monitor_result = monitor => {
            match monitor_result {
                Ok(()) => {
                    let limit = output_limit
                        .as_ref()
                        .map(output_limit_target_limit)
                        .expect("an output monitor has a configured limit");
                    terminate_process_group(&mut child).await;
                    group_cleanup.disarm();
                    return Err(CommandError::OutputLimitExceeded { program, limit });
                }
                Err(source) => {
                    let directory = output_limit
                        .as_ref()
                        .map(output_limit_target_path)
                        .expect("an output monitor has a configured path");
                    terminate_process_group(&mut child).await;
                    group_cleanup.disarm();
                    return Err(CommandError::OutputMonitor { program, directory, source });
                }
            }
        }
    };
    group_cleanup.disarm();

    if let Some(target) = output_limit.as_ref() {
        let size = output_limit_target_size(target).await.map_err(|source| {
            CommandError::OutputMonitor {
                program: program.clone(),
                directory: output_limit_target_path(target),
                source,
            }
        })?;
        if size > output_limit_target_limit(target) {
            return Err(CommandError::OutputLimitExceeded {
                program,
                limit: output_limit_target_limit(target),
            });
        }
    }

    Ok(ExternalCommandOutput {
        success: status.success(),
        exit_code: status.code(),
        stdout: stdout.bytes,
        stderr: stderr.bytes,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    })
}

async fn monitor_output_limit(target: OutputLimitTarget) -> Result<(), std::io::Error> {
    loop {
        if output_limit_target_size(&target).await? > output_limit_target_limit(&target) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn output_limit_target_path(target: &OutputLimitTarget) -> PathBuf {
    match target {
        OutputLimitTarget::File { path, .. } | OutputLimitTarget::Directory { path, .. } => {
            path.clone()
        }
    }
}

fn output_limit_target_limit(target: &OutputLimitTarget) -> u64 {
    match target {
        OutputLimitTarget::File { max_bytes, .. }
        | OutputLimitTarget::Directory { max_bytes, .. } => *max_bytes,
    }
}

async fn output_limit_target_size(target: &OutputLimitTarget) -> Result<u64, std::io::Error> {
    match target {
        OutputLimitTarget::File { path, .. } => file_size(path).await,
        OutputLimitTarget::Directory { path, .. } => sequence_directory_size(path).await,
    }
}

async fn file_size(path: &Path) -> Result<u64, std::io::Error> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("bounded output is not a regular file: {}", path.display()),
        ));
    }
    Ok(metadata.len())
}

pub(crate) async fn sequence_directory_size(directory: &Path) -> Result<u64, std::io::Error> {
    let mut entries = tokio::fs::read_dir(directory).await?;
    let mut total = 0_u64;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            // A concurrent bounded consumer may remove a frame between
            // read_dir and metadata. It is no longer part of the producer
            // lead, so continue the accounting pass.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if metadata.file_type().is_symlink() || metadata.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("sequence output contains a non-regular entry: {}", path.display()),
            ));
        }
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
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
    #[error("external command {program} sequence output exceeded the {limit}-byte limit")]
    OutputLimitExceeded { program: PathBuf, limit: u64 },
    #[error("could not monitor external command {program} output directory {directory}: {source}")]
    OutputMonitor { program: PathBuf, directory: PathBuf, source: std::io::Error },
}

impl CommandError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::TimedOut { .. })
    }

    pub fn is_output_limit_exceeded(&self) -> bool {
        matches!(self, Self::OutputLimitExceeded { .. })
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
        let command = ExternalCommand::new("ffprobe")
            .current_dir("/var/lib/sooqa/work")
            .arg("-v")
            .arg("error")
            .arg("file with spaces.mp4");

        assert_eq!(command.program(), Path::new("ffprobe"));
        assert_eq!(command.current_directory(), Some(Path::new("/var/lib/sooqa/work")));
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
    async fn process_runner_stops_when_one_output_file_exceeds_limit() {
        let root = std::env::temp_dir().join(format!("sooqa-file-limit-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.expect("test root should be created");
        let output = root.join("output.bin");
        let command = ExternalCommand::new("/bin/sh")
            // The shell is only a test fixture: production commands still use
            // argument arrays and never invoke a shell.
            .arg("-c")
            .arg("while :; do printf 1234567890 >> \"$1\"; sleep 0.01; done")
            .arg("sooqa-file-limit")
            .arg(output.to_string_lossy().into_owned());

        let error = ProcessCommandRunner
            .run_file(command, &output, 32)
            .await
            .expect_err("bounded output should stop the producer");
        assert!(error.is_output_limit_exceeded());
        assert!(
            tokio::fs::metadata(&output).await.expect("partial output should exist").len() > 32
        );
        tokio::fs::remove_dir_all(root).await.expect("test root should be removed");
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

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelling_process_runner_kills_and_reaps_descendants() {
        use std::{os::unix::fs::PermissionsExt, process::Stdio};

        let script =
            std::env::temp_dir().join(format!("sooqa-cancel-process-{}.sh", uuid::Uuid::new_v4()));
        let pid_file = script.with_extension("pid");
        std::fs::write(
            &script,
            "#!/bin/sh\nsleep 30 &\nchild=$!\nprintf '%s' \"$child\" > \"$1\"\nwait \"$child\"\n",
        )
        .expect("cancellation fixture should be written");
        let mut permissions = std::fs::metadata(&script)
            .expect("cancellation fixture metadata should be readable")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions)
            .expect("cancellation fixture should be executable");

        let command = ExternalCommand::new(&script)
            .arg(pid_file.to_string_lossy().into_owned())
            .timeout(Duration::from_secs(30));
        let task = tokio::spawn(async move { ProcessCommandRunner.run(command).await });
        let mut child_pid = None;
        for _ in 0..100 {
            if let Ok(pid) = tokio::fs::read_to_string(&pid_file).await {
                child_pid = Some(pid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        task.abort();
        assert!(task.await.expect_err("runner task should be cancelled").is_cancelled());

        let child_pid = child_pid.expect("fixture should have started its child");
        let mut terminated = false;
        for _ in 0..100 {
            let status = std::process::Command::new("/bin/kill")
                .arg("-0")
                .arg(child_pid.trim())
                .stderr(Stdio::null())
                .status()
                .expect("kill probe should run");
            if !status.success() {
                terminated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(terminated, "cancellation should kill and reap the descendant");
        let _ = tokio::fs::remove_file(&pid_file).await;
        let _ = tokio::fs::remove_file(&script).await;
    }
}
