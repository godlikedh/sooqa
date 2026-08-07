use std::{ffi::OsString, path::PathBuf, sync::Arc, time::Duration};

use crate::{ExternalCommand, ExternalCommandRunner};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BinaryCheck {
    pub name: String,
    pub executable: PathBuf,
    pub version_args: Vec<OsString>,
}

impl BinaryCheck {
    pub fn new(
        name: impl Into<String>,
        executable: impl Into<PathBuf>,
        version_args: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Self {
        Self {
            name: name.into(),
            executable: executable.into(),
            version_args: version_args.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BinaryDiagnostic {
    pub name: String,
    pub executable: PathBuf,
    pub version: Option<String>,
    pub error: Option<String>,
}

impl BinaryDiagnostic {
    pub fn available(&self) -> bool {
        self.version.is_some() && self.error.is_none()
    }
}

pub async fn diagnose_binaries(
    runner: Arc<dyn ExternalCommandRunner>,
    checks: &[BinaryCheck],
    timeout: Duration,
) -> Vec<BinaryDiagnostic> {
    let mut diagnostics = Vec::with_capacity(checks.len());
    for check in checks {
        diagnostics.push(diagnose_binary(Arc::clone(&runner), check, timeout).await);
    }
    diagnostics
}

async fn diagnose_binary(
    runner: Arc<dyn ExternalCommandRunner>,
    check: &BinaryCheck,
    timeout: Duration,
) -> BinaryDiagnostic {
    let command = check
        .version_args
        .iter()
        .cloned()
        .fold(ExternalCommand::new(check.executable.clone()), |command, arg| command.arg(arg))
        .timeout(timeout);
    match runner.run(command).await {
        Ok(output) if output.success && !output.stdout_truncated && !output.stderr_truncated => {
            BinaryDiagnostic {
                name: check.name.clone(),
                executable: check.executable.clone(),
                version: first_line(&output.stdout),
                error: None,
            }
        }
        Ok(output) => BinaryDiagnostic {
            name: check.name.clone(),
            executable: check.executable.clone(),
            version: None,
            error: Some(command_failure(&output)),
        },
        Err(error) => BinaryDiagnostic {
            name: check.name.clone(),
            executable: check.executable.clone(),
            version: None,
            error: Some(error.to_string()),
        },
    }
}

fn first_line(bytes: &[u8]) -> Option<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
}

fn command_failure(output: &crate::ExternalCommandOutput) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if stderr.is_empty() {
        format!("command exited unsuccessfully with status {:?}", output.exit_code)
    } else {
        format!("command exited unsuccessfully with status {:?}: {stderr}", output.exit_code)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::{CommandError, ExternalCommandOutput};

    struct FakeRunner {
        calls: Mutex<Vec<ExternalCommand>>,
    }

    #[async_trait]
    impl ExternalCommandRunner for FakeRunner {
        async fn run(
            &self,
            command: ExternalCommand,
        ) -> Result<ExternalCommandOutput, CommandError> {
            self.calls.lock().expect("test mutex should not be poisoned").push(command);
            Ok(ExternalCommandOutput {
                success: true,
                exit_code: Some(0),
                stdout: b"ffprobe version 7.0\n".to_vec(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            })
        }
    }

    #[tokio::test]
    async fn diagnostics_capture_version_without_exposing_command_output() {
        let runner = Arc::new(FakeRunner { calls: Mutex::new(Vec::new()) });
        let checks = [BinaryCheck::new("ffprobe", "ffprobe", ["-version"])];
        let diagnostics = diagnose_binaries(runner, &checks, Duration::from_secs(1)).await;

        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].available());
        assert_eq!(diagnostics[0].version.as_deref(), Some("ffprobe version 7.0"));
    }
}
