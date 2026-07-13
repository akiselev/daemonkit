use crate::{ConfigError, GenerationId};
use std::{fmt, path::PathBuf};

/// Stable identity used in operation diagnostics.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InstanceId(pub(crate) [u8; 32]);

/// Startup boundary at which an error occurred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartupPhase {
    StateDirectory,
    LifecycleLock,
    BootstrapChannel,
    Spawn,
    Prepare,
    Authenticate,
    Publish,
    Commit,
}

/// Safe startup context. Secret command values must already be redacted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupDiagnostics {
    pub phase: StartupPhase,
    pub command: String,
    pub process_id: Option<u32>,
    pub stdout_tail: Vec<u8>,
    pub stderr_tail: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub log_path: Option<PathBuf>,
    pub instance_path: PathBuf,
}

/// Public lifecycle error skeleton. Variants will gain typed backend sources as
/// their native implementations land.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("instance state directory is unsafe: {path:?}: {message}")]
    StateDirectory { path: PathBuf, message: String },
    #[error("timed out acquiring the lifecycle lock for {instance:?}")]
    Lock { instance: InstanceId },
    #[error("daemon startup failed during {phase:?}: {message}")]
    Startup {
        phase: StartupPhase,
        diagnostics: Box<StartupDiagnostics>,
        message: String,
    },
    #[error("daemon generation changed")]
    GenerationChanged {
        expected: GenerationId,
        observed: Option<GenerationId>,
    },
    #[error("daemon authentication failed")]
    AuthenticationFailed,
    #[error("daemon bootstrap failed: {message}")]
    Bootstrap { message: String },
    #[error("daemon is unavailable")]
    Unavailable { generation: Option<GenerationId> },
    #[error("instance state is corrupt at {path:?}: {message}")]
    CorruptState { path: PathBuf, message: String },
    #[error("operation is not implemented by the current backend")]
    Unsupported,
}

impl Error {
    pub fn startup_diagnostics(&self) -> Option<&StartupDiagnostics> {
        match self {
            Self::Startup { diagnostics, .. } => Some(diagnostics),
            _ => None,
        }
    }
}

/// Opaque source text with redacted formatting.
pub struct RedactedSource(String);

impl RedactedSource {
    pub fn new(secret_source: impl Into<String>) -> Self {
        Self(secret_source.into())
    }
}

impl fmt::Debug for RedactedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _retained_source_bytes = self.0.len();
        f.write_str("RedactedSource([REDACTED])")
    }
}

impl fmt::Display for RedactedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl std::error::Error for RedactedSource {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_source_formatting_is_redacted() {
        let source = RedactedSource::new("canary-secret");
        assert!(!format!("{source:?}: {source}").contains("canary-secret"));
        assert_eq!(source.0, "canary-secret");
    }
}
