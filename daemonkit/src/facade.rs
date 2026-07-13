use crate::{ConfigError, DaemonKey, DaemonSpec, Error, GenerationId, InstanceId};
use sha2::{Digest, Sha256};
use std::{
    ffi::{OsStr, OsString},
    fmt,
    marker::PhantomData,
    path::PathBuf,
};

/// Sealed production platform bundle.
pub trait Platform: private::Sealed + Clone + Send + Sync + 'static {}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultPlatform;
impl private::Sealed for DefaultPlatform {}
impl Platform for DefaultPlatform {}

mod private {
    pub trait Sealed {}
}

#[derive(Clone)]
pub struct Spawn {
    program: OsString,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
    current_dir: Option<PathBuf>,
    redacted_args: Vec<usize>,
    redacted_env: Vec<OsString>,
}

impl Spawn {
    pub fn current_exe() -> Result<Self, ConfigError> {
        std::env::current_exe()
            .map(Self::program)
            .map_err(|error| ConfigError::CurrentExecutable(error.to_string()))
    }

    pub fn program(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            current_dir: None,
            redacted_args: Vec::new(),
            redacted_env: Vec::new(),
        }
    }

    pub fn arg(mut self, value: impl Into<OsString>) -> Self {
        self.args.push(value.into());
        self
    }

    pub fn args<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(values.into_iter().map(Into::into));
        self
    }

    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    pub fn current_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(path.into());
        self
    }

    pub fn redact_arg(mut self, index: usize) -> Result<Self, ConfigError> {
        if index >= self.args.len() {
            return Err(ConfigError::InvalidIdentifier(format!(
                "redacted argument index {index} is out of range"
            )));
        }
        self.redacted_args.push(index);
        Ok(self)
    }

    pub fn redact_env(mut self, key: impl Into<OsString>) -> Self {
        self.redacted_env.push(key.into());
        self
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.program.is_empty() {
            Err(ConfigError::EmptyProgram)
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for Spawn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args: Vec<&OsStr> = self
            .args
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if self.redacted_args.contains(&index) {
                    OsStr::new("[REDACTED]")
                } else {
                    value.as_os_str()
                }
            })
            .collect();
        let env: Vec<(&OsStr, &OsStr)> = self
            .env
            .iter()
            .map(|(key, value)| {
                let value = if self.redacted_env.iter().any(|redacted| redacted == key) {
                    OsStr::new("[REDACTED]")
                } else {
                    value.as_os_str()
                };
                (key.as_os_str(), value)
            })
            .collect();
        f.debug_struct("Spawn")
            .field("program", &self.program)
            .field("args", &args)
            .field("env", &env)
            .field("current_dir", &self.current_dir)
            .finish()
    }
}

#[derive(Clone)]
struct Controller<P> {
    instance: InstanceId,
    platform: P,
}

fn controller<K: DaemonKey>(spec: DaemonSpec<K>) -> Controller<DefaultPlatform> {
    let mut hash = Sha256::new();
    hash.update(b"daemonkit:instance:v1\0");
    hash.update(format!("{:?}", spec.app));
    hash.update(spec.key.digest().0);
    if let Some(isolation) = spec.isolation {
        hash.update(format!("{isolation:?}"));
    }
    // Policy fields are deliberately consumed into the controller construction
    // boundary even before native orchestration lands.
    let _policy = (
        spec.startup_timeout,
        spec.shutdown_timeout,
        spec.drain_timeout,
        spec.idle_timeout,
        spec.compatibility,
        spec.logs,
        spec.transport,
        spec.runtime_root,
    );
    Controller {
        instance: InstanceId(hash.finalize().into()),
        platform: DefaultPlatform,
    }
}

pub struct Daemon<M, P = DefaultPlatform> {
    controller: Controller<P>,
    spawn: Spawn,
    _mode: PhantomData<fn() -> M>,
}

pub struct DaemonClient<M, P = DefaultPlatform> {
    controller: Controller<P>,
    _mode: PhantomData<fn() -> M>,
}

impl Daemon<crate::Embedded> {
    pub fn embedded<K: DaemonKey>(spec: DaemonSpec<K>, spawn: Spawn) -> Result<Self, ConfigError> {
        spawn.validate()?;
        Ok(Self {
            controller: controller(spec),
            spawn,
            _mode: PhantomData,
        })
    }
}

impl Daemon<crate::Managed> {
    pub fn managed<K: DaemonKey>(spec: DaemonSpec<K>, spawn: Spawn) -> Result<Self, ConfigError> {
        spawn.validate()?;
        Ok(Self {
            controller: controller(spec),
            spawn,
            _mode: PhantomData,
        })
    }
}

impl<M, P: Platform> Daemon<M, P> {
    pub async fn ensure(&self) -> Result<Instance<M>, Error> {
        let _ = (&self.spawn, &self.controller.platform);
        Err(Error::Unsupported)
    }

    pub async fn status(&self) -> Result<InstanceStatus, Error> {
        Err(Error::Unsupported)
    }

    pub async fn stop(&self) -> Result<StopOutcome, Error> {
        Err(Error::Unsupported)
    }

    pub async fn restart(&self) -> Result<Instance<M>, Error> {
        Err(Error::Unsupported)
    }

    pub async fn repair(&self) -> Result<RepairOutcome, Error> {
        Err(Error::Unsupported)
    }

    pub fn client(&self) -> DaemonClient<M, P> {
        DaemonClient {
            controller: self.controller.clone(),
            _mode: PhantomData,
        }
    }

    pub fn paths(&self) -> InstancePaths<'_> {
        InstancePaths {
            instance: &self.controller.instance,
        }
    }
}

impl DaemonClient<crate::Embedded> {
    pub fn embedded<K: DaemonKey>(spec: DaemonSpec<K>) -> Result<Self, ConfigError> {
        Ok(Self {
            controller: controller(spec),
            _mode: PhantomData,
        })
    }

    #[cfg(feature = "runtime-tokio")]
    pub async fn connect(&self) -> Result<AuthenticatedStream, Error> {
        Err(Error::Unsupported)
    }
}

impl DaemonClient<crate::Managed> {
    pub fn managed<K: DaemonKey>(spec: DaemonSpec<K>) -> Result<Self, ConfigError> {
        Ok(Self {
            controller: controller(spec),
            _mode: PhantomData,
        })
    }

    pub async fn service_endpoint(&self) -> Result<PublishedEndpoint, Error> {
        Err(Error::Unsupported)
    }
}

impl<M, P: Platform> DaemonClient<M, P> {
    pub async fn attach(&self) -> Result<Option<Instance<M>>, Error> {
        let _ = &self.controller.platform;
        Err(Error::Unsupported)
    }

    pub async fn status(&self) -> Result<InstanceStatus, Error> {
        Err(Error::Unsupported)
    }

    pub async fn stop(&self) -> Result<StopOutcome, Error> {
        Err(Error::Unsupported)
    }

    pub fn paths(&self) -> InstancePaths<'_> {
        InstancePaths {
            instance: &self.controller.instance,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Instance<M> {
    id: InstanceId,
    generation: GenerationId,
    summary: InstanceSummary,
    service_endpoint: Option<PublishedEndpoint>,
    _mode: PhantomData<fn() -> M>,
}

impl<M> Instance<M> {
    pub fn id(&self) -> &InstanceId {
        &self.id
    }

    pub fn generation(&self) -> GenerationId {
        self.generation
    }

    pub fn summary(&self) -> &InstanceSummary {
        &self.summary
    }

    pub async fn status(&self) -> Result<GenerationStatus, Error> {
        Err(Error::Unsupported)
    }

    pub async fn stop(self) -> Result<StopOutcome, Error> {
        Err(Error::Unsupported)
    }
}

impl Instance<crate::Embedded> {
    #[cfg(feature = "runtime-tokio")]
    pub async fn connect(&self) -> Result<AuthenticatedStream, Error> {
        Err(Error::Unsupported)
    }
}

impl Instance<crate::Managed> {
    pub fn service_endpoint(&self) -> Result<&PublishedEndpoint, Error> {
        self.service_endpoint.as_ref().ok_or(Error::Unavailable {
            generation: Some(self.generation),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceSummary {
    pub generation: GenerationId,
    pub process_id: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InstanceStatus {
    Absent,
    Starting(InstanceSummary),
    Ready(InstanceSummary),
    Quiescing(InstanceSummary),
    Stopping(InstanceSummary),
    Unhealthy { record: Option<InstanceSummary> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GenerationStatus {
    Ready,
    Quiescing,
    Stopping,
    Exited,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopOutcome {
    AlreadyAbsent,
    Stopped {
        generation: GenerationId,
        forced: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairAction {
    RemovedStaleManifest,
    RemovedOwnedEndpoint,
    RecoveredInterruptedPublication,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairOutcome {
    Clean,
    Repaired { actions: Vec<RepairAction> },
    Busy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublishedEndpoint {
    Tcp(std::net::SocketAddr),
    Path(PathBuf),
    Opaque(String),
}

pub struct InstancePaths<'a> {
    instance: &'a InstanceId,
}

impl InstancePaths<'_> {
    pub fn instance_id(&self) -> &InstanceId {
        self.instance
    }
}

#[cfg(feature = "runtime-tokio")]
pub trait TransportIo:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static
{
}

#[cfg(feature = "runtime-tokio")]
impl<T> TransportIo for T where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static
{
}

#[cfg(feature = "runtime-tokio")]
pub struct AuthenticatedStream(std::pin::Pin<Box<dyn TransportIo>>);

#[cfg(feature = "runtime-tokio")]
impl AuthenticatedStream {
    pub(crate) fn from_stream(stream: impl TransportIo) -> Self {
        Self(Box::pin(stream))
    }
}

#[cfg(feature = "runtime-tokio")]
impl tokio::io::AsyncRead for AuthenticatedStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.0.as_mut().poll_read(context, buffer)
    }
}

#[cfg(feature = "runtime-tokio")]
impl tokio::io::AsyncWrite for AuthenticatedStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        self.0.as_mut().poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.0.as_mut().poll_flush(context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.0.as_mut().poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Compatibility, DaemonSpec};

    #[test]
    fn spawn_debug_applies_argument_and_environment_redaction() {
        let spawn = Spawn::program("worker")
            .arg("visible")
            .arg("canary-argument")
            .env("TOKEN", "canary-environment")
            .redact_arg(1)
            .unwrap()
            .redact_env("TOKEN");
        let debug = format!("{spawn:?}");
        assert!(!debug.contains("canary-argument"));
        assert!(!debug.contains("canary-environment"));
        assert!(debug.contains("visible"));
    }

    #[test]
    fn common_constructors_infer_mode_and_platform() {
        let spec = DaemonSpec::new("com.example.worker")
            .unwrap()
            .compatibility(Compatibility::current_binary());
        let daemon = Daemon::embedded(spec, Spawn::program("worker")).unwrap();
        let _: DaemonClient<crate::Embedded> = daemon.client();

        let managed = Daemon::managed(
            DaemonSpec::new("com.example.managed").unwrap(),
            Spawn::program("supervisor"),
        )
        .unwrap();
        let _: DaemonClient<crate::Managed> = managed.client();
    }

    #[test]
    fn instance_identity_is_stable_for_same_spec() {
        let one = DaemonClient::embedded(DaemonSpec::new("com.example.worker").unwrap()).unwrap();
        let two = DaemonClient::embedded(DaemonSpec::new("com.example.worker").unwrap()).unwrap();
        assert_eq!(one.paths().instance_id(), two.paths().instance_id());
    }
}
