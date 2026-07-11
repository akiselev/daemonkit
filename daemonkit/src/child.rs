use crate::{AuthenticatedStream, Error, PublishedEndpoint, Spawn};
use futures_core::Stream;
use std::{
    error::Error as StdError,
    ffi::OsString,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// Validated one-shot inherited bootstrap channel.
pub struct Bootstrap {
    _private: (),
}

impl Bootstrap {
    /// Phase 3 adds the native inherited-handle parser. Public environment
    /// variables are deliberately not interpreted as daemon authority.
    pub fn detect() -> Result<Option<Self>, BootstrapError> {
        Ok(None)
    }

    pub async fn run_embedded<S>(self, service: S) -> Result<(), Error>
    where
        S: EmbeddedService,
    {
        let _ = service;
        Err(Error::Unsupported)
    }

    pub async fn run_embedded_fn<F, Fut, E>(self, service: F) -> Result<(), Error>
    where
        F: FnOnce(ServiceContext, Incoming, Shutdown) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        let _ = service;
        Err(Error::Unsupported)
    }

    pub async fn run_managed(self, child: ManagedChild) -> Result<(), Error> {
        let _ = child;
        Err(Error::Unsupported)
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BootstrapError {
    #[error("inherited bootstrap channel has an unsupported version")]
    Version,
    #[error("inherited bootstrap channel is malformed")]
    Malformed,
    #[error("failed to inspect inherited bootstrap channel: {0}")]
    Io(#[from] std::io::Error),
}

pub trait EmbeddedService: Send + 'static {
    type Error: StdError + Send + Sync + 'static;
    fn run(
        self,
        context: ServiceContext,
        incoming: Incoming,
        shutdown: Shutdown,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Clone, Debug)]
pub struct ServiceContext {
    generation: crate::GenerationId,
}
impl ServiceContext {
    pub fn generation(&self) -> crate::GenerationId {
        self.generation
    }
}

#[derive(Debug, thiserror::Error)]
#[error("authenticated accept failed: {message}")]
pub struct AcceptError {
    message: String,
}

pub struct Incoming {
    receiver: tokio::sync::mpsc::Receiver<Result<AuthenticatedStream, AcceptError>>,
}
impl Stream for Incoming {
    type Item = Result<AuthenticatedStream, AcceptError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

pub struct Shutdown {
    receiver: tokio::sync::watch::Receiver<bool>,
}
impl Shutdown {
    pub async fn requested(&mut self) {
        while !*self.receiver.borrow() && self.receiver.changed().await.is_ok() {}
    }
}

pub struct ManagedChild {
    spawn: Spawn,
    readiness: Readiness,
    endpoint: EndpointPublication,
    shutdown: ShutdownStrategy,
    tree_policy: ProcessTreePolicy,
    logs: ChildLogPolicy,
}
impl ManagedChild {
    pub fn new(program: impl Into<OsString>) -> Self {
        Self {
            spawn: Spawn::program(program),
            readiness: Readiness::immediate(),
            endpoint: EndpointPublication::None,
            shutdown: ShutdownStrategy::graceful_then_kill(),
            tree_policy: ProcessTreePolicy::OwnedTree,
            logs: ChildLogPolicy::default(),
        }
    }
    pub fn arg(mut self, value: impl Into<OsString>) -> Self {
        self.spawn = self.spawn.arg(value);
        self
    }
    pub fn args<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.spawn = self.spawn.args(values);
        self
    }
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.spawn = self.spawn.env(key, value);
        self
    }
    pub fn current_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.spawn = self.spawn.current_dir(path);
        self
    }
    pub fn readiness(mut self, value: Readiness) -> Self {
        self.readiness = value;
        self
    }
    pub fn service_endpoint(mut self, value: EndpointPublication) -> Self {
        self.endpoint = value;
        self
    }
    pub fn shutdown(mut self, value: ShutdownStrategy) -> Self {
        self.shutdown = value;
        self
    }
    pub fn tree_policy(mut self, value: ProcessTreePolicy) -> Self {
        self.tree_policy = value;
        self
    }
    pub fn logs(mut self, value: ChildLogPolicy) -> Self {
        self.logs = value;
        self
    }
}

type ReadinessFuture = Pin<Box<dyn Future<Output = Result<ReadinessResult, String>> + Send>>;
type ReadinessProbe = Box<dyn FnOnce(ReadinessContext) -> ReadinessFuture + Send>;
pub enum Readiness {
    Immediate,
    TcpConnect(SocketAddr),
    Custom(ReadinessProbe),
}
impl Readiness {
    pub fn immediate() -> Self {
        Self::Immediate
    }
    pub fn tcp_connect(endpoint: SocketAddr) -> Self {
        Self::TcpConnect(endpoint)
    }
    pub fn custom<F, Fut, E>(probe: F) -> Self
    where
        F: FnOnce(ReadinessContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<ReadinessResult, E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        Self::Custom(Box::new(move |context| {
            Box::pin(async move { probe(context).await.map_err(|error| error.to_string()) })
        }))
    }
}

pub struct ReadinessContext {
    stdout: OutputEvents,
    stderr: OutputEvents,
    elapsed: Duration,
}
impl ReadinessContext {
    pub fn stdout(&mut self) -> &mut OutputEvents {
        &mut self.stdout
    }
    pub fn stderr(&mut self) -> &mut OutputEvents {
        &mut self.stderr
    }
    pub async fn exited(&mut self) -> ProcessExit {
        ProcessExit { code: None }
    }
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}

pub struct OutputEvents {
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
}
impl Stream for OutputEvents {
    type Item = Vec<u8>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessExit {
    pub code: Option<i32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadinessResult {
    service_endpoint: Option<PublishedEndpoint>,
}
impl ReadinessResult {
    pub fn ready() -> Self {
        Self::default()
    }
    pub fn service_endpoint(mut self, endpoint: impl Into<PublishedEndpoint>) -> Self {
        self.service_endpoint = Some(endpoint.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointPublication {
    None,
    FromReadiness,
    Fixed(PublishedEndpoint),
}
impl EndpointPublication {
    pub fn from_readiness() -> Self {
        Self::FromReadiness
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShutdownStrategy {
    graceful_timeout: Duration,
    terminate_timeout: Duration,
}
impl ShutdownStrategy {
    pub fn graceful_then_kill() -> Self {
        Self {
            graceful_timeout: Duration::from_secs(10),
            terminate_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessTreePolicy {
    OwnedTree,
    DirectChildOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildLogPolicy {
    pub tail_bytes: usize,
}
impl Default for ChildLogPolicy {
    fn default() -> Self {
        Self {
            tail_bytes: 64 * 1024,
        }
    }
}

impl From<SocketAddr> for PublishedEndpoint {
    fn from(value: SocketAddr) -> Self {
        Self::Tcp(value)
    }
}
impl From<PathBuf> for PublishedEndpoint {
    fn from(value: PathBuf) -> Self {
        Self::Path(value)
    }
}
