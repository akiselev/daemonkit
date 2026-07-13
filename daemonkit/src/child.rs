use crate::{
    native::{
        bootstrap::{
            self, BootstrapCommand, BOOTSTRAP_MARKER_ENV, BOOTSTRAP_NONCE_ENV, BOOTSTRAP_VERSION,
        },
        transport::NativeListener,
    },
    protocol::{
        read_control_request, read_frame, server_handshake, write_control_response, ConnectionRole,
        ControlMessage, ControlResponse, ControlSession,
    },
};
use crate::{AuthenticatedStream, Error, PublishedEndpoint, Spawn};
use futures_core::Stream;
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};
use std::{
    error::Error as StdError,
    ffi::OsString,
    future::Future,
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Validated one-shot inherited bootstrap channel.
pub struct Bootstrap {
    nonce: String,
}

impl Bootstrap {
    pub fn detect() -> Result<Option<Self>, BootstrapError> {
        let Some(marker) = std::env::var_os(BOOTSTRAP_MARKER_ENV) else {
            return Ok(None);
        };
        if marker != "stdio-v1" {
            return Err(BootstrapError::Version);
        }
        let nonce = std::env::var(BOOTSTRAP_NONCE_ENV).map_err(|_| BootstrapError::Malformed)?;
        if nonce.len() != 64 || !nonce.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(BootstrapError::Malformed);
        }
        Ok(Some(Self { nonce }))
    }

    pub async fn run_embedded<S>(self, service: S) -> Result<(), Error>
    where
        S: EmbeddedService,
    {
        self.run_embedded_core(move |context, incoming, shutdown| async move {
            service
                .run(context, incoming, shutdown)
                .await
                .map_err(|error| error.to_string())
        })
        .await
    }

    pub async fn run_embedded_fn<F, Fut, E>(self, service: F) -> Result<(), Error>
    where
        F: FnOnce(ServiceContext, Incoming, Shutdown) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        self.run_embedded_core(move |context, incoming, shutdown| async move {
            service(context, incoming, shutdown)
                .await
                .map_err(|error| error.to_string())
        })
        .await
    }

    pub async fn run_managed(self, child: ManagedChild) -> Result<(), Error> {
        let _ = child;
        Err(Error::Unsupported)
    }
}

impl Bootstrap {
    async fn run_embedded_core<F, Fut>(self, service: F) -> Result<(), Error>
    where
        F: FnOnce(ServiceContext, Incoming, Shutdown) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let mut input = tokio::io::stdin();
        let launch =
            match bootstrap::decode(&read_frame(&mut input).await.map_err(bootstrap_error)?)
                .map_err(bootstrap_error)?
            {
                BootstrapCommand::Launch(launch) => launch,
                BootstrapCommand::Commit { .. } => {
                    return Err(bootstrap_error("commit before launch"))
                }
            };
        if launch.version != BOOTSTRAP_VERSION || launch.nonce != self.nonce {
            return Err(bootstrap_error("bootstrap version or nonce mismatch"));
        }
        let _lease = crate::native::locks::StableLock::new(&launch.lease_path)
            .acquire(Duration::from_secs(10))
            .map_err(bootstrap_error)?;
        let mut listener = NativeListener::bind(&launch.endpoint).map_err(bootstrap_error)?;
        let identity = launch.server_identity(STATE_PUBLISHED);

        // Authentication of the final endpoint is the Prepared acknowledgement.
        // The child still cannot serve application bytes until Commit arrives.
        let (preparing, role) = {
            let prepared = async {
                let mut preparing = listener.accept().await.map_err(bootstrap_error)?;
                let (_, role) =
                    server_handshake(&mut preparing, &identity, Duration::from_secs(10))
                        .await
                        .map_err(bootstrap_error)?;
                Ok::<_, Error>((preparing, role))
            };
            let early_commit = async {
                let bytes = read_frame(&mut input).await.map_err(bootstrap_error)?;
                bootstrap::decode(&bytes).map_err(bootstrap_error)
            };
            tokio::pin!(prepared);
            tokio::pin!(early_commit);
            tokio::select! {
                prepared = &mut prepared => prepared?,
                early = &mut early_commit => {
                    let _ = early?;
                    return Err(bootstrap_error("commit arrived before endpoint authentication"));
                }
            }
        };
        if role != ConnectionRole::Control {
            return Err(bootstrap_error("prepared probe used application role"));
        }
        drop(preparing);

        let commit = bootstrap::decode(&read_frame(&mut input).await.map_err(bootstrap_error)?)
            .map_err(bootstrap_error)?;
        match commit {
            BootstrapCommand::Commit { version, nonce }
                if version == BOOTSTRAP_VERSION && nonce == self.nonce => {}
            _ => return Err(bootstrap_error("invalid or mismatched commit")),
        }

        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(64);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let state = Arc::new(AtomicU8::new(STATE_READY));
        let active_streams = Arc::new(AtomicUsize::new(0));
        let (activity_tx, activity_rx) = tokio::sync::watch::channel(tokio::time::Instant::now());
        let accept_state = state.clone();
        let accept_active = active_streams.clone();
        let accept_identity = launch.server_identity(STATE_READY);
        let accept_shutdown = shutdown_tx.clone();
        let accept_task = tokio::spawn(async move {
            accept_loop(
                &mut listener,
                accept_identity,
                accept_state,
                incoming_tx,
                accept_shutdown,
                accept_active,
                activity_tx,
            )
            .await
        });
        let idle_task = launch.idle_timeout_millis.map(|millis| {
            tokio::spawn(idle_monitor(
                Duration::from_millis(millis),
                state.clone(),
                active_streams,
                activity_rx,
                shutdown_tx.clone(),
            ))
        });
        let context = ServiceContext {
            generation: crate::GenerationId(u128::from_be_bytes(launch.generation)),
        };
        let incoming = Incoming {
            receiver: incoming_rx,
        };
        let shutdown = Shutdown {
            receiver: shutdown_rx,
        };
        let service_result = service(context, incoming, shutdown).await;
        state.store(STATE_STOPPING, Ordering::SeqCst);
        let _ = shutdown_tx.send(true);
        accept_task.abort();
        if let Some(task) = idle_task {
            task.abort();
        }
        service_result.map_err(bootstrap_error)
    }
}

const STATE_PUBLISHED: u8 = 2;
const STATE_READY: u8 = 3;
const STATE_QUIESCING: u8 = 4;
const STATE_STOPPING: u8 = 5;

async fn accept_loop(
    listener: &mut NativeListener,
    identity: crate::protocol::ServerIdentity,
    state: Arc<AtomicU8>,
    incoming: tokio::sync::mpsc::Sender<Result<AuthenticatedStream, AcceptError>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    active_streams: Arc<AtomicUsize>,
    activity: tokio::sync::watch::Sender<tokio::time::Instant>,
) -> Result<(), Error> {
    loop {
        let mut stream = listener.accept().await.map_err(bootstrap_error)?;
        let mut connection_identity = identity.clone();
        connection_identity.state = state.load(Ordering::SeqCst);
        let (_, role) = match server_handshake(
            &mut stream,
            &connection_identity,
            Duration::from_secs(10),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => continue,
        };
        match role {
            ConnectionRole::Application if state.load(Ordering::SeqCst) == STATE_READY => {
                active_streams.fetch_add(1, Ordering::SeqCst);
                let _ = activity.send(tokio::time::Instant::now());
                if state.load(Ordering::SeqCst) != STATE_READY {
                    active_streams.fetch_sub(1, Ordering::SeqCst);
                    let _ = activity.send(tokio::time::Instant::now());
                    continue;
                }
                let tracked = TrackedStream {
                    inner: stream,
                    active_streams: active_streams.clone(),
                    activity: activity.clone(),
                };
                if incoming
                    .send(Ok(AuthenticatedStream::from_stream(tracked)))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            ConnectionRole::Application => {}
            ConnectionRole::Control => {
                let connection_state = state.clone();
                let connection_shutdown = shutdown.clone();
                let connection_active = active_streams.clone();
                let capabilities = identity.capabilities;
                tokio::spawn(async move {
                    while let Ok(request) = read_control_request(&mut stream).await {
                        let current = connection_state.load(Ordering::SeqCst);
                        let mut session =
                            ControlSession::new(ConnectionRole::Control, current, capabilities);
                        let response = match session.handle(&request, &[]) {
                            Ok(response) => response,
                            Err(_) => break,
                        };
                        match (&request, &response) {
                            (ControlMessage::Drain { .. }, ControlResponse::Accepted { .. }) => {
                                connection_state.store(STATE_QUIESCING, Ordering::SeqCst);
                                while connection_active.load(Ordering::SeqCst) != 0 {
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                            }
                            (ControlMessage::Shutdown { .. }, ControlResponse::Accepted { .. }) => {
                                connection_state.store(STATE_STOPPING, Ordering::SeqCst);
                                let _ = connection_shutdown.send(true);
                            }
                            _ => {}
                        }
                        if write_control_response(&mut stream, &response)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            }
        }
    }
}

async fn idle_monitor(
    timeout: Duration,
    state: Arc<AtomicU8>,
    active_streams: Arc<AtomicUsize>,
    mut activity: tokio::sync::watch::Receiver<tokio::time::Instant>,
    shutdown: tokio::sync::watch::Sender<bool>,
) {
    loop {
        let deadline = *activity.borrow() + timeout;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                if active_streams.load(Ordering::SeqCst) == 0
                    && state.compare_exchange(
                        STATE_READY,
                        STATE_QUIESCING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ).is_ok()
                {
                    let _ = shutdown.send(true);
                    return;
                }
            }
            changed = activity.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

struct TrackedStream {
    inner: crate::native::transport::NativeStream,
    active_streams: Arc<AtomicUsize>,
    activity: tokio::sync::watch::Sender<tokio::time::Instant>,
}

impl Drop for TrackedStream {
    fn drop(&mut self) {
        self.active_streams.fetch_sub(1, Ordering::SeqCst);
        let _ = self.activity.send(tokio::time::Instant::now());
    }
}

impl AsyncRead for TrackedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for TrackedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

fn bootstrap_error(error: impl std::fmt::Display) -> Error {
    Error::Bootstrap {
        message: error.to_string(),
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
