use crate::{
    native::{
        bootstrap::{
            self, BootstrapCommand, BOOTSTRAP_MARKER_ENV, BOOTSTRAP_NONCE_ENV, BOOTSTRAP_VERSION,
        },
        transport::{NativeListener, NativeStream},
    },
    protocol::{
        read_control_request, read_frame, server_handshake, write_control_response, ConnectionRole,
        ControlMessage, ControlResponse, ControlSession,
    },
};
use crate::{AuthenticatedStream, Error, PublishedEndpoint, Spawn};
use futures_core::Stream;
use std::io::Write as _;
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::{
    collections::VecDeque,
    error::Error as StdError,
    ffi::OsString,
    fs::OpenOptions,
    future::Future,
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::process::Child;

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

    pub async fn run_managed(self, mut child: ManagedChild) -> Result<(), Error> {
        child.spawn.validate()?;
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

        let (mut managed, readiness_result) = start_managed(&mut child, &launch).await?;
        let endpoint = select_managed_endpoint(&child.endpoint, readiness_result)?;
        let encoded_endpoint = endpoint
            .as_ref()
            .map(service_endpoint_record)
            .map(|record| serde_json::to_string(&record))
            .transpose()
            .map_err(bootstrap_error)?;
        if encoded_endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.len() > 2048)
        {
            return Err(bootstrap_error("managed service endpoint is too large"));
        }

        let mut listener = NativeListener::bind(&launch.endpoint).map_err(bootstrap_error)?;
        let mut identity = launch.server_identity(STATE_PUBLISHED);
        identity.capabilities = crate::protocol::Capabilities::STATUS
            .union(crate::protocol::Capabilities::DRAIN)
            .union(crate::protocol::Capabilities::SHUTDOWN)
            .union(crate::protocol::Capabilities::DIAGNOSTICS);
        identity.service_endpoint = encoded_endpoint;
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
                    return Err(bootstrap_error("commit arrived before managed readiness authentication"));
                }
            }
        };
        if role != ConnectionRole::Control {
            return Err(bootstrap_error(
                "managed prepared probe used application role",
            ));
        }
        drop(preparing);
        let commit = bootstrap::decode(&read_frame(&mut input).await.map_err(bootstrap_error)?)
            .map_err(bootstrap_error)?;
        match commit {
            BootstrapCommand::Commit { version, nonce }
                if version == BOOTSTRAP_VERSION && nonce == self.nonce => {}
            _ => return Err(bootstrap_error("invalid or mismatched managed commit")),
        }

        let state = Arc::new(AtomicU8::new(STATE_READY));
        identity.state = STATE_READY;
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let control_state = state.clone();
        let control_task = tokio::spawn(async move {
            managed_control_loop(&mut listener, identity, control_state, shutdown_tx).await
        });
        enum Completion {
            Exited(std::process::ExitStatus),
            Shutdown,
        }
        let completion = tokio::select! {
            result = managed.child.wait() => Completion::Exited(result.map_err(bootstrap_error)?),
            _ = wait_shutdown(&mut shutdown_rx) => Completion::Shutdown,
        };
        match completion {
            Completion::Exited(status) => {
                if child.tree_policy == ProcessTreePolicy::OwnedTree {
                    // The wrapper may exit while descendants still retain output
                    // pipes. Ownership outlives the direct child handle.
                    force_managed(&mut managed, child.tree_policy).await?;
                }
                managed.release_tree().await;
                control_task.abort();
                managed.finish_output().await;
                if status.success() {
                    Ok(())
                } else {
                    Err(bootstrap_error(format!(
                        "managed child exited with {status}"
                    )))
                }
            }
            Completion::Shutdown => {
                state.store(STATE_STOPPING, Ordering::SeqCst);
                terminate_managed(&mut managed, &child.shutdown, child.tree_policy).await?;
                managed.release_tree().await;
                control_task.abort();
                managed.finish_output().await;
                Ok(())
            }
        }
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
        let drain_timeout = launch
            .drain_timeout_millis
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(10));
        let accept_task = tokio::spawn(async move {
            accept_loop(
                &mut listener,
                accept_identity,
                accept_state,
                incoming_tx,
                accept_shutdown,
                accept_active,
                activity_tx,
                drain_timeout,
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

#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    listener: &mut NativeListener,
    identity: crate::protocol::ServerIdentity,
    state: Arc<AtomicU8>,
    incoming: tokio::sync::mpsc::Sender<Result<AuthenticatedStream, AcceptError>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    active_streams: Arc<AtomicUsize>,
    activity: tokio::sync::watch::Sender<tokio::time::Instant>,
    drain_timeout: Duration,
) -> Result<(), Error> {
    let mut consecutive_failures: u32 = 0;
    loop {
        // A transient accept failure (EMFILE under load, a peer aborting
        // mid-accept) must not kill the loop: that would leave a zombie
        // daemon holding the lease with its socket gone. A persistent
        // cascade instead requests shutdown so the process exits and a
        // fresh generation can start.
        let stream = match listener.accept().await {
            Ok(stream) => {
                consecutive_failures = 0;
                stream
            }
            Err(error) => {
                consecutive_failures += 1;
                if consecutive_failures >= 32 {
                    let _ = shutdown.send(true);
                    return Err(bootstrap_error(format!(
                        "accept failed {consecutive_failures} times consecutively: {error}"
                    )));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        // Handshakes run per-connection: one peer that connects and never
        // sends its hello must not head-of-line-block every other client
        // for the whole handshake deadline.
        let connection = ConnectionContext {
            identity: identity.clone(),
            state: state.clone(),
            incoming: incoming.clone(),
            shutdown: shutdown.clone(),
            active_streams: active_streams.clone(),
            activity: activity.clone(),
            drain_timeout,
        };
        tokio::spawn(handle_accepted(stream, connection));
    }
}

struct ConnectionContext {
    identity: crate::protocol::ServerIdentity,
    state: Arc<AtomicU8>,
    incoming: tokio::sync::mpsc::Sender<Result<AuthenticatedStream, AcceptError>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    active_streams: Arc<AtomicUsize>,
    activity: tokio::sync::watch::Sender<tokio::time::Instant>,
    drain_timeout: Duration,
}

async fn handle_accepted(mut stream: NativeStream, connection: ConnectionContext) {
    let mut connection_identity = connection.identity;
    connection_identity.state = connection.state.load(Ordering::SeqCst);
    let (_, role) =
        match server_handshake(&mut stream, &connection_identity, Duration::from_secs(10)).await {
            Ok(result) => result,
            Err(_) => return,
        };
    match role {
        ConnectionRole::Application if connection.state.load(Ordering::SeqCst) == STATE_READY => {
            connection.active_streams.fetch_add(1, Ordering::SeqCst);
            let _ = connection.activity.send(tokio::time::Instant::now());
            if connection.state.load(Ordering::SeqCst) != STATE_READY {
                connection.active_streams.fetch_sub(1, Ordering::SeqCst);
                let _ = connection.activity.send(tokio::time::Instant::now());
                return;
            }
            let tracked = TrackedStream {
                inner: stream,
                active_streams: connection.active_streams.clone(),
                activity: connection.activity.clone(),
            };
            let _ = connection
                .incoming
                .send(Ok(AuthenticatedStream::from_stream(tracked)))
                .await;
        }
        ConnectionRole::Application => {}
        ConnectionRole::Control => {
            let connection_state = connection.state.clone();
            let connection_shutdown = connection.shutdown.clone();
            let connection_active = connection.active_streams.clone();
            let capabilities = connection_identity.capabilities;
            let drain_timeout = connection.drain_timeout;
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
                        // Bounded drain: waiting forever on one long-lived
                        // application stream would withhold the response,
                        // stall this control loop (the follow-up Shutdown
                        // would never be read), and wedge the instance in
                        // QUIESCING. On expiry the instance returns to
                        // READY: an abandoned drain must not poison the
                        // generation for every future client.
                        connection_state.store(STATE_QUIESCING, Ordering::SeqCst);
                        let deadline = tokio::time::Instant::now() + drain_timeout;
                        while connection_active.load(Ordering::SeqCst) != 0
                            && tokio::time::Instant::now() < deadline
                        {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        // Always return to READY once the drain wait ends. The
                        // authoritative stop is the follow-up Shutdown, which
                        // this same loop reads next and which CASes to STOPPING.
                        // Reverting only when streams remained (the old guard)
                        // left QUIESCING latched whenever a drain arrived at
                        // zero active streams and no Shutdown followed (e.g. the
                        // controller died between Drain and Shutdown): every
                        // future Application handshake then got BusyQuiescing
                        // forever while ensure() kept re-attaching via the
                        // ungated Control role. A concurrent Shutdown that
                        // already moved us to STOPPING makes this CAS a no-op.
                        let _ = connection_state.compare_exchange(
                            STATE_QUIESCING,
                            STATE_READY,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        );
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
                // The deadline fired but we could not shut down: a stream is
                // still active, or the state already left READY. Re-arming the
                // already-elapsed deadline would busy-spin a core (sleep_until
                // on a past instant resolves immediately, forever). Wait for
                // the next activity update instead — a stream drop always sends
                // one, re-arming a fresh deadline. Cap the wait at `timeout` so
                // a state transition back to READY (which does not ping
                // activity) is still re-evaluated within one idle period.
                match tokio::time::timeout(timeout, activity.changed()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => return,
                    Err(_) => {}
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

struct ManagedRuntime {
    child: Child,
    #[cfg(unix)]
    process_id: u32,
    stdout_task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    stderr_task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    #[cfg(windows)]
    job: WindowsJob,
    #[cfg(unix)]
    watchdog: Option<UnixWatchdog>,
}

impl ManagedRuntime {
    async fn finish_output(&mut self) {
        // A descendant can accidentally inherit a pipe and keep it open after the
        // managed process exits. Output EOF is therefore bounded independently
        // from process termination.
        for task in [&mut self.stdout_task, &mut self.stderr_task] {
            if let Some(mut task) = task.take() {
                if tokio::time::timeout(Duration::from_secs(2), &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                }
            }
        }
    }

    async fn release_tree(&mut self) {
        #[cfg(unix)]
        if let Some(watchdog) = self.watchdog.take() {
            watchdog.finish().await;
        }
    }
}

async fn start_managed(
    config: &mut ManagedChild,
    launch: &bootstrap::LaunchConfig,
) -> Result<(ManagedRuntime, ReadinessResult), Error> {
    let mut command = config.spawn.command();
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(0x0000_0004 | 0x0000_0200 | 0x0800_0000); // SUSPENDED | NEW_PROCESS_GROUP | NO_WINDOW

    let mut process = command.spawn().map_err(bootstrap_error)?;
    #[cfg(unix)]
    let process_id = process
        .id()
        .ok_or_else(|| bootstrap_error("managed child has no process id"))?;
    #[cfg(windows)]
    let job = WindowsJob::assign(&process).map_err(bootstrap_error)?;
    #[cfg(windows)]
    job.resume(&process).map_err(bootstrap_error)?;
    #[cfg(unix)]
    let watchdog = match config.tree_policy {
        ProcessTreePolicy::OwnedTree => {
            Some(UnixWatchdog::spawn(process_id).map_err(bootstrap_error)?)
        }
        ProcessTreePolicy::DirectChildOnly => None,
    };

    let stdout_pipe = process
        .stdout
        .take()
        .ok_or_else(|| bootstrap_error("managed stdout pipe was not created"))?;
    let stderr_pipe = process
        .stderr
        .take()
        .ok_or_else(|| bootstrap_error("managed stderr pipe was not created"))?;
    let (stdout_tx, stdout) = output_events(config.logs.tail_bytes);
    let (stderr_tx, stderr) = output_events(config.logs.tail_bytes);
    let stdout_task = tokio::spawn(capture_output(
        stdout_pipe,
        launch.generation_directory.join("managed-stdout.log"),
        stdout_tx,
    ));
    let stderr_task = tokio::spawn(capture_output(
        stderr_pipe,
        launch.generation_directory.join("managed-stderr.log"),
        stderr_tx,
    ));
    let readiness = std::mem::replace(&mut config.readiness, Readiness::Immediate);
    let readiness_timeout = launch
        .readiness_timeout_millis
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_READINESS_TIMEOUT);
    let result =
        wait_for_readiness(readiness, &mut process, stdout, stderr, readiness_timeout).await;
    if let Err(error) = result {
        let mut runtime = ManagedRuntime {
            child: process,
            #[cfg(unix)]
            process_id,
            stdout_task: Some(stdout_task),
            stderr_task: Some(stderr_task),
            #[cfg(windows)]
            job,
            #[cfg(unix)]
            watchdog,
        };
        let _ = force_managed(&mut runtime, config.tree_policy).await;
        runtime.release_tree().await;
        runtime.finish_output().await;
        return Err(error);
    }
    Ok((
        ManagedRuntime {
            child: process,
            #[cfg(unix)]
            process_id,
            stdout_task: Some(stdout_task),
            stderr_task: Some(stderr_task),
            #[cfg(windows)]
            job,
            #[cfg(unix)]
            watchdog,
        },
        result.expect("readiness result checked above"),
    ))
}

async fn capture_output<R>(mut input: R, path: PathBuf, events: OutputSink) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut output = OpenOptions::new().create(true).append(true).open(path)?;
    let mut buffer = vec![0; 16 * 1024];
    loop {
        let read = input.read(&mut buffer).await?;
        if read == 0 {
            output.flush()?;
            return Ok(());
        }
        output.write_all(&buffer[..read])?;
        // Logging is authoritative. A slow readiness observer sheds the oldest
        // events rather than exerting backpressure on a child's output pipes.
        events.send(buffer[..read].to_vec());
    }
}

/// Fallback managed-readiness bound when the parent did not send one (older
/// parent, or the consumer left `DaemonSpec::readiness_timeout` unset).
const DEFAULT_READINESS_TIMEOUT: Duration = Duration::from_secs(30);

async fn wait_for_readiness(
    readiness: Readiness,
    process: &mut Child,
    stdout: OutputEvents,
    stderr: OutputEvents,
    timeout: Duration,
) -> Result<ReadinessResult, Error> {
    let started = tokio::time::Instant::now();
    match readiness {
        Readiness::Immediate => {
            reject_early_exit(process)?;
            Ok(ReadinessResult::ready())
        }
        Readiness::TcpConnect(endpoint) => {
            let probe = async {
                loop {
                    reject_early_exit(process)?;
                    match tokio::net::TcpStream::connect(endpoint).await {
                        Ok(stream) => {
                            drop(stream);
                            return Ok(ReadinessResult::ready().service_endpoint(endpoint));
                        }
                        Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                    }
                }
            };
            tokio::time::timeout(timeout, probe).await.map_err(|_| {
                bootstrap_error(format!("managed readiness timed out after {timeout:?}"))
            })?
        }
        Readiness::Custom(probe) => {
            let (exit_tx, exit_rx) = tokio::sync::watch::channel(None);
            let monitor = async {
                loop {
                    if let Some(status) = process.try_wait().map_err(bootstrap_error)? {
                        let exit = ProcessExit {
                            code: status.code(),
                        };
                        let _ = exit_tx.send(Some(exit));
                        return Err(bootstrap_error(format!(
                            "managed child exited before readiness with {status}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            let context = ReadinessContext {
                stdout,
                stderr,
                elapsed: started.elapsed(),
                exit: exit_rx,
            };
            let custom = async { probe(context).await.map_err(bootstrap_error) };
            tokio::time::timeout(timeout, async {
                tokio::select! {
                    result = custom => result,
                    result = monitor => result,
                }
            })
            .await
            .map_err(|_| {
                bootstrap_error(format!("managed readiness timed out after {timeout:?}"))
            })?
        }
    }
}

fn reject_early_exit(process: &mut Child) -> Result<(), Error> {
    match process.try_wait().map_err(bootstrap_error)? {
        Some(status) => Err(bootstrap_error(format!(
            "managed child exited before readiness with {status}"
        ))),
        None => Ok(()),
    }
}

fn select_managed_endpoint(
    publication: &EndpointPublication,
    readiness: ReadinessResult,
) -> Result<Option<PublishedEndpoint>, Error> {
    match publication {
        EndpointPublication::None => Ok(None),
        EndpointPublication::Fixed(endpoint) => Ok(Some(endpoint.clone())),
        EndpointPublication::FromReadiness => {
            readiness.service_endpoint.map(Some).ok_or_else(|| {
                bootstrap_error(
                    "endpoint publication requested a readiness endpoint, but none was returned",
                )
            })
        }
    }
}

fn service_endpoint_record(endpoint: &PublishedEndpoint) -> crate::record::ServiceEndpointRecord {
    match endpoint {
        PublishedEndpoint::Tcp(value) => crate::record::ServiceEndpointRecord::Tcp(*value),
        PublishedEndpoint::Path(value) => crate::record::ServiceEndpointRecord::Path(value.clone()),
        PublishedEndpoint::Opaque(value) => {
            crate::record::ServiceEndpointRecord::Opaque(value.clone())
        }
    }
}

async fn managed_control_loop(
    listener: &mut NativeListener,
    identity: crate::protocol::ServerIdentity,
    state: Arc<AtomicU8>,
    shutdown: tokio::sync::watch::Sender<bool>,
) -> Result<(), Error> {
    loop {
        let mut stream = listener.accept().await.map_err(bootstrap_error)?;
        let mut current_identity = identity.clone();
        current_identity.state = state.load(Ordering::SeqCst);
        let (_, role) =
            match server_handshake(&mut stream, &current_identity, Duration::from_secs(10)).await {
                Ok(result) => result,
                Err(_) => continue,
            };
        if role != ConnectionRole::Control {
            continue;
        }
        let connection_state = state.clone();
        let connection_shutdown = shutdown.clone();
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

async fn wait_shutdown(shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

async fn terminate_managed(
    runtime: &mut ManagedRuntime,
    strategy: &ShutdownStrategy,
    policy: ProcessTreePolicy,
) -> Result<(), Error> {
    if runtime.child.try_wait().map_err(bootstrap_error)?.is_some() {
        return Ok(());
    }
    request_graceful(runtime, policy)?;
    if tokio::time::timeout(strategy.graceful_timeout, runtime.child.wait())
        .await
        .is_ok()
    {
        return Ok(());
    }
    force_managed(runtime, policy).await?;
    tokio::time::timeout(strategy.terminate_timeout, runtime.child.wait())
        .await
        .map_err(|_| bootstrap_error("managed process tree did not exit after forced termination"))?
        .map_err(bootstrap_error)?;
    Ok(())
}

#[cfg(unix)]
fn request_graceful(runtime: &mut ManagedRuntime, policy: ProcessTreePolicy) -> Result<(), Error> {
    let target = match policy {
        ProcessTreePolicy::OwnedTree => -(runtime.process_id as i32),
        ProcessTreePolicy::DirectChildOnly => runtime.process_id as i32,
    };
    let result = unsafe { libc::kill(target, libc::SIGTERM) };
    if result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(bootstrap_error(io::Error::last_os_error()))
    }
}

#[cfg(windows)]
fn request_graceful(
    _runtime: &mut ManagedRuntime,
    _policy: ProcessTreePolicy,
) -> Result<(), Error> {
    // There is no protocol-independent graceful signal for arbitrary Windows
    // processes. The grace interval permits an application-requested exit; the
    // Job Object is the authoritative escalation mechanism.
    Ok(())
}

async fn force_managed(
    runtime: &mut ManagedRuntime,
    policy: ProcessTreePolicy,
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        let target = match policy {
            ProcessTreePolicy::OwnedTree => -(runtime.process_id as i32),
            ProcessTreePolicy::DirectChildOnly => runtime.process_id as i32,
        };
        let result = unsafe { libc::kill(target, libc::SIGKILL) };
        if result != 0 && io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err(bootstrap_error(io::Error::last_os_error()));
        }
    }
    #[cfg(windows)]
    match policy {
        ProcessTreePolicy::OwnedTree => runtime.job.terminate().map_err(bootstrap_error)?,
        ProcessTreePolicy::DirectChildOnly => {
            runtime.child.start_kill().map_err(bootstrap_error)?
        }
    }
    Ok(())
}

#[cfg(windows)]
struct WindowsJob(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl WindowsJob {
    fn assign(child: &Child) -> io::Result<Self> {
        use std::mem::size_of;
        use windows_sys::Win32::{
            Foundation::{CloseHandle, INVALID_HANDLE_VALUE},
            System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
                SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            },
        };
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let mut information: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&information as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        let child_handle = child
            .raw_handle()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "child handle is closed"))?;
        if configured == 0 || unsafe { AssignProcessToJobObject(handle, child_handle as _) } == 0 {
            let error = io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(error);
        }
        Ok(Self(handle))
    }

    fn terminate(&self) -> io::Result<()> {
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        if unsafe { TerminateJobObject(self.0, 1) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn resume(&self, child: &Child) -> io::Result<()> {
        #[link(name = "ntdll")]
        unsafe extern "system" {
            fn NtResumeProcess(process: windows_sys::Win32::Foundation::HANDLE) -> i32;
        }
        let handle = child
            .raw_handle()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "child handle is closed"))?;
        let status = unsafe { NtResumeProcess(handle as _) };
        if status < 0 {
            Err(io::Error::other(format!(
                "NtResumeProcess failed with NTSTATUS 0x{:08x}",
                status as u32
            )))
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
impl Drop for WindowsJob {
    fn drop(&mut self) {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

/// A tiny detached reaper closes Unix's supervisor-crash gap. It owns no
/// application descriptors: EOF on its private pipe means the supervisor died
/// or deliberately released ownership, at which point it kills the verified
/// managed process group and exits.
#[cfg(unix)]
struct UnixWatchdog {
    writer: libc::c_int,
    process_id: libc::pid_t,
}

#[cfg(unix)]
impl UnixWatchdog {
    fn spawn(process_id: u32) -> io::Result<Self> {
        let mut pipe = [-1; 2];
        if unsafe { libc::pipe(pipe.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        for descriptor in pipe {
            if unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
                let error = io::Error::last_os_error();
                unsafe {
                    libc::close(pipe[0]);
                    libc::close(pipe[1]);
                }
                return Err(error);
            }
        }
        let maximum_descriptor = unsafe { libc::sysconf(libc::_SC_OPEN_MAX) };
        let maximum_descriptor = if maximum_descriptor > 0 {
            maximum_descriptor.min(65_536) as libc::c_int
        } else {
            1024
        };
        let watchdog = unsafe { libc::fork() };
        if watchdog == -1 {
            let error = io::Error::last_os_error();
            unsafe {
                libc::close(pipe[0]);
                libc::close(pipe[1]);
            }
            return Err(error);
        }
        if watchdog == 0 {
            unsafe {
                libc::close(pipe[1]);
                // Escape the supervisor's group so an external group kill does
                // not race the watchdog before it can clean the managed group.
                let _ = libc::setsid();
                for descriptor in 0..maximum_descriptor {
                    if descriptor != pipe[0] {
                        libc::close(descriptor);
                    }
                }
                let mut byte = 0_u8;
                loop {
                    let result = libc::read(pipe[0], (&mut byte as *mut u8).cast(), 1);
                    if result == 0 {
                        break;
                    }
                    if result < 0 && io::Error::last_os_error().raw_os_error() != Some(libc::EINTR)
                    {
                        break;
                    }
                }
                let _ = libc::kill(-(process_id as libc::pid_t), libc::SIGKILL);
                libc::_exit(0);
            }
        }
        unsafe { libc::close(pipe[0]) };
        Ok(Self {
            writer: pipe[1],
            process_id: watchdog,
        })
    }

    async fn finish(mut self) {
        self.close_writer();
        let process_id = self.process_id;
        let _ = tokio::task::spawn_blocking(move || unsafe {
            libc::waitpid(process_id, std::ptr::null_mut(), 0)
        })
        .await;
    }

    fn close_writer(&mut self) {
        if self.writer >= 0 {
            unsafe { libc::close(self.writer) };
            self.writer = -1;
        }
    }
}

#[cfg(unix)]
impl Drop for UnixWatchdog {
    fn drop(&mut self) {
        self.close_writer();
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
    exit: tokio::sync::watch::Receiver<Option<ProcessExit>>,
}
impl ReadinessContext {
    pub fn stdout(&mut self) -> &mut OutputEvents {
        &mut self.stdout
    }
    pub fn stderr(&mut self) -> &mut OutputEvents {
        &mut self.stderr
    }
    pub async fn exited(&mut self) -> ProcessExit {
        loop {
            if let Some(exit) = *self.exit.borrow() {
                return exit;
            }
            if self.exit.changed().await.is_err() {
                return ProcessExit { code: None };
            }
        }
    }
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}

pub struct OutputEvents {
    shared: Arc<Mutex<OutputEventState>>,
}
impl Stream for OutputEvents {
    type Item = Vec<u8>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(event) = state.events.pop_front() {
            return Poll::Ready(Some(event));
        }
        if state.closed {
            return Poll::Ready(None);
        }
        if state
            .waker
            .as_ref()
            .map_or(true, |current| !current.will_wake(cx.waker()))
        {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

struct OutputSink {
    shared: Arc<Mutex<OutputEventState>>,
}

struct OutputEventState {
    events: VecDeque<Vec<u8>>,
    retained_bytes: usize,
    capacity_bytes: usize,
    closed: bool,
    waker: Option<Waker>,
}

fn output_events(capacity_bytes: usize) -> (OutputSink, OutputEvents) {
    let shared = Arc::new(Mutex::new(OutputEventState {
        events: VecDeque::new(),
        retained_bytes: 0,
        capacity_bytes: capacity_bytes.max(1),
        closed: false,
        waker: None,
    }));
    (
        OutputSink {
            shared: shared.clone(),
        },
        OutputEvents { shared },
    )
}

impl OutputSink {
    fn send(&self, event: Vec<u8>) {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while !state.events.is_empty()
            && state.retained_bytes.saturating_add(event.len()) > state.capacity_bytes
        {
            if let Some(discarded) = state.events.pop_front() {
                state.retained_bytes = state.retained_bytes.saturating_sub(discarded.len());
            }
        }
        let retained = if event.len() > state.capacity_bytes {
            let offset = event.len() - state.capacity_bytes;
            event[offset..].to_vec()
        } else {
            event
        };
        state.retained_bytes = state.retained_bytes.saturating_add(retained.len());
        state.events.push_back(retained);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

impl Drop for OutputSink {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
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

    /// Configures the application grace interval and the final kill wait.
    pub fn with_timeouts(graceful_timeout: Duration, terminate_timeout: Duration) -> Self {
        Self {
            graceful_timeout,
            terminate_timeout,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn output_events_retain_newest_bytes_without_backpressure() {
        let (sink, mut events) = output_events(4);
        sink.send(b"old".to_vec());
        sink.send(b"new!".to_vec());
        drop(sink);

        let newest = std::future::poll_fn(|context| Pin::new(&mut events).poll_next(context))
            .await
            .unwrap();
        assert_eq!(newest, b"new!");
        assert!(
            std::future::poll_fn(|context| Pin::new(&mut events).poll_next(context))
                .await
                .is_none()
        );
    }
}
