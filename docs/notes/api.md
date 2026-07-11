## Core recommendation

Use a **hybrid API**:

1. A concrete, ergonomic façade for the normal case.
2. A statically generic internal kernel for transport, locking, spawning, storage, and service execution.
3. A smaller object-safe extension layer for runtime-selected transports or external integrations.
4. GATs only where a returned value genuinely borrows from the implementing object.

Do not expose a type such as this to ordinary users:

```rust
Daemon<
    MyKey,
    TokioRuntime,
    InterprocessTransport,
    Fs4LockBackend,
    JsonManifestStore,
    PlatformSpawner,
    ...
>
```

That would be flexible but hostile to inference, documentation, compile times, error messages, and semver.

The normal API should look closer to:

```rust
let daemon = Daemon::new("com.akiselev.debugger-cli")
    .key("default")
    .service(DebuggerService::new(config))
    .build()?;

let connection = daemon.ensure().await?;
```

Advanced users can reach a lower-level generic API:

```rust
let daemon = DaemonBuilder::with_platform(CustomPlatform {
    runtime,
    transport,
    locks,
    process,
    store,
})
.key(project_key)
.spawn(spawn_spec)
.build()?;
```

GATs are stable and allow an associated type to be parameterized by lifetimes, types, or consts. Their main benefit here is expressing families of guards, transactions, streams, or futures that borrow from `self`. ([Rust Documentation][1]) ([Rust Blog][2])

# Start from the user-facing concepts

I would make these the principal public types:

```rust
pub struct DaemonSpec<K = String> {
    app: AppId,
    key: K,
    isolation: IsolationId,
    compatibility: Compatibility,
    lifecycle: LifecyclePolicy,
    logging: LogPolicy,
}

pub struct Daemon<S, P = DefaultPlatform> {
    spec: DaemonSpec<S::Key>,
    service: S,
    platform: P,
}

pub struct DaemonClient<K = String> {
    spec: DaemonSpec<K>,
}

pub struct RunningInstance {
    identity: InstanceIdentity,
    control: ControlClient,
    service_endpoint: Option<Endpoint>,
}
```

There are really three roles:

* `Daemon`: code that can run or spawn the daemon.
* `DaemonClient`: code that only discovers, ensures, stops, or connects.
* `RunningInstance`: a verified connection to one specific generation.

That distinction matters because not every executable needs to contain the daemon implementation. For example, Ghidra’s client might know only how to ensure a supervisor, while a hidden subcommand contains the supervisor implementation.

## Basic embedded-service API

For `debugger-cli`:

```rust
#[derive(Clone)]
struct DebuggerDaemon {
    config: Config,
}

impl EmbeddedService for DebuggerDaemon {
    type Key = GlobalKey;
    type Error = debugger::Error;

    async fn run(
        self,
        context: ServiceContext,
        mut incoming: IncomingConnections,
        shutdown: Shutdown,
    ) -> Result<(), Self::Error> {
        let actor = DebuggerActor::spawn(self.config);

        loop {
            tokio::select! {
                Some(connection) = incoming.next() => {
                    actor.serve(connection?).await?;
                }
                _ = shutdown.requested() => {
                    actor.shutdown().await?;
                    return Ok(());
                }
            }
        }
    }
}
```

Invocation:

```rust
let daemon = Daemon::embedded(
    DaemonSpec::new("com.akiselev.debugger-cli")
        .global()
        .idle_timeout(Duration::from_minutes(30)),
    DebuggerDaemon { config },
);

let mut client = daemon.ensure().await?;
client.send(StartRequest { target }).await?;
```

## Managed-child API

For `ghidra-cli`:

```rust
let daemon = Daemon::managed(
    DaemonSpec::new("com.akiselev.ghidra-cli")
        .key(ProjectKey::canonical(&project_path)?)
        .compatibility(Compatibility::restart_on_mismatch()),
    ManagedChild::new(analyze_headless)
        .args(args)
        .readiness(Readiness::tcp_json_ping {
            command: json!({ "command": "ping" }),
        })
        .shutdown(ShutdownStrategy::protocol_then_kill())
        .publish_service_endpoint(),
);

let instance = daemon.ensure().await?;
let endpoint = instance.service_endpoint()?;
```

This lets the crate own the supervisor lifecycle without requiring it to proxy the application protocol.

# Builder design options

## Option A: one large configuration struct

```rust
pub struct DaemonOptions {
    pub transport: Transport,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub logging: LogPolicy,
    pub restart: RestartPolicy,
}
```

Usage:

```rust
let daemon = Daemon::new(app, key, service)
    .options(DaemonOptions {
        transport: Transport::Local,
        startup_timeout: Duration::from_secs(10),
        ..Default::default()
    });
```

Advantages:

* Simple types.
* Excellent compile errors.
* Stable public API.
* Easy serialization and configuration-file integration.
* Runtime selection of options.

Costs:

* Invalid combinations can exist temporarily.
* Some platform-specific options are irrelevant on other platforms.
* Extension points tend to become enums with many variants.
* Custom transports require either trait objects or changes to the enum.

This is the correct default façade.

## Option B: type-state builder

```rust
pub struct DaemonBuilder<K, S, T = Missing, P = Missing> {
    key: K,
    service: S,
    transport: T,
    process: P,
}
```

Usage:

```rust
let daemon = DaemonBuilder::new(app)
    .key(project_key)
    .service(service)
    .transport(LocalTransport::default())
    .process(CurrentExecutable::hidden_subcommand("__daemon"))
    .build();
```

Advantages:

* Missing mandatory components are compile-time errors.
* Invalid embedded/managed combinations can be prohibited.
* The final daemon is fully statically dispatched.
* No runtime branches for policy selection.

Costs:

* Type signatures become large.
* Compiler diagnostics expose implementation details.
* Adding or changing builder states can become a semver concern.
* Wrapper functions often need verbose bounds.
* IDE hover output becomes noisy.
* Compile time and monomorphization increase.

Use type state only for a few structural distinctions:

```rust
DaemonBuilder<NoKey, NoService>
DaemonBuilder<HasKey<K>, Embedded<S>>
DaemonBuilder<HasKey<K>, Managed<C>>
```

Do not encode every timeout, logging option, transport property, and restart policy in the type system.

## Option C: typestate through separate constructors

This is usually more ergonomic:

```rust
Daemon::embedded(spec, service)
Daemon::managed(spec, child)
Daemon::external(spec, endpoint_resolver)
```

Each constructor produces a valid mode directly.

Advantages:

* Most type-state correctness without a complex builder.
* Clear documentation.
* Better inference.
* Easier semver evolution.

Costs:

* Slight duplication among constructors.
* Less “one universal builder” aesthetic.

I would use this design.

# The generic kernel

The lower-level architecture should use a `Platform` bundle:

```rust
pub trait Platform: Clone + Send + Sync + 'static {
    type Runtime: Runtime;
    type Transport: Transport;
    type Locks: LockBackend;
    type Processes: ProcessBackend;
    type Store: InstanceStore;
    type Clock: Clock;

    fn runtime(&self) -> &Self::Runtime;
    fn transport(&self) -> &Self::Transport;
    fn locks(&self) -> &Self::Locks;
    fn processes(&self) -> &Self::Processes;
    fn store(&self) -> &Self::Store;
    fn clock(&self) -> &Self::Clock;
}
```

Default:

```rust
pub struct TokioPlatform {
    runtime: TokioRuntime,
    transport: NativeLocalTransport,
    locks: NativeFileLocks,
    processes: NativeProcesses,
    store: JsonInstanceStore,
    clock: SystemClock,
}
```

This avoids carrying six generic parameters everywhere:

```rust
pub struct Daemon<S, P = TokioPlatform> {
    service: S,
    platform: P,
}
```

A platform bundle is less algebraically pure than independent parameters, but much easier to use.

For testing:

```rust
let platform = TestPlatform::new()
    .with_fake_clock()
    .with_process_backend(FaultInjectingProcesses::at(
        FailurePoint::AfterEndpointBind,
    ));

let daemon = Daemon::with_platform(spec, service, platform);
```

# Where GATs are genuinely useful

## 1. Lifecycle lock guards

This is an excellent GAT use because the guard should borrow the lock backend or an opened lock file:

```rust
pub trait LockBackend {
    type Guard<'a>: LifecycleGuard + 'a
    where
        Self: 'a;

    fn try_lock<'a>(
        &'a self,
        instance: &'a InstancePaths,
    ) -> Result<Option<Self::Guard<'a>>, LockError>;
}
```

Implementation:

```rust
pub struct FileLockBackend;

pub struct FileLockGuard<'a> {
    backend: &'a FileLockBackend,
    file: std::fs::File,
}

impl LockBackend for FileLockBackend {
    type Guard<'a> = FileLockGuard<'a>;

    fn try_lock<'a>(
        &'a self,
        instance: &'a InstancePaths,
    ) -> Result<Option<Self::Guard<'a>>, LockError> {
        // Open stable lifecycle.lock and acquire the OS lock.
        todo!()
    }
}
```

Why this is better than a non-GAT associated type:

```rust
trait LockBackend {
    type Guard;
}
```

Without the GAT, the guard generally must own everything it depends on, use `Arc`, or erase the borrow. The GAT lets the guard borrow safely and cheaply.

However, there is a simpler alternative:

```rust
pub struct LifecycleLock {
    file: File,
}
```

The owning guard is easier and probably sufficient for an OS file lock. `fs4` exposes cross-platform whole-file advisory locks, but its documentation explicitly warns that lock semantics vary across platforms and duplicated or concurrently locked file handles require care. ([Docs.rs][3])

My recommendation: make the production guard owning. Use the GAT only in the internal abstraction if test backends or specialized stores benefit from borrowing.

## 2. Atomic store transactions

A stronger GAT use:

```rust
pub trait InstanceStore {
    type Transaction<'a>: InstanceTransaction + 'a
    where
        Self: 'a;

    fn begin<'a>(
        &'a self,
        paths: &'a InstancePaths,
    ) -> Result<Self::Transaction<'a>, StoreError>;

    fn read(
        &self,
        paths: &InstancePaths,
    ) -> Result<Option<InstanceRecord>, StoreError>;
}

pub trait InstanceTransaction {
    fn stage(&mut self, record: &InstanceRecord) -> Result<(), StoreError>;
    fn commit(self) -> Result<(), StoreError>;
}
```

Possible implementations:

* filesystem temporary file plus atomic rename;
* in-memory transactional test store;
* SQLite;
* registry-like Windows implementation;
* a future system-service-backed registry.

The transaction logically borrows the store and instance paths. A GAT models that relationship accurately.

Still, I would keep this trait private or under an `advanced` module. Most users should never implement a manifest store.

## 3. Borrowed stream halves

For a transport whose split halves borrow the original connection:

```rust
pub trait Connection {
    type Reader<'a>: AsyncRead + Unpin + Send + 'a
    where
        Self: 'a;

    type Writer<'a>: AsyncWrite + Unpin + Send + 'a
    where
        Self: 'a;

    fn split<'a>(
        &'a mut self,
    ) -> (Self::Reader<'a>, Self::Writer<'a>);
}
```

This permits:

* Tokio borrowed split halves;
* custom framed connections;
* shared-memory transports;
* test transports whose reader and writer borrow shared state.

But it prevents storing the halves independently beyond the connection borrow.

For daemon connections, owned halves are usually more useful:

```rust
pub trait Connection: Sized {
    type Reader: AsyncRead + Unpin + Send + 'static;
    type Writer: AsyncWrite + Unpin + Send + 'static;

    fn into_split(self) -> (Self::Reader, Self::Writer);
}
```

That allows separate read and write tasks.

Recommendation: prefer owned halves in the main transport contract. Add borrowed splitting as an optional secondary trait:

```rust
pub trait BorrowSplit: Connection {
    type Reader<'a>: AsyncRead + Unpin + 'a
    where Self: 'a;

    type Writer<'a>: AsyncWrite + Unpin + 'a
    where Self: 'a;

    fn split(&mut self) -> (Self::Reader<'_>, Self::Writer<'_>);
}
```

## 4. Async operations without boxed futures

A manual async trait using GATs could be:

```rust
pub trait Transport {
    type Listener: Listener<Connection = Self::Connection>;
    type Connection: Connection;

    type BindFuture<'a>: Future<
            Output = Result<Self::Listener, TransportError>,
        > + Send
        + 'a
    where
        Self: 'a;

    type ConnectFuture<'a>: Future<
            Output = Result<Self::Connection, TransportError>,
        > + Send
        + 'a
    where
        Self: 'a;

    fn bind<'a>(
        &'a self,
        endpoint: &'a EndpointSpec,
    ) -> Self::BindFuture<'a>;

    fn connect<'a>(
        &'a self,
        endpoint: &'a Endpoint,
    ) -> Self::ConnectFuture<'a>;
}
```

This is zero-allocation and permits futures borrowing `self`.

The implementation problem is naming the concrete future type. Stable Rust does not make every closure-generated future conveniently nameable in an associated type. Implementations often need:

* a named future struct;
* `Pin<Box<dyn Future<...>>>`;
* a helper macro;
* or RPITIT/`async fn` in the trait.

A modern internal trait can be simpler:

```rust
trait Transport {
    type Listener: Listener;
    type Connection: Connection;

    fn bind(
        &self,
        endpoint: &EndpointSpec,
    ) -> impl Future<Output = Result<Self::Listener, TransportError>> + Send;

    fn connect(
        &self,
        endpoint: &Endpoint,
    ) -> impl Future<Output = Result<Self::Connection, TransportError>> + Send;
}
```

Return-position `impl Trait` and `async fn` in traits have been stable since Rust 1.75. However, Rust’s own guidance notes that public RPITIT APIs make it difficult for downstream users to impose additional bounds, and these traits are not dynamically dispatchable. ([Rust Blog][4]) ([Rust Blog][4])

My choice:

* Use RPITIT for private or sealed internal traits.
* Use explicit associated future GATs only when implementing a runtime-independent core is genuinely valuable.
* Use boxed futures for object-safe extension traits.
* Do not expose handwritten future GATs in the normal public API.

## 5. Service handlers that borrow service state

This is a legitimate GAT model:

```rust
pub trait ConnectionService {
    type Connection: Connection;
    type Error;

    type ServeFuture<'a>: Future<Output = Result<(), Self::Error>>
        + Send
        + 'a
    where
        Self: 'a,
        Self::Connection: 'a;

    fn serve<'a>(
        &'a self,
        connection: Self::Connection,
        context: &'a ConnectionContext,
    ) -> Self::ServeFuture<'a>;
}
```

This permits a service future to borrow:

* immutable service configuration;
* shared actor handles;
* the connection context;
* metrics and tracing state.

But a simpler sealed trait can use RPITIT:

```rust
pub trait ConnectionService: Send + Sync + 'static {
    type Connection: Connection;
    type Error;

    fn serve(
        &self,
        connection: Self::Connection,
        context: &ConnectionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

Again, good for static dispatch, bad for `dyn ConnectionService`.

## 6. Key serialization without allocation

It is technically possible:

```rust
pub trait DaemonKey {
    type Bytes<'a>: AsRef<[u8]> + 'a
    where
        Self: 'a;

    fn stable_bytes(&self) -> Self::Bytes<'_>;
    fn display(&self) -> Cow<'_, str>;
}
```

Implementations could return:

* `&[u8]`;
* a borrowed `OsStr` encoding;
* a small stack buffer;
* a `Cow<[u8]>`.

This is too sophisticated for the benefit gained. Use:

```rust
pub trait DaemonKey {
    fn write_stable(
        &self,
        output: &mut dyn std::io::Write,
    ) -> Result<(), KeyError>;

    fn display(&self) -> Cow<'_, str>;
}
```

Or require:

```rust
pub trait DaemonKey {
    fn stable_digest(&self) -> KeyDigest;
    fn display(&self) -> Cow<'_, str>;
}
```

A key is computed once per invocation. Avoiding one allocation is not important.

# Where GATs are the wrong choice

## Runtime-selectable transports

A trait containing GATs is not dyn-compatible. The Rust Reference explicitly excludes traits with generic associated types from trait-object use. `async fn` and RPITIT methods are likewise not dyn-compatible. ([Rust Documentation][5])

This will not work:

```rust
let transport: Box<dyn Transport> = match config.transport {
    TransportKind::Local => Box::new(LocalTransport::new()),
    TransportKind::Tcp => Box::new(TcpTransport::new()),
};
```

when `Transport` contains GATs or RPITIT methods.

You then have three choices.

### Static enum dispatch

```rust
pub enum AnyTransport {
    Local(LocalTransport),
    Tcp(TcpTransport),
}

pub enum AnyListener {
    Local(LocalListener),
    Tcp(TcpListener),
}

pub enum AnyConnection {
    Local(LocalConnection),
    Tcp(TcpConnection),
}
```

This is close to what `interprocess` itself does: its portable local socket types use enum dispatch over platform implementations, and the underlying primitive is selected by platform and name type. ([Docs.rs][6])

Advantages:

* No boxing.
* Runtime selection.
* Exhaustive handling.
* Good performance.
* Easy platform defaults.

Costs:

* The crate must know each variant.
* Third-party transport plugins cannot extend the enum.
* Matching boilerplate.

For built-in transports, this is the best approach.

### Object-safe erased transport

```rust
pub trait DynTransport: Send + Sync {
    fn bind<'a>(
        &'a self,
        endpoint: &'a EndpointSpec,
    ) -> BoxFuture<'a, Result<Box<dyn DynListener>, TransportError>>;

    fn connect<'a>(
        &'a self,
        endpoint: &'a Endpoint,
    ) -> BoxFuture<'a, Result<Box<dyn DynConnection>, TransportError>>;
}
```

Advantages:

* Runtime plugins.
* Configuration can select an implementation.
* Stable ABI-like conceptual boundary.

Costs:

* Heap allocation for futures and connection objects.
* Dynamic dispatch.
* More complex downcasting for platform handles.
* Harder integration with APIs requiring concrete stream types.

For control-plane IPC, this overhead is immaterial. Startup occurs once and control calls are low-frequency.

### Generic static transport

```rust
pub struct Daemon<S, T> {
    service: S,
    transport: T,
}
```

Advantages:

* Maximum type safety and performance.
* Easy platform-specific APIs.
* No erased handles.

Costs:

* Generic propagation.
* Code bloat.
* Harder runtime configuration.
* Harder public API.

This is suitable for the internal daemon server loop, but not necessary for the ordinary client façade.

# Recommended two-level transport API

Use a concrete built-in transport enum publicly:

```rust
#[non_exhaustive]
pub enum TransportConfig {
    NativeLocal,
    TcpLoopback(TcpConfig),

    #[cfg(target_os = "linux")]
    LinuxAbstract(AbstractConfig),
}
```

The builder exposes:

```rust
impl DaemonSpec {
    pub fn transport(mut self, transport: TransportConfig) -> Self;
}
```

Internally:

```rust
enum BuiltinTransport {
    NativeLocal(NativeLocalTransport),
    Tcp(TcpTransport),

    #[cfg(target_os = "linux")]
    Abstract(AbstractTransport),
}
```

For advanced extensions:

```rust
pub fn custom_transport(
    mut self,
    transport: Arc<dyn DynTransport>,
) -> Self;
```

Then the internal runtime can normalize both forms:

```rust
enum SelectedTransport {
    Builtin(BuiltinTransport),
    Custom(Arc<dyn DynTransport>),
}
```

This gives you:

* fast and ergonomic defaults;
* runtime configuration;
* third-party extensibility;
* no GAT or associated-type leakage into the façade.

# Runtime abstraction options

You mentioned making the crate highly generic. The most tempting abstraction is executor independence:

```rust
pub trait Runtime {
    type Sleep<'a>: Future<Output = ()> + Send + 'a
    where Self: 'a;

    type Join<T>: Future<Output = Result<T, JoinError>> + Send
    where T: Send + 'static;

    fn sleep(&self, duration: Duration) -> Self::Sleep<'_>;

    fn spawn<F, T>(&self, future: F) -> Self::Join<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;
}
```

This looks attractive but expands rapidly. A full daemon runtime also needs:

* timers;
* signals;
* async process I/O;
* async sockets and pipes;
* task cancellation;
* filesystem operations;
* blocking-task execution;
* platform handles.

A generic runtime trait would either become a miniature Tokio portability layer or regress to lowest-common-denominator semantics.

Recommended approach:

* Make the default server runtime Tokio.
* Keep lifecycle state-machine logic independent of Tokio.
* Abstract clocks and fault injection for tests.
* Abstract transport and process operations where platform variation is real.
* Do not promise executor independence in version 1.

You can split the crate:

```text
daemon-core
    identity
    state machine
    protocol structs
    manifest model
    compatibility
    error taxonomy

daemon-runtime
    Tokio orchestration
    local transport
    startup channel
    locks
    process backend

daemon-testkit
    fake clock
    fake process backend
    in-memory store
    fault injection
```

This gives substantial genericity without building an async-runtime framework.

# Service API alternatives

## Model 1: crate owns the accept loop

```rust
pub trait RequestHandler {
    type Request;
    type Response;
    type Error;

    async fn handle(
        &self,
        request: Self::Request,
        context: RequestContext,
    ) -> Result<Self::Response, Self::Error>;
}
```

The crate handles framing, request IDs, concurrency, shutdown, and backpressure.

Advantages:

* Extremely ergonomic.
* Consistent concurrency and cancellation.
* Easy diagnostics.
* Strong protocol invariants.

Costs:

* The crate becomes an RPC framework.
* Applications may already have DAP, gRPC, JSON Lines, or custom framing.
* Streaming and notifications complicate the abstraction.
* Ghidra cannot easily reuse its direct Java protocol.

Reject this as the core design.

It might become an optional `daemon-rpc` companion crate later.

## Model 2: crate passes connections to the service

```rust
pub trait EmbeddedService {
    async fn run(
        self,
        context: ServiceContext,
        incoming: IncomingConnections,
        shutdown: Shutdown,
    ) -> Result<(), Self::Error>;
}
```

Advantages:

* Protocol neutral.
* Supports DAP, gRPC, JSON, and binary protocols.
* Service owns concurrency semantics.
* Appropriate for debugger-cli.

Costs:

* Application must implement framing and connection handling.
* Less automatic backpressure policy.
* Harder for the crate to report per-request diagnostics.

This should be the main embedded-service model.

## Model 3: crate runs a closure

```rust
Daemon::embedded_fn(spec, |context, incoming, shutdown| async move {
    serve(context, incoming, shutdown).await
});
```

Advantages:

* Minimal boilerplate.
* Good for small applications.
* No named service type required.

Costs:

* Complex closure bounds.
* Error types can become difficult to infer.
* Documentation is weaker than a named trait.

Support both the trait and closure form:

```rust
pub fn embedded_fn<F, Fut, E>(...)
where
    F: FnOnce(ServiceContext, IncomingConnections, Shutdown) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
```

This does not need GATs because the closure and its captures are moved into the daemon runtime.

# Process backend API

I would distinguish spawning from lifetime ownership:

```rust
pub trait ProcessBackend {
    type Child: ManagedProcess;

    fn spawn(
        &self,
        command: &CommandSpec,
        inheritance: &InheritancePlan,
        tree: ProcessTreePolicy,
    ) -> Result<Self::Child, SpawnError>;
}

pub trait ManagedProcess {
    fn identity(&self) -> ProcessIdentity;
    fn stdout(&mut self) -> Option<ProcessOutput>;
    fn stderr(&mut self) -> Option<ProcessOutput>;

    async fn wait(&mut self) -> Result<ExitStatus, ProcessError>;
    async fn terminate(&mut self) -> Result<(), ProcessError>;
    async fn kill_tree(&mut self) -> Result<(), ProcessError>;
}
```

This trait can remain statically dispatched. There is normally one process backend per target platform.

A GAT for child handles is not useful because the child should own its OS process handle. It must survive independently of a borrow of `ProcessBackend`.

# Compatibility policy API

Compatibility should be composable but not trait-heavy:

```rust
#[derive(Clone)]
pub struct Compatibility {
    protocol: VersionReq,
    binary: BinaryCompatibility,
    configuration: ConfigCompatibility,
    on_mismatch: MismatchAction,
}

pub enum BinaryCompatibility {
    Any,
    ExactFingerprint(BuildFingerprint),
    SemverCompatible(Version),
    Custom(Arc<dyn CompatibilityCheck>),
}

pub enum ConfigCompatibility {
    Ignore,
    Exact(ConfigFingerprint),
    SelectedFields(ConfigFingerprint),
}

pub enum MismatchAction {
    Error,
    Restart,
    UseSeparateIsolation,
}
```

Usage:

```rust
let compatibility = Compatibility::new(ProtocolVersion::new(1))
    .binary(BinaryCompatibility::ExactFingerprint(
        build_fingerprint!(),
    ))
    .configuration(ConfigCompatibility::Exact(
        config.daemon_fingerprint(),
    ))
    .on_mismatch(MismatchAction::Restart);
```

Do not make compatibility a deeply generic policy parameter. It is configuration data and is naturally runtime-selected.

# Key API

Use a simple trait:

```rust
pub trait DaemonKey: Send + Sync + 'static {
    fn digest(&self) -> KeyDigest;
    fn metadata(&self) -> KeyMetadata;
}
```

Built-ins:

```rust
pub struct GlobalKey;

pub struct StringKey(String);

pub struct PathKey {
    display_path: PathBuf,
    identity: PathIdentity,
}

pub struct UuidKey(Uuid);
```

Path constructors should force the caller to choose semantics:

```rust
PathKey::lexical_absolute(path)?
PathKey::canonical(path)?
PathKey::file_identity(path)?
PathKey::application_uuid(uuid, path)
```

Then:

```rust
DaemonSpec::new(APP_ID).key(PathKey::canonical(project)?)
```

Avoid:

```rust
DaemonSpec::new(APP_ID).key(project_path)
```

because that hides identity semantics.

# Type-state for lifecycle results

Type state is useful after `ensure`:

```rust
pub struct Instance<State> {
    record: InstanceRecord,
    control: ControlClient,
    state: PhantomData<State>,
}

pub struct Verified;
pub struct Draining;
pub struct Stopped;
```

For example:

```rust
impl Instance<Verified> {
    pub fn service_endpoint(&self) -> Option<&Endpoint>;

    pub async fn drain(
        self,
    ) -> Result<Instance<Draining>, LifecycleError>;
}

impl Instance<Draining> {
    pub async fn stop(
        self,
    ) -> Result<Instance<Stopped>, LifecycleError>;
}
```

This is conceptually clean, but it may be inconvenient because real processes can die asynchronously. A compile-time `Verified` state is only a statement that the instance was verified when created, not that it remains alive.

Therefore name it carefully:

```rust
AuthenticatedInstance
```

rather than:

```rust
RunningInstance
```

and retain runtime errors for every operation.

I would not expose elaborate lifecycle typestate in version 1.

# Error design

The API should preserve structured lifecycle phases:

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid daemon key")]
    Key(#[from] KeyError),

    #[error("failed to acquire lifecycle lock")]
    Lock {
        instance: InstanceId,
        source: LockError,
    },

    #[error("daemon startup failed during {phase}")]
    Startup {
        phase: StartupPhase,
        diagnostics: StartupDiagnostics,
        source: Option<Box<dyn Error + Send + Sync>>,
    },

    #[error("daemon is incompatible: {reason}")]
    Incompatible {
        reason: CompatibilityMismatch,
        running: InstanceSummary,
    },

    #[error("daemon generation changed")]
    GenerationChanged {
        expected: GenerationId,
        observed: GenerationId,
    },

    #[error("daemon authentication failed")]
    AuthenticationFailed,

    #[error("daemon shutdown failed")]
    Shutdown {
        graceful_error: Option<String>,
        force_error: Option<String>,
    },
}
```

The most important ergonomic feature is:

```rust
error.diagnostics()
```

returning:

```rust
pub struct StartupDiagnostics {
    pub phase: StartupPhase,
    pub command: SanitizedCommand,
    pub pid: Option<u32>,
    pub stdout_tail: Vec<String>,
    pub stderr_tail: Vec<String>,
    pub log_path: Option<PathBuf>,
    pub instance_dir: PathBuf,
}
```

That prevents another round of bugs where daemon startup discards the only useful information.

# My recommended trait boundary

Public and stable:

```rust
pub trait DaemonKey;

pub trait EmbeddedService;

pub trait ManagedChildFactory;
```

Advanced but public:

```rust
pub trait DynTransport;
pub trait CompatibilityCheck;
pub trait LogSink;
```

Sealed or private:

```rust
trait LockBackend;
trait InstanceStore;
trait ProcessBackend;
trait BootstrapChannel;
trait Runtime;
trait TransportImpl;
```

GATs should primarily appear in the sealed/private layer:

```rust
trait LockBackend {
    type Guard<'a>
    where Self: 'a;
}

trait InstanceStore {
    type Transaction<'a>
    where Self: 'a;
}

trait BorrowSplit {
    type Reader<'a>
    where Self: 'a;

    type Writer<'a>
    where Self: 'a;
}
```

That gives you their zero-copy and borrowing benefits without making every consumer understand higher-ranked bounds such as:

```rust
for<'a> P::Store::Transaction<'a>: Send
```

GAT higher-ranked bounds have historically produced difficult diagnostics and subtle lifetime constraints; the Rust Reference still notes that required where-clauses on GATs are governed by special rules that may be loosened in the future. ([Rust Documentation][1])

# Proposed initial API

```rust
pub struct DaemonSpec<K = GlobalKey> {
    // private
}

impl DaemonSpec<GlobalKey> {
    pub fn new(app: impl TryInto<AppId>) -> Result<Self, Error>;
}

impl<K> DaemonSpec<K> {
    pub fn key<K2>(self, key: K2) -> DaemonSpec<K2>
    where
        K2: DaemonKey;

    pub fn isolation(self, isolation: IsolationId) -> Self;
    pub fn transport(self, transport: TransportConfig) -> Self;
    pub fn compatibility(self, compatibility: Compatibility) -> Self;
    pub fn startup_timeout(self, timeout: Duration) -> Self;
    pub fn shutdown_timeout(self, timeout: Duration) -> Self;
    pub fn idle_timeout(self, timeout: Option<Duration>) -> Self;
    pub fn logs(self, policy: LogPolicy) -> Self;
}

pub struct Daemon<S, P = DefaultPlatform> {
    // private
}

impl<S> Daemon<S, DefaultPlatform> {
    pub fn embedded<K>(
        spec: DaemonSpec<K>,
        service: S,
    ) -> Self
    where
        K: DaemonKey,
        S: EmbeddedService<Key = K>;

    pub fn managed<K>(
        spec: DaemonSpec<K>,
        child: ManagedChild<S>,
    ) -> Self
    where
        K: DaemonKey,
        S: ManagedChildFactory;
}

impl<S, P> Daemon<S, P>
where
    P: Platform,
{
    pub async fn ensure(
        &self,
    ) -> Result<AuthenticatedInstance, Error>;

    pub async fn status(
        &self,
    ) -> Result<InstanceStatus, Error>;

    pub async fn stop(
        &self,
    ) -> Result<StopOutcome, Error>;

    pub async fn restart(
        &self,
    ) -> Result<AuthenticatedInstance, Error>;

    pub fn client(&self) -> DaemonClient;
}
```

For client-only use:

```rust
let client = DaemonClient::new(
    DaemonSpec::new(APP_ID)?
        .key(ProjectKey::canonical(project)?),
);

let instance = client.ensure().await?;
```

For daemon entry:

```rust
fn main() -> Result<()> {
    daemontools::entrypoint()
        .normal(run_cli)
        .embedded("__daemon", DebuggerDaemon::from_bootstrap)
        .managed("__supervisor", GhidraSupervisor::from_bootstrap)
        .run()
}
```

Potential macro:

```rust
daemontools::main! {
    cli = run_cli,
    daemon "__daemon" = DebuggerDaemon,
}
```

I would defer the macro. Hidden subprocess entry is security-sensitive and needs to be explicit before it is made syntactically magical.

# Final architectural choice

Use this composition:

```text
Concrete ergonomic façade
        |
        v
Daemon<S, P = DefaultPlatform>
        |
        +-- statically dispatched Platform bundle
        |      +-- transport
        |      +-- locks
        |      +-- process backend
        |      +-- instance store
        |
        +-- optional dyn extension adapters
        |
        +-- GATs inside borrowing-oriented implementation traits
```

The rule for GAT adoption should be:

> Add a GAT only when the associated value must borrow from the implementor and making it own its dependencies would materially harm correctness, allocation behavior, or composability.

That supports GATs for lock guards, store transactions, borrowed connection halves, and possibly zero-box async internals. It does not justify GATs for keys, configuration, process handles, compatibility policies, or the primary user-facing builder.

The most maintainable initial implementation would therefore use:

* concrete `DaemonSpec`;
* separate `embedded` and `managed` constructors;
* one `Platform` generic parameter;
* enum dispatch for built-in transports;
* boxed object-safe traits for custom runtime-selected extensions;
* private GAT-based traits for transactional and borrowing internals;
* Tokio as the supported executor;
* owned process and connection handles wherever possible.

[1]: https://doc.rust-lang.org/reference/items/associated-items.html "Associated items - The Rust Reference"
[2]: https://blog.rust-lang.org/2022/10/28/gats-stabilization/ "Generic associated types to be stable in Rust 1.65 | Rust Blog"
[3]: https://docs.rs/fs4/latest/fs4/fs_std/trait.FileExt.html "FileExt in fs4::fs_std - Rust"
[4]: https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/ "Announcing `async fn` and return-position `impl Trait` in traits | Rust Blog"
[5]: https://doc.rust-lang.org/reference/items/traits.html "Traits - The Rust Reference"
[6]: https://docs.rs/interprocess/latest/interprocess/local_socket/index.html "interprocess::local_socket - Rust"
