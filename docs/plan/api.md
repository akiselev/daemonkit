# v1 public API

This document fixes the intended v1 surface. Names may move during an early
implementation spike, but semantic changes must be reflected here before they
become implementation accidents.

## Design rules

- Optimize `new -> ensure -> connect` and `detect -> run`.
- Use separate constructors for embedded and managed modes.
- Use type parameters only for the structural mode and advanced platform
  bundle. Do not type-encode timeouts, policies, or every builder field.
- Keep key identity choices explicit, especially for paths.
- Return authenticated generation handles, not bare endpoint strings.
- Keep control-plane and application data-plane APIs distinct.
- Prefer structured values and outcomes over booleans.
- Make all operations cancellation-safe or document their commit point.
- Preserve source errors and diagnostics without leaking tokens.

## Modules and feature flags

```text
daemonkit
  identity       AppId, keys, isolation, fingerprints
  lifecycle      status and operation outcome types
  managed        child, readiness, shutdown, endpoint publication
  transport      endpoint descriptions and authenticated stream
  diagnostics    startup phase, log paths, captured tails
  blocking       synchronous facade                    [feature = "blocking"]
  advanced       Platform and object-safe adapters     [feature = "advanced"]
  test_support   isolated roots and fault injection    [feature = "test-support"]
```

Default features include the Tokio runtime, native local transport, and the
platform process backend. The initial MSRV is recorded in `Cargo.toml` and CI;
the implementation must not accidentally raise it.

## The happy paths

### Embedded service

The parent-side CLI constructs a lifecycle controller:

```rust
use daemonkit::{Compatibility, Daemon, DaemonSpec, Spawn};

const APP_ID: &str = "com.akiselev.debugger-cli";

let spec = DaemonSpec::new(APP_ID)?
    .idle_timeout(Some(std::time::Duration::from_secs(30 * 60)))
    .compatibility(
        Compatibility::current_binary()
            .config(debugger_config.fingerprint()),
    );

let daemon = Daemon::embedded(
    spec,
    Spawn::current_exe()?.arg("__daemon"),
)?;

let instance = daemon.ensure().await?;
let stream = instance.connect().await?;
debugger_protocol::serve_client(stream).await?;
```

The hidden child entry constructs the actual application service:

```rust
use daemonkit::Bootstrap;

if let Some(bootstrap) = Bootstrap::detect()? {
    bootstrap
        .run_embedded(DebuggerService::new(debugger_config))
        .await?;
    return Ok(());
}
```

`AuthenticatedStream` implements Tokio `AsyncRead` and `AsyncWrite`. The
daemonkit handshake is consumed before the stream reaches the application; the
application owns every subsequent byte.

### Managed child

The parent uses the same lifecycle shape but receives a published endpoint:

```rust
let spec = DaemonSpec::new("com.akiselev.ghidra-cli")?
    .key(PathKey::canonical_existing(&project_path)?)
    .compatibility(
        Compatibility::current_binary()
            .config(ghidra_config.fingerprint()),
    )
    .startup_timeout(std::time::Duration::from_secs(180));

let daemon = Daemon::managed(
    spec,
    Spawn::current_exe()?.arg("__ghidra_supervisor"),
)?;

let instance = daemon.ensure().await?;
let endpoint = instance.service_endpoint()?;
let ghidra = ghidra_protocol::connect(endpoint).await?;
```

The supervisor entry owns the process tree and publishes readiness:

```rust
if let Some(bootstrap) = Bootstrap::detect()? {
    let child = ManagedChild::new(analyze_headless)
        .args(ghidra_args)
        .readiness(Readiness::custom(|mut readiness| async move {
            let endpoint = readiness
                .stdout()
                .find_map(ghidra_protocol::parse_listening_line)
                .await?;
            ghidra_protocol::ping(&endpoint).await?;
            Ok(ReadinessResult::ready().service_endpoint(endpoint))
        }))
        .service_endpoint(EndpointPublication::from_readiness())
        .shutdown(ShutdownStrategy::graceful_then_kill());

    bootstrap.run_managed(child).await?;
    return Ok(());
}
```

daemonkit authenticates the supervisor control plane and binds the published
endpoint to that generation. The child-owned application endpoint remains the
application's security responsibility; its documentation and type must not
imply that daemonkit wraps or authenticates traffic sent directly to it.

## Identity

```rust
pub struct AppId { /* validated reverse-DNS-like identifier */ }
pub struct IsolationId { /* validated short opaque identifier */ }
pub struct KeyDigest([u8; 32]);
pub struct ConfigFingerprint([u8; 32]);
pub struct BuildFingerprint { /* stable build identity */ }

pub trait DaemonKey: Send + Sync + 'static {
    fn digest(&self) -> KeyDigest;
    fn metadata(&self) -> KeyMetadata;
}

pub struct GlobalKey;
pub struct StringKey;
pub struct UuidKey;
pub struct PathKey;
```

Path identity constructors force the policy choice:

```rust
PathKey::lexical_absolute(path)?;     // works before creation; preserves aliases
PathKey::canonical_existing(path)?;  // follows aliases; path must exist
PathKey::file_identity(path)?;        // existing OS object identity
PathKey::application_uuid(uuid, path); // app-stable identity + display metadata
```

There is no `impl DaemonKey for PathBuf`, no implicit canonicalization, and no
lossy UTF-8 conversion. Stable key bytes are domain-separated by key kind and
schema version before hashing. Human-readable metadata is diagnostics only.

`AppId`, `IsolationId`, and endpoint-name components reject traversal,
separators, control characters, reserved Windows names, and lengths which make
portable endpoint construction unsafe.

## Specification and policies

```rust
pub struct DaemonSpec<K = GlobalKey> { /* private fields */ }

impl DaemonSpec<GlobalKey> {
    pub fn new(app: impl TryInto<AppId>) -> Result<Self, ConfigError>;
}

impl<K: DaemonKey> DaemonSpec<K> {
    pub fn key<K2: DaemonKey>(self, key: K2) -> DaemonSpec<K2>;
    pub fn isolation(self, value: IsolationId) -> Self;
    pub fn compatibility(self, value: Compatibility) -> Self;
    pub fn startup_timeout(self, value: Duration) -> Self;
    pub fn shutdown_timeout(self, value: Duration) -> Self;
    pub fn drain_timeout(self, value: Duration) -> Self;
    pub fn idle_timeout(self, value: Option<Duration>) -> Self;
    pub fn logs(self, value: LogPolicy) -> Self;
    pub fn transport(self, value: TransportConfig) -> Self;
    pub fn runtime_root(self, value: RuntimeRoot) -> Self;
}
```

`runtime_root` is primarily for tests and controlled embedding. Production
defaults choose a per-user runtime location and verify it every time.

Compatibility is runtime configuration, not a trait parameter:

```rust
pub struct Compatibility { /* private */ }

impl Compatibility {
    pub fn current_binary() -> Self;
    pub fn protocol(self, versions: ProtocolRange) -> Self;
    pub fn binary(self, policy: BinaryCompatibility) -> Self;
    pub fn config(self, fingerprint: ConfigFingerprint) -> Self;
    pub fn on_mismatch(self, action: MismatchAction) -> Self;
}

#[non_exhaustive]
pub enum BinaryCompatibility {
    Exact(BuildFingerprint),
    SemverCompatible(semver::Version),
    Any,
    Custom(std::sync::Arc<dyn CompatibilityCheck>),
}

pub enum MismatchAction {
    Restart,
    Error,
}
```

Side-by-side operation is expressed with a different `IsolationId`, not as a
surprising side effect of `ensure()`. This keeps the computed instance identity
stable and inspectable.

## Parent-side controllers

```rust
pub enum Embedded {}
pub enum Managed {}

pub struct Daemon<M, P = DefaultPlatform> { /* private */ }
pub struct DaemonClient<M, P = DefaultPlatform> { /* private */ }

impl Daemon<Embedded> {
    pub fn embedded<K: DaemonKey>(
        spec: DaemonSpec<K>,
        spawn: Spawn,
    ) -> Result<Self, ConfigError>;
}

impl Daemon<Managed> {
    pub fn managed<K: DaemonKey>(
        spec: DaemonSpec<K>,
        spawn: Spawn,
    ) -> Result<Self, ConfigError>;
}

impl<M, P: Platform> Daemon<M, P> {
    pub async fn ensure(&self) -> Result<Instance<M>, Error>;
    pub async fn status(&self) -> Result<InstanceStatus, Error>;
    pub async fn stop(&self) -> Result<StopOutcome, Error>;
    pub async fn restart(&self) -> Result<Instance<M>, Error>;
    pub async fn repair(&self) -> Result<RepairOutcome, Error>;
    pub fn client(&self) -> DaemonClient<M, P>;
    pub fn paths(&self) -> InstancePaths<'_>;
}
```

The spawn recipe is data, not a callback executed after `fork`:

```rust
pub struct Spawn { /* private command plus detachment/log policy */ }

impl Spawn {
    pub fn current_exe() -> Result<Self, ConfigError>;
    pub fn program(program: impl Into<OsString>) -> Self;
    pub fn arg(self, value: impl Into<OsString>) -> Self;
    pub fn args(self, values: impl IntoIterator<Item = impl Into<OsString>>) -> Self;
    pub fn env(self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self;
    pub fn current_dir(self, path: impl Into<PathBuf>) -> Self;
    pub fn redact_arg(self, index: usize) -> Result<Self, ConfigError>;
    pub fn redact_env(self, key: impl Into<OsString>) -> Self;
}
```

daemonkit adds its bootstrap channel through reserved inherited state; callers
cannot override the reserved names or handles. Spawn values validate interior
NULs and platform constraints at construction/build time. `Debug` and all
diagnostic rendering apply the declared redactions.

Attach-only construction and operations are explicit:

```rust
impl DaemonClient<Embedded> {
    pub fn embedded<K: DaemonKey>(spec: DaemonSpec<K>) -> Result<Self, ConfigError>;
    pub async fn connect(&self) -> Result<AuthenticatedStream, Error>;
}

impl DaemonClient<Managed> {
    pub fn managed<K: DaemonKey>(spec: DaemonSpec<K>) -> Result<Self, ConfigError>;
    pub async fn service_endpoint(&self) -> Result<PublishedEndpoint, Error>;
}

impl<M, P: Platform> DaemonClient<M, P> {
    pub async fn attach(&self) -> Result<Option<Instance<M>>, Error>;
    pub async fn status(&self) -> Result<InstanceStatus, Error>;
    pub async fn stop(&self) -> Result<StopOutcome, Error>;
    pub fn paths(&self) -> InstancePaths<'_>;
}
```

`DaemonClient` is attach-only: it has `status`, `connect`/`endpoint`, and `stop`,
but not `ensure` or `restart`, because it has no spawn recipe. Naming this
constraint in the type avoids a client-only executable unexpectedly starting a
different binary.

`attach()` returns `Ok(None)` only for a cleanly absent instance. Corrupt,
unhealthy, incompatible, or authentication-failing state is not collapsed into
absence. The convenience `connect()` and `service_endpoint()` methods turn
absence into the structured `Unavailable` error.

`status()` never repairs or deletes state. `repair()` is explicit and obtains
the lifecycle lock. `ensure`, `stop`, `restart`, and `repair` are serialized
mutations. `restart` is one locked transaction, not `stop()` followed by a
separately locked `ensure()`.

Cancellation contract:

- before spawn, cancelling an operation releases the lock with no state change;
- after spawn, a detached cleanup guard owns rollback;
- after manifest publication, the child will not serve until commit;
- after commit, cancellation leaves a valid discoverable daemon rather than
  killing a successfully started generation.

## Generation handle

```rust
pub struct Instance<M> { /* authenticated at construction */ }

impl<M> Instance<M> {
    pub fn id(&self) -> &InstanceId;
    pub fn generation(&self) -> GenerationId;
    pub fn summary(&self) -> &InstanceSummary;
    pub async fn status(&self) -> Result<GenerationStatus, Error>;
    pub async fn stop(self) -> Result<StopOutcome, Error>;
}

impl Instance<Embedded> {
    pub async fn connect(&self) -> Result<AuthenticatedStream, Error>;
}

impl Instance<Managed> {
    pub fn service_endpoint(&self) -> Result<&PublishedEndpoint, Error>;
}
```

The name is `Instance`, not `RunningInstance`: authentication is a point-in-time
fact and the process can exit immediately afterward. Every method revalidates
the generation or reports `GenerationChanged`/`Unavailable`.

`InstanceStatus` distinguishes at least:

```rust
pub enum InstanceStatus {
    Absent,
    Starting(InstanceSummary),
    Ready(InstanceSummary),
    Quiescing(InstanceSummary),
    Stopping(InstanceSummary),
    Incompatible { running: InstanceSummary, reason: CompatibilityMismatch },
    Unhealthy { record: Option<InstanceSummary>, reason: UnhealthyReason },
}
```

Outcomes are idempotent and descriptive:

```rust
pub enum StopOutcome { AlreadyAbsent, Stopped { generation: GenerationId, forced: bool } }
pub enum RepairOutcome { Clean, Repaired { actions: Vec<RepairAction> }, Busy }
```

## Child-side bootstrap

```rust
pub struct Bootstrap { /* inherited private channel and launch identity */ }

impl Bootstrap {
    pub fn detect() -> Result<Option<Self>, BootstrapError>;

    pub async fn run_embedded<S>(self, service: S) -> Result<(), Error>
    where
        S: EmbeddedService;

    pub async fn run_embedded_fn<F, Fut, E>(self, service: F) -> Result<(), Error>
    where
        F: FnOnce(ServiceContext, Incoming, Shutdown) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static;

    pub async fn run_managed(self, child: ManagedChild) -> Result<(), Error>;
}

pub trait EmbeddedService: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn run(
        self,
        context: ServiceContext,
        incoming: Incoming,
        shutdown: Shutdown,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

The public service trait uses RPITIT for an ergonomic static implementation and
is intentionally not object-safe. `run_embedded_fn` is the no-boilerplate form.
The trait owns `self`, so no public future GAT is necessary.

`Incoming` is a `Stream<Item = Result<AuthenticatedStream, AcceptError>>` and
stops yielding before shutdown completes. daemonkit handles authentication and
the accept loop's lifecycle; the service decides framing, per-connection tasks,
backpressure, and request cancellation.

`Bootstrap::detect()` validates a versioned inherited channel. It does not
interpret arbitrary public environment variables as permission to enter daemon
mode. On Unix the launcher passes only a deliberately inherited descriptor; on
Windows it uses an explicit `STARTUPINFOEX` handle list. The bootstrap token and
other secrets travel inside that private channel.

## Managed child configuration

```rust
pub struct ManagedChild { /* owned command and policies */ }

impl ManagedChild {
    pub fn new(program: impl Into<OsString>) -> Self;
    pub fn arg(self, value: impl Into<OsString>) -> Self;
    pub fn args(self, values: impl IntoIterator<Item = impl Into<OsString>>) -> Self;
    pub fn env(self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self;
    pub fn current_dir(self, path: impl Into<PathBuf>) -> Self;
    pub fn readiness(self, value: Readiness) -> Self;
    pub fn service_endpoint(self, value: EndpointPublication) -> Self;
    pub fn shutdown(self, value: ShutdownStrategy) -> Self;
    pub fn tree_policy(self, value: ProcessTreePolicy) -> Self;
    pub fn logs(self, value: ChildLogPolicy) -> Self;
}
```

There is no arbitrary Unix `pre_exec` hook and no unrestricted inherited-handle
API in the safe facade. Advanced platform APIs can expose audited, explicitly
unsafe escape hatches later if a real integration requires them.

Readiness supports built-in immediate/TCP probes and an async closure. A
successful probe returns the endpoint to publish; path existence alone is never
a readiness signal. Probe failures retain structured attempts and output tails.

The readiness contract is endpoint-producing and observes tee'd output rather
than taking ownership of the child's raw pipes:

```rust
pub struct ReadinessContext { /* child status + subscribed output events */ }
pub struct ReadinessResult { /* optional endpoint + safe diagnostic metadata */ }

impl ReadinessContext {
    pub fn stdout(&mut self) -> &mut OutputEvents;
    pub fn stderr(&mut self) -> &mut OutputEvents;
    pub async fn exited(&mut self) -> ProcessExit;
    pub fn elapsed(&self) -> Duration;
}

impl Readiness {
    pub fn immediate() -> Self;
    pub fn tcp_connect(endpoint: SocketAddr) -> Self;
    pub fn custom<F, Fut, E>(probe: F) -> Self
    where
        F: FnOnce(ReadinessContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<ReadinessResult, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static;
}
```

`tcp_connect` proves only TCP readiness and returns that socket address.
Applications needing a ping, port parsed from output, or another protocol use
`custom`; that protocol code remains in the application closure. Output events
are bounded and broadcast by daemonkit's capture tasks, so readiness cannot
deadlock logging or steal bytes from diagnostics. A path merely appearing is
never treated as readiness because it may belong to a stale generation.

Shutdown is a ladder rather than one callback: optional application request,
grace period, platform terminate, then owned-tree kill. The supervisor retains
the process/Job handle until exit and output drain.

## Transport and extensions

```rust
#[non_exhaustive]
pub enum TransportConfig {
    NativeLocal,
    Custom(std::sync::Arc<dyn advanced::DynTransport>),
}
```

Native local means a short generation-specific filesystem UDS on Unix and a
current-user, local-only named pipe on Windows. Application-visible endpoint
types are non-exhaustive and never require callers to format raw socket or pipe
names themselves.

The advanced kernel has one generic parameter. In v1 the public marker is
sealed; it permits daemonkit's default and test platforms without freezing all
backend traits as downstream contracts:

```rust
pub trait Platform: private::Sealed + Clone + Send + Sync + 'static {}

// Private kernel trait implemented for each sealed Platform.
trait PlatformImpl: Platform {
    type Transport: TransportImpl;
    type Locks: LockBackend;
    type Processes: ProcessBackend;
    type Store: InstanceStore;
    type Clock: Clock;

    fn transport(&self) -> &Self::Transport;
    fn locks(&self) -> &Self::Locks;
    fn processes(&self) -> &Self::Processes;
    fn store(&self) -> &Self::Store;
    fn clock(&self) -> &Self::Clock;
}
```

`Platform` and object-safe extension traits are visible under `advanced`, but
downstream crates customize v1 through the object-safe adapters rather than by
implementing the platform bundle. Implementation traits with GAT guards or
store transactions remain private until multiple real backends prove a stable
downstream contract. The generic bundle still prevents backend parameters from
spreading through the kernel and permits a deterministic `TestPlatform`. The
default platform is Tokio-based; v1 does not claim executor independence.

## Blocking facade

The blocking facade mirrors parent-side operations:

```rust
let instance = daemonkit::blocking::Daemon::embedded(spec, spawn)?.ensure()?;
let stream = instance.connect()?;
```

It owns a small runtime and blocks only the calling thread. It detects entry
from a Tokio runtime and returns `BlockingError::InsideRuntime` instead of
deadlocking or nesting runtimes. Child-side services remain async in v1.

## Errors and diagnostics

```rust
#[non_exhaustive]
pub enum Error {
    Config(ConfigError),
    StateDirectory(StateDirectoryError),
    Lock { instance: InstanceId, source: LockError },
    Startup { phase: StartupPhase, diagnostics: StartupDiagnostics, source: ErrorSource },
    Incompatible { reason: CompatibilityMismatch, running: InstanceSummary },
    GenerationChanged { expected: GenerationId, observed: Option<GenerationId> },
    AuthenticationFailed,
    Unavailable { generation: Option<GenerationId>, source: ErrorSource },
    Shutdown { diagnostics: ShutdownDiagnostics, source: ErrorSource },
    CorruptState { path: PathBuf, source: ErrorSource },
}
```

Every error exposes `diagnostics() -> Option<&dyn DiagnosticReport>`. Startup
diagnostics include the phase, redacted command, PID when known, bounded stdout
and stderr tails, persistent log path, and instance directory. Byte counts and
truncation are explicit. Non-UTF-8 output is retained losslessly internally and
rendered safely.

Tokens use a secret wrapper with redacted `Debug`/`Display`; serialized records
and bootstrap messages use zeroizing buffers where practical. Errors must be
tested for accidental secret inclusion through their full source chain.

## API review gates

Before v1 is frozen:

- compile every example with no turbofish or backend type annotations;
- test errors for invalid app IDs, keys, paths, policies, and mode operations;
- verify rustdoc groups the happy path ahead of `advanced` APIs;
- run compile-fail tests proving embedded instances have `connect()` but not
  `service_endpoint()`, with the inverse for managed instances;
- confirm attach-only clients cannot call `ensure()`;
- confirm public structs containing secrets are non-constructible and redacted;
- build minimal wrappers around both debugger-cli and ghidra-cli usage to catch
  generic-bound and ownership friction before stabilization.
