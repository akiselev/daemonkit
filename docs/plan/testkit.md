# daemonkit-testkit plan

`daemonkit-testkit` is a first-class workspace crate, not a collection of
private test helpers. Its purpose is to make lifecycle behavior reproducible,
fast, and inspectable enough for red/green development while keeping native
cross-platform tests as the final backend proof.

## Goals

- Run complete multi-client and daemon lifecycle scenarios without spawning OS
  processes or waiting on wall-clock time.
- Control every nondeterministic input: scheduling, time, entropy, locks,
  storage, process exit, transport, partial I/O, and injected crashes.
- Explore many bounded interleavings, then replay any failure exactly from one
  compact artifact.
- Check daemonkit's safety invariants after every observable transition, not
  only at the end of a test.
- Give downstream applications the same tools for testing their daemon
  configuration and lifecycle integration without testing their application
  protocol for them.
- Run the simulation suite on every edit; reserve slower native process suites
  for backend conformance and operating-system semantics.

It is not intended to emulate Unix or Windows perfectly. The simulator models
the daemonkit backend contracts. Native contract suites prove that each real
backend implements those contracts.

## Workspace boundary

```text
workspace
  daemonkit/
    public lifecycle API
    state machine and backend contracts
    native DefaultPlatform
    feature-gated simulator hooks and SimPlatform implementation

  daemonkit-testkit/
    public scenario DSL
    deterministic scheduler and explorer
    virtual-world configuration
    invariant assertions and trace rendering
    reusable embedded/managed fixtures
```

`daemonkit-testkit` depends on `daemonkit` with a non-default `testkit` feature.
There is no reverse dependency. Because the v1 `Platform` marker is sealed,
the low-level `SimPlatform` implementation lives behind that feature inside
`daemonkit` and is re-exported through `daemonkit-testkit`. Ordinary daemonkit
builds contain no simulator code.

Both crates share a version and MSRV. The testkit is publishable and documented,
but applications normally use it only as a dev-dependency:

```toml
[dev-dependencies]
daemonkit-testkit = "1"
```

## Deterministic execution model

The simulator runs on a single logical thread. Concurrency exists as actors and
explicit yield points at daemonkit backend operations. The scheduler chooses
one runnable transition at a time, making the ordering fully controlled.

The virtual world contains:

- a monotonic logical clock and deterministic timers;
- a domain-separated deterministic entropy source;
- a transactional virtual instance store and directory tree;
- lifecycle and lease locks with explicit owners and wait queues;
- a process table with parent/child trees, identities, handles, exit states,
  stdout/stderr streams, and kill semantics;
- bootstrap and control transports supporting connect, accept, partial I/O,
  close, delay, corruption, and endpoint reuse;
- a compatibility/build/config registry;
- bounded log buffers and retained generation artifacts;
- named launcher, daemon, service, and client actors.

Backend calls do not perform hidden scheduling. Each point at which another
actor could observe a different state emits a documented `SimEvent` and yields.
This makes traces stable and prevents incidental executor changes from silently
changing the modeled interleavings.

Tokio paused time may be used inside native-adjacent tests, but it is not the
simulation scheduler. The simulator must not depend on wall-clock sleeps,
thread races, or probabilistic timing to expose lifecycle bugs.

## Public API direction

The smallest scenario should read like the lifecycle story being tested:

```rust
use daemonkit_testkit::{ClientAction, Scenario};

let result = Scenario::embedded("com.example.worker")?
    .client("first", ClientAction::ensure())
    .client("second", ClientAction::ensure())
    .run()?;

result.assert_invariants()?;
result.assert_one_ready_generation()?;
assert_eq!(result.generation("first")?, result.generation("second")?);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Core types:

```rust
pub struct Scenario<M> { /* world, actors, actions, faults, assertions */ }
pub struct Simulation { /* configured executable scenario */ }
pub struct SimResult { /* outcomes, final world, trace, replay token */ }
pub struct Trace { /* ordered, structured SimEvent values */ }
pub struct ReplayToken { /* schema + scenario digest + schedule + faults */ }

pub enum Schedule {
    Fifo,
    Seeded(u64),
    Replay(ReplayToken),
}

pub struct Explorer { /* bounded schedule search */ }
pub struct ExploreReport { /* explored/pruned schedules + first failure */ }
```

Scenario construction provides:

```rust
impl Scenario<Embedded> {
    pub fn embedded(app: impl TryInto<AppId>) -> Result<Self, ConfigError>;
}

impl Scenario<Managed> {
    pub fn managed(app: impl TryInto<AppId>) -> Result<Self, ConfigError>;
}

impl<M> Scenario<M> {
    pub fn spec<K: DaemonKey>(self, spec: DaemonSpec<K>) -> Self;
    pub fn world(self, f: impl FnOnce(&mut WorldBuilder)) -> Self;
    pub fn client(self, name: impl Into<ActorName>, action: ClientAction) -> Self;
    pub fn actor<F, Fut>(self, name: impl Into<ActorName>, action: F) -> Self
    where
        F: FnOnce(SimDaemon<M>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), SimError>> + 'static;
    pub fn fault(self, fault: Fault) -> Self;
    pub fn invariant(self, invariant: impl Invariant) -> Self;
    pub fn schedule(self, schedule: Schedule) -> Self;
    pub fn run(self) -> Result<SimResult, SimError>;
    pub fn explore(self, explorer: Explorer) -> Result<ExploreReport, SimError>;
}
```

`ClientAction` covers the typed standard operations and records a structured
`OperationOutcome` retrievable by actor name. `actor` is the escape hatch for
composing the normal daemon API; it returns `()` so one scenario can contain
heterogeneous custom actors without erased user result types. No platform or
simulator generic appears in ordinary scenarios.

`WorldBuilder` can declare pre-existing records, endpoints, generations,
processes, permissions, output, and compatibility. Unsafe/impossible world
states require an explicit `WorldBuilder::corrupt_*` method so test intent is
obvious.

## Actions, events, and traces

Built-in actions cover `ensure`, `attach`, `connect`, `status`, `stop`,
`restart`, `repair`, advance-to-idle, launcher crash, daemon crash, managed-child
exit, and application stream open/close. Custom async client actions can compose
the normal daemonkit API.

Every transition emits a structured event, for example:

```rust
pub enum SimEvent {
    ActorScheduled { actor: ActorId },
    LifecycleLockRequested { instance: InstanceId, actor: ActorId },
    LifecycleLockAcquired { instance: InstanceId, actor: ActorId },
    ProcessSpawned { process: SimProcessId, parent: Option<SimProcessId> },
    BootstrapPrepared { generation: GenerationId },
    ManifestPublished { generation: GenerationId },
    CommitSent { generation: GenerationId },
    HandshakeAccepted { generation: GenerationId, role: ConnectionRole },
    ShutdownEscalated { generation: GenerationId, stage: ShutdownStage },
    FaultTriggered { fault: FaultId, at: FaultPoint },
    InvariantChecked { invariant: InvariantId },
}
```

Traces use stable actor, process, connection, and generation labels. Human
rendering is concise; machine serialization is versioned. A failure always
prints the seed/replay token, minimized action schedule when available, violated
invariant, final world summary, and a tail of relevant events. Secrets remain
redacted in both formats.

## Fault model

Faults target semantic points rather than source line numbers:

```rust
Fault::once(FaultPoint::AfterManifestPublish, FaultAction::CrashLauncher)
Fault::nth(FaultPoint::ControlWrite, 3, FaultAction::ShortWrite(2))
Fault::while_actor("stopper", FaultPoint::LeaseWait, FaultAction::Pause("gate"))
```

Actions include return error, crash actor, close channel, partial read/write,
delay until logical time, pause on a named barrier, corrupt staged bytes, lose
an unflushed write, fail rename, report child exit, and deny termination.

Fault-point names and semantics are a testkit compatibility surface. Adding
points is non-breaking; removing or silently moving their semantic boundary
requires a documented migration. Production builds compile failpoint branches
out.

## Scheduling and exploration

Three modes serve different development needs:

- `Fifo` is the fast, obvious default for focused red/green tests.
- `Seeded` chooses runnable actors deterministically and is suitable for broad
  regression sweeps. The seed is always reported.
- `Replay` executes the exact recorded choices and faults from a prior failure.

`Explorer` performs bounded depth-first schedule exploration. Bounds include
maximum transitions, preemptions, actors, logical time, and schedules. It uses
safe partial-order reduction only when two transitions declare disjoint modeled
resources; ambiguous operations remain ordered. State hashes may prune repeated
world states but must include actor continuations, wait queues, faults, clock,
and all authority-bearing state.

The explorer stops at the first invariant violation by default and emits a
replay token. Optional shrinking removes actors/actions and schedule choices
while replaying the violation. Shrinking is an aid, never a reason to discard
the original trace.

## Always-on invariants

The default invariant set runs after every transition:

1. At most one actor owns an instance lifecycle lock.
2. At most one live generation owns the instance lease.
3. At most one committed `Ready` generation is discoverable per instance.
4. A published manifest is complete and names one generation's endpoint.
5. A serviceable generation is committed, lease-owning, and authenticated.
6. Cleanup never removes a resource owned by another live generation.
7. Destructive process action targets a retained, revalidated identity.
8. Secrets never occur in trace or rendered diagnostic values.
9. Waiters blocked only on logical time or modeled resources are explainable;
   an unexplained no-runnable-actor state is a deadlock failure.
10. Completed operations satisfy their documented idempotence and cancellation
    postconditions.

Tests can add application-specific invariants but cannot disable safety
invariants. A deliberately corrupt initial world is exempt only until the first
named repair boundary; safety of actions taken during repair remains checked.

## Backend contract suites

The testkit exports contract suites which workspace CI runs against
`SimPlatform` and daemonkit-provided opaque native test factories:

```rust
daemonkit_testkit::contracts::lock_backend(factory);
daemonkit_testkit::contracts::instance_store(factory);
daemonkit_testkit::contracts::control_transport(factory);
daemonkit_testkit::contracts::process_backend(factory);
```

Contracts describe observable semantics, including crash release, atomic
replace, partial I/O, identity retention, output EOF, and tree kill. A simulator
test is trustworthy only when native backends pass the same relevant contract.
OS-specific properties such as named-pipe ACLs and Unix descriptor inheritance
remain additional native tests.

## Red/green development workflow

Every lifecycle slice follows this order:

1. Red model: add the smallest deterministic scenario that states the desired
   transition or reproduces the bug; confirm it fails for the intended reason.
2. Red exploration: add the relevant invariant/fault point and confirm a
   bounded explorer or fixed replay finds the unsafe interleaving.
3. Green kernel: implement only enough state-machine behavior to make the
   focused simulation and invariant suite pass.
4. Green contract: implement or update the fake/native backend contract test.
5. Green native: add at least one real-process test on each affected platform;
   platform-specific behavior lands with its platform test.
6. Refactor: simplify while the focused replay, bounded exploration, contracts,
   and native regression remain green.

A bug fix begins by checking in its replay scenario. CI runs focused FIFO/replay
tests first, then a bounded explorer, then native suites. Flaky timing-dependent
tests are bugs in the test or product; they are not papered over with sleeps or
retries.

## Testkit's own tests

The simulator itself requires:

- golden traces and serialization compatibility tests;
- scheduler determinism across repeated runs;
- replay rejection when scenario or schema digests do not match;
- explorer completeness on small hand-enumerated state spaces;
- partial-order reduction comparison against unpruned exploration;
- invariant mutation tests proving each invariant can fail;
- fault trigger cardinality and semantic-boundary tests;
- secret-redaction tests for every renderer;
- differential contract runs between simulator and native backends;
- resource bounds for traces, schedules, and state snapshots.

## v1 exit criteria

- Every lifecycle phase in `implementation.md` is developed through committed
  red/green simulation scenarios.
- The full default invariant set has positive and negative tests.
- Any seeded failure is exactly replayable locally and in CI.
- Bounded exploration covers the prepare/publish/commit and drain/stop cleanup
  transactions before their native implementations are considered complete.
- All backend contract suites pass for simulation and supported native OSes.
- The debugger-cli-shaped and ghidra-cli-shaped fixtures are expressible through
  the public scenario API without private daemonkit access.
- Testkit rustdoc includes quick-start, race reproduction, crash injection,
  exploration, replay, and native contract examples.
