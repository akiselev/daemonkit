# v1 cross-platform test plan

daemon lifecycle correctness is primarily a concurrency, crash-consistency,
security, and operating-system semantics problem. Unit tests are necessary but
cannot substitute for real multi-process tests on every supported platform.
Deterministic lifecycle development and schedule exploration live in the
first-class `daemonkit-testkit` crate described in [`testkit.md`](testkit.md).

## Test layers

1. `daemonkit-testkit` scenarios and bounded exploration enumerate lifecycle
   transitions, interleavings, faults, and policy outcomes.
2. Unit tests cover parsing, validation, hashing, framing, redaction, and error
   construction.
3. Testkit backend contract suites run against `SimPlatform` and native
   lock/store/transport/process implementations.
4. Multi-process fixture tests exercise actual locks, spawn, IPC, signals,
   handles, logs, and crashes.
5. Consumer-shaped fixtures exercise debugger-cli embedded mode and ghidra-cli
   managed-child mode without importing their application protocols.
6. CI platform suites run on Linux, macOS, and Windows; platform behavior is not
   inferred from `cfg` compilation alone.

Every test receives a random isolated runtime root. Concurrency tests must not
use a global serial lock. Test cleanup reports leaked processes and artifacts as
failures rather than silently removing the evidence first.

## Deterministic simulation and fault injection

The testkit schedules named actors against virtual time, storage, locks,
processes, and transport. It checks the default safety invariants after every
transition, explores bounded schedules, and emits a replay token for every
failure. The kernel exposes feature-gated semantic failpoints at:

- before and after lifecycle lock acquisition;
- after reading an existing record;
- before old-generation drain and after it stops;
- before creating a bootstrap channel;
- immediately before and after spawn;
- before and after child lease acquisition;
- before and after final endpoint bind;
- before and after service/managed-child initialization;
- immediately before and after `Prepared`;
- before temporary-record write, flush, rename, and directory sync;
- immediately after manifest publication;
- immediately before and after `Commit`;
- during application accept and idle transition;
- during drain, graceful shutdown, process terminate, and tree kill;
- immediately before endpoint, record, and generation cleanup.

Each failpoint can return an error, pause behind a barrier, abort the launcher,
abort the daemon, or simulate I/O cancellation as appropriate. The harness
records a structured phase trace so a timeout identifies the blocked
transition. Failures are first reproduced in simulation, then covered by the
relevant native regression; simulation does not replace OS validation.

## Required lifecycle scenarios

- Fifty simultaneous `ensure()` calls yield one committed generation.
- Fifty more calls reuse it without serial startup latency after authentication.
- Launcher death at every startup failpoint leaves either the previous valid
  generation or recoverable uncommitted state.
- Daemon death before/after publication and before/after commit is repaired.
- An old generation exiting after a successor starts cannot remove or shadow
  the successor endpoint.
- `stop` races with `ensure`; `restart` races with `restart`; `status` races with
  cleanup; cancellation races with commit.
- Repeated `stop` and `repair` calls are idempotent.
- Idle shutdown racing a new application connection produces either a served
  connection or typed retry, never a black hole.
- A quiescing generation cannot accept new application streams.
- Lock timeout and operation timeout use monotonic time and preserve useful
  diagnostics.
- A crashed lock holder releases the OS lock; stable lock files remain usable.
- A corrupt/truncated/old/new-schema manifest never authorizes connection or
  deletion and is retained diagnostically.
- Mismatched PID, process identity, generation, endpoint, token, app, key,
  isolation, binary, config, and protocol each produce the intended distinct
  outcome.
- PID and endpoint reuse by an unrelated process cannot pass authentication or
  trigger destructive cleanup.

## Transport and protocol scenarios

- Partial headers/bodies, zero-length frames, maximum frames, one-byte-over
  maximum frames, unknown message kinds, duplicate fields, and invalid enums.
- Authentication tokens are compared in constant time and absent from all
  formatted errors/logs.
- Slowloris clients, connection floods, early EOF, write-half closure, and
  cancellation remain resource-bounded.
- Protocol range overlap selects one documented version; no overlap reports
  incompatibility without consuming application bytes.
- Control and application roles cannot be confused or changed midstream.
- An authenticated application stream is byte-transparent after handoff,
  including arbitrary binary, empty writes, large transfers, and half-close.
- Endpoint path/name length stays portable with long app, key metadata, Unicode,
  and deeply nested runtime roots.
- Removing a live Unix socket pathname is detected as unhealthy and never causes
  the old server to unlink a successor.
- Named-pipe accept replenishment handles many simultaneous clients.

## Identity and state-directory scenarios

- Relative, absolute, `.`/`..`, symlink, hard-link/file identity, Unicode, and
  non-UTF-8 Unix paths behave according to the selected `PathKey` policy.
- Windows drive-letter case, UNC, verbatim paths, reparse points, and
  case-sensitive directories are covered without blanket lowercasing.
- macOS case-sensitive and case-insensitive volume behavior is not assumed;
  tests use available fixtures and explicit policies.
- App/isolation traversal, reserved names, separators, NUL/control characters,
  and excessive length are rejected.
- Existing runtime roots with wrong owner, ACL/mode, type, or symlink/reparse
  indirection fail closed.
- Concurrent first creation is safe and does not temporarily expose secrets.
- Project directories on network/removable-like locations do not host lifecycle
  files; only key metadata references them.
- Atomic replacement and parent sync behavior is probed on each CI OS.

## Process, detachment, and log scenarios

- Closing the launching terminal/stdin does not kill or block a committed
  daemon unless policy requests parent-bound lifetime.
- Ctrl-C during each startup/shutdown phase respects cancellation semantics.
- Launcher logout/exit closes only intended handles; the child does not retain
  unrelated descriptors/handles.
- Startup stdout/stderr larger than pipe capacity cannot deadlock readiness.
- Non-UTF-8 output, very long lines, no trailing newline, and continuous output
  are captured with bounded tails and persistent logs.
- A child exits successfully/unsuccessfully before readiness, during readiness,
  after readiness, and during shutdown with structured diagnostics.
- A wrapper exits while a grandchild retains stdout/stderr; output drain is
  bounded and reports inherited-handle suspicion.
- Graceful shutdown hangs, terminate fails, and kill-tree is required.
- Supervisor crash kills managed descendants according to policy.
- Orphaned grandchildren and grandchildren which create their own groups/jobs
  exercise documented limits and failure reporting.

## Unix-specific matrix

Run on Linux and macOS:

- advisory lock semantics across duplicate descriptors, fork/exec, crash, and
  two independently opened handles;
- UDS bind/connect/unlink, stale files, peer identity where available, and path
  limits;
- session and process-group membership after spawn;
- SIGTERM/SIGKILL escalation and signal races;
- descriptor inheritance allowlist verified through a fixture enumerating open
  descriptors;
- root mode/owner verification and symlink swap attempts;
- non-UTF-8 paths and output.

Linux-only optimizations, if later enabled, require separate fallback-equivalence
tests. macOS must never pass only because Linux-specific behavior is compiled
out.

## Windows-specific matrix

- Explicit DACL permits the launching logon identity and denies an unauthorized
  test identity where CI permits creating one.
- Remote named-pipe clients are rejected and first-instance arbitration works.
- Handle inheritance fixture proves only bootstrap/log handles cross spawn.
- Job Object contains child and grandchild and kill-on-close removes both.
- Suspended-create/assign/resume error paths do not leak a runnable child.
- Nested/existing Job Object constraints on hosted CI produce a documented
  fallback or explicit unsupported error, never silent loss of ownership.
- Process identity uses retained handles/creation metadata rather than
  `tasklist`; termination uses native APIs rather than `taskkill`.
- UNC, drive, verbatim, Unicode, long-path, ACL, and reparse-point cases cover
  identity and root validation.
- Pipe clients connecting/disconnecting rapidly do not exhaust handles.

## Security and resource bounds

- Fuzz instance records, bootstrap messages, control frames, and endpoint
  decoding with strict size limits.
- Attempt local endpoint squatting before bind, between prepare/publish, and
  after crash. First-instance/generation names plus authentication must fail
  safe.
- Attempt record/token reads under unsafe directory permissions and verify
  startup refuses to continue.
- Search formatted diagnostics, trace output, serialized failure fixtures, and
  nested error sources for known canary secrets.
- Bound concurrent unauthenticated connections, handshake duration, frame size,
  captured tail bytes, retained generations, readiness attempts, and shutdown
  waits.
- Repeated failed starts do not grow files, processes, handles, descriptors, or
  tasks without bound.
- daemonkit never deletes application-owned files, even when their names look
  like stale locks or endpoints.

## CI gates

Required pull-request jobs:

- stable Rust format, lint, docs, unit, doctest, and compile-fail checks;
- daemonkit-testkit self-tests, focused FIFO/replay scenarios, and default
  invariant mutation tests;
- bounded schedule exploration with failure replay tokens retained as artifacts;
- MSRV build and supported-feature combinations;
- Linux real-process and stress suite;
- macOS real-process and platform suite;
- Windows real-process, named-pipe, Job Object, and handle suite;
- dependency/license/advisory checks;
- deterministic model/fault suite with multiple seeds recorded on failure.

Nightly or scheduled jobs increase stress iterations, run fuzz targets for a
fixed budget, repeat flaky-prone process tests, and retain logs/artifacts. A
platform test may be skipped only for a documented unavailable OS capability;
the corresponding core behavior must still have a native test somewhere.

## Consumer acceptance fixtures

The embedded fixture mirrors debugger-cli:

- global key;
- current-executable hidden entry;
- concurrent binary streams;
- compatibility restart;
- idle shutdown;
- terminal/launcher exit;
- no client-side socket deletion.

The managed fixture mirrors ghidra-cli:

- canonical-existing project key;
- supervisor plus wrapper plus long-lived grandchild;
- slow TCP readiness with an application-level ping;
- direct published service endpoint;
- large startup output and inherited-handle traps;
- bounded graceful shutdown and forced tree cleanup;
- no PID/port files and no cleanup of application project locks.

These fixtures are protocol-neutral. Their purpose is to prove daemonkit owns
the lifecycle responsibilities without absorbing debugger or Ghidra protocol
semantics.
