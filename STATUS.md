# Status

Phases 0-2 are implemented. Phase 3 bootstrap and embedded lifecycle is in
progress; the native transactional embedded path now runs end to end.

## Latest

- Created the `daemonkit` and publishable `daemonkit-testkit` workspace crates,
  shared MSRV/version policy, documented feature layout, and rustdoc landing
  pages.
- Added validated app/isolation IDs, domain-separated global/string/UUID/path
  key digests, explicit path identity constructors, and redacted secret values.
- Added the lifecycle transition kernel, control-message and semantic fault-point
  skeletons, and rejected service-before-commit transitions.
- Added the first scenario DSL, FIFO/seeded/replay schedules, structured traces,
  replay validation, and concurrent-ensure reuse tests.
- Expanded the world model so all ten default invariants inspect distinct
  authority-bearing state; mutation tests now prove each invariant fails on its
  corresponding unsafe state.
- Added checked monotonic logical time and domain-separated deterministic
  entropy, with reproducibility, separation, and overflow tests.
- Wired one-shot and nth-occurrence semantic startup faults into generation,
  spawn, bootstrap, and manifest-publication boundaries. Faulted uncommitted
  starts roll back manifest/lease/generation state, and seeded traces replay
  exactly with fault configuration included in the scenario digest.
- Added bounded exhaustive actor-order exploration, including explicit
  truncation reporting and hand-enumerated completeness/bound tests.
- Added a strict versioned instance-record schema that rejects unknown fields,
  invalid versions/ranges, and absent generation records, plus the public error,
  startup-phase, diagnostic, instance-ID, and redacted-source skeletons.
- Added the inferred parent facade: compatibility policy, redaction-aware spawn
  recipes, sealed default platform, embedded/managed `Daemon`, attach-only
  `DaemonClient`, typed `Instance`, status/outcome/endpoint values, instance
  paths, and Tokio authenticated-stream type. Native operations currently return
  the explicit `Unsupported` skeleton error.
- Added compile-pass inference coverage and compile-fail gates proving embedded
  instances have no managed endpoint method, managed instances have no embedded
  connect method, and attach-only clients cannot call `ensure`.
- Added child-side bootstrap detection/run contracts, the static
  `EmbeddedService` trait, authenticated incoming/shutdown types, and managed
  child/readiness/endpoint/shutdown/tree/log configuration builders.
- Added the blocking parent facade with inside-Tokio-runtime rejection, plus the
  object-safe advanced custom-transport seam and compile examples for child,
  blocking, and custom-transport usage.
- Added public world-building for consistent absent/ready/unhealthy state and
  explicitly named corrupt lock/lease/manifest/authority/cleanup/identity/
  secret/wait/postcondition states. Initial world state is replay-digest-bound.
- Exhaustively verified all 49 lifecycle state pairs against the declared legal
  transition graph.
- Completed the spec policy skeleton with drain timeout, bounded log retention,
  explicit runtime roots, native/custom transport selection, and OS file-object
  `PathKey` identity (Unix device/inode; Windows volume serial/file index).
- Refactored simulation from whole-operation ordering to explicit lifecycle
  transition scheduling: request/acquire lock, prepare, generation, lease,
  bootstrap, publish, commit, ready, and release. Invariants run after every
  transition, seeded runs interleave waiters and replay exactly, and bounded DFS
  explores/prunes transition schedules (168 for the three-actor contract case).
- Added native private runtime-root creation/reverification. Unix verifies real
  directory type, current UID, and mode on every access; unsafe modes and
  symlinks fail without silent repair. Windows currently rejects roots until a
  current-logon-only DACL verifier is installed.
- Added stable OS file locks with no-follow/CLOEXEC Unix opens, monotonic
  contention timeout, RAII release, and stable lock-file retention.
- Added bounded atomic records: unique same-directory 0600 staging, write + file
  sync + atomic replace + Unix parent sync, no-follow reads, retained corrupt
  JSON, and semantic crash injection around every publication boundary.
- Added verified digest-only instance/generation layouts with stable lifecycle
  and lease authority paths, typed instance-record validation/publication, and
  bounded count/byte retention that always preserves the active and latest
  failed generations.
- Made native lock waits cancellation-aware without coupling the lock primitive
  to an async runtime. Real child-process fixtures now prove cross-process lock
  exclusion, release-on-exit, and old-or-new atomic record visibility when a
  publisher exits at write, sync, and rename boundaries.
- Implemented Windows runtime roots under `%LOCALAPPDATA%` with explicit,
  protected, inheritable current-logon-only DACLs. Reverification checks the
  owner SID, protected-DACL flag, and every granting ACE; unsafe roots fail
  closed. Added a three-OS GitHub Actions test matrix plus format/lint/docs gate.
- Added strict 64 KiB length-delimited binary framing, partial-I/O-safe reads,
  whole-handshake deadlines, highest-overlap protocol negotiation, role and
  capability exchange, constant-time token comparison, and uniform rejection
  of app/key/isolation/generation/token/protocol mismatches. Application-role
  tests prove that bytes are transparent immediately after authentication.
- Added native local transport streams/listeners. Unix uses generation-owned
  filesystem UDS, rejects squatters and over-budget paths, and verifies socket
  file identity before cleanup. Windows uses current-logon-only security
  attributes, first-instance/local-only byte pipes, replenishes accepts before
  handing off connections, and bounds busy-client retries. Native tests cover
  UDS replacement races and Windows pipe security/concurrency in OS CI.
- Added request-ID-preserving control codecs and a capability-aware control
  session for status, drain, shutdown, diagnostics, and application handoff.
  Quiescing is entered before new streams are rejected with typed
  `BusyQuiescing`; application-role streams cannot issue control requests.
- Completed the object-safe custom transport seam with erased async byte
  streams and accept-capable listeners. `AuthenticatedStream` now erases native
  or custom transports without exposing backend traits. Native UDS and Windows
  pipe tests run the real authenticated handshake before application echo.
- Implemented the portable inherited bootstrap transaction: a private piped
  stdin carries launch/commit, stdout/stderr are captured from first spawn,
  endpoint authentication proves `Prepared`, and the child cannot serve before
  `Commit`. EOF or cancellation kills and asynchronously cleans an uncommitted
  child, matching record, and generation directory.
- Wired the parent facade to real native ensure/attach/status/connect/stop/
  restart/repair operations. Fifty cold concurrent ensures serialize through
  the stable lock and reuse one generation; final-endpoint identity is checked
  against the typed record before every handle is returned. Graceful stop is
  idempotent and waits for the child-held lease before owned cleanup.
- Added a real-process embedded fixture covering authenticated byte echo,
  cancellation after spawn, recovery, repeated ensure, and repeated stop. UDS
  endpoints now use a short verified-root socket namespace so long digest-based
  instance paths remain within the portable Unix path budget.
- Cross-compilation checks pass for Linux, macOS (`x86_64-apple-darwin`), and
  Windows (`x86_64-pc-windows-gnu`); native runtime tests have only run on Linux.
- Current local gates pass: 58 unit/integration tests, seven compile API cases,
  one doctest,
  formatting, strict Clippy, rustdoc, and `git diff --check`.

## Next

Continue Phase 3: add idle shutdown/race handling, richer startup diagnostics,
compatibility and stale-repair real-process cases, and the remaining
interruption-matrix schedules before closing the phase. Native Windows/macOS
execution remains CI evidence, not local evidence.
