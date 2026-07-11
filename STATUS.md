# Status

Phase 0's executable foundation and exit-gate model are in place. Native
lifecycle behavior has not started; Phase 1 red scenarios are next.

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
- Current gates pass: 27 unit tests, seven compile API cases, one doctest,
  formatting, strict Clippy, rustdoc, and `git diff --check`.

## Next

Begin Phase 1 with red secure-root, stable-lock, interrupted-publication, and
corrupt-record scenarios; then implement native identity/root/lock/store
contracts on Unix and Windows. Extend custom actor/spec testkit composition as
the first downstream contract needs it.
