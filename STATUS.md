# Status

Phases 0-4 are implemented. The native runtime now covers embedded daemons and
supervised managed children; the cross-platform CI matrix is active and passing.

## Implemented

- Phase 0: workspace/API foundation, validated identities and key digests,
  redaction-safe spawn recipes, typed embedded/managed facades, blocking and
  custom-transport seams, lifecycle model, deterministic scenario/replay DSL,
  semantic fault injection, invariant mutation tests, and bounded schedule
  exploration.
- Phase 1: private keyed layouts, cross-process startup/lease locks with async
  cancellation, strict typed instance records, durable bounded atomic replace,
  crash-boundary tests, bounded generation retention, Unix owner/mode checks,
  and Windows current-logon-only protected DACL creation/reverification.
- Phase 2: strict 64 KiB binary framing, deadline-bounded authenticated
  negotiation, constant-time token checks, role/capability separation, typed
  control messages, quiescing behavior, authenticated Unix sockets, secured
  Windows named pipes, and object-safe custom byte transports.
- Phase 3: portable inherited bootstrap launch/commit transaction, authenticated
  Prepared proof, rollback on cancellation/EOF, native ensure/attach/status/
  connect/stop/restart/repair, embedded drain/idle semantics, startup diagnostics,
  and real-process concurrency/recovery regressions.
- Phase 4: managed command validation, immediate/TCP/custom readiness, typed
  service-endpoint publication, lossless persistent stdout/stderr capture with
  bounded newest-event readiness rings, graceful/forced shutdown ladders, Unix
  process groups plus detached crash watchdog, and Windows suspended spawn plus
  kill-on-close Job Object ownership. JVM-like wrapper/grandchild fixtures prove
  large-output readiness, typed endpoint discovery, forced tree cleanup, and
  cleanup after supervisor crash without application PID/port files.

## Verification

- Local Linux: native lifecycle (14 real-process cases), layout/path-limit
  regression, rustfmt, strict Clippy, rustdoc, MSRV 1.81 library check, and
  `git diff --check` pass.
- Hosted GitHub Actions: Ubuntu, macOS, and Windows all execute platform
  storage/security/transport/protocol contracts plus real embedded and managed
  lifecycle suites. The matrix also covers six feature configurations, public
  API compilation, repeated race/cancellation/crash/tree-cleanup stress,
  package validation, and RustSec audit. PR #1 merged to `master` as
  09f5d3a; post-merge run 29271759559 passed all 15 jobs.
- Hosted failures improved the runtime: compact 128-bit state-directory routing
  avoids Windows atomic-store temporary-path limits, while Unix control sockets
  use a verified short per-user directory so macOS handles deep runtime roots.

## Next

Release preparation: public README, crate READMEs, usage/security guide, and
release checklist are complete for `0.1.0`; tag-triggered publishing and the
GitHub release remain intentionally paused pending final release workflow
review. After that: Phase 5 platform/race/security/resource hardening,
benchmarks, and debugger-cli/ghidra-cli migration spikes.
