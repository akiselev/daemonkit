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

- Interactive events (2026-09-28): every daemon runs an in-process event hub
  (bounded ring, non-blocking broadcast, `daemon.log` mirror). Clients tail it
  over the authenticated control channel via `events()` (replay, follow,
  `tracing`-style filter, reattach across restarts, `Lagged` markers) using
  new capability-gated `OpenEventStream`/`SetLogFilter` messages; `Diagnostics`
  now returns rendered recent events. Managed stdout/stderr stream as bounded
  line events. The `tracing` feature adds a per-layer-filtered forwarding
  Layer with a runtime-reloadable filter (`set_log_filter`). `logs()` tails
  daemon/stdout/stderr/managed log files without a live daemon, following
  generation changes. Follow-ups landed 2026-09-29: capability-gated
  `Inspect` returning a typed `InstanceReport` (mode, state, pids, uptime,
  streams, event counters, log filter); clean stop and crash repair now keep
  generation logs within `LogPolicy` retention and `logs()` falls back to the
  newest retained generation; `blocking` wrappers for events, logs, filter,
  diagnostics, and inspect. Docs in `docs/USAGE.md`.

## Verification

- Local Linux: native lifecycle (18 real-process cases, including tracing
  replay/filter/reload/restart, file tail and retention across restart and
  stop, managed line events, inspect reports, and blocking wrappers),
  layout/path-limit regression, rustfmt, strict Clippy, rustdoc, MSRV 1.81
  library check, all CI feature rows, and `git diff --check` pass. The MSRV
  check had been broken on `master` by the lock resolving uuid 1.24.1 and
  tempfile 3.27 to getrandom 0.4.3 (Edition 2024 manifest); the lock now
  pins uuid 1.16.0 and tempfile 3.20.0. Do not `cargo update` those without
  re-running the MSRV job.
- Hosted GitHub Actions: Ubuntu, macOS, and Windows all execute platform
  storage/security/transport/protocol contracts plus real embedded and managed
  lifecycle suites. The matrix also covers six feature configurations, public
  API compilation, repeated race/cancellation/crash/tree-cleanup stress,
  package validation, and RustSec audit. PR #1 merged to `master` as
  09f5d3a; post-merge run 29271759559 passed all 15 jobs. The `v0.1.0`
  release run 29273351819 passed every gate, published both crates to
  crates.io, and created the public GitHub release.
- Hosted failures improved the runtime: compact 128-bit state-directory routing
  avoids Windows atomic-store temporary-path limits, while Unix control sockets
  use a verified short per-user directory so macOS handles deep runtime roots.

## Next

The event/tail/inspect work is on branch `interactive-events` as PR #3.
CI run 36579888712 passed all 16 jobs (Ubuntu, macOS, Windows contracts,
seven feature rows including the new `tracing-native`, MSRV 1.81, stress,
trybuild, package, audit). Two CI-only fixes landed on the way: the layout
mtime test opens directories with backup semantics and write-attribute
access on Windows, and the pre-existing idle-shutdown test now uses a 400 ms
idle timeout so a loaded runner can connect before it fires. Awaiting review
and merge. Then: Phase 5 hardening, benchmarks, and the
debugger-cli/ghidra-cli migration spikes, which should exercise
`tail --follow`, `status --json` via `inspect`, and `set_log_filter` end to
end from a real CLI.
