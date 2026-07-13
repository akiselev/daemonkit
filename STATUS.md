# Status

Phases 0-4 are implemented. The native runtime now covers embedded daemons and
supervised managed children; Phase 5 hardening and consumer spikes are next.

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

- Local Linux: 69 unit/integration tests, seven compile API cases, one doctest,
  rustfmt, strict Clippy, rustdoc, and `git diff --check` pass.
- Cross-compilation: `x86_64-apple-darwin` and `x86_64-pc-windows-gnu` pass with
  all features. Native macOS/Windows execution remains CI evidence; it cannot be
  run from this Linux host.

## Next

Phase 5: run the broader platform/race/security/resource matrix, benchmark and
bound hot paths, exercise debugger-cli and ghidra-cli migration spikes, and
finish release documentation, support matrix, changelog, and security model.
