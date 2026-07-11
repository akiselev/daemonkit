# daemonkit v1 plan

This directory is the plan of record for daemonkit v1. The design follows the
"Final architectural choice" in [`../notes/api.md`](../notes/api.md): a concrete
facade, one statically dispatched platform bundle, optional object-safe
extensions, and GATs only in private borrowing-oriented traits.

The plan is split into:

- [`api.md`](api.md): the public contract and ergonomic examples;
- [`implementation.md`](implementation.md): internal invariants, components,
  phases, and deliverables;
- [`testkit.md`](testkit.md): the first-class deterministic simulator, scenario
  API, invariant engine, and red/green workflow;
- [`testing.md`](testing.md): the cross-platform verification and fault matrix.

The notes remain design inputs. When a sketch in the notes conflicts with this
directory, this directory is the v1 decision.

## v1 outcome

daemonkit v1 will let an application describe one global or keyed local daemon,
start or reuse exactly one compatible generation, and receive either:

- an authenticated local byte stream for an embedded service; or
- a verified, child-published endpoint for an externally managed service.

The crate owns identity, secure state directories, lifecycle serialization,
bootstrap, spawning, detachment, readiness, compatibility, discovery, control,
shutdown escalation, logs, process-tree cleanup, and stale-state repair. It does
not own application framing, requests, responses, or concurrency.

## architectural decision

```text
Public facade
  Daemon<Mode, P = DefaultPlatform>       parent-side lifecycle controller
  DaemonClient<Mode, P = DefaultPlatform> attach-only controller
  Instance<Mode>                          authenticated generation snapshot
  Bootstrap                               child-side entry
       |
       v
Generic kernel over one Platform bundle
  transport + locks + processes + store + clock
       |
       +-- enum dispatch for built-in transports
       +-- boxed object-safe adapters for optional extensions
       +-- private GATs only for genuinely borrowed guards/transactions
```

`Mode` is `Embedded` or `Managed`; callers normally never write it. This keeps
invalid operations out of method completion without exposing a many-parameter
typestate builder.

The service object is deliberately not stored in the parent-side `Daemon`.
Values cannot be carried through `exec`, and pretending otherwise makes the API
misleading on Windows. The executable constructs the service after
`Bootstrap::detect()` in the spawned child.

## non-negotiable invariants

1. Every lifecycle mutation is performed while holding the stable lifecycle
   lock for the instance.
2. The daemon holds the stable lease lock for its entire lifetime.
3. A manifest is discovery data, never proof that a daemon is alive.
4. Reuse requires a token-authenticated handshake matching app, key,
   isolation, generation, protocol, binary policy, and configuration policy.
5. Publication is transactional: bind and prepare, atomically publish, then
   commit. A child that loses its uncommitted bootstrap channel exits.
6. An endpoint is generation-specific. Cleanup removes it only after proving
   ownership and while holding the lifecycle lock.
7. PID is diagnostic metadata. It is never sufficient identity for reuse or
   destructive cleanup.
8. Runtime roots are verified on every use; symlinks, wrong ownership, and
   unsafe permissions are errors, not conditions daemonkit silently repairs.
9. Secrets are never emitted by `Debug`, `Display`, logs, diagnostics, or error
   sources.
10. Graceful shutdown is bounded and always has a platform-native tree-kill
    escalation path for managed children.

## v1 scope

Included:

- a two-crate workspace: `daemonkit` plus the publishable
  `daemonkit-testkit` dev dependency;
- an `async` primary API and a `blocking` facade;
- Tokio as the only executor integration;
- global, string, UUID, and explicit path keys;
- filesystem Unix-domain control sockets on Unix;
- secured local named-pipe control transport on Windows;
- embedded service and managed child modes;
- one-shot inherited bootstrap channel;
- atomic JSON instance records and retained generation logs;
- compatibility checks and restart/error mismatch actions;
- Unix session/process-group handling and Windows Job Objects;
- deterministic simulation, schedule exploration, replayable traces, invariant
  checking, isolated native roots, and fault injection through
  `daemonkit-testkit`;
- CI and platform tests on Linux, macOS, and Windows.

Explicitly deferred until the lifecycle protocol is stable:

- an RPC or framing layer;
- executor independence;
- a global broker or multi-tenant daemon;
- systemd, launchd, or Windows Service installation;
- Linux abstract control sockets and pidfd optimizations;
- daemon enumeration and garbage collection across applications;
- a daemon entry macro;
- persistent ABI/plugin loading. Custom v1 adapters are Rust trait objects, not
  a stable binary plugin ABI.

## defaults chosen for ergonomics

- `DaemonSpec::new(app)` uses `GlobalKey` and default isolation.
- Native local control transport is automatic.
- Compatibility defaults to the daemonkit control protocol plus exact
  application binary and configuration fingerprints supplied by the caller.
- A mismatch restarts by default; applications can choose error or a distinct
  isolation explicitly.
- Logs capture bounded in-memory tails and persist full output under the
  generation directory.
- Shutdown drains, requests graceful exit, then kills the owned tree after the
  configured deadline.
- `ensure()` is the main parent operation; `Instance::connect()` is the main
  embedded data-plane operation.

Defaults that could destroy unrelated state are intentionally absent. In
particular, daemonkit never deletes application-owned locks such as Ghidra
project locks.

## definition of done

v1 is complete only when:

- the API examples in `api.md` compile as documentation tests;
- every lifecycle transition has a durable invariant test;
- every implementation slice begins with a failing deterministic scenario and
  finishes with simulation, backend-contract, and affected-platform tests green;
- the interruption matrix in `testing.md` passes deterministically;
- seeded simulation failures replay exactly from their emitted replay token;
- real process tests pass on Linux, macOS, and Windows CI runners;
- sanitizer/loom-style model tests cover in-process synchronization where
  applicable, without replacing real multi-process tests;
- a small fixture application demonstrates embedded and managed modes;
- migration spikes prove that debugger-cli can consume the embedded API and
  ghidra-cli can consume the managed API without application protocol code in
  daemonkit;
- startup errors retain phase, safe command summary, stdout/stderr tails, log
  path, and instance path;
- API review confirms that the common examples require no explicit generic
  arguments and do not expose internal backend types in ordinary errors.
