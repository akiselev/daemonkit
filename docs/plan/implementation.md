# v1 implementation plan

## Internal model

The lifecycle kernel is an explicit state machine driven under the instance's
lifecycle lock:

```text
Absent -> Preparing -> Published -> Ready -> Quiescing -> Stopping -> Absent
             |             |
             +-- rollback -+----> Unhealthy -> repair -> Absent
```

`Published` is not serviceable until the launcher sends `Commit`. On launcher
failure, the uncommitted child's bootstrap channel closes and the child rolls
back its generation-specific resources.

The kernel is independent of Tokio types where practical. Async orchestration,
signals, local sockets/pipes, and process I/O use Tokio through
`DefaultPlatform`; v1 does not build a general runtime abstraction.

The workspace contains `daemonkit` and `daemonkit-testkit`. The low-level
simulator platform lives feature-gated in daemonkit so it can implement the
sealed platform contract without a circular dependency; the companion crate
owns the public scenario, scheduling, exploration, invariant, trace, fixture,
and backend-contract APIs described in [`testkit.md`](testkit.md).

## On-disk layout

```text
<verified-runtime-root>/daemonkit/
  <app-digest>/
    app.json                         diagnostic metadata
    <key-digest>/
      <isolation>/
        lifecycle.lock              stable; never removed during normal repair
        lease.lock                  stable; daemon holds for its lifetime
        instance.json               atomic discovery record
        generations/
          <generation>/
            control.sock            Unix only; generation-owned
            daemon.log
            stdout.log
            stderr.log
            startup.json
```

Windows pipe names are derived from the same digests and generation but are not
filesystem children. Their security descriptor restricts access to the current
logon identity, disallows remote clients, and requests first-instance ownership.

`instance.json` contains schema, app/key/isolation digests, generation, process
metadata, control endpoint, optional service endpoint, protocol range, build and
config fingerprints, timestamps, and log paths. The authentication token is
stored only if protected state-directory permissions meet the platform policy.
If a platform cannot make that guarantee, startup fails closed.

Atomic update means create a unique temporary file in the same directory,
serialize, flush the file, rename/replace atomically, and sync the parent
directory where the OS supports a meaningful operation. Tests model crashes at
every step. A corrupt record is retained for diagnostics and never partly
interpreted as authority.

## Secure runtime roots

Unix preference order is a valid `$XDG_RUNTIME_DIR`, then an OS-appropriate
per-user cache/runtime location with a securely created private directory. A
`/tmp` fallback, if supported, uses a per-user directory opened and verified
without following symlinks. Verify owner, file type, and mode on every use.

Windows uses a current-user local application-data/runtime location with ACL
verification. Usernames are never security identities or pipe namespace keys;
use the current access token/logon SID.

All later path operations are relative to trusted directory handles where the
platform API permits it. Hash-based names keep UDS paths beneath the smallest
portable path budget. No project directory is used for daemonkit lifecycle
state.

## Locks and ownership

- `lifecycle.lock` is an exclusive cross-process OS file lock around `ensure`,
  `stop`, `restart`, and `repair`. The stable file is opened securely and is not
  replaced while locks may exist.
- `lease.lock` is acquired by the child before binding and held through service
  shutdown and endpoint cleanup. It answers whether a generation still claims
  the instance; it does not authenticate that generation.
- Lock acquisition has a timeout and cancellation-aware wait loop with jittered
  notification/backoff. Timeout diagnostics identify the instance and elapsed
  time, never another process's unverifiable PID as fact.
- A manifest, PID, endpoint, or lock-file existence is not a substitute for an
  acquired OS lock.

Start with owning production lock guards. Introduce a private guard GAT only if
the in-memory/fault backend materially benefits; do not add it merely to satisfy
the architecture diagram.

## Control protocol

Use a small length-delimited binary protocol with:

- a fixed magic and framing version;
- strict maximum frame sizes before allocation;
- request IDs for control operations;
- an explicit client/server protocol negotiation range;
- exhaustive internal messages and non-exhaustive public capability values;
- constant-time token comparison;
- deadlines for every handshake and control request.

Handshake fields cover app, key digest, isolation, generation, token, protocol,
server process identity, build fingerprint, config fingerprint, lifecycle state,
and capabilities. A connection declares its role after authentication:
`Control` or `Application`. Application-role streams are handed to the embedded
service with framing removed.

Control v1 includes `Hello`, `Status`, `Drain`, `Shutdown`, `Diagnostics`, and
`OpenApplicationStream`. `BusyQuiescing` is a typed response which tells a
caller to re-run `ensure`; it is not an opaque I/O error.

Malformed, oversized, unauthenticated, replayed-to-a-different-generation, and
slowloris handshakes are rejected without reaching application code.

## Transactional ensure

Under the lifecycle lock:

1. Read the atomic record, if any.
2. Authenticate its generation within a bounded deadline.
3. If healthy and compatible, return an `Instance`.
4. If healthy but incompatible, apply the explicit mismatch policy.
5. If unhealthy, prove what is stale using handshake, lease state, and retained
   native process identity; never delete from PID alone.
6. Quiesce/stop an incompatible live generation and wait for lease release,
   escalating only after identity is rechecked.
7. Create the generation directory, random generation ID, random token, and a
   private inherited bootstrap channel before spawn.
8. Spawn the hidden child with deliberate stdio, session/tree, and handle
   inheritance policy.
9. Child acquires the lease, binds its generation endpoint, initializes its
   embedded service or managed child, and reports `Prepared` plus endpoint and
   process metadata.
10. Launcher connects to and authenticates the final control endpoint.
11. Launcher writes and atomically publishes `instance.json`.
12. Launcher sends `Commit`; child enters `Ready` and begins accepting
    application streams.
13. Return an authenticated `Instance` and release the lifecycle lock.

Concurrent callers double-check only inside the lock; fifty callers must reuse
the same committed generation. Endpoint names are never reused between
generations, so an old process cannot unlink a successor's address.

## Shutdown, restart, and repair

Shutdown under the lifecycle lock re-authenticates the record's generation,
requests drain, waits for active application streams within policy, requests
shutdown, waits for lease release, and escalates to platform-native tree
termination. Cleanup then removes only resources inside the proven generation
directory and atomically clears the matching manifest.

Idle shutdown uses the same state machine. The daemon transitions to
`Quiescing` before deciding that it is idle, stops accepting application roles,
and aborts or completes according to a documented race policy. A newly arriving
client receives `BusyQuiescing` and retries through `ensure`.

`restart` holds one lifecycle lock across old-generation shutdown and successor
commit. `repair` never touches application-owned files. If state cannot be
proved safe, it returns an actionable `Unhealthy`/`Busy` result instead of
guessing.

Retain a bounded number/size of previous generation logs. Pruning occurs only
under the lifecycle lock and never deletes the active or most recently failed
generation.

## Process implementations

### Unix

- Spawn the daemon child in a new session using a narrow audited platform
  implementation; avoid user-provided `pre_exec` closures.
- Deliberately route stdin, stdout, and stderr. Close launcher copies promptly.
- For a managed child, retain its real child handle and establish an owned
  process group. Use cooperative lifetime channels where supported.
- Graceful signals target the verified process; forceful escalation targets the
  owned group after rechecking identity.
- A supervisor is the portable ownership mechanism. Linux pidfds and cgroups
  are later optimizations, not v1 correctness dependencies.

### Windows

- Create with explicit inherited handles using `STARTUPINFOEX`; default to no
  inheritance.
- Put managed descendants in a Job Object with kill-on-close. Use suspended
  creation when required to assign the process before it can spawn children.
- Retain process and Job handles until exit and output EOF.
- Use native handles and creation identity for status/termination. Do not shell
  out to `tasklist` or `taskkill` as the primary implementation.
- Build named-pipe listeners with explicit current-user/logon ACLs,
  first-instance semantics, local-only clients, and correct multi-instance
  accept replenishment.

### macOS

Use filesystem UDS and retained supervisor/child handles. Do not assume Linux
abstract sockets, pidfds, `/proc`, or case-insensitive volumes. Path policies
remain caller-selected.

## Logs and diagnostics

The launcher captures startup output from the first instruction after spawn.
The child keeps persistent daemon and managed-child logs while bounded ring
buffers retain tails for errors. Readers run concurrently so a full pipe cannot
deadlock readiness or exit. EOF is awaited after process exit with a separate
bound so leaked descendant handles become a diagnostic, not an infinite wait.

Commands are rendered with secret environment values and configured secret
arguments redacted. Diagnostics record every startup phase and readiness attempt
with monotonic timing. UTF-8 rendering is lossy only at display time; raw logs
retain original bytes.

## Build phases

Each phase lands only through the red/green sequence in `testkit.md`: failing
focused scenario, failing unsafe interleaving/invariant where applicable, green
kernel, green backend contract, then green native platform coverage. Later
phases must not weaken earlier invariants.

### Phase 0: workspace, testkit, API, and protocol skeleton

- Create the two-crate workspace, feature layout, shared MSRV/version policy,
  rustdoc landing pages, and public types from `api.md` and `testkit.md`.
- Add compile-only examples for embedded, managed, attach-only, blocking, and
  custom-transport use.
- Define internal state transitions, protocol messages, record schema, error
  taxonomy, redaction wrappers, and fault points.
- Implement the deterministic clock, entropy, scheduler, event trace, world
  model, `SimPlatform`, replay token, and default invariant engine before native
  spawning.
- Add the first public scenario DSL examples and simulator self-tests.

Exit gate: public examples infer cleanly, compile-fail mode tests pass, and a
pure model can execute every legal transition, reject illegal ones, replay a
seeded schedule exactly, and demonstrate every default invariant failing in a
negative test.

### Phase 1: identity, roots, locks, and store

- Implement validation and domain-separated key hashing.
- Write red scenarios for alias policy, unsafe roots, concurrent lock ownership,
  interrupted atomic publication, and corrupt records.
- Implement secure roots and stable lifecycle/lease locks on all platforms.
- Implement atomic records, corruption handling, generation directories, and
  retention.
- Add cross-process lock and crash-at-rename test executables.

Exit gate: concurrent processes cannot enter a lifecycle mutation together;
root attacks fail closed; store fault tests never yield a partially valid
record on Linux, macOS, or Windows.

### Phase 2: native transport and authenticated control

- Implement UDS and secured Windows named pipes.
- Write red scenarios for partial I/O, endpoint squatting/reuse, handshake
  mismatch, slow authentication, and application-stream handoff.
- Implement bounded framing, handshake, capabilities, control state, and
  application stream handoff.
- Implement custom object-safe transport adaptation behind `advanced` without
  exposing private backend traits.

Exit gate: wrong app/key/isolation/generation/token/protocol is rejected;
malformed and oversized traffic stays bounded; embedded stream echo works on
all platforms.

### Phase 3: bootstrap and embedded lifecycle

- Implement inherited one-shot bootstrap channel, controlled spawn/detachment,
  prepared/publish/commit, cancellation cleanup, compatibility, and stale
  repair.
- Explore bounded prepare/publish/commit schedules and retain replay cases for
  every discovered failure before implementing the native path.
- Implement embedded service trait/closure, drain, shutdown, idle timeout,
  diagnostics, and blocking parent facade.
- Stress concurrent ensure/restart/stop using real processes.

Exit gate: the entire interruption matrix passes and the debugger-cli-shaped
fixture never creates two discoverable generations or loses startup output.

### Phase 4: managed children

- Implement command safety, readiness probes, endpoint publication, concurrent
  output capture, graceful shutdown ladders, and process-tree ownership.
- Model readiness/output/exit and drain/terminate/kill interleavings first;
  preserve minimized failing schedules as regressions.
- Implement Unix groups/supervisor behavior and Windows Job Objects.
- Add JVM-like wrapper/grandchild fixtures that reproduce inherited-pipe and
  orphan-tree failures.

Exit gate: every managed descendant is gone after forced stop or supervisor
exit; readiness cannot publish the wrong child endpoint; ghidra-cli-shaped
fixture uses no PID/port files.

### Phase 5: hardening and consumer spikes

- Run the full platform, race, corruption, resource-limit, and security matrix.
- Measure ensure latency, binary size, allocations, and log bounds; optimize
  only where results justify it.
- Build temporary migration branches or examples for debugger-cli and
  ghidra-cli and feed API friction back into this plan.
- Complete rustdoc, changelog, security model, support matrix, and release
  checklist.

Exit gate: all `README.md` definition-of-done items hold and no unresolved API
question requires a breaking change known before 1.0.

## Dependency selection rules

Choose dependencies during the phase that needs them, after auditing current
platform behavior and MSRV. Prefer small Rust libraries with active Windows and
Unix coverage. Do not select a lock, IPC, or process crate from its API shape
alone: run semantic probes for duplicate handles, lock release on crash, rename
replacement, named-pipe ACLs, process-tree termination, and cancellation.

Cryptographic randomness must come from an OS-backed CSPRNG. Key and fingerprint
hashing must be domain-separated and collision-resistant. Serialization and
error libraries must support strict input limits and source preservation.

## Documentation deliverables

- crate-level quick start for embedded and managed modes;
- security model and local-attacker assumptions;
- state directory and retention contract;
- key identity guide with path-policy examples;
- compatibility and upgrade guide;
- platform behavior/support table;
- startup and shutdown troubleshooting guide centered on diagnostics;
- migration guides for debugger-cli and ghidra-cli patterns;
- advanced platform/transport guide clearly marked as a stability boundary.
