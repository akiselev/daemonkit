

## What is going wrong now

### `debugger-cli`: unsafe socket ownership

The largest bug is that socket cleanup is not serialized.

The client removes the socket before spawning. The daemon removes it again before binding. The daemon removes it again during shutdown.

That creates several races:

1. Two clients both fail to connect.
2. Both decide to spawn.
3. Each removes the socket path.
4. Both attempt to bind.
5. A losing or older daemon can later remove the winner’s socket during cleanup.

On Unix, unlinking a live socket pathname does not necessarily stop the already-bound listener. It can leave the original daemon alive but undiscoverable, while another daemon binds a new socket at the same path. You then have two generations running, with the old generation capable of deleting the new generation’s endpoint when it exits.

Other weaknesses:

* There is no lifecycle lock around `ensure → spawn → publish`.
* “Ready” means only that a transport connection succeeded; there is no version, generation, identity, or configuration handshake.
* Startup stdout and stderr are discarded, so the most valuable failure diagnostics disappear.
* `process_group(0)` creates a signal group, but it is not full daemonization or supervision.
* On Windows, the named-pipe identity is derived from `USERNAME`, and there is no explicit application-level security policy or handshake.
* The existing runtime directory gets mode `0700` only when newly created; ownership and permissions of an existing directory are not revalidated.

The concurrent actor/server refactor fixed application-level serialization problems, but daemon lifecycle serialization remains a separate unsolved problem.

### `ghidra-cli`: substantially better, but still generation-unsafe

`ghidra-cli` has several good mechanisms:

* per-project keys;
* a startup lock;
* double-checked startup after acquiring the lock;
* liveness-aware readiness;
* port plus ping verification;
* bounded startup separated from unbounded analysis;
* process-group/tree termination;
* captured startup output.

Those changes directly addressed previous orphaned JVM and pipe-hang failures.

The residual problems are structural.

#### The startup lock is not a lifecycle lock

`ensure_bridge_running` takes the `.starting` lock, but `stop_bridge` and `bridge_status` do not. Both may delete port, PID, and Ghidra lock files concurrently with a new startup.

A correct lock must serialize every lifecycle mutation:

* start;
* stop;
* restart;
* upgrade;
* stale cleanup;
* endpoint replacement;
* manifest replacement.

#### PID and port are separate, non-atomic generations

The port and PID are written to separate files. A reader can observe:

* the new port with the old PID;

* the old port with the new PID;

* one file missing;

* a wrapper PID before the Java PID replaces it.

A PID is also not process identity. PIDs are reused. `kill(pid, 0)` proves only that some process currently has that PID; the Windows `tasklist` substring check is weaker still.

#### Stale-lock recovery is brittle

The `.starting` file is created and then the PID is written. A crash between those operations can leave an empty lock file. Recovery removes the file only when it contains a parsable PID that is demonstrably dead; an empty or malformed file causes callers to wait until the full timeout.

A real OS file lock automatically releases when its owning process dies. It should be preferred over “lock-file existence plus PID contents.”

#### Project identity is underspecified

The project key is MD5 over `project_path.to_string_lossy()`.

That permits multiple daemon identities for the same project through:

* relative versus absolute paths;
* symlink aliases;
* Windows drive-letter and UNC variants;
* case variants on case-insensitive volumes;
* `..` components;
* lossy conversion of non-UTF-8 Unix paths.

The key policy needs to be explicit rather than hidden inside a hashing function.

#### TCP is unauthenticated

The Java bridge binds localhost and the Rust client connects by discovered port, but the shown client protocol has no generation token or authentication token.

Loopback prevents remote-network access; it does not prove that the client reached the correct project, generation, executable, or user-owned process.

#### External lock cleanup is dangerous

`cleanup_stale_files` removes Ghidra’s own project lock files.

Without proving that the lock belongs to the daemon generation being cleaned, this can remove a valid lock held by a manually launched Ghidra instance or another process.

## Patterns worth adopting

### Buck2: one lifecycle lock and compatibility constraints

Buck2 uses a stable lifecycle file and a cross-platform OS file lock. Cleanup is performed while holding that lock, and the lock file itself is preserved. It also retains the previous daemon directory for diagnostics.

After connecting, Buck2 checks daemon constraints such as version and startup configuration rather than assuming that any responding process is reusable.

This is the correct model for:

* one mutator at a time;
* restart on version mismatch;
* restart on incompatible configuration;
* explicit side-by-side isolation;
* retaining failed-generation logs.

### sccache: a private bootstrap channel created before spawn

sccache creates a temporary startup listener before launching the daemon. The child reports a typed startup result over that private channel. On Unix it uses a temporary Unix socket; on Windows it uses a random named pipe.

That eliminates ambiguous polling:

* the launcher knows exactly which child is reporting;
* bind errors can be reported directly;
* startup diagnostics are preserved;
* endpoint publication happens only after successful initialization;
* parallel bootstrap can be distinguished from real startup failure.

### Bazel: key daemon compatibility to workspace and startup configuration

Bazel treats its server as belonging to a particular workspace/output base and verifies that an existing server has appropriate startup options before reuse.

This maps directly to keyed project daemons.

# Proposed architecture

## 1. Instance identity

Define:

```rust
pub struct InstanceId {
    pub app: AppId,
    pub key: DaemonKey,
    pub isolation: IsolationId,
}
```

Examples:

```rust
DaemonKey::Global
DaemonKey::Path(PathKey::canonical(project_path)?)
DaemonKey::Opaque(project_uuid)
```

The instance directory should be:

```text
<runtime-root>/<app-id>/<key-digest>/<isolation>/
```

Use a cryptographic digest such as BLAKE3 or SHA-256 over the exact key bytes. Do not put raw project paths into socket names or pipe names.

Store the human-readable key representation inside metadata for diagnostics, but never use it as the endpoint pathname directly.

## 2. Two distinct locks

Use two stable files:

```text
lifecycle.lock
lease.lock
```

The **lifecycle lock** is acquired by clients during:

* ensure;
* start;
* stop;
* restart;
* repair;
* upgrade.

It is held only during the mutation.

The **lease lock** is held exclusively by the daemon for its entire lifetime. It answers a narrower question:

> Is some daemon generation still claiming this instance?

OS file locks automatically release when the process exits, including abnormal exits. They are much safer than creating and deleting lock files.

A manifest or PID must never substitute for either lock.

## 3. One atomic instance record

Replace separate PID, port, socket, and version files with one record:

```rust
#[derive(Serialize, Deserialize)]
pub struct InstanceRecord {
    pub schema_version: u32,

    pub app_id: String,
    pub key_digest: String,
    pub generation: Uuid,

    pub pid: u32,
    pub process_identity: ProcessIdentity,

    pub control_endpoint: Endpoint,
    pub service_endpoint: Option<Endpoint>,

    pub auth_token: SecretBytes,

    pub protocol: ProtocolRange,
    pub binary_fingerprint: String,
    pub config_fingerprint: String,

    pub started_at: SystemTime,
    pub log_path: PathBuf,
}
```

Write it as:

1. create a generation-specific temporary file;
2. serialize completely;
3. flush;
4. atomically rename to `instance.json`.

The manifest is discovery metadata, not proof of liveness. Proof comes from:

1. the lease lock;
2. connecting to the endpoint;
3. completing an authenticated generation handshake.

## 4. Transactional startup protocol

The startup sequence should be:

1. Acquire the lifecycle lock.
2. Read the existing instance record.
3. Attempt an authenticated handshake with the existing daemon.
4. Reuse it if it satisfies compatibility requirements.
5. Otherwise shut it down or repair stale state.
6. Create a private one-shot bootstrap listener before spawning.
7. Generate a random generation UUID and authentication token.
8. Spawn the daemon with the bootstrap endpoint and token passed through inherited handles or protected environment variables.
9. The child:

   * acquires the lease lock;
   * binds its final control endpoint;
   * initializes required application state;
   * reports `Prepared` over the bootstrap channel.
10. The launcher verifies the final endpoint and handshake.
11. The launcher atomically publishes `instance.json`.
12. The launcher sends `Commit`.
13. The child begins normal service.
14. Release the lifecycle lock.

If the bootstrap connection closes before `Commit`, the child exits and cleans up its generation-specific endpoint. That handles launcher crashes between spawn and publication.

Do not poll for arbitrary path existence.

## 5. Versioned control handshake

Every transport should begin with a small control handshake:

```rust
struct ClientHello {
    generation: Uuid,
    token: SecretBytes,
    supported_protocols: ProtocolRange,
    expected_app: AppId,
    expected_key_digest: Digest,
    client_build: BuildFingerprint,
}

struct ServerHello {
    generation: Uuid,
    pid: u32,
    process_identity: ProcessIdentity,
    selected_protocol: ProtocolVersion,
    server_build: BuildFingerprint,
    config_fingerprint: ConfigFingerprint,
    capabilities: Capabilities,
}
```

This protects against:

* stale endpoints;
* PID reuse;
* port reuse;
* a different local process listening at the address;
* an old daemon after a binary upgrade;
* the wrong project daemon;
* protocol skew.

Token authentication should be used even for Unix sockets and named pipes. Filesystem or ACL security restricts access; the token also identifies the intended generation.

## 6. Separate control plane from application data plane

This is important for Ghidra.

The crate’s control endpoint handles only:

* hello;
* status;
* shutdown;
* drain;
* diagnostics;
* endpoint discovery;
* compatibility metadata.

The service endpoint may be:

* the same stream for `debugger-cli`;
* a direct Java TCP endpoint for `ghidra-cli`;
* a child-owned socket;
* another protocol entirely.

For Ghidra, a small Rust supervisor can own the Ghidra process and publish the Java bridge’s TCP endpoint without proxying all Ghidra traffic.

That gives you:

```text
CLI ──control──> daemonkit supervisor
CLI ──data─────> Ghidra Java bridge
                       ^
                       |
                 owned child process
```

The extra Rust process retains the actual process handle, output pipes, readiness state, and shutdown authority. It removes the need for persistent PID and port files.

# Process models and tradeoffs

| Model                                | Advantages                                                                                          | Costs                                                                                                            |
| ------------------------------------ | --------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Direct daemon process                | One background process; simplest; appropriate for `debugger-cli`                                    | Limited ability to supervise its own crash; weaker orphan-child cleanup on Unix                                  |
| Per-key supervisor plus worker       | Retains child handles; captures logs; robust external-process management; best for Ghidra/JVM trees | One additional process per key; more implementation complexity                                                   |
| One global broker with keyed workers | Central registry, logs, upgrades, process handles                                                   | Broker becomes a single point of failure; bootstrap and versioning become harder; unrelated projects are coupled |
| One multi-tenant daemon              | Lowest process overhead                                                                             | Crash/resource/config isolation is poor; one bad project affects all projects                                    |
| systemd/launchd/Windows Service      | Strong native supervision and restart behavior                                                      | Requires installation and platform-specific configuration; awkward for arbitrary per-project instances           |

My recommendation:

* `EmbeddedService` mode for `debugger-cli`.
* `ManagedChild` mode for `ghidra-cli`.
* No global broker in the first design.
* Abstract the spawn backend so native service-manager integration can be added later.

## Suggested service interfaces

```rust
pub enum ServiceMode<S> {
    Embedded(S),
    ManagedChild(ManagedChildSpec),
}
```

An embedded service receives incoming streams:

```rust
#[async_trait]
pub trait DaemonService: Send + 'static {
    async fn run(
        self,
        context: DaemonContext,
        incoming: Incoming,
        shutdown: ShutdownSignal,
    ) -> Result<()>;
}
```

A managed child describes:

```rust
pub struct ManagedChildSpec {
    pub command: CommandSpec,
    pub readiness: ReadinessProbe,
    pub tree_policy: ProcessTreePolicy,
    pub service_endpoint: EndpointPublication,
    pub shutdown: ShutdownStrategy,
}
```

This is general enough for:

* Java processes;
* language servers;
* database sidecars;
* browser automation servers;
* debugger adapters;
* Python workers.

# Transport design

Use a public configuration enum rather than immediately exposing a highly generic transport trait:

```rust
pub enum Transport {
    Local(LocalTransportOptions),
    TcpLoopback(TcpOptions),
    #[cfg(target_os = "linux")]
    LinuxAbstract(AbstractSocketOptions),
}
```

Internally, normalize all of them into:

```rust
trait Listener {
    type Stream: AsyncRead + AsyncWrite;
    fn endpoint(&self) -> Endpoint;
    async fn accept(&self) -> io::Result<Self::Stream>;
}
```

Recommended defaults:

* Linux/macOS/BSD: filesystem Unix-domain socket.
* Windows: named pipe.
* TCP loopback: opt-in, mainly for foreign-language workers such as Java.
* Linux abstract sockets: optional optimization, never the portable default.

Unix-domain socket paths have small platform-specific limits; Linux’s `sun_path` is 108 bytes, and other Unix variants can be more restrictive. Use short hashed paths beneath the runtime directory. Linux abstract sockets are not portable to macOS or BSD. ([man7.org][1])

The transport layer should provide only a bounded framed byte stream. Application JSON, DAP, gRPC, or custom RPC should remain above it.

# Cross-platform process nuances

## Unix

### Process groups are not sessions

Rust’s `process_group(0)` maps to process-group configuration. It does not create a new session or fully detach the process from its controlling terminal. `setsid` is a separate operation. ([Rust Documentation][2])

Avoid a generic `pre_exec` callback API. Running arbitrary Rust code between `fork` and `exec` in a multithreaded process is unsafe and heavily constrained. ([Rust Documentation][2])

Preferred approaches:

* use stable `CommandExt` operations where available;
* run the daemon in the foreground from the child process;
* have the launcher detach it through controlled file descriptors and process-group/session configuration;
* avoid traditional double-fork daemonization unless explicitly required.

### Process groups do not prevent orphaned grandchildren

Killing a process group works when you still know the correct group and no identity reuse has occurred. It does not automatically kill grandchildren when the daemon itself crashes.

For strict tree ownership, use one of:

* a persistent supervisor process;
* Linux cgroups/systemd;
* cooperative lifetime pipes where workers exit when the parent-control pipe closes;
* Linux parent-death signals for narrowly controlled direct children;
* a native service manager.

For Ghidra, the per-key supervisor is the cleanest portable answer.

### PID files are inherently weak

On Linux, pidfds provide a stable reference to a particular process and avoid PID-reuse ambiguity while the descriptor is held. They are pollable and can be used for signaling. ([man7.org][3])

But a pidfd cannot simply be serialized into `instance.json`. A process must keep the file descriptor open. Therefore:

* a persistent supervisor can benefit greatly from pidfds;
* independent CLI invocations still require handshake, lease lock, and process-start identity;
* the PID remains diagnostic metadata and a last-resort lookup key.

### Runtime-directory security

Prefer `$XDG_RUNTIME_DIR`. For `/tmp` fallback:

* create a per-user directory;
* verify ownership every time;
* reject symlinks;
* verify mode;
* open relative to a directory handle where practical;
* never blindly remove a path that is not proven to belong to your generation.

Do not place lifecycle files in the project directory. Project directories may reside on NFS, SMB, FUSE, removable disks, or synced folders with unsuitable lock and rename semantics.

## Windows

### Creation flags are not supervision

`CREATE_NEW_PROCESS_GROUP` primarily establishes console-control signal grouping. `DETACHED_PROCESS` controls console attachment. Neither one owns the descendant tree. ([Microsoft Learn][4])

Use Job Objects for process-tree ownership. A Job Object can group processes, terminate the group, and use `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` to kill members when the final job handle closes. ([Microsoft Learn][5])

For `ManagedChild`:

1. create the process suspended;
2. create/configure a Job Object;
3. assign the process;
4. establish deliberate handle inheritance;
5. resume it;
6. retain the job handle in the supervisor.

Be aware that nested Job Object behavior depends on Windows version and job restrictions. ([Microsoft Learn][5])

### Handle inheritance is a major source of hangs

Your tests already document a real instance: a restarted JVM inherited stdout/stderr pipe handles and kept the parent waiting indefinitely.

The crate should default to:

* no inherited handles;
* an explicit allowlist for bootstrap and log handles;
* `STARTUPINFOEX` plus a handle list on Windows where needed;
* closing the parent’s duplicate copies immediately after spawn.

### Named pipes need explicit security

Windows named pipes support security descriptors. The default descriptor can grant more access than a per-user daemon should permit, including read access to broader principals. Use an explicit DACL based on the current user or logon SID, and reject remote clients. ([Microsoft Learn][6])

Tokio exposes named-pipe controls including first-instance behavior and raw security attributes. ([Docs.rs][7])

Use:

* a random or hashed pipe name;
* `first_pipe_instance(true)` for ownership arbitration;
* `reject_remote_clients(true)`;
* explicit current-user/logon-session ACLs;
* application token authentication.

Named pipes also have an unusual listener model: multiple server instances are created for simultaneous connections. The transport wrapper must create the next server instance before or immediately after handing the connected instance to a client task.

### Do not shell out to `tasklist` or `taskkill` as the primary API

Use native process handles, creation timestamps, Job Objects, and `TerminateJobObject`. Shell commands should remain an emergency fallback, not the core abstraction.

## macOS

Use filesystem Unix sockets. There is no Linux abstract namespace or pidfd equivalent.

For process observation, a retained child handle or supervisor is preferable. `launchd` can be an optional backend, but installing a launch agent for every transient project daemon is too intrusive as the default.

Path identity also requires care because different macOS volumes may have different case-sensitivity behavior. Do not normalize by blindly lowercasing paths.

# Keyed daemon semantics

Path keying requires an explicit policy:

```rust
pub enum PathIdentityPolicy {
    LexicalAbsolute,
    CanonicalExisting,
    FileIdentity,
    ApplicationUuid,
}
```

Tradeoffs:

* `LexicalAbsolute`: deterministic and works before creation, but aliases through symlinks.
* `CanonicalExisting`: deduplicates most aliases, but requires the path to exist and follows symlinks.
* `FileIdentity`: device/inode on Unix or volume/file ID on Windows; strongest for an existing object, but identity changes after copying and may behave poorly on network filesystems.
* `ApplicationUuid`: strongest long-term identity when the project format can store its own UUID.

For Ghidra, use either:

1. canonical project path initially; or
2. a project UUID persisted in ghidra-cli metadata, with canonical path retained only as descriptive metadata.

The application should decide the identity policy. The lifecycle crate should not silently canonicalize everything.

# Shutdown state machine

Model lifecycle explicitly:

```text
Absent
  → Starting
  → Ready
  → Quiescing
  → Stopping
  → Absent
```

Shutdown should be:

1. acquire lifecycle lock;
2. reconnect and authenticate the expected generation;
3. issue `Drain`;
4. stop accepting new application work;
5. wait for active requests and application leases;
6. issue `Shutdown`;
7. wait for process/lease termination;
8. escalate to tree termination;
9. remove only files and endpoints belonging to that generation;
10. clear the manifest.

Clients that reach a daemon in `Quiescing` should receive a typed retry response. They can release their connection, reacquire through `ensure`, and connect to the successor.

This closes the idle-shutdown race where a client connects just as the daemon decides to exit.

# Ergonomic API direction

Client side:

```rust
let ghidra = Daemon::builder(AppId::new("com.akiselev.ghidra-cli")?)
    .key(ProjectKey::canonical(project_path)?)
    .isolation(IsolationId::default())
    .transport(Transport::local())
    .compatibility(
        Compatibility::restart_on_mismatch()
            .protocol(1..=2)
            .binary(build_fingerprint!())
            .config(config_fingerprint),
    )
    .spawn(Spawn::current_exe("__ghidra_supervisor"))
    .startup_timeout(Duration::from_secs(180))
    .shutdown_timeout(Duration::from_secs(10))
    .logs(LogPolicy::rotating())
    .build()?;

let instance = ghidra.ensure().await?;
let endpoint = instance.service_endpoint()?;
```

Daemon entry:

```rust
if let Some(bootstrap) = Bootstrap::from_process()? {
    bootstrap
        .run_managed_child(
            CommandSpec::new(analyze_headless)
                .args(args)
                .tree_policy(ProcessTreePolicy::KillOnSupervisorExit),
            ReadinessProbe::tcp_json_ping("ping"),
        )
        .await?;

    return Ok(());
}
```

Debugger entry:

```rust
if let Some(bootstrap) = Bootstrap::from_process()? {
    bootstrap
        .run_embedded(|context, incoming, shutdown| async move {
            debugger_service
                .serve(context, incoming, shutdown)
                .await
        })
        .await?;

    return Ok(());
}
```

Provide both async and blocking client facades:

```rust
Daemon::ensure().await
BlockingDaemon::ensure()
```

Internally, I would make the primary implementation Tokio-based. A split such as `daemon-runtime-core` plus `daemon-runtime-tokio` is architecturally clean, but premature until the lifecycle protocol stabilizes.

# Testing strategy

The crate’s principal test target is not ordinary success. It is every interruption point in the lifecycle transaction.

Add deterministic fault injection at:

* before lifecycle lock;
* after lock;
* before spawn;
* immediately after spawn;
* after child lease acquisition;
* after final endpoint bind;
* after `Prepared`;
* before manifest rename;
* after manifest rename;
* before `Commit`;
* during graceful shutdown;
* immediately before endpoint removal.

Essential tests:

1. Fifty concurrent `ensure()` calls produce exactly one generation.
2. The startup client crashes at each injection point.
3. The daemon crashes immediately before and after manifest publication.
4. An old generation exits after a successor starts.
5. Stop races with start.
6. Restart races with restart.
7. Status races with cleanup.
8. Manifest contains mismatched PID, port, token, and generation.
9. A PID is reused by an unrelated process.
10. The executable or configuration fingerprint changes.
11. The endpoint exists but speaks the wrong protocol.
12. Unix socket pathname is removed while the server is alive.
13. Very long Unicode paths and non-UTF-8 Unix paths.
14. Windows UNC paths, drive-letter variants, and inherited handles.
15. JVM wrapper exits while a grandchild remains alive.
16. Terminal closure, Ctrl-C, logout, and parent-process death.
17. Malformed and oversized control frames.
18. Runtime directory has the wrong owner, mode, or is a symlink.

Do not serialize these tests globally. Lifecycle concurrency tests need isolated random instance roots so they can run concurrently and expose the races that `#[serial]` currently conceals.

# Implementation sequence

### Phase 1: correctness foundation

Implement:

* instance keys and local runtime layout;
* lifecycle and lease locks;
* atomic instance record;
* generation/token handshake;
* private bootstrap channel;
* filesystem UDS and Windows named-pipe transports;
* embedded daemon mode;
* structured logs and diagnostics;
* concurrent lifecycle stress tests.

Migrate `debugger-cli` first. It has the simpler one-process topology and the most obvious socket race.

### Phase 2: managed children

Implement:

* retained child handles;
* readiness probes;
* stdout/stderr capture without inherited-handle leaks;
* graceful-to-forceful shutdown ladders;
* Unix process groups/supervisor behavior;
* Windows Job Objects;
* optional Linux pidfd support;
* separate control and service endpoints.

Then migrate `ghidra-cli` from port/PID files to a per-project supervisor.

### Phase 3: extensions

Add:

* TCP loopback transport;
* Linux abstract sockets;
* custom transport extension points;
* systemd user units;
* launchd agents;
* Windows Service backend;
* daemon listing and garbage collection;
* observability hooks and metrics.

## Bottom line

The central invariant should be:

> An endpoint may be removed or replaced only while holding the instance lifecycle lock and only after proving that it does not belong to a live authenticated generation.

The next most important invariant is:

> A daemon is reusable only after a versioned, token-authenticated handshake confirms its application, key, generation, protocol, binary, and configuration.

Those two rules eliminate most of the current socket, PID-file, stale-lock, parallel-start, and wrong-generation failure modes.


[1]: https://man7.org/linux/man-pages/man7/unix.7.html "unix(7) - Linux manual page"
[2]: https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html "CommandExt in std::os::unix::process - Rust"
[3]: https://man7.org/linux/man-pages/man2/pidfd_open.2.html "pidfd_open(2) - Linux manual page"
[4]: https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags "Process Creation Flags (WinBase.h) - Win32 apps | Microsoft Learn"
[5]: https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects "Job Objects - Win32 apps | Microsoft Learn"
[6]: https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-security-and-access-rights "Named Pipe Security and Access Rights - Win32 apps | Microsoft Learn"
[7]: https://docs.rs/tokio/latest/tokio/net/windows/named_pipe/struct.ServerOptions.html "ServerOptions in tokio::net::windows::named_pipe - Rust"
