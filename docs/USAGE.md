# Using daemonkit

## Choose a daemon shape

Use `Daemon::embedded` when your executable can host the service directly. The
parent calls `ensure()` and receives an `Instance<Embedded>`; the bootstrap
path receives authenticated application streams and chooses the application
framing.

Use `Daemon::managed` when the bootstrap process must supervise a separate
program. It configures a `ManagedChild` with readiness and shutdown policy;
after `ensure()`, the returned `Instance<Managed>` exposes the published
application endpoint. daemonkit does not proxy or reinterpret that protocol.

## Bootstrap boundary

At the very start of the executable's daemon entry path, check for the private
bootstrap channel:

```rust,no_run
use daemonkit::Bootstrap;

async fn maybe_run_daemon() -> Result<bool, daemonkit::Error> {
    let Some(bootstrap) = Bootstrap::detect()? else {
        return Ok(false);
    };

    bootstrap
        .run_embedded_fn(|_context, _incoming, mut shutdown| async move {
            shutdown.requested().await;
            Ok::<_, std::io::Error>(())
        })
        .await?;
    Ok(true)
}
```

Do not add an application-controlled environment-variable switch for daemon
mode. `Bootstrap::detect()` validates daemonkit's versioned inherited channel;
the launcher reserves and rejects the associated environment variable names.

## Identity and state

`DaemonSpec::new` takes a reverse-DNS-style application identifier. Add a
`DaemonKey`—for example `PathKey`, `StringKey`, or `UuidKey`—when instances
need distinct state. Use `IsolationId` when separate users, profiles, or
tenants must not share a daemon instance.

By default, daemonkit selects a platform runtime location. `RuntimeRoot` is
mainly for controlled embedding and tests. On Unix, a deep runtime root is
safe: control sockets use a verified short per-user directory to stay within
portable Unix-domain socket limits.

## Lifecycle operations

- `ensure()` attaches to a compatible authenticated instance or starts one.
- `status()` reports current lifecycle state without creating an instance.
- `restart()` replaces the generation after a controlled shutdown.
- `stop()` requests graceful shutdown and forces cleanup if required.
- `repair()` removes only stale state that can be proven owned by the instance.

An `Instance` carries a generation identity. If another caller replaces the
generation, operations on an older instance return `Error::GenerationChanged`
rather than acting on the successor.

## Failure handling

Treat `Error::Startup` as a diagnostic-rich failure: it includes the startup
phase, redacted command description, bounded output tails, and the generation
log path. Do not retry arbitrary errors by deleting state yourself; use
`repair()` or a fresh `ensure()` so ownership and generation checks remain in
force.

For deterministic tests, pair native process tests with `daemonkit-testkit`.
Its scenarios model lifecycle interleavings, faults, and replayable invariant
violations without weakening the native platform coverage.
