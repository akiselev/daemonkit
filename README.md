# daemonkit

`daemonkit` is a secure, cross-platform Rust runtime for owning the lifecycle
of a local daemon. It serializes startup, gives each instance private keyed
state, launches and authenticates the daemon, discovers its endpoint, and
cleans up after graceful shutdowns or crashes.

It deliberately does **not** define your application protocol. For embedded
services it hands your application an authenticated local byte stream; for
supervised services it publishes the application endpoint only after readiness
has been proved.

## What it owns

- Stable instance identity, keyed private state, and cross-process locks.
- Transactional spawn/bootstrap/commit, compatibility negotiation, endpoint
  discovery, startup diagnostics, and stale-state repair.
- Authenticated Unix sockets on Unix and authenticated local named pipes on
  Windows.
- Graceful shutdown with forced process-tree cleanup as a fallback.
- A deterministic companion simulator, `daemonkit-testkit`, for lifecycle
  races, injected faults, replay, and invariant testing.

## Quick start

An embedded daemon is normally the same executable in a separate bootstrap
mode. Construct the lifecycle owner in the parent path and enter the service
only when `Bootstrap::detect()` validates daemonkit's private bootstrap
channel.

```rust,no_run
use daemonkit::{Bootstrap, Daemon, DaemonSpec, Spawn};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(bootstrap) = Bootstrap::detect()? {
        bootstrap
            .run_embedded_fn(|_context, _incoming, mut shutdown| async move {
                shutdown.requested().await;
                Ok::<_, std::io::Error>(())
            })
            .await?;
        return Ok(());
    }

    let daemon = Daemon::embedded(
        DaemonSpec::new("com.example.worker")?,
        Spawn::current_exe()?.arg("__daemon"),
    )?;
    let instance = daemon.ensure().await?;
    println!("ready generation: {:?}", instance.generation());
    Ok(())
}
```

The child service owns application framing and request handling. `daemonkit`
owns lifecycle control and authenticates each stream before handing it over.
For an external program (JVM, Python, language server, and so on), use
`Daemon::managed` and run a `ManagedChild` from the bootstrap path; see the
[usage guide](docs/USAGE.md).

## Crates

- [`daemonkit`](daemonkit): production lifecycle runtime.
- [`daemonkit-testkit`](daemonkit-testkit): deterministic lifecycle simulator,
  scenario DSL, fault injection, replay artifacts, and invariant checks.

## Security model

State is identity-derived rather than chosen from user-controlled path text.
The runtime requires private runtime directories, serializes startup with
stable locks, authenticates control and application connections, and treats
stale or corrupt state as repairable—not authoritative. Process cleanup is
ownership-scoped: a daemonkit instance only removes state and endpoints it can
authenticate as its own.

Read [docs/USAGE.md](docs/USAGE.md) for lifecycle and deployment guidance, and
[docs/RELEASING.md](docs/RELEASING.md) for the release checklist.

## Status

Version `0.1.0` is the first public release candidate. The current support and
verification status lives in [STATUS.md](STATUS.md).
