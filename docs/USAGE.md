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

## Events, logs, and `tail --follow`

Every daemon publishes structured events through an in-process hub: runtime
notices (ready, drain, shutdown, idle), managed-child stdout and stderr
lines, and whatever the application publishes. Clients tail them over the
authenticated control channel, so a CLI `tail --follow` needs no application
protocol support:

```rust,ignore
let mut events = client.events().replay(200).filter("info,my_crate=debug").open().await?;
while let Some(item) = events.next().await {
    match item? {
        daemonkit::EventItem::Event(event) => println!("{}", event.render()),
        daemonkit::EventItem::Lagged { dropped } => eprintln!("dropped {dropped} events"),
        daemonkit::EventItem::GenerationChanged { .. } => eprintln!("daemon restarted"),
        daemonkit::EventItem::Detached { .. } | daemonkit::EventItem::Ended { .. } => {}
    }
}
```

`Event` is `serde`-serializable, so `--json` output is one line per event.
Replay serves `tail` without follow (`.follow(false)` ends with
`EventItem::Ended`); following survives restarts by default and reports the
new generation. Subscribers never apply backpressure: a slow client gets
`Lagged` markers, and the daemon's log files stay authoritative.

Forward `tracing` events with the `tracing` feature. The application keeps
its own subscriber; add daemonkit's layer next to it and give other layers
per-layer filters so the runtime-reloadable filter can raise verbosity later:

```rust,ignore
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
let bootstrap = daemonkit::Bootstrap::detect()?.expect("launched by daemonkit");
tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_filter(env_filter))
    .with(daemonkit::tracing::layer(&bootstrap.events()))
    .init();
bootstrap.run_embedded(service).await?;
```

`client.set_log_filter("debug,hyper=off")` swaps that layer's directives in
the running daemon; daemons without a reloadable filter (managed supervisors,
or embedded daemons that never installed the layer) answer
`Error::Unsupported` or `Error::Rejected`. `client.diagnostics()` returns the
newest retained events rendered as text, and `client.inspect()` returns a
typed, serializable `InstanceReport` (generation, mode, state, pids, uptime,
active streams, event counters, current log filter) for `status --json`.
Applications can also publish directly through `ServiceContext::events()` or
`Bootstrap::events()`, for example progress notices for agents.

Events are also mirrored to `daemon.log` in the generation directory.
`client.logs()` tails that file, the daemon's own stdout/stderr, or a managed
child's captured output without a live daemon and without protocol support,
following generation changes across restarts:

```rust,ignore
let mut logs = client.logs().source(daemonkit::LogSource::Stderr).last_lines(50).follow(true).open();
while let Some(item) = logs.next().await {
    if let daemonkit::LogItem::Chunk { bytes, .. } = item? {
        std::io::Write::write_all(&mut std::io::stdout(), &bytes)?;
    }
}
```

Use the event stream for structure and filtering; use the file tail when the
daemon is dead, crashed, or older than the events capability. A clean stop or
a crash repair keeps the generation's files within `LogPolicy` retention, and
`logs()` falls back to the newest retained generation when no discovery
record exists, so `tail` after `stop` still explains what happened.

The `blocking` feature mirrors all of this: `blocking::Daemon::events()` and
`logs()` return builders whose `open()` yields plain iterators, plus blocking
`set_log_filter`, `diagnostics`, and `inspect`.

## Failure handling

Treat `Error::Startup` as a diagnostic-rich failure: it includes the startup
phase, redacted command description, bounded output tails, and the generation
log path. Do not retry arbitrary errors by deleting state yourself; use
`repair()` or a fresh `ensure()` so ownership and generation checks remain in
force.

For deterministic tests, pair native process tests with `daemonkit-testkit`.
Its scenarios model lifecycle interleavings, faults, and replayable invariant
violations without weakening the native platform coverage.
