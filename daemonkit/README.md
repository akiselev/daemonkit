# daemonkit

`daemonkit` securely owns the lifecycle of a local daemon without owning the
application protocol. It provides private keyed state, startup serialization,
authenticated local transport, compatibility/readiness negotiation, endpoint
discovery, diagnostics, shutdown, and stale-state repair.

Use `Daemon::embedded` when the daemon is an authenticated byte-stream service
within your executable. Use `Daemon::managed` when a supervisor launches an
external program and publishes its ready application endpoint. In either case,
enter daemon mode only through `Bootstrap::detect()` and a `Bootstrap` runner.

See the repository [README](https://github.com/akiselev/daemonkit#readme) and
[usage guide](https://github.com/akiselev/daemonkit/blob/master/docs/USAGE.md)
for complete examples and security constraints.
