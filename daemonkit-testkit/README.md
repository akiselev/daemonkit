# daemonkit-testkit

`daemonkit-testkit` is the deterministic testing companion for `daemonkit`.
It supplies a lifecycle scenario DSL, controlled scheduling, fault injection,
invariant checks, and replay artifacts for testing daemon lifecycle races
without relying solely on timing-sensitive process tests.

It is useful alongside native integration tests: use the simulator to explore
interleavings and reproduce failures, then validate platform behavior through
`daemonkit`'s native lifecycle contracts.

See the repository [testing plan](https://github.com/akiselev/daemonkit/blob/master/docs/plan/testkit.md)
and the crate-level documentation for the public scenario API.
