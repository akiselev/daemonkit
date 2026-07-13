# Releasing daemonkit

This workspace publishes two crates in dependency order:

1. `daemonkit`
2. `daemonkit-testkit`, which depends on the released `daemonkit` version

Before creating a release tag:

1. Set the workspace version in the root `Cargo.toml` and ensure the release
   tag is exactly `v<version>` (for example, `v0.1.0`).
2. Update the root README, both crate READMEs, `STATUS.md`, and any API or
   support-matrix documentation affected by the release.
3. Run the workspace validation gates: formatting, strict Clippy, rustdoc with
   warnings denied, MSRV, all feature configurations, package checks, native
   lifecycle contracts, deterministic testkit tests, and RustSec audit.
4. Confirm `cargo package -p daemonkit --locked` succeeds. After publishing
   `daemonkit`, confirm the packaged `daemonkit-testkit` resolves against the
   registry version before publishing it.
5. Create the tag only after the tag-triggered release workflow is in place.
   That workflow must run validation before publishing either crate, publish
   `daemonkit` before `daemonkit-testkit`, and create the GitHub release only
   after publication succeeds.

Never publish a crate solely because a branch CI run passed: the tagged source
is the release authority. If a publication partially succeeds, do not retag or
overwrite the published version. Diagnose the failure, bump the version, and
issue a new tag.
