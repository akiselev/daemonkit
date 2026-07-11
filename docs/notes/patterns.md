# Patterns worth adopting

## Buck2: one lifecycle lock and compatibility constraints

Buck2 uses a stable lifecycle file and a cross-platform OS file lock. Cleanup is performed while holding that lock, and the lock file itself is preserved. It also retains the previous daemon directory for diagnostics.

After connecting, Buck2 checks daemon constraints such as version and startup configuration rather than assuming that any responding process is reusable.

This is the correct model for:

one mutator at a time;
restart on version mismatch;
restart on incompatible configuration;
explicit side-by-side isolation;
retaining failed-generation logs.


## sccache: a private bootstrap channel created before spawn

sccache creates a temporary startup listener before launching the daemon. The child reports a typed startup result over that private channel. On Unix it uses a temporary Unix socket; on Windows it uses a random named pipe.

That eliminates ambiguous polling:

the launcher knows exactly which child is reporting;
bind errors can be reported directly;
startup diagnostics are preserved;
endpoint publication happens only after successful initialization;
parallel bootstrap can be distinguished from real startup failure.


## Bazel: key daemon compatibility to workspace and startup configuration

Bazel treats its server as belonging to a particular workspace/output base and verifies that an existing server has appropriate startup options before reuse.

This maps directly to keyed project daemons.