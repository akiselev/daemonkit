# AutoPCB Routing Integration Boundary

Date: 2026-08-19

Daemonkit should support AutoPCB as the lifecycle layer for long-running routing campaigns, external solver workers, and reproducible experiment services.

## Intended AutoPCB uses

- persistent native DRC oracle workers;
- routing benchmark campaign daemons;
- GPU/model candidate scorer workers;
- resumable dataset generation;
- long-running failure minimization jobs;
- local route-cache/index services.

## Boundary rule

Daemonkit owns process lifecycle and authenticated local IPC.  AutoPCB owns the application protocol, route transactions, verification, and artifacts.

```text
AutoPCB campaign CLI
    -> daemonkit-managed worker
    -> authenticated local stream
    -> AutoPCB protocol frames
    -> artifacted run evidence
```

## Requirements from AutoPCB

For every daemon-backed run, AutoPCB needs to record:

- daemon instance identity;
- generation number;
- worker executable digest or build ID;
- startup diagnostics;
- protocol version;
- crash/restart history;
- deterministic seed and input snapshot hash;
- output artifact IDs.

## First implementation slice

Once AutoPCB's route-session runner exists, add a daemonkit-backed campaign worker that can:

1. accept one immutable route task;
2. run a deterministic route session;
3. emit event JSONL and certificates;
4. write a final status artifact;
5. exit cleanly or preserve crash evidence.

## Non-goals

Daemonkit should not know PCB geometry, routing rules, DRC semantics, or score interpretation.  It is lifecycle infrastructure only.
