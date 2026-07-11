`daemonkit` is a local daemon lifecycle runtime crate. It handles all the necessary stuff for stating and connecting to a daemon, handing over a transport that can be used however the user wants.

The crate should own:

* instance identity and keyed state directories;
* startup serialization;
* process spawning and detachment;
* readiness and compatibility negotiation;
* endpoint discovery;
* graceful and forced shutdown;
* logs and startup diagnostics;
* process-tree cleanup;
* transport security;
* crash recovery and stale-state repair.

It should not own the application protocol. It should hand the application an authenticated local byte stream or publish the application’s external endpoint.


Meant to replace the adhoc hacky code in ~/git/debugger-cli and ~/git/ghidra-cli


Make sure to test everything across all platforms. This is a crossplatform implementation.

After finishing work, always update @STATUS.md with what you did so user has a quick place to go to get up to date on latest work. Keep this file short, under two thousand tokens and compact it as needed.