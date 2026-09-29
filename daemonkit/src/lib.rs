//! Secure lifecycle management for local daemons.
//!
//! `daemonkit` owns instance identity, serialization, discovery, authenticated
//! control, startup, and shutdown. It deliberately does not own an application's
//! wire protocol. The native lifecycle implementation is being built in phases;
//! the stable identity and model contracts already live here.

#[cfg(all(feature = "advanced", feature = "runtime-tokio"))]
pub mod advanced;
#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "runtime-tokio")]
mod child;
mod error;
pub mod events;
mod facade;
mod identity;
mod model;
#[allow(dead_code)]
mod native;
#[allow(dead_code)]
mod protocol;
#[allow(dead_code)]
mod record;
mod secret;
#[cfg(all(
    feature = "runtime-tokio",
    feature = "native-process",
    feature = "native-transport"
))]
mod tail;
#[cfg(all(feature = "tracing", feature = "runtime-tokio"))]
pub mod tracing;

#[cfg(feature = "runtime-tokio")]
pub use child::{
    AcceptError, Bootstrap, BootstrapError, ChildLogPolicy, EmbeddedService, EndpointPublication,
    Incoming, ManagedChild, OutputEvents, ProcessExit, ProcessTreePolicy, Readiness,
    ReadinessContext, ReadinessResult, ServiceContext, Shutdown, ShutdownStrategy,
};
pub use error::{Error, InstanceId, RedactedSource, StartupDiagnostics, StartupPhase};
#[cfg(feature = "runtime-tokio")]
pub use events::EventSink;
pub use events::{Event, EventFilter, EventPolicy, EventSource, FilterError, Level};
#[cfg(feature = "runtime-tokio")]
pub use facade::AuthenticatedStream;
pub use facade::{
    Daemon, DaemonClient, DefaultPlatform, GenerationStatus, Instance, InstancePaths,
    InstanceStatus, InstanceSummary, Platform, PublishedEndpoint, RepairAction, RepairOutcome,
    Spawn, StopOutcome,
};
pub use identity::{
    AppId, BinaryCompatibility, BuildFingerprint, Compatibility, CompatibilityCheck, ConfigError,
    ConfigFingerprint, DaemonKey, DaemonSpec, GlobalKey, IsolationId, KeyDigest, KeyMetadata,
    LogPolicy, MismatchAction, PathKey, ProtocolRange, RuntimeRoot, StringKey, TransportConfig,
    UuidKey,
};
pub use model::{DaemonMode, GenerationId, InstanceReport, LifecycleState, TransitionError};
pub use protocol::EndReason;
pub use secret::Secret;
#[cfg(all(
    feature = "runtime-tokio",
    feature = "native-process",
    feature = "native-transport"
))]
pub use tail::{EventItem, EventStream, EventTail, LogItem, LogSource, LogStream, LogTail};

/// Marker selecting an embedded byte-stream service.
#[derive(Debug)]
pub enum Embedded {}

/// Marker selecting a supervised externally managed service.
#[derive(Debug)]
pub enum Managed {}

/// Internal simulator hooks used only by `daemonkit-testkit`.
#[cfg(feature = "testkit")]
#[doc(hidden)]
pub mod __testkit {
    pub use crate::model::{GenerationId, LifecycleState, TransitionError};
    pub use crate::protocol::{ConnectionRole, ControlMessage, FaultPoint, ProtocolRange};

    /// Sealed marker for the deterministic backend bundle.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct SimPlatform;
}
