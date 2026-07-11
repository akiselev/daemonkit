use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolRange {
    pub minimum: u16,
    pub maximum: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionRole {
    Control,
    Application,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlMessage {
    Hello {
        request_id: u64,
        versions: ProtocolRange,
    },
    Status {
        request_id: u64,
    },
    Drain {
        request_id: u64,
    },
    Shutdown {
        request_id: u64,
    },
    Diagnostics {
        request_id: u64,
    },
    OpenApplicationStream {
        request_id: u64,
    },
}

/// Semantic failure boundaries. Their meaning is a testkit compatibility API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FaultPoint {
    LifecycleLockWait,
    AfterGenerationCreated,
    AfterChildSpawned,
    AfterBootstrapPrepared,
    BeforeManifestPublish,
    AfterManifestPublish,
    BeforeCommit,
    ControlRead,
    ControlWrite,
    LeaseWait,
    BeforeCleanup,
    AfterCleanup,
}
