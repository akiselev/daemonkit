use super::transport::NativeEndpoint;
use crate::protocol::{Capabilities, ProtocolRange, ServerIdentity};
use serde::{Deserialize, Serialize};

pub(crate) const BOOTSTRAP_MARKER_ENV: &str = "DAEMONKIT_PRIVATE_BOOTSTRAP";
pub(crate) const BOOTSTRAP_NONCE_ENV: &str = "DAEMONKIT_PRIVATE_BOOTSTRAP_NONCE";
pub(crate) const BOOTSTRAP_VERSION: u16 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LaunchConfig {
    pub(crate) version: u16,
    pub(crate) nonce: String,
    pub(crate) app: [u8; 32],
    pub(crate) key: [u8; 32],
    pub(crate) isolation: [u8; 32],
    pub(crate) generation: [u8; 16],
    pub(crate) token: [u8; 32],
    pub(crate) protocol_minimum: u16,
    pub(crate) protocol_maximum: u16,
    pub(crate) process_id: u32,
    pub(crate) process_identity: Vec<u8>,
    pub(crate) build: [u8; 32],
    pub(crate) config: [u8; 32],
    pub(crate) endpoint: NativeEndpoint,
    pub(crate) lease_path: std::path::PathBuf,
    pub(crate) idle_timeout_millis: Option<u64>,
    /// Added after 0.1.0; `default` keeps launches from older parents
    /// decodable (embedded children are the same binary as the parent, so
    /// the mixed-version case only arises for out-of-tree bootstrap hosts).
    #[serde(default)]
    pub(crate) drain_timeout_millis: Option<u64>,
    pub(crate) generation_directory: std::path::PathBuf,
}

impl LaunchConfig {
    pub(crate) fn server_identity(&self, state: u8) -> ServerIdentity {
        ServerIdentity {
            app: self.app,
            key: self.key,
            isolation: self.isolation,
            generation: u128::from_be_bytes(self.generation),
            token: self.token,
            versions: ProtocolRange {
                minimum: self.protocol_minimum,
                maximum: self.protocol_maximum,
            },
            process_id: self.process_id,
            process_identity: self.process_identity.clone(),
            build: self.build,
            config: self.config,
            state,
            capabilities: Capabilities::STATUS
                .union(Capabilities::DRAIN)
                .union(Capabilities::SHUTDOWN)
                .union(Capabilities::DIAGNOSTICS)
                .union(Capabilities::APPLICATION_STREAM),
            service_endpoint: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum BootstrapCommand {
    Launch(Box<LaunchConfig>),
    Commit { version: u16, nonce: String },
}

pub(crate) fn encode(command: &BootstrapCommand) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(command)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<BootstrapCommand, serde_json::Error> {
    serde_json::from_slice(bytes)
}
