use crate::{GenerationId, LifecycleState};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub(crate) const INSTANCE_RECORD_SCHEMA: u16 = 1;

/// Generation-specific control endpoint stored as discovery data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "kebab-case")]
pub(crate) enum ControlEndpointRecord {
    UnixSocket(PathBuf),
    WindowsNamedPipe(String),
}

/// Atomic discovery record. Possession of this value never proves liveness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstanceRecord {
    pub schema: u16,
    pub app_digest: [u8; 32],
    pub key_digest: [u8; 32],
    pub isolation_digest: [u8; 32],
    pub generation: GenerationId,
    pub state: LifecycleState,
    pub process_id: Option<u32>,
    pub process_identity: Option<Vec<u8>>,
    pub control_endpoint: ControlEndpointRecord,
    pub service_endpoint: Option<String>,
    pub protocol_minimum: u16,
    pub protocol_maximum: u16,
    pub build_fingerprint: [u8; 32],
    pub config_fingerprint: [u8; 32],
    pub authentication_token: [u8; 32],
    pub created_unix_millis: u64,
    pub generation_directory: PathBuf,
    pub daemon_log: PathBuf,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
}

impl InstanceRecord {
    pub fn validate(&self) -> Result<(), RecordValidationError> {
        if self.schema != INSTANCE_RECORD_SCHEMA {
            return Err(RecordValidationError::Schema {
                expected: INSTANCE_RECORD_SCHEMA,
                observed: self.schema,
            });
        }
        if self.protocol_minimum > self.protocol_maximum {
            return Err(RecordValidationError::ProtocolRange);
        }
        if matches!(self.state, LifecycleState::Absent) {
            return Err(RecordValidationError::AbsentGeneration);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RecordValidationError {
    #[error("unsupported record schema {observed}; expected {expected}")]
    Schema { expected: u16, observed: u16 },
    #[error("record protocol range is reversed")]
    ProtocolRange,
    #[error("a generation record cannot describe the absent state")]
    AbsentGeneration,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> InstanceRecord {
        InstanceRecord {
            schema: INSTANCE_RECORD_SCHEMA,
            app_digest: [1; 32],
            key_digest: [2; 32],
            isolation_digest: [3; 32],
            generation: GenerationId(4),
            state: LifecycleState::Ready,
            process_id: Some(5),
            process_identity: Some(vec![6]),
            control_endpoint: ControlEndpointRecord::UnixSocket("control.sock".into()),
            service_endpoint: None,
            protocol_minimum: 1,
            protocol_maximum: 1,
            build_fingerprint: [7; 32],
            config_fingerprint: [8; 32],
            authentication_token: [10; 32],
            created_unix_millis: 9,
            generation_directory: "generation".into(),
            daemon_log: "daemon.log".into(),
            stdout_log: "stdout.log".into(),
            stderr_log: "stderr.log".into(),
        }
    }

    #[test]
    fn record_round_trip_preserves_authority_fields() {
        let expected = record();
        let bytes = serde_json::to_vec(&expected).unwrap();
        let observed: InstanceRecord = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(observed, expected);
        observed.validate().unwrap();
    }

    #[test]
    fn unknown_fields_and_invalid_schema_fail_closed() {
        let mut value = serde_json::to_value(record()).unwrap();
        value["unknown_authority"] = serde_json::json!(true);
        assert!(serde_json::from_value::<InstanceRecord>(value).is_err());

        let mut invalid = record();
        invalid.schema += 1;
        assert!(matches!(
            invalid.validate(),
            Err(RecordValidationError::Schema { .. })
        ));
    }
}
