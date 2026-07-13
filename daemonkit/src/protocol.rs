use serde::{Deserialize, Serialize};
#[cfg(feature = "runtime-tokio")]
use std::{io, time::Duration};
#[cfg(feature = "runtime-tokio")]
use subtle::ConstantTimeEq;
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: [u8; 4] = *b"DK01";
const FRAMING_VERSION: u8 = 1;
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Capabilities(u64);

impl Capabilities {
    pub(crate) const STATUS: Self = Self(1 << 0);
    pub(crate) const DRAIN: Self = Self(1 << 1);
    pub(crate) const SHUTDOWN: Self = Self(1 << 2);
    pub(crate) const DIAGNOSTICS: Self = Self(1 << 3);
    pub(crate) const APPLICATION_STREAM: Self = Self(1 << 4);

    pub(crate) const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub(crate) const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

#[cfg(feature = "runtime-tokio")]
#[derive(Clone)]
pub(crate) struct ClientHello {
    pub(crate) app: [u8; 32],
    pub(crate) key: [u8; 32],
    pub(crate) isolation: [u8; 32],
    pub(crate) generation: u128,
    pub(crate) token: [u8; 32],
    pub(crate) versions: ProtocolRange,
    pub(crate) role: ConnectionRole,
}

#[cfg(feature = "runtime-tokio")]
impl std::fmt::Debug for ClientHello {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientHello")
            .field("app", &self.app)
            .field("key", &self.key)
            .field("isolation", &self.isolation)
            .field("generation", &self.generation)
            .field("token", &"[REDACTED]")
            .field("versions", &self.versions)
            .field("role", &self.role)
            .finish()
    }
}

#[cfg(feature = "runtime-tokio")]
#[derive(Clone)]
pub(crate) struct ServerIdentity {
    pub(crate) app: [u8; 32],
    pub(crate) key: [u8; 32],
    pub(crate) isolation: [u8; 32],
    pub(crate) generation: u128,
    pub(crate) token: [u8; 32],
    pub(crate) versions: ProtocolRange,
    pub(crate) process_id: u32,
    pub(crate) process_identity: Vec<u8>,
    pub(crate) build: [u8; 32],
    pub(crate) config: [u8; 32],
    pub(crate) state: u8,
    pub(crate) capabilities: Capabilities,
}

#[cfg(feature = "runtime-tokio")]
impl std::fmt::Debug for ServerIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerIdentity")
            .field("app", &self.app)
            .field("key", &self.key)
            .field("isolation", &self.isolation)
            .field("generation", &self.generation)
            .field("token", &"[REDACTED]")
            .field("versions", &self.versions)
            .field("process_id", &self.process_id)
            .field("process_identity", &self.process_identity)
            .field("build", &self.build)
            .field("config", &self.config)
            .field("state", &self.state)
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

#[cfg(feature = "runtime-tokio")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Negotiated {
    pub(crate) version: u16,
    pub(crate) generation: u128,
    pub(crate) process_id: u32,
    pub(crate) process_identity: Vec<u8>,
    pub(crate) build: [u8; 32],
    pub(crate) config: [u8; 32],
    pub(crate) state: u8,
    pub(crate) capabilities: Capabilities,
}

#[cfg(feature = "runtime-tokio")]
pub(crate) async fn server_handshake<S>(
    stream: &mut S,
    identity: &ServerIdentity,
    timeout: Duration,
) -> Result<(Negotiated, ConnectionRole), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, server_handshake_inner(stream, identity))
        .await
        .map_err(|_| ProtocolError::Deadline)?
}

#[cfg(feature = "runtime-tokio")]
async fn server_handshake_inner<S>(
    stream: &mut S,
    identity: &ServerIdentity,
) -> Result<(Negotiated, ConnectionRole), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let bytes = read_frame(stream).await?;
    let hello = decode_client_hello(&bytes)?;
    let version = negotiate(hello.versions, identity.versions);
    let authenticated = hello.app == identity.app
        && hello.key == identity.key
        && hello.isolation == identity.isolation
        && hello.generation == identity.generation
        && bool::from(hello.token.ct_eq(&identity.token))
        && version.is_some();
    if !authenticated {
        // All authority mismatches use one response so the endpoint does not
        // become an oracle for which identity or secret field was incorrect.
        write_frame(stream, &[WIRE_REJECT]).await?;
        return Err(ProtocolError::Authentication);
    }
    let negotiated = Negotiated {
        version: version.expect("authenticated version overlap"),
        generation: identity.generation,
        process_id: identity.process_id,
        process_identity: identity.process_identity.clone(),
        build: identity.build,
        config: identity.config,
        state: identity.state,
        capabilities: identity.capabilities,
    };
    write_frame(stream, &encode_server_hello(&negotiated)).await?;
    Ok((negotiated, hello.role))
}

#[cfg(feature = "runtime-tokio")]
pub(crate) async fn client_handshake<S>(
    stream: &mut S,
    hello: &ClientHello,
    timeout: Duration,
) -> Result<Negotiated, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, async {
        write_frame(stream, &encode_client_hello(hello)).await?;
        let response = read_frame(stream).await?;
        if response.as_slice() == [WIRE_REJECT] {
            return Err(ProtocolError::Authentication);
        }
        let negotiated = decode_server_hello(&response)?;
        if negotiated.generation != hello.generation
            || negotiate(
                hello.versions,
                ProtocolRange {
                    minimum: negotiated.version,
                    maximum: negotiated.version,
                },
            )
            .is_none()
        {
            return Err(ProtocolError::Authentication);
        }
        Ok(negotiated)
    })
    .await
    .map_err(|_| ProtocolError::Deadline)?
}

#[cfg(feature = "runtime-tokio")]
pub(crate) async fn read_frame<S>(stream: &mut S) -> Result<Vec<u8>, ProtocolError>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; 9];
    stream.read_exact(&mut header).await?;
    if header[..4] != MAGIC || header[4] != FRAMING_VERSION {
        return Err(ProtocolError::Malformed("invalid magic or framing version"));
    }
    let length = u32::from_be_bytes(header[5..9].try_into().expect("fixed header")) as usize;
    if length == 0 {
        return Err(ProtocolError::Malformed("zero-length frame"));
    }
    if length > MAX_FRAME_BYTES {
        return Err(ProtocolError::Oversized {
            observed: length,
            maximum: MAX_FRAME_BYTES,
        });
    }
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

#[cfg(feature = "runtime-tokio")]
pub(crate) async fn write_frame<S>(stream: &mut S, payload: &[u8]) -> Result<(), ProtocolError>
where
    S: AsyncWrite + Unpin,
{
    if payload.is_empty() {
        return Err(ProtocolError::Malformed("zero-length frame"));
    }
    if payload.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::Oversized {
            observed: payload.len(),
            maximum: MAX_FRAME_BYTES,
        });
    }
    let mut header = [0u8; 9];
    header[..4].copy_from_slice(&MAGIC);
    header[4] = FRAMING_VERSION;
    header[5..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

const WIRE_CLIENT_HELLO: u8 = 1;
const WIRE_SERVER_HELLO: u8 = 2;
const WIRE_REJECT: u8 = 255;
const MAX_PROCESS_IDENTITY: usize = 1024;

#[cfg(feature = "runtime-tokio")]
fn encode_client_hello(hello: &ClientHello) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(151);
    bytes.push(WIRE_CLIENT_HELLO);
    bytes.extend_from_slice(&hello.app);
    bytes.extend_from_slice(&hello.key);
    bytes.extend_from_slice(&hello.isolation);
    bytes.extend_from_slice(&hello.generation.to_be_bytes());
    bytes.extend_from_slice(&hello.token);
    bytes.extend_from_slice(&hello.versions.minimum.to_be_bytes());
    bytes.extend_from_slice(&hello.versions.maximum.to_be_bytes());
    bytes.push(match hello.role {
        ConnectionRole::Control => 0,
        ConnectionRole::Application => 1,
    });
    bytes
}

#[cfg(feature = "runtime-tokio")]
fn decode_client_hello(bytes: &[u8]) -> Result<ClientHello, ProtocolError> {
    let mut decoder = Decoder::new(bytes);
    if decoder.byte()? != WIRE_CLIENT_HELLO {
        return Err(ProtocolError::Malformed("expected client hello"));
    }
    let hello = ClientHello {
        app: decoder.array()?,
        key: decoder.array()?,
        isolation: decoder.array()?,
        generation: u128::from_be_bytes(decoder.array()?),
        token: decoder.array()?,
        versions: ProtocolRange {
            minimum: u16::from_be_bytes(decoder.array()?),
            maximum: u16::from_be_bytes(decoder.array()?),
        },
        role: match decoder.byte()? {
            0 => ConnectionRole::Control,
            1 => ConnectionRole::Application,
            _ => return Err(ProtocolError::Malformed("invalid connection role")),
        },
    };
    decoder.finish()?;
    if hello.versions.minimum == 0 || hello.versions.minimum > hello.versions.maximum {
        return Err(ProtocolError::Malformed("invalid protocol range"));
    }
    Ok(hello)
}

#[cfg(feature = "runtime-tokio")]
fn encode_server_hello(hello: &Negotiated) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(128 + hello.process_identity.len());
    bytes.push(WIRE_SERVER_HELLO);
    bytes.extend_from_slice(&hello.version.to_be_bytes());
    bytes.extend_from_slice(&hello.generation.to_be_bytes());
    bytes.extend_from_slice(&hello.process_id.to_be_bytes());
    bytes.extend_from_slice(&(hello.process_identity.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&hello.process_identity);
    bytes.extend_from_slice(&hello.build);
    bytes.extend_from_slice(&hello.config);
    bytes.push(hello.state);
    bytes.extend_from_slice(&hello.capabilities.0.to_be_bytes());
    bytes
}

#[cfg(feature = "runtime-tokio")]
fn decode_server_hello(bytes: &[u8]) -> Result<Negotiated, ProtocolError> {
    let mut decoder = Decoder::new(bytes);
    if decoder.byte()? != WIRE_SERVER_HELLO {
        return Err(ProtocolError::Malformed("expected server hello"));
    }
    let version = u16::from_be_bytes(decoder.array()?);
    let generation = u128::from_be_bytes(decoder.array()?);
    let process_id = u32::from_be_bytes(decoder.array()?);
    let identity_length = u16::from_be_bytes(decoder.array()?) as usize;
    if identity_length > MAX_PROCESS_IDENTITY {
        return Err(ProtocolError::Malformed("process identity is too large"));
    }
    let process_identity = decoder.bytes(identity_length)?.to_vec();
    let negotiated = Negotiated {
        version,
        generation,
        process_id,
        process_identity,
        build: decoder.array()?,
        config: decoder.array()?,
        state: decoder.byte()?,
        capabilities: Capabilities(u64::from_be_bytes(decoder.array()?)),
    };
    decoder.finish()?;
    if negotiated.version == 0 {
        return Err(ProtocolError::Malformed("invalid negotiated version"));
    }
    Ok(negotiated)
}

fn negotiate(client: ProtocolRange, server: ProtocolRange) -> Option<u16> {
    let minimum = client.minimum.max(server.minimum);
    let maximum = client.maximum.min(server.maximum);
    (minimum <= maximum && minimum != 0).then_some(maximum)
}

#[cfg(feature = "runtime-tokio")]
struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

#[cfg(feature = "runtime-tokio")]
impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ProtocolError::Malformed("field length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ProtocolError::Malformed("truncated message"))?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ProtocolError> {
        self.bytes(N)?
            .try_into()
            .map_err(|_| ProtocolError::Malformed("truncated fixed field"))
    }

    fn byte(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.array::<1>()?[0])
    }

    fn finish(self) -> Result<(), ProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ProtocolError::Malformed("trailing message bytes"))
        }
    }
}

#[cfg(feature = "runtime-tokio")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolError {
    #[error("control I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("malformed control traffic: {0}")]
    Malformed(&'static str),
    #[error("control frame has {observed} bytes, above the {maximum}-byte limit")]
    Oversized { observed: usize, maximum: usize },
    #[error("control authentication failed")]
    Authentication,
    #[error("control handshake deadline elapsed")]
    Deadline,
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

#[cfg(all(test, feature = "runtime-tokio"))]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn identity() -> ServerIdentity {
        ServerIdentity {
            app: [1; 32],
            key: [2; 32],
            isolation: [3; 32],
            generation: 4,
            token: [5; 32],
            versions: ProtocolRange {
                minimum: 2,
                maximum: 4,
            },
            process_id: 6,
            process_identity: vec![7, 8],
            build: [9; 32],
            config: [10; 32],
            state: 3,
            capabilities: Capabilities::STATUS
                .union(Capabilities::SHUTDOWN)
                .union(Capabilities::APPLICATION_STREAM),
        }
    }

    fn hello() -> ClientHello {
        let identity = identity();
        ClientHello {
            app: identity.app,
            key: identity.key,
            isolation: identity.isolation,
            generation: identity.generation,
            token: identity.token,
            versions: ProtocolRange {
                minimum: 1,
                maximum: 3,
            },
            role: ConnectionRole::Application,
        }
    }

    #[tokio::test]
    async fn handshake_negotiates_highest_overlap_then_hands_off_raw_bytes() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let server_task = tokio::spawn(async move {
            let (negotiated, role) =
                server_handshake(&mut server, &identity(), Duration::from_secs(1))
                    .await
                    .unwrap();
            assert_eq!(role, ConnectionRole::Application);
            assert_eq!(negotiated.version, 3);
            let mut application = [0; 4];
            server.read_exact(&mut application).await.unwrap();
            assert_eq!(&application, b"echo");
            server.write_all(&application).await.unwrap();
        });
        let negotiated = client_handshake(&mut client, &hello(), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(negotiated.version, 3);
        assert!(negotiated
            .capabilities
            .contains(Capabilities::APPLICATION_STREAM));
        client.write_all(b"echo").await.unwrap();
        let mut echoed = [0; 4];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"echo");
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn every_authority_mismatch_is_rejected_with_the_same_result() {
        for field in 0..6 {
            let (mut client, mut server) = tokio::io::duplex(1024);
            let mut invalid = hello();
            match field {
                0 => invalid.app[0] ^= 1,
                1 => invalid.key[0] ^= 1,
                2 => invalid.isolation[0] ^= 1,
                3 => invalid.generation += 1,
                4 => invalid.token[0] ^= 1,
                5 => {
                    invalid.versions = ProtocolRange {
                        minimum: 8,
                        maximum: 9,
                    }
                }
                _ => unreachable!(),
            }
            let server_task = tokio::spawn(async move {
                server_handshake(&mut server, &identity(), Duration::from_secs(1)).await
            });
            let client_error = client_handshake(&mut client, &invalid, Duration::from_secs(1))
                .await
                .unwrap_err();
            assert!(matches!(client_error, ProtocolError::Authentication));
            assert!(matches!(
                server_task.await.unwrap(),
                Err(ProtocolError::Authentication)
            ));
        }
    }

    #[tokio::test]
    async fn framing_accepts_one_byte_io_and_rejects_bad_lengths_before_allocation() {
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let task = tokio::spawn(async move { write_frame(&mut writer, b"partial").await });
        assert_eq!(read_frame(&mut reader).await.unwrap(), b"partial");
        task.await.unwrap().unwrap();

        let (mut writer, mut reader) = tokio::io::duplex(32);
        writer.write_all(&MAGIC).await.unwrap();
        writer.write_u8(FRAMING_VERSION).await.unwrap();
        writer
            .write_u32((MAX_FRAME_BYTES as u32) + 1)
            .await
            .unwrap();
        assert!(matches!(
            read_frame(&mut reader).await,
            Err(ProtocolError::Oversized { .. })
        ));
    }

    #[tokio::test]
    async fn handshake_deadline_bounds_slowloris_clients() {
        let (_client, mut server) = tokio::io::duplex(64);
        assert!(matches!(
            server_handshake(&mut server, &identity(), Duration::from_millis(5)).await,
            Err(ProtocolError::Deadline)
        ));
    }

    #[test]
    fn debug_renderers_redact_tokens() {
        let hello = hello();
        let identity = identity();
        assert!(format!("{hello:?}").contains("[REDACTED]"));
        assert!(format!("{identity:?}").contains("[REDACTED]"));
        assert!(!format!("{hello:?}{identity:?}").contains(&format!("{:?}", [5u8; 32])));
    }
}
