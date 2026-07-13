use sha2::{Digest, Sha256};
use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use uuid::Uuid;

const ID_MAX: usize = 128;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("identifier is empty")]
    EmptyIdentifier,
    #[error("identifier is not a portable daemonkit identifier: {0}")]
    InvalidIdentifier(String),
    #[error("path must be absolute: {0:?}")]
    RelativePath(PathBuf),
    #[error("path identity failed for {path:?}: {message}")]
    PathIdentity { path: PathBuf, message: String },
    #[error("failed to identify the current executable: {0}")]
    CurrentExecutable(String),
    #[error("spawn program cannot be empty")]
    EmptyProgram,
    #[error("spawn environment key is reserved by daemonkit: {0:?}")]
    ReservedEnvironment(OsString),
}

fn validate_id(value: &str, reverse_dns: bool) -> Result<(), ConfigError> {
    let valid_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_');
    if value.is_empty() {
        return Err(ConfigError::EmptyIdentifier);
    }
    if value.len() > ID_MAX
        || value.starts_with('.')
        || value.ends_with('.')
        || value.contains("..")
        || value.chars().any(|c| !valid_char(c))
        || (reverse_dns && !value.contains('.'))
    {
        return Err(ConfigError::InvalidIdentifier(value.to_owned()));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AppId(String);

impl AppId {
    pub(crate) fn digest(&self) -> [u8; 32] {
        authority_digest(b"app", self.0.as_bytes())
    }
}

impl TryFrom<&str> for AppId {
    type Error = ConfigError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_id(value, true)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for AppId {
    type Error = ConfigError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IsolationId(String);

impl IsolationId {
    pub(crate) fn digest(&self) -> [u8; 32] {
        authority_digest(b"isolation", self.0.as_bytes())
    }
}

impl TryFrom<&str> for IsolationId {
    type Error = ConfigError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_id(value, false)?;
        Ok(Self(value.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KeyDigest(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConfigFingerprint(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BuildFingerprint(pub [u8; 32]);

impl ConfigFingerprint {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl BuildFingerprint {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

#[derive(Clone)]
pub struct Compatibility {
    pub(crate) protocol: ProtocolRange,
    pub(crate) binary: BinaryCompatibility,
    pub(crate) config: ConfigFingerprint,
    pub(crate) mismatch: MismatchAction,
}

impl Compatibility {
    pub fn current_binary() -> Self {
        Self {
            protocol: ProtocolRange::new(1, 1).expect("constant range is valid"),
            binary: BinaryCompatibility::Exact(BuildFingerprint([0; 32])),
            config: ConfigFingerprint([0; 32]),
            mismatch: MismatchAction::Restart,
        }
    }

    pub fn protocol(mut self, versions: ProtocolRange) -> Self {
        self.protocol = versions;
        self
    }

    pub fn binary(mut self, policy: BinaryCompatibility) -> Self {
        self.binary = policy;
        self
    }

    pub fn config(mut self, fingerprint: ConfigFingerprint) -> Self {
        self.config = fingerprint;
        self
    }

    pub fn on_mismatch(mut self, action: MismatchAction) -> Self {
        self.mismatch = action;
        self
    }
}

impl std::fmt::Debug for Compatibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compatibility")
            .field("protocol", &self.protocol)
            .field("binary", &self.binary)
            .field("config", &"[REDACTED]")
            .field("mismatch", &self.mismatch)
            .finish()
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub enum BinaryCompatibility {
    Exact(BuildFingerprint),
    SemverCompatible(semver::Version),
    Any,
    Custom(Arc<dyn CompatibilityCheck>),
}

impl std::fmt::Debug for BinaryCompatibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact(_) => f.write_str("Exact([REDACTED])"),
            Self::SemverCompatible(version) => {
                f.debug_tuple("SemverCompatible").field(version).finish()
            }
            Self::Any => f.write_str("Any"),
            Self::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

pub trait CompatibilityCheck: Send + Sync + 'static {
    fn compatible(&self, running: &BuildFingerprint, requested: &BuildFingerprint) -> bool;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MismatchAction {
    Restart,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProtocolRange {
    minimum: u16,
    maximum: u16,
}

impl ProtocolRange {
    pub fn new(minimum: u16, maximum: u16) -> Result<Self, ConfigError> {
        if minimum == 0 || minimum > maximum {
            return Err(ConfigError::InvalidIdentifier(format!(
                "protocol range {minimum}..={maximum}"
            )));
        }
        Ok(Self { minimum, maximum })
    }

    pub(crate) fn minimum(&self) -> u16 {
        self.minimum
    }
    pub(crate) fn maximum(&self) -> u16 {
        self.maximum
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyMetadata {
    pub kind: &'static str,
    pub display: Option<String>,
}

pub trait DaemonKey: Send + Sync + 'static {
    fn digest(&self) -> KeyDigest;
    fn metadata(&self) -> KeyMetadata;
}

fn digest(kind: &str, bytes: &[u8]) -> KeyDigest {
    let mut hash = Sha256::new();
    hash.update(b"daemonkit:key:v1\0");
    hash.update(kind.as_bytes());
    hash.update([0]);
    hash.update(bytes);
    KeyDigest(hash.finalize().into())
}

fn authority_digest(kind: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"daemonkit:authority:v1\0");
    hash.update(kind);
    hash.update([0]);
    hash.update(bytes);
    hash.finalize().into()
}

pub(crate) fn default_isolation_digest() -> [u8; 32] {
    authority_digest(b"isolation", b"default")
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GlobalKey;
impl DaemonKey for GlobalKey {
    fn digest(&self) -> KeyDigest {
        digest("global", &[])
    }
    fn metadata(&self) -> KeyMetadata {
        KeyMetadata {
            kind: "global",
            display: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StringKey(String);
impl StringKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}
impl DaemonKey for StringKey {
    fn digest(&self) -> KeyDigest {
        digest("string", self.0.as_bytes())
    }
    fn metadata(&self) -> KeyMetadata {
        KeyMetadata {
            kind: "string",
            display: Some(self.0.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct UuidKey(Uuid);
impl UuidKey {
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }
}
impl DaemonKey for UuidKey {
    fn digest(&self) -> KeyDigest {
        digest("uuid", self.0.as_bytes())
    }
    fn metadata(&self) -> KeyMetadata {
        KeyMetadata {
            kind: "uuid",
            display: Some(self.0.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PathKey {
    kind: &'static str,
    path: PathBuf,
    stable: Vec<u8>,
}

fn path_bytes(path: &OsStr) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        path.to_string_lossy()
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect()
    }
}

impl PathKey {
    pub fn lexical_absolute(path: impl Into<PathBuf>) -> Result<Self, ConfigError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(ConfigError::RelativePath(path));
        }
        Ok(Self {
            kind: "path-lexical",
            stable: path_bytes(path.as_os_str()),
            path,
        })
    }

    pub fn canonical_existing(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let input = path.as_ref();
        let path = std::fs::canonicalize(input).map_err(|e| ConfigError::PathIdentity {
            path: input.to_owned(),
            message: e.to_string(),
        })?;
        Ok(Self {
            kind: "path-canonical",
            stable: path_bytes(path.as_os_str()),
            path,
        })
    }

    pub fn file_identity(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let input = path.as_ref();
        #[cfg(unix)]
        let stable = {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(input).map_err(|error| ConfigError::PathIdentity {
                path: input.to_owned(),
                message: error.to_string(),
            })?;
            [metadata.dev().to_le_bytes(), metadata.ino().to_le_bytes()].concat()
        };
        #[cfg(windows)]
        let stable = windows_file_identity(input)?;
        Ok(Self {
            kind: "path-file-identity",
            path: input.to_owned(),
            stable,
        })
    }

    pub fn application_uuid(id: Uuid, path: impl Into<PathBuf>) -> Self {
        Self {
            kind: "path-application-uuid",
            path: path.into(),
            stable: id.as_bytes().to_vec(),
        }
    }
}

#[cfg(windows)]
fn windows_file_identity(path: &Path) -> Result<Vec<u8>, ConfigError> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::{
        Foundation::{CloseHandle, INVALID_HANDLE_VALUE},
        Storage::FileSystem::{
            CreateFileW, GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
            FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
            OPEN_EXISTING,
        },
    };
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(ConfigError::PathIdentity {
            path: path.to_owned(),
            message: std::io::Error::last_os_error().to_string(),
        });
    }
    let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let result = unsafe { GetFileInformationByHandle(handle, &mut information) };
    unsafe { CloseHandle(handle) };
    if result == 0 {
        return Err(ConfigError::PathIdentity {
            path: path.to_owned(),
            message: std::io::Error::last_os_error().to_string(),
        });
    }
    let index = ((information.nFileIndexHigh as u64) << 32) | information.nFileIndexLow as u64;
    Ok([
        information.dwVolumeSerialNumber.to_le_bytes().to_vec(),
        index.to_le_bytes().to_vec(),
    ]
    .concat())
}

impl DaemonKey for PathKey {
    fn digest(&self) -> KeyDigest {
        digest(self.kind, &self.stable)
    }
    fn metadata(&self) -> KeyMetadata {
        KeyMetadata {
            kind: self.kind,
            display: Some(self.path.to_string_lossy().into_owned()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DaemonSpec<K = GlobalKey> {
    pub(crate) app: AppId,
    pub(crate) key: K,
    pub(crate) isolation: Option<IsolationId>,
    pub(crate) startup_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) drain_timeout: Duration,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) compatibility: Compatibility,
    pub(crate) logs: LogPolicy,
    pub(crate) transport: TransportConfig,
    pub(crate) runtime_root: Option<RuntimeRoot>,
}

impl DaemonSpec<GlobalKey> {
    pub fn new(app: impl TryInto<AppId, Error = ConfigError>) -> Result<Self, ConfigError> {
        Ok(Self {
            app: app.try_into()?,
            key: GlobalKey,
            isolation: None,
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
            drain_timeout: Duration::from_secs(10),
            idle_timeout: None,
            compatibility: Compatibility::current_binary(),
            logs: LogPolicy::default(),
            transport: TransportConfig::NativeLocal,
            runtime_root: None,
        })
    }
}

impl<K: DaemonKey> DaemonSpec<K> {
    pub fn key<K2: DaemonKey>(self, key: K2) -> DaemonSpec<K2> {
        DaemonSpec {
            app: self.app,
            key,
            isolation: self.isolation,
            startup_timeout: self.startup_timeout,
            shutdown_timeout: self.shutdown_timeout,
            drain_timeout: self.drain_timeout,
            idle_timeout: self.idle_timeout,
            compatibility: self.compatibility,
            logs: self.logs,
            transport: self.transport,
            runtime_root: self.runtime_root,
        }
    }
    pub fn isolation(mut self, value: IsolationId) -> Self {
        self.isolation = Some(value);
        self
    }
    pub fn startup_timeout(mut self, value: Duration) -> Self {
        self.startup_timeout = value;
        self
    }
    pub fn shutdown_timeout(mut self, value: Duration) -> Self {
        self.shutdown_timeout = value;
        self
    }
    pub fn drain_timeout(mut self, value: Duration) -> Self {
        self.drain_timeout = value;
        self
    }
    pub fn idle_timeout(mut self, value: Option<Duration>) -> Self {
        self.idle_timeout = value;
        self
    }
    pub fn compatibility(mut self, value: Compatibility) -> Self {
        self.compatibility = value;
        self
    }
    pub fn logs(mut self, value: LogPolicy) -> Self {
        self.logs = value;
        self
    }
    pub fn transport(mut self, value: TransportConfig) -> Self {
        self.transport = value;
        self
    }
    pub fn runtime_root(mut self, value: RuntimeRoot) -> Self {
        self.runtime_root = Some(value);
        self
    }
    pub fn app(&self) -> &AppId {
        &self.app
    }
    pub fn key_digest(&self) -> KeyDigest {
        self.key.digest()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogPolicy {
    pub tail_bytes: usize,
    pub retained_generations: usize,
    pub retained_bytes: u64,
}

impl Default for LogPolicy {
    fn default() -> Self {
        Self {
            tail_bytes: 64 * 1024,
            retained_generations: 5,
            retained_bytes: 100 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeRoot(PathBuf);

impl RuntimeRoot {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, ConfigError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(ConfigError::RelativePath(path));
        }
        Ok(Self(path))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub enum TransportConfig {
    NativeLocal,
    #[cfg(all(feature = "advanced", feature = "runtime-tokio"))]
    Custom(Arc<dyn crate::advanced::DynTransport>),
}

impl std::fmt::Debug for TransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NativeLocal => f.write_str("NativeLocal"),
            #[cfg(all(feature = "advanced", feature = "runtime-tokio"))]
            Self::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn key_kinds_are_domain_separated() {
        assert_ne!(GlobalKey.digest(), StringKey::new("").digest());
    }
    #[test]
    fn app_ids_reject_traversal_and_separators() {
        for value in ["", "single", "com..bad", "com/bad", "../bad"] {
            assert!(AppId::try_from(value).is_err(), "{value}");
        }
    }
}
