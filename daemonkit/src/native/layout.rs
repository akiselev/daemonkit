use crate::{record::InstanceRecord, GenerationId, KeyDigest, LogPolicy};
use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
    time::SystemTime,
};

use super::{root::VerifiedRoot, store::AtomicStore};

/// Digest-derived, authority-scoped filesystem layout for one daemon instance.
/// Human-readable key metadata never participates in these paths.
#[derive(Clone, Debug)]
pub(crate) struct InstanceLayout {
    directory: PathBuf,
}

impl InstanceLayout {
    pub(crate) fn create(
        root: &VerifiedRoot,
        app_digest: [u8; 32],
        key_digest: KeyDigest,
        isolation_digest: [u8; 32],
    ) -> Result<Self, LayoutError> {
        root.reverify().map_err(LayoutError::Root)?;
        let directory = root
            .path()
            // Full digests remain in the record and are authenticated before
            // use.  The filesystem hierarchy only needs a collision-resistant
            // routing prefix.  Keeping 128 bits per component avoids Windows'
            // legacy Win32 path limit once the atomic store adds its temporary
            // filename, while making an accidental collision infeasible.
            .join(directory_component(&app_digest))
            .join(directory_component(&key_digest.0))
            .join(directory_component(&isolation_digest));
        create_private_tree(root.path(), &directory)?;
        Ok(Self { directory })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.directory
    }

    #[cfg(unix)]
    pub(crate) fn control_endpoint_path(
        &self,
        instance: &crate::InstanceId,
        generation: GenerationId,
    ) -> Result<PathBuf, LayoutError> {
        // A caller-controlled runtime root may be much too deep for macOS's
        // `sockaddr_un`.  Put the socket in a separate, user-private, fixed
        // short directory.  The endpoint is still generation-specific and
        // the handshake checks the complete identity plus its random token,
        // so shortening this path never broadens authority.
        let sockets = short_socket_directory()?;
        Ok(sockets.join(format!(
            "{}-{:016x}.sock",
            hex(&instance.0[..8]),
            generation.0 as u64
        )))
    }

    pub(crate) fn lifecycle_lock(&self) -> super::locks::StableLock {
        super::locks::StableLock::new(self.directory.join("lifecycle.lock"))
    }

    pub(crate) fn lease_lock(&self) -> super::locks::StableLock {
        super::locks::StableLock::new(self.directory.join("lease.lock"))
    }

    pub(crate) fn record_store(&self) -> AtomicStore {
        AtomicStore::new(self.directory.join("instance.json"))
    }

    pub(crate) fn create_generation(
        &self,
        generation: GenerationId,
    ) -> Result<GenerationLayout, LayoutError> {
        let generations = self.directory.join("generations");
        create_private(&generations)?;
        let directory = generations.join(format!("{:032x}", generation.0));
        create_private(&directory)?;
        Ok(GenerationLayout { directory })
    }

    /// Prune oldest inactive generations by both count and aggregate bytes.
    /// The active and most recently failed generations are never candidates.
    /// Newest retained generation directory by modification time, if any.
    /// Used to read logs after the discovery record has been cleared.
    pub(crate) fn newest_generation_directory(&self) -> Option<(GenerationId, PathBuf)> {
        let generations = self.directory.join("generations");
        let entries = fs::read_dir(&generations).ok()?;
        let mut newest: Option<(SystemTime, GenerationId, PathBuf)> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(metadata) = fs::symlink_metadata(&path) else {
                continue;
            };
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                continue;
            }
            let Some(generation) = entry
                .file_name()
                .to_str()
                .and_then(|name| u128::from_str_radix(name, 16).ok())
                .map(GenerationId)
            else {
                continue;
            };
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            if newest
                .as_ref()
                .map_or(true, |(current, ..)| modified >= *current)
            {
                newest = Some((modified, generation, path));
            }
        }
        newest.map(|(_, generation, path)| (generation, path))
    }

    pub(crate) fn prune_generations(
        &self,
        policy: &LogPolicy,
        active: Option<GenerationId>,
        most_recent_failed: Option<GenerationId>,
    ) -> Result<Vec<PathBuf>, LayoutError> {
        let generations = self.directory.join("generations");
        let entries = match fs::read_dir(&generations) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(io_error(&generations, source)),
        };
        let protected: HashSet<String> = [active, most_recent_failed]
            .into_iter()
            .flatten()
            .map(|id| format!("{:032x}", id.0))
            .collect();
        let mut candidates = Vec::new();
        let mut retained_count = 0usize;
        let mut retained_bytes = 0u64;
        for entry in entries {
            let entry = entry.map_err(|source| io_error(&generations, source))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                continue;
            }
            let bytes = directory_bytes(&path)?;
            retained_count = retained_count.saturating_add(1);
            retained_bytes = retained_bytes.saturating_add(bytes);
            let name = entry.file_name().to_string_lossy().into_owned();
            if !protected.contains(&name) {
                let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                candidates.push((modified, path, bytes));
            }
        }
        candidates.sort_by_key(|(modified, _, _)| *modified);
        let mut removed = Vec::new();
        for (_, path, bytes) in candidates {
            if retained_count <= policy.retained_generations
                && retained_bytes <= policy.retained_bytes
            {
                break;
            }
            fs::remove_dir_all(&path).map_err(|source| io_error(&path, source))?;
            retained_count = retained_count.saturating_sub(1);
            retained_bytes = retained_bytes.saturating_sub(bytes);
            removed.push(path);
        }
        Ok(removed)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GenerationLayout {
    directory: PathBuf,
}

impl GenerationLayout {
    pub(crate) fn path(&self) -> &Path {
        &self.directory
    }

    pub(crate) fn record_paths(&self, record: &mut InstanceRecord) {
        record.generation_directory = self.directory.clone();
        record.daemon_log = self.directory.join("daemon.log");
        record.stdout_log = self.directory.join("stdout.log");
        record.stderr_log = self.directory.join("stderr.log");
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    output
}

fn directory_component(digest: &[u8; 32]) -> String {
    hex(&digest[..16])
}

#[cfg(unix)]
fn short_socket_directory() -> Result<PathBuf, LayoutError> {
    use std::os::unix::fs::MetadataExt;

    // `/tmp` itself is intentionally world writable, but this child is
    // created as 0700 and verified on every use.  It is deliberately not
    // derived from `temp_dir()`: macOS commonly points that variable at a
    // long per-user path which cannot host a portable Unix socket pathname.
    let path = PathBuf::from(format!("/tmp/daemonkit-{}", unsafe { libc::geteuid() }));
    create_private(&path)?;
    let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(LayoutError::UnsafeEntry { path });
    }
    Ok(path)
}

fn create_private_tree(root: &Path, target: &Path) -> Result<(), LayoutError> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| LayoutError::EscapedRoot {
            path: target.to_owned(),
        })?;
    let mut current = root.to_owned();
    for component in relative.components() {
        current.push(component);
        create_private(&current)?;
    }
    Ok(())
}

fn create_private(path: &Path) -> Result<(), LayoutError> {
    match private_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path).map_err(|source| io_error(path, source))?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                Ok(())
            } else {
                Err(LayoutError::UnsafeEntry {
                    path: path.to_owned(),
                })
            }
        }
        Err(source) => Err(io_error(path, source)),
    }
}

#[cfg(unix)]
fn private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().mode(0o700).create(path)
}

#[cfg(windows)]
fn private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

fn directory_bytes(path: &Path) -> Result<u64, LayoutError> {
    let mut total = 0u64;
    for entry in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        let entry = entry.map_err(|source| io_error(path, source))?;
        let child = entry.path();
        let metadata = fs::symlink_metadata(&child).map_err(|source| io_error(&child, source))?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        } else if metadata.is_dir() && !metadata.file_type().is_symlink() {
            total = total.saturating_add(directory_bytes(&child)?);
        }
    }
    Ok(total)
}

fn io_error(path: &Path, source: io::Error) -> LayoutError {
    LayoutError::Io {
        path: path.to_owned(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LayoutError {
    #[error(transparent)]
    Root(#[from] super::root::RootError),
    #[error("instance layout escaped verified root: {path:?}")]
    EscapedRoot { path: PathBuf },
    #[error("unsafe non-directory entry in instance layout: {path:?}")]
    UnsafeEntry { path: PathBuf },
    #[error("layout operation failed at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeRoot;
    use std::io::Write;

    fn layout() -> (tempfile::TempDir, InstanceLayout) {
        let parent = tempfile::tempdir().unwrap();
        let root = RuntimeRoot::new(parent.path().join("runtime")).unwrap();
        let root = VerifiedRoot::open(Some(&root)).unwrap();
        let layout = InstanceLayout::create(&root, [1; 32], KeyDigest([2; 32]), [3; 32]).unwrap();
        (parent, layout)
    }

    #[test]
    fn creates_digest_only_layout_and_stable_authority_paths() {
        let (_parent, layout) = layout();
        assert!(layout.path().is_dir());
        assert_eq!(layout.path().file_name().unwrap().len(), 32);
        assert!(layout
            .lifecycle_lock()
            .acquire(std::time::Duration::from_secs(1))
            .is_ok());
        assert!(layout
            .lease_lock()
            .acquire(std::time::Duration::from_secs(1))
            .is_ok());
        assert!(layout.record_store().read().unwrap().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn control_endpoint_remains_portable_for_a_deep_runtime_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = RuntimeRoot::new(
            parent
                .path()
                .join("deep-runtime-root-component-00000001")
                .join("deep-runtime-root-component-00000002")
                .join("deep-runtime-root-component-00000003")
                .join("runtime"),
        )
        .unwrap();
        let root = VerifiedRoot::open(Some(&root)).unwrap();
        let layout = InstanceLayout::create(&root, [1; 32], KeyDigest([2; 32]), [3; 32]).unwrap();
        let endpoint = layout
            .control_endpoint_path(&crate::InstanceId([7; 32]), GenerationId(8))
            .unwrap();
        assert!(endpoint.as_os_str().as_encoded_bytes().len() <= 100);
        assert!(endpoint
            .parent()
            .and_then(|path| path.file_name())
            .is_some_and(|name| name.to_string_lossy().starts_with("daemonkit-")));
    }

    /// Directories need backup semantics to be opened as files on Windows.
    fn open_directory(path: &Path) -> fs::File {
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES,
            };
            // `set_modified` needs write-attribute access, not generic write.
            options
                .access_mode(FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
        }
        options.open(path).unwrap()
    }

    #[test]
    fn newest_generation_directory_ignores_foreign_entries() {
        let (_parent, layout) = layout();
        assert!(layout.newest_generation_directory().is_none());
        for id in [5u128, 9, 2] {
            let generation = layout.create_generation(GenerationId(id)).unwrap();
            fs::write(generation.path().join("daemon.log"), b"x").unwrap();
            let later = SystemTime::now() + std::time::Duration::from_secs(id as u64);
            open_directory(generation.path())
                .set_modified(later)
                .unwrap();
        }
        fs::write(layout.path().join("generations").join("notes"), b"x").unwrap();
        fs::create_dir(layout.path().join("generations").join("not-hex")).unwrap();
        let (generation, path) = layout.newest_generation_directory().unwrap();
        assert_eq!(generation, GenerationId(9));
        assert!(path.ends_with(format!("{:032x}", 9)));
    }

    #[test]
    fn retention_preserves_active_and_failed_generations() {
        let (_parent, layout) = layout();
        for id in 1..=4 {
            let generation = layout.create_generation(GenerationId(id)).unwrap();
            let mut log = fs::File::create(generation.path().join("daemon.log")).unwrap();
            log.write_all(&[id as u8; 10]).unwrap();
        }
        let policy = LogPolicy {
            tail_bytes: 10,
            retained_generations: 2,
            retained_bytes: 20,
        };
        let removed = layout
            .prune_generations(&policy, Some(GenerationId(1)), Some(GenerationId(2)))
            .unwrap();
        assert_eq!(removed.len(), 2);
        assert!(layout
            .create_generation(GenerationId(1))
            .unwrap()
            .path()
            .is_dir());
        assert!(layout
            .create_generation(GenerationId(2))
            .unwrap()
            .path()
            .is_dir());
    }
}
