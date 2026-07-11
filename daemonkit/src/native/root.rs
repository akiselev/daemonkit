use crate::RuntimeRoot;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// A runtime root which passed the platform security policy during this access.
/// It is reverified before later authority-bearing operations.
#[derive(Clone, Debug)]
pub(crate) struct VerifiedRoot {
    path: PathBuf,
}

impl VerifiedRoot {
    pub(crate) fn open(explicit: Option<&RuntimeRoot>) -> Result<Self, RootError> {
        let path = match explicit {
            Some(root) => root.as_path().to_owned(),
            None => default_root()?,
        };
        ensure_private_directory(&path)?;
        verify_private_directory(&path)?;
        Ok(Self { path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn reverify(&self) -> Result<(), RootError> {
        verify_private_directory(&self.path)
    }
}

fn default_root() -> Result<PathBuf, RootError> {
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
        let runtime = PathBuf::from(runtime);
        if runtime.is_absolute() {
            return Ok(runtime.join("daemonkit"));
        }
    }
    let home = std::env::var_os("HOME").ok_or(RootError::NoRuntimeLocation)?;
    Ok(PathBuf::from(home).join(".cache").join("daemonkit"))
}

fn ensure_private_directory(path: &Path) -> Result<(), RootError> {
    if path.exists() {
        return Ok(());
    }
    let parent = path.parent().ok_or_else(|| RootError::Unsafe {
        path: path.to_owned(),
        reason: "runtime root has no parent".into(),
    })?;
    fs::create_dir_all(parent).map_err(|source| RootError::Io {
        path: parent.to_owned(),
        source,
    })?;
    create_private(path)
        .or_else(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(|source| RootError::Io {
            path: path.to_owned(),
            source,
        })
}

#[cfg(unix)]
fn create_private(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(path)
}

#[cfg(windows)]
fn create_private(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

#[cfg(unix)]
fn verify_private_directory(path: &Path) -> Result<(), RootError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path).map_err(|source| RootError::Io {
        path: path.to_owned(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RootError::Unsafe {
            path: path.to_owned(),
            reason: "not a real directory".into(),
        });
    }
    let expected_uid = unsafe { libc::geteuid() };
    if metadata.uid() != expected_uid {
        return Err(RootError::Unsafe {
            path: path.to_owned(),
            reason: format!(
                "owner uid {} does not match current uid {expected_uid}",
                metadata.uid()
            ),
        });
    }
    let mode = metadata.mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(RootError::Unsafe {
            path: path.to_owned(),
            reason: format!("mode {mode:o} permits group or other access"),
        });
    }
    Ok(())
}

#[cfg(windows)]
fn verify_private_directory(path: &Path) -> Result<(), RootError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| RootError::Io {
        path: path.to_owned(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RootError::Unsafe {
            path: path.to_owned(),
            reason: "not a real directory".into(),
        });
    }
    // Ordinary metadata cannot prove the DACL is current-logon-only. Stay
    // fail-closed until the windows-sys ACL verifier is implemented.
    Err(RootError::UnsupportedSecurityPolicy {
        path: path.to_owned(),
    })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RootError {
    #[error("no secure per-user runtime location is available")]
    NoRuntimeLocation,
    #[error("unsafe runtime root {path:?}: {reason}")]
    Unsafe { path: PathBuf, reason: String },
    #[error("runtime-root operation failed at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[cfg(windows)]
    #[error("runtime-root ACL verification is not implemented for {path:?}")]
    UnsupportedSecurityPolicy { path: PathBuf },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_and_reverifies_a_private_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = RuntimeRoot::new(parent.path().join("runtime")).unwrap();
        let verified = VerifiedRoot::open(Some(&root)).unwrap();
        assert_eq!(verified.path(), root.as_path());
        verified.reverify().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_unsafe_permissions_without_repairing_them() {
        use std::os::unix::fs::PermissionsExt;
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("runtime");
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        let root = RuntimeRoot::new(&path).unwrap();
        assert!(matches!(
            VerifiedRoot::open(Some(&root)),
            Err(RootError::Unsafe { .. })
        ));
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlink_root() {
        use std::os::unix::fs::symlink;
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = parent.path().join("runtime");
        symlink(target, &link).unwrap();
        let root = RuntimeRoot::new(link).unwrap();
        assert!(matches!(
            VerifiedRoot::open(Some(&root)),
            Err(RootError::Unsafe { .. })
        ));
    }
}
