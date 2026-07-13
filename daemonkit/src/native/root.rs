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

#[cfg(unix)]
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

#[cfg(windows)]
fn default_root() -> Result<PathBuf, RootError> {
    let local = std::env::var_os("LOCALAPPDATA").ok_or(RootError::NoRuntimeLocation)?;
    let local = PathBuf::from(local);
    if !local.is_absolute() {
        return Err(RootError::NoRuntimeLocation);
    }
    Ok(local.join("daemonkit").join("runtime"))
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
    windows::create_private_directory(path)
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
    windows::verify_private_directory_acl(path).map_err(|source| RootError::Io {
        path: path.to_owned(),
        source,
    })
}

#[cfg(windows)]
pub(crate) mod windows {
    use super::*;
    use std::{ffi::c_void, mem, os::windows::ffi::OsStrExt, ptr};
    use windows_sys::Win32::{
        Foundation::{CloseHandle, LocalFree, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS, HANDLE},
        Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT},
        Security::{
            AclSizeInformation, AddAccessAllowedAceEx, EqualSid, GetAce, GetAclInformation,
            GetLengthSid, GetSecurityDescriptorControl, GetTokenInformation, InitializeAcl,
            InitializeSecurityDescriptor, SetSecurityDescriptorControl, SetSecurityDescriptorDacl,
            TokenUser, ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, ACL_SIZE_INFORMATION,
            CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, OBJECT_INHERIT_ACE,
            OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
            SECURITY_DESCRIPTOR, SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
        },
        Storage::FileSystem::{CreateDirectoryW, FILE_ALL_ACCESS},
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };

    const ACCESS_ALLOWED_ACE_TYPE: u32 = 0;
    const SECURITY_DESCRIPTOR_REVISION: u32 = 1;

    /// Aligned storage for a TOKEN_USER response. The SID pointer refers into
    /// this allocation, so callers must keep the value alive while using it.
    struct CurrentUserSid {
        _storage: Vec<usize>,
        sid: PSID,
    }

    impl CurrentUserSid {
        fn query() -> io::Result<Self> {
            let mut token: HANDLE = ptr::null_mut();
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let result = (|| {
                let mut bytes = 0u32;
                unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut bytes) };
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) {
                    return Err(error);
                }
                let words = (bytes as usize).div_ceil(mem::size_of::<usize>());
                let mut storage = vec![0usize; words];
                if unsafe {
                    GetTokenInformation(
                        token,
                        TokenUser,
                        storage.as_mut_ptr().cast(),
                        bytes,
                        &mut bytes,
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                let user = unsafe { &*storage.as_ptr().cast::<TOKEN_USER>() };
                Ok(Self {
                    sid: user.User.Sid,
                    _storage: storage,
                })
            })();
            unsafe { CloseHandle(token) };
            result
        }
    }

    pub(super) fn create_private_directory(path: &Path) -> io::Result<()> {
        with_current_user_security(|attributes| {
            let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
            if unsafe { CreateDirectoryW(wide.as_ptr(), attributes) } == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
    }

    /// Build current-logon-only security attributes and keep every referenced
    /// allocation alive for the duration of the native object creation call.
    pub(crate) fn with_current_user_security<T>(
        create: impl FnOnce(*const SECURITY_ATTRIBUTES) -> io::Result<T>,
    ) -> io::Result<T> {
        let user = CurrentUserSid::query()?;
        let sid_bytes = unsafe { GetLengthSid(user.sid) } as usize;
        if sid_bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        let acl_bytes = mem::size_of::<ACL>() + mem::size_of::<ACCESS_ALLOWED_ACE>()
            - mem::size_of::<u32>()
            + sid_bytes;
        let mut acl_storage = vec![0u32; acl_bytes.div_ceil(mem::size_of::<u32>())];
        let acl = acl_storage.as_mut_ptr().cast::<ACL>();
        if unsafe { InitializeAcl(acl, acl_bytes as u32, ACL_REVISION) } == 0
            || unsafe {
                AddAccessAllowedAceEx(
                    acl,
                    ACL_REVISION,
                    CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
                    FILE_ALL_ACCESS,
                    user.sid,
                )
            } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut descriptor: SECURITY_DESCRIPTOR = unsafe { mem::zeroed() };
        let descriptor_ptr = (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast::<c_void>();
        if unsafe { InitializeSecurityDescriptor(descriptor_ptr, SECURITY_DESCRIPTOR_REVISION) }
            == 0
            || unsafe { SetSecurityDescriptorDacl(descriptor_ptr, 1, acl, 0) } == 0
            || unsafe {
                SetSecurityDescriptorControl(descriptor_ptr, SE_DACL_PROTECTED, SE_DACL_PROTECTED)
            } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor_ptr,
            bInheritHandle: 0,
        };
        create(&attributes)
    }

    pub(super) fn verify_private_directory_acl(path: &Path) -> io::Result<()> {
        let user = CurrentUserSid::query()?;
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let mut owner: PSID = ptr::null_mut();
        let mut dacl: *mut ACL = ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        let result = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                ptr::null_mut(),
                &mut dacl,
                ptr::null_mut(),
                &mut descriptor,
            )
        };
        if result != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let checked = (|| {
            if owner.is_null() || unsafe { EqualSid(owner, user.sid) } == 0 {
                return Err(unsafe_acl("directory owner is not the current logon SID"));
            }
            if dacl.is_null() {
                return Err(unsafe_acl("directory has a null or absent DACL"));
            }
            let mut control = 0u16;
            let mut revision = 0u32;
            if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
            {
                return Err(io::Error::last_os_error());
            }
            if control & SE_DACL_PROTECTED == 0 {
                return Err(unsafe_acl("directory DACL inherits authority"));
            }
            let mut info = ACL_SIZE_INFORMATION::default();
            if unsafe {
                GetAclInformation(
                    dacl,
                    (&mut info as *mut ACL_SIZE_INFORMATION).cast(),
                    mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            if info.AceCount == 0 {
                return Err(unsafe_acl("directory DACL grants no current-user access"));
            }
            for index in 0..info.AceCount {
                let mut raw_ace: *mut c_void = ptr::null_mut();
                if unsafe { GetAce(dacl, index, &mut raw_ace) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                let ace = unsafe { &*raw_ace.cast::<ACCESS_ALLOWED_ACE>() };
                if ace.Header.AceType as u32 != ACCESS_ALLOWED_ACE_TYPE {
                    return Err(unsafe_acl("directory DACL contains a non-allow ACE"));
                }
                let sid = (&ace.SidStart as *const u32).cast_mut().cast::<c_void>();
                if unsafe { EqualSid(sid, user.sid) } == 0 {
                    return Err(unsafe_acl("directory DACL grants another identity access"));
                }
            }
            Ok(())
        })();
        unsafe { LocalFree(descriptor) };
        checked
    }

    fn unsafe_acl(reason: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, reason)
    }
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
