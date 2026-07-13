use crate::record::{InstanceRecord, RecordValidationError};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

const MAX_RECORD_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct AtomicStore {
    path: PathBuf,
}

impl AtomicStore {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub(crate) fn read(&self) -> Result<Option<Vec<u8>>, StoreError> {
        let file = match open_read(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(StoreError::Io {
                    path: self.path.clone(),
                    source,
                })
            }
        };
        let length = file
            .metadata()
            .map_err(|source| StoreError::Io {
                path: self.path.clone(),
                source,
            })?
            .len();
        if length > MAX_RECORD_BYTES {
            return Err(StoreError::TooLarge {
                path: self.path.clone(),
                bytes: length,
                maximum: MAX_RECORD_BYTES,
            });
        }
        let mut bytes = Vec::with_capacity(length as usize);
        file.take(MAX_RECORD_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| StoreError::Io {
                path: self.path.clone(),
                source,
            })?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(StoreError::TooLarge {
                path: self.path.clone(),
                bytes: bytes.len() as u64,
                maximum: MAX_RECORD_BYTES,
            });
        }
        Ok(Some(bytes))
    }

    pub(crate) fn publish(&self, bytes: &[u8]) -> Result<(), StoreError> {
        self.publish_with_fault(bytes, None)
    }

    pub(crate) fn clear(&self) -> Result<(), StoreError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => {
                if let Some(parent) = self.path.parent() {
                    sync_parent(parent).map_err(|source| StoreError::Io {
                        path: parent.to_owned(),
                        source,
                    })?;
                }
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(StoreError::Io {
                path: self.path.clone(),
                source,
            }),
        }
    }

    pub(crate) fn publish_with_fault(
        &self,
        bytes: &[u8],
        fault: Option<StoreFault>,
    ) -> Result<(), StoreError> {
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(StoreError::TooLarge {
                path: self.path.clone(),
                bytes: bytes.len() as u64,
                maximum: MAX_RECORD_BYTES,
            });
        }
        let parent = self.path.parent().ok_or_else(|| StoreError::InvalidPath {
            path: self.path.clone(),
        })?;
        let temporary = parent.join(format!(
            ".{}.{}.tmp",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("instance"),
            uuid::Uuid::new_v4().simple()
        ));
        let mut file = open_temporary(&temporary).map_err(|source| StoreError::Io {
            path: temporary.clone(),
            source,
        })?;
        inject(fault, StoreFault::AfterTempCreate, &temporary)?;
        file.write_all(bytes).map_err(|source| StoreError::Io {
            path: temporary.clone(),
            source,
        })?;
        inject(fault, StoreFault::AfterWrite, &temporary)?;
        file.sync_all().map_err(|source| StoreError::Io {
            path: temporary.clone(),
            source,
        })?;
        inject(fault, StoreFault::AfterSync, &temporary)?;
        drop(file);
        inject(fault, StoreFault::BeforeRename, &temporary)?;
        replace_atomic(&temporary, &self.path).map_err(|source| StoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        inject(fault, StoreFault::AfterRename, &temporary)?;
        sync_parent(parent).map_err(|source| StoreError::Io {
            path: parent.to_owned(),
            source,
        })?;
        Ok(())
    }

    pub(crate) fn decode_json<T: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<Option<T>, StoreError> {
        self.read()?
            .map(|bytes| {
                serde_json::from_slice(&bytes).map_err(|source| StoreError::Corrupt {
                    path: self.path.clone(),
                    source,
                })
            })
            .transpose()
    }

    pub(crate) fn read_record(&self) -> Result<Option<InstanceRecord>, StoreError> {
        let record = self.decode_json::<InstanceRecord>()?;
        if let Some(record) = &record {
            record
                .validate()
                .map_err(|source| StoreError::InvalidRecord {
                    path: self.path.clone(),
                    source,
                })?;
        }
        Ok(record)
    }

    pub(crate) fn publish_record(&self, record: &InstanceRecord) -> Result<(), StoreError> {
        record
            .validate()
            .map_err(|source| StoreError::InvalidRecord {
                path: self.path.clone(),
                source,
            })?;
        let bytes = serde_json::to_vec(record).map_err(|source| StoreError::Corrupt {
            path: self.path.clone(),
            source,
        })?;
        self.publish(&bytes)
    }
}

fn inject(
    fault: Option<StoreFault>,
    point: StoreFault,
    temporary: &Path,
) -> Result<(), StoreError> {
    if fault == Some(point) {
        Err(StoreError::Injected {
            point,
            temporary: temporary.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn open_temporary(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn open_read(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    options.open(path)
}

#[cfg(unix)]
fn replace_atomic(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::rename(from, to)
}

#[cfg(windows)]
fn replace_atomic(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::{
        Foundation::GetLastError,
        Storage::FileSystem::{
            MoveFileExW, ReplaceFileW, MOVEFILE_WRITE_THROUGH, REPLACEFILE_WRITE_THROUGH,
        },
    };
    let destination_exists = to.exists();
    let from: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    let result = unsafe {
        if destination_exists {
            ReplaceFileW(
                to.as_ptr(),
                from.as_ptr(),
                std::ptr::null(),
                REPLACEFILE_WRITE_THROUGH,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        } else {
            MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH)
        }
    };
    if result == 0 {
        Err(io::Error::from_raw_os_error(
            unsafe { GetLastError() } as i32
        ))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent(parent: &Path) -> io::Result<()> {
    File::open(parent)?.sync_all()
}

#[cfg(windows)]
fn sync_parent(_parent: &Path) -> io::Result<()> {
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreFault {
    AfterTempCreate,
    AfterWrite,
    AfterSync,
    BeforeRename,
    AfterRename,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("invalid atomic-store path {path:?}")]
    InvalidPath { path: PathBuf },
    #[error("store operation failed at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("record {path:?} has {bytes} bytes, above the {maximum}-byte limit")]
    TooLarge {
        path: PathBuf,
        bytes: u64,
        maximum: u64,
    },
    #[error("corrupt record retained at {path:?}: {source}")]
    Corrupt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid instance record retained at {path:?}: {source}")]
    InvalidRecord {
        path: PathBuf,
        #[source]
        source: RecordValidationError,
    },
    #[error("injected crash at {point:?}; staged file retained at {temporary:?}")]
    Injected {
        point: StoreFault,
        temporary: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, process::Command};

    #[test]
    fn publishes_and_atomically_replaces_records() {
        let directory = tempfile::tempdir().unwrap();
        let store = AtomicStore::new(directory.path().join("instance.json"));
        assert_eq!(store.read().unwrap(), None);
        store.publish(br#"{"generation":1}"#).unwrap();
        assert_eq!(store.read().unwrap().unwrap(), br#"{"generation":1}"#);
        store.publish(br#"{"generation":2}"#).unwrap();
        assert_eq!(store.read().unwrap().unwrap(), br#"{"generation":2}"#);
    }

    #[test]
    fn pre_rename_crashes_preserve_the_previous_record() {
        let directory = tempfile::tempdir().unwrap();
        let store = AtomicStore::new(directory.path().join("instance.json"));
        store.publish(b"old").unwrap();
        for fault in [
            StoreFault::AfterTempCreate,
            StoreFault::AfterWrite,
            StoreFault::AfterSync,
            StoreFault::BeforeRename,
        ] {
            assert!(matches!(
                store.publish_with_fault(b"new", Some(fault)),
                Err(StoreError::Injected { point, .. }) if point == fault
            ));
            assert_eq!(store.read().unwrap().unwrap(), b"old");
        }
    }

    #[test]
    fn post_rename_crash_leaves_the_complete_new_record() {
        let directory = tempfile::tempdir().unwrap();
        let store = AtomicStore::new(directory.path().join("instance.json"));
        store.publish(b"old").unwrap();
        assert!(matches!(
            store.publish_with_fault(b"new", Some(StoreFault::AfterRename)),
            Err(StoreError::Injected { .. })
        ));
        assert_eq!(store.read().unwrap().unwrap(), b"new");
    }

    #[test]
    fn corrupt_json_is_retained_and_size_is_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("instance.json");
        let store = AtomicStore::new(&path);
        store.publish(b"{truncated").unwrap();
        assert!(matches!(
            store.decode_json::<serde_json::Value>(),
            Err(StoreError::Corrupt { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), b"{truncated");
        fs::write(&path, vec![0; (MAX_RECORD_BYTES + 1) as usize]).unwrap();
        assert!(matches!(store.read(), Err(StoreError::TooLarge { .. })));
    }

    #[test]
    fn interrupted_publisher_child() {
        let Some(path) = std::env::var_os("DAEMONKIT_STORE_FIXTURE") else {
            return;
        };
        let fault = match std::env::var("DAEMONKIT_STORE_FAULT").unwrap().as_str() {
            "write" => StoreFault::AfterWrite,
            "sync" => StoreFault::AfterSync,
            "rename" => StoreFault::AfterRename,
            other => panic!("unknown store fixture fault {other}"),
        };
        assert!(matches!(
            AtomicStore::new(path).publish_with_fault(b"new-record", Some(fault)),
            Err(StoreError::Injected { .. })
        ));
    }

    #[test]
    fn process_exit_at_publication_boundaries_never_exposes_partial_record() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("instance.json");
        let store = AtomicStore::new(&path);
        store.publish(b"old-record").unwrap();
        for (fault, expected) in [
            ("write", b"old-record".as_slice()),
            ("sync", b"old-record".as_slice()),
            ("rename", b"new-record".as_slice()),
        ] {
            store.publish(b"old-record").unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "native::store::tests::interrupted_publisher_child",
                ])
                .env("DAEMONKIT_STORE_FIXTURE", &path)
                .env("DAEMONKIT_STORE_FAULT", fault)
                .status()
                .unwrap();
            assert!(status.success());
            assert_eq!(store.read().unwrap().unwrap(), expected);
        }
    }
}
