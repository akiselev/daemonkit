use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub(crate) struct StableLock {
    path: PathBuf,
}

impl StableLock {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub(crate) fn acquire(&self, timeout: Duration) -> Result<LockGuard, LockError> {
        self.acquire_until(timeout, || false)
    }

    /// Acquire the lock while periodically observing caller cancellation.
    ///
    /// Cancellation is deliberately a callback rather than a runtime-specific
    /// token: the native kernel can bridge Tokio or blocking cancellation into
    /// this loop without making the authority-bearing lock primitive async.
    pub(crate) fn acquire_until(
        &self,
        timeout: Duration,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<LockGuard, LockError> {
        let started = Instant::now();
        let file = open_stable(&self.path).map_err(|source| LockError::Io {
            path: self.path.clone(),
            source,
        })?;
        loop {
            if cancelled() {
                return Err(LockError::Cancelled {
                    path: self.path.clone(),
                    elapsed: started.elapsed(),
                });
            }
            match file.try_lock_exclusive() {
                Ok(()) => {
                    return Ok(LockGuard {
                        file,
                        path: self.path.clone(),
                    })
                }
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= timeout {
                        return Err(LockError::Timeout {
                            path: self.path.clone(),
                            elapsed: started.elapsed(),
                        });
                    }
                    thread::sleep(Duration::from_millis(2));
                }
                Err(source) => {
                    return Err(LockError::Io {
                        path: self.path.clone(),
                        source,
                    })
                }
            }
        }
    }
}

fn open_stable(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    options.open(path)
}

#[derive(Debug)]
pub(crate) struct LockGuard {
    file: File,
    path: PathBuf,
}

impl LockGuard {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LockError {
    #[error("lock operation failed at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("timed out acquiring stable lock {path:?} after {elapsed:?}")]
    Timeout { path: PathBuf, elapsed: Duration },
    #[error("cancelled while acquiring stable lock {path:?} after {elapsed:?}")]
    Cancelled { path: PathBuf, elapsed: Duration },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };

    #[test]
    fn contention_times_out_and_stable_file_survives_release() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lifecycle.lock");
        let lock = StableLock::new(&path);
        let first = lock.acquire(Duration::from_secs(1)).unwrap();
        assert_eq!(first.path(), path);
        assert!(matches!(
            lock.acquire(Duration::from_millis(10)),
            Err(LockError::Timeout { .. })
        ));
        drop(first);
        let second = lock.acquire(Duration::from_secs(1)).unwrap();
        drop(second);
        assert!(path.is_file());
    }

    #[test]
    fn contention_observes_cancellation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lifecycle.lock");
        let lock = StableLock::new(&path);
        let _first = lock.acquire(Duration::from_secs(1)).unwrap();
        let mut polls = 0;
        let error = lock
            .acquire_until(Duration::from_secs(1), || {
                polls += 1;
                polls > 2
            })
            .unwrap_err();
        assert!(matches!(error, LockError::Cancelled { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_follow_a_lock_symlink() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        File::create(&target).unwrap();
        let path = directory.path().join("lifecycle.lock");
        symlink(target, &path).unwrap();
        let error = StableLock::new(path)
            .acquire(Duration::from_millis(10))
            .unwrap_err();
        assert!(matches!(error, LockError::Io { .. }));
    }

    #[test]
    fn child_lock_holder() {
        let Some(path) = std::env::var_os("DAEMONKIT_LOCK_FIXTURE") else {
            return;
        };
        let _guard = StableLock::new(path)
            .acquire(Duration::from_secs(2))
            .unwrap();
        println!("LOCKED");
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
        thread::sleep(Duration::from_millis(200));
    }

    #[test]
    fn lock_is_exclusive_across_processes_and_released_on_exit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lifecycle.lock");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native::locks::tests::child_lock_holder",
                "--nocapture",
            ])
            .env("DAEMONKIT_LOCK_FIXTURE", &path)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        while output.read_line(&mut line).unwrap() != 0 {
            if line.contains("LOCKED") {
                break;
            }
            line.clear();
        }
        assert!(
            line.contains("LOCKED"),
            "child never reported lock ownership"
        );
        assert!(matches!(
            StableLock::new(&path).acquire(Duration::from_millis(20)),
            Err(LockError::Timeout { .. })
        ));
        assert!(child.wait().unwrap().success());
        StableLock::new(&path)
            .acquire(Duration::from_secs(1))
            .unwrap();
    }
}
