use crate::{ConfigError, DaemonKey, DaemonSpec, Error, Spawn};

#[cfg(all(feature = "native-process", feature = "native-transport"))]
pub use tail::{EventStream, EventTail, LogStream, LogTail};

pub struct Daemon<M> {
    inner: crate::Daemon<M>,
}

impl Daemon<crate::Embedded> {
    pub fn embedded<K: DaemonKey>(spec: DaemonSpec<K>, spawn: Spawn) -> Result<Self, ConfigError> {
        Ok(Self {
            inner: crate::Daemon::embedded(spec, spawn)?,
        })
    }
}

impl Daemon<crate::Managed> {
    pub fn managed<K: DaemonKey>(spec: DaemonSpec<K>, spawn: Spawn) -> Result<Self, ConfigError> {
        Ok(Self {
            inner: crate::Daemon::managed(spec, spawn)?,
        })
    }
}

impl<M> Daemon<M> {
    pub fn ensure(&self) -> Result<Instance<M>, BlockingError> {
        block_on(self.inner.ensure()).map(|inner| Instance { inner })
    }

    pub fn status(&self) -> Result<crate::InstanceStatus, BlockingError> {
        block_on(self.inner.status())
    }

    pub fn stop(&self) -> Result<crate::StopOutcome, BlockingError> {
        block_on(self.inner.stop())
    }

    pub fn restart(&self) -> Result<Instance<M>, BlockingError> {
        block_on(self.inner.restart()).map(|inner| Instance { inner })
    }

    pub fn repair(&self) -> Result<crate::RepairOutcome, BlockingError> {
        block_on(self.inner.repair())
    }
}

#[cfg(all(feature = "native-process", feature = "native-transport"))]
impl<M> Daemon<M> {
    /// Blocking builder over [`crate::Daemon::events`].
    pub fn events(&self) -> EventTail {
        EventTail {
            inner: self.inner.events(),
        }
    }

    /// Blocking builder over [`crate::Daemon::logs`].
    pub fn logs(&self) -> LogTail {
        LogTail {
            inner: self.inner.logs(),
        }
    }

    pub fn set_log_filter(&self, directives: &str) -> Result<(), BlockingError> {
        block_on(self.inner.set_log_filter(directives))
    }

    pub fn diagnostics(&self) -> Result<Vec<u8>, BlockingError> {
        block_on(self.inner.diagnostics())
    }

    pub fn inspect(&self) -> Result<crate::InstanceReport, BlockingError> {
        block_on(self.inner.inspect())
    }
}

pub struct Instance<M> {
    inner: crate::Instance<M>,
}

impl<M> Instance<M> {
    pub fn generation(&self) -> crate::GenerationId {
        self.inner.generation()
    }

    pub fn stop(self) -> Result<crate::StopOutcome, BlockingError> {
        block_on(self.inner.stop())
    }
}

#[cfg(all(feature = "native-process", feature = "native-transport"))]
impl<M> Instance<M> {
    pub fn events(&self) -> EventTail {
        EventTail {
            inner: self.inner.events(),
        }
    }

    pub fn logs(&self) -> LogTail {
        LogTail {
            inner: self.inner.logs(),
        }
    }

    pub fn inspect(&self) -> Result<crate::InstanceReport, BlockingError> {
        block_on(self.inner.inspect())
    }
}

impl Instance<crate::Embedded> {
    pub fn connect(&self) -> Result<crate::AuthenticatedStream, BlockingError> {
        block_on(self.inner.connect())
    }
}

impl Instance<crate::Managed> {
    pub fn service_endpoint(&self) -> Result<&crate::PublishedEndpoint, Error> {
        self.inner.service_endpoint()
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, BlockingError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(BlockingError::InsideRuntime);
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(BlockingError::Runtime)
}

fn block_on<T>(
    future: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, BlockingError> {
    runtime()?
        .block_on(future)
        .map_err(BlockingError::Lifecycle)
}

#[cfg(all(feature = "native-process", feature = "native-transport"))]
mod tail {
    use super::{runtime, BlockingError};
    use crate::{Error, EventItem, LogItem, LogSource};
    use std::time::Duration;

    /// Blocking counterpart of [`crate::EventTail`].
    #[derive(Debug)]
    pub struct EventTail {
        pub(super) inner: crate::EventTail,
    }

    impl EventTail {
        pub fn replay(mut self, events: u32) -> Self {
            self.inner = self.inner.replay(events);
            self
        }

        pub fn follow(mut self, follow: bool) -> Self {
            self.inner = self.inner.follow(follow);
            self
        }

        pub fn filter(mut self, directives: impl Into<String>) -> Self {
            self.inner = self.inner.filter(directives);
            self
        }

        pub fn reconnect(mut self, reconnect: bool) -> Self {
            self.inner = self.inner.reconnect(reconnect);
            self
        }

        pub fn poll_interval(mut self, interval: Duration) -> Self {
            self.inner = self.inner.poll_interval(interval);
            self
        }

        /// Open the subscription on a private runtime that lives as long as
        /// the returned iterator.
        pub fn open(self) -> Result<EventStream, BlockingError> {
            let runtime = runtime()?;
            let inner = runtime
                .block_on(self.inner.open())
                .map_err(BlockingError::Lifecycle)?;
            Ok(EventStream { runtime, inner })
        }
    }

    /// Iterator over live events; drop it to close the subscription.
    #[derive(Debug)]
    pub struct EventStream {
        runtime: tokio::runtime::Runtime,
        inner: crate::EventStream,
    }

    impl Iterator for EventStream {
        type Item = Result<EventItem, Error>;
        fn next(&mut self) -> Option<Self::Item> {
            self.runtime.block_on(self.inner.next())
        }
    }

    /// Blocking counterpart of [`crate::LogTail`].
    #[derive(Debug)]
    pub struct LogTail {
        pub(super) inner: crate::LogTail,
    }

    impl LogTail {
        pub fn source(mut self, source: LogSource) -> Self {
            self.inner = self.inner.source(source);
            self
        }

        pub fn last_lines(mut self, lines: usize) -> Self {
            self.inner = self.inner.last_lines(lines);
            self
        }

        pub fn follow(mut self, follow: bool) -> Self {
            self.inner = self.inner.follow(follow);
            self
        }

        pub fn poll_interval(mut self, interval: Duration) -> Self {
            self.inner = self.inner.poll_interval(interval);
            self
        }

        pub fn open(self) -> Result<LogStream, BlockingError> {
            let runtime = runtime()?;
            let inner = {
                let _guard = runtime.enter();
                self.inner.open()
            };
            Ok(LogStream { runtime, inner })
        }
    }

    /// Iterator over appended log bytes; drop it to stop polling.
    #[derive(Debug)]
    pub struct LogStream {
        runtime: tokio::runtime::Runtime,
        inner: crate::LogStream,
    }

    impl Iterator for LogStream {
        type Item = Result<LogItem, Error>;
        fn next(&mut self) -> Option<Self::Item> {
            self.runtime.block_on(self.inner.next())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlockingError {
    #[error("blocking daemonkit API cannot run inside a Tokio runtime")]
    InsideRuntime,
    #[error("failed to build blocking runtime: {0}")]
    Runtime(std::io::Error),
    #[error(transparent)]
    Lifecycle(Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_calls_from_inside_a_tokio_runtime() {
        let daemon = Daemon::embedded(
            DaemonSpec::new("com.example.blocking").unwrap(),
            Spawn::program("worker"),
        )
        .unwrap();
        assert!(matches!(daemon.ensure(), Err(BlockingError::InsideRuntime)));
    }
}
