use crate::{ConfigError, DaemonKey, DaemonSpec, Error, Spawn};

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

fn block_on<T>(
    future: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, BlockingError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(BlockingError::InsideRuntime);
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(BlockingError::Runtime)?
        .block_on(future)
        .map_err(BlockingError::Lifecycle)
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
