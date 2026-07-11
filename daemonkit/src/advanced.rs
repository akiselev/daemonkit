use crate::{AuthenticatedStream, Error};
use std::{future::Future, pin::Pin};

pub use crate::TransportConfig;

pub type TransportFuture<T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send>>;

/// Object-safe extension seam for application-supplied local transports.
pub trait DynTransport: Send + Sync + 'static {
    fn bind(&self, endpoint: &str) -> TransportFuture<()>;
    fn connect(&self, endpoint: &str) -> TransportFuture<AuthenticatedStream>;
}
