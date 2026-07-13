use crate::Error;
use std::{future::Future, pin::Pin};

pub use crate::facade::TransportIo;
pub use crate::TransportConfig;

pub type TransportFuture<T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send>>;
pub type BoxedTransportStream = Pin<Box<dyn TransportIo>>;

/// Object-safe accepting half returned by a custom local transport.
pub trait DynListener: Send + 'static {
    fn accept(&mut self) -> TransportFuture<BoxedTransportStream>;
}

/// Object-safe extension seam for application-supplied local transports.
pub trait DynTransport: Send + Sync + 'static {
    fn bind(&self, endpoint: &str) -> TransportFuture<Box<dyn DynListener>>;
    fn connect(&self, endpoint: &str) -> TransportFuture<BoxedTransportStream>;
}
