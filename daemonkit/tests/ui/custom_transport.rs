use daemonkit::{
    advanced::{DynTransport, TransportConfig, TransportFuture},
    AuthenticatedStream, DaemonSpec, Error,
};
use std::sync::Arc;

struct Transport;

impl DynTransport for Transport {
    fn bind(&self, _endpoint: &str) -> TransportFuture<()> {
        Box::pin(async { Err(Error::Unsupported) })
    }

    fn connect(&self, _endpoint: &str) -> TransportFuture<AuthenticatedStream> {
        Box::pin(async { Err(Error::Unsupported) })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _spec = DaemonSpec::new("com.example.custom")?
        .transport(TransportConfig::Custom(Arc::new(Transport)));
    Ok(())
}
