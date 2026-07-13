use daemonkit::{
    advanced::{BoxedTransportStream, DynListener, DynTransport, TransportConfig, TransportFuture},
    DaemonSpec, Error,
};
use std::sync::Arc;

struct Transport;
struct Listener;

impl DynListener for Listener {
    fn accept(&mut self) -> TransportFuture<BoxedTransportStream> {
        Box::pin(async { Err(Error::Unsupported) })
    }
}

impl DynTransport for Transport {
    fn bind(&self, _endpoint: &str) -> TransportFuture<Box<dyn DynListener>> {
        Box::pin(async { Err(Error::Unsupported) })
    }

    fn connect(&self, _endpoint: &str) -> TransportFuture<BoxedTransportStream> {
        Box::pin(async { Err(Error::Unsupported) })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _spec = DaemonSpec::new("com.example.custom")?
        .transport(TransportConfig::Custom(Arc::new(Transport)));
    Ok(())
}
