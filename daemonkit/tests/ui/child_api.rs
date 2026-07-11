use daemonkit::{
    Bootstrap, EmbeddedService, EndpointPublication, Incoming, ManagedChild, Readiness,
    ReadinessResult, ServiceContext, Shutdown, ShutdownStrategy,
};

struct Service;

impl EmbeddedService for Service {
    type Error = std::io::Error;

    async fn run(
        self,
        _context: ServiceContext,
        _incoming: Incoming,
        _shutdown: Shutdown,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

async fn embedded_entry() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(bootstrap) = Bootstrap::detect()? {
        bootstrap.run_embedded(Service).await?;
    }
    Ok(())
}

async fn managed_entry() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(bootstrap) = Bootstrap::detect()? {
        let child = ManagedChild::new("managed-program")
            .arg("--listen")
            .readiness(Readiness::custom(|_context| async move {
                Ok::<_, std::io::Error>(ReadinessResult::ready())
            }))
            .service_endpoint(EndpointPublication::from_readiness())
            .shutdown(ShutdownStrategy::graceful_then_kill());
        bootstrap.run_managed(child).await?;
    }
    Ok(())
}

fn main() {
    let _ = embedded_entry;
    let _ = managed_entry;
}
