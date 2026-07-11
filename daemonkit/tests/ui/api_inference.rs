use daemonkit::{Daemon, DaemonClient, DaemonSpec, Embedded, Managed, Spawn};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let embedded = Daemon::embedded(
        DaemonSpec::new("com.example.embedded")?,
        Spawn::program("embedded-child"),
    )?;
    let _: DaemonClient<Embedded> = embedded.client();

    let managed = Daemon::managed(
        DaemonSpec::new("com.example.managed")?,
        Spawn::program("managed-supervisor"),
    )?;
    let _: DaemonClient<Managed> = managed.client();
    Ok(())
}

