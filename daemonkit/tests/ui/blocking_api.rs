use daemonkit::{blocking, DaemonSpec, Spawn};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let daemon = blocking::Daemon::embedded(
        DaemonSpec::new("com.example.blocking")?,
        Spawn::program("worker"),
    )?;
    let _result = daemon.ensure();
    Ok(())
}

