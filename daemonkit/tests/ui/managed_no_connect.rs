use daemonkit::{Instance, Managed};

async fn invalid(instance: Instance<Managed>) {
    let _ = instance.connect().await;
}

fn main() {}

