use daemonkit::{DaemonClient, Embedded};

async fn invalid(client: DaemonClient<Embedded>) {
    let _ = client.ensure().await;
}

fn main() {}

