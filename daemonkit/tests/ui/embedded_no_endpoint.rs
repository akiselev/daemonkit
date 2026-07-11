use daemonkit::{Embedded, Instance};

fn invalid(instance: Instance<Embedded>) {
    let _ = instance.service_endpoint();
}

fn main() {}

