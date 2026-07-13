#![cfg(all(
    feature = "runtime-tokio",
    feature = "native-process",
    feature = "native-transport"
))]

use daemonkit::{Bootstrap, Daemon, DaemonSpec, InstanceStatus, RuntimeRoot, Spawn};
use futures_core::Stream;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn lifecycle_fixture_child() {
    if let Ok(delay) = std::env::var("DAEMONKIT_TEST_BOOTSTRAP_DELAY_MS") {
        std::thread::sleep(Duration::from_millis(delay.parse().unwrap()));
    }
    let Some(bootstrap) = Bootstrap::detect().unwrap() else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(bootstrap.run_embedded_fn(|_context, mut incoming, mut shutdown| async move {
            loop {
                tokio::select! {
                    item = std::future::poll_fn(|context| std::pin::Pin::new(&mut incoming).poll_next(context)) => {
                        let Some(Ok(mut stream)) = item else { break; };
                        tokio::spawn(async move {
                            let mut bytes = [0; 4];
                            if stream.read_exact(&mut bytes).await.is_ok() {
                                let _ = stream.write_all(&bytes).await;
                            }
                        });
                    }
                    _ = shutdown.requested() => break,
                }
            }
            Ok::<_, std::io::Error>(())
        }))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_after_spawn_kills_and_removes_uncommitted_generation() {
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let spec = DaemonSpec::new("com.example.cancelled-start")
        .unwrap()
        .runtime_root(root.clone())
        .startup_timeout(Duration::from_secs(5));
    let delayed = Spawn::program(std::env::current_exe().unwrap())
        .args(["--exact", "lifecycle_fixture_child", "--nocapture"])
        .env("DAEMONKIT_TEST_BOOTSTRAP_DELAY_MS", "1000");
    let daemon = Arc::new(Daemon::embedded(spec, delayed).unwrap());
    let operation = {
        let daemon = daemon.clone();
        tokio::spawn(async move { daemon.ensure().await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    operation.abort();
    assert!(matches!(operation.await, Err(error) if error.is_cancelled()));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Absent
    ));
    assert_eq!(generation_directory_count(root.as_path()), 0);

    let normal = Daemon::embedded(
        DaemonSpec::new("com.example.cancelled-start")
            .unwrap()
            .runtime_root(root),
        Spawn::program(std::env::current_exe().unwrap()).args([
            "--exact",
            "lifecycle_fixture_child",
            "--nocapture",
        ]),
    )
    .unwrap();
    normal.ensure().await.unwrap();
    normal.stop().await.unwrap();
}

fn generation_directory_count(path: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let child = entry.path();
            if child.file_name().is_some_and(|name| name == "generations") {
                std::fs::read_dir(child).map_or(0, |entries| entries.count())
            } else if child.is_dir() {
                generation_directory_count(&child)
            } else {
                0
            }
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_lifecycle_is_transactional_across_real_processes() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let spec = DaemonSpec::new("com.example.native-lifecycle")
        .unwrap()
        .runtime_root(root)
        .startup_timeout(Duration::from_secs(10))
        .shutdown_timeout(Duration::from_secs(10));
    let spawn = Spawn::program(std::env::current_exe().unwrap()).args([
        "--exact",
        "lifecycle_fixture_child",
        "--nocapture",
    ]);
    let daemon = Arc::new(Daemon::embedded(spec, spawn).unwrap());

    let mut callers = Vec::new();
    for _ in 0..50 {
        let daemon = daemon.clone();
        callers.push(tokio::spawn(async move {
            daemon.ensure().await.unwrap().generation()
        }));
    }
    let mut generations = Vec::new();
    for caller in callers {
        generations.push(caller.await.unwrap());
    }
    assert!(generations
        .iter()
        .all(|generation| *generation == generations[0]));
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Ready(_)
    ));

    let instance = daemon.ensure().await.unwrap();
    let mut stream = instance.connect().await.unwrap();
    stream.write_all(b"echo").await.unwrap();
    let mut echoed = [0; 4];
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"echo");

    let stopped = daemon.stop().await.unwrap();
    assert!(matches!(
        stopped,
        daemonkit::StopOutcome::Stopped { forced: false, .. }
    ));
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Absent
    ));
    assert!(matches!(
        daemon.stop().await.unwrap(),
        daemonkit::StopOutcome::AlreadyAbsent
    ));
}
