#![cfg(all(
    feature = "runtime-tokio",
    feature = "native-process",
    feature = "native-transport"
))]

use daemonkit::{
    Bootstrap, Compatibility, ConfigFingerprint, Daemon, DaemonSpec, InstanceStatus, RepairOutcome,
    RuntimeRoot, Spawn,
};
use futures_core::Stream;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn lifecycle_fixture_child() {
    if std::env::var_os("DAEMONKIT_TEST_FAIL_BEFORE_BOOTSTRAP").is_some() {
        panic!("startup-diagnostic-canary");
    }
    if let Ok(delay) = std::env::var("DAEMONKIT_TEST_BOOTSTRAP_DELAY_MS") {
        std::thread::sleep(Duration::from_millis(delay.parse().unwrap()));
    }
    let Some(bootstrap) = Bootstrap::detect().unwrap() else {
        return;
    };
    let hold_stream = std::env::var_os("DAEMONKIT_TEST_HOLD_STREAM").is_some();
    let exit_after = std::env::var("DAEMONKIT_TEST_EXIT_AFTER_MS")
        .ok()
        .map(|value| Duration::from_millis(value.parse().unwrap()));
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(bootstrap.run_embedded_fn(move |_context, mut incoming, mut shutdown| async move {
            let forced_exit = async {
                match exit_after {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending().await,
                }
            };
            tokio::pin!(forced_exit);
            loop {
                tokio::select! {
                    item = std::future::poll_fn(|context| std::pin::Pin::new(&mut incoming).poll_next(context)) => {
                        let Some(Ok(mut stream)) = item else { break; };
                        tokio::spawn(async move {
                            let mut bytes = [0; 4];
                            if stream.read_exact(&mut bytes).await.is_ok() {
                                let _ = stream.write_all(&bytes).await;
                                if hold_stream {
                                    let mut remainder = Vec::new();
                                    let _ = stream.read_to_end(&mut remainder).await;
                                }
                            }
                        });
                    }
                    _ = shutdown.requested() => break,
                    _ = &mut forced_exit => break,
                }
            }
            Ok::<_, std::io::Error>(())
        }))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_child_reports_redacted_command_and_captured_startup_tail() {
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let spawn = fixture_spawn()
        .env("DAEMONKIT_TEST_FAIL_BEFORE_BOOTSTRAP", "1")
        .env("SECRET", "secret-canary")
        .redact_env("SECRET");
    let daemon = Daemon::embedded(
        DaemonSpec::new("com.example.startup-diagnostics")
            .unwrap()
            .runtime_root(root)
            .startup_timeout(Duration::from_millis(300)),
        spawn,
    )
    .unwrap();
    let error = match daemon.ensure().await {
        Ok(_) => panic!("failing child unexpectedly became ready"),
        Err(error) => error,
    };
    let diagnostics = error.startup_diagnostics().unwrap();
    assert!(String::from_utf8_lossy(&diagnostics.stderr_tail).contains("startup-diagnostic-canary"));
    assert!(!diagnostics.command.contains("secret-canary"));
    assert!(diagnostics.command.contains("[REDACTED]"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_shutdown_waits_for_active_stream_then_ensure_repairs_and_restarts() {
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let daemon = Daemon::embedded(
        DaemonSpec::new("com.example.idle")
            .unwrap()
            .runtime_root(root)
            .idle_timeout(Some(Duration::from_millis(100))),
        fixture_spawn().env("DAEMONKIT_TEST_HOLD_STREAM", "1"),
    )
    .unwrap();
    let first = daemon.ensure().await.unwrap();
    let first_generation = first.generation();
    let mut stream = first.connect().await.unwrap();
    stream.write_all(b"echo").await.unwrap();
    let mut echoed = [0; 4];
    stream.read_exact(&mut echoed).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Ready(_)
    ));
    drop(stream);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let successor = daemon.ensure().await.unwrap();
    assert_ne!(successor.generation(), first_generation);
    daemon.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compatibility_restart_and_explicit_restart_replace_generation() {
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let one = Daemon::embedded(
        DaemonSpec::new("com.example.compatibility")
            .unwrap()
            .runtime_root(root.clone())
            .compatibility(Compatibility::current_binary().config(ConfigFingerprint::new([1; 32]))),
        fixture_spawn(),
    )
    .unwrap();
    let first = one.ensure().await.unwrap().generation();
    let two = Daemon::embedded(
        DaemonSpec::new("com.example.compatibility")
            .unwrap()
            .runtime_root(root)
            .compatibility(Compatibility::current_binary().config(ConfigFingerprint::new([2; 32]))),
        fixture_spawn(),
    )
    .unwrap();
    let second = two.ensure().await.unwrap().generation();
    assert_ne!(first, second);
    let third = two.restart().await.unwrap().generation();
    assert_ne!(second, third);
    two.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crashed_committed_child_is_explicitly_repaired() {
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let daemon = Daemon::embedded(
        DaemonSpec::new("com.example.stale")
            .unwrap()
            .runtime_root(root),
        fixture_spawn().env("DAEMONKIT_TEST_EXIT_AFTER_MS", "50"),
    )
    .unwrap();
    daemon.ensure().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(matches!(
        daemon.repair().await.unwrap(),
        RepairOutcome::Repaired { .. }
    ));
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Absent
    ));
}

fn fixture_spawn() -> Spawn {
    Spawn::program(std::env::current_exe().unwrap()).args([
        "--exact",
        "lifecycle_fixture_child",
        "--nocapture",
    ])
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ensure_restart_and_stop_races_remain_serialized_and_recoverable() {
    let state = tempfile::tempdir().unwrap();
    let daemon = Arc::new(
        Daemon::embedded(
            DaemonSpec::new("com.example.operation-races")
                .unwrap()
                .runtime_root(RuntimeRoot::new(state.path().join("runtime")).unwrap()),
            fixture_spawn(),
        )
        .unwrap(),
    );
    daemon.ensure().await.unwrap();
    let mut operations = Vec::new();
    for index in 0..18 {
        let daemon = daemon.clone();
        operations.push(tokio::spawn(async move {
            match index % 3 {
                0 => daemon.ensure().await.map(|_| ()),
                1 => daemon.restart().await.map(|_| ()),
                _ => daemon.stop().await.map(|_| ()),
            }
        }));
    }
    for operation in operations {
        operation.await.unwrap().unwrap();
    }
    let final_instance = daemon.ensure().await.unwrap();
    assert!(matches!(
        final_instance.status().await.unwrap(),
        daemonkit::GenerationStatus::Ready
    ));
    daemon.stop().await.unwrap();
}
