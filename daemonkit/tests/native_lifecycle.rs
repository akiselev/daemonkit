#![cfg(all(
    feature = "runtime-tokio",
    feature = "native-process",
    feature = "native-transport"
))]

use daemonkit::{
    Bootstrap, Compatibility, ConfigFingerprint, Daemon, DaemonSpec, EndReason,
    EndpointPublication, EventItem, EventSource, InstanceStatus, Level, LogItem, LogSource,
    ManagedChild, PublishedEndpoint, Readiness, ReadinessResult, RepairOutcome, RuntimeRoot,
    ShutdownStrategy, Spawn,
};
use futures_core::Stream;
use std::{io::Write as _, net::SocketAddr, sync::Arc, time::Duration};
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
    if std::env::var_os("DAEMONKIT_TEST_MANAGED").is_some() {
        let endpoint: SocketAddr = std::env::var("DAEMONKIT_TEST_MANAGED_ENDPOINT")
            .unwrap()
            .parse()
            .unwrap();
        let executable = std::env::current_exe().unwrap();
        let omit_endpoint = std::env::var_os("DAEMONKIT_TEST_OMIT_ENDPOINT").is_some();
        let mut child = ManagedChild::new(executable)
            .args(["--exact", "managed_program_fixture", "--nocapture"])
            .env("DAEMONKIT_TEST_MANAGED_PROGRAM", "1")
            .env("DAEMONKIT_TEST_MANAGED_ENDPOINT", endpoint.to_string())
            .readiness(Readiness::custom(move |mut context| async move {
                let mut observed = Vec::new();
                while let Some(chunk) = std::future::poll_fn(|poll| {
                    std::pin::Pin::new(context.stdout()).poll_next(poll)
                })
                .await
                {
                    observed.extend_from_slice(&chunk);
                    if String::from_utf8_lossy(&observed).contains(&format!("READY {endpoint}")) {
                        let readiness = if omit_endpoint {
                            ReadinessResult::ready()
                        } else {
                            ReadinessResult::ready().service_endpoint(endpoint)
                        };
                        return Ok::<_, std::io::Error>(readiness);
                    }
                }
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "managed stdout closed before readiness",
                ))
            }))
            .service_endpoint(EndpointPublication::from_readiness())
            .shutdown(ShutdownStrategy::with_timeouts(
                Duration::from_millis(200),
                Duration::from_secs(5),
            ));
        if let Some(path) = std::env::var_os("DAEMONKIT_TEST_GRANDCHILD_PID") {
            child = child.env("DAEMONKIT_TEST_GRANDCHILD_PID", path);
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(bootstrap.run_managed(child))
            .unwrap();
        return;
    }
    let hold_stream = std::env::var_os("DAEMONKIT_TEST_HOLD_STREAM").is_some();
    let exit_after = std::env::var("DAEMONKIT_TEST_EXIT_AFTER_MS")
        .ok()
        .map(|value| Duration::from_millis(value.parse().unwrap()));
    let ticking = std::env::var_os("DAEMONKIT_TEST_TRACING").is_some();
    if ticking {
        use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
        let registry = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr),
        );
        #[cfg(feature = "tracing")]
        let registry = registry.with(daemonkit::tracing::layer(&bootstrap.events()));
        registry.init();
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(bootstrap.run_embedded_fn(move |context, mut incoming, mut shutdown| async move {
            if ticking {
                let events = context.events().clone();
                tokio::spawn(async move {
                    let span = tracing::info_span!("ticker", generation = %context.generation().0);
                    let _entered = span.enter();
                    let mut tick: u64 = 0;
                    loop {
                        tick += 1;
                        tracing::info!(tick, "tick");
                        tracing::debug!(tick, "debug tick");
                        events.publish(
                            daemonkit::Event::new(EventSource::Application, Level::Info, "direct", "direct publish")
                                .field("tick", tick.to_string()),
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                });
            }
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

#[test]
#[allow(clippy::zombie_processes)] // The supervisor deliberately kills this fixture's whole tree.
fn managed_program_fixture() {
    if std::env::var_os("DAEMONKIT_TEST_MANAGED_PROGRAM").is_none() {
        return;
    }
    let endpoint: SocketAddr = std::env::var("DAEMONKIT_TEST_MANAGED_ENDPOINT")
        .unwrap()
        .parse()
        .unwrap();
    let _listener = std::net::TcpListener::bind(endpoint).unwrap();
    let _grandchild = std::env::var_os("DAEMONKIT_TEST_GRANDCHILD_PID").map(|path| {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "managed_grandchild_fixture", "--nocapture"])
            .env("DAEMONKIT_TEST_GRANDCHILD_PID", path)
            .spawn()
            .unwrap()
    });
    let noise = vec![b'x'; 2 * 1024 * 1024];
    std::io::stdout().write_all(&noise).unwrap();
    std::io::stderr().write_all(&noise).unwrap();
    println!("\nREADY {endpoint}");
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn managed_grandchild_fixture() {
    let Some(path) = std::env::var_os("DAEMONKIT_TEST_GRANDCHILD_PID") else {
        return;
    };
    std::fs::write(path, std::process::id().to_string()).unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
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
            // Generous enough that a loaded CI runner can connect before the
            // idle timer fires; the sleeps below scale with it.
            .idle_timeout(Some(Duration::from_millis(400))),
        fixture_spawn().env("DAEMONKIT_TEST_HOLD_STREAM", "1"),
    )
    .unwrap();
    let first = daemon.ensure().await.unwrap();
    let first_generation = first.generation();
    let mut stream = first.connect().await.unwrap();
    stream.write_all(b"echo").await.unwrap();
    let mut echoed = [0; 4];
    stream.read_exact(&mut echoed).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Ready(_)
    ));
    drop(stream);
    tokio::time::sleep(Duration::from_millis(1000)).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_supervisor_publishes_readiness_drains_output_and_kills_tree() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = reservation.local_addr().unwrap();
    drop(reservation);
    let grandchild_pid = state.path().join("grandchild.pid");
    let daemon = Daemon::managed(
        DaemonSpec::new("com.example.managed-tree")
            .unwrap()
            .runtime_root(root.clone())
            .startup_timeout(Duration::from_secs(10))
            .shutdown_timeout(Duration::from_secs(10)),
        fixture_spawn()
            .env("DAEMONKIT_TEST_MANAGED", "1")
            .env("DAEMONKIT_TEST_MANAGED_ENDPOINT", endpoint.to_string())
            .env("DAEMONKIT_TEST_GRANDCHILD_PID", &grandchild_pid),
    )
    .unwrap();

    let instance = daemon.ensure().await.unwrap();
    assert_eq!(
        instance.service_endpoint().unwrap(),
        &PublishedEndpoint::Tcp(endpoint)
    );
    tokio::net::TcpStream::connect(endpoint).await.unwrap();
    let descendant = wait_for_pid(&grandchild_pid).await;
    let stdout_log = find_file(root.as_path(), "managed-stdout.log").unwrap();
    let stderr_log = find_file(root.as_path(), "managed-stderr.log").unwrap();
    assert!(std::fs::metadata(stdout_log).unwrap().len() >= 2 * 1024 * 1024);
    assert!(std::fs::metadata(stderr_log).unwrap().len() >= 2 * 1024 * 1024);

    daemon.stop().await.unwrap();
    wait_until_process_gone(descendant).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_supervisor_crash_still_kills_owned_descendants() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = reservation.local_addr().unwrap();
    drop(reservation);
    let grandchild_pid = state.path().join("crash-grandchild.pid");
    let daemon = Daemon::managed(
        DaemonSpec::new("com.example.managed-supervisor-crash")
            .unwrap()
            .runtime_root(root.clone())
            .startup_timeout(Duration::from_secs(10)),
        fixture_spawn()
            .env("DAEMONKIT_TEST_MANAGED", "1")
            .env("DAEMONKIT_TEST_MANAGED_ENDPOINT", endpoint.to_string())
            .env("DAEMONKIT_TEST_GRANDCHILD_PID", &grandchild_pid),
    )
    .unwrap();
    daemon.ensure().await.unwrap();
    let descendant = wait_for_pid(&grandchild_pid).await;
    let supervisor = instance_process_id(root.as_path());

    terminate_process(supervisor);
    wait_until_process_gone(descendant).await;
    assert!(matches!(
        daemon.repair().await.unwrap(),
        RepairOutcome::Repaired { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_readiness_endpoint_fails_closed_and_kills_managed_tree() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = reservation.local_addr().unwrap();
    drop(reservation);
    let grandchild_pid = state.path().join("failed-readiness-grandchild.pid");
    let daemon = Daemon::managed(
        DaemonSpec::new("com.example.managed-missing-endpoint")
            .unwrap()
            .runtime_root(root.clone())
            .startup_timeout(Duration::from_secs(3)),
        fixture_spawn()
            .env("DAEMONKIT_TEST_MANAGED", "1")
            .env("DAEMONKIT_TEST_MANAGED_ENDPOINT", endpoint.to_string())
            .env("DAEMONKIT_TEST_GRANDCHILD_PID", &grandchild_pid)
            .env("DAEMONKIT_TEST_OMIT_ENDPOINT", "1"),
    )
    .unwrap();

    let error = match daemon.ensure().await {
        Ok(_) => panic!("missing readiness endpoint unexpectedly published"),
        Err(error) => error,
    };
    assert!(error.startup_diagnostics().is_some());
    let descendant = wait_for_pid(&grandchild_pid).await;
    wait_until_process_gone(descendant).await;
    wait_for_generation_cleanup(root.as_path()).await;
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Absent
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_timeout_kills_child_and_rolls_back_generation() {
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let daemon = Daemon::embedded(
        DaemonSpec::new("com.example.bootstrap-timeout")
            .unwrap()
            .runtime_root(root.clone())
            .startup_timeout(Duration::from_millis(150)),
        fixture_spawn().env("DAEMONKIT_TEST_BOOTSTRAP_DELAY_MS", "2000"),
    )
    .unwrap();

    let error = match daemon.ensure().await {
        Ok(_) => panic!("delayed bootstrap unexpectedly became ready"),
        Err(error) => error,
    };
    let diagnostics = error.startup_diagnostics().unwrap();
    assert!(diagnostics.process_id.is_some());
    wait_for_generation_cleanup(root.as_path()).await;
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Absent
    ));
}

async fn wait_for_pid(path: &std::path::Path) -> u32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(path) {
                break value.parse().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

async fn wait_until_process_gone(process_id: u32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_exists(process_id) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_generation_cleanup(root: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while generation_directory_count(root) != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[cfg(unix)]
fn process_exists(process_id: u32) -> bool {
    let result = unsafe { libc::kill(process_id as i32, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
fn process_exists(process_id: u32) -> bool {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, STILL_ACTIVE},
        System::Threading::{GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION},
    };
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
    if handle.is_null() {
        return false;
    }
    let mut code = 0;
    let active =
        unsafe { GetExitCodeProcess(handle, &mut code) } != 0 && code == STILL_ACTIVE as u32;
    unsafe { CloseHandle(handle) };
    active
}

fn find_file(root: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(root).ok()? {
        let path = entry.ok()?.path();
        if path.file_name().is_some_and(|value| value == name) {
            return Some(path);
        }
        if path.is_dir() {
            if let Some(found) = find_file(&path, name) {
                return Some(found);
            }
        }
    }
    None
}

fn instance_process_id(root: &std::path::Path) -> u32 {
    let record = find_file(root, "instance.json").unwrap();
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(record).unwrap()).unwrap();
    value["process_id"].as_u64().unwrap() as u32
}

#[cfg(unix)]
fn terminate_process(process_id: u32) {
    assert_eq!(unsafe { libc::kill(process_id as i32, libc::SIGKILL) }, 0);
}

#[cfg(windows)]
fn terminate_process(process_id: u32) {
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::Threading::{OpenProcess, TerminateProcess, PROCESS_TERMINATE},
    };
    let handle = unsafe { OpenProcess(PROCESS_TERMINATE, 0, process_id) };
    assert!(!handle.is_null());
    assert_ne!(unsafe { TerminateProcess(handle, 1) }, 0);
    unsafe { CloseHandle(handle) };
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

/// Read items until `predicate` matches, failing after `timeout`.
async fn wait_for_event(
    stream: &mut daemonkit::EventStream,
    timeout: Duration,
    mut predicate: impl FnMut(&EventItem) -> bool,
) -> EventItem {
    tokio::time::timeout(timeout, async {
        loop {
            let item = stream.next().await.expect("event stream ended").unwrap();
            if predicate(&item) {
                return item;
            }
        }
    })
    .await
    .expect("timed out waiting for event")
}

fn message_is(item: &EventItem, message: &str) -> bool {
    matches!(item, EventItem::Event(event) if event.message == message)
}

#[cfg(feature = "tracing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tracing_events_replay_follow_filter_and_reload_across_restart() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let daemon = Daemon::embedded(
        DaemonSpec::new("com.example.events")
            .unwrap()
            .runtime_root(root.clone())
            .startup_timeout(Duration::from_secs(10))
            .shutdown_timeout(Duration::from_secs(10)),
        fixture_spawn().env("DAEMONKIT_TEST_TRACING", "1"),
    )
    .unwrap();
    let first = daemon.ensure().await.unwrap();

    // Replay delivers the runtime readiness notice published before any
    // client attached; live events then follow, filtered at info.
    let mut events = daemon
        .events()
        .replay(64)
        .filter("info")
        .open()
        .await
        .unwrap();
    let ready = wait_for_event(&mut events, Duration::from_secs(5), |item| {
        message_is(item, "embedded daemon is ready")
    })
    .await;
    let EventItem::Event(ready) = ready else {
        unreachable!()
    };
    assert_eq!(ready.source, EventSource::Runtime);
    assert_eq!(ready.level, Level::Info);
    assert_eq!(
        ready.fields,
        vec![(
            "generation".to_string(),
            format!("{:032x}", first.generation().0)
        )]
    );
    let tick = wait_for_event(&mut events, Duration::from_secs(5), |item| {
        message_is(item, "tick")
    })
    .await;
    let EventItem::Event(tick) = tick else {
        unreachable!()
    };
    assert_eq!(tick.source, EventSource::Application);
    assert_eq!(tick.spans, vec!["ticker".to_string()]);
    assert!(tick.fields.iter().any(|(key, _)| key == "tick"));
    assert!(tick.target.contains("native_lifecycle"));
    let direct = wait_for_event(&mut events, Duration::from_secs(5), |item| {
        message_is(item, "direct publish")
    })
    .await;
    assert!(matches!(direct, EventItem::Event(event) if event.target == "direct"));

    // The daemon's own layer filter starts at info, so no debug event exists
    // to stream; raising it at runtime makes debug events flow.
    let mut seen_debug = false;
    for _ in 0..20 {
        if let Some(Ok(EventItem::Event(event))) = events.next().await {
            seen_debug |= event.level == Level::Debug;
        }
    }
    assert!(!seen_debug, "debug events leaked through the info filter");
    daemon.set_log_filter("debug").await.unwrap();
    wait_for_event(&mut events, Duration::from_secs(5), |item| {
        message_is(item, "log filter changed")
    })
    .await;
    // This subscription itself still filters at info; a fresh one at debug
    // sees the newly enabled events.
    let mut verbose = daemon.events().filter("debug").open().await.unwrap();
    wait_for_event(&mut verbose, Duration::from_secs(5), |item| {
        message_is(item, "debug tick")
    })
    .await;
    drop(verbose);
    let rejected = daemon.set_log_filter("not-a-level").await.unwrap_err();
    assert!(
        matches!(rejected, daemonkit::Error::Rejected { .. }),
        "{rejected:?}"
    );

    let diagnostics = String::from_utf8(daemon.diagnostics().await.unwrap()).unwrap();
    assert!(diagnostics.contains("embedded daemon is ready"));
    assert!(diagnostics.contains("log filter changed"));

    let report = first.inspect().await.unwrap();
    assert_eq!(report.generation, first.generation());
    assert_eq!(report.mode, daemonkit::DaemonMode::Embedded);
    assert_eq!(report.state, daemonkit::LifecycleState::Ready);
    assert_eq!(report.process_id, first.summary().process_id.unwrap());
    assert_eq!(report.managed_process_id, None);
    assert_eq!(report.active_streams, 0);
    assert!(report.events_published > 0);
    assert_eq!(report.event_subscribers, 1, "{report:?}");
    assert_eq!(report.log_filter.as_deref(), Some("debug"));
    let json = serde_json::to_string(&report).unwrap();
    assert!(json.contains("\"mode\":\"embedded\""), "{json}");

    // Restart: the follow subscription detaches on shutdown and reattaches
    // to the replacement generation with its early replay.
    let second = daemon.restart().await.unwrap();
    assert_ne!(first.generation(), second.generation());
    let detached = wait_for_event(&mut events, Duration::from_secs(10), |item| {
        matches!(item, EventItem::Detached { .. })
    })
    .await;
    assert_eq!(
        detached,
        EventItem::Detached {
            reason: EndReason::Shutdown
        }
    );
    let changed = wait_for_event(&mut events, Duration::from_secs(10), |item| {
        matches!(item, EventItem::GenerationChanged { .. })
    })
    .await;
    assert_eq!(
        changed,
        EventItem::GenerationChanged {
            from: first.generation(),
            to: second.generation()
        }
    );
    wait_for_event(&mut events, Duration::from_secs(5), |item| {
        message_is(item, "embedded daemon is ready")
    })
    .await;
    drop(events);

    // Replay without follow ends explicitly.
    let mut replay = daemon
        .events()
        .replay(8)
        .follow(false)
        .open()
        .await
        .unwrap();
    let mut count = 0;
    loop {
        match replay.next().await.unwrap().unwrap() {
            EventItem::Event(_) => count += 1,
            EventItem::Ended { reason } => {
                assert_eq!(reason, EndReason::Replayed);
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(count > 0 && count <= 8);
    assert!(replay.next().await.is_none());

    // A stopped daemon ends a non-reconnecting subscription.
    let mut ending = daemon.events().reconnect(false).open().await.unwrap();
    daemon.stop().await.unwrap();
    let ended = wait_for_event(&mut ending, Duration::from_secs(10), |item| {
        matches!(item, EventItem::Ended { .. })
    })
    .await;
    assert_eq!(
        ended,
        EventItem::Ended {
            reason: EndReason::Shutdown
        }
    );
    assert!(ending.next().await.is_none());
    assert!(matches!(
        daemon.events().open().await,
        Err(daemonkit::Error::Unavailable { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_tail_follows_daemon_log_across_restart() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let daemon = Daemon::embedded(
        DaemonSpec::new("com.example.file-tail")
            .unwrap()
            .runtime_root(root.clone())
            .startup_timeout(Duration::from_secs(10))
            .shutdown_timeout(Duration::from_secs(10))
            .logs(daemonkit::LogPolicy {
                retained_generations: 1,
                ..daemonkit::LogPolicy::default()
            }),
        fixture_spawn().env("DAEMONKIT_TEST_TRACING", "1"),
    )
    .unwrap();
    let first = daemon.ensure().await.unwrap();

    // Without a live subscription the rendered daemon log is the record. The
    // child writes its readiness notice just after commit, so a read issued
    // the instant `ensure` returns may precede it: poll briefly.
    let text = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut whole = daemon.logs().source(LogSource::Daemon).open();
            let mut text = Vec::new();
            while let Some(item) = whole.next().await {
                match item.unwrap() {
                    LogItem::Chunk { generation, bytes } => {
                        assert_eq!(generation, first.generation());
                        text.extend_from_slice(&bytes);
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
            let text = String::from_utf8(text).unwrap();
            if text.contains("embedded daemon is ready") {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("daemon.log never recorded readiness");
    assert!(text.lines().all(|line| line.contains('Z')), "{text}");

    // The fixture's fmt layer writes to stderr, which the parent redirects
    // into stderr.log: the same events are visible there, unstructured.
    let mut stderr = daemon
        .logs()
        .source(LogSource::Stderr)
        .last_lines(2)
        .follow(true)
        .poll_interval(Duration::from_millis(50))
        .open();
    let chunk = tokio::time::timeout(Duration::from_secs(5), stderr.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let LogItem::Chunk { bytes, .. } = chunk else {
        panic!("expected chunk");
    };
    let last_lines = String::from_utf8_lossy(&bytes);
    assert!(last_lines.contains("tick"), "{last_lines}");
    assert!(last_lines.lines().count() <= 2, "{last_lines}");
    drop(stderr);

    let mut following = daemon
        .logs()
        .source(LogSource::Daemon)
        .last_lines(0)
        .follow(true)
        .poll_interval(Duration::from_millis(50))
        .open();
    let second = daemon.restart().await.unwrap();
    let mut changed = false;
    let mut fresh = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(item) = following.next().await {
            match item.unwrap() {
                LogItem::GenerationChanged { from, to } => {
                    assert_eq!((from, to), (first.generation(), second.generation()));
                    changed = true;
                }
                LogItem::Chunk { generation, bytes } if changed => {
                    assert_eq!(generation, second.generation());
                    fresh.extend_from_slice(&bytes);
                    if String::from_utf8_lossy(&fresh).contains("embedded daemon is ready") {
                        return;
                    }
                }
                LogItem::Chunk { generation, .. } => assert_eq!(generation, first.generation()),
                other => panic!("unexpected {other:?}"),
            }
        }
        panic!("log stream ended");
    })
    .await
    .unwrap();
    drop(following);

    // A clean stop keeps the generation's logs within the retention policy
    // and `logs()` falls back to the newest retained generation once the
    // discovery record is gone.
    daemon.stop().await.unwrap();
    assert!(matches!(
        daemon.status().await.unwrap(),
        InstanceStatus::Absent
    ));
    assert_eq!(generation_directory_count(root.as_path()), 1);
    let mut after = daemon.logs().source(LogSource::Daemon).open();
    let mut text = Vec::new();
    while let Some(item) = after.next().await {
        match item.unwrap() {
            LogItem::Chunk { generation, bytes } => {
                assert_eq!(generation, second.generation());
                text.extend_from_slice(&bytes);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    let text = String::from_utf8(text).unwrap();
    assert!(text.contains("shutdown requested"), "{text}");
    assert!(text.contains("embedded service exited"), "{text}");
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_wrappers_tail_events_logs_and_inspect() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let daemon = daemonkit::blocking::Daemon::embedded(
        DaemonSpec::new("com.example.blocking-tail")
            .unwrap()
            .runtime_root(root)
            .startup_timeout(Duration::from_secs(10))
            .shutdown_timeout(Duration::from_secs(10)),
        fixture_spawn(),
    )
    .unwrap();
    let instance = daemon.ensure().unwrap();
    let report = instance.inspect().unwrap();
    assert_eq!(report.state, daemonkit::LifecycleState::Ready);

    let items: Vec<EventItem> = daemon
        .events()
        .replay(16)
        .follow(false)
        .open()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(items
        .iter()
        .any(|item| message_is(item, "embedded daemon is ready")));
    assert_eq!(
        items.last(),
        Some(&EventItem::Ended {
            reason: EndReason::Replayed
        })
    );

    let mut text = Vec::new();
    for item in daemon.logs().source(LogSource::Daemon).open().unwrap() {
        if let LogItem::Chunk { bytes, .. } = item.unwrap() {
            text.extend_from_slice(&bytes);
        }
    }
    assert!(String::from_utf8_lossy(&text).contains("embedded daemon is ready"));
    assert!(matches!(
        daemon.set_log_filter("debug"),
        Err(daemonkit::blocking::BlockingError::Lifecycle(
            daemonkit::Error::Rejected { .. }
        ))
    ));
    daemon.stop().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_child_output_streams_as_line_events() {
    if Bootstrap::detect().unwrap().is_some() {
        return;
    }
    let state = tempfile::tempdir().unwrap();
    let root = RuntimeRoot::new(state.path().join("runtime")).unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = reservation.local_addr().unwrap();
    drop(reservation);
    let daemon = Daemon::managed(
        DaemonSpec::new("com.example.managed-events")
            .unwrap()
            .runtime_root(root.clone())
            .startup_timeout(Duration::from_secs(10))
            .shutdown_timeout(Duration::from_secs(10)),
        fixture_spawn()
            .env("DAEMONKIT_TEST_MANAGED", "1")
            .env("DAEMONKIT_TEST_MANAGED_ENDPOINT", endpoint.to_string()),
    )
    .unwrap();
    let instance = daemon.ensure().await.unwrap();

    let mut events = instance
        .events()
        .replay(u32::MAX)
        .filter("stdout=info,daemonkit=info,stderr=off")
        .follow(false)
        .open()
        .await
        .unwrap();
    let mut ready_line = None;
    let mut ready_notice = false;
    let mut saw_stderr = false;
    let mut truncated_noise = false;
    while let Some(item) = events.next().await {
        match item.unwrap() {
            EventItem::Event(event) => {
                saw_stderr |= event.source == EventSource::Stderr;
                truncated_noise |= event.source == EventSource::Stdout && event.truncated;
                ready_notice |= event.message == "managed child is ready";
                if event.source == EventSource::Stdout && event.message.starts_with("READY ") {
                    ready_line = Some(event);
                }
            }
            EventItem::Ended { reason } => {
                assert_eq!(reason, EndReason::Replayed);
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    let ready_line = ready_line.expect("READY line was not streamed");
    assert_eq!(ready_line.message, format!("READY {endpoint}"));
    assert_eq!(ready_line.target, "stdout");
    assert!(ready_notice);
    assert!(!saw_stderr, "stderr=off directive was ignored");
    assert!(
        truncated_noise,
        "2 MiB noise line should be bounded and marked"
    );
    assert!(matches!(
        daemon.set_log_filter("debug").await,
        Err(daemonkit::Error::Unsupported)
    ));

    let diagnostics = String::from_utf8(daemon.diagnostics().await.unwrap()).unwrap();
    assert!(diagnostics.contains("[stdout] READY"), "{diagnostics}");

    let report = daemon.inspect().await.unwrap();
    assert_eq!(report.mode, daemonkit::DaemonMode::Managed);
    assert_eq!(report.state, daemonkit::LifecycleState::Ready);
    assert_eq!(report.generation, instance.generation());
    let managed_pid = report.managed_process_id.expect("managed pid");
    assert_ne!(managed_pid, report.process_id);
    assert!(process_exists(managed_pid));
    assert_eq!(report.log_filter, None);
    daemon.stop().await.unwrap();
    assert!(matches!(
        daemon.inspect().await,
        Err(daemonkit::Error::Unavailable { .. })
    ));
}
