use super::{
    bootstrap::{
        self, BootstrapCommand, LaunchConfig, BOOTSTRAP_MARKER_ENV, BOOTSTRAP_NONCE_ENV,
        BOOTSTRAP_VERSION,
    },
    layout::InstanceLayout,
    root::VerifiedRoot,
    transport::{self, NativeEndpoint, NativeStream},
};
use crate::{
    protocol::{
        client_handshake, read_control_response, write_control_request, ClientHello,
        ConnectionRole, ControlMessage, ControlResponse, ProtocolRange,
    },
    record::{
        ControlEndpointRecord, InstanceRecord, ServiceEndpointRecord, INSTANCE_RECORD_SCHEMA,
    },
    BinaryCompatibility, Compatibility, DaemonKey, DaemonSpec, Error, GenerationId, InstanceId,
    InstanceStatus, InstanceSummary, LifecycleState, LogPolicy, MismatchAction, RepairAction,
    RepairOutcome, RuntimeRoot, Spawn, StartupDiagnostics, StartupPhase, StopOutcome,
};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub(crate) struct NativeController {
    pub(crate) instance_id: InstanceId,
    app: [u8; 32],
    key: [u8; 32],
    isolation: [u8; 32],
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    drain_timeout: Duration,
    idle_timeout: Option<Duration>,
    readiness_timeout: Option<Duration>,
    compatibility: Compatibility,
    logs: LogPolicy,
    runtime_root: Option<RuntimeRoot>,
}

impl NativeController {
    pub(crate) fn new<K: DaemonKey>(spec: &DaemonSpec<K>, instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            app: spec.app.digest(),
            key: spec.key.digest().0,
            isolation: spec
                .isolation
                .as_ref()
                .map_or_else(crate::identity::default_isolation_digest, |value| {
                    value.digest()
                }),
            startup_timeout: spec.startup_timeout,
            shutdown_timeout: spec.shutdown_timeout,
            drain_timeout: spec.drain_timeout,
            idle_timeout: spec.idle_timeout,
            readiness_timeout: spec.readiness_timeout,
            compatibility: spec.compatibility.clone(),
            logs: spec.logs.clone(),
            runtime_root: spec.runtime_root.clone(),
        }
    }

    pub(crate) async fn ensure(&self, spawn: &Spawn) -> Result<NativeInstance, Error> {
        let layout = self.layout()?;
        let _guard = acquire_lifecycle(&layout, self.startup_timeout, &self.instance_id).await?;
        self.ensure_locked(&layout, spawn).await
    }

    async fn ensure_locked(
        &self,
        layout: &InstanceLayout,
        spawn: &Spawn,
    ) -> Result<NativeInstance, Error> {
        if let Some(record) = layout.record_store().read_record().map_err(map_store)? {
            self.verify_record_authority(&record)?;
            if self
                .authenticate(&record, ConnectionRole::Control)
                .await
                .is_ok()
            {
                if self.compatible(&record) && record.state == LifecycleState::Ready {
                    return Ok(self.native_instance(record));
                }
                if self.compatibility.mismatch == MismatchAction::Error {
                    return Err(Error::Unavailable {
                        generation: Some(record.generation),
                    });
                }
                self.stop_record(layout, &record).await?;
            } else {
                self.repair_stale(layout, &record)?;
            }
        }
        self.start_locked(layout, spawn).await
    }

    async fn start_locked(
        &self,
        layout: &InstanceLayout,
        spawn: &Spawn,
    ) -> Result<NativeInstance, Error> {
        let generation = GenerationId(uuid::Uuid::new_v4().as_u128());
        let generation_layout = layout.create_generation(generation).map_err(map_layout)?;
        let endpoint = endpoint_for(&self.instance_id, generation, layout)?;
        let token = random_32();
        let nonce = hex(&random_32());
        let process_identity = random_32().to_vec();
        let mut command = spawn.command();
        command
            .env(BOOTSTRAP_MARKER_ENV, "stdio-v1")
            .env(BOOTSTRAP_NONCE_ENV, &nonce)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(open_log(
                &generation_layout.path().join("stdout.log"),
            )?))
            .stderr(Stdio::from(open_log(
                &generation_layout.path().join("stderr.log"),
            )?));
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(windows)]
        command.creation_flags(0x0000_0200 | 0x0800_0000); // NEW_PROCESS_GROUP | NO_WINDOW
        let mut child = command.spawn().map_err(|error| {
            startup_error(
                spawn,
                layout,
                generation_layout.path(),
                None,
                StartupPhase::Spawn,
                error,
                self.logs.tail_bytes,
            )
        })?;
        let process_id = child.id().ok_or_else(|| Error::Bootstrap {
            message: "spawned child has no process id".into(),
        })?;
        let mut stdin = child.stdin.take().ok_or_else(|| Error::Bootstrap {
            message: "bootstrap stdin was not piped".into(),
        })?;
        let mut rollback = StartupRollback::new(
            child,
            layout.record_store(),
            layout.lease_lock(),
            generation,
            generation_layout.path().to_owned(),
        );
        let launch = LaunchConfig {
            version: BOOTSTRAP_VERSION,
            nonce: nonce.clone(),
            app: self.app,
            key: self.key,
            isolation: self.isolation,
            generation: generation.0.to_be_bytes(),
            token,
            protocol_minimum: self.compatibility.protocol.minimum(),
            protocol_maximum: self.compatibility.protocol.maximum(),
            process_id,
            process_identity: process_identity.clone(),
            build: requested_build(&self.compatibility),
            config: self.compatibility.config.0,
            endpoint: endpoint.clone(),
            lease_path: layout.path().join("lease.lock"),
            idle_timeout_millis: self
                .idle_timeout
                .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX)),
            drain_timeout_millis: Some(
                self.drain_timeout
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
            ),
            readiness_timeout_millis: self
                .readiness_timeout
                .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX)),
            generation_directory: generation_layout.path().to_owned(),
        };
        write_bootstrap(&mut stdin, &BootstrapCommand::Launch(Box::new(launch)))
            .await
            .map_err(|error| {
                startup_error(
                    spawn,
                    layout,
                    generation_layout.path(),
                    Some(process_id),
                    StartupPhase::BootstrapChannel,
                    error,
                    self.logs.tail_bytes,
                )
            })?;

        let hello = self.client_hello(generation, token, ConnectionRole::Control);
        // A managed child only binds this control endpoint after its readiness
        // probe finishes, so the parent's connect budget must cover the whole
        // readiness window plus the handshake. Embedded children bind
        // immediately (readiness_timeout is None), leaving this at
        // startup_timeout — the historical behavior.
        let prepare_budget = self
            .startup_timeout
            .saturating_add(self.readiness_timeout.unwrap_or(Duration::ZERO));
        let mut prepared = self
            .connect_until(&endpoint, prepare_budget)
            .await
            .map_err(|error| {
                startup_error(
                    spawn,
                    layout,
                    generation_layout.path(),
                    Some(process_id),
                    StartupPhase::Prepare,
                    error,
                    self.logs.tail_bytes,
                )
            })?;
        let prepared_identity = client_handshake(&mut prepared, &hello, self.startup_timeout)
            .await
            .map_err(|error| {
                startup_error(
                    spawn,
                    layout,
                    generation_layout.path(),
                    Some(process_id),
                    StartupPhase::Authenticate,
                    error,
                    self.logs.tail_bytes,
                )
            })?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let mut record = InstanceRecord {
            schema: INSTANCE_RECORD_SCHEMA,
            app_digest: self.app,
            key_digest: self.key,
            isolation_digest: self.isolation,
            generation,
            state: LifecycleState::Published,
            process_id: Some(process_id),
            process_identity: Some(process_identity),
            control_endpoint: endpoint_record(&endpoint),
            service_endpoint: prepared_identity
                .service_endpoint
                .as_deref()
                .map(serde_json::from_str::<ServiceEndpointRecord>)
                .transpose()
                .map_err(|error| {
                    startup_error(
                        spawn,
                        layout,
                        generation_layout.path(),
                        Some(process_id),
                        StartupPhase::Prepare,
                        error,
                        self.logs.tail_bytes,
                    )
                })?,
            protocol_minimum: self.compatibility.protocol.minimum(),
            protocol_maximum: self.compatibility.protocol.maximum(),
            build_fingerprint: requested_build(&self.compatibility),
            config_fingerprint: self.compatibility.config.0,
            authentication_token: token,
            created_unix_millis: now.as_millis().try_into().unwrap_or(u64::MAX),
            generation_directory: PathBuf::new(),
            daemon_log: PathBuf::new(),
            stdout_log: PathBuf::new(),
            stderr_log: PathBuf::new(),
        };
        generation_layout.record_paths(&mut record);
        layout
            .record_store()
            .publish_record(&record)
            .map_err(|error| {
                startup_error(
                    spawn,
                    layout,
                    generation_layout.path(),
                    Some(process_id),
                    StartupPhase::Publish,
                    error,
                    self.logs.tail_bytes,
                )
            })?;
        write_bootstrap(
            &mut stdin,
            &BootstrapCommand::Commit {
                version: BOOTSTRAP_VERSION,
                nonce,
            },
        )
        .await
        .map_err(|error| {
            startup_error(
                spawn,
                layout,
                generation_layout.path(),
                Some(process_id),
                StartupPhase::Commit,
                error,
                self.logs.tail_bytes,
            )
        })?;
        drop(stdin);
        record.state = LifecycleState::Ready;
        layout
            .record_store()
            .publish_record(&record)
            .map_err(|error| {
                startup_error(
                    spawn,
                    layout,
                    generation_layout.path(),
                    Some(process_id),
                    StartupPhase::Commit,
                    error,
                    self.logs.tail_bytes,
                )
            })?;
        let _ = layout.prune_generations(&self.logs, Some(generation), None);
        rollback.disarm();
        Ok(self.native_instance(record))
    }

    pub(crate) async fn attach(&self) -> Result<Option<NativeInstance>, Error> {
        let layout = self.layout()?;
        let Some(record) = layout.record_store().read_record().map_err(map_store)? else {
            return Ok(None);
        };
        self.verify_record_authority(&record)?;
        self.authenticate(&record, ConnectionRole::Control).await?;
        Ok(Some(self.native_instance(record)))
    }

    pub(crate) async fn status(&self) -> Result<InstanceStatus, Error> {
        let Some(instance) = self.attach().await? else {
            return Ok(InstanceStatus::Absent);
        };
        Ok(status_from(&instance.record))
    }

    pub(crate) async fn stop(&self) -> Result<StopOutcome, Error> {
        let layout = self.layout()?;
        let _guard = acquire_lifecycle(&layout, self.shutdown_timeout, &self.instance_id).await?;
        let Some(record) = layout.record_store().read_record().map_err(map_store)? else {
            return Ok(StopOutcome::AlreadyAbsent);
        };
        self.verify_record_authority(&record)?;
        self.stop_record(&layout, &record).await
    }

    pub(crate) async fn restart(&self, spawn: &Spawn) -> Result<NativeInstance, Error> {
        let layout = self.layout()?;
        let timeout = self.startup_timeout.saturating_add(self.shutdown_timeout);
        let _guard = acquire_lifecycle(&layout, timeout, &self.instance_id).await?;
        if let Some(record) = layout.record_store().read_record().map_err(map_store)? {
            self.verify_record_authority(&record)?;
            self.stop_record(&layout, &record).await?;
        }
        self.start_locked(&layout, spawn).await
    }

    pub(crate) async fn stop_expected(&self, expected: GenerationId) -> Result<StopOutcome, Error> {
        let layout = self.layout()?;
        let _guard = acquire_lifecycle(&layout, self.shutdown_timeout, &self.instance_id).await?;
        let Some(record) = layout.record_store().read_record().map_err(map_store)? else {
            return Err(Error::GenerationChanged {
                expected,
                observed: None,
            });
        };
        if record.generation != expected {
            return Err(Error::GenerationChanged {
                expected,
                observed: Some(record.generation),
            });
        }
        self.stop_record(&layout, &record).await
    }

    async fn stop_record(
        &self,
        layout: &InstanceLayout,
        record: &InstanceRecord,
    ) -> Result<StopOutcome, Error> {
        if let Ok(mut stream) = self.authenticate(record, ConnectionRole::Control).await {
            let _ = request(
                &mut stream,
                ControlMessage::Drain { request_id: 1 },
                self.shutdown_timeout,
            )
            .await;
            let _ = request(
                &mut stream,
                ControlMessage::Shutdown { request_id: 2 },
                self.shutdown_timeout,
            )
            .await;
        }
        let lease = layout.lease_lock();
        let lease_guard = tokio::time::timeout(self.shutdown_timeout, async {
            loop {
                match lease.acquire(Duration::ZERO) {
                    Ok(guard) => break guard,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .map_err(|_| Error::Unavailable {
            generation: Some(record.generation),
        })?;
        drop(lease_guard);
        layout.record_store().clear().map_err(map_store)?;
        let _ = std::fs::remove_dir_all(&record.generation_directory);
        Ok(StopOutcome::Stopped {
            generation: record.generation,
            forced: false,
        })
    }

    pub(crate) async fn repair(&self) -> Result<RepairOutcome, Error> {
        let layout = self.layout()?;
        let _guard = acquire_lifecycle(&layout, self.shutdown_timeout, &self.instance_id).await?;
        let Some(record) = layout.record_store().read_record().map_err(map_store)? else {
            return Ok(RepairOutcome::Clean);
        };
        self.verify_record_authority(&record)?;
        if self
            .authenticate(&record, ConnectionRole::Control)
            .await
            .is_ok()
        {
            return Ok(RepairOutcome::Busy);
        }
        self.repair_stale(&layout, &record)?;
        Ok(RepairOutcome::Repaired {
            actions: vec![RepairAction::RemovedStaleManifest],
        })
    }

    fn repair_stale(&self, layout: &InstanceLayout, record: &InstanceRecord) -> Result<(), Error> {
        let guard =
            layout
                .lease_lock()
                .acquire(Duration::ZERO)
                .map_err(|_| Error::Unavailable {
                    generation: Some(record.generation),
                })?;
        drop(guard);
        layout.record_store().clear().map_err(map_store)?;
        let _ = std::fs::remove_dir_all(&record.generation_directory);
        Ok(())
    }

    pub(crate) async fn connect_application(
        &self,
        record: &InstanceRecord,
    ) -> Result<NativeStream, Error> {
        self.authenticate(record, ConnectionRole::Application).await
    }

    async fn authenticate(
        &self,
        record: &InstanceRecord,
        role: ConnectionRole,
    ) -> Result<NativeStream, Error> {
        let endpoint = endpoint_from_record(&record.control_endpoint)?;
        let mut stream = transport::connect(&endpoint)
            .await
            .map_err(|_| Error::Unavailable {
                generation: Some(record.generation),
            })?;
        let hello = self.client_hello(record.generation, record.authentication_token, role);
        let negotiated = client_handshake(&mut stream, &hello, self.startup_timeout)
            .await
            .map_err(|error| match error {
                crate::protocol::ProtocolError::BusyQuiescing => Error::BusyQuiescing,
                // The daemon dying mid-handshake or missing its deadline is
                // an availability problem; reporting it as a credential
                // failure sends consumers down the wrong path.
                crate::protocol::ProtocolError::Io(_)
                | crate::protocol::ProtocolError::Deadline => Error::Unavailable {
                    generation: Some(record.generation),
                },
                _ => Error::AuthenticationFailed,
            })?;
        if negotiated.process_identity != record.process_identity.clone().unwrap_or_default()
            || negotiated.process_id != record.process_id.unwrap_or_default()
            || negotiated.build != record.build_fingerprint
            || negotiated.config != record.config_fingerprint
        {
            return Err(Error::AuthenticationFailed);
        }
        Ok(stream)
    }

    async fn connect_until(
        &self,
        endpoint: &NativeEndpoint,
        timeout: Duration,
    ) -> Result<NativeStream, Error> {
        let started = tokio::time::Instant::now();
        loop {
            match transport::connect(endpoint).await {
                Ok(stream) => return Ok(stream),
                Err(error) if started.elapsed() < timeout => {
                    let _ = error;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => {
                    return Err(Error::Bootstrap {
                        message: error.to_string(),
                    })
                }
            }
        }
    }

    fn client_hello(
        &self,
        generation: GenerationId,
        token: [u8; 32],
        role: ConnectionRole,
    ) -> ClientHello {
        ClientHello {
            app: self.app,
            key: self.key,
            isolation: self.isolation,
            generation: generation.0,
            token,
            versions: ProtocolRange {
                minimum: self.compatibility.protocol.minimum(),
                maximum: self.compatibility.protocol.maximum(),
            },
            role,
        }
    }

    fn layout(&self) -> Result<InstanceLayout, Error> {
        let root = VerifiedRoot::open(self.runtime_root.as_ref()).map_err(|error| {
            Error::StateDirectory {
                path: self
                    .runtime_root
                    .as_ref()
                    .map(|root| root.as_path().to_owned())
                    .unwrap_or_default(),
                message: error.to_string(),
            }
        })?;
        InstanceLayout::create(&root, self.app, crate::KeyDigest(self.key), self.isolation)
            .map_err(map_layout)
    }

    fn verify_record_authority(&self, record: &InstanceRecord) -> Result<(), Error> {
        if record.app_digest != self.app
            || record.key_digest != self.key
            || record.isolation_digest != self.isolation
        {
            return Err(Error::CorruptState {
                path: record.generation_directory.clone(),
                message: "record authority does not match requested instance".into(),
            });
        }
        Ok(())
    }

    fn compatible(&self, record: &InstanceRecord) -> bool {
        let protocol = record.protocol_minimum <= self.compatibility.protocol.maximum()
            && self.compatibility.protocol.minimum() <= record.protocol_maximum;
        let config = record.config_fingerprint == self.compatibility.config.0;
        let binary = match &self.compatibility.binary {
            BinaryCompatibility::Exact(expected) => record.build_fingerprint == expected.0,
            BinaryCompatibility::Any => true,
            BinaryCompatibility::Custom(check) => check.compatible(
                &crate::BuildFingerprint(record.build_fingerprint),
                &crate::BuildFingerprint(requested_build(&self.compatibility)),
            ),
            BinaryCompatibility::SemverCompatible(_) => false,
        };
        protocol && config && binary
    }

    fn native_instance(&self, record: InstanceRecord) -> NativeInstance {
        NativeInstance {
            instance_id: self.instance_id.clone(),
            record,
        }
    }
}

pub(crate) struct NativeInstance {
    pub(crate) instance_id: InstanceId,
    pub(crate) record: InstanceRecord,
}

fn requested_build(compatibility: &Compatibility) -> [u8; 32] {
    match &compatibility.binary {
        BinaryCompatibility::Exact(value) => value.0,
        _ => [0; 32],
    }
}

fn endpoint_for(
    instance: &InstanceId,
    generation: GenerationId,
    layout: &InstanceLayout,
) -> Result<NativeEndpoint, Error> {
    #[cfg(unix)]
    {
        Ok(NativeEndpoint::Unix(
            layout
                .control_endpoint_path(instance, generation)
                .map_err(map_layout)?,
        ))
    }
    #[cfg(windows)]
    {
        let _ = layout;
        Ok(NativeEndpoint::WindowsPipe(format!(
            r"\\.\pipe\daemonkit-{}-{:032x}",
            hex(&instance.0[..8]),
            generation.0
        )))
    }
}

fn endpoint_record(endpoint: &NativeEndpoint) -> ControlEndpointRecord {
    match endpoint {
        #[cfg(unix)]
        NativeEndpoint::Unix(path) => ControlEndpointRecord::UnixSocket(path.clone()),
        #[cfg(windows)]
        NativeEndpoint::WindowsPipe(name) => ControlEndpointRecord::WindowsNamedPipe(name.clone()),
    }
}

fn endpoint_from_record(endpoint: &ControlEndpointRecord) -> Result<NativeEndpoint, Error> {
    match endpoint {
        #[cfg(unix)]
        ControlEndpointRecord::UnixSocket(path) => Ok(NativeEndpoint::Unix(path.clone())),
        #[cfg(windows)]
        ControlEndpointRecord::WindowsNamedPipe(name) => {
            Ok(NativeEndpoint::WindowsPipe(name.clone()))
        }
        _ => Err(Error::CorruptState {
            path: PathBuf::new(),
            message: "record endpoint belongs to another platform".into(),
        }),
    }
}

async fn write_bootstrap(
    stdin: &mut tokio::process::ChildStdin,
    command: &BootstrapCommand,
) -> Result<(), Error> {
    let payload = bootstrap::encode(command).map_err(|error| Error::Bootstrap {
        message: error.to_string(),
    })?;
    crate::protocol::write_frame(stdin, &payload)
        .await
        .map_err(|error| Error::Bootstrap {
            message: error.to_string(),
        })
}

async fn request(
    stream: &mut NativeStream,
    message: ControlMessage,
    timeout: Duration,
) -> Result<ControlResponse, Error> {
    tokio::time::timeout(timeout, async {
        write_control_request(stream, &message)
            .await
            .map_err(|_| Error::Unavailable { generation: None })?;
        read_control_response(stream)
            .await
            .map_err(|_| Error::Unavailable { generation: None })
    })
    .await
    .map_err(|_| Error::Unavailable { generation: None })?
}

fn status_from(record: &InstanceRecord) -> InstanceStatus {
    let summary = InstanceSummary {
        generation: record.generation,
        process_id: record.process_id,
    };
    match record.state {
        LifecycleState::Preparing | LifecycleState::Published => InstanceStatus::Starting(summary),
        LifecycleState::Ready => InstanceStatus::Ready(summary),
        LifecycleState::Quiescing => InstanceStatus::Quiescing(summary),
        LifecycleState::Stopping => InstanceStatus::Stopping(summary),
        LifecycleState::Absent | LifecycleState::Unhealthy => InstanceStatus::Unhealthy {
            record: Some(summary),
        },
    }
}

fn open_log(path: &std::path::Path) -> Result<File, Error> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(|error| Error::Bootstrap {
        message: error.to_string(),
    })
}

fn random_32() -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn map_store(error: super::store::StoreError) -> Error {
    Error::CorruptState {
        path: PathBuf::new(),
        message: error.to_string(),
    }
}
fn map_layout(error: super::layout::LayoutError) -> Error {
    Error::StateDirectory {
        path: PathBuf::new(),
        message: error.to_string(),
    }
}

fn startup_error(
    spawn: &Spawn,
    layout: &InstanceLayout,
    generation_directory: &std::path::Path,
    process_id: Option<u32>,
    phase: StartupPhase,
    error: impl std::fmt::Display,
    tail_bytes: usize,
) -> Error {
    let stdout = generation_directory.join("stdout.log");
    let stderr = generation_directory.join("stderr.log");
    let (stdout_tail, stdout_truncated) = read_tail(&stdout, tail_bytes);
    let (stderr_tail, stderr_truncated) = read_tail(&stderr, tail_bytes);
    Error::Startup {
        phase,
        diagnostics: Box::new(StartupDiagnostics {
            phase,
            command: format!("{spawn:?}"),
            process_id,
            stdout_tail,
            stderr_tail,
            stdout_truncated,
            stderr_truncated,
            log_path: Some(generation_directory.join("daemon.log")),
            instance_path: layout.path().to_owned(),
        }),
        message: error.to_string(),
    }
}

fn read_tail(path: &std::path::Path, maximum: usize) -> (Vec<u8>, bool) {
    let Ok(bytes) = std::fs::read(path) else {
        return (Vec::new(), false);
    };
    if bytes.len() <= maximum {
        (bytes, false)
    } else {
        (bytes[bytes.len() - maximum..].to_vec(), true)
    }
}

struct StartupRollback {
    child: Option<tokio::process::Child>,
    store: super::store::AtomicStore,
    lease: super::locks::StableLock,
    generation: GenerationId,
    generation_directory: PathBuf,
    armed: bool,
}

impl StartupRollback {
    fn new(
        child: tokio::process::Child,
        store: super::store::AtomicStore,
        lease: super::locks::StableLock,
        generation: GenerationId,
        generation_directory: PathBuf,
    ) -> Self {
        Self {
            child: Some(child),
            store,
            lease,
            generation,
            generation_directory,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
        self.child.take();
    }
}

impl Drop for StartupRollback {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.start_kill();
        let store = self.store.clone();
        let lease = self.lease.clone();
        let generation = self.generation;
        let directory = self.generation_directory.clone();
        let cleanup = async move {
            let _ = child.wait().await;
            if let Ok(Ok(guard)) =
                tokio::task::spawn_blocking(move || lease.acquire(Duration::from_secs(5))).await
            {
                drop(guard);
            }
            if matches!(store.read_record(), Ok(Some(ref record)) if record.generation == generation)
            {
                let _ = store.clear();
            }
            let _ = std::fs::remove_dir_all(directory);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(cleanup);
        }
    }
}

async fn acquire_lifecycle(
    layout: &InstanceLayout,
    timeout: Duration,
    instance: &InstanceId,
) -> Result<super::locks::LockGuard, Error> {
    let lock = layout.lifecycle_lock();
    let instance = instance.clone();
    tokio::task::spawn_blocking(move || lock.acquire(timeout))
        .await
        .map_err(|error| Error::Bootstrap {
            message: error.to_string(),
        })?
        .map_err(|_| Error::Lock { instance })
}
