//! Deterministic, single-threaded lifecycle simulation for `daemonkit`.
//!
//! ```
//! use daemonkit_testkit::{ClientAction, Scenario};
//!
//! let result = Scenario::embedded("com.example.worker")?
//!     .client("first", ClientAction::ensure())
//!     .client("second", ClientAction::ensure())
//!     .run()?;
//! result.assert_invariants()?;
//! result.assert_one_ready_generation()?;
//! assert_eq!(result.generation("first")?, result.generation("second")?);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use daemonkit::{AppId, ConfigError, Embedded, GenerationId, LifecycleState, Managed};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    time::Duration,
};
use thiserror::Error;

pub use daemonkit::__testkit::{FaultPoint, SimPlatform};

const REPLAY_SCHEMA: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientAction {
    Ensure,
    Stop,
    Restart,
    Repair,
    Status,
}
impl ClientAction {
    pub fn ensure() -> Self {
        Self::Ensure
    }
    pub fn stop() -> Self {
        Self::Stop
    }
    pub fn restart() -> Self {
        Self::Restart
    }
    pub fn status() -> Self {
        Self::Status
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationOutcome {
    Generation(GenerationId),
    Stopped,
    Absent,
    Repaired,
    Status(LifecycleState),
    Faulted(FaultPoint),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimEvent {
    ActorScheduled {
        actor: String,
    },
    LifecycleLockRequested {
        actor: String,
    },
    LifecycleLockAcquired {
        actor: String,
    },
    StateChanged {
        from: LifecycleState,
        to: LifecycleState,
    },
    LeaseAcquired {
        generation: GenerationId,
    },
    BootstrapPrepared {
        generation: GenerationId,
    },
    ManifestPublished {
        generation: GenerationId,
    },
    CommitSent {
        generation: GenerationId,
    },
    InvariantChecked {
        invariant: InvariantId,
    },
    ClockAdvanced {
        from_nanos: u64,
        to_nanos: u64,
    },
    EntropyGenerated {
        domain: String,
        bytes: usize,
    },
    FaultTriggered {
        fault: FaultId,
        at: FaultPoint,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FaultId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultAction {
    /// Model abrupt launcher loss. Uncommitted child state must roll back.
    CrashLauncher,
    /// Return a backend error at the semantic boundary and roll back startup.
    ReturnError,
    /// Close the uncommitted bootstrap channel and force child rollback.
    CloseBootstrap,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fault {
    id: FaultId,
    point: FaultPoint,
    occurrence: u64,
    action: FaultAction,
}

impl Fault {
    pub fn once(point: FaultPoint, action: FaultAction) -> Self {
        Self::nth(point, 1, action)
    }

    pub fn nth(point: FaultPoint, occurrence: u64, action: FaultAction) -> Self {
        assert!(occurrence > 0, "fault occurrence is one-based");
        let mut hash = Sha256::new();
        hash.update(b"daemonkit-testkit:fault:v1\0");
        hash.update(format!("{point:?}:{occurrence}:{action:?}"));
        let bytes: [u8; 32] = hash.finalize().into();
        Self {
            id: FaultId(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
            point,
            occurrence,
            action,
        }
    }

    pub fn id(&self) -> FaultId {
        self.id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum InvariantId {
    ExclusiveLifecycleLock,
    ExclusiveLease,
    OneReadyGeneration,
    CompleteManifest,
    ServiceableAuthority,
    GenerationOwnedCleanup,
    RevalidatedProcessIdentity,
    SecretRedaction,
    ExplainableWaits,
    OperationPostconditions,
}

const DEFAULT_INVARIANTS: [InvariantId; 10] = [
    InvariantId::ExclusiveLifecycleLock,
    InvariantId::ExclusiveLease,
    InvariantId::OneReadyGeneration,
    InvariantId::CompleteManifest,
    InvariantId::ServiceableAuthority,
    InvariantId::GenerationOwnedCleanup,
    InvariantId::RevalidatedProcessIdentity,
    InvariantId::SecretRedaction,
    InvariantId::ExplainableWaits,
    InvariantId::OperationPostconditions,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct World {
    state: Option<LifecycleState>,
    generation: Option<GenerationId>,
    manifest: Option<GenerationId>,
    manifest_complete: bool,
    leases: BTreeSet<GenerationId>,
    lock_owners: BTreeSet<String>,
    ready_generations: BTreeSet<GenerationId>,
    committed: bool,
    cleanup_respected_ownership: bool,
    destructive_identity_revalidated: bool,
    rendered_values: Vec<String>,
    secret_canaries: Vec<String>,
    blocked_waits_explainable: bool,
    operation_postconditions_hold: bool,
    next_generation: u128,
}

impl Default for World {
    fn default() -> Self {
        Self {
            state: None,
            generation: None,
            manifest: None,
            manifest_complete: true,
            leases: BTreeSet::new(),
            lock_owners: BTreeSet::new(),
            ready_generations: BTreeSet::new(),
            committed: false,
            cleanup_respected_ownership: true,
            destructive_identity_revalidated: true,
            rendered_values: Vec::new(),
            secret_canaries: Vec::new(),
            blocked_waits_explainable: true,
            operation_postconditions_hold: true,
            next_generation: 0,
        }
    }
}

impl World {
    fn absent() -> Self {
        Self {
            state: Some(LifecycleState::Absent),
            next_generation: 1,
            ..Self::default()
        }
    }
}

/// Declares deterministic pre-existing state for a scenario.
///
/// Safe methods create internally consistent worlds. Impossible or unsafe
/// states are available only through explicitly named `corrupt_*` methods.
pub struct WorldBuilder {
    world: World,
}

impl WorldBuilder {
    pub fn absent(&mut self) -> &mut Self {
        self.world = World::absent();
        self
    }

    pub fn ready_generation(&mut self, generation: GenerationId) -> &mut Self {
        self.world = World::absent();
        self.world.state = Some(LifecycleState::Ready);
        self.world.generation = Some(generation);
        self.world.manifest = Some(generation);
        self.world.leases.insert(generation);
        self.world.ready_generations.insert(generation);
        self.world.committed = true;
        self.world.next_generation = generation.0.saturating_add(1);
        self
    }

    pub fn unhealthy_generation(&mut self, generation: GenerationId) -> &mut Self {
        self.world = World::absent();
        self.world.state = Some(LifecycleState::Unhealthy);
        self.world.generation = Some(generation);
        self.world.next_generation = generation.0.saturating_add(1);
        self
    }

    pub fn corrupt_multiple_lock_owners(&mut self) -> &mut Self {
        self.world.lock_owners = BTreeSet::from(["corrupt-one".into(), "corrupt-two".into()]);
        self
    }

    pub fn corrupt_multiple_lease_owners(&mut self) -> &mut Self {
        self.world.leases = BTreeSet::from([GenerationId(1), GenerationId(2)]);
        self
    }

    pub fn corrupt_multiple_ready_generations(&mut self) -> &mut Self {
        self.world.ready_generations = BTreeSet::from([GenerationId(1), GenerationId(2)]);
        self
    }

    pub fn corrupt_incomplete_manifest(&mut self) -> &mut Self {
        let generation = self.world.generation.unwrap_or(GenerationId(1));
        self.world.generation = Some(generation);
        self.world.manifest = Some(generation);
        self.world.manifest_complete = false;
        self
    }

    pub fn corrupt_serviceable_without_authority(&mut self) -> &mut Self {
        let generation = self.world.generation.unwrap_or(GenerationId(1));
        self.world.generation = Some(generation);
        self.world.manifest = Some(generation);
        self.world.ready_generations = BTreeSet::from([generation]);
        self.world.committed = false;
        self.world.leases.clear();
        self
    }

    pub fn corrupt_foreign_generation_cleanup(&mut self) -> &mut Self {
        self.world.cleanup_respected_ownership = false;
        self
    }

    pub fn corrupt_unvalidated_process_action(&mut self) -> &mut Self {
        self.world.destructive_identity_revalidated = false;
        self
    }

    pub fn corrupt_secret_leak(
        &mut self,
        secret: impl Into<String>,
        rendered: impl Into<String>,
    ) -> &mut Self {
        self.world.secret_canaries.push(secret.into());
        self.world.rendered_values.push(rendered.into());
        self
    }

    pub fn corrupt_unexplained_wait(&mut self) -> &mut Self {
        self.world.blocked_waits_explainable = false;
        self
    }

    pub fn corrupt_operation_postcondition(&mut self) -> &mut Self {
        self.world.operation_postconditions_hold = false;
        self
    }
}

/// Monotonic logical time. Simulation code never reads a wall clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicClock {
    now_nanos: u64,
}

impl DeterministicClock {
    pub fn now_nanos(&self) -> u64 {
        self.now_nanos
    }

    pub fn advance(&mut self, duration: Duration) -> Result<u64, SimError> {
        let nanos = u64::try_from(duration.as_nanos()).map_err(|_| SimError::TimeOverflow)?;
        self.now_nanos = self
            .now_nanos
            .checked_add(nanos)
            .ok_or(SimError::TimeOverflow)?;
        Ok(self.now_nanos)
    }
}

/// Domain-separated deterministic entropy for reproducible identities/tokens.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicEntropy {
    seed: [u8; 32],
    counters: BTreeMap<String, u64>,
}

impl DeterministicEntropy {
    pub fn from_seed(seed: u64) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"daemonkit-testkit:entropy-seed:v1\0");
        hash.update(seed.to_le_bytes());
        Self {
            seed: hash.finalize().into(),
            counters: BTreeMap::new(),
        }
    }

    pub fn bytes(&mut self, domain: impl Into<String>, length: usize) -> Vec<u8> {
        let domain = domain.into();
        let counter = self.counters.entry(domain.clone()).or_default();
        let mut output = Vec::with_capacity(length);
        while output.len() < length {
            let mut hash = Sha256::new();
            hash.update(b"daemonkit-testkit:entropy:v1\0");
            hash.update(self.seed);
            hash.update(domain.as_bytes());
            hash.update(counter.to_le_bytes());
            output.extend_from_slice(&hash.finalize());
            *counter += 1;
        }
        output.truncate(length);
        output
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace(Vec<SimEvent>);
impl Trace {
    pub fn events(&self) -> &[SimEvent] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayToken {
    schema: u16,
    scenario_digest: [u8; 32],
    choices: Vec<usize>,
}
impl ReplayToken {
    pub fn encode(&self) -> Result<String, SimError> {
        serde_json::to_string(self).map_err(SimError::serialization)
    }
    pub fn decode(value: &str) -> Result<Self, SimError> {
        serde_json::from_str(value).map_err(SimError::serialization)
    }
}

#[derive(Clone, Debug, Default)]
pub enum Schedule {
    #[default]
    Fifo,
    Seeded(u64),
    Replay(ReplayToken),
}
#[derive(Clone, Debug)]
struct Actor {
    name: String,
    action: ClientAction,
}

pub struct Scenario<M> {
    app: AppId,
    actors: Vec<Actor>,
    faults: Vec<Fault>,
    initial_world: World,
    schedule: Schedule,
    _mode: PhantomData<M>,
}

impl Scenario<Embedded> {
    pub fn embedded(app: impl TryInto<AppId, Error = ConfigError>) -> Result<Self, ConfigError> {
        Ok(Self {
            app: app.try_into()?,
            actors: vec![],
            faults: vec![],
            initial_world: World::absent(),
            schedule: Schedule::Fifo,
            _mode: PhantomData,
        })
    }
}
impl Scenario<Managed> {
    pub fn managed(app: impl TryInto<AppId, Error = ConfigError>) -> Result<Self, ConfigError> {
        Ok(Self {
            app: app.try_into()?,
            actors: vec![],
            faults: vec![],
            initial_world: World::absent(),
            schedule: Schedule::Fifo,
            _mode: PhantomData,
        })
    }
}
impl<M> Scenario<M> {
    pub fn client(mut self, name: impl Into<String>, action: ClientAction) -> Self {
        self.actors.push(Actor {
            name: name.into(),
            action,
        });
        self
    }
    pub fn schedule(mut self, schedule: Schedule) -> Self {
        self.schedule = schedule;
        self
    }
    pub fn fault(mut self, fault: Fault) -> Self {
        self.faults.push(fault);
        self
    }
    pub fn world(mut self, configure: impl FnOnce(&mut WorldBuilder)) -> Self {
        let mut builder = WorldBuilder {
            world: self.initial_world,
        };
        configure(&mut builder);
        self.initial_world = builder.world;
        self
    }
    pub fn run(self) -> Result<SimResult, SimError> {
        Simulation::new(
            self.app,
            self.actors,
            self.faults,
            self.initial_world,
            self.schedule,
        )
        .run()
    }

    pub fn explore(self, explorer: Explorer) -> Result<ExploreReport, SimError> {
        explorer.run(self.app, self.actors, self.faults, self.initial_world)
    }
}

/// Bounds for exhaustive actor-order exploration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Explorer {
    max_schedules: usize,
}

impl Explorer {
    pub fn new() -> Self {
        Self {
            max_schedules: 10_000,
        }
    }

    pub fn max_schedules(mut self, value: usize) -> Self {
        self.max_schedules = value;
        self
    }

    fn run(
        self,
        app: AppId,
        actors: Vec<Actor>,
        faults: Vec<Fault>,
        initial_world: World,
    ) -> Result<ExploreReport, SimError> {
        if self.max_schedules == 0 {
            return Err(SimError::InvalidExplorerBound);
        }
        let digest = scenario_digest(&app, &actors, &faults, &initial_world);
        let mut schedules = Vec::new();
        enumerate_choices(
            actors.len(),
            &mut Vec::new(),
            &mut schedules,
            self.max_schedules,
        );
        let truncated = schedules.len() == self.max_schedules
            && factorial_saturating(actors.len()) > self.max_schedules;
        let mut report = ExploreReport {
            explored: 0,
            truncated,
            first_failure: None,
        };
        for choices in schedules {
            let replay = ReplayToken {
                schema: REPLAY_SCHEMA,
                scenario_digest: digest,
                choices,
            };
            report.explored += 1;
            if let Err(error) = Simulation::new(
                app.clone(),
                actors.clone(),
                faults.clone(),
                initial_world.clone(),
                Schedule::Replay(replay.clone()),
            )
            .run()
            {
                report.first_failure = Some(ExploreFailure {
                    message: error.to_string(),
                    replay,
                });
                break;
            }
        }
        Ok(report)
    }
}

impl Default for Explorer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExploreFailure {
    message: String,
    replay: ReplayToken,
}

impl ExploreFailure {
    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn replay_token(&self) -> &ReplayToken {
        &self.replay
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExploreReport {
    explored: usize,
    truncated: bool,
    first_failure: Option<ExploreFailure>,
}

impl ExploreReport {
    pub fn explored_schedules(&self) -> usize {
        self.explored
    }

    pub fn was_truncated(&self) -> bool {
        self.truncated
    }

    pub fn first_failure(&self) -> Option<&ExploreFailure> {
        self.first_failure.as_ref()
    }
}

fn enumerate_choices(
    remaining: usize,
    prefix: &mut Vec<usize>,
    output: &mut Vec<Vec<usize>>,
    limit: usize,
) {
    if output.len() >= limit {
        return;
    }
    if remaining == 0 {
        output.push(prefix.clone());
        return;
    }
    for choice in 0..remaining {
        prefix.push(choice);
        enumerate_choices(remaining - 1, prefix, output, limit);
        prefix.pop();
        if output.len() >= limit {
            return;
        }
    }
}

fn factorial_saturating(value: usize) -> usize {
    (1..=value).fold(1, usize::saturating_mul)
}

pub struct Simulation {
    app: AppId,
    actors: Vec<Actor>,
    faults: Vec<Fault>,
    initial_world: World,
    schedule: Schedule,
}
impl Simulation {
    fn new(
        app: AppId,
        actors: Vec<Actor>,
        faults: Vec<Fault>,
        initial_world: World,
        schedule: Schedule,
    ) -> Self {
        Self {
            app,
            actors,
            faults,
            initial_world,
            schedule,
        }
    }
    pub fn run(mut self) -> Result<SimResult, SimError> {
        let digest = scenario_digest(&self.app, &self.actors, &self.faults, &self.initial_world);
        let choices = schedule_choices(&self.schedule, digest, self.actors.len())?;
        let mut world = self.initial_world;
        let mut trace = Vec::new();
        let mut outcomes = BTreeMap::new();
        let mut fault_runtime = FaultRuntime::new(self.faults);
        let mut remaining: Vec<Actor> = self.actors.drain(..).collect();
        for choice in &choices {
            let actor = remaining.remove(*choice % remaining.len());
            trace.push(SimEvent::ActorScheduled {
                actor: actor.name.clone(),
            });
            apply(
                &actor,
                &mut world,
                &mut trace,
                &mut outcomes,
                &mut fault_runtime,
            )?;
            check_invariants(&world, &mut trace)?;
        }
        Ok(SimResult {
            world,
            outcomes,
            trace: Trace(trace),
            replay: ReplayToken {
                schema: REPLAY_SCHEMA,
                scenario_digest: digest,
                choices,
            },
        })
    }
}

fn schedule_choices(
    schedule: &Schedule,
    digest: [u8; 32],
    count: usize,
) -> Result<Vec<usize>, SimError> {
    match schedule {
        Schedule::Fifo => Ok(vec![0; count]),
        Schedule::Seeded(mut seed) => Ok((0..count)
            .map(|left| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (seed as usize) % (count - left)
            })
            .collect()),
        Schedule::Replay(token) if token.schema != REPLAY_SCHEMA => Err(SimError::ReplaySchema {
            expected: REPLAY_SCHEMA,
            observed: token.schema,
        }),
        Schedule::Replay(token) if token.scenario_digest != digest => {
            Err(SimError::ReplayScenarioMismatch)
        }
        Schedule::Replay(token) if token.choices.len() != count => {
            Err(SimError::ReplayScenarioMismatch)
        }
        Schedule::Replay(token) => Ok(token.choices.clone()),
    }
}

fn scenario_digest(app: &AppId, actors: &[Actor], faults: &[Fault], world: &World) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(format!("{app:?}"));
    for actor in actors {
        h.update(&actor.name);
        h.update(format!("{:?}", actor.action));
    }
    for fault in faults {
        h.update(format!("{fault:?}"));
    }
    h.update(serde_json::to_vec(world).expect("world serialization cannot fail"));
    h.finalize().into()
}

struct FaultRuntime {
    faults: Vec<Fault>,
    hits: BTreeMap<FaultPoint, u64>,
    fired: BTreeSet<FaultId>,
}

impl FaultRuntime {
    fn new(faults: Vec<Fault>) -> Self {
        Self {
            faults,
            hits: BTreeMap::new(),
            fired: BTreeSet::new(),
        }
    }

    fn trigger(&mut self, point: FaultPoint, trace: &mut Vec<SimEvent>) -> Option<FaultAction> {
        let hit = self.hits.entry(point).or_default();
        *hit += 1;
        let fault = self.faults.iter().find(|fault| {
            fault.point == point && fault.occurrence == *hit && !self.fired.contains(&fault.id)
        })?;
        self.fired.insert(fault.id);
        trace.push(SimEvent::FaultTriggered {
            fault: fault.id,
            at: point,
        });
        Some(fault.action)
    }
}

fn transition(
    world: &mut World,
    trace: &mut Vec<SimEvent>,
    next: LifecycleState,
) -> Result<(), SimError> {
    let from = world.state.unwrap_or(LifecycleState::Absent);
    world.state = Some(from.transition(next)?);
    trace.push(SimEvent::StateChanged { from, to: next });
    Ok(())
}

fn apply(
    actor: &Actor,
    world: &mut World,
    trace: &mut Vec<SimEvent>,
    outcomes: &mut BTreeMap<String, OperationOutcome>,
    faults: &mut FaultRuntime,
) -> Result<(), SimError> {
    trace.push(SimEvent::LifecycleLockRequested {
        actor: actor.name.clone(),
    });
    world.lock_owners.insert(actor.name.clone());
    if world.lock_owners.len() > 1 {
        return Err(SimError::Invariant(InvariantId::ExclusiveLifecycleLock));
    }
    trace.push(SimEvent::LifecycleLockAcquired {
        actor: actor.name.clone(),
    });
    let outcome = match actor.action {
        ClientAction::Ensure => {
            if world.state == Some(LifecycleState::Ready) {
                OperationOutcome::Generation(world.generation.unwrap())
            } else {
                transition(world, trace, LifecycleState::Preparing)?;
                let generation = GenerationId(world.next_generation);
                world.next_generation += 1;
                if faults
                    .trigger(FaultPoint::AfterGenerationCreated, trace)
                    .is_some()
                {
                    rollback_start(world, trace)?;
                    OperationOutcome::Faulted(FaultPoint::AfterGenerationCreated)
                } else {
                    world.generation = Some(generation);
                    world.leases.insert(generation);
                    if faults
                        .trigger(FaultPoint::AfterChildSpawned, trace)
                        .is_some()
                    {
                        rollback_start(world, trace)?;
                        OperationOutcome::Faulted(FaultPoint::AfterChildSpawned)
                    } else {
                        trace.push(SimEvent::LeaseAcquired { generation });
                        trace.push(SimEvent::BootstrapPrepared { generation });
                        if faults
                            .trigger(FaultPoint::AfterBootstrapPrepared, trace)
                            .is_some()
                        {
                            rollback_start(world, trace)?;
                            OperationOutcome::Faulted(FaultPoint::AfterBootstrapPrepared)
                        } else {
                            transition(world, trace, LifecycleState::Published)?;
                            world.manifest = Some(generation);
                            trace.push(SimEvent::ManifestPublished { generation });
                            if faults
                                .trigger(FaultPoint::AfterManifestPublish, trace)
                                .is_some()
                            {
                                rollback_start(world, trace)?;
                                OperationOutcome::Faulted(FaultPoint::AfterManifestPublish)
                            } else {
                                world.committed = true;
                                trace.push(SimEvent::CommitSent { generation });
                                transition(world, trace, LifecycleState::Ready)?;
                                world.ready_generations.insert(generation);
                                OperationOutcome::Generation(generation)
                            }
                        }
                    }
                }
            }
        }
        ClientAction::Stop => {
            if world.state == Some(LifecycleState::Absent) {
                OperationOutcome::Absent
            } else {
                transition(world, trace, LifecycleState::Stopping)?;
                world.committed = false;
                world.manifest = None;
                world.leases.clear();
                world.ready_generations.clear();
                world.generation = None;
                transition(world, trace, LifecycleState::Absent)?;
                OperationOutcome::Stopped
            }
        }
        ClientAction::Restart => {
            if world.state != Some(LifecycleState::Absent) {
                transition(world, trace, LifecycleState::Stopping)?;
                world.committed = false;
                world.manifest = None;
                world.leases.clear();
                world.ready_generations.clear();
                world.generation = None;
                transition(world, trace, LifecycleState::Absent)?;
            }
            transition(world, trace, LifecycleState::Preparing)?;
            let generation = GenerationId(world.next_generation);
            world.next_generation += 1;
            world.generation = Some(generation);
            world.leases.insert(generation);
            transition(world, trace, LifecycleState::Published)?;
            world.manifest = Some(generation);
            world.committed = true;
            transition(world, trace, LifecycleState::Ready)?;
            world.ready_generations.insert(generation);
            OperationOutcome::Generation(generation)
        }
        ClientAction::Repair => OperationOutcome::Repaired,
        ClientAction::Status => OperationOutcome::Status(world.state.unwrap()),
    };
    outcomes.insert(actor.name.clone(), outcome);
    world.lock_owners.remove(&actor.name);
    Ok(())
}

fn rollback_start(world: &mut World, trace: &mut Vec<SimEvent>) -> Result<(), SimError> {
    world.committed = false;
    world.manifest = None;
    world.leases.clear();
    world.ready_generations.clear();
    world.generation = None;
    transition(world, trace, LifecycleState::Absent)
}

fn check_invariants(world: &World, trace: &mut Vec<SimEvent>) -> Result<(), SimError> {
    for invariant in DEFAULT_INVARIANTS {
        let valid = match invariant {
            InvariantId::ExclusiveLifecycleLock => world.lock_owners.len() <= 1,
            InvariantId::ExclusiveLease => world.leases.len() <= 1,
            InvariantId::OneReadyGeneration => world.ready_generations.len() <= 1,
            InvariantId::CompleteManifest => {
                world.manifest.is_none()
                    || (world.manifest_complete && world.manifest == world.generation)
            }
            InvariantId::ServiceableAuthority => {
                world.ready_generations.is_empty()
                    || (world.committed
                        && world.generation.is_some_and(|generation| {
                            world.leases.contains(&generation)
                                && world.manifest == Some(generation)
                                && world.ready_generations.contains(&generation)
                        }))
            }
            InvariantId::GenerationOwnedCleanup => world.cleanup_respected_ownership,
            InvariantId::RevalidatedProcessIdentity => world.destructive_identity_revalidated,
            InvariantId::SecretRedaction => !world.secret_canaries.iter().any(|secret| {
                world
                    .rendered_values
                    .iter()
                    .any(|rendered| rendered.contains(secret))
            }),
            InvariantId::ExplainableWaits => world.blocked_waits_explainable,
            InvariantId::OperationPostconditions => world.operation_postconditions_hold,
        };
        if !valid {
            return Err(SimError::Invariant(invariant));
        }
        trace.push(SimEvent::InvariantChecked { invariant });
    }
    Ok(())
}

#[derive(Debug)]
pub struct SimResult {
    world: World,
    outcomes: BTreeMap<String, OperationOutcome>,
    trace: Trace,
    replay: ReplayToken,
}
impl SimResult {
    pub fn assert_invariants(&self) -> Result<(), SimError> {
        let mut ignored = vec![];
        check_invariants(&self.world, &mut ignored)
    }
    pub fn assert_one_ready_generation(&self) -> Result<(), SimError> {
        (self.world.state == Some(LifecycleState::Ready) && self.world.generation.is_some())
            .then_some(())
            .ok_or(SimError::ExpectedReady)
    }
    pub fn generation(&self, actor: &str) -> Result<GenerationId, SimError> {
        match self.outcomes.get(actor) {
            Some(OperationOutcome::Generation(id)) => Ok(*id),
            _ => Err(SimError::MissingActor(actor.to_owned())),
        }
    }
    pub fn trace(&self) -> &Trace {
        &self.trace
    }
    pub fn replay_token(&self) -> &ReplayToken {
        &self.replay
    }
}

#[derive(Debug, Error)]
pub enum SimError {
    #[error(transparent)]
    Transition(#[from] daemonkit::TransitionError),
    #[error("invariant failed: {0:?}")]
    Invariant(InvariantId),
    #[error("expected exactly one ready generation")]
    ExpectedReady,
    #[error("actor has no generation outcome: {0}")]
    MissingActor(String),
    #[error("replay schema mismatch: expected {expected}, observed {observed}")]
    ReplaySchema { expected: u16, observed: u16 },
    #[error("replay token does not match this scenario")]
    ReplayScenarioMismatch,
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("logical time overflow")]
    TimeOverflow,
    #[error("explorer requires at least one schedule")]
    InvalidExplorerBound,
}
impl SimError {
    fn serialization(error: impl std::fmt::Display) -> Self {
        Self::Serialization(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_ensure_reuses_generation_and_checks_every_invariant() {
        let result = Scenario::embedded("com.example.worker")
            .unwrap()
            .client("one", ClientAction::ensure())
            .client("two", ClientAction::ensure())
            .run()
            .unwrap();
        assert_eq!(
            result.generation("one").unwrap(),
            result.generation("two").unwrap()
        );
        let checked: BTreeSet<_> = result
            .trace()
            .events()
            .iter()
            .filter_map(|e| match e {
                SimEvent::InvariantChecked { invariant } => Some(*invariant),
                _ => None,
            })
            .collect();
        assert_eq!(checked.len(), DEFAULT_INVARIANTS.len());
    }

    #[test]
    fn seeded_schedule_replays_exactly() {
        fn scenario(schedule: Schedule) -> Scenario<Embedded> {
            Scenario::embedded("com.example.worker")
                .unwrap()
                .client("a", ClientAction::ensure())
                .client("b", ClientAction::status())
                .client("c", ClientAction::stop())
                .schedule(schedule)
        }
        let first = scenario(Schedule::Seeded(42)).run().unwrap();
        let replay = scenario(Schedule::Replay(first.replay_token().clone()))
            .run()
            .unwrap();
        assert_eq!(first.trace(), replay.trace());
    }

    #[test]
    fn replay_rejects_a_different_scenario() {
        let first = Scenario::embedded("com.example.one")
            .unwrap()
            .client("a", ClientAction::ensure())
            .run()
            .unwrap();
        let error = Scenario::embedded("com.example.two")
            .unwrap()
            .client("a", ClientAction::ensure())
            .schedule(Schedule::Replay(first.replay_token().clone()))
            .run()
            .unwrap_err();
        assert!(matches!(error, SimError::ReplayScenarioMismatch));
    }

    #[test]
    fn deterministic_clock_is_monotonic_and_checked() {
        let mut clock = DeterministicClock::default();
        assert_eq!(clock.advance(Duration::from_millis(2)).unwrap(), 2_000_000);
        assert_eq!(clock.advance(Duration::from_nanos(7)).unwrap(), 2_000_007);
        clock.now_nanos = u64::MAX;
        assert!(matches!(
            clock.advance(Duration::from_nanos(1)),
            Err(SimError::TimeOverflow)
        ));
    }

    #[test]
    fn entropy_is_reproducible_and_domain_separated() {
        let mut first = DeterministicEntropy::from_seed(7);
        let mut second = DeterministicEntropy::from_seed(7);
        assert_eq!(
            first.bytes("generation", 48),
            second.bytes("generation", 48)
        );

        let mut entropy = DeterministicEntropy::from_seed(7);
        assert_ne!(entropy.bytes("generation", 32), entropy.bytes("token", 32));
        assert_ne!(entropy.bytes("token", 32), entropy.bytes("token", 32));
    }

    #[test]
    fn every_default_invariant_has_a_working_negative_model() {
        let generation = GenerationId(1);
        let cases: Vec<(InvariantId, World)> = vec![
            (
                InvariantId::ExclusiveLifecycleLock,
                World {
                    lock_owners: BTreeSet::from(["one".into(), "two".into()]),
                    ..World::default()
                },
            ),
            (
                InvariantId::ExclusiveLease,
                World {
                    leases: BTreeSet::from([generation, GenerationId(2)]),
                    ..World::default()
                },
            ),
            (
                InvariantId::OneReadyGeneration,
                World {
                    ready_generations: BTreeSet::from([generation, GenerationId(2)]),
                    ..World::default()
                },
            ),
            (
                InvariantId::CompleteManifest,
                World {
                    generation: Some(generation),
                    manifest: Some(generation),
                    manifest_complete: false,
                    ..World::default()
                },
            ),
            (
                InvariantId::ServiceableAuthority,
                World {
                    generation: Some(generation),
                    manifest: Some(generation),
                    ready_generations: BTreeSet::from([generation]),
                    committed: false,
                    ..World::default()
                },
            ),
            (
                InvariantId::GenerationOwnedCleanup,
                World {
                    cleanup_respected_ownership: false,
                    ..World::default()
                },
            ),
            (
                InvariantId::RevalidatedProcessIdentity,
                World {
                    destructive_identity_revalidated: false,
                    ..World::default()
                },
            ),
            (
                InvariantId::SecretRedaction,
                World {
                    rendered_values: vec!["diagnostic contains canary".into()],
                    secret_canaries: vec!["canary".into()],
                    ..World::default()
                },
            ),
            (
                InvariantId::ExplainableWaits,
                World {
                    blocked_waits_explainable: false,
                    ..World::default()
                },
            ),
            (
                InvariantId::OperationPostconditions,
                World {
                    operation_postconditions_hold: false,
                    ..World::default()
                },
            ),
        ];

        assert_eq!(cases.len(), DEFAULT_INVARIANTS.len());
        for (expected, world) in cases {
            let error = check_invariants(&world, &mut Vec::new()).unwrap_err();
            assert!(
                matches!(error, SimError::Invariant(observed) if observed == expected),
                "mutation for {expected:?} produced {error:?}"
            );
        }
    }

    #[test]
    fn semantic_startup_fault_rolls_back_and_replays_exactly() {
        fn scenario(schedule: Schedule) -> Scenario<Embedded> {
            Scenario::embedded("com.example.worker")
                .unwrap()
                .client("launcher", ClientAction::ensure())
                .fault(Fault::once(
                    FaultPoint::AfterManifestPublish,
                    FaultAction::CrashLauncher,
                ))
                .schedule(schedule)
        }

        let first = scenario(Schedule::Seeded(99)).run().unwrap();
        assert_eq!(first.world.state, Some(LifecycleState::Absent));
        assert!(first.world.manifest.is_none());
        assert!(first.world.leases.is_empty());
        assert!(!first.world.committed);
        assert!(matches!(
            first.outcomes.get("launcher"),
            Some(OperationOutcome::Faulted(FaultPoint::AfterManifestPublish))
        ));

        let replay = scenario(Schedule::Replay(first.replay_token().clone()))
            .run()
            .unwrap();
        assert_eq!(first.trace(), replay.trace());
    }

    #[test]
    fn nth_fault_fires_once_at_the_requested_occurrence() {
        let result = Scenario::embedded("com.example.worker")
            .unwrap()
            .client("first", ClientAction::ensure())
            .client("stop", ClientAction::stop())
            .client("second", ClientAction::ensure())
            .fault(Fault::nth(
                FaultPoint::AfterGenerationCreated,
                2,
                FaultAction::ReturnError,
            ))
            .run()
            .unwrap();
        let fired: Vec<_> = result
            .trace()
            .events()
            .iter()
            .filter(|event| matches!(event, SimEvent::FaultTriggered { .. }))
            .collect();
        assert_eq!(fired.len(), 1);
        assert!(matches!(
            result.outcomes.get("second"),
            Some(OperationOutcome::Faulted(
                FaultPoint::AfterGenerationCreated
            ))
        ));
    }

    #[test]
    fn explorer_covers_the_hand_enumerated_actor_order_space() {
        let report = Scenario::embedded("com.example.worker")
            .unwrap()
            .client("a", ClientAction::status())
            .client("b", ClientAction::status())
            .client("c", ClientAction::status())
            .explore(Explorer::new())
            .unwrap();
        assert_eq!(report.explored_schedules(), 6);
        assert!(!report.was_truncated());
        assert!(report.first_failure().is_none());
    }

    #[test]
    fn explorer_honors_its_schedule_bound() {
        let report = Scenario::embedded("com.example.worker")
            .unwrap()
            .client("a", ClientAction::status())
            .client("b", ClientAction::status())
            .client("c", ClientAction::status())
            .explore(Explorer::new().max_schedules(2))
            .unwrap();
        assert_eq!(report.explored_schedules(), 2);
        assert!(report.was_truncated());
    }

    #[test]
    fn world_builder_creates_a_consistent_preexisting_ready_generation() {
        let generation = GenerationId(41);
        let result = Scenario::embedded("com.example.worker")
            .unwrap()
            .world(|world| {
                world.ready_generation(generation);
            })
            .client("attacher", ClientAction::ensure())
            .run()
            .unwrap();
        assert_eq!(result.generation("attacher").unwrap(), generation);
        result.assert_invariants().unwrap();
    }

    #[test]
    fn corrupt_world_construction_is_explicit_and_detected() {
        let error = Scenario::embedded("com.example.worker")
            .unwrap()
            .world(|world| {
                world.corrupt_incomplete_manifest();
            })
            .client("observer", ClientAction::status())
            .run()
            .unwrap_err();
        assert!(matches!(
            error,
            SimError::Invariant(InvariantId::CompleteManifest)
        ));
    }

    #[test]
    fn replay_digest_includes_the_initial_world() {
        let first = Scenario::embedded("com.example.worker")
            .unwrap()
            .client("observer", ClientAction::status())
            .run()
            .unwrap();
        let error = Scenario::embedded("com.example.worker")
            .unwrap()
            .world(|world| {
                world.unhealthy_generation(GenerationId(9));
            })
            .client("observer", ClientAction::status())
            .schedule(Schedule::Replay(first.replay_token().clone()))
            .run()
            .unwrap_err();
        assert!(matches!(error, SimError::ReplayScenarioMismatch));
    }
}
