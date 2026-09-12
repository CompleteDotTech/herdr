use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use super::{
    client::CovenProviderClient, OperationTicket, ProviderActivity, ProviderBinding,
    ProviderCommand, ProviderConnection, ProviderDurability, ProviderFailure, ProviderFailureKind,
    ProviderGenerationError, ProviderLifecycle, ProviderOperation, ProviderOperationResult,
    ProviderSnapshot, ProviderStartError, ProviderUpdate, SubmitError,
};

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(100);

// Count retiring workers as well as attached workers. Repeated detach/reattach
// must not bypass the process bound while old transport requests time out.
const MAX_PROVIDER_WORKERS: usize = 64;
static ACTIVE_PROVIDER_WORKERS: AtomicUsize = AtomicUsize::new(0);
struct WorkerSlot(&'static AtomicUsize);
impl WorkerSlot {
    fn acquire(counter: &'static AtomicUsize) -> Result<Self, ProviderStartError> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < MAX_PROVIDER_WORKERS).then(|| active + 1)
            })
            .map_err(|_| ProviderStartError::WorkerLimitReached)?;
        Ok(Self(counter))
    }
}
impl Drop for WorkerSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

// Fences are unique across replacement actors in this Herdr process, not just
// within one handle. An old binding cannot become valid again after reattach.
static NEXT_RUNTIME_GENERATION: AtomicU64 = AtomicU64::new(1);

fn allocate_generation(counter: &AtomicU64) -> Result<u64, ProviderGenerationError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
            (next > 0 && next <= i64::MAX as u64).then(|| next + 1)
        })
        .map_err(|_| ProviderGenerationError::Exhausted)
}

struct CommandEnvelope {
    ticket: OperationTicket,
    runtime_generation: u64,
    command: ProviderCommand,
}

struct SharedState {
    // The fence serializes state transitions with snapshot publication. It is
    // intentionally held only across local admission/check/publication, never
    // across blocking Coven I/O.
    fence: Mutex<()>,
    snapshot: Mutex<Arc<ProviderSnapshot>>,
    updates: SyncSender<ProviderUpdate>,
    update_receiver: Mutex<Receiver<ProviderUpdate>>,
    next_ticket: AtomicU64,
    runtime_generation: AtomicU64,
    detached: AtomicBool,
    stopped: AtomicBool,
    last_external_resize: Mutex<Option<(u16, u16, u16, u16)>>,
    resize_admission: Mutex<()>,
}

/// Factory for the provider actor.  The actor owns the only mutable
/// `coven-client::DaemonClient` and all blocking I/O runs on its worker thread.
pub(crate) struct ProviderRuntime;

impl ProviderRuntime {
    pub(crate) fn start(
        config: super::ProviderConfig,
    ) -> Result<ProviderRuntimeHandle, ProviderStartError> {
        let mut client = CovenProviderClient::new(config.clone());
        let (handle, _worker) = Self::start_worker(config, move |command, generation| {
            let result = client.execute(command, generation);
            if matches!(
                &result,
                ProviderOperationResult::Failure(ProviderFailure {
                    kind: ProviderFailureKind::Disconnected | ProviderFailureKind::IdentityMismatch,
                    ..
                })
            ) {
                client.reset();
            }
            result
        })?;
        // Production retains the existing detached worker lifetime. Tests retain
        // the join handle to prove shutdown has finished dispatching queued work.
        Ok(handle)
    }

    fn start_worker<E>(
        config: super::ProviderConfig,
        executor: E,
    ) -> Result<(ProviderRuntimeHandle, thread::JoinHandle<()>), ProviderStartError>
    where
        E: FnMut(&ProviderCommand, u64) -> ProviderOperationResult + Send + 'static,
    {
        let worker_slot = WorkerSlot::acquire(&ACTIVE_PROVIDER_WORKERS)?;
        Self::start_worker_with_slot(config, executor, worker_slot)
    }

    fn start_worker_with_slot<E>(
        config: super::ProviderConfig,
        executor: E,
        worker_slot: WorkerSlot,
    ) -> Result<(ProviderRuntimeHandle, thread::JoinHandle<()>), ProviderStartError>
    where
        E: FnMut(&ProviderCommand, u64) -> ProviderOperationResult + Send + 'static,
    {
        let capacity = config
            .queue_capacity()
            .get()
            .min(super::MAX_PROVIDER_QUEUE_CAPACITY);
        let (command_tx, command_rx) = mpsc::sync_channel(capacity);
        let (updates_tx, updates_rx) = mpsc::sync_channel(capacity);
        let runtime_generation = allocate_generation(&NEXT_RUNTIME_GENERATION)
            .map_err(|_| ProviderStartError::GenerationExhausted)?;
        let snapshot = Arc::new(ProviderSnapshot {
            identity: config.identity.clone(),
            runtime_generation,
            lifecycle: ProviderLifecycle::Unknown,
            activity: super::ProviderActivity::Unknown,
            connection: ProviderConnection::Disconnected,
            durability: ProviderDurability::Unknown,
            health: None,
            source: None,
            last_operation: None,
            retained_operations: std::collections::BTreeMap::new(),
            revision: 1,
            dropped_updates: 0,
        });
        let shared = Arc::new(SharedState {
            fence: Mutex::new(()),
            snapshot: Mutex::new(snapshot),
            updates: updates_tx,
            update_receiver: Mutex::new(updates_rx),
            next_ticket: AtomicU64::new(1),
            runtime_generation: AtomicU64::new(runtime_generation),
            detached: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            last_external_resize: Mutex::new(None),
            resize_admission: Mutex::new(()),
        });
        let worker_shared = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("herdr-coven-provider".to_owned())
            .spawn(move || {
                let _worker_slot = worker_slot;
                worker_loop(command_rx, worker_shared, executor)
            })
            .map_err(ProviderStartError::Worker)?;

        Ok((ProviderRuntimeHandle { command_tx, shared }, worker))
    }
}

pub(crate) struct ProviderRuntimeHandle {
    command_tx: SyncSender<CommandEnvelope>,
    shared: Arc<SharedState>,
}

impl Clone for ProviderRuntimeHandle {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl ProviderRuntimeHandle {
    pub(crate) fn submit(&self, command: ProviderCommand) -> Result<OperationTicket, SubmitError> {
        command.validate_for_submission()?;
        let _fence = self.fence_guard();
        if self.shared.stopped.load(Ordering::Acquire)
            || self.shared.detached.load(Ordering::Acquire)
        {
            return Err(SubmitError::Disconnected);
        }
        let runtime_generation = self.shared.runtime_generation.load(Ordering::Acquire);
        if command.binding().is_some_and(|binding| {
            binding.runtime_generation != runtime_generation
                || !binding.matches_identity(&self.snapshot().identity)
        }) {
            return Err(SubmitError::IdentityMismatch);
        }
        let ticket = self.allocate_ticket()?;
        let binding = command.binding().cloned();
        let kind = command.kind();
        // Publish before enqueueing so a fast worker cannot publish a terminal
        // result before the corresponding pending state reaches the cache.
        self.publish_locked(
            Some(ticket),
            binding.clone(),
            kind,
            ProviderOperationResult::Pending { kind },
        );
        match self.command_tx.try_send(CommandEnvelope {
            ticket,
            runtime_generation,
            command,
        }) {
            Ok(()) => Ok(ticket),
            Err(TrySendError::Full(_)) => {
                self.publish_locked(
                    Some(ticket),
                    binding,
                    kind,
                    ProviderOperationResult::Failure(ProviderFailure::new(
                        ProviderFailureKind::Busy,
                        "Coven provider command queue is full",
                    )),
                );
                Err(SubmitError::Busy)
            }
            Err(TrySendError::Disconnected(_)) => {
                self.publish_locked(
                    Some(ticket),
                    binding,
                    kind,
                    ProviderOperationResult::Failure(ProviderFailure::new(
                        ProviderFailureKind::Disconnected,
                        "Coven provider worker is unavailable",
                    )),
                );
                Err(SubmitError::Disconnected)
            }
        }
    }

    /// Submit a remote resize only when geometry changed. Rendering can call
    /// this once per frame; the shared cache keeps a slow provider queue from
    /// filling with duplicate dimensions.
    pub(crate) fn submit_external_resize(
        &self,
        binding: super::ProviderBinding,
        rows: u16,
        cols: u16,
        pixel_width: u16,
        pixel_height: u16,
    ) -> Result<bool, SubmitError> {
        let geometry = (rows, cols, pixel_width, pixel_height);
        // Serialize the cache reservation with other resize callers. Reserve
        // before queue admission so a fast worker failure (or an inline queue
        // rejection) cannot complete before the deduplication state exists.
        // The reservation is released on admission failure; worker failures
        // clear it from the normal terminal-control failure publication path.
        let _admission = self
            .shared
            .resize_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        {
            let mut cached = self
                .shared
                .last_external_resize
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if *cached == Some(geometry) {
                return Ok(false);
            }
            *cached = Some(geometry);
        }
        let result = self.submit(ProviderCommand::TerminalResize {
            binding,
            rows,
            cols,
            pixel_width,
            pixel_height,
        });
        if let Err(error) = result {
            let mut cached = self
                .shared
                .last_external_resize
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if *cached == Some(geometry) {
                *cached = None;
            }
            return Err(error);
        }
        Ok(true)
    }

    fn fence_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.shared
            .fence
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn allocate_ticket(&self) -> Result<OperationTicket, SubmitError> {
        self.shared
            .next_ticket
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current != 0 && current != u64::MAX).then_some(current + 1)
            })
            .map(OperationTicket)
            .map_err(|_| SubmitError::Busy)
    }

    pub(crate) fn snapshot(&self) -> Arc<ProviderSnapshot> {
        self.shared
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Drain at most `limit` updates without waiting.  If the bounded update
    /// channel overflowed, the latest immutable snapshot remains available via
    /// `snapshot`; `dropped_updates` records that coalescing occurred.
    pub(crate) fn try_drain_updates(&self, limit: usize) -> Vec<ProviderUpdate> {
        let limit = limit.min(super::MAX_PROVIDER_QUEUE_CAPACITY);
        if limit == 0 {
            return Vec::new();
        }
        let receiver = self
            .shared
            .update_receiver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut updates = Vec::with_capacity(limit);
        for _ in 0..limit {
            match receiver.try_recv() {
                Ok(update) => updates.push(update),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        updates
    }

    pub(crate) fn runtime_generation(&self) -> u64 {
        self.shared.runtime_generation.load(Ordering::Acquire)
    }

    pub(crate) fn binding(
        &self,
        session_id: coven_client::execution::ExecutionSessionId,
        generation: u64,
    ) -> Result<super::ManagedBinding, super::ProviderBindingError> {
        let snapshot = self.snapshot();
        super::ManagedBinding::from_identity(
            &snapshot.identity,
            session_id,
            generation,
            snapshot.runtime_generation,
        )
    }

    #[cfg(test)]
    pub(crate) fn bump_generation(&self) -> Result<u64, ProviderGenerationError> {
        let _fence = self.fence_guard();
        let next = allocate_generation(&NEXT_RUNTIME_GENERATION)?;
        self.shared
            .runtime_generation
            .store(next, Ordering::Release);
        self.mutate_snapshot(|snapshot| {
            snapshot.runtime_generation = next;
            snapshot.connection = ProviderConnection::Unknown;
            snapshot.lifecycle = ProviderLifecycle::Unknown;
            snapshot.health = None;
            snapshot.source = None;
            snapshot.last_operation = None;
            snapshot.retained_operations.clear();
        });
        Ok(next)
    }

    /// Detach only this Herdr consumer. No Coven request is sent for queued
    /// commands that have not crossed the worker's dispatch fence; an already
    /// admitted request may finish, but its result is discarded.
    pub(crate) fn detach(&self) -> Arc<ProviderSnapshot> {
        let _fence = self.fence_guard();
        if !self.shared.stopped.load(Ordering::Acquire) {
            self.shared.detached.store(true, Ordering::Release);
            self.mutate_snapshot(|snapshot| {
                snapshot.connection = ProviderConnection::Disconnected;
                snapshot.last_operation = None;
                snapshot.retained_operations.clear();
            });
            self.publish_locked(
                None,
                None,
                super::ProviderOperationKind::Health,
                ProviderOperationResult::Detached,
            );
        }
        self.snapshot()
    }

    /// Stop only the local actor.  This never invokes Coven's stop operation.
    pub(crate) fn shutdown(&self) {
        let _fence = self.fence_guard();
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.detached.store(true, Ordering::Release);
        self.mutate_snapshot(|snapshot| {
            snapshot.connection = ProviderConnection::Disconnected;
            snapshot.last_operation = None;
            snapshot.retained_operations.clear();
        });
        self.publish_locked(
            None,
            None,
            super::ProviderOperationKind::Health,
            ProviderOperationResult::Shutdown,
        );
    }

    fn mutate_snapshot(&self, mutate: impl FnOnce(&mut ProviderSnapshot)) {
        let mut guard = self
            .shared
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut current = guard.as_ref().clone();
        mutate(&mut current);
        current.revision = current.revision.saturating_add(1);
        current.runtime_generation = self.runtime_generation();
        *guard = Arc::new(current);
    }

    /// Publish while the shared fence is held. Keeping the state transition
    /// and its update enqueue in one critical section prevents a generation
    /// change from being hidden by a result stamped with the new generation.
    fn publish_locked(
        &self,
        ticket: Option<OperationTicket>,
        binding: Option<ProviderBinding>,
        kind: super::ProviderOperationKind,
        result: ProviderOperationResult,
    ) {
        let snapshot = self.update_snapshot(ticket, kind, &result);
        let update = ProviderUpdate {
            ticket,
            binding,
            kind,
            runtime_generation: self.runtime_generation(),
            result,
            snapshot,
        };
        self.send_update_locked(update);
    }

    /// Complete an operation that was admitted under an older fence after a
    /// local detach, shutdown, or generation change. The stale update is
    /// stamped with the envelope generation and carries the current snapshot,
    /// but it never applies the old result to current state.
    fn publish_stale_locked(
        &self,
        ticket: OperationTicket,
        binding: Option<ProviderBinding>,
        envelope_generation: u64,
        kind: super::ProviderOperationKind,
        failure: ProviderFailure,
    ) {
        let update = ProviderUpdate {
            ticket: Some(ticket),
            binding,
            kind,
            runtime_generation: envelope_generation,
            result: ProviderOperationResult::Failure(failure),
            snapshot: self.snapshot(),
        };
        self.send_update_locked(update);
    }

    /// The caller must hold `SharedState::fence` while invoking this helper.
    fn send_update_locked(&self, update: ProviderUpdate) {
        tracing::trace!(
            operation_ticket = update.ticket.map(OperationTicket::raw),
            operation = ?update.kind,
            "Coven provider operation update"
        );
        if self.shared.updates.try_send(update).is_err() {
            let mut guard = self
                .shared
                .snapshot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut current = guard.as_ref().clone();
            current.dropped_updates = current.dropped_updates.saturating_add(1);
            current.revision = current.revision.saturating_add(1);
            *guard = Arc::new(current);
        }
    }

    fn update_snapshot(
        &self,
        ticket: Option<OperationTicket>,
        kind: super::ProviderOperationKind,
        result: &ProviderOperationResult,
    ) -> Arc<ProviderSnapshot> {
        let mut guard = self
            .shared
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut current = guard.as_ref().clone();
        current.revision = current.revision.saturating_add(1);
        current.runtime_generation = self.runtime_generation();
        match result {
            ProviderOperationResult::Pending { .. } => {
                if current.connection != ProviderConnection::Connected {
                    current.connection = ProviderConnection::Connecting;
                }
            }
            ProviderOperationResult::Health(health) => {
                current.connection = ProviderConnection::Connected;
                current.health = Some(health.clone());
            }
            ProviderOperationResult::Source(source) => {
                current.connection = ProviderConnection::Connected;
                current.durability = ProviderDurability::Durable;
                current.source = Some(source.clone());
                if let coven_client::source::SourceResult::Snapshot { projection, .. } =
                    &source.result
                {
                    current.lifecycle = map_lifecycle(projection.lifecycle);
                    current.activity = map_activity(projection.lifecycle);
                } else if matches!(
                    source.result,
                    coven_client::source::SourceResult::Reset { .. }
                ) {
                    current.lifecycle = ProviderLifecycle::Unknown;
                    current.activity = super::ProviderActivity::Unknown;
                }
            }
            ProviderOperationResult::Transcript(reply) => {
                current.connection = ProviderConnection::Connected;
                current.durability = ProviderDurability::Durable;
                current.lifecycle = map_lifecycle(reply.projection.lifecycle);
                current.activity = map_activity(reply.projection.lifecycle);
                if matches!(
                    reply.result,
                    coven_client::transcript::TranscriptResult::Reset { .. }
                ) {
                    current.lifecycle = ProviderLifecycle::Unknown;
                    current.activity = super::ProviderActivity::Unknown;
                }
            }
            ProviderOperationResult::CheckpointAttached(_) => {
                current.connection = ProviderConnection::Connected;
                current.durability = ProviderDurability::Ephemeral;
            }
            ProviderOperationResult::Checkpoint(message) => {
                current.connection = ProviderConnection::Connected;
                current.durability = ProviderDurability::Ephemeral;
                current.revision = current.revision.max(message.revision);
                // A terminal Close ends observation, not the execution truth.
                // Lifecycle remains provider-owned and is read separately.
            }
            ProviderOperationResult::Execution(outcome)
            | ProviderOperationResult::LookupExecution(outcome) => {
                current.connection = ProviderConnection::Connected;
                current.durability = ProviderDurability::Durable;
                if let Some(status) = outcome.session_status.as_deref() {
                    current.lifecycle = map_lifecycle_name(status);
                    current.activity = map_activity_name(status);
                }
            }
            ProviderOperationResult::TerminalControl(_) => {
                current.connection = ProviderConnection::Connected;
            }
            ProviderOperationResult::Failure(failure) => {
                if kind == super::ProviderOperationKind::TerminalControl {
                    // A rejected or failed resize must not poison the
                    // geometry de-duplication cache. The next render may need
                    // to retry after a lease/daemon/attachment transition.
                    *self
                        .shared
                        .last_external_resize
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                }
                current.connection = match failure.kind {
                    ProviderFailureKind::Disconnected | ProviderFailureKind::IdentityMismatch => {
                        ProviderConnection::Disconnected
                    }
                    ProviderFailureKind::Unsupported => ProviderConnection::Unknown,
                    ProviderFailureKind::Busy
                    | ProviderFailureKind::Rejected
                    | ProviderFailureKind::Failed => current.connection,
                };
            }
            ProviderOperationResult::Detached | ProviderOperationResult::Shutdown => {
                current.connection = ProviderConnection::Disconnected;
                current.activity = ProviderActivity::Unknown;
            }
        }
        if let Some(ticket) = ticket {
            let operation = ProviderOperation {
                ticket,
                kind,
                result: result.clone(),
            };
            current.last_operation = Some(operation.clone());
            // Retain settled results by ticket, oldest first, so that a
            // consumer whose ticket is no longer the latest can still
            // reconcile it. Pending entries are harmless and age out.
            current.retained_operations.insert(ticket.raw(), operation);
            while current.retained_operations.len() > super::RETAINED_OPERATION_RESULTS {
                let oldest = current
                    .retained_operations
                    .keys()
                    .next()
                    .copied()
                    .expect("retention map is non-empty here");
                current.retained_operations.remove(&oldest);
            }
        } else if !matches!(result, ProviderOperationResult::Pending { .. }) {
            current.last_operation = None;
        }
        let snapshot = Arc::new(current);
        *guard = Arc::clone(&snapshot);
        snapshot
    }
}

fn worker_loop<E>(command_rx: Receiver<CommandEnvelope>, shared: Arc<SharedState>, mut executor: E)
where
    E: FnMut(&ProviderCommand, u64) -> ProviderOperationResult,
{
    // The worker never submits commands, so it only needs the handle's
    // publication helpers.  The sender is intentionally disconnected.
    let handle = ProviderRuntimeHandle {
        command_tx: {
            let (sender, _receiver) = mpsc::sync_channel(1);
            sender
        },
        shared: Arc::clone(&shared),
    };
    while !shared.stopped.load(Ordering::Acquire) {
        let envelope = match command_rx.recv_timeout(WORKER_POLL_INTERVAL) {
            Ok(envelope) => envelope,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let binding = envelope.command.binding().cloned();
        let kind = envelope.command.kind();
        // This short critical section is the pre-dispatch fence. Once it
        // passes, the request is considered in flight and the mutex is
        // released before potentially blocking Coven I/O. A concurrent
        // generation change will discard the result at the final fence.
        let runtime_generation = {
            let _fence = handle.fence_guard();
            let current_generation = shared.runtime_generation.load(Ordering::Acquire);
            if shared.stopped.load(Ordering::Acquire) || shared.detached.load(Ordering::Acquire) {
                handle.publish_stale_locked(
                    envelope.ticket,
                    binding.clone(),
                    envelope.runtime_generation,
                    kind,
                    ProviderFailure::new(
                        ProviderFailureKind::Disconnected,
                        "provider consumer is detached",
                    ),
                );
                None
            } else if envelope.runtime_generation != current_generation
                || binding
                    .as_ref()
                    .is_some_and(|binding| binding.runtime_generation != current_generation)
            {
                handle.publish_stale_locked(
                    envelope.ticket,
                    binding.clone(),
                    envelope.runtime_generation,
                    kind,
                    ProviderFailure::new(
                        ProviderFailureKind::IdentityMismatch,
                        "provider runtime generation no longer matches the command",
                    ),
                );
                None
            } else {
                Some(current_generation)
            }
        };
        let Some(runtime_generation) = runtime_generation else {
            continue;
        };
        let result = executor(&envelope.command, runtime_generation);
        let _fence = handle.fence_guard();
        let current_generation = shared.runtime_generation.load(Ordering::Acquire);
        if shared.stopped.load(Ordering::Acquire) || shared.detached.load(Ordering::Acquire) {
            handle.publish_stale_locked(
                envelope.ticket,
                binding,
                envelope.runtime_generation,
                kind,
                ProviderFailure::new(
                    ProviderFailureKind::Disconnected,
                    "provider result arrived after the consumer detached",
                ),
            );
        } else if current_generation != envelope.runtime_generation {
            handle.publish_stale_locked(
                envelope.ticket,
                binding,
                envelope.runtime_generation,
                kind,
                ProviderFailure::new(
                    ProviderFailureKind::IdentityMismatch,
                    "Coven provider result arrived after its runtime fence changed",
                ),
            );
        } else {
            handle.publish_locked(Some(envelope.ticket), binding, kind, result);
        }
    }
}

fn map_lifecycle(lifecycle: coven_client::source::SourceLifecycle) -> ProviderLifecycle {
    match lifecycle {
        coven_client::source::SourceLifecycle::Created => ProviderLifecycle::Created,
        coven_client::source::SourceLifecycle::Starting => ProviderLifecycle::Starting,
        coven_client::source::SourceLifecycle::Running => ProviderLifecycle::Running,
        coven_client::source::SourceLifecycle::Idle => ProviderLifecycle::Idle,
        coven_client::source::SourceLifecycle::Completed => ProviderLifecycle::Completed,
        coven_client::source::SourceLifecycle::Failed => ProviderLifecycle::Failed,
        coven_client::source::SourceLifecycle::Cancelled => ProviderLifecycle::Cancelled,
        coven_client::source::SourceLifecycle::Killed => ProviderLifecycle::Killed,
        coven_client::source::SourceLifecycle::Orphaned => ProviderLifecycle::Orphaned,
    }
}

fn map_lifecycle_name(name: &str) -> ProviderLifecycle {
    match name {
        "created" => ProviderLifecycle::Created,
        "starting" => ProviderLifecycle::Starting,
        "running" => ProviderLifecycle::Running,
        "idle" => ProviderLifecycle::Idle,
        "completed" => ProviderLifecycle::Completed,
        "failed" => ProviderLifecycle::Failed,
        "cancelled" => ProviderLifecycle::Cancelled,
        "killed" => ProviderLifecycle::Killed,
        "orphaned" => ProviderLifecycle::Orphaned,
        _ => ProviderLifecycle::Unknown,
    }
}

fn map_activity(lifecycle: coven_client::source::SourceLifecycle) -> super::ProviderActivity {
    match lifecycle {
        coven_client::source::SourceLifecycle::Starting
        | coven_client::source::SourceLifecycle::Running => super::ProviderActivity::Working,
        coven_client::source::SourceLifecycle::Created
        | coven_client::source::SourceLifecycle::Idle
        | coven_client::source::SourceLifecycle::Completed
        | coven_client::source::SourceLifecycle::Failed
        | coven_client::source::SourceLifecycle::Cancelled
        | coven_client::source::SourceLifecycle::Killed
        | coven_client::source::SourceLifecycle::Orphaned => super::ProviderActivity::Quiet,
    }
}

fn map_activity_name(name: &str) -> super::ProviderActivity {
    match name {
        "starting" | "running" => super::ProviderActivity::Working,
        "created" | "idle" | "completed" | "failed" | "cancelled" | "killed" | "orphaned" => {
            super::ProviderActivity::Quiet
        }
        _ => super::ProviderActivity::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, path::PathBuf, sync::mpsc, thread};

    use coven_client::{
        execution::{
            AuthorityId, ExecutionAuthority, ExecutionScope, ExecutionSessionId, ProfileId,
            ProjectId,
        },
        source::{
            SourceCursor, SourceHealth, SourceId, SourceIdentity, SourceLifecycle,
            SourceProjection, SourceReply, SourceResult, WriterState,
        },
    };

    use super::*;
    use crate::runtime_provider::{ProviderHealth, ProviderOperationKind};

    fn config() -> super::super::ProviderConfig {
        let identity = super::super::ProviderIdentity::new(
            super::super::ProviderId::new("actor-fixture").unwrap(),
            SourceId::new("host").unwrap(),
            ExecutionScope {
                project_id: ProjectId::new("project").unwrap(),
                profile_id: ProfileId::new("profile").unwrap(),
                policy_generation: 1,
            },
            ExecutionAuthority::new(AuthorityId::new("authority").unwrap(), 1).unwrap(),
        )
        .unwrap();
        super::super::ProviderConfig::new(
            identity,
            PathBuf::from("/definitely/missing/coven-home"),
            NonZeroUsize::new(2).unwrap(),
        )
        .unwrap()
    }

    fn observed_source() -> SourceReply {
        let scope = ExecutionScope {
            project_id: ProjectId::new("project").unwrap(),
            profile_id: ProfileId::new("profile").unwrap(),
            policy_generation: 1,
        };
        let ledger_id = SourceId::new("ledger").unwrap();
        SourceReply {
            contract: coven_client::source::CONTRACT.to_owned(),
            source: SourceIdentity {
                host_id: SourceId::new("host").unwrap(),
                profile_id: scope.profile_id.clone(),
                ledger_id: ledger_id.clone(),
                epoch: 1,
            },
            scope,
            session_id: ExecutionSessionId::new("session").unwrap(),
            generation: 1,
            health: SourceHealth {
                daemon_live: true,
                writer_state: WriterState::Healthy,
                writer_queued_bytes: None,
                writer_dropped_output_bytes: None,
            },
            result: SourceResult::Snapshot {
                cursor: SourceCursor {
                    ledger_id,
                    epoch: 1,
                    after_seq: 0,
                    revision: 1,
                },
                projection: SourceProjection {
                    lifecycle: SourceLifecycle::Running,
                    exit_code: None,
                    archived: false,
                    dropped_output_bytes: None,
                },
            },
        }
    }

    #[test]
    fn stale_publication_preserves_current_snapshot_and_stamps_envelope_generation() {
        let handle = ProviderRuntime::start(config()).unwrap();
        let envelope_generation = handle.runtime_generation();
        handle.bump_generation().unwrap();
        let before = handle.snapshot();
        {
            let _fence = handle.fence_guard();
            handle.publish_stale_locked(
                OperationTicket(7),
                None,
                envelope_generation,
                ProviderOperationKind::Health,
                ProviderFailure::new(ProviderFailureKind::IdentityMismatch, "stale result"),
            );
        }
        let after = handle.snapshot();
        assert!(after.runtime_generation > envelope_generation);
        assert_eq!(after.connection, before.connection);
        assert_eq!(after.health, before.health);
        assert_eq!(after.source, before.source);
        assert_eq!(after.last_operation, before.last_operation);
        let updates = handle.try_drain_updates(1);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].kind, ProviderOperationKind::Health);
        assert_eq!(updates[0].runtime_generation, envelope_generation);
        assert_eq!(
            updates[0].snapshot.runtime_generation,
            after.runtime_generation
        );
        assert!(matches!(
            updates[0].result,
            ProviderOperationResult::Failure(ProviderFailure {
                kind: ProviderFailureKind::IdentityMismatch,
                ..
            })
        ));
        handle.shutdown();
    }

    #[test]
    fn generation_bump_discards_observations_from_the_old_runtime() {
        let handle = ProviderRuntime::start(config()).unwrap();
        let authority = handle.snapshot().identity.authority.clone();
        {
            let _fence = handle.fence_guard();
            handle.mutate_snapshot(|snapshot| {
                snapshot.lifecycle = ProviderLifecycle::Running;
                snapshot.health = Some(ProviderHealth {
                    ok: true,
                    api_version: "coven.daemon.v1".to_owned(),
                    coven_version: "fixture".to_owned(),
                    sessions: true,
                    events: true,
                    event_cursor: None,
                    structured_errors: true,
                    execution_request_contracts: Vec::new(),
                    execution_request_operations: Vec::new(),
                    execution_source_contracts: Vec::new(),
                    authority: Some(authority.clone()),
                });
                snapshot.source = Some(observed_source());
            });
        }
        let before = handle.snapshot();
        assert!(before.health.is_some());
        assert!(before.source.is_some());
        assert_eq!(before.lifecycle, ProviderLifecycle::Running);

        handle.bump_generation().unwrap();
        let after = handle.snapshot();
        assert_eq!(after.lifecycle, ProviderLifecycle::Unknown);
        assert!(after.health.is_none());
        assert!(after.source.is_none());
        handle.shutdown();
    }

    #[test]
    fn generation_allocator_refuses_wraparound() {
        let counter = AtomicU64::new(i64::MAX as u64);
        assert_eq!(allocate_generation(&counter).unwrap(), i64::MAX as u64);
        assert_eq!(
            allocate_generation(&counter),
            Err(ProviderGenerationError::Exhausted)
        );
        assert_eq!(counter.load(Ordering::Acquire), i64::MAX as u64 + 1);
    }

    #[test]
    fn replacement_actor_rejects_the_previous_actor_binding() {
        let old = ProviderRuntime::start(config()).unwrap();
        let binding = old
            .binding(ExecutionSessionId::new("session").unwrap(), 1)
            .unwrap();
        old.shutdown();
        let replacement = ProviderRuntime::start(config()).unwrap();
        assert_ne!(old.runtime_generation(), replacement.runtime_generation());
        assert_eq!(
            replacement.submit(ProviderCommand::ReadSource {
                binding,
                cursor: None,
                limit: 1
            }),
            Err(SubmitError::IdentityMismatch)
        );
        replacement.shutdown();
    }

    #[test]
    fn settled_tickets_survive_a_destructive_shared_update_drain() {
        // One runtime backs both the attachment poll and permission-bearing
        // execution operations. The app layer drains the shared bounded update
        // channel from more than one place, so a ticket that is no longer the
        // latest must still reconcile from the retained snapshot instead of
        // freezing its poll forever.
        let (handle, worker) = ProviderRuntime::start_worker(config(), |command, _| match command {
            ProviderCommand::Health => ProviderOperationResult::Health(ProviderHealth {
                ok: true,
                api_version: "coven.daemon.v1".to_owned(),
                coven_version: "fixture".to_owned(),
                sessions: true,
                events: true,
                event_cursor: None,
                structured_errors: true,
                execution_request_contracts: Vec::new(),
                execution_request_operations: Vec::new(),
                execution_source_contracts: Vec::new(),
                authority: None,
            }),
            _ => ProviderOperationResult::Source(observed_source()),
        })
        .unwrap();
        let first = handle.submit(ProviderCommand::Health).unwrap();
        let binding = handle
            .binding(ExecutionSessionId::new("session").unwrap(), 1)
            .unwrap();
        let second = handle
            .submit(ProviderCommand::ReadSource {
                binding,
                cursor: None,
                limit: 1,
            })
            .unwrap();
        // Wait until both commands have settled, then let a competing consumer
        // drain the entire shared channel before the ticket owner runs; the
        // results must remain recoverable from the snapshot.
        let deadline = std::time::Instant::now() + TEST_GUARD;
        loop {
            let settled = handle
                .snapshot()
                .operation_result(second)
                .is_some_and(|operation| {
                    !matches!(operation.result, ProviderOperationResult::Pending { .. })
                });
            if settled {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "fixture worker did not publish its results"
            );
            thread::sleep(Duration::from_millis(5));
        }
        while !handle.try_drain_updates(64).is_empty() {}
        let snapshot = handle.snapshot();
        let first_result = snapshot
            .operation_result(first)
            .expect("superseded ticket was dropped");
        assert!(matches!(
            first_result.result,
            ProviderOperationResult::Health(_)
        ));
        let second_result = snapshot
            .operation_result(second)
            .expect("latest ticket was dropped");
        assert!(matches!(
            second_result.result,
            ProviderOperationResult::Source(_)
        ));
        assert!(snapshot.retained_operations.len() >= 2);
        handle.shutdown();
        worker.join().unwrap();
    }

    #[test]
    fn generation_transition_waits_for_publication_fence() {
        let handle = ProviderRuntime::start(config()).unwrap();
        let initial_generation = handle.runtime_generation();
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let worker = handle.clone();
        let join = thread::spawn(move || {
            started_tx.send(()).unwrap();
            worker.bump_generation().unwrap()
        });
        let _fence = handle.fence_guard();
        // The worker has announced its attempt, but cannot cross the fence
        // until this test drops the guard below.
        started_rx.recv().unwrap();
        assert_eq!(handle.runtime_generation(), initial_generation);
        drop(_fence);
        assert!(join.join().unwrap() > initial_generation);
        handle.shutdown();
    }
    // These are hang guards for a deliberately blocked worker, not latency
    // assertions. Ordering is established by channels and worker completion.
    const TEST_GUARD: Duration = Duration::from_secs(30);

    fn recv_update(handle: &ProviderRuntimeHandle) -> ProviderUpdate {
        handle
            .shared
            .update_receiver
            .lock()
            .unwrap()
            .recv_timeout(TEST_GUARD)
            .expect("provider worker did not publish an update")
    }

    fn delayed_worker() -> (
        ProviderRuntimeHandle,
        thread::JoinHandle<()>,
        Receiver<()>,
        SyncSender<()>,
        Arc<AtomicU64>,
    ) {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let calls = Arc::new(AtomicU64::new(0));
        let worker_calls = Arc::clone(&calls);
        let (handle, worker) = ProviderRuntime::start_worker(config(), move |_, _| {
            if worker_calls.fetch_add(1, Ordering::AcqRel) == 0 {
                entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(TEST_GUARD)
                    .expect("test did not release the provider executor");
            }
            ProviderOperationResult::Source(observed_source())
        })
        .unwrap();
        (handle, worker, entered_rx, release_tx, calls)
    }

    #[test]
    fn generation_change_discards_a_result_from_the_actual_blocked_worker() {
        let (handle, worker, entered, release, calls) = delayed_worker();
        let generation = handle.runtime_generation();
        let ticket = handle.submit(ProviderCommand::Health).unwrap();
        entered.recv_timeout(TEST_GUARD).unwrap();
        assert!(matches!(
            recv_update(&handle).result,
            ProviderOperationResult::Pending { .. }
        ));

        // These local operations must finish while the executor is still held.
        assert_eq!(handle.snapshot().runtime_generation, generation);
        assert!(handle.bump_generation().unwrap() > generation);
        let current = handle.snapshot();
        release.send(()).unwrap();
        let update = recv_update(&handle);
        assert_eq!(update.ticket, Some(ticket));
        assert_eq!(update.runtime_generation, generation);
        assert!(matches!(
            update.result,
            ProviderOperationResult::Failure(ProviderFailure {
                kind: ProviderFailureKind::IdentityMismatch,
                ..
            })
        ));
        assert!(Arc::ptr_eq(&current, &handle.snapshot()));
        assert!(current.source.is_none());
        assert_eq!(current.lifecycle, ProviderLifecycle::Unknown);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        handle.shutdown();
        drop(handle);
        worker.join().unwrap();
    }

    #[test]
    fn detach_discards_a_result_from_the_actual_blocked_worker() {
        let (handle, worker, entered, release, calls) = delayed_worker();
        let ticket = handle.submit(ProviderCommand::Health).unwrap();
        entered.recv_timeout(TEST_GUARD).unwrap();
        let _pending = recv_update(&handle);
        let current = handle.detach();
        assert!(matches!(
            recv_update(&handle).result,
            ProviderOperationResult::Detached
        ));
        assert_eq!(
            handle.submit(ProviderCommand::Health),
            Err(SubmitError::Disconnected)
        );
        release.send(()).unwrap();
        let update = recv_update(&handle);
        assert_eq!(update.ticket, Some(ticket));
        assert!(matches!(
            update.result,
            ProviderOperationResult::Failure(ProviderFailure {
                kind: ProviderFailureKind::Disconnected,
                ..
            })
        ));
        assert!(Arc::ptr_eq(&current, &handle.snapshot()));
        assert_eq!(current.connection, ProviderConnection::Disconnected);
        assert!(current.last_operation.is_none());
        assert!(current.source.is_none());
        assert_eq!(calls.load(Ordering::Acquire), 1);
        handle.shutdown();
        drop(handle);
        worker.join().unwrap();
    }

    #[test]
    fn retiring_worker_holds_its_capacity_slot_until_thread_exit() {
        // Reserve the other slots synthetically to avoid perturbing concurrent
        // tests using the real process-wide counter. The last slot belongs to
        // an actual worker executing the production loop below.
        static TEST_WORKERS: AtomicUsize = AtomicUsize::new(MAX_PROVIDER_WORKERS - 1);
        let slot = WorkerSlot::acquire(&TEST_WORKERS).unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let (handle, worker) = ProviderRuntime::start_worker_with_slot(
            config(),
            move |_, _| {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(TEST_GUARD).unwrap();
                ProviderOperationResult::Failure(ProviderFailure::new(
                    ProviderFailureKind::Disconnected,
                    "fixture",
                ))
            },
            slot,
        )
        .unwrap();
        handle.submit(ProviderCommand::Health).unwrap();
        entered_rx.recv_timeout(TEST_GUARD).unwrap();
        handle.shutdown();
        drop(handle);
        assert!(matches!(
            WorkerSlot::acquire(&TEST_WORKERS),
            Err(ProviderStartError::WorkerLimitReached)
        ));
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(
            TEST_WORKERS.load(Ordering::Acquire),
            MAX_PROVIDER_WORKERS - 1
        );
        let reusable = WorkerSlot::acquire(&TEST_WORKERS).unwrap();
        assert_eq!(TEST_WORKERS.load(Ordering::Acquire), MAX_PROVIDER_WORKERS);
        drop(reusable);
        assert_eq!(
            TEST_WORKERS.load(Ordering::Acquire),
            MAX_PROVIDER_WORKERS - 1
        );
    }

    #[test]
    fn shutdown_of_blocked_worker_does_not_dispatch_queued_commands() {
        let (handle, worker, entered, release, calls) = delayed_worker();
        let first_ticket = handle.submit(ProviderCommand::Health).unwrap();
        entered.recv_timeout(TEST_GUARD).unwrap();
        let _pending = recv_update(&handle);
        // The in-flight command leaves exactly the configured two queue slots.
        handle.submit(ProviderCommand::Health).unwrap();
        let _pending = recv_update(&handle);
        handle.submit(ProviderCommand::Health).unwrap();
        let _pending = recv_update(&handle);
        assert_eq!(
            handle.submit(ProviderCommand::Health),
            Err(SubmitError::Busy)
        );
        let _ = handle.try_drain_updates(super::super::MAX_PROVIDER_QUEUE_CAPACITY);
        handle.shutdown();
        assert_eq!(
            handle.submit(ProviderCommand::Health),
            Err(SubmitError::Disconnected)
        );
        assert!(matches!(
            recv_update(&handle).result,
            ProviderOperationResult::Shutdown
        ));
        let current = handle.snapshot();
        release.send(()).unwrap();
        let update = recv_update(&handle);
        assert_eq!(update.ticket, Some(first_ticket));
        assert!(matches!(
            update.result,
            ProviderOperationResult::Failure(ProviderFailure {
                kind: ProviderFailureKind::Disconnected,
                ..
            })
        ));
        assert!(Arc::ptr_eq(&current, &handle.snapshot()));
        assert_eq!(current.connection, ProviderConnection::Disconnected);
        assert!(current.last_operation.is_none());
        assert!(current.source.is_none());
        drop(handle);
        worker.join().unwrap();
        assert_eq!(calls.load(Ordering::Acquire), 1);
    }
    #[test]
    fn generation_change_prevents_dispatch_of_queued_commands() {
        let (handle, worker, entered, release, calls) = delayed_worker();
        handle.submit(ProviderCommand::Health).unwrap();
        entered.recv_timeout(TEST_GUARD).unwrap();
        let _pending = recv_update(&handle);
        handle.submit(ProviderCommand::Health).unwrap();
        let _pending = recv_update(&handle);
        handle.bump_generation().unwrap();
        release.send(()).unwrap();
        // Dropping the last sender makes the worker exit after it examines all
        // queued envelopes; unlike shutdown this exercises the generation fence.
        drop(handle);
        worker.join().unwrap();
        assert_eq!(calls.load(Ordering::Acquire), 1);
    }
}
