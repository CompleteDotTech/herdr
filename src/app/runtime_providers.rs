//! Server-owned provider workers. Only initialization reads owner settings;
//! the event loop exchanges bounded commands and immutable snapshots.
use super::App;
use crate::{
    runtime_provider::{
        OwnerProviderSettings, ProviderCommand, ProviderConfig, ProviderConnection,
        ProviderFailureKind, ProviderObservation, ProviderOperation, ProviderOperationResult,
        ProviderRuntime, ProviderRuntimeHandle, SubmitError,
    },
    terminal::{backend::ExternalBinding, TerminalId},
};
use coven_client::{
    execution::{
        ExecutionHarness, ExecutionIntent, ExecutionMode, ExecutionOutcome, ExecutionRequest,
        ExecutionSessionId, ExecutionTarget, RequestId,
    },
    source::{SourceCursor, SourceResult},
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const READ_INTERVAL: Duration = Duration::from_secs(1);
const MAX_ATTACHMENTS: usize = 64;
const MAX_OPERATIONS: usize = 64;
const OPERATION_RETENTION: Duration = Duration::from_secs(15 * 60);

#[derive(Default)]
pub(crate) struct RuntimeProviders {
    settings: OwnerProviderSettings,
    diagnostic: Option<String>,
    polls: HashMap<TerminalId, PollState>,
    operations: HashMap<OperationKey, OperationState>,
    deadline: Option<Instant>,
}

impl From<RuntimeProviderHarness> for ExecutionHarness {
    fn from(value: RuntimeProviderHarness) -> Self {
        match value {
            RuntimeProviderHarness::Codex => Self::Codex,
            RuntimeProviderHarness::Claude => Self::Claude,
            RuntimeProviderHarness::Copilot => Self::Copilot,
        }
    }
}

impl From<RuntimeProviderMode> for ExecutionMode {
    fn from(value: RuntimeProviderMode) -> Self {
        match value {
            RuntimeProviderMode::Transcript => Self::Transcript,
            RuntimeProviderMode::Terminal => Self::Terminal,
        }
    }
}

fn execution_target_from_api(
    target: RuntimeProviderExecutionTarget,
) -> Result<ExecutionTarget, ()> {
    let target = match target {
        RuntimeProviderExecutionTarget::Launch => ExecutionTarget::Launch,
        RuntimeProviderExecutionTarget::Existing {
            session_id,
            generation,
        } => ExecutionTarget::Existing {
            session_id: ExecutionSessionId::new(session_id).map_err(|_| ())?,
            generation,
        },
        RuntimeProviderExecutionTarget::Continuation {
            previous_session_id,
            previous_generation,
        } => ExecutionTarget::Continuation {
            previous_session_id: ExecutionSessionId::new(previous_session_id).map_err(|_| ())?,
            previous_generation,
        },
    };
    target.validate().map_err(|_| ())?;
    Ok(target)
}

fn execution_target_name(target: &ExecutionTarget) -> &'static str {
    match target {
        ExecutionTarget::Launch => "launch",
        ExecutionTarget::Existing { .. } => "existing",
        ExecutionTarget::Continuation { .. } => "continuation",
    }
}

fn execution_state_name(state: coven_client::execution::RequestState) -> &'static str {
    match state {
        coven_client::execution::RequestState::Admitted => "admitted",
        coven_client::execution::RequestState::Establishing => "establishing",
        coven_client::execution::RequestState::Running => "running",
        coven_client::execution::RequestState::OutcomeKnown => "outcome_known",
        coven_client::execution::RequestState::Unresolved => "unresolved",
    }
}

fn execution_delivery_name(delivery: coven_client::execution::DeliveryState) -> &'static str {
    match delivery {
        coven_client::execution::DeliveryState::NotAttempted => "not_attempted",
        coven_client::execution::DeliveryState::Dispatched => "dispatched",
        coven_client::execution::DeliveryState::Unknown => "unknown",
    }
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(i64::MAX)
}

fn resolve_launch_cwd(config: &ProviderConfig, requested: &str) -> Result<String, ()> {
    if requested.is_empty()
        || requested.len() > 4096
        || requested.contains('\0')
        || requested.chars().any(char::is_control)
    {
        return Err(());
    }
    let root = std::fs::canonicalize(config.project_root()).map_err(|_| ())?;
    let candidate = std::path::Path::new(requested);
    let candidate = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        config.project_root().join(candidate)
    };
    let cwd = std::fs::canonicalize(candidate).map_err(|_| ())?;
    if !cwd.is_dir() || !cwd.starts_with(&root) {
        return Err(());
    }
    cwd.to_str().map(str::to_owned).ok_or(())
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OperationKey {
    provider_id: String,
    request_id: RequestId,
}

struct OperationState {
    provider_id: String,
    request_id: RequestId,
    ticket: crate::runtime_provider::OperationTicket,
    payload_digest: String,
    target: ExecutionTarget,
    binding: Option<crate::runtime_provider::ProviderBinding>,
    terminal_id: Option<TerminalId>,
    runtime: ProviderRuntimeHandle,
    result: Option<ProviderOperationResult>,
    completed_at: Option<Instant>,
}

/// A settled continuation creates a new Coven execution. Move the Herdr
/// presentation binding to that child before the next source poll; otherwise
/// a successful Continue would leave the pane reading the consumed parent.
fn continuation_binding_update(
    operation: &OperationState,
    outcome: &ExecutionOutcome,
) -> Option<(
    TerminalId,
    ExecutionSessionId,
    u64,
    crate::runtime_provider::ManagedBinding,
)> {
    if !matches!(operation.target, ExecutionTarget::Continuation { .. }) {
        return None;
    }
    let terminal_id = operation.terminal_id.clone()?;
    let parent = operation.binding.as_ref()?;
    if outcome.parent_session_id.as_ref() != Some(&parent.session_id)
        || outcome.session_id == parent.session_id
        || outcome.generation <= parent.generation
    {
        return None;
    }
    let mut child = parent.clone();
    child.session_id = outcome.session_id.clone();
    child.generation = outcome.generation;
    child.pinned_source = None;
    Some((
        terminal_id,
        parent.session_id.clone(),
        parent.generation,
        child,
    ))
}
struct PollState {
    health: bool,
    transcript: bool,
    checkpoint: bool,
    checkpoint_attached: bool,
    checkpoint_content_revision: u64,
    cursor: Option<SourceCursor>,
    pending: Option<crate::runtime_provider::OperationTicket>,
    next_read: Instant,
    failures: u32,
    stopped: bool,
}
impl RuntimeProviders {
    pub(crate) fn load(enabled: bool) -> Self {
        if !enabled {
            return Self::default();
        }
        match OwnerProviderSettings::load(&crate::config::config_dir()) {
            Ok(settings) => Self {
                settings,
                ..Self::default()
            },
            Err(error) => Self {
                diagnostic: Some(error.to_string()),
                ..Self::default()
            },
        }
    }
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub(crate) fn remove(&mut self, terminal_id: &TerminalId) {
        self.polls.remove(terminal_id);
        self.refresh_deadline();
    }
    fn refresh_deadline(&mut self) {
        self.deadline = self
            .polls
            .values()
            .filter(|poll| !poll.stopped)
            .map(|poll| {
                if poll.pending.is_some() {
                    Instant::now() + POLL_INTERVAL
                } else {
                    poll.next_read
                }
            })
            .chain(self.operations.values().filter_map(|operation| {
                operation
                    .completed_at
                    .is_none()
                    .then_some(Instant::now() + POLL_INTERVAL)
            }))
            .min();
    }
    fn config(&self, provider: &str) -> Option<&ProviderConfig> {
        self.settings.enabled.then_some(())?;
        self.settings
            .providers
            .iter()
            .find(|config| config.identity.provider_id.as_str() == provider)
    }
}
impl App {
    pub(crate) fn restore_runtime_providers(&mut self) {
        let bindings: Vec<_> = self
            .state
            .terminals
            .values()
            .filter_map(|terminal| terminal.external_binding.clone())
            .collect();
        for binding in bindings {
            if self
                .terminal_runtimes
                .external(&binding.terminal_id)
                .is_some()
            {
                continue;
            }
            let terminal_id = binding.terminal_id.clone();
            if let Err(message) = self.start_runtime_provider(binding) {
                if let Some(terminal) = self.state.terminals.get_mut(&terminal_id) {
                    terminal.external_observation = Some(ProviderObservation {
                        diagnostic: Some(message.into()),
                        ..ProviderObservation::default()
                    });
                }
            }
        }
    }
    fn start_runtime_provider(&mut self, mut binding: ExternalBinding) -> Result<(), &'static str> {
        let saved_checkpoint = self
            .state
            .terminals
            .get(&binding.terminal_id)
            .and_then(|terminal| terminal.external_checkpoint.clone());
        if let Some(checkpoint) = saved_checkpoint.as_ref() {
            checkpoint.validate()?;
        }
        let config = self
            .runtime_providers
            .config(&binding.execution.provider_id)
            .ok_or("provider is disabled or not configured")?
            .clone();
        if !binding.execution.matches_identity(&config.identity) {
            return Err("configured provider identity changed");
        }
        let runtime = ProviderRuntime::start(config).map_err(|error| match error {
            crate::runtime_provider::ProviderStartError::WorkerLimitReached => {
                "provider worker capacity is busy; retry after retiring workers exit"
            }
            _ => "could not start provider worker",
        })?;
        binding.execution = runtime
            .binding(
                binding.execution.session_id.clone(),
                binding.execution.generation,
            )
            .and_then(|fresh| fresh.with_pinned_source(binding.execution.pinned_source.clone()))
            .map_err(|_| "invalid runtime generation")?;
        let terminal_id = binding.terminal_id.clone();
        self.terminal_runtimes
            .attach_external(terminal_id.clone(), runtime)?;
        if let Some(terminal) = self.state.terminals.get_mut(&terminal_id) {
            terminal.external_transcript = Some(
                crate::runtime_provider::transcript::TranscriptProjection::new(
                    binding.execution.clone(),
                ),
            );
            terminal.external_binding = Some(binding);
            terminal.external_observation = Some(ProviderObservation {
                connection: ProviderConnection::Connecting,
                ..ProviderObservation::default()
            });
        }
        self.runtime_providers.polls.insert(
            terminal_id,
            PollState {
                health: true,
                // Negotiate the typed raw stream first. `transcript` remains
                // a deliberate fallback for daemons that do not advertise
                // checkpointV1.
                transcript: false,
                checkpoint: true,
                checkpoint_attached: false,
                checkpoint_content_revision: 0,
                cursor: None,
                pending: None,
                next_read: Instant::now(),
                failures: 0,
                stopped: false,
            },
        );
        self.runtime_providers.refresh_deadline();
        Ok(())
    }

    /// O(attachments) only at a due provider deadline. Native-only servers return
    /// before traversing terminals; no transport or filesystem call occurs here.
    pub(crate) fn poll_runtime_providers(&mut self, now: Instant) -> bool {
        if self
            .runtime_providers
            .deadline
            .is_none_or(|deadline| now < deadline)
        {
            return false;
        }
        let mut changed = self.poll_execution_operations(now);
        let mut persist_pin = false;
        for (terminal_id, poll) in &mut self.runtime_providers.polls {
            if poll.stopped {
                continue;
            }
            let Some(runtime) = self.terminal_runtimes.external(terminal_id).cloned() else {
                poll.stopped = true;
                continue;
            };
            let Some(terminal) = self.state.terminals.get_mut(terminal_id) else {
                poll.stopped = true;
                continue;
            };
            let Some(binding) = terminal.external_binding.as_mut() else {
                poll.stopped = true;
                continue;
            };
            // Drain notifications to keep the channel bounded; the latest
            // snapshot remains authoritative even if notifications were dropped.
            // The immutable snapshot retains a bounded ticket-indexed history
            // of settled results, so a read ticket that is no longer the latest
            // still reconciles. Consume the ticketed update first so observation
            // polling cannot get stuck behind an unrelated control operation;
            // retain the snapshot as the fallback when no matching update is
            // available.
            let updates = runtime.try_drain_updates(64);
            let snapshot = runtime.snapshot();
            let pending_operation = poll.pending.and_then(|ticket| {
                updates
                    .iter()
                    .rfind(|update| {
                        update.ticket == Some(ticket)
                            && !matches!(update.result, ProviderOperationResult::Pending { .. })
                    })
                    .map(|update| ProviderOperation {
                        ticket,
                        kind: update.kind,
                        result: update.result.clone(),
                    })
                    .or_else(|| snapshot.operation_result(ticket))
            });
            if let Some(operation) = pending_operation.as_ref() {
                if !matches!(operation.result, ProviderOperationResult::Pending { .. }) {
                    poll.pending = None;
                    poll.next_read = now + READ_INTERVAL;
                    let mut projection_error = false;
                    let mut diagnostic_override = None;
                    match &operation.result {
                        ProviderOperationResult::CheckpointAttached(attached) => {
                            attached
                                .session
                                .set_theme(crate::terminal::external::external_theme(
                                    &self.state.palette,
                                ));
                            // Keep this field-level update inline. Calling the
                            // `App` helper here would borrow all of `self`
                            // while the poll and terminal entries are already
                            // mutably borrowed by this loop.
                            let attach_result = self.terminal_runtimes.attach_external_session(
                                terminal_id.clone(),
                                std::sync::Arc::clone(&attached.session),
                            );
                            if attach_result.is_ok() {
                                terminal.external_transcript = None;
                                poll.transcript = false;
                                poll.cursor = None;
                                poll.pending = None;
                                poll.checkpoint_attached = true;
                                poll.failures = 0;
                                poll.next_read = now;
                                poll.checkpoint_content_revision =
                                    attached.session.content_revision();
                                terminal.external_transcript = None;
                                terminal.external_observed_at = Some(now);
                                terminal.external_observed_contract =
                                    Some("coven.terminal-checkpoint.v1");
                                self.render_dirty.request_generic();
                                changed = true;
                            } else {
                                projection_error = true;
                                poll.stopped = true;
                                diagnostic_override = Some("Provider checkpoint model could not be attached to the terminal.".to_owned());
                            }
                        }
                        ProviderOperationResult::Checkpoint(update) => {
                            poll.failures = 0;
                            poll.next_read = now + Duration::from_millis(20);
                            terminal.external_observed_at = Some(now);
                            if poll.checkpoint_content_revision != update.content_revision {
                                poll.checkpoint_content_revision = update.content_revision;
                                changed = true;
                            }
                            if let Some(checkpoint) = &update.checkpoint {
                                terminal.external_checkpoint = Some(checkpoint.clone());
                                persist_pin = true;
                            }
                            if update.closed {
                                // Keep the final terminal model visible. Read
                                // authoritative lifecycle metadata after Close;
                                // a stream boundary alone is not a settled exit.
                                poll.checkpoint = false;
                                poll.transcript = false;
                                poll.cursor = None;
                                poll.next_read = now;
                            }
                        }
                        ProviderOperationResult::Health(_) => {
                            poll.health = false;
                            poll.failures = 0;
                            poll.next_read = now;
                            terminal.external_observed_at = Some(now);
                            terminal.external_observed_contract = Some("coven.health.v1");
                            changed = true;
                        }
                        ProviderOperationResult::Transcript(reply) => {
                            let projection =
                                terminal.external_transcript.get_or_insert_with(|| {
                                    crate::runtime_provider::transcript::TranscriptProjection::new(
                                        binding.execution.clone(),
                                    )
                                });
                            let before = projection.content_revision();
                            match projection.apply(reply, snapshot.runtime_generation) {
                                Ok(_) => {
                                    changed |= projection.content_revision() != before;
                                    poll.failures = 0;
                                    terminal.external_observed_at = Some(now);
                                    terminal.external_observed_contract =
                                        Some(coven_client::transcript::CONTRACT);
                                    poll.cursor = projection.cursor().cloned();
                                    if binding.execution.pinned_source.is_none() {
                                        binding.execution.pinned_source =
                                            Some(reply.source.clone());
                                        persist_pin = true;
                                    }
                                    if matches!(
                                        &reply.result,
                                        coven_client::transcript::TranscriptResult::Reset { .. }
                                            | coven_client::transcript::TranscriptResult::Page {
                                                has_more: true,
                                                ..
                                            }
                                    ) {
                                        poll.next_read = now;
                                    }
                                }
                                Err(_) => {
                                    projection_error = true;
                                    poll.stopped = true;
                                    terminal.external_transcript = None;
                                    diagnostic_override = Some("Transcript identity or cursor validation failed; reopen the attachment after checking its binding.".to_owned());
                                    changed = true;
                                }
                            }
                        }
                        ProviderOperationResult::Source(reply) => {
                            poll.failures = 0;
                            terminal.external_observed_at = Some(now);
                            terminal.external_observed_contract =
                                Some(coven_client::source::CONTRACT);
                            match &reply.result {
                                SourceResult::Snapshot { cursor, .. } => {
                                    poll.cursor = Some(cursor.clone());
                                    if binding.execution.pinned_source.is_none() {
                                        binding.execution.pinned_source =
                                            Some(reply.source.clone());
                                        persist_pin = true;
                                    }
                                }
                                SourceResult::Page { next_cursor, .. } => {
                                    if let Some(cursor) = next_cursor {
                                        poll.cursor = Some(cursor.clone());
                                    }
                                }
                                SourceResult::Reset { .. } => {
                                    poll.cursor = None;
                                    poll.next_read = now;
                                }
                            }
                        }
                        ProviderOperationResult::Failure(failure) => {
                            if poll.checkpoint && failure.kind == ProviderFailureKind::Disconnected
                            {
                                poll.checkpoint_attached = false;
                            }
                            poll.failures = poll.failures.saturating_add(1).min(5);
                            poll.next_read = now + Duration::from_secs(1u64 << poll.failures);
                            poll.stopped = matches!(
                                failure.kind,
                                ProviderFailureKind::IdentityMismatch
                                    | ProviderFailureKind::Unsupported
                                    | ProviderFailureKind::Rejected
                            );
                            if poll.checkpoint
                                && !poll.checkpoint_attached
                                && failure.kind == ProviderFailureKind::Unsupported
                            {
                                // A daemon may expose execution metadata and
                                // transcript reads without the owner-local
                                // checkpoint stream. Degrade explicitly and
                                // keep normalized transcript as the fallback.
                                poll.checkpoint = false;
                                poll.transcript = true;
                                poll.stopped = false;
                                poll.next_read = now;
                                // Keep the empty projection installed while
                                // the first transcript page is being fetched.
                                // Clearing it here makes a just-created pane
                                // report `transcript_unavailable` during the
                                // short checkpoint-to-transcript negotiation
                                // window, even though the provider has
                                // explicitly accepted the fallback. The
                                // projection is replaced only when the
                                // fallback read is rejected or its identity
                                // validation fails below.
                                diagnostic_override = Some(
                                    "Provider does not expose checkpoint terminal output; using transcript fallback."
                                        .to_owned(),
                                );
                                changed = true;
                            } else if poll.transcript
                                && failure.kind == ProviderFailureKind::Unsupported
                            {
                                // Metadata-only degradation is explicit and never creates a native runtime.
                                poll.transcript = false;
                                poll.cursor = None;
                                poll.stopped = false;
                                poll.next_read = now;
                                terminal.external_transcript = None;
                                diagnostic_override = Some("Provider offers metadata only; transcript output is unavailable.".to_owned());
                                changed = true;
                            } else if failure.kind == ProviderFailureKind::IdentityMismatch {
                                terminal.external_transcript = None;
                                changed = true;
                            }
                        }
                        _ => {}
                    }
                    let observation = ProviderObservation {
                        lifecycle: snapshot.lifecycle,
                        activity: snapshot.activity,
                        connection: if projection_error {
                            ProviderConnection::Disconnected
                        } else {
                            snapshot.connection
                        },
                        durability: snapshot.durability,
                        diagnostic: diagnostic_override.or_else(|| match &operation.result {
                            ProviderOperationResult::Failure(failure) => {
                                Some(failure.message.clone())
                            }
                            _ => None,
                        }),
                        writer_state: match &operation.result {
                            ProviderOperationResult::Transcript(reply) => {
                                Some(reply.health.writer_state)
                            }
                            _ => snapshot
                                .source
                                .as_ref()
                                .map(|reply| reply.health.writer_state),
                        },
                    };
                    if terminal.external_observation.as_ref() != Some(&observation) {
                        terminal.external_observation = Some(observation);
                        changed = true;
                    }
                }
            }
            if !poll.stopped && poll.pending.is_none() && now >= poll.next_read {
                let command = if poll.health {
                    ProviderCommand::Health
                } else if poll.checkpoint && !poll.checkpoint_attached {
                    ProviderCommand::AttachCheckpoint {
                        binding: binding.execution.clone(),
                        attachment_id: terminal_id.as_str().to_owned(),
                        checkpoint: terminal.external_checkpoint.clone(),
                    }
                } else if poll.checkpoint {
                    ProviderCommand::ReadCheckpoint {
                        binding: binding.execution.clone(),
                    }
                } else if poll.transcript {
                    ProviderCommand::ReadTranscript {
                        binding: binding.execution.clone(),
                        cursor: poll.cursor.clone(),
                        limit: 32,
                    }
                } else {
                    ProviderCommand::ReadSource {
                        binding: binding.execution.clone(),
                        cursor: poll.cursor.clone(),
                        limit: 128,
                    }
                };
                match runtime.submit(command) {
                    Ok(ticket) => poll.pending = Some(ticket),
                    Err(error @ (SubmitError::Disconnected | SubmitError::IdentityMismatch)) => {
                        // The provider worker is gone (thread exit, panic or
                        // local shutdown) or the binding is fenced. Reconcile
                        // staleness explicitly instead of rendering the last
                        // connected/working observation as current forever;
                        // stop polling this attachment.
                        poll.stopped = true;
                        let diagnostic = match error {
                            SubmitError::IdentityMismatch => {
                                "provider binding changed; observation is stale"
                            }
                            _ => "provider worker is unavailable; observation is stale",
                        };
                        let snapshot = runtime.snapshot();
                        let observation = ProviderObservation {
                            lifecycle: snapshot.lifecycle,
                            activity: snapshot.activity,
                            connection: ProviderConnection::Disconnected,
                            durability: snapshot.durability,
                            diagnostic: Some(diagnostic.into()),
                            writer_state: snapshot
                                .source
                                .as_ref()
                                .map(|reply| reply.health.writer_state),
                        };
                        if terminal.external_observation.as_ref() != Some(&observation) {
                            terminal.external_observation = Some(observation);
                            changed = true;
                        }
                    }
                    Err(_) => {
                        poll.next_read = now + READ_INTERVAL;
                    }
                }
            }
        }
        self.runtime_providers.refresh_deadline();
        if persist_pin {
            self.schedule_session_save();
        }
        changed
    }

    /// Reconcile permission-bearing execution commands without doing any
    /// blocking Coven work on the app thread. The actor snapshot is the only
    /// cross-thread read; completed records remain available for a bounded
    /// interval so a caller can inspect an outcome after a lost response.
    fn poll_execution_operations(&mut self, now: Instant) -> bool {
        let mut changed = false;
        let mut continuation_updates = Vec::new();
        for operation in self
            .runtime_providers
            .operations
            .values_mut()
            .filter(|operation| operation.completed_at.is_none())
        {
            operation.runtime.try_drain_updates(8);
            let snapshot = operation.runtime.snapshot();
            let Some(last) = snapshot.operation_result(operation.ticket) else {
                // The bounded retention map can evict a settled ticket when
                // many operations share one runtime. Re-submit the idempotent
                // lookup (read-only, bound to the same request id, digest and
                // target) so a lost ticket self-heals instead of freezing the
                // operation forever.
                if let Ok(ticket) = operation.runtime.submit(ProviderCommand::LookupExecution {
                    binding: operation.binding.clone(),
                    request_id: operation.request_id.clone(),
                    expected_digest: operation.payload_digest.clone(),
                    expected_target: operation.target.clone(),
                }) {
                    operation.ticket = ticket;
                    operation.result = None;
                    changed = true;
                }
                continue;
            };
            if matches!(last.result, ProviderOperationResult::Pending { .. }) {
                continue;
            }
            let settled_result = last.result.clone();
            let continuation_update = match &settled_result {
                ProviderOperationResult::Execution(outcome)
                | ProviderOperationResult::LookupExecution(outcome) => {
                    continuation_binding_update(operation, outcome)
                }
                _ => None,
            };
            let mut settled = true;
            match &settled_result {
                ProviderOperationResult::Execution(outcome)
                | ProviderOperationResult::LookupExecution(outcome)
                    if matches!(
                        outcome.state,
                        coven_client::execution::RequestState::Admitted
                            | coven_client::execution::RequestState::Establishing
                            | coven_client::execution::RequestState::Running
                    ) =>
                {
                    // An execution request response describes the durable
                    // row at one instant. Continue with the same request id,
                    // digest and target until Coven reports a terminal state;
                    // never submit the launch intent again.
                    settled = false;
                    if let Ok(ticket) = operation.runtime.submit(ProviderCommand::LookupExecution {
                        binding: operation.binding.clone(),
                        request_id: operation.request_id.clone(),
                        expected_digest: operation.payload_digest.clone(),
                        expected_target: operation.target.clone(),
                    }) {
                        operation.ticket = ticket;
                        operation.result = None;
                        changed = true;
                    }
                }
                _ => {}
            }
            if settled {
                operation.result = Some(settled_result);
                operation.completed_at = Some(now);
                changed = true;
                if let Some(update) = continuation_update {
                    continuation_updates.push(update);
                }
                // A launch worker is not attached to a terminal. Stop it
                // after the result is retained; the durable Coven execution
                // remains independent and can be reconciled by request id.
                if operation.terminal_id.is_none() {
                    operation.runtime.shutdown();
                }
            }
        }
        for (terminal_id, parent_session_id, parent_generation, child_binding) in
            continuation_updates
        {
            let Some(terminal) = self.state.terminals.get_mut(&terminal_id) else {
                continue;
            };
            let Some(binding) = terminal.external_binding.as_mut() else {
                continue;
            };
            if binding.execution.session_id != parent_session_id
                || binding.execution.generation != parent_generation
            {
                continue;
            }
            binding.execution = child_binding.clone();
            terminal.external_checkpoint = None;
            terminal.external_observed_at = None;
            terminal.external_observed_contract = None;
            let transcript = self
                .runtime_providers
                .polls
                .get(&terminal_id)
                .is_some_and(|poll| poll.transcript);
            terminal.external_transcript = transcript.then(|| {
                crate::runtime_provider::transcript::TranscriptProjection::new(child_binding)
            });
            terminal.external_observation = Some(ProviderObservation {
                connection: ProviderConnection::Connected,
                durability: crate::runtime_provider::ProviderDurability::Durable,
                ..ProviderObservation::default()
            });
            if let Some(poll) = self.runtime_providers.polls.get_mut(&terminal_id) {
                poll.cursor = None;
                poll.pending = None;
                poll.next_read = Instant::now();
                poll.failures = 0;
                poll.stopped = false;
                poll.checkpoint_attached = false;
                poll.checkpoint_content_revision = 0;
            }
            self.schedule_session_save();
            self.render_dirty.request_generic();
            changed = true;
        }
        self.runtime_providers.operations.retain(|_, operation| {
            operation.completed_at.is_none_or(|completed| {
                now.saturating_duration_since(completed) < OPERATION_RETENTION
            })
        });
        self.runtime_providers.refresh_deadline();
        changed
    }
}

use super::api::responses::{encode_error, encode_success};
use crate::api::schema::{
    ProviderAttachmentInfo, ResponseResult, RuntimeProviderAttachParams,
    RuntimeProviderAttachmentTarget, RuntimeProviderExecuteParams, RuntimeProviderExecutionIntent,
    RuntimeProviderExecutionTarget, RuntimeProviderHarness, RuntimeProviderInfo,
    RuntimeProviderMode, RuntimeProviderOperationGetParams, RuntimeProviderOperationInfo,
    RuntimeProviderTarget,
};

impl App {
    fn provider_info(&self, config: &ProviderConfig) -> RuntimeProviderInfo {
        let identity = &config.identity;
        RuntimeProviderInfo {
            provider_id: identity.provider_id.as_str().into(),
            host_id: identity.host_id.as_str().into(),
            project_id: identity.scope.project_id.as_str().into(),
            profile_id: identity.scope.profile_id.as_str().into(),
            policy_generation: identity.scope.policy_generation,
            authority_id: identity.authority.id.as_str().into(),
            authority_generation: identity.authority.generation,
            enabled: self.runtime_providers.settings.enabled,
            capabilities: if self.runtime_providers.settings.enabled {
                let mut capabilities = vec![
                    "execution_projection.v1".into(),
                    "execution_transcript.v1".into(),
                    "terminal_checkpoint.v1".into(),
                ];
                if config.actions_allowed() {
                    capabilities.push("execution_actions.v1".into());
                }
                capabilities
            } else {
                Vec::new()
            },
        }
    }
    pub(super) fn handle_runtime_provider_list(&self, id: String) -> String {
        encode_success(
            id,
            ResponseResult::RuntimeProviderList {
                enabled: self.runtime_providers.settings.enabled,
                diagnostic: self.runtime_providers.diagnostic.clone(),
                providers: self
                    .runtime_providers
                    .settings
                    .providers
                    .iter()
                    .map(|config| self.provider_info(config))
                    .collect(),
            },
        )
    }
    pub(super) fn handle_runtime_provider_get(
        &self,
        id: String,
        target: RuntimeProviderTarget,
    ) -> String {
        let Some(config) = self
            .runtime_providers
            .settings
            .providers
            .iter()
            .find(|config| config.identity.provider_id.as_str() == target.provider_id)
        else {
            return encode_error(id, "provider_not_found", "provider is not configured");
        };
        encode_success(
            id,
            ResponseResult::RuntimeProviderInfo {
                provider: self.provider_info(config),
            },
        )
    }
    fn provider_attachment_info(&self, terminal_id: &TerminalId) -> Option<ProviderAttachmentInfo> {
        let terminal = self.state.terminals.get(terminal_id)?;
        let binding = &terminal.external_binding.as_ref()?.execution;
        let fallback = ProviderObservation::default();
        let observation = terminal.external_observation.as_ref().unwrap_or(&fallback);
        Some(ProviderAttachmentInfo {
            terminal_id: terminal_id.as_str().into(),
            provider_id: binding.provider_id.clone(),
            host_id: binding.host_id.as_str().into(),
            project_id: binding.scope.project_id.as_str().into(),
            profile_id: binding.scope.profile_id.as_str().into(),
            session_id: binding.session_id.as_str().into(),
            generation: binding.generation,
            attached: self.terminal_runtimes.external(terminal_id).is_some(),
            lifecycle: format!("{:?}", observation.lifecycle).to_ascii_lowercase(),
            activity: format!("{:?}", observation.activity).to_ascii_lowercase(),
            connection: format!("{:?}", observation.connection).to_ascii_lowercase(),
            durability: format!("{:?}", observation.durability).to_ascii_lowercase(),
            diagnostic: observation.diagnostic.clone(),
            observation_age_ms: terminal.external_observed_at.map(|observed| {
                u64::try_from(
                    Instant::now()
                        .saturating_duration_since(observed)
                        .as_millis(),
                )
                .unwrap_or(u64::MAX)
            }),
            observation_source: terminal.external_observed_at.map(|_| {
                terminal
                    .external_observed_contract
                    .unwrap_or(coven_client::source::CONTRACT)
                    .to_owned()
            }),
            source_ledger_id: binding
                .pinned_source
                .as_ref()
                .map(|source| source.ledger_id.as_str().to_owned()),
            source_epoch: binding.pinned_source.as_ref().map(|source| source.epoch),
        })
    }
    pub(super) fn handle_runtime_provider_attachment_get(
        &self,
        id: String,
        target: RuntimeProviderAttachmentTarget,
    ) -> String {
        let attachment = self
            .state
            .terminals
            .keys()
            .find(|key| key.as_str() == target.terminal_id)
            .and_then(|key| self.provider_attachment_info(key));
        match attachment {
            Some(attachment) => {
                encode_success(id, ResponseResult::RuntimeProviderAttachment { attachment })
            }
            None => encode_error(
                id,
                "attachment_not_found",
                "provider attachment does not exist",
            ),
        }
    }

    /// Explicitly replace the owner-local terminal controller for an existing
    /// checkpoint attachment. Queue admission is immediate; the provider
    /// worker performs the blocking Coven takeover and fences subsequent
    /// mutations with the newly issued generation.
    pub(super) fn handle_runtime_provider_takeover(
        &mut self,
        id: String,
        target: RuntimeProviderAttachmentTarget,
    ) -> String {
        let terminal_id = self
            .state
            .terminals
            .iter()
            .find(|(key, terminal)| {
                key.as_str() == target.terminal_id
                    && terminal.external_binding.is_some()
                    && self.terminal_runtimes.external(key).is_some()
            })
            .map(|(key, _)| key.clone());
        let Some(terminal_id) = terminal_id else {
            return encode_error(
                id,
                "attachment_not_found",
                "provider attachment does not exist",
            );
        };
        let Some(binding) = self
            .state
            .terminals
            .get(&terminal_id)
            .and_then(|terminal| terminal.external_binding.as_ref())
            .cloned()
        else {
            return encode_error(
                id,
                "attachment_not_found",
                "provider attachment does not exist",
            );
        };
        // A remote controller replacement is a permission-bearing action like
        // execute: require the same owner-local opt-in so the shell command
        // lane cannot reach it by configuration default.
        match self
            .runtime_providers
            .config(&binding.execution.provider_id)
            .is_some_and(|config| config.actions_allowed())
        {
            true => {}
            false => {
                return encode_error(
                    id,
                    "action_unavailable",
                    "permission-bearing provider actions are not enabled by owner configuration",
                );
            }
        }
        if self
            .terminal_runtimes
            .external_session(&terminal_id)
            .is_none()
        {
            return encode_error(
                id,
                "control_unavailable",
                "terminal control has not been negotiated for this attachment",
            );
        }
        let Some(runtime) = self.terminal_runtimes.external(&terminal_id).cloned() else {
            return encode_error(id, "provider_unavailable", "provider worker is unavailable");
        };
        match runtime.submit(ProviderCommand::TerminalTakeover {
            binding: binding.execution,
        }) {
            Ok(_) => encode_success(id, ResponseResult::Ok {}),
            Err(error) => encode_error(
                id,
                "takeover_not_admitted",
                format!("terminal takeover was not admitted: {error:?}"),
            ),
        }
    }

    pub(super) fn handle_runtime_provider_detach(
        &mut self,
        id: String,
        target: RuntimeProviderAttachmentTarget,
    ) -> String {
        let terminal_id = self
            .state
            .terminals
            .iter()
            .find(|(key, terminal)| {
                key.as_str() == target.terminal_id && terminal.external_binding.is_some()
            })
            .map(|(key, _)| key.clone());
        let Some(terminal_id) = terminal_id else {
            return encode_error(
                id,
                "attachment_not_found",
                "provider attachment does not exist",
            );
        };
        self.runtime_providers.remove(&terminal_id);
        self.terminal_runtimes.shutdown_external(&terminal_id);
        if let Some(terminal) = self.state.terminals.get_mut(&terminal_id) {
            terminal
                .external_observation
                .get_or_insert_with(ProviderObservation::default)
                .connection = ProviderConnection::Disconnected;
        }
        self.handle_runtime_provider_attachment_get(id, target)
    }

    pub(super) fn handle_runtime_provider_execute(
        &mut self,
        id: String,
        params: RuntimeProviderExecuteParams,
    ) -> String {
        let Some(config) = self.runtime_providers.config(&params.provider_id).cloned() else {
            return encode_error(
                id,
                "provider_unavailable",
                "provider is disabled or not configured",
            );
        };
        if !config.actions_allowed() {
            return encode_error(
                id,
                "action_unavailable",
                "permission-bearing provider actions are not enabled by owner configuration",
            );
        }
        let Ok(request_id) = RequestId::new(params.request_id.clone()) else {
            return encode_error(id, "invalid_params", "invalid request id");
        };

        let mut terminal_id = None;
        let mut binding = None;
        let intent = match params.intent {
            RuntimeProviderExecutionIntent::Launch {
                harness,
                mode,
                model,
                cwd,
                prompt,
            } => ExecutionIntent::Launch {
                harness: harness.into(),
                mode: mode.into(),
                model,
                cwd: match resolve_launch_cwd(&config, &cwd) {
                    Ok(cwd) => cwd,
                    Err(()) => {
                        return encode_error(
                            id,
                            "invalid_params",
                            "launch working directory is outside the configured project root",
                        );
                    }
                },
                prompt,
            },
            RuntimeProviderExecutionIntent::Turn {
                terminal_id: requested_terminal,
                prompt,
            } => {
                let Some((resolved_terminal, resolved_binding, _runtime)) =
                    self.provider_attachment_runtime(&params.provider_id, &requested_terminal)
                else {
                    return encode_error(
                        id,
                        "attachment_not_found",
                        "an attached provider execution is required for a turn",
                    );
                };
                terminal_id = Some(resolved_terminal);
                binding = Some(resolved_binding.clone());
                ExecutionIntent::Turn {
                    session_id: resolved_binding.session_id,
                    generation: resolved_binding.generation,
                    prompt,
                }
            }
            RuntimeProviderExecutionIntent::Continue {
                terminal_id: requested_terminal,
                prompt,
            } => {
                let Some((resolved_terminal, resolved_binding, _runtime)) =
                    self.provider_attachment_runtime(&params.provider_id, &requested_terminal)
                else {
                    return encode_error(
                        id,
                        "attachment_not_found",
                        "an attached provider execution is required for continuation",
                    );
                };
                terminal_id = Some(resolved_terminal);
                binding = Some(resolved_binding.clone());
                ExecutionIntent::Continue {
                    session_id: resolved_binding.session_id,
                    generation: resolved_binding.generation,
                    prompt,
                }
            }
            RuntimeProviderExecutionIntent::Stop {
                terminal_id: requested_terminal,
            } => {
                let Some((resolved_terminal, resolved_binding, _runtime)) =
                    self.provider_attachment_runtime(&params.provider_id, &requested_terminal)
                else {
                    return encode_error(
                        id,
                        "attachment_not_found",
                        "an attached provider execution is required for stop",
                    );
                };
                terminal_id = Some(resolved_terminal);
                binding = Some(resolved_binding.clone());
                ExecutionIntent::Stop {
                    session_id: resolved_binding.session_id,
                    generation: resolved_binding.generation,
                }
            }
        };
        let request = ExecutionRequest {
            contract: coven_client::execution::CONTRACT.to_owned(),
            request_id: request_id.clone(),
            scope: config.identity.scope.clone(),
            authority: config.identity.authority.clone(),
            expires_at: params.expires_at,
            intent,
        };
        if request.validate_new_admission(unix_seconds()).is_err() {
            return encode_error(
                id,
                "invalid_params",
                "execution request expiry is outside the owner admission window",
            );
        }
        let Ok(payload_digest) = request.digest() else {
            return encode_error(
                id,
                "invalid_params",
                "execution request could not be encoded",
            );
        };
        let target = request.intent.target();
        let key = OperationKey {
            provider_id: params.provider_id.clone(),
            request_id: request_id.clone(),
        };
        if let Some(existing) = self.runtime_providers.operations.get(&key) {
            if existing.payload_digest != payload_digest || existing.target != target {
                return encode_error(
                    id,
                    "identity_mismatch",
                    "request id is already bound to a different execution",
                );
            }
            return encode_success(
                id,
                ResponseResult::RuntimeProviderOperation {
                    operation: self.runtime_provider_operation_info(existing),
                },
            );
        }
        if self.runtime_providers.operations.len() >= MAX_OPERATIONS {
            return encode_error(id, "operation_limit", "provider operation limit reached");
        }
        let runtime = if let Some(terminal_id) = terminal_id.as_ref() {
            self.terminal_runtimes.external(terminal_id).cloned()
        } else {
            match ProviderRuntime::start(config) {
                Ok(runtime) => Some(runtime),
                Err(crate::runtime_provider::ProviderStartError::WorkerLimitReached) => {
                    return encode_error(
                        id,
                        "provider_busy",
                        "provider worker capacity is busy; retry after workers retire",
                    );
                }
                Err(_) => {
                    return encode_error(
                        id,
                        "provider_unavailable",
                        "could not start provider worker",
                    );
                }
            }
        };
        let Some(runtime) = runtime else {
            return encode_error(
                id,
                "attachment_not_found",
                "provider attachment runtime is no longer available",
            );
        };
        let Ok(ticket) = runtime.submit(ProviderCommand::Execution {
            binding: binding.clone(),
            request,
        }) else {
            return encode_error(id, "provider_busy", "provider operation was not admitted");
        };
        self.runtime_providers.operations.insert(
            key,
            OperationState {
                provider_id: params.provider_id,
                request_id,
                ticket,
                payload_digest,
                target,
                binding,
                terminal_id,
                runtime,
                result: None,
                completed_at: None,
            },
        );
        self.runtime_providers.refresh_deadline();
        let operation = self
            .runtime_providers
            .operations
            .values()
            .find(|operation| operation.ticket == ticket)
            .map(|operation| self.runtime_provider_operation_info(operation));
        match operation {
            Some(operation) => {
                encode_success(id, ResponseResult::RuntimeProviderOperation { operation })
            }
            None => encode_error(id, "provider_unavailable", "operation was not retained"),
        }
    }

    pub(super) fn handle_runtime_provider_operation_get(
        &mut self,
        id: String,
        params: RuntimeProviderOperationGetParams,
    ) -> String {
        let Some(config) = self.runtime_providers.config(&params.provider_id).cloned() else {
            return encode_error(
                id,
                "provider_unavailable",
                "provider is disabled or not configured",
            );
        };
        let Ok(request_id) = RequestId::new(params.request_id.clone()) else {
            return encode_error(id, "invalid_params", "invalid request id");
        };
        if coven_client::execution::validate_digest(&params.payload_digest).is_err() {
            return encode_error(id, "invalid_params", "invalid payload digest");
        }
        let Ok(target) = execution_target_from_api(params.expected_target) else {
            return encode_error(id, "invalid_params", "invalid expected execution target");
        };
        let key = OperationKey {
            provider_id: params.provider_id.clone(),
            request_id: request_id.clone(),
        };
        if let Some(existing) = self.runtime_providers.operations.get(&key) {
            if existing.payload_digest != params.payload_digest || existing.target != target {
                return encode_error(
                    id,
                    "identity_mismatch",
                    "request id is already bound to a different execution",
                );
            }
            return encode_success(
                id,
                ResponseResult::RuntimeProviderOperation {
                    operation: self.runtime_provider_operation_info(existing),
                },
            );
        }
        if self.runtime_providers.operations.len() >= MAX_OPERATIONS {
            return encode_error(id, "operation_limit", "provider operation limit reached");
        }
        let runtime = match ProviderRuntime::start(config) {
            Ok(runtime) => runtime,
            Err(crate::runtime_provider::ProviderStartError::WorkerLimitReached) => {
                return encode_error(
                    id,
                    "provider_busy",
                    "provider worker capacity is busy; retry after workers retire",
                );
            }
            Err(_) => {
                return encode_error(
                    id,
                    "provider_unavailable",
                    "could not start provider worker",
                );
            }
        };
        let binding = match &target {
            ExecutionTarget::Launch => None,
            ExecutionTarget::Existing {
                session_id,
                generation,
            }
            | ExecutionTarget::Continuation {
                previous_session_id: session_id,
                previous_generation: generation,
            } => match runtime.binding(session_id.clone(), *generation) {
                Ok(binding) => Some(binding),
                Err(_) => {
                    return encode_error(
                        id,
                        "invalid_params",
                        "invalid expected execution binding",
                    );
                }
            },
        };
        let Ok(ticket) = runtime.submit(ProviderCommand::LookupExecution {
            binding: binding.clone(),
            request_id: request_id.clone(),
            expected_digest: params.payload_digest.clone(),
            expected_target: target.clone(),
        }) else {
            return encode_error(id, "provider_busy", "provider lookup was not admitted");
        };
        self.runtime_providers.operations.insert(
            key,
            OperationState {
                provider_id: params.provider_id,
                request_id,
                ticket,
                payload_digest: params.payload_digest,
                target,
                binding,
                terminal_id: None,
                runtime,
                result: None,
                completed_at: None,
            },
        );
        self.runtime_providers.refresh_deadline();
        let operation = self
            .runtime_providers
            .operations
            .values()
            .find(|operation| operation.ticket == ticket)
            .map(|operation| self.runtime_provider_operation_info(operation));
        match operation {
            Some(operation) => {
                encode_success(id, ResponseResult::RuntimeProviderOperation { operation })
            }
            None => encode_error(id, "provider_unavailable", "operation was not retained"),
        }
    }

    fn provider_attachment_runtime(
        &self,
        provider_id: &str,
        terminal_id: &str,
    ) -> Option<(
        TerminalId,
        crate::runtime_provider::ProviderBinding,
        ProviderRuntimeHandle,
    )> {
        let (terminal_id, terminal) = self
            .state
            .terminals
            .iter()
            .find(|(key, _)| key.as_str() == terminal_id)?;
        let binding = terminal.external_binding.as_ref()?.execution.clone();
        if binding.provider_id != provider_id {
            return None;
        }
        let runtime = self.terminal_runtimes.external(terminal_id).cloned()?;
        Some((terminal_id.clone(), binding, runtime))
    }

    fn runtime_provider_operation_info(
        &self,
        operation: &OperationState,
    ) -> RuntimeProviderOperationInfo {
        let mut info = RuntimeProviderOperationInfo {
            provider_id: operation.provider_id.clone(),
            request_id: operation.request_id.as_str().to_owned(),
            ticket: operation.ticket.raw(),
            payload_digest: operation.payload_digest.clone(),
            target: execution_target_name(&operation.target).to_owned(),
            status: "pending".to_owned(),
            state: None,
            delivery: None,
            session_id: None,
            parent_session_id: None,
            generation: None,
            session_status: None,
            exit_code: None,
            terminal_id: operation
                .terminal_id
                .as_ref()
                .map(|terminal_id| terminal_id.as_str().to_owned()),
            diagnostic: None,
        };
        let Some(result) = operation.result.as_ref() else {
            return info;
        };
        match result {
            ProviderOperationResult::Execution(outcome)
            | ProviderOperationResult::LookupExecution(outcome) => {
                info.status = execution_state_name(outcome.state).to_owned();
                info.state = Some(execution_state_name(outcome.state).to_owned());
                info.delivery = Some(execution_delivery_name(outcome.delivery).to_owned());
                info.session_id = Some(outcome.session_id.as_str().to_owned());
                info.parent_session_id = outcome
                    .parent_session_id
                    .as_ref()
                    .map(|session_id| session_id.as_str().to_owned());
                info.generation = Some(outcome.generation);
                info.session_status = outcome.session_status.clone();
                info.exit_code = outcome.exit_code;
            }
            ProviderOperationResult::Failure(failure) => {
                info.status = "failed".to_owned();
                info.diagnostic = Some(failure.message.clone());
            }
            ProviderOperationResult::Detached | ProviderOperationResult::Shutdown => {
                info.status = "unresolved".to_owned();
                info.diagnostic =
                    Some("provider operation actor stopped before settlement".to_owned());
            }
            ProviderOperationResult::Pending { .. } => {}
            _ => {
                info.status = "failed".to_owned();
                info.diagnostic =
                    Some("provider returned an unexpected operation result".to_owned());
            }
        }
        info
    }

    pub(super) fn handle_runtime_provider_attach(
        &mut self,
        id: String,
        params: RuntimeProviderAttachParams,
    ) -> String {
        let Some(config) = self.runtime_providers.config(&params.provider_id).cloned() else {
            return encode_error(
                id,
                "provider_unavailable",
                "provider is disabled or not configured",
            );
        };
        let Ok(session_id) = ExecutionSessionId::new(params.session_id.clone()) else {
            return encode_error(id, "invalid_params", "invalid session id");
        };
        if params.generation == 0
            || params.generation > i64::MAX as u64
            || params
                .label
                .as_ref()
                .is_some_and(|label| label.len() > 256 || label.chars().any(char::is_control))
        {
            return encode_error(id, "invalid_params", "invalid generation or label");
        }
        let existing = self
            .state
            .terminals
            .values()
            .filter_map(|terminal| terminal.external_binding.as_ref())
            .find(|binding| {
                binding.execution.provider_id == params.provider_id
                    && binding.execution.session_id == session_id
            })
            .cloned();
        if let Some(binding) = existing {
            if binding.execution.generation != params.generation
                || !binding.execution.matches_identity(&config.identity)
            {
                return encode_error(
                    id,
                    "identity_mismatch",
                    "existing attachment has a different execution identity",
                );
            }
            let terminal_id = binding.terminal_id.as_str().to_owned();
            if self
                .runtime_providers
                .polls
                .get(&binding.terminal_id)
                .is_some_and(|poll| poll.stopped)
            {
                self.runtime_providers.remove(&binding.terminal_id);
                self.terminal_runtimes
                    .shutdown_external(&binding.terminal_id);
            }
            if self
                .terminal_runtimes
                .external(&binding.terminal_id)
                .is_none()
            {
                if let Err(message) = self.start_runtime_provider(binding) {
                    return encode_error(id, "provider_unavailable", message);
                }
            }
            return self.handle_runtime_provider_attachment_get(
                id,
                RuntimeProviderAttachmentTarget { terminal_id },
            );
        }
        if self
            .state
            .terminals
            .values()
            .filter(|terminal| terminal.external_binding.is_some())
            .count()
            >= MAX_ATTACHMENTS
        {
            return encode_error(id, "attachment_limit", "external terminal limit reached");
        }
        let ws_idx = match params.workspace_id.as_ref() {
            Some(workspace_id) => self.parse_workspace_id(workspace_id),
            None => self.state.active,
        };
        let ws_idx = ws_idx.filter(|index| *index < self.state.workspaces.len());
        if params.workspace_id.is_some() && ws_idx.is_none() {
            return encode_error(id, "workspace_not_found", "no matching workspace");
        }
        let Some(terminal_id) =
            TerminalId::try_alloc_external().filter(|key| !self.state.terminals.contains_key(key))
        else {
            return encode_error(
                id,
                "attachment_unavailable",
                "terminal identity allocation failed",
            );
        };
        let Ok(execution) = crate::runtime_provider::ManagedBinding::from_identity(
            &config.identity,
            session_id,
            params.generation,
            1,
        ) else {
            return encode_error(id, "invalid_params", "invalid execution identity");
        };
        let binding = ExternalBinding {
            terminal_id: terminal_id.clone(),
            execution,
        };
        let mut terminal =
            crate::terminal::TerminalState::new(terminal_id.clone(), std::path::PathBuf::new());
        terminal.external_binding = Some(binding.clone());
        self.state.terminals.insert(terminal_id.clone(), terminal);
        if let Err(message) = self.start_runtime_provider(binding) {
            self.state.terminals.remove(&terminal_id);
            return encode_error(id, "provider_unavailable", message);
        }
        let pane_id = crate::layout::PaneId::alloc();
        let moved = crate::workspace::MovedPane {
            pane_id,
            pane_state: crate::pane::PaneState::new(terminal_id.clone()),
        };
        let (ws_idx, tab_idx) = if let Some(ws_idx) = ws_idx {
            let tab_idx = self.state.workspaces[ws_idx].create_tab_from_existing_pane(
                moved,
                params.label,
                self.event_tx.clone(),
                self.render_notify.clone(),
                self.render_dirty.clone(),
            );
            (ws_idx, tab_idx)
        } else {
            let workspace = crate::workspace::Workspace::from_existing_pane(
                None,
                params.label,
                std::path::PathBuf::new(),
                false,
                moved,
                self.event_tx.clone(),
                self.render_notify.clone(),
                self.render_dirty.clone(),
            );
            let ws_idx = self.state.workspaces.len();
            self.state.workspaces.push(workspace);
            (ws_idx, 0)
        };
        self.state.remove_alias_shadowed_by_new_pane(pane_id);
        if params.focus {
            self.state.switch_workspace_tab(ws_idx, tab_idx);
            self.state.mode = super::Mode::Terminal;
        }
        self.emit_tab_created_events(ws_idx, tab_idx);
        self.schedule_session_save();
        self.handle_runtime_provider_attachment_get(
            id,
            RuntimeProviderAttachmentTarget {
                terminal_id: terminal_id.as_str().into(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_provider::{ProviderId, ProviderIdentity};
    use coven_client::{
        execution::{AuthorityId, ExecutionAuthority, ExecutionScope, ProfileId, ProjectId},
        source::SourceId,
    };

    fn app(enabled: bool) -> App {
        let (_sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            super::super::AppPolicy::TEST,
            None,
            receiver,
            crate::api::EventHub::default(),
        );
        let identity = ProviderIdentity::new(
            ProviderId::new("owner").unwrap(),
            SourceId::new("host").unwrap(),
            ExecutionScope {
                project_id: ProjectId::new("project").unwrap(),
                profile_id: ProfileId::new("profile").unwrap(),
                policy_generation: 1,
            },
            ExecutionAuthority::new(AuthorityId::new("authority").unwrap(), 1).unwrap(),
        )
        .unwrap();
        app.runtime_providers.settings = OwnerProviderSettings {
            enabled,
            providers: vec![ProviderConfig::new(
                identity,
                std::env::temp_dir().join(TerminalId::alloc().as_str()),
                std::num::NonZeroUsize::new(2).unwrap(),
            )
            .unwrap()],
        };
        app
    }
    fn params() -> RuntimeProviderAttachParams {
        RuntimeProviderAttachParams {
            provider_id: "owner".into(),
            session_id: "session".into(),
            generation: 1,
            workspace_id: None,
            focus: true,
            label: None,
        }
    }

    fn multi_provider_app(extra: &[&str]) -> App {
        let mut app = action_app();
        for (index, id) in extra.iter().enumerate() {
            let suffix = index + 2;
            let identity = ProviderIdentity::new(
                ProviderId::new(*id).unwrap(),
                SourceId::new(format!("host-{suffix}")).unwrap(),
                ExecutionScope {
                    project_id: ProjectId::new(format!("project-{suffix}")).unwrap(),
                    profile_id: ProfileId::new(format!("profile-{suffix}")).unwrap(),
                    policy_generation: 1,
                },
                ExecutionAuthority::new(
                    AuthorityId::new(format!("authority-{suffix}")).unwrap(),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
            app.runtime_providers.settings.providers.push(
                ProviderConfig::new(
                    identity,
                    std::env::temp_dir().join(format!("coven-home-{suffix}")),
                    std::num::NonZeroUsize::new(2).unwrap(),
                )
                .unwrap()
                .with_project_root(std::env::temp_dir())
                .unwrap()
                .with_actions_allowed(true),
            );
        }
        app
    }

    fn attach_params(provider_id: &str) -> RuntimeProviderAttachParams {
        RuntimeProviderAttachParams {
            provider_id: provider_id.into(),
            ..params()
        }
    }

    fn action_app() -> App {
        let mut app = app(true);
        let provider = app.runtime_providers.settings.providers[0]
            .clone()
            .with_project_root(std::env::temp_dir())
            .unwrap()
            .with_actions_allowed(true);
        app.runtime_providers.settings.providers[0] = provider;
        app
    }

    fn launch_params(request_id: &str) -> RuntimeProviderExecuteParams {
        RuntimeProviderExecuteParams {
            provider_id: "owner".into(),
            request_id: request_id.into(),
            expires_at: unix_seconds().saturating_add(60),
            intent: RuntimeProviderExecutionIntent::Launch {
                harness: RuntimeProviderHarness::Codex,
                mode: RuntimeProviderMode::Transcript,
                model: None,
                cwd: ".".into(),
                prompt: "synthetic provider request".into(),
            },
        }
    }
    fn result(response: String) -> serde_json::Value {
        serde_json::from_str(&response).unwrap()
    }

    #[test]
    fn disabled_catalog_cannot_attach_or_schedule_work() {
        let mut app = app(false);
        assert!(
            result(app.handle_runtime_provider_attach("a".into(), params()))
                .get("error")
                .is_some()
        );
        assert!(app.state.terminals.is_empty());
        assert!(app.runtime_providers.deadline().is_none());
        assert!(!app.poll_runtime_providers(Instant::now()));
    }

    #[test]
    fn permission_bearing_actions_require_owner_opt_in() {
        let mut app = app(true);
        let response = result(
            app.handle_runtime_provider_execute("execute".into(), launch_params("disabled-action")),
        );
        assert_eq!(response["error"]["code"], "action_unavailable");
        assert!(app.runtime_providers.operations.is_empty());
    }

    #[test]
    fn provider_takeover_requires_owner_opt_in_and_control() {
        // Owner opt-in is checked before control negotiation, so a disabled
        // provider action can never be reached through takeover.
        let mut app = app(true);
        let attached = result(app.handle_runtime_provider_attach("a".into(), params()));
        assert!(attached.get("error").is_none(), "{attached}");
        let terminal_id = app.state.terminals.keys().next().unwrap().clone();
        let target = RuntimeProviderAttachmentTarget {
            terminal_id: terminal_id.as_str().into(),
        };
        let rejected = result(app.handle_runtime_provider_takeover("t".into(), target.clone()));
        assert_eq!(rejected["error"]["code"], "action_unavailable");
        app.shutdown_terminal_runtime(terminal_id);

        // With the owner opt-in enabled, the next gate is negotiated control.
        let mut app = action_app();
        let attached = result(app.handle_runtime_provider_attach("a".into(), params()));
        assert!(attached.get("error").is_none(), "{attached}");
        let terminal_id = app.state.terminals.keys().next().unwrap().clone();
        let target = RuntimeProviderAttachmentTarget {
            terminal_id: terminal_id.as_str().into(),
        };
        let unavailable = result(app.handle_runtime_provider_takeover("t".into(), target));
        assert_eq!(unavailable["error"]["code"], "control_unavailable");
        app.shutdown_terminal_runtime(terminal_id);
    }

    #[test]
    fn one_provider_failure_does_not_stop_another_provider() {
        // Two providers fail and one stays live. Asserting that BOTH failing
        // providers are reconciled in a single poll is order-independent proof
        // that the loop does not abort on the first failure; asserting that the
        // live provider submitted a poll command proves it was still visited.
        let mut app = multi_provider_app(&["second", "third"]);
        for (request, provider) in [
            ("a", "owner"),
            ("b", "second"),
            ("c", "third"),
        ] {
            let attached =
                result(app.handle_runtime_provider_attach(request.into(), attach_params(provider)));
            assert!(attached.get("error").is_none(), "{attached}");
        }
        assert_eq!(app.state.terminals.len(), 3);
        let terminal_for = |app: &App, provider: &str| {
            app.state
                .terminals
                .iter()
                .find(|(_, terminal)| {
                    terminal
                        .external_binding
                        .as_ref()
                        .is_some_and(|binding| binding.execution.provider_id == provider)
                })
                .map(|(id, _)| id.clone())
                .expect("provider attachment")
        };
        let owner_terminal = terminal_for(&app, "owner");
        let second_terminal = terminal_for(&app, "second");
        let third_terminal = terminal_for(&app, "third");
        for terminal_id in [&owner_terminal, &second_terminal, &third_terminal] {
            if let Some(terminal) = app.state.terminals.get_mut(terminal_id) {
                terminal.external_observation = Some(ProviderObservation {
                    lifecycle: crate::runtime_provider::ProviderLifecycle::Running,
                    activity: crate::runtime_provider::ProviderActivity::Working,
                    connection: ProviderConnection::Connected,
                    durability: crate::runtime_provider::ProviderDurability::Durable,
                    ..ProviderObservation::default()
                });
            }
            let poll = app.runtime_providers.polls.get_mut(terminal_id).unwrap();
            poll.pending = None;
            poll.next_read = Instant::now();
        }
        // The first two providers fail; the third stays live.
        app.terminal_runtimes
            .external(&owner_terminal)
            .unwrap()
            .shutdown();
        app.terminal_runtimes
            .external(&second_terminal)
            .unwrap()
            .shutdown();
        app.runtime_providers.refresh_deadline();
        assert!(app.poll_runtime_providers(Instant::now()));

        for terminal_id in [&owner_terminal, &second_terminal] {
            assert!(app.runtime_providers.polls[terminal_id].stopped);
            assert_eq!(
                app.state.terminals[terminal_id]
                    .external_observation
                    .as_ref()
                    .unwrap()
                    .connection,
                ProviderConnection::Disconnected
            );
        }
        // The live provider was still visited: it submitted a poll command and
        // its observation was not clobbered by the other providers' failures.
        let third = &app.runtime_providers.polls[&third_terminal];
        assert!(!third.stopped);
        assert!(third.pending.is_some());
        assert_eq!(
            app.state.terminals[&third_terminal]
                .external_observation
                .as_ref()
                .unwrap()
                .connection,
            ProviderConnection::Connected
        );
    }

    #[test]
    fn dead_provider_worker_reconciles_stale_observation() {
        let mut app = action_app();
        let attached = result(app.handle_runtime_provider_attach("a".into(), params()));
        assert!(attached.get("error").is_none(), "{attached}");
        let terminal_id = app.state.terminals.keys().next().unwrap().clone();
        // Leave behind the live-looking observation a running worker publishes.
        if let Some(terminal) = app.state.terminals.get_mut(&terminal_id) {
            terminal.external_observation = Some(ProviderObservation {
                lifecycle: crate::runtime_provider::ProviderLifecycle::Running,
                activity: crate::runtime_provider::ProviderActivity::Working,
                connection: ProviderConnection::Connected,
                durability: crate::runtime_provider::ProviderDurability::Durable,
                ..ProviderObservation::default()
            });
        }
        // The worker is gone, so the next poll cannot submit any command.
        app.terminal_runtimes
            .external(&terminal_id)
            .unwrap()
            .shutdown();
        {
            let poll = app.runtime_providers.polls.get_mut(&terminal_id).unwrap();
            poll.pending = None;
            poll.next_read = Instant::now();
        }
        app.runtime_providers.refresh_deadline();
        assert!(app.poll_runtime_providers(Instant::now()));
        let observation = app.state.terminals[&terminal_id]
            .external_observation
            .as_ref()
            .expect("observation");
        assert_eq!(observation.connection, ProviderConnection::Disconnected);
        assert_eq!(
            observation.diagnostic.as_deref(),
            Some("provider worker is unavailable; observation is stale")
        );
        assert!(app.runtime_providers.polls[&terminal_id].stopped);
    }

    #[test]
    fn expired_execution_is_rejected_before_worker_admission() {
        let mut app = action_app();
        let mut params = launch_params("expired-action");
        params.expires_at = unix_seconds().saturating_sub(1);
        let response = result(app.handle_runtime_provider_execute("execute".into(), params));
        assert_eq!(response["error"]["code"], "invalid_params");
        assert!(app.runtime_providers.operations.is_empty());
    }

    #[test]
    fn execution_request_ids_are_idempotent_and_digest_bound() {
        let mut app = action_app();
        let first = result(
            app.handle_runtime_provider_execute("first".into(), launch_params("same-request")),
        );
        assert!(first.get("error").is_none(), "{first}");
        let first_ticket = first["result"]["operation"]["ticket"].clone();
        assert_eq!(first["result"]["operation"]["status"], "pending");

        let replay = result(
            app.handle_runtime_provider_execute("replay".into(), launch_params("same-request")),
        );
        assert!(replay.get("error").is_none(), "{replay}");
        assert_eq!(replay["result"]["operation"]["ticket"], first_ticket);
        assert_eq!(app.runtime_providers.operations.len(), 1);

        let mut conflicting = launch_params("same-request");
        let RuntimeProviderExecutionIntent::Launch { prompt, .. } = &mut conflicting.intent else {
            panic!("launch intent expected")
        };
        *prompt = "different payload".into();
        let conflict = result(app.handle_runtime_provider_execute("conflict".into(), conflicting));
        assert_eq!(conflict["error"]["code"], "identity_mismatch");
        assert_eq!(app.runtime_providers.operations.len(), 1);
        for operation in app.runtime_providers.operations.values() {
            operation.runtime.shutdown();
        }
    }

    #[test]
    fn execution_targets_validate_session_identity_and_generation() {
        assert!(execution_target_from_api(RuntimeProviderExecutionTarget::Launch).is_ok());
        assert!(
            execution_target_from_api(RuntimeProviderExecutionTarget::Existing {
                session_id: "session/%2F1".into(),
                generation: 1,
            })
            .is_ok()
        );
        assert!(
            execution_target_from_api(RuntimeProviderExecutionTarget::Existing {
                session_id: String::new(),
                generation: 1,
            })
            .is_err()
        );
        assert!(
            execution_target_from_api(RuntimeProviderExecutionTarget::Continuation {
                previous_session_id: "session".into(),
                previous_generation: 0,
            })
            .is_err()
        );
    }

    #[test]
    fn launch_working_directory_is_resolved_inside_owner_root() {
        let mut app = action_app();
        let root = std::env::temp_dir();
        let mut params = launch_params("outside-root");
        let outside = if root.parent().is_some() {
            root.parent().unwrap().to_path_buf()
        } else {
            std::path::PathBuf::from("/")
        };
        let outside = outside.to_string_lossy().into_owned();
        let RuntimeProviderExecutionIntent::Launch { cwd, .. } = &mut params.intent else {
            panic!("launch intent expected")
        };
        *cwd = outside;
        let response = result(app.handle_runtime_provider_execute("execute".into(), params));
        assert_eq!(response["error"]["code"], "invalid_params");
        assert!(app.runtime_providers.operations.is_empty());
    }

    #[test]
    fn unobserved_attachment_never_claims_fresh_source_evidence() {
        let mut app = app(true);
        let response = result(app.handle_runtime_provider_attach("a".into(), params()));
        assert!(response.get("error").is_none());
        let terminal_id = app.state.terminals.keys().next().unwrap().clone();
        let info = app.provider_attachment_info(&terminal_id).unwrap();
        assert_eq!(info.observation_age_ms, None);
        assert_eq!(info.observation_source, None);
        assert_eq!(info.source_ledger_id, None);
        assert_eq!(info.source_epoch, None);
        // Model a previously validated observation without contacting a daemon.
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .external_observed_at = Some(Instant::now() - Duration::from_secs(2));
        app.handle_runtime_provider_detach(
            "d".into(),
            RuntimeProviderAttachmentTarget {
                terminal_id: terminal_id.as_str().into(),
            },
        );
        let retained = app.provider_attachment_info(&terminal_id).unwrap();
        assert!(!retained.attached);
        assert!(retained.observation_age_ms.unwrap() >= 2000);
        assert_eq!(
            retained.observation_source.as_deref(),
            Some(coven_client::source::CONTRACT)
        );
    }

    #[test]
    fn attachment_is_idempotent_and_detach_preserves_execution_and_layout() {
        let mut app = app(true);
        let first = result(app.handle_runtime_provider_attach("a".into(), params()));
        assert!(first.get("error").is_none(), "{first}");
        assert_eq!(app.state.terminals.len(), 1);
        let terminal_id = app.state.terminals.keys().next().unwrap().clone();
        assert!(app.terminal_runtimes.get(&terminal_id).is_none());
        assert!(app.terminal_runtimes.external(&terminal_id).is_some());
        let binding = app.state.terminals[&terminal_id]
            .external_binding
            .clone()
            .unwrap();
        let workspace_id = app.state.workspaces[0].id.clone();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let again = result(app.handle_runtime_provider_attach("b".into(), params()));
        assert!(again.get("error").is_none(), "{again}");
        assert_eq!(app.state.terminals.len(), 1);
        let mut wrong = params();
        wrong.generation = 2;
        assert!(
            result(app.handle_runtime_provider_attach("c".into(), wrong))
                .get("error")
                .is_some()
        );
        let target = RuntimeProviderAttachmentTarget {
            terminal_id: terminal_id.as_str().into(),
        };
        let detached = result(app.handle_runtime_provider_detach("d".into(), target));
        assert!(detached.get("error").is_none(), "{detached}");
        assert!(app.terminal_runtimes.external(&terminal_id).is_none());
        assert!(app.runtime_providers.deadline().is_none());
        assert_eq!(
            app.state.terminals[&terminal_id].external_binding.as_ref(),
            Some(&binding)
        );
        assert_eq!(app.state.workspaces[0].id, workspace_id);
        assert_eq!(app.state.workspaces[0].tabs[0].root_pane, pane_id);
        let reattached = result(app.handle_runtime_provider_attach("e".into(), params()));
        assert!(reattached.get("error").is_none(), "{reattached}");
        assert_ne!(
            app.state.terminals[&terminal_id]
                .external_binding
                .as_ref()
                .unwrap()
                .execution
                .runtime_generation,
            binding.execution.runtime_generation
        );
        app.shutdown_terminal_runtime(terminal_id);
    }

    #[test]
    fn transcript_pane_read_and_renderer_share_normalized_retained_text() {
        use coven_client::{
            source::{
                SourceCursor, SourceHealth, SourceIdentity, SourceLifecycle, SourceProjection,
                WriterState,
            },
            transcript::{TranscriptChunk, TranscriptContent, TranscriptReply, TranscriptResult},
        };
        let mut app = app(true);
        let attached = result(app.handle_runtime_provider_attach("attach".into(), params()));
        assert!(attached.get("error").is_none(), "{attached}");
        let terminal_id = app.state.terminals.keys().next().unwrap().clone();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let public_pane = app.public_pane_id(0, pane_id).unwrap();
        app.handle_runtime_provider_detach(
            "detach".into(),
            RuntimeProviderAttachmentTarget {
                terminal_id: terminal_id.as_str().into(),
            },
        );
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        let binding = terminal
            .external_binding
            .as_ref()
            .unwrap()
            .execution
            .clone();
        let source = SourceIdentity {
            host_id: binding.host_id.clone(),
            profile_id: binding.scope.profile_id.clone(),
            ledger_id: SourceId::new("transcript-ledger").unwrap(),
            epoch: 1,
        };
        let reply = TranscriptReply {
            contract: coven_client::transcript::CONTRACT.into(),
            source: source.clone(),
            scope: binding.scope.clone(),
            authority: binding.authority.clone(),
            session_id: binding.session_id.clone(),
            generation: binding.generation,
            health: SourceHealth {
                daemon_live: true,
                writer_state: WriterState::Healthy,
                writer_queued_bytes: None,
                writer_dropped_output_bytes: None,
            },
            projection: SourceProjection {
                lifecycle: SourceLifecycle::Running,
                exit_code: None,
                archived: false,
                dropped_output_bytes: Some(2),
            },
            result: TranscriptResult::Snapshot {
                cursor: SourceCursor {
                    ledger_id: source.ledger_id,
                    epoch: 1,
                    after_seq: 1,
                    revision: 1,
                },
                chunks: vec![TranscriptChunk {
                    seq: 1,
                    content: TranscriptContent::Text {
                        data: "visible\tcolumn\ncontrol:\x1b[31m".into(),
                        omitted_prefix_bytes: 0,
                    },
                }],
                prefix_omitted: false,
            },
        };
        let mut projection =
            crate::runtime_provider::transcript::TranscriptProjection::new(binding.clone());
        projection
            .apply(&reply, binding.runtime_generation)
            .unwrap();
        let expected = projection
            .read_lines(2)
            .iter()
            .map(|line| line.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        terminal.external_transcript = Some(projection);
        let mut buffer = ratatui::buffer::Buffer::empty(ratatui::layout::Rect::new(0, 0, 80, 4));
        crate::runtime_provider::transcript_view::render(
            terminal,
            &mut buffer,
            ratatui::layout::Rect::new(0, 0, 80, 4),
            &app.state.palette,
        );
        let rendered = (1..=2)
            .map(|y| {
                (0..80)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(rendered, expected);
        assert!(!expected.contains('\x1b'));
        assert!(!expected.contains('\t'));
        assert!(expected.contains("visible\\tcolumn"));
        let request = serde_json::from_value(serde_json::json!({ "id": "read", "method": "pane.read", "params": { "pane_id": public_pane, "source": "recent_unwrapped", "format": "ansi", "lines": 2 } })).unwrap();
        let read = result(app.handle_api_request(request));
        assert!(read.get("error").is_none(), "{read}");
        assert_eq!(read["result"]["read"]["text"], expected);
        assert_eq!(read["result"]["read"]["format"], "text");
        assert_eq!(read["result"]["read"]["truncated"], true);
        assert!(app.terminal_runtimes.get(&terminal_id).is_none());
    }
}
