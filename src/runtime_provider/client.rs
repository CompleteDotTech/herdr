use coven_client::{
    execution::{ExecutionRequest, ExecutionTarget, RequestId},
    source::{SourceCursor, SourceRead, SourceReply, MAX_EVENTS},
    terminal_control::{
        TerminalControlAction, TerminalControlClient, TerminalControlClientError,
        TerminalControlIdentity, TerminalControlOutcome, TerminalControlReply,
        TerminalMutationStatus,
    },
    ClientError, DaemonClient, DaemonEndpoint,
};

use super::{
    ProviderBinding, ProviderCommand, ProviderFailure, ProviderFailureKind, ProviderHealth,
    ProviderIdentity, ProviderOperationResult,
};

pub(super) struct CovenProviderClient {
    config: super::ProviderConfig,
    daemon: Option<DaemonClient>,
    checkpoint_connection: Option<super::checkpoint::CheckpointConnection>,
    control_identity: Option<TerminalControlIdentity>,
    control_lease: Option<ControlLease>,
    /// Test-only transport script for the terminal-control lease machine; the
    /// real path always goes through the daemon.
    #[cfg(test)]
    scripted_control: Option<std::collections::VecDeque<Result<TerminalControlReply, ProviderFailure>>>,
    #[cfg(test)]
    control_actions: Vec<TerminalControlAction>,
}

#[derive(Clone, Copy, Debug)]
struct ControlLease {
    generation: u64,
    next_mutation_seq: u64,
    last_renewed: std::time::Instant,
}

// The daemon's default owner controller intentionally uses a 30-second cap;
// keep the provider's automatic first lease within that configured bound.
const CONTROL_LEASE_DURATION_MS: u64 = 30_000;
const CONTROL_LEASE_RENEW_AFTER: std::time::Duration =
    std::time::Duration::from_millis(CONTROL_LEASE_DURATION_MS / 2);

impl CovenProviderClient {
    pub(super) fn new(config: super::ProviderConfig) -> Self {
        Self {
            config,
            daemon: None,
            checkpoint_connection: None,
            control_identity: None,
            control_lease: None,
            #[cfg(test)]
            scripted_control: None,
            #[cfg(test)]
            control_actions: Vec::new(),
        }
    }

    pub(super) fn reset(&mut self) {
        self.daemon = None;
        self.checkpoint_connection = None;
        self.control_identity = None;
        self.control_lease = None;
    }

    pub(super) fn execute(
        &mut self,
        command: &ProviderCommand,
        runtime_generation: u64,
    ) -> ProviderOperationResult {
        let result = match command {
            ProviderCommand::Health => self.health().map(ProviderOperationResult::Health),
            ProviderCommand::ReadSource {
                binding,
                cursor,
                limit,
            } => self
                .read_source(binding, cursor.clone(), *limit, runtime_generation)
                .map(ProviderOperationResult::Source),
            ProviderCommand::ReadTranscript {
                binding,
                cursor,
                limit,
            } => self
                .read_transcript(binding, cursor.clone(), *limit, runtime_generation)
                .map(|reply| ProviderOperationResult::Transcript(std::sync::Arc::new(reply))),
            ProviderCommand::AttachCheckpoint {
                binding,
                attachment_id,
                checkpoint,
            } => self
                .attach_checkpoint(
                    binding,
                    attachment_id,
                    checkpoint.as_ref(),
                    runtime_generation,
                )
                .map(|attached| {
                    ProviderOperationResult::CheckpointAttached(std::sync::Arc::new(attached))
                }),
            ProviderCommand::ReadCheckpoint { binding } => self
                .read_checkpoint(binding, runtime_generation)
                .map(|update| ProviderOperationResult::Checkpoint(std::sync::Arc::new(update))),
            ProviderCommand::TerminalTakeover { binding } => self
                .terminal_takeover(binding, runtime_generation)
                .map(|reply| ProviderOperationResult::TerminalControl(std::sync::Arc::new(reply))),
            ProviderCommand::TerminalInput { binding, bytes } => self
                .terminal_input(binding, bytes, runtime_generation)
                .map(|reply| ProviderOperationResult::TerminalControl(std::sync::Arc::new(reply))),
            ProviderCommand::TerminalResize {
                binding,
                rows,
                cols,
                pixel_width,
                pixel_height,
            } => self
                .terminal_resize(
                    binding,
                    *rows,
                    *cols,
                    *pixel_width,
                    *pixel_height,
                    runtime_generation,
                )
                .map(|reply| ProviderOperationResult::TerminalControl(std::sync::Arc::new(reply))),
            ProviderCommand::Execution { binding, request } => self
                .execution(binding.as_ref(), request, runtime_generation)
                .map(ProviderOperationResult::Execution),
            ProviderCommand::LookupExecution {
                binding,
                request_id,
                expected_digest,
                expected_target,
            } => self
                .lookup_execution(
                    binding.as_ref(),
                    request_id,
                    expected_digest,
                    expected_target,
                    runtime_generation,
                )
                .map(ProviderOperationResult::LookupExecution),
        };
        result.unwrap_or_else(ProviderOperationResult::Failure)
    }

    fn daemon(&mut self) -> Result<&mut DaemonClient, ProviderFailure> {
        if self.daemon.is_none() {
            let endpoint =
                DaemonEndpoint::discover(self.config.coven_home()).map_err(map_client_error)?;
            self.daemon = Some(DaemonClient::new(endpoint));
        }
        self.daemon.as_mut().ok_or_else(|| {
            ProviderFailure::new(
                ProviderFailureKind::Disconnected,
                "Coven daemon client was not initialized",
            )
        })
    }

    fn health(&mut self) -> Result<ProviderHealth, ProviderFailure> {
        let health = match self.daemon()?.health() {
            Ok(health) => health,
            Err(error) => {
                let failure = map_client_error(error);
                self.reset_if_disconnected(&failure);
                return Err(failure);
            }
        };
        let projected = match ProviderHealth::try_from(health) {
            Ok(projected) => projected,
            Err(_) => {
                // DaemonClient retains negotiated capability vectors after a
                // successful health exchange. Drop it when the projection
                // bound is exceeded so oversized/untrusted fields are not
                // kept alive by the worker.
                self.reset();
                return Err(ProviderFailure::new(
                    ProviderFailureKind::Rejected,
                    "Coven daemon health projection exceeded its bound",
                ));
            }
        };
        if projected.authority.as_ref() != Some(&self.config.identity.authority) {
            self.daemon = None;
            return Err(ProviderFailure::new(
                ProviderFailureKind::IdentityMismatch,
                "Coven daemon execution authority does not match the trusted provider binding",
            ));
        }
        Ok(projected)
    }

    fn ensure_health(&mut self) -> Result<(), ProviderFailure> {
        self.health().map(|_| ())
    }

    fn validate_binding(
        &self,
        binding: &ProviderBinding,
        runtime_generation: u64,
    ) -> Result<(), ProviderFailure> {
        if !binding.matches_identity(&self.config.identity)
            || binding.runtime_generation != runtime_generation
        {
            return Err(ProviderFailure::new(
                ProviderFailureKind::IdentityMismatch,
                "Coven provider binding identity or runtime generation did not match",
            ));
        }
        Ok(())
    }

    fn read_source(
        &mut self,
        binding: &ProviderBinding,
        cursor: Option<SourceCursor>,
        limit: u16,
        runtime_generation: u64,
    ) -> Result<SourceReply, ProviderFailure> {
        self.validate_binding(binding, runtime_generation)?;
        if limit == 0 || limit > MAX_EVENTS {
            return Err(ProviderFailure::new(
                ProviderFailureKind::Rejected,
                "source read limit is outside the Coven client bound",
            ));
        }
        self.ensure_health()?;
        let request = SourceRead {
            contract: coven_client::source::CONTRACT.to_owned(),
            host_id: binding.host_id.clone(),
            scope: binding.scope.clone(),
            session_id: binding.session_id.clone(),
            generation: binding.generation,
            cursor,
            limit,
        };
        request.validate().map_err(|error| {
            ProviderFailure::new(ProviderFailureKind::Rejected, error.to_string())
        })?;
        let result = self.daemon()?.read_execution_source(&request);
        match result {
            Ok(reply) => {
                if binding
                    .pinned_source
                    .as_ref()
                    .is_some_and(|expected| *expected != reply.source)
                {
                    self.reset();
                    return Err(ProviderFailure::new(
                        ProviderFailureKind::IdentityMismatch,
                        "Coven source identity changed for the bound execution",
                    ));
                }
                Ok(reply)
            }
            Err(error) => {
                let failure = map_client_error(error);
                self.reset_if_disconnected(&failure);
                Err(failure)
            }
        }
    }

    fn read_transcript(
        &mut self,
        binding: &ProviderBinding,
        cursor: Option<SourceCursor>,
        limit: u16,
        runtime_generation: u64,
    ) -> Result<coven_client::transcript::TranscriptReply, ProviderFailure> {
        self.validate_binding(binding, runtime_generation)?;
        if limit == 0 || limit > coven_client::transcript::MAX_CHUNKS {
            return Err(ProviderFailure::new(
                ProviderFailureKind::Rejected,
                "transcript read limit is outside the Coven client bound",
            ));
        }
        self.ensure_health()?;
        let request = coven_client::transcript::TranscriptRead {
            contract: coven_client::transcript::CONTRACT.to_owned(),
            host_id: binding.host_id.clone(),
            scope: binding.scope.clone(),
            authority: binding.authority.clone(),
            session_id: binding.session_id.clone(),
            generation: binding.generation,
            cursor,
            limit,
        };
        request.validate().map_err(|error| {
            ProviderFailure::new(ProviderFailureKind::Rejected, error.to_string())
        })?;
        let result = self.daemon()?.read_execution_transcript(&request);
        match result {
            Ok(reply) => {
                if binding
                    .pinned_source
                    .as_ref()
                    .is_some_and(|expected| *expected != reply.source)
                {
                    self.reset();
                    return Err(ProviderFailure::new(
                        ProviderFailureKind::IdentityMismatch,
                        "Coven source identity changed for the bound execution",
                    ));
                }
                Ok(reply)
            }
            Err(error) => {
                let failure = map_client_error(error);
                self.reset_if_disconnected(&failure);
                Err(failure)
            }
        }
    }

    fn attach_checkpoint(
        &mut self,
        binding: &ProviderBinding,
        attachment_id: &str,
        checkpoint: Option<&crate::terminal::backend::ExternalCheckpointState>,
        runtime_generation: u64,
    ) -> Result<super::ProviderCheckpointAttachment, ProviderFailure> {
        self.validate_binding(binding, runtime_generation)?;
        self.ensure_health()?;
        let connection = super::checkpoint::CheckpointConnection::attach(
            self.daemon()?,
            binding,
            attachment_id,
            checkpoint,
        )?;
        let attached = connection.attachment();
        let control_identity = attached
            .response
            .control_identity()
            .map_err(map_client_error)?;
        self.checkpoint_connection = Some(connection);
        self.control_identity = Some(control_identity);
        self.control_lease = None;
        Ok(attached)
    }

    fn terminal_input(
        &mut self,
        binding: &ProviderBinding,
        bytes: &[u8],
        runtime_generation: u64,
    ) -> Result<TerminalControlReply, ProviderFailure> {
        self.terminal_mutation(
            binding,
            TerminalControlAction::Input {
                lease_generation: 0,
                mutation_seq: 0,
                bytes: bytes.to_vec(),
            },
            runtime_generation,
        )
    }

    fn terminal_takeover(
        &mut self,
        binding: &ProviderBinding,
        runtime_generation: u64,
    ) -> Result<TerminalControlReply, ProviderFailure> {
        self.validate_binding(binding, runtime_generation)?;
        let identity = self.control_identity.clone().ok_or_else(|| {
            ProviderFailure::new(
                ProviderFailureKind::Unsupported,
                "terminal control was not negotiated for this attachment",
            )
        })?;
        self.ensure_health()?;
        let reply = self.send_terminal_control(
            identity,
            TerminalControlAction::takeover(CONTROL_LEASE_DURATION_MS),
        )?;
        let TerminalControlOutcome::TakenOver {
            lease_generation,
            next_mutation_seq,
        } = &reply.outcome
        else {
            return Err(ProviderFailure::new(
                ProviderFailureKind::Rejected,
                "Coven returned an unexpected terminal takeover response",
            ));
        };
        self.control_lease = Some(ControlLease {
            generation: *lease_generation,
            next_mutation_seq: *next_mutation_seq,
            last_renewed: std::time::Instant::now(),
        });
        Ok(reply)
    }

    fn terminal_resize(
        &mut self,
        binding: &ProviderBinding,
        rows: u16,
        cols: u16,
        pixel_width: u16,
        pixel_height: u16,
        runtime_generation: u64,
    ) -> Result<TerminalControlReply, ProviderFailure> {
        self.terminal_mutation(
            binding,
            TerminalControlAction::Resize {
                lease_generation: 0,
                mutation_seq: 0,
                rows,
                cols,
                pixel_width,
                pixel_height,
            },
            runtime_generation,
        )
    }

    /// Return a usable control lease, renewing an about-to-expire one and
    /// re-acquiring an expired one. `Acquire` only succeeds when no other
    /// unexpired controller owns the terminal, so a renewal failure after an
    /// idle gap is recovered without silently dropping the admitted mutation.
    fn ensure_control_lease(
        &mut self,
        identity: &TerminalControlIdentity,
    ) -> Result<ControlLease, ProviderFailure> {
        if let Some(lease) = self.control_lease {
            if lease.last_renewed.elapsed() < CONTROL_LEASE_RENEW_AFTER {
                return Ok(lease);
            }
            let renewed = self.send_terminal_control(
                identity.clone(),
                TerminalControlAction::renew(lease.generation, CONTROL_LEASE_DURATION_MS),
            );
            match renewed {
                Ok(reply) => {
                    let TerminalControlOutcome::Renewed {
                        lease_generation,
                        next_mutation_seq,
                    } = reply.outcome
                    else {
                        self.control_lease = None;
                        return Err(ProviderFailure::new(
                            ProviderFailureKind::Rejected,
                            "Coven returned an unexpected terminal lease renewal response",
                        ));
                    };
                    let lease = ControlLease {
                        generation: lease_generation,
                        next_mutation_seq,
                        last_renewed: std::time::Instant::now(),
                    };
                    self.control_lease = Some(lease);
                    return Ok(lease);
                }
                Err(error) => {
                    self.control_lease = None;
                    if self.control_identity.is_none() {
                        // The transport was reset (disconnect or identity
                        // mismatch) and the control identity dropped with it.
                        // Re-acquiring from the stale clone would leave the
                        // pending-mutation poll without an identity, so fail
                        // closed instead of dispatching.
                        return Err(error);
                    }
                    // The daemon rejected the renewal because the lease
                    // expired while the worker was idle. Fall through to a
                    // fresh acquire: a different unexpired controller still
                    // fences us out, but our own expiry does not discard the
                    // user's admitted mutation.
                }
            }
        }
        self.acquire_control_lease(identity)
    }

    fn acquire_control_lease(
        &mut self,
        identity: &TerminalControlIdentity,
    ) -> Result<ControlLease, ProviderFailure> {
        let acquired = self.send_terminal_control(
            identity.clone(),
            TerminalControlAction::acquire(CONTROL_LEASE_DURATION_MS),
        )?;
        let TerminalControlOutcome::Acquired {
            lease_generation,
            next_mutation_seq,
        } = acquired.outcome
        else {
            return Err(ProviderFailure::new(
                ProviderFailureKind::Rejected,
                "Coven returned an unexpected terminal lease response",
            ));
        };
        let lease = ControlLease {
            generation: lease_generation,
            next_mutation_seq,
            last_renewed: std::time::Instant::now(),
        };
        self.control_lease = Some(lease);
        Ok(lease)
    }

    /// Acquire a lease only as part of a user-admitted mutation. The worker is
    /// the sole owner of lease and mutation sequencing, so concurrent UI
    /// requests cannot race or reuse a sequence. Unknown transport outcomes
    /// are returned without retrying or replaying the bytes.
    fn terminal_mutation(
        &mut self,
        binding: &ProviderBinding,
        action: TerminalControlAction,
        runtime_generation: u64,
    ) -> Result<TerminalControlReply, ProviderFailure> {
        self.validate_binding(binding, runtime_generation)?;
        if !matches!(
            action,
            TerminalControlAction::Input { .. } | TerminalControlAction::Resize { .. }
        ) {
            return Err(ProviderFailure::new(
                ProviderFailureKind::Rejected,
                "unsupported terminal mutation",
            ));
        }
        let identity = self.control_identity.clone().ok_or_else(|| {
            ProviderFailure::new(
                ProviderFailureKind::Unsupported,
                "terminal control was not negotiated for this attachment",
            )
        })?;
        self.ensure_health()?;
        let lease = self.ensure_control_lease(&identity)?;
        let sequence = lease.next_mutation_seq;
        let action = match action {
            TerminalControlAction::Input { bytes, .. } => {
                TerminalControlAction::input(lease.generation, sequence, bytes)
            }
            TerminalControlAction::Resize {
                rows,
                cols,
                pixel_width,
                pixel_height,
                ..
            } => TerminalControlAction::resize(
                lease.generation,
                sequence,
                rows,
                cols,
                pixel_width,
                pixel_height,
            ),
            _ => unreachable!("terminal mutation action validated above"),
        };
        let mut reply = self.send_terminal_control(identity, action)?;
        let pending = matches!(
            &reply.outcome,
            TerminalControlOutcome::Mutation {
                status: TerminalMutationStatus::Pending,
                ..
            }
        );
        if let TerminalControlOutcome::Mutation {
            mutation_seq,
            status,
            ..
        } = &reply.outcome
        {
            if *mutation_seq == sequence && !matches!(status, TerminalMutationStatus::Rejected) {
                self.control_lease = Some(ControlLease {
                    generation: lease.generation,
                    next_mutation_seq: sequence.saturating_add(1),
                    last_renewed: lease.last_renewed,
                });
            }
            if matches!(status, TerminalMutationStatus::Stopped) {
                self.control_lease = None;
            }
        }
        // A successful enqueue is not proof that the PTY observed the bytes.
        // Poll the same idempotent receipt for a short bounded interval so a
        // user-facing call normally reflects writer completion, while never
        // replaying the mutation or converting an uncertain transport result
        // into another input request.
        if pending {
            for _ in 0..50 {
                std::thread::sleep(std::time::Duration::from_millis(20));
                let polled = self.send_terminal_control(
                    self.control_identity
                        .clone()
                        .expect("control identity retained while polling"),
                    TerminalControlAction::poll(sequence),
                );
                let Ok(polled) = polled else {
                    break;
                };
                if !matches!(
                    polled.outcome,
                    TerminalControlOutcome::Polled {
                        status: TerminalMutationStatus::Pending,
                        ..
                    }
                ) {
                    // Return the terminal receipt observed by the idempotent
                    // poll when it settled. Never submit the mutation again;
                    // the original sequence remains the sole side effect key.
                    reply = polled;
                    break;
                }
            }
        }
        Ok(reply)
    }

    fn send_terminal_control(
        &mut self,
        identity: TerminalControlIdentity,
        action: TerminalControlAction,
    ) -> Result<TerminalControlReply, ProviderFailure> {
        #[cfg(test)]
        if let Some(script) = self.scripted_control.as_mut() {
            self.control_actions.push(action.clone());
            return script.pop_front().unwrap_or_else(|| {
                Err(ProviderFailure::new(
                    ProviderFailureKind::Failed,
                    "scripted terminal control is exhausted",
                ))
            });
        }
        let request = coven_client::terminal_control::TerminalControlRequest::new(identity, action);
        let result = self.daemon().and_then(|daemon| {
            daemon
                .terminal_control(&request)
                .map_err(map_terminal_control_error)
        });
        match result {
            Ok(reply) => Ok(reply),
            Err(failure) => {
                self.reset_if_disconnected(&failure);
                Err(failure)
            }
        }
    }

    fn read_checkpoint(
        &mut self,
        binding: &ProviderBinding,
        runtime_generation: u64,
    ) -> Result<super::ProviderCheckpointUpdate, ProviderFailure> {
        self.validate_binding(binding, runtime_generation)?;
        self.checkpoint_connection
            .as_mut()
            .ok_or_else(|| {
                ProviderFailure::new(
                    ProviderFailureKind::Disconnected,
                    "Coven checkpoint stream is not attached",
                )
            })?
            .read()
    }

    fn execution(
        &mut self,
        binding: Option<&ProviderBinding>,
        request: &ExecutionRequest,
        runtime_generation: u64,
    ) -> Result<coven_client::execution::ExecutionOutcome, ProviderFailure> {
        validate_execution_binding(&self.config.identity, binding, request, runtime_generation)?;
        self.ensure_health()?;
        let result = self.daemon()?.execution_request(request);
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let failure = map_client_error(error);
                self.reset_if_disconnected(&failure);
                Err(failure)
            }
        }
    }

    fn lookup_execution(
        &mut self,
        binding: Option<&ProviderBinding>,
        request_id: &RequestId,
        expected_digest: &str,
        expected_target: &ExecutionTarget,
        runtime_generation: u64,
    ) -> Result<coven_client::execution::ExecutionOutcome, ProviderFailure> {
        validate_lookup_binding(
            &self.config.identity,
            binding,
            expected_target,
            runtime_generation,
        )?;
        self.ensure_health()?;
        let scope = self.config.identity.scope.clone();
        let result = self.daemon()?.lookup_execution_request(
            &scope,
            request_id,
            expected_digest,
            expected_target,
        );
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let failure = map_client_error(error);
                self.reset_if_disconnected(&failure);
                Err(failure)
            }
        }
    }

    fn reset_if_disconnected(&mut self, failure: &ProviderFailure) {
        if matches!(
            failure.kind,
            ProviderFailureKind::Disconnected | ProviderFailureKind::IdentityMismatch
        ) {
            self.reset();
        }
    }
}

fn validate_execution_binding(
    identity: &ProviderIdentity,
    binding: Option<&ProviderBinding>,
    request: &ExecutionRequest,
    runtime_generation: u64,
) -> Result<(), ProviderFailure> {
    if request.scope != identity.scope || request.authority != identity.authority {
        return Err(ProviderFailure::new(
            ProviderFailureKind::IdentityMismatch,
            "Coven execution request scope or authority did not match the provider",
        ));
    }
    match (binding, request.intent.target()) {
        (None, ExecutionTarget::Launch) => Ok(()),
        (None, _) => Err(ProviderFailure::new(
            ProviderFailureKind::IdentityMismatch,
            "an existing Coven execution requires a bound provider session",
        )),
        (Some(binding), target) => {
            if binding.runtime_generation != runtime_generation
                || !binding.matches_identity(identity)
            {
                return Err(ProviderFailure::new(
                    ProviderFailureKind::IdentityMismatch,
                    "Coven execution binding identity or runtime generation did not match",
                ));
            }
            validate_target_binding(binding, &target)
        }
    }
}

fn validate_lookup_binding(
    identity: &ProviderIdentity,
    binding: Option<&ProviderBinding>,
    target: &ExecutionTarget,
    runtime_generation: u64,
) -> Result<(), ProviderFailure> {
    match (binding, target) {
        (None, ExecutionTarget::Launch) => Ok(()),
        (None, _) => Err(ProviderFailure::new(
            ProviderFailureKind::IdentityMismatch,
            "an existing Coven lookup requires a bound provider session",
        )),
        (Some(binding), target) => {
            if binding.runtime_generation != runtime_generation
                || !binding.matches_identity(identity)
            {
                return Err(ProviderFailure::new(
                    ProviderFailureKind::IdentityMismatch,
                    "Coven lookup binding identity or runtime generation did not match",
                ));
            }
            validate_target_binding(binding, target)
        }
    }
}

fn validate_target_binding(
    binding: &ProviderBinding,
    target: &ExecutionTarget,
) -> Result<(), ProviderFailure> {
    let matches = match target {
        ExecutionTarget::Launch => false,
        ExecutionTarget::Existing {
            session_id,
            generation,
        } => session_id == &binding.session_id && *generation == binding.generation,
        ExecutionTarget::Continuation {
            previous_session_id,
            previous_generation,
        } => {
            previous_session_id == &binding.session_id && *previous_generation == binding.generation
        }
    };
    if matches {
        Ok(())
    } else {
        Err(ProviderFailure::new(
            ProviderFailureKind::IdentityMismatch,
            "Coven execution target did not match the bound session generation",
        ))
    }
}

pub(super) fn map_client_error(error: ClientError) -> ProviderFailure {
    let kind = match &error {
        ClientError::CapabilityUnavailable { .. } | ClientError::UnsupportedPlatform => {
            ProviderFailureKind::Unsupported
        }
        ClientError::DaemonInstanceChanged => ProviderFailureKind::IdentityMismatch,
        ClientError::Discovery(_)
        | ClientError::Io { .. }
        | ClientError::HealthNotReady
        | ClientError::ProtocolVersion { .. } => ProviderFailureKind::Disconnected,
        ClientError::InvalidTranscriptRequest(_)
        | ClientError::InvalidTranscriptResponse(_)
        | ClientError::InvalidCatalogRequest(_)
        | ClientError::InvalidCatalogResponse(_)
        | ClientError::InvalidExecutionRequest(_)
        | ClientError::InvalidExecutionResponse(_)
        | ClientError::InvalidSourceResponse(_)
        | ClientError::RequestTooLarge { .. }
        | ClientError::ResponseTooLarge { .. }
        | ClientError::InvalidHttpResponse(_)
        | ClientError::InvalidUtf8(_)
        | ClientError::InvalidJson(_)
        | ClientError::StructuredErrorsUnavailable
        | ClientError::InvalidRouteParameter(_)
        | ClientError::LegacyShutdownUpgradeRequired { .. } => ProviderFailureKind::Rejected,
        ClientError::Daemon { .. } | ClientError::HttpStatus(_) => ProviderFailureKind::Failed,
    };
    ProviderFailure::new(kind, safe_client_error_message(&error))
}

fn map_terminal_control_error(error: TerminalControlClientError) -> ProviderFailure {
    match error {
        TerminalControlClientError::Transport(error) => map_client_error(error),
        TerminalControlClientError::InvalidRequest(_)
        | TerminalControlClientError::InvalidResponse(_)
        | TerminalControlClientError::Json(_)
        | TerminalControlClientError::RequestTooLarge { .. }
        | TerminalControlClientError::ResponseTooLarge { .. } => ProviderFailure::new(
            ProviderFailureKind::Rejected,
            "Coven terminal control response or request was invalid",
        ),
    }
}

/// Keep provider diagnostics useful without forwarding owner-local paths,
/// transport error text, response bodies, or daemon supplied messages to a
/// caller.  The typed failure kind above carries the machine-readable result;
/// this message is only a bounded human-facing summary.
fn safe_client_error_message(error: &ClientError) -> &'static str {
    match error {
        ClientError::InvalidTranscriptRequest(_) => "Coven transcript request was invalid",
        ClientError::InvalidTranscriptResponse(_) => {
            "Coven daemon returned an invalid transcript response"
        }
        ClientError::InvalidCatalogRequest(_) => "Coven catalog request was invalid",
        ClientError::InvalidCatalogResponse(_) => {
            "Coven daemon returned an invalid catalog response"
        }
        ClientError::InvalidExecutionRequest(_) => "Coven execution request was invalid",
        ClientError::InvalidExecutionResponse(_) => {
            "Coven daemon returned an invalid execution response"
        }
        ClientError::InvalidSourceResponse(_) => "Coven daemon returned an invalid source response",
        ClientError::Discovery(_) => "Coven daemon discovery failed",
        ClientError::Io { .. } => "Coven daemon transport failed",
        ClientError::ResponseTooLarge { .. } => "Coven daemon response was too large",
        ClientError::RequestTooLarge { .. } => "Coven daemon request was too large",
        ClientError::InvalidHttpResponse(_) => "Coven daemon returned an invalid HTTP response",
        ClientError::InvalidUtf8(_) => "Coven daemon returned invalid UTF-8",
        ClientError::InvalidJson(_) => "Coven daemon returned invalid JSON",
        ClientError::ProtocolVersion { .. } => "Coven daemon protocol version is incompatible",
        ClientError::StructuredErrorsUnavailable => {
            "Coven daemon does not support structured errors"
        }
        ClientError::CapabilityUnavailable { .. } => {
            "Coven daemon does not advertise the required capability"
        }
        ClientError::HealthNotReady => "Coven daemon is not ready",
        ClientError::DaemonInstanceChanged => "Coven daemon identity changed",
        ClientError::Daemon { .. } => "Coven daemon rejected the request",
        ClientError::HttpStatus(_) => "Coven daemon returned an HTTP error",
        ClientError::InvalidRouteParameter(_) => "Coven daemon route parameter was invalid",
        ClientError::LegacyShutdownUpgradeRequired { .. } => {
            "Coven daemon requires an upgrade for this operation"
        }
        ClientError::UnsupportedPlatform => "Coven provider is unsupported on this platform",
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn lease_config() -> super::super::ProviderConfig {
        use coven_client::{
            execution::{AuthorityId, ExecutionAuthority, ExecutionScope, ProfileId, ProjectId},
            source::SourceId,
        };
        let identity = super::super::ProviderIdentity::new(
            super::super::ProviderId::new("lease-fixture").unwrap(),
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
            std::path::PathBuf::from("/definitely/missing/coven-home"),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .unwrap()
    }

    fn lease_identity() -> TerminalControlIdentity {
        TerminalControlIdentity {
            session_id: "session".to_owned(),
            stream_id: "stream".to_owned(),
            stream_generation: 1,
            execution_generation: 1,
            authority_epoch: 1,
            attachment_id: "attachment".to_owned(),
        }
    }

    fn control_reply(outcome: TerminalControlOutcome) -> TerminalControlReply {
        TerminalControlReply {
            session_id: "session".to_owned(),
            stream_id: "stream".to_owned(),
            stream_generation: 1,
            execution_generation: 1,
            authority_epoch: 1,
            attachment_id: "attachment".to_owned(),
            outcome,
        }
    }

    #[test]
    fn expired_lease_is_reacquired_instead_of_dropping_the_mutation() {
        let identity = lease_identity();
        let mut client = CovenProviderClient::new(lease_config());
        client.control_identity = Some(identity.clone());
        client.control_lease = Some(ControlLease {
            generation: 7,
            next_mutation_seq: 4,
            last_renewed: std::time::Instant::now()
                - CONTROL_LEASE_RENEW_AFTER
                - std::time::Duration::from_secs(20),
        });
        client.scripted_control = Some(std::collections::VecDeque::from(vec![
            Err(ProviderFailure::new(
                ProviderFailureKind::Rejected,
                "lease expired",
            )),
            Ok(control_reply(TerminalControlOutcome::Acquired {
                lease_generation: 8,
                next_mutation_seq: 1,
            })),
        ]));
        let lease = client
            .ensure_control_lease(&identity)
            .expect("lease reacquired after expiry");
        assert_eq!(lease.generation, 8);
        assert_eq!(lease.next_mutation_seq, 1);
        assert!(matches!(
            client.control_actions[0],
            TerminalControlAction::Renew {
                lease_generation: 7,
                ..
            }
        ));
        assert!(matches!(
            client.control_actions[1],
            TerminalControlAction::Acquire { .. }
        ));
        assert_eq!(client.control_actions.len(), 2);
    }

    #[test]
    fn disconnected_renewal_does_not_reacquire_without_an_identity() {
        let identity = lease_identity();
        let mut client = CovenProviderClient::new(lease_config());
        // A disconnected send resets the client and clears the identity; the
        // guard must then fail closed rather than re-acquire from the clone.
        client.control_identity = None;
        client.control_lease = Some(ControlLease {
            generation: 7,
            next_mutation_seq: 4,
            last_renewed: std::time::Instant::now()
                - CONTROL_LEASE_RENEW_AFTER
                - std::time::Duration::from_secs(20),
        });
        client.scripted_control = Some(std::collections::VecDeque::from(vec![Err(
            ProviderFailure::new(ProviderFailureKind::Disconnected, "transport lost"),
        )]));
        let error = client
            .ensure_control_lease(&identity)
            .expect_err("reset renewal must fail closed");
        assert_eq!(error.kind, ProviderFailureKind::Disconnected);
        assert_eq!(client.control_actions.len(), 1);
        assert!(matches!(
            client.control_actions[0],
            TerminalControlAction::Renew { .. }
        ));
        assert!(client.control_lease.is_none());
    }

    #[test]
    fn fresh_lease_is_used_without_a_renewal_round_trip() {
        let identity = lease_identity();
        let mut client = CovenProviderClient::new(lease_config());
        client.control_identity = Some(identity.clone());
        client.control_lease = Some(ControlLease {
            generation: 3,
            next_mutation_seq: 9,
            last_renewed: std::time::Instant::now(),
        });
        let lease = client.ensure_control_lease(&identity).unwrap();
        assert_eq!(lease.generation, 3);
        assert_eq!(lease.next_mutation_seq, 9);
        assert!(client.control_actions.is_empty());
    }

    #[test]
    fn provider_failures_do_not_forward_owner_local_error_text() {
        let discovery = map_client_error(ClientError::Discovery(
            "/owner/private/coven-home/coven.sock is not a socket".to_owned(),
        ));
        assert_eq!(discovery.kind, ProviderFailureKind::Disconnected);
        assert!(!discovery.message.contains("/owner/private"));

        let transport = map_client_error(ClientError::Io {
            operation: "failed to connect to Coven daemon socket",
            source: io::Error::new(
                io::ErrorKind::PermissionDenied,
                "/owner/private/coven-home/coven.sock",
            ),
        });
        assert_eq!(transport.kind, ProviderFailureKind::Disconnected);
        assert!(!transport.message.contains("/owner/private"));
    }
}
