use coven_client::{
    execution::{ExecutionRequest, ExecutionTarget, RequestId},
    source::{SourceCursor, SourceRead, SourceReply, MAX_EVENTS},
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
}

impl CovenProviderClient {
    pub(super) fn new(config: super::ProviderConfig) -> Self {
        Self {
            config,
            daemon: None,
            checkpoint_connection: None,
        }
    }

    pub(super) fn reset(&mut self) {
        self.daemon = None;
        self.checkpoint_connection = None;
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
        self.checkpoint_connection = Some(connection);
        Ok(attached)
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
