//! A bounded, server-owned seam for executions backed by a local Coven daemon.
//!
//! The native terminal runtime owns a PTY and therefore exposes process and
//! handoff operations.  A Coven execution has none of those properties.  This
//! module keeps the provider handle, its durable binding, and its immutable
//! projection separate so callers can add an external backend without making
//! up a PID, PTY, or shell command.

mod actor;
mod checkpoint;
mod client;
pub(crate) use checkpoint::{ProviderCheckpointAttachment, ProviderCheckpointUpdate};
mod settings;
pub(crate) mod transcript;
pub(crate) mod transcript_view;

#[cfg(test)]
mod tests;

use std::{
    fmt,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use coven_client::{
    execution::{
        ExecutionAuthority, ExecutionOutcome, ExecutionScope, ExecutionSessionId, ExecutionTarget,
        RequestId,
    },
    source::{SourceCursor, SourceId, SourceIdentity, SourceReply},
};

pub(crate) use actor::{ProviderRuntime, ProviderRuntimeHandle};
pub(crate) use settings::OwnerProviderSettings;

/// Hard cap for both the blocking command queue and the nonblocking update
/// queue.  Callers may choose a smaller capacity for their workload.
pub(crate) const MAX_PROVIDER_QUEUE_CAPACITY: usize = 256;

// Health is an owner-local projection, so it must not retain arbitrary data
// from a daemon response even though the transport has a larger body bound.
// These limits are deliberately well above the currently published Coven
// capability set while keeping snapshots and queued updates small.
const MAX_HEALTH_LABEL_BYTES: usize = 128;
const MAX_HEALTH_CAPABILITIES: usize = 32;

/// A local, validated name for one configured provider instance.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ProviderId(String);

impl ProviderId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, ProviderConfigError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(ProviderConfigError::InvalidField("provider_id"));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The identity that must remain stable while a provider actor is serving
/// requests.  It intentionally contains no daemon path or credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderIdentity {
    pub(crate) provider_id: ProviderId,
    pub(crate) host_id: SourceId,
    pub(crate) scope: ExecutionScope,
    pub(crate) authority: ExecutionAuthority,
}

impl ProviderIdentity {
    pub(crate) fn new(
        provider_id: ProviderId,
        host_id: SourceId,
        scope: ExecutionScope,
        authority: ExecutionAuthority,
    ) -> Result<Self, ProviderConfigError> {
        scope
            .validate()
            .map_err(|_| ProviderConfigError::InvalidField("scope"))?;
        ExecutionAuthority::new(authority.id.clone(), authority.generation)
            .map_err(|_| ProviderConfigError::InvalidField("authority"))?;
        Ok(Self {
            provider_id,
            host_id,
            scope,
            authority,
        })
    }
}

/// Local trusted configuration.  It is deliberately constructed by code that
/// already selected the owner-local Coven home and profile; no remote request
/// may supply a daemon endpoint, executable, environment, or credential path.
#[derive(Clone)]
pub(crate) struct ProviderConfig {
    pub(crate) identity: ProviderIdentity,
    coven_home: PathBuf,
    project_root: PathBuf,
    queue_capacity: NonZeroUsize,
    actions_allowed: bool,
}

impl fmt::Debug for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderConfig")
            .field("identity", &self.identity)
            .field("coven_home", &"<owner-local>")
            .field("project_root", &"<owner-local>")
            .field("queue_capacity", &self.queue_capacity)
            .field("actions_allowed", &self.actions_allowed)
            .finish()
    }
}

impl ProviderConfig {
    pub(crate) fn new(
        identity: ProviderIdentity,
        coven_home: PathBuf,
        queue_capacity: NonZeroUsize,
    ) -> Result<Self, ProviderConfigError> {
        if coven_home.as_os_str().is_empty()
            || coven_home.as_os_str().to_string_lossy().contains('\0')
        {
            return Err(ProviderConfigError::InvalidField("coven_home"));
        }
        if queue_capacity.get() > MAX_PROVIDER_QUEUE_CAPACITY {
            return Err(ProviderConfigError::InvalidField("queue_capacity"));
        }
        Ok(Self {
            identity,
            project_root: coven_home.clone(),
            coven_home,
            queue_capacity,
            actions_allowed: false,
        })
    }

    pub(crate) fn coven_home(&self) -> &Path {
        &self.coven_home
    }

    /// Set the owner-selected project root used to resolve managed launch
    /// working directories. Remote requests may select a directory beneath
    /// this root, but cannot replace the root itself or escape it.
    pub(crate) fn with_project_root(
        mut self,
        project_root: PathBuf,
    ) -> Result<Self, ProviderConfigError> {
        if project_root.as_os_str().is_empty()
            || !project_root.is_absolute()
            || project_root.as_os_str().len() > 4096
            || project_root.as_os_str().to_string_lossy().contains('\0')
        {
            return Err(ProviderConfigError::InvalidField("project_root"));
        }
        self.project_root = project_root;
        Ok(self)
    }

    pub(crate) fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub(crate) fn queue_capacity(&self) -> NonZeroUsize {
        self.queue_capacity
    }

    /// Permission-bearing execution methods require an explicit owner-local
    /// opt-in. Discovery and read-only attachment remain available without it.
    pub(crate) fn with_actions_allowed(mut self, allowed: bool) -> Self {
        self.actions_allowed = allowed;
        self
    }

    pub(crate) fn actions_allowed(&self) -> bool {
        self.actions_allowed
    }
}

/// A durable managed execution binding.  The first six fields are suitable for
/// persistence; `runtime_generation` is a process-local Herdr fence and should
/// be refreshed when a binding is restored into a new actor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManagedBinding {
    pub(crate) provider_id: String,
    pub(crate) host_id: SourceId,
    pub(crate) scope: ExecutionScope,
    pub(crate) authority: ExecutionAuthority,
    pub(crate) session_id: ExecutionSessionId,
    pub(crate) generation: u64,
    pub(crate) runtime_generation: u64,
    /// Once observed, a source ledger can be pinned to prevent a replacement
    /// execution with the same session ID from being mistaken for this one.
    pub(crate) pinned_source: Option<SourceIdentity>,
}

/// Internal spelling retained for command APIs; it is the same persisted
/// binding type so the app cannot accidentally maintain two contracts.
pub(crate) type ProviderBinding = ManagedBinding;

impl ManagedBinding {
    pub(crate) fn new(
        provider_id: impl Into<String>,
        host_id: SourceId,
        scope: ExecutionScope,
        authority: ExecutionAuthority,
        session_id: ExecutionSessionId,
        generation: u64,
        runtime_generation: u64,
    ) -> Result<Self, ProviderBindingError> {
        let provider_id = provider_id.into();
        ProviderId::new(provider_id.clone())
            .map_err(|_| ProviderBindingError::InvalidField("provider_id"))?;
        scope
            .validate()
            .map_err(|_| ProviderBindingError::InvalidField("scope"))?;
        ExecutionAuthority::new(authority.id.clone(), authority.generation)
            .map_err(|_| ProviderBindingError::InvalidField("authority"))?;
        validate_generation(generation, "generation")?;
        validate_generation(runtime_generation, "runtime_generation")?;
        Ok(Self {
            provider_id,
            host_id,
            scope,
            authority,
            session_id,
            generation,
            runtime_generation,
            pinned_source: None,
        })
    }

    pub(crate) fn from_identity(
        identity: &ProviderIdentity,
        session_id: ExecutionSessionId,
        generation: u64,
        runtime_generation: u64,
    ) -> Result<Self, ProviderBindingError> {
        Self::new(
            identity.provider_id.as_str(),
            identity.host_id.clone(),
            identity.scope.clone(),
            identity.authority.clone(),
            session_id,
            generation,
            runtime_generation,
        )
    }

    pub(crate) fn provider_identity(&self) -> Result<ProviderIdentity, ProviderBindingError> {
        ProviderIdentity::new(
            ProviderId::new(self.provider_id.clone())
                .map_err(|_| ProviderBindingError::InvalidField("provider_id"))?,
            self.host_id.clone(),
            self.scope.clone(),
            self.authority.clone(),
        )
        .map_err(|_| ProviderBindingError::InvalidField("identity"))
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_generation(
        &self,
        runtime_generation: u64,
    ) -> Result<Self, ProviderBindingError> {
        let mut binding = Self::new(
            self.provider_id.clone(),
            self.host_id.clone(),
            self.scope.clone(),
            self.authority.clone(),
            self.session_id.clone(),
            self.generation,
            runtime_generation,
        )?;
        binding.pinned_source = self.pinned_source.clone();
        Ok(binding)
    }

    pub(crate) fn with_pinned_source(
        mut self,
        pinned_source: Option<SourceIdentity>,
    ) -> Result<Self, ProviderBindingError> {
        if let Some(source) = &pinned_source {
            if source.host_id != self.host_id
                || source.profile_id != self.scope.profile_id
                || source.epoch == 0
                || source.epoch > i64::MAX as u64
            {
                return Err(ProviderBindingError::InvalidField("pinned_source"));
            }
        }
        self.pinned_source = pinned_source;
        Ok(self)
    }

    pub(crate) fn matches_identity(&self, identity: &ProviderIdentity) -> bool {
        self.provider_identity()
            .is_ok_and(|binding| binding == *identity)
    }
}

fn validate_generation(value: u64, field: &'static str) -> Result<(), ProviderBindingError> {
    if value == 0 || value > i64::MAX as u64 {
        return Err(ProviderBindingError::InvalidField(field));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderLifecycle {
    Unknown,
    Created,
    Starting,
    Running,
    Idle,
    Completed,
    Failed,
    Cancelled,
    Killed,
    Orphaned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderActivity {
    Working,
    Quiet,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderConnection {
    Connecting,
    Connected,
    Disconnected,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderDurability {
    Durable,
    Ephemeral,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderHealth {
    pub(crate) ok: bool,
    pub(crate) api_version: String,
    pub(crate) coven_version: String,
    pub(crate) sessions: bool,
    pub(crate) events: bool,
    pub(crate) event_cursor: Option<String>,
    pub(crate) structured_errors: bool,
    pub(crate) execution_request_contracts: Vec<String>,
    pub(crate) execution_request_operations: Vec<String>,
    pub(crate) execution_source_contracts: Vec<String>,
    pub(crate) authority: Option<ExecutionAuthority>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProviderHealthProjectionError;

impl TryFrom<coven_client::Health> for ProviderHealth {
    type Error = ProviderHealthProjectionError;

    fn try_from(health: coven_client::Health) -> Result<Self, Self::Error> {
        let api_version = bounded_health_label(health.api_version)?;
        let coven_version = bounded_health_label(health.coven_version)?;
        let event_cursor = health
            .capabilities
            .event_cursor
            .map(bounded_health_label)
            .transpose()?;
        let execution_request_contracts =
            bounded_health_capabilities(health.capabilities.execution_request_contracts)?;
        let execution_request_operations =
            bounded_health_capabilities(health.capabilities.execution_request_operations)?;
        let execution_source_contracts =
            bounded_health_capabilities(health.capabilities.execution_source_contracts)?;
        Ok(Self {
            ok: health.ok,
            api_version,
            coven_version,
            sessions: health.capabilities.sessions,
            events: health.capabilities.events,
            event_cursor,
            structured_errors: health.capabilities.structured_errors,
            execution_request_contracts,
            execution_request_operations,
            execution_source_contracts,
            authority: health.execution_authority,
        })
    }
}

fn bounded_health_label(value: String) -> Result<String, ProviderHealthProjectionError> {
    if value.is_empty()
        || value.len() > MAX_HEALTH_LABEL_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ProviderHealthProjectionError);
    }
    let mut value = value;
    value.shrink_to_fit();
    Ok(value)
}

fn bounded_health_capabilities(
    values: Vec<String>,
) -> Result<Vec<String>, ProviderHealthProjectionError> {
    if values.len() > MAX_HEALTH_CAPABILITIES {
        return Err(ProviderHealthProjectionError);
    }
    values.into_iter().map(bounded_health_label).collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderOperationKind {
    Health,
    ReadSource,
    ReadTranscript,
    AttachCheckpoint,
    ReadCheckpoint,
    Execution,
    LookupExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderFailureKind {
    Disconnected,
    Busy,
    Unsupported,
    IdentityMismatch,
    Rejected,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderFailure {
    pub(crate) kind: ProviderFailureKind,
    pub(crate) message: String,
}

impl ProviderFailure {
    pub(crate) fn new(kind: ProviderFailureKind, message: impl Into<String>) -> Self {
        let mut message = message.into();
        const MAX_MESSAGE_BYTES: usize = 512;
        if message.len() > MAX_MESSAGE_BYTES {
            let mut end = MAX_MESSAGE_BYTES;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        Self { kind, message }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProviderOperationResult {
    Pending { kind: ProviderOperationKind },
    Health(ProviderHealth),
    Source(SourceReply),
    Transcript(Arc<coven_client::transcript::TranscriptReply>),
    CheckpointAttached(Arc<ProviderCheckpointAttachment>),
    Checkpoint(Arc<ProviderCheckpointUpdate>),
    Execution(ExecutionOutcome),
    LookupExecution(ExecutionOutcome),
    Failure(ProviderFailure),
    Detached,
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OperationTicket(u64);

impl OperationTicket {
    pub(crate) fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderOperation {
    pub(crate) ticket: OperationTicket,
    pub(crate) kind: ProviderOperationKind,
    pub(crate) result: ProviderOperationResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderSnapshot {
    pub(crate) identity: ProviderIdentity,
    pub(crate) runtime_generation: u64,
    pub(crate) lifecycle: ProviderLifecycle,
    pub(crate) activity: ProviderActivity,
    pub(crate) connection: ProviderConnection,
    pub(crate) durability: ProviderDurability,
    pub(crate) health: Option<ProviderHealth>,
    pub(crate) source: Option<SourceReply>,
    pub(crate) last_operation: Option<ProviderOperation>,
    pub(crate) revision: u64,
    pub(crate) dropped_updates: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderUpdate {
    pub(crate) ticket: Option<OperationTicket>,
    pub(crate) binding: Option<ProviderBinding>,
    pub(crate) kind: ProviderOperationKind,
    pub(crate) runtime_generation: u64,
    pub(crate) result: ProviderOperationResult,
    pub(crate) snapshot: Arc<ProviderSnapshot>,
}

#[derive(Clone)]
pub(crate) enum ProviderCommand {
    Health,
    ReadSource {
        binding: ProviderBinding,
        cursor: Option<SourceCursor>,
        limit: u16,
    },
    ReadTranscript {
        binding: ProviderBinding,
        cursor: Option<SourceCursor>,
        limit: u16,
    },
    /// Negotiate the typed checkpoint stream and retain its reader in the
    /// provider worker. The reader never crosses the actor boundary.
    AttachCheckpoint {
        binding: ProviderBinding,
        attachment_id: String,
        checkpoint: Option<crate::terminal::backend::ExternalCheckpointState>,
    },
    /// Drain and apply a bounded batch on the provider worker. Quiet polls
    /// return an unchanged projection without disconnecting the attachment.
    ReadCheckpoint {
        binding: ProviderBinding,
    },
    #[allow(
        dead_code,
        reason = "provider action endpoint remains available for the typed worker contract and test harness"
    )]
    Execution {
        binding: Option<ProviderBinding>,
        request: coven_client::execution::ExecutionRequest,
    },
    LookupExecution {
        /// A launch lookup has no existing binding. Existing and continuation
        /// lookups must carry the durable binding that fences the target.
        binding: Option<ProviderBinding>,
        request_id: RequestId,
        expected_digest: String,
        expected_target: ExecutionTarget,
    },
}

impl ProviderCommand {
    pub(crate) fn kind(&self) -> ProviderOperationKind {
        match self {
            Self::Health => ProviderOperationKind::Health,
            Self::ReadSource { .. } => ProviderOperationKind::ReadSource,
            Self::ReadTranscript { .. } => ProviderOperationKind::ReadTranscript,
            Self::AttachCheckpoint { .. } => ProviderOperationKind::AttachCheckpoint,
            Self::ReadCheckpoint { .. } => ProviderOperationKind::ReadCheckpoint,
            Self::Execution { .. } => ProviderOperationKind::Execution,
            Self::LookupExecution { .. } => ProviderOperationKind::LookupExecution,
        }
    }

    fn binding(&self) -> Option<&ProviderBinding> {
        match self {
            Self::Health => None,
            Self::ReadSource { binding, .. }
            | Self::ReadTranscript { binding, .. }
            | Self::AttachCheckpoint { binding, .. }
            | Self::ReadCheckpoint { binding, .. }
            | Self::LookupExecution {
                binding: Some(binding),
                ..
            } => Some(binding),
            Self::LookupExecution { binding: None, .. } => None,
            Self::Execution { binding, .. } => binding.as_ref(),
        }
    }

    /// Validate all attacker-controlled payload bounds before a command is
    /// admitted to the bounded queue.  The client repeats these checks before
    /// I/O, but doing them here prevents malformed values from consuming queue
    /// slots or retaining unbounded strings while waiting for the worker.
    fn validate_for_submission(&self) -> Result<(), SubmitError> {
        match self {
            Self::Health => Ok(()),
            Self::ReadSource { cursor, limit, .. } => {
                if *limit == 0 || *limit > coven_client::source::MAX_EVENTS {
                    return Err(SubmitError::Rejected);
                }
                cursor
                    .as_ref()
                    .map(SourceCursor::validate)
                    .transpose()
                    .map_err(|_| SubmitError::Rejected)?;
                Ok(())
            }
            Self::ReadTranscript { cursor, limit, .. } => {
                if *limit == 0 || *limit > coven_client::transcript::MAX_CHUNKS {
                    return Err(SubmitError::Rejected);
                }
                cursor
                    .as_ref()
                    .map(SourceCursor::validate)
                    .transpose()
                    .map_err(|_| SubmitError::Rejected)?;
                Ok(())
            }
            Self::AttachCheckpoint { attachment_id, .. } => {
                if attachment_id.is_empty() || attachment_id.len() > 128 {
                    return Err(SubmitError::Rejected);
                }
                // Checkpoint decoding is bounded but can be substantial. The
                // worker validates it before any attach I/O, outside admission.
                Ok(())
            }
            Self::ReadCheckpoint { .. } => Ok(()),
            Self::Execution { request, .. } => request
                .canonical_bytes()
                .map(|_| ())
                .map_err(|_| SubmitError::Rejected),
            Self::LookupExecution {
                expected_digest,
                expected_target,
                ..
            } => {
                coven_client::execution::validate_digest(expected_digest)
                    .map_err(|_| SubmitError::Rejected)?;
                expected_target
                    .validate()
                    .map_err(|_| SubmitError::Rejected)?;
                Ok(())
            }
        }
    }
}

impl fmt::Debug for ProviderCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("ProviderCommand");
        debug.field("kind", &self.kind());
        if let Some(binding) = self.binding() {
            debug.field("binding", binding);
        }
        debug.finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmitError {
    Busy,
    Disconnected,
    IdentityMismatch,
    Rejected,
}

#[derive(Debug)]
pub(crate) enum ProviderStartError {
    WorkerLimitReached,
    GenerationExhausted,
    Worker(std::io::Error),
}

impl fmt::Display for ProviderStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerLimitReached => formatter.write_str("Coven provider worker limit reached"),
            Self::GenerationExhausted => {
                formatter.write_str("Coven provider runtime generation exhausted")
            }
            Self::Worker(error) => {
                write!(formatter, "failed to start Coven provider worker: {error}")
            }
        }
    }
}

impl std::error::Error for ProviderStartError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderConfigError {
    InvalidField(&'static str),
}

impl fmt::Display for ProviderConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidField(field) => write!(formatter, "invalid Coven provider field: {field}"),
        }
    }
}

impl std::error::Error for ProviderConfigError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderBindingError {
    InvalidField(&'static str),
}

impl fmt::Display for ProviderBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidField(field) => {
                write!(formatter, "invalid Coven provider binding field: {field}")
            }
        }
    }
}

impl std::error::Error for ProviderBindingError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderGenerationError {
    Exhausted,
}

impl fmt::Display for ProviderGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Coven provider runtime generation exhausted")
    }
}

impl std::error::Error for ProviderGenerationError {}

/// Pure presentation facts retained independently from worker handles and layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderObservation {
    pub(crate) lifecycle: ProviderLifecycle,
    pub(crate) activity: ProviderActivity,
    pub(crate) connection: ProviderConnection,
    pub(crate) durability: ProviderDurability,
    pub(crate) diagnostic: Option<String>,
    pub(crate) writer_state: Option<coven_client::source::WriterState>,
}

impl Default for ProviderObservation {
    fn default() -> Self {
        Self {
            lifecycle: ProviderLifecycle::Unknown,
            activity: ProviderActivity::Unknown,
            connection: ProviderConnection::Disconnected,
            durability: ProviderDurability::Unknown,
            diagnostic: None,
            writer_state: None,
        }
    }
}
