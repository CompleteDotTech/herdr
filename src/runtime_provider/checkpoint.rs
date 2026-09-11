//! Worker-owned terminal decoding and persistence. The event loop receives
//! committed model revisions and shares immutable checkpoint bytes.
use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use coven_client::{
    terminal_checkpoint::{
        TerminalCheckpointAttachRequest, TerminalCheckpointAttachResponse, TerminalCheckpointReader,
    },
    ClientError, DaemonClient,
};
use coven_terminal::SessionCursor;
use coven_terminal_checkpoint_transport::ConsumerEvent;

use super::{ProviderBinding, ProviderFailure, ProviderFailureKind};
use crate::terminal::{backend::ExternalCheckpointState, external::ExternalTerminalSession};

const MAX_BATCH_MESSAGES: usize = 256;
const BATCH_BUDGET: Duration = Duration::from_millis(20);
const READ_WAIT: Duration = Duration::from_millis(2);
const SAVE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct ProviderCheckpointAttachment {
    pub(crate) response: TerminalCheckpointAttachResponse,
    pub(crate) session: Arc<ExternalTerminalSession>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_client::execution::{
        AuthorityId, ExecutionAuthority, ExecutionScope, ExecutionSessionId, ProfileId, ProjectId,
    };
    use coven_client::source::SourceId;
    use coven_terminal_checkpoint_transport::{BoundCheckpoint, StreamBinding};

    fn binding() -> ProviderBinding {
        ProviderBinding::new(
            "provider",
            SourceId::new("host").unwrap(),
            ExecutionScope {
                project_id: ProjectId::new("project").unwrap(),
                profile_id: ProfileId::new("profile").unwrap(),
                policy_generation: 1,
            },
            ExecutionAuthority::new(AuthorityId::new("execution-authority").unwrap(), 7).unwrap(),
            ExecutionSessionId::new("session").unwrap(),
            2,
            1,
        )
        .unwrap()
    }

    fn saved() -> ExternalCheckpointState {
        let stream = StreamBinding::new("session", [7; 16], 1, 2, 913).unwrap();
        let cursor = SessionCursor {
            sequence: 9,
            offset: 1234,
        };
        let bound = BoundCheckpoint::new(stream.clone(), cursor, 4, vec![1]).unwrap();
        ExternalCheckpointState {
            blob: bound.encode().unwrap().into(),
            binding: stream,
            cursor,
            revision: 4,
            source_cursor: None,
            omitted: false,
            stream_id: "07070707-0707-0707-0707-070707070707".into(),
            stream_generation: 1,
        }
    }

    #[test]
    fn checkpoint_first_attach_discovers_the_terminal_epoch() {
        let request = attach_request(&binding(), "attachment", None).unwrap();
        coven_client::terminal::TerminalStreamId::parse(&request.attachment_id).unwrap();
        let second = attach_request(&binding(), "attachment", None).unwrap();
        assert_ne!(request.attachment_id, second.attachment_id);
        assert_eq!(request.authority_epoch, None);
        assert_eq!(request.execution_generation, None);
        assert_eq!(request.stream_id, None);
        request.validate().unwrap();
    }

    #[test]
    fn checkpoint_reconnect_pins_terminal_identity_without_an_exact_old_ticket() {
        let binding = binding();
        let saved = saved();
        assert_ne!(binding.authority.generation, saved.binding.authority_epoch);
        let request = attach_request(&binding, "attachment", Some(&saved)).unwrap();
        assert_eq!(request.authority_epoch, Some(913));
        assert_eq!(request.execution_generation, Some(2));
        assert_eq!(request.stream_id.as_deref(), Some(saved.stream_id.as_str()));
        assert_eq!(request.cursor, None);
        assert_eq!(request.revision, None);
        request.validate().unwrap();
    }

    #[test]
    fn checkpoint_reconnect_rejects_another_execution_before_io() {
        let mut binding = binding();
        binding.generation += 1;
        let error = attach_request(&binding, "attachment", Some(&saved())).unwrap_err();
        assert_eq!(error.kind, ProviderFailureKind::IdentityMismatch);
    }

    #[test]
    fn checkpoint_absence_is_an_explicit_transcript_fallback() {
        let error = ClientError::Daemon {
            status: 404,
            error: coven_client::DaemonError {
                code: "session_not_live".to_owned(),
                message: "session is not live in this daemon".to_owned(),
                details: serde_json::Value::Null,
            },
        };
        let failure = map_checkpoint_attach_error(error);
        assert_eq!(failure.kind, ProviderFailureKind::Unsupported);
        assert_eq!(
            failure.message,
            "Coven checkpoint stream is unavailable for this session"
        );
    }

    #[test]
    fn checkpoint_server_failure_does_not_degrade_to_transcript() {
        let error = ClientError::Daemon {
            status: 503,
            error: coven_client::DaemonError {
                code: "service_unavailable".to_owned(),
                message: "temporary failure".to_owned(),
                details: serde_json::Value::Null,
            },
        };
        assert_eq!(
            map_checkpoint_attach_error(error).kind,
            ProviderFailureKind::Failed
        );
    }
}

impl fmt::Debug for ProviderCheckpointAttachment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderCheckpointAttachment")
            .field("response", &self.response)
            .finish_non_exhaustive()
    }
}
impl PartialEq for ProviderCheckpointAttachment {
    fn eq(&self, other: &Self) -> bool {
        self.response == other.response && Arc::ptr_eq(&self.session, &other.session)
    }
}
impl Eq for ProviderCheckpointAttachment {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderCheckpointUpdate {
    pub(crate) cursor: SessionCursor,
    pub(crate) revision: u64,
    pub(crate) content_revision: u64,
    pub(crate) checkpoint: Option<ExternalCheckpointState>,
    pub(crate) closed: bool,
    pub(crate) messages: usize,
}

pub(super) struct CheckpointConnection {
    reader: TerminalCheckpointReader,
    session: Arc<ExternalTerminalSession>,
    last_saved: Option<(SessionCursor, u64)>,
    next_save: Instant,
    closed: bool,
}

fn projection_error(_: impl fmt::Display) -> ProviderFailure {
    // Model errors may describe untrusted payloads. Publish a bounded static
    // diagnostic, without echoing checkpoint or terminal contents.
    ProviderFailure::new(
        ProviderFailureKind::Rejected,
        "Coven terminal checkpoint could not be applied",
    )
}

fn fresh_attachment_id(label: &str) -> String {
    use sha2::{Digest, Sha256};
    // Attachment UUIDs name this observer, independently of Herdr terminal
    // identity. A fresh allocation also avoids reconnect racing old IPC cleanup.
    // These are identifiers; authenticated IPC and owner leases grant authority.
    let nonce = crate::terminal::TerminalId::alloc();
    let mut hash = Sha256::new();
    hash.update(b"herdr/coven/checkpoint-attachment/v1\0");
    hash.update(label.as_bytes());
    hash.update(std::process::id().to_le_bytes());
    hash.update(nonce.as_str().as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80; // UUID version 8, locally defined format.
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn attach_request(
    binding: &ProviderBinding,
    attachment_id: &str,
    saved: Option<&ExternalCheckpointState>,
) -> Result<TerminalCheckpointAttachRequest, ProviderFailure> {
    let mut request = TerminalCheckpointAttachRequest::new(
        binding.session_id.as_str(),
        fresh_attachment_id(attachment_id),
    );
    if let Some(saved) = saved {
        saved.validate().map_err(projection_error)?;
        let stream = &saved.binding;
        if stream.session_id != binding.session_id.as_str()
            || stream.execution_generation != binding.generation
        {
            return Err(ProviderFailure::new(
                ProviderFailureKind::IdentityMismatch,
                "Persisted terminal checkpoint belongs to a different execution",
            ));
        }
        request = request
            .with_stream_identity(saved.stream_id.clone(), saved.stream_generation)
            .with_incarnation(binding.generation, stream.authority_epoch);
        // The owner returns a fresh bootstrap. A saved cursor is the local
        // consumer's rollback floor; it is not an exact current-owner ticket.
    }
    request
        .validate()
        .map_err(super::client::map_client_error)?;
    Ok(request)
}

impl CheckpointConnection {
    pub(super) fn attach(
        daemon: &mut DaemonClient,
        binding: &ProviderBinding,
        attachment_id: &str,
        saved: Option<&ExternalCheckpointState>,
    ) -> Result<Self, ProviderFailure> {
        let request = attach_request(binding, attachment_id, saved)?;
        let reader = daemon
            .attach_terminal_checkpoint(request)
            .map_err(map_checkpoint_attach_error)?;
        let response = reader.negotiation();
        let stream = reader.binding();
        // Execution authority generation and terminal daemon epoch are
        // independent identities. First attach learns the latter only from
        // this authenticated response; reconnect pins the saved terminal epoch.
        if stream.session_id != binding.session_id.as_str()
            || stream.execution_generation != binding.generation
            || response.geometry.columns < 2
            || response.geometry.rows == 0
        {
            return Err(ProviderFailure::new(
                ProviderFailureKind::IdentityMismatch,
                "Coven checkpoint stream identity did not match the provider binding",
            ));
        }
        let session = Arc::new(
            ExternalTerminalSession::from_stream_default(
                stream.clone(),
                response.geometry,
                response.max_frame_bytes,
                Default::default(),
            )
            .map_err(projection_error)?,
        );
        if let Some(saved) = saved {
            session
                .restore_checkpoint_bytes(&saved.blob, coven_terminal::QueryReplyPolicy::Quiet)
                .map_err(projection_error)?;
        }
        session.prepare_model_snapshot().map_err(projection_error)?;
        Ok(Self {
            reader,
            session,
            last_saved: saved.map(|saved| (saved.cursor, saved.revision)),
            next_save: Instant::now(),
            closed: false,
        })
    }

    pub(super) fn attachment(&self) -> ProviderCheckpointAttachment {
        ProviderCheckpointAttachment {
            response: self.reader.negotiation().clone(),
            session: Arc::clone(&self.session),
        }
    }

    pub(super) fn read(&mut self) -> Result<ProviderCheckpointUpdate, ProviderFailure> {
        let started = Instant::now();
        let mut messages = 0;
        let mut bootstrapped = false;
        let mut model_changed = false;
        while !self.closed && messages < MAX_BATCH_MESSAGES && started.elapsed() < BATCH_BUDGET {
            let message = match self.reader.next_message_with_timeout(READ_WAIT) {
                Ok(Some(message)) => message,
                Ok(None) if self.reader.is_closed() => {
                    self.closed = true;
                    break;
                }
                Ok(None) => break,
                Err(ClientError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(error) => return Err(super::client::map_client_error(error)),
            };
            messages += 1;
            match self
                .session
                .accept_stream_message(message)
                .map_err(projection_error)?
            {
                Some(ConsumerEvent::Bootstrapped { .. }) => {
                    bootstrapped = true;
                    model_changed = true;
                }
                Some(ConsumerEvent::Applied { .. }) => model_changed = true,
                Some(ConsumerEvent::Closed) => self.closed = true,
                Some(ConsumerEvent::Gap { .. }) | None => {}
            }
        }
        let (cursor, revision) = self
            .session
            .stream_cursor_revision()
            .ok_or_else(|| projection_error("missing stream"))?;
        let mut checkpoint = None;
        if self.last_saved != Some((cursor, revision))
            && (bootstrapped || self.closed || Instant::now() >= self.next_save)
        {
            if let Some(blob) = self.session.checkpoint_blob().map_err(projection_error)? {
                let binding = self.reader.binding();
                checkpoint = Some(ExternalCheckpointState {
                    blob: blob.into(),
                    binding: binding.clone(),
                    cursor,
                    revision,
                    source_cursor: None,
                    omitted: false,
                    stream_id: self.reader.negotiation().stream_id.clone(),
                    stream_generation: binding.stream_generation,
                });
                self.last_saved = Some((cursor, revision));
                self.next_save = Instant::now() + SAVE_INTERVAL;
            }
        }
        if model_changed {
            // Populate the model cache on the worker. UI theme/scroll changes
            // then derive a bounded viewport from this captured revision.
            self.session
                .prepare_model_snapshot()
                .map_err(projection_error)?;
        }
        Ok(ProviderCheckpointUpdate {
            cursor,
            revision,
            content_revision: self.session.content_revision(),
            checkpoint,
            closed: self.closed,
            messages,
        })
    }
}

/// A transcript-mode managed execution has durable source state but no live
/// PTY checkpoint stream.  Coven reports that distinction as a structured
/// `session_not_live` response on the shared attach route.  Treat that one
/// explicit capability result as a normal fallback; all transport, identity,
/// and malformed-response failures retain their original classification.
fn map_checkpoint_attach_error(error: ClientError) -> ProviderFailure {
    match &error {
        ClientError::Daemon { status, error } => {
            tracing::debug!(status, code = %error.code, "Coven checkpoint attach returned a daemon error");
        }
        ClientError::HttpStatus(status) => {
            tracing::debug!(status, "Coven checkpoint attach returned an HTTP error");
        }
        ClientError::Io { operation, source } => {
            tracing::debug!(operation, kind = ?source.kind(), "Coven checkpoint attach transport failed");
        }
        _ => {
            tracing::debug!(kind = ?std::mem::discriminant(&error), "Coven checkpoint attach failed")
        }
    }
    let unavailable = match &error {
        ClientError::HttpStatus(404) => true,
        ClientError::Daemon { status: 404, .. } => true,
        ClientError::Daemon { error, .. } => matches!(
            error.code.as_str(),
            "session_not_live"
                | "checkpoint_protocol_required"
                | "checkpoint_protocol_unavailable"
                | "terminal_checkpoint_unavailable"
        ),
        _ => false,
    };
    if unavailable {
        return ProviderFailure::new(
            ProviderFailureKind::Unsupported,
            "Coven checkpoint stream is unavailable for this session",
        );
    }
    super::client::map_client_error(error)
}
