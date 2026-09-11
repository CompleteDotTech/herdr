//! Owner-local checkpoint-aware terminal attachment.
//!
//! Version 2 is deliberately a separate client surface from
//! [`crate::terminal`].  A caller that asks for `CheckpointV1` receives a
//! CTS2 stream or an error; it is never silently downgraded to RawBytesV1.
//! The HTTP request carries a bounded typed negotiation and keeps the
//! authenticated owner connection open.  The remaining bytes are decoded by
//! `coven-terminal-checkpoint-transport`, which is the single implementation
//! of the CTS2 frame and checkpoint envelopes.

use std::{
    collections::VecDeque,
    io::Read,
    thread,
    time::{Duration, Instant},
};

use coven_terminal_checkpoint_transport as checkpoint_transport;
use serde::{Deserialize, Serialize};

use crate::{
    terminal::{
        TerminalAuthority, TerminalBackend, TerminalPermissions, TerminalStreamId,
        MAX_TERMINAL_ATTACH_BODY_BYTES,
    },
    terminal_control::TerminalControlIdentity,
    transport, ClientError,
};

pub use checkpoint_transport::{GapReason, MessageMeta, StreamBinding, StreamMessage};
pub use checkpoint_transport::{Geometry, SessionCursor};

/// The exact protocol selected by this module. Version 2 is reserved for the
/// coherent checkpoint stream and is not compatible with the CTS1 framing.
pub const TERMINAL_CHECKPOINT_PROTOCOL_VERSION: u16 =
    checkpoint_transport::CHECKPOINT_PROTOCOL_VERSION;
/// Stable route shared with the legacy attachment. The body selects the
/// version and codec after the owner has authenticated the peer.
pub const TERMINAL_CHECKPOINT_ATTACH_PATH: &str = "/api/v1/terminal/attach";
/// The negotiated codec name as it appears in the JSON and HTTP headers.
pub const TERMINAL_CHECKPOINT_CODEC: &str = "checkpointV1";
/// The request body cap shared with the owner HTTP parser.
pub const MAX_TERMINAL_CHECKPOINT_ATTACH_BODY_BYTES: usize = MAX_TERMINAL_ATTACH_BODY_BYTES;
/// Maximum response header bytes accepted before any handshake JSON is used.
pub const MAX_TERMINAL_CHECKPOINT_RESPONSE_HEADERS_BYTES: usize = 64 * 1024;
/// Maximum handshake remainder retained before the incremental CTS2 decoder.
/// The platform transports normally return at most one 4 KiB read beyond the
/// JSON body; this independent cap keeps that implementation detail from
/// becoming an unbounded client allocation contract.
pub const MAX_TERMINAL_CHECKPOINT_BUFFERED_BYTES: usize =
    checkpoint_transport::MAX_CHECKPOINT_FRAME_BYTES;
/// The default CTS2 frame size, including its fixed header.
pub const DEFAULT_MAX_CHECKPOINT_FRAME_BYTES: usize = checkpoint_transport::DEFAULT_MAX_FRAME_BYTES;
/// The absolute CTS2 frame size, including its fixed header.
pub const MAX_CHECKPOINT_FRAME_BYTES: usize = checkpoint_transport::MAX_CHECKPOINT_FRAME_BYTES;

/// Request header marker emitted on the HTTP connection before CTS2 bytes.
pub const TERMINAL_CHECKPOINT_UPGRADE_TOKEN: &str = "coven-terminal-cts2";
/// Header name used by the daemon to echo the selected terminal protocol.
pub const TERMINAL_PROTOCOL_HEADER: &str = "X-Coven-Terminal-Protocol";
/// Header name used by the daemon to echo the selected terminal codec.
pub const TERMINAL_CODEC_HEADER: &str = "X-Coven-Terminal-Codec";

/// Typed owner-local request for a checkpoint-aware terminal attachment.
///
/// `execution_generation` and `authority_epoch` are optional expectations on
/// the request, but mandatory in the response.  This lets an observer pin a
/// known source incarnation when reconnecting while still allowing its first
/// attach to discover the current owner identity. `control` only requests the
/// separate owner control capability; CTS2 itself remains a read-only stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalCheckpointAttachRequest {
    #[serde(default = "default_protocol_version", alias = "version")]
    pub protocol_version: u16,
    pub session_id: String,
    pub attachment_id: String,
    #[serde(default)]
    pub authority: TerminalAuthority,
    #[serde(default, alias = "stream_id")]
    pub stream_id: Option<String>,
    #[serde(default)]
    pub stream_generation: Option<u64>,
    #[serde(default)]
    pub execution_generation: Option<u64>,
    #[serde(default)]
    pub authority_epoch: Option<u64>,
    #[serde(default)]
    pub codec: checkpoint_transport::TerminalCodec,
    #[serde(default = "default_observe")]
    pub observe: bool,
    /// A control-capable attach still receives only CTS2 observation bytes.
    /// Mutations use the existing `/api/v1/terminal/control` contract and the
    /// identity returned by [`TerminalCheckpointAttachResponse::control_identity`].
    #[serde(default)]
    pub control: bool,
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: usize,
    #[serde(default)]
    pub cursor: Option<SessionCursor>,
    #[serde(default)]
    pub revision: Option<u64>,
}

impl TerminalCheckpointAttachRequest {
    /// Construct a first observer attach. The daemon supplies the stream and
    /// source incarnation fields in its response.
    pub fn new(session_id: impl Into<String>, attachment_id: impl Into<String>) -> Self {
        Self {
            protocol_version: TERMINAL_CHECKPOINT_PROTOCOL_VERSION,
            session_id: session_id.into(),
            attachment_id: attachment_id.into(),
            authority: TerminalAuthority::OwnerLocalIpc,
            stream_id: None,
            stream_generation: None,
            execution_generation: None,
            authority_epoch: None,
            codec: checkpoint_transport::TerminalCodec::CheckpointV1,
            observe: true,
            control: false,
            max_frame_bytes: DEFAULT_MAX_CHECKPOINT_FRAME_BYTES,
            cursor: None,
            revision: None,
        }
    }

    /// Request the owner to bind the separate control capability to this
    /// attachment. The stream remains observe-only until a lease is acquired
    /// through the existing terminal-control client.
    pub const fn with_control(mut self, control: bool) -> Self {
        self.control = control;
        self
    }

    /// Pin a reconnect to one previously observed stream identity.
    pub fn with_stream_identity(mut self, stream_id: impl Into<String>, generation: u64) -> Self {
        self.stream_id = Some(stream_id.into());
        self.stream_generation = Some(generation);
        self
    }

    /// Pin a reconnect to the source/control incarnation returned by an
    /// earlier response.
    pub const fn with_incarnation(
        mut self,
        execution_generation: u64,
        authority_epoch: u64,
    ) -> Self {
        self.execution_generation = Some(execution_generation);
        self.authority_epoch = Some(authority_epoch);
        self
    }

    /// Resume from a trusted cursor and stable model revision. Both values
    /// travel in the body so the session id and cursor never become a route
    /// component.
    pub const fn with_cursor(mut self, cursor: SessionCursor, revision: u64) -> Self {
        self.cursor = Some(cursor);
        self.revision = Some(revision);
        self
    }

    /// Validate before any socket or pipe bytes are written.
    pub fn validate(&self) -> Result<(), ClientError> {
        if self.protocol_version != TERMINAL_CHECKPOINT_PROTOCOL_VERSION {
            return Err(invalid_request("terminal checkpoint protocol version"));
        }
        if self.session_id.is_empty()
            || self.session_id.len() > checkpoint_transport::MAX_SESSION_ID_BYTES
        {
            return Err(invalid_request("terminal checkpoint session id"));
        }
        if self.attachment_id.is_empty() {
            return Err(invalid_request("terminal checkpoint attachment id"));
        }
        if self.authority != TerminalAuthority::OwnerLocalIpc {
            return Err(invalid_request("terminal checkpoint authority"));
        }
        if self.codec != checkpoint_transport::TerminalCodec::CheckpointV1 || !self.observe {
            return Err(invalid_request(
                "terminal checkpoint codec or observe permission",
            ));
        }
        if self.stream_id.is_some() != self.stream_generation.is_some() {
            return Err(invalid_request("terminal checkpoint stream identity"));
        }
        if self.execution_generation.is_some() != self.authority_epoch.is_some() {
            return Err(invalid_request("terminal checkpoint source incarnation"));
        }
        if self.cursor.is_some() != self.revision.is_some() {
            return Err(invalid_request("terminal checkpoint cursor and revision"));
        }
        if self
            .stream_id
            .as_deref()
            .is_some_and(|value| TerminalStreamId::parse(value).is_err())
        {
            return Err(invalid_request("terminal checkpoint stream id"));
        }
        if self.stream_generation.is_some_and(|value| value == 0)
            || self.execution_generation.is_some_and(|value| value == 0)
            || self.authority_epoch.is_some_and(|value| value == 0)
        {
            return Err(invalid_request("terminal checkpoint generation"));
        }
        let cursor = self.cursor;
        if cursor.is_some_and(|cursor| cursor.sequence == 0) {
            return Err(invalid_request("terminal checkpoint cursor sequence"));
        }
        if self.revision.is_some_and(|revision| revision & 1 != 0) {
            return Err(invalid_request("terminal checkpoint revision"));
        }
        validate_frame_limit(self.max_frame_bytes)
    }

    /// Serialize after semantic and encoded-size validation.
    pub fn to_bounded_json(&self) -> Result<Vec<u8>, ClientError> {
        self.validate()?;
        let body = serde_json::to_vec(self).map_err(ClientError::InvalidJson)?;
        if body.len() > MAX_TERMINAL_CHECKPOINT_ATTACH_BODY_BYTES {
            return Err(ClientError::RequestTooLarge {
                max_bytes: MAX_TERMINAL_CHECKPOINT_ATTACH_BODY_BYTES,
                actual_bytes: body.len(),
            });
        }
        Ok(body)
    }
}

/// Typed result of the checkpoint attach negotiation. The source identity is
/// always present because every CTS2 frame and checkpoint envelope is bound
/// to all three owner generations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalCheckpointAttachResponse {
    pub protocol_version: u16,
    pub authority: TerminalAuthority,
    pub session_id: String,
    pub attachment_id: String,
    pub stream_id: String,
    pub stream_generation: u64,
    pub execution_generation: u64,
    pub authority_epoch: u64,
    pub backend: TerminalBackend,
    pub codec: checkpoint_transport::TerminalCodec,
    pub permissions: TerminalPermissions,
    pub max_frame_bytes: usize,
    /// The exact trusted cursor at which the first CTS2 message begins.
    pub cursor: SessionCursor,
    /// Stable runtime revision associated with `cursor`.
    pub revision: u64,
    pub geometry: Geometry,
}

impl TerminalCheckpointAttachResponse {
    /// Construct the transport binding used by every subsequent frame.
    pub fn binding(&self) -> Result<StreamBinding, ClientError> {
        let stream_id = TerminalStreamId::parse(&self.stream_id)
            .map_err(|error| invalid_response(error.to_string()))?;
        StreamBinding::new(
            self.session_id.clone(),
            *stream_id.as_bytes(),
            self.stream_generation,
            self.execution_generation,
            self.authority_epoch,
        )
        .map_err(|error| invalid_response(error.to_string()))
    }

    /// Build the identity required by the existing terminal-control API.
    /// Control is a separate lease and this method never grants it.
    pub fn control_identity(&self) -> Result<TerminalControlIdentity, ClientError> {
        if !self.permissions.control {
            return Err(invalid_response(
                "checkpoint attachment did not receive control permission",
            ));
        }
        TerminalControlIdentity::new(
            self.session_id.clone(),
            self.stream_id.clone(),
            self.stream_generation,
            self.execution_generation,
            self.authority_epoch,
            self.attachment_id.clone(),
        )
        .map_err(|error| invalid_response(error.to_string()))
    }

    fn validate_for(
        &self,
        request: &TerminalCheckpointAttachRequest,
    ) -> Result<StreamBinding, ClientError> {
        if self.protocol_version != TERMINAL_CHECKPOINT_PROTOCOL_VERSION
            || self.authority != TerminalAuthority::OwnerLocalIpc
            || self.session_id != request.session_id
            || self.attachment_id != request.attachment_id
            || self.codec != checkpoint_transport::TerminalCodec::CheckpointV1
            || self.backend != TerminalBackend::PtyRaw
            || !self.permissions.observe
            || self.permissions.control != request.control
            || self.max_frame_bytes != request.max_frame_bytes
            || self.cursor.sequence == 0
            || self.revision & 1 != 0
        {
            return Err(invalid_response(
                "daemon checkpoint negotiation did not match the requested stream",
            ));
        }
        if request
            .stream_id
            .as_deref()
            .is_some_and(|value| value != self.stream_id)
            || request
                .stream_generation
                .is_some_and(|value| value != self.stream_generation)
            || request
                .execution_generation
                .is_some_and(|value| value != self.execution_generation)
            || request
                .authority_epoch
                .is_some_and(|value| value != self.authority_epoch)
            || request.cursor.is_some_and(|value| value != self.cursor)
            || request.revision.is_some_and(|value| value != self.revision)
        {
            return Err(invalid_response(
                "daemon checkpoint negotiation changed the requested identity",
            ));
        }
        self.geometry
            .validate()
            .map_err(|error| invalid_response(error.to_string()))?;
        validate_frame_limit(self.max_frame_bytes)?;
        self.binding()
    }
}

/// One identity-checked CTS2 message delivered by [`TerminalCheckpointReader`].
/// The event keeps the transport metadata intact, including the source
/// binding, cursor, and stable revision. Consumers can pass its underlying
/// message to the shared checkpoint coordinator without reparsing bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalCheckpointEvent {
    CheckpointStart {
        meta: MessageMeta,
        total_bytes: u32,
        part_count: u32,
        digest: [u8; 32],
    },
    CheckpointPart {
        meta: MessageMeta,
        part_index: u32,
        part_count: u32,
        bytes: Vec<u8>,
    },
    CheckpointEnd {
        meta: MessageMeta,
        total_bytes: u32,
        part_count: u32,
        digest: [u8; 32],
    },
    Output {
        meta: MessageMeta,
        part_index: u32,
        part_count: u32,
        bytes: Vec<u8>,
    },
    Resize {
        meta: MessageMeta,
        geometry: Geometry,
    },
    SyncFlush {
        meta: MessageMeta,
    },
    Gap {
        binding: StreamBinding,
        requested: SessionCursor,
        available: SessionCursor,
        available_revision: u64,
        reason: GapReason,
    },
    Close {
        meta: MessageMeta,
    },
}

impl TerminalCheckpointEvent {
    pub fn binding(&self) -> &StreamBinding {
        match self {
            Self::CheckpointStart { meta, .. }
            | Self::CheckpointPart { meta, .. }
            | Self::CheckpointEnd { meta, .. }
            | Self::Output { meta, .. }
            | Self::Resize { meta, .. }
            | Self::SyncFlush { meta }
            | Self::Close { meta } => &meta.binding,
            Self::Gap { binding, .. } => binding,
        }
    }

    pub fn cursor(&self) -> SessionCursor {
        match self {
            Self::CheckpointStart { meta, .. }
            | Self::CheckpointPart { meta, .. }
            | Self::CheckpointEnd { meta, .. }
            | Self::Output { meta, .. }
            | Self::Resize { meta, .. }
            | Self::SyncFlush { meta }
            | Self::Close { meta } => meta.cursor,
            Self::Gap { requested, .. } => *requested,
        }
    }

    pub fn revision(&self) -> u64 {
        match self {
            Self::CheckpointStart { meta, .. }
            | Self::CheckpointPart { meta, .. }
            | Self::CheckpointEnd { meta, .. }
            | Self::Output { meta, .. }
            | Self::Resize { meta, .. }
            | Self::SyncFlush { meta }
            | Self::Close { meta } => meta.revision,
            Self::Gap { .. } => 0,
        }
    }

    pub fn into_message(self) -> StreamMessage {
        match self {
            Self::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                digest,
            } => StreamMessage::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                digest,
            },
            Self::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes,
            } => StreamMessage::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes,
            },
            Self::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                digest,
            } => StreamMessage::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                digest,
            },
            Self::Output {
                meta,
                part_index,
                part_count,
                bytes,
            } => StreamMessage::Output {
                meta,
                part_index,
                part_count,
                bytes,
            },
            Self::Resize { meta, geometry } => StreamMessage::Resize { meta, geometry },
            Self::SyncFlush { meta } => StreamMessage::SyncFlush { meta },
            Self::Gap {
                binding,
                requested,
                available,
                available_revision,
                reason,
            } => StreamMessage::Gap {
                binding,
                requested,
                available,
                available_revision,
                reason,
            },
            Self::Close { meta } => StreamMessage::Close { meta },
        }
    }
}

impl From<StreamMessage> for TerminalCheckpointEvent {
    fn from(message: StreamMessage) -> Self {
        match message {
            StreamMessage::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                digest,
            } => Self::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                digest,
            },
            StreamMessage::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes,
            } => Self::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes,
            },
            StreamMessage::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                digest,
            } => Self::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                digest,
            },
            StreamMessage::Output {
                meta,
                part_index,
                part_count,
                bytes,
            } => Self::Output {
                meta,
                part_index,
                part_count,
                bytes,
            },
            StreamMessage::Resize { meta, geometry } => Self::Resize { meta, geometry },
            StreamMessage::SyncFlush { meta } => Self::SyncFlush { meta },
            StreamMessage::Gap {
                binding,
                requested,
                available,
                available_revision,
                reason,
            } => Self::Gap {
                binding,
                requested,
                available,
                available_revision,
                reason,
            },
            StreamMessage::Close { meta } => Self::Close { meta },
        }
    }
}

/// A bounded CTS2 reader retaining the authenticated Unix socket or Windows
/// named-pipe connection after HTTP negotiation.
pub struct TerminalCheckpointReader {
    connection: Option<Box<dyn transport::ReadWrite + Send>>,
    decoder: checkpoint_transport::CheckpointFrameReader,
    buffered: VecDeque<u8>,
    messages: VecDeque<StreamMessage>,
    response: TerminalCheckpointAttachResponse,
    binding: StreamBinding,
    closed: bool,
    detached: bool,
    failed: bool,
}

impl TerminalCheckpointReader {
    pub(crate) fn from_connection(
        connection: transport::TerminalConnection,
        request: &TerminalCheckpointAttachRequest,
    ) -> Result<Self, ClientError> {
        if connection.response.status != 200 {
            return Err(ClientError::HttpStatus(connection.response.status));
        }
        validate_upgrade_headers(&connection.response_headers)?;
        if connection.buffered_remainder.len() > MAX_TERMINAL_CHECKPOINT_BUFFERED_BYTES {
            return Err(ClientError::InvalidHttpResponse(
                "checkpoint stream buffered remainder exceeded its bound".to_owned(),
            ));
        }
        let response: TerminalCheckpointAttachResponse =
            serde_json::from_slice(&connection.response.body).map_err(ClientError::InvalidJson)?;
        let binding = response.validate_for(request)?;
        let decoder = checkpoint_transport::CheckpointFrameReader::new(
            binding.clone(),
            response.max_frame_bytes,
        )
        .map_err(|error| invalid_response(error.to_string()))?;
        Ok(Self {
            connection: Some(connection.stream),
            decoder,
            buffered: connection.buffered_remainder.into_iter().collect(),
            messages: VecDeque::new(),
            response,
            binding,
            closed: false,
            detached: false,
            failed: false,
        })
    }

    pub fn negotiation(&self) -> &TerminalCheckpointAttachResponse {
        &self.response
    }

    pub fn binding(&self) -> &StreamBinding {
        &self.binding
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn is_detached(&self) -> bool {
        self.detached
    }

    /// Read one identity-checked logical CTS2 message.
    pub fn next_message(&mut self) -> Result<Option<StreamMessage>, ClientError> {
        self.next_message_with_timeout(Duration::from_secs(30))
    }

    pub fn next_message_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<StreamMessage>, ClientError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| ClientError::Io {
                operation: "failed to calculate Coven checkpoint read deadline",
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "checkpoint read deadline overflowed",
                ),
            })?;
        self.next_message_until(deadline)
    }

    /// Read one typed event. This is the renderer-facing convenience layer;
    /// [`next_message`](Self::next_message) remains available for callers that
    /// feed the shared transport coordinator directly.
    pub fn next_event(&mut self) -> Result<Option<TerminalCheckpointEvent>, ClientError> {
        self.next_event_with_timeout(Duration::from_secs(30))
    }

    pub fn next_event_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TerminalCheckpointEvent>, ClientError> {
        self.next_message_with_timeout(timeout)
            .map(|message| message.map(TerminalCheckpointEvent::from))
    }

    fn next_message_until(
        &mut self,
        deadline: Instant,
    ) -> Result<Option<StreamMessage>, ClientError> {
        if let Some(message) = self.messages.pop_front() {
            return self.take_message(message);
        }
        if self.detached || self.failed || self.closed {
            return Ok(None);
        }

        loop {
            if let Some(message) = self.messages.pop_front() {
                return self.take_message(message);
            }
            if Instant::now() >= deadline {
                return Err(ClientError::Io {
                    operation: "failed to read Coven checkpoint stream",
                    source: std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timed out waiting for a Coven checkpoint frame",
                    ),
                });
            }
            if !self.buffered.is_empty() {
                let count = self.buffered.len().min(16 * 1024);
                let bytes = self.buffered.drain(..count).collect::<Vec<_>>();
                self.decoder
                    .push(&bytes, &mut self.messages)
                    .map_err(|error| self.fail_codec(error.to_string()))?;
                continue;
            }
            let Some(connection) = self.connection.as_mut() else {
                return Ok(None);
            };
            let mut bytes = [0_u8; 16 * 1024];
            match connection.read(&mut bytes) {
                Ok(0) => {
                    return match self.decoder.finish() {
                        Ok(()) => Err(self.fail_codec(
                            "checkpoint transport ended without an explicit close frame",
                        )),
                        Err(error) => Err(self.fail_codec(error.to_string())),
                    };
                }
                Ok(read) => {
                    self.decoder
                        .push(&bytes[..read], &mut self.messages)
                        .map_err(|error| self.fail_codec(error.to_string()))?;
                    continue;
                }
                Err(error) if checkpoint_read_would_block(&error) => {
                    if error.kind() != std::io::ErrorKind::Interrupted {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            return Err(ClientError::Io {
                                operation: "failed to read Coven checkpoint stream",
                                source: std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "timed out waiting for a Coven checkpoint frame",
                                ),
                            });
                        }
                        thread::sleep(remaining.min(Duration::from_millis(2)));
                    }
                }
                Err(error) => {
                    return Err(self.fail_io(error));
                }
            }
        }
    }

    fn take_message(
        &mut self,
        message: StreamMessage,
    ) -> Result<Option<StreamMessage>, ClientError> {
        if message.binding() != &self.binding {
            return Err(self.fail_codec("checkpoint message identity mismatch"));
        }
        if self.closed {
            return Err(self.fail_codec("checkpoint message arrived after close"));
        }
        if matches!(message, StreamMessage::Close { .. }) {
            self.closed = true;
            if !self.messages.is_empty() {
                return Err(self.fail_codec("checkpoint stream carried bytes after close"));
            }
        }
        Ok(Some(message))
    }

    fn fail_codec(&mut self, message: impl Into<String>) -> ClientError {
        self.failed = true;
        self.closed = true;
        self.messages.clear();
        self.buffered.clear();
        self.connection.take();
        ClientError::InvalidHttpResponse(message.into())
    }

    fn fail_io(&mut self, error: std::io::Error) -> ClientError {
        self.failed = true;
        self.closed = true;
        self.messages.clear();
        self.buffered.clear();
        self.connection.take();
        ClientError::Io {
            operation: "failed to read Coven checkpoint stream",
            source: error,
        }
    }

    /// Explicitly detach the observer. No control or stream message is sent;
    /// dropping the retained socket/pipe is the owner-local detach signal.
    /// The operation is idempotent and never retries or waits on the daemon.
    pub fn detach(&mut self) {
        self.detached = true;
        self.closed = true;
        self.messages.clear();
        self.buffered.clear();
        self.connection.take();
    }

    /// Require the explicit CTS2 close event and release the underlying
    /// connection. EOF without `Close` is an error and also detaches.
    pub fn finish(&mut self) -> Result<(), ClientError> {
        if self.failed {
            return Err(invalid_response("checkpoint reader is failed closed"));
        }
        if !self.closed {
            self.detach();
            return Err(invalid_response(
                "checkpoint reader finished before an explicit close event",
            ));
        }
        self.decoder
            .finish()
            .map_err(|error| invalid_response(error.to_string()))?;
        self.detached = true;
        self.connection.take();
        Ok(())
    }
}

fn validate_upgrade_headers(headers: &[u8]) -> Result<(), ClientError> {
    if headers.len() > MAX_TERMINAL_CHECKPOINT_RESPONSE_HEADERS_BYTES {
        return Err(invalid_response(
            "checkpoint response headers exceeded their bound",
        ));
    }
    let text = std::str::from_utf8(headers)
        .map_err(|_| invalid_response("checkpoint response headers were not UTF-8"))?;
    let mut protocol = None;
    let mut codec = None;
    for line in text.lines().skip(1) {
        if line.trim().is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(invalid_response("checkpoint response header was malformed"));
        };
        let value = value.trim();
        if value.chars().any(char::is_control) {
            return Err(invalid_response(
                "checkpoint response header contained control bytes",
            ));
        }
        if name.eq_ignore_ascii_case(TERMINAL_PROTOCOL_HEADER) {
            if protocol.replace(value).is_some() {
                return Err(invalid_response(
                    "checkpoint protocol header was duplicated",
                ));
            }
        } else if name.eq_ignore_ascii_case(TERMINAL_CODEC_HEADER) && codec.replace(value).is_some()
        {
            return Err(invalid_response("checkpoint codec header was duplicated"));
        }
    }
    if protocol != Some("2") || codec != Some(TERMINAL_CHECKPOINT_CODEC) {
        return Err(invalid_response(
            "daemon did not acknowledge the checkpoint terminal upgrade",
        ));
    }
    Ok(())
}

fn validate_frame_limit(max_frame_bytes: usize) -> Result<(), ClientError> {
    if !(checkpoint_transport::MIN_CHECKPOINT_FRAME_BYTES..=MAX_CHECKPOINT_FRAME_BYTES)
        .contains(&max_frame_bytes)
    {
        return Err(invalid_request("terminal checkpoint frame limit"));
    }
    Ok(())
}

fn checkpoint_read_would_block(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
    ) || (cfg!(windows) && error.raw_os_error() == Some(232))
}

fn invalid_request(message: &'static str) -> ClientError {
    ClientError::InvalidHttpResponse(format!("invalid terminal checkpoint request: {message}"))
}

fn invalid_response(message: impl Into<String>) -> ClientError {
    ClientError::InvalidHttpResponse(format!(
        "invalid terminal checkpoint response: {}",
        message.into()
    ))
}

fn default_protocol_version() -> u16 {
    TERMINAL_CHECKPOINT_PROTOCOL_VERSION
}

fn default_observe() -> bool {
    true
}

fn default_max_frame_bytes() -> usize {
    DEFAULT_MAX_CHECKPOINT_FRAME_BYTES
}

/// Compatibility aliases used by integration code that names the codec
/// before the attach route.
pub type TerminalAttachCheckpointRequest = TerminalCheckpointAttachRequest;
pub type TerminalAttachCheckpointResponse = TerminalCheckpointAttachResponse;
pub type CheckpointTerminalReader = TerminalCheckpointReader;
pub type CheckpointTerminalEvent = TerminalCheckpointEvent;

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION: &str = "engine/events?literal";
    const ATTACHMENT: &str = "00000000-0000-0000-0000-000000000001";
    const STREAM: &str = "00112233-4455-6677-8899-aabbccddeeff";

    #[test]
    fn request_serializes_checkpoint_codec_and_keeps_session_in_body() {
        let request = TerminalCheckpointAttachRequest::new(SESSION, ATTACHMENT);
        let value = serde_json::to_value(&request).expect("request JSON");
        assert_eq!(value["protocolVersion"], 2);
        assert_eq!(value["codec"], "checkpointV1");
        assert_eq!(value["sessionId"], SESSION);
        assert!(value.get("session_id").is_none());
    }

    #[test]
    fn request_frame_minimum_can_carry_mandatory_checkpoint_control() {
        let mut request = TerminalCheckpointAttachRequest::new(SESSION, ATTACHMENT);
        request.max_frame_bytes = checkpoint_transport::MIN_CHECKPOINT_FRAME_BYTES - 1;
        assert!(request.to_bounded_json().is_err());
        request.max_frame_bytes += 1;
        assert!(request.to_bounded_json().is_ok());
        let control = StreamMessage::CheckpointStart {
            meta: MessageMeta {
                binding: StreamBinding::new(SESSION, [1; 16], 1, 1, 1).unwrap(),
                cursor: SessionCursor::START,
                revision: 0,
            },
            total_bytes: 1,
            part_count: 1,
            digest: [0; 32],
        };
        let encoded = control.encode(request.max_frame_bytes).unwrap();
        assert_eq!(encoded.len(), request.max_frame_bytes);
    }

    #[test]
    fn request_rejects_raw_codec_and_unpaired_resume_fields() {
        let mut request = TerminalCheckpointAttachRequest::new(SESSION, ATTACHMENT);
        request.codec = checkpoint_transport::TerminalCodec::RawBytesV1;
        assert!(request.validate().is_err());

        let mut request = TerminalCheckpointAttachRequest::new(SESSION, ATTACHMENT);
        request.cursor = Some(SessionCursor {
            sequence: 1,
            offset: 0,
        });
        assert!(request.validate().is_err());
    }

    #[test]
    fn response_binding_requires_all_source_generations() {
        let response = TerminalCheckpointAttachResponse {
            protocol_version: 2,
            authority: TerminalAuthority::OwnerLocalIpc,
            session_id: SESSION.to_owned(),
            attachment_id: ATTACHMENT.to_owned(),
            stream_id: STREAM.to_owned(),
            stream_generation: 3,
            execution_generation: 7,
            authority_epoch: 11,
            backend: TerminalBackend::PtyRaw,
            codec: checkpoint_transport::TerminalCodec::CheckpointV1,
            permissions: TerminalPermissions {
                observe: true,
                control: false,
            },
            max_frame_bytes: DEFAULT_MAX_CHECKPOINT_FRAME_BYTES,
            cursor: SessionCursor {
                sequence: 1,
                offset: 0,
            },
            revision: 0,
            geometry: Geometry::new(80, 24),
        };
        let binding = response.binding().expect("valid binding");
        assert_eq!(binding.stream_generation, 3);
        assert_eq!(binding.execution_generation, 7);
        assert_eq!(binding.authority_epoch, 11);
    }

    #[test]
    fn upgrade_headers_require_exact_protocol_and_codec() {
        let headers = b"HTTP/1.1 200 OK\r\nX-Coven-Terminal-Protocol: 2\r\nX-Coven-Terminal-Codec: checkpointV1\r\n\r\n";
        validate_upgrade_headers(headers).expect("checkpoint upgrade headers");

        let raw = b"HTTP/1.1 200 OK\r\nX-Coven-Terminal-Protocol: 1\r\nX-Coven-Terminal-Codec: rawBytesV1\r\n\r\n";
        assert!(validate_upgrade_headers(raw).is_err());
    }

    #[test]
    fn event_mapping_preserves_binding_and_revision() {
        let binding = StreamBinding::new(SESSION, [1; 16], 3, 7, 11).expect("binding");
        let message = StreamMessage::SyncFlush {
            meta: MessageMeta {
                binding: binding.clone(),
                cursor: SessionCursor {
                    sequence: 2,
                    offset: 0,
                },
                revision: 2,
            },
        };
        let event = TerminalCheckpointEvent::from(message);
        assert_eq!(event.binding(), &binding);
        assert_eq!(event.revision(), 2);
        assert_eq!(event.cursor().sequence, 2);
    }
}
