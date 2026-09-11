//! Owner-local, observe-only terminal attachment.
//!
//! The HTTP request is used only for the bounded typed negotiation.  Once the
//! daemon accepts it, the same authenticated connection carries the fixed
//! length terminal frame protocol below.  Keeping the reader here means
//! callers never have to treat a live stream as an ordinary whole-body HTTP
//! response.

use std::{
    collections::VecDeque,
    fmt,
    io::{Read, Write},
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{transport, ClientError};

pub const TERMINAL_PROTOCOL_VERSION: u16 = 1;
pub const TERMINAL_FRAME_HEADER_BYTES: usize = 52;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_REPLAY_BYTES: usize = 1024 * 1024;
pub const MAX_TERMINAL_ATTACH_BODY_BYTES: usize = 64 * 1024;
const ABSOLUTE_MAX_FRAME_BYTES: usize = 1024 * 1024;
const ABSOLUTE_MAX_REPLAY_BYTES: usize = 16 * 1024 * 1024;
const FRAME_MAGIC: &[u8; 4] = b"CTS1";

/// A UUID without adding a UUID dependency to the small client crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TerminalStreamId([u8; 16]);

impl TerminalStreamId {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn parse(value: &str) -> Result<Self, TerminalCodecError> {
        let bytes = value.as_bytes();
        if bytes.len() != 36
            || !matches!(bytes[8], b'-')
            || !matches!(bytes[13], b'-')
            || !matches!(bytes[18], b'-')
            || !matches!(bytes[23], b'-')
        {
            return Err(TerminalCodecError::InvalidStreamId);
        }
        let mut parsed = [0_u8; 16];
        let mut output = 0;
        for (index, byte) in bytes.iter().enumerate() {
            if matches!(index, 8 | 13 | 18 | 23) {
                continue;
            }
            if output % 2 == 0 {
                parsed[output / 2] = hex(*byte)? << 4;
            } else {
                parsed[output / 2] |= hex(*byte)?;
            }
            output += 1;
        }
        if parsed == [0; 16] {
            return Err(TerminalCodecError::InvalidStreamId);
        }
        Ok(Self(parsed))
    }
}

impl fmt::Display for TerminalStreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = self.0;
        write!(
            formatter,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12],
            b[13], b[14], b[15]
        )
    }
}

impl Serialize for TerminalStreamId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TerminalStreamId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

fn hex(value: u8) -> Result<u8, TerminalCodecError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(TerminalCodecError::InvalidStreamId),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TerminalAuthority {
    #[default]
    OwnerLocalIpc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TerminalCodec {
    #[default]
    RawBytesV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TerminalBackend {
    PtyRaw,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalPermissions {
    pub observe: bool,
    pub control: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalCursor {
    pub offset: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalAttachRequest {
    #[serde(default = "default_protocol_version", alias = "version")]
    pub protocol_version: u16,
    #[serde(alias = "session_id")]
    pub session_id: String,
    #[serde(alias = "attachment_id")]
    pub attachment_id: String,
    #[serde(default)]
    pub authority: TerminalAuthority,
    #[serde(default, alias = "stream_id")]
    pub stream_id: Option<String>,
    #[serde(default)]
    pub stream_generation: Option<u64>,
    #[serde(default)]
    pub codec: TerminalCodec,
    #[serde(default = "default_observe")]
    pub observe: bool,
    #[serde(default)]
    pub control: bool,
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: usize,
    #[serde(default = "default_max_replay_bytes")]
    pub replay_bytes: usize,
    #[serde(default)]
    pub cursor: Option<TerminalCursor>,
}

impl TerminalAttachRequest {
    pub fn new(session_id: impl Into<String>, attachment_id: impl Into<String>) -> Self {
        Self {
            protocol_version: TERMINAL_PROTOCOL_VERSION,
            session_id: session_id.into(),
            attachment_id: attachment_id.into(),
            authority: TerminalAuthority::OwnerLocalIpc,
            stream_id: None,
            stream_generation: None,
            codec: TerminalCodec::RawBytesV1,
            observe: true,
            control: false,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            replay_bytes: DEFAULT_MAX_REPLAY_BYTES,
            cursor: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalAttachResponse {
    pub protocol_version: u16,
    pub authority: TerminalAuthority,
    pub session_id: String,
    pub attachment_id: String,
    pub stream_id: String,
    pub stream_generation: u64,
    pub backend: TerminalBackend,
    pub codec: TerminalCodec,
    pub permissions: TerminalPermissions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_epoch: Option<u64>,
    pub max_frame_bytes: usize,
    pub replay_bytes: usize,
    /// The exact trusted cursor at which the first raw frame begins.
    pub cursor: TerminalCursor,
}

fn default_protocol_version() -> u16 {
    TERMINAL_PROTOCOL_VERSION
}

fn default_observe() -> bool {
    true
}

fn default_max_frame_bytes() -> usize {
    DEFAULT_MAX_FRAME_BYTES
}

fn default_max_replay_bytes() -> usize {
    DEFAULT_MAX_REPLAY_BYTES
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TerminalFrameKind {
    Output = 1,
    Close = 2,
}

impl TerminalFrameKind {
    fn from_wire(value: u8) -> Result<Self, TerminalCodecError> {
        match value {
            1 => Ok(Self::Output),
            2 => Ok(Self::Close),
            other => Err(TerminalCodecError::UnknownFrameKind(other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalFrame {
    pub stream_id: TerminalStreamId,
    pub stream_generation: u64,
    pub sequence: u64,
    pub offset: u64,
    pub kind: TerminalFrameKind,
    pub payload: Vec<u8>,
}

impl TerminalFrame {
    pub fn wire_len(&self) -> usize {
        TERMINAL_FRAME_HEADER_BYTES.saturating_add(self.payload.len())
    }

    pub fn encode(&self, max_frame_bytes: usize) -> Result<Vec<u8>, TerminalCodecError> {
        validate_limits(max_frame_bytes, max_frame_bytes)?;
        if self.stream_generation == 0 || self.sequence == 0 {
            return Err(TerminalCodecError::InvalidFrameIdentity);
        }
        if self.kind == TerminalFrameKind::Close && !self.payload.is_empty() {
            return Err(TerminalCodecError::InvalidClosePayload);
        }
        let max_payload = max_frame_bytes - TERMINAL_FRAME_HEADER_BYTES;
        if self.payload.len() > max_payload {
            return Err(TerminalCodecError::PayloadTooLarge {
                actual: self.payload.len(),
                maximum: max_payload,
            });
        }
        let payload_len =
            u32::try_from(self.payload.len()).map_err(|_| TerminalCodecError::LengthOverflow)?;
        u64::try_from(self.payload.len())
            .ok()
            .and_then(|len| self.offset.checked_add(len))
            .ok_or(TerminalCodecError::OffsetOverflow)?;
        let mut encoded = Vec::with_capacity(self.wire_len());
        encoded.extend_from_slice(FRAME_MAGIC);
        encoded.extend_from_slice(&TERMINAL_PROTOCOL_VERSION.to_be_bytes());
        encoded.push(self.kind as u8);
        encoded.push(0);
        encoded.extend_from_slice(self.stream_id.as_bytes());
        encoded.extend_from_slice(&self.stream_generation.to_be_bytes());
        encoded.extend_from_slice(&self.sequence.to_be_bytes());
        encoded.extend_from_slice(&self.offset.to_be_bytes());
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        Ok(encoded)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalCodecError {
    InvalidStreamId,
    InvalidFrameIdentity,
    InvalidSequence,
    InvalidFlags(u8),
    UnsupportedVersion(u16),
    UnknownFrameKind(u8),
    InvalidClosePayload,
    InvalidLimits,
    FrameTooLarge { actual: usize, maximum: usize },
    PayloadTooLarge { actual: usize, maximum: usize },
    LengthOverflow,
    OffsetOverflow,
    TruncatedHeader { actual: usize },
    TruncatedPayload { expected: usize, actual: usize },
    IdentityMismatch,
    SequenceMismatch { expected: u64, actual: u64 },
    OffsetMismatch { expected: u64, actual: u64 },
}

impl fmt::Display for TerminalCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStreamId => formatter.write_str("terminal stream id is invalid"),
            Self::InvalidFrameIdentity => formatter.write_str("terminal frame identity is invalid"),
            Self::InvalidSequence => formatter.write_str("terminal frame sequence is invalid"),
            Self::InvalidFlags(flags) => {
                write!(formatter, "terminal frame flags {flags:#x} are unsupported")
            }
            Self::UnsupportedVersion(version) => {
                write!(formatter, "terminal frame version {version} is unsupported")
            }
            Self::UnknownFrameKind(kind) => {
                write!(formatter, "terminal frame kind {kind} is unknown")
            }
            Self::InvalidClosePayload => formatter.write_str("terminal close frame has a payload"),
            Self::InvalidLimits => formatter.write_str("terminal frame limits are invalid"),
            Self::FrameTooLarge { actual, maximum } => write!(
                formatter,
                "terminal frame is {actual} bytes; maximum is {maximum}"
            ),
            Self::PayloadTooLarge { actual, maximum } => write!(
                formatter,
                "terminal payload is {actual} bytes; maximum is {maximum}"
            ),
            Self::LengthOverflow => formatter.write_str("terminal frame length overflow"),
            Self::OffsetOverflow => formatter.write_str("terminal frame offset overflow"),
            Self::TruncatedHeader { actual } => write!(
                formatter,
                "terminal frame header is truncated at {actual} bytes"
            ),
            Self::TruncatedPayload { expected, actual } => write!(
                formatter,
                "terminal frame payload is truncated at {actual} of {expected} bytes"
            ),
            Self::IdentityMismatch => formatter.write_str("terminal frame identity mismatch"),
            Self::SequenceMismatch { expected, actual } => write!(
                formatter,
                "terminal frame sequence {actual} does not follow expected {expected}"
            ),
            Self::OffsetMismatch { expected, actual } => write!(
                formatter,
                "terminal frame offset {actual} does not follow expected {expected}"
            ),
        }
    }
}

impl std::error::Error for TerminalCodecError {}

fn validate_limits(max_frame_bytes: usize, replay_bytes: usize) -> Result<(), TerminalCodecError> {
    if !(TERMINAL_FRAME_HEADER_BYTES + 1..=ABSOLUTE_MAX_FRAME_BYTES).contains(&max_frame_bytes)
        || replay_bytes < max_frame_bytes
        || replay_bytes > ABSOLUTE_MAX_REPLAY_BYTES
    {
        return Err(TerminalCodecError::InvalidLimits);
    }
    Ok(())
}

struct PendingFrame {
    sequence: u64,
    offset: u64,
    kind: TerminalFrameKind,
    payload_length: usize,
    payload: Vec<u8>,
}

/// Header-first bounded decoder used by [`TerminalReader`].
pub struct TerminalFrameReader {
    expected_stream_id: TerminalStreamId,
    expected_generation: u64,
    max_frame_bytes: usize,
    header: [u8; TERMINAL_FRAME_HEADER_BYTES],
    header_len: usize,
    pending: Option<PendingFrame>,
    next_sequence: Option<u64>,
    next_offset: Option<u64>,
}

impl TerminalFrameReader {
    pub fn new(
        stream_id: TerminalStreamId,
        stream_generation: u64,
        max_frame_bytes: usize,
        cursor: Option<TerminalCursor>,
    ) -> Result<Self, TerminalCodecError> {
        validate_limits(max_frame_bytes, max_frame_bytes)?;
        if stream_generation == 0 {
            return Err(TerminalCodecError::InvalidFrameIdentity);
        }
        let (next_sequence, next_offset) = cursor
            .map(|cursor| {
                if cursor.sequence == 0 {
                    Err(TerminalCodecError::InvalidSequence)
                } else {
                    Ok((Some(cursor.sequence), Some(cursor.offset)))
                }
            })
            .transpose()?
            .unwrap_or((None, None));
        Ok(Self {
            expected_stream_id: stream_id,
            expected_generation: stream_generation,
            max_frame_bytes,
            header: [0; TERMINAL_FRAME_HEADER_BYTES],
            header_len: 0,
            pending: None,
            next_sequence,
            next_offset,
        })
    }

    pub(crate) fn push(
        &mut self,
        mut bytes: &[u8],
        frames: &mut VecDeque<TerminalFrame>,
    ) -> Result<(), TerminalCodecError> {
        while !bytes.is_empty() {
            if self.pending.is_none() {
                let needed = TERMINAL_FRAME_HEADER_BYTES - self.header_len;
                let copied = needed.min(bytes.len());
                let end = self.header_len + copied;
                self.header[self.header_len..end].copy_from_slice(&bytes[..copied]);
                self.header_len = end;
                bytes = &bytes[copied..];
                if self.header_len < TERMINAL_FRAME_HEADER_BYTES {
                    break;
                }
                self.pending = Some(self.start_payload()?);
            }
            let pending = self.pending.as_mut().expect("pending frame after header");
            let needed = pending.payload_length - pending.payload.len();
            let copied = needed.min(bytes.len());
            pending.payload.extend_from_slice(&bytes[..copied]);
            bytes = &bytes[copied..];
            if pending.payload.len() < pending.payload_length {
                break;
            }
            let pending = self.pending.take().expect("pending frame remains present");
            let next_sequence = pending
                .sequence
                .checked_add(1)
                .ok_or(TerminalCodecError::InvalidSequence)?;
            let next_offset = pending
                .offset
                .checked_add(
                    u64::try_from(pending.payload.len())
                        .map_err(|_| TerminalCodecError::OffsetOverflow)?,
                )
                .ok_or(TerminalCodecError::OffsetOverflow)?;
            frames.push_back(TerminalFrame {
                stream_id: self.expected_stream_id,
                stream_generation: self.expected_generation,
                sequence: pending.sequence,
                offset: pending.offset,
                kind: pending.kind,
                payload: pending.payload,
            });
            self.next_sequence = Some(next_sequence);
            self.next_offset = Some(next_offset);
            self.header_len = 0;
        }
        Ok(())
    }

    pub(crate) fn finish(&self) -> Result<(), TerminalCodecError> {
        if let Some(pending) = &self.pending {
            return Err(TerminalCodecError::TruncatedPayload {
                expected: pending.payload_length,
                actual: pending.payload.len(),
            });
        }
        if self.header_len != 0 {
            return Err(TerminalCodecError::TruncatedHeader {
                actual: self.header_len,
            });
        }
        Ok(())
    }

    fn start_payload(&self) -> Result<PendingFrame, TerminalCodecError> {
        let encoded = &self.header;
        if &encoded[..4] != FRAME_MAGIC {
            return Err(TerminalCodecError::InvalidFrameIdentity);
        }
        let version = u16::from_be_bytes([encoded[4], encoded[5]]);
        if version != TERMINAL_PROTOCOL_VERSION {
            return Err(TerminalCodecError::UnsupportedVersion(version));
        }
        let kind = TerminalFrameKind::from_wire(encoded[6])?;
        if encoded[7] != 0 {
            return Err(TerminalCodecError::InvalidFlags(encoded[7]));
        }
        let stream_id = TerminalStreamId::from_bytes(
            encoded[8..24]
                .try_into()
                .expect("fixed terminal stream id field"),
        );
        if stream_id != self.expected_stream_id {
            return Err(TerminalCodecError::IdentityMismatch);
        }
        let generation = u64::from_be_bytes(
            encoded[24..32]
                .try_into()
                .expect("fixed terminal generation field"),
        );
        if generation != self.expected_generation || generation == 0 {
            return Err(TerminalCodecError::IdentityMismatch);
        }
        let sequence = u64::from_be_bytes(
            encoded[32..40]
                .try_into()
                .expect("fixed terminal sequence field"),
        );
        if sequence == 0 {
            return Err(TerminalCodecError::InvalidSequence);
        }
        if let Some(expected) = self.next_sequence {
            if sequence != expected {
                return Err(TerminalCodecError::SequenceMismatch {
                    expected,
                    actual: sequence,
                });
            }
        }
        let offset = u64::from_be_bytes(
            encoded[40..48]
                .try_into()
                .expect("fixed terminal offset field"),
        );
        if let Some(expected) = self.next_offset {
            if offset != expected {
                return Err(TerminalCodecError::OffsetMismatch {
                    expected,
                    actual: offset,
                });
            }
        }
        let payload_length = u32::from_be_bytes(
            encoded[48..52]
                .try_into()
                .expect("fixed terminal payload length field"),
        ) as usize;
        let maximum = self.max_frame_bytes - TERMINAL_FRAME_HEADER_BYTES;
        if payload_length > maximum {
            return Err(TerminalCodecError::PayloadTooLarge {
                actual: payload_length,
                maximum,
            });
        }
        if kind == TerminalFrameKind::Close && payload_length != 0 {
            return Err(TerminalCodecError::InvalidClosePayload);
        }
        u64::try_from(payload_length)
            .ok()
            .and_then(|len| offset.checked_add(len))
            .ok_or(TerminalCodecError::OffsetOverflow)?;
        Ok(PendingFrame {
            sequence,
            offset,
            kind,
            payload_length,
            payload: Vec::with_capacity(payload_length),
        })
    }
}

/// Stable observe-only stream reader.  It emits complete validated frames and
/// never exposes an unbounded read or allocation to the caller.
pub struct TerminalReader {
    connection: Box<dyn transport::ReadWrite + Send>,
    decoder: TerminalFrameReader,
    buffered: VecDeque<u8>,
    frames: VecDeque<TerminalFrame>,
    payload_remainder: VecDeque<u8>,
    closed: bool,
    response: TerminalAttachResponse,
}

impl TerminalReader {
    pub(crate) fn from_connection(
        connection: transport::TerminalConnection,
        request: &TerminalAttachRequest,
    ) -> Result<Self, ClientError> {
        if connection.response.status != 200 {
            return Err(ClientError::HttpStatus(connection.response.status));
        }
        let response: TerminalAttachResponse =
            serde_json::from_slice(&connection.response.body).map_err(ClientError::InvalidJson)?;
        if response.protocol_version != TERMINAL_PROTOCOL_VERSION
            || response.authority != TerminalAuthority::OwnerLocalIpc
            || response.session_id != request.session_id
            || response.attachment_id != request.attachment_id
            || request
                .stream_id
                .as_deref()
                .is_some_and(|stream_id| stream_id != response.stream_id)
            || request
                .stream_generation
                .is_some_and(|generation| generation != response.stream_generation)
            || response.codec != TerminalCodec::RawBytesV1
            || response.backend != TerminalBackend::PtyRaw
            || !response.permissions.observe
            || response.permissions.control != request.control
            || if request.control {
                response.execution_generation.is_none_or(|value| value == 0)
                    || response.authority_epoch.is_none_or(|value| value == 0)
            } else {
                response.execution_generation.is_some() || response.authority_epoch.is_some()
            }
            || response.max_frame_bytes != request.max_frame_bytes
            || response.replay_bytes < response.max_frame_bytes
            || response.replay_bytes > request.replay_bytes
            || response.cursor.sequence == 0
        {
            return Err(ClientError::InvalidHttpResponse(
                "daemon terminal negotiation did not match the requested terminal stream"
                    .to_owned(),
            ));
        }
        let stream_id = TerminalStreamId::parse(&response.stream_id)
            .map_err(|error| ClientError::InvalidHttpResponse(error.to_string()))?;
        let decoder = TerminalFrameReader::new(
            stream_id,
            response.stream_generation,
            response.max_frame_bytes,
            request.cursor.or(Some(response.cursor)),
        )
        .map_err(|error| ClientError::InvalidHttpResponse(error.to_string()))?;
        Ok(Self {
            connection: connection.stream,
            decoder,
            buffered: connection.buffered_remainder.into_iter().collect(),
            frames: VecDeque::new(),
            payload_remainder: VecDeque::new(),
            closed: false,
            response,
        })
    }

    pub fn negotiation(&self) -> &TerminalAttachResponse {
        &self.response
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn next_frame(&mut self) -> Result<Option<TerminalFrame>, ClientError> {
        self.next_frame_with_timeout(Duration::from_secs(30))
    }

    /// Wait for one frame for at most `timeout`. A timeout leaves the
    /// attachment open and the decoder unchanged, so a bounded worker can
    /// poll or cancel its wait without turning an idle stream into execution
    /// completion.
    pub fn next_frame_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TerminalFrame>, ClientError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| ClientError::Io {
                operation: "failed to calculate Coven terminal read deadline",
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "terminal read deadline overflowed",
                ),
            })?;
        self.next_frame_until(deadline)
    }

    fn next_frame_until(
        &mut self,
        deadline: Instant,
    ) -> Result<Option<TerminalFrame>, ClientError> {
        if let Some(frame) = self.frames.pop_front() {
            if frame.kind == TerminalFrameKind::Close {
                self.closed = true;
            }
            return Ok(Some(frame));
        }
        if self.closed {
            return Ok(None);
        }
        loop {
            if Instant::now() >= deadline {
                return Err(ClientError::Io {
                    operation: "failed to read Coven terminal stream",
                    source: std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timed out waiting for a Coven terminal frame",
                    ),
                });
            }
            if !self.buffered.is_empty() {
                let bytes = self.buffered.drain(..).collect::<Vec<_>>();
                self.decoder
                    .push(&bytes, &mut self.frames)
                    .map_err(|error| ClientError::InvalidHttpResponse(error.to_string()))?;
                if let Some(frame) = self.frames.pop_front() {
                    if frame.kind == TerminalFrameKind::Close {
                        self.closed = true;
                    }
                    return Ok(Some(frame));
                }
                continue;
            }
            let mut bytes = [0_u8; 16 * 1024];
            match self.connection.read(&mut bytes) {
                Ok(0) => {
                    self.decoder
                        .finish()
                        .map_err(|error| ClientError::InvalidHttpResponse(error.to_string()))?;
                    return Err(ClientError::InvalidHttpResponse(
                        "terminal transport ended without an explicit close frame".to_owned(),
                    ));
                }
                Ok(read) => {
                    self.decoder
                        .push(&bytes[..read], &mut self.frames)
                        .map_err(|error| ClientError::InvalidHttpResponse(error.to_string()))?;
                    if let Some(frame) = self.frames.pop_front() {
                        if frame.kind == TerminalFrameKind::Close {
                            self.closed = true;
                        }
                        return Ok(Some(frame));
                    }
                }
                Err(error) if terminal_read_would_block(&error) => {
                    if error.kind() != std::io::ErrorKind::Interrupted {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            return Err(ClientError::Io {
                                operation: "failed to read Coven terminal stream",
                                source: std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "timed out waiting for a Coven terminal frame",
                                ),
                            });
                        }
                        thread::sleep(remaining.min(Duration::from_millis(2)));
                    }
                }
                Err(error) => {
                    return Err(ClientError::Io {
                        operation: "failed to read Coven terminal stream",
                        source: error,
                    })
                }
            }
        }
    }

    pub fn read_frame(&mut self) -> Result<Option<TerminalFrame>, ClientError> {
        self.next_frame()
    }
}

fn terminal_read_would_block(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
    ) || (cfg!(windows) && error.raw_os_error() == Some(232))
}

impl fmt::Debug for TerminalReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminalReader")
            .field("negotiation", &self.response)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl Read for TerminalReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if !self.payload_remainder.is_empty() {
            let count = buffer.len().min(self.payload_remainder.len());
            for slot in &mut buffer[..count] {
                *slot = self
                    .payload_remainder
                    .pop_front()
                    .expect("payload remainder has the advertised length");
            }
            return Ok(count);
        }
        let Some(frame) = self
            .next_frame()
            .map_err(|error| std::io::Error::other(error.to_string()))?
        else {
            return Ok(0);
        };
        if frame.kind == TerminalFrameKind::Close {
            return Ok(0);
        }
        let count = buffer.len().min(frame.payload.len());
        buffer[..count].copy_from_slice(&frame.payload[..count]);
        self.payload_remainder
            .extend(frame.payload[count..].iter().copied());
        Ok(count)
    }
}

impl Write for TerminalReader {
    fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "terminal attachment is observe-only",
        ))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_round_trips_without_uuid_dependency() {
        let id = TerminalStreamId::parse("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        assert_eq!(id.to_string(), "00112233-4455-6677-8899-aabbccddeeff");
        assert_eq!(TerminalStreamId::parse(&id.to_string()), Ok(id));
    }

    #[test]
    fn attach_request_uses_body_for_reserved_session_ids() {
        let request = TerminalAttachRequest::new("engine/events?literal", "attachment");
        let value = serde_json::to_value(request).unwrap();
        assert_eq!(value["sessionId"], "engine/events?literal");
        assert!(value.get("session_id").is_none());
    }

    #[test]
    fn frame_reader_rejects_identity_and_size_before_payload_allocation() {
        let id = TerminalStreamId::from_bytes([1; 16]);
        let frame = TerminalFrame {
            stream_id: id,
            stream_generation: 7,
            sequence: 1,
            offset: 0,
            kind: TerminalFrameKind::Output,
            payload: vec![1, 2, 3],
        };
        let encoded = frame.encode(64).unwrap();
        let mut reader = TerminalFrameReader::new(id, 7, 64, None).unwrap();
        let mut frames = VecDeque::new();
        reader.push(&encoded, &mut frames).unwrap();
        assert_eq!(frames.pop_front(), Some(frame));

        let mut malicious = encoded[..TERMINAL_FRAME_HEADER_BYTES].to_vec();
        malicious[48..52].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut reader = TerminalFrameReader::new(id, 7, 64, None).unwrap();
        assert!(matches!(
            reader.push(&malicious, &mut VecDeque::new()),
            Err(TerminalCodecError::PayloadTooLarge { .. })
        ));
    }
}
