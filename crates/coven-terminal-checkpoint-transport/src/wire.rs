//! Bounded version-2 checkpoint stream framing.

use std::{collections::VecDeque, error::Error, fmt};

use coven_terminal::{Geometry, SessionCursor, MAX_RAW_CHUNK_BYTES, REVISION_STEP};

use crate::{StreamBinding, MAX_BOUND_CHECKPOINT_BYTES};

/// Fixed header size for a checkpoint-aware frame.
pub const CHECKPOINT_FRAME_HEADER_BYTES: usize = 84;
/// Smallest frame that can carry either fixed-size checkpoint control
/// message. The limit includes the fixed header and the 36-byte control
/// payload (four bytes of total length plus a 32-byte digest).
pub const MIN_CHECKPOINT_FRAME_BYTES: usize = CHECKPOINT_FRAME_HEADER_BYTES + 36;
/// Default encoded frame limit. The limit includes the fixed header.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024;
/// Absolute encoded frame limit.
pub const MAX_CHECKPOINT_FRAME_BYTES: usize = 1024 * 1024;
/// Maximum number of checkpoint or output fragments in one logical transfer.
pub const MAX_CHECKPOINT_PARTS: u32 = 4_096;
/// Maximum number of complete frames accepted from one input call. A caller
/// that needs to submit more data must make another call, which keeps the
/// decoded message and event vectors bounded.
pub const MAX_MESSAGES_PER_PUSH: usize = 256;

const FRAME_MAGIC: &[u8; 4] = b"CTS2";
const FRAME_VERSION: u16 = 2;
const MAX_FRAME_PAYLOAD: usize = MAX_CHECKPOINT_FRAME_BYTES - CHECKPOINT_FRAME_HEADER_BYTES;

/// The metadata shared by model events and checkpoint transfer messages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageMeta {
    pub binding: StreamBinding,
    /// Cursor at which this event or checkpoint begins.
    pub cursor: SessionCursor,
    /// Stable even model revision associated with the event.
    pub revision: u64,
}

impl MessageMeta {
    pub fn validate(&self) -> Result<(), WireCodecError> {
        self.binding.validate().map_err(WireCodecError::Binding)?;
        if self.cursor.sequence == 0 {
            return Err(WireCodecError::InvalidCursor);
        }
        if self.revision % REVISION_STEP != 0 {
            return Err(WireCodecError::InvalidRevision);
        }
        Ok(())
    }
}

/// A typed reason for a consumer needing a fresh checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum GapReason {
    SlowConsumer = 1,
    ReplayUnavailable = 2,
    ProtocolReset = 3,
}

impl GapReason {
    fn from_wire(value: u8) -> Result<Self, WireCodecError> {
        match value {
            1 => Ok(Self::SlowConsumer),
            2 => Ok(Self::ReplayUnavailable),
            3 => Ok(Self::ProtocolReset),
            other => Err(WireCodecError::UnknownGapReason { reason: other }),
        }
    }
}

/// Logical messages emitted by the producer. Output is kept as one logical
/// raw-byte event and is fragmented only by [`StreamMessage::encode_parts`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamMessage {
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
    /// An owner-controlled synchronized-update timer boundary. It consumes
    /// one ordered sequence without consuming provider bytes, so a renderer
    /// can reproduce timer-driven parser state through the same stream.
    SyncFlush {
        meta: MessageMeta,
    },
    Gap {
        binding: StreamBinding,
        requested: SessionCursor,
        available: SessionCursor,
        /// Revision at the producer cursor advertised by `available`.
        /// This is the terminal/resynchronization ticket for delayed close
        /// and checkpoint frames within one generation binding.
        available_revision: u64,
        reason: GapReason,
    },
    Close {
        meta: MessageMeta,
    },
}

impl StreamMessage {
    pub fn meta(&self) -> Option<&MessageMeta> {
        match self {
            Self::CheckpointStart { meta, .. }
            | Self::CheckpointPart { meta, .. }
            | Self::CheckpointEnd { meta, .. }
            | Self::Output { meta, .. }
            | Self::Resize { meta, .. }
            | Self::SyncFlush { meta }
            | Self::Close { meta } => Some(meta),
            Self::Gap { .. } => None,
        }
    }

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

    pub fn estimated_bytes(&self) -> usize {
        match self {
            Self::CheckpointStart { .. } | Self::CheckpointEnd { .. } => {
                CHECKPOINT_FRAME_HEADER_BYTES + 36
            }
            Self::CheckpointPart { bytes, .. } | Self::Output { bytes, .. } => {
                CHECKPOINT_FRAME_HEADER_BYTES.saturating_add(bytes.len())
            }
            Self::Resize { .. } => CHECKPOINT_FRAME_HEADER_BYTES + 8,
            Self::SyncFlush { .. } => CHECKPOINT_FRAME_HEADER_BYTES,
            Self::Gap { .. } => CHECKPOINT_FRAME_HEADER_BYTES + 25,
            Self::Close { .. } => CHECKPOINT_FRAME_HEADER_BYTES,
        }
    }

    pub fn validate(&self) -> Result<(), WireCodecError> {
        match self {
            Self::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                ..
            } => {
                meta.validate()?;
                validate_checkpoint_shape(*total_bytes as usize, *part_count)?;
            }
            Self::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes,
            } => {
                meta.validate()?;
                validate_part_shape(*part_index, *part_count)?;
                if bytes.is_empty() {
                    return Err(WireCodecError::EmptyPayload);
                }
                if bytes.len() > MAX_FRAME_PAYLOAD {
                    return Err(WireCodecError::PayloadTooLarge {
                        actual: bytes.len(),
                        maximum: MAX_FRAME_PAYLOAD,
                    });
                }
            }
            Self::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                ..
            } => {
                meta.validate()?;
                validate_checkpoint_shape(*total_bytes as usize, *part_count)?;
            }
            Self::Output {
                meta,
                part_index,
                part_count,
                bytes,
            } => {
                meta.validate()?;
                validate_part_shape(*part_index, *part_count)?;
                if bytes.is_empty() {
                    return Err(WireCodecError::EmptyPayload);
                }
                if bytes.len() > MAX_RAW_CHUNK_BYTES {
                    return Err(WireCodecError::PayloadTooLarge {
                        actual: bytes.len(),
                        maximum: MAX_RAW_CHUNK_BYTES,
                    });
                }
            }
            Self::Resize { meta, geometry } => {
                meta.validate()?;
                geometry
                    .validate()
                    .map_err(WireCodecError::InvalidGeometry)?;
            }
            Self::SyncFlush { meta } => meta.validate()?,
            Self::Gap {
                binding,
                requested,
                available,
                available_revision,
                ..
            } => {
                binding.validate().map_err(WireCodecError::Binding)?;
                if requested.sequence == 0
                    || available.sequence == 0
                    || available < requested
                    || available_revision % REVISION_STEP != 0
                {
                    return Err(WireCodecError::InvalidCursor);
                }
            }
            Self::Close { meta } => meta.validate()?,
        }
        Ok(())
    }

    /// Encode one logical message into one or more bounded wire frames.
    /// Raw output bytes are split only at transport boundaries and retain
    /// their original order and contents.
    pub fn encode_parts(&self, max_frame_bytes: usize) -> Result<Vec<Vec<u8>>, WireCodecError> {
        validate_frame_limit(max_frame_bytes)?;
        self.validate()?;
        let maximum = max_frame_bytes - CHECKPOINT_FRAME_HEADER_BYTES;
        match self {
            Self::Output { meta, bytes, .. } => {
                let part_count = checked_part_count(bytes.len(), maximum)?;
                (0..part_count)
                    .map(|part_index| {
                        let start = part_index as usize * maximum;
                        let end = (start + maximum).min(bytes.len());
                        encode_frame(
                            FrameKind::Output,
                            meta,
                            part_index,
                            part_count,
                            &bytes[start..end],
                            max_frame_bytes,
                        )
                    })
                    .collect()
            }
            Self::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                digest,
            } => {
                validate_checkpoint_capacity(*total_bytes as usize, *part_count, maximum)?;
                let mut payload = Vec::with_capacity(36);
                payload.extend_from_slice(&total_bytes.to_be_bytes());
                payload.extend_from_slice(digest);
                Ok(vec![encode_frame(
                    FrameKind::CheckpointStart,
                    meta,
                    0,
                    *part_count,
                    &payload,
                    max_frame_bytes,
                )?])
            }
            Self::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes,
            } => Ok(vec![encode_frame(
                FrameKind::CheckpointPart,
                meta,
                *part_index,
                *part_count,
                bytes,
                max_frame_bytes,
            )?]),
            Self::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                digest,
            } => {
                validate_checkpoint_capacity(*total_bytes as usize, *part_count, maximum)?;
                let mut payload = Vec::with_capacity(36);
                payload.extend_from_slice(&total_bytes.to_be_bytes());
                payload.extend_from_slice(digest);
                Ok(vec![encode_frame(
                    FrameKind::CheckpointEnd,
                    meta,
                    *part_count,
                    *part_count,
                    &payload,
                    max_frame_bytes,
                )?])
            }
            Self::Resize { meta, geometry } => Ok(vec![encode_frame(
                FrameKind::Resize,
                meta,
                0,
                0,
                &geometry_payload(*geometry),
                max_frame_bytes,
            )?]),
            Self::SyncFlush { meta } => Ok(vec![encode_frame(
                FrameKind::SyncFlush,
                meta,
                0,
                0,
                &[],
                max_frame_bytes,
            )?]),
            Self::Gap {
                binding,
                requested,
                available,
                available_revision,
                reason,
            } => {
                let meta = MessageMeta {
                    binding: binding.clone(),
                    cursor: *requested,
                    revision: 0,
                };
                let mut payload = Vec::with_capacity(25);
                payload.extend_from_slice(&available.sequence.to_be_bytes());
                payload.extend_from_slice(&available.offset.to_be_bytes());
                payload.extend_from_slice(&available_revision.to_be_bytes());
                payload.push(*reason as u8);
                Ok(vec![encode_frame(
                    FrameKind::Gap,
                    &meta,
                    0,
                    0,
                    &payload,
                    max_frame_bytes,
                )?])
            }
            Self::Close { meta } => Ok(vec![encode_frame(
                FrameKind::Close,
                meta,
                0,
                0,
                &[],
                max_frame_bytes,
            )?]),
        }
    }

    /// Encode only messages that fit in one frame. Callers carrying raw
    /// output should use [`Self::encode_parts`] instead.
    pub fn encode(&self, max_frame_bytes: usize) -> Result<Vec<u8>, WireCodecError> {
        let mut frames = self.encode_parts(max_frame_bytes)?;
        if frames.len() != 1 {
            return Err(WireCodecError::RequiresMultipart {
                parts: frames.len(),
            });
        }
        Ok(frames.pop().expect("one encoded frame"))
    }
}

fn validate_checkpoint_shape(total_bytes: usize, part_count: u32) -> Result<(), WireCodecError> {
    if total_bytes == 0 {
        return Err(WireCodecError::EmptyPayload);
    }
    if total_bytes > MAX_BOUND_CHECKPOINT_BYTES {
        return Err(WireCodecError::CheckpointTooLarge {
            actual: total_bytes,
            maximum: MAX_BOUND_CHECKPOINT_BYTES,
        });
    }
    if part_count == 0 || part_count > MAX_CHECKPOINT_PARTS {
        return Err(WireCodecError::InvalidPartCount { part_count });
    }
    Ok(())
}

fn validate_checkpoint_capacity(
    total_bytes: usize,
    part_count: u32,
    maximum: usize,
) -> Result<(), WireCodecError> {
    if maximum == 0 {
        return Err(WireCodecError::InvalidFrameLimit);
    }
    let required_parts = total_bytes
        .checked_add(maximum - 1)
        .ok_or(WireCodecError::LengthOverflow)?
        / maximum;
    if required_parts > part_count as usize {
        return Err(WireCodecError::InvalidCheckpointFrame);
    }
    Ok(())
}

fn validate_part_shape(part_index: u32, part_count: u32) -> Result<(), WireCodecError> {
    if part_count == 0 || part_count > MAX_CHECKPOINT_PARTS {
        return Err(WireCodecError::InvalidPartCount { part_count });
    }
    if part_index >= part_count {
        return Err(WireCodecError::InvalidPartIndex {
            part_index,
            part_count,
        });
    }
    Ok(())
}

fn checked_part_count(bytes: usize, maximum: usize) -> Result<u32, WireCodecError> {
    if maximum == 0 {
        return Err(WireCodecError::InvalidFrameLimit);
    }
    let parts = bytes
        .checked_add(maximum - 1)
        .ok_or(WireCodecError::LengthOverflow)?
        / maximum;
    let parts = u32::try_from(parts).map_err(|_| WireCodecError::InvalidPartCount {
        part_count: u32::MAX,
    })?;
    if parts == 0 || parts > MAX_CHECKPOINT_PARTS {
        return Err(WireCodecError::InvalidPartCount { part_count: parts });
    }
    Ok(parts)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum FrameKind {
    CheckpointStart = 1,
    CheckpointPart = 2,
    CheckpointEnd = 3,
    Output = 4,
    Resize = 5,
    Gap = 6,
    Close = 7,
    /// Appended after the original kinds so the version-2 wire assignments
    /// for Gap and Close remain stable for already encoded messages.
    SyncFlush = 8,
}

impl FrameKind {
    fn from_wire(value: u8) -> Result<Self, WireCodecError> {
        match value {
            1 => Ok(Self::CheckpointStart),
            2 => Ok(Self::CheckpointPart),
            3 => Ok(Self::CheckpointEnd),
            4 => Ok(Self::Output),
            5 => Ok(Self::Resize),
            6 => Ok(Self::Gap),
            7 => Ok(Self::Close),
            8 => Ok(Self::SyncFlush),
            other => Err(WireCodecError::UnknownFrameKind { kind: other }),
        }
    }
}

fn encode_frame(
    kind: FrameKind,
    meta: &MessageMeta,
    part_index: u32,
    part_count: u32,
    payload: &[u8],
    max_frame_bytes: usize,
) -> Result<Vec<u8>, WireCodecError> {
    if payload.len() > max_frame_bytes - CHECKPOINT_FRAME_HEADER_BYTES {
        return Err(WireCodecError::PayloadTooLarge {
            actual: payload.len(),
            maximum: max_frame_bytes - CHECKPOINT_FRAME_HEADER_BYTES,
        });
    }
    let frame_len = CHECKPOINT_FRAME_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(WireCodecError::LengthOverflow)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| WireCodecError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(frame_len);
    encoded.extend_from_slice(FRAME_MAGIC);
    encoded.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    encoded.push(kind as u8);
    encoded.push(0);
    encoded.extend_from_slice(&meta.binding.stream_id);
    encoded.extend_from_slice(&meta.binding.stream_generation.to_be_bytes());
    encoded.extend_from_slice(&meta.binding.execution_generation.to_be_bytes());
    encoded.extend_from_slice(&meta.binding.authority_epoch.to_be_bytes());
    encoded.extend_from_slice(&meta.cursor.sequence.to_be_bytes());
    encoded.extend_from_slice(&meta.cursor.offset.to_be_bytes());
    encoded.extend_from_slice(&meta.revision.to_be_bytes());
    encoded.extend_from_slice(&part_index.to_be_bytes());
    encoded.extend_from_slice(&part_count.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(payload);
    debug_assert_eq!(encoded.len(), frame_len);
    Ok(encoded)
}

fn geometry_payload(geometry: Geometry) -> [u8; 8] {
    let mut payload = [0_u8; 8];
    payload[..2].copy_from_slice(&geometry.columns.to_be_bytes());
    payload[2..4].copy_from_slice(&geometry.rows.to_be_bytes());
    payload[4..6].copy_from_slice(&geometry.cell_width.to_be_bytes());
    payload[6..8].copy_from_slice(&geometry.cell_height.to_be_bytes());
    payload
}

fn decode_geometry(payload: &[u8]) -> Geometry {
    Geometry::with_cell_size(
        u16::from_be_bytes(payload[..2].try_into().expect("fixed geometry field")),
        u16::from_be_bytes(payload[2..4].try_into().expect("fixed geometry field")),
        u16::from_be_bytes(payload[4..6].try_into().expect("fixed geometry field")),
        u16::from_be_bytes(payload[6..8].try_into().expect("fixed geometry field")),
    )
}

fn validate_frame_limit(max_frame_bytes: usize) -> Result<(), WireCodecError> {
    if !(MIN_CHECKPOINT_FRAME_BYTES..=MAX_CHECKPOINT_FRAME_BYTES).contains(&max_frame_bytes) {
        return Err(WireCodecError::InvalidFrameLimit);
    }
    Ok(())
}

/// Decode exactly one frame after binding it to the authenticated stream.
/// The payload is copied only after its declared size passes the configured
/// frame bound and the complete frame length matches.
pub fn decode_frame(
    encoded: &[u8],
    max_frame_bytes: usize,
    expected: &StreamBinding,
) -> Result<StreamMessage, WireCodecError> {
    validate_frame_limit(max_frame_bytes)?;
    expected.validate().map_err(WireCodecError::Binding)?;
    if encoded.len() < CHECKPOINT_FRAME_HEADER_BYTES {
        return Err(WireCodecError::TruncatedHeader {
            actual: encoded.len(),
        });
    }
    if encoded.len() > max_frame_bytes {
        return Err(WireCodecError::FrameTooLarge {
            actual: encoded.len(),
            maximum: max_frame_bytes,
        });
    }
    if &encoded[..4] != FRAME_MAGIC {
        return Err(WireCodecError::BadMagic);
    }
    let version = read_u16(encoded, 4);
    if version != FRAME_VERSION {
        return Err(WireCodecError::UnsupportedVersion { version });
    }
    let kind = FrameKind::from_wire(encoded[6])?;
    if encoded[7] != 0 {
        return Err(WireCodecError::InvalidFlags { flags: encoded[7] });
    }
    let stream_id = read_array::<16>(encoded, 8);
    let stream_generation = read_u64(encoded, 24);
    let execution_generation = read_u64(encoded, 32);
    let authority_epoch = read_u64(encoded, 40);
    if stream_id != expected.stream_id
        || stream_generation != expected.stream_generation
        || execution_generation != expected.execution_generation
        || authority_epoch != expected.authority_epoch
    {
        return Err(WireCodecError::IdentityMismatch);
    }
    let sequence = read_u64(encoded, 48);
    if sequence == 0 {
        return Err(WireCodecError::InvalidCursor);
    }
    let offset = read_u64(encoded, 56);
    let revision = read_u64(encoded, 64);
    if kind != FrameKind::Gap && revision % REVISION_STEP != 0 {
        return Err(WireCodecError::InvalidRevision);
    }
    if kind == FrameKind::Gap && revision != 0 {
        return Err(WireCodecError::InvalidRevision);
    }
    let part_index = read_u32(encoded, 72);
    let part_count = read_u32(encoded, 76);
    let declared_payload = read_u32(encoded, 80) as usize;
    let maximum = max_frame_bytes - CHECKPOINT_FRAME_HEADER_BYTES;
    if declared_payload > maximum {
        return Err(WireCodecError::PayloadTooLarge {
            actual: declared_payload,
            maximum,
        });
    }
    let expected_len = CHECKPOINT_FRAME_HEADER_BYTES
        .checked_add(declared_payload)
        .ok_or(WireCodecError::LengthOverflow)?;
    if encoded.len() != expected_len {
        return Err(WireCodecError::LengthMismatch {
            declared: declared_payload,
            actual: encoded.len().saturating_sub(CHECKPOINT_FRAME_HEADER_BYTES),
        });
    }
    let payload = &encoded[CHECKPOINT_FRAME_HEADER_BYTES..];
    let meta = MessageMeta {
        binding: expected.clone(),
        cursor: SessionCursor { sequence, offset },
        revision,
    };
    match kind {
        FrameKind::CheckpointStart => {
            if part_index != 0 {
                return Err(WireCodecError::InvalidPartIndex {
                    part_index,
                    part_count,
                });
            }
            validate_checkpoint_shape_from_wire(part_count, payload, maximum)?;
            let total_bytes = read_u32(payload, 0);
            let digest = read_array::<32>(payload, 4);
            Ok(StreamMessage::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                digest,
            })
        }
        FrameKind::CheckpointPart => {
            validate_part_shape(part_index, part_count)?;
            if payload.is_empty() {
                return Err(WireCodecError::EmptyPayload);
            }
            Ok(StreamMessage::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes: payload.to_vec(),
            })
        }
        FrameKind::CheckpointEnd => {
            if part_index != part_count {
                return Err(WireCodecError::InvalidPartIndex {
                    part_index,
                    part_count,
                });
            }
            validate_checkpoint_shape_from_wire(part_count, payload, maximum)?;
            let total_bytes = read_u32(payload, 0);
            let digest = read_array::<32>(payload, 4);
            Ok(StreamMessage::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                digest,
            })
        }
        FrameKind::Output => {
            validate_part_shape(part_index, part_count)?;
            if payload.is_empty() {
                return Err(WireCodecError::EmptyPayload);
            }
            Ok(StreamMessage::Output {
                meta,
                part_index,
                part_count,
                bytes: payload.to_vec(),
            })
        }
        FrameKind::Resize => {
            if part_index != 0 || part_count != 0 || payload.len() != 8 {
                return Err(WireCodecError::InvalidResizeFrame);
            }
            let geometry = decode_geometry(payload);
            geometry
                .validate()
                .map_err(WireCodecError::InvalidGeometry)?;
            Ok(StreamMessage::Resize { meta, geometry })
        }
        FrameKind::SyncFlush => {
            if part_index != 0 || part_count != 0 || !payload.is_empty() {
                return Err(WireCodecError::InvalidSyncFlushFrame);
            }
            Ok(StreamMessage::SyncFlush { meta })
        }
        FrameKind::Gap => {
            if part_index != 0 || part_count != 0 || payload.len() != 25 {
                return Err(WireCodecError::InvalidGapFrame);
            }
            let available = SessionCursor {
                sequence: read_u64(payload, 0),
                offset: read_u64(payload, 8),
            };
            if available.sequence == 0 {
                return Err(WireCodecError::InvalidCursor);
            }
            let available_revision = read_u64(payload, 16);
            if available < meta.cursor || available_revision % REVISION_STEP != 0 {
                return Err(WireCodecError::InvalidGapFrame);
            }
            let reason = GapReason::from_wire(payload[24])?;
            Ok(StreamMessage::Gap {
                binding: expected.clone(),
                requested: meta.cursor,
                available,
                available_revision,
                reason,
            })
        }
        FrameKind::Close => {
            if part_index != 0 || part_count != 0 || !payload.is_empty() {
                return Err(WireCodecError::InvalidCloseFrame);
            }
            Ok(StreamMessage::Close { meta })
        }
    }
}

fn validate_checkpoint_shape_from_wire(
    part_count: u32,
    payload: &[u8],
    maximum: usize,
) -> Result<(), WireCodecError> {
    if payload.len() != 36 {
        return Err(WireCodecError::InvalidCheckpointFrame);
    }
    let total_bytes = read_u32(payload, 0) as usize;
    validate_checkpoint_shape(total_bytes, part_count)?;
    validate_checkpoint_capacity(total_bytes, part_count, maximum)
}

fn read_u16(bytes: &[u8], start: usize) -> u16 {
    u16::from_be_bytes(
        bytes[start..start + 2]
            .try_into()
            .expect("fixed frame field"),
    )
}

fn read_u32(bytes: &[u8], start: usize) -> u32 {
    u32::from_be_bytes(
        bytes[start..start + 4]
            .try_into()
            .expect("fixed frame field"),
    )
}

fn read_u64(bytes: &[u8], start: usize) -> u64 {
    u64::from_be_bytes(
        bytes[start..start + 8]
            .try_into()
            .expect("fixed frame field"),
    )
}

fn read_array<const N: usize>(bytes: &[u8], start: usize) -> [u8; N] {
    bytes[start..start + N]
        .try_into()
        .expect("fixed frame array field")
}

/// Incremental header-first decoder for version-2 frames.
pub struct CheckpointFrameReader {
    expected: StreamBinding,
    max_frame_bytes: usize,
    header: [u8; CHECKPOINT_FRAME_HEADER_BYTES],
    header_len: usize,
    pending_payload_len: Option<usize>,
    payload: Vec<u8>,
    failed: bool,
    finished: bool,
}

impl CheckpointFrameReader {
    pub fn new(expected: StreamBinding, max_frame_bytes: usize) -> Result<Self, WireCodecError> {
        validate_frame_limit(max_frame_bytes)?;
        expected.validate().map_err(WireCodecError::Binding)?;
        Ok(Self {
            expected,
            max_frame_bytes,
            header: [0; CHECKPOINT_FRAME_HEADER_BYTES],
            header_len: 0,
            pending_payload_len: None,
            payload: Vec::new(),
            failed: false,
            finished: false,
        })
    }

    /// Feed arbitrary transport chunks and emit complete typed messages.
    /// Declared payload lengths are checked before the first payload
    /// allocation; malformed input permanently fails closed.
    pub fn push(
        &mut self,
        mut bytes: &[u8],
        messages: &mut VecDeque<StreamMessage>,
    ) -> Result<(), WireCodecError> {
        if self.failed {
            return Err(WireCodecError::ReaderFailed);
        }
        if self.finished {
            return Err(WireCodecError::ReaderFinished);
        }
        let initial_messages = messages.len();
        while !bytes.is_empty() {
            if messages.len().saturating_sub(initial_messages) >= MAX_MESSAGES_PER_PUSH {
                self.failed = true;
                return Err(WireCodecError::BatchTooLarge {
                    maximum: MAX_MESSAGES_PER_PUSH,
                });
            }
            if self.pending_payload_len.is_none() {
                let needed = CHECKPOINT_FRAME_HEADER_BYTES - self.header_len;
                let copied = needed.min(bytes.len());
                let end = self.header_len + copied;
                self.header[self.header_len..end].copy_from_slice(&bytes[..copied]);
                self.header_len = end;
                bytes = &bytes[copied..];
                if self.header_len < CHECKPOINT_FRAME_HEADER_BYTES {
                    break;
                }
                let payload_len = match self.start_payload() {
                    Ok(length) => length,
                    Err(error) => {
                        self.failed = true;
                        return Err(error);
                    }
                };
                self.payload.clear();
                self.payload.reserve(payload_len);
                self.pending_payload_len = Some(payload_len);
                if payload_len == 0 {
                    let message =
                        match decode_frame(&self.header, self.max_frame_bytes, &self.expected) {
                            Ok(message) => message,
                            Err(error) => {
                                self.failed = true;
                                return Err(error);
                            }
                        };
                    messages.push_back(message);
                    self.header_len = 0;
                    self.pending_payload_len = None;
                    self.payload.clear();
                    continue;
                }
            }

            let payload_len = self
                .pending_payload_len
                .expect("payload length is set after a complete header");
            let needed = payload_len.saturating_sub(self.payload.len());
            let copied = needed.min(bytes.len());
            self.payload.extend_from_slice(&bytes[..copied]);
            bytes = &bytes[copied..];
            if self.payload.len() < payload_len {
                break;
            }

            let mut encoded = Vec::with_capacity(CHECKPOINT_FRAME_HEADER_BYTES + payload_len);
            encoded.extend_from_slice(&self.header);
            encoded.extend_from_slice(&self.payload);
            let message = match decode_frame(&encoded, self.max_frame_bytes, &self.expected) {
                Ok(message) => message,
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            };
            messages.push_back(message);
            self.header_len = 0;
            self.pending_payload_len = None;
            self.payload.clear();
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), WireCodecError> {
        if self.failed {
            return Err(WireCodecError::ReaderFailed);
        }
        if self.finished {
            return Ok(());
        }
        if let Some(expected) = self.pending_payload_len {
            let error = WireCodecError::TruncatedPayload {
                expected,
                actual: self.payload.len(),
            };
            self.failed = true;
            return Err(error);
        }
        if self.header_len != 0 {
            let error = WireCodecError::TruncatedHeader {
                actual: self.header_len,
            };
            self.failed = true;
            return Err(error);
        }
        self.finished = true;
        Ok(())
    }

    fn start_payload(&self) -> Result<usize, WireCodecError> {
        let header = &self.header;
        if &header[..4] != FRAME_MAGIC {
            return Err(WireCodecError::BadMagic);
        }
        let version = read_u16(header, 4);
        if version != FRAME_VERSION {
            return Err(WireCodecError::UnsupportedVersion { version });
        }
        let kind = FrameKind::from_wire(header[6])?;
        if header[7] != 0 {
            return Err(WireCodecError::InvalidFlags { flags: header[7] });
        }
        let stream_id = read_array::<16>(header, 8);
        let stream_generation = read_u64(header, 24);
        let execution_generation = read_u64(header, 32);
        let authority_epoch = read_u64(header, 40);
        if stream_id != self.expected.stream_id
            || stream_generation != self.expected.stream_generation
            || execution_generation != self.expected.execution_generation
            || authority_epoch != self.expected.authority_epoch
        {
            return Err(WireCodecError::IdentityMismatch);
        }
        if read_u64(header, 48) == 0 {
            return Err(WireCodecError::InvalidCursor);
        }
        let revision = read_u64(header, 64);
        if (kind == FrameKind::Gap && revision != 0)
            || (kind != FrameKind::Gap && revision % REVISION_STEP != 0)
        {
            return Err(WireCodecError::InvalidRevision);
        }
        let declared = read_u32(header, 80) as usize;
        let maximum = self.max_frame_bytes - CHECKPOINT_FRAME_HEADER_BYTES;
        if declared > maximum {
            return Err(WireCodecError::PayloadTooLarge {
                actual: declared,
                maximum,
            });
        }
        Ok(declared)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireCodecError {
    Binding(crate::binding::BindingError),
    InvalidFrameLimit,
    TruncatedHeader { actual: usize },
    TruncatedPayload { expected: usize, actual: usize },
    FrameTooLarge { actual: usize, maximum: usize },
    BadMagic,
    UnsupportedVersion { version: u16 },
    UnknownFrameKind { kind: u8 },
    InvalidFlags { flags: u8 },
    IdentityMismatch,
    InvalidCursor,
    InvalidRevision,
    InvalidPartCount { part_count: u32 },
    InvalidPartIndex { part_index: u32, part_count: u32 },
    InvalidCheckpointFrame,
    InvalidResizeFrame,
    InvalidSyncFlushFrame,
    InvalidGapFrame,
    InvalidCloseFrame,
    UnknownGapReason { reason: u8 },
    EmptyPayload,
    PayloadTooLarge { actual: usize, maximum: usize },
    CheckpointTooLarge { actual: usize, maximum: usize },
    LengthOverflow,
    LengthMismatch { declared: usize, actual: usize },
    RequiresMultipart { parts: usize },
    BatchTooLarge { maximum: usize },
    InvalidGeometry(String),
    ReaderFailed,
    ReaderFinished,
}

impl fmt::Display for WireCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => error.fmt(formatter),
            Self::InvalidFrameLimit => formatter.write_str("checkpoint frame limit is invalid"),
            Self::TruncatedHeader { actual } => {
                write!(
                    formatter,
                    "checkpoint frame header is truncated at {actual} bytes"
                )
            }
            Self::TruncatedPayload { expected, actual } => write!(
                formatter,
                "checkpoint frame payload is truncated at {actual} of {expected} bytes"
            ),
            Self::FrameTooLarge { actual, maximum } => {
                write!(
                    formatter,
                    "checkpoint frame is {actual} bytes; maximum is {maximum}"
                )
            }
            Self::BadMagic => formatter.write_str("checkpoint frame magic is invalid"),
            Self::UnsupportedVersion { version } => {
                write!(
                    formatter,
                    "checkpoint frame version {version} is unsupported"
                )
            }
            Self::UnknownFrameKind { kind } => {
                write!(formatter, "checkpoint frame kind {kind} is unknown")
            }
            Self::InvalidFlags { flags } => {
                write!(
                    formatter,
                    "checkpoint frame flags {flags:#x} are unsupported"
                )
            }
            Self::IdentityMismatch => formatter.write_str("checkpoint frame identity mismatch"),
            Self::InvalidCursor => formatter.write_str("checkpoint frame cursor is invalid"),
            Self::InvalidRevision => formatter.write_str("checkpoint frame revision is invalid"),
            Self::InvalidPartCount { part_count } => {
                write!(
                    formatter,
                    "checkpoint frame part count {part_count} is invalid"
                )
            }
            Self::InvalidPartIndex {
                part_index,
                part_count,
            } => write!(
                formatter,
                "checkpoint frame part index {part_index} is invalid for {part_count} parts"
            ),
            Self::InvalidCheckpointFrame => {
                formatter.write_str("checkpoint frame metadata is invalid")
            }
            Self::InvalidResizeFrame => formatter.write_str("resize frame is invalid"),
            Self::InvalidSyncFlushFrame => formatter.write_str("sync-flush frame is invalid"),
            Self::InvalidGapFrame => formatter.write_str("gap frame is invalid"),
            Self::InvalidCloseFrame => formatter.write_str("close frame is invalid"),
            Self::UnknownGapReason { reason } => {
                write!(formatter, "gap reason {reason} is unknown")
            }
            Self::EmptyPayload => formatter.write_str("checkpoint frame payload is empty"),
            Self::PayloadTooLarge { actual, maximum } => {
                write!(
                    formatter,
                    "frame payload is {actual} bytes; maximum is {maximum}"
                )
            }
            Self::CheckpointTooLarge { actual, maximum } => write!(
                formatter,
                "checkpoint transfer is {actual} bytes; maximum is {maximum}"
            ),
            Self::LengthOverflow => formatter.write_str("checkpoint frame length overflow"),
            Self::LengthMismatch { declared, actual } => write!(
                formatter,
                "checkpoint frame declares {declared} payload bytes but carries {actual}"
            ),
            Self::RequiresMultipart { parts } => {
                write!(
                    formatter,
                    "logical message requires {parts} transport frames"
                )
            }
            Self::BatchTooLarge { maximum } => {
                write!(
                    formatter,
                    "checkpoint frame batch exceeds {maximum} messages"
                )
            }
            Self::InvalidGeometry(error) => write!(formatter, "invalid terminal geometry: {error}"),
            Self::ReaderFailed => formatter.write_str("checkpoint frame reader is failed closed"),
            Self::ReaderFinished => formatter.write_str("checkpoint frame reader is finished"),
        }
    }
}

impl Error for WireCodecError {}
