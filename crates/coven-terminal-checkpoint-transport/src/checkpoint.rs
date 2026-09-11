//! Identity-bound checkpoint blob used by the version-2 stream codec.

use std::{error::Error, fmt};

use coven_terminal::{SessionCursor, MAX_CHECKPOINT_BYTES, REVISION_STEP};
use sha2::{Digest, Sha256};

use crate::StreamBinding;

/// The total encoded bound checkpoint, including its fixed header, is capped
/// at the shared runtime's 16 MiB persistence limit.
pub const MAX_BOUND_CHECKPOINT_BYTES: usize = MAX_CHECKPOINT_BYTES;
const CHECKPOINT_MAGIC: &[u8; 4] = b"CTB1";
const CHECKPOINT_VERSION: u16 = 1;
const CHECKPOINT_HEADER_BYTES: usize = 76;

/// A complete shared-runtime checkpoint with the immutable stream binding
/// duplicated outside the model JSON. The duplicate cursor/revision fields
/// let a consumer validate routing before handing the inner model/parser
/// bytes to `TerminalSession::restore_checkpoint_bytes`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundCheckpoint {
    pub binding: StreamBinding,
    pub cursor: SessionCursor,
    pub revision: u64,
    /// The shared runtime's complete model and processor checkpoint bytes.
    /// These bytes are opaque to this transport and are never reconstructed
    /// from ANSI output.
    pub checkpoint_bytes: Vec<u8>,
}

impl BoundCheckpoint {
    pub fn new(
        binding: StreamBinding,
        cursor: SessionCursor,
        revision: u64,
        checkpoint_bytes: Vec<u8>,
    ) -> Result<Self, CheckpointBlobError> {
        let checkpoint = Self {
            binding,
            cursor,
            revision,
            checkpoint_bytes,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    pub fn validate(&self) -> Result<(), CheckpointBlobError> {
        self.binding
            .validate()
            .map_err(CheckpointBlobError::Binding)?;
        if self.cursor.sequence == 0 {
            return Err(CheckpointBlobError::InvalidCursor);
        }
        if self.revision % REVISION_STEP != 0 {
            return Err(CheckpointBlobError::InvalidRevision);
        }
        if self.checkpoint_bytes.is_empty() {
            return Err(CheckpointBlobError::EmptyPayload);
        }
        let maximum = MAX_BOUND_CHECKPOINT_BYTES.saturating_sub(CHECKPOINT_HEADER_BYTES);
        if self.checkpoint_bytes.len() > maximum {
            return Err(CheckpointBlobError::PayloadTooLarge {
                actual: self.checkpoint_bytes.len(),
                maximum,
            });
        }
        Ok(())
    }

    pub fn encoded_len(&self) -> Result<usize, CheckpointBlobError> {
        self.validate()?;
        CHECKPOINT_HEADER_BYTES
            .checked_add(self.checkpoint_bytes.len())
            .ok_or(CheckpointBlobError::LengthOverflow)
    }

    /// Encode a bounded binary envelope. The inner runtime checkpoint remains
    /// byte-for-byte unchanged inside this envelope.
    pub fn encode(&self) -> Result<Vec<u8>, CheckpointBlobError> {
        let encoded_len = self.encoded_len()?;
        let payload_len = u32::try_from(self.checkpoint_bytes.len())
            .map_err(|_| CheckpointBlobError::LengthOverflow)?;
        let mut encoded = Vec::with_capacity(encoded_len);
        encoded.extend_from_slice(CHECKPOINT_MAGIC);
        encoded.extend_from_slice(&CHECKPOINT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&0_u16.to_be_bytes());
        encoded.extend_from_slice(&self.binding.stream_id);
        encoded.extend_from_slice(&self.binding.stream_generation.to_be_bytes());
        encoded.extend_from_slice(&self.binding.execution_generation.to_be_bytes());
        encoded.extend_from_slice(&self.binding.authority_epoch.to_be_bytes());
        encoded.extend_from_slice(&self.cursor.sequence.to_be_bytes());
        encoded.extend_from_slice(&self.cursor.offset.to_be_bytes());
        encoded.extend_from_slice(&self.revision.to_be_bytes());
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(&self.checkpoint_bytes);
        debug_assert_eq!(encoded.len(), encoded_len);
        Ok(encoded)
    }

    /// Decode after checking every fixed field and the payload bound. The
    /// expected binding supplies the authenticated session id, which is not
    /// repeated in the compact binary header.
    pub fn decode_for(
        encoded: &[u8],
        expected: &StreamBinding,
    ) -> Result<Self, CheckpointBlobError> {
        expected.validate().map_err(CheckpointBlobError::Binding)?;
        if encoded.len() > MAX_BOUND_CHECKPOINT_BYTES {
            return Err(CheckpointBlobError::FrameTooLarge {
                actual: encoded.len(),
                maximum: MAX_BOUND_CHECKPOINT_BYTES,
            });
        }
        if encoded.len() < CHECKPOINT_HEADER_BYTES {
            return Err(CheckpointBlobError::TruncatedHeader {
                actual: encoded.len(),
            });
        }
        if &encoded[..4] != CHECKPOINT_MAGIC {
            return Err(CheckpointBlobError::BadMagic);
        }
        let version = read_u16(encoded, 4);
        if version != CHECKPOINT_VERSION {
            return Err(CheckpointBlobError::UnsupportedVersion { version });
        }
        if read_u16(encoded, 6) != 0 {
            return Err(CheckpointBlobError::InvalidFlags);
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
            return Err(CheckpointBlobError::IdentityMismatch);
        }
        let sequence = read_u64(encoded, 48);
        if sequence == 0 {
            return Err(CheckpointBlobError::InvalidCursor);
        }
        let offset = read_u64(encoded, 56);
        let revision = read_u64(encoded, 64);
        if revision % REVISION_STEP != 0 {
            return Err(CheckpointBlobError::InvalidRevision);
        }
        let declared_payload = read_u32(encoded, 72) as usize;
        let maximum = MAX_BOUND_CHECKPOINT_BYTES.saturating_sub(CHECKPOINT_HEADER_BYTES);
        if declared_payload == 0 {
            return Err(CheckpointBlobError::EmptyPayload);
        }
        if declared_payload > maximum {
            return Err(CheckpointBlobError::PayloadTooLarge {
                actual: declared_payload,
                maximum,
            });
        }
        let expected_len = CHECKPOINT_HEADER_BYTES
            .checked_add(declared_payload)
            .ok_or(CheckpointBlobError::LengthOverflow)?;
        if encoded.len() != expected_len {
            return Err(CheckpointBlobError::LengthMismatch {
                declared: declared_payload,
                actual: encoded.len().saturating_sub(CHECKPOINT_HEADER_BYTES),
            });
        }
        Self::new(
            expected.clone(),
            SessionCursor { sequence, offset },
            revision,
            encoded[CHECKPOINT_HEADER_BYTES..].to_vec(),
        )
    }

    pub fn digest(encoded: &[u8]) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(encoded);
        digest.finalize().into()
    }
}

fn read_u16(bytes: &[u8], start: usize) -> u16 {
    u16::from_be_bytes(
        bytes[start..start + 2]
            .try_into()
            .expect("fixed checkpoint field"),
    )
}

fn read_u32(bytes: &[u8], start: usize) -> u32 {
    u32::from_be_bytes(
        bytes[start..start + 4]
            .try_into()
            .expect("fixed checkpoint field"),
    )
}

fn read_u64(bytes: &[u8], start: usize) -> u64 {
    u64::from_be_bytes(
        bytes[start..start + 8]
            .try_into()
            .expect("fixed checkpoint field"),
    )
}

fn read_array<const N: usize>(bytes: &[u8], start: usize) -> [u8; N] {
    bytes[start..start + N]
        .try_into()
        .expect("fixed checkpoint array field")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointBlobError {
    Binding(crate::binding::BindingError),
    InvalidCursor,
    InvalidRevision,
    EmptyPayload,
    PayloadTooLarge { actual: usize, maximum: usize },
    FrameTooLarge { actual: usize, maximum: usize },
    TruncatedHeader { actual: usize },
    BadMagic,
    UnsupportedVersion { version: u16 },
    InvalidFlags,
    IdentityMismatch,
    LengthOverflow,
    LengthMismatch { declared: usize, actual: usize },
}

impl fmt::Display for CheckpointBlobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => error.fmt(formatter),
            Self::InvalidCursor => formatter.write_str("checkpoint cursor is invalid"),
            Self::InvalidRevision => formatter.write_str("checkpoint revision is not stable"),
            Self::EmptyPayload => formatter.write_str("checkpoint payload is empty"),
            Self::PayloadTooLarge { actual, maximum } => write!(
                formatter,
                "checkpoint payload is {actual} bytes; maximum is {maximum}"
            ),
            Self::FrameTooLarge { actual, maximum } => {
                write!(
                    formatter,
                    "checkpoint blob is {actual} bytes; maximum is {maximum}"
                )
            }
            Self::TruncatedHeader { actual } => {
                write!(
                    formatter,
                    "checkpoint blob header is truncated at {actual} bytes"
                )
            }
            Self::BadMagic => formatter.write_str("checkpoint blob magic is invalid"),
            Self::UnsupportedVersion { version } => {
                write!(
                    formatter,
                    "checkpoint blob version {version} is unsupported"
                )
            }
            Self::InvalidFlags => formatter.write_str("checkpoint blob flags are unsupported"),
            Self::IdentityMismatch => formatter.write_str("checkpoint blob identity mismatch"),
            Self::LengthOverflow => formatter.write_str("checkpoint blob length overflow"),
            Self::LengthMismatch { declared, actual } => write!(
                formatter,
                "checkpoint blob declares {declared} payload bytes but carries {actual}"
            ),
        }
    }
}

impl Error for CheckpointBlobError {}
