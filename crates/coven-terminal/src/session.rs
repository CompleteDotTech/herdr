//! The serialized terminal owner and raw input transaction boundary.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::{CheckpointDecodeError, CheckpointEnvelopeError, FullTerminalCheckpoint},
    effects::{EffectKind, OrderedEffect, QueryReplyPolicy},
    model::TerminalEngine,
    replay::{ReplayError, ReplayEvent, ReplayFrame, ReplayWindow},
};

/// Maximum one raw provider chunk accepted by the owner.
pub const MAX_RAW_CHUNK_BYTES: usize = 1024 * 1024;
/// Maximum total retained replay bytes.
pub const MAX_REPLAY_BYTES: usize = 16 * 1024 * 1024;
/// Maximum retained raw frames.
pub const MAX_REPLAY_FRAMES: usize = 16_384;
/// Maximum terminal columns accepted by the shared model boundary.
pub const MAX_COLUMNS: u16 = 1_024;
/// Maximum terminal rows accepted by the shared model boundary.
pub const MAX_ROWS: u16 = 1_024;
/// Stable revisions are even. The owner may reserve the intervening odd
/// value while publishing an in-flight render, but committed states exposed by
/// this draft advance by two.
pub const REVISION_STEP: u64 = 2;

/// Model dimensions. Cell pixel dimensions are retained for ordered resize
/// metadata even though the VT model consumes only columns and rows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Geometry {
    pub columns: u16,
    pub rows: u16,
    #[serde(default)]
    pub cell_width: u16,
    #[serde(default)]
    pub cell_height: u16,
}

impl Geometry {
    pub const fn new(columns: u16, rows: u16) -> Self {
        Self {
            columns,
            rows,
            cell_width: 0,
            cell_height: 0,
        }
    }

    pub const fn with_cell_size(
        columns: u16,
        rows: u16,
        cell_width: u16,
        cell_height: u16,
    ) -> Self {
        Self {
            columns,
            rows,
            cell_width,
            cell_height,
        }
    }

    pub fn validate(self) -> Result<(), String> {
        if self.columns < 2 || self.columns > MAX_COLUMNS {
            return Err(format!("columns must be between 2 and {MAX_COLUMNS}"));
        }
        if self.rows == 0 || self.rows > MAX_ROWS {
            return Err(format!("rows must be between 1 and {MAX_ROWS}"));
        }
        Ok(())
    }
}

/// Exact position at which the next output frame begins.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionCursor {
    pub sequence: u64,
    pub offset: u64,
}

impl SessionCursor {
    pub const START: Self = Self {
        sequence: 1,
        offset: 0,
    };

    pub fn after(self, byte_len: usize) -> Result<Self, ReplayError> {
        Ok(Self {
            sequence: self
                .sequence
                .checked_add(1)
                .ok_or(ReplayError::CursorOverflow)?,
            offset: self
                .offset
                .checked_add(byte_len as u64)
                .ok_or(ReplayError::CursorOverflow)?,
        })
    }
}

/// A raw output frame supplied by the trusted owner-local transport.
///
/// The bytes are deliberately opaque. The runtime does not strip ANSI,
/// transcode UTF-8, append a newline, or turn the frame into text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawChunk {
    pub cursor: SessionCursor,
    pub bytes: Vec<u8>,
}

impl RawChunk {
    pub fn new(cursor: SessionCursor, bytes: Vec<u8>) -> Self {
        Self { cursor, bytes }
    }

    pub fn from_parts(sequence: u64, offset: u64, bytes: Vec<u8>) -> Self {
        Self::new(SessionCursor { sequence, offset }, bytes)
    }

    pub fn into_frame(self) -> ReplayFrame {
        ReplayFrame::output(self.cursor, self.bytes)
    }
}

/// Result of one successful model operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyReport {
    pub cursor: SessionCursor,
    pub revision: u64,
    pub effects: Vec<OrderedEffect>,
}

impl ApplyReport {
    pub fn replies(&self) -> impl Iterator<Item = &[u8]> {
        self.effects.iter().filter_map(OrderedEffect::reply)
    }

    pub fn resizes(&self) -> impl Iterator<Item = crate::ResizeNotice> + '_ {
        self.effects.iter().filter_map(OrderedEffect::resize)
    }
}

/// Errors while applying a raw frame.
#[derive(Debug)]
pub enum ApplyError<E> {
    EmptyChunk,
    ChunkTooLarge {
        actual: usize,
        maximum: usize,
    },
    CursorMismatch {
        expected: SessionCursor,
        actual: SessionCursor,
    },
    /// The source supplied a frame from an already committed cursor. The
    /// owner never replays a provider frame to make an uncertain operation
    /// appear successful; callers must obtain a checkpoint and resynchronize.
    Retransmission {
        expected: SessionCursor,
        actual: SessionCursor,
    },
    InvalidGeometry(String),
    CursorOverflow,
    Replay(ReplayError),
    Engine(E),
}

impl<E: fmt::Display> fmt::Display for ApplyError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyChunk => f.write_str("empty terminal output chunk"),
            Self::ChunkTooLarge { actual, maximum } => {
                write!(
                    f,
                    "terminal output chunk is {actual} bytes, maximum is {maximum}"
                )
            }
            Self::CursorMismatch { expected, actual } => {
                write!(
                    f,
                    "terminal cursor mismatch: expected {expected:?}, got {actual:?}"
                )
            }
            Self::Retransmission { expected, actual } => write!(
                f,
                "terminal frame retransmission rejected at {actual:?}; owner is at {expected:?}, checkpoint resynchronization required"
            ),
            Self::InvalidGeometry(error) => write!(f, "invalid terminal geometry: {error}"),
            Self::CursorOverflow => f.write_str("terminal cursor overflow"),
            Self::Replay(error) => error.fmt(f),
            Self::Engine(error) => write!(f, "terminal engine rejected output: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for ApplyError<E> {}

/// Errors constructing, checkpointing, or restoring a session.
#[derive(Debug)]
pub enum SessionError<E, S> {
    InvalidGeometry(String),
    GeometryMismatch {
        expected: Geometry,
        actual: Geometry,
    },
    InvalidReplayLimit,
    Engine(E),
    Snapshot(S),
    Envelope(CheckpointEnvelopeError),
    Decode(CheckpointDecodeError),
    Replay(ReplayError),
}

/// The concrete checkpoint envelope for an engine implementation.
pub type TerminalCheckpoint<M> = FullTerminalCheckpoint<
    <M as TerminalEngine>::ModelCheckpoint,
    <M as TerminalEngine>::ProcessorCheckpoint,
>;

/// Result alias used by owner construction, checkpoint, and restore methods.
pub type TerminalSessionResult<M, T> =
    Result<T, SessionError<<M as TerminalEngine>::Error, <M as TerminalEngine>::SnapshotError>>;

impl<E: fmt::Display, S: fmt::Display> fmt::Display for SessionError<E, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGeometry(error) => write!(f, "invalid terminal geometry: {error}"),
            Self::GeometryMismatch { expected, actual } => write!(
                f,
                "terminal engine geometry mismatch: expected {expected:?}, got {actual:?}"
            ),
            Self::InvalidReplayLimit => f.write_str("invalid terminal replay limit"),
            Self::Engine(error) => write!(f, "terminal engine rejected state: {error}"),
            Self::Snapshot(error) => write!(f, "terminal snapshot failed: {error}"),
            Self::Envelope(error) => error.fmt(f),
            Self::Decode(error) => error.fmt(f),
            Self::Replay(error) => error.fmt(f),
        }
    }
}

impl<E: Error + 'static, S: Error + 'static> Error for SessionError<E, S> {}

/// One serialized owner for a model and its parser/processor.
pub struct TerminalSession<M: TerminalEngine> {
    engine: M,
    geometry: Geometry,
    cursor: SessionCursor,
    revision: u64,
    policy: QueryReplyPolicy,
    replay: ReplayWindow,
}

impl<M: TerminalEngine> TerminalSession<M> {
    pub fn new(
        geometry: Geometry,
        policy: QueryReplyPolicy,
        max_replay_bytes: usize,
        max_replay_frames: usize,
    ) -> Result<Self, SessionError<M::Error, M::SnapshotError>> {
        geometry.validate().map_err(SessionError::InvalidGeometry)?;
        let engine = M::new(geometry, policy).map_err(SessionError::Engine)?;
        Self::from_engine(
            engine,
            geometry,
            policy,
            max_replay_bytes,
            max_replay_frames,
        )
    }

    pub fn from_engine(
        engine: M,
        geometry: Geometry,
        policy: QueryReplyPolicy,
        max_replay_bytes: usize,
        max_replay_frames: usize,
    ) -> Result<Self, SessionError<M::Error, M::SnapshotError>> {
        geometry.validate().map_err(SessionError::InvalidGeometry)?;
        if let Some(actual) = engine.geometry() {
            if actual != geometry {
                return Err(SessionError::GeometryMismatch {
                    expected: geometry,
                    actual,
                });
            }
        }
        let replay = ReplayWindow::new(max_replay_bytes, max_replay_frames, SessionCursor::START)
            .map_err(|_| SessionError::InvalidReplayLimit)?;
        Ok(Self {
            engine,
            geometry,
            cursor: SessionCursor::START,
            revision: 0,
            policy,
            replay,
        })
    }

    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    pub fn cursor(&self) -> SessionCursor {
        self.cursor
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn query_policy(&self) -> QueryReplyPolicy {
        self.policy
    }

    pub fn engine(&self) -> &M {
        &self.engine
    }

    /// Nonmutating source-owner preflight for a timer-driven synchronized
    /// update boundary. The owner must call this while it holds the same
    /// serialized session lock used for `ingest`, `resize`, and `flush`.
    /// Consumers replay the explicit `SyncFlush` event and must not consult
    /// their local wall clock to decide whether a frame is valid.
    pub fn needs_flush(&self) -> bool {
        self.engine.needs_flush()
    }

    /// Emit an ordered flush only when the source engine reports an elapsed
    /// synchronized-update deadline. The check and mutation share this
    /// `&mut self` boundary, so callers do not need to retry a possibly
    /// partially applied output frame after an engine error.
    pub fn flush_if_needed(&mut self) -> Result<Option<ApplyReport>, ApplyError<M::Error>> {
        if self.engine.needs_flush() {
            self.flush().map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn replay(&self) -> &ReplayWindow {
        &self.replay
    }

    /// Apply exactly one raw output frame. Cursor and replay state are
    /// committed only after the engine accepts the whole frame.
    pub fn ingest(&mut self, chunk: RawChunk) -> Result<ApplyReport, ApplyError<M::Error>> {
        self.apply_frame(chunk.into_frame())
    }

    /// Apply one ordered output, resize, or synchronized-update flush event.
    /// Every event advances the sequence; only output advances the byte offset.
    /// Cursor, revision, geometry, and replay are committed only after the
    /// engine accepts the complete event.
    pub fn apply_frame(&mut self, frame: ReplayFrame) -> Result<ApplyReport, ApplyError<M::Error>> {
        if frame.cursor() < self.cursor {
            return Err(ApplyError::Retransmission {
                expected: self.cursor,
                actual: frame.cursor(),
            });
        }
        if frame.cursor() != self.cursor {
            return Err(ApplyError::CursorMismatch {
                expected: self.cursor,
                actual: frame.cursor(),
            });
        }
        match &frame.event {
            ReplayEvent::Output(bytes) if bytes.is_empty() => return Err(ApplyError::EmptyChunk),
            ReplayEvent::Output(bytes) if bytes.len() > MAX_RAW_CHUNK_BYTES => {
                return Err(ApplyError::ChunkTooLarge {
                    actual: bytes.len(),
                    maximum: MAX_RAW_CHUNK_BYTES,
                })
            }
            ReplayEvent::Resize(geometry) => {
                geometry.validate().map_err(ApplyError::InvalidGeometry)?;
            }
            ReplayEvent::Output(_) | ReplayEvent::SyncFlush => {}
        }
        let next = self
            .replay
            .preflight_append(&frame)
            .map_err(ApplyError::Replay)?;
        // Check this before invoking the engine. In particular, a resize must
        // not mutate the model and then discover that revision bookkeeping
        // cannot advance.
        let next_revision = self
            .revision
            .checked_add(REVISION_STEP)
            .ok_or(ApplyError::CursorOverflow)?;
        debug_assert_eq!(next, frame.end_cursor().expect("preflight checked"));

        let (effects, next_geometry) = match &frame.event {
            ReplayEvent::Output(bytes) => (
                self.engine.apply_raw(bytes).map_err(ApplyError::Engine)?,
                None,
            ),
            ReplayEvent::Resize(geometry) => (
                self.engine.resize(*geometry).map_err(ApplyError::Engine)?,
                Some(*geometry),
            ),
            ReplayEvent::SyncFlush => (self.engine.flush().map_err(ApplyError::Engine)?, None),
        };
        self.replay.append(frame).map_err(ApplyError::Replay)?;
        self.cursor = next;
        self.revision = next_revision;
        if let Some(geometry) = next_geometry {
            self.geometry = geometry;
        }

        // A model adapter should enforce this too, but the owner is the final
        // policy boundary: no quiet session can leak reply bytes.
        let effects = match self.policy {
            QueryReplyPolicy::Owner => effects,
            QueryReplyPolicy::Quiet => effects
                .into_iter()
                .filter(|effect| !matches!(effect.kind, EffectKind::Reply(_)))
                .collect(),
        };
        Ok(ApplyReport {
            cursor: self.cursor,
            revision: self.revision,
            effects,
        })
    }

    /// Resize the same model owner through the ordered frame path. The
    /// sequence advances while the raw byte offset remains unchanged.
    pub fn resize(&mut self, geometry: Geometry) -> Result<ApplyReport, ApplyError<M::Error>> {
        self.apply_frame(ReplayFrame::resize(self.cursor, geometry))
    }

    /// Advance the owner-controlled synchronized-update timer boundary. This
    /// operation consumes no provider bytes but is retained as an ordered
    /// [`ReplayEvent::SyncFlush`] so replay and a renderer that remains quiet
    /// see the same parser/model transition. Callers must issue this before
    /// admitting the next provider frame when a timer may have expired.
    pub fn flush(&mut self) -> Result<ApplyReport, ApplyError<M::Error>> {
        self.apply_frame(ReplayFrame::sync_flush(self.cursor))
    }

    pub fn replay_from(&self, cursor: SessionCursor) -> Result<Vec<ReplayFrame>, ReplayError> {
        self.replay.replay_from(cursor)
    }

    /// Snapshot model and processor state without changing the owner.
    pub fn checkpoint(&self) -> TerminalSessionResult<M, TerminalCheckpoint<M>> {
        let model = self
            .engine
            .model_checkpoint()
            .map_err(SessionError::Snapshot)?;
        let processor = self
            .engine
            .processor_checkpoint()
            .map_err(SessionError::Snapshot)?;
        Ok(FullTerminalCheckpoint {
            schema: crate::CHECKPOINT_SCHEMA.to_owned(),
            version: crate::CHECKPOINT_VERSION,
            engine: M::fingerprint(),
            geometry: self.geometry,
            next_sequence: self.cursor.sequence,
            next_offset: self.cursor.offset,
            revision: self.revision,
            model,
            processor,
        })
    }

    #[cfg(feature = "serde-json")]
    pub fn checkpoint_bytes(&self) -> Result<Vec<u8>, SessionError<M::Error, M::SnapshotError>>
    where
        M::ModelCheckpoint: serde::Serialize + serde::de::DeserializeOwned,
        M::ProcessorCheckpoint: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.checkpoint()
            .and_then(|checkpoint| checkpoint.encode_bounded().map_err(SessionError::Decode))
    }

    /// Construct a complete session off to the side. No live state is touched
    /// until this returns successfully.
    pub fn from_checkpoint(
        checkpoint: &FullTerminalCheckpoint<M::ModelCheckpoint, M::ProcessorCheckpoint>,
        policy: QueryReplyPolicy,
        max_replay_bytes: usize,
        max_replay_frames: usize,
    ) -> Result<Self, SessionError<M::Error, M::SnapshotError>> {
        checkpoint
            .validate_envelope(&M::fingerprint())
            .map_err(SessionError::Envelope)?;
        let engine = M::from_checkpoint(
            checkpoint.geometry,
            &checkpoint.model,
            &checkpoint.processor,
            policy,
        )
        .map_err(SessionError::Snapshot)?;
        if let Some(actual) = engine.geometry() {
            if actual != checkpoint.geometry {
                return Err(SessionError::GeometryMismatch {
                    expected: checkpoint.geometry,
                    actual,
                });
            }
        }
        let replay = ReplayWindow::new(max_replay_bytes, max_replay_frames, checkpoint.cursor())
            .map_err(|_| SessionError::InvalidReplayLimit)?;
        Ok(Self {
            engine,
            geometry: checkpoint.geometry,
            cursor: checkpoint.cursor(),
            revision: checkpoint.revision,
            policy,
            replay,
        })
    }

    /// Validate and atomically replace this session from a full checkpoint.
    pub fn restore_checkpoint(
        &mut self,
        checkpoint: &FullTerminalCheckpoint<M::ModelCheckpoint, M::ProcessorCheckpoint>,
        policy: QueryReplyPolicy,
    ) -> Result<(), SessionError<M::Error, M::SnapshotError>> {
        let restored = Self::from_checkpoint(
            checkpoint,
            policy,
            self.replay.max_bytes(),
            self.replay.max_frames(),
        )?;
        *self = restored;
        Ok(())
    }

    #[cfg(feature = "serde-json")]
    pub fn restore_checkpoint_bytes(
        &mut self,
        bytes: &[u8],
        policy: QueryReplyPolicy,
    ) -> Result<(), SessionError<M::Error, M::SnapshotError>>
    where
        M::ModelCheckpoint: serde::Serialize + serde::de::DeserializeOwned,
        M::ProcessorCheckpoint: serde::Serialize + serde::de::DeserializeOwned,
    {
        let checkpoint =
            FullTerminalCheckpoint::<M::ModelCheckpoint, M::ProcessorCheckpoint>::decode_bounded(
                bytes,
            )
            .map_err(SessionError::Decode)?;
        self.restore_checkpoint(&checkpoint, policy)
    }
}
