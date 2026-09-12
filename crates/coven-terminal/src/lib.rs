//! Shared owner-local raw terminal runtime.
//!
//! The runtime has three deliberately separate responsibilities:
//!
//! * [`TerminalEngine`] owns the actual VT model and parser/processor.
//! * [`TerminalSession`] is the one owner that applies raw chunks, assigns
//!   stream cursors, and records the bounded replay window.
//! * [`FullTerminalCheckpoint`] is a versioned envelope for model state,
//!   parser continuation, processor continuation, geometry, and the trusted
//!   source cursor.
//!
//! A provider chunk is passed to the engine byte-for-byte exactly once.  The
//! runtime never creates an ANSI history and never restores by replaying a
//! prefix.  A model implementation must expose a complete checkpoint of its
//! screen state and the parser implementation must expose its continuation
//! state.

mod checkpoint;
mod effects;
mod model;
mod replay;
mod session;

pub use checkpoint::{
    CheckpointDecodeError, CheckpointEnvelopeError, EngineFingerprint, FullTerminalCheckpoint,
    CHECKPOINT_SCHEMA, CHECKPOINT_VERSION, MAX_CHECKPOINT_BYTES,
};
pub use effects::{EffectKind, EffectOrder, OrderedEffect, QueryReplyPolicy, ResizeNotice};
pub use model::{EngineError, EngineSnapshotError, ModelError, TerminalEngine};
pub use replay::{ReplayError, ReplayEvent, ReplayFrame, ReplayWindow};
pub use session::{
    ApplyError, ApplyReport, Geometry, RawChunk, SessionCursor, SessionError, TerminalCheckpoint,
    TerminalSession, TerminalSessionResult, MAX_RAW_CHUNK_BYTES, MAX_REPLAY_BYTES,
    MAX_REPLAY_FRAMES, REVISION_STEP,
};

#[cfg(feature = "alacritty-engine")]
pub mod alacritty;
