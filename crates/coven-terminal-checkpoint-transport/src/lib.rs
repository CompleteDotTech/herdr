//! Checkpoint-aware transport for the shared raw terminal owner.
//!
//! This draft adds a second negotiated wire codec beside Coven's existing
//! `RawBytesV1` observer stream. The old codec remains version 1 and keeps its
//! fixed `CTS1` frame layout. `CheckpointV1` uses version 2 and carries a
//! complete bound checkpoint followed by ordered raw output and resize events.
//!
//! The producer owns one [`coven_terminal::TerminalSession`] and serializes
//! output and resize operations under one lock. A consumer never queries a
//! terminal model while idle: it only drains already queued messages, and a
//! full checkpoint is produced only for an explicit initial attach or resync.
//! Checkpoint payloads are bounded, multipart, identity-bound, and validated
//! before the consumer replaces its model.

mod binding;
mod checkpoint;
mod coordinator;
mod negotiation;
mod wire;

pub use binding::{BindingError, StreamBinding, MAX_SESSION_ID_BYTES};
pub use checkpoint::{BoundCheckpoint, CheckpointBlobError, MAX_BOUND_CHECKPOINT_BYTES};
pub use coordinator::{
    ConsumerError, ConsumerEvent, ConsumerState, ProducerError, ProducerLimitError, ProducerLimits,
    ProducerReport, ProducerStatus, SubscriberId, SubscriberReceipt, TerminalStreamConsumer,
    TerminalStreamProducer,
};
pub use negotiation::{
    negotiate, CodecNegotiation, NegotiatedCodec, NegotiationError, TerminalCodec,
    CHECKPOINT_PROTOCOL_VERSION, RAW_PROTOCOL_VERSION,
};
pub use wire::{
    decode_frame, CheckpointFrameReader, GapReason, MessageMeta, StreamMessage, WireCodecError,
    CHECKPOINT_FRAME_HEADER_BYTES, DEFAULT_MAX_FRAME_BYTES, MAX_CHECKPOINT_FRAME_BYTES,
    MAX_CHECKPOINT_PARTS, MAX_MESSAGES_PER_PUSH, MIN_CHECKPOINT_FRAME_BYTES,
};

// Re-export the shared owner primitives used by the generic coordinator. This
// keeps the handoff API explicit while the draft is later moved beside the
// runtime crate in Herdr.
pub use coven_terminal::SessionCursor;
pub use coven_terminal::{
    ApplyError, EffectKind, EffectOrder, EngineFingerprint, Geometry, OrderedEffect,
    QueryReplyPolicy, RawChunk, ReplayEvent, ReplayFrame, ResizeNotice, SessionError,
    TerminalEngine, TerminalSession, MAX_RAW_CHUNK_BYTES,
};
