//! Engine boundary used by the shared owner.

use std::{error::Error, fmt};

use serde::{de::DeserializeOwned, Serialize};

use crate::{checkpoint::EngineFingerprint, effects::OrderedEffect, session::Geometry};

/// An engine error is kept opaque by the protocol layer but must be printable
/// for a typed rejection response.
pub trait EngineError: Error + Send + Sync + 'static {}

impl<T> EngineError for T where T: Error + Send + Sync + 'static {}

/// Errors raised while taking or restoring an engine snapshot.
pub trait EngineSnapshotError: Error + Send + Sync + 'static {}

impl<T> EngineSnapshotError for T where T: Error + Send + Sync + 'static {}

/// The model/parser engine consumed by [`crate::TerminalSession`].
///
/// Implementations normally own one terminal model, one VTE processor, and a
/// single event sink. `apply_raw` must be transactional at the engine level
/// whenever it can preserve the prior state: if it returns an ordinary error,
/// neither the model nor parser continuation may have advanced. A bounded
/// parser may discover after consuming a prefix that it discarded source
/// bytes. In that case an implementation may return a terminal error only if
/// it permanently poisons the engine, rejects every subsequent operation and
/// snapshot, and requires replacement from a known-good checkpoint. The outer
/// owner commits its cursor and replay frame only after a successful operation.
pub trait TerminalEngine: Sized + Send + 'static {
    type ModelCheckpoint: Clone + Serialize + DeserializeOwned + Send + 'static;
    type ProcessorCheckpoint: Clone + Serialize + DeserializeOwned + Send + 'static;
    type Error: EngineError;
    type SnapshotError: EngineSnapshotError;

    /// The exact engine fingerprint accepted by this implementation.
    fn fingerprint() -> EngineFingerprint;

    /// Construct a fresh model and parser at the supplied geometry.
    fn new(geometry: Geometry, policy: crate::QueryReplyPolicy) -> Result<Self, Self::Error>;

    /// Feed one raw provider chunk. The bytes must not be normalized or
    /// passed to any other parser. Returned effects retain model event order.
    fn apply_raw(&mut self, bytes: &[u8]) -> Result<Vec<OrderedEffect>, Self::Error>;

    /// Apply a resize operation owned by the same serialized session owner.
    fn resize(&mut self, geometry: Geometry) -> Result<Vec<OrderedEffect>, Self::Error>;

    /// Report the dimensions currently owned by the engine when the adapter
    /// can expose them.  A concrete adapter should implement this whenever its
    /// model has a readable geometry; the session then rejects an
    /// accidentally mismatched `from_engine` admission before it publishes a
    /// cursor or accepts input.  `None` is retained as a compatibility escape
    /// hatch for engines whose model API does not expose dimensions.
    fn geometry(&self) -> Option<Geometry> {
        None
    }

    /// Nonmutating owner-side preflight for a timer-driven synchronized
    /// update boundary.  The source owner should call this while holding its
    /// serialized model lock and publish [`crate::ReplayEvent::SyncFlush`]
    /// before the next output, resize, or idle transition when it returns
    /// `true`.  Replay consumers must not infer timer transitions from their
    /// local wall clock; the ordered flush event is the replay authority.
    fn needs_flush(&self) -> bool {
        false
    }

    /// Flush an owner-controlled timer boundary without consuming provider
    /// bytes.  Engines with synchronized-update buffering should flush only
    /// when their timer boundary has elapsed.  The default keeps simple test
    /// and adapter engines source-compatible; the ordered replay event is
    /// still recorded by [`crate::TerminalSession`].
    fn flush(&mut self) -> Result<Vec<OrderedEffect>, Self::Error> {
        Ok(Vec::new())
    }

    /// Export model state, including both primary and alternate grids,
    /// cursor/modes/attributes, titles, and every parse-affecting setting.
    fn model_checkpoint(&self) -> Result<Self::ModelCheckpoint, Self::SnapshotError>;

    /// Export parser and synchronized-update processor continuation state.
    fn processor_checkpoint(&self) -> Result<Self::ProcessorCheckpoint, Self::SnapshotError>;

    /// Restore a complete engine without replaying an ANSI prefix. The caller
    /// has already checked envelope identity, size, cursor, and geometry; this
    /// method must still validate every fork-level field before construction.
    fn from_checkpoint(
        geometry: Geometry,
        model: &Self::ModelCheckpoint,
        processor: &Self::ProcessorCheckpoint,
        policy: crate::QueryReplyPolicy,
    ) -> Result<Self, Self::SnapshotError>;
}

/// A simple error useful for test engines and adapters that need to reject a
/// malformed operation without another dependency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelError(pub String);

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ModelError {}
