//! Concrete adapter for the checkpointable Alacritty/vte forks.
//!
//! This module is the only place where the shared owner knows the selected
//! engine. Both the daemon and Herdr can depend on this crate and therefore
//! feed the same `Term` and `vte::ansi::Processor` implementation.

use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use alacritty_terminal::{
    event::{Event, EventListener, WindowSize},
    grid::Dimensions,
    term::{Config, Osc52, Term},
};
use serde::{Deserialize, Serialize};
use vte::ansi::{self, checkpoint::CheckpointSyncTimeout, Timeout};

use crate::{
    checkpoint::EngineFingerprint,
    effects::{EffectKind, EffectOrder, OrderedEffect, QueryReplyPolicy, ResizeNotice},
    model::TerminalEngine,
    session::Geometry,
};

const ALACRITTY_UPSTREAM_COMMIT: &str = "94e7c8874e526b1e67b349d9ba30ddf81669119e";
const VTE_UPSTREAM_COMMIT: &str = "3b3da71c34cc1256c7e20981cf03f8eb95e08ffc";

/// Maximum number of combining characters retained by one live cell. The
/// model checkpoint uses the same bound; the owner checks the live grid after
/// every parser byte so an attacker cannot grow an unbounded cell before a
/// later snapshot happens to notice it.
pub const MAX_LIVE_ZERO_WIDTH_CHARS: usize = alacritty_terminal::term::cell::MAX_ZERO_WIDTH_CHARS;

/// A concrete model/parser error. Normal raw ingestion advances the model;
/// construction and restoration can reject checkpoint invariants. A bounded
/// parser overflow is terminal for this engine because the fork has discarded
/// source bytes and cannot provide a trustworthy continuation.
#[derive(Debug)]
pub enum AlacrittyError {
    Geometry(String),
    Model(alacritty_terminal::checkpoint::CheckpointError),
    Processor(vte::checkpoint::CheckpointError),
    /// The parser discarded bytes after an unterminated OSC exceeded the
    /// checkpointable parser limit. The model may have consumed a prefix of
    /// the frame, so this engine is poisoned and must be replaced from a
    /// known-good checkpoint before it can be used again.
    ParserOverflow,
    /// A live cell exceeded the combining-character budget. The model may
    /// have consumed the byte that crossed the boundary, so this engine is
    /// poisoned and must be replaced from a known-good checkpoint.
    LiveStateLimitExceeded {
        actual: usize,
        maximum: usize,
    },
    /// A parser overflow or other unrecoverable ingestion failure has already
    /// poisoned this engine. Callers must restore a complete checkpoint.
    Unavailable,
}

impl fmt::Display for AlacrittyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Geometry(error) => write!(f, "invalid Alacritty geometry: {error}"),
            Self::Model(error) => write!(f, "Alacritty checkpoint error: {error}"),
            Self::Processor(error) => write!(f, "vte processor checkpoint error: {error}"),
            Self::ParserOverflow => f.write_str(
                "vte parser discarded an oversized unterminated OSC; terminal engine is poisoned",
            ),
            Self::LiveStateLimitExceeded { actual, maximum } => write!(
                f,
                "live terminal cell has {actual} combining characters; maximum is {maximum}; terminal engine is poisoned"
            ),
            Self::Unavailable => {
                f.write_str("Alacritty terminal engine is unavailable; restore a checkpoint")
            }
        }
    }
}

impl Error for AlacrittyError {}

/// Snapshot errors are split out so callers can distinguish model and parser
/// validation failures without losing the typed source error.
#[derive(Debug)]
pub enum AlacrittySnapshotError {
    Model(alacritty_terminal::checkpoint::CheckpointError),
    Processor(vte::checkpoint::CheckpointError),
    /// The live engine has been poisoned and cannot produce a trustworthy
    /// checkpoint. A caller must restore from a previously captured one.
    Unavailable,
}

impl fmt::Display for AlacrittySnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(error) => write!(f, "Alacritty model snapshot error: {error}"),
            Self::Processor(error) => write!(f, "vte processor snapshot error: {error}"),
            Self::Unavailable => {
                f.write_str("Alacritty terminal snapshot unavailable; restore a checkpoint")
            }
        }
    }
}

impl Error for AlacrittySnapshotError {}

#[derive(Clone, Copy)]
struct AlacrittyDimensions {
    geometry: Geometry,
}

impl Dimensions for AlacrittyDimensions {
    fn total_lines(&self) -> usize {
        usize::from(self.geometry.rows)
    }

    fn screen_lines(&self) -> usize {
        usize::from(self.geometry.rows)
    }

    fn columns(&self) -> usize {
        usize::from(self.geometry.columns)
    }
}

enum PendingEvent {
    Reply(Vec<u8>),
    Color {
        index: usize,
        /// The model captures this override while parsing the query. It must
        /// be used after synchronized output is flushed instead of looking up
        /// the mutable palette again.
        captured: Option<ansi::Rgb>,
        format: Arc<dyn Fn(ansi::Rgb) -> String + Sync + Send + 'static>,
    },
    TextArea {
        format: Arc<dyn Fn(WindowSize) -> String + Sync + Send + 'static>,
    },
    Suppressed(EffectKind),
}

struct PendingRecord {
    order: EffectOrder,
    event: PendingEvent,
}

#[derive(Default)]
struct PendingEvents {
    next_order: u32,
    events: Vec<PendingRecord>,
}

/// Event listener that never talks to a host. It records only owner-visible
/// query replies or typed suppression diagnostics. Clipboard, bell, title,
/// and file/media operations cannot escape this boundary.
#[derive(Clone)]
pub struct EffectListener {
    policy: QueryReplyPolicy,
    pending: Arc<Mutex<PendingEvents>>,
}

impl EffectListener {
    fn new(policy: QueryReplyPolicy) -> Self {
        Self {
            policy,
            pending: Arc::new(Mutex::new(PendingEvents::default())),
        }
    }

    fn record(&self, event: PendingEvent) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let order = EffectOrder(pending.next_order);
        pending.next_order = pending.next_order.saturating_add(1);
        pending.events.push(PendingRecord { order, event });
    }

    fn clear(&self) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending.next_order = 0;
        pending.events.clear();
    }

    fn take(&self) -> Vec<PendingRecord> {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut pending.events)
    }
}

impl EventListener for EffectListener {
    fn send_event(&self, event: Event) {
        let event = match event {
            Event::PtyWrite(text) => PendingEvent::Reply(text.into_bytes()),
            Event::ColorRequest(index, captured, format) => PendingEvent::Color {
                index,
                captured,
                format,
            },
            Event::TextAreaSizeRequest(format) => PendingEvent::TextArea { format },
            Event::ClipboardStore(_, _) | Event::ClipboardLoad(_, _) => {
                PendingEvent::Suppressed(EffectKind::ClipboardSuppressed)
            }
            Event::Title(_) | Event::ResetTitle => {
                PendingEvent::Suppressed(EffectKind::TitleSuppressed)
            }
            Event::Bell => PendingEvent::Suppressed(EffectKind::BellSuppressed),
            Event::Exit | Event::ChildExit(_) => {
                PendingEvent::Suppressed(EffectKind::ProcessSuppressed)
            }
            Event::MouseCursorDirty | Event::CursorBlinkingChange | Event::Wakeup => return,
        };
        self.record(event);
    }
}

/// Alacritty model and vte processor owned by one [`crate::TerminalSession`].
pub struct AlacrittyEngine {
    term: Term<EffectListener>,
    processor: vte::ansi::Processor<CheckpointSyncTimeout>,
    listener: EffectListener,
    geometry: Geometry,
    /// Once the parser reports an OSC overflow, the parser has intentionally
    /// discarded input and the model/parser pair no longer represents a
    /// trustworthy source prefix. Keep the engine fail-closed instead of
    /// allowing a caller to continue from an ambiguous state.
    poisoned: bool,
}

impl AlacrittyEngine {
    pub fn term(&self) -> &Term<EffectListener> {
        &self.term
    }

    pub fn processor(&self) -> &vte::ansi::Processor<CheckpointSyncTimeout> {
        &self.processor
    }

    /// Whether this engine has consumed input after an unrecoverable parser
    /// overflow. A poisoned engine can only become usable through
    /// `TerminalSession::restore_checkpoint`.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    fn config() -> Config {
        // Keep model semantics for negotiated keyboard/VT modes. The listener
        // suppresses their replies and all host actions independently.
        Config {
            kitty_keyboard: true,
            osc52: Osc52::OnlyCopy,
            ..Config::default()
        }
    }

    fn resolve_events(&self, records: Vec<PendingRecord>) -> Vec<OrderedEffect> {
        records
            .into_iter()
            .filter_map(|record| match record.event {
                PendingEvent::Reply(bytes) => {
                    if self.listener.policy == QueryReplyPolicy::Owner && !bytes.is_empty() {
                        Some(OrderedEffect {
                            order: record.order,
                            kind: EffectKind::Reply(bytes),
                        })
                    } else {
                        None
                    }
                }
                PendingEvent::Color {
                    index,
                    captured,
                    format,
                } => {
                    if self.listener.policy != QueryReplyPolicy::Owner {
                        return None;
                    }
                    // The fork's palette has 269 entries. An invalid query is
                    // ignored instead of producing a reply. Use the value
                    // captured at parse time. `None` means the configured
                    // display override was unset. There is no display-palette
                    // authority in this engine boundary, so suppress the
                    // query instead of fabricating RGB(0, 0, 0) with
                    // `Rgb::default()`.
                    if index >= 269 {
                        return None;
                    }
                    let color = captured?;
                    let bytes = format(color).into_bytes();
                    (!bytes.is_empty()).then_some(OrderedEffect {
                        order: record.order,
                        kind: EffectKind::Reply(bytes),
                    })
                }
                PendingEvent::TextArea { format } => {
                    if self.listener.policy != QueryReplyPolicy::Owner {
                        return None;
                    }
                    let window = WindowSize {
                        num_lines: self.geometry.rows,
                        num_cols: self.geometry.columns,
                        // Keep the calculation in the fork's u16 formatter
                        // bounded even if a caller supplied a large pixel size.
                        cell_width: self.geometry.cell_width.min(255),
                        cell_height: self.geometry.cell_height.min(255),
                    };
                    let bytes = format(window).into_bytes();
                    (!bytes.is_empty()).then_some(OrderedEffect {
                        order: record.order,
                        kind: EffectKind::Reply(bytes),
                    })
                }
                PendingEvent::Suppressed(kind) => Some(OrderedEffect {
                    order: record.order,
                    kind,
                }),
            })
            .collect()
    }

    fn validate_geometry(geometry: Geometry) -> Result<(), AlacrittyError> {
        geometry.validate().map_err(AlacrittyError::Geometry)
    }

    fn mark_parser_overflow(&mut self) -> Result<(), AlacrittyError> {
        if self.poisoned {
            return Err(AlacrittyError::Unavailable);
        }
        if self.processor.osc_overflowed() {
            self.poisoned = true;
            // Effects collected before the overflow belong to a frame that
            // the owner must reject. Drop them so a later diagnostic cannot
            // accidentally expose a partial response.
            self.listener.clear();
            return Err(AlacrittyError::ParserOverflow);
        }
        Ok(())
    }

    fn ensure_available(&self) -> Result<(), AlacrittyError> {
        if self.poisoned {
            Err(AlacrittyError::Unavailable)
        } else {
            Ok(())
        }
    }

    /// Reject a live zero-width write that crossed the shared storage limit.
    /// The Alacritty fork enforces the cap inside `Cell::push_zerowidth`,
    /// before it appends to the cell vector, and records the rejected write on
    /// `Term`. Checking that marker is O(1), which also keeps a large terminal
    /// from paying a full-grid scan for every parser byte.
    fn ensure_live_state_bounded(&mut self) -> Result<(), AlacrittyError> {
        if self.term.zero_width_overflowed() {
            self.poisoned = true;
            self.listener.clear();
            return Err(AlacrittyError::LiveStateLimitExceeded {
                actual: MAX_LIVE_ZERO_WIDTH_CHARS.saturating_add(1),
                maximum: MAX_LIVE_ZERO_WIDTH_CHARS,
            });
        }
        Ok(())
    }

    fn flush_sync(&mut self) -> Result<(), AlacrittyError> {
        // A replayed SyncFlush must reproduce the source transition even if
        // the local wall clock has not reached the source deadline. The owner
        // only emits this event at its timeout boundary, but the event itself
        // is the authority during replay.
        if self.processor.sync_timeout().pending_timeout() {
            self.processor.stop_sync(&mut self.term);
        }
        // `stop_sync` flushes buffered bytes through the parser. That flush
        // can discover an oversized OSC, so check immediately after it too.
        self.mark_parser_overflow()?;
        self.ensure_live_state_bounded()
    }
}

impl TerminalEngine for AlacrittyEngine {
    type ModelCheckpoint = alacritty_terminal::checkpoint::TermCheckpointV1;
    type ProcessorCheckpoint = vte::ansi::checkpoint::ProcessorCheckpointV1;
    type Error = AlacrittyError;
    type SnapshotError = AlacrittySnapshotError;

    fn fingerprint() -> EngineFingerprint {
        EngineFingerprint {
            model_package: "alacritty_terminal".to_owned(),
            model_version: "0.26.0".to_owned(),
            model_revision: ALACRITTY_UPSTREAM_COMMIT.to_owned(),
            model_checkpoint_schema: alacritty_terminal::term::checkpoint::CHECKPOINT_SCHEMA
                .to_owned(),
            model_checkpoint_version: alacritty_terminal::term::checkpoint::CHECKPOINT_VERSION,
            parser_package: "vte".to_owned(),
            parser_version: "0.15.0".to_owned(),
            parser_revision: VTE_UPSTREAM_COMMIT.to_owned(),
            processor_checkpoint_schema: vte::ansi::checkpoint::ProcessorCheckpointV1::SCHEMA
                .to_owned(),
            processor_checkpoint_version: vte::ansi::checkpoint::ProcessorCheckpointV1::VERSION,
        }
    }

    fn new(geometry: Geometry, policy: QueryReplyPolicy) -> Result<Self, Self::Error> {
        Self::validate_geometry(geometry)?;
        let listener = EffectListener::new(policy);
        let dimensions = AlacrittyDimensions { geometry };
        let term = Term::new(Self::config(), &dimensions, listener.clone());
        Ok(Self {
            term,
            processor: vte::ansi::Processor::new(),
            listener,
            geometry,
            poisoned: false,
        })
    }

    fn geometry(&self) -> Option<Geometry> {
        Some(self.geometry)
    }

    fn needs_flush(&self) -> bool {
        self.processor.sync_timeout_expired()
    }

    fn flush(&mut self) -> Result<Vec<OrderedEffect>, Self::Error> {
        self.ensure_available()?;
        self.listener.clear();
        self.flush_sync()?;
        Ok(self.resolve_events(self.listener.take()))
    }

    fn apply_raw(&mut self, bytes: &[u8]) -> Result<Vec<OrderedEffect>, Self::Error> {
        self.ensure_available()?;
        self.listener.clear();
        let mut effects = Vec::new();
        // Advance one byte at a time so a ColorRequest is resolved against the
        // palette at the query point, before a later byte can mutate it.
        for byte in bytes {
            self.processor
                .advance(&mut self.term, std::slice::from_ref(byte));
            // Check after every byte: the fork marks the parser sticky as
            // soon as it would discard the next OSC payload byte, and marks
            // the model when a bounded cell rejects a combining character.
            // This must remain unconditional: a second BSU can flush the
            // prior synchronized buffer while retaining a new one, so neither
            // the input byte's high bit nor a zero sync-buffer length is a
            // reliable signal that model input just occurred.
            self.mark_parser_overflow()?;
            self.ensure_live_state_bounded()?;
            effects.extend(self.resolve_events(self.listener.take()));
        }
        Ok(effects)
    }

    fn resize(&mut self, geometry: Geometry) -> Result<Vec<OrderedEffect>, Self::Error> {
        self.ensure_available()?;
        Self::validate_geometry(geometry)?;
        self.listener.clear();
        self.term.resize(AlacrittyDimensions { geometry });
        self.geometry = geometry;
        self.ensure_live_state_bounded()?;
        let mut effects = self.resolve_events(self.listener.take());
        effects.push(OrderedEffect {
            order: EffectOrder(u32::MAX),
            kind: EffectKind::Resize(ResizeNotice {
                order: EffectOrder(u32::MAX),
                geometry,
            }),
        });
        Ok(effects)
    }

    fn model_checkpoint(&self) -> Result<Self::ModelCheckpoint, Self::SnapshotError> {
        if self.poisoned {
            return Err(AlacrittySnapshotError::Unavailable);
        }
        self.term
            .try_checkpoint()
            .map_err(AlacrittySnapshotError::Model)
    }

    fn processor_checkpoint(&self) -> Result<Self::ProcessorCheckpoint, Self::SnapshotError> {
        if self.poisoned {
            return Err(AlacrittySnapshotError::Unavailable);
        }
        self.processor
            .try_checkpoint()
            .map_err(AlacrittySnapshotError::Processor)
    }

    fn from_checkpoint(
        geometry: Geometry,
        model: &Self::ModelCheckpoint,
        processor: &Self::ProcessorCheckpoint,
        policy: QueryReplyPolicy,
    ) -> Result<Self, Self::SnapshotError> {
        let expected = Self::fingerprint();
        let model_engine = &model.engine;
        if model_engine.package != expected.model_package
            || model_engine.package_version != expected.model_version
            || model_engine.upstream_commit != expected.model_revision
            || model_engine.schema_revision != expected.model_checkpoint_version
            || model_engine.vte_package != expected.parser_package
            || model_engine.vte_version != expected.parser_version
            || model_engine.vte_commit != expected.parser_revision
            || model.schema != expected.model_checkpoint_schema
            || model.version != expected.model_checkpoint_version
            || processor.schema != expected.processor_checkpoint_schema
            || processor.version != expected.processor_checkpoint_version
        {
            return Err(AlacrittySnapshotError::Model(
                alacritty_terminal::checkpoint::CheckpointError::EngineMismatch,
            ));
        }
        if model.dimensions.columns != u32::from(geometry.columns)
            || model.dimensions.screen_lines != u32::from(geometry.rows)
        {
            return Err(AlacrittySnapshotError::Model(
                alacritty_terminal::checkpoint::CheckpointError::Invalid("geometry"),
            ));
        }
        let listener = EffectListener::new(policy);
        let term = Term::from_checkpoint(model.clone(), listener.clone())
            .map_err(AlacrittySnapshotError::Model)?;
        let processor = vte::ansi::Processor::from_checkpoint(processor)
            .map_err(AlacrittySnapshotError::Processor)?;
        Ok(Self {
            term,
            processor,
            listener,
            geometry,
            poisoned: false,
        })
    }
}

// Keep the associated checkpoint DTOs visibly serializable at this boundary;
// this also catches accidental feature regressions in the fork dependency.
#[allow(dead_code)]
fn _checkpoint_types_are_wire_safe<M: Serialize + for<'de> Deserialize<'de>>() {}
