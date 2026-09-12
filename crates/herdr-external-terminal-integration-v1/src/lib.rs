//! Private, compile-oriented Herdr integration for a checkpointed external
//! terminal.
//!
//! `ExternalTerminalSession` is the single owner of one
//! `coven_terminal::TerminalSession<AlacrittyEngine>`.  Provider polling calls
//! [`ExternalTerminalSession::ingest`] once for each accepted raw chunk;
//! renderers and reads take immutable snapshots from that same model.  This
//! draft intentionally has no provider client, PTY, ANSI replay path, or
//! native Ghostty dependency.

#![deny(unsafe_code)]

use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, RwLock,
    },
};

use alacritty_terminal::checkpoint::{
    CellCheckpointV1, ColorCheckpointV1, CursorShapeV1, NamedColorV1, RowCheckpointV1,
    TermCheckpointV1,
};
use coven_terminal::{
    alacritty::AlacrittyEngine, ApplyReport, Geometry, QueryReplyPolicy, RawChunk, SessionError,
    TerminalSession,
};
use coven_terminal_checkpoint_transport::{
    BoundCheckpoint, ConsumerEvent, StreamBinding, StreamMessage, TerminalStreamConsumer,
};
use herdr_external_terminal_renderer_v1::{
    render_into_buffer, ExternalCellView, ExternalColor, ExternalCursor, ExternalHyperlink,
    ExternalModes, ExternalPalette, ExternalRenderError, ExternalRgb, ExternalRowView,
    ExternalTerminalView,
};
pub use herdr_external_terminal_renderer_v1::{ExternalCellFlags, ExternalCellWidth};
pub use herdr_external_terminal_renderer_v1::{
    ExternalCursorPolicy, ExternalCursorShape, ExternalMouseMode, ExternalRenderOptions,
    ExternalRenderOutput, ExternalScrollMetrics, ExternalTheme,
};
use ratatui::{buffer::Buffer, layout::Rect};

const SHOW_CURSOR: u32 = 1 << 0;
const MOUSE_REPORT_CLICK: u32 = 1 << 3;
const BRACKETED_PASTE: u32 = 1 << 4;
const SGR_MOUSE: u32 = 1 << 5;
const MOUSE_MOTION: u32 = 1 << 6;
const LINE_WRAP: u32 = 1 << 7;
const ORIGIN: u32 = 1 << 9;
const INSERT: u32 = 1 << 10;
const FOCUS_IN_OUT: u32 = 1 << 11;
const ALT_SCREEN: u32 = 1 << 12;
const MOUSE_DRAG: u32 = 1 << 13;

/// The concrete session result used by the shared Alacritty engine.
pub type SessionResult<T> = Result<
    T,
    SessionError<
        coven_terminal::alacritty::AlacrittyError,
        coven_terminal::alacritty::AlacrittySnapshotError,
    >,
>;

/// A read-only line returned by the external model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalReadLine {
    pub text: String,
    pub model_row: usize,
    /// True when this row is a continuation of the preceding logical line.
    /// The owner uses this flag for `RecentUnwrapped` reads.
    pub soft_wrapped: bool,
}

/// A structured hyperlink in the current model viewport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalHyperlinkSnapshot {
    pub row: u16,
    pub column: u16,
    pub symbol: String,
    pub id: String,
    pub uri: String,
}

/// Errors at the session-to-renderer boundary.
#[derive(Debug)]
pub enum ExternalSessionError {
    Session(String),
    Model(String),
    Transport(String),
    Closed,
    RevisionExhausted,
    Render(ExternalRenderError),
    Scroll(String),
}

impl fmt::Display for ExternalSessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => write!(f, "external terminal session error: {error}"),
            Self::Model(error) => write!(f, "external terminal model error: {error}"),
            Self::Transport(error) => write!(f, "external terminal transport error: {error}"),
            Self::Closed => f.write_str("external terminal session is closed"),
            Self::RevisionExhausted => f.write_str("external terminal revision exhausted"),
            Self::Render(error) => write!(f, "external terminal render failed: {error}"),
            Self::Scroll(error) => write!(f, "external terminal scroll failed: {error}"),
        }
    }
}

impl std::error::Error for ExternalSessionError {}

impl From<ExternalRenderError> for ExternalSessionError {
    fn from(error: ExternalRenderError) -> Self {
        Self::Render(error)
    }
}

/// Owned cell data copied from the fork checkpoint.  Keeping this owned lets
/// the model lock be released before ratatui rendering or API serialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalCellSnapshot {
    pub base: char,
    pub zero_width: Vec<char>,
    pub fg: ExternalColor,
    pub bg: ExternalColor,
    pub underline_color: Option<ExternalColor>,
    pub flags: ExternalCellFlags,
    pub width: herdr_external_terminal_renderer_v1::ExternalCellWidth,
    pub hyperlink: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRowSnapshot {
    pub cells: Vec<ExternalCellSnapshot>,
}

/// A complete, owned external viewport snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalTerminalSnapshot {
    pub content_revision: u64,
    pub columns: u16,
    pub screen_lines: u16,
    pub rows: Vec<ExternalRowSnapshot>,
    pub palette: ExternalPalette,
    pub theme: ExternalTheme,
    pub cursor: Option<ExternalCursor>,
    pub scroll: ExternalScrollMetrics,
    pub modes: ExternalModes,
    pub hyperlinks: Vec<(String, String)>,
}

impl ExternalTerminalSnapshot {
    /// Render the exact snapshot into one complete ratatui surface.
    pub fn render_into_buffer(
        &self,
        buffer: &mut Buffer,
        area: Rect,
        options: ExternalRenderOptions,
    ) -> Result<ExternalRenderOutput, ExternalRenderError> {
        let mut borrowed_cells = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            borrowed_cells.push(
                row.cells
                    .iter()
                    .map(|cell| ExternalCellView {
                        base: cell.base,
                        zero_width: &cell.zero_width,
                        fg: cell.fg,
                        bg: cell.bg,
                        underline_color: cell.underline_color,
                        flags: cell.flags,
                        width: cell.width,
                        hyperlink: cell.hyperlink,
                    })
                    .collect::<Vec<_>>(),
            );
        }
        let borrowed_rows = borrowed_cells
            .iter()
            .map(|cells| ExternalRowView { cells })
            .collect::<Vec<_>>();
        let borrowed_links = self
            .hyperlinks
            .iter()
            .map(|(id, uri)| ExternalHyperlink { id, uri })
            .collect::<Vec<_>>();
        let view = ExternalTerminalView {
            content_revision: self.content_revision,
            columns: self.columns,
            screen_lines: self.screen_lines,
            rows: &borrowed_rows,
            palette: &self.palette,
            theme: self.theme,
            cursor: self.cursor,
            scroll: self.scroll,
            modes: self.modes,
            hyperlinks: &borrowed_links,
        };
        render_into_buffer(buffer, area, &view, options)
    }

    /// Preserve model row order and cell payload for `pane.read`.
    pub fn visible_lines(&self) -> Vec<ExternalReadLine> {
        self.rows
            .iter()
            .enumerate()
            .map(|(model_row, row)| ExternalReadLine {
                text: row_to_text(row),
                model_row,
                soft_wrapped: row_is_soft_wrapped_snapshot(row),
            })
            .collect()
    }

    pub fn visible_hyperlinks(&self) -> Vec<ExternalHyperlinkSnapshot> {
        self.rows
            .iter()
            .enumerate()
            .flat_map(|(row, cells)| {
                cells
                    .cells
                    .iter()
                    .enumerate()
                    .filter_map(move |(column, cell)| {
                        let index = usize::try_from(cell.hyperlink?).ok()?;
                        let (id, uri) = self.hyperlinks.get(index)?;
                        let symbol = if cell.width
                            == herdr_external_terminal_renderer_v1::ExternalCellWidth::Narrow
                            || cell.width
                                == herdr_external_terminal_renderer_v1::ExternalCellWidth::Wide
                        {
                            std::iter::once(cell.base)
                                .chain(cell.zero_width.iter().copied())
                                .collect()
                        } else {
                            String::from(" ")
                        };
                        Some(ExternalHyperlinkSnapshot {
                            row: u16::try_from(row).ok()?,
                            column: u16::try_from(column).ok()?,
                            symbol,
                            id: id.clone(),
                            uri: uri.clone(),
                        })
                    })
            })
            .collect()
    }
}

/// One shared external terminal model and its presentation-only scroll view.
///
/// The model lock protects both Alacritty's `Term` and the VTE processor. The
/// local scroll offset is separate so rendering and scroll requests never
/// mutate parser/model state. The owner can replace this one offset with a
/// per-client map when several direct observers need independent views.
enum ExternalModel {
    /// Used by focused unit tests and by a caller that has already admitted a
    /// trusted model. Provider attachments use `Consumer`, which admits only
    /// identity-checked CTS2 messages.
    Direct(TerminalSession<AlacrittyEngine>),
    Consumer(TerminalStreamConsumer<AlacrittyEngine>),
}

impl ExternalModel {
    fn session(&self) -> &TerminalSession<AlacrittyEngine> {
        match self {
            Self::Direct(session) => session,
            Self::Consumer(consumer) => consumer.session(),
        }
    }

    fn ingest(&mut self, chunk: RawChunk) -> Result<ApplyReport, String> {
        match self {
            Self::Direct(session) => session.ingest(chunk).map_err(|error| error.to_string()),
            Self::Consumer(_) => Err("raw ingestion is owned by the CTS2 consumer".to_owned()),
        }
    }

    fn resize(&mut self, geometry: Geometry) -> Result<ApplyReport, String> {
        match self {
            Self::Direct(session) => session.resize(geometry).map_err(|error| error.to_string()),
            Self::Consumer(_) => {
                Err("provider resize must arrive as an ordered CTS2 message".to_owned())
            }
        }
    }

    fn accept_message(&mut self, message: StreamMessage) -> Result<Option<ConsumerEvent>, String> {
        match self {
            Self::Direct(_) => Err("CTS2 messages require a checkpoint consumer".to_owned()),
            Self::Consumer(consumer) => consumer
                .accept_message(message)
                .map_err(|error| error.to_string()),
        }
    }

    fn checkpoint_bytes(&self) -> Result<Vec<u8>, String> {
        self.session()
            .checkpoint_bytes()
            .map_err(|error| error.to_string())
    }

    fn restore_checkpoint_bytes(
        &mut self,
        bytes: &[u8],
        policy: QueryReplyPolicy,
    ) -> Result<(), String> {
        match self {
            Self::Direct(session) => session
                .restore_checkpoint_bytes(bytes, policy)
                .map_err(|error| error.to_string()),
            Self::Consumer(consumer) => consumer
                .restore_checkpoint_for_reconnect(bytes)
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }
    }
}

struct SessionFence {
    closed: AtomicBool,
    /// The lock is held for the complete admission and model mutation. A
    /// detach takes it before setting `closed`, so an in-flight provider
    /// result cannot commit after the registry removes this session.
    gate: Mutex<()>,
}

impl SessionFence {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            gate: Mutex::new(()),
        }
    }
}

#[derive(Clone)]
struct CachedSnapshot {
    model_revision: u64,
    offset: Option<usize>,
    theme: ExternalTheme,
    snapshot: ExternalTerminalSnapshot,
}

/// A complete engine checkpoint is captured at most once for each committed
/// model revision. Scroll and theme changes derive a bounded viewport from
/// this cache instead of checkpointing the full scrollback on every render.
struct CachedModelCheckpoint {
    model_revision: u64,
    model: TermCheckpointV1,
    processor: vte::ansi::checkpoint::ProcessorCheckpointV1,
}

pub struct ExternalTerminalSession {
    model: RwLock<ExternalModel>,
    theme: RwLock<ExternalTheme>,
    view_offset: Mutex<Option<usize>>,
    cache: Mutex<Option<CachedSnapshot>>,
    model_cache: Mutex<Option<CachedModelCheckpoint>>,
    fence: SessionFence,
}

impl ExternalTerminalSession {
    pub fn new(
        geometry: Geometry,
        policy: QueryReplyPolicy,
        max_replay_bytes: usize,
        max_replay_frames: usize,
        theme: ExternalTheme,
    ) -> SessionResult<Self> {
        let model = TerminalSession::new(geometry, policy, max_replay_bytes, max_replay_frames)?;
        Ok(Self::from_external_model(
            ExternalModel::Direct(model),
            theme,
        ))
    }

    /// Construct a model whose only mutation path is the identity-checked
    /// CTS2 consumer. Provider output must never be reparsed through a second
    /// ANSI path in Herdr.
    pub fn from_stream(
        binding: StreamBinding,
        geometry: Geometry,
        policy: QueryReplyPolicy,
        max_replay_bytes: usize,
        max_replay_frames: usize,
        max_frame_bytes: usize,
        theme: ExternalTheme,
    ) -> Result<Self, ExternalSessionError> {
        let consumer = TerminalStreamConsumer::from_geometry(
            binding,
            geometry,
            policy,
            max_replay_bytes,
            max_replay_frames,
            max_frame_bytes,
        )
        .map_err(|error| ExternalSessionError::Transport(error.to_string()))?;
        Ok(Self::from_external_model(
            ExternalModel::Consumer(consumer),
            theme,
        ))
    }

    /// Construct the provider attachment with the shared bounded replay
    /// limits. The owner should use this entry point so every attachment has
    /// the same persistence and allocation ceilings.
    pub fn from_stream_default(
        binding: StreamBinding,
        geometry: Geometry,
        max_frame_bytes: usize,
        theme: ExternalTheme,
    ) -> Result<Self, ExternalSessionError> {
        Self::from_stream(
            binding,
            geometry,
            QueryReplyPolicy::Quiet,
            16 * 1024 * 1024,
            16_384,
            max_frame_bytes,
            theme,
        )
    }

    pub fn from_model(model: TerminalSession<AlacrittyEngine>, theme: ExternalTheme) -> Self {
        Self::from_external_model(ExternalModel::Direct(model), theme)
    }

    fn from_external_model(model: ExternalModel, theme: ExternalTheme) -> Self {
        Self {
            model: RwLock::new(model),
            theme: RwLock::new(theme),
            view_offset: Mutex::new(None),
            cache: Mutex::new(None),
            model_cache: Mutex::new(None),
            fence: SessionFence::new(),
        }
    }

    fn open(&self) -> Result<std::sync::MutexGuard<'_, ()>, ExternalSessionError> {
        let guard = lock_unpoisoned(&self.fence.gate);
        if self.fence.closed.load(Ordering::Acquire) {
            return Err(ExternalSessionError::Closed);
        }
        Ok(guard)
    }

    fn invalidate_snapshot_cache(&self) {
        *lock_unpoisoned(&self.cache) = None;
    }

    fn invalidate_model_cache(&self) {
        self.invalidate_snapshot_cache();
        *lock_unpoisoned(&self.model_cache) = None;
    }

    pub fn is_closed(&self) -> bool {
        self.fence.closed.load(Ordering::Acquire)
    }

    /// Linearization point for registry detach. Any mutable operation that
    /// already acquired the gate completes before this returns; later stale
    /// handles receive `Closed`.
    pub fn close(&self) {
        let _gate = lock_unpoisoned(&self.fence.gate);
        self.fence.closed.store(true, Ordering::Release);
        self.invalidate_model_cache();
    }

    pub fn binding(&self) -> Option<StreamBinding> {
        let _gate = lock_unpoisoned(&self.fence.gate);
        match &*read_unpoisoned(&self.model) {
            ExternalModel::Direct(_) => None,
            ExternalModel::Consumer(consumer) => Some(consumer.binding().clone()),
        }
    }

    pub fn stream_id_string(&self) -> Option<String> {
        let binding = self.binding()?;
        let b = binding.stream_id;
        Some(format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
            b[14], b[15]
        ))
    }

    /// Return the source cursor and runtime revision represented by the
    /// current consumer model. The revision is the transport revision (the
    /// UI content revision remains the stable doubled value).
    pub fn stream_cursor_revision(&self) -> Option<(coven_terminal::SessionCursor, u64)> {
        let _gate = lock_unpoisoned(&self.fence.gate);
        match &*read_unpoisoned(&self.model) {
            ExternalModel::Direct(_) => None,
            ExternalModel::Consumer(consumer) => Some((consumer.cursor(), consumer.revision())),
        }
    }

    pub fn geometry(&self) -> Geometry {
        let _gate = lock_unpoisoned(&self.fence.gate);
        read_unpoisoned(&self.model).session().geometry()
    }

    pub fn content_revision(&self) -> u64 {
        let _gate = lock_unpoisoned(&self.fence.gate);
        content_revision_of(read_unpoisoned(&self.model).session().revision())
    }

    /// The legacy/direct caller may feed raw bytes. A provider attachment must
    /// call `accept_stream_message`, which keeps source ordering and fences in
    /// the typed transport coordinator.
    pub fn ingest(&self, chunk: RawChunk) -> Result<ApplyReport, ExternalSessionError> {
        let _gate = self.open()?;
        let report = write_unpoisoned(&self.model)
            .ingest(chunk)
            .map_err(ExternalSessionError::Session)?;
        self.invalidate_model_cache();
        Ok(report)
    }

    pub fn accept_stream_message(
        &self,
        message: StreamMessage,
    ) -> Result<Option<ConsumerEvent>, ExternalSessionError> {
        let _gate = self.open()?;
        let mut model = write_unpoisoned(&self.model);
        let before = model.session().revision();
        let result = model
            .accept_message(message)
            .map_err(ExternalSessionError::Transport)?;
        if before != model.session().revision() {
            self.invalidate_model_cache();
        }
        Ok(result)
    }

    pub fn resize(&self, geometry: Geometry) -> Result<ApplyReport, ExternalSessionError> {
        let _gate = self.open()?;
        let report = write_unpoisoned(&self.model)
            .resize(geometry)
            .map_err(ExternalSessionError::Session)?;
        self.invalidate_model_cache();
        Ok(report)
    }

    pub fn resize_cells(
        &self,
        columns: u16,
        rows: u16,
        cell_width: u16,
        cell_height: u16,
    ) -> Result<ApplyReport, ExternalSessionError> {
        self.resize(Geometry::with_cell_size(
            columns,
            rows,
            cell_width,
            cell_height,
        ))
    }

    /// Emit the owner-authorized synchronized-output boundary. Provider
    /// consumers receive this as an ordered CTS2 `SyncFlush` event; they never
    /// derive flushes from their own wall clock.
    pub fn flush_if_needed(&self) -> Result<Option<ApplyReport>, ExternalSessionError> {
        let _gate = self.open()?;
        let mut model = write_unpoisoned(&self.model);
        let result = match &mut *model {
            ExternalModel::Direct(session) => session
                .flush_if_needed()
                .map_err(|error| ExternalSessionError::Session(error.to_string()))?,
            ExternalModel::Consumer(_) => {
                return Err(ExternalSessionError::Transport(
                    "provider flush must arrive as an ordered CTS2 message".to_owned(),
                ));
            }
        };
        if result.is_some() {
            self.invalidate_model_cache();
        }
        Ok(result)
    }

    pub fn needs_flush(&self) -> Result<bool, ExternalSessionError> {
        let _gate = self.open()?;
        Ok(read_unpoisoned(&self.model).session().needs_flush())
    }

    /// Prepare the complete model cache on the provider worker after a batch.
    /// Presentation helpers can then project bounded viewports from one revision.
    pub fn prepare_model_snapshot(&self) -> Result<(), ExternalSessionError> {
        let _gate = self.open()?;
        let model = read_unpoisoned(&self.model);
        let model_revision = model.session().revision();
        let mut cache = lock_unpoisoned(&self.model_cache);
        if cache
            .as_ref()
            .is_some_and(|cached| cached.model_revision == model_revision)
        {
            return Ok(());
        }
        let checkpoint = model
            .session()
            .checkpoint()
            .map_err(|error| ExternalSessionError::Session(error.to_string()))?;
        *cache = Some(CachedModelCheckpoint {
            model_revision,
            model: checkpoint.model,
            processor: checkpoint.processor,
        });
        Ok(())
    }

    /// Capture the model and parser state at the current presentation offset.
    /// A revision/theme/offset keyed cache makes all metadata derived during a
    /// single client frame share one owned projection and avoids repeating a
    /// full persistence checkpoint for every helper call.
    pub fn render_snapshot(&self) -> Result<ExternalTerminalSnapshot, ExternalSessionError> {
        let _gate = self.open()?;
        let model = read_unpoisoned(&self.model);
        let model_revision = model.session().revision();
        let theme = *read_unpoisoned(&self.theme);
        let local_offset = *lock_unpoisoned(&self.view_offset);
        if let Some(cached) = lock_unpoisoned(&self.cache).as_ref() {
            if cached.model_revision == model_revision
                && cached.offset == local_offset
                && cached.theme == theme
            {
                return Ok(cached.snapshot.clone());
            }
        }
        let revision = content_revision_of(model_revision);
        if let Some(cached_model) = lock_unpoisoned(&self.model_cache).as_ref() {
            if cached_model.model_revision == model_revision {
                let snapshot = snapshot_from_checkpoint(
                    &cached_model.model,
                    &cached_model.processor,
                    revision,
                    theme,
                    local_offset,
                )?;
                *lock_unpoisoned(&self.cache) = Some(CachedSnapshot {
                    model_revision,
                    offset: local_offset,
                    theme,
                    snapshot: snapshot.clone(),
                });
                return Ok(snapshot);
            }
        }
        let checkpoint = model
            .session()
            .checkpoint()
            .map_err(|error| ExternalSessionError::Session(error.to_string()))?;
        let mut model_cache = lock_unpoisoned(&self.model_cache);
        *model_cache = Some(CachedModelCheckpoint {
            model_revision,
            model: checkpoint.model,
            processor: checkpoint.processor,
        });
        let cached_model = model_cache
            .as_ref()
            .expect("model checkpoint inserted above");
        let snapshot = snapshot_from_checkpoint(
            &cached_model.model,
            &cached_model.processor,
            revision,
            theme,
            local_offset,
        )?;
        *lock_unpoisoned(&self.cache) = Some(CachedSnapshot {
            model_revision,
            offset: local_offset,
            theme,
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    pub fn render(
        &self,
        buffer: &mut Buffer,
        area: Rect,
        focused: bool,
    ) -> Result<ExternalRenderOutput, ExternalSessionError> {
        let snapshot = self.render_snapshot()?;
        let options = ExternalRenderOptions {
            cursor: ExternalCursorPolicy::ReadOnlyFocused { focused },
        };
        snapshot
            .render_into_buffer(buffer, area, options)
            .map_err(ExternalSessionError::Render)
    }

    /// Move the presentation offset without modifying the terminal model.
    /// Negative values mean toward older history; positive values mean toward
    /// the newest output.
    pub fn scroll(&self, delta: i32) -> Result<ExternalScrollMetrics, ExternalSessionError> {
        let _gate = self.open()?;
        let model = read_unpoisoned(&self.model);
        let checkpoint = model
            .session()
            .checkpoint()
            .map_err(|error| ExternalSessionError::Session(error.to_string()))?;
        let metrics = scroll_metrics(&checkpoint.model)?;
        let mut offset_guard = lock_unpoisoned(&self.view_offset);
        let current = offset_guard
            .unwrap_or(metrics.offset_from_bottom)
            .min(metrics.max_offset_from_bottom);
        let next = if delta.is_negative() {
            current
                .saturating_add(delta.unsigned_abs() as usize)
                .min(metrics.max_offset_from_bottom)
        } else {
            current.saturating_sub(delta as usize)
        };
        *offset_guard = Some(next);
        self.invalidate_snapshot_cache();
        Ok(ExternalScrollMetrics {
            offset_from_bottom: next,
            ..metrics
        })
    }

    /// Set the presentation offset used by a pane/API view. This does not
    /// change the terminal model or its parser state.
    pub fn set_scroll_offset(
        &self,
        offset_from_bottom: usize,
    ) -> Result<ExternalScrollMetrics, ExternalSessionError> {
        let _gate = self.open()?;
        let model = read_unpoisoned(&self.model);
        let checkpoint = model
            .session()
            .checkpoint()
            .map_err(|error| ExternalSessionError::Session(error.to_string()))?;
        let metrics = scroll_metrics(&checkpoint.model)?;
        let offset = offset_from_bottom.min(metrics.max_offset_from_bottom);
        *lock_unpoisoned(&self.view_offset) = Some(offset);
        self.invalidate_snapshot_cache();
        Ok(ExternalScrollMetrics {
            offset_from_bottom: offset,
            ..metrics
        })
    }

    pub fn reset_scroll(&self) {
        let _gate = lock_unpoisoned(&self.fence.gate);
        if self.fence.closed.load(Ordering::Acquire) {
            return;
        }
        *lock_unpoisoned(&self.view_offset) = None;
        self.invalidate_snapshot_cache();
    }

    /// Read the same model used for rendering. `limit` is bounded by the API
    /// caller; this method applies the shared 1000-line safety ceiling too.
    pub fn recent_lines(
        &self,
        limit: usize,
    ) -> Result<Vec<ExternalReadLine>, ExternalSessionError> {
        Ok(self.recent_lines_with_truncation(limit)?.0)
    }

    pub fn recent_lines_with_truncation(
        &self,
        limit: usize,
    ) -> Result<(Vec<ExternalReadLine>, bool), ExternalSessionError> {
        let (lines, truncated, _) = self.recent_lines_with_revision(limit)?;
        Ok((lines, truncated))
    }

    /// Read recent rows and return the stable even content revision used for
    /// that exact checkpoint. The revision is captured while the model read
    /// lock is held so `pane.read` can report the model it actually copied.
    pub fn recent_lines_with_revision(
        &self,
        limit: usize,
    ) -> Result<(Vec<ExternalReadLine>, bool, u64), ExternalSessionError> {
        let _gate = self.open()?;
        let model = read_unpoisoned(&self.model);
        let checkpoint = model
            .session()
            .checkpoint()
            .map_err(|error| ExternalSessionError::Session(error.to_string()))?;
        let revision = content_revision_of(model.session().revision());
        let (range, truncated) = recent_checkpoint_range(&checkpoint.model, limit);
        let rows = &checkpoint.model.active_grid.rows;
        let start = range.start;
        let mut lines = rows[range]
            .iter()
            .enumerate()
            .map(|(index, row)| ExternalReadLine {
                text: row_to_text_from_checkpoint(row),
                model_row: start + index,
                soft_wrapped: row_is_soft_wrapped(row),
            })
            .collect::<Vec<_>>();
        // Match the native formatter: trailing blank rows do not become
        // phantom output lines. The range itself is selected from the active
        // model screen, so primary scrollback is never reported as recent
        // output and alternate screen reads cover the full active screen.
        while lines.last().is_some_and(|line| line.text.is_empty()) {
            lines.pop();
        }
        Ok((lines, truncated, revision))
    }

    pub fn selection_to_string(&self) -> Option<String> {
        let _gate = self.open().ok()?;
        read_unpoisoned(&self.model)
            .session()
            .engine()
            .term()
            .selection_to_string()
    }

    /// Extract a coordinate range from the same coherent snapshot used for
    /// rendering. Coordinates are clipped to model bounds and spacer cells are
    /// omitted, so wide glyphs cannot produce duplicate trailing characters.
    pub fn selection_text(
        &self,
        anchor: (u32, u16),
        cursor: (u32, u16),
        expected_revision: Option<u64>,
    ) -> Result<String, ExternalSessionError> {
        use alacritty_terminal::{
            grid::Dimensions,
            index::{Column, Line, Point},
        };

        let _gate = self.open()?;
        let model = read_unpoisoned(&self.model);
        let revision = content_revision_of(model.session().revision());
        if expected_revision.is_some_and(|expected| expected != revision) {
            return Err(ExternalSessionError::RevisionExhausted);
        }
        let term = model.session().engine().term();
        let history = term.grid().history_size();
        let total_rows = history + term.screen_lines();
        let columns = term.columns();
        let mut first = anchor;
        let mut last = cursor;
        if first > last {
            std::mem::swap(&mut first, &mut last);
        }
        let point = |(row, column): (u32, u16)| -> Result<Point, ExternalSessionError> {
            let row = usize::try_from(row).map_err(|_| {
                ExternalSessionError::Model("selection row is outside retained history".into())
            })?;
            if row >= total_rows || columns == 0 {
                return Err(ExternalSessionError::Model(
                    "selection row is outside retained history".into(),
                ));
            }
            let line = i32::try_from(row as i64 - history as i64).map_err(|_| {
                ExternalSessionError::Model("selection row is outside retained history".into())
            })?;
            Ok(Point::new(
                Line(line),
                Column(usize::from(column).min(columns - 1)),
            ))
        };
        // Read directly from the authoritative terminal model. Its selection
        // formatter handles history, wide spacers, combining characters, tabs,
        // blank cells and soft wraps without copying a full checkpoint or
        // clipping absolute coordinates to the current presentation viewport.
        Ok(term.bounds_to_string(point(first)?, point(last)?))
    }

    pub fn visible_hyperlinks(
        &self,
    ) -> Result<Vec<ExternalHyperlinkSnapshot>, ExternalSessionError> {
        Ok(self.render_snapshot()?.visible_hyperlinks())
    }

    pub fn checkpoint_bytes(&self) -> Result<Vec<u8>, ExternalSessionError> {
        let _gate = self.open()?;
        read_unpoisoned(&self.model)
            .checkpoint_bytes()
            .map_err(|error| ExternalSessionError::Session(error.to_string()))
    }

    /// Export the identity-bound persistence blob used to resume a typed
    /// provider attachment. Direct test models return `None`; only a CTS2
    /// stream has an authenticated binding to embed in the envelope.
    pub fn checkpoint_blob(&self) -> Result<Option<Vec<u8>>, ExternalSessionError> {
        let _gate = self.open()?;
        let model = read_unpoisoned(&self.model);
        let ExternalModel::Consumer(consumer) = &*model else {
            return Ok(None);
        };
        if !matches!(
            consumer.state(),
            coven_terminal_checkpoint_transport::ConsumerState::Live
                | coven_terminal_checkpoint_transport::ConsumerState::Closed
        ) {
            return Ok(None);
        }
        let checkpoint_bytes = consumer
            .session()
            .checkpoint_bytes()
            .map_err(|error| ExternalSessionError::Session(error.to_string()))?;
        let bound = BoundCheckpoint::new(
            consumer.binding().clone(),
            consumer.cursor(),
            consumer.revision(),
            checkpoint_bytes,
        )
        .map_err(|error| ExternalSessionError::Transport(error.to_string()))?;
        bound
            .encode()
            .map(Some)
            .map_err(|error| ExternalSessionError::Transport(error.to_string()))
    }

    /// Restore is transactional inside `TerminalSession` or the transport
    /// consumer: decode and validate a replacement model before assignment.
    pub fn restore_checkpoint_bytes(
        &self,
        bytes: &[u8],
        policy: QueryReplyPolicy,
    ) -> Result<(), ExternalSessionError> {
        let _gate = self.open()?;
        write_unpoisoned(&self.model)
            .restore_checkpoint_bytes(bytes, policy)
            .map_err(|error| ExternalSessionError::Session(error.to_string()))?;
        self.invalidate_model_cache();
        Ok(())
    }

    pub fn set_theme(&self, theme: ExternalTheme) {
        let _gate = lock_unpoisoned(&self.fence.gate);
        if self.fence.closed.load(Ordering::Acquire) {
            return;
        }
        *write_unpoisoned(&self.theme) = theme;
        self.invalidate_snapshot_cache();
    }
}

fn content_revision_of(revision: u64) -> u64 {
    revision.checked_mul(2).unwrap_or(u64::MAX - 1)
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_unpoisoned<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn snapshot_from_checkpoint(
    model: &TermCheckpointV1,
    processor: &vte::ansi::checkpoint::ProcessorCheckpointV1,
    revision: u64,
    theme: ExternalTheme,
    requested_offset: Option<usize>,
) -> Result<ExternalTerminalSnapshot, ExternalSessionError> {
    model
        .validate()
        .map_err(|error| ExternalSessionError::Model(error.to_string()))?;
    let columns = u16::try_from(model.dimensions.columns)
        .map_err(|_| ExternalSessionError::Model("columns exceed u16".into()))?;
    let screen_lines = u16::try_from(model.dimensions.screen_lines)
        .map_err(|_| ExternalSessionError::Model("screen lines exceed u16".into()))?;
    let grid = &model.active_grid;
    let visible_lines = usize::from(screen_lines);
    let history_rows = grid.rows.len().saturating_sub(visible_lines);
    let model_offset = usize::try_from(grid.display_offset)
        .unwrap_or(history_rows)
        .min(history_rows);
    let offset = requested_offset.unwrap_or(model_offset).min(history_rows);
    let start = history_rows.saturating_sub(offset);
    let end = start.saturating_add(visible_lines).min(grid.rows.len());

    let mut links = Vec::<(String, String)>::new();
    let mut link_indices = HashMap::<(String, String), u32>::new();
    let rows = grid.rows[start..end]
        .iter()
        .map(|row| row_from_checkpoint(row, &mut links, &mut link_indices))
        .collect::<Result<Vec<_>, _>>()?;

    let palette = palette_from_checkpoint(&model.colors)?;
    let cursor = cursor_from_checkpoint(model, grid, columns, screen_lines)?;
    let mode_bits = model.mode_bits;
    let modes = ExternalModes {
        alternate_screen: mode_bits & ALT_SCREEN != 0,
        line_wrap: mode_bits & LINE_WRAP != 0,
        origin: mode_bits & ORIGIN != 0,
        insert: mode_bits & INSERT != 0,
        mouse: if mode_bits & MOUSE_MOTION != 0 {
            herdr_external_terminal_renderer_v1::ExternalMouseMode::AllMotion
        } else if mode_bits & MOUSE_DRAG != 0 {
            herdr_external_terminal_renderer_v1::ExternalMouseMode::CellMotion
        } else if mode_bits & MOUSE_REPORT_CLICK != 0 {
            herdr_external_terminal_renderer_v1::ExternalMouseMode::Button
        } else {
            herdr_external_terminal_renderer_v1::ExternalMouseMode::None
        },
        sgr_pixel_mouse: mode_bits & SGR_MOUSE != 0,
        focus_reporting: mode_bits & FOCUS_IN_OUT != 0,
        bracketed_paste: mode_bits & BRACKETED_PASTE != 0,
        synchronized_output: processor.sync_timeout.pending
            || !processor.synchronized_bytes.is_empty(),
    };
    Ok(ExternalTerminalSnapshot {
        content_revision: revision,
        columns,
        screen_lines,
        rows,
        palette,
        theme,
        cursor,
        scroll: ExternalScrollMetrics {
            offset_from_bottom: offset,
            max_offset_from_bottom: history_rows,
            viewport_rows: visible_lines,
            history_rows,
        },
        modes,
        hyperlinks: links,
    })
}

fn scroll_metrics(model: &TermCheckpointV1) -> Result<ExternalScrollMetrics, ExternalSessionError> {
    let screen = usize::try_from(model.dimensions.screen_lines)
        .map_err(|_| ExternalSessionError::Scroll("screen lines overflow".into()))?;
    let grid = &model.active_grid;
    let history = grid.rows.len().saturating_sub(screen);
    let offset = usize::try_from(grid.display_offset)
        .unwrap_or(history)
        .min(history);
    Ok(ExternalScrollMetrics {
        offset_from_bottom: offset,
        max_offset_from_bottom: history,
        viewport_rows: screen,
        history_rows: history,
    })
}

fn row_from_checkpoint(
    row: &RowCheckpointV1,
    links: &mut Vec<(String, String)>,
    link_indices: &mut HashMap<(String, String), u32>,
) -> Result<ExternalRowSnapshot, ExternalSessionError> {
    let cells = row
        .cells
        .iter()
        .map(|cell| cell_from_checkpoint(cell, links, link_indices))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ExternalRowSnapshot { cells })
}

fn cell_from_checkpoint(
    cell: &CellCheckpointV1,
    links: &mut Vec<(String, String)>,
    link_indices: &mut HashMap<(String, String), u32>,
) -> Result<ExternalCellSnapshot, ExternalSessionError> {
    let hyperlink = cell
        .extra
        .as_ref()
        .and_then(|extra| extra.hyperlink.as_ref())
        .map(|link| {
            let key = (link.id.clone(), link.uri.clone());
            if let Some(index) = link_indices.get(&key) {
                return *index;
            }
            let index = u32::try_from(links.len()).unwrap_or(u32::MAX);
            if index != u32::MAX {
                links.push(key.clone());
                link_indices.insert(key, index);
            }
            index
        });
    if hyperlink == Some(u32::MAX) {
        return Err(ExternalSessionError::Model("too many hyperlinks".into()));
    }
    let flags = ExternalCellFlags::from_bits_retain(cell.flags);
    let width = if flags.contains(ExternalCellFlags::WIDE_CHAR_SPACER) {
        herdr_external_terminal_renderer_v1::ExternalCellWidth::WideSpacer
    } else if flags.contains(ExternalCellFlags::LEADING_WIDE_CHAR_SPACER) {
        herdr_external_terminal_renderer_v1::ExternalCellWidth::LeadingWideSpacer
    } else if flags.contains(ExternalCellFlags::WIDE_CHAR) {
        herdr_external_terminal_renderer_v1::ExternalCellWidth::Wide
    } else {
        herdr_external_terminal_renderer_v1::ExternalCellWidth::Narrow
    };
    Ok(ExternalCellSnapshot {
        base: cell.c,
        zero_width: cell
            .extra
            .as_ref()
            .map(|extra| extra.zero_width.clone())
            .unwrap_or_default(),
        fg: color_from_checkpoint(cell.fg),
        bg: color_from_checkpoint(cell.bg),
        underline_color: cell
            .extra
            .as_ref()
            .and_then(|extra| extra.underline_color.map(color_from_checkpoint)),
        flags,
        width,
        hyperlink,
    })
}

fn palette_from_checkpoint(
    colors: &[Option<alacritty_terminal::checkpoint::RgbV1>],
) -> Result<ExternalPalette, ExternalSessionError> {
    if colors.len() != 269 {
        return Err(ExternalSessionError::Model(
            "palette length is not 269".into(),
        ));
    }
    let mut palette = ExternalPalette::empty();
    for (index, color) in colors.iter().copied().enumerate() {
        if let Some(color) = color {
            let inserted = palette.set_named(index, ExternalRgb::new(color.r, color.g, color.b));
            if !inserted {
                return Err(ExternalSessionError::Model(
                    "palette index out of range".into(),
                ));
            }
        }
    }
    Ok(palette)
}

fn color_from_checkpoint(color: ColorCheckpointV1) -> ExternalColor {
    match color {
        ColorCheckpointV1::Named(color) => ExternalColor::Named(named_color_index(color)),
        ColorCheckpointV1::Spec(color) => {
            ExternalColor::Spec(ExternalRgb::new(color.r, color.g, color.b))
        }
        ColorCheckpointV1::Indexed(color) => ExternalColor::Indexed(color),
    }
}

fn named_color_index(color: NamedColorV1) -> u16 {
    match color {
        NamedColorV1::Black => 0,
        NamedColorV1::Red => 1,
        NamedColorV1::Green => 2,
        NamedColorV1::Yellow => 3,
        NamedColorV1::Blue => 4,
        NamedColorV1::Magenta => 5,
        NamedColorV1::Cyan => 6,
        NamedColorV1::White => 7,
        NamedColorV1::BrightBlack => 8,
        NamedColorV1::BrightRed => 9,
        NamedColorV1::BrightGreen => 10,
        NamedColorV1::BrightYellow => 11,
        NamedColorV1::BrightBlue => 12,
        NamedColorV1::BrightMagenta => 13,
        NamedColorV1::BrightCyan => 14,
        NamedColorV1::BrightWhite => 15,
        NamedColorV1::Foreground => 256,
        NamedColorV1::Background => 257,
        NamedColorV1::Cursor => 258,
        NamedColorV1::DimBlack => 259,
        NamedColorV1::DimRed => 260,
        NamedColorV1::DimGreen => 261,
        NamedColorV1::DimYellow => 262,
        NamedColorV1::DimBlue => 263,
        NamedColorV1::DimMagenta => 264,
        NamedColorV1::DimCyan => 265,
        NamedColorV1::DimWhite => 266,
        NamedColorV1::BrightForeground => 267,
        NamedColorV1::DimForeground => 268,
    }
}

fn cursor_from_checkpoint(
    model: &TermCheckpointV1,
    grid: &alacritty_terminal::checkpoint::GridCheckpointV1,
    columns: u16,
    screen_lines: u16,
) -> Result<Option<ExternalCursor>, ExternalSessionError> {
    let x = u16::try_from(grid.cursor.point.column)
        .map_err(|_| ExternalSessionError::Model("cursor column overflow".into()))?;
    let y = u16::try_from(grid.cursor.point.line)
        .map_err(|_| ExternalSessionError::Model("cursor row overflow".into()))?;
    if x >= columns || y >= screen_lines {
        return Err(ExternalSessionError::Model(
            "cursor outside viewport".into(),
        ));
    }
    let style = model
        .cursor_style
        .unwrap_or(model.config.default_cursor_style);
    let shape = match style.shape {
        CursorShapeV1::Block | CursorShapeV1::HollowBlock => ExternalCursorShape::Block {
            blinking: style.blinking,
        },
        CursorShapeV1::Underline => ExternalCursorShape::Underline {
            blinking: style.blinking,
        },
        CursorShapeV1::Beam => ExternalCursorShape::Bar {
            blinking: style.blinking,
        },
        CursorShapeV1::Hidden => ExternalCursorShape::Hidden,
    };
    Ok(Some(ExternalCursor {
        x,
        y,
        visible: model.mode_bits & SHOW_CURSOR != 0,
        shape,
    }))
}

/// Select the same bounded rendered-row range as the native `recent` read.
/// Primary-screen reads are restricted to the current viewport; alternate
/// screen reads use the complete active screen because it has no scrollback.
/// The cursor extends the primary end row so a prompt below the last glyph is
/// considered, while trailing blank rows are removed by the caller's text
/// formatter just as they are by the native implementation.
fn recent_checkpoint_range(
    model: &TermCheckpointV1,
    limit: usize,
) -> (std::ops::Range<usize>, bool) {
    let rows = &model.active_grid.rows;
    let total_rows = rows.len();
    let cap = limit.min(1000);
    if total_rows == 0 || cap == 0 {
        return (0..0, total_rows > cap);
    }

    let physical_end = total_rows - 1;
    let alternate_screen = model.mode_bits & ALT_SCREEN != 0;
    let (viewport_start, end) = if alternate_screen {
        // The alternate grid is validated to contain exactly the active
        // screen, but using the actual row count keeps this helper defensive
        // if it is called with a future checkpoint version.
        (
            total_rows.saturating_sub(usize::try_from(model.dimensions.screen_lines).unwrap_or(0)),
            physical_end,
        )
    } else {
        let screen_lines = usize::try_from(model.dimensions.screen_lines).unwrap_or(0);
        let viewport_start = total_rows.saturating_sub(screen_lines);
        let cursor_row = viewport_start
            .saturating_add(usize::try_from(model.active_grid.cursor.point.line).unwrap_or(0))
            .min(physical_end);
        let last_content_row = (viewport_start..total_rows)
            .rev()
            .find(|&row| !row_to_text_from_checkpoint(&rows[row]).trim().is_empty());
        (
            viewport_start,
            last_content_row.map_or(physical_end, |row| row.max(cursor_row)),
        )
    };
    let start = end
        .saturating_add(1)
        .saturating_sub(cap)
        .max(viewport_start);
    (start..end.saturating_add(1), total_rows > cap)
}

fn row_to_text(row: &ExternalRowSnapshot) -> String {
    row.cells
        .iter()
        .filter(|cell| {
            !matches!(
                cell.width,
                herdr_external_terminal_renderer_v1::ExternalCellWidth::WideSpacer
                    | herdr_external_terminal_renderer_v1::ExternalCellWidth::LeadingWideSpacer
            )
        })
        .flat_map(|cell| std::iter::once(cell.base).chain(cell.zero_width.iter().copied()))
        .collect::<String>()
        .trim_end()
        .to_owned()
}

fn row_is_soft_wrapped_snapshot(row: &ExternalRowSnapshot) -> bool {
    row.cells
        .last()
        .is_some_and(|cell| cell.flags.contains(ExternalCellFlags::WRAPLINE))
}

fn row_to_text_from_checkpoint(row: &RowCheckpointV1) -> String {
    row.cells
        .iter()
        .filter(|cell| cell.flags & (1 << 6 | 1 << 10) == 0)
        .flat_map(|cell| {
            std::iter::once(cell.c).chain(
                cell.extra
                    .as_ref()
                    .into_iter()
                    .flat_map(|extra| extra.zero_width.iter().copied()),
            )
        })
        .collect::<String>()
        .trim_end()
        .to_owned()
}

fn row_is_soft_wrapped(row: &RowCheckpointV1) -> bool {
    row.cells
        .last()
        .is_some_and(|cell| cell.flags & (1 << 4) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_uses_row_order_and_retained_history_independent_of_scroll() {
        let session = ExternalTerminalSession::new(
            Geometry::new(8, 3),
            QueryReplyPolicy::Quiet,
            64 * 1024,
            256,
            ExternalTheme::default(),
        )
        .unwrap();
        session
            .ingest(RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"abcdefgh\r\nijklmnop\r\nqrstuvwx\r\nyz012345\r\n6789abcd".to_vec(),
            ))
            .unwrap();
        let revision = session.content_revision();
        let expected = "fgh\nijklmnop\nqr";
        assert_eq!(
            session
                .selection_text((0, 5), (2, 1), Some(revision))
                .unwrap(),
            expected
        );
        assert_eq!(
            session
                .selection_text((2, 1), (0, 5), Some(revision))
                .unwrap(),
            expected
        );
        session.set_scroll_offset(2).unwrap();
        assert_eq!(
            session
                .selection_text((0, 5), (2, 1), Some(revision))
                .unwrap(),
            expected
        );
        assert!(session.selection_text((100, 0), (100, 1), None).is_err());
    }

    #[test]
    fn closed_sessions_reject_stale_mutation_handles() {
        let session = session();
        let geometry = session.geometry();
        session.close();
        assert!(session.is_closed());
        assert!(matches!(
            session.ingest(RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"late".to_vec()
            )),
            Err(ExternalSessionError::Closed)
        ));
        assert!(matches!(
            session.resize(Geometry::new(99, 33)),
            Err(ExternalSessionError::Closed)
        ));
        assert!(matches!(
            session.restore_checkpoint_bytes(b"invalid", QueryReplyPolicy::Quiet),
            Err(ExternalSessionError::Closed)
        ));
        assert_eq!(session.geometry(), geometry);
    }

    fn session() -> ExternalTerminalSession {
        ExternalTerminalSession::new(
            Geometry::new(20, 4),
            QueryReplyPolicy::Quiet,
            1024 * 1024,
            256,
            ExternalTheme::default(),
        )
        .expect("session")
    }

    #[test]
    fn render_and_read_share_model_revision_and_cells() {
        let session = session();
        session
            .ingest(RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"hello\x1b[31m red".to_vec(),
            ))
            .expect("ingest");
        let snapshot = session.render_snapshot().expect("snapshot");
        let (lines, truncated, read_revision) =
            session.recent_lines_with_revision(4).expect("read");
        assert!(!truncated);
        assert_eq!(snapshot.content_revision, read_revision);
        assert!(snapshot.content_revision > 0);
        assert_eq!(snapshot.content_revision % 2, 0);
        assert!(lines.iter().any(|line| line.text.contains("hello")));
        assert!(!snapshot.modes.alternate_screen);
    }

    #[test]
    fn recent_primary_reads_current_viewport_instead_of_scrollback() {
        let session = session();
        session
            .ingest(RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"one\r\ntwo\r\nthree\r\nfour\r\nfive".to_vec(),
            ))
            .expect("ingest");
        let (lines, truncated, _) = session
            .recent_lines_with_revision(1000)
            .expect("recent read");
        assert!(!truncated);
        assert!(
            lines.len() <= 4,
            "recent primary read must stay in viewport"
        );
        assert!(!lines.iter().any(|line| line.text.contains("one")));
        assert!(lines.iter().any(|line| line.text.contains("five")));
    }

    #[test]
    fn recent_alternate_reads_the_full_active_screen() {
        let session = session();
        session
            .ingest(RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"\x1b[?1049hfirst\r\nsecond\r\nthird\r\nfourth".to_vec(),
            ))
            .expect("ingest");
        let (lines, truncated, _) = session
            .recent_lines_with_revision(1000)
            .expect("recent alternate read");
        assert!(!truncated);
        assert!(lines.iter().any(|line| line.text.contains("first")));
        assert!(lines.iter().any(|line| line.text.contains("fourth")));
    }

    #[test]
    fn scroll_changes_only_presentation_offset() {
        let session = session();
        session
            .ingest(RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"one\r\ntwo\r\nthree\r\nfour\r\nfive".to_vec(),
            ))
            .expect("ingest");
        let before = session.content_revision();
        let metrics = session.scroll(-3).expect("scroll");
        assert!(metrics.offset_from_bottom > 0);
        assert_eq!(session.content_revision(), before);
    }

    #[test]
    fn invalid_restore_leaves_model_usable() {
        let session = session();
        session
            .ingest(RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"stable".to_vec(),
            ))
            .expect("ingest");
        assert!(session
            .restore_checkpoint_bytes(b"{}", QueryReplyPolicy::Quiet)
            .is_err());
        assert!(session
            .recent_lines(4)
            .expect("read after failed restore")
            .iter()
            .any(|line| line.text.contains("stable")));
    }
}
