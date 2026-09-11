//! Direct ratatui rendering for a checkpoint-backed external terminal.
//!
//! The view types in this module are the zero-loss boundary between the
//! checkpoint fork and Herdr. A fork adapter should copy the fields exposed by
//! `Term::renderable_content()` (or an equivalent explicit snapshot) into
//! these borrowed types while holding the model read lock. The adapter owns
//! parser/model state; this renderer only consumes the stable view.
//!
//! In particular, this file must never grow an ANSI replay path. Replaying an
//! escaped transcript would lose parser continuation, alternate-screen state,
//! scrollback, wide cells, combining characters, and mode metadata.

use std::{
    fmt,
    num::NonZeroU16,
    ops::{BitOr, BitOrAssign},
};

use ratatui::{
    buffer::{Buffer, CellDiffOption},
    layout::{Position, Rect},
    style::{Color, Modifier, Style},
};

/// Number of palette entries in `alacritty_terminal::term::color::Colors`.
pub const EXTERNAL_PALETTE_LEN: usize = 269;

/// Herdr's stable wire protocol reserves the high four modifier bits for the
/// underline variant. Keeping that encoding in the projection lets a later
/// `FrameData` adapter preserve double, curly, dotted, and dashed underlines.
pub const UNDERLINE_STYLE_SHIFT: u16 = 12;
pub const UNDERLINE_STYLE_MASK: u16 = 0xF000;

/// A three-component color copied from the terminal model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ExternalRgb {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl ExternalRgb {
    pub const fn new(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b }
    }
}

/// The 269-entry palette owned by one external terminal model.
///
/// `None` means that the model has no explicit override for that named entry.
/// The renderer falls back to the supplied [`ExternalTheme`] for foreground,
/// background, cursor, bright-foreground, and dim-foreground entries; ordinary
/// named entries remain indexed when there is no override.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalPalette {
    named: [Option<ExternalRgb>; EXTERNAL_PALETTE_LEN],
}

impl Default for ExternalPalette {
    fn default() -> Self {
        Self {
            named: [None; EXTERNAL_PALETTE_LEN],
        }
    }
}

impl ExternalPalette {
    pub const fn empty() -> Self {
        Self {
            named: [None; EXTERNAL_PALETTE_LEN],
        }
    }

    /// Set one named palette entry. Returns `false` for an out-of-range index.
    pub fn set_named(&mut self, index: usize, color: ExternalRgb) -> bool {
        let Some(slot) = self.named.get_mut(index) else {
            return false;
        };
        *slot = Some(color);
        true
    }

    pub const fn named(&self, index: usize) -> Option<ExternalRgb> {
        if index < EXTERNAL_PALETTE_LEN {
            self.named[index]
        } else {
            None
        }
    }
}

/// Host/theme defaults used when an external model uses a named foreground
/// or background without an explicit color override.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalTheme {
    pub foreground: Color,
    pub background: Color,
    pub cursor: Color,
}

impl Default for ExternalTheme {
    fn default() -> Self {
        Self {
            foreground: Color::Reset,
            background: Color::Reset,
            cursor: Color::Reset,
        }
    }
}

/// A color reference as retained by the terminal model.
///
/// `Named` uses the exact Alacritty named-color discriminant, including the
/// special entries 256 through 268. It is intentionally `u16` so this module
/// does not depend on a particular fork's private `NamedColor` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExternalColor {
    Named(u16),
    Indexed(u8),
    Spec(ExternalRgb),
    DefaultForeground,
    DefaultBackground,
    Reset,
}

/// The cell flags from the pinned Alacritty model.
///
/// The numeric values match `alacritty_terminal::term::cell::Flags`, which
/// allows a fork adapter to copy its public bit representation without a
/// lossy translation. Unknown future bits are retained by the projection and
/// ignored by this renderer until a corresponding visual mapping is added.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ExternalCellFlags(u16);

impl ExternalCellFlags {
    pub const INVERSE: Self = Self(1 << 0);
    pub const BOLD: Self = Self(1 << 1);
    pub const ITALIC: Self = Self(1 << 2);
    pub const UNDERLINE: Self = Self(1 << 3);
    pub const WRAPLINE: Self = Self(1 << 4);
    pub const WIDE_CHAR: Self = Self(1 << 5);
    pub const WIDE_CHAR_SPACER: Self = Self(1 << 6);
    pub const DIM: Self = Self(1 << 7);
    pub const HIDDEN: Self = Self(1 << 8);
    pub const STRIKEOUT: Self = Self(1 << 9);
    pub const LEADING_WIDE_CHAR_SPACER: Self = Self(1 << 10);
    pub const DOUBLE_UNDERLINE: Self = Self(1 << 11);
    pub const UNDERCURL: Self = Self(1 << 12);
    pub const DOTTED_UNDERLINE: Self = Self(1 << 13);
    pub const DASHED_UNDERLINE: Self = Self(1 << 14);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn from_bits_retain(bits: u16) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }
}

impl BitOr for ExternalCellFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for ExternalCellFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Explicit terminal width for one cell in a canonical row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExternalCellWidth {
    /// A cell whose base character is visible in its own column.
    Narrow,
    /// The leading cell of a two-column glyph.
    Wide,
    /// The trailing spacer covered by a wide glyph.
    WideSpacer,
    /// A spacer at the beginning of a row for a wrapped wide glyph.
    LeadingWideSpacer,
}

impl ExternalCellWidth {
    const fn is_spacer(self) -> bool {
        matches!(self, Self::WideSpacer | Self::LeadingWideSpacer)
    }
}

/// Underline variants represented by the pinned VTE/Alacritty model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExternalUnderline {
    Single,
    Double,
    Curly,
    Dotted,
    Dashed,
}

impl ExternalUnderline {
    const fn wire_code(self) -> u8 {
        match self {
            Self::Single => 1,
            Self::Double => 2,
            Self::Curly => 3,
            Self::Dotted => 4,
            Self::Dashed => 5,
        }
    }
}

/// One complete visual cell borrowed from the terminal model.
///
/// `base` plus `zero_width` is the full grapheme payload retained by
/// Alacritty's `Cell`; the renderer never rebuilds it from normalized text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalCellView<'a> {
    pub base: char,
    pub zero_width: &'a [char],
    pub fg: ExternalColor,
    pub bg: ExternalColor,
    pub underline_color: Option<ExternalColor>,
    pub flags: ExternalCellFlags,
    pub width: ExternalCellWidth,
    /// Index into [`ExternalTerminalView::hyperlinks`].
    pub hyperlink: Option<u32>,
}

impl<'a> ExternalCellView<'a> {
    /// A plain model cell with no styles or hyperlink.
    pub const fn plain(base: char) -> Self {
        Self {
            base,
            zero_width: &[],
            fg: ExternalColor::DefaultForeground,
            bg: ExternalColor::DefaultBackground,
            underline_color: None,
            flags: ExternalCellFlags::empty(),
            width: ExternalCellWidth::Narrow,
            hyperlink: None,
        }
    }

    /// Return the visual underline variant encoded by this cell's flags.
    pub const fn underline(self) -> Option<ExternalUnderline> {
        if self.flags.contains(ExternalCellFlags::DASHED_UNDERLINE) {
            Some(ExternalUnderline::Dashed)
        } else if self.flags.contains(ExternalCellFlags::DOTTED_UNDERLINE) {
            Some(ExternalUnderline::Dotted)
        } else if self.flags.contains(ExternalCellFlags::UNDERCURL) {
            Some(ExternalUnderline::Curly)
        } else if self.flags.contains(ExternalCellFlags::DOUBLE_UNDERLINE) {
            Some(ExternalUnderline::Double)
        } else if self.flags.contains(ExternalCellFlags::UNDERLINE) {
            Some(ExternalUnderline::Single)
        } else {
            None
        }
    }
}

/// A row in canonical top-to-bottom viewport order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalRowView<'a> {
    pub cells: &'a [ExternalCellView<'a>],
}

/// One OSC 8 hyperlink retained as structured metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalHyperlink<'a> {
    pub id: &'a str,
    pub uri: &'a str,
}

/// Cursor shape retained by the model. The shape is returned as metadata; a
/// ratatui `Buffer` has no cursor primitive to mutate directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExternalCursorShape {
    Block { blinking: bool },
    Underline { blinking: bool },
    Bar { blinking: bool },
    Hidden,
}

/// Cursor position in terminal viewport coordinates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalCursor {
    pub x: u16,
    pub y: u16,
    pub visible: bool,
    pub shape: ExternalCursorShape,
}

/// Mouse reporting mode retained from the terminal mode bits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ExternalMouseMode {
    #[default]
    None,
    Button,
    CellMotion,
    AllMotion,
}

/// Presentation-relevant terminal mode metadata.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExternalModes {
    pub alternate_screen: bool,
    pub line_wrap: bool,
    pub origin: bool,
    pub insert: bool,
    pub mouse: ExternalMouseMode,
    pub sgr_pixel_mouse: bool,
    pub focus_reporting: bool,
    pub bracketed_paste: bool,
    pub synchronized_output: bool,
}

/// Scroll state copied from the model's display offset and history limits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExternalScrollMetrics {
    /// Current display offset from the newest visible line.
    pub offset_from_bottom: usize,
    /// Maximum offset accepted by the model.
    pub max_offset_from_bottom: usize,
    /// Number of visible model rows.
    pub viewport_rows: usize,
    /// Number of retained history rows available before the viewport.
    pub history_rows: usize,
}

impl ExternalScrollMetrics {
    pub const fn at_bottom(self) -> bool {
        self.offset_from_bottom == 0
    }
}

/// Stable, borrowed model snapshot consumed by the renderer.
///
/// `rows` must already be the viewport selected by the model's display offset,
/// in top-to-bottom order. A local client scroll offset belongs outside this
/// type and must be resolved by the owner into another read snapshot; it must
/// never mutate a shared model from a render path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalTerminalView<'a> {
    /// Stable even revision from the model owner. Odd revisions are in-flight.
    pub content_revision: u64,
    pub columns: u16,
    pub screen_lines: u16,
    pub rows: &'a [ExternalRowView<'a>],
    pub palette: &'a ExternalPalette,
    pub theme: ExternalTheme,
    pub cursor: Option<ExternalCursor>,
    pub scroll: ExternalScrollMetrics,
    pub modes: ExternalModes,
    pub hyperlinks: &'a [ExternalHyperlink<'a>],
}

/// Policy for exposing an external model cursor to a host/client frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExternalCursorPolicy {
    /// Do not expose a host cursor for this read-only attachment.
    #[default]
    Hidden,
    /// Expose it only for a focused view at the model's bottom offset.
    ReadOnlyFocused { focused: bool },
}

/// Options that affect presentation only. No option mutates the model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExternalRenderOptions {
    pub cursor: ExternalCursorPolicy,
}

/// Cursor metadata returned alongside the ratatui buffer.
///
/// Coordinates are local to the `area` passed to [`render_into_buffer`], just
/// like Herdr's native `CursorState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalRenderedCursor {
    pub x: u16,
    pub y: u16,
    pub visible: bool,
    pub shape: ExternalCursorShape,
}

/// One visible hyperlink cell returned alongside the ratatui buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRenderedHyperlink {
    /// Absolute buffer coordinates, matching Herdr's existing hyperlink list.
    pub position: Position,
    pub symbol: String,
    pub id: String,
    pub uri: String,
}

/// Counts useful for tests and render profiling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExternalRenderStats {
    pub cleared_cells: usize,
    pub visible_cells: usize,
    pub spacer_cells: usize,
    pub wide_cells: usize,
    pub combining_cells: usize,
}

/// Structured result that accompanies a rendered buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRenderOutput {
    pub cursor: Option<ExternalRenderedCursor>,
    pub hyperlinks: Vec<ExternalRenderedHyperlink>,
    pub content_revision: u64,
    pub scroll: ExternalScrollMetrics,
    pub modes: ExternalModes,
    pub stats: ExternalRenderStats,
}

/// Error returned before a malformed or unstable model view can touch the
/// destination buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalRenderError {
    UnstableRevision(u64),
    EmptyDimensions,
    RowCount {
        expected: usize,
        actual: usize,
    },
    CellCount {
        row: usize,
        expected: usize,
        actual: usize,
    },
    InvalidScrollMetrics,
    CursorOutsideModel,
    HyperlinkIndex {
        row: usize,
        column: usize,
        index: u32,
    },
    HyperlinkTooLong {
        index: usize,
    },
    InvalidNamedColor(u16),
    BufferAreaOutside {
        buffer: Rect,
        requested: Rect,
    },
    InconsistentWidthFlags {
        row: usize,
        column: usize,
    },
}

impl fmt::Display for ExternalRenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnstableRevision(revision) => {
                write!(
                    formatter,
                    "external terminal revision {revision} is in flight"
                )
            }
            Self::EmptyDimensions => formatter.write_str("external terminal has empty dimensions"),
            Self::RowCount { expected, actual } => {
                write!(
                    formatter,
                    "external terminal row count {actual} != {expected}"
                )
            }
            Self::CellCount {
                row,
                expected,
                actual,
            } => {
                write!(
                    formatter,
                    "external terminal row {row} cell count {actual} != {expected}"
                )
            }
            Self::InvalidScrollMetrics => {
                formatter.write_str("external terminal scroll metrics are invalid")
            }
            Self::CursorOutsideModel => {
                formatter.write_str("external terminal cursor is outside the model viewport")
            }
            Self::HyperlinkIndex { row, column, index } => write!(
                formatter,
                "external terminal hyperlink index {index} is invalid at row {row}, column {column}"
            ),
            Self::HyperlinkTooLong { index } => {
                write!(
                    formatter,
                    "external terminal hyperlink {index} exceeds the metadata bound"
                )
            }
            Self::InvalidNamedColor(index) => {
                write!(
                    formatter,
                    "external terminal named color {index} is invalid"
                )
            }
            Self::BufferAreaOutside { buffer, requested } => write!(
                formatter,
                "requested render area {requested:?} is outside buffer {buffer:?}"
            ),
            Self::InconsistentWidthFlags { row, column } => write!(
                formatter,
                "external terminal cell width flags disagree at row {row}, column {column}"
            ),
        }
    }
}

impl std::error::Error for ExternalRenderError {}

/// A source that can provide one borrowed, stable model view.
///
/// The checkpoint sibling can implement this for its `ExternalTerminalSession`
/// by taking a read lock, copying or borrowing the fork's structured cells,
/// and returning a view stamped with the session's even content revision.
pub trait ExternalRenderSource {
    fn external_render_view(&self) -> ExternalTerminalView<'_>;
}

/// Render through a source adapter without introducing a fake PTY/runtime.
pub fn render_model_into_buffer<S: ExternalRenderSource>(
    source: &S,
    buffer: &mut Buffer,
    area: Rect,
    options: ExternalRenderOptions,
) -> Result<ExternalRenderOutput, ExternalRenderError> {
    let view = source.external_render_view();
    render_into_buffer(buffer, area, &view, options)
}

/// Validate and render one complete external terminal surface into `buffer`.
///
/// The entire requested area is first reset to the supplied theme, then the
/// model viewport is copied directly cell-by-cell. The function never obtains
/// a provider handle, writes input, starts a child process, or emits terminal
/// escape bytes.
pub fn render_into_buffer(
    buffer: &mut Buffer,
    area: Rect,
    view: &ExternalTerminalView<'_>,
    options: ExternalRenderOptions,
) -> Result<ExternalRenderOutput, ExternalRenderError> {
    view.validate()?;
    if !rect_contains(buffer.area, area) {
        return Err(ExternalRenderError::BufferAreaOutside {
            buffer: buffer.area,
            requested: area,
        });
    }

    let mut stats = ExternalRenderStats::default();
    let clear_style = Style::new()
        .fg(view.theme.foreground)
        .bg(view.theme.background);
    for y in area.y..area.y.saturating_add(area.height) {
        for x in area.x..area.x.saturating_add(area.width) {
            let cell = &mut buffer[(x, y)];
            cell.reset();
            cell.set_style(clear_style);
            stats.cleared_cells += 1;
        }
    }

    let rows_to_write = usize::from(area.height).min(view.rows.len());
    let columns_to_write = usize::from(area.width).min(usize::from(view.columns));
    let mut hyperlinks = Vec::new();
    let mut symbol = String::new();
    for row_index in 0..rows_to_write {
        let row = view.rows[row_index];
        for column in 0..columns_to_write {
            let model_cell = row.cells[column];
            let style = cell_style(model_cell, view.palette, view.theme);
            let x = area.x + u16::try_from(column).expect("column is bounded by u16 area");
            let y = area.y + u16::try_from(row_index).expect("row is bounded by u16 area");
            let cell = &mut buffer[(x, y)];
            cell.reset();
            cell.set_style(style);

            if model_cell.width.is_spacer() {
                // Keep the spacer's style so a wide glyph's background and
                // underline remain coherent, but never emit it as a second
                // visible glyph during ratatui diffing or ANSI frame output.
                cell.set_symbol(" ");
                cell.set_diff_option(CellDiffOption::Skip);
                stats.spacer_cells += 1;
            } else {
                symbol.clear();
                symbol.push(model_cell.base);
                symbol.extend(model_cell.zero_width.iter().copied());
                cell.set_symbol(&symbol);
                if model_cell.width == ExternalCellWidth::Wide {
                    // The explicit width survives even when a model uses a
                    // glyph whose Unicode width table differs from the model.
                    cell.set_diff_option(CellDiffOption::ForcedWidth(
                        NonZeroU16::new(2).expect("2 is non-zero"),
                    ));
                    stats.wide_cells += 1;
                }
                stats.visible_cells += 1;
                if !model_cell.zero_width.is_empty() {
                    stats.combining_cells += 1;
                }
                if let Some(index) = model_cell.hyperlink {
                    let link_index = usize::try_from(index).unwrap_or(usize::MAX);
                    let link = view.hyperlinks.get(link_index).ok_or(
                        ExternalRenderError::HyperlinkIndex {
                            row: row_index,
                            column,
                            index,
                        },
                    )?;
                    hyperlinks.push(ExternalRenderedHyperlink {
                        position: Position::new(x, y),
                        symbol: symbol.clone(),
                        id: link.id.to_owned(),
                        uri: link.uri.to_owned(),
                    });
                }
            }
        }
    }

    let cursor = rendered_cursor(view, area, options.cursor);
    Ok(ExternalRenderOutput {
        cursor,
        hyperlinks,
        content_revision: view.content_revision,
        scroll: view.scroll,
        modes: view.modes,
        stats,
    })
}

impl ExternalTerminalView<'_> {
    /// Check all shape and metadata invariants before a render begins.
    pub fn validate(&self) -> Result<(), ExternalRenderError> {
        if !self.content_revision.is_multiple_of(2) {
            return Err(ExternalRenderError::UnstableRevision(self.content_revision));
        }
        if self.columns == 0 || self.screen_lines == 0 {
            return Err(ExternalRenderError::EmptyDimensions);
        }
        let expected_rows = usize::from(self.screen_lines);
        if self.rows.len() != expected_rows {
            return Err(ExternalRenderError::RowCount {
                expected: expected_rows,
                actual: self.rows.len(),
            });
        }
        if self.scroll.viewport_rows != expected_rows
            || self.scroll.offset_from_bottom > self.scroll.max_offset_from_bottom
            || self.scroll.max_offset_from_bottom > self.scroll.history_rows
        {
            return Err(ExternalRenderError::InvalidScrollMetrics);
        }
        if self.modes.alternate_screen && self.scroll.history_rows != 0 {
            // Alternate screen normally has no primary scrollback. The model
            // owner may still expose an explicit history if its policy permits
            // it, so this remains a metadata check rather than a hard reject.
        }
        if let Some(cursor) = self.cursor {
            if cursor.x >= self.columns || cursor.y >= self.screen_lines {
                return Err(ExternalRenderError::CursorOutsideModel);
            }
        }
        for (index, link) in self.hyperlinks.iter().enumerate() {
            if link.id.len() > 128 || link.uri.len() > 4096 {
                return Err(ExternalRenderError::HyperlinkTooLong { index });
            }
        }
        let expected_cells = usize::from(self.columns);
        for (row_index, row) in self.rows.iter().enumerate() {
            if row.cells.len() != expected_cells {
                return Err(ExternalRenderError::CellCount {
                    row: row_index,
                    expected: expected_cells,
                    actual: row.cells.len(),
                });
            }
            for (column, cell) in row.cells.iter().enumerate() {
                validate_color(cell.fg)?;
                validate_color(cell.bg)?;
                if let Some(color) = cell.underline_color {
                    validate_color(color)?;
                }
                if let Some(index) = cell.hyperlink {
                    let link_index = usize::try_from(index).unwrap_or(usize::MAX);
                    if link_index >= self.hyperlinks.len() {
                        return Err(ExternalRenderError::HyperlinkIndex {
                            row: row_index,
                            column,
                            index,
                        });
                    }
                }
                let wide_bits = ExternalCellFlags::WIDE_CHAR
                    | ExternalCellFlags::WIDE_CHAR_SPACER
                    | ExternalCellFlags::LEADING_WIDE_CHAR_SPACER;
                let inconsistent = match cell.width {
                    ExternalCellWidth::Narrow => cell.flags.intersects(wide_bits),
                    ExternalCellWidth::Wide => {
                        !cell.flags.contains(ExternalCellFlags::WIDE_CHAR)
                            || cell.flags.intersects(
                                ExternalCellFlags::WIDE_CHAR_SPACER
                                    | ExternalCellFlags::LEADING_WIDE_CHAR_SPACER,
                            )
                    }
                    ExternalCellWidth::WideSpacer => {
                        !cell.flags.contains(ExternalCellFlags::WIDE_CHAR_SPACER)
                    }
                    ExternalCellWidth::LeadingWideSpacer => !cell
                        .flags
                        .contains(ExternalCellFlags::LEADING_WIDE_CHAR_SPACER),
                };
                if inconsistent {
                    return Err(ExternalRenderError::InconsistentWidthFlags {
                        row: row_index,
                        column,
                    });
                }
            }
        }
        Ok(())
    }
}

fn validate_color(color: ExternalColor) -> Result<(), ExternalRenderError> {
    if let ExternalColor::Named(index) = color {
        if usize::from(index) >= EXTERNAL_PALETTE_LEN {
            return Err(ExternalRenderError::InvalidNamedColor(index));
        }
    }
    Ok(())
}

fn rect_contains(outer: Rect, inner: Rect) -> bool {
    let outer_right = u32::from(outer.x) + u32::from(outer.width);
    let outer_bottom = u32::from(outer.y) + u32::from(outer.height);
    let inner_right = u32::from(inner.x) + u32::from(inner.width);
    let inner_bottom = u32::from(inner.y) + u32::from(inner.height);
    inner.x >= outer.x
        && inner.y >= outer.y
        && inner_right <= outer_right
        && inner_bottom <= outer_bottom
}

fn rendered_cursor(
    view: &ExternalTerminalView<'_>,
    area: Rect,
    policy: ExternalCursorPolicy,
) -> Option<ExternalRenderedCursor> {
    let allow = match policy {
        ExternalCursorPolicy::Hidden => false,
        ExternalCursorPolicy::ReadOnlyFocused { focused } => focused && view.scroll.at_bottom(),
    };
    let cursor = view.cursor?;
    if !allow || !cursor.visible || matches!(cursor.shape, ExternalCursorShape::Hidden) {
        return None;
    }
    if cursor.x >= area.width || cursor.y >= area.height {
        return None;
    }
    Some(ExternalRenderedCursor {
        x: cursor.x,
        y: cursor.y,
        visible: true,
        shape: cursor.shape,
    })
}

fn resolve_color(color: ExternalColor, palette: &ExternalPalette, theme: ExternalTheme) -> Color {
    match color {
        ExternalColor::DefaultForeground => theme.foreground,
        ExternalColor::DefaultBackground => theme.background,
        ExternalColor::Reset => Color::Reset,
        ExternalColor::Indexed(index) => Color::Indexed(index),
        ExternalColor::Spec(rgb) => Color::Rgb(rgb.r, rgb.g, rgb.b),
        ExternalColor::Named(index) => {
            let index = usize::from(index);
            if let Some(rgb) = palette.named(index) {
                Color::Rgb(rgb.r, rgb.g, rgb.b)
            } else {
                match index {
                    256 | 267 | 268 => theme.foreground,
                    257 => theme.background,
                    258 => theme.cursor,
                    259..=266 => Color::Indexed((index - 259) as u8),
                    0..=255 => Color::Indexed(index as u8),
                    _ => Color::Reset,
                }
            }
        }
    }
}

fn cell_style(
    cell: ExternalCellView<'_>,
    palette: &ExternalPalette,
    theme: ExternalTheme,
) -> Style {
    let mut fg = resolve_color(cell.fg, palette, theme);
    let mut bg = resolve_color(cell.bg, palette, theme);
    if cell.flags.contains(ExternalCellFlags::INVERSE) {
        std::mem::swap(&mut fg, &mut bg);
    }
    let mut style = Style::new().fg(fg).bg(bg);
    let mut modifiers = Modifier::empty();
    if cell.flags.contains(ExternalCellFlags::BOLD) {
        modifiers |= Modifier::BOLD;
    }
    if cell.flags.contains(ExternalCellFlags::ITALIC) {
        modifiers |= Modifier::ITALIC;
    }
    if cell.flags.contains(ExternalCellFlags::DIM) {
        modifiers |= Modifier::DIM;
    }
    if cell.flags.contains(ExternalCellFlags::HIDDEN) {
        // Keep the explicit HIDDEN modifier for downstream ANSI/frame
        // encoders and also collapse the foreground to the resolved background
        // for renderers that only consume ratatui colors.
        fg = bg;
        style = style.fg(fg);
        modifiers |= Modifier::HIDDEN;
    }
    if cell.flags.contains(ExternalCellFlags::STRIKEOUT) {
        modifiers |= Modifier::CROSSED_OUT;
    }
    if let Some(underline) = cell.underline() {
        modifiers |= Modifier::UNDERLINED;
        let bits = modifiers.bits()
            | ((u16::from(underline.wire_code()) << UNDERLINE_STYLE_SHIFT) & UNDERLINE_STYLE_MASK);
        modifiers = Modifier::from_bits_retain(bits);
    }
    style = style.add_modifier(modifiers);
    if let Some(underline_color) = cell.underline_color {
        style = style.underline_color(resolve_color(underline_color, palette, theme));
    }
    style
}

#[cfg(test)]
mod tests {
    use super::*;

    fn palette() -> ExternalPalette {
        let mut palette = ExternalPalette::empty();
        assert!(palette.set_named(1, ExternalRgb::new(0x11, 0x22, 0x33)));
        palette
    }

    fn rows<'a>(cells: &'a [ExternalCellView<'a>]) -> [ExternalRowView<'a>; 1] {
        [ExternalRowView { cells }]
    }

    fn view<'a>(
        rows: &'a [ExternalRowView<'a>],
        palette: &'a ExternalPalette,
        hyperlinks: &'a [ExternalHyperlink<'a>],
    ) -> ExternalTerminalView<'a> {
        ExternalTerminalView {
            content_revision: 2,
            columns: 4,
            screen_lines: 1,
            rows,
            palette,
            theme: ExternalTheme {
                foreground: Color::Rgb(0xAA, 0xBB, 0xCC),
                background: Color::Rgb(0x01, 0x02, 0x03),
                cursor: Color::Rgb(0xFE, 0xFE, 0xFE),
            },
            cursor: Some(ExternalCursor {
                x: 1,
                y: 0,
                visible: true,
                shape: ExternalCursorShape::Bar { blinking: false },
            }),
            scroll: ExternalScrollMetrics {
                offset_from_bottom: 0,
                max_offset_from_bottom: 3,
                viewport_rows: 1,
                history_rows: 3,
            },
            modes: ExternalModes {
                alternate_screen: false,
                line_wrap: true,
                origin: false,
                insert: false,
                mouse: ExternalMouseMode::CellMotion,
                sgr_pixel_mouse: true,
                focus_reporting: true,
                bracketed_paste: true,
                synchronized_output: false,
            },
            hyperlinks,
        }
    }

    #[test]
    fn writes_styles_combining_wide_spacer_and_link_metadata_directly() {
        let zero_width = ['\u{0301}'];
        let flags =
            ExternalCellFlags::BOLD | ExternalCellFlags::UNDERCURL | ExternalCellFlags::WIDE_CHAR;
        let cells = [
            ExternalCellView {
                base: 'e',
                zero_width: &zero_width,
                fg: ExternalColor::Named(1),
                bg: ExternalColor::DefaultBackground,
                underline_color: Some(ExternalColor::Spec(ExternalRgb::new(9, 8, 7))),
                flags,
                width: ExternalCellWidth::Wide,
                hyperlink: Some(0),
            },
            ExternalCellView {
                base: ' ',
                zero_width: &[],
                fg: ExternalColor::Named(1),
                bg: ExternalColor::DefaultBackground,
                underline_color: None,
                flags: ExternalCellFlags::WIDE_CHAR_SPACER,
                width: ExternalCellWidth::WideSpacer,
                hyperlink: None,
            },
            ExternalCellView::plain('x'),
            ExternalCellView::plain(' '),
        ];
        let palette = palette();
        let rows = rows(&cells);
        let links = [ExternalHyperlink {
            id: "id",
            uri: "https://example.invalid/path",
        }];
        let view = view(&rows, &palette, &links);
        let mut buffer = Buffer::empty(Rect::new(0, 0, 4, 1));
        let output = render_into_buffer(
            &mut buffer,
            Rect::new(0, 0, 4, 1),
            &view,
            ExternalRenderOptions {
                cursor: ExternalCursorPolicy::ReadOnlyFocused { focused: true },
            },
        )
        .expect("valid view");

        assert_eq!(buffer[(0, 0)].symbol(), "e\u{0301}");
        assert_eq!(buffer[(0, 0)].fg, Color::Rgb(0x11, 0x22, 0x33));
        assert_eq!(buffer[(0, 0)].bg, Color::Rgb(0x01, 0x02, 0x03));
        assert!(buffer[(0, 0)].modifier.contains(Modifier::BOLD));
        assert!(buffer[(0, 0)].modifier.contains(Modifier::UNDERLINED));
        assert_eq!(
            (buffer[(0, 0)].modifier.bits() & UNDERLINE_STYLE_MASK) >> UNDERLINE_STYLE_SHIFT,
            3
        );
        assert_eq!(buffer[(1, 0)].symbol(), " ");
        assert_eq!(buffer[(1, 0)].diff_option, CellDiffOption::Skip);
        assert_eq!(output.hyperlinks.len(), 1);
        assert_eq!(output.hyperlinks[0].position, Position::new(0, 0));
        assert_eq!(output.hyperlinks[0].symbol, "e\u{0301}");
        assert_eq!(output.cursor.unwrap().x, 1);
        assert_eq!(output.modes.mouse, ExternalMouseMode::CellMotion);
        assert!(output.modes.bracketed_paste);
        assert!(output.modes.sgr_pixel_mouse);
        assert_eq!(output.scroll.max_offset_from_bottom, 3);
        assert_eq!(output.stats.wide_cells, 1);
        assert_eq!(output.stats.combining_cells, 1);
    }

    #[test]
    fn inverse_swaps_resolved_colors_and_hidden_collapses_foreground() {
        let cells = [
            ExternalCellView {
                base: 'i',
                zero_width: &[],
                fg: ExternalColor::Spec(ExternalRgb::new(10, 20, 30)),
                bg: ExternalColor::Spec(ExternalRgb::new(40, 50, 60)),
                underline_color: None,
                flags: ExternalCellFlags::INVERSE,
                width: ExternalCellWidth::Narrow,
                hyperlink: None,
            },
            ExternalCellView {
                base: 'h',
                zero_width: &[],
                fg: ExternalColor::Spec(ExternalRgb::new(70, 80, 90)),
                bg: ExternalColor::Spec(ExternalRgb::new(1, 2, 3)),
                underline_color: None,
                flags: ExternalCellFlags::HIDDEN,
                width: ExternalCellWidth::Narrow,
                hyperlink: None,
            },
            ExternalCellView::plain(' '),
            ExternalCellView::plain(' '),
        ];
        let palette = palette();
        let rows = rows(&cells);
        let links = [];
        let view = view(&rows, &palette, &links);
        let mut buffer = Buffer::empty(Rect::new(0, 0, 4, 1));
        let area = buffer.area;
        render_into_buffer(&mut buffer, area, &view, Default::default()).unwrap();
        assert_eq!(buffer[(0, 0)].fg, Color::Rgb(40, 50, 60));
        assert_eq!(buffer[(0, 0)].bg, Color::Rgb(10, 20, 30));
        assert_eq!(buffer[(1, 0)].fg, Color::Rgb(1, 2, 3));
        assert!(buffer[(1, 0)].modifier.contains(Modifier::HIDDEN));
    }

    #[test]
    fn focused_cursor_is_hidden_while_scrolled_back() {
        let cells = [
            ExternalCellView::plain('a'),
            ExternalCellView::plain('b'),
            ExternalCellView::plain('c'),
            ExternalCellView::plain('d'),
        ];
        let palette = palette();
        let rows = rows(&cells);
        let links = [];
        let mut view = view(&rows, &palette, &links);
        view.scroll.offset_from_bottom = 1;
        let mut buffer = Buffer::empty(Rect::new(0, 0, 4, 1));
        let area = buffer.area;
        let output = render_into_buffer(
            &mut buffer,
            area,
            &view,
            ExternalRenderOptions {
                cursor: ExternalCursorPolicy::ReadOnlyFocused { focused: true },
            },
        )
        .unwrap();
        assert!(output.cursor.is_none());
    }

    #[test]
    fn validates_shape_before_mutating_destination() {
        let cells = [ExternalCellView::plain('a')];
        let palette = palette();
        let rows = [ExternalRowView { cells: &cells }];
        let links = [];
        let mut view = view(&rows, &palette, &links);
        view.content_revision = 3;
        let mut buffer = Buffer::with_lines(["old!"]);
        let before = buffer.clone();
        let area = buffer.area;
        assert_eq!(
            render_into_buffer(&mut buffer, area, &view, Default::default()),
            Err(ExternalRenderError::UnstableRevision(3))
        );
        assert_eq!(buffer, before);
    }

    #[test]
    fn source_adapter_uses_one_view_without_pty_or_ansi() {
        struct Source<'a> {
            view: ExternalTerminalView<'a>,
        }
        impl<'a> ExternalRenderSource for Source<'a> {
            fn external_render_view(&self) -> ExternalTerminalView<'_> {
                self.view
            }
        }
        let cells = [
            ExternalCellView::plain('o'),
            ExternalCellView::plain('k'),
            ExternalCellView::plain('!'),
            ExternalCellView::plain(' '),
        ];
        let palette = palette();
        let rows = rows(&cells);
        let links = [];
        let source = Source {
            view: view(&rows, &palette, &links),
        };
        let mut buffer = Buffer::empty(Rect::new(0, 0, 4, 1));
        let area = buffer.area;
        let result =
            render_model_into_buffer(&source, &mut buffer, area, Default::default()).unwrap();
        assert_eq!(result.content_revision, 2);
        assert_eq!(buffer[(0, 0)].symbol(), "o");
        assert_eq!(buffer[(1, 0)].symbol(), "k");
    }
}
