//! Stable, explicit checkpoint support for the terminal model.
//!
//! The upstream terminal type derives serde for a few implementation structs,
//! but that representation intentionally skips cursors and includes the
//! storage ring.  It is therefore not a restart contract.  This module is the
//! fork-owned contract: it records the semantic rows and every terminal field
//! that can affect later input or rendering, and it restores a fresh model
//! only after validation has completed.

use std::convert::TryFrom;
use std::fmt;

#[cfg(feature = "serde")]
use serde::de::{self, DeserializeSeed, Deserializer, Error as DeError, SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use std::marker::PhantomData;

use crate::grid::{Charsets, Cursor, Dimensions, Grid, Row};
use crate::index::{Column, Line, Point};
use crate::term::cell::{Cell, Flags, Hyperlink, MAX_ZERO_WIDTH_CHARS};
use crate::term::color::{Colors, COUNT as COLOR_COUNT};
use crate::term::{Config, Osc52, Term, TermDamageState, TermMode};
use crate::vi_mode::ViModeCursor;
use crate::vte::ansi::{
    CharsetIndex, Color, CursorShape, CursorStyle, KeyboardModes, NamedColor, Rgb, StandardCharset,
};
use unicode_width::UnicodeWidthChar;

use super::{TabStops, KEYBOARD_MODE_STACK_MAX_DEPTH, TITLE_STACK_MAX_DEPTH};

/// Schema identifier for the standalone external terminal model.
pub const CHECKPOINT_SCHEMA: &str = "herdr.external-terminal";

/// Version of [`TermCheckpointV1`].
pub const CHECKPOINT_VERSION: u16 = 1;

/// Upstream Alacritty revision used by this fork.
pub const ALACRITTY_UPSTREAM_COMMIT: &str = "94e7c8874e526b1e67b349d9ba30ddf81669119e";

/// Upstream vte revision expected by this fork.
pub const VTE_UPSTREAM_COMMIT: &str = "3b3da71c34cc1256c7e20981cf03f8eb95e08ffc";

const KNOWN_TERM_MODE_BITS: u32 = 0x007f_ffff;

// These are the default semantic limits used by `TermCheckpointV1::validate`.
// The serde visitors below use the same values so an untrusted payload cannot
// first allocate a collection which semantic validation would reject later.
const CHECKPOINT_MAX_COLUMNS: usize = 16_384;
const CHECKPOINT_MAX_SCREEN_LINES: usize = 16_384;
const CHECKPOINT_MAX_HISTORY_ROWS: usize = 1_000_000;
const CHECKPOINT_MAX_TITLE_LENGTH: usize = 16_384;
const CHECKPOINT_MAX_HYPERLINK_LENGTH: usize = 16_384;
const CHECKPOINT_MAX_ZERO_WIDTH_CHARS: usize = MAX_ZERO_WIDTH_CHARS;
const CHECKPOINT_MAX_SEMANTIC_ESCAPE_CHARS: usize = 4_096;
#[cfg(feature = "serde")]
const CHECKPOINT_MAX_IDENTITY_STRING_LENGTH: usize = 1_024;
#[cfg(feature = "serde")]
const CHECKPOINT_MAX_ROWS: usize = CHECKPOINT_MAX_HISTORY_ROWS + CHECKPOINT_MAX_SCREEN_LINES;

/// Stable engine identity embedded in every checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct EngineFingerprintV1 {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_identity_string")
    )]
    pub package: String,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_identity_string")
    )]
    pub package_version: String,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_identity_string")
    )]
    pub upstream_commit: String,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_identity_string")
    )]
    pub vte_package: String,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_identity_string")
    )]
    pub vte_version: String,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_identity_string")
    )]
    pub vte_commit: String,
    pub schema_revision: u16,
}

impl EngineFingerprintV1 {
    /// Identity of the fork that produced this checkpoint.
    #[must_use]
    pub fn current() -> Self {
        Self {
            package: "alacritty_terminal".to_owned(),
            package_version: env!("CARGO_PKG_VERSION").to_owned(),
            upstream_commit: ALACRITTY_UPSTREAM_COMMIT.to_owned(),
            vte_package: "vte".to_owned(),
            vte_version: "0.15.0".to_owned(),
            vte_commit: VTE_UPSTREAM_COMMIT.to_owned(),
            schema_revision: CHECKPOINT_VERSION,
        }
    }
}

/// Terminal dimensions in the checkpoint wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct DimensionsV1 {
    pub columns: u32,
    pub screen_lines: u32,
}

/// A half-open terminal line range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct RangeV1 {
    pub start: u32,
    pub end: u32,
}

/// A terminal point.  Negative lines are history rows in the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct PointV1 {
    pub line: i32,
    pub column: u32,
}

/// Explicit terminal configuration needed to interpret future input.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct ConfigV1 {
    pub scrolling_history: u32,
    pub default_cursor_style: CursorStyleV1,
    pub vi_mode_cursor_style: Option<CursorStyleV1>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_semantic_escape_chars")
    )]
    pub semantic_escape_chars: String,
    pub kitty_keyboard: bool,
    pub osc52: Osc52V1,
}

/// OSC 52 policy in the stable DTO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub enum Osc52V1 {
    Disabled,
    OnlyCopy,
    OnlyPaste,
    CopyPaste,
}

/// A cursor style independent of the upstream enum layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct CursorStyleV1 {
    pub shape: CursorShapeV1,
    pub blinking: bool,
}

/// Cursor shape in the stable DTO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub enum CursorShapeV1 {
    Block,
    Underline,
    Beam,
    HollowBlock,
    Hidden,
}

/// An explicit snapshot of one screen/history grid.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct GridCheckpointV1 {
    pub columns: u32,
    pub lines: u32,
    pub max_scroll_limit: u32,
    pub display_offset: u32,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_rows"))]
    pub rows: Vec<RowCheckpointV1>,
    pub cursor: CursorCheckpointV1,
    pub saved_cursor: CursorCheckpointV1,
}

/// A row in semantic order, oldest retained history through bottom viewport.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct RowCheckpointV1 {
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_cells"))]
    pub cells: Vec<CellCheckpointV1>,
}

/// Writing state associated with a grid cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct CursorCheckpointV1 {
    pub point: PointV1,
    pub template: CellCheckpointV1,
    pub charsets: [StandardCharsetV1; 4],
    pub input_needs_wrap: bool,
}

/// All visual and future-writing state of one cell.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct CellCheckpointV1 {
    pub c: char,
    pub fg: ColorCheckpointV1,
    pub bg: ColorCheckpointV1,
    pub flags: u16,
    pub extra: Option<CellExtraCheckpointV1>,
}

/// Rare cell state which still affects rendering or later terminal input.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct CellExtraCheckpointV1 {
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_zero_width"))]
    pub zero_width: Vec<char>,
    pub underline_color: Option<ColorCheckpointV1>,
    pub hyperlink: Option<HyperlinkCheckpointV1>,
}

/// Hyperlink identity and URI attached to a cell.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct HyperlinkCheckpointV1 {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_hyperlink_string")
    )]
    pub id: String,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_hyperlink_string")
    )]
    pub uri: String,
}

/// Cell colors with explicit distinction between named, truecolor and indexed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub enum ColorCheckpointV1 {
    Named(NamedColorV1),
    Spec(RgbV1),
    Indexed(u8),
}

/// RGB color in the stable DTO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct RgbV1 {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

/// Named colors in the exact Alacritty palette.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub enum NamedColorV1 {
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,
    BrightBlack,
    BrightRed,
    BrightGreen,
    BrightYellow,
    BrightBlue,
    BrightMagenta,
    BrightCyan,
    BrightWhite,
    Foreground,
    Background,
    Cursor,
    DimBlack,
    DimRed,
    DimGreen,
    DimYellow,
    DimBlue,
    DimMagenta,
    DimCyan,
    DimWhite,
    BrightForeground,
    DimForeground,
}

/// Character set designation in the stable DTO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub enum StandardCharsetV1 {
    Ascii,
    SpecialCharacterAndLineDrawing,
}

/// Active character set index in the stable DTO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub enum CharsetIndexV1 {
    G0,
    G1,
    G2,
    G3,
}

/// Kitty keyboard protocol mode stack entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct KeyboardModesV1 {
    pub bits: u8,
}

/// Complete version-one terminal model checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct TermCheckpointV1 {
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_schema_string")
    )]
    pub schema: String,
    pub version: u16,
    pub engine: EngineFingerprintV1,
    pub dimensions: DimensionsV1,
    pub config: ConfigV1,
    /// The grid currently selected by the ALT_SCREEN mode bit.
    pub active_grid: GridCheckpointV1,
    /// The grid not currently selected by the ALT_SCREEN mode bit.
    pub inactive_grid: GridCheckpointV1,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_tabs"))]
    pub tabs: Vec<bool>,
    pub active_charset: CharsetIndexV1,
    pub mode_bits: u32,
    pub scroll_region: RangeV1,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_colors"))]
    pub colors: Vec<Option<RgbV1>>,
    pub cursor_style: Option<CursorStyleV1>,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_title"))]
    pub title: Option<String>,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_title_stack"))]
    pub title_stack: Vec<Option<String>>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_keyboard_stack")
    )]
    pub keyboard_mode_stack: Vec<KeyboardModesV1>,
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_keyboard_stack")
    )]
    pub inactive_keyboard_mode_stack: Vec<KeyboardModesV1>,
    pub vi_mode_cursor: PointV1,
}

/// Upper bounds applied before restoring an untrusted checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointLimits {
    pub max_columns: usize,
    pub max_screen_lines: usize,
    pub max_history_rows: usize,
    pub max_cells: usize,
    pub max_title_length: usize,
    pub max_title_stack_depth: usize,
    pub max_keyboard_stack_depth: usize,
    pub max_hyperlink_length: usize,
    pub max_zero_width_chars: usize,
    pub max_semantic_escape_chars: usize,
    pub max_serialized_bytes: usize,
}

impl Default for CheckpointLimits {
    fn default() -> Self {
        Self {
            max_columns: CHECKPOINT_MAX_COLUMNS,
            max_screen_lines: CHECKPOINT_MAX_SCREEN_LINES,
            max_history_rows: CHECKPOINT_MAX_HISTORY_ROWS,
            max_cells: 64_000_000,
            max_title_length: CHECKPOINT_MAX_TITLE_LENGTH,
            max_title_stack_depth: TITLE_STACK_MAX_DEPTH,
            max_keyboard_stack_depth: KEYBOARD_MODE_STACK_MAX_DEPTH,
            max_hyperlink_length: CHECKPOINT_MAX_HYPERLINK_LENGTH,
            max_zero_width_chars: CHECKPOINT_MAX_ZERO_WIDTH_CHARS,
            max_semantic_escape_chars: CHECKPOINT_MAX_SEMANTIC_ESCAPE_CHARS,
            max_serialized_bytes: 16 * 1024 * 1024,
        }
    }
}

impl CheckpointLimits {
    /// Reject a serialized payload before deserializing it.
    pub fn validate_serialized_len(&self, len: usize) -> Result<(), CheckpointError> {
        if len > self.max_serialized_bytes {
            Err(CheckpointError::LimitExceeded("serialized checkpoint"))
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "serde")]
fn deserialize_bounded_vec<'de, D, T>(
    deserializer: D,
    max: usize,
    field: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserializer.deserialize_seq(BoundedVecVisitor {
        max,
        field,
        marker: PhantomData,
    })
}

#[cfg(feature = "serde")]
struct BoundedVecVisitor<T> {
    max: usize,
    field: &'static str,
    marker: PhantomData<fn() -> T>,
}

#[cfg(feature = "serde")]
impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sequence")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let size_hint = sequence.size_hint();
        if let Some(size_hint) = size_hint {
            if size_hint > self.max {
                return Err(A::Error::custom(CheckpointError::LimitExceeded(self.field)));
            }
        }

        // Keep the initial allocation small even when a format supplies a
        // large but still semantically valid size hint.  The length check
        // below remains the authority for formats without a size hint.
        let initial_capacity = size_hint.unwrap_or(0).min(self.max).min(1024);
        let mut values = Vec::with_capacity(initial_capacity);
        loop {
            if values.len() == self.max {
                // Use IgnoredAny for the probe so an over-bound element is
                // never materialized as T before it is rejected.
                if sequence.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom(CheckpointError::LimitExceeded(self.field)));
                }
                break;
            }

            match sequence.next_element()? {
                Some(value) => values.push(value),
                None => break,
            }
        }
        Ok(values)
    }
}

#[cfg(feature = "serde")]
fn deserialize_bounded_string<'de, D>(
    deserializer: D,
    max: usize,
    field: &'static str,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_string(BoundedStringVisitor { max, field })
}

#[cfg(feature = "serde")]
struct BoundedStringVisitor {
    max: usize,
    field: &'static str,
}

#[cfg(feature = "serde")]
impl<'de> Visitor<'de> for BoundedStringVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded string")
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > self.max {
            return Err(E::custom(CheckpointError::LimitExceeded(self.field)));
        }
        Ok(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > self.max {
            return Err(E::custom(CheckpointError::LimitExceeded(self.field)));
        }
        Ok(value)
    }
}

#[cfg(feature = "serde")]
fn deserialize_bounded_option_string<'de, D>(
    deserializer: D,
    max: usize,
    field: &'static str,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_option(BoundedOptionStringVisitor { max, field })
}

#[cfg(feature = "serde")]
struct BoundedOptionStringVisitor {
    max: usize,
    field: &'static str,
}

#[cfg(feature = "serde")]
impl<'de> Visitor<'de> for BoundedOptionStringVisitor {
    type Value = Option<String>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("null or a bounded string")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_bounded_string(deserializer, self.max, self.field).map(Some)
    }
}

#[cfg(feature = "serde")]
struct BoundedOptionStringSeed {
    max: usize,
    field: &'static str,
}

#[cfg(feature = "serde")]
impl<'de> DeserializeSeed<'de> for BoundedOptionStringSeed {
    type Value = Option<String>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_bounded_option_string(deserializer, self.max, self.field)
    }
}

#[cfg(feature = "serde")]
fn deserialize_bounded_option_string_vec<'de, D>(
    deserializer: D,
    max: usize,
    field: &'static str,
) -> Result<Vec<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(BoundedOptionStringVecVisitor { max, field })
}

#[cfg(feature = "serde")]
struct BoundedOptionStringVecVisitor {
    max: usize,
    field: &'static str,
}

#[cfg(feature = "serde")]
impl<'de> Visitor<'de> for BoundedOptionStringVecVisitor {
    type Value = Vec<Option<String>>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sequence of optional strings")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let size_hint = sequence.size_hint();
        if let Some(size_hint) = size_hint {
            if size_hint > self.max {
                return Err(A::Error::custom(CheckpointError::LimitExceeded(self.field)));
            }
        }
        let initial_capacity = size_hint.unwrap_or(0).min(self.max).min(1024);
        let mut values = Vec::with_capacity(initial_capacity);
        loop {
            if values.len() == self.max {
                if sequence.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom(CheckpointError::LimitExceeded(self.field)));
                }
                break;
            }

            match sequence.next_element_seed(BoundedOptionStringSeed {
                max: self.max,
                field: self.field,
            })? {
                Some(value) => values.push(value),
                None => break,
            }
        }
        Ok(values)
    }
}

#[cfg(feature = "serde")]
fn deserialize_identity_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_string(
        deserializer,
        CHECKPOINT_MAX_IDENTITY_STRING_LENGTH,
        "engine identity",
    )
}

#[cfg(feature = "serde")]
fn deserialize_schema_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_string(
        deserializer,
        CHECKPOINT_MAX_IDENTITY_STRING_LENGTH,
        "schema",
    )
}

#[cfg(feature = "serde")]
fn deserialize_semantic_escape_chars<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_string(
        deserializer,
        CHECKPOINT_MAX_SEMANTIC_ESCAPE_CHARS,
        "semantic_escape_chars",
    )
}

#[cfg(feature = "serde")]
fn deserialize_hyperlink_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_string(
        deserializer,
        CHECKPOINT_MAX_HYPERLINK_LENGTH,
        "hyperlink string",
    )
}

#[cfg(feature = "serde")]
fn deserialize_title<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_option_string(deserializer, CHECKPOINT_MAX_TITLE_LENGTH, "title")
}

#[cfg(feature = "serde")]
fn deserialize_title_stack<'de, D>(deserializer: D) -> Result<Vec<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_option_string_vec(deserializer, TITLE_STACK_MAX_DEPTH, "title_stack")
}

#[cfg(feature = "serde")]
fn deserialize_rows<'de, D>(deserializer: D) -> Result<Vec<RowCheckpointV1>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, CHECKPOINT_MAX_ROWS, "rows")
}

#[cfg(feature = "serde")]
fn deserialize_cells<'de, D>(deserializer: D) -> Result<Vec<CellCheckpointV1>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, CHECKPOINT_MAX_COLUMNS, "cells")
}

#[cfg(feature = "serde")]
fn deserialize_zero_width<'de, D>(deserializer: D) -> Result<Vec<char>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, CHECKPOINT_MAX_ZERO_WIDTH_CHARS, "zero_width")
}

#[cfg(feature = "serde")]
fn deserialize_tabs<'de, D>(deserializer: D) -> Result<Vec<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, CHECKPOINT_MAX_COLUMNS, "tabs")
}

#[cfg(feature = "serde")]
fn deserialize_colors<'de, D>(deserializer: D) -> Result<Vec<Option<RgbV1>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, COLOR_COUNT, "colors")
}

#[cfg(feature = "serde")]
fn deserialize_keyboard_stack<'de, D>(deserializer: D) -> Result<Vec<KeyboardModesV1>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(
        deserializer,
        KEYBOARD_MODE_STACK_MAX_DEPTH,
        "keyboard mode stack",
    )
}

/// Errors returned by checkpoint validation and transactional restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    SchemaMismatch,
    VersionMismatch,
    EngineMismatch,
    LimitExceeded(&'static str),
    Invalid(&'static str),
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaMismatch => f.write_str("terminal checkpoint schema mismatch"),
            Self::VersionMismatch => f.write_str("terminal checkpoint version mismatch"),
            Self::EngineMismatch => f.write_str("terminal checkpoint engine mismatch"),
            Self::LimitExceeded(field) => write!(f, "terminal checkpoint limit exceeded: {field}"),
            Self::Invalid(field) => write!(f, "invalid terminal checkpoint field: {field}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

impl TermCheckpointV1 {
    /// Validate against the default bounds.
    pub fn validate(&self) -> Result<(), CheckpointError> {
        self.validate_with_limits(&CheckpointLimits::default())
    }

    /// Validate the entire semantic model before any restore allocation.
    pub fn validate_with_limits(&self, limits: &CheckpointLimits) -> Result<(), CheckpointError> {
        if self.schema != CHECKPOINT_SCHEMA {
            return Err(CheckpointError::SchemaMismatch);
        }
        if self.version != CHECKPOINT_VERSION {
            return Err(CheckpointError::VersionMismatch);
        }
        if self.engine != EngineFingerprintV1::current() {
            return Err(CheckpointError::EngineMismatch);
        }

        let columns = bounded_u32(self.dimensions.columns, limits.max_columns, "columns")?;
        let screen_lines = bounded_u32(
            self.dimensions.screen_lines,
            limits.max_screen_lines,
            "screen_lines",
        )?;
        if columns < 2 {
            return Err(CheckpointError::Invalid("columns"));
        }
        if screen_lines == 0 {
            return Err(CheckpointError::Invalid("screen_lines"));
        }

        if self.config.semantic_escape_chars.len() > limits.max_semantic_escape_chars {
            return Err(CheckpointError::LimitExceeded("semantic_escape_chars"));
        }
        bounded_u32(
            self.config.scrolling_history,
            limits.max_history_rows,
            "scrolling_history",
        )?;
        validate_cursor_style(self.config.default_cursor_style)?;
        if let Some(style) = self.config.vi_mode_cursor_style {
            validate_cursor_style(style)?;
        }
        validate_osc52(self.config.osc52)?;

        let active_cells = self
            .active_grid
            .validate_with_limits(columns, screen_lines, limits)?;
        let inactive_cells =
            self.inactive_grid
                .validate_with_limits(columns, screen_lines, limits)?;
        if active_cells
            .checked_add(inactive_cells)
            .ok_or(CheckpointError::LimitExceeded("cells"))?
            > limits.max_cells
        {
            return Err(CheckpointError::LimitExceeded("cells"));
        }

        if self.tabs.len() != columns {
            return Err(CheckpointError::Invalid("tabs"));
        }
        validate_charset_index(self.active_charset)?;
        if self.mode_bits & !KNOWN_TERM_MODE_BITS != 0 {
            return Err(CheckpointError::Invalid("mode_bits"));
        }
        // `Term::new` gives the primary grid the configured scrollback limit
        // and the alternate grid no history.  `swap_alt` preserves those
        // roles while changing which grid is active.  Keep this relationship
        // explicit at the wire boundary: accepting an arbitrary limit here
        // would let a checkpoint restore a model with scrollback semantics
        // that no live `Term` can produce for the recorded mode.
        let alt_screen = self.mode_bits & TermMode::ALT_SCREEN.bits() != 0;
        let active_max_scroll_limit = if alt_screen {
            0
        } else {
            self.config.scrolling_history
        };
        let inactive_max_scroll_limit = if alt_screen {
            self.config.scrolling_history
        } else {
            0
        };
        if self.active_grid.max_scroll_limit != active_max_scroll_limit {
            return Err(CheckpointError::Invalid("active_grid.max_scroll_limit"));
        }
        if self.inactive_grid.max_scroll_limit != inactive_max_scroll_limit {
            return Err(CheckpointError::Invalid("inactive_grid.max_scroll_limit"));
        }
        let range = self.scroll_region;
        if range.start >= range.end || range.end > screen_lines as u32 {
            return Err(CheckpointError::Invalid("scroll_region"));
        }
        validate_colors(&self.colors)?;
        if let Some(style) = self.cursor_style {
            validate_cursor_style(style)?;
        }
        validate_title(self.title.as_deref(), limits)?;
        if self.title_stack.len() > limits.max_title_stack_depth.min(TITLE_STACK_MAX_DEPTH) {
            return Err(CheckpointError::LimitExceeded("title_stack"));
        }
        for title in &self.title_stack {
            validate_title(title.as_deref(), limits)?;
        }
        validate_keyboard_stack(
            &self.keyboard_mode_stack,
            limits
                .max_keyboard_stack_depth
                .min(KEYBOARD_MODE_STACK_MAX_DEPTH),
        )?;
        validate_keyboard_stack(
            &self.inactive_keyboard_mode_stack,
            limits
                .max_keyboard_stack_depth
                .min(KEYBOARD_MODE_STACK_MAX_DEPTH),
        )?;
        // The vi cursor is tied to the primary grid.  `swap_alt` leaves that
        // point untouched while making the alternate grid active, so consult
        // the inactive grid during alternate-screen checkpoints.  Validating
        // it against the active alternate grid would reject a perfectly
        // valid history point because that grid intentionally has no
        // scrollback.
        let vi_grid = if alt_screen {
            &self.inactive_grid
        } else {
            &self.active_grid
        };
        validate_vi_point(self.vi_mode_cursor, vi_grid, screen_lines, "vi_mode_cursor")?;
        Ok(())
    }
}

impl GridCheckpointV1 {
    fn validate_with_limits(
        &self,
        columns: usize,
        screen_lines: usize,
        limits: &CheckpointLimits,
    ) -> Result<usize, CheckpointError> {
        if self.columns as usize != columns || self.lines as usize != screen_lines {
            return Err(CheckpointError::Invalid("grid dimensions"));
        }
        let max_scroll_limit = bounded_u32(
            self.max_scroll_limit,
            limits.max_history_rows,
            "max_scroll_limit",
        )?;
        let display_offset = self.display_offset as usize;
        if display_offset > max_scroll_limit {
            return Err(CheckpointError::Invalid("display_offset"));
        }
        let history_size = self
            .rows
            .len()
            .checked_sub(screen_lines)
            .ok_or(CheckpointError::Invalid("rows"))?;
        if display_offset > history_size {
            return Err(CheckpointError::Invalid("display_offset"));
        }
        if history_size > max_scroll_limit || history_size > limits.max_history_rows {
            return Err(CheckpointError::LimitExceeded("history rows"));
        }
        if self.rows.len() > limits.max_history_rows.saturating_add(screen_lines) {
            return Err(CheckpointError::LimitExceeded("rows"));
        }
        if self.rows.len() != history_size + screen_lines {
            return Err(CheckpointError::Invalid("rows"));
        }

        let mut cells = 0usize;
        for (row_index, row) in self.rows.iter().enumerate() {
            if row.cells.len() != columns {
                return Err(CheckpointError::Invalid("row cell count"));
            }
            cells = cells
                .checked_add(row.cells.len())
                .ok_or(CheckpointError::LimitExceeded("cells"))?;
            if cells > limits.max_cells {
                return Err(CheckpointError::LimitExceeded("cells"));
            }
            for (column, cell) in row.cells.iter().enumerate() {
                validate_cell(cell, &self.rows, row_index, column, limits)?;
            }
        }
        validate_cursor(&self.cursor, columns, screen_lines, limits)?;
        validate_cursor(&self.saved_cursor, columns, screen_lines, limits)?;
        Ok(cells)
    }
}

impl<T> Term<T> {
    /// Capture a model checkpoint.
    ///
    /// This method expects the terminal to stay under the version-one numeric
    /// bounds.  Call [`Self::try_checkpoint`] when the terminal dimensions or
    /// configured history are supplied by an untrusted source.
    #[must_use]
    pub fn checkpoint(&self) -> TermCheckpointV1 {
        self.try_checkpoint()
            .expect("terminal state exceeds the version-one checkpoint bounds")
    }

    /// Capture a checkpoint without panicking for values that do not fit V1.
    pub fn try_checkpoint(&self) -> Result<TermCheckpointV1, CheckpointError> {
        // A rejected combining character means the live model no longer
        // represents the complete input stream.  Do not emit a checkpoint
        // which would appear trustworthy and silently clear that condition on
        // restore.
        if self.zero_width_overflowed() {
            return Err(CheckpointError::LimitExceeded("zero_width"));
        }

        let dimensions = DimensionsV1 {
            columns: u32::try_from(self.columns())
                .map_err(|_| CheckpointError::LimitExceeded("columns"))?,
            screen_lines: u32::try_from(self.screen_lines())
                .map_err(|_| CheckpointError::LimitExceeded("screen_lines"))?,
        };
        let config = ConfigV1::from_config(&self.config)?;
        let active_grid = GridCheckpointV1::from_grid(&self.grid)?;
        let inactive_grid = GridCheckpointV1::from_grid(&self.inactive_grid)?;
        let colors = self
            .colors
            .checkpoint_values()
            .into_iter()
            .map(|color| color.map(RgbV1::from))
            .collect();

        let checkpoint = TermCheckpointV1 {
            schema: CHECKPOINT_SCHEMA.to_owned(),
            version: CHECKPOINT_VERSION,
            engine: EngineFingerprintV1::current(),
            dimensions,
            config,
            active_grid,
            inactive_grid,
            tabs: self.tabs.tabs.clone(),
            active_charset: CharsetIndexV1::from(self.active_charset),
            mode_bits: self.mode.bits(),
            scroll_region: RangeV1 {
                start: u32::try_from(self.scroll_region.start.0)
                    .map_err(|_| CheckpointError::Invalid("scroll_region"))?,
                end: u32::try_from(self.scroll_region.end.0)
                    .map_err(|_| CheckpointError::Invalid("scroll_region"))?,
            },
            colors,
            cursor_style: self.cursor_style.map(CursorStyleV1::from),
            title: self.title.clone(),
            title_stack: self.title_stack.clone(),
            keyboard_mode_stack: self
                .keyboard_mode_stack
                .iter()
                .copied()
                .map(KeyboardModesV1::from)
                .collect(),
            inactive_keyboard_mode_stack: self
                .inactive_keyboard_mode_stack
                .iter()
                .copied()
                .map(KeyboardModesV1::from)
                .collect(),
            vi_mode_cursor: PointV1::from(self.vi_mode_cursor.point),
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Restore a new terminal transactionally from a validated checkpoint.
    ///
    /// All conversion and allocation happen before the returned value exists;
    /// no event-listener callback is invoked while the model is rebuilt.
    pub fn from_checkpoint(
        checkpoint: TermCheckpointV1,
        event_proxy: T,
    ) -> Result<Self, CheckpointError> {
        checkpoint.validate()?;

        let config = Config::try_from(&checkpoint.config)?;
        let grid = GridCheckpointV1::into_grid(&checkpoint.active_grid)?;
        let inactive_grid = GridCheckpointV1::into_grid(&checkpoint.inactive_grid)?;
        let colors = colors_from_checkpoint(&checkpoint.colors)?;
        let mode = TermMode::from_bits(checkpoint.mode_bits)
            .ok_or(CheckpointError::Invalid("mode_bits"))?;
        let columns = checkpoint.dimensions.columns as usize;
        let screen_lines = checkpoint.dimensions.screen_lines as usize;
        let scroll_region =
            Line(checkpoint.scroll_region.start as i32)..Line(checkpoint.scroll_region.end as i32);
        let tabs = TabStops {
            tabs: checkpoint.tabs,
        };
        let vi_mode_cursor = ViModeCursor::new(checkpoint.vi_mode_cursor.into());

        Ok(Self {
            is_focused: false,
            vi_mode_cursor,
            selection: None,
            grid,
            inactive_grid,
            active_charset: checkpoint.active_charset.into(),
            tabs,
            mode,
            scroll_region,
            colors,
            cursor_style: checkpoint.cursor_style.map(Into::into),
            event_proxy,
            title: checkpoint.title,
            title_stack: checkpoint.title_stack,
            keyboard_mode_stack: checkpoint
                .keyboard_mode_stack
                .into_iter()
                .map(Into::into)
                .collect(),
            inactive_keyboard_mode_stack: checkpoint
                .inactive_keyboard_mode_stack
                .into_iter()
                .map(Into::into)
                .collect(),
            damage: TermDamageState::new(columns, screen_lines),
            zero_width_overflowed: false,
            config,
        })
    }

    /// Export the active viewport as owned structured rows.
    ///
    /// The rows are copied while the model is borrowed, so callers can release
    /// their session lock before rendering or serving an API response.
    #[must_use]
    pub fn visible_checkpoint_rows(&self) -> Vec<RowCheckpointV1> {
        let rows = self.grid.checkpoint_rows();
        let history_size = self.grid.history_size();
        let start = history_size.saturating_sub(self.grid.display_offset());
        rows.into_iter()
            .skip(start)
            .take(self.screen_lines())
            .map(RowCheckpointV1::from_row)
            .collect()
    }
}

impl GridCheckpointV1 {
    fn from_grid(grid: &Grid<Cell>) -> Result<Self, CheckpointError> {
        let columns =
            u32::try_from(grid.columns()).map_err(|_| CheckpointError::LimitExceeded("columns"))?;
        let lines = u32::try_from(grid.screen_lines())
            .map_err(|_| CheckpointError::LimitExceeded("screen_lines"))?;
        let max_scroll_limit = u32::try_from(grid.max_scroll_limit())
            .map_err(|_| CheckpointError::LimitExceeded("max_scroll_limit"))?;
        let display_offset = u32::try_from(grid.display_offset())
            .map_err(|_| CheckpointError::LimitExceeded("display_offset"))?;
        let rows = grid
            .checkpoint_rows()
            .into_iter()
            .map(RowCheckpointV1::from_row)
            .collect();
        Ok(Self {
            columns,
            lines,
            max_scroll_limit,
            display_offset,
            rows,
            cursor: CursorCheckpointV1::from_cursor(&grid.cursor),
            saved_cursor: CursorCheckpointV1::from_cursor(&grid.saved_cursor),
        })
    }

    fn into_grid(value: &Self) -> Result<Grid<Cell>, CheckpointError> {
        let rows = value
            .rows
            .iter()
            .map(RowCheckpointV1::to_row)
            .collect::<Result<Vec<_>, _>>()?;
        let mut grid = Grid::from_checkpoint_rows(
            value.lines as usize,
            value.columns as usize,
            value.max_scroll_limit as usize,
            value.display_offset as usize,
            rows,
        )
        .ok_or(CheckpointError::Invalid("grid rows"))?;
        grid.cursor = value.cursor.to_cursor()?;
        grid.saved_cursor = value.saved_cursor.to_cursor()?;
        Ok(grid)
    }
}

impl RowCheckpointV1 {
    fn from_row(row: Row<Cell>) -> Self {
        Self {
            cells: row
                .into_iter()
                .cloned()
                .map(CellCheckpointV1::from)
                .collect(),
        }
    }

    fn to_row(&self) -> Result<Row<Cell>, CheckpointError> {
        let cells = self
            .cells
            .iter()
            .cloned()
            .map(Cell::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Row::from_vec(cells, 0))
    }
}

impl CursorCheckpointV1 {
    fn from_cursor(cursor: &Cursor<Cell>) -> Self {
        let charsets = [
            StandardCharsetV1::from(cursor.charsets[CharsetIndex::G0]),
            StandardCharsetV1::from(cursor.charsets[CharsetIndex::G1]),
            StandardCharsetV1::from(cursor.charsets[CharsetIndex::G2]),
            StandardCharsetV1::from(cursor.charsets[CharsetIndex::G3]),
        ];
        Self {
            point: cursor.point.into(),
            template: cursor.template.clone().into(),
            charsets,
            input_needs_wrap: cursor.input_needs_wrap,
        }
    }

    fn to_cursor(&self) -> Result<Cursor<Cell>, CheckpointError> {
        let mut charsets: Charsets = Default::default();
        charsets[CharsetIndex::G0] = self.charsets[0].into();
        charsets[CharsetIndex::G1] = self.charsets[1].into();
        charsets[CharsetIndex::G2] = self.charsets[2].into();
        charsets[CharsetIndex::G3] = self.charsets[3].into();
        Ok(Cursor {
            point: self.point.into(),
            template: self.template.clone().try_into()?,
            charsets,
            input_needs_wrap: self.input_needs_wrap,
        })
    }
}

impl From<Cell> for CellCheckpointV1 {
    fn from(cell: Cell) -> Self {
        let extra = cell.extra.as_ref().map(|_| CellExtraCheckpointV1 {
            zero_width: cell.zerowidth().unwrap_or_default().to_vec(),
            underline_color: cell.underline_color().map(ColorCheckpointV1::from),
            hyperlink: cell.hyperlink().map(|hyperlink| HyperlinkCheckpointV1 {
                id: hyperlink.id().to_owned(),
                uri: hyperlink.uri().to_owned(),
            }),
        });
        Self {
            c: cell.c,
            fg: cell.fg.into(),
            bg: cell.bg.into(),
            flags: cell.flags.bits(),
            extra,
        }
    }
}

impl TryFrom<CellCheckpointV1> for Cell {
    type Error = CheckpointError;

    fn try_from(value: CellCheckpointV1) -> Result<Self, Self::Error> {
        let flags = Flags::from_bits(value.flags).ok_or(CheckpointError::Invalid("cell flags"))?;
        let mut cell = Self {
            c: value.c,
            fg: value.fg.into(),
            bg: value.bg.into(),
            flags,
            extra: None,
        };
        if let Some(extra) = value.extra {
            for character in extra.zero_width {
                if !cell.push_zerowidth(character) {
                    return Err(CheckpointError::LimitExceeded("zero_width"));
                }
            }
            cell.set_underline_color(extra.underline_color.map(Into::into));
            cell.set_hyperlink(
                extra
                    .hyperlink
                    .map(|hyperlink| Hyperlink::new(Some(hyperlink.id), hyperlink.uri)),
            );
        }
        Ok(cell)
    }
}

impl From<Point> for PointV1 {
    fn from(point: Point) -> Self {
        Self {
            line: point.line.0,
            column: point.column.0 as u32,
        }
    }
}

impl From<PointV1> for Point {
    fn from(point: PointV1) -> Self {
        Self::new(Line(point.line), Column(point.column as usize))
    }
}

impl From<Rgb> for RgbV1 {
    fn from(rgb: Rgb) -> Self {
        Self {
            r: rgb.r,
            g: rgb.g,
            b: rgb.b,
        }
    }
}

impl From<RgbV1> for Rgb {
    fn from(rgb: RgbV1) -> Self {
        Self {
            r: rgb.r,
            g: rgb.g,
            b: rgb.b,
        }
    }
}

impl From<Color> for ColorCheckpointV1 {
    fn from(color: Color) -> Self {
        match color {
            Color::Named(color) => Self::Named(color.into()),
            Color::Spec(color) => Self::Spec(color.into()),
            Color::Indexed(index) => Self::Indexed(index),
        }
    }
}

impl From<ColorCheckpointV1> for Color {
    fn from(color: ColorCheckpointV1) -> Self {
        match color {
            ColorCheckpointV1::Named(color) => Self::Named(color.into()),
            ColorCheckpointV1::Spec(color) => Self::Spec(color.into()),
            ColorCheckpointV1::Indexed(index) => Self::Indexed(index),
        }
    }
}

impl From<CursorStyle> for CursorStyleV1 {
    fn from(style: CursorStyle) -> Self {
        Self {
            shape: style.shape.into(),
            blinking: style.blinking,
        }
    }
}

impl From<CursorStyleV1> for CursorStyle {
    fn from(style: CursorStyleV1) -> Self {
        Self {
            shape: style.shape.into(),
            blinking: style.blinking,
        }
    }
}

impl From<CursorShape> for CursorShapeV1 {
    fn from(shape: CursorShape) -> Self {
        match shape {
            CursorShape::Block => Self::Block,
            CursorShape::Underline => Self::Underline,
            CursorShape::Beam => Self::Beam,
            CursorShape::HollowBlock => Self::HollowBlock,
            CursorShape::Hidden => Self::Hidden,
        }
    }
}

impl From<CursorShapeV1> for CursorShape {
    fn from(shape: CursorShapeV1) -> Self {
        match shape {
            CursorShapeV1::Block => Self::Block,
            CursorShapeV1::Underline => Self::Underline,
            CursorShapeV1::Beam => Self::Beam,
            CursorShapeV1::HollowBlock => Self::HollowBlock,
            CursorShapeV1::Hidden => Self::Hidden,
        }
    }
}

impl From<StandardCharset> for StandardCharsetV1 {
    fn from(charset: StandardCharset) -> Self {
        match charset {
            StandardCharset::Ascii => Self::Ascii,
            StandardCharset::SpecialCharacterAndLineDrawing => Self::SpecialCharacterAndLineDrawing,
        }
    }
}

impl From<StandardCharsetV1> for StandardCharset {
    fn from(charset: StandardCharsetV1) -> Self {
        match charset {
            StandardCharsetV1::Ascii => Self::Ascii,
            StandardCharsetV1::SpecialCharacterAndLineDrawing => {
                Self::SpecialCharacterAndLineDrawing
            }
        }
    }
}

impl From<CharsetIndex> for CharsetIndexV1 {
    fn from(index: CharsetIndex) -> Self {
        match index {
            CharsetIndex::G0 => Self::G0,
            CharsetIndex::G1 => Self::G1,
            CharsetIndex::G2 => Self::G2,
            CharsetIndex::G3 => Self::G3,
        }
    }
}

impl From<CharsetIndexV1> for CharsetIndex {
    fn from(index: CharsetIndexV1) -> Self {
        match index {
            CharsetIndexV1::G0 => Self::G0,
            CharsetIndexV1::G1 => Self::G1,
            CharsetIndexV1::G2 => Self::G2,
            CharsetIndexV1::G3 => Self::G3,
        }
    }
}

impl From<KeyboardModes> for KeyboardModesV1 {
    fn from(mode: KeyboardModes) -> Self {
        Self { bits: mode.bits() }
    }
}

impl From<KeyboardModesV1> for KeyboardModes {
    fn from(mode: KeyboardModesV1) -> Self {
        Self::from_bits_retain(mode.bits)
    }
}

impl From<NamedColor> for NamedColorV1 {
    fn from(color: NamedColor) -> Self {
        match color {
            NamedColor::Black => Self::Black,
            NamedColor::Red => Self::Red,
            NamedColor::Green => Self::Green,
            NamedColor::Yellow => Self::Yellow,
            NamedColor::Blue => Self::Blue,
            NamedColor::Magenta => Self::Magenta,
            NamedColor::Cyan => Self::Cyan,
            NamedColor::White => Self::White,
            NamedColor::BrightBlack => Self::BrightBlack,
            NamedColor::BrightRed => Self::BrightRed,
            NamedColor::BrightGreen => Self::BrightGreen,
            NamedColor::BrightYellow => Self::BrightYellow,
            NamedColor::BrightBlue => Self::BrightBlue,
            NamedColor::BrightMagenta => Self::BrightMagenta,
            NamedColor::BrightCyan => Self::BrightCyan,
            NamedColor::BrightWhite => Self::BrightWhite,
            NamedColor::Foreground => Self::Foreground,
            NamedColor::Background => Self::Background,
            NamedColor::Cursor => Self::Cursor,
            NamedColor::DimBlack => Self::DimBlack,
            NamedColor::DimRed => Self::DimRed,
            NamedColor::DimGreen => Self::DimGreen,
            NamedColor::DimYellow => Self::DimYellow,
            NamedColor::DimBlue => Self::DimBlue,
            NamedColor::DimMagenta => Self::DimMagenta,
            NamedColor::DimCyan => Self::DimCyan,
            NamedColor::DimWhite => Self::DimWhite,
            NamedColor::BrightForeground => Self::BrightForeground,
            NamedColor::DimForeground => Self::DimForeground,
        }
    }
}

impl From<NamedColorV1> for NamedColor {
    fn from(color: NamedColorV1) -> Self {
        match color {
            NamedColorV1::Black => Self::Black,
            NamedColorV1::Red => Self::Red,
            NamedColorV1::Green => Self::Green,
            NamedColorV1::Yellow => Self::Yellow,
            NamedColorV1::Blue => Self::Blue,
            NamedColorV1::Magenta => Self::Magenta,
            NamedColorV1::Cyan => Self::Cyan,
            NamedColorV1::White => Self::White,
            NamedColorV1::BrightBlack => Self::BrightBlack,
            NamedColorV1::BrightRed => Self::BrightRed,
            NamedColorV1::BrightGreen => Self::BrightGreen,
            NamedColorV1::BrightYellow => Self::BrightYellow,
            NamedColorV1::BrightBlue => Self::BrightBlue,
            NamedColorV1::BrightMagenta => Self::BrightMagenta,
            NamedColorV1::BrightCyan => Self::BrightCyan,
            NamedColorV1::BrightWhite => Self::BrightWhite,
            NamedColorV1::Foreground => Self::Foreground,
            NamedColorV1::Background => Self::Background,
            NamedColorV1::Cursor => Self::Cursor,
            NamedColorV1::DimBlack => Self::DimBlack,
            NamedColorV1::DimRed => Self::DimRed,
            NamedColorV1::DimGreen => Self::DimGreen,
            NamedColorV1::DimYellow => Self::DimYellow,
            NamedColorV1::DimBlue => Self::DimBlue,
            NamedColorV1::DimMagenta => Self::DimMagenta,
            NamedColorV1::DimCyan => Self::DimCyan,
            NamedColorV1::DimWhite => Self::DimWhite,
            NamedColorV1::BrightForeground => Self::BrightForeground,
            NamedColorV1::DimForeground => Self::DimForeground,
        }
    }
}

impl ConfigV1 {
    fn from_config(config: &Config) -> Result<Self, CheckpointError> {
        if config.scrolling_history > u32::MAX as usize {
            return Err(CheckpointError::LimitExceeded("scrolling_history"));
        }
        Ok(Self {
            scrolling_history: config.scrolling_history as u32,
            default_cursor_style: config.default_cursor_style.into(),
            vi_mode_cursor_style: config.vi_mode_cursor_style.map(Into::into),
            semantic_escape_chars: config.semantic_escape_chars.clone(),
            kitty_keyboard: config.kitty_keyboard,
            osc52: config.osc52.into(),
        })
    }
}

impl TryFrom<&ConfigV1> for Config {
    type Error = CheckpointError;

    fn try_from(config: &ConfigV1) -> Result<Self, Self::Error> {
        Ok(Self {
            scrolling_history: config.scrolling_history as usize,
            default_cursor_style: config.default_cursor_style.into(),
            vi_mode_cursor_style: config.vi_mode_cursor_style.map(Into::into),
            semantic_escape_chars: config.semantic_escape_chars.clone(),
            kitty_keyboard: config.kitty_keyboard,
            osc52: config.osc52.into(),
        })
    }
}

impl From<Osc52> for Osc52V1 {
    fn from(mode: Osc52) -> Self {
        match mode {
            Osc52::Disabled => Self::Disabled,
            Osc52::OnlyCopy => Self::OnlyCopy,
            Osc52::OnlyPaste => Self::OnlyPaste,
            Osc52::CopyPaste => Self::CopyPaste,
        }
    }
}

impl From<Osc52V1> for Osc52 {
    fn from(mode: Osc52V1) -> Self {
        match mode {
            Osc52V1::Disabled => Self::Disabled,
            Osc52V1::OnlyCopy => Self::OnlyCopy,
            Osc52V1::OnlyPaste => Self::OnlyPaste,
            Osc52V1::CopyPaste => Self::CopyPaste,
        }
    }
}

fn bounded_u32(value: u32, limit: usize, field: &'static str) -> Result<usize, CheckpointError> {
    let value = value as usize;
    if value > limit {
        Err(CheckpointError::LimitExceeded(field))
    } else {
        Ok(value)
    }
}

fn validate_cursor(
    cursor: &CursorCheckpointV1,
    columns: usize,
    screen_lines: usize,
    limits: &CheckpointLimits,
) -> Result<(), CheckpointError> {
    if cursor.point.line < 0 || cursor.point.line as usize >= screen_lines {
        return Err(CheckpointError::Invalid("cursor point"));
    }
    if cursor.point.column as usize >= columns {
        return Err(CheckpointError::Invalid("cursor point"));
    }
    if cursor.input_needs_wrap && cursor.point.column as usize != columns - 1 {
        return Err(CheckpointError::Invalid("input_needs_wrap"));
    }
    for charset in cursor.charsets {
        validate_charset(charset)?;
    }
    validate_cell_attributes(&cursor.template, limits).map(|_| ())
}

fn validate_vi_point(
    point: PointV1,
    grid: &GridCheckpointV1,
    screen_lines: usize,
    field: &'static str,
) -> Result<(), CheckpointError> {
    let history_size = grid.rows.len().saturating_sub(screen_lines);
    if point.line < -(history_size as i32) || point.line >= screen_lines as i32 {
        return Err(CheckpointError::Invalid(field));
    }
    if point.column as usize >= grid.columns as usize {
        return Err(CheckpointError::Invalid(field));
    }
    Ok(())
}

fn validate_cell(
    cell: &CellCheckpointV1,
    rows: &[RowCheckpointV1],
    row_index: usize,
    column: usize,
    limits: &CheckpointLimits,
) -> Result<(), CheckpointError> {
    let flags = validate_cell_attributes(cell, limits)?;

    if flags.contains(Flags::WRAPLINE) && column + 1 != rows[row_index].cells.len() {
        return Err(CheckpointError::Invalid("wrapline position"));
    }

    // Preserve the structural wide-cell relationships created by the model.
    if flags.contains(Flags::WIDE_CHAR) {
        if cell.c.width() != Some(2) {
            return Err(CheckpointError::Invalid("wide cell character"));
        }
        let next = rows
            .get(row_index)
            .and_then(|row| row.cells.get(column + 1))
            .ok_or(CheckpointError::Invalid("wide cell pair"))?;
        if !Flags::from_bits(next.flags)
            .is_some_and(|next_flags| next_flags.contains(Flags::WIDE_CHAR_SPACER))
        {
            return Err(CheckpointError::Invalid("wide cell pair"));
        }
        if next.c != ' ' {
            return Err(CheckpointError::Invalid("wide spacer character"));
        }
    }
    if flags.contains(Flags::WIDE_CHAR_SPACER) {
        if cell.c != ' ' {
            return Err(CheckpointError::Invalid("wide spacer character"));
        }
        if column == 0 {
            return Err(CheckpointError::Invalid("wide spacer pair"));
        }
        let previous = &rows[row_index].cells[column - 1];
        if !Flags::from_bits(previous.flags)
            .is_some_and(|previous_flags| previous_flags.contains(Flags::WIDE_CHAR))
        {
            return Err(CheckpointError::Invalid("wide spacer pair"));
        }
    }
    if flags.contains(Flags::LEADING_WIDE_CHAR_SPACER) {
        if cell.c != ' ' {
            return Err(CheckpointError::Invalid("leading wide spacer character"));
        }
        if column + 1 != rows[row_index].cells.len() {
            return Err(CheckpointError::Invalid("leading wide spacer"));
        }
        let next = rows
            .get(row_index + 1)
            .and_then(|row| row.cells.first())
            .ok_or(CheckpointError::Invalid("leading wide spacer"))?;
        if !Flags::from_bits(next.flags)
            .is_some_and(|next_flags| next_flags.contains(Flags::WIDE_CHAR))
        {
            return Err(CheckpointError::Invalid("leading wide spacer"));
        }
    }
    Ok(())
}

fn validate_cell_attributes(
    cell: &CellCheckpointV1,
    limits: &CheckpointLimits,
) -> Result<Flags, CheckpointError> {
    let flags = Flags::from_bits(cell.flags).ok_or(CheckpointError::Invalid("cell flags"))?;
    if flags.contains(Flags::WIDE_CHAR)
        && flags.intersects(Flags::WIDE_CHAR_SPACER | Flags::LEADING_WIDE_CHAR_SPACER)
    {
        return Err(CheckpointError::Invalid("wide cell flags"));
    }
    if flags.contains(Flags::WIDE_CHAR_SPACER) && flags.contains(Flags::LEADING_WIDE_CHAR_SPACER) {
        return Err(CheckpointError::Invalid("wide cell flags"));
    }
    if let Some(extra) = &cell.extra {
        if extra.zero_width.len() > limits.max_zero_width_chars {
            return Err(CheckpointError::LimitExceeded("zero_width"));
        }
        if extra
            .zero_width
            .iter()
            .any(|character| character.width() != Some(0))
        {
            return Err(CheckpointError::Invalid("zero_width character"));
        }
        if let Some(color) = extra.underline_color {
            validate_color(color)?;
        }
        if let Some(link) = &extra.hyperlink {
            if link.id.len() > limits.max_hyperlink_length {
                return Err(CheckpointError::LimitExceeded("hyperlink id"));
            }
            if link.uri.len() > limits.max_hyperlink_length {
                return Err(CheckpointError::LimitExceeded("hyperlink uri"));
            }
        }
    }
    validate_color(cell.fg)?;
    validate_color(cell.bg)?;
    Ok(flags)
}

fn validate_color(color: ColorCheckpointV1) -> Result<(), CheckpointError> {
    if let ColorCheckpointV1::Named(_) = color {
        // The enum itself is closed; retaining this function keeps validation
        // explicit at every color-bearing field.
    }
    Ok(())
}

fn validate_colors(colors: &[Option<RgbV1>]) -> Result<(), CheckpointError> {
    if colors.len() != COLOR_COUNT {
        return Err(CheckpointError::Invalid("colors"));
    }
    for color in colors.iter().flatten().copied() {
        validate_color(ColorCheckpointV1::Spec(color))?;
    }
    Ok(())
}

fn colors_from_checkpoint(colors: &[Option<RgbV1>]) -> Result<Colors, CheckpointError> {
    validate_colors(colors)?;
    let values = colors
        .iter()
        .copied()
        .map(|color| color.map(Into::into))
        .collect::<Vec<_>>();
    let values: [Option<Rgb>; COLOR_COUNT] = values
        .try_into()
        .map_err(|_| CheckpointError::Invalid("colors"))?;
    Ok(Colors::from_checkpoint_values(values))
}

fn validate_charset(charset: StandardCharsetV1) -> Result<(), CheckpointError> {
    match charset {
        StandardCharsetV1::Ascii | StandardCharsetV1::SpecialCharacterAndLineDrawing => Ok(()),
    }
}

fn validate_charset_index(index: CharsetIndexV1) -> Result<(), CheckpointError> {
    match index {
        CharsetIndexV1::G0 | CharsetIndexV1::G1 | CharsetIndexV1::G2 | CharsetIndexV1::G3 => Ok(()),
    }
}

fn validate_cursor_style(style: CursorStyleV1) -> Result<(), CheckpointError> {
    match style.shape {
        CursorShapeV1::Block
        | CursorShapeV1::Underline
        | CursorShapeV1::Beam
        | CursorShapeV1::HollowBlock
        | CursorShapeV1::Hidden => Ok(()),
    }
}

fn validate_osc52(mode: Osc52V1) -> Result<(), CheckpointError> {
    match mode {
        Osc52V1::Disabled | Osc52V1::OnlyCopy | Osc52V1::OnlyPaste | Osc52V1::CopyPaste => Ok(()),
    }
}

fn validate_keyboard_stack(
    stack: &[KeyboardModesV1],
    max_depth: usize,
) -> Result<(), CheckpointError> {
    if stack.len() > max_depth {
        return Err(CheckpointError::LimitExceeded("keyboard mode stack"));
    }
    for mode in stack {
        if KeyboardModes::from_bits(mode.bits).is_none() {
            return Err(CheckpointError::Invalid("keyboard mode bits"));
        }
    }
    Ok(())
}

fn validate_title(title: Option<&str>, limits: &CheckpointLimits) -> Result<(), CheckpointError> {
    if title.is_some_and(|title| title.len() > limits.max_title_length) {
        Err(CheckpointError::LimitExceeded("title"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::event::{Event, EventListener, VoidListener};
    use crate::term::test::TermSize;
    use crate::vte::ansi::Processor;

    #[derive(Clone)]
    struct CountingListener(Arc<AtomicUsize>);

    impl EventListener for CountingListener {
        fn send_event(&self, _event: Event) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn feed<T: EventListener>(term: &mut Term<T>, bytes: &[u8]) {
        let mut processor = Processor::<crate::vte::ansi::StdSyncHandler>::new();
        processor.advance(term, bytes);
    }

    #[test]
    fn round_trip_preserves_model_state_and_continuation() {
        let events = Arc::new(AtomicUsize::new(0));
        let listener = CountingListener(events.clone());
        let config = Config {
            scrolling_history: 32,
            default_cursor_style: CursorStyle {
                shape: CursorShape::Beam,
                blinking: true,
            },
            vi_mode_cursor_style: Some(CursorStyle {
                shape: CursorShape::Underline,
                blinking: false,
            }),
            semantic_escape_chars: "|:()".to_owned(),
            kitty_keyboard: true,
            osc52: Osc52::Disabled,
        };
        let size = TermSize::new(8, 3);
        let mut original = Term::new(config, &size, listener.clone());

        feed(&mut original, b"one\r\ntwo\r\nthree\r\nfour\r\n");
        feed(
            &mut original,
            b"\x1b[31;48;2;1;2;3m\x1b]4;1;rgb:12/34/56\x07\x1b]8;id=link;https://example.test\x07\
               \x1b[22t\x1b]2;checkpoint-title\x07\x1b[>1u\x1b[?25l\x1b[3g\x1bH\x1b)0\x0e",
        );
        feed(&mut original, "界\u{301}\x1b[0m\x1b]8;;\x07".as_bytes());

        // Keep both primary and alternate grids live at the checkpoint.
        feed(&mut original, b"\x1b[?1049hALT\x1b[?2004h");
        let checkpoint = original.checkpoint();
        assert_eq!(checkpoint.active_grid.max_scroll_limit, 0);
        assert!(checkpoint
            .active_grid
            .rows
            .iter()
            .chain(checkpoint.inactive_grid.rows.iter())
            .any(|row| {
                row.cells
                    .iter()
                    .any(|cell| cell.flags & Flags::WIDE_CHAR.bits() != 0)
            }));
        assert!(checkpoint
            .active_grid
            .rows
            .iter()
            .chain(checkpoint.inactive_grid.rows.iter())
            .flat_map(|row| row.cells.iter())
            .any(|cell| cell
                .extra
                .as_ref()
                .is_some_and(|extra| extra.hyperlink.is_some())));
        assert_eq!(checkpoint.tabs.len(), 8);
        assert_eq!(checkpoint.mode_bits & TermMode::SHOW_CURSOR.bits(), 0);
        assert_eq!(checkpoint.title_stack.len(), 1);
        assert_eq!(checkpoint.inactive_keyboard_mode_stack.len(), 1);
        assert_eq!(checkpoint.active_charset, CharsetIndexV1::G1);
        assert_eq!(
            checkpoint.colors[1],
            Some(RgbV1 {
                r: 0x12,
                g: 0x34,
                b: 0x56
            })
        );
        checkpoint.validate().unwrap();

        let events_before_restore = events.load(Ordering::Relaxed);
        let mut restored =
            Term::from_checkpoint(checkpoint.clone(), listener.clone()).expect("valid restore");
        assert_eq!(events.load(Ordering::Relaxed), events_before_restore);
        assert_eq!(restored.checkpoint(), checkpoint);

        // A suffix applied to the original and restored model must produce the
        // same model state, including the primary grid recovered by ALT exit.
        let suffix = b"\x1b[?1049l\x1b[4hresize-after\r\n";
        feed(&mut original, suffix);
        feed(&mut restored, suffix);
        assert_eq!(restored.checkpoint(), original.checkpoint());

        // Resizing after restore must preserve the same reflow behavior.
        original.resize(TermSize::new(10, 4));
        restored.resize(TermSize::new(10, 4));
        assert_eq!(restored.checkpoint(), original.checkpoint());

        // The model-line export is an owned snapshot and follows the viewport
        // display offset rather than the storage ring order.
        assert_eq!(restored.visible_checkpoint_rows().len(), 4);
    }

    #[test]
    fn alternate_screen_checkpoint_validates_primary_vi_history_cursor() {
        let config = Config {
            scrolling_history: 32,
            ..Config::default()
        };
        let size = TermSize::new(8, 3);
        let mut term = Term::new(config, &size, VoidListener);

        feed(
            &mut term,
            b"one\r\ntwo\r\nthree\r\nfour\r\nfive\r\nsix\r\nseven\r\neight\r\n",
        );
        term.scroll_display(crate::grid::Scroll::Top);
        assert!(term.vi_mode_cursor.point.line < Line(0));
        let primary_vi_point = term.vi_mode_cursor.point;

        // The vi cursor remains a point in the primary grid while the
        // alternate screen is active.  The alternate grid has no history.
        feed(&mut term, b"\x1b[?1049h\x1b[?2004h");
        let checkpoint = term
            .try_checkpoint()
            .expect("alternate-screen checkpoint should preserve primary vi history");
        assert_eq!(checkpoint.vi_mode_cursor, primary_vi_point.into());

        let restored = Term::from_checkpoint(checkpoint, VoidListener)
            .expect("alternate-screen checkpoint should restore");
        assert_eq!(restored.vi_mode_cursor.point, primary_vi_point);
    }

    #[test]
    fn invalid_checkpoint_is_rejected_before_restore() {
        let size = TermSize::new(6, 2);
        let mut term = Term::new(Config::default(), &size, VoidListener);
        feed(&mut term, b"stable");
        let before = term.checkpoint();

        let mut invalid = before.clone();
        invalid.mode_bits |= 1 << 31;
        assert_eq!(
            Term::from_checkpoint(invalid, VoidListener).err().unwrap(),
            CheckpointError::Invalid("mode_bits")
        );
        assert_eq!(term.checkpoint(), before);

        let mut invalid_rows = before.clone();
        invalid_rows.active_grid.rows[0].cells.pop();
        assert_eq!(
            invalid_rows.validate().unwrap_err(),
            CheckpointError::Invalid("row cell count")
        );

        let mut invalid_active_limit = before.clone();
        invalid_active_limit.active_grid.max_scroll_limit = 0;
        assert_eq!(
            invalid_active_limit.validate().unwrap_err(),
            CheckpointError::Invalid("active_grid.max_scroll_limit")
        );

        let mut invalid_inactive_limit = before.clone();
        invalid_inactive_limit.inactive_grid.max_scroll_limit = 1;
        assert_eq!(
            invalid_inactive_limit.validate().unwrap_err(),
            CheckpointError::Invalid("inactive_grid.max_scroll_limit")
        );

        let mut invalid_alt_limit = before.clone();
        invalid_alt_limit.mode_bits |= TermMode::ALT_SCREEN.bits();
        assert_eq!(
            invalid_alt_limit.validate().unwrap_err(),
            CheckpointError::Invalid("active_grid.max_scroll_limit")
        );

        let mut invalid_offset = before.clone();
        invalid_offset.active_grid.display_offset = 1;
        assert_eq!(
            invalid_offset.validate().unwrap_err(),
            CheckpointError::Invalid("display_offset")
        );

        let mut invalid_zero_width = before.clone();
        invalid_zero_width.active_grid.rows[0].cells[0].extra = Some(CellExtraCheckpointV1 {
            zero_width: vec!['x'],
            underline_color: None,
            hyperlink: None,
        });
        assert_eq!(
            invalid_zero_width.validate().unwrap_err(),
            CheckpointError::Invalid("zero_width character")
        );
    }

    #[test]
    fn checkpoint_rejects_live_zero_width_overflow() {
        let size = TermSize::new(8, 3);
        let mut term = Term::new(Config::default(), &size, VoidListener);
        let mut input = String::from("A");
        input.extend(std::iter::repeat_n('\u{301}', MAX_ZERO_WIDTH_CHARS + 1));
        feed(&mut term, input.as_bytes());

        assert!(term.zero_width_overflowed());
        assert_eq!(
            term.try_checkpoint().unwrap_err(),
            CheckpointError::LimitExceeded("zero_width")
        );

        // A trusted checkpoint can still restore a fresh model and clears the
        // marker by construction; an overflowed live model cannot create one.
        let fresh = Term::new(Config::default(), &size, VoidListener);
        let valid = fresh.try_checkpoint().expect("fresh checkpoint");
        let restored = Term::from_checkpoint(valid, VoidListener).expect("fresh restore");
        assert!(!restored.zero_width_overflowed());
    }

    #[test]
    #[cfg(feature = "serde")]
    fn serialized_checkpoint_round_trip_is_bounded() {
        let size = TermSize::new(4, 2);
        let mut term = Term::new(Config::default(), &size, VoidListener);
        feed(&mut term, b"serde\r\ncheckpoint");
        let checkpoint = term.checkpoint();
        let encoded = serde_json::to_vec(&checkpoint).unwrap();
        CheckpointLimits::default()
            .validate_serialized_len(encoded.len())
            .unwrap();
        let decoded: TermCheckpointV1 = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    #[cfg(feature = "serde")]
    fn serde_rejects_checkpoint_fields_before_semantic_validation() {
        let size = TermSize::new(4, 2);
        let mut term = Term::new(Config::default(), &size, VoidListener);
        feed(&mut term, b"bounded");
        let checkpoint = term.checkpoint();
        let encoded = serde_json::to_value(&checkpoint).unwrap();

        fn rejected(payload: serde_json::Value, field: &str) {
            let encoded = serde_json::to_vec(&payload).unwrap();
            let error = serde_json::from_slice::<TermCheckpointV1>(&encoded)
                .expect_err(field)
                .to_string();
            assert!(
                error.contains("limit exceeded"),
                "{field} should be rejected by its bounded visitor: {error}"
            );
        }

        let mut schema = encoded.clone();
        schema["schema"] =
            serde_json::Value::String("x".repeat(CHECKPOINT_MAX_IDENTITY_STRING_LENGTH + 1));
        rejected(schema, "schema");

        let mut semantic_escape_chars = encoded.clone();
        semantic_escape_chars["config"]["semantic_escape_chars"] =
            serde_json::Value::String("x".repeat(CHECKPOINT_MAX_SEMANTIC_ESCAPE_CHARS + 1));
        rejected(semantic_escape_chars, "semantic_escape_chars");

        let mut title = encoded.clone();
        title["title"] = serde_json::Value::String("x".repeat(CHECKPOINT_MAX_TITLE_LENGTH + 1));
        rejected(title, "title");

        let mut title_stack = encoded.clone();
        title_stack["title_stack"] = serde_json::Value::Array(
            (0..=TITLE_STACK_MAX_DEPTH)
                .map(|_| serde_json::Value::Null)
                .collect(),
        );
        rejected(title_stack, "title_stack");

        let mut keyboard_stack = encoded.clone();
        keyboard_stack["keyboard_mode_stack"] = serde_json::Value::Array(
            (0..=KEYBOARD_MODE_STACK_MAX_DEPTH)
                .map(|_| serde_json::json!({ "bits": 0 }))
                .collect(),
        );
        rejected(keyboard_stack, "keyboard_mode_stack");

        let mut tabs = encoded.clone();
        tabs["tabs"] = serde_json::Value::Array(
            (0..=CHECKPOINT_MAX_COLUMNS)
                .map(|_| serde_json::Value::Bool(false))
                .collect(),
        );
        rejected(tabs, "tabs");

        let mut colors = encoded.clone();
        colors["colors"] =
            serde_json::Value::Array((0..=COLOR_COUNT).map(|_| serde_json::Value::Null).collect());
        rejected(colors, "colors");

        let mut zero_width = encoded.clone();
        zero_width["active_grid"]["rows"][0]["cells"][0]["extra"] = serde_json::json!({
            "zero_width": (0..=CHECKPOINT_MAX_ZERO_WIDTH_CHARS)
                .map(|_| serde_json::Value::String("\u{301}".to_owned()))
                .collect::<Vec<_>>(),
            "underline_color": null,
            "hyperlink": null,
        });
        rejected(zero_width, "zero_width");

        let mut hyperlink = encoded.clone();
        hyperlink["active_grid"]["rows"][0]["cells"][0]["extra"] = serde_json::json!({
            "zero_width": [],
            "underline_color": null,
            "hyperlink": {
                "id": "x".repeat(CHECKPOINT_MAX_HYPERLINK_LENGTH + 1),
                "uri": "",
            },
        });
        rejected(hyperlink, "hyperlink");

        let mut cells = encoded.clone();
        let cell = cells["active_grid"]["rows"][0]["cells"][0].clone();
        cells["active_grid"]["rows"][0]["cells"] =
            serde_json::Value::Array((0..=CHECKPOINT_MAX_COLUMNS).map(|_| cell.clone()).collect());
        rejected(cells, "cells");

        // The row helper has a deliberately high default bound because it
        // includes the configured history.  Exercise its generic visitor
        // directly with a small bound so its over-bound probe is covered
        // without constructing a multi-gigabyte checkpoint fixture.
        let mut deserializer = serde_json::Deserializer::from_str("[1, 2, 3]");
        let error = deserialize_bounded_vec::<_, u8>(&mut deserializer, 2, "rows")
            .expect_err("rows should be rejected by its bounded visitor")
            .to_string();
        assert!(error.contains("limit exceeded"));
    }
}
