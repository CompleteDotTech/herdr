//! The owner-side adapter for a checkpoint-backed external terminal.
//!
//! This module only names the private integration crate's safe API. Provider
//! transport remains responsible for validating and ingesting raw chunks; the
//! UI and API call immutable snapshot/read methods below.

pub(crate) use herdr_external_terminal_integration_v1::{
    ExternalCellWidth, ExternalCursorPolicy, ExternalCursorShape, ExternalHyperlinkSnapshot,
    ExternalMouseMode, ExternalReadLine, ExternalRenderOptions, ExternalRenderOutput,
    ExternalScrollMetrics, ExternalSessionError, ExternalTerminalSession, ExternalTerminalSnapshot,
};

pub(crate) fn cursor_shape(shape: ExternalCursorShape) -> crate::protocol::CursorShapeParam {
    match shape {
        ExternalCursorShape::Block { blinking: true } => 1,
        ExternalCursorShape::Block { blinking: false } => 2,
        ExternalCursorShape::Underline { blinking: true } => 3,
        ExternalCursorShape::Underline { blinking: false } => 4,
        ExternalCursorShape::Bar { blinking: true } => 5,
        ExternalCursorShape::Bar { blinking: false } => 6,
        ExternalCursorShape::Hidden => 0,
    }
}

pub(crate) fn pane_scroll_metrics(metrics: ExternalScrollMetrics) -> crate::pane::ScrollMetrics {
    crate::pane::ScrollMetrics {
        offset_from_bottom: metrics.offset_from_bottom,
        max_offset_from_bottom: metrics.max_offset_from_bottom,
        viewport_rows: metrics.viewport_rows,
    }
}

pub(crate) fn external_hyperlinks(
    snapshot: &ExternalTerminalSnapshot,
    inner: ratatui::layout::Rect,
) -> Vec<((u16, u16), String, String)> {
    snapshot
        .visible_hyperlinks()
        .into_iter()
        .filter(|link| link.column < inner.width && link.row < inner.height)
        .map(|link: ExternalHyperlinkSnapshot| {
            (
                (
                    inner.x.saturating_add(link.column),
                    inner.y.saturating_add(link.row),
                ),
                link.symbol,
                link.uri,
            )
        })
        .collect()
}

pub(crate) fn external_theme(
    palette: &crate::app::state::Palette,
) -> herdr_external_terminal_integration_v1::ExternalTheme {
    herdr_external_terminal_integration_v1::ExternalTheme {
        foreground: palette.text,
        background: palette.panel_bg,
        cursor: palette.accent,
    }
}
