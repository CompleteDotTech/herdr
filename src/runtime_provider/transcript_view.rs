//! Plain transcript presentation. All normalization and retention happens when
//! owner replies are applied; rendering only borrows the retained tail.

use ratatui::{buffer::Buffer, layout::Rect, style::Style};

use super::{ProviderConnection, ProviderLifecycle};
use crate::{app::state::Palette, terminal::TerminalState};

pub(crate) fn render(terminal: &TerminalState, buffer: &mut Buffer, area: Rect, palette: &Palette) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let observation = terminal.external_observation.as_ref();
    let connection = observation
        .map(|value| value.connection)
        .unwrap_or(ProviderConnection::Unknown);
    let title = if observation.is_some_and(|value| value.diagnostic.is_some()) {
        "Coven transcript / read failed; retained text may be stale"
    } else {
        match connection {
            ProviderConnection::Connecting => "Coven transcript / connecting",
            ProviderConnection::Connected => "Coven transcript / connected",
            ProviderConnection::Disconnected => {
                "Coven transcript / disconnected; retained text may be stale"
            }
            ProviderConnection::Unknown => "Coven transcript / unavailable",
        }
    };
    buffer.set_stringn(
        area.x,
        area.y,
        title,
        usize::from(area.width),
        Style::default().fg(palette.accent),
    );
    if area.height < 2 {
        return;
    }
    let footer_y = area.y.saturating_add(area.height - 1);
    let footer = if terminal.external_transcript.as_ref().is_some_and(|value| {
        value
            .source_dropped_output_bytes()
            .is_some_and(|bytes| bytes > 0)
    }) {
        "Provider dropped output / transcript is read-only"
    } else if terminal
        .external_transcript
        .as_ref()
        .is_some_and(|value| value.has_omissions())
    {
        "Some output omitted / transcript is read-only"
    } else {
        match observation
            .map(|value| value.lifecycle)
            .unwrap_or(ProviderLifecycle::Unknown)
        {
            ProviderLifecycle::Running | ProviderLifecycle::Starting => {
                "Running / transcript is read-only"
            }
            ProviderLifecycle::Idle => "Idle / transcript is read-only",
            ProviderLifecycle::Completed => "Completed / transcript is read-only",
            ProviderLifecycle::Failed => "Failed / transcript is read-only",
            ProviderLifecycle::Cancelled | ProviderLifecycle::Killed => {
                "Stopped / transcript is read-only"
            }
            ProviderLifecycle::Orphaned => "Orphaned / transcript is read-only",
            _ => "Lifecycle unknown / transcript is read-only",
        }
    };
    buffer.set_stringn(
        area.x,
        footer_y,
        footer,
        usize::from(area.width),
        Style::default().fg(palette.overlay0),
    );
    let rows = usize::from(area.height.saturating_sub(2));
    let Some(projection) = terminal.external_transcript.as_ref() else {
        if rows > 0 {
            buffer.set_stringn(
                area.x,
                area.y + 1,
                "Transcript output is unavailable from this provider.",
                usize::from(area.width),
                Style::default().fg(palette.text),
            );
        }
        return;
    };
    for (index, line) in projection.read_lines(rows).iter().enumerate() {
        buffer.set_stringn(
            area.x,
            area.y + 1 + index as u16,
            line.as_str(),
            usize::from(area.width),
            Style::default().fg(palette.text),
        );
    }
}
