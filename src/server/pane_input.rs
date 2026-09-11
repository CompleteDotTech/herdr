use bytes::Bytes;
use crossterm::event::{KeyCode, KeyEventKind, KeyModifiers, MouseEventKind};

use crate::protocol::{AttachScrollDirection, AttachScrollSource, ClientPaneInputEvent};

pub(super) fn downgrade_ineligible_pixel_mouse(
    events: &mut [ClientPaneInputEvent],
    pixel_mouse: bool,
    runtime_size: (u16, u16),
    runtime_pixels: Option<(u32, u32)>,
) {
    let (runtime_rows, runtime_cols) = runtime_size;
    for event in events {
        let ClientPaneInputEvent::Mouse {
            position, geometry, ..
        } = event
        else {
            continue;
        };
        let crate::protocol::ClientMousePosition::Pixels { x, y, column, row } = *position else {
            continue;
        };
        let exact = pixel_mouse
            && geometry.is_some_and(|geometry| {
                (runtime_rows, runtime_cols) == (geometry.rows, geometry.cols)
                    && runtime_pixels == Some((geometry.width_px, geometry.height_px))
                    && column < geometry.cols
                    && row < geometry.rows
                    && x > 0
                    && y > 0
                    && x <= geometry.width_px
                    && y <= geometry.height_px
            });
        if !exact {
            *position = crate::protocol::ClientMousePosition::Cell { column, row };
            *geometry = None;
        }
    }
}

pub(super) fn terminal_attach_mouse_position(
    runtime: &crate::terminal::TerminalRuntime,
    terminal_size: (u16, u16),
    cell_size: crate::kitty_graphics::HostCellSize,
    pixel_mouse: bool,
    host_sgr_pixels_active: bool,
    position: crate::protocol::ClientMousePosition,
    geometry: Option<crate::protocol::ClientMouseGeometry>,
) -> Option<crate::protocol::ClientMousePosition> {
    let runtime_size = runtime.current_size();
    let cell_fallback = |column, row| {
        (column < runtime_size.1 && row < runtime_size.0)
            .then_some(crate::protocol::ClientMousePosition::Cell { column, row })
    };
    let (x, y, column, row) = match position {
        crate::protocol::ClientMousePosition::Cell { column, row } => {
            return cell_fallback(column, row);
        }
        crate::protocol::ClientMousePosition::Pixels { x, y, column, row } => (x, y, column, row),
    };
    let Some(geometry) = geometry else {
        return cell_fallback(column, row);
    };
    let host_geometry = crate::input::mouse::HostGeometry::new(
        geometry.cols,
        geometry.rows,
        geometry.width_px,
        geometry.height_px,
    )?;
    if host_geometry.cell(x, y) != Some((column, row)) {
        return None;
    }
    let exact = (|| {
        let average_width = (geometry.width_px / u32::from(geometry.cols)).max(1);
        let average_height = (geometry.height_px / u32::from(geometry.rows)).max(1);
        let (child_width_px, child_height_px) = runtime.pixel_size()?;
        if !pixel_mouse
            || !host_sgr_pixels_active
            || !runtime.sgr_pixel_mouse_enabled()
            || terminal_size != (geometry.cols, geometry.rows)
            || runtime_size != (geometry.rows, geometry.cols)
            || !cell_size.is_known()
            || average_width != cell_size.width_px
            || average_height != cell_size.height_px
        {
            return None;
        }
        let crate::input::mouse::Position::Pixels { x, y } = (crate::input::mouse::HostPixels {
            x,
            y,
            geometry: host_geometry,
        })
        .pane_position(
            ratatui::layout::Rect::new(0, 0, geometry.cols, geometry.rows),
            child_width_px,
            child_height_px,
        )?
        else {
            return None;
        };
        Some(crate::protocol::ClientMousePosition::Pixels { x, y, column, row })
    })();
    exact.or_else(|| cell_fallback(column, row))
}

pub(super) fn apply_terminal_attach_scroll(
    runtime: &crate::terminal::TerminalRuntime,
    source: AttachScrollSource,
    direction: AttachScrollDirection,
    lines: u16,
    column: Option<u16>,
    row: Option<u16>,
    modifiers: u8,
) -> Result<(), String> {
    apply_scroll(
        runtime,
        source,
        direction,
        lines,
        crate::input::mouse::Position::Cell {
            column: column.unwrap_or(0),
            row: row.unwrap_or(0),
        },
        modifiers,
    )
}

fn apply_scroll(
    runtime: &crate::terminal::TerminalRuntime,
    source: AttachScrollSource,
    direction: AttachScrollDirection,
    lines: u16,
    position: crate::input::mouse::Position,
    modifiers: u8,
) -> Result<(), String> {
    let wheel_kind = match direction {
        AttachScrollDirection::Up => MouseEventKind::ScrollUp,
        AttachScrollDirection::Down => MouseEventKind::ScrollDown,
    };
    if let AttachScrollSource::PageKey { input } = source {
        let host_scroll = runtime
            .plain_page_keys_use_host_scrollback()
            .unwrap_or(false);
        if host_scroll {
            match direction {
                AttachScrollDirection::Up => runtime.scroll_up(lines.max(1) as usize),
                AttachScrollDirection::Down => runtime.scroll_down(lines.max(1) as usize),
            }
            return Ok(());
        }
        return apply_terminal_attach_input(runtime, input);
    }

    match runtime.wheel_routing() {
        Some(crate::pane::WheelRouting::MouseReport) => {
            runtime.scroll_reset();
            let Some(bytes) = runtime.encode_mouse_wheel(
                wheel_kind,
                position,
                KeyModifiers::from_bits_truncate(modifiers),
            ) else {
                return Err(format!(
                    "failed to encode terminal attach mouse wheel event: {wheel_kind:?}"
                ));
            };
            runtime
                .try_send_bytes(Bytes::from(bytes))
                .map_err(|err| format!("terminal attach mouse wheel input failed: {err}"))?;
        }
        Some(crate::pane::WheelRouting::AlternateScroll) => {
            runtime.scroll_reset();
            let Some(bytes) = runtime.encode_alternate_scroll(wheel_kind) else {
                return Ok(());
            };
            runtime
                .try_send_bytes(Bytes::from(bytes))
                .map_err(|err| format!("terminal attach alternate scroll input failed: {err}"))?;
        }
        Some(crate::pane::WheelRouting::HostScroll) | None => match direction {
            AttachScrollDirection::Up => runtime.scroll_up(lines.max(1) as usize),
            AttachScrollDirection::Down => runtime.scroll_down(lines.max(1) as usize),
        },
    }
    Ok(())
}

pub(super) fn apply_terminal_attach_input(
    runtime: &crate::terminal::TerminalRuntime,
    data: Vec<u8>,
) -> Result<(), String> {
    runtime.scroll_reset();
    if let Some(text) = crate::raw_input::complete_text_bracketed_paste(&data) {
        runtime
            .try_send_paste(text.to_owned())
            .map_err(|err| format!("terminal attach paste failed: {err}"))
    } else {
        runtime
            .try_send_bytes(Bytes::from(data))
            .map_err(|err| format!("terminal attach input failed: {err}"))
    }
}

pub(super) fn apply_client_pane_input_events(
    runtime: &crate::terminal::TerminalRuntime,
    events: &[ClientPaneInputEvent],
) -> Result<(), String> {
    apply_client_terminal_input_events(runtime, events, true)
}

pub(super) fn apply_client_popup_input_events(
    runtime: &crate::terminal::TerminalRuntime,
    events: &[ClientPaneInputEvent],
) -> Result<(), String> {
    apply_client_terminal_input_events(runtime, events, false)
}

/// Encode pane events for a checkpoint-backed external terminal.
///
/// External models do not expose Herdr's native Ghostty keyboard protocol, so
/// the negotiated checkpoint mode is intentionally translated to the legacy
/// terminal byte contract.  Host scrollback is handled by the headless server
/// when mouse reporting is disabled; when the child requested mouse reporting,
/// wheel/button/motion events are encoded for the child instead. Unsupported
/// host-only events are ignored rather than being reinterpreted as terminal
/// bytes.
pub(super) fn encode_external_client_input(
    session: &crate::terminal::external::ExternalTerminalSession,
    events: &[ClientPaneInputEvent],
) -> Result<Vec<u8>, String> {
    let snapshot = session
        .render_snapshot()
        .map_err(|error| error.to_string())?;
    let modes = snapshot.modes;
    let columns = snapshot.columns;
    let rows = snapshot.screen_lines;
    let mouse_encoding = if modes.sgr_pixel_mouse {
        crate::input::MouseProtocolEncoding::SgrPixels
    } else {
        crate::input::MouseProtocolEncoding::Default
    };
    let mut bytes = Vec::new();
    for event in events {
        if let ClientPaneInputEvent::Mouse {
            kind,
            position,
            geometry,
            modifiers,
            ..
        } = event
        {
            let (column, row) = match position {
                crate::protocol::ClientMousePosition::Cell { column, row } => {
                    if *column >= columns || *row >= rows {
                        continue;
                    }
                    (*column, *row)
                }
                crate::protocol::ClientMousePosition::Pixels { x, y, column, row } => {
                    if *column >= columns || *row >= rows {
                        continue;
                    }
                    if geometry.is_some_and(|geometry| {
                        *x == 0
                            || *y == 0
                            || *x > geometry.width_px
                            || *y > geometry.height_px
                    }) {
                        continue;
                    }
                    if modes.sgr_pixel_mouse {
                        (
                            u16::try_from(*x).unwrap_or(u16::MAX),
                            u16::try_from(*y).unwrap_or(u16::MAX),
                        )
                    } else {
                        (*column, *row)
                    }
                }
            };
            let Some(encoded) = encode_external_mouse(
                kind.to_crossterm(),
                column,
                row,
                crossterm::event::KeyModifiers::from_bits_truncate(*modifiers),
                modes.mouse,
                mouse_encoding,
            ) else {
                continue;
            };
            bytes.extend(encoded);
            continue;
        }
        match event.to_raw_input_event() {
            crate::raw_input::RawInputEvent::Key(key) => {
                bytes.extend(crate::input::encode_terminal_key(
                    key,
                    crate::input::KeyboardProtocol::Legacy,
                ));
            }
            crate::raw_input::RawInputEvent::Text(text) => {
                // TextCommit is already the host's composed text. Unlike a
                // clipboard paste it must not acquire bracketed-paste framing.
                bytes.extend_from_slice(text.as_str().as_bytes());
            }
            crate::raw_input::RawInputEvent::Paste(text) => {
                append_external_text(&mut bytes, &text, modes.bracketed_paste);
            }
            crate::raw_input::RawInputEvent::Mouse(_) => unreachable!("mouse handled above"),
            crate::raw_input::RawInputEvent::OuterFocusGained
            | crate::raw_input::RawInputEvent::OuterFocusLost
            | crate::raw_input::RawInputEvent::HostDefaultColor { .. }
            | crate::raw_input::RawInputEvent::HostPaletteColors { .. }
            | crate::raw_input::RawInputEvent::HostColorSchemeChanged(_)
            | crate::raw_input::RawInputEvent::HostCellSizeReport { .. }
            | crate::raw_input::RawInputEvent::Unsupported => {}
        }
    }
    if bytes.len() > 64 * 1024 {
        return Err("external terminal input exceeded the bounded request size".to_owned());
    }
    Ok(bytes)
}

fn encode_external_mouse(
    kind: crossterm::event::MouseEventKind,
    column: u16,
    row: u16,
    modifiers: crossterm::event::KeyModifiers,
    mode: crate::terminal::external::ExternalMouseMode,
    encoding: crate::input::MouseProtocolEncoding,
) -> Option<Vec<u8>> {
    use crate::terminal::external::ExternalMouseMode;

    let allowed = match kind {
        crossterm::event::MouseEventKind::Down(_)
        | crossterm::event::MouseEventKind::Up(_) => !matches!(mode, ExternalMouseMode::None),
        crossterm::event::MouseEventKind::Drag(_) => {
            matches!(mode, ExternalMouseMode::CellMotion | ExternalMouseMode::AllMotion)
        }
        crossterm::event::MouseEventKind::Moved => matches!(mode, ExternalMouseMode::AllMotion),
        crossterm::event::MouseEventKind::ScrollUp
        | crossterm::event::MouseEventKind::ScrollDown
        | crossterm::event::MouseEventKind::ScrollLeft
        | crossterm::event::MouseEventKind::ScrollRight => !matches!(mode, ExternalMouseMode::None),
    };
    if !allowed {
        return None;
    }
    match kind {
        crossterm::event::MouseEventKind::Moved => {
            crate::input::encode_mouse_motion(kind, column, row, modifiers, encoding)
        }
        crossterm::event::MouseEventKind::ScrollUp
        | crossterm::event::MouseEventKind::ScrollDown
        | crossterm::event::MouseEventKind::ScrollLeft
        | crossterm::event::MouseEventKind::ScrollRight => {
            crate::input::encode_mouse_scroll(kind, column, row, modifiers, encoding)
        }
        _ => crate::input::encode_mouse_button(kind, column, row, modifiers, encoding),
    }
}

fn append_external_text(bytes: &mut Vec<u8>, text: &str, bracketed: bool) {
    if bracketed {
        bytes.extend_from_slice(b"\x1b[200~");
        bytes.extend_from_slice(text.as_bytes());
        bytes.extend_from_slice(b"\x1b[201~");
    } else {
        bytes.extend_from_slice(text.as_bytes());
    }
}

fn apply_client_terminal_input_events(
    runtime: &crate::terminal::TerminalRuntime,
    events: &[ClientPaneInputEvent],
    host_page_keys: bool,
) -> Result<(), String> {
    for event in events {
        if let ClientPaneInputEvent::Mouse {
            kind,
            position,
            modifiers,
            lines,
            ..
        } = event
        {
            let kind = kind.to_crossterm();
            let modifiers = KeyModifiers::from_bits_truncate(*modifiers);
            let position = match position {
                crate::protocol::ClientMousePosition::Cell { column, row } => {
                    crate::input::mouse::Position::Cell {
                        column: *column,
                        row: *row,
                    }
                }
                crate::protocol::ClientMousePosition::Pixels { x, y, column, row } => {
                    if runtime.sgr_pixel_mouse_enabled() {
                        crate::input::mouse::Position::Pixels { x: *x, y: *y }
                    } else {
                        crate::input::mouse::Position::Cell {
                            column: *column,
                            row: *row,
                        }
                    }
                }
            };
            let bytes = match kind {
                MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                    let direction = if kind == MouseEventKind::ScrollUp {
                        AttachScrollDirection::Up
                    } else {
                        AttachScrollDirection::Down
                    };
                    apply_scroll(
                        runtime,
                        AttachScrollSource::Wheel,
                        direction,
                        (*lines).max(1),
                        position,
                        modifiers.bits(),
                    )?;
                    continue;
                }
                MouseEventKind::ScrollLeft | MouseEventKind::ScrollRight => runtime
                    .encode_mouse_wheel(kind, position, modifiers)
                    .unwrap_or_default(),
                MouseEventKind::Down(_) | MouseEventKind::Up(_) | MouseEventKind::Drag(_) => {
                    runtime
                        .encode_mouse_button(kind, position, modifiers)
                        .unwrap_or_default()
                }
                MouseEventKind::Moved => runtime
                    .encode_mouse_motion(kind, position, modifiers)
                    .unwrap_or_default(),
            };
            if !bytes.is_empty() {
                if kind != MouseEventKind::Moved {
                    runtime.scroll_reset();
                }
                runtime
                    .try_send_bytes(Bytes::from(bytes))
                    .map_err(|err| format!("targeted pane mouse input failed: {err}"))?;
            }
            continue;
        }

        match event.to_raw_input_event() {
            crate::raw_input::RawInputEvent::Key(key) => {
                let key_event = key.as_key_event();
                if host_page_keys
                    && matches!(key_event.code, KeyCode::PageUp | KeyCode::PageDown)
                    && key_event.modifiers.is_empty()
                    && runtime.plain_page_keys_use_host_scrollback() == Some(true)
                {
                    match key_event.kind {
                        KeyEventKind::Release => continue,
                        KeyEventKind::Press | KeyEventKind::Repeat => {
                            let lines = runtime.current_size().0.max(1) as usize;
                            if key_event.code == KeyCode::PageUp {
                                runtime.scroll_up(lines);
                            } else {
                                runtime.scroll_down(lines);
                            }
                            continue;
                        }
                    }
                }

                runtime.scroll_reset();
                let bytes = runtime.encode_terminal_key(key);
                if !bytes.is_empty() {
                    runtime
                        .try_send_bytes(Bytes::from(bytes))
                        .map_err(|err| format!("targeted pane key input failed: {err}"))?;
                }
            }
            crate::raw_input::RawInputEvent::Text(text) => {
                runtime.scroll_reset();
                runtime
                    .try_send_bytes(Bytes::copy_from_slice(text.as_str().as_bytes()))
                    .map_err(|err| format!("targeted pane text input failed: {err}"))?;
            }
            crate::raw_input::RawInputEvent::Paste(text) => {
                runtime.scroll_reset();
                runtime
                    .try_send_paste(text)
                    .map_err(|err| format!("targeted pane paste failed: {err}"))?;
            }
            crate::raw_input::RawInputEvent::Mouse(_)
            | crate::raw_input::RawInputEvent::OuterFocusGained
            | crate::raw_input::RawInputEvent::OuterFocusLost
            | crate::raw_input::RawInputEvent::HostDefaultColor { .. }
            | crate::raw_input::RawInputEvent::HostPaletteColors { .. }
            | crate::raw_input::RawInputEvent::HostColorSchemeChanged(_)
            | crate::raw_input::RawInputEvent::HostCellSizeReport { .. }
            | crate::raw_input::RawInputEvent::Unsupported => {
                return Err("non-pane input reached targeted pane input".to_owned());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn external_test_session() -> crate::terminal::external::ExternalTerminalSession {
        crate::terminal::external::ExternalTerminalSession::new(
            coven_terminal::Geometry::new(20, 4),
            coven_terminal::QueryReplyPolicy::Quiet,
            1024 * 1024,
            256,
            herdr_external_terminal_integration_v1::ExternalTheme::default(),
        )
        .expect("external session")
    }

    #[test]
    fn external_input_uses_checkpoint_modes_and_legacy_bytes() {
        let session = external_test_session();
        session
            .ingest(coven_terminal::RawChunk::new(
                coven_terminal::SessionCursor::START,
                b"\x1b[?1000h\x1b[?1006h\x1b[?2004h".to_vec(),
            ))
            .expect("mode report");
        let key = crate::protocol::ClientPaneInputEvent::Key {
            code: crate::protocol::ClientKeyCode::Char('c'),
            modifiers: crossterm::event::KeyModifiers::CONTROL.bits(),
            kind: crate::protocol::ClientKeyKind::Press,
            repeat_count: 1,
            shifted_codepoint: None,
            generated_text: None,
            tracks_release: true,
            physical_key_id: None,
            windows_record: None,
        };
        let events = vec![
            crate::protocol::ClientPaneInputEvent::TextCommit("typed".into()),
            crate::protocol::ClientPaneInputEvent::Paste("clip".into()),
            key,
            crate::protocol::ClientPaneInputEvent::Mouse {
                kind: crate::protocol::ClientMouseKind::Down(
                    crate::protocol::ClientMouseButton::Left,
                ),
                position: crate::protocol::ClientMousePosition::Cell { column: 2, row: 1 },
                geometry: None,
                modifiers: 0,
                lines: 1,
            },
        ];
        assert_eq!(
            encode_external_client_input(&session, &events).expect("encoded input"),
            b"typed\x1b[200~clip\x1b[201~\x03\x1b[<0;3;2M"
        );
    }

    #[test]
    fn external_input_drops_motion_without_a_matching_mouse_mode() {
        let session = external_test_session();
        let event = crate::protocol::ClientPaneInputEvent::Mouse {
            kind: crate::protocol::ClientMouseKind::Moved,
            position: crate::protocol::ClientMousePosition::Cell { column: 2, row: 1 },
            geometry: None,
            modifiers: 0,
            lines: 1,
        };
        assert!(encode_external_client_input(&session, &[event])
            .expect("encoded input")
            .is_empty());
    }

    #[tokio::test]
    async fn terminal_attach_stale_geometry_falls_back_to_the_canonical_cell() {
        let runtime = crate::terminal::TerminalRuntime::test_with_screen_bytes(20, 5, b"");
        let position = crate::protocol::ClientMousePosition::Pixels {
            x: 121,
            y: 81,
            column: 12,
            row: 4,
        };

        assert_eq!(
            terminal_attach_mouse_position(
                &runtime,
                (20, 5),
                crate::kitty_graphics::HostCellSize {
                    width_px: 10,
                    height_px: 20,
                },
                true,
                false,
                position,
                Some(crate::protocol::ClientMouseGeometry {
                    cols: 20,
                    rows: 5,
                    width_px: 200,
                    height_px: 100,
                }),
            ),
            Some(crate::protocol::ClientMousePosition::Cell { column: 12, row: 4 })
        );
        assert_eq!(
            terminal_attach_mouse_position(
                &runtime,
                (20, 5),
                crate::kitty_graphics::HostCellSize {
                    width_px: 10,
                    height_px: 20,
                },
                true,
                false,
                crate::protocol::ClientMousePosition::Pixels {
                    x: 120,
                    y: 80,
                    column: 12,
                    row: 4,
                },
                Some(crate::protocol::ClientMouseGeometry {
                    cols: 20,
                    rows: 5,
                    width_px: 200,
                    height_px: 100,
                }),
            ),
            None
        );
        assert_eq!(
            terminal_attach_mouse_position(
                &runtime,
                (80, 24),
                crate::kitty_graphics::HostCellSize::default(),
                false,
                false,
                crate::protocol::ClientMousePosition::Cell { column: 12, row: 4 },
                None,
            ),
            Some(crate::protocol::ClientMousePosition::Cell { column: 12, row: 4 })
        );
    }

    #[test]
    fn ineligible_shell_pixel_mouse_uses_its_canonical_cell_position() {
        let mut events = vec![ClientPaneInputEvent::Mouse {
            kind: crate::protocol::ClientMouseKind::Down(crate::protocol::ClientMouseButton::Left),
            position: crate::protocol::ClientMousePosition::Pixels {
                x: 121,
                y: 81,
                column: 12,
                row: 4,
            },
            geometry: Some(crate::protocol::ClientMouseGeometry {
                cols: 20,
                rows: 5,
                width_px: 200,
                height_px: 100,
            }),
            modifiers: 0,
            lines: 1,
        }];

        downgrade_ineligible_pixel_mouse(&mut events, false, (5, 20), Some((200, 100)));

        assert!(matches!(
            events.as_slice(),
            [ClientPaneInputEvent::Mouse {
                position: crate::protocol::ClientMousePosition::Cell { column: 12, row: 4 },
                ..
            }]
        ));
    }

    #[test]
    fn eligible_shell_pixel_mouse_remains_exact() {
        let position = crate::protocol::ClientMousePosition::Pixels {
            x: 121,
            y: 81,
            column: 12,
            row: 4,
        };
        let mut events = vec![ClientPaneInputEvent::Mouse {
            kind: crate::protocol::ClientMouseKind::Moved,
            position,
            geometry: Some(crate::protocol::ClientMouseGeometry {
                cols: 20,
                rows: 5,
                width_px: 200,
                height_px: 100,
            }),
            modifiers: 0,
            lines: 1,
        }];

        downgrade_ineligible_pixel_mouse(&mut events, true, (5, 20), Some((200, 100)));

        assert!(matches!(
            events.as_slice(),
            [ClientPaneInputEvent::Mouse {
                position: current,
                ..
            }] if *current == position
        ));
    }

    #[test]
    fn stale_shell_pixel_geometry_downgrades_to_its_canonical_cell() {
        let mut events = vec![ClientPaneInputEvent::Mouse {
            kind: crate::protocol::ClientMouseKind::Moved,
            position: crate::protocol::ClientMousePosition::Pixels {
                x: 121,
                y: 81,
                column: 12,
                row: 4,
            },
            geometry: Some(crate::protocol::ClientMouseGeometry {
                cols: 20,
                rows: 5,
                width_px: 200,
                height_px: 100,
            }),
            modifiers: 0,
            lines: 1,
        }];

        downgrade_ineligible_pixel_mouse(&mut events, true, (6, 20), Some((200, 120)));

        assert!(matches!(
            events.as_slice(),
            [ClientPaneInputEvent::Mouse {
                position: crate::protocol::ClientMousePosition::Cell { column: 12, row: 4 },
                geometry: None,
                ..
            }]
        ));
    }
}
