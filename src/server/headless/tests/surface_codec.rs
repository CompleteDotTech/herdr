use super::*;
use crate::protocol::surface::SurfaceCodec;

fn connect(
    server: &mut HeadlessServer,
    client_id: u64,
    surface_codec: SurfaceCodec,
) -> (
    std::sync::mpsc::Receiver<Vec<u8>>,
    std::sync::mpsc::Receiver<Vec<u8>>,
) {
    let (writer, control, render) = test_client_writer();
    server.handle_server_event(ServerEvent::ClientShellConnected {
        client_id,
        surface_codec,
        surface_cols: 80,
        surface_rows: 23,
        cell_width_px: 0,
        cell_height_px: 0,
        pixel_mouse: false,
        direct_graphics: false,
        endpoint_keybindings: false,
        mouse_capture: false,
        surface_active: true,
        writer,
    });
    (control, render)
}

fn receive(render: &std::sync::mpsc::Receiver<Vec<u8>>, codec: SurfaceCodec) -> ServerMessage {
    let bytes = render
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let message = read_server_message(bytes);
    if codec == SurfaceCodec::V2 {
        assert!(
            matches!(&message, ServerMessage::EndpointControl { kind, .. }
            if kind == protocol::surface::SURFACE_CODEC_V2)
        );
    }
    codec.decode(message, MAX_FRAME_SIZE).unwrap()
}

#[tokio::test]
async fn external_surface_preserves_widths_for_v2_and_retains_v1_fallback() {
    let mut server = test_headless_server();
    let terminal_id = crate::terminal::TerminalId::try_alloc_external().unwrap();
    let pane_id = crate::layout::PaneId::alloc();
    let session = Arc::new(
        crate::terminal::external::ExternalTerminalSession::new(
            coven_terminal::Geometry::new(80, 23),
            coven_terminal::QueryReplyPolicy::Quiet,
            1024 * 1024,
            256,
            herdr_external_terminal_integration_v1::ExternalTheme::default(),
        )
        .unwrap(),
    );
    session
        .ingest(coven_terminal::RawChunk::new(
            coven_terminal::SessionCursor::START,
            "❤\u{fe0f}B 界".as_bytes().to_vec(),
        ))
        .unwrap();
    server
        .app
        .terminal_runtimes
        .attach_external_session(terminal_id.clone(), session.clone())
        .unwrap();
    server.app.state.terminals.insert(
        terminal_id.clone(),
        crate::terminal::TerminalState::new(terminal_id.clone(), std::path::PathBuf::new()),
    );
    let workspace = crate::workspace::Workspace::from_existing_pane(
        None,
        Some("external-width".into()),
        std::path::PathBuf::new(),
        false,
        crate::workspace::MovedPane {
            pane_id,
            pane_state: crate::pane::PaneState::new(terminal_id.clone()),
        },
        server.app.event_tx.clone(),
        server.app.render_notify.clone(),
        server.app.render_dirty.clone(),
    );
    server.app.state.workspaces = vec![workspace];
    server.app.state.active = Some(0);
    server.app.state.selected = 0;
    server.app.state.mode = crate::app::Mode::Terminal;
    let (_old_control, old) = connect(&mut server, 81, SurfaceCodec::V1);
    let (_new_control, new) = connect(&mut server, 82, SurfaceCodec::V2);
    assert_eq!(server.clients[&81].surface_codec, SurfaceCodec::V1);
    assert_eq!(server.clients[&82].surface_codec, SurfaceCodec::V2);
    server.render_and_stream();
    let ServerMessage::PaneSurface(legacy) = receive(&old, SurfaceCodec::V1) else {
        panic!("surface expected")
    };
    let ServerMessage::PaneSurface(modern) = receive(&new, SurfaceCodec::V2) else {
        panic!("surface expected")
    };
    assert!(legacy.frame.cells.iter().all(|cell| cell.width == 0));
    assert!(modern.frame.cells.iter().any(|cell| cell.width != 0));
    assert_eq!(frame_text(&legacy.frame), frame_text(&modern.frame));
    assert!(frame_text(&modern.frame).contains('界'));
    let committed = server.clients[&82]
        .render_state
        .last_pane_surface()
        .unwrap();
    assert_eq!(modern.frame, committed.frame);
    assert!(
        server.app.terminal_runtimes.get(&terminal_id).is_none(),
        "external fixture must not allocate a native PTY"
    );
    server.app.terminal_runtimes.shutdown_external(&terminal_id);
    assert!(session.is_closed());
}

#[tokio::test]
async fn retained_surface_patch_uses_the_connections_negotiated_codec() {
    let mut server = test_headless_server();
    let pane_id = install_shared_view_test_runtime(&mut server);
    let (_control, render) = connect(&mut server, 83, SurfaceCodec::V2);
    server.render_and_stream();
    let ServerMessage::PaneSurface(initial) = receive(&render, SurfaceCodec::V2) else {
        panic!("surface expected")
    };
    write_shared_test_pane(&mut server, pane_id, b"\rPATCHED");
    assert!(server.render_retained_pane_surface_and_stream(&HashSet::from([pane_id])));
    let ServerMessage::PaneSurfacePatch(patch) = receive(&render, SurfaceCodec::V2) else {
        panic!("patch expected")
    };
    assert_eq!(patch.base_surface_revision, initial.surface_revision);
    assert!(patch
        .rows
        .iter()
        .flat_map(|row| &row.cells)
        .any(|cell| cell.symbol == "P"));
    shutdown_test_runtimes(&mut server);
}
