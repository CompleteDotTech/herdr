#![cfg(feature = "alacritty-engine")]

use std::time::Duration;

use alacritty_terminal::term::TermMode;
use coven_terminal::{
    ApplyError, EffectKind, Geometry, QueryReplyPolicy, RawChunk, ReplayEvent, SessionCursor,
    SessionError, TerminalSession,
};

use coven_terminal::alacritty::AlacrittyEngine;
use coven_terminal::alacritty::MAX_LIVE_ZERO_WIDTH_CHARS;

fn session(policy: QueryReplyPolicy) -> TerminalSession<AlacrittyEngine> {
    TerminalSession::new(Geometry::new(20, 4), policy, 1024 * 1024, 256).unwrap()
}

#[test]
fn every_byte_boundary_preserves_term_and_processor_continuation() {
    let input = b"plain\r\n\x1b[31;48;2;1;2;3mcolors\x1b[2J\x1b[?1049hALT\x1b[?25l\xe7\x95\x8c\x1b]2;split-title\x07tail\x1b[?1049l";
    for split in 0..=input.len() {
        let mut original = session(QueryReplyPolicy::Quiet);
        if split != 0 {
            original
                .ingest(RawChunk::new(SessionCursor::START, input[..split].to_vec()))
                .unwrap();
        }

        let bytes = original.checkpoint_bytes().unwrap();
        let mut restored = session(QueryReplyPolicy::Quiet);
        restored
            .restore_checkpoint_bytes(&bytes, QueryReplyPolicy::Quiet)
            .unwrap();

        if split != input.len() {
            let cursor = original.cursor();
            let suffix = input[split..].to_vec();
            original
                .ingest(RawChunk::new(cursor, suffix.clone()))
                .unwrap();
            restored.ingest(RawChunk::new(cursor, suffix)).unwrap();
        }

        assert_eq!(
            restored.engine().term().checkpoint(),
            original.engine().term().checkpoint(),
            "model differs at byte split {split}"
        );
        assert_eq!(
            restored.engine().processor().checkpoint(),
            original.engine().processor().checkpoint(),
            "processor differs at byte split {split}"
        );
    }
}

#[test]
fn alternate_screen_modes_and_resize_are_checkpointed_as_model_state() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    let bytes = b"primary\x1b[?1049hALT\x1b[?25l\x1b[?2004h";
    owner
        .ingest(RawChunk::new(SessionCursor::START, bytes.to_vec()))
        .unwrap();
    let model = owner.engine().term().checkpoint();
    assert!(model.mode_bits & TermMode::ALT_SCREEN.bits() != 0);
    assert!(model.mode_bits & TermMode::SHOW_CURSOR.bits() == 0);
    assert!(model.mode_bits & TermMode::BRACKETED_PASTE.bits() != 0);
    assert!(model
        .inactive_grid
        .rows
        .iter()
        .any(|row| { row.cells.iter().any(|cell| cell.c == 'p') }));

    let cursor = owner.cursor();
    owner
        .resize(Geometry::with_cell_size(30, 6, 8, 16))
        .unwrap();
    assert_eq!(
        owner.cursor(),
        SessionCursor {
            sequence: cursor.sequence + 1,
            offset: cursor.offset
        },
        "resize is an ordered frame with no output bytes"
    );
    let resized = owner.engine().term().checkpoint();
    assert_eq!(resized.dimensions.columns, 30);
    assert_eq!(resized.dimensions.screen_lines, 6);
    assert_eq!(owner.geometry().cell_width, 8);
}

#[test]
fn owner_collects_queries_in_order_and_quiet_drops_only_reply_bytes() {
    let query = b"\x1b[5n\x1b[6n\x1b[c\x1b[?25$p\x07".to_vec();
    let mut owner = session(QueryReplyPolicy::Owner);
    let report = owner
        .ingest(RawChunk::new(SessionCursor::START, query.clone()))
        .unwrap();
    let replies: Vec<Vec<u8>> = report.replies().map(ToOwned::to_owned).collect();
    assert!(replies.iter().any(|reply| reply == b"\x1b[0n"));
    assert!(replies.iter().any(|reply| reply.starts_with(b"\x1b[")));
    assert!(report
        .effects
        .windows(2)
        .all(|pair| pair[0].order <= pair[1].order));

    let mut quiet = session(QueryReplyPolicy::Quiet);
    let quiet_report = quiet
        .ingest(RawChunk::new(SessionCursor::START, query))
        .unwrap();
    assert_eq!(quiet_report.replies().count(), 0);
    assert!(quiet_report
        .effects
        .iter()
        .all(|effect| { !matches!(effect.kind, EffectKind::Reply(_)) }));
}

#[test]
fn synchronized_color_query_uses_palette_at_query_point() {
    let mut owner = session(QueryReplyPolicy::Owner);
    let set_blue = b"\x1b]4;1;rgb:1111/2222/3333\x07".to_vec();
    owner
        .ingest(RawChunk::new(SessionCursor::START, set_blue))
        .unwrap();

    // The processor intentionally buffers synchronized output. The query is
    // before the red mutation, so the fork's ColorRequest snapshot must keep
    // the earlier blue value when the batch is eventually flushed.
    let cursor = owner.cursor();
    let synchronized = b"\x1b[?2026h\x1b]4;1;?\x07\x1b]4;1;rgb:aaaa/bbbb/cccc\x07\x1b[?2026l";
    let report = owner
        .ingest(RawChunk::new(cursor, synchronized.to_vec()))
        .unwrap();
    let replies: Vec<Vec<u8>> = report.replies().map(ToOwned::to_owned).collect();
    assert!(replies
        .iter()
        .any(|reply| { reply == b"\x1b]4;1;rgb:1111/2222/3333\x07" }));
    assert!(!replies
        .iter()
        .any(|reply| { reply == b"\x1b]4;1;rgb:aaaa/bbbb/cccc\x07" }));
}

#[test]
fn unset_color_query_is_suppressed_without_an_authoritative_display_palette() {
    let mut owner = session(QueryReplyPolicy::Owner);
    let report = owner
        .ingest(RawChunk::new(
            SessionCursor::START,
            b"\x1b]4;1;?\x07".to_vec(),
        ))
        .unwrap();

    assert_eq!(report.replies().count(), 0);
}

#[test]
fn clipboard_and_bell_events_never_escape_the_listener() {
    let bytes = b"\x07\x1b]52;c;SGVsbG8=\x07".to_vec();
    for policy in [QueryReplyPolicy::Owner, QueryReplyPolicy::Quiet] {
        let mut owner = session(policy);
        let report = owner
            .ingest(RawChunk::new(SessionCursor::START, bytes.clone()))
            .unwrap();
        assert!(report.effects.iter().any(|effect| {
            matches!(
                effect.kind,
                EffectKind::BellSuppressed | EffectKind::ClipboardSuppressed
            )
        }));
        assert!(report.replies().next().is_none());
    }
}

#[test]
fn synchronized_update_continuation_survives_checkpoint_without_ansi_replay() {
    let mut original = session(QueryReplyPolicy::Quiet);
    let prefix = b"\x1b[?2026hpartial";
    original
        .ingest(RawChunk::new(SessionCursor::START, prefix.to_vec()))
        .unwrap();
    let checkpoint = original.checkpoint_bytes().unwrap();
    assert!(
        original
            .engine()
            .processor()
            .checkpoint()
            .sync_timeout
            .pending
    );
    let mut restored = session(QueryReplyPolicy::Quiet);
    restored
        .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    assert!(
        restored
            .engine()
            .processor()
            .checkpoint()
            .sync_timeout
            .pending
    );

    let suffix = b"\x1b[?2026lvisible".to_vec();
    let cursor = original.cursor();
    original
        .ingest(RawChunk::new(cursor, suffix.clone()))
        .unwrap();
    restored.ingest(RawChunk::new(cursor, suffix)).unwrap();
    assert_eq!(
        restored.engine().term().checkpoint(),
        original.engine().term().checkpoint()
    );
}

#[test]
fn expired_synchronized_update_requires_and_replays_an_ordered_flush() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    owner
        .ingest(RawChunk::new(
            SessionCursor::START,
            b"\x1b[?2026hpartial".to_vec(),
        ))
        .unwrap();
    let checkpoint = owner.checkpoint_bytes().unwrap();
    let pending_cursor = owner.cursor();

    std::thread::sleep(Duration::from_millis(220));
    assert!(owner.needs_flush());
    let report = owner
        .flush_if_needed()
        .unwrap()
        .expect("an expired source timeout must emit an explicit owner event");
    assert_eq!(report.cursor.sequence, pending_cursor.sequence + 1);
    assert_eq!(owner.cursor(), report.cursor);
    assert!(!owner.needs_flush());

    let flush_cursor = pending_cursor;
    assert_eq!(
        owner.cursor(),
        SessionCursor {
            sequence: flush_cursor.sequence + 1,
            offset: flush_cursor.offset,
        }
    );
    assert!(!owner.engine().processor().checkpoint().sync_timeout.pending);

    let flush = owner.replay_from(flush_cursor).unwrap();
    assert!(matches!(flush.as_slice(), [frame] if matches!(frame.event, ReplayEvent::SyncFlush)));

    // Replay is authoritative: the restored timeout has not elapsed locally,
    // but applying the ordered event must still flush the buffered prefix.
    let mut restored = session(QueryReplyPolicy::Quiet);
    restored
        .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    restored.apply_frame(flush[0].clone()).unwrap();
    assert_eq!(
        restored.engine().term().checkpoint(),
        owner.engine().term().checkpoint()
    );
    assert_eq!(
        restored.engine().processor().checkpoint(),
        owner.engine().processor().checkpoint()
    );

    // A source owner admits the following frame after the explicit flush;
    // replaying that frame does not consult the consumer's wall clock.
    owner
        .ingest(RawChunk::new(owner.cursor(), b"next".to_vec()))
        .unwrap();
}

#[test]
fn delayed_replay_does_not_infer_a_local_timeout() {
    let mut source = session(QueryReplyPolicy::Quiet);
    source
        .ingest(RawChunk::new(
            SessionCursor::START,
            b"\x1b[?2026hpartial".to_vec(),
        ))
        .unwrap();
    let checkpoint = source.checkpoint_bytes().unwrap();
    let cursor = source.cursor();
    source
        .ingest(RawChunk::new(cursor, b"still-buffered".to_vec()))
        .unwrap();
    let frame = source
        .replay_from(cursor)
        .unwrap()
        .into_iter()
        .next()
        .expect("source output must be retained for replay");
    let expected_model = source.engine().term().checkpoint();
    let expected_processor = source.engine().processor().checkpoint();

    let mut delayed = session(QueryReplyPolicy::Quiet);
    delayed
        .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    std::thread::sleep(Duration::from_millis(220));
    delayed
        .apply_frame(frame)
        .expect("replay uses the ordered source event, not consumer wall clock");

    assert_eq!(delayed.engine().term().checkpoint(), expected_model);
    let delayed_processor = delayed.engine().processor().checkpoint();
    assert_eq!(
        delayed_processor.synchronized_bytes,
        expected_processor.synchronized_bytes
    );
    assert_eq!(
        delayed_processor.sync_timeout.pending,
        expected_processor.sync_timeout.pending
    );
}

#[test]
fn live_combining_cell_budget_poison_requires_checkpoint_replacement() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    let checkpoint = owner.checkpoint_bytes().unwrap();
    let mut bytes = b"A".to_vec();
    for _ in 0..=MAX_LIVE_ZERO_WIDTH_CHARS {
        bytes.extend_from_slice("\u{301}".as_bytes());
    }

    let error = owner
        .ingest(RawChunk::new(SessionCursor::START, bytes))
        .expect_err("a cell must not grow past the live combining budget");
    assert!(matches!(
        error,
        ApplyError::Engine(
            coven_terminal::alacritty::AlacrittyError::LiveStateLimitExceeded { .. }
        )
    ));
    assert!(owner.engine().is_poisoned());

    owner
        .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    assert!(!owner.engine().is_poisoned());
    owner
        .ingest(RawChunk::new(SessionCursor::START, b"recovered".to_vec()))
        .unwrap();
}

#[test]
fn repeated_bsu_flush_checks_the_previous_buffer_before_retaining_new_sync() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    let mut bytes = b"\x1b[?2026hA".to_vec();
    for _ in 0..=MAX_LIVE_ZERO_WIDTH_CHARS {
        bytes.extend_from_slice("\u{301}".as_bytes());
    }
    // The second BSU plus ESU flushes the first synchronized buffer but
    // retains a new synchronization buffer. Its bytes are ASCII and the new
    // buffer remains nonempty, so a conditional high-bit/empty-buffer check
    // would miss the overflow.
    bytes.extend_from_slice(b"\x1b[?2026h");
    bytes.extend_from_slice(b"\x1b[?2026l");

    let error = owner
        .ingest(RawChunk::new(SessionCursor::START, bytes))
        .expect_err("a repeated BSU must still enforce the live cell bound");
    assert!(matches!(
        error,
        ApplyError::Engine(
            coven_terminal::alacritty::AlacrittyError::LiveStateLimitExceeded { .. }
        )
    ));
    assert!(owner.engine().is_poisoned());
}

#[test]
fn actual_engine_replay_preserves_resize_between_output_frames_and_checkpoint_tail() {
    let mut original = session(QueryReplyPolicy::Quiet);
    original
        .ingest(RawChunk::new(SessionCursor::START, b"before\r\n".to_vec()))
        .unwrap();
    let checkpoint = original.checkpoint_bytes().unwrap();
    let resize_cursor = original.cursor();
    original.resize(Geometry::new(30, 6)).unwrap();
    let after_resize_cursor = original.cursor();
    original
        .ingest(RawChunk::new(after_resize_cursor, b"after".to_vec()))
        .unwrap();

    let replay = original.replay_from(SessionCursor::START).unwrap();
    assert_eq!(replay.len(), 3);
    assert!(matches!(
        &replay[0].event,
        ReplayEvent::Output(bytes) if bytes == b"before\r\n"
    ));
    assert!(matches!(
        &replay[1].event,
        ReplayEvent::Resize(geometry) if *geometry == Geometry::new(30, 6)
    ));
    assert_eq!(replay[1].cursor(), resize_cursor);
    assert_eq!(replay[1].end_cursor().unwrap(), after_resize_cursor);
    assert!(matches!(
        &replay[2].event,
        ReplayEvent::Output(bytes) if bytes == b"after"
    ));

    let mut restored = session(QueryReplyPolicy::Quiet);
    restored
        .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    restored.apply_frame(replay[1].clone()).unwrap();
    restored.apply_frame(replay[2].clone()).unwrap();
    assert_eq!(restored.geometry(), original.geometry());
    assert_eq!(restored.cursor(), original.cursor());
    assert_eq!(
        restored.engine().term().checkpoint(),
        original.engine().term().checkpoint()
    );
    assert_eq!(
        restored.engine().processor().checkpoint(),
        original.engine().processor().checkpoint()
    );
}

#[test]
fn oversized_unterminated_osc_poison_is_immediate_and_checkpoint_restore_recovers() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    let max = vte::MAX_CHECKPOINT_OSC_RAW;

    // Leave the parser two bytes below its bounded OSC payload limit. The
    // first two bytes of the next frame fill the limit; the third byte is the
    // discarded byte that makes the fork's sticky overflow flag observable.
    // The short ESC ] introducer keeps the first frame within the owner's
    // one-megabyte raw-chunk limit while still leaving the OSC unterminated.
    let mut prefix = Vec::with_capacity(max);
    prefix.extend_from_slice(b"\x1b]");
    prefix.resize(max, b"x"[0]);
    let checkpoint = {
        owner
            .ingest(RawChunk::new(SessionCursor::START, prefix))
            .unwrap();
        owner.checkpoint_bytes().unwrap()
    };
    let cursor = owner.cursor();

    let error = owner
        .ingest(RawChunk::new(cursor, b"xxx".to_vec()))
        .expect_err("the byte beyond the OSC limit must fail closed");
    assert!(matches!(
        error,
        ApplyError::Engine(coven_terminal::alacritty::AlacrittyError::ParserOverflow)
    ));
    assert!(owner.engine().is_poisoned());
    assert!(matches!(
        owner.ingest(RawChunk::new(cursor, b"ignored".to_vec())),
        Err(ApplyError::Engine(
            coven_terminal::alacritty::AlacrittyError::Unavailable
        ))
    ));
    assert!(matches!(
        owner.checkpoint_bytes(),
        Err(SessionError::Snapshot(
            coven_terminal::alacritty::AlacrittySnapshotError::Unavailable
        ))
    ));

    // Restore the known-good pre-overflow continuation. BEL terminates the
    // pending OSC without appending another payload byte, proving that the
    // parser/model pair is usable again after replacement.
    owner
        .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    assert!(!owner.engine().is_poisoned());
    owner
        .ingest(RawChunk::new(owner.cursor(), b"\x07ok".to_vec()))
        .unwrap();
    assert!(owner.checkpoint_bytes().is_ok());
}
