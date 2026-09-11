#![cfg(feature = "checkpoint")]

use std::fmt::Write;

use vte::checkpoint::{ParserCheckpointV1, ParserStateV1, MAX_CHECKPOINT_OSC_RAW};
use vte::{Params, Parser, Perform};

#[derive(Default, Debug, PartialEq, Eq)]
struct ParserEvents(Vec<String>);

impl Perform for ParserEvents {
    fn print(&mut self, c: char) {
        self.0.push(format!("print:{c}"));
    }

    fn execute(&mut self, byte: u8) {
        self.0.push(format!("execute:{byte:02x}"));
    }

    fn hook(&mut self, params: &Params, intermediates: &[u8], ignore: bool, action: char) {
        self.0.push(format!("hook:{params:?}:{intermediates:?}:{ignore}:{action}"));
    }

    fn put(&mut self, byte: u8) {
        self.0.push(format!("put:{byte:02x}"));
    }

    fn unhook(&mut self) {
        self.0.push("unhook".to_owned());
    }

    fn osc_dispatch(&mut self, params: &[&[u8]], bell_terminated: bool) {
        let mut event = format!("osc:{bell_terminated}:");
        for param in params {
            write!(event, "{param:?};").unwrap();
        }
        self.0.push(event);
    }

    fn csi_dispatch(&mut self, params: &Params, intermediates: &[u8], ignore: bool, action: char) {
        self.0.push(format!("csi:{params:?}:{intermediates:?}:{ignore}:{action}"));
    }

    fn esc_dispatch(&mut self, intermediates: &[u8], ignore: bool, byte: u8) {
        self.0.push(format!("esc:{intermediates:?}:{ignore}:{byte:02x}"));
    }
}

fn assert_parser_continuation(input: &[u8]) {
    for split in 0..=input.len() {
        let mut original = Parser::new();
        let mut prefix_events = ParserEvents::default();
        original.advance(&mut prefix_events, &input[..split]);

        let checkpoint = match original.try_checkpoint() {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                eprintln!(
                    "boundary {split} input {input:?}: {error}; checkpoint={:?}",
                    original.checkpoint()
                );
                panic!("checkpoint failed")
            },
        };
        let mut restored = Parser::from_checkpoint(&checkpoint).unwrap();

        let mut expected_events = ParserEvents::default();
        let mut restored_events = ParserEvents::default();
        original.advance(&mut expected_events, &input[split..]);
        restored.advance(&mut restored_events, &input[split..]);

        assert_eq!(
            expected_events, restored_events,
            "event mismatch at byte boundary {split} for {input:?}"
        );
        assert_eq!(
            original.try_checkpoint().unwrap(),
            restored.try_checkpoint().unwrap(),
            "checkpoint mismatch at byte boundary {split} for {input:?}"
        );
    }
}

#[test]
fn parser_round_trips_every_utf8_and_escape_boundary() {
    for input in [
        b"ascii \xe2\x98\x83 and \xf0\x9f\x8c\x8b".as_slice(),
        b"\x1b[38:2:255:1:2mwide \xe7\x95\x8c\x1b[0m".as_slice(),
        b"\x1b]52;c;payload\x1b\\after".as_slice(),
        b"\x1bP1;2+qdevice-control\x1b\\after".as_slice(),
        b"\x1b^apc-data\x1b\\after".as_slice(),
        b"\x1b_SOS-PM-data\x1b\\after".as_slice(),
        b"\x1b[?25h\x1b[2;5Hcursor".as_slice(),
        b"\x1b".as_slice(),
        b"\x1b(".as_slice(),
        b"\x1b[".as_slice(),
        b"\x1b[ ".as_slice(),
        b"\x1b[1".as_slice(),
        b"\x1b[1<".as_slice(),
        b"\x1b[? ".as_slice(),
        b"\x1b[   A".as_slice(),
        b"\x1bP".as_slice(),
        b"\x1bP ".as_slice(),
        b"\x1bP1".as_slice(),
        b"\x1bP1<".as_slice(),
        b"\x1bP? ".as_slice(),
        b"\x1bP   q".as_slice(),
        b"\x1bP1+qpayload".as_slice(),
        b"\x1b]unterminated".as_slice(),
        b"\x1b^unterminated".as_slice(),
    ] {
        assert_parser_continuation(input);
    }
}

#[test]
fn parser_checkpoint_contains_all_private_continuation_fields() {
    let mut parser = Parser::new();
    let mut events = ParserEvents::default();
    parser.advance(&mut events, b"\x1b[38:2:1");
    let checkpoint = parser.try_checkpoint().unwrap();

    assert_eq!(checkpoint.schema, ParserCheckpointV1::SCHEMA);
    assert_eq!(checkpoint.version, ParserCheckpointV1::VERSION);
    assert_eq!(checkpoint.state, ParserStateV1::CsiParam);
    assert_eq!(checkpoint.intermediate_idx, 0);
    assert_eq!(checkpoint.params_len, 2);
    assert_eq!(checkpoint.param, 1);
    assert_eq!(checkpoint.partial_utf8_len, 0);

    let restored = Parser::from_checkpoint(&checkpoint).unwrap();
    assert_eq!(restored.try_checkpoint().unwrap(), checkpoint);
}

#[test]
fn malformed_parser_checkpoints_are_rejected_before_restore() {
    let mut parser = Parser::new();
    let mut events = ParserEvents::default();
    parser.advance(&mut events, b"\x1b]52;c;data");
    let mut checkpoint = parser.try_checkpoint().unwrap();

    checkpoint.intermediate_idx = 3;
    assert!(Parser::from_checkpoint(&checkpoint).is_err());

    let mut checkpoint = parser.try_checkpoint().unwrap();
    checkpoint.partial_utf8 = [0xe2, 0x82, 0xac, 0];
    checkpoint.partial_utf8_len = 3;
    checkpoint.state = ParserStateV1::Ground;
    assert!(Parser::from_checkpoint(&checkpoint).is_err());

    let mut checkpoint = parser.try_checkpoint().unwrap();
    checkpoint.osc_num_params = 1;
    checkpoint.osc_params[0] = (1, 0);
    assert!(Parser::from_checkpoint(&checkpoint).is_err());

    let mut params_subparams = [0; 32];
    params_subparams[0] = 1;
    params_subparams[1] = 1;
    let checkpoint = ParserCheckpointV1 {
        params_len: 2,
        params_subparams,
        current_subparams: 2,
        ..ParserCheckpointV1::default()
    };
    assert!(Parser::from_checkpoint(&checkpoint).is_err());

    let checkpoint = ParserCheckpointV1 {
        state: ParserStateV1::EscapeIntermediate,
        intermediate_idx: 1,
        intermediates: [0, 0],
        ..ParserCheckpointV1::default()
    };
    assert!(Parser::from_checkpoint(&checkpoint).is_err());

    let checkpoint = ParserCheckpointV1 {
        state: ParserStateV1::EscapeIntermediate,
        intermediate_idx: 1,
        intermediates: [b'?', 0],
        ..ParserCheckpointV1::default()
    };
    assert!(Parser::from_checkpoint(&checkpoint).is_err());
}

#[test]
fn malformed_intermediate_indexes_never_panic_parser_or_processor() {
    for state in [ParserStateV1::CsiIntermediate, ParserStateV1::DcsIntermediate] {
        for intermediate_idx in 0..=2 {
            let checkpoint =
                ParserCheckpointV1 { state, intermediate_idx, ..ParserCheckpointV1::default() };
            let result = std::panic::catch_unwind(|| Parser::from_checkpoint(&checkpoint));
            assert!(result.is_ok(), "parser validation panicked for {state:?}/{intermediate_idx}");
            assert!(result.unwrap().is_err());

            #[cfg(feature = "ansi")]
            {
                let processor_checkpoint = vte::ansi::checkpoint::ProcessorCheckpointV1 {
                    parser: checkpoint,
                    ..vte::ansi::checkpoint::ProcessorCheckpointV1::default()
                };
                let result = std::panic::catch_unwind(|| {
                    vte::ansi::Processor::<vte::ansi::checkpoint::CheckpointSyncTimeout>::from_checkpoint(
                        &processor_checkpoint,
                    )
                });
                assert!(
                    result.is_ok(),
                    "processor validation panicked for {state:?}/{intermediate_idx}"
                );
                assert!(result.unwrap().is_err());
            }
        }
    }
}

#[test]
fn parser_accepts_reused_parameter_storage_for_csi_and_dcs() {
    let mut parser = Parser::new();
    let mut events = ParserEvents::default();

    parser.advance(&mut events, b"\x1b[1;2:3m");
    parser.advance(&mut events, b"\x1b[4:5:6");
    let csi_checkpoint = parser.try_checkpoint().unwrap();
    let mut restored = Parser::from_checkpoint(&csi_checkpoint).unwrap();

    let mut expected_events = ParserEvents::default();
    let mut restored_events = ParserEvents::default();
    parser.advance(&mut expected_events, b"m");
    restored.advance(&mut restored_events, b"m");
    assert_eq!(expected_events, restored_events);
    assert_eq!(parser.try_checkpoint().unwrap(), restored.try_checkpoint().unwrap());

    let mut parser = Parser::new();
    let mut expected_events = ParserEvents::default();
    let mut restored_events = ParserEvents::default();
    parser.advance(&mut events, b"\x1bP1;2:3+qpayload\x1b\\");
    parser.advance(&mut events, b"\x1bP4:5:6+q");
    let dcs_checkpoint = parser.try_checkpoint().unwrap();
    let mut restored = Parser::from_checkpoint(&dcs_checkpoint).unwrap();
    parser.advance(&mut expected_events, b"payload\x1b\\");
    restored.advance(&mut restored_events, b"payload\x1b\\");
    assert_eq!(expected_events, restored_events);
    assert_eq!(parser.try_checkpoint().unwrap(), restored.try_checkpoint().unwrap());
}

#[test]
fn oversized_osc_is_reported_and_not_partially_dispatched() {
    let mut parser = Parser::new();
    let mut events = ParserEvents::default();
    parser.advance(&mut events, b"\x1b]52;c;");

    parser.advance(&mut events, &vec![b'a'; MAX_CHECKPOINT_OSC_RAW - 3]);
    assert!(!parser.osc_overflowed());
    assert!(parser.try_checkpoint().is_ok());
    parser.advance(&mut events, b"a");
    assert!(parser.osc_overflowed());
    parser.advance(&mut events, b"\x07");

    assert!(!events.0.iter().any(|event| event.starts_with("osc:")));
    assert!(parser.try_checkpoint().is_err());
}

#[test]
fn private_marker_then_intermediate_is_checkpointable() {
    let mut parser = Parser::new();
    let mut events = ParserEvents::default();
    parser.advance(&mut events, b"\x1b[? ");
    let checkpoint = parser.try_checkpoint().unwrap();
    assert_eq!(checkpoint.state, ParserStateV1::CsiIntermediate);
    assert_eq!(checkpoint.intermediate_idx, 2);
    assert_eq!(checkpoint.intermediates, [b'?', b' ']);

    let mut parser = Parser::new();
    parser.advance(&mut events, b"\x1bP? ");
    let checkpoint = parser.try_checkpoint().unwrap();
    assert_eq!(checkpoint.state, ParserStateV1::DcsIntermediate);
    assert_eq!(checkpoint.intermediate_idx, 2);
    assert_eq!(checkpoint.intermediates, [b'?', b' ']);
}

#[test]
fn infallible_checkpoint_rejects_osc_overflow() {
    let mut parser = Parser::new();
    let mut events = ParserEvents::default();
    parser.advance(&mut events, b"\x1b]52;c;");
    parser.advance(&mut events, &vec![b'a'; MAX_CHECKPOINT_OSC_RAW + 1]);
    assert!(parser.osc_overflowed());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parser.checkpoint()));
    assert!(result.is_err());
}

#[test]
fn checkpoint_deserialization_bounds_variable_sequences() {
    let mut parser_value = serde_json::to_value(ParserCheckpointV1::default()).unwrap();
    parser_value["osc_raw"] =
        serde_json::Value::Array(vec![serde_json::Value::from(0); MAX_CHECKPOINT_OSC_RAW + 1]);
    let error = serde_json::from_value::<ParserCheckpointV1>(parser_value).unwrap_err();
    assert!(error.to_string().contains("limit"));
}

#[test]
fn parser_checkpoint_validates_every_byte_of_malformed_and_control_input() {
    // This is a deterministic input corpus rather than a claim of exhaustive
    // protocol fuzzing. It exercises arbitrary state transitions, C0/C1
    // controls, malformed UTF-8, and repeated escape introducers.
    let mut seed = 0x9e37_79b9_u32;
    for _case in 0..64 {
        let mut parser = Parser::new();
        let mut events = ParserEvents::default();
        for boundary in 0..256 {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let byte = match boundary % 19 {
                0 => 0x1b,
                1 => b'[',
                2 => b']',
                3 => b'P',
                _ => (seed >> 24) as u8,
            };
            parser.advance(&mut events, &[byte]);
            let checkpoint = parser.try_checkpoint().unwrap();
            let restored = Parser::from_checkpoint(&checkpoint).unwrap();
            assert_eq!(restored.try_checkpoint().unwrap(), checkpoint);
        }
    }
}

#[cfg(feature = "ansi")]
mod processor_tests {
    use std::thread;
    use std::time::Duration;

    use vte::ansi::checkpoint::{
        CheckpointSyncTimeout, ProcessorCheckpointV1, SyncTimeoutCheckpointV1,
    };
    use vte::ansi::{Handler, NamedPrivateMode, PrivateMode, Processor, StdSyncHandler, Timeout};
    use vte::MAX_CHECKPOINT_OSC_RAW;

    #[derive(Default, Debug, PartialEq, Eq)]
    struct HandlerEvents(Vec<String>);

    impl Handler for HandlerEvents {
        fn input(&mut self, c: char) {
            self.0.push(format!("input:{c}"));
        }

        fn set_private_mode(&mut self, mode: PrivateMode) {
            self.0.push(format!("set-private:{}", mode.raw()));
        }

        fn unset_private_mode(&mut self, mode: PrivateMode) {
            self.0.push(format!("unset-private:{}", mode.raw()));
        }
    }

    fn assert_processor_continuation(input: &[u8]) {
        for split in 0..=input.len() {
            let mut original = Processor::<CheckpointSyncTimeout>::new();
            let mut prefix_events = HandlerEvents::default();
            original.advance(&mut prefix_events, &input[..split]);
            let checkpoint = original.try_checkpoint().unwrap();
            let mut restored =
                Processor::<CheckpointSyncTimeout>::from_checkpoint(&checkpoint).unwrap();

            let mut expected_events = HandlerEvents::default();
            let mut restored_events = HandlerEvents::default();
            original.advance(&mut expected_events, &input[split..]);
            restored.advance(&mut restored_events, &input[split..]);

            assert_eq!(
                expected_events, restored_events,
                "processor event mismatch at boundary {split}"
            );
            let expected = original.try_checkpoint().unwrap();
            let actual = restored.try_checkpoint().unwrap();
            assert_eq!(expected.parser, actual.parser);
            assert_eq!(expected.preceding_char, actual.preceding_char);
            assert_eq!(expected.synchronized_bytes, actual.synchronized_bytes);
            assert_eq!(expected.sync_timeout.pending, actual.sync_timeout.pending);
        }
    }

    #[test]
    fn processor_round_trips_every_sync_buffer_boundary() {
        assert_processor_continuation(b"before\x1b[?2026hbuffer \xe2\x98\x83\x1b[?2026l after");
        assert_processor_continuation(b"A\x1b[2b");
    }

    #[test]
    fn processor_restores_relative_timeout_and_expiry() {
        let mut processor = Processor::<CheckpointSyncTimeout>::new();
        let mut before = HandlerEvents::default();
        processor.advance(&mut before, b"\x1b[?2026hheld");
        let mut checkpoint = processor.try_checkpoint().unwrap();
        assert!(checkpoint.sync_timeout.pending);
        assert!(checkpoint.sync_timeout.remaining_ms <= 150);
        assert!(checkpoint.sync_timeout.remaining_ms > 0);

        // A persisted deadline that has elapsed is represented explicitly by
        // pending=true and remaining_ms=0. Restore must preserve that fact so
        // the owner can flush it before accepting/rendering more input.
        checkpoint.sync_timeout = SyncTimeoutCheckpointV1 { pending: true, remaining_ms: 0 };
        let restored = Processor::<CheckpointSyncTimeout>::from_checkpoint(&checkpoint).unwrap();
        assert!(restored.sync_timeout_expired());

        // The normal timeout remains clock-relative after restore and does not
        // serialize platform Instant bytes. Give the first checkpoint a tiny
        // amount of time to expire, then verify the owner-facing predicate.
        let mut processor = Processor::<CheckpointSyncTimeout>::new();
        processor.advance(&mut before, b"\x1b[?2026hheld");
        let mut checkpoint = processor.try_checkpoint().unwrap();
        checkpoint.sync_timeout.remaining_ms = 1;
        let restored = Processor::<CheckpointSyncTimeout>::from_checkpoint(&checkpoint).unwrap();
        assert!(!restored.sync_timeout_expired());
        thread::sleep(Duration::from_millis(3));
        assert!(restored.sync_timeout_expired());
    }

    #[test]
    fn processor_checkpoint_requires_buffer_timeout_consistency() {
        let mut processor = Processor::<CheckpointSyncTimeout>::new();
        let mut events = HandlerEvents::default();
        processor.advance(&mut events, b"\x1b[?2026hheld");
        let mut checkpoint = processor.try_checkpoint().unwrap();
        checkpoint.sync_timeout = SyncTimeoutCheckpointV1::default();
        assert!(Processor::<CheckpointSyncTimeout>::from_checkpoint(&checkpoint).is_err());

        let mut checkpoint = processor.try_checkpoint().unwrap();
        checkpoint.synchronized_bytes = vec![0; ProcessorCheckpointV1::MAX_SYNC_BYTES + 1];
        assert!(Processor::<CheckpointSyncTimeout>::from_checkpoint(&checkpoint).is_err());

        // Keep the compile-time import of NamedPrivateMode useful as a check
        // that the synchronized mode remains the same protocol value.
        assert_eq!(NamedPrivateMode::SyncUpdate as u16, 2026);
    }

    #[test]
    fn supplied_timeout_must_match_checkpoint_pending_state() {
        let mut processor = Processor::<CheckpointSyncTimeout>::new();
        let mut events = HandlerEvents::default();
        processor.advance(&mut events, b"\x1b[?2026hheld");
        let pending = processor.try_checkpoint().unwrap();

        assert!(Processor::<CheckpointSyncTimeout>::from_checkpoint_with_timeout(
            &pending,
            CheckpointSyncTimeout::default(),
        )
        .is_err());

        let inactive = Processor::<CheckpointSyncTimeout>::new().try_checkpoint().unwrap();
        let mut active_timeout = CheckpointSyncTimeout::default();
        active_timeout.set_timeout(Duration::from_millis(1));
        assert!(Processor::<CheckpointSyncTimeout>::from_checkpoint_with_timeout(
            &inactive,
            active_timeout,
        )
        .is_err());
    }

    #[test]
    fn nested_timeout_checkpoint_rejects_unknown_fields() {
        let value = serde_json::json!({
            "pending": false,
            "remaining_ms": 0,
            "unexpected": true,
        });
        assert!(serde_json::from_value::<SyncTimeoutCheckpointV1>(value).is_err());
    }

    #[test]
    fn processor_checkpoint_deserialization_bounds_sync_sequence() {
        let mut value = serde_json::to_value(ProcessorCheckpointV1::default()).unwrap();
        value["synchronized_bytes"] = serde_json::Value::Array(vec![
            serde_json::Value::from(0);
            ProcessorCheckpointV1::MAX_SYNC_BYTES
                + 1
        ]);
        let error = serde_json::from_value::<ProcessorCheckpointV1>(value).unwrap_err();
        assert!(error.to_string().contains("limit"));
    }

    #[test]
    fn processor_checkpoint_rejects_osc_overflow() {
        let mut processor = Processor::<CheckpointSyncTimeout>::new();
        let mut events = HandlerEvents::default();
        processor.advance(&mut events, b"\x1b]52;c;");
        processor.advance(&mut events, &vec![b'a'; MAX_CHECKPOINT_OSC_RAW + 1]);
        assert!(processor.osc_overflowed());

        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| processor.checkpoint()));
        assert!(result.is_err());
    }

    #[test]
    fn default_processor_checkpoint_api_is_public() {
        let processor = Processor::<StdSyncHandler>::new();
        let checkpoint: ProcessorCheckpointV1 = processor.checkpoint();
        assert!(checkpoint.validate().is_ok());
        let _restored = Processor::<StdSyncHandler>::from_checkpoint(&checkpoint).unwrap();
    }

    #[test]
    fn stock_timeout_checkpoint_round_trips_pending_state() {
        let mut processor = Processor::<StdSyncHandler>::new();
        let mut events = HandlerEvents::default();
        processor.advance(&mut events, b"\x1b[?2026hheld");
        let checkpoint = processor.try_checkpoint().unwrap();
        assert!(checkpoint.sync_timeout.pending);
        let restored = Processor::<StdSyncHandler>::from_checkpoint(&checkpoint).unwrap();
        assert!(restored.checkpoint().sync_timeout.pending);
    }
}
