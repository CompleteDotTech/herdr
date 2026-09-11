use std::{error::Error, fmt};

use coven_terminal::{
    EffectKind, EffectOrder, EngineFingerprint, Geometry, OrderedEffect, QueryReplyPolicy,
    RawChunk, ReplayError, ReplayEvent, SessionCursor, TerminalEngine, TerminalSession,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct TestModelCheckpoint {
    geometry: Geometry,
    bytes: Vec<u8>,
    calls: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct TestProcessorCheckpoint {
    continuation: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for TestError {}

#[derive(Clone, Debug)]
struct TestEngine {
    geometry: Geometry,
    bytes: Vec<u8>,
    continuation: Vec<u8>,
    calls: u64,
}

impl TestEngine {
    fn checkpoint(&self) -> TestModelCheckpoint {
        TestModelCheckpoint {
            geometry: self.geometry,
            bytes: self.bytes.clone(),
            calls: self.calls,
        }
    }
}

impl TerminalEngine for TestEngine {
    type ModelCheckpoint = TestModelCheckpoint;
    type ProcessorCheckpoint = TestProcessorCheckpoint;
    type Error = TestError;
    type SnapshotError = TestError;

    fn fingerprint() -> EngineFingerprint {
        EngineFingerprint {
            model_package: "test-model".to_owned(),
            model_version: "1".to_owned(),
            model_revision: "test".to_owned(),
            model_checkpoint_schema: "test-model-checkpoint".to_owned(),
            model_checkpoint_version: 1,
            parser_package: "test-parser".to_owned(),
            parser_version: "1".to_owned(),
            parser_revision: "test".to_owned(),
            processor_checkpoint_schema: "test-processor-checkpoint".to_owned(),
            processor_checkpoint_version: 1,
        }
    }

    fn new(geometry: Geometry, _policy: QueryReplyPolicy) -> Result<Self, Self::Error> {
        Ok(Self {
            geometry,
            bytes: Vec::new(),
            continuation: Vec::new(),
            calls: 0,
        })
    }

    fn apply_raw(&mut self, bytes: &[u8]) -> Result<Vec<OrderedEffect>, Self::Error> {
        self.calls += 1;
        self.bytes.extend_from_slice(bytes);
        self.continuation.clear();
        if bytes.ends_with(b"\x1b[") {
            self.continuation.extend_from_slice(b"\x1b[");
        }
        Ok(vec![
            OrderedEffect {
                order: EffectOrder(0),
                kind: EffectKind::Reply(b"query-reply".to_vec()),
            },
            OrderedEffect {
                order: EffectOrder(1),
                kind: EffectKind::Resize(coven_terminal::ResizeNotice {
                    order: EffectOrder(1),
                    geometry: self.geometry,
                }),
            },
        ])
    }

    fn resize(&mut self, geometry: Geometry) -> Result<Vec<OrderedEffect>, Self::Error> {
        self.geometry = geometry;
        Ok(Vec::new())
    }

    fn geometry(&self) -> Option<Geometry> {
        Some(self.geometry)
    }

    fn model_checkpoint(&self) -> Result<Self::ModelCheckpoint, Self::SnapshotError> {
        Ok(self.checkpoint())
    }

    fn processor_checkpoint(&self) -> Result<Self::ProcessorCheckpoint, Self::SnapshotError> {
        Ok(TestProcessorCheckpoint {
            continuation: self.continuation.clone(),
        })
    }

    fn from_checkpoint(
        geometry: Geometry,
        model: &Self::ModelCheckpoint,
        processor: &Self::ProcessorCheckpoint,
        _policy: QueryReplyPolicy,
    ) -> Result<Self, Self::SnapshotError> {
        if model.geometry != geometry {
            return Err(TestError("geometry mismatch"));
        }
        Ok(Self {
            geometry,
            bytes: model.bytes.clone(),
            continuation: processor.continuation.clone(),
            calls: model.calls,
        })
    }
}

fn session(policy: QueryReplyPolicy) -> TerminalSession<TestEngine> {
    TerminalSession::new(Geometry::new(20, 4), policy, 64, 64).unwrap()
}

#[test]
fn raw_bytes_are_applied_once_and_cursor_commits_after_success() {
    let mut owner = session(QueryReplyPolicy::Owner);
    let payload = b"prefix\x1b[31m\xffsuffix".to_vec();
    let report = owner
        .ingest(RawChunk::new(SessionCursor::START, payload.clone()))
        .unwrap();
    assert_eq!(report.revision, 2);
    assert_eq!(
        report.cursor,
        SessionCursor {
            sequence: 2,
            offset: payload.len() as u64
        }
    );
    assert_eq!(
        report.replies().collect::<Vec<_>>(),
        vec![b"query-reply".as_slice()]
    );
    assert_eq!(owner.engine().bytes, payload);
    assert_eq!(owner.engine().calls, 1);
}

#[test]
fn from_engine_rejects_geometry_that_does_not_match_the_live_engine() {
    let engine = TestEngine::new(Geometry::new(20, 4), QueryReplyPolicy::Quiet).unwrap();
    let error = TerminalSession::from_engine(
        engine,
        Geometry::new(30, 4),
        QueryReplyPolicy::Quiet,
        64,
        64,
    )
    .err()
    .expect("the owner must not admit a model under the wrong geometry");
    assert!(matches!(
        error,
        coven_terminal::SessionError::GeometryMismatch {
            expected: Geometry {
                columns: 30,
                rows: 4,
                ..
            },
            actual: Geometry {
                columns: 20,
                rows: 4,
                ..
            },
        }
    ));
}

#[test]
fn quiet_policy_drops_replies_but_keeps_model_event_order_metadata() {
    let mut quiet = session(QueryReplyPolicy::Quiet);
    let report = quiet
        .ingest(RawChunk::new(SessionCursor::START, b"query".to_vec()))
        .unwrap();
    assert_eq!(report.replies().count(), 0);
    assert_eq!(
        report.resizes().collect::<Vec<_>>(),
        vec![coven_terminal::ResizeNotice {
            order: EffectOrder(1),
            geometry: Geometry::new(20, 4),
        }]
    );
}

#[test]
fn duplicate_frame_is_rejected_as_retransmission_without_a_second_engine_call() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    let first = RawChunk::new(SessionCursor::START, b"once".to_vec());
    owner.ingest(first.clone()).unwrap();
    let cursor = owner.cursor();
    let calls = owner.engine().calls;

    let error = owner
        .ingest(first)
        .expect_err("committed input is never replayed");
    assert!(matches!(
        error,
        coven_terminal::ApplyError::Retransmission { .. }
    ));
    assert_eq!(owner.engine().calls, calls);
    assert_eq!(owner.cursor(), cursor);
}

#[test]
fn sync_flush_is_an_ordered_zero_byte_event() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    let report = owner.flush().unwrap();
    assert_eq!(
        report.cursor,
        SessionCursor {
            sequence: 2,
            offset: 0,
        }
    );
    assert!(matches!(
        owner.replay_from(SessionCursor::START).unwrap().as_slice(),
        [frame] if matches!(frame.event, ReplayEvent::SyncFlush)
    ));
}

#[test]
fn every_byte_boundary_restores_exact_model_and_processor_continuation() {
    let input = b"hello\x1b[31;2mwide\xe7\x95\x8c\x1b]2;title\x07tail";
    for split in 0..=input.len() {
        let mut original = session(QueryReplyPolicy::Quiet);
        let first = input[..split].to_vec();
        if !first.is_empty() {
            original
                .ingest(RawChunk::new(SessionCursor::START, first))
                .unwrap();
        }
        let checkpoint = original.checkpoint_bytes().unwrap();
        let mut restored = session(QueryReplyPolicy::Quiet);
        restored
            .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
            .unwrap();
        let cursor = original.cursor();
        if split < input.len() {
            restored
                .ingest(RawChunk::new(cursor, input[split..].to_vec()))
                .unwrap();
            original
                .ingest(RawChunk::new(cursor, input[split..].to_vec()))
                .unwrap();
        }
        assert_eq!(
            restored.engine().model_checkpoint().unwrap(),
            original.engine().model_checkpoint().unwrap(),
            "split {split}"
        );
        assert_eq!(
            restored.engine().processor_checkpoint().unwrap(),
            original.engine().processor_checkpoint().unwrap(),
            "split {split}"
        );
    }
}

#[test]
fn resize_advances_sequence_but_not_output_offset() {
    let mut owner = session(QueryReplyPolicy::Owner);
    owner
        .ingest(RawChunk::new(SessionCursor::START, b"a".to_vec()))
        .unwrap();
    let cursor = owner.cursor();
    let report = owner.resize(Geometry::new(40, 8)).unwrap();
    assert_eq!(
        report.cursor,
        SessionCursor {
            sequence: cursor.sequence + 1,
            offset: cursor.offset
        }
    );
    owner
        .ingest(RawChunk::new(owner.cursor(), b"b".to_vec()))
        .unwrap();
    assert_eq!(owner.geometry(), Geometry::new(40, 8));
}

#[test]
fn restore_rejects_invalid_checkpoint_without_touching_live_state() {
    let mut live = session(QueryReplyPolicy::Quiet);
    live.ingest(RawChunk::new(SessionCursor::START, b"stable".to_vec()))
        .unwrap();
    let before = live.checkpoint_bytes().unwrap();
    let mut decoded: serde_json::Value = serde_json::from_slice(&before).unwrap();
    decoded["version"] = serde_json::json!(99);
    let invalid = serde_json::to_vec(&decoded).unwrap();
    assert!(live
        .restore_checkpoint_bytes(&invalid, QueryReplyPolicy::Quiet)
        .is_err());
    assert_eq!(live.checkpoint_bytes().unwrap(), before);
}

#[test]
fn restore_rejects_checkpoint_codec_revision_mismatch() {
    let mut live = session(QueryReplyPolicy::Quiet);
    let before = live.checkpoint_bytes().unwrap();
    let mut decoded: serde_json::Value = serde_json::from_slice(&before).unwrap();
    decoded["engine"]["modelCheckpointVersion"] = serde_json::json!(2);
    let invalid = serde_json::to_vec(&decoded).unwrap();
    assert!(live
        .restore_checkpoint_bytes(&invalid, QueryReplyPolicy::Quiet)
        .is_err());
    assert_eq!(live.checkpoint_bytes().unwrap(), before);
}

#[test]
fn replay_window_reports_gap_and_never_fabricates_missing_bytes() {
    let mut owner =
        TerminalSession::<TestEngine>::new(Geometry::new(20, 4), QueryReplyPolicy::Quiet, 4, 8)
            .unwrap();
    owner
        .ingest(RawChunk::new(SessionCursor::START, b"abc".to_vec()))
        .unwrap();
    owner
        .ingest(RawChunk::new(owner.cursor(), b"def".to_vec()))
        .unwrap();
    assert!(matches!(
        owner.replay_from(SessionCursor::START),
        Err(ReplayError::Gap { .. })
    ));
    let retained = owner
        .replay_from(SessionCursor {
            sequence: 2,
            offset: 3,
        })
        .unwrap();
    assert_eq!(
        retained
            .iter()
            .map(|frame| match &frame.event {
                ReplayEvent::Output(bytes) => bytes.as_slice(),
                ReplayEvent::Resize(_) | ReplayEvent::SyncFlush => &[][..],
            })
            .collect::<Vec<_>>(),
        vec![b"def".as_slice()]
    );
}

#[test]
fn replay_preserves_output_resize_output_order_and_checkpoint_tail() {
    let mut original = session(QueryReplyPolicy::Quiet);
    original
        .ingest(RawChunk::new(SessionCursor::START, b"before".to_vec()))
        .unwrap();
    let checkpoint = original.checkpoint_bytes().unwrap();
    let resize_cursor = original.cursor();
    original.resize(Geometry::new(32, 6)).unwrap();
    let after_resize_cursor = original.cursor();
    original
        .ingest(RawChunk::new(after_resize_cursor, b"after".to_vec()))
        .unwrap();

    let replay = original.replay_from(SessionCursor::START).unwrap();
    assert_eq!(replay.len(), 3);
    assert!(matches!(replay[0].event, ReplayEvent::Output(ref bytes) if bytes == b"before"));
    assert_eq!(replay[0].cursor(), SessionCursor::START);
    assert!(
        matches!(replay[1].event, ReplayEvent::Resize(geometry) if geometry == Geometry::new(32, 6))
    );
    assert_eq!(replay[1].cursor(), resize_cursor);
    assert_eq!(replay[1].end_cursor().unwrap(), after_resize_cursor);
    assert!(matches!(replay[2].event, ReplayEvent::Output(ref bytes) if bytes == b"after"));

    let mut restored = session(QueryReplyPolicy::Quiet);
    restored
        .restore_checkpoint_bytes(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    restored.apply_frame(replay[1].clone()).unwrap();
    restored.apply_frame(replay[2].clone()).unwrap();
    assert_eq!(restored.geometry(), original.geometry());
    assert_eq!(restored.cursor(), original.cursor());
    assert_eq!(
        restored.engine().model_checkpoint().unwrap(),
        original.engine().model_checkpoint().unwrap()
    );
    assert_eq!(
        restored.engine().processor_checkpoint().unwrap(),
        original.engine().processor_checkpoint().unwrap()
    );
}

#[test]
fn revision_overflow_rejects_output_and_resize_before_model_mutation() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    let mut checkpoint = owner.checkpoint().unwrap();
    checkpoint.revision = u64::MAX;
    assert!(owner
        .restore_checkpoint(&checkpoint, QueryReplyPolicy::Quiet)
        .is_err());
    checkpoint.revision = u64::MAX - 1;
    owner
        .restore_checkpoint(&checkpoint, QueryReplyPolicy::Quiet)
        .unwrap();
    let before_model = owner.engine().model_checkpoint().unwrap();
    let before_processor = owner.engine().processor_checkpoint().unwrap();
    let before_cursor = owner.cursor();
    let before_geometry = owner.geometry();

    assert!(matches!(
        owner.ingest(RawChunk::new(before_cursor, b"output".to_vec())),
        Err(coven_terminal::ApplyError::CursorOverflow)
    ));
    assert!(matches!(
        owner.resize(Geometry::new(32, 6)),
        Err(coven_terminal::ApplyError::CursorOverflow)
    ));
    assert_eq!(owner.engine().model_checkpoint().unwrap(), before_model);
    assert_eq!(
        owner.engine().processor_checkpoint().unwrap(),
        before_processor
    );
    assert_eq!(owner.cursor(), before_cursor);
    assert_eq!(owner.geometry(), before_geometry);
    assert_eq!(owner.revision(), u64::MAX - 1);
}

#[test]
fn checkpoint_bytes_reject_empty_and_trailing_payloads() {
    let mut owner = session(QueryReplyPolicy::Quiet);
    assert!(owner
        .restore_checkpoint_bytes(b"", QueryReplyPolicy::Quiet)
        .is_err());
    let mut bytes = owner.checkpoint_bytes().unwrap();
    bytes.extend_from_slice(b"{}\n");
    assert!(owner
        .restore_checkpoint_bytes(&bytes, QueryReplyPolicy::Quiet)
        .is_err());
}
