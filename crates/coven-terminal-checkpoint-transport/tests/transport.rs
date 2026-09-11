use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use coven_terminal_checkpoint_transport::{
    decode_frame, BoundCheckpoint, CheckpointFrameReader, ConsumerEvent, ConsumerState, EffectKind,
    EffectOrder, EngineFingerprint, Geometry, MessageMeta, NegotiationError, ProducerLimits,
    ProducerStatus, QueryReplyPolicy, SessionCursor, StreamBinding, StreamMessage,
    SubscriberReceipt, TerminalCodec, TerminalEngine, TerminalStreamConsumer,
    TerminalStreamProducer, CHECKPOINT_FRAME_HEADER_BYTES, MAX_BOUND_CHECKPOINT_BYTES,
    MAX_CHECKPOINT_PARTS, MIN_CHECKPOINT_FRAME_BYTES,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ModelCheckpoint {
    geometry: Geometry,
    bytes: Vec<u8>,
    calls: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ProcessorCheckpoint {
    continuation: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
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

impl TerminalEngine for TestEngine {
    type ModelCheckpoint = ModelCheckpoint;
    type ProcessorCheckpoint = ProcessorCheckpoint;
    type Error = TestError;
    type SnapshotError = TestError;

    fn fingerprint() -> EngineFingerprint {
        EngineFingerprint {
            model_package: "transport-test-model".to_owned(),
            model_version: "1".to_owned(),
            model_revision: "transport-test".to_owned(),
            model_checkpoint_schema: "transport-test-model".to_owned(),
            model_checkpoint_version: 1,
            parser_package: "transport-test-parser".to_owned(),
            parser_version: "1".to_owned(),
            parser_revision: "transport-test".to_owned(),
            processor_checkpoint_schema: "transport-test-processor".to_owned(),
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

    fn apply_raw(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<coven_terminal_checkpoint_transport::OrderedEffect>, Self::Error> {
        self.calls += 1;
        self.bytes.extend_from_slice(bytes);
        self.continuation = bytes.iter().copied().rev().take(3).collect();
        Ok(vec![
            coven_terminal_checkpoint_transport::OrderedEffect {
                order: EffectOrder(0),
                kind: EffectKind::Reply(b"owner-only-query-reply".to_vec()),
            },
            coven_terminal_checkpoint_transport::OrderedEffect {
                order: EffectOrder(1),
                kind: EffectKind::Resize(coven_terminal_checkpoint_transport::ResizeNotice {
                    order: EffectOrder(1),
                    geometry: self.geometry,
                }),
            },
        ])
    }

    fn resize(
        &mut self,
        geometry: Geometry,
    ) -> Result<Vec<coven_terminal_checkpoint_transport::OrderedEffect>, Self::Error> {
        self.geometry = geometry;
        Ok(Vec::new())
    }

    fn needs_flush(&self) -> bool {
        NEEDS_FLUSH.load(Ordering::SeqCst)
    }

    fn model_checkpoint(&self) -> Result<Self::ModelCheckpoint, Self::SnapshotError> {
        Ok(ModelCheckpoint {
            geometry: self.geometry,
            bytes: self.bytes.clone(),
            calls: self.calls,
        })
    }

    fn processor_checkpoint(&self) -> Result<Self::ProcessorCheckpoint, Self::SnapshotError> {
        Ok(ProcessorCheckpoint {
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
            return Err(TestError("checkpoint geometry mismatch"));
        }
        Ok(Self {
            geometry,
            bytes: model.bytes.clone(),
            continuation: processor.continuation.clone(),
            calls: model.calls,
        })
    }
}

static FAIL_NEXT_OPERATION: AtomicBool = AtomicBool::new(false);
static NEEDS_FLUSH: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct FailingModelCheckpoint {
    geometry: Geometry,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct FailingProcessorCheckpoint {
    marker: u8,
}

#[derive(Clone, Debug)]
struct FailingEngine {
    geometry: Geometry,
    bytes: Vec<u8>,
    poisoned: bool,
}

impl FailingEngine {
    fn fail_if_requested(&mut self) -> Result<(), TestError> {
        if self.poisoned {
            return Err(TestError("engine is poisoned"));
        }
        if FAIL_NEXT_OPERATION.swap(false, Ordering::SeqCst) {
            self.poisoned = true;
            return Err(TestError("engine operation failed"));
        }
        Ok(())
    }
}

impl TerminalEngine for FailingEngine {
    type ModelCheckpoint = FailingModelCheckpoint;
    type ProcessorCheckpoint = FailingProcessorCheckpoint;
    type Error = TestError;
    type SnapshotError = TestError;

    fn fingerprint() -> EngineFingerprint {
        EngineFingerprint {
            model_package: "transport-failing-model".to_owned(),
            model_version: "1".to_owned(),
            model_revision: "transport-failing".to_owned(),
            model_checkpoint_schema: "transport-failing-model".to_owned(),
            model_checkpoint_version: 1,
            parser_package: "transport-failing-parser".to_owned(),
            parser_version: "1".to_owned(),
            parser_revision: "transport-failing".to_owned(),
            processor_checkpoint_schema: "transport-failing-processor".to_owned(),
            processor_checkpoint_version: 1,
        }
    }

    fn new(geometry: Geometry, _policy: QueryReplyPolicy) -> Result<Self, Self::Error> {
        Ok(Self {
            geometry,
            bytes: Vec::new(),
            poisoned: false,
        })
    }

    fn apply_raw(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<coven_terminal_checkpoint_transport::OrderedEffect>, Self::Error> {
        self.fail_if_requested()?;
        self.bytes.extend_from_slice(bytes);
        Ok(Vec::new())
    }

    fn resize(
        &mut self,
        geometry: Geometry,
    ) -> Result<Vec<coven_terminal_checkpoint_transport::OrderedEffect>, Self::Error> {
        self.fail_if_requested()?;
        self.geometry = geometry;
        Ok(Vec::new())
    }

    fn model_checkpoint(&self) -> Result<Self::ModelCheckpoint, Self::SnapshotError> {
        if self.poisoned {
            return Err(TestError("poisoned model snapshot"));
        }
        Ok(FailingModelCheckpoint {
            geometry: self.geometry,
            bytes: self.bytes.clone(),
        })
    }

    fn processor_checkpoint(&self) -> Result<Self::ProcessorCheckpoint, Self::SnapshotError> {
        if self.poisoned {
            return Err(TestError("poisoned processor snapshot"));
        }
        Ok(FailingProcessorCheckpoint { marker: 1 })
    }

    fn from_checkpoint(
        geometry: Geometry,
        model: &Self::ModelCheckpoint,
        processor: &Self::ProcessorCheckpoint,
        _policy: QueryReplyPolicy,
    ) -> Result<Self, Self::SnapshotError> {
        if model.geometry != geometry || processor.marker != 1 {
            return Err(TestError("invalid failing checkpoint"));
        }
        Ok(Self {
            geometry,
            bytes: model.bytes.clone(),
            poisoned: false,
        })
    }
}

fn binding() -> StreamBinding {
    StreamBinding::new("terminal-test", [7; 16], 3, 5, 11).unwrap()
}

fn failing_session() -> coven_terminal_checkpoint_transport::TerminalSession<FailingEngine> {
    coven_terminal_checkpoint_transport::TerminalSession::from_engine(
        FailingEngine {
            geometry: Geometry::new(20, 4),
            bytes: Vec::new(),
            poisoned: false,
        },
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
    )
    .unwrap()
}

fn limits(max_frame_bytes: usize, max_subscriber_messages: usize) -> ProducerLimits {
    ProducerLimits {
        max_frame_bytes,
        max_subscriber_bytes: 64 * 1024,
        max_subscriber_messages,
    }
}

fn consumer<M: TerminalEngine>(
    binding: StreamBinding,
    max_frame_bytes: usize,
) -> TerminalStreamConsumer<M> {
    TerminalStreamConsumer::<M>::from_geometry(
        binding,
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        max_frame_bytes,
    )
    .unwrap()
}

fn deliver_message<M: TerminalEngine>(
    consumer: &mut TerminalStreamConsumer<M>,
    message: StreamMessage,
    max_frame_bytes: usize,
) -> Vec<ConsumerEvent> {
    let mut events = Vec::new();
    for frame in message.encode_parts(max_frame_bytes).unwrap() {
        events.extend(consumer.accept_encoded(&frame).unwrap());
    }
    events
}

fn bootstrap<M: TerminalEngine>(
    producer: &TerminalStreamProducer<M>,
    subscriber: coven_terminal_checkpoint_transport::SubscriberId,
    consumer: &mut TerminalStreamConsumer<M>,
    max_frame_bytes: usize,
) {
    let mut bootstrapped = false;
    while let Some(message) = producer.poll(subscriber).unwrap() {
        let events = deliver_message(consumer, message, max_frame_bytes);
        bootstrapped |= events
            .iter()
            .any(|event| matches!(event, ConsumerEvent::Bootstrapped { .. }));
    }
    assert!(
        bootstrapped,
        "subscriber should receive a complete checkpoint"
    );
    assert_eq!(consumer.state(), ConsumerState::Live);
}

#[test]
fn producer_and_consumer_preserve_checkpointed_model_and_ordered_events() {
    let binding = binding();
    let max_frame_bytes = 128;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding.clone(), max_frame_bytes);

    assert_eq!(producer.checkpoint_query_count().unwrap(), 1);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);
    assert_eq!(producer.checkpoint_query_count().unwrap(), 1);

    let first = b"raw\x1b[31m\xff";
    let first_report = producer.publish_output(first).unwrap();
    assert_eq!(
        first_report
            .effects
            .iter()
            .filter(|effect| effect.reply().is_some())
            .count(),
        0
    );
    let first_message = producer.poll(subscriber).unwrap().unwrap();
    assert_eq!(
        deliver_message(&mut consumer, first_message, max_frame_bytes),
        vec![ConsumerEvent::Applied {
            cursor: SessionCursor {
                sequence: 2,
                offset: first.len() as u64,
            },
            revision: 2,
        }]
    );

    let resize = Geometry::with_cell_size(40, 8, 9, 17);
    let resize_report = producer.publish_resize(resize).unwrap();
    let resize_message = producer.poll(subscriber).unwrap().unwrap();
    assert_eq!(resize_report.cursor.sequence, 3);
    assert_eq!(
        deliver_message(&mut consumer, resize_message, max_frame_bytes),
        vec![ConsumerEvent::Applied {
            cursor: SessionCursor {
                sequence: 3,
                offset: first.len() as u64,
            },
            revision: 4,
        }]
    );

    let second = b"\x1b[2Jtail\0";
    producer.publish_output(second).unwrap();
    let second_message = producer.poll(subscriber).unwrap().unwrap();
    let events = deliver_message(&mut consumer, second_message, max_frame_bytes);
    assert!(matches!(
        events.as_slice(),
        [ConsumerEvent::Applied { revision: 6, .. }]
    ));
    assert_eq!(consumer.geometry(), resize);

    let owner_bound =
        BoundCheckpoint::decode_for(&producer.session_checkpoint().unwrap(), &binding).unwrap();
    assert_eq!(
        owner_bound.checkpoint_bytes,
        consumer.session().checkpoint_bytes().unwrap()
    );
}

#[test]
fn synchronized_flush_is_an_ordered_zero_byte_event() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    let report = producer.publish_flush().unwrap();
    assert_eq!(
        report.cursor,
        SessionCursor {
            sequence: 2,
            offset: 0
        }
    );
    assert_eq!(report.revision, 2);
    let message = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(message, StreamMessage::SyncFlush { .. }));
    assert_eq!(
        deliver_message(&mut consumer, message, max_frame_bytes),
        vec![ConsumerEvent::Applied {
            cursor: SessionCursor {
                sequence: 2,
                offset: 0
            },
            revision: 2,
        }]
    );

    producer.publish_output(b"after flush").unwrap();
    let message = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, message, max_frame_bytes).as_slice(),
        [ConsumerEvent::Applied { revision: 4, .. }]
    ));
}

#[test]
fn expired_flush_is_published_only_when_engine_reports_due() {
    NEEDS_FLUSH.store(false, Ordering::SeqCst);
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding,
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    while producer.poll(subscriber).unwrap().is_some() {}
    assert!(producer.publish_expired_flush().unwrap().is_none());
    assert_eq!(producer.cursor().unwrap(), SessionCursor::START);

    NEEDS_FLUSH.store(true, Ordering::SeqCst);
    let report = producer
        .publish_expired_flush()
        .unwrap()
        .expect("engine-reported expiration should publish a flush");
    NEEDS_FLUSH.store(false, Ordering::SeqCst);
    assert_eq!(
        report.cursor,
        SessionCursor {
            sequence: 2,
            offset: 0
        }
    );
    assert_eq!(report.revision, 2);
    assert!(matches!(
        producer.poll(subscriber).unwrap(),
        Some(StreamMessage::SyncFlush { .. })
    ));
}

#[test]
fn owner_side_poison_fails_existing_and_future_subscribers() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    // Simulate a trusted query-effect sink rejecting effects after the model
    // frame was committed. Poisoning must discard the queued output rather
    // than let a subscriber observe a stream with an unadmitted side effect.
    producer.publish_output(b"owner output").unwrap();
    producer.poison("trusted query-effect sink closed").unwrap();
    assert_eq!(producer.status().unwrap(), ProducerStatus::Failed);
    assert_eq!(
        producer.failure_reason().unwrap().as_deref(),
        Some("trusted query-effect sink closed")
    );
    assert!(matches!(
        producer.subscribe(),
        Err(coven_terminal_checkpoint_transport::ProducerError::Failed)
    ));

    let gap = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, gap, max_frame_bytes).as_slice(),
        [ConsumerEvent::Gap {
            reason: coven_terminal_checkpoint_transport::GapReason::ProtocolReset,
            ..
        }]
    ));
    let close = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, close, max_frame_bytes).as_slice(),
        [ConsumerEvent::Closed]
    ));
    assert_eq!(consumer.state(), ConsumerState::Closed);
    assert!(matches!(
        producer.session_checkpoint(),
        Err(coven_terminal_checkpoint_transport::ProducerError::Failed)
    ));
}

#[test]
fn subscribe_receipt_matches_bootstrap_ticket() {
    let binding = binding();
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding,
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(128, 256),
    )
    .unwrap();
    let receipt: SubscriberReceipt = producer.subscribe_with_receipt().unwrap();
    assert_eq!(receipt.id(), receipt.subscriber);
    assert_eq!(receipt.cursor, SessionCursor::START);
    assert_eq!(receipt.revision, 0);
    assert_eq!(receipt.geometry, Geometry::new(20, 4));

    let first = producer.poll(receipt.subscriber).unwrap().unwrap();
    match first {
        StreamMessage::CheckpointStart { meta, .. } => {
            assert_eq!(meta.cursor, receipt.cursor);
            assert_eq!(meta.revision, receipt.revision);
        }
        other => panic!("new subscription started with {other:?}"),
    }
}

#[test]
fn checkpoint_queries_are_cached_until_a_model_revision_changes() {
    let binding = binding();
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding,
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(256, 256),
    )
    .unwrap();
    let first = producer.subscribe().unwrap();
    let second = producer.subscribe().unwrap();
    assert_eq!(producer.checkpoint_query_count().unwrap(), 1);
    producer.request_bootstrap(first).unwrap();
    assert_eq!(producer.checkpoint_query_count().unwrap(), 1);
    bootstrap(
        &producer,
        second,
        &mut consumer(producer.binding().clone(), 256),
        256,
    );
    producer.publish_output(b"change").unwrap();
    producer.request_bootstrap(first).unwrap();
    assert_eq!(producer.checkpoint_query_count().unwrap(), 2);
}

#[test]
fn slow_subscriber_gets_typed_gap_and_can_resynchronize() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 1),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    producer.publish_output(b"one").unwrap();
    producer.publish_output(b"two").unwrap();
    let gap = producer.poll(subscriber).unwrap().unwrap();
    let events = deliver_message(&mut consumer, gap, max_frame_bytes);
    assert!(matches!(
        events.as_slice(),
        [ConsumerEvent::Gap {
            reason: coven_terminal_checkpoint_transport::GapReason::SlowConsumer,
            ..
        }]
    ));
    assert_eq!(consumer.state(), ConsumerState::Gap);

    producer.request_bootstrap(subscriber).unwrap();
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);
    assert_eq!(consumer.cursor(), producer.cursor().unwrap());
}

#[test]
fn gap_requested_cursor_advances_after_a_polled_event() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        ProducerLimits {
            max_frame_bytes,
            max_subscriber_bytes: 101,
            max_subscriber_messages: 256,
        },
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    producer.publish_output(b"a").unwrap();
    let first = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, first, max_frame_bytes).as_slice(),
        [ConsumerEvent::Applied { .. }]
    ));

    // This event is larger than the subscriber byte bound while the queue is
    // empty, so the gap must use the consumed event's end cursor.
    producer.publish_output(b"01234567890123456789").unwrap();
    let gap = producer.poll(subscriber).unwrap().unwrap();
    let events = deliver_message(&mut consumer, gap, max_frame_bytes);
    assert!(matches!(
        events.as_slice(),
        [ConsumerEvent::Gap {
            requested: SessionCursor {
                sequence: 2,
                offset: 1,
            },
            ..
        }]
    ));
}

#[test]
fn output_frames_are_raw_and_incremental_reader_is_bound() {
    let binding = binding();
    let meta = MessageMeta {
        binding: binding.clone(),
        cursor: SessionCursor::START,
        revision: 0,
    };
    let bytes: Vec<u8> = (0..100).map(|value| value as u8).collect();
    let message = StreamMessage::Output {
        meta,
        part_index: 0,
        part_count: 1,
        bytes: bytes.clone(),
    };
    let frames = message.encode_parts(128).unwrap();
    assert_eq!(frames.len(), 3);
    let mut reader = CheckpointFrameReader::new(binding.clone(), 128).unwrap();
    let mut decoded = Vec::new();
    for frame in &frames {
        // Feed one byte at a time to exercise header and payload carry-over.
        for byte in frame {
            let mut messages = std::collections::VecDeque::new();
            reader
                .push(std::slice::from_ref(byte), &mut messages)
                .unwrap();
            decoded.extend(messages);
        }
    }
    assert_eq!(decoded.len(), 3);
    let joined: Vec<u8> = decoded
        .iter()
        .flat_map(|message| match message {
            StreamMessage::Output { bytes, .. } => bytes.clone(),
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(joined, bytes);

    let wrong = StreamBinding::new("terminal-test", [8; 16], 3, 5, 11).unwrap();
    assert!(matches!(
        decode_frame(&frames[0], 128, &wrong),
        Err(coven_terminal_checkpoint_transport::WireCodecError::IdentityMismatch)
    ));
}

#[test]
fn malformed_declared_payload_fails_reader_before_payload_allocation() {
    let binding = binding();
    let message = StreamMessage::Close {
        meta: MessageMeta {
            binding: binding.clone(),
            cursor: SessionCursor::START,
            revision: 0,
        },
    };
    let mut frame = message.encode(128).unwrap();
    frame[80..84].copy_from_slice(&1_000_u32.to_be_bytes());
    let mut reader = CheckpointFrameReader::new(binding, 128).unwrap();
    let mut messages = std::collections::VecDeque::new();
    assert!(matches!(
        reader.push(&frame, &mut messages),
        Err(coven_terminal_checkpoint_transport::WireCodecError::PayloadTooLarge { .. })
    ));
    assert!(matches!(
        reader.push(&[], &mut messages),
        Err(coven_terminal_checkpoint_transport::WireCodecError::ReaderFailed)
    ));
}

#[test]
fn reader_emits_zero_payload_close_when_header_is_the_final_chunk() {
    let binding = binding();
    let message = StreamMessage::Close {
        meta: MessageMeta {
            binding: binding.clone(),
            cursor: SessionCursor::START,
            revision: 0,
        },
    };
    let frame = message.encode(128).unwrap();
    assert_eq!(
        frame.len(),
        coven_terminal_checkpoint_transport::CHECKPOINT_FRAME_HEADER_BYTES
    );
    let mut reader = CheckpointFrameReader::new(binding, 128).unwrap();
    let mut messages = std::collections::VecDeque::new();
    reader.push(&frame, &mut messages).unwrap();
    assert_eq!(messages.pop_front(), Some(message));
    reader.finish().unwrap();
}

#[test]
fn frame_limit_fits_checkpoint_controls_and_multipart_capacity() {
    let binding = binding();
    let too_small = MIN_CHECKPOINT_FRAME_BYTES - 1;
    assert!(matches!(
        CheckpointFrameReader::new(binding.clone(), too_small),
        Err(coven_terminal_checkpoint_transport::WireCodecError::InvalidFrameLimit)
    ));
    assert_eq!(
        ProducerLimits {
            max_frame_bytes: too_small,
            max_subscriber_bytes: 64 * 1024,
            max_subscriber_messages: 1,
        }
        .validate(),
        Err(coven_terminal_checkpoint_transport::ProducerLimitError::FrameLimit)
    );

    let meta = MessageMeta {
        binding: binding.clone(),
        cursor: SessionCursor::START,
        revision: 0,
    };
    let control = StreamMessage::CheckpointStart {
        meta: meta.clone(),
        total_bytes: 1,
        part_count: 1,
        digest: [0; 32],
    };
    assert_eq!(
        control.encode(MIN_CHECKPOINT_FRAME_BYTES).unwrap().len(),
        MIN_CHECKPOINT_FRAME_BYTES
    );
    let mut impossible = control.encode(MIN_CHECKPOINT_FRAME_BYTES).unwrap();
    impossible[CHECKPOINT_FRAME_HEADER_BYTES..CHECKPOINT_FRAME_HEADER_BYTES + 4]
        .copy_from_slice(&(MAX_BOUND_CHECKPOINT_BYTES as u32).to_be_bytes());
    assert!(matches!(
        decode_frame(&impossible, MIN_CHECKPOINT_FRAME_BYTES, &binding),
        Err(coven_terminal_checkpoint_transport::WireCodecError::InvalidCheckpointFrame)
    ));

    let oversized_transfer = StreamMessage::CheckpointStart {
        meta,
        total_bytes: MAX_BOUND_CHECKPOINT_BYTES as u32,
        part_count: MAX_CHECKPOINT_PARTS,
        digest: [0; 32],
    };
    assert!(matches!(
        oversized_transfer.encode_parts(MIN_CHECKPOINT_FRAME_BYTES),
        Err(coven_terminal_checkpoint_transport::WireCodecError::InvalidCheckpointFrame)
    ));
}

#[test]
fn negotiation_keeps_raw_v1_and_checkpoint_v1_as_distinct_protocols() {
    let raw = coven_terminal_checkpoint_transport::negotiate(
        coven_terminal_checkpoint_transport::CodecNegotiation::raw_observer(),
    )
    .unwrap();
    assert_eq!(raw.codec, TerminalCodec::RawBytesV1);
    assert_eq!(raw.protocol_version, 1);

    let checkpoint = coven_terminal_checkpoint_transport::negotiate(
        coven_terminal_checkpoint_transport::CodecNegotiation::checkpoint_observer(),
    )
    .unwrap();
    assert_eq!(checkpoint.codec, TerminalCodec::CheckpointV1);
    assert_eq!(checkpoint.protocol_version, 2);

    // Codec negotiation may carry a control capability request on the same
    // coherent stream. The authenticated owner route still has to authorize
    // the request and grant its independent single-writer lease.
    let checkpoint_control = coven_terminal_checkpoint_transport::negotiate(
        coven_terminal_checkpoint_transport::CodecNegotiation {
            protocol_version: 2,
            codec: TerminalCodec::CheckpointV1,
            observe: true,
            control: true,
        },
    )
    .unwrap();
    assert_eq!(checkpoint_control, checkpoint);

    let control_without_observe = coven_terminal_checkpoint_transport::negotiate(
        coven_terminal_checkpoint_transport::CodecNegotiation {
            protocol_version: 2,
            codec: TerminalCodec::CheckpointV1,
            observe: false,
            control: true,
        },
    );
    assert_eq!(control_without_observe, Err(NegotiationError::ObserverOnly));

    let unsupported = coven_terminal_checkpoint_transport::negotiate(
        coven_terminal_checkpoint_transport::CodecNegotiation {
            protocol_version: 9,
            codec: TerminalCodec::RawBytesV1,
            observe: true,
            control: false,
        },
    );
    assert_eq!(
        unsupported,
        Err(NegotiationError::UnsupportedVersion { version: 9 })
    );
}

#[test]
fn close_after_dropped_output_is_delivered_after_a_gap() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 1),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    producer.publish_output(b"queued").unwrap();
    producer.close().unwrap();
    let gap = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, gap, max_frame_bytes).as_slice(),
        [ConsumerEvent::Gap { .. }]
    ));
    let close = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, close, max_frame_bytes).as_slice(),
        [ConsumerEvent::Closed]
    ));
    assert_eq!(consumer.state(), ConsumerState::Closed);
}

#[test]
fn output_after_a_gap_is_suppressed_until_bootstrap() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 1),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    producer.publish_output(b"first").unwrap();
    producer.publish_output(b"second").unwrap();
    producer.publish_output(b"third").unwrap();
    let gap = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, gap, max_frame_bytes).as_slice(),
        [ConsumerEvent::Gap { .. }]
    ));
    // The third output was published after the gap was pending and must not
    // be allowed to pass the consumer's bootstrap barrier.
    assert!(producer.poll(subscriber).unwrap().is_none());

    producer.request_bootstrap(subscriber).unwrap();
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);
    assert_eq!(consumer.cursor(), producer.cursor().unwrap());
}

#[test]
fn producer_engine_error_fails_closed_and_queues_protocol_reset() {
    FAIL_NEXT_OPERATION.store(false, Ordering::SeqCst);
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::new(
        binding.clone(),
        failing_session(),
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer::<FailingEngine>(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    FAIL_NEXT_OPERATION.store(true, Ordering::SeqCst);
    assert!(matches!(
        producer.publish_output(b"poison"),
        Err(coven_terminal_checkpoint_transport::ProducerError::Apply(
            coven_terminal_checkpoint_transport::ApplyError::Engine(_)
        ))
    ));
    assert_eq!(producer.status().unwrap(), ProducerStatus::Failed);
    assert!(matches!(
        producer.subscribe(),
        Err(coven_terminal_checkpoint_transport::ProducerError::Failed)
    ));
    assert!(matches!(
        producer.session_checkpoint(),
        Err(coven_terminal_checkpoint_transport::ProducerError::Failed)
    ));

    let gap = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, gap, max_frame_bytes).as_slice(),
        [ConsumerEvent::Gap {
            reason: coven_terminal_checkpoint_transport::GapReason::ProtocolReset,
            ..
        }]
    ));
    let close = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        deliver_message(&mut consumer, close, max_frame_bytes).as_slice(),
        [ConsumerEvent::Closed]
    ));
}

#[test]
fn consumer_engine_error_enters_gap_and_recovers_from_checkpoint() {
    FAIL_NEXT_OPERATION.store(false, Ordering::SeqCst);
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::new(
        binding.clone(),
        failing_session(),
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer::<FailingEngine>(binding, max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    producer.publish_output(b"before failure").unwrap();
    let output = producer.poll(subscriber).unwrap().unwrap();
    FAIL_NEXT_OPERATION.store(true, Ordering::SeqCst);
    assert!(matches!(
        consumer.accept_encoded(&output.encode(max_frame_bytes).unwrap()),
        Err(coven_terminal_checkpoint_transport::ConsumerError::Apply(_))
    ));
    assert_eq!(consumer.state(), ConsumerState::Gap);

    // The producer has a valid checkpoint at the event's committed cursor;
    // replacing the poisoned consumer engine restores the stream barrier.
    producer.request_bootstrap(subscriber).unwrap();
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);
    assert_eq!(consumer.state(), ConsumerState::Live);
    assert_eq!(consumer.cursor(), producer.cursor().unwrap());
}

#[test]
fn stale_checkpoint_and_close_ticket_are_rejected_after_gap() {
    let binding = binding();
    let max_frame_bytes = 256;
    let runtime = coven_terminal_checkpoint_transport::TerminalSession::<TestEngine>::new(
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
    )
    .unwrap();
    let checkpoint = runtime.checkpoint_bytes().unwrap();
    let bound = BoundCheckpoint::new(binding.clone(), SessionCursor::START, 0, checkpoint)
        .unwrap()
        .encode()
        .unwrap();
    let digest = BoundCheckpoint::digest(&bound);
    let meta = MessageMeta {
        binding: binding.clone(),
        cursor: SessionCursor::START,
        revision: 0,
    };
    let mut consumer = consumer::<TestEngine>(binding.clone(), max_frame_bytes);
    consumer
        .accept_message(StreamMessage::Gap {
            binding: binding.clone(),
            requested: SessionCursor::START,
            available: SessionCursor {
                sequence: 2,
                offset: 0,
            },
            available_revision: 2,
            reason: coven_terminal_checkpoint_transport::GapReason::SlowConsumer,
        })
        .unwrap();
    assert!(matches!(
        consumer.accept_message(StreamMessage::CheckpointStart {
            meta: meta.clone(),
            total_bytes: bound.len() as u32,
            part_count: 1,
            digest,
        }),
        Err(coven_terminal_checkpoint_transport::ConsumerError::StaleCheckpoint { .. })
    ));
    assert!(matches!(
        consumer.accept_message(StreamMessage::Close {
            meta: MessageMeta {
                binding: binding.clone(),
                cursor: SessionCursor::START,
                revision: 0,
            },
        }),
        Err(coven_terminal_checkpoint_transport::ConsumerError::UnexpectedClose)
    ));
    consumer
        .accept_message(StreamMessage::Close {
            meta: MessageMeta {
                binding,
                cursor: SessionCursor {
                    sequence: 2,
                    offset: 0,
                },
                revision: 2,
            },
        })
        .unwrap();
    assert_eq!(consumer.state(), ConsumerState::Closed);
}

#[test]
fn later_gap_cannot_lower_close_ticket_revision() {
    let binding = binding();
    let mut consumer = consumer::<TestEngine>(binding.clone(), 256);
    let requested = SessionCursor::START;
    consumer
        .accept_message(StreamMessage::Gap {
            binding: binding.clone(),
            requested,
            available: SessionCursor {
                sequence: 2,
                offset: 0,
            },
            available_revision: 4,
            reason: coven_terminal_checkpoint_transport::GapReason::SlowConsumer,
        })
        .unwrap();
    consumer
        .accept_message(StreamMessage::Gap {
            binding: binding.clone(),
            requested,
            available: SessionCursor {
                sequence: 3,
                offset: 0,
            },
            available_revision: 2,
            reason: coven_terminal_checkpoint_transport::GapReason::ReplayUnavailable,
        })
        .unwrap();
    assert!(matches!(
        consumer.accept_message(StreamMessage::Close {
            meta: MessageMeta {
                binding,
                cursor: SessionCursor {
                    sequence: 3,
                    offset: 0,
                },
                revision: 2,
            },
        }),
        Err(coven_terminal_checkpoint_transport::ConsumerError::UnexpectedClose)
    ));
    assert_eq!(consumer.state(), ConsumerState::Gap);
}

#[test]
fn encoded_batch_semantic_error_fails_to_gap_after_prior_frame_is_applied() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding.clone(), max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);
    producer.publish_output(b"one").unwrap();
    producer.publish_output(b"two").unwrap();
    let first = producer.poll(subscriber).unwrap().unwrap();
    let mut second = producer.poll(subscriber).unwrap().unwrap();
    if let StreamMessage::Output { meta, .. } = &mut second {
        meta.revision += 2;
    }
    let mut batch = first.encode(max_frame_bytes).unwrap();
    batch.extend_from_slice(&second.encode(max_frame_bytes).unwrap());
    assert!(matches!(
        consumer.accept_encoded(&batch),
        Err(coven_terminal_checkpoint_transport::ConsumerError::RevisionMismatch { .. })
    ));
    assert_eq!(consumer.state(), ConsumerState::Gap);
    assert_eq!(consumer.cursor().sequence, 2);
}

#[test]
fn input_batch_and_oversized_publish_are_bounded() {
    let binding = binding();
    let max_frame_bytes = 128;
    let mut consumer = consumer::<TestEngine>(binding.clone(), max_frame_bytes);
    let close = StreamMessage::Close {
        meta: MessageMeta {
            binding: binding.clone(),
            cursor: SessionCursor::START,
            revision: 0,
        },
    }
    .encode(max_frame_bytes)
    .unwrap();
    let mut batch = Vec::with_capacity(
        close.len() * (coven_terminal_checkpoint_transport::MAX_MESSAGES_PER_PUSH + 1),
    );
    for _ in 0..=coven_terminal_checkpoint_transport::MAX_MESSAGES_PER_PUSH {
        batch.extend_from_slice(&close);
    }
    assert!(matches!(
        consumer.accept_encoded(&batch),
        Err(coven_terminal_checkpoint_transport::ConsumerError::Wire(
            coven_terminal_checkpoint_transport::WireCodecError::BatchTooLarge { .. }
        ))
    ));

    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding,
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let oversized = vec![b'x'; coven_terminal_checkpoint_transport::MAX_RAW_CHUNK_BYTES + 1];
    assert!(matches!(
        producer.publish_output(&oversized),
        Err(coven_terminal_checkpoint_transport::ProducerError::Apply(
            coven_terminal_checkpoint_transport::ApplyError::ChunkTooLarge { .. }
        ))
    ));
    assert_eq!(producer.status().unwrap(), ProducerStatus::Live);
    producer.publish_output(b"still live").unwrap();
}

#[test]
fn consumer_requires_quiet_policy_and_reader_rejects_data_after_finish() {
    let binding = binding();
    assert!(matches!(
        TerminalStreamConsumer::<TestEngine>::from_geometry(
            binding.clone(),
            Geometry::new(20, 4),
            QueryReplyPolicy::Owner,
            64 * 1024,
            256,
            256,
        ),
        Err(coven_terminal_checkpoint_transport::ConsumerError::OwnerPolicyNotAllowed)
    ));
    let mut reader = CheckpointFrameReader::new(binding.clone(), 256).unwrap();
    reader.finish().unwrap();
    let mut messages = std::collections::VecDeque::new();
    assert!(matches!(
        reader.push(&[], &mut messages),
        Err(coven_terminal_checkpoint_transport::WireCodecError::ReaderFinished)
    ));

    let mut consumer = consumer::<TestEngine>(binding.clone(), 256);
    consumer
        .accept_message(StreamMessage::Gap {
            binding: binding.clone(),
            requested: SessionCursor::START,
            available: SessionCursor::START,
            available_revision: 0,
            reason: coven_terminal_checkpoint_transport::GapReason::ProtocolReset,
        })
        .unwrap();
    consumer
        .accept_message(StreamMessage::Close {
            meta: MessageMeta {
                binding,
                cursor: SessionCursor::START,
                revision: 0,
            },
        })
        .unwrap();
    consumer.finish().unwrap();
    assert!(matches!(
        consumer.accept_encoded(&[]),
        Err(coven_terminal_checkpoint_transport::ConsumerError::Closed)
    ));
}

#[test]
fn revision_mismatch_is_rejected_before_model_mutation() {
    let binding = binding();
    let max_frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(max_frame_bytes, 256),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = consumer(binding.clone(), max_frame_bytes);
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    let before = consumer.session().checkpoint_bytes().unwrap();
    let malformed = StreamMessage::Output {
        meta: MessageMeta {
            binding,
            cursor: consumer.cursor(),
            revision: consumer.revision() + 4,
        },
        part_index: 0,
        part_count: 1,
        bytes: b"must-not-apply".to_vec(),
    };
    assert!(matches!(
        consumer.accept_message(malformed),
        Err(coven_terminal_checkpoint_transport::ConsumerError::RevisionMismatch { .. })
    ));
    assert_eq!(consumer.session().checkpoint_bytes().unwrap(), before);
}

#[test]
fn finish_rejects_an_incomplete_logical_transfer() {
    let binding = binding();
    let message = StreamMessage::Output {
        meta: MessageMeta {
            binding: binding.clone(),
            cursor: SessionCursor::START,
            revision: 0,
        },
        part_index: 0,
        part_count: 1,
        bytes: b"partial".to_vec(),
    };
    let frame = message.encode(256).unwrap();
    let mut consumer = consumer::<TestEngine>(binding, 256);
    // Output before bootstrap is rejected, but a complete wire frame has
    // still been consumed by the reader. This also verifies finish itself is
    // the transport boundary rather than a second parser.
    assert!(consumer.accept_encoded(&frame).is_err());
    assert!(matches!(
        consumer.finish(),
        Err(coven_terminal_checkpoint_transport::ConsumerError::UnexpectedEof)
    ));
}

#[test]
fn checkpoint_digest_and_identity_are_checked_before_restore() {
    let binding = binding();
    let runtime = coven_terminal_checkpoint_transport::TerminalSession::<TestEngine>::new(
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
    )
    .unwrap();
    let checkpoint = runtime.checkpoint_bytes().unwrap();
    let bound = BoundCheckpoint::new(binding.clone(), SessionCursor::START, 0, checkpoint).unwrap();
    let encoded = bound.encode().unwrap();
    let mut consumer = consumer::<TestEngine>(binding.clone(), 256);
    let meta = MessageMeta {
        binding,
        cursor: SessionCursor::START,
        revision: 0,
    };
    let digest = BoundCheckpoint::digest(&encoded);
    consumer
        .accept_message(StreamMessage::CheckpointStart {
            meta: meta.clone(),
            total_bytes: encoded.len() as u32,
            part_count: 1,
            digest,
        })
        .unwrap();
    let mut corrupted = encoded;
    let index = corrupted.len() - 1;
    corrupted[index] ^= 1;
    consumer
        .accept_message(StreamMessage::CheckpointPart {
            meta: meta.clone(),
            part_index: 0,
            part_count: 1,
            bytes: corrupted,
        })
        .unwrap();
    assert!(matches!(
        consumer.accept_message(StreamMessage::CheckpointEnd {
            meta,
            total_bytes: (index + 1) as u32,
            part_count: 1,
            digest,
        }),
        Err(coven_terminal_checkpoint_transport::ConsumerError::CheckpointDigestMismatch)
    ));
    assert_eq!(consumer.state(), ConsumerState::Gap);
}

#[test]
fn persisted_reconnect_requires_fresh_bootstrap_and_preserves_the_rollback_floor() {
    let binding = binding();
    let frame_bytes = 256;
    let producer = TerminalStreamProducer::<TestEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
        limits(frame_bytes, 256),
    )
    .unwrap();
    producer.publish_output(b"before").unwrap();
    let first = producer.subscribe().unwrap();
    let mut original = consumer::<TestEngine>(binding.clone(), frame_bytes);
    bootstrap(&producer, first, &mut original, frame_bytes);
    let saved = BoundCheckpoint::new(
        binding.clone(),
        original.cursor(),
        original.revision(),
        original.session().checkpoint_bytes().unwrap(),
    )
    .unwrap()
    .encode()
    .unwrap();
    producer.unsubscribe(first).unwrap();
    producer.publish_output(b"-while-detached").unwrap();

    let mut resumed = consumer::<TestEngine>(binding.clone(), frame_bytes);
    resumed.restore_checkpoint_for_reconnect(&saved).unwrap();
    assert_eq!(resumed.state(), ConsumerState::AwaitingBootstrap);
    assert_eq!(resumed.session().engine().bytes, b"before");
    // Even an otherwise well-ordered output frame cannot bypass bootstrap.
    assert!(matches!(
        resumed.accept_message(StreamMessage::Output {
            meta: MessageMeta {
                binding: binding.clone(),
                cursor: original.cursor(),
                revision: original.revision() + 2
            },
            part_index: 0,
            part_count: 1,
            bytes: b"-while-detached".to_vec(),
        }),
        Err(coven_terminal_checkpoint_transport::ConsumerError::ExpectedBootstrap)
    ));
    assert_eq!(resumed.session().engine().bytes, b"before");
    let second = producer.subscribe().unwrap();
    bootstrap(&producer, second, &mut resumed, frame_bytes);
    assert!(resumed.cursor() > original.cursor());
    assert_eq!(resumed.session().engine().bytes, b"before-while-detached");
    let latest = BoundCheckpoint::new(
        binding.clone(),
        resumed.cursor(),
        resumed.revision(),
        resumed.session().checkpoint_bytes().unwrap(),
    )
    .unwrap()
    .encode()
    .unwrap();
    resumed.restore_checkpoint_for_reconnect(&latest).unwrap();
    assert!(matches!(
        resumed.accept_message(StreamMessage::CheckpointStart {
            meta: MessageMeta {
                binding,
                cursor: original.cursor(),
                revision: original.revision()
            },
            total_bytes: saved.len() as u32,
            part_count: 1,
            digest: BoundCheckpoint::digest(&saved),
        }),
        Err(coven_terminal_checkpoint_transport::ConsumerError::StaleCheckpoint { .. })
    ));
    assert_eq!(resumed.session().engine().bytes, b"before-while-detached");
}

#[test]
fn inner_checkpoint_ticket_is_checked_before_restore() {
    let binding = binding();
    let runtime = coven_terminal_checkpoint_transport::TerminalSession::<TestEngine>::new(
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        64 * 1024,
        256,
    )
    .unwrap();
    let checkpoint = runtime.checkpoint_bytes().unwrap();
    let mut inner: serde_json::Value = serde_json::from_slice(&checkpoint).unwrap();
    inner["nextSequence"] = serde_json::json!(9_u64);
    inner["nextOffset"] = serde_json::json!(99_u64);
    inner["revision"] = serde_json::json!(2_u64);
    let forged_inner = serde_json::to_vec(&inner).unwrap();
    let encoded = BoundCheckpoint::new(binding.clone(), SessionCursor::START, 0, forged_inner)
        .unwrap()
        .encode()
        .unwrap();
    let encoded_len = encoded.len();
    let digest = BoundCheckpoint::digest(&encoded);
    let mut persisted_consumer = consumer::<TestEngine>(binding.clone(), 256);
    let persisted_before = persisted_consumer.session().checkpoint_bytes().unwrap();
    assert!(matches!(
        persisted_consumer.restore_checkpoint_bytes(&encoded),
        Err(coven_terminal_checkpoint_transport::ConsumerError::CheckpointBindingMismatch)
    ));
    assert_eq!(
        persisted_consumer.session().checkpoint_bytes().unwrap(),
        persisted_before
    );
    let mut consumer = consumer::<TestEngine>(binding.clone(), 256);
    let meta = MessageMeta {
        binding,
        cursor: SessionCursor::START,
        revision: 0,
    };
    let before = consumer.session().checkpoint_bytes().unwrap();
    consumer
        .accept_message(StreamMessage::CheckpointStart {
            meta: meta.clone(),
            total_bytes: encoded.len() as u32,
            part_count: 1,
            digest,
        })
        .unwrap();
    consumer
        .accept_message(StreamMessage::CheckpointPart {
            meta: meta.clone(),
            part_index: 0,
            part_count: 1,
            bytes: encoded,
        })
        .unwrap();
    assert!(matches!(
        consumer.accept_message(StreamMessage::CheckpointEnd {
            meta,
            total_bytes: encoded_len as u32,
            part_count: 1,
            digest,
        }),
        Err(coven_terminal_checkpoint_transport::ConsumerError::CheckpointBindingMismatch)
    ));
    assert_eq!(consumer.session().checkpoint_bytes().unwrap(), before);
    assert_eq!(consumer.state(), ConsumerState::Gap);
}

#[test]
fn binding_is_send_sync_for_shared_owner_handles() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StreamBinding>();
    assert_send_sync::<Arc<StreamBinding>>();
}
