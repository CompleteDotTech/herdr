#![cfg(feature = "alacritty-engine")]

use coven_terminal::alacritty::AlacrittyEngine;
use coven_terminal_checkpoint_transport::{
    ConsumerError, ConsumerState, Geometry, ProducerLimits, QueryReplyPolicy, SessionCursor,
    StreamBinding, StreamMessage, TerminalStreamConsumer, TerminalStreamProducer,
    MAX_RAW_CHUNK_BYTES,
};

fn binding() -> StreamBinding {
    StreamBinding::new("actual-alacritty", [19; 16], 7, 9, 13).unwrap()
}

fn bootstrap(
    producer: &TerminalStreamProducer<AlacrittyEngine>,
    subscriber: coven_terminal_checkpoint_transport::SubscriberId,
    consumer: &mut TerminalStreamConsumer<AlacrittyEngine>,
    max_frame_bytes: usize,
) {
    while let Some(message) = producer.poll(subscriber).unwrap() {
        for frame in message.encode_parts(max_frame_bytes).unwrap() {
            consumer.accept_encoded(&frame).unwrap();
        }
    }
    assert_eq!(consumer.state(), ConsumerState::Live);
}

fn unterminated_osc_prefix() -> Vec<u8> {
    // Leave the parser two bytes below the fork's one MiB OSC limit. The
    // following three byte frame fills the limit and then supplies the
    // discarded byte that makes overflow sticky. Each provider frame remains
    // within the shared one MiB input bound.
    let mut bytes = b"\x1b]".to_vec();
    bytes.resize(MAX_RAW_CHUNK_BYTES, b'x');
    bytes
}

#[test]
fn actual_alacritty_consumer_poison_requires_checkpoint_replacement() {
    let binding = binding();
    let max_frame_bytes = 64 * 1024;
    let producer = TerminalStreamProducer::<AlacrittyEngine>::from_geometry(
        binding.clone(),
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        2 * MAX_RAW_CHUNK_BYTES,
        256,
        ProducerLimits::default(),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let mut consumer = TerminalStreamConsumer::<AlacrittyEngine>::from_geometry(
        binding,
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        2 * MAX_RAW_CHUNK_BYTES,
        256,
        max_frame_bytes,
    )
    .unwrap();
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);

    let prefix = unterminated_osc_prefix();
    // Commit the bounded prefix through the producer first, so the producer's
    // replacement checkpoint is anchored after the same parser continuation
    // that the consumer has accepted.
    producer.publish_output(&prefix).unwrap();
    let first = producer.poll(subscriber).unwrap().unwrap();
    for frame in first.encode_parts(max_frame_bytes).unwrap() {
        consumer.accept_encoded(&frame).unwrap();
    }
    let poison = StreamMessage::Output {
        meta: coven_terminal_checkpoint_transport::MessageMeta {
            binding: consumer.binding().clone(),
            cursor: SessionCursor {
                sequence: 2,
                offset: prefix.len() as u64,
            },
            revision: 4,
        },
        part_index: 0,
        part_count: 1,
        bytes: b"xxx".to_vec(),
    };
    assert!(matches!(
        consumer.accept_message(poison),
        Err(ConsumerError::Apply(_))
    ));
    assert_eq!(consumer.state(), ConsumerState::Gap);

    // The producer never consumed the malformed direct message, so its
    // checkpoint at the committed prefix is still a valid replacement for
    // the poisoned consumer engine.
    producer.request_bootstrap(subscriber).unwrap();
    bootstrap(&producer, subscriber, &mut consumer, max_frame_bytes);
    assert_eq!(consumer.state(), ConsumerState::Live);
    assert_eq!(
        consumer.cursor(),
        SessionCursor {
            sequence: 2,
            offset: prefix.len() as u64,
        }
    );
}

#[test]
fn actual_alacritty_producer_poison_is_failed_closed() {
    let binding = binding();
    let producer = TerminalStreamProducer::<AlacrittyEngine>::from_geometry(
        binding,
        Geometry::new(20, 4),
        QueryReplyPolicy::Quiet,
        2 * MAX_RAW_CHUNK_BYTES,
        256,
        ProducerLimits::default(),
    )
    .unwrap();
    let subscriber = producer.subscribe().unwrap();
    let prefix = unterminated_osc_prefix();
    producer.publish_output(&prefix).unwrap();
    assert!(matches!(
        producer.publish_output(b"xxx"),
        Err(coven_terminal_checkpoint_transport::ProducerError::Apply(
            coven_terminal_checkpoint_transport::ApplyError::Engine(_)
        ))
    ));
    assert_eq!(
        producer.status().unwrap(),
        coven_terminal_checkpoint_transport::ProducerStatus::Failed
    );
    let gap = producer.poll(subscriber).unwrap().unwrap();
    assert!(matches!(
        gap,
        StreamMessage::Gap {
            reason: coven_terminal_checkpoint_transport::GapReason::ProtocolReset,
            ..
        }
    ));
}
