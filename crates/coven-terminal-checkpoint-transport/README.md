# Checkpoint aware terminal transport draft

This crate is an isolated implementation draft for the shared external
terminal owner. It depends on the sibling `coven-terminal-runtime-v1` draft,
which owns the model, parser or processor checkpoint, ordered replay window,
and stable even revisions.

The transport has two negotiated codecs:

* `RawBytesV1` remains protocol version 1 and is intentionally outside this
  crate's version 2 framing.
* `CheckpointV1` is protocol version 2. It sends a complete bound checkpoint,
  ordered raw output, explicit resize and synchronized-flush events, typed
  gaps, and close events.

`StreamBinding` carries the authenticated session id plus stream id, stream
generation, execution generation, and authority epoch. The binary envelope
and every `CTS2` frame repeat the opaque generation fields. The session id is
resolved by the authenticated attach route and is kept in the decoded binding.
An old stream or a reused session id therefore cannot be accepted solely from
an apparently valid byte cursor.

`CodecNegotiation` can carry a `control` capability request alongside
`observe` when one coherent attachment is used for terminal control. Selecting
the codec does not grant that capability: the authenticated owner attach route
must validate the source/controller incarnation and grant the independent
single-writer lease before accepting control input. A denied control request
must fail at that owner boundary rather than silently downgrading the stream.

The producer owns one `TerminalSession` behind one mutex. `publish_output`,
`publish_resize`, and `publish_flush` enter the same ordered frame path. A
flush is an explicit owner timer boundary: it consumes one sequence and
revision without provider bytes and lets synchronized-output engines settle
expired parser state while quiet consumers replay the same transition.
The owner should call `publish_expired_flush` after an ingress timeout and
before admitting output or resize; it returns `Some(report)` only when the
engine's synchronized-update deadline is due, and performs the due check and
flush under the same ordering lock.
Subscriber queues are bounded; overflow clears the tail and emits
`GapReason::SlowConsumer` with a cursor/revision ticket. Output, resize, and
flush events are suppressed while a gap is pending or while a subscriber
awaits bootstrap. A subscriber must then call `request_bootstrap`, which uses
a cached checkpoint when the stable revision has not changed. Polling and
consumer idle operation never query the model.

An engine error is treated as terminal at this transport boundary because the
runtime contract permits an engine to return an error after it has discarded
input and poisoned its parser. The producer enters `ProducerStatus::Failed`,
drops its checkpoint cache, rejects new publishes/subscriptions, and queues a
`ProtocolReset` gap followed by a close ticket for existing subscribers. An
owner that has already committed a model frame but cannot enqueue its trusted
query effects must call `fail` (or its `poison` alias) after releasing the
writer/controller lock; this clears queued live output, records the reason,
and applies the same terminal barrier. A snapshot failure follows the same
fail-closed path, so a stale checkpoint is never reused after an unavailable
engine.

Checkpoint transfer is multipart and bounded. `max_frame_bytes` must be at
least `MIN_CHECKPOINT_FRAME_BYTES` so checkpoint start and end controls fit;
the advertised part count must also be large enough for the configured frame
payload capacity. Each part is checked before it is copied, the complete
envelope is SHA-256 checked, and the shared runtime restores the model and
processor atomically. The consumer accepts output only at its current cursor
and next even revision. A resize consumes a sequence but does not advance the
raw byte offset. Any semantic or engine error moves the consumer to `Gap`;
callers must establish a complete checkpoint before live events are accepted
again. Checkpoint tickets below the gap floor and close tickets below the
gap's advertised terminal ticket are rejected.

One encoded input call is limited to `MAX_MESSAGES_PER_PUSH` complete frames.
This bounds decoded message and event vectors; a semantic error may have
already applied earlier frames from that call, and the resulting `Gap` state is
the acknowledgement that a replacement checkpoint is required. The producer
caps attachment count with `MAX_PRODUCER_SUBSCRIBERS` and bounds each
attachment by its configured byte and message limits. Those per-attachment
caps provide a finite aggregate bound for this transport; a deployment that
requires a tighter producer-wide byte budget must impose that quota at its
owner boundary.

Consumers are renderer-side observers and require `QueryReplyPolicy::Quiet`.
The session accessor is immutable, and query replies are never collected and
then silently discarded by this coordinator. `finish` is the explicit stream
lifecycle boundary: it succeeds only after a `Close` message, and the frame
reader rejects further input after a successful finish.

The public handoff surface is:

```rust
let subscriber = producer.subscribe()?;
let mut consumer = TerminalStreamConsumer::from_geometry(/* ... */)?;

while let Some(message) = producer.poll(subscriber)? {
    for frame in message.encode_parts(max_frame_bytes)? {
        consumer.accept_encoded(&frame)?;
    }
}
```

An attach route that persists or returns bootstrap metadata should call
`subscribe_with_receipt()` instead. Its `SubscriberReceipt` captures the
subscriber id, cursor, revision, and geometry for that exact checkpoint while
the producer lock is held; separate metadata reads can race a subsequent
publish.

The owner integration still needs to supply authenticated attach and session
identity allocation, route this codec beside the existing raw observer codec,
persist the bound runtime checkpoint with the provider cursor, and connect the
consumer's read snapshot to the external renderer. The draft does not alter
native Ghostty paths or claim that normalized transcript text is terminal
state.

Validation from this directory:

```text
cargo test --offline --all-targets
cargo clippy --offline --all-targets -- -D warnings
```

The integration tests cover raw byte preservation across incremental multipart
frames, binding and digest rejection, atomic revision checks, cache behavior,
slow subscriber resynchronization, close after dropped output, and the quiet
side effect policy.
