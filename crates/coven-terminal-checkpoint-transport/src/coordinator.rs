//! Single-owner producer and checkpoint-restoring consumer.

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use coven_terminal::{
    ApplyError, FullTerminalCheckpoint, Geometry, QueryReplyPolicy, RawChunk, ReplayEvent,
    ReplayFrame, SessionCursor, SessionError, TerminalEngine, TerminalSession, MAX_RAW_CHUNK_BYTES,
    REVISION_STEP,
};

use crate::{
    BoundCheckpoint, CheckpointBlobError, CheckpointFrameReader, GapReason, MessageMeta,
    StreamBinding, StreamMessage, WireCodecError, CHECKPOINT_FRAME_HEADER_BYTES,
    DEFAULT_MAX_FRAME_BYTES, MAX_BOUND_CHECKPOINT_BYTES, MAX_CHECKPOINT_FRAME_BYTES,
    MAX_CHECKPOINT_PARTS, MAX_MESSAGES_PER_PUSH, MIN_CHECKPOINT_FRAME_BYTES,
};

/// Maximum number of bytes retained in one subscriber's pending event tail by
/// default. A slow subscriber receives a typed gap once this bound is crossed.
pub const DEFAULT_MAX_SUBSCRIBER_BYTES: usize = 2 * 1024 * 1024;
/// Maximum number of queued logical events per subscriber by default.
pub const DEFAULT_MAX_SUBSCRIBER_MESSAGES: usize = 2_048;
/// Hard cap on attachments held by one producer. This bounds fan-out and
/// aggregate queue state when an outer attach service has no quota.
pub const MAX_PRODUCER_SUBSCRIBERS: usize = 1_024;

/// Bounded producer/attachment settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerLimits {
    pub max_frame_bytes: usize,
    pub max_subscriber_bytes: usize,
    pub max_subscriber_messages: usize,
}

impl Default for ProducerLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_subscriber_bytes: DEFAULT_MAX_SUBSCRIBER_BYTES,
            max_subscriber_messages: DEFAULT_MAX_SUBSCRIBER_MESSAGES,
        }
    }
}

impl ProducerLimits {
    pub fn validate(self) -> Result<(), ProducerLimitError> {
        if !(MIN_CHECKPOINT_FRAME_BYTES..=MAX_CHECKPOINT_FRAME_BYTES)
            .contains(&self.max_frame_bytes)
        {
            return Err(ProducerLimitError::FrameLimit);
        }
        // Gap metadata is held as the mode ticket rather than in the byte
        // queue, so the queue bound only needs to admit the largest queued
        // ordinary control event plus its frame header.
        if self.max_subscriber_bytes < CHECKPOINT_FRAME_HEADER_BYTES + 17
            || self.max_subscriber_bytes > MAX_BOUND_CHECKPOINT_BYTES
        {
            return Err(ProducerLimitError::SubscriberBytes);
        }
        if self.max_subscriber_messages == 0 || self.max_subscriber_messages > 65_536 {
            return Err(ProducerLimitError::SubscriberMessages);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerLimitError {
    FrameLimit,
    SubscriberBytes,
    SubscriberMessages,
}

impl fmt::Display for ProducerLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameLimit => formatter.write_str("checkpoint frame limit is invalid"),
            Self::SubscriberBytes => formatter.write_str("subscriber byte limit is invalid"),
            Self::SubscriberMessages => formatter.write_str("subscriber message limit is invalid"),
        }
    }
}

impl Error for ProducerLimitError {}

/// Producer lifecycle visible to the attach layer. A failed producer has
/// observed an engine or snapshot error and cannot advertise its old model as
/// live; replacement must begin from a known-good checkpoint/session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerStatus {
    Live,
    Closed,
    Failed,
}

/// An opaque attachment id local to one producer instance.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SubscriberId(u64);

impl SubscriberId {
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The atomically captured identity and bootstrap ticket for a new
/// subscription. The cursor, revision, and geometry describe the checkpoint
/// transfer installed on the returned subscriber under the same producer lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriberReceipt {
    pub subscriber: SubscriberId,
    pub cursor: SessionCursor,
    pub revision: u64,
    pub geometry: Geometry,
}

impl SubscriberReceipt {
    pub const fn id(self) -> SubscriberId {
        self.subscriber
    }
}

#[derive(Clone, Debug)]
struct CachedCheckpoint {
    revision: u64,
    cursor: SessionCursor,
    bytes: Arc<Vec<u8>>,
    digest: [u8; 32],
    part_count: u32,
}

#[derive(Clone, Debug)]
struct CheckpointTransfer {
    revision: u64,
    cursor: SessionCursor,
    bytes: Arc<Vec<u8>>,
    digest: [u8; 32],
    part_count: u32,
    start_emitted: bool,
    next_part: u32,
}

impl CheckpointTransfer {
    fn from_cached(cached: &CachedCheckpoint) -> Self {
        Self {
            revision: cached.revision,
            cursor: cached.cursor,
            bytes: Arc::clone(&cached.bytes),
            digest: cached.digest,
            part_count: cached.part_count,
            start_emitted: false,
            next_part: 0,
        }
    }
}

enum SubscriberMode {
    AwaitingBootstrap,
    Bootstrapping(CheckpointTransfer),
    Live,
    GapPending(StreamMessage),
}

struct Subscriber {
    mode: SubscriberMode,
    queue: VecDeque<StreamMessage>,
    queued_bytes: usize,
    next_cursor: SessionCursor,
}

impl Subscriber {
    fn new(transfer: CheckpointTransfer) -> Self {
        Self {
            next_cursor: transfer.cursor,
            mode: SubscriberMode::Bootstrapping(transfer),
            queue: VecDeque::new(),
            queued_bytes: 0,
        }
    }

    fn requested_cursor(&self) -> SessionCursor {
        self.queue
            .front()
            .and_then(|message| message.meta().map(|meta| meta.cursor))
            .unwrap_or(self.next_cursor)
    }

    fn clear_queue(&mut self) {
        self.queue.clear();
        self.queued_bytes = 0;
    }

    fn enter_gap_with_revision(
        &mut self,
        binding: &StreamBinding,
        available: SessionCursor,
        available_revision: u64,
    ) {
        self.enter_gap_with_reason(
            binding,
            available,
            available_revision,
            GapReason::SlowConsumer,
        );
    }

    fn enter_gap_with_reason(
        &mut self,
        binding: &StreamBinding,
        available: SessionCursor,
        available_revision: u64,
        reason: GapReason,
    ) {
        let requested = self.requested_cursor();
        self.clear_queue();
        self.mode = SubscriberMode::GapPending(StreamMessage::Gap {
            binding: binding.clone(),
            requested,
            available,
            available_revision,
            reason,
        });
    }

    fn enqueue(
        &mut self,
        message: StreamMessage,
        binding: &StreamBinding,
        available: SessionCursor,
        available_revision: u64,
        limits: ProducerLimits,
    ) {
        // Once a gap has been announced or is waiting to be announced, the
        // old live tail is unusable. Keep the queue empty until a new
        // checkpoint transfer is explicitly requested.
        if matches!(
            self.mode,
            SubscriberMode::AwaitingBootstrap | SubscriberMode::GapPending(_)
        ) {
            return;
        }
        let bytes = message.estimated_bytes();
        if bytes > limits.max_subscriber_bytes
            || self.queue.len() >= limits.max_subscriber_messages
            || self.queued_bytes.saturating_add(bytes) > limits.max_subscriber_bytes
        {
            self.enter_gap_with_revision(binding, available, available_revision);
            return;
        }
        self.queued_bytes = self.queued_bytes.saturating_add(bytes);
        self.queue.push_back(message);
    }

    fn enqueue_close(
        &mut self,
        message: StreamMessage,
        binding: &StreamBinding,
        available: SessionCursor,
        available_revision: u64,
        limits: ProducerLimits,
    ) {
        let bytes = message.estimated_bytes();
        if bytes > limits.max_subscriber_bytes
            || self.queue.len() >= limits.max_subscriber_messages
            || self.queued_bytes.saturating_add(bytes) > limits.max_subscriber_bytes
        {
            // A close after dropped events must carry an explicit gap first;
            // otherwise its current cursor would be impossible for a slow
            // consumer to apply. The close is retained behind that gap.
            if !matches!(self.mode, SubscriberMode::GapPending(_)) {
                self.enter_gap_with_revision(binding, available, available_revision);
            }
        }
        self.queued_bytes = self.queued_bytes.saturating_add(bytes);
        self.queue.push_back(message);
    }
}

fn message_end_cursor(message: &StreamMessage) -> Option<SessionCursor> {
    let meta = message.meta()?;
    if !matches!(
        message,
        StreamMessage::Output { .. }
            | StreamMessage::Resize { .. }
            | StreamMessage::SyncFlush { .. }
    ) {
        return None;
    }
    let byte_len = match message {
        StreamMessage::Output { bytes, .. } => bytes.len() as u64,
        StreamMessage::Resize { .. } | StreamMessage::SyncFlush { .. } => 0,
        _ => return None,
    };
    Some(SessionCursor {
        sequence: meta.cursor.sequence.checked_add(1)?,
        offset: meta.cursor.offset.checked_add(byte_len)?,
    })
}

struct ProducerState<M: TerminalEngine> {
    session: TerminalSession<M>,
    limits: ProducerLimits,
    subscribers: BTreeMap<SubscriberId, Subscriber>,
    next_subscriber: u64,
    checkpoint_cache: Option<CachedCheckpoint>,
    checkpoint_queries: u64,
    status: ProducerStatus,
    failure_reason: Option<String>,
}

type ProducerStateGuard<'a, M> = std::sync::MutexGuard<'a, ProducerState<M>>;

/// The only writer of a shared terminal model. All output and resize calls
/// take this same lock, so subscriber event order is exactly model mutation
/// order.
#[derive(Clone)]
pub struct TerminalStreamProducer<M: TerminalEngine> {
    binding: StreamBinding,
    state: Arc<Mutex<ProducerState<M>>>,
}

impl<M: TerminalEngine> TerminalStreamProducer<M> {
    pub fn new(
        binding: StreamBinding,
        session: TerminalSession<M>,
        limits: ProducerLimits,
    ) -> Result<Self, ProducerError<M::Error, M::SnapshotError>> {
        binding.validate().map_err(ProducerError::Binding)?;
        limits.validate().map_err(ProducerError::Limits)?;
        Ok(Self {
            binding,
            state: Arc::new(Mutex::new(ProducerState {
                session,
                limits,
                subscribers: BTreeMap::new(),
                next_subscriber: 1,
                checkpoint_cache: None,
                checkpoint_queries: 0,
                status: ProducerStatus::Live,
                failure_reason: None,
            })),
        })
    }

    pub fn from_geometry(
        binding: StreamBinding,
        geometry: Geometry,
        policy: QueryReplyPolicy,
        max_replay_bytes: usize,
        max_replay_frames: usize,
        limits: ProducerLimits,
    ) -> Result<Self, ProducerError<M::Error, M::SnapshotError>> {
        let session = TerminalSession::new(geometry, policy, max_replay_bytes, max_replay_frames)
            .map_err(ProducerError::Session)?;
        Self::new(binding, session, limits)
    }

    pub fn binding(&self) -> &StreamBinding {
        &self.binding
    }

    pub fn status(&self) -> Result<ProducerStatus, ProducerError<M::Error, M::SnapshotError>> {
        Ok(self.lock()?.status)
    }

    pub fn subscribe(&self) -> Result<SubscriberId, ProducerError<M::Error, M::SnapshotError>> {
        Ok(self.subscribe_with_receipt()?.subscriber)
    }

    /// Subscribe and capture the exact bootstrap ticket while the producer
    /// lock is held. Callers that need to persist or route subscription
    /// metadata must use this method instead of combining `subscribe()` with
    /// later cursor/revision/geometry reads.
    pub fn subscribe_with_receipt(
        &self,
    ) -> Result<SubscriberReceipt, ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        match state.status {
            ProducerStatus::Live => {}
            ProducerStatus::Closed => return Err(ProducerError::Closed),
            ProducerStatus::Failed => return Err(ProducerError::Failed),
        }
        if state.subscribers.len() >= MAX_PRODUCER_SUBSCRIBERS {
            return Err(ProducerError::SubscriberLimit);
        }
        let transfer = {
            let cached = self.cached_checkpoint_locked(&mut state)?;
            CheckpointTransfer::from_cached(&cached)
        };
        let id = SubscriberId(state.next_subscriber);
        state.next_subscriber = state
            .next_subscriber
            .checked_add(1)
            .ok_or(ProducerError::SubscriberExhausted)?;
        let receipt = SubscriberReceipt {
            subscriber: id,
            cursor: transfer.cursor,
            revision: transfer.revision,
            geometry: state.session.geometry(),
        };
        state.subscribers.insert(id, Subscriber::new(transfer));
        Ok(receipt)
    }

    /// Explicitly discard a subscriber's pending tail and begin a fresh full
    /// checkpoint transfer. The snapshot is cached by stable model revision,
    /// so repeated requests while the model is quiet do not query the model a
    /// second time.
    pub fn request_bootstrap(
        &self,
        subscriber: SubscriberId,
    ) -> Result<(), ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        match state.status {
            ProducerStatus::Live => {}
            ProducerStatus::Closed => return Err(ProducerError::Closed),
            ProducerStatus::Failed => return Err(ProducerError::Failed),
        }
        if !state.subscribers.contains_key(&subscriber) {
            return Err(ProducerError::UnknownSubscriber);
        }
        let transfer = {
            let cached = self.cached_checkpoint_locked(&mut state)?;
            CheckpointTransfer::from_cached(&cached)
        };
        let attachment = state
            .subscribers
            .get_mut(&subscriber)
            .ok_or(ProducerError::UnknownSubscriber)?;
        *attachment = Subscriber::new(transfer);
        Ok(())
    }

    /// Poll only already queued transport state. This method never invokes a
    /// model checkpoint or asks the terminal engine to answer a query.
    pub fn poll(
        &self,
        subscriber: SubscriberId,
    ) -> Result<Option<StreamMessage>, ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        let max_frame_bytes = state.limits.max_frame_bytes;
        let attachment = state
            .subscribers
            .get_mut(&subscriber)
            .ok_or(ProducerError::UnknownSubscriber)?;
        match &mut attachment.mode {
            SubscriberMode::AwaitingBootstrap | SubscriberMode::Live => {
                let message = attachment.queue.pop_front();
                if let Some(message) = &message {
                    attachment.queued_bytes = attachment
                        .queued_bytes
                        .saturating_sub(message.estimated_bytes());
                    if matches!(
                        message,
                        StreamMessage::Output { .. }
                            | StreamMessage::Resize { .. }
                            | StreamMessage::SyncFlush { .. }
                    ) {
                        // `meta.cursor` is the event's start cursor. Record
                        // the end cursor once the event has been handed to
                        // the subscriber so a later gap points at the first
                        // event it has not consumed.
                        if let Some(next) = message_end_cursor(message) {
                            attachment.next_cursor = next;
                        }
                    }
                }
                Ok(message)
            }
            SubscriberMode::GapPending(message) => {
                let message = message.clone();
                attachment.mode = SubscriberMode::AwaitingBootstrap;
                Ok(Some(message))
            }
            SubscriberMode::Bootstrapping(transfer) => {
                let message = if !transfer.start_emitted {
                    transfer.start_emitted = true;
                    Some(StreamMessage::CheckpointStart {
                        meta: MessageMeta {
                            binding: self.binding.clone(),
                            cursor: transfer.cursor,
                            revision: transfer.revision,
                        },
                        total_bytes: transfer.bytes.len() as u32,
                        part_count: transfer.part_count,
                        digest: transfer.digest,
                    })
                } else if transfer.next_part < transfer.part_count {
                    let maximum = max_frame_bytes - CHECKPOINT_FRAME_HEADER_BYTES;
                    let start = transfer.next_part as usize * maximum;
                    let end = (start + maximum).min(transfer.bytes.len());
                    let bytes = transfer.bytes[start..end].to_vec();
                    let index = transfer.next_part;
                    transfer.next_part += 1;
                    Some(StreamMessage::CheckpointPart {
                        meta: MessageMeta {
                            binding: self.binding.clone(),
                            cursor: transfer.cursor,
                            revision: transfer.revision,
                        },
                        part_index: index,
                        part_count: transfer.part_count,
                        bytes,
                    })
                } else {
                    let message = StreamMessage::CheckpointEnd {
                        meta: MessageMeta {
                            binding: self.binding.clone(),
                            cursor: transfer.cursor,
                            revision: transfer.revision,
                        },
                        total_bytes: transfer.bytes.len() as u32,
                        part_count: transfer.part_count,
                        digest: transfer.digest,
                    };
                    attachment.next_cursor = transfer.cursor;
                    attachment.mode = SubscriberMode::Live;
                    Some(message)
                };
                Ok(message)
            }
        }
    }

    pub fn unsubscribe(
        &self,
        subscriber: SubscriberId,
    ) -> Result<(), ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        if state.subscribers.remove(&subscriber).is_none() {
            return Err(ProducerError::UnknownSubscriber);
        }
        Ok(())
    }

    pub fn publish_output(
        &self,
        bytes: &[u8],
    ) -> Result<ProducerReport, ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        match state.status {
            ProducerStatus::Live => {}
            ProducerStatus::Closed => return Err(ProducerError::Closed),
            ProducerStatus::Failed => return Err(ProducerError::Failed),
        }
        // Reject before constructing `ReplayFrame`, so an oversized provider
        // buffer cannot force a duplicate allocation before the runtime bound.
        if bytes.is_empty() {
            return Err(ProducerError::Apply(ApplyError::EmptyChunk));
        }
        if bytes.len() > MAX_RAW_CHUNK_BYTES {
            return Err(ProducerError::Apply(ApplyError::ChunkTooLarge {
                actual: bytes.len(),
                maximum: MAX_RAW_CHUNK_BYTES,
            }));
        }
        state.checkpoint_cache = None;
        let cursor = state.session.cursor();
        let frame = ReplayFrame::output(cursor, bytes.to_vec());
        let report = match state.session.apply_frame(frame.clone()) {
            Ok(report) => report,
            Err(error) => {
                if matches!(error, ApplyError::Engine(_)) {
                    self.fail_locked(&mut state, error.to_string());
                }
                return Err(ProducerError::Apply(error));
            }
        };
        let event = StreamMessage::Output {
            meta: MessageMeta {
                binding: self.binding.clone(),
                cursor,
                revision: report.revision,
            },
            part_index: 0,
            part_count: 1,
            bytes: match frame.event {
                ReplayEvent::Output(bytes) => bytes,
                ReplayEvent::Resize(_) | ReplayEvent::SyncFlush => {
                    unreachable!("output frame has output event")
                }
            },
        };
        let available = state.session.cursor();
        let available_revision = state.session.revision();
        let limits = state.limits;
        for attachment in state.subscribers.values_mut() {
            attachment.enqueue(
                event.clone(),
                &self.binding,
                available,
                available_revision,
                limits,
            );
        }
        Ok(ProducerReport {
            cursor: report.cursor,
            revision: report.revision,
            effects: report.effects,
        })
    }

    pub fn publish_resize(
        &self,
        geometry: Geometry,
    ) -> Result<ProducerReport, ProducerError<M::Error, M::SnapshotError>> {
        geometry
            .validate()
            .map_err(ApplyError::InvalidGeometry)
            .map_err(ProducerError::Apply)?;
        let mut state = self.lock()?;
        match state.status {
            ProducerStatus::Live => {}
            ProducerStatus::Closed => return Err(ProducerError::Closed),
            ProducerStatus::Failed => return Err(ProducerError::Failed),
        }
        state.checkpoint_cache = None;
        let cursor = state.session.cursor();
        let frame = ReplayFrame::resize(cursor, geometry);
        let report = match state.session.apply_frame(frame) {
            Ok(report) => report,
            Err(error) => {
                if matches!(error, ApplyError::Engine(_)) {
                    self.fail_locked(&mut state, error.to_string());
                }
                return Err(ProducerError::Apply(error));
            }
        };
        let event = StreamMessage::Resize {
            meta: MessageMeta {
                binding: self.binding.clone(),
                cursor,
                revision: report.revision,
            },
            geometry,
        };
        let available = state.session.cursor();
        let available_revision = state.session.revision();
        let limits = state.limits;
        for attachment in state.subscribers.values_mut() {
            attachment.enqueue(
                event.clone(),
                &self.binding,
                available,
                available_revision,
                limits,
            );
        }
        Ok(ProducerReport {
            cursor: report.cursor,
            revision: report.revision,
            effects: report.effects,
        })
    }

    /// Advance the owner-controlled synchronized-output boundary. The event
    /// consumes one sequence and revision even when the engine has no pending
    /// bytes, so quiet consumers replay the same timer-driven parser history.
    pub fn publish_flush(
        &self,
    ) -> Result<ProducerReport, ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        match state.status {
            ProducerStatus::Live => {}
            ProducerStatus::Closed => return Err(ProducerError::Closed),
            ProducerStatus::Failed => return Err(ProducerError::Failed),
        }
        self.publish_flush_locked(&mut state)
    }

    /// Publish a synchronized-output boundary only when the engine reports
    /// that its timer has elapsed. The due check and the ordered flush happen
    /// under the same producer lock, so a concurrent output or resize cannot
    /// pass the boundary without first seeing the committed `SyncFlush`.
    pub fn publish_expired_flush(
        &self,
    ) -> Result<Option<ProducerReport>, ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        match state.status {
            ProducerStatus::Live => {}
            ProducerStatus::Closed => return Err(ProducerError::Closed),
            ProducerStatus::Failed => return Err(ProducerError::Failed),
        }
        if !state.session.engine().needs_flush() {
            return Ok(None);
        }
        self.publish_flush_locked(&mut state).map(Some)
    }

    fn publish_flush_locked(
        &self,
        state: &mut ProducerState<M>,
    ) -> Result<ProducerReport, ProducerError<M::Error, M::SnapshotError>> {
        state.checkpoint_cache = None;
        let cursor = state.session.cursor();
        let frame = ReplayFrame::sync_flush(cursor);
        let report = match state.session.apply_frame(frame) {
            Ok(report) => report,
            Err(error) => {
                if matches!(error, ApplyError::Engine(_)) {
                    self.fail_locked(state, error.to_string());
                }
                return Err(ProducerError::Apply(error));
            }
        };
        let event = StreamMessage::SyncFlush {
            meta: MessageMeta {
                binding: self.binding.clone(),
                cursor,
                revision: report.revision,
            },
        };
        let available = state.session.cursor();
        let available_revision = state.session.revision();
        let limits = state.limits;
        for attachment in state.subscribers.values_mut() {
            attachment.enqueue(
                event.clone(),
                &self.binding,
                available,
                available_revision,
                limits,
            );
        }
        Ok(ProducerReport {
            cursor: report.cursor,
            revision: report.revision,
            effects: report.effects,
        })
    }

    pub fn close(&self) -> Result<(), ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        if state.status == ProducerStatus::Closed || state.status == ProducerStatus::Failed {
            return Ok(());
        }
        state.status = ProducerStatus::Closed;
        let event = StreamMessage::Close {
            meta: MessageMeta {
                binding: self.binding.clone(),
                cursor: state.session.cursor(),
                revision: state.session.revision(),
            },
        };
        let available = state.session.cursor();
        let available_revision = state.session.revision();
        let limits = state.limits;
        for attachment in state.subscribers.values_mut() {
            attachment.enqueue_close(
                event.clone(),
                &self.binding,
                available,
                available_revision,
                limits,
            );
        }
        Ok(())
    }

    /// Permanently fail this producer after an owner-side admission failure,
    /// such as a trusted query-effect writer rejecting effects from a frame
    /// that was already applied to the terminal model. The lock is held only
    /// while the bounded subscriber state is updated; callers must invoke this
    /// after releasing any model, controller, or writer lock.
    pub fn fail(
        &self,
        reason: impl Into<String>,
    ) -> Result<(), ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        self.fail_locked(&mut state, reason.into());
        Ok(())
    }

    /// Alias for [`Self::fail`] that names the terminal state transition.
    pub fn poison(
        &self,
        reason: impl Into<String>,
    ) -> Result<(), ProducerError<M::Error, M::SnapshotError>> {
        self.fail(reason)
    }

    pub fn revision(&self) -> Result<u64, ProducerError<M::Error, M::SnapshotError>> {
        Ok(self.lock()?.session.revision())
    }

    pub fn cursor(&self) -> Result<SessionCursor, ProducerError<M::Error, M::SnapshotError>> {
        Ok(self.lock()?.session.cursor())
    }

    pub fn checkpoint_query_count(&self) -> Result<u64, ProducerError<M::Error, M::SnapshotError>> {
        Ok(self.lock()?.checkpoint_queries)
    }

    /// Return the diagnostic supplied when the producer was failed, if any.
    pub fn failure_reason(
        &self,
    ) -> Result<Option<String>, ProducerError<M::Error, M::SnapshotError>> {
        Ok(self.lock()?.failure_reason.clone())
    }

    pub fn session_checkpoint(&self) -> Result<Vec<u8>, ProducerError<M::Error, M::SnapshotError>> {
        let mut state = self.lock()?;
        if state.status == ProducerStatus::Failed {
            return Err(ProducerError::Failed);
        }
        let cached = self.cached_checkpoint_locked(&mut state)?;
        Ok(cached.bytes.as_ref().clone())
    }

    fn cached_checkpoint_locked(
        &self,
        state: &mut ProducerState<M>,
    ) -> Result<CachedCheckpoint, ProducerError<M::Error, M::SnapshotError>> {
        let revision = state.session.revision();
        let cursor = state.session.cursor();
        if let Some(cached) = &state.checkpoint_cache {
            if cached.revision == revision && cached.cursor == cursor {
                return Ok(cached.clone());
            }
        }
        let runtime_bytes = match state.session.checkpoint_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.fail_locked(state, error.to_string());
                return Err(ProducerError::Session(error));
            }
        };
        let bound =
            match BoundCheckpoint::new(self.binding.clone(), cursor, revision, runtime_bytes) {
                Ok(bound) => bound,
                Err(error) => {
                    self.fail_locked(state, error.to_string());
                    return Err(ProducerError::Checkpoint(error));
                }
            };
        let bytes = match bound.encode() {
            Ok(bytes) => Arc::new(bytes),
            Err(error) => {
                self.fail_locked(state, error.to_string());
                return Err(ProducerError::Checkpoint(error));
            }
        };
        let digest = BoundCheckpoint::digest(bytes.as_ref());
        let maximum = state.limits.max_frame_bytes - CHECKPOINT_FRAME_HEADER_BYTES;
        let part_count = bytes.len().checked_add(maximum - 1).ok_or_else(|| {
            self.fail_locked(state, "checkpoint part count overflow");
            ProducerError::CheckpointTooLarge
        })? / maximum;
        let part_count = match u32::try_from(part_count) {
            Ok(part_count) => part_count,
            Err(_) => {
                self.fail_locked(state, "checkpoint part count exceeds u32");
                return Err(ProducerError::CheckpointTooLarge);
            }
        };
        if part_count == 0 || part_count > MAX_CHECKPOINT_PARTS {
            self.fail_locked(state, "checkpoint part count exceeds transport bound");
            return Err(ProducerError::CheckpointTooLarge);
        }
        let cached = CachedCheckpoint {
            revision,
            cursor,
            bytes,
            digest,
            part_count,
        };
        state.checkpoint_queries = state.checkpoint_queries.saturating_add(1);
        state.checkpoint_cache = Some(cached.clone());
        Ok(cached)
    }

    /// Mark the producer unusable after a runtime or snapshot failure. Every
    /// existing attachment receives one protocol-reset gap followed by a
    /// close ticket, so a live consumer cannot wait forever at a poisoned
    /// cursor. The cache is always dropped before this method returns.
    fn fail_locked(&self, state: &mut ProducerState<M>, reason: impl Into<String>) {
        if state.status == ProducerStatus::Failed {
            return;
        }
        state.status = ProducerStatus::Failed;
        state.failure_reason = Some(reason.into());
        state.checkpoint_cache = None;
        let available = state.session.cursor();
        let available_revision = state.session.revision();
        let close = StreamMessage::Close {
            meta: MessageMeta {
                binding: self.binding.clone(),
                cursor: available,
                revision: available_revision,
            },
        };
        for attachment in state.subscribers.values_mut() {
            attachment.enter_gap_with_reason(
                &self.binding,
                available,
                available_revision,
                GapReason::ProtocolReset,
            );
            attachment.enqueue_close(
                close.clone(),
                &self.binding,
                available,
                available_revision,
                state.limits,
            );
        }
    }

    fn lock(&self) -> Result<ProducerStateGuard<'_, M>, ProducerError<M::Error, M::SnapshotError>> {
        self.state.lock().map_err(|_| ProducerError::Poisoned)
    }
}

/// Stable producer report; raw effects are retained for an owner that elects
/// to answer queries, while quiet consumers simply ignore them.
#[derive(Clone, Debug)]
pub struct ProducerReport {
    pub cursor: SessionCursor,
    pub revision: u64,
    pub effects: Vec<coven_terminal::OrderedEffect>,
}

#[derive(Debug)]
pub enum ProducerError<E, S> {
    Binding(crate::binding::BindingError),
    Limits(ProducerLimitError),
    Closed,
    Failed,
    UnknownSubscriber,
    SubscriberLimit,
    SubscriberExhausted,
    Apply(ApplyError<E>),
    Session(SessionError<E, S>),
    Checkpoint(CheckpointBlobError),
    CheckpointTooLarge,
    Poisoned,
}

impl<E: fmt::Display, S: fmt::Display> fmt::Display for ProducerError<E, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => error.fmt(formatter),
            Self::Limits(error) => error.fmt(formatter),
            Self::Closed => formatter.write_str("terminal producer is closed"),
            Self::Failed => formatter.write_str("terminal producer has failed closed"),
            Self::UnknownSubscriber => formatter.write_str("terminal subscriber is unknown"),
            Self::SubscriberLimit => formatter.write_str("terminal subscriber limit reached"),
            Self::SubscriberExhausted => formatter.write_str("terminal subscriber id exhausted"),
            Self::Apply(error) => error.fmt(formatter),
            Self::Session(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::CheckpointTooLarge => formatter.write_str("terminal checkpoint is too large"),
            Self::Poisoned => formatter.write_str("terminal producer state is poisoned"),
        }
    }
}

impl<E: Error + 'static, S: Error + 'static> Error for ProducerError<E, S> {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerState {
    AwaitingBootstrap,
    Live,
    Gap,
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerEvent {
    Bootstrapped {
        cursor: SessionCursor,
        revision: u64,
    },
    Applied {
        cursor: SessionCursor,
        revision: u64,
    },
    Gap {
        requested: SessionCursor,
        available: SessionCursor,
        available_revision: u64,
        reason: GapReason,
    },
    Closed,
}

struct CheckpointAssembly {
    meta: MessageMeta,
    total_bytes: usize,
    part_count: u32,
    digest: [u8; 32],
    next_part: u32,
    bytes: Vec<u8>,
}

struct OutputAssembly {
    meta: MessageMeta,
    part_count: u32,
    next_part: u32,
    bytes: Vec<u8>,
}

/// Consumer-side coordinator. It restores a complete checkpoint into the
/// existing session only after the bound blob, digest, cursor, and runtime
/// model/parser validators all succeed.
pub struct TerminalStreamConsumer<M: TerminalEngine> {
    binding: StreamBinding,
    session: TerminalSession<M>,
    policy: QueryReplyPolicy,
    state: ConsumerState,
    checkpoint: Option<CheckpointAssembly>,
    output: Option<OutputAssembly>,
    reader: CheckpointFrameReader,
    /// A checkpoint accepted after a gap must be at or beyond this ticket.
    /// This prevents delayed frames from rolling the renderer back within an
    /// otherwise unchanged generation binding.
    resync_floor: (SessionCursor, u64),
    gap_ticket: Option<(SessionCursor, u64)>,
}

impl<M: TerminalEngine> TerminalStreamConsumer<M> {
    pub fn new(
        binding: StreamBinding,
        session: TerminalSession<M>,
        max_frame_bytes: usize,
    ) -> Result<Self, ConsumerError<M::Error, M::SnapshotError>> {
        binding.validate().map_err(ConsumerError::Binding)?;
        let policy = session.query_policy();
        if policy != QueryReplyPolicy::Quiet {
            return Err(ConsumerError::OwnerPolicyNotAllowed);
        }
        let reader = CheckpointFrameReader::new(binding.clone(), max_frame_bytes)
            .map_err(ConsumerError::Wire)?;
        Ok(Self {
            binding,
            session,
            policy,
            state: ConsumerState::AwaitingBootstrap,
            checkpoint: None,
            output: None,
            reader,
            resync_floor: (SessionCursor::START, 0),
            gap_ticket: None,
        })
    }

    pub fn from_geometry(
        binding: StreamBinding,
        geometry: Geometry,
        policy: QueryReplyPolicy,
        max_replay_bytes: usize,
        max_replay_frames: usize,
        max_frame_bytes: usize,
    ) -> Result<Self, ConsumerError<M::Error, M::SnapshotError>> {
        if policy != QueryReplyPolicy::Quiet {
            return Err(ConsumerError::OwnerPolicyNotAllowed);
        }
        let session = TerminalSession::new(geometry, policy, max_replay_bytes, max_replay_frames)
            .map_err(ConsumerError::Session)?;
        Self::new(binding, session, max_frame_bytes)
    }

    pub fn binding(&self) -> &StreamBinding {
        &self.binding
    }

    pub fn state(&self) -> ConsumerState {
        self.state
    }

    pub fn cursor(&self) -> SessionCursor {
        self.session.cursor()
    }

    pub fn revision(&self) -> u64 {
        self.session.revision()
    }

    pub fn geometry(&self) -> Geometry {
        self.session.geometry()
    }

    pub fn session(&self) -> &TerminalSession<M> {
        &self.session
    }

    /// Restore a persisted identity-bound checkpoint before consuming a
    /// reconnecting stream. The replacement is validated by the transport
    /// envelope and the shared runtime before this consumer is marked live;
    /// malformed persistence therefore cannot partially advance the model.
    pub fn restore_checkpoint_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<ConsumerEvent, ConsumerError<M::Error, M::SnapshotError>> {
        if self.state == ConsumerState::Closed {
            return Err(ConsumerError::Closed);
        }
        let bound =
            BoundCheckpoint::decode_for(bytes, &self.binding).map_err(ConsumerError::Checkpoint)?;
        if self.checkpoint.is_some() || self.output.is_some() {
            return Err(ConsumerError::UnexpectedCheckpoint);
        }
        let checkpoint =
            FullTerminalCheckpoint::<M::ModelCheckpoint, M::ProcessorCheckpoint>::decode_bounded(
                &bound.checkpoint_bytes,
            )
            .map_err(|error| ConsumerError::Session(SessionError::Decode(error)))?;
        if checkpoint.cursor() != bound.cursor || checkpoint.revision != bound.revision {
            return Err(ConsumerError::CheckpointBindingMismatch);
        }
        self.session
            .restore_checkpoint(&checkpoint, self.policy)
            .map_err(ConsumerError::Session)?;
        if self.session.cursor() != bound.cursor || self.session.revision() != bound.revision {
            return Err(ConsumerError::CheckpointBindingMismatch);
        }
        self.resync_floor = (bound.cursor, bound.revision);
        self.gap_ticket = None;
        self.state = ConsumerState::Live;
        Ok(ConsumerEvent::Bootstrapped {
            cursor: bound.cursor,
            revision: bound.revision,
        })
    }

    /// Restore the last saved model while waiting for a fresh owner bootstrap.
    /// The saved cursor and revision remain the rollback floor, and raw output
    /// cannot advance the model until the complete new checkpoint is accepted.
    /// Use this when attach returns a checkpoint rather than replay from the
    /// exact persisted ticket.
    pub fn restore_checkpoint_for_reconnect(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), ConsumerError<M::Error, M::SnapshotError>> {
        self.restore_checkpoint_bytes(bytes)?;
        self.state = ConsumerState::AwaitingBootstrap;
        Ok(())
    }

    /// Feed arbitrary transport bytes. The frame reader enforces the header
    /// and payload limits before this method receives a typed message.
    pub fn accept_encoded(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        if self.state == ConsumerState::Closed {
            return Err(ConsumerError::Closed);
        }
        let mut messages = VecDeque::new();
        if let Err(error) = self.reader.push(bytes, &mut messages) {
            self.enter_gap();
            return Err(ConsumerError::Wire(error));
        }
        let mut events = Vec::new();
        // `CheckpointFrameReader` enforces this same bound. Keep the result
        // vector bounded as an explicit coordinator invariant in case a
        // future reader implementation changes its batching policy.
        debug_assert!(messages.len() <= MAX_MESSAGES_PER_PUSH);
        while let Some(message) = messages.pop_front() {
            match self.accept_message(message) {
                Ok(Some(event)) => events.push(event),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(events)
    }

    pub fn finish(&mut self) -> Result<(), ConsumerError<M::Error, M::SnapshotError>> {
        if self.state != ConsumerState::Closed {
            self.enter_gap();
            return Err(ConsumerError::UnexpectedEof);
        }
        if self.checkpoint.is_some() {
            return Err(ConsumerError::UnexpectedCheckpointEnd);
        }
        if self.output.is_some() {
            return Err(ConsumerError::OutputPartMismatch);
        }
        self.reader.finish().map_err(ConsumerError::Wire)?;
        Ok(())
    }

    /// Accept a pre-decoded typed message. This is useful for an owner-local
    /// queue, while network transports should use [`Self::accept_encoded`].
    pub fn accept_message(
        &mut self,
        message: StreamMessage,
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        let result = self.accept_message_inner(message);
        if result.is_err() && self.state != ConsumerState::Closed {
            self.enter_gap();
        }
        result
    }

    fn accept_message_inner(
        &mut self,
        message: StreamMessage,
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        message.validate().map_err(ConsumerError::Wire)?;
        if message.binding() != &self.binding {
            return Err(ConsumerError::BindingMismatch);
        }
        if self.state == ConsumerState::Closed {
            return Err(ConsumerError::Closed);
        }
        match message {
            StreamMessage::CheckpointStart {
                meta,
                total_bytes,
                part_count,
                digest,
            } => self.accept_checkpoint_start(meta, total_bytes, part_count, digest),
            StreamMessage::CheckpointPart {
                meta,
                part_index,
                part_count,
                bytes,
            } => self.accept_checkpoint_part(meta, part_index, part_count, bytes),
            StreamMessage::CheckpointEnd {
                meta,
                total_bytes,
                part_count,
                digest,
            } => self.accept_checkpoint_end(meta, total_bytes, part_count, digest),
            StreamMessage::Output {
                meta,
                part_index,
                part_count,
                bytes,
            } => self.accept_output(meta, part_index, part_count, bytes),
            StreamMessage::Resize { meta, geometry } => self.accept_resize(meta, geometry),
            StreamMessage::SyncFlush { meta } => self.accept_sync_flush(meta),
            StreamMessage::Gap {
                requested,
                available,
                available_revision,
                reason,
                ..
            } => {
                if available < requested
                    || available < self.session.cursor()
                    || available_revision < self.session.revision()
                {
                    return Err(ConsumerError::StaleGap);
                }
                self.checkpoint = None;
                self.output = None;
                self.state = ConsumerState::Gap;
                let ticket = match self.gap_ticket {
                    Some(existing) => (
                        existing.0.max(available),
                        existing.1.max(available_revision),
                    ),
                    None => (available, available_revision),
                };
                self.resync_floor.0 = self.resync_floor.0.max(ticket.0);
                self.resync_floor.1 = self.resync_floor.1.max(ticket.1);
                self.gap_ticket = Some(ticket);
                Ok(Some(ConsumerEvent::Gap {
                    requested,
                    available,
                    available_revision,
                    reason,
                }))
            }
            StreamMessage::Close { meta } => {
                if self.state == ConsumerState::Gap
                    && self.checkpoint.is_none()
                    && self.output.is_none()
                {
                    let Some((ticket_cursor, ticket_revision)) = self.gap_ticket else {
                        return Err(ConsumerError::UnexpectedClose);
                    };
                    if meta.cursor < ticket_cursor || meta.revision < ticket_revision {
                        return Err(ConsumerError::UnexpectedClose);
                    }
                    self.state = ConsumerState::Closed;
                    return Ok(Some(ConsumerEvent::Closed));
                }
                if self.state != ConsumerState::Live
                    || self.output.is_some()
                    || meta.cursor != self.session.cursor()
                    || meta.revision != self.session.revision()
                {
                    return Err(ConsumerError::UnexpectedClose);
                }
                self.state = ConsumerState::Closed;
                Ok(Some(ConsumerEvent::Closed))
            }
        }
    }

    fn enter_gap(&mut self) {
        self.checkpoint = None;
        self.output = None;
        if self.state != ConsumerState::Closed {
            self.state = ConsumerState::Gap;
            let ticket = (self.session.cursor(), self.session.revision());
            self.resync_floor.0 = self.resync_floor.0.max(ticket.0);
            self.resync_floor.1 = self.resync_floor.1.max(ticket.1);
            self.gap_ticket = Some(match self.gap_ticket {
                Some(existing) => (existing.0.max(ticket.0), existing.1.max(ticket.1)),
                None => ticket,
            });
        }
    }

    fn accept_checkpoint_start(
        &mut self,
        meta: MessageMeta,
        total_bytes: u32,
        part_count: u32,
        digest: [u8; 32],
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        if !matches!(
            self.state,
            ConsumerState::AwaitingBootstrap | ConsumerState::Gap
        ) || self.checkpoint.is_some()
        {
            return Err(ConsumerError::UnexpectedCheckpoint);
        }
        let total_bytes = total_bytes as usize;
        if total_bytes > MAX_BOUND_CHECKPOINT_BYTES {
            return Err(ConsumerError::CheckpointTooLarge);
        }
        if meta.cursor < self.resync_floor.0 || meta.revision < self.resync_floor.1 {
            return Err(ConsumerError::StaleCheckpoint {
                floor_cursor: self.resync_floor.0,
                floor_revision: self.resync_floor.1,
                actual_cursor: meta.cursor,
                actual_revision: meta.revision,
            });
        }
        // `part_count` and total bytes are bounded before this allocation.
        self.checkpoint = Some(CheckpointAssembly {
            meta,
            total_bytes,
            part_count,
            digest,
            next_part: 0,
            bytes: Vec::with_capacity(total_bytes),
        });
        Ok(None)
    }

    fn accept_checkpoint_part(
        &mut self,
        meta: MessageMeta,
        part_index: u32,
        part_count: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        let assembly = self
            .checkpoint
            .as_mut()
            .ok_or(ConsumerError::UnexpectedCheckpointPart)?;
        if assembly.meta != meta
            || assembly.part_count != part_count
            || assembly.next_part != part_index
        {
            return Err(ConsumerError::CheckpointPartMismatch);
        }
        if assembly.bytes.len().saturating_add(bytes.len()) > assembly.total_bytes {
            return Err(ConsumerError::CheckpointTooLarge);
        }
        assembly.bytes.extend_from_slice(&bytes);
        assembly.next_part = assembly
            .next_part
            .checked_add(1)
            .ok_or(ConsumerError::CheckpointPartMismatch)?;
        Ok(None)
    }

    fn accept_checkpoint_end(
        &mut self,
        meta: MessageMeta,
        total_bytes: u32,
        part_count: u32,
        digest: [u8; 32],
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        let assembly = self
            .checkpoint
            .take()
            .ok_or(ConsumerError::UnexpectedCheckpointEnd)?;
        if assembly.meta != meta
            || assembly.total_bytes != total_bytes as usize
            || assembly.part_count != part_count
            || assembly.digest != digest
            || assembly.next_part != assembly.part_count
            || assembly.bytes.len() != assembly.total_bytes
        {
            return Err(ConsumerError::CheckpointPartMismatch);
        }
        if BoundCheckpoint::digest(&assembly.bytes) != digest {
            return Err(ConsumerError::CheckpointDigestMismatch);
        }
        let bound = BoundCheckpoint::decode_for(&assembly.bytes, &self.binding)
            .map_err(ConsumerError::Checkpoint)?;
        if bound.cursor != meta.cursor || bound.revision != meta.revision {
            return Err(ConsumerError::CheckpointBindingMismatch);
        }
        // Decode the inner runtime envelope and check its source ticket before
        // constructing the replacement. A valid runtime checkpoint with a
        // forged cursor/revision must not mutate the live consumer before the
        // outer transport binding rejects it.
        let checkpoint =
            FullTerminalCheckpoint::<M::ModelCheckpoint, M::ProcessorCheckpoint>::decode_bounded(
                &bound.checkpoint_bytes,
            )
            .map_err(|error| ConsumerError::Session(SessionError::Decode(error)))?;
        if checkpoint.cursor() != bound.cursor || checkpoint.revision != bound.revision {
            return Err(ConsumerError::CheckpointBindingMismatch);
        }
        self.session
            .restore_checkpoint(&checkpoint, self.policy)
            .map_err(ConsumerError::Session)?;
        if self.session.cursor() != bound.cursor || self.session.revision() != bound.revision {
            return Err(ConsumerError::CheckpointBindingMismatch);
        }
        self.resync_floor = (bound.cursor, bound.revision);
        self.gap_ticket = None;
        self.state = ConsumerState::Live;
        Ok(Some(ConsumerEvent::Bootstrapped {
            cursor: bound.cursor,
            revision: bound.revision,
        }))
    }

    fn accept_output(
        &mut self,
        meta: MessageMeta,
        part_index: u32,
        part_count: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        if self.state != ConsumerState::Live {
            return Err(ConsumerError::ExpectedBootstrap);
        }
        if part_count == 1 {
            if part_index != 0 {
                return Err(ConsumerError::OutputPartMismatch);
            }
            return self.apply_output(meta, bytes);
        }
        if self.output.is_none() {
            if part_index != 0 {
                return Err(ConsumerError::OutputPartMismatch);
            }
            self.output = Some(OutputAssembly {
                meta: meta.clone(),
                part_count,
                next_part: 0,
                bytes: Vec::with_capacity(bytes.len()),
            });
        }
        let assembly = self
            .output
            .as_mut()
            .expect("output assembly was initialized");
        if assembly.meta != meta
            || assembly.part_count != part_count
            || assembly.next_part != part_index
        {
            return Err(ConsumerError::OutputPartMismatch);
        }
        if assembly.bytes.len().saturating_add(bytes.len()) > MAX_RAW_CHUNK_BYTES {
            return Err(ConsumerError::OutputTooLarge);
        }
        assembly.bytes.extend_from_slice(&bytes);
        assembly.next_part += 1;
        if assembly.next_part != part_count {
            return Ok(None);
        }
        let assembly = self.output.take().expect("completed output assembly");
        self.apply_output(assembly.meta, assembly.bytes)
    }

    fn apply_output(
        &mut self,
        meta: MessageMeta,
        bytes: Vec<u8>,
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        if meta.cursor != self.session.cursor() {
            return Err(ConsumerError::CursorMismatch {
                expected: self.session.cursor(),
                actual: meta.cursor,
            });
        }
        let expected_revision = self
            .session
            .revision()
            .checked_add(REVISION_STEP)
            .ok_or(ConsumerError::RevisionOverflow)?;
        if meta.revision != expected_revision {
            return Err(ConsumerError::RevisionMismatch {
                expected: expected_revision,
                actual: meta.revision,
            });
        }
        let report = self
            .session
            .ingest(RawChunk::new(meta.cursor, bytes))
            .map_err(ConsumerError::Apply)?;
        if report.revision != meta.revision {
            return Err(ConsumerError::RevisionMismatch {
                expected: report.revision,
                actual: meta.revision,
            });
        }
        Ok(Some(ConsumerEvent::Applied {
            cursor: report.cursor,
            revision: report.revision,
        }))
    }

    fn accept_resize(
        &mut self,
        meta: MessageMeta,
        geometry: Geometry,
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        if self.state != ConsumerState::Live || self.output.is_some() {
            return Err(ConsumerError::ExpectedBootstrap);
        }
        if meta.cursor != self.session.cursor() {
            return Err(ConsumerError::CursorMismatch {
                expected: self.session.cursor(),
                actual: meta.cursor,
            });
        }
        let expected_revision = self
            .session
            .revision()
            .checked_add(REVISION_STEP)
            .ok_or(ConsumerError::RevisionOverflow)?;
        if meta.revision != expected_revision {
            return Err(ConsumerError::RevisionMismatch {
                expected: expected_revision,
                actual: meta.revision,
            });
        }
        let report = self
            .session
            .resize(geometry)
            .map_err(ConsumerError::Apply)?;
        if report.revision != meta.revision {
            return Err(ConsumerError::RevisionMismatch {
                expected: report.revision,
                actual: meta.revision,
            });
        }
        Ok(Some(ConsumerEvent::Applied {
            cursor: report.cursor,
            revision: report.revision,
        }))
    }

    fn accept_sync_flush(
        &mut self,
        meta: MessageMeta,
    ) -> Result<Option<ConsumerEvent>, ConsumerError<M::Error, M::SnapshotError>> {
        if self.state != ConsumerState::Live || self.output.is_some() {
            return Err(ConsumerError::ExpectedBootstrap);
        }
        if meta.cursor != self.session.cursor() {
            return Err(ConsumerError::CursorMismatch {
                expected: self.session.cursor(),
                actual: meta.cursor,
            });
        }
        let expected_revision = self
            .session
            .revision()
            .checked_add(REVISION_STEP)
            .ok_or(ConsumerError::RevisionOverflow)?;
        if meta.revision != expected_revision {
            return Err(ConsumerError::RevisionMismatch {
                expected: expected_revision,
                actual: meta.revision,
            });
        }
        let report = self.session.flush().map_err(ConsumerError::Apply)?;
        if report.revision != meta.revision {
            return Err(ConsumerError::RevisionMismatch {
                expected: report.revision,
                actual: meta.revision,
            });
        }
        Ok(Some(ConsumerEvent::Applied {
            cursor: report.cursor,
            revision: report.revision,
        }))
    }
}

#[derive(Debug)]
pub enum ConsumerError<E, S> {
    Binding(crate::binding::BindingError),
    BindingMismatch,
    Wire(WireCodecError),
    Session(SessionError<E, S>),
    Apply(ApplyError<E>),
    Checkpoint(CheckpointBlobError),
    UnexpectedCheckpoint,
    UnexpectedCheckpointPart,
    UnexpectedCheckpointEnd,
    CheckpointPartMismatch,
    CheckpointDigestMismatch,
    CheckpointBindingMismatch,
    CheckpointTooLarge,
    StaleCheckpoint {
        floor_cursor: SessionCursor,
        floor_revision: u64,
        actual_cursor: SessionCursor,
        actual_revision: u64,
    },
    StaleGap,
    ExpectedBootstrap,
    OutputPartMismatch,
    OutputTooLarge,
    Closed,
    CursorMismatch {
        expected: SessionCursor,
        actual: SessionCursor,
    },
    RevisionMismatch {
        expected: u64,
        actual: u64,
    },
    RevisionOverflow,
    UnexpectedClose,
    UnexpectedEof,
    OwnerPolicyNotAllowed,
}

impl<E: fmt::Display, S: fmt::Display> fmt::Display for ConsumerError<E, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => error.fmt(formatter),
            Self::BindingMismatch => formatter.write_str("terminal consumer binding mismatch"),
            Self::Wire(error) => error.fmt(formatter),
            Self::Session(error) => error.fmt(formatter),
            Self::Apply(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::UnexpectedCheckpoint => formatter.write_str("unexpected checkpoint start"),
            Self::UnexpectedCheckpointPart => formatter.write_str("unexpected checkpoint part"),
            Self::UnexpectedCheckpointEnd => formatter.write_str("unexpected checkpoint end"),
            Self::CheckpointPartMismatch => {
                formatter.write_str("checkpoint part ordering mismatch")
            }
            Self::CheckpointDigestMismatch => formatter.write_str("checkpoint digest mismatch"),
            Self::CheckpointBindingMismatch => formatter.write_str("checkpoint binding mismatch"),
            Self::CheckpointTooLarge => formatter.write_str("checkpoint transfer is too large"),
            Self::StaleCheckpoint {
                floor_cursor,
                floor_revision,
                actual_cursor,
                actual_revision,
            } => write!(
                formatter,
                "checkpoint ticket is stale: floor {floor_cursor:?}/{floor_revision}, got {actual_cursor:?}/{actual_revision}"
            ),
            Self::StaleGap => formatter.write_str("terminal gap ticket is stale"),
            Self::ExpectedBootstrap => {
                formatter.write_str("terminal consumer requires a bootstrap")
            }
            Self::OutputPartMismatch => formatter.write_str("output part ordering mismatch"),
            Self::OutputTooLarge => formatter.write_str("output transfer is too large"),
            Self::Closed => formatter.write_str("terminal consumer is closed"),
            Self::CursorMismatch { expected, actual } => {
                write!(
                    formatter,
                    "terminal cursor mismatch: expected {expected:?}, got {actual:?}"
                )
            }
            Self::RevisionMismatch { expected, actual } => {
                write!(
                    formatter,
                    "terminal revision mismatch: expected {expected}, got {actual}"
                )
            }
            Self::RevisionOverflow => formatter.write_str("terminal revision overflow"),
            Self::UnexpectedClose => formatter.write_str("unexpected terminal close"),
            Self::UnexpectedEof => formatter.write_str("terminal stream ended without close"),
            Self::OwnerPolicyNotAllowed => {
                formatter.write_str("terminal stream consumers must use quiet query policy")
            }
        }
    }
}

impl<E: Error + 'static, S: Error + 'static> Error for ConsumerError<E, S> {}
