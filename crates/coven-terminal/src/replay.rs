//! Bounded ordered terminal-event replay and explicit gap/resynchronization
//! errors.

use std::{collections::VecDeque, error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::session::{Geometry, SessionCursor, MAX_RAW_CHUNK_BYTES};

/// One ordered event in the owner stream.
///
/// Resize and synchronized-update flush events intentionally consume a
/// sequence number while consuming no output bytes. Keeping them in the same
/// stream as output makes a replay describe the exact model history, including
/// geometry changes and timer-driven parser transitions between two byte
/// chunks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReplayEvent {
    Output(Vec<u8>),
    Resize(Geometry),
    /// The owner observed its synchronized-update timer boundary and asked the
    /// engine to flush any expired buffered update. This is an ordered event
    /// even when the timer was not yet expired, so a quiet renderer and a
    /// checkpoint/replay consumer observe the same owner operation order.
    SyncFlush,
}

impl ReplayEvent {
    pub fn byte_len(&self) -> usize {
        match self {
            Self::Output(bytes) => bytes.len(),
            Self::Resize(_) | Self::SyncFlush => 0,
        }
    }

    pub fn validate(&self) -> Result<(), ReplayError> {
        match self {
            Self::Output(bytes) if bytes.is_empty() => Err(ReplayError::EmptyFrame),
            Self::Output(bytes) if bytes.len() > MAX_RAW_CHUNK_BYTES => {
                Err(ReplayError::FrameTooLarge {
                    actual: bytes.len(),
                    maximum: MAX_RAW_CHUNK_BYTES,
                })
            }
            Self::Output(_) => Ok(()),
            Self::Resize(geometry) => geometry.validate().map_err(ReplayError::InvalidGeometry),
            Self::SyncFlush => Ok(()),
        }
    }
}

/// A retained ordered terminal event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayFrame {
    pub sequence: u64,
    pub offset: u64,
    pub event: ReplayEvent,
}

impl ReplayFrame {
    pub fn output(cursor: SessionCursor, bytes: Vec<u8>) -> Self {
        Self {
            sequence: cursor.sequence,
            offset: cursor.offset,
            event: ReplayEvent::Output(bytes),
        }
    }

    pub fn resize(cursor: SessionCursor, geometry: Geometry) -> Self {
        Self {
            sequence: cursor.sequence,
            offset: cursor.offset,
            event: ReplayEvent::Resize(geometry),
        }
    }

    pub fn sync_flush(cursor: SessionCursor) -> Self {
        Self {
            sequence: cursor.sequence,
            offset: cursor.offset,
            event: ReplayEvent::SyncFlush,
        }
    }

    pub fn cursor(&self) -> SessionCursor {
        SessionCursor {
            sequence: self.sequence,
            offset: self.offset,
        }
    }

    pub fn byte_len(&self) -> usize {
        self.event.byte_len()
    }

    pub fn output_bytes(&self) -> Option<&[u8]> {
        match &self.event {
            ReplayEvent::Output(bytes) => Some(bytes),
            ReplayEvent::Resize(_) | ReplayEvent::SyncFlush => None,
        }
    }

    pub fn end_cursor(&self) -> Result<SessionCursor, ReplayError> {
        let offset = self
            .offset
            .checked_add(self.byte_len() as u64)
            .ok_or(ReplayError::CursorOverflow)?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(ReplayError::CursorOverflow)?;
        Ok(SessionCursor { sequence, offset })
    }
}

/// A bounded in-memory replay window. Checkpoints intentionally do not embed
/// this potentially large buffer; restore starts a new window anchored at the
/// checkpoint cursor, so an older cursor receives a typed gap response.
#[derive(Clone, Debug)]
pub struct ReplayWindow {
    max_bytes: usize,
    max_frames: usize,
    bytes: usize,
    oldest: SessionCursor,
    next: SessionCursor,
    frames: VecDeque<ReplayFrame>,
}

impl ReplayWindow {
    pub fn new(
        max_bytes: usize,
        max_frames: usize,
        anchor: SessionCursor,
    ) -> Result<Self, ReplayError> {
        if max_bytes == 0 || max_bytes > crate::session::MAX_REPLAY_BYTES {
            return Err(ReplayError::InvalidLimit);
        }
        if max_frames == 0 || max_frames > crate::session::MAX_REPLAY_FRAMES {
            return Err(ReplayError::InvalidLimit);
        }
        if anchor.sequence == 0 {
            return Err(ReplayError::InvalidCursor(anchor));
        }
        Ok(Self {
            max_bytes,
            max_frames,
            bytes: 0,
            oldest: anchor,
            next: anchor,
            frames: VecDeque::new(),
        })
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn max_frames(&self) -> usize {
        self.max_frames
    }

    pub fn oldest_cursor(&self) -> SessionCursor {
        self.oldest
    }

    pub fn next_cursor(&self) -> SessionCursor {
        self.next
    }

    pub fn frames(&self) -> impl Iterator<Item = &ReplayFrame> {
        self.frames.iter()
    }

    /// Check the cursor and limits before the owner mutates its model.
    pub fn preflight_append(&self, frame: &ReplayFrame) -> Result<SessionCursor, ReplayError> {
        if frame.cursor() != self.next {
            return Err(ReplayError::CursorMismatch {
                expected: self.next,
                actual: frame.cursor(),
            });
        }
        frame.event.validate()?;
        if frame.byte_len() > self.max_bytes {
            return Err(ReplayError::FrameTooLarge {
                actual: frame.byte_len(),
                maximum: self.max_bytes,
            });
        }
        frame.end_cursor()
    }

    /// Append an event after the owner has successfully advanced its model.
    /// `preflight_append` must be called first. This method itself has no
    /// fallible allocation-sensitive operation after the frame is accepted.
    pub fn append(&mut self, frame: ReplayFrame) -> Result<(), ReplayError> {
        frame.event.validate()?;
        if frame.byte_len() > self.max_bytes {
            return Err(ReplayError::FrameTooLarge {
                actual: frame.byte_len(),
                maximum: self.max_bytes,
            });
        }
        if frame.cursor() != self.next {
            return Err(ReplayError::CursorMismatch {
                expected: self.next,
                actual: frame.cursor(),
            });
        }
        let end = frame.end_cursor()?;
        self.bytes = self
            .bytes
            .checked_add(frame.byte_len())
            .ok_or(ReplayError::CursorOverflow)?;
        self.frames.push_back(frame);
        self.next = end;
        while self.bytes > self.max_bytes || self.frames.len() > self.max_frames {
            let Some(oldest) = self.frames.pop_front() else {
                break;
            };
            self.bytes -= oldest.byte_len();
            self.oldest = oldest.end_cursor()?;
        }
        Ok(())
    }

    /// Return retained events starting at an exact cursor. A caller receiving
    /// `Gap` must resynchronize from `available_from` using a fresh full
    /// checkpoint/surface; it must never invent missing ANSI bytes or resize
    /// events.
    pub fn replay_from(&self, cursor: SessionCursor) -> Result<Vec<ReplayFrame>, ReplayError> {
        if cursor.sequence == 0 {
            return Err(ReplayError::InvalidCursor(cursor));
        }
        if cursor == self.next {
            return Ok(Vec::new());
        }
        if cursor.sequence < self.oldest.sequence
            || (cursor.sequence == self.oldest.sequence && cursor.offset < self.oldest.offset)
        {
            return Err(ReplayError::Gap {
                requested: cursor,
                available_from: self.oldest,
            });
        }
        let Some(index) = self
            .frames
            .iter()
            .position(|frame| frame.cursor() == cursor)
        else {
            return Err(ReplayError::CursorMismatch {
                expected: self.next,
                actual: cursor,
            });
        };
        Ok(self.frames.iter().skip(index).cloned().collect())
    }
}

/// A replay request cannot be fulfilled or is malformed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayError {
    InvalidLimit,
    InvalidCursor(SessionCursor),
    EmptyFrame,
    InvalidGeometry(String),
    FrameTooLarge {
        actual: usize,
        maximum: usize,
    },
    CursorMismatch {
        expected: SessionCursor,
        actual: SessionCursor,
    },
    Gap {
        requested: SessionCursor,
        available_from: SessionCursor,
    },
    CursorOverflow,
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit => f.write_str("invalid replay limit"),
            Self::InvalidCursor(cursor) => write!(f, "invalid replay cursor {cursor:?}"),
            Self::EmptyFrame => f.write_str("empty raw output frame"),
            Self::InvalidGeometry(error) => write!(f, "invalid resize geometry: {error}"),
            Self::FrameTooLarge { actual, maximum } => {
                write!(
                    f,
                    "raw output frame is {actual} bytes, maximum is {maximum}"
                )
            }
            Self::CursorMismatch { expected, actual } => {
                write!(f, "cursor mismatch: expected {expected:?}, got {actual:?}")
            }
            Self::Gap {
                requested,
                available_from,
            } => write!(
                f,
                "replay gap at {requested:?}; retained events begin at {available_from:?}"
            ),
            Self::CursorOverflow => f.write_str("terminal stream cursor overflow"),
        }
    }
}

impl Error for ReplayError {}
