//! A bounded, server-owned presentation of a Coven execution transcript.
//!
//! This is deliberately separate from the native terminal runtime.  A
//! transcript is text that an external owner made available for display; it
//! is not a PTY, does not have a cursor, and cannot accept input.  The
//! projection owns the only normalization and retention step so the renderer
//! and the API read exactly the same text.

use std::{fmt, ops::Deref};

use coven_client::{
    execution::ContractError,
    source::{SourceCursor, SourceIdentity},
    transcript::{
        TranscriptChunk, TranscriptContent, TranscriptRead, TranscriptReply, TranscriptResult,
        MAX_CHUNKS,
    },
};

use super::ProviderBinding;

/// Maximum UTF-8 bytes retained by one external transcript projection.
pub(crate) const MAX_RETAINED_TEXT_BYTES: usize = 256 * 1024;
/// Maximum logical lines retained by one external transcript projection.
pub(crate) const MAX_RETAINED_LOGICAL_LINES: usize = 4096;

/// The externally visible portion of a transcript projection.  Keeping this
/// as a small immutable value makes it safe to share an `Arc` with both the
/// pane renderer and the API read path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TranscriptLine(String);

impl TranscriptLine {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for TranscriptLine {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// The reason an incoming response could not be applied.  Responses are
/// rejected before changing the projection, so callers can leave the last
/// known display in place and retry with a fresh snapshot when appropriate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TranscriptApplyError {
    RuntimeGenerationMismatch,
    InvalidReply(ContractError),
    SourceIdentityMismatch,
    CursorMismatch,
    MissingCursor,
    RevisionWentBackwards,
    Duplicate,
    SequenceBackwards { expected: i64, actual: i64 },
    RevisionExhausted,
}

impl fmt::Display for TranscriptApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeGenerationMismatch => {
                formatter.write_str("Coven transcript runtime generation mismatch")
            }
            Self::InvalidReply(error) => {
                write!(formatter, "invalid Coven transcript reply: {error}")
            }
            Self::SourceIdentityMismatch => {
                formatter.write_str("Coven transcript source identity mismatch")
            }
            Self::CursorMismatch => formatter.write_str("Coven transcript cursor mismatch"),
            Self::MissingCursor => {
                formatter.write_str("Coven transcript has no cursor to page from")
            }
            Self::RevisionWentBackwards => {
                formatter.write_str("Coven transcript cursor revision went backwards")
            }
            Self::Duplicate => formatter.write_str("duplicate Coven transcript response"),
            Self::SequenceBackwards { expected, actual } => write!(
                formatter,
                "Coven transcript sequence moved backwards: expected at least {expected}, got {actual}"
            ),
            Self::RevisionExhausted => {
                formatter.write_str("Coven transcript content revision exhausted")
            }
        }
    }
}

impl std::error::Error for TranscriptApplyError {}

/// Facts about a successfully applied response.  Omission counters are
/// cumulative for the current cursor/source anchor and can be rendered as an
/// explicit notice without inserting synthetic transcript text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TranscriptApply {
    pub(crate) content_revision: u64,
    pub(crate) replaced: bool,
    pub(crate) appended: bool,
    pub(crate) omitted_chunks: u64,
    pub(crate) omitted_bytes: u64,
    pub(crate) dropped_lines: u64,
}

/// Cumulative bounded-retention information for the current source anchor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TranscriptOmissions {
    /// The owner did not include some prefix in its snapshot.
    pub(crate) prefix_omitted: bool,
    /// Number of source chunks represented by an explicit omitted marker.
    pub(crate) chunks: u64,
    /// Bytes omitted by source-side per-chunk truncation or local retention.
    pub(crate) bytes: u64,
    /// Logical lines discarded from the local head to satisfy the line cap.
    pub(crate) lines: u64,
}

impl TranscriptOmissions {
    pub(crate) fn any(self) -> bool {
        self.prefix_omitted || self.chunks != 0 || self.bytes != 0 || self.lines != 0
    }
}

/// A server-owned bounded transcript.  `binding` is retained in full so the
/// runtime generation and all execution identity fields remain visible at the
/// owner seam; the remote response never gets to select either one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TranscriptProjection {
    binding: ProviderBinding,
    source: Option<SourceIdentity>,
    cursor: Option<SourceCursor>,
    lines: Vec<TranscriptLine>,
    retained_text_bytes: usize,
    omissions: TranscriptOmissions,
    source_dropped_output_bytes: Option<u64>,
    content_revision: u64,
}

impl TranscriptProjection {
    /// Start an empty projection for one exact managed provider binding.
    pub(crate) fn new(binding: ProviderBinding) -> Self {
        Self {
            source: binding.pinned_source.clone(),
            binding,
            cursor: None,
            lines: Vec::new(),
            retained_text_bytes: 0,
            omissions: TranscriptOmissions::default(),
            source_dropped_output_bytes: None,
            content_revision: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> Option<&SourceIdentity> {
        self.source.as_ref()
    }

    pub(crate) fn cursor(&self) -> Option<&SourceCursor> {
        self.cursor.as_ref()
    }

    /// Even revisions match Herdr's content revision convention.  Zero is the
    /// revision of an empty projection; every accepted response advances by
    /// two, including a reset that clears an already empty projection.
    pub(crate) fn content_revision(&self) -> u64 {
        self.content_revision
    }

    /// Borrow every retained normalized line.  This is the shared source for
    /// rendering and API reads; neither consumer reparses or normalizes text.
    #[cfg(test)]
    pub(crate) fn lines(&self) -> &[TranscriptLine] {
        &self.lines
    }

    /// Borrow the bounded tail requested by an API read.  The returned slice
    /// points into this projection and therefore performs no text copy.
    pub(crate) fn read_lines(&self, limit: usize) -> &[TranscriptLine] {
        let start = self.lines.len().saturating_sub(limit);
        &self.lines[start..]
    }

    pub(crate) fn line_count(&self) -> usize {
        self.lines.len()
    }

    #[cfg(test)]
    pub(crate) fn retained_text_bytes(&self) -> usize {
        self.retained_text_bytes
    }

    #[cfg(test)]
    pub(crate) fn omissions(&self) -> TranscriptOmissions {
        self.omissions
    }

    pub(crate) fn has_omissions(&self) -> bool {
        self.omissions.any()
            || self
                .source_dropped_output_bytes
                .is_some_and(|dropped| dropped > 0)
    }

    /// Session-scoped output drops reported by the owner.  This is kept
    /// separate from local tail eviction and explicit omitted chunks so a UI
    /// can distinguish provider loss from Herdr's bounded presentation.
    pub(crate) fn source_dropped_output_bytes(&self) -> Option<u64> {
        self.source_dropped_output_bytes
    }

    /// Apply one already fetched transcript response.  Validation is repeated
    /// here at the state boundary because callers may have queued a response
    /// and changed the current cursor before it was delivered.
    pub(crate) fn apply(
        &mut self,
        reply: &TranscriptReply,
        runtime_generation: u64,
    ) -> Result<TranscriptApply, TranscriptApplyError> {
        if runtime_generation != self.binding.runtime_generation {
            return Err(TranscriptApplyError::RuntimeGenerationMismatch);
        }

        let result = &reply.result;
        match result {
            TranscriptResult::Reset { .. } => self.apply_reset(reply, runtime_generation),
            TranscriptResult::Snapshot {
                cursor,
                chunks,
                prefix_omitted,
            } => self.apply_snapshot(reply, runtime_generation, cursor, chunks, *prefix_omitted),
            TranscriptResult::Page {
                after,
                cursor,
                chunks,
                has_more: _,
            } => self.apply_page(reply, runtime_generation, after, cursor, chunks),
        }
    }

    fn apply_reset(
        &mut self,
        reply: &TranscriptReply,
        _runtime_generation: u64,
    ) -> Result<TranscriptApply, TranscriptApplyError> {
        let current = self
            .cursor
            .clone()
            .ok_or(TranscriptApplyError::MissingCursor)?;
        self.ensure_revision_available()?;
        self.validate_source_for_non_reset(&reply.source)?;
        self.validate_reply(reply, Some(current))?;

        // A reset invalidates the cursor and retained text, but it never
        // authorizes a response to replace the trusted ledger/epoch.  A
        // source change therefore requires a new binding and projection.
        self.cursor = None;
        self.lines.clear();
        self.retained_text_bytes = 0;
        self.omissions = TranscriptOmissions::default();
        self.source_dropped_output_bytes = reply.projection.dropped_output_bytes;
        self.bump_revision()?;
        Ok(self.applied(true, false))
    }

    fn apply_snapshot(
        &mut self,
        reply: &TranscriptReply,
        _runtime_generation: u64,
        cursor: &SourceCursor,
        chunks: &[TranscriptChunk],
        prefix_omitted: bool,
    ) -> Result<TranscriptApply, TranscriptApplyError> {
        self.ensure_revision_available()?;
        self.validate_reply(reply, None)?;
        self.validate_source_for_non_reset(&reply.source)?;
        validate_snapshot_sequences(cursor, chunks, prefix_omitted)?;

        if let Some(current) = &self.cursor {
            if reply.source
                != *self
                    .source
                    .as_ref()
                    .ok_or(TranscriptApplyError::SourceIdentityMismatch)?
            {
                return Err(TranscriptApplyError::SourceIdentityMismatch);
            }
            if cursor.revision < current.revision {
                return Err(TranscriptApplyError::RevisionWentBackwards);
            }
            if cursor.revision == current.revision {
                if cursor.after_seq < current.after_seq {
                    return Err(TranscriptApplyError::SequenceBackwards {
                        expected: current.after_seq,
                        actual: cursor.after_seq,
                    });
                }
                if cursor.after_seq == current.after_seq {
                    return Err(TranscriptApplyError::Duplicate);
                }
            }
        }

        self.source = Some(reply.source.clone());
        self.cursor = Some(cursor.clone());
        self.lines.clear();
        self.retained_text_bytes = 0;
        self.omissions = TranscriptOmissions {
            prefix_omitted,
            ..TranscriptOmissions::default()
        };
        self.source_dropped_output_bytes = reply.projection.dropped_output_bytes;
        self.append_chunks(chunks);
        self.bump_revision()?;
        Ok(self.applied(true, false))
    }

    fn apply_page(
        &mut self,
        reply: &TranscriptReply,
        _runtime_generation: u64,
        after: &SourceCursor,
        cursor: &SourceCursor,
        chunks: &[TranscriptChunk],
    ) -> Result<TranscriptApply, TranscriptApplyError> {
        let current = self
            .cursor
            .clone()
            .ok_or(TranscriptApplyError::MissingCursor)?;
        self.validate_source_for_non_reset(&reply.source)?;
        if after != &current {
            return Err(TranscriptApplyError::CursorMismatch);
        }
        self.ensure_revision_available()?;
        self.validate_reply(reply, Some(current.clone()))?;
        self.observe_source_dropped(reply.projection.dropped_output_bytes);
        if cursor.revision != current.revision {
            return Err(TranscriptApplyError::RevisionWentBackwards);
        }
        if cursor.after_seq < current.after_seq {
            return Err(TranscriptApplyError::SequenceBackwards {
                expected: current.after_seq,
                actual: cursor.after_seq,
            });
        }
        if cursor.after_seq == current.after_seq {
            // Polling an unchanged source position is a valid no-op.  Keep
            // the cursor and content revision stable so idle providers do not
            // create render churn on every observation cycle.
            return Ok(self.applied(false, false));
        }
        validate_page_sequences(&current, cursor, chunks)?;

        self.cursor = Some(cursor.clone());
        self.append_chunks(chunks);
        self.bump_revision()?;
        Ok(self.applied(false, true))
    }

    fn validate_reply(
        &self,
        reply: &TranscriptReply,
        cursor: Option<SourceCursor>,
    ) -> Result<(), TranscriptApplyError> {
        let request = TranscriptRead {
            contract: coven_client::transcript::CONTRACT.to_owned(),
            host_id: self.binding.host_id.clone(),
            scope: self.binding.scope.clone(),
            authority: self.binding.authority.clone(),
            session_id: self.binding.session_id.clone(),
            generation: self.binding.generation,
            cursor,
            limit: MAX_CHUNKS,
        };
        reply
            .validate_for(&request)
            .map_err(TranscriptApplyError::InvalidReply)
    }

    fn validate_source_for_non_reset(
        &self,
        source: &SourceIdentity,
    ) -> Result<(), TranscriptApplyError> {
        let expected = self.source.as_ref().or(self.binding.pinned_source.as_ref());
        if expected.is_some_and(|expected| expected != source) {
            return Err(TranscriptApplyError::SourceIdentityMismatch);
        }
        Ok(())
    }

    fn append_chunks(&mut self, chunks: &[TranscriptChunk]) {
        for chunk in chunks {
            match &chunk.content {
                TranscriptContent::Text {
                    data,
                    omitted_prefix_bytes,
                } => {
                    self.omissions.bytes =
                        self.omissions.bytes.saturating_add(*omitted_prefix_bytes);
                    self.append_normalized(data);
                }
                TranscriptContent::Omitted { .. } => {
                    self.omissions.chunks = self.omissions.chunks.saturating_add(1);
                }
            }
        }
        self.trim_to_bounds();
    }

    fn append_normalized(&mut self, data: &str) {
        if data.is_empty() {
            return;
        }
        if self.lines.is_empty() {
            self.lines.push(TranscriptLine(String::new()));
        }
        // Bound transient expansion while escaping controls.  A source chunk
        // is bounded by the wire contract, but escaping each control can
        // expand it several times; periodic trimming keeps even that
        // intermediate line storage close to the retained bound.
        const NORMALIZATION_CHECK_CHARS: usize = 1024;
        const NORMALIZATION_HEADROOM_BYTES: usize = 16 * 1024;
        let mut since_check = 0usize;
        for character in data.chars() {
            if character == '\n' {
                self.append_display_character(character);
            } else if character.is_control() || is_unicode_format_control(character) {
                for escaped in character.escape_default() {
                    self.append_display_character(escaped);
                }
            } else {
                self.append_display_character(character);
            }
            since_check += 1;
            if since_check >= NORMALIZATION_CHECK_CHARS {
                if self.retained_text_bytes
                    > MAX_RETAINED_TEXT_BYTES.saturating_add(NORMALIZATION_HEADROOM_BYTES)
                    || self.lines.len()
                        > MAX_RETAINED_LOGICAL_LINES.saturating_add(NORMALIZATION_CHECK_CHARS)
                {
                    self.trim_to_bounds();
                }
                since_check = 0;
            }
        }
    }

    fn append_display_character(&mut self, character: char) {
        if character == '\n' {
            self.lines.push(TranscriptLine(String::new()));
            self.retained_text_bytes = self.retained_text_bytes.saturating_add(1);
        } else {
            // `append_normalized` only calls this with printable text or an
            // ASCII escape representation, so the line vector always has a
            // current line here.
            if self.lines.is_empty() {
                self.lines.push(TranscriptLine(String::new()));
            }
            if let Some(line) = self.lines.last_mut() {
                line.0.push(character);
                self.retained_text_bytes = self
                    .retained_text_bytes
                    .saturating_add(character.len_utf8());
            }
        }
    }

    fn trim_to_bounds(&mut self) {
        if self.lines.len() > MAX_RETAINED_LOGICAL_LINES {
            let remove_count = self.lines.len() - MAX_RETAINED_LOGICAL_LINES;
            self.drop_oldest_lines(remove_count);
        }
        while self.retained_text_bytes > MAX_RETAINED_TEXT_BYTES {
            let excess = self.retained_text_bytes - MAX_RETAINED_TEXT_BYTES;
            let Some(first) = self.lines.first() else {
                self.retained_text_bytes = 0;
                break;
            };
            let mut remove_count = 0usize;
            let mut remove_bytes = 0usize;
            for line in &self.lines {
                let next_bytes = remove_bytes.saturating_add(line.0.len());
                if next_bytes > excess {
                    break;
                }
                remove_count += 1;
                remove_bytes = next_bytes;
            }
            if remove_count != 0 {
                self.drop_oldest_lines(remove_count);
                continue;
            }

            let first_len = first.0.len();
            let boundary = ceil_char_boundary(&first.0, excess);
            let Some(first) = self.lines.first_mut() else {
                break;
            };
            let dropped = boundary.min(first_len);
            let suffix = first.0.split_off(dropped);
            first.0 = suffix;
            self.retained_text_bytes = self.retained_text_bytes.saturating_sub(dropped);
            self.omissions.bytes = self.omissions.bytes.saturating_add(dropped as u64);
            self.omissions.prefix_omitted = true;
            break;
        }
    }

    fn drop_oldest_lines(&mut self, count: usize) {
        let count = count.min(self.lines.len());
        if count == 0 {
            return;
        }
        let dropped_bytes = self
            .lines
            .iter()
            .take(count)
            .map(|line| line.0.len())
            .sum::<usize>();
        let separator_bytes = count.min(self.lines.len().saturating_sub(1));
        self.lines.drain(..count);
        self.retained_text_bytes = self
            .retained_text_bytes
            .saturating_sub(dropped_bytes.saturating_add(separator_bytes));
        self.omissions.lines = self.omissions.lines.saturating_add(count as u64);
        self.omissions.prefix_omitted = true;
    }

    fn bump_revision(&mut self) -> Result<(), TranscriptApplyError> {
        self.content_revision = self
            .content_revision
            .checked_add(2)
            .ok_or(TranscriptApplyError::RevisionExhausted)?;
        Ok(())
    }

    fn observe_source_dropped(&mut self, dropped: Option<u64>) {
        if let Some(dropped) = dropped {
            self.source_dropped_output_bytes = Some(
                self.source_dropped_output_bytes
                    .unwrap_or_default()
                    .max(dropped),
            );
        }
    }

    fn ensure_revision_available(&self) -> Result<(), TranscriptApplyError> {
        self.content_revision
            .checked_add(2)
            .map(|_| ())
            .ok_or(TranscriptApplyError::RevisionExhausted)
    }

    fn applied(&self, replaced: bool, appended: bool) -> TranscriptApply {
        TranscriptApply {
            content_revision: self.content_revision,
            replaced,
            appended,
            omitted_chunks: self.omissions.chunks,
            omitted_bytes: self.omissions.bytes,
            dropped_lines: self.omissions.lines,
        }
    }
}

fn ceil_char_boundary(value: &str, mut target: usize) -> usize {
    while target < value.len() && !value.is_char_boundary(target) {
        target += 1;
    }
    target
}

fn validate_snapshot_sequences(
    _cursor: &SourceCursor,
    chunks: &[TranscriptChunk],
    _prefix_omitted: bool,
) -> Result<(), TranscriptApplyError> {
    let mut previous: Option<i64> = None;
    for chunk in chunks {
        if let Some(previous) = previous {
            let expected = previous.saturating_add(1);
            if chunk.seq < expected {
                return Err(TranscriptApplyError::SequenceBackwards {
                    expected,
                    actual: chunk.seq,
                });
            }
        }
        previous = Some(chunk.seq);
    }
    // The transcript filters out non-output events, so ordinary gaps between
    // chunks and the source cursor carry no loss meaning and remain valid.
    Ok(())
}

fn validate_page_sequences(
    after: &SourceCursor,
    _cursor: &SourceCursor,
    chunks: &[TranscriptChunk],
) -> Result<(), TranscriptApplyError> {
    let expected_first =
        after
            .after_seq
            .checked_add(1)
            .ok_or(TranscriptApplyError::SequenceBackwards {
                expected: after.after_seq,
                actual: after.after_seq,
            })?;
    let Some(first) = chunks.first() else {
        // A page may advance over non-output source events without returning
        // a chunk.  The cursor still makes progress, and the contract's
        // `has_more` validation prevents an empty page from claiming hidden
        // output remains.
        return Ok(());
    };
    if first.seq < expected_first {
        return Err(TranscriptApplyError::SequenceBackwards {
            expected: expected_first,
            actual: first.seq,
        });
    }
    let mut previous = first.seq;
    for chunk in &chunks[1..] {
        let expected = previous.saturating_add(1);
        if chunk.seq < expected {
            return Err(TranscriptApplyError::SequenceBackwards {
                expected,
                actual: chunk.seq,
            });
        }
        previous = chunk.seq;
    }
    // As with snapshots, filtered source rows can leave a gap between the
    // final output sequence and the source cursor.
    Ok(())
}

/// `char::is_control` covers C0/C1 but Unicode format controls are a separate
/// category.  Keep the list local and explicit so no terminal parser or
/// platform Unicode service is involved in the presentation boundary.
fn is_unicode_format_control(character: char) -> bool {
    matches!(
        character,
        '\u{00ad}'
            | '\u{0600}'..='\u{0605}'
            | '\u{061c}'
            | '\u{06dd}'
            | '\u{070f}'
            | '\u{0890}'..='\u{0891}'
            | '\u{08e2}'
            | '\u{180e}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206f}'
            | '\u{feff}'
            | '\u{fff9}'..='\u{fffb}'
            | '\u{110bd}'
            | '\u{110cd}'
            | '\u{13430}'..='\u{1343f}'
            | '\u{1bca0}'..='\u{1bca3}'
            | '\u{1d173}'..='\u{1d17a}'
            | '\u{e0001}'
            | '\u{e0020}'..='\u{e007f}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_client::{
        execution::{
            AuthorityId, ExecutionAuthority, ExecutionScope, ExecutionSessionId, ProfileId,
            ProjectId,
        },
        source::{
            ResetReason, SourceHealth, SourceId, SourceLifecycle, SourceProjection, WriterState,
        },
    };

    fn binding() -> ProviderBinding {
        ProviderBinding::new(
            "coven-test",
            SourceId::new("host").expect("host"),
            ExecutionScope {
                project_id: ProjectId::new("project").expect("project"),
                profile_id: ProfileId::new("profile").expect("profile"),
                policy_generation: 1,
            },
            ExecutionAuthority::new(AuthorityId::new("authority").expect("authority"), 1)
                .expect("authority"),
            ExecutionSessionId::new("session").expect("session"),
            1,
            7,
        )
        .expect("binding")
    }

    fn source() -> SourceIdentity {
        SourceIdentity {
            host_id: SourceId::new("host").expect("host"),
            profile_id: ProfileId::new("profile").expect("profile"),
            ledger_id: SourceId::new("ledger").expect("ledger"),
            epoch: 1,
        }
    }

    fn cursor(after_seq: i64, revision: u64) -> SourceCursor {
        SourceCursor {
            ledger_id: source().ledger_id,
            epoch: 1,
            after_seq,
            revision,
        }
    }

    fn reply(result: TranscriptResult) -> TranscriptReply {
        TranscriptReply {
            contract: coven_client::transcript::CONTRACT.to_owned(),
            source: source(),
            scope: binding().scope,
            authority: binding().authority,
            session_id: binding().session_id,
            generation: 1,
            health: SourceHealth {
                daemon_live: true,
                writer_state: WriterState::Healthy,
                writer_queued_bytes: None,
                writer_dropped_output_bytes: None,
            },
            projection: SourceProjection {
                lifecycle: SourceLifecycle::Running,
                exit_code: None,
                archived: false,
                dropped_output_bytes: None,
            },
            result,
        }
    }

    fn text(seq: i64, data: &str) -> TranscriptChunk {
        TranscriptChunk {
            seq,
            content: TranscriptContent::Text {
                data: data.to_owned(),
                omitted_prefix_bytes: 0,
            },
        }
    }

    fn snapshot(after_seq: i64, revision: u64, chunks: Vec<TranscriptChunk>) -> TranscriptReply {
        reply(TranscriptResult::Snapshot {
            cursor: cursor(after_seq, revision),
            chunks,
            prefix_omitted: false,
        })
    }

    fn page(after: i64, revision: u64, chunks: Vec<TranscriptChunk>) -> TranscriptReply {
        let next = chunks.last().map_or(after, |chunk| chunk.seq);
        reply(TranscriptResult::Page {
            after: cursor(after, revision),
            cursor: cursor(next, revision),
            chunks,
            has_more: false,
        })
    }

    #[test]
    fn snapshot_and_page_are_ordered_and_shared_lines_are_borrowed() {
        let mut projection = TranscriptProjection::new(binding());
        let first = snapshot(1, 1, vec![text(1, "one\n")]);
        let applied = projection.apply(&first, 7).expect("snapshot");
        assert_eq!(applied.content_revision, 2);
        assert_eq!(
            projection
                .lines()
                .iter()
                .map(|line| line.as_str())
                .collect::<Vec<_>>(),
            ["one", ""]
        );

        let second = page(1, 1, vec![text(2, "two")]);
        projection.apply(&second, 7).expect("page");
        assert_eq!(projection.read_lines(1)[0].as_str(), "two");
        assert_eq!(projection.lines().len(), projection.line_count());
        assert_eq!(projection.content_revision() % 2, 0);
    }

    #[test]
    fn unchanged_and_backward_pages_do_not_mutate_the_projection() {
        let mut projection = TranscriptProjection::new(binding());
        projection
            .apply(&snapshot(1, 1, vec![text(1, "one")]), 7)
            .expect("snapshot");
        let before = projection.clone();

        let unchanged = projection
            .apply(&page(1, 1, vec![]), 7)
            .expect("unchanged page");
        assert!(!unchanged.appended);
        assert_eq!(unchanged.content_revision, before.content_revision());
        assert_eq!(projection, before);
        assert!(matches!(
            projection.apply(&page(1, 1, vec![text(1, "again")]), 7),
            Err(TranscriptApplyError::InvalidReply(_))
        ));
        assert_eq!(projection, before);
        assert!(matches!(
            projection.apply(&page(1, 1, vec![text(3, "three")]), 7),
            Ok(TranscriptApply { .. })
        ));
        assert_eq!(projection.cursor().map(|value| value.after_seq), Some(3));
    }

    #[test]
    fn filtered_source_sequence_gaps_do_not_infer_loss() {
        let mut projection = TranscriptProjection::new(binding());
        projection
            .apply(&snapshot(4, 1, vec![text(1, "one"), text(4, "four")]), 7)
            .expect("gapped snapshot");
        assert_eq!(projection.cursor().map(|value| value.after_seq), Some(4));
        projection
            .apply(&page(4, 1, vec![text(7, "seven")]), 7)
            .expect("gapped page");
        assert_eq!(projection.cursor().map(|value| value.after_seq), Some(7));
        assert!(!projection.has_omissions());
    }

    #[test]
    fn out_of_order_snapshot_sequences_are_rejected_without_mutation() {
        let mut projection = TranscriptProjection::new(binding());
        let invalid = snapshot(4, 1, vec![text(3, "three"), text(2, "two")]);
        assert!(matches!(
            projection.apply(&invalid, 7),
            Err(TranscriptApplyError::InvalidReply(_))
                | Err(TranscriptApplyError::SequenceBackwards { .. })
        ));
        assert!(projection.cursor().is_none());
    }

    #[test]
    fn backward_snapshot_is_rejected_without_mutation() {
        let mut projection = TranscriptProjection::new(binding());
        projection
            .apply(&snapshot(3, 2, vec![text(3, "three")]), 7)
            .expect("snapshot");
        let before = projection.clone();
        assert!(matches!(
            projection.apply(&snapshot(2, 1, vec![text(2, "two")]), 7),
            Err(TranscriptApplyError::RevisionWentBackwards)
                | Err(TranscriptApplyError::SequenceBackwards { .. })
                | Err(TranscriptApplyError::InvalidReply(_))
        ));
        assert_eq!(projection, before);
    }

    #[test]
    fn page_with_source_cursor_progress_but_no_output_is_accepted() {
        let mut projection = TranscriptProjection::new(binding());
        projection
            .apply(&snapshot(1, 1, vec![text(1, "one")]), 7)
            .expect("snapshot");
        let empty_progress = reply(TranscriptResult::Page {
            after: cursor(1, 1),
            cursor: cursor(3, 1),
            chunks: vec![],
            has_more: false,
        });
        projection.apply(&empty_progress, 7).expect("progress");
        assert_eq!(projection.cursor().map(|value| value.after_seq), Some(3));
    }

    #[test]
    fn source_and_runtime_generation_are_fenced() {
        let mut projection = TranscriptProjection::new(binding());
        let first = snapshot(1, 1, vec![text(1, "one")]);
        assert_eq!(
            projection.apply(&first, 8),
            Err(TranscriptApplyError::RuntimeGenerationMismatch)
        );
        assert!(projection.source().is_none());

        let mut wrong_source = first.clone();
        let other_ledger = SourceId::new("other-ledger").expect("ledger");
        wrong_source.source.ledger_id = other_ledger.clone();
        if let TranscriptResult::Snapshot { cursor, .. } = &mut wrong_source.result {
            cursor.ledger_id = other_ledger;
        }
        // A first source can only be selected by the response when the
        // durable binding had no pin; the reply still must be internally
        // consistent, so this is accepted as a fresh source anchor.
        projection.apply(&wrong_source, 7).expect("fresh source");
        let page_reply = page(1, 1, vec![text(2, "two")]);
        assert!(matches!(
            projection.apply(&page_reply, 7),
            Err(TranscriptApplyError::SourceIdentityMismatch)
        ));
    }

    #[test]
    fn source_pin_rejects_first_reply_from_another_ledger() {
        let pinned = binding().with_pinned_source(Some(source())).expect("pin");
        let mut projection = TranscriptProjection::new(pinned);
        let mut wrong_source = snapshot(1, 1, vec![text(1, "one")]);
        let other_ledger = SourceId::new("other-ledger").expect("ledger");
        wrong_source.source.ledger_id = other_ledger.clone();
        if let TranscriptResult::Snapshot { cursor, .. } = &mut wrong_source.result {
            cursor.ledger_id = other_ledger;
        }
        assert_eq!(
            projection.apply(&wrong_source, 7),
            Err(TranscriptApplyError::SourceIdentityMismatch)
        );
    }

    #[test]
    fn malformed_page_is_rejected_without_mutation() {
        let mut projection = TranscriptProjection::new(binding());
        projection
            .apply(&snapshot(1, 1, vec![text(1, "one")]), 7)
            .expect("snapshot");
        let before = projection.clone();
        let mut malformed = page(1, 1, vec![text(2, "two")]);
        malformed.contract = "wrong".to_owned();
        assert!(matches!(
            projection.apply(&malformed, 7),
            Err(TranscriptApplyError::InvalidReply(_))
        ));
        assert_eq!(projection, before);
    }

    #[test]
    fn reset_reanchors_and_replaces_the_old_revision() {
        let mut projection = TranscriptProjection::new(binding());
        projection
            .apply(&snapshot(1, 1, vec![text(1, "old")]), 7)
            .expect("snapshot");
        let old_revision = projection.content_revision();
        let reset = reply(TranscriptResult::Reset {
            reason: ResetReason::RevisionChanged,
        });
        projection.apply(&reset, 7).expect("reset");
        assert!(projection.lines().is_empty());
        assert!(projection.cursor().is_none());
        assert!(projection.content_revision() > old_revision);

        projection
            .apply(&snapshot(1, 2, vec![text(1, "new")]), 7)
            .expect("new snapshot");
        assert_eq!(projection.lines()[0].as_str(), "new");
    }

    #[test]
    fn controls_and_format_characters_are_escaped_but_lf_remains() {
        let mut projection = TranscriptProjection::new(binding());
        projection
            .apply(
                &snapshot(1, 1, vec![text(1, "ok\t\n\r\x1b\u{0000}\u{200d}")]),
                7,
            )
            .expect("snapshot");
        assert_eq!(projection.lines()[0].as_str(), "ok\\t");
        assert_eq!(projection.lines()[1].as_str(), "\\r\\u{1b}\\u{0}\\u{200d}");
    }

    #[test]
    fn byte_and_line_caps_are_bounded_and_report_omissions() {
        let mut projection = TranscriptProjection::new(binding());
        let data = "x".repeat(coven_client::transcript::MAX_CHUNK_BYTES);
        projection
            .apply(&snapshot(1, 1, vec![text(1, &data)]), 7)
            .expect("snapshot");
        for sequence in 2..=5 {
            projection
                .apply(&page(sequence - 1, 1, vec![text(sequence, &data)]), 7)
                .expect("bounded page");
        }
        assert!(projection.retained_text_bytes() <= MAX_RETAINED_TEXT_BYTES);
        assert!(projection.has_omissions());
        assert!(projection.omissions().bytes > 0);

        let mut bounded = TranscriptProjection::new(binding());
        let total_lines = MAX_RETAINED_LOGICAL_LINES + 8;
        let per_response = usize::from(MAX_CHUNKS);
        let mut start = 1usize;
        while start <= total_lines {
            let end = (start + per_response - 1).min(total_lines);
            let chunks = (start..=end)
                .map(|sequence| text(i64::try_from(sequence).expect("seq"), "line\n"))
                .collect();
            let response = if start == 1 {
                snapshot(i64::try_from(end).expect("seq"), 1, chunks)
            } else {
                page(i64::try_from(start - 1).expect("seq"), 1, chunks)
            };
            bounded.apply(&response, 7).expect("bounded lines");
            start = end + 1;
        }
        assert!(bounded.line_count() <= MAX_RETAINED_LOGICAL_LINES);
        assert!(bounded.omissions().lines > 0);
    }

    #[test]
    fn known_source_drops_mark_the_projection_incomplete() {
        let mut projection = TranscriptProjection::new(binding());
        let mut response = snapshot(1, 1, vec![text(1, "retained")]);
        response.projection.dropped_output_bytes = Some(12);

        assert!(!projection.has_omissions());
        projection.apply(&response, 7).expect("snapshot");
        assert_eq!(projection.source_dropped_output_bytes(), Some(12));
        assert!(projection.has_omissions());
    }
}
