//! Persisted external execution identity, separate from native resume metadata.
use coven_client::execution::{ExecutionAuthority, ExecutionScope, ExecutionSessionId};
use coven_client::source::{SourceCursor, SourceId, SourceIdentity};
use coven_client::terminal_checkpoint::SessionCursor;
use coven_terminal_checkpoint_transport::BoundCheckpoint;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::TerminalId;
use crate::runtime_provider::ManagedBinding;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BindingWire", into = "BindingWire")]
pub(crate) struct ExternalBinding {
    pub(crate) terminal_id: TerminalId,
    pub(crate) execution: ManagedBinding,
}

/// Atomic persisted state for a CTS2 observer. The bound blob includes the
/// stream identity, model/parser checkpoint, source cursor, and stable model
/// revision; the duplicated fields let restore reject torn or mismatched
/// records before any provider read is submitted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalCheckpointState {
    #[serde(
        serialize_with = "serialize_checkpoint_blob",
        deserialize_with = "deserialize_checkpoint_blob"
    )]
    pub(crate) blob: Arc<[u8]>,
    pub(crate) binding: coven_client::terminal_checkpoint::StreamBinding,
    pub(crate) cursor: SessionCursor,
    pub(crate) revision: u64,
    #[serde(default)]
    pub(crate) source_cursor: Option<SourceCursor>,
    #[serde(default)]
    pub(crate) omitted: bool,
    pub(crate) stream_id: String,
    pub(crate) stream_generation: u64,
}

impl ExternalCheckpointState {
    /// Validate the duplicated persistence fields against the bound envelope
    /// before they influence a reconnect request. This keeps a torn or
    /// hand-edited outer record from causing the owner to skip source bytes or
    /// restore a checkpoint under a different stream generation.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        let bound = BoundCheckpoint::decode_for(&self.blob, &self.binding)
            .map_err(|_| "persisted external checkpoint envelope is invalid")?;
        if bound.cursor != self.cursor {
            return Err("persisted external checkpoint cursor does not match its blob");
        }
        if bound.revision != self.revision {
            return Err("persisted external checkpoint revision does not match its blob");
        }
        if coven_client::terminal::TerminalStreamId::parse(&self.stream_id)
            .map(|id| *id.as_bytes() != bound.binding.stream_id)
            .unwrap_or(true)
        {
            return Err("persisted external checkpoint stream id does not match its blob");
        }
        if bound.binding.stream_generation != self.stream_generation {
            return Err("persisted external checkpoint stream generation does not match its blob");
        }
        Ok(())
    }
}

const MAX_EXTERNAL_CHECKPOINT_BLOB_BYTES: usize = 16 * 1024 * 1024;

fn serialize_checkpoint_blob<S>(bytes: &Arc<[u8]>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    bytes.as_ref().serialize(serializer)
}

fn deserialize_checkpoint_blob<'de, D>(deserializer: D) -> Result<Arc<[u8]>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BoundedBlob;
    impl<'de> serde::de::Visitor<'de> for BoundedBlob {
        type Value = Arc<[u8]>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a nonempty bounded terminal checkpoint byte array")
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            if seq
                .size_hint()
                .is_some_and(|size| size > MAX_EXTERNAL_CHECKPOINT_BLOB_BYTES)
            {
                return Err(serde::de::Error::custom(
                    "external checkpoint exceeds its bound",
                ));
            }
            let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
            while let Some(byte) = seq.next_element::<u8>()? {
                if bytes.len() == MAX_EXTERNAL_CHECKPOINT_BLOB_BYTES {
                    return Err(serde::de::Error::custom(
                        "external checkpoint exceeds its bound",
                    ));
                }
                bytes.push(byte);
            }
            if bytes.is_empty() {
                return Err(serde::de::Error::custom("external checkpoint is empty"));
            }
            Ok(bytes.into())
        }
    }
    deserializer.deserialize_seq(BoundedBlob)
}

/// Only this JSON persistence DTO crosses a restart. Runtime generations and
/// worker handles belong to the new server; neither is restored as authority.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BindingWire {
    Coven {
        terminal_id: TerminalId,
        provider_id: String,
        host_id: SourceId,
        scope: ExecutionScope,
        authority: ExecutionAuthority,
        session_id: ExecutionSessionId,
        generation: u64,
        pinned_source: Option<SourceIdentity>,
    },
}

impl TryFrom<BindingWire> for ExternalBinding {
    type Error = String;

    fn try_from(wire: BindingWire) -> Result<Self, Self::Error> {
        let BindingWire::Coven {
            terminal_id,
            provider_id,
            host_id,
            scope,
            authority,
            session_id,
            generation,
            pinned_source,
        } = wire;
        let id = terminal_id.as_str();
        if !id.starts_with("coven_")
            || id.len() <= "coven_".len()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        {
            return Err("invalid external terminal binding id".into());
        }
        let execution = ManagedBinding::new(
            provider_id,
            host_id,
            scope,
            authority,
            session_id,
            generation,
            1,
        )
        .and_then(|binding| binding.with_pinned_source(pinned_source))
        .map_err(|_| "invalid external execution binding".to_owned())?;
        Ok(Self {
            terminal_id,
            execution,
        })
    }
}

impl From<ExternalBinding> for BindingWire {
    fn from(binding: ExternalBinding) -> Self {
        let execution = binding.execution;
        Self::Coven {
            terminal_id: binding.terminal_id,
            provider_id: execution.provider_id,
            host_id: execution.host_id,
            scope: execution.scope,
            authority: execution.authority,
            session_id: execution.session_id,
            generation: execution.generation,
            pinned_source: execution.pinned_source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_client::terminal_checkpoint::StreamBinding;

    fn state() -> ExternalCheckpointState {
        let binding = StreamBinding::new("session", [7; 16], 3, 4, 5).expect("binding");
        let cursor = SessionCursor {
            sequence: 2,
            offset: 11,
        };
        let bound = BoundCheckpoint::new(binding.clone(), cursor, 2, b"checkpoint".to_vec())
            .expect("bound checkpoint");
        ExternalCheckpointState {
            blob: bound.encode().expect("encode").into(),
            binding: binding.clone(),
            cursor,
            revision: 2,
            source_cursor: None,
            omitted: false,
            stream_id: "07070707-0707-0707-0707-070707070707".into(),
            stream_generation: binding.stream_generation,
        }
    }

    #[test]
    fn checkpoint_outer_fields_must_match_bound_blob() {
        assert!(state().validate().is_ok());

        let mut cursor_tampered = state();
        cursor_tampered.cursor.offset += 1;
        assert!(cursor_tampered.validate().is_err());

        let mut stream_tampered = state();
        stream_tampered.stream_generation += 1;
        assert!(stream_tampered.validate().is_err());
    }

    #[test]
    fn checkpoint_json_preserves_uuid_and_shares_snapshot_bytes() {
        let original = state();
        let clone = original.clone();
        assert!(Arc::ptr_eq(&original.blob, &clone.blob));
        let json = serde_json::to_vec(&original).unwrap();
        let restored: ExternalCheckpointState = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, original);
        restored.validate().unwrap();
        let mut wrong_epoch = restored;
        wrong_epoch.binding.authority_epoch += 1;
        assert!(wrong_epoch.validate().is_err());
    }

    struct CountingBytes<'a> {
        announced: Option<usize>,
        observed: &'a std::cell::Cell<usize>,
    }

    impl<'de> serde::de::SeqAccess<'de> for CountingBytes<'_> {
        type Error = serde::de::value::Error;
        fn size_hint(&self) -> Option<usize> {
            self.announced
        }
        fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where
            T: serde::de::DeserializeSeed<'de>,
        {
            self.observed.set(self.observed.get() + 1);
            seed.deserialize(serde::de::value::U8Deserializer::new(0))
                .map(Some)
        }
    }

    #[test]
    fn checkpoint_decode_rejects_an_oversized_hint_before_reading_bytes() {
        let observed = std::cell::Cell::new(0);
        let seq = CountingBytes {
            announced: Some(usize::MAX),
            observed: &observed,
        };
        let result = deserialize_checkpoint_blob(serde::de::value::SeqAccessDeserializer::new(seq));
        assert!(result.is_err());
        assert_eq!(observed.get(), 0);
    }

    #[test]
    fn checkpoint_decode_stops_an_unbounded_sequence_at_the_limit() {
        let observed = std::cell::Cell::new(0);
        let seq = CountingBytes {
            announced: None,
            observed: &observed,
        };
        let result = deserialize_checkpoint_blob(serde::de::value::SeqAccessDeserializer::new(seq));
        assert!(result.is_err());
        assert_eq!(observed.get(), MAX_EXTERNAL_CHECKPOINT_BLOB_BYTES + 1);
    }
}
