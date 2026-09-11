//! Query and side-effect policy at the terminal owner boundary.

use serde::{Deserialize, Serialize};

use crate::session::Geometry;

/// There is one owner for terminal query replies.  Renderers and API readers
/// never receive a processor copy and never reply to a query themselves.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryReplyPolicy {
    /// Collect reply bytes and resize metadata in the owner's ordered report.
    Owner,
    /// Keep VT model semantics, but drop bytes and host side effects.
    #[default]
    Quiet,
}

/// A monotonically increasing order within one applied raw chunk.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct EffectOrder(pub u32);

/// A resize request observed while processing a raw chunk.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResizeNotice {
    pub order: EffectOrder,
    pub geometry: Geometry,
}

/// A side effect emitted by the model's handler.
///
/// Only `Reply` and `Resize` are exported to the caller. Clipboard, bell,
/// title, and file/media actions are represented as suppressed diagnostics so
/// a caller can audit policy without accidentally performing the action.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EffectKind {
    Reply(Vec<u8>),
    Resize(ResizeNotice),
    ClipboardSuppressed,
    BellSuppressed,
    TitleSuppressed,
    FileSuppressed,
    ProcessSuppressed,
}

/// One ordered side effect from the owner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderedEffect {
    pub order: EffectOrder,
    pub kind: EffectKind,
}

impl OrderedEffect {
    pub fn reply(&self) -> Option<&[u8]> {
        match &self.kind {
            EffectKind::Reply(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn resize(&self) -> Option<ResizeNotice> {
        match self.kind {
            EffectKind::Resize(notice) => Some(notice),
            _ => None,
        }
    }
}
