use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Opaque identity for a server-owned terminal.
///
/// During the pane-backed transition this is stored one-to-one beside panes,
/// but callers must not derive it from a pane id or layout position.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TerminalId(String);

static NEXT_TERMINAL_ID: AtomicU64 = AtomicU64::new(1);

impl TerminalId {
    pub fn alloc() -> Self {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_micros())
            .unwrap_or(0);
        let counter = NEXT_TERMINAL_ID.fetch_add(1, Ordering::Relaxed);
        Self(format!("term_{micros:x}{counter:x}"))
    }

    pub(crate) fn try_alloc_external() -> Option<Self> {
        static NEXT_EXTERNAL_ID: AtomicU64 = AtomicU64::new(1);
        let counter = NEXT_EXTERNAL_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .ok()?;
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_micros();
        Some(Self(format!("coven_{micros:x}_{counter:x}")))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TerminalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
