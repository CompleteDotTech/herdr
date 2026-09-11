use std::{collections::HashMap, sync::Arc};

use super::{TerminalId, TerminalRuntime};

/// Server-owned live terminal runtimes, keyed by durable terminal id.
///
/// This sits outside `AppState` so pure state can stay focused on workspace,
/// pane, and terminal metadata while the server/application layer owns PTYs,
/// parser backends, detector tasks, and channels.
#[derive(Default)]
pub(crate) struct TerminalRuntimeRegistry {
    runtimes: HashMap<TerminalId, TerminalRuntime>,
    external: HashMap<TerminalId, crate::runtime_provider::ProviderRuntimeHandle>,
    external_sessions: HashMap<TerminalId, Arc<crate::terminal::external::ExternalTerminalSession>>,
}

impl TerminalRuntimeRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get(&self, terminal_id: &TerminalId) -> Option<&TerminalRuntime> {
        self.runtimes.get(terminal_id)
    }

    pub(crate) fn insert(
        &mut self,
        terminal_id: TerminalId,
        runtime: TerminalRuntime,
    ) -> Option<TerminalRuntime> {
        if self.external.contains_key(&terminal_id)
            || self.external_sessions.contains_key(&terminal_id)
        {
            tracing::error!(terminal = %terminal_id, "refusing native runtime for external terminal");
            // Return ownership to the caller just as a displaced runtime is returned.
            return Some(runtime);
        }
        self.runtimes.insert(terminal_id, runtime)
    }

    /// External ownership is explicit; native accessors never invent a PTY.
    pub(crate) fn external(
        &self,
        terminal_id: &TerminalId,
    ) -> Option<&crate::runtime_provider::ProviderRuntimeHandle> {
        self.external.get(terminal_id)
    }

    pub(crate) fn external_session(
        &self,
        terminal_id: &TerminalId,
    ) -> Option<&Arc<crate::terminal::external::ExternalTerminalSession>> {
        self.external_sessions.get(terminal_id)
    }

    pub(crate) fn attach_external_session(
        &mut self,
        terminal_id: TerminalId,
        session: Arc<crate::terminal::external::ExternalTerminalSession>,
    ) -> Result<(), &'static str> {
        if self.runtimes.contains_key(&terminal_id)
            || self.external_sessions.contains_key(&terminal_id)
        {
            return Err("terminal already has an external model");
        }
        self.external_sessions.insert(terminal_id, session);
        Ok(())
    }

    pub(crate) fn remove_external_session(
        &mut self,
        terminal_id: &TerminalId,
    ) -> Option<Arc<crate::terminal::external::ExternalTerminalSession>> {
        let session = self.external_sessions.remove(terminal_id)?;
        session.close();
        Some(session)
    }

    pub(crate) fn attach_external(
        &mut self,
        terminal_id: TerminalId,
        runtime: crate::runtime_provider::ProviderRuntimeHandle,
    ) -> Result<(), &'static str> {
        const MAX_EXTERNAL_TERMINALS: usize = 64;
        if self.runtimes.contains_key(&terminal_id)
            || self.external.contains_key(&terminal_id)
            || self.external_sessions.contains_key(&terminal_id)
        {
            return Err("terminal already has a runtime");
        }
        if self.external.len() >= MAX_EXTERNAL_TERMINALS {
            return Err("external terminal limit reached");
        }
        self.external.insert(terminal_id, runtime);
        Ok(())
    }

    pub(crate) fn shutdown_external(&mut self, terminal_id: &TerminalId) {
        self.remove_external_session(terminal_id);
        if let Some(runtime) = self.external.remove(terminal_id) {
            runtime.detach();
            runtime.shutdown();
        }
    }

    pub(crate) fn remove(&mut self, terminal_id: &TerminalId) -> Option<TerminalRuntime> {
        self.runtimes.remove(terminal_id)
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &TerminalRuntime> {
        self.runtimes.values()
    }

    #[cfg(unix)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&TerminalId, &TerminalRuntime)> {
        self.runtimes.iter()
    }

    #[cfg(unix)]
    pub(crate) fn set_handoff_readers_paused(&self, paused: bool) {
        for runtime in self.runtimes.values() {
            runtime.set_handoff_reader_paused(paused);
        }
    }

    #[cfg(unix)]
    pub(crate) fn assume_handoff_ownership(&mut self) {
        for runtime in self.runtimes.values_mut() {
            runtime.assume_handoff_ownership();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.runtimes.len()
            + self.external.len()
            + self
                .external_sessions
                .keys()
                .filter(|id| !self.external.contains_key(*id))
                .count()
    }

    #[cfg(unix)]
    pub(crate) fn nudge_child_redraw_after_handoff(&self) {
        for runtime in self.runtimes.values() {
            runtime.nudge_child_redraw_after_handoff();
        }
    }

    #[cfg(unix)]
    pub(crate) fn drain_for_handoff(
        &mut self,
    ) -> impl Iterator<Item = (TerminalId, TerminalRuntime)> + '_ {
        self.runtimes.drain()
    }

    #[cfg(test)]
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = (TerminalId, TerminalRuntime)> + '_ {
        self.runtimes.drain()
    }
}

impl From<HashMap<TerminalId, TerminalRuntime>> for TerminalRuntimeRegistry {
    fn from(runtimes: HashMap<TerminalId, TerminalRuntime>) -> Self {
        Self {
            runtimes,
            external: HashMap::new(),
            external_sessions: HashMap::new(),
        }
    }
}
