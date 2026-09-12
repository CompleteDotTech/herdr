use super::App;
use crate::cleanup::service::{Action, Service};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Default)]
pub(crate) struct CleanupRuntime {
    service: Option<Service>,
    next_sync: Option<Instant>,
    last_paths: Vec<PathBuf>,
    generation: u64,
    report_generation: u64,
    notice: Option<String>,
}

impl App {
    pub(crate) fn poll_cleanup(&mut self, now: Instant) {
        if !self.policy.persist_session {
            return;
        }
        if self.cleanup.next_sync.is_some_and(|next| next > now) {
            return;
        }
        self.cleanup.next_sync = Some(now + Duration::from_secs(5));
        let paths = self.cleanup_owned_paths();
        let source = if self.cleanup.service.is_none() {
            "startup"
        } else if paths != self.cleanup.last_paths {
            "session_ownership_changed"
        } else {
            "periodic"
        };
        let service = self.cleanup.service.get_or_insert_with(Service::start);
        if let Err(error) = service.sync(paths.clone(), source.into()) {
            tracing::warn!(%error, "cleanup sync deferred");
            return;
        }
        self.cleanup.last_paths = paths;
        if let Ok(snapshot) = service.snapshot() {
            if snapshot.generation != self.cleanup.generation {
                self.cleanup.generation = snapshot.generation;
                if snapshot.report_generation != self.cleanup.report_generation {
                    self.cleanup.report_generation = snapshot.report_generation;
                    let removed = snapshot
                        .records
                        .iter()
                        .filter(|r| r.state == crate::cleanup::policy::State::Removed)
                        .count();
                    let deferred = snapshot
                        .records
                        .iter()
                        .filter(|r| {
                            r.state == crate::cleanup::policy::State::Deferred
                                && !r.candidate.primary
                        })
                        .count();
                    if removed > 0 {
                        self.cleanup.notice = Some(format!("Removed {removed} merged items; retained {deferred}. Details: herdr worktree cleanup inspect"));
                    }
                }
                self.event_hub.push(crate::api::schema::EventEnvelope {
                    event: crate::api::schema::EventKind::WorktreeCleanup,
                    data: crate::api::schema::EventData::WorktreeCleanup {
                        generation: snapshot.generation,
                        records: snapshot
                            .records
                            .iter()
                            .filter_map(|r| serde_json::to_value(r).ok())
                            .collect(),
                        diagnostic: snapshot.diagnostic.clone(),
                    },
                });
                if let Some(error) = snapshot.diagnostic {
                    tracing::warn!(%error, "worktree cleanup deferred");
                }
            }
        }
    }

    pub(crate) fn cleanup_trigger(&mut self) {
        self.cleanup.next_sync = None;
    }

    pub(crate) fn take_cleanup_notice(&mut self) -> Option<String> {
        self.cleanup.notice.take()
    }

    fn cleanup_owned_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        // A workspace remains resumable even with no live terminal runtime.
        for workspace in &self.state.workspaces {
            paths.push(workspace.identity_cwd.clone());
            if let Some(space) = workspace.worktree_space() {
                paths.push(space.checkout_path.clone());
            }
            for tab in &workspace.tabs {
                for pane in tab.panes.values() {
                    if let Some(terminal) = self.state.terminals.get(&pane.attached_terminal_id) {
                        paths.push(terminal.cwd.clone());
                    }
                }
            }
        }
        for terminal in self.state.terminals.values() {
            paths.push(terminal.cwd.clone());
        }
        paths.sort();
        paths.dedup();
        paths
    }

    pub(crate) fn handle_cleanup_api(&mut self, id: String, action: Action) -> String {
        self.poll_cleanup(Instant::now());
        let result = (|| {
            let service = self
                .cleanup
                .service
                .as_ref()
                .ok_or("cleanup unavailable in this runtime")?;
            let operation_id = if action != Action::Inspect {
                Some(service.submit(action)?)
            } else {
                None
            };
            service.snapshot().map(|snapshot| (snapshot, operation_id))
        })();
        match result {
            Ok((snapshot, operation_id)) => serde_json::json!({"id": id, "result": {"type": "worktree_cleanup", "snapshot": snapshot, "operation_id": operation_id}}).to_string(),
            Err(error) => serde_json::json!({"id": id, "error": {"code": "cleanup_unavailable", "message": error}}).to_string(),
        }
    }
}
