use super::{
    git::Repository,
    ownership,
    policy::{self, Candidate, MergeEvidence, Policy, Record, State},
    store::{self, Lease},
    Result,
};
use std::path::Path;

pub(crate) struct Engine<'a> {
    pub repository: &'a Repository,
    pub ownership_root: &'a Path,
    pub policy: &'a Policy,
    pub trigger: &'a str,
    pub preview: bool,
    pub settings_path: Option<&'a Path>,
    pub process_use: fn(&Path) -> crate::platform::CleanupProcessUse,
}

impl Engine<'_> {
    pub fn reconcile(&self) -> Result<Vec<Record>> {
        let repo = self.repository;
        let _policy_lease = self
            .settings_path
            .map(|path| Lease::shared(&path.with_file_name("settings.lock")))
            .transpose()?;
        let _lease = Lease::exclusive(&repo.state_dir().join("repository.lock"))?;
        if let Some(path) = self.settings_path {
            let settings: store::Settings = store::read(path)?;
            let current = settings
                .repositories
                .iter()
                .find(|r| r.repository == repo.root)
                .ok_or("repository registration changed")?
                .policy
                .as_ref()
                .unwrap_or(&settings.policy);
            if current != self.policy {
                return Err("cleanup policy changed; reconciliation deferred".into());
            }
        }
        let history = self.history()?;
        let integration = repo.integration(self.policy);
        let candidates = repo.discover()?;
        let mut records = Vec::new();
        for candidate in candidates {
            let attempts = history
                .iter()
                .rev()
                .find(|r| r.candidate == candidate)
                .map_or(1, |r| r.attempts.saturating_add(1));
            let mut record = Record {
                repository: repo.root.clone(),
                candidate: candidate.clone(),
                evidence: None,
                trigger: self.trigger.into(),
                timestamp: super::now(),
                attempts,
                state: State::Pending,
                reason: String::new(),
                worktree_removed: false,
                branch_removed: false,
            };
            let (branch, commit) = match &integration {
                Ok(integration) => integration,
                Err(reason) => {
                    record.state = State::Deferred;
                    record.reason = format!("integration unavailable: {reason}");
                    self.record(&record)?;
                    records.push(record);
                    continue;
                }
            };
            // Cheap exclusions precede provider/network checks.
            let early = policy::retention_reason(self.policy, &candidate, branch, None, None, None);
            if early
                .as_deref()
                .is_some_and(|r| r != "merge not verified at current commit")
            {
                record.state = State::Deferred;
                record.reason = early.unwrap_or_default();
                self.record(&record)?;
                records.push(record);
                continue;
            }
            record.evidence = repo.evidence(&candidate, self.policy, branch, commit);
            let outcome = self.evaluate_and_apply(&mut record, branch);
            if let Err(reason) = outcome {
                record.state = State::Deferred;
                record.reason = reason;
            }
            self.record(&record)?;
            records.push(record);
        }
        // Recovery is evidence-driven: the next discovery treats any surviving
        // branch as its own candidate and verifies its current commit afresh.
        // Finish interrupted records only when both old resources are absent.
        for mut previous in history.into_iter().filter(|r| {
            r.state == State::Removing
                || (r.worktree_removed && !r.branch_removed && r.candidate.branch.is_some())
        }) {
            if self.resources_absent(&previous.candidate)? {
                previous.state = State::Removed;
                previous.reason = "reconciled removal after interrupted operation".into();
                previous.timestamp = super::now();
                previous.trigger = self.trigger.into();
                previous.worktree_removed = previous.candidate.path.is_some();
                previous.branch_removed = previous.candidate.branch.is_some();
                self.record(&previous)?;
                records.push(previous);
            }
        }
        Ok(records)
    }

    fn evaluate_and_apply(&self, record: &mut Record, integration_branch: &str) -> Result<()> {
        let candidate = &record.candidate;
        // Check the durable owner record before attempting the path lease. Some
        // platforms coalesce advisory locks held by one process, so an in-process
        // cleanup pass must still honour a live runtime admission.
        let owned = self.ownership_reason(candidate)?;
        let local = self.repository.local_reason(candidate)?;
        if let Some(reason) = policy::retention_reason(
            self.policy,
            candidate,
            integration_branch,
            record.evidence.as_ref(),
            owned.as_deref(),
            local.as_deref(),
        ) {
            return Err(reason);
        }
        let _path_lease = match &candidate.path {
            Some(path) => Some(Lease::exclusive(&ownership::path_lock(
                self.ownership_root,
                path,
            ))?),
            None => None,
        };
        // Recheck after acquiring the lease so a concurrent admission cannot
        // publish ownership between the initial read and a destructive action.
        let owned = self.ownership_reason(candidate)?;
        let local = self.repository.local_reason(candidate)?;
        if let Some(reason) = policy::retention_reason(
            self.policy,
            candidate,
            integration_branch,
            record.evidence.as_ref(),
            owned.as_deref(),
            local.as_deref(),
        ) {
            return Err(reason);
        }
        if self.preview {
            record.reason = "eligible; preview made no changes".into();
            return Ok(());
        }
        // Persist intent before mutation, then revalidate under exclusive ownership.
        record.state = State::Removing;
        record.reason = "verified and preparing removal".into();
        self.record(record)?;
        self.repository.unchanged(&record.candidate)?;
        self.revalidate_evidence(record, integration_branch)?;
        if let Some(reason) = self.ownership_reason(&record.candidate)? {
            return Err(reason);
        }
        if record.candidate.path.is_some() {
            self.repository.remove_worktree(&record.candidate)?;
            record.worktree_removed = true;
            self.record(record)?;
        }
        if let Some(branch) = &record.candidate.branch {
            self.repository
                .remove_branch(branch, &record.candidate.commit)?;
            record.branch_removed = true;
        }
        record.state = State::Removed;
        record.reason = "verified merged resources removed".into();
        Ok(())
    }

    fn revalidate_evidence(&self, record: &Record, integration_branch: &str) -> Result<()> {
        let evidence = record.evidence.as_ref().ok_or("missing merge evidence")?;
        let current = self.repository.git(&[
            "rev-parse",
            "--verify",
            &format!("refs/remotes/{}/{}", self.policy.remote, integration_branch),
        ])?;
        if current.trim() != evidence.integration() {
            return Err("integration reference changed".into());
        }
        let ancestor = match evidence {
            MergeEvidence::Ancestor { .. } => &record.candidate.commit,
            MergeEvidence::Github { source, merge, .. } if source == &record.candidate.commit => {
                merge
            }
            _ => return Err("merge source changed".into()),
        };
        if !self.repository.ancestor(ancestor, evidence.integration()) {
            return Err("merge evidence changed".into());
        }
        Ok(())
    }

    fn ownership_reason(&self, candidate: &Candidate) -> Result<Option<String>> {
        let Some(path) = &candidate.path else {
            return Ok(None);
        };
        if let Some(reason) = ownership::reason(self.ownership_root, path)? {
            return Ok(Some(reason));
        }
        Ok(match (self.process_use)(path) {
            crate::platform::CleanupProcessUse::Clear => None,
            crate::platform::CleanupProcessUse::Active(pid) => {
                Some(format!("worktree in use by process {pid}"))
            }
            crate::platform::CleanupProcessUse::Unknown(reason) => {
                Some(format!("process ownership uncertain: {reason}"))
            }
        })
    }

    fn resources_absent(&self, candidate: &Candidate) -> Result<bool> {
        if let Some(path) = &candidate.path {
            if path.try_exists().map_err(|e| e.to_string())? {
                return Ok(false);
            }
        }
        Ok(!self.repository.discover()?.iter().any(|c| {
            candidate
                .path
                .as_ref()
                .is_some_and(|p| c.path.as_ref() == Some(p))
                || candidate
                    .branch
                    .as_ref()
                    .is_some_and(|b| c.branch.as_ref() == Some(b))
        }))
    }

    fn record(&self, record: &Record) -> Result<()> {
        let mut records = self.history()?;
        if let Some(previous) = records.iter_mut().find(|r| r.candidate == record.candidate) {
            *previous = record.clone();
        } else {
            records.push(record.clone());
        }
        store::save(&self.repository.state_dir().join("records.json"), &records)?;
        store::append(&self.repository.state_dir().join("audit.jsonl"), record)
    }

    pub fn history(&self) -> Result<Vec<Record>> {
        store::read(&self.repository.state_dir().join("records.json"))
    }
}
