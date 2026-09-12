use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub enabled: bool,
    pub remote: String,
    /// Branch name on `remote`; absent means resolve the live remote HEAD.
    pub integration_branch: Option<String>,
    pub protected_branches: Vec<String>,
    pub retained_branches: Vec<String>,
    pub retained_worktrees: Vec<PathBuf>,
    pub github_merge_evidence: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: true,
            remote: "origin".into(),
            integration_branch: None,
            protected_branches: ["main", "master", "develop", "development", "trunk"]
                .map(str::to_owned)
                .to_vec(),
            retained_branches: Vec::new(),
            retained_worktrees: Vec::new(),
            github_merge_evidence: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Candidate {
    pub path: Option<PathBuf>,
    pub branch: Option<String>,
    pub commit: String,
    pub git_dir: Option<PathBuf>,
    /// Canonical .git file contents and filesystem identity, captured together.
    pub identity: Option<String>,
    pub primary: bool,
    pub locked: bool,
    pub unavailable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum MergeEvidence {
    Ancestor {
        integration: String,
    },
    Github {
        integration: String,
        pull_request: u64,
        source: String,
        merge: String,
    },
}

impl MergeEvidence {
    pub fn integration(&self) -> &str {
        match self {
            Self::Ancestor { integration } | Self::Github { integration, .. } => integration,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum State {
    Pending,
    Deferred,
    Removing,
    Removed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    pub repository: PathBuf,
    pub candidate: Candidate,
    pub evidence: Option<MergeEvidence>,
    pub trigger: String,
    pub timestamp: u64,
    pub attempts: u32,
    pub state: State,
    pub reason: String,
    pub worktree_removed: bool,
    pub branch_removed: bool,
}

/// Pure policy: ordering is stable so repeated triggers explain the same facts.
pub(crate) fn retention_reason(
    policy: &Policy,
    candidate: &Candidate,
    integration_branch: &str,
    evidence: Option<&MergeEvidence>,
    ownership_reason: Option<&str>,
    local_reason: Option<&str>,
) -> Option<String> {
    let reason = if !policy.enabled {
        Some("automatic cleanup disabled")
    } else if candidate.primary {
        Some("primary checkout")
    } else if candidate.unavailable {
        Some("worktree identity unavailable")
    } else if candidate.locked {
        Some("worktree locked")
    } else if candidate.branch.as_ref().is_some_and(|branch| {
        branch == integration_branch || policy.protected_branches.contains(branch)
    }) {
        Some("protected branch")
    } else if candidate
        .branch
        .as_ref()
        .is_some_and(|branch| policy.retained_branches.contains(branch))
        || candidate
            .path
            .as_ref()
            .is_some_and(|path| policy.retained_worktrees.contains(path))
    {
        Some("explicit retention exception")
    } else if let Some(reason) = ownership_reason {
        Some(reason)
    } else if let Some(reason) = local_reason {
        Some(reason)
    } else if evidence.is_none() {
        Some("merge not verified at current commit")
    } else {
        None
    };
    reason.map(str::to_owned)
}
