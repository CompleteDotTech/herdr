//! Strict Git discovery and expected-commit mutations. Every subprocess is
//! noninteractive and bounded; no shell interpolation is used.
use super::{
    policy::{Candidate, MergeEvidence, Policy},
    Result,
};
use std::{
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

const OUTPUT_LIMIT: usize = 8 * 1024 * 1024;

pub(crate) fn command(program: &str, args: &[&str], cwd: &Path) -> Result<String> {
    let mut process = crate::noninteractive_process::command(program);
    // Git's environment overrides -C/current_dir, including config-injection
    // variables inherited from hooks. Keep credentials in normal Git config.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("GIT_") {
            process.env_remove(name);
        }
    }
    let mut child = process
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GCM_INTERACTIVE", "never")
        .env("GH_PROMPT_DISABLED", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("{program}: {e}"))?;
    let stdout = child.stdout.take().ok_or("missing stdout")?;
    let stderr = child.stderr.take().ok_or("missing stderr")?;
    fn drain(mut input: impl Read) -> std::io::Result<Vec<u8>> {
        let mut result = Vec::new();
        let mut buf = [0; 8192];
        loop {
            let count = input.read(&mut buf)?;
            if count == 0 {
                break;
            }
            if result.len() < OUTPUT_LIMIT + 1 {
                let keep = count.min(OUTPUT_LIMIT + 1 - result.len());
                result.extend_from_slice(&buf[..keep]);
            }
        }
        Ok(result)
    }
    let (out_tx, out_rx) = std::sync::mpsc::sync_channel(1);
    let (err_tx, err_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = out_tx.send(drain(stdout));
    });
    std::thread::spawn(move || {
        let _ = err_tx.send(drain(stderr));
    });
    let deadline = Instant::now() + Duration::from_secs(45);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            result => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(format!("{program} did not finish: {result:?}"));
            }
        }
    };
    // Descendants may inherit the pipes; don't join readers after timeout.
    let status = status?;
    let output = out_rx
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|_| "stdout unavailable before deadline")?
        .map_err(|e| e.to_string())?;
    let error = err_rx
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|_| "stderr unavailable before deadline")?
        .map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&error)
        ));
    }
    if output.len() > OUTPUT_LIMIT {
        return Err("Git output exceeds cleanup limit".into());
    }
    String::from_utf8(output).map_err(|_| "non-UTF-8 Git output; cleanup deferred".into())
}

#[derive(Clone, Debug)]
pub(crate) struct Repository {
    pub root: PathBuf,
    pub common: PathBuf,
}

impl Repository {
    pub fn open(path: &Path) -> Result<Self> {
        let common = command(
            "git",
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
            path,
        )?;
        let common = std::fs::canonicalize(common.trim()).map_err(|e| e.to_string())?;
        // The first entry is the primary checkout, which survives cleanup.
        let raw = command("git", &["worktree", "list", "--porcelain", "-z"], path)?;
        let first = raw
            .split('\0')
            .next()
            .and_then(|s| s.strip_prefix("worktree "))
            .ok_or("missing primary checkout")?;
        let root = std::fs::canonicalize(first).map_err(|e| e.to_string())?;
        Ok(Self { root, common })
    }

    pub fn git(&self, args: &[&str]) -> Result<String> {
        command("git", args, &self.root)
    }
    pub fn state_dir(&self) -> PathBuf {
        self.common.join("herdr-cleanup")
    }

    pub fn discover(&self) -> Result<Vec<Candidate>> {
        let raw = self.git(&["worktree", "list", "--porcelain", "-z"])?;
        let mut candidates = Vec::new();
        for block in raw.split("\0\0").filter(|block| !block.is_empty()) {
            let mut path = None;
            let mut branch = None;
            let mut commit = String::new();
            let mut locked = false;
            let mut unavailable = false;
            for field in block.split('\0') {
                if let Some(value) = field.strip_prefix("worktree ") {
                    path = Some(PathBuf::from(value));
                } else if let Some(value) = field.strip_prefix("HEAD ") {
                    commit = value.into();
                } else if let Some(value) = field.strip_prefix("branch refs/heads/") {
                    branch = Some(value.into());
                } else if field == "locked" || field.starts_with("locked ") {
                    locked = true;
                } else if field == "bare" || field.starts_with("prunable") {
                    unavailable = true;
                } else if field != "detached" && !field.is_empty() {
                    return Err("unknown worktree field".into());
                }
            }
            let path = path.ok_or("worktree path missing")?;
            // Store the same canonical spelling used by ownership leases and
            // configured retention exceptions. If a prunable worktree is gone,
            // retain its reported path and mark it unavailable below.
            let path = std::fs::canonicalize(&path).unwrap_or(path);
            let primary = candidates.is_empty();
            let details = self.worktree_identity(&path);
            let (git_dir, identity) = match details {
                Ok((dir, identity)) => (Some(dir), Some(identity)),
                Err(_) => {
                    unavailable = true;
                    (None, None)
                }
            };
            candidates.push(Candidate {
                path: Some(path),
                branch,
                commit,
                git_dir,
                identity,
                primary,
                locked,
                unavailable,
            });
        }
        let refs = self.git(&[
            "for-each-ref",
            "--format=%(refname)%00%(objectname)%00%(symref)",
            "refs/heads/",
        ])?;
        for line in refs.lines() {
            let fields: Vec<_> = line.split('\0').collect();
            if fields.len() != 3 {
                return Err("invalid branch metadata".into());
            }
            let branch = fields[0]
                .strip_prefix("refs/heads/")
                .ok_or("invalid branch ref")?;
            if candidates
                .iter()
                .any(|c| c.branch.as_deref() == Some(branch))
            {
                continue;
            }
            candidates.push(Candidate {
                path: None,
                branch: Some(branch.into()),
                commit: fields[1].into(),
                git_dir: None,
                identity: None,
                primary: false,
                locked: false,
                unavailable: !fields[2].is_empty(),
            });
        }
        Ok(candidates)
    }

    fn worktree_identity(&self, path: &Path) -> Result<(PathBuf, String)> {
        let actual = command(
            "git",
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
                "--git-dir",
            ],
            path,
        )?;
        let lines: Vec<_> = actual.lines().collect();
        if lines.len() != 2
            || std::fs::canonicalize(lines[0]).map_err(|e| e.to_string())? != self.common
        {
            return Err("worktree repository changed".into());
        }
        let git_dir = std::fs::canonicalize(lines[1]).map_err(|e| e.to_string())?;
        let marker = path.join(".git");
        let marker_metadata = std::fs::symlink_metadata(&marker).map_err(|e| e.to_string())?;
        if marker_metadata.file_type().is_symlink() {
            return Err("symlinked .git marker".into());
        }
        let identity = format!(
            "{}:{}:{}",
            crate::platform::cleanup_file_identity(path)?,
            crate::platform::cleanup_file_identity(&marker)?,
            if marker_metadata.is_file() {
                std::fs::read_to_string(marker).map_err(|e| e.to_string())?
            } else {
                String::new()
            }
        );
        Ok((git_dir, identity))
    }

    pub fn integration(&self, policy: &Policy) -> Result<(String, String)> {
        if self
            .common
            .join("info/grafts")
            .try_exists()
            .map_err(|e| e.to_string())?
        {
            return Err("legacy grafts make merge ancestry ambiguous".into());
        }
        if policy.remote.starts_with('-') || policy.remote.is_empty() {
            return Err("invalid cleanup remote".into());
        }
        let branch = match &policy.integration_branch {
            Some(branch) => branch.clone(),
            None => {
                let remote = self.git(&["ls-remote", "--symref", &policy.remote, "HEAD"])?;
                remote
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("ref: refs/heads/")
                            .and_then(|line| line.strip_suffix("\tHEAD"))
                    })
                    .ok_or("remote default branch unavailable")?
                    .into()
            }
        };
        self.git(&["check-ref-format", &format!("refs/heads/{branch}")])?;
        let remote_ref = format!("refs/remotes/{}/{branch}", policy.remote);
        let refspec = format!("+refs/heads/{branch}:{remote_ref}");
        self.git(&["fetch", "--no-tags", &policy.remote, &refspec])?;
        let commit = self.git(&["rev-parse", "--verify", &format!("{remote_ref}^{{commit}}")])?;
        Ok((branch, commit.trim().into()))
    }

    pub fn ancestor(&self, commit: &str, integration: &str) -> bool {
        self.git(&["merge-base", "--is-ancestor", commit, integration])
            .is_ok()
    }

    pub fn evidence(
        &self,
        c: &Candidate,
        policy: &Policy,
        branch: &str,
        integration: &str,
    ) -> Option<MergeEvidence> {
        if self.ancestor(&c.commit, integration) {
            return Some(MergeEvidence::Ancestor {
                integration: integration.into(),
            });
        }
        if !policy.github_merge_evidence {
            return None;
        }
        let remote = self.git(&["remote", "get-url", &policy.remote]).ok()?;
        let name = github_repository(remote.trim())?;
        if !matches!(c.commit.len(), 40 | 64) || !c.commit.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        // Commit association also covers detached worktrees and deleted source
        // branches. Exact head and integrated merge checks below remain required.
        let endpoint = format!("repos/{name}/commits/{}/pulls?per_page=100", c.commit);
        let result = command(
            "gh",
            &[
                "api",
                "--hostname",
                "github.com",
                "--method",
                "GET",
                &endpoint,
            ],
            &self.root,
        )
        .ok()?;
        let records: Vec<serde_json::Value> = serde_json::from_str(&result).ok()?;
        let records: Vec<_> = records.iter().map(|pr| serde_json::json!({
            "number": pr.get("number"), "headRefOid": pr.pointer("/head/sha"),
            "baseRefName": pr.pointer("/base/ref"), "mergeCommit": {"oid":pr.get("merge_commit_sha")},
            "mergedAt": pr.get("merged_at")
        })).collect();
        self.github_evidence(c, branch, integration, &records)
    }

    pub(super) fn github_evidence(
        &self,
        c: &Candidate,
        branch: &str,
        integration: &str,
        records: &[serde_json::Value],
    ) -> Option<MergeEvidence> {
        records.iter().find_map(|pr| {
            let source = pr.get("headRefOid")?.as_str()?;
            let merge = pr.get("mergeCommit")?.get("oid")?.as_str()?;
            if source != c.commit
                || pr.get("baseRefName")?.as_str()? != branch
                || pr.get("mergedAt")?.as_str()?.is_empty()
                || !self.ancestor(merge, integration)
            {
                return None;
            }
            Some(MergeEvidence::Github {
                integration: integration.into(),
                pull_request: pr.get("number")?.as_u64()?,
                source: source.into(),
                merge: merge.into(),
            })
        })
    }

    pub fn local_reason(&self, c: &Candidate) -> Result<Option<String>> {
        let Some(path) = &c.path else {
            return Ok(None);
        };
        let status = command(
            "git",
            &[
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--ignored=matching",
                "--ignore-submodules=none",
            ],
            path,
        )?;
        if !status.is_empty() {
            return Ok(Some("tracked, untracked, or ignored local files".into()));
        }
        let flags = command("git", &["ls-files", "-v", "-z"], path)?;
        if flags.split('\0').filter(|s| !s.is_empty()).any(|s| {
            s.as_bytes()
                .first()
                .is_some_and(|b| b.is_ascii_lowercase() || *b == b'S')
        }) {
            return Ok(Some("index flags can conceal tracked changes".into()));
        }
        let dir = c.git_dir.as_ref().ok_or("missing worktree Git directory")?;
        for name in [
            "index.lock",
            "config.worktree",
            "HEAD.lock",
            "MERGE_HEAD",
            "CHERRY_PICK_HEAD",
            "REVERT_HEAD",
            "BISECT_START",
            "rebase-apply",
            "rebase-merge",
            "sequencer",
        ] {
            if dir.join(name).try_exists().map_err(|e| e.to_string())? {
                return Ok(Some(format!("unfinished Git operation: {name}")));
            }
        }
        if !command("git", &["submodule", "status", "--recursive"], path)?
            .trim()
            .is_empty()
        {
            return Ok(Some("worktree contains submodules".into()));
        }
        Ok(None)
    }

    pub fn unchanged(&self, candidate: &Candidate) -> Result<()> {
        let found = self
            .discover()?
            .into_iter()
            .any(|current| current == *candidate);
        if found {
            Ok(())
        } else {
            Err("candidate identity or branch tip changed".into())
        }
    }

    pub fn remove_worktree(&self, candidate: &Candidate) -> Result<()> {
        self.unchanged(candidate)?;
        if let Some(reason) = self.local_reason(candidate)? {
            return Err(reason);
        }
        let path = candidate.path.as_ref().ok_or("missing worktree path")?;
        self.git(&[
            "worktree",
            "remove",
            "--",
            path.to_str().ok_or("non-UTF-8 worktree path")?,
        ])?;
        if path.try_exists().map_err(|e| e.to_string())? {
            return Err("worktree removal not verified".into());
        }
        Ok(())
    }

    pub fn remove_branch(&self, branch: &str, expected: &str) -> Result<()> {
        let candidates = self.discover()?;
        if candidates
            .iter()
            .any(|c| c.branch.as_deref() == Some(branch) && c.path.is_some())
        {
            return Err("branch is checked out".into());
        }
        let current = candidates
            .iter()
            .find(|c| c.branch.as_deref() == Some(branch))
            .ok_or("branch no longer exists")?;
        if current.commit != expected || current.unavailable {
            return Err("branch identity changed".into());
        }
        let reference = format!("refs/heads/{branch}");
        self.git(&["update-ref", "--no-deref", "-d", &reference, expected])?;
        if self
            .git(&["for-each-ref", "--format=%(refname)", &reference])?
            .lines()
            .any(|r| r == reference)
        {
            return Err("branch removal not verified".into());
        }
        Ok(())
    }
}

fn github_repository(remote: &str) -> Option<String> {
    let name = remote
        .strip_prefix("https://github.com/")
        .or_else(|| remote.strip_prefix("git@github.com:"))?
        .trim_end_matches(".git");
    let parts: Vec<_> = name.split('/').collect();
    (parts.len() == 2
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
        }))
    .then(|| name.into())
}
