use super::{
    engine::Engine,
    git::Repository,
    ownership::{self, Admission},
    policy::{Policy, State},
    store::Lease,
};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    _test_lock: Lease,
    dir: PathBuf,
    repo: Repository,
    policy: Policy,
}

impl Fixture {
    fn new() -> Self {
        // nextest launches unit tests in separate processes. Serialize these
        // Git worktree fixtures because their process-visibility assertions
        // intentionally inspect process-wide state.
        let test_lock_path = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|parent| parent.to_path_buf()))
            .unwrap_or_else(std::env::temp_dir)
            .join("herdr-cleanup-fixtures-v1.lock");
        let test_lock =
            Lease::exclusive_wait(&test_lock_path, std::time::Duration::from_secs(120)).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "herdr-cleanup-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        git(&dir, &["init", "--bare", "remote.git"]);
        git(&dir, &["init", "--initial-branch=main", "repo"]);
        let root = dir.join("repo");
        git(&root, &["config", "user.email", "cleanup@example.invalid"]);
        git(&root, &["config", "user.name", "Cleanup Test"]);
        git(&root, &["commit", "--allow-empty", "-m", "initial"]);
        git(
            &root,
            &[
                "remote",
                "add",
                "origin",
                dir.join("remote.git").to_str().unwrap(),
            ],
        );
        git(&root, &["push", "origin", "main"]);
        git(
            &dir.join("remote.git"),
            &["symbolic-ref", "HEAD", "refs/heads/main"],
        );
        let repo = Repository::open(&root).unwrap();
        Self {
            _test_lock: test_lock,
            dir,
            repo,
            policy: Policy {
                github_merge_evidence: false,
                ..Policy::default()
            },
        }
    }
    fn worktree(&self, name: &str) -> PathBuf {
        let path = self.dir.join(name);
        git(
            &self.repo.root,
            &[
                "worktree",
                "add",
                "-b",
                name,
                path.to_str().unwrap(),
                "main",
            ],
        );
        path
    }
    fn reconcile(&self, preview: bool) -> Vec<super::policy::Record> {
        let engine = Engine {
            settings_path: None,
            repository: &self.repo,
            ownership_root: &self.dir.join("ownership"),
            policy: &self.policy,
            trigger: "test",
            preview,
            process_use: |_| crate::platform::CleanupProcessUse::Clear,
        };
        // Concurrent fixture subprocesses can briefly inherit a CLOEXEC lock
        // across fork. Real reconciliation retries lock contention as well.
        for _ in 0..20 {
            match engine.reconcile() {
                Ok(records) => return records,
                Err(error) if error.contains("ownership unavailable") => {
                    std::thread::sleep(std::time::Duration::from_millis(10))
                }
                Err(error) => panic!("{error}"),
            }
        }
        engine.reconcile().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
fn git(path: &Path, args: &[&str]) -> String {
    let result = Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap()
}

#[test]
fn cleanup_removes_merged_shell_created_worktree_and_branch_preserves_primary_and_remote() {
    let f = Fixture::new();
    let path = f.worktree("merged");
    let records = f.reconcile(false);
    assert!(!path.exists());
    assert!(records
        .iter()
        .any(|r| r.candidate.branch.as_deref() == Some("merged")
            && r.state == State::Removed
            && r.branch_removed));
    assert!(f.repo.root.exists());
    assert_eq!(
        git(&f.repo.root, &["branch", "--format=%(refname:short)"]).trim(),
        "main"
    );
    assert!(
        git(&f.repo.root, &["ls-remote", "origin", "refs/heads/main"]).contains("refs/heads/main")
    );
    assert!(f.reconcile(false).iter().all(|r| r.state != State::Removed));
}

#[test]
fn cleanup_preview_and_local_work_are_preserved() {
    let f = Fixture::new();
    let clean = f.worktree("clean");
    let dirty = f.worktree("dirty");
    let ignored = f.worktree("ignored");
    std::fs::write(dirty.join("draft"), "valuable").unwrap();
    git(
        &ignored,
        &[
            "config",
            "core.excludesFile",
            f.dir.join("exclude").to_str().unwrap(),
        ],
    );
    std::fs::write(f.dir.join("exclude"), "secret\n").unwrap();
    std::fs::write(ignored.join("secret"), "valuable").unwrap();
    f.reconcile(true);
    assert!(clean.exists());
    let results = f.reconcile(false);
    assert!(!clean.exists());
    assert!(dirty.join("draft").exists());
    assert!(ignored.join("secret").exists());
    assert_eq!(
        results
            .iter()
            .filter(|r| r.reason.contains("local files"))
            .count(),
        2
    );
}

#[test]
fn cleanup_preserves_locked_protected_and_unmerged_worktrees() {
    let mut f = Fixture::new();
    let locked = f.worktree("locked");
    let protected = f.worktree("protected");
    let retained = f.worktree("retained");
    let unmerged = f.worktree("unmerged");
    git(
        &f.repo.root,
        &["worktree", "lock", locked.to_str().unwrap()],
    );
    f.policy.protected_branches.push("protected".into());
    f.policy.retained_worktrees.push(retained.clone());
    git(
        &unmerged,
        &["commit", "--allow-empty", "-m", "unmerged change"],
    );
    f.reconcile(false);
    assert!(locked.exists() && protected.exists() && retained.exists() && unmerged.exists());
}

#[test]
fn cleanup_ownership_blocks_removal_and_admission_while_exclusive() {
    let f = Fixture::new();
    let path = f.worktree("owned");
    let owner_root = f.dir.join("ownership");
    let session = f.dir.join("session");
    let admission = Admission::at(&owner_root, &session, &path).unwrap();
    f.reconcile(false);
    assert!(path.exists());
    drop(admission);
    assert!(ownership::reason(&owner_root, &path).unwrap().is_some());
    f.reconcile(false); // resumable ownership survives the process lease
    assert!(path.exists());
    ownership::sync(&owner_root, &session, &[]).unwrap();
    let exclusive = Lease::exclusive(&ownership::path_lock(&owner_root, &path)).unwrap();
    assert!(Admission::at(&owner_root, &session, &path).is_err());
    drop(exclusive);
    f.reconcile(false);
    assert!(!path.exists());
}

#[test]
fn cleanup_admission_waits_for_owner_metadata_publication() {
    let f = Fixture::new();
    let path = f.worktree("metadata-lock");
    let root = f.dir.join("ownership");
    let session = f.dir.join("session");
    let lock = Lease::exclusive(&root.join("owners.lock")).unwrap();
    let admission = std::thread::spawn({
        let root = root.clone();
        let session = session.clone();
        let path = path.clone();
        move || Admission::at(&root, &session, &path)
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    drop(lock);
    drop(admission.join().unwrap().unwrap());
}

#[test]
fn cleanup_rechecks_candidate_commit_and_branch_checkout() {
    let f = Fixture::new();
    let path = f.worktree("changed");
    let old = f
        .repo
        .discover()
        .unwrap()
        .into_iter()
        .find(|c| c.path.as_ref() == Some(&path))
        .unwrap();
    git(&path, &["commit", "--allow-empty", "-m", "new work"]);
    assert!(f.repo.remove_worktree(&old).is_err());
    assert!(f.repo.remove_branch("changed", &old.commit).is_err());
    git(&f.repo.root, &["branch", "loose", "main"]);
    let original = git(&f.repo.root, &["rev-parse", "loose"]).trim().to_owned();
    git(&f.repo.root, &["branch", "-f", "loose", "changed"]);
    assert!(f.repo.remove_branch("loose", &original).is_err());
    assert!(path.exists());
}

#[test]
fn cleanup_repository_lock_and_offline_remote_defer_without_mutation() {
    let f = Fixture::new();
    let path = f.worktree("merged");
    let lock = Lease::exclusive(&f.repo.state_dir().join("repository.lock")).unwrap();
    assert!(Lease::exclusive(&f.repo.state_dir().join("repository.lock")).is_err());
    drop(lock);
    git(
        &f.repo.root,
        &[
            "remote",
            "set-url",
            "origin",
            f.dir.join("missing.git").to_str().unwrap(),
        ],
    );
    assert!(f
        .reconcile(false)
        .iter()
        .all(|r| r.state == State::Deferred));
    assert!(path.exists());
}

#[test]
fn cleanup_strict_discovery_preserves_newline_paths() {
    let f = Fixture::new();
    let path = f.dir.join("line\nbreak");
    git(
        &f.repo.root,
        &[
            "worktree",
            "add",
            "--detach",
            path.to_str().unwrap(),
            "main",
        ],
    );
    assert!(f
        .repo
        .discover()
        .unwrap()
        .iter()
        .any(|c| c.path.as_ref() == Some(&path) && !c.unavailable));
    f.reconcile(false);
    assert!(!path.exists());
}

#[test]
fn cleanup_squash_evidence_requires_exact_source_and_integrated_merge() {
    let f = Fixture::new();
    let path = f.worktree("squashed");
    std::fs::write(path.join("feature"), "first").unwrap();
    git(&path, &["add", "feature"]);
    git(&path, &["commit", "-m", "feature"]);
    let candidate = f
        .repo
        .discover()
        .unwrap()
        .into_iter()
        .find(|c| c.path.as_ref() == Some(&path))
        .unwrap();
    git(&f.repo.root, &["merge", "--squash", "squashed"]);
    git(&f.repo.root, &["commit", "-m", "squash"]);
    let integration = git(&f.repo.root, &["rev-parse", "HEAD"]).trim().to_owned();
    assert!(!f.repo.ancestor(&candidate.commit, &integration));
    let evidence = vec![
        serde_json::json!({"number": 1, "headRefOid": candidate.commit,
        "baseRefName": "main", "mergeCommit": {"oid": integration}, "mergedAt": "2026-09-12T00:00:00Z"}),
    ];
    assert!(f
        .repo
        .github_evidence(&candidate, "main", &integration, &evidence)
        .is_some());
    git(
        &path,
        &["commit", "--allow-empty", "-m", "new work after squash"],
    );
    let newer = f
        .repo
        .discover()
        .unwrap()
        .into_iter()
        .find(|c| c.path.as_ref() == Some(&path))
        .unwrap();
    assert!(f
        .repo
        .github_evidence(&newer, "main", &integration, &evidence)
        .is_none());
    assert!(f
        .repo
        .github_evidence(&candidate, "other", &integration, &evidence)
        .is_none());
    assert!(f
        .repo
        .github_evidence(&candidate, "main", &candidate.commit, &evidence)
        .is_none());
}

#[test]
fn cleanup_recovers_after_worktree_removal_and_preserves_recreated_branch() {
    let f = Fixture::new();
    let path = f.worktree("interrupted");
    let candidate = f
        .repo
        .discover()
        .unwrap()
        .into_iter()
        .find(|c| c.path.as_ref() == Some(&path))
        .unwrap();
    let state = super::policy::Record {
        repository: f.repo.root.clone(),
        candidate: candidate.clone(),
        evidence: Some(super::policy::MergeEvidence::Ancestor {
            integration: candidate.commit.clone(),
        }),
        trigger: "interrupted".into(),
        timestamp: 1,
        attempts: 1,
        state: State::Removing,
        reason: "before removal".into(),
        worktree_removed: false,
        branch_removed: false,
    };
    super::store::save(
        &f.repo.state_dir().join("records.json"),
        &vec![state.clone()],
    )
    .unwrap();
    f.repo.remove_worktree(&candidate).unwrap();
    let results = f.reconcile(false);
    assert!(results
        .iter()
        .any(|r| r.candidate == candidate && r.state == State::Removed));
    git(&f.repo.root, &["branch", "interrupted", "main"]);
    let new = f.worktree("new");
    git(&new, &["commit", "--allow-empty", "-m", "new work"]);
    git(&f.repo.root, &["branch", "-f", "interrupted", "new"]);
    super::store::save(&f.repo.state_dir().join("records.json"), &vec![state]).unwrap();
    f.reconcile(false);
    assert_eq!(
        git(&f.repo.root, &["rev-parse", "interrupted"]),
        git(&f.repo.root, &["rev-parse", "new"])
    );
}

#[test]
fn cleanup_unknown_process_visibility_and_unfinished_operation_defer() {
    let f = Fixture::new();
    let path = f.worktree("process");
    let engine = Engine {
        settings_path: None,
        repository: &f.repo,
        ownership_root: &f.dir.join("ownership"),
        policy: &f.policy,
        trigger: "test",
        preview: false,
        process_use: |_| crate::platform::CleanupProcessUse::Unknown("access denied".into()),
    };
    let result = engine.reconcile().unwrap();
    assert!(result.iter().any(|r| r.reason.contains("access denied")));
    assert!(path.exists());
    let candidate = f
        .repo
        .discover()
        .unwrap()
        .into_iter()
        .find(|c| c.path.as_ref() == Some(&path))
        .unwrap();
    std::fs::write(
        candidate.git_dir.unwrap().join("MERGE_HEAD"),
        &candidate.commit,
    )
    .unwrap();
    f.reconcile(false);
    assert!(path.exists());
}

#[test]
fn cleanup_policy_is_independent_of_agent_tool_and_trigger() {
    let f = Fixture::new();
    f.worktree("eligible");
    let mut expected = None;
    for trigger in [
        "merge",
        "agent_exit",
        "startup",
        "periodic",
        "shell",
        "provider",
    ] {
        let records = Engine {
            settings_path: None,
            repository: &f.repo,
            ownership_root: &f.dir.join("ownership"),
            policy: &f.policy,
            trigger,
            preview: true,
            process_use: |_| crate::platform::CleanupProcessUse::Clear,
        }
        .reconcile()
        .unwrap();
        let decisions: Vec<_> = records
            .into_iter()
            .map(|r| (r.candidate, r.state, r.reason))
            .collect();
        if let Some(expected) = &expected {
            assert_eq!(&decisions, expected);
        } else {
            expected = Some(decisions);
        }
    }
}

#[test]
fn cleanup_stale_session_snapshot_cannot_erase_a_new_admission() {
    let f = Fixture::new();
    let path = f.worktree("new-session");
    let root = f.dir.join("ownership");
    let session = f.dir.join("session");
    let admission = Admission::at(&root, &session, &path).unwrap();
    ownership::sync(&root, &session, &[]).unwrap();
    assert!(ownership::reason(&root, &path).unwrap().is_some());
    drop(admission);
    ownership::sync(&root, &session, &[]).unwrap();
    assert!(ownership::reason(&root, &path).unwrap().is_none());
}

#[test]
fn cleanup_preserves_tracked_changes_hidden_by_index_flags() {
    for flag in ["--assume-unchanged", "--skip-worktree"] {
        let f = Fixture::new();
        std::fs::write(f.repo.root.join("tracked"), "committed").unwrap();
        git(&f.repo.root, &["add", "tracked"]);
        git(&f.repo.root, &["commit", "-m", "tracked file"]);
        git(&f.repo.root, &["push", "origin", "main"]);
        let path = f.worktree("hidden");
        git(&path, &["update-index", flag, "tracked"]);
        std::fs::write(path.join("tracked"), "uncommitted").unwrap();
        assert!(f
            .reconcile(false)
            .iter()
            .any(|r| r.reason.contains("index flags")));
        assert_eq!(
            std::fs::read_to_string(path.join("tracked")).unwrap(),
            "uncommitted"
        );
    }
}

#[test]
fn cleanup_stale_policy_is_rejected_under_settings_lease() {
    let f = Fixture::new();
    let path = f.worktree("retained-later");
    let settings_file = f.dir.join("settings.json");
    super::store::save(
        &settings_file,
        &super::store::Settings {
            policy: Policy {
                enabled: false,
                ..Policy::default()
            },
            repositories: vec![super::store::Registration {
                repository: f.repo.root.clone(),
                policy: None,
                next_attempt: 0,
                failures: 0,
            }],
        },
    )
    .unwrap();
    let engine = Engine {
        settings_path: Some(&settings_file),
        repository: &f.repo,
        ownership_root: &f.dir.join("ownership"),
        policy: &f.policy,
        trigger: "old-policy",
        preview: false,
        process_use: |_| crate::platform::CleanupProcessUse::Clear,
    };
    assert!(engine.reconcile().unwrap_err().contains("policy changed"));
    assert!(path.exists());
}

#[test]
fn cleanup_worker_persists_registration_policy_and_reports_operations() {
    use super::service::{Action, Service};
    let f = Fixture::new();
    let directory = f.dir.join("config/cleanup");
    let settings_file = directory.join("settings.json");
    super::store::save(
        &settings_file,
        &super::store::Settings {
            policy: Policy {
                enabled: false,
                ..Policy::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let service = Service::start_at(directory, f.dir.join("config"), f.dir.join("ownership"));
    service
        .sync(vec![f.repo.root.clone()], "startup".into())
        .unwrap();
    let operation = service
        .submit(Action::Preview {
            repository: Some(f.repo.root.clone()),
        })
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let snapshot = service.snapshot().unwrap();
        if let Some(op) = snapshot
            .operations
            .iter()
            .find(|op| op.id == operation && op.state != "running")
        {
            assert_eq!(op.state, "completed", "{:?}", op.diagnostic);
            assert_eq!(snapshot.settings.repositories.len(), 1);
            assert!(!snapshot.settings.policy.enabled);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "worker did not complete: {snapshot:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let settings: super::store::Settings = super::store::read(&settings_file).unwrap();
    assert_eq!(settings.repositories[0].repository, f.repo.root);
}

#[test]
fn cleanup_inspect_never_reconciles_or_removes_resources() {
    use super::service::{Action, Service};
    let f = Fixture::new();
    let path = f.worktree("inspect-only");
    let directory = f.dir.join("config/cleanup");
    super::store::save(
        &directory.join("settings.json"),
        &super::store::Settings {
            policy: Policy::default(),
            repositories: vec![super::store::Registration {
                repository: f.repo.root.clone(),
                policy: None,
                next_attempt: 0,
                failures: 0,
            }],
        },
    )
    .unwrap();
    let service = Service::start_at(directory, f.dir.join("config"), f.dir.join("ownership"));
    let operation = service.submit(Action::Inspect).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let snapshot = service.snapshot().unwrap();
        if snapshot
            .operations
            .iter()
            .any(|op| op.id == operation && op.state == "completed")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "inspect did not settle"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        path.exists(),
        "inspect must not remove an eligible worktree"
    );
}

#[test]
fn cleanup_git_subprocess_ignores_inherited_repository_routing() {
    let f = Fixture::new();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cleanup::tests::cleanup_git_environment_probe",
            "--nocapture",
        ])
        .current_dir(&f.repo.root)
        .env("HERDR_CLEANUP_ENV_PROBE", "1")
        .env("GIT_DIR", f.dir.join("wrong.git"))
        .env("GIT_WORK_TREE", f.dir.join("wrong-tree"))
        .env("GIT_INDEX_FILE", f.dir.join("wrong-index"))
        .env("GIT_NAMESPACE", "wrong")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cleanup_git_environment_probe() {
    if std::env::var_os("HERDR_CLEANUP_ENV_PROBE").is_none() {
        return;
    }
    let cwd = std::env::current_dir().unwrap();
    let repo = Repository::open(&cwd).unwrap();
    assert_eq!(repo.root, cwd);
    assert_eq!(
        repo.git(&["rev-parse", "--abbrev-ref", "HEAD"])
            .unwrap()
            .trim(),
        "main"
    );
}

#[test]
fn cleanup_replacement_objects_do_not_manufacture_merge_evidence() {
    let f = Fixture::new();
    let path = f.worktree("unmerged");
    git(&path, &["commit", "--allow-empty", "-m", "unmerged"]);
    let commit = git(&path, &["rev-parse", "HEAD"]).trim().to_owned();
    let integration = git(&f.repo.root, &["rev-parse", "main"]).trim().to_owned();
    git(&f.repo.root, &["replace", &integration, &commit]);
    assert!(!f.repo.ancestor(&commit, &integration));
    f.reconcile(false);
    assert!(path.exists());
}
