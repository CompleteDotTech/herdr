//! One bounded background command queue per runtime. Repository locks coordinate
//! separate runtimes; settings and ownership survive client detach and crashes.
use super::{
    engine::Engine,
    git::Repository,
    ownership,
    policy::{Policy, Record},
    store::{self, Lease, Settings},
    Result,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, RwLock,
    },
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Inspect,
    Register {
        repository: PathBuf,
    },
    Preview {
        repository: Option<PathBuf>,
    },
    Reconcile {
        repository: Option<PathBuf>,
    },
    Configure {
        repository: Option<PathBuf>,
        policy: Policy,
    },
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Snapshot {
    pub boot_id: String,
    pub report_generation: u64,
    pub generation: u64,
    pub busy: bool,
    pub settings: Settings,
    pub records: Vec<Record>,
    pub diagnostic: Option<String>,
    pub operations: Vec<Operation>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Operation {
    pub id: u64,
    pub state: String,
    pub diagnostic: Option<String>,
}

enum Command {
    Sync {
        paths: Vec<PathBuf>,
        trigger: String,
    },
    Action(u64, Action),
}

pub(crate) struct Service {
    sender: mpsc::SyncSender<Command>,
    snapshot: Arc<RwLock<Snapshot>>,
    sync_pending: Arc<AtomicBool>,
    sequence: AtomicU64,
}

impl Service {
    pub fn start() -> Self {
        Self::start_at(
            crate::config::config_dir().join("cleanup"),
            crate::session::data_dir(),
            ownership::root(),
        )
    }

    pub(crate) fn start_at(directory: PathBuf, session: PathBuf, owners: PathBuf) -> Self {
        let (sender, receiver) = mpsc::sync_channel(16);
        let snapshot = Arc::new(RwLock::new(Snapshot {
            boot_id: format!("{}-{:?}", std::process::id(), std::time::SystemTime::now()),
            ..Snapshot::default()
        }));
        let shared = snapshot.clone();
        let sync_pending = Arc::new(AtomicBool::new(false));
        let worker_pending = sync_pending.clone();
        std::thread::spawn(move || {
            Worker {
                directory,
                session,
                owners,
                shared,
                fingerprints: HashMap::new(),
                ready: false,
                owned_paths: Vec::new(),
                sync_pending: worker_pending,
            }
            .run(receiver)
        });
        Self {
            sender,
            snapshot,
            sync_pending,
            sequence: AtomicU64::new(1),
        }
    }

    pub fn sync(&self, paths: Vec<PathBuf>, trigger: String) -> Result<()> {
        if self.sync_pending.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = self
            .sender
            .try_send(Command::Sync { paths, trigger })
            .map_err(|e| e.to_string());
        if result.is_err() {
            self.sync_pending.store(false, Ordering::Release);
        }
        result
    }

    pub fn submit(&self, action: Action) -> Result<u64> {
        let id = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.sender
            .try_send(Command::Action(id, action))
            .map_err(|e| e.to_string())?;
        Ok(id)
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        self.snapshot
            .read()
            .map(|s| s.clone())
            .map_err(|_| "cleanup snapshot unavailable".into())
    }
}

struct Worker {
    directory: PathBuf,
    session: PathBuf,
    owners: PathBuf,
    shared: Arc<RwLock<Snapshot>>,
    fingerprints: HashMap<PathBuf, String>,
    ready: bool,
    owned_paths: Vec<PathBuf>,
    sync_pending: Arc<AtomicBool>,
}

impl Worker {
    fn run(mut self, receiver: mpsc::Receiver<Command>) {
        loop {
            let command = match receiver.recv_timeout(Duration::from_secs(30)) {
                Ok(command) => Some(command),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            let operation_id = match &command {
                Some(Command::Action(id, _)) => Some(*id),
                _ => None,
            };
            if matches!(&command, Some(Command::Sync { .. })) {
                self.sync_pending.store(false, Ordering::Release);
            }
            self.publish(|s| {
                s.busy = true;
                if let Some(id) = operation_id {
                    s.operations.push(Operation {
                        id,
                        state: "running".into(),
                        diagnostic: None,
                    });
                }
            });
            let result = self.step(command);
            self.publish(|s| {
                s.busy = false;
                s.generation = s.generation.saturating_add(1);
                s.diagnostic = result.err();
                if let Some(id) = operation_id {
                    if let Some(operation) = s.operations.iter_mut().find(|op| op.id == id) {
                        operation.state = if s.diagnostic.is_some() {
                            "failed"
                        } else {
                            "completed"
                        }
                        .into();
                        operation.diagnostic = s.diagnostic.clone();
                    }
                }
                if s.operations.len() > 64 {
                    s.operations.drain(..s.operations.len() - 64);
                }
            });
        }
    }

    fn publish(&self, update: impl FnOnce(&mut Snapshot)) {
        if let Ok(mut snapshot) = self.shared.write() {
            update(&mut snapshot);
        }
    }

    fn step(&mut self, command: Option<Command>) -> Result<()> {
        let settings_path = self.directory.join("settings.json");
        // Inspection reports the shared worker state. It must not become an
        // accidental reconciliation request merely because it traverses this
        // common command path.
        if matches!(&command, Some(Command::Action(_, Action::Inspect))) {
            let _lease = Lease::shared(&self.directory.join("settings.lock"))?;
            let settings: Settings = store::read(&settings_path)?;
            self.publish(|snapshot| snapshot.settings = settings);
            return Ok(());
        }
        let (mut settings, preview, selected, trigger, force) = {
            let _lease = Lease::exclusive(&self.directory.join("settings.lock"))?;
            let mut settings: Settings = store::read(&settings_path)?;
            let mut preview = false;
            let mut selected = None;
            let mut trigger = "periodic".to_string();
            let mut force = false;
            match command {
                Some(Command::Sync {
                    paths,
                    trigger: source,
                }) => {
                    if !self.ready {
                        let config = self
                            .directory
                            .parent()
                            .ok_or("cleanup configuration directory missing")?;
                        ownership::import_sessions(&self.owners, config)?;
                        if let Some(parent) = config.parent() {
                            for name in ["herdr", "herdr-dev"] {
                                ownership::import_sessions(&self.owners, &parent.join(name))?;
                            }
                        }
                    }
                    let changed = self.owned_paths != paths;
                    force = !self.ready || changed;
                    ownership::sync(&self.owners, &self.session, &paths)?;
                    self.owned_paths = paths.clone();
                    self.ready = true;
                    trigger = source;
                    for path in paths.into_iter().filter(|_| force) {
                        // Non-Git workspaces are valid; they do not register repositories.
                        if let Ok(repo) = Repository::open(&path) {
                            if !settings
                                .repositories
                                .iter()
                                .any(|r| r.repository == repo.root)
                            {
                                settings.repositories.push(store::Registration {
                                    repository: repo.root,
                                    policy: None,
                                    next_attempt: 0,
                                    failures: 0,
                                });
                            }
                        }
                    }
                }
                Some(Command::Action(_, action)) => match action {
                    Action::Inspect => {}
                    Action::Register { repository } => {
                        let repo = Repository::open(&repository)?;
                        if !settings
                            .repositories
                            .iter()
                            .any(|r| r.repository == repo.root)
                        {
                            settings.repositories.push(store::Registration {
                                repository: repo.root,
                                policy: None,
                                next_attempt: 0,
                                failures: 0,
                            });
                        }
                        trigger = "repository_registered".into();
                    }
                    Action::Configure {
                        repository,
                        mut policy,
                    } => {
                        for path in &mut policy.retained_worktrees {
                            if !path.is_absolute() {
                                return Err("retained worktree paths must be absolute".into());
                            }
                            if let Ok(canonical) = std::fs::canonicalize(&path) {
                                *path = canonical;
                            }
                        }
                        if let Some(repository) = repository {
                            let root = Repository::open(&repository)?.root;
                            let entry = settings
                                .repositories
                                .iter_mut()
                                .find(|r| r.repository == root)
                                .ok_or("repository must be registered before configuring")?;
                            entry.policy = Some(policy);
                            entry.next_attempt = 0;
                        } else {
                            settings.policy = policy;
                        }
                        trigger = "configuration_changed".into();
                        // Policy changes, especially a newly enabled policy or
                        // removed retention exception, are an explicit trigger.
                        force = true;
                    }
                    Action::Preview { repository } => {
                        preview = true;
                        selected = repository;
                        trigger = "preview".into();
                        force = true;
                    }
                    Action::Reconcile { repository } => {
                        selected = repository;
                        trigger = "manual".into();
                        force = true;
                    }
                },
                None => {}
            }
            store::save(&settings_path, &settings)?;
            (settings, preview, selected, trigger, force)
        };
        self.publish(|s| s.settings = settings.clone());
        if !self.ready {
            return Err("waiting for authoritative session ownership".into());
        }
        let selected = selected
            .map(|p| Repository::open(&p).map(|r| r.root))
            .transpose()?;
        if selected
            .as_ref()
            .is_some_and(|path| !settings.repositories.iter().any(|r| &r.repository == path))
        {
            return Err("repository is not registered".into());
        }
        let mut records = Vec::new();
        let mut diagnostics = Vec::new();
        for registration in &mut settings.repositories {
            if selected
                .as_ref()
                .is_some_and(|p| p != &registration.repository)
            {
                continue;
            }
            let policy = registration.policy.as_ref().unwrap_or(&settings.policy);
            if !policy.enabled && !preview {
                continue;
            }
            let current = super::now();
            let attempt = (|| -> Result<Option<Vec<Record>>> {
                let repo = Repository::open(&registration.repository)?;
                let fingerprint = repo.git(&[
                    "for-each-ref",
                    "--format=%(refname) %(objectname)",
                    "refs/heads/",
                    "refs/remotes/",
                ])?;
                let changed = self
                    .fingerprints
                    .insert(repo.common.clone(), fingerprint.clone())
                    .as_ref()
                    != Some(&fingerprint);
                if !force
                    && registration.next_attempt > current
                    && (!changed || registration.failures > 0)
                {
                    return Ok(None);
                }
                let engine = Engine {
                    settings_path: Some(&settings_path),
                    repository: &repo,
                    ownership_root: &self.owners,
                    policy,
                    trigger: if changed {
                        "git_references_changed"
                    } else {
                        &trigger
                    },
                    preview,
                    process_use: crate::platform::cleanup_process_use,
                };
                engine.reconcile().map(Some)
            })();
            match attempt {
                Ok(None) => continue,
                Ok(Some(result)) => {
                    let offline = result
                        .iter()
                        .any(|r| r.reason.starts_with("integration unavailable:"));
                    registration.failures = if offline {
                        registration.failures.saturating_add(1)
                    } else {
                        0
                    };
                    records.extend(result);
                }
                Err(error) => {
                    registration.failures = registration.failures.saturating_add(1);
                    tracing::warn!(repository = %registration.repository.display(), %error, "cleanup deferred");
                    diagnostics.push(format!("{}: {error}", registration.repository.display()));
                }
            }
            let delay = if registration.failures == 0 {
                300
            } else {
                (30u64.saturating_mul(1u64 << registration.failures.min(7))).min(3600)
            };
            registration.next_attempt = current.saturating_add(delay);
        }
        // Merge scheduling fields into fresh settings; preserve concurrent policy edits.
        let _lease = Lease::exclusive(&self.directory.join("settings.lock"))?;
        let mut fresh: Settings = store::read(&settings_path)?;
        for update in settings.repositories {
            if let Some(entry) = fresh
                .repositories
                .iter_mut()
                .find(|r| r.repository == update.repository)
            {
                entry.next_attempt = update.next_attempt;
                entry.failures = update.failures;
            }
        }
        store::save(&settings_path, &fresh)?;
        self.publish(|s| {
            s.settings = fresh;
            if !records.is_empty() {
                s.report_generation = s.report_generation.saturating_add(1);
                s.records = records;
            }
        });
        if diagnostics.is_empty() {
            Ok(())
        } else {
            Err(diagnostics.join("; "))
        }
    }
}
