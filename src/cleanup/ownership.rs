//! Cross-runtime admission leases and durable session ownership. Path locks
//! cover ancestors so launching in a worktree subdirectory protects its root.
use super::{
    store::{self, Lease},
    Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub(crate) fn root() -> PathBuf {
    crate::config::config_dir()
        .parent()
        .unwrap_or(Path::new("."))
        .join("herdr-worktree-ownership-v1")
}

pub(crate) fn key(path: &Path) -> String {
    format!("{:x}", Sha256::digest(path.as_os_str().as_encoded_bytes()))
}

pub(crate) fn path_lock(root: &Path, path: &Path) -> PathBuf {
    root.join("leases").join(key(path))
}

pub(crate) struct Admission {
    _leases: Vec<Lease>,
}

pub(crate) fn admit_runtime(path: &Path) -> std::io::Result<Option<Admission>> {
    // Unit tests construct real PTYs but must never claim the user's session.
    // Ownership behavior is exercised against explicit isolated roots in tests.
    if cfg!(test) {
        return Ok(None);
    }
    Admission::acquire(path)
        .map(Some)
        .map_err(std::io::Error::other)
}

impl Admission {
    pub fn acquire(path: &Path) -> Result<Self> {
        Self::at(&root(), &crate::session::data_dir(), path)
    }

    pub fn at(root: &Path, session: &Path, path: &Path) -> Result<Self> {
        let path = std::fs::canonicalize(path)
            .map_err(|e| format!("launch directory unavailable: {e}"))?;
        let mut leases = Vec::new();
        for ancestor in path.ancestors() {
            leases.push(Lease::shared(&path_lock(root, ancestor))?);
        }
        // Persist intent before spawn. A crash cannot turn a resumable session
        // into an unowned candidate. The next authoritative session sync clears it.
        let _lock =
            Lease::exclusive_wait(&root.join("owners.lock"), std::time::Duration::from_secs(2))?;
        let file = root.join("owners").join(format!("{}.json", key(session)));
        let mut owner: Owner = store::read(&file)?;
        owner.session = session.to_path_buf();
        if !owner.paths.contains(&path) {
            owner.paths.push(path);
        }
        store::save(&file, &owner)?;
        Ok(Self { _leases: leases })
    }
}

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Owner {
    pub session: PathBuf,
    pub paths: Vec<PathBuf>,
}

/// Called with authoritative live and resumable session paths. Hold shared
/// leases while publishing so cleanup cannot pass between inspection and save.
pub(crate) fn sync(root: &Path, session: &Path, paths: &[PathBuf]) -> Result<()> {
    let mut canonical = Vec::new();
    let mut leases = Vec::new();
    for path in paths {
        let path =
            std::fs::canonicalize(path).map_err(|e| format!("session path unavailable: {e}"))?;
        for ancestor in path.ancestors() {
            leases.push(Lease::shared(&path_lock(root, ancestor))?);
        }
        if !canonical.contains(&path) {
            canonical.push(path);
        }
    }
    let _lock = Lease::exclusive(&root.join("owners.lock"))?;
    let file = root.join("owners").join(format!("{}.json", key(session)));
    let previous: Owner = store::read(&file)?;
    // A queued snapshot can predate a new launch. Keep its claim while an
    // admission lease exists, even when that snapshot omits the directory.
    for path in previous.paths {
        if !canonical.contains(&path) && Lease::exclusive(&path_lock(root, &path)).is_err() {
            canonical.push(path);
        }
    }
    store::save(
        &file,
        &Owner {
            session: session.to_path_buf(),
            paths: canonical,
        },
    )
}

pub(crate) fn reason(root: &Path, path: &Path) -> Result<Option<String>> {
    let dir = root.join("owners");
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    for entry in entries {
        let file = entry.map_err(|e| e.to_string())?.path();
        if file.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let owner: Owner = store::read(&file)?;
        if owner.paths.iter().any(|owned| owned.starts_with(path)) {
            return Ok(Some("live or resumable session owns worktree".into()));
        }
    }
    Ok(None)
}

pub(crate) fn import_sessions(root: &Path, config: &Path) -> Result<()> {
    let mut sessions = vec![config.to_path_buf()];
    match std::fs::read_dir(config.join("sessions")) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(|e| e.to_string())?;
                if entry.file_type().map_err(|e| e.to_string())?.is_dir() {
                    sessions.push(entry.path());
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.to_string()),
    }
    let _lock = Lease::exclusive(&root.join("owners.lock"))?;
    for session in sessions {
        let owner_file = root.join("owners").join(format!("{}.json", key(&session)));
        if owner_file.try_exists().map_err(|e| e.to_string())? {
            continue;
        }
        let bytes = match std::fs::read(session.join("session.json")) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.to_string()),
        };
        let snapshot: crate::persist::SessionSnapshot = serde_json::from_slice(&bytes)
            .map_err(|e| format!("saved session ownership cannot be established: {e}"))?;
        let mut paths = Vec::new();
        for workspace in snapshot.workspaces {
            paths.push(workspace.identity_cwd);
            if let Some(space) = workspace.worktree_space {
                paths.push(space.checkout_path);
            }
            for tab in workspace.tabs {
                for pane in tab.panes.into_values() {
                    paths.push(pane.cwd);
                }
            }
        }
        paths = paths
            .into_iter()
            .map(|p| std::fs::canonicalize(&p).unwrap_or(p))
            .collect();
        store::save(&owner_file, &Owner { session, paths })?;
    }
    Ok(())
}
