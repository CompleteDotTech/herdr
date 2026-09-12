use super::{
    policy::{Policy, Record},
    Result,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

/// The handle owns the OS lock; never unlink a lock file (including on drop).
pub(crate) struct Lease {
    _file: File,
}

impl Drop for Lease {
    fn drop(&mut self) {
        // A concurrently forked child can briefly inherit the open description
        // before exec closes CLOEXEC descriptors. Release our lock explicitly
        // so that child cannot extend this owner's lifetime accidentally.
        if let Err(error) = self._file.unlock() {
            tracing::warn!(%error, "cleanup lease unlock failed");
        }
    }
}

impl Lease {
    pub fn exclusive(path: &Path) -> Result<Self> {
        Self::open(path, false)
    }
    /// Metadata publication is short-lived. Wait briefly rather than rejecting
    /// a newly launched runtime solely because another runtime is updating its
    /// ownership record. Path leases remain nonblocking safety barriers.
    pub fn exclusive_wait(path: &Path, timeout: std::time::Duration) -> Result<Self> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match Self::exclusive(path) {
                Ok(lease) => return Ok(lease),
                Err(error) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    if std::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
    pub fn shared(path: &Path) -> Result<Self> {
        Self::open(path, true)
    }

    fn open(path: &Path, shared: bool) -> Result<Self> {
        let parent = path.parent().ok_or("lock has no parent")?;
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        if shared {
            file.try_lock_shared()
        } else {
            file.try_lock()
        }
        .map_err(|e| format!("cleanup ownership unavailable at {}: {e}", path.display()))?;
        Ok(Self { _file: file })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Registration {
    pub repository: PathBuf,
    pub policy: Option<Policy>,
    pub next_attempt: u64,
    pub failures: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Settings {
    pub policy: Policy,
    pub repositories: Vec<Registration>,
}

/// Write under a caller-held lock. Sync the new contents before publishing them.
pub(crate) fn save<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().ok_or("state path has no parent")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let temporary = path.with_extension("json.next");
    let mut file = File::create(&temporary).map_err(|e| e.to_string())?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    crate::platform::cleanup_publish_file(&temporary, path).map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) fn read<T: serde::de::DeserializeOwned + Default>(path: &Path) -> Result<T> {
    match std::fs::read(path) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|e| format!("invalid cleanup state: {e}"))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(e) => Err(e.to_string()),
    }
}

/// Append-only audit, synced before a destructive step. A partial last line is
/// reported as invalid by readers rather than silently authorizing a retry.
pub(crate) fn append(path: &Path, record: &Record) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let mut bytes = serde_json::to_vec(record).map_err(|e| e.to_string())?;
    bytes.push(b'\n');
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())
}
