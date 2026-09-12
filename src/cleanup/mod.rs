//! Deterministic, owner-local Git cleanup. No agent or model participates.
pub(crate) mod engine;
pub(crate) mod git;
pub(crate) mod ownership;
pub(crate) mod policy;
pub(crate) mod service;
pub(crate) mod store;

#[cfg(test)]
mod tests;

pub(crate) type Result<T> = std::result::Result<T, String>;

pub(crate) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_secs())
        .unwrap_or_default()
}
