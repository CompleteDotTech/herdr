//! Owner-local provider selection. JSON API callers select a configured id;
//! they cannot supply a Coven home, credentials, executable, or environment.
use std::{
    collections::HashSet,
    fmt,
    fs::File,
    io::Read,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use coven_client::{
    execution::{AuthorityId, ExecutionAuthority, ExecutionScope, ProfileId, ProjectId},
    source::SourceId,
};
use serde::Deserialize;

use super::{ProviderConfig, ProviderId, ProviderIdentity};

pub(crate) const OWNER_SETTINGS_FILE: &str = "runtime-providers.toml";
const MAX_SETTINGS_BYTES: usize = 64 * 1024;
const MAX_PROVIDERS: usize = 16;
const DEFAULT_QUEUE_CAPACITY: usize = 8;

#[derive(Default)]
pub(crate) struct OwnerProviderSettings {
    pub(crate) enabled: bool,
    pub(crate) providers: Vec<ProviderConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SettingsError {
    ReadFailed,
    TooLarge,
    InvalidDocument,
    UnsupportedVersion,
    TooManyProviders,
    InvalidEntry { index: usize, field: &'static str },
    DuplicateProvider,
    DuplicateScope,
}

impl fmt::Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadFailed => f.write_str("could not read owner provider settings"),
            Self::TooLarge => f.write_str("owner provider settings exceed the size limit"),
            Self::InvalidDocument => f.write_str("invalid owner provider settings document"),
            Self::UnsupportedVersion => f.write_str("unsupported owner provider settings version"),
            Self::TooManyProviders => f.write_str("too many configured providers"),
            Self::InvalidEntry { index, field } => {
                write!(f, "invalid provider entry {index} field {field}")
            }
            Self::DuplicateProvider => f.write_str("duplicate provider id"),
            Self::DuplicateScope => f.write_str("duplicate provider host/project/profile scope"),
        }
    }
}

impl std::error::Error for SettingsError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    providers: Vec<Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    id: String,
    host_id: String,
    project_id: String,
    profile_id: String,
    policy_generation: u64,
    authority_id: String,
    authority_generation: u64,
    #[serde(default)]
    allow_actions: bool,
    #[serde(default)]
    project_root: Option<PathBuf>,
    coven_home: PathBuf,
}

impl OwnerProviderSettings {
    /// Called once at server initialization, never from a view or render path.
    /// Missing settings preserve the native-only default. Error messages omit
    /// paths and parser input because diagnostics may cross the JSON API.
    pub(crate) fn load(config_dir: &Path) -> Result<Self, SettingsError> {
        let file = match File::open(config_dir.join(OWNER_SETTINGS_FILE)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(_) => return Err(SettingsError::ReadFailed),
        };
        let mut bytes = Vec::new();
        file.take((MAX_SETTINGS_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| SettingsError::ReadFailed)?;
        Self::parse(&bytes)
    }

    fn parse(bytes: &[u8]) -> Result<Self, SettingsError> {
        if bytes.len() > MAX_SETTINGS_BYTES {
            return Err(SettingsError::TooLarge);
        }
        let text = std::str::from_utf8(bytes).map_err(|_| SettingsError::InvalidDocument)?;
        let document: Document =
            toml::from_str(text).map_err(|_| SettingsError::InvalidDocument)?;
        if document.version != 1 {
            return Err(SettingsError::UnsupportedVersion);
        }
        if document.providers.len() > MAX_PROVIDERS {
            return Err(SettingsError::TooManyProviders);
        }
        let mut ids = HashSet::new();
        let mut scopes = HashSet::new();
        let mut providers = Vec::with_capacity(document.providers.len());
        for (index, entry) in document.providers.into_iter().enumerate() {
            let invalid = |field| SettingsError::InvalidEntry { index, field };
            if !entry.coven_home.is_absolute() || entry.coven_home.as_os_str().len() > 4096 {
                return Err(invalid("coven_home"));
            }
            let provider_id = ProviderId::new(entry.id).map_err(|_| invalid("id"))?;
            let host_id = SourceId::new(entry.host_id).map_err(|_| invalid("host_id"))?;
            let scope = ExecutionScope {
                project_id: ProjectId::new(entry.project_id).map_err(|_| invalid("project_id"))?,
                profile_id: ProfileId::new(entry.profile_id).map_err(|_| invalid("profile_id"))?,
                policy_generation: entry.policy_generation,
            };
            let authority = ExecutionAuthority::new(
                AuthorityId::new(entry.authority_id).map_err(|_| invalid("authority_id"))?,
                entry.authority_generation,
            )
            .map_err(|_| invalid("authority_generation"))?;
            if !ids.insert(provider_id.clone()) {
                return Err(SettingsError::DuplicateProvider);
            }
            if !scopes.insert((
                host_id.as_str().to_owned(),
                scope.project_id.as_str().to_owned(),
                scope.profile_id.as_str().to_owned(),
            )) {
                return Err(SettingsError::DuplicateScope);
            }
            let identity = ProviderIdentity::new(provider_id, host_id, scope, authority)
                .map_err(|_| invalid("identity"))?;
            let queue_capacity = NonZeroUsize::new(DEFAULT_QUEUE_CAPACITY)
                .ok_or_else(|| invalid("queue_capacity"))?;
            providers.push(
                ProviderConfig::new(identity, entry.coven_home.clone(), queue_capacity)
                    .map_err(|_| invalid("coven_home"))?
                    .with_project_root(
                        entry
                            .project_root
                            .unwrap_or_else(|| entry.coven_home.clone()),
                    )
                    .map_err(|_| invalid("project_root"))?
                    .with_actions_allowed(entry.allow_actions),
            );
        }
        Ok(Self {
            enabled: document.enabled,
            providers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, home: &str) -> String {
        format!(
            r#"[[providers]]
id = "{id}"
host_id = "host"
project_id = "project"
profile_id = "profile"
policy_generation = 1
authority_id = "authority"
authority_generation = 1
coven_home = {home:?}
"#
        )
    }

    #[test]
    fn explicit_opt_in_is_required() {
        let settings = OwnerProviderSettings::parse(b"version = 1").unwrap();
        assert!(!settings.enabled);
        assert!(settings.providers.is_empty());
    }

    #[test]
    fn settings_errors_do_not_echo_owner_local_input() {
        let secret = b"version = 1\ncredential = 'sensitive-owner-value'";
        let error = OwnerProviderSettings::parse(secret).err().unwrap();
        assert_eq!(error, SettingsError::InvalidDocument);
        assert!(!error.to_string().contains("sensitive-owner-value"));
        assert_eq!(
            OwnerProviderSettings::parse(&vec![b'x'; MAX_SETTINGS_BYTES + 1]).err(),
            Some(SettingsError::TooLarge)
        );
    }

    #[test]
    fn relative_provider_home_is_rejected_before_starting_workers() {
        let text = format!(
            "version = 1\nenabled = true\n{}",
            entry("provider", "relative/home")
        );
        assert_eq!(
            OwnerProviderSettings::parse(text.as_bytes()).err(),
            Some(SettingsError::InvalidEntry {
                index: 0,
                field: "coven_home"
            })
        );
    }

    #[test]
    fn duplicate_provider_and_scope_are_rejected() {
        let home = std::env::current_dir().unwrap();
        let home = home.to_str().unwrap();
        for (second_id, expected) in [
            ("first", SettingsError::DuplicateProvider),
            ("second", SettingsError::DuplicateScope),
        ] {
            let text = format!(
                "version = 1\nenabled = true\n{}{}",
                entry("first", home),
                entry(second_id, home)
            );
            assert_eq!(
                OwnerProviderSettings::parse(text.as_bytes()).err(),
                Some(expected)
            );
        }
    }
}
