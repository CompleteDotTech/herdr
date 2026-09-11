use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use coven_client::execution::{
    AuthorityId, ExecutionAuthority, ExecutionHarness, ExecutionIntent, ExecutionMode,
    ExecutionRequest, ExecutionScope, ExecutionTarget, ProfileId, ProjectId, RequestId,
};
use coven_client::source::SourceId;

use super::*;

fn identity() -> ProviderIdentity {
    ProviderIdentity::new(
        ProviderId::new("fixture").unwrap(),
        SourceId::new("host").unwrap(),
        ExecutionScope {
            project_id: ProjectId::new("project").unwrap(),
            profile_id: ProfileId::new("profile").unwrap(),
            policy_generation: 1,
        },
        ExecutionAuthority::new(AuthorityId::new("authority").unwrap(), 1).unwrap(),
    )
    .unwrap()
}

fn config() -> ProviderConfig {
    ProviderConfig::new(
        identity(),
        PathBuf::from("/definitely/missing/coven-home"),
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap()
}

fn launch_request(identity: &ProviderIdentity) -> ExecutionRequest {
    ExecutionRequest {
        contract: coven_client::execution::CONTRACT.to_owned(),
        request_id: RequestId::new("request").unwrap(),
        scope: identity.scope.clone(),
        authority: identity.authority.clone(),
        expires_at: 2_000_000_000,
        intent: ExecutionIntent::Launch {
            harness: ExecutionHarness::Codex,
            mode: ExecutionMode::Transcript,
            model: None,
            cwd: "/tmp".to_owned(),
            prompt: "secret prompt must never appear in command diagnostics".to_owned(),
        },
    }
}

#[test]
fn provider_ids_reject_route_like_values() {
    for value in ["", "a/b", "a b", "a\\b"] {
        assert!(ProviderId::new(value).is_err());
    }
    assert_eq!(
        ProviderId::new("local.coven").unwrap().as_str(),
        "local.coven"
    );
}

#[test]
fn binding_requires_nonzero_execution_and_runtime_generations() {
    let session = coven_client::execution::ExecutionSessionId::new("session").unwrap();
    let identity = identity();
    assert!(ProviderBinding::from_identity(&identity, session.clone(), 0, 1).is_err());
    assert!(ProviderBinding::from_identity(&identity, session.clone(), 1, 0).is_err());
    let binding = ProviderBinding::from_identity(&identity, session, 1, 1).unwrap();
    assert!(binding.with_runtime_generation(2).is_ok());
}

#[test]
fn command_debug_redacts_prompt_bearing_execution_request() {
    let identity = identity();
    let command = ProviderCommand::Execution {
        binding: None,
        request: launch_request(&identity),
    };
    let debug = format!("{command:?}");
    assert!(!debug.contains("secret prompt"));
    assert!(debug.contains("Execution"));
}

#[test]
fn provider_config_debug_redacts_owner_local_home() {
    let debug = format!("{:?}", config());
    assert!(!debug.contains("definitely/missing"));
    assert!(debug.contains("owner-local"));
}

#[test]
fn detached_runtime_rejects_commands_without_remote_control() {
    let handle = ProviderRuntime::start(config()).unwrap();
    let detached = handle.detach();
    assert_eq!(detached.connection, ProviderConnection::Disconnected);
    assert_eq!(
        handle.submit(ProviderCommand::Health),
        Err(SubmitError::Disconnected)
    );
    handle.shutdown();
}

#[test]
fn stale_binding_is_rejected_before_worker_submission() {
    let handle = ProviderRuntime::start(config()).unwrap();
    let binding = ProviderBinding::from_identity(
        &identity(),
        coven_client::execution::ExecutionSessionId::new("session").unwrap(),
        1,
        handle.runtime_generation(),
    )
    .unwrap();
    handle.bump_generation().unwrap();
    let result = handle.submit(ProviderCommand::ReadSource {
        binding,
        cursor: None,
        limit: 1,
    });
    assert_eq!(result, Err(SubmitError::IdentityMismatch));
    handle.shutdown();
}

#[test]
fn snapshots_are_shared_immutable_values() {
    let handle = ProviderRuntime::start(config()).unwrap();
    let first: Arc<ProviderSnapshot> = handle.snapshot();
    let second = handle.snapshot();
    assert_eq!(first.revision, second.revision);
    assert_eq!(first.identity, second.identity);
    handle.shutdown();
}

#[test]
fn invalid_commands_are_rejected_before_queue_admission() {
    let handle = ProviderRuntime::start(config()).unwrap();
    let binding = ProviderBinding::from_identity(
        &identity(),
        coven_client::execution::ExecutionSessionId::new("session").unwrap(),
        1,
        handle.runtime_generation(),
    )
    .unwrap();
    let before = handle.snapshot();
    let result = handle.submit(ProviderCommand::LookupExecution {
        binding: Some(binding),
        request_id: RequestId::new("request").unwrap(),
        expected_digest: "x".repeat(1024 * 1024),
        expected_target: ExecutionTarget::Existing {
            session_id: coven_client::execution::ExecutionSessionId::new("session").unwrap(),
            generation: 1,
        },
    });
    assert_eq!(result, Err(SubmitError::Rejected));
    assert_eq!(handle.snapshot().revision, before.revision);
    assert!(handle.try_drain_updates(1).is_empty());
    handle.shutdown();
}

#[test]
fn oversized_execution_is_rejected_before_queue_admission() {
    let handle = ProviderRuntime::start(config()).unwrap();
    let mut request = launch_request(&identity());
    if let coven_client::execution::ExecutionIntent::Launch { prompt, .. } = &mut request.intent {
        *prompt = "x".repeat(coven_client::execution::MAX_PROMPT_BYTES + 1);
    }
    let before = handle.snapshot();
    assert_eq!(
        handle.submit(ProviderCommand::Execution {
            binding: None,
            request,
        }),
        Err(SubmitError::Rejected)
    );
    assert_eq!(handle.snapshot().revision, before.revision);
    assert!(handle.try_drain_updates(1).is_empty());
    handle.shutdown();
}

#[test]
fn oversized_health_projection_is_rejected_before_snapshot_retention() {
    let identity = identity();
    let health = coven_client::Health {
        ok: true,
        api_version: "coven.daemon.v1".to_owned(),
        coven_version: "fixture".to_owned(),
        capabilities: coven_client::HealthCapabilities::default(),
        execution_authority: Some(identity.authority.clone()),
    };
    assert!(ProviderHealth::try_from(health.clone()).is_ok());
    let mut oversized = health;
    oversized.coven_version = "x".repeat(MAX_HEALTH_LABEL_BYTES + 1);
    assert!(ProviderHealth::try_from(oversized).is_err());

    let mut too_many = coven_client::Health {
        ok: true,
        api_version: "coven.daemon.v1".to_owned(),
        coven_version: "fixture".to_owned(),
        capabilities: coven_client::HealthCapabilities::default(),
        execution_authority: Some(identity.authority),
    };
    too_many.capabilities.execution_request_operations = (0..=MAX_HEALTH_CAPABILITIES)
        .map(|i| format!("op{i}"))
        .collect();
    assert!(ProviderHealth::try_from(too_many).is_err());
}
