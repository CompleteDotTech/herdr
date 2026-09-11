use serde::Deserialize;
#[cfg(unix)]
use serde::Serialize;

pub const PROTOCOL_VERSION: &str = "coven.daemon.v1";

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Health {
    pub ok: bool,
    pub api_version: String,
    pub coven_version: String,
    pub capabilities: HealthCapabilities,
    #[serde(default)]
    pub execution_authority: Option<crate::execution::ExecutionAuthority>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCapabilities {
    #[serde(default)]
    pub sessions: bool,
    #[serde(default)]
    pub events: bool,
    #[serde(default)]
    pub event_cursor: Option<String>,
    #[serde(default)]
    pub structured_errors: bool,
    #[serde(default)]
    pub execution_request_contracts: Vec<String>,
    #[serde(default)]
    pub execution_request_operations: Vec<String>,
    #[serde(default)]
    pub execution_source_contracts: Vec<String>,
    /// Owner-local managed execution catalog contracts.
    #[serde(default)]
    pub execution_catalog_contracts: Vec<String>,
    #[serde(default)]
    pub execution_transcript_contracts: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum ReadEndpoint {
    ExecutionRequest {
        project_id: crate::execution::ProjectId,
        profile_id: crate::execution::ProfileId,
        request_id: crate::execution::RequestId,
    },
    Session {
        session_id: String,
    },
    /// Session listing.
    ///
    /// The inherited `v1` route serves two response shapes and the query
    /// selects between them: the daemon inspects only `limit`, `cursor`, and
    /// `includeArchived`, and any one of the three switches it to the
    /// `{ sessions, next_cursor }` envelope, while a query carrying none of
    /// them returns the unpaginated `SessionRecord[]`. Callers that decode the
    /// envelope must therefore set at least one of the three, exactly as they
    /// already had to for `limit` alone. Note the envelope key is snake_case,
    /// unlike the event envelope's camelCase `nextCursor`.
    Sessions {
        limit: Option<u16>,
        /// Opaque page cursor echoed back from a previous page's
        /// `next_cursor`. It is never composed locally: a caller can only send
        /// a cursor the daemon just issued, so an older daemon that never
        /// issues one is never asked to honor one.
        cursor: Option<String>,
        /// Include archived sessions. `false` keeps the daemon's default
        /// `archived_at IS NULL` filter and is not sent on the wire.
        include_archived: bool,
    },
    Events {
        session_id: String,
        after_seq: Option<i64>,
        limit: Option<i64>,
    },
}

#[derive(Clone, Debug)]
pub enum WriteEndpoint {
    Sessions,
    SessionInput { session_id: String },
    SessionKill { session_id: String },
}

#[doc(hidden)]
#[cfg(unix)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleDaemonStatus {
    pub pid: u32,
    pub started_at: String,
    pub socket: String,
}

#[doc(hidden)]
#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnixDaemonShutdown {
    Unavailable,
    IdentityMismatch,
    Exited,
    TimedOut,
}

#[cfg(test)]
mod authority_tests {
    use super::*;

    #[test]
    fn older_health_omits_authority_and_current_health_preserves_it() {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/health.json")).unwrap();
        let older: Health = serde_json::from_value(value.clone()).unwrap();
        assert!(older.execution_authority.is_none());
        value["executionAuthority"] = serde_json::json!({"id":"authority-one","generation":7});
        let current: Health = serde_json::from_value(value).unwrap();
        let binding = current.execution_authority.unwrap();
        assert_eq!(binding.id.as_str(), "authority-one");
        assert_eq!(binding.generation, 7);
    }

    #[test]
    fn malformed_authority_health_is_rejected_instead_of_ignored() {
        for authority in [
            serde_json::json!({"id":"authority-one","generation":0}),
            serde_json::json!({"id":"authority-one","generation":9223372036854775808u64}),
            serde_json::json!({"id":"bad/id","generation":1}),
            serde_json::json!({"id":"authority-one","generation":1,"approved":true}),
        ] {
            let mut value: serde_json::Value =
                serde_json::from_str(include_str!("../fixtures/health.json")).unwrap();
            value["executionAuthority"] = authority;
            assert!(serde_json::from_value::<Health>(value).is_err());
        }
    }
}
