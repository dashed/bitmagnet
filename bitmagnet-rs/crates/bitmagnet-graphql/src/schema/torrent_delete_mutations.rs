//! Disabled-by-default torrent deletion through the Go-compatible blocking store.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use async_graphql::{Error, Result};
use async_trait::async_trait;
use bitmagnet_blocking::{BlockingError, BlockingManager};
use bitmagnet_db::PgPool;
use bitmagnet_model::InfoHash;
use thiserror::Error;

use super::scalars::Hash20;

/// Maximum raw hashes accepted by one `torrent.delete` mutation.
pub const MAX_TORRENT_DELETE_INFO_HASHES: usize = 10_000;

/// Normalized request for `torrent.delete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentDeleteRequest {
    /// Deduplicated hashes in caller order, matching the Go manager's map buffer.
    pub info_hashes: Vec<InfoHash>,
}

/// Typed failures from the torrent-delete adapter.
#[derive(Debug, Error)]
pub enum TorrentDeleteMutationsError {
    /// The schema was built without the separately authenticated delete writer.
    #[error("torrent delete mutations are disabled")]
    Disabled,
    /// The atomic torrent-delete and bloom-filter checkpoint failed.
    #[error("torrent delete blocking-store write failed: {0}")]
    Blocking(#[from] BlockingError),
}

/// Runtime seam for `torrent.delete`.
#[async_trait]
pub trait TorrentDeleteMutationsRuntime: Send + Sync {
    async fn delete(
        &self,
        request: TorrentDeleteRequest,
    ) -> std::result::Result<(), TorrentDeleteMutationsError>;
}

struct DisabledTorrentDeleteMutationsRuntime;

#[async_trait]
impl TorrentDeleteMutationsRuntime for DisabledTorrentDeleteMutationsRuntime {
    async fn delete(
        &self,
        _request: TorrentDeleteRequest,
    ) -> std::result::Result<(), TorrentDeleteMutationsError> {
        Err(TorrentDeleteMutationsError::Disabled)
    }
}

/// PostgreSQL implementation backed by a caller-owned, separately authorized pool.
pub struct PgTorrentDeleteMutationsRuntime {
    manager: BlockingManager,
}

impl PgTorrentDeleteMutationsRuntime {
    /// Constructs one serialized production blocking manager.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            manager: BlockingManager::new(pool),
        }
    }
}

#[async_trait]
impl TorrentDeleteMutationsRuntime for PgTorrentDeleteMutationsRuntime {
    async fn delete(
        &self,
        request: TorrentDeleteRequest,
    ) -> std::result::Result<(), TorrentDeleteMutationsError> {
        // Go's resolver always forces a checkpoint, including for an empty list.
        self.manager.block(&request.info_hashes, true).await?;
        Ok(())
    }
}

/// GraphQL context wrapper for the torrent-delete runtime.
#[derive(Clone)]
pub struct TorrentDeleteMutationsRuntimeData(Arc<dyn TorrentDeleteMutationsRuntime>);

impl TorrentDeleteMutationsRuntimeData {
    /// Wraps an enabled runtime.
    #[must_use]
    pub fn new(runtime: Arc<dyn TorrentDeleteMutationsRuntime>) -> Self {
        Self(runtime)
    }

    /// Constructs the default fail-loud runtime.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledTorrentDeleteMutationsRuntime))
    }

    /// Constructs the production PostgreSQL writer runtime.
    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgTorrentDeleteMutationsRuntime::new(pool)))
    }
}

pub(super) async fn resolve(
    runtime: &TorrentDeleteMutationsRuntimeData,
    info_hashes: Vec<Hash20>,
) -> Result<()> {
    let request = normalize(info_hashes)?;
    runtime
        .0
        .delete(request)
        .await
        .map_err(|error| Error::new(error.to_string()))
}

fn normalize(raw: Vec<Hash20>) -> Result<TorrentDeleteRequest> {
    if raw.len() > MAX_TORRENT_DELETE_INFO_HASHES {
        return Err(Error::new(format!(
            "torrent.delete infoHashes has more than {MAX_TORRENT_DELETE_INFO_HASHES} entries"
        )));
    }

    let mut seen = HashSet::with_capacity(raw.len());
    let mut info_hashes = Vec::with_capacity(raw.len());
    for Hash20(value) in raw {
        let hash = InfoHash::from_str(&value)
            .map_err(|error| Error::new(format!("invalid Hash20: {error}")))?;
        if seen.insert(hash) {
            info_hashes.push(hash);
        }
    }
    Ok(TorrentDeleteRequest { info_hashes })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    struct FakeRuntime {
        calls: Arc<Mutex<Vec<TorrentDeleteRequest>>>,
    }

    #[async_trait]
    impl TorrentDeleteMutationsRuntime for FakeRuntime {
        async fn delete(
            &self,
            request: TorrentDeleteRequest,
        ) -> std::result::Result<(), TorrentDeleteMutationsError> {
            self.calls.lock().expect("calls lock").push(request);
            Ok(())
        }
    }

    fn schema_with_fake(calls: Arc<Mutex<Vec<TorrentDeleteRequest>>>) -> crate::schema::Schema {
        let runtime: Arc<dyn TorrentDeleteMutationsRuntime> = Arc::new(FakeRuntime { calls });
        async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentDeleteMutationsRuntimeData::new(runtime))
            .finish()
    }

    fn hash(raw: &str) -> InfoHash {
        raw.parse().expect("test hash")
    }

    #[tokio::test]
    async fn graphql_delete_deduplicates_calls_and_returns_void() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let schema = schema_with_fake(Arc::clone(&calls));
        let first = "0123456789abcdef0123456789abcdef01234567";
        let second = "1111111111111111111111111111111111111111";
        let response = schema
            .execute(format!(
                "mutation {{ torrent {{ delete(infoHashes: [\"{first}\", \"{first}\", \"{second}\"]) }} }}"
            ))
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(response.data, value!({ "torrent": { "delete": null } }));
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![TorrentDeleteRequest {
                info_hashes: vec![hash(first), hash(second)],
            }]
        );
    }

    #[tokio::test]
    async fn empty_delete_reaches_runtime_like_go() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let response = schema_with_fake(Arc::clone(&calls))
            .execute("mutation { torrent { delete(infoHashes: []) } }")
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![TorrentDeleteRequest {
                info_hashes: Vec::new(),
            }]
        );
    }

    #[tokio::test]
    async fn invalid_and_oversized_inputs_fail_before_the_runtime() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let schema = schema_with_fake(Arc::clone(&calls));
        let invalid = schema
            .execute("mutation { torrent { delete(infoHashes: [\"not-a-hash\"]) } }")
            .await;
        assert_eq!(invalid.errors.len(), 1);
        assert!(invalid.errors[0].message.contains("invalid Hash20"));

        let oversized = vec![
            Hash20("0123456789abcdef0123456789abcdef01234567".to_owned());
            MAX_TORRENT_DELETE_INFO_HASHES + 1
        ];
        let error = normalize(oversized).expect_err("oversized delete must fail");
        assert!(error.message.contains("has more than"));
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[tokio::test]
    async fn disabled_runtime_fails_loudly() {
        let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentDeleteMutationsRuntimeData::disabled())
            .finish();
        let response = schema
            .execute("mutation { torrent { delete(infoHashes: []) } }")
            .await;
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0]
            .message
            .contains("torrent delete mutations are disabled"));
    }
}
