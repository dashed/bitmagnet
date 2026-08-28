//! Bounded, read-only implementation of `torrent.listSources`.

use std::sync::Arc;

use async_trait::async_trait;
use bitmagnet_db::PgPool;
use sqlx::FromRow;
use thiserror::Error;

use super::objects::{TorrentListSourcesResult, TorrentSource};

/// Maximum configured torrent sources returned by the read API.
pub const MAX_TORRENT_SOURCES: usize = 1_024;

/// One source row projected from PostgreSQL.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct TorrentSourceRecord {
    /// Stable source key.
    pub key: String,
    /// Human-readable source name.
    pub name: String,
}

/// Typed failures from the source-list adapter.
#[derive(Debug, Error)]
pub enum TorrentSourcesError {
    /// No database runtime was attached to this schema.
    #[error("torrent.listSources is unavailable without a PostgreSQL runtime")]
    Disabled,
    /// The read-only query failed.
    #[error("torrent.listSources PostgreSQL read failed: {0}")]
    Database(#[from] sqlx::Error),
    /// The configured table exceeded the defensive response bound.
    #[error("torrent.listSources has more than {limit} rows")]
    LimitExceeded { limit: usize },
}

/// Runtime seam used by the source-list resolver.
#[async_trait]
pub trait TorrentSourcesRuntime: Send + Sync {
    /// Lists source keys and names in ascending key order.
    async fn list_sources(
        &self,
        limit: usize,
    ) -> Result<Vec<TorrentSourceRecord>, TorrentSourcesError>;
}

struct DisabledTorrentSourcesRuntime;

#[async_trait]
impl TorrentSourcesRuntime for DisabledTorrentSourcesRuntime {
    async fn list_sources(
        &self,
        _limit: usize,
    ) -> Result<Vec<TorrentSourceRecord>, TorrentSourcesError> {
        Err(TorrentSourcesError::Disabled)
    }
}

/// PostgreSQL implementation of the source-list seam.
pub struct PgTorrentSourcesRuntime {
    pool: PgPool,
}

impl PgTorrentSourcesRuntime {
    /// Constructs a lazy adapter over a caller-owned pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TorrentSourcesRuntime for PgTorrentSourcesRuntime {
    async fn list_sources(
        &self,
        limit: usize,
    ) -> Result<Vec<TorrentSourceRecord>, TorrentSourcesError> {
        let fetch_limit = limit.saturating_add(1);
        let rows = sqlx::query_as::<_, TorrentSourceRecord>(
            "SELECT key, name FROM torrent_sources ORDER BY key ASC LIMIT $1",
        )
        .bind(i64::try_from(fetch_limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > limit {
            return Err(TorrentSourcesError::LimitExceeded { limit });
        }
        Ok(rows)
    }
}

/// GraphQL context wrapper for a torrent-sources runtime.
#[derive(Clone)]
pub struct TorrentSourcesRuntimeData(Arc<dyn TorrentSourcesRuntime>);

impl TorrentSourcesRuntimeData {
    /// Wraps an enabled runtime.
    #[must_use]
    pub fn new(runtime: Arc<dyn TorrentSourcesRuntime>) -> Self {
        Self(runtime)
    }

    /// Constructs the fail-loud context used by non-runtime schema builders.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledTorrentSourcesRuntime))
    }

    /// Constructs the production PostgreSQL runtime.
    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgTorrentSourcesRuntime::new(pool)))
    }
}

pub(super) async fn resolve(
    runtime: &TorrentSourcesRuntimeData,
) -> async_graphql::Result<TorrentListSourcesResult> {
    let mut sources = runtime
        .0
        .list_sources(MAX_TORRENT_SOURCES)
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
    sources.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(TorrentListSourcesResult {
        sources: sources
            .into_iter()
            .map(|source| TorrentSource {
                key: source.key,
                name: source.name,
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    struct FakeRuntime;

    #[async_trait]
    impl TorrentSourcesRuntime for FakeRuntime {
        async fn list_sources(
            &self,
            limit: usize,
        ) -> Result<Vec<TorrentSourceRecord>, TorrentSourcesError> {
            assert_eq!(limit, MAX_TORRENT_SOURCES);
            Ok(vec![
                TorrentSourceRecord {
                    key: "rarbg".to_owned(),
                    name: "RARBG".to_owned(),
                },
                TorrentSourceRecord {
                    key: "dht".to_owned(),
                    name: "DHT".to_owned(),
                },
            ])
        }
    }

    #[tokio::test]
    async fn schema_lists_sources_in_go_key_order() {
        let runtime: Arc<dyn TorrentSourcesRuntime> = Arc::new(FakeRuntime);
        let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentSourcesRuntimeData::new(runtime))
            .finish();
        let response = schema
            .execute("{ torrent { listSources { sources { key name } } } }")
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            response.data,
            value!({
                "torrent": { "listSources": { "sources": [
                    { "key": "dht", "name": "DHT" },
                    { "key": "rarbg", "name": "RARBG" },
                ] } }
            })
        );
    }
}
