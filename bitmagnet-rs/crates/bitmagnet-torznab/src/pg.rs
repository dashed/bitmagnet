//! PostgreSQL composition and read-only schema admission for the Torznab adapter.

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use bitmagnet_db::{
    assert_goose_applied_head, read_goose_applied_head, DbError, GooseAppliedHead,
    GooseHeadMismatch, PgPool,
};
use bitmagnet_search_query::{SearchResultItem, TorznabSearchParams};
use thiserror::Error;

use crate::config::Config;
use crate::service::{router, SearchClient, SearchError};

struct PgSearchClient {
    pool: PgPool,
}

#[async_trait]
impl SearchClient for PgSearchClient {
    async fn search(
        &self,
        params: TorznabSearchParams,
    ) -> Result<Vec<SearchResultItem>, SearchError> {
        let query = bitmagnet_search_query::build_query(&params)?;
        Ok(query.fetch(&self.pool).await?)
    }
}

/// Builds the production Torznab router over a caller-owned PostgreSQL pool.
///
/// Construction starts no task and performs no query. The caller remains
/// responsible for exact schema admission and for closing the pool after the
/// HTTP server stops.
pub fn pg_router(config: Config, pool: PgPool) -> Router {
    router(config, Arc::new(PgSearchClient { pool }))
}

/// Typed failure from read-only PostgreSQL admission.
#[derive(Debug, Error)]
pub enum PgAdmissionError {
    /// The required migration version is not meaningful.
    #[error("required Goose migration version must be positive, got {required}")]
    InvalidRequiredVersion { required: i64 },
    /// Reading Goose's effective applied head failed.
    #[error("could not read the applied Goose migration head: {0}")]
    Read(#[source] DbError),
    /// The effective applied head is absent or different.
    #[error("database failed the exact Goose migration-head assertion: {0}")]
    Head(#[source] GooseHeadMismatch),
}

/// Admits a read-only Torznab process only at the exact Goose applied head.
///
/// This never creates the Goose table and never applies or rolls back a
/// migration. Rollback and reapply history follows Goose's newest-row semantics
/// through [`read_goose_applied_head`].
pub async fn admit_pg(pool: &PgPool, required: i64) -> Result<GooseAppliedHead, PgAdmissionError> {
    if required <= 0 {
        return Err(PgAdmissionError::InvalidRequiredVersion { required });
    }

    let actual = read_goose_applied_head(pool)
        .await
        .map_err(PgAdmissionError::Read)?;
    assert_goose_applied_head(actual, required).map_err(PgAdmissionError::Head)
}
