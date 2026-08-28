//! Read-only PostgreSQL admission for the GraphQL server.

use bitmagnet_db::{
    assert_goose_applied_head, read_goose_applied_head, DbError, GooseAppliedHead,
    GooseHeadMismatch, PgPool,
};
use thiserror::Error;

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

/// Admits GraphQL only at the exact Goose applied head.
///
/// This never creates the Goose table and never applies or rolls back a
/// migration. Rollback and reapply history follows Goose's newest-row
/// semantics through [`read_goose_applied_head`].
pub async fn admit_pg(pool: &PgPool, required: i64) -> Result<GooseAppliedHead, PgAdmissionError> {
    if required <= 0 {
        return Err(PgAdmissionError::InvalidRequiredVersion { required });
    }

    let actual = read_goose_applied_head(pool)
        .await
        .map_err(PgAdmissionError::Read)?;
    assert_goose_applied_head(actual, required).map_err(PgAdmissionError::Head)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_required_version_fails_before_database_access() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/bitmagnet")
            .expect("lazy PostgreSQL pool");
        assert!(matches!(
            admit_pg(&pool, 0).await,
            Err(PgAdmissionError::InvalidRequiredVersion { required: 0 })
        ));
    }
}
