//! Connection-pool construction and health check.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::config::DbConfig;
use crate::error::Result;

/// Opens a pooled connection to PostgreSQL using `cfg`.
///
/// Establishes (and validates) one connection eagerly, so a bad DSN or an
/// unreachable server surfaces here rather than on first query.
pub async fn connect(cfg: &DbConfig) -> Result<PgPool> {
    let options = cfg.connect_options()?;
    tracing::debug!(db = %cfg.log_target(), "connecting to postgres");
    let pool = PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .connect_with(options)
        .await?;
    Ok(pool)
}

/// Runs a trivial `SELECT 1` to confirm the pool can reach the database.
pub async fn ping(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}
