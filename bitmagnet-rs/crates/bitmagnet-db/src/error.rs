//! Error type for the database layer.

/// Result alias defaulting to [`DbError`].
pub type Result<T, E = DbError> = std::result::Result<T, E>;

/// Errors raised by the database layer.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// An error from SQLx (connection, query, or row decoding).
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Invalid or missing configuration.
    #[error("configuration error: {0}")]
    Config(String),
    /// A row held data that could not be decoded into a domain type (e.g. an
    /// `info_hash` column that was not 20 bytes).
    #[error("decode error: {0}")]
    Decode(String),
}

impl From<DbError> for bitmagnet_common::Error {
    fn from(err: DbError) -> Self {
        bitmagnet_common::Error::Other(err.to_string())
    }
}
