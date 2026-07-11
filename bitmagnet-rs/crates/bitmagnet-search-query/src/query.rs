//! SQL construction + execution: [`build_query`] turns a
//! [`TorznabSearchParams`] into a [`SearchQuery`] (a `$N`-parameterised SQL
//! string plus its ordered binds), and [`SearchQuery`] runs it against a
//! `PgPool`.
//!
//! House style (matching `bitmagnet-db`): the runtime `sqlx::query` API with
//! `$N` placeholders and explicit binds — NO compile-time `query!` macros — so
//! the crate builds and unit-tests green without a live database or
//! `DATABASE_URL`. Only the DB-gated integration test (Q3, `#[ignore]`) needs a
//! server.
//!
//! Q2 implements `build_query` and the fetch methods; Q1 fixes the signatures
//! and the bind model so Lane T can code against them today.

use crate::params::TorznabSearchParams;
use crate::result::SearchResultItem;
use bitmagnet_model::InfoHash;
use sqlx::PgPool;

/// Errors from building or running a search query.
#[derive(Debug, thiserror::Error)]
pub enum SearchQueryError {
    /// The params could not be lowered to SQL (e.g. an identifier criterion
    /// with no source). Mirrors the Go builder's option/criteria errors.
    #[error("invalid search params: {0}")]
    InvalidParams(String),
    /// A database / execution error.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

/// A `SearchQueryError` result alias.
pub type Result<T> = std::result::Result<T, SearchQueryError>;

/// A single positional bind value for a `$N` placeholder, in declaration order.
///
/// The variants cover exactly the column/parameter types the Torznab subset
/// needs. `Tsquery` is bound as text and cast `::tsquery` in the SQL (Go binds
/// the pre-tokenised tsquery string the same way — see
/// `internal/database/query/query.go` `applyPre`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    /// A `bytea` value (info hash / content id bytes).
    Bytea(Vec<u8>),
    /// A `text` value (content type, resolution, source, id, tag ...).
    Text(String),
    /// A signed integer (`bigint`) value.
    BigInt(i64),
    /// A pre-tokenised tsquery string, cast `::tsquery` at the placeholder.
    Tsquery(String),
}

/// A parameterised SQL statement ready to run: the `$N`-placeholder SQL text
/// plus its positional [`Bind`]s.
///
/// Exposing the SQL and binds (rather than only executing) lets Q2's unit tests
/// assert SQL *shape* without a database, and lets Lane G's shadow harness log
/// the exact query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQuery {
    sql: String,
    binds: Vec<Bind>,
}

impl SearchQuery {
    /// Construct from raw parts (used by [`build_query`] and by shape tests).
    pub fn new(sql: impl Into<String>, binds: Vec<Bind>) -> Self {
        Self {
            sql: sql.into(),
            binds,
        }
    }

    /// The `$N`-placeholder SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The positional binds, in `$1..$N` order.
    pub fn binds(&self) -> &[Bind] {
        &self.binds
    }

    /// Execute and return the ordered info-hash list — the Q3 parity output.
    pub async fn fetch_info_hashes(&self, _pool: &PgPool) -> Result<Vec<InfoHash>> {
        unimplemented!("Q2: bind {:?} and scan info_hash column", self.binds)
    }

    /// Execute and return fully-hydrated result rows for Lane T's XML.
    pub async fn fetch(&self, _pool: &PgPool) -> Result<Vec<SearchResultItem>> {
        unimplemented!("Q2: execute + hydrate torrents/content joins")
    }
}

/// Lower a [`TorznabSearchParams`] to a [`SearchQuery`].
///
/// This is the single entry point Lane T calls. It ports the resolved-option →
/// SQL path of `internal/database/search` + `internal/database/query`:
/// `SELECT` (with order-alias columns), the dynamically-required joins
/// (`torrents`/`content` only when a criterion or ordering needs them), the
/// `tsv @@ $::tsquery` predicate, the filter tree, `ORDER BY`, `LIMIT`,
/// `OFFSET`. See `CONTRACT.md` for the full mapping. Implemented in Q2.
pub fn build_query(_params: &TorznabSearchParams) -> Result<SearchQuery> {
    unimplemented!("Q2: port internal/database/{{search,query}} resolved-option -> SQL")
}
