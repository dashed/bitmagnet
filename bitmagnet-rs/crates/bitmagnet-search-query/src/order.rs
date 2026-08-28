//! Result ordering for the Torznab and Phase-2 GraphQL search surfaces.
//!
//! Torznab only ever picks two orderings (see `internal/torznab/adapter/
//! search_options.go`): when a free-text query is present it orders by
//! `relevance` (unless the profile sets `DisableOrderByRelevance`, then
//! `published_at`); with no query it inherits the default browse order
//! (`published_at DESC`, single column) from
//! `search.TorrentContentDefaultOption`. Direction is always descending on the
//! served path. GraphQL may select any of the nine fields ported from
//! `internal/database/search/order_torrent_content_enum.go` and
//! `order_torrent_content.go`; their SQL lowering is Lane S S4.
//!
//! The exact ORDER BY clause each variant expands to — including the tie-break
//! columns and the `ts_rank_cd` select alias — is fixed by Q2 against
//! `internal/database/search/order_torrent_content.go` and the alias machinery
//! in `internal/database/query/query.go` (`applySelect` / `applyPost`). See
//! `CONTRACT.md` §Ordering for the enumerated clauses.

use serde::{Deserialize, Serialize};

/// Sort direction (Go `search.OrderDirection`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OrderDirection {
    Ascending,
    #[default]
    Descending,
}

impl OrderDirection {
    /// True for descending (Go `direction == OrderDirectionDescending`).
    pub const fn is_desc(self) -> bool {
        matches!(self, Self::Descending)
    }
}

/// The full `search.TorrentContentOrderBy` field set.
///
/// Ported from `internal/database/search/order_torrent_content_enum.go` and
/// `order_torrent_content.go` `TorrentContentOrderBy.Clauses`. For every field
/// with an `info_hash` tie-break, the tie-break inherits the primary column's
/// direction. Lane S S4 implements these documented clauses; the Phase-1
/// [`crate::build_query`] accepts only relevance and published-at today.
///
/// * [`TorrentContentOrderField::Relevance`] → a single `query_string_rank` column
///   (`ts_rank_cd(torrent_contents.tsv, $tsquery)`), **no tie-break** — so
///   parity fixtures must give matched rows distinct ranks.
/// * [`TorrentContentOrderField::PublishedAt`] → `torrent_contents.published_at`
///   **then `torrent_contents.info_hash`** as a deterministic tie-break.
///
/// The default browse order (no explicit ordering, i.e. [`None`] in
/// [`crate::TorznabSearchParams::order`]) is `published_at DESC` as a **single**
/// column with NO info_hash tie-break — this differs from `PublishedAt` above
/// and is a genuine Go quirk that Q2 reproduces (see `CONTRACT.md` §Ordering).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentContentOrderField {
    /// `query_string_rank` (`ts_rank_cd(...)`), with no tie-break.
    Relevance,
    /// `torrent_contents.published_at`, then `torrent_contents.info_hash` in
    /// the same direction.
    PublishedAt,
    /// `torrent_contents.updated_at`, then `torrent_contents.info_hash` in the
    /// same direction.
    UpdatedAt,
    /// `torrent_contents.size`, then `torrent_contents.info_hash` in the same
    /// direction.
    Size,
    /// `COALESCE(torrent_contents.files_count, 0)`, then
    /// `torrent_contents.info_hash` in the same direction.
    FilesCount,
    /// `coalesce(torrent_contents.seeders, -1)`, then
    /// `torrent_contents.info_hash` in the same direction.
    Seeders,
    /// `coalesce(torrent_contents.leechers, -1)`, then
    /// `torrent_contents.info_hash` in the same direction.
    Leechers,
    /// `torrents.name`, with no tie-break; requires the `torrents` join.
    Name,
    /// `torrent_contents.info_hash`, which is its own deterministic key.
    InfoHash,
}

/// A resolved ordering choice: a field plus a direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentContentOrder {
    pub field: TorrentContentOrderField,
    #[serde(default)]
    pub direction: OrderDirection,
}

impl TorrentContentOrder {
    /// `relevance` descending — the Torznab default when a query is present and
    /// the profile has relevance ordering enabled.
    pub const fn relevance_desc() -> Self {
        Self {
            field: TorrentContentOrderField::Relevance,
            direction: OrderDirection::Descending,
        }
    }

    /// `published_at` descending — the Torznab order when a query is present but
    /// the profile sets `DisableOrderByRelevance`.
    pub const fn published_at_desc() -> Self {
        Self {
            field: TorrentContentOrderField::PublishedAt,
            direction: OrderDirection::Descending,
        }
    }
}
