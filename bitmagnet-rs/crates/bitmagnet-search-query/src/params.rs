//! [`TorznabSearchParams`] — the input contract Lane T builds and Lane Q
//! compiles to SQL.
//!
//! This is the *search-domain projection* of a Torznab request: Lane T
//! (`bitmagnet-torznab`) owns HTTP parsing and all Torznab category logic, and
//! reduces a request to these fields (a filter tree, an optional free-text
//! query, an ordering, and a page window). It maps 1:1 onto what the Go adapter
//! feeds `search.TorrentContent` after `searchRequestToQueryOptions` has run —
//! but with the Torznab-specific `t=`/`cat=` interpretation already applied, per
//! the crate boundary (query construction only; no HTTP, XML, or category IDs).

use crate::criteria::Criteria;
use crate::order::TorrentContentOrder;
use serde::{Deserialize, Serialize};

/// A fully-resolved search request in search-domain terms.
///
/// Construct via [`TorznabSearchParams::new`] and the `with_*` setters, or
/// build the struct literally. Deserializable so the Q3 parity fixtures can
/// carry it as the `input` field (see `CONTRACT.md` §Parity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorznabSearchParams {
    /// Raw free-text query in bitmagnet's app-query syntax (Go
    /// `torznab.SearchRequest.Query`, passed to `query.SearchString`). `None`
    /// or empty means no full-text predicate AND no relevance ordering is
    /// applicable. Q tokenizes this to a `tsquery` internally (see
    /// `CONTRACT.md` §Full-text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,

    /// The predicate tree. `None` means an unfiltered scan (Torznab `t=search`
    /// with no `cat=`/id params). Lane T assembles this from the Torznab
    /// function, categories, and id params.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Criteria>,

    /// Result ordering. `None` selects the default browse order
    /// (`published_at DESC`, single column — see [`TorrentContentOrder`] docs).
    /// Torznab sets `Some(relevance_desc)` / `Some(published_at_desc)` only when
    /// a query is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<TorrentContentOrder>,

    /// Row limit (Go `query.Limit`). Torznab always sets this — the resolved
    /// value after clamping to the profile's `MaxLimit` (default 100). A limit
    /// of 0 is a valid Go state (returns no items); Q2 mirrors it.
    pub limit: u32,

    /// Row offset (Go `query.Offset`). `None`/`0` means no offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
}

impl TorznabSearchParams {
    /// A params set with only a limit (the one always-present field); all
    /// predicates/ordering default off.
    pub fn new(limit: u32) -> Self {
        Self {
            query: None,
            filter: None,
            order: None,
            limit,
            offset: None,
        }
    }

    /// Set the free-text query.
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Set the filter predicate tree.
    pub fn with_filter(mut self, filter: Criteria) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the ordering.
    pub fn with_order(mut self, order: TorrentContentOrder) -> Self {
        self.order = Some(order);
        self
    }

    /// Set the row offset.
    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }
}
