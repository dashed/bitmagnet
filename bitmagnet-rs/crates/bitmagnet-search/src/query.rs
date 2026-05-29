//! Translation from bitmagnet's Postgres `tsquery` search syntax into Tantivy
//! queries, and the `Search` RPC entry point the server delegates to.

use tantivy::query::Query;
use tantivy::schema::Schema;
use tantivy::{Index, IndexReader};

use crate::proto::{SearchRequest, SearchResponse};
use crate::schema::Fields;

/// Run a full search: translate `request.query` (via [`tsquery_to_tantivy`]),
/// apply `request.filters`, paginate, sort, and collect ranked hits into a
/// [`SearchResponse`]. This is the entry point [`crate::server::SearchServer`]
/// delegates the `Search` RPC to.
///
/// Boost terms across the weight tiers using
/// [`Fields::weighted_text_fields`](crate::schema::Fields::weighted_text_fields).
/// Reconstruct each hit's [`crate::proto::TorrentDocument`] from the STORED /
/// FAST fields named in [`crate::schema`]; `file_paths` is intentionally not
/// retrievable.
///
/// # Errors
/// Returns an error if the query cannot be parsed or the search fails.
///
/// # Panics
/// Currently always panics — the read path (Task #3) fills this in.
pub fn run_search(
    _index: &Index,
    _reader: &IndexReader,
    _fields: &Fields,
    _request: SearchRequest,
) -> anyhow::Result<SearchResponse> {
    unimplemented!("read path (Task #3): run_search")
}

/// Translate a `tsquery`-style search string into a boxed Tantivy [`Query`].
///
/// Phase 3 ports the Go `tsquery` handling (prefix `:*` matches, the `&`/`|`/`!`
/// operators and phrase grouping) onto Tantivy's query AST, targeting the text
/// fields declared in `schema`.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if `raw` cannot be parsed into a valid
/// query for `schema`.
///
/// # Panics
/// Always panics — not implemented until Phase 3.
pub fn tsquery_to_tantivy(_schema: &Schema, _raw: &str) -> tantivy::Result<Box<dyn Query>> {
    unimplemented!("Phase 3: translate tsquery syntax into a Tantivy query")
}
