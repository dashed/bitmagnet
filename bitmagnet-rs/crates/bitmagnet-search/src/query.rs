//! Translation from bitmagnet's Postgres `tsquery` search syntax into Tantivy
//! queries.

use tantivy::query::Query;
use tantivy::schema::Schema;

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
