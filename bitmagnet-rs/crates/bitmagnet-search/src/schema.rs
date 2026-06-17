//! Tantivy schema for torrent documents.
//!
//! This is the source-of-truth mapping from bitmagnet's Postgres/`tsvector`
//! model to Tantivy fields, per the "Field Mapping" table in
//! `docs/rust-rewrite-plan.md`. Keeping it in one place lets the index writer,
//! the query translator and the facet aggregator agree on field names and flags.
//!
//! Flag conventions:
//! - `STRING` — indexed, untokenized (exact match), used for keys/enum values.
//! - `TEXT` — indexed and tokenized, used for human-readable search text.
//! - `STORED` — value is retrievable from the index.
//! - `FAST` — value lives in the columnar store (needed for sorting/faceting).

use tantivy::schema::{Schema, FAST, INDEXED, STORED, STRING, TEXT};

/// Build the Tantivy [`Schema`] for torrent documents.
///
/// Multi-valued fields (`file_types`, `languages`) are modelled as plain text
/// fields with more than one value added per document; Tantivy needs no special
/// flag for that.
#[must_use]
pub fn build_schema() -> Schema {
    let mut builder = Schema::builder();

    // Identity + relevance.
    builder.add_text_field("info_hash", STRING | STORED); // hex hash, exact match
    builder.add_text_field("name", TEXT | STORED); // primary display + relevance
    builder.add_text_field("search_text", TEXT); // combined haystack, not stored

    // Faceted / filtered string fields: indexed (exact), stored, and fast so the
    // columnar store can drive facet counts.
    let facet_string = (STRING | STORED).set_fast(None);
    builder.add_text_field("content_type", facet_string.clone());
    builder.add_text_field("file_types", facet_string.clone()); // multi-valued
    builder.add_text_field("languages", facet_string); // multi-valued

    // Numeric fields. `published_at` is i64 Unix seconds to match the proto.
    builder.add_u64_field("size_bytes", STORED | FAST | INDEXED);
    builder.add_u64_field("file_count", STORED | FAST);
    builder.add_i64_field("published_at", STORED | FAST | INDEXED);
    builder.add_u64_field("seeders", STORED | FAST);
    builder.add_u64_field("leechers", STORED | FAST);
    builder.add_u64_field("release_year", STORED | FAST);

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::build_schema;

    #[test]
    fn schema_contains_all_mapped_fields() {
        let schema = build_schema();
        for name in [
            "info_hash",
            "name",
            "search_text",
            "content_type",
            "file_types",
            "languages",
            "size_bytes",
            "file_count",
            "published_at",
            "seeders",
            "leechers",
            "release_year",
        ] {
            assert!(
                schema.get_field(name).is_ok(),
                "schema is missing field `{name}`"
            );
        }
    }
}
