//! Tantivy schema for the **per-torrent path-bag** typeahead index (PS-T3 §2.2).
//!
//! One document per torrent. The only searchable field is `path_grams`, into
//! which every file path of the torrent is added as a **separate field value**
//! (so no ngram ever spans a file boundary). Identity + ranking come from a
//! handful of cheap columns:
//!
//! | field        | type  | flags                | why                                  |
//! |--------------|-------|----------------------|--------------------------------------|
//! | `info_hash`  | bytes | INDEXED + STORED     | delete/upsert key + the hit identity |
//! | `path_grams` | text  | ngram(2,3), WithFreqs| the ONLY searchable field; NO positions, never STORED |
//! | `seeders`    | u64   | FAST                 | index-sort key + typeahead rank + returned |
//! | `size`       | u64   | FAST                 | returned (coarse filter delegated to DuckDB) |
//! | `files_count`| u64   | FAST                 | shortest-path proxy + returned       |
//!
//! Decisions, each grounded in PS-T3:
//! * `path_grams` is `WithFreqs`, **not** `WithFreqsAndPositions` — every ngram
//!   is at position 0, so positions are dead weight (~83.5 % of the index). Never
//!   STORED: storing paths is the 273 GB cost the blob migration removed.
//! * `info_hash` is INDEXED (the mandatory `delete_term` supersession key — adds
//!   no measurable read cost) and STORED (returned directly in the hit, no
//!   doc-store of paths needed).
//! * `seeders`/`size`/`files_count` are FAST-only (read per-hit from the columnar
//!   store for the top-k page); not STORED, keeping the index minimal.

use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    FAST,
};

use super::tokenizer::PATH_TOKENIZER_NAME;

mod name {
    pub(super) const INFO_HASH: &str = "info_hash";
    pub(super) const PATH_GRAMS: &str = "path_grams";
    pub(super) const SEEDERS: &str = "seeders";
    pub(super) const SIZE: &str = "size";
    pub(super) const FILES_COUNT: &str = "files_count";
}

/// Every field name in the path-bag schema, in declaration order.
pub const FIELD_NAMES: [&str; 5] = [
    name::INFO_HASH,
    name::PATH_GRAMS,
    name::SEEDERS,
    name::SIZE,
    name::FILES_COUNT,
];

/// Build the Tantivy [`Schema`] for the per-torrent path-bag typeahead index.
#[must_use]
pub fn build_path_schema() -> Schema {
    let mut builder = Schema::builder();

    // path_grams: the ngram tokenizer + freqs, NO positions (all grams are
    // position 0 → positions are dead weight), indexed-only (never stored).
    let grams = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(PATH_TOKENIZER_NAME)
            .set_index_option(IndexRecordOption::WithFreqs),
    );

    // 20-byte info hash: INDEXED (delete key) + STORED (returned hit identity).
    builder.add_bytes_field(
        name::INFO_HASH,
        BytesOptions::default().set_indexed().set_stored(),
    );
    builder.add_text_field(name::PATH_GRAMS, grams);

    // Ranking / display signals: FAST-only (read per top-k hit; not stored).
    let fast: NumericOptions = FAST.into();
    builder.add_u64_field(name::SEEDERS, fast.clone());
    builder.add_u64_field(name::SIZE, fast.clone());
    builder.add_u64_field(name::FILES_COUNT, fast);

    builder.build()
}

/// Resolved [`Field`] handles for the path-bag schema. All [`Copy`].
#[derive(Debug, Clone, Copy)]
pub struct PathFields {
    pub info_hash: Field,
    pub path_grams: Field,
    pub seeders: Field,
    pub size: Field,
    pub files_count: Field,
}

impl PathFields {
    /// Resolve every handle against `schema`.
    ///
    /// # Errors
    /// Returns the underlying [`tantivy::TantivyError`] if `schema` is missing a
    /// field — i.e. it was not produced by [`build_path_schema`].
    pub fn from_schema(schema: &Schema) -> tantivy::Result<Self> {
        Ok(Self {
            info_hash: schema.get_field(name::INFO_HASH)?,
            path_grams: schema.get_field(name::PATH_GRAMS)?,
            seeders: schema.get_field(name::SEEDERS)?,
            size: schema.get_field(name::SIZE)?,
            files_count: schema.get_field(name::FILES_COUNT)?,
        })
    }

    /// The FAST field name used as the index-sort key and the typeahead rank key.
    #[must_use]
    pub const fn seeders_fast_name() -> &'static str {
        name::SEEDERS
    }

    /// FAST field names read per-hit to populate a [`crate::proto::PathHit`].
    #[must_use]
    pub const fn fast_names() -> (&'static str, &'static str, &'static str) {
        (name::SEEDERS, name::SIZE, name::FILES_COUNT)
    }
}

#[cfg(test)]
mod tests {
    use super::{build_path_schema, PathFields, FIELD_NAMES};

    #[test]
    fn schema_contains_all_fields() {
        let schema = build_path_schema();
        for n in FIELD_NAMES {
            assert!(schema.get_field(n).is_ok(), "missing field `{n}`");
        }
    }

    #[test]
    fn fields_resolve() {
        let schema = build_path_schema();
        PathFields::from_schema(&schema).expect("all path field handles resolve");
    }

    #[test]
    fn path_grams_has_no_positions() {
        // WithFreqs (not WithFreqsAndPositions): the .pos segment is never written.
        let schema = build_path_schema();
        let entry = schema.get_field_entry(schema.get_field("path_grams").unwrap());
        let opt = entry
            .field_type()
            .index_record_option()
            .expect("path_grams is indexed");
        assert!(opt.has_freq());
        assert!(!opt.has_positions(), "positions must be dropped (dead weight)");
    }
}
