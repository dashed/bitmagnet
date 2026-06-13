//! Tantivy schema for the L3 per-torrent path-bag index.

use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    FAST,
};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer};
use tantivy::Index;

/// Runtime tokenizer name bound to the path-bag field.
pub const PATH_TOKENIZER: &str = "path_ngram";

mod name {
    pub(super) const PATH: &str = "path";
    pub(super) const INFO_HASH: &str = "info_hash";
    pub(super) const SIZE: &str = "size";
    pub(super) const FILES_COUNT: &str = "files_count";
    pub(super) const SEEDERS: &str = "seeders";
    pub(super) const PUBLISHED_AT: &str = "published_at";
}

/// Field names in declaration order.
pub const FIELD_NAMES: [&str; 6] = [
    name::PATH,
    name::INFO_HASH,
    name::SIZE,
    name::FILES_COUNT,
    name::SEEDERS,
    name::PUBLISHED_AT,
];

/// Build the production L3 path-bag schema.
///
/// The `path` field is `WithFreqs` only: ngram substring queries are boolean
/// conjunctions over grams and never use positions. The `info_hash` is indexed
/// for `delete_term(info_hash)` supersession and stored for candidate output.
#[must_use]
pub fn build_schema() -> Schema {
    let mut builder = Schema::builder();

    let path_indexing = TextFieldIndexing::default()
        .set_tokenizer(PATH_TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqs);
    let path_options = TextOptions::default().set_indexing_options(path_indexing);
    builder.add_text_field(name::PATH, path_options);

    builder.add_bytes_field(
        name::INFO_HASH,
        BytesOptions::default().set_indexed().set_stored(),
    );

    let fast_u64: NumericOptions = FAST.into();
    builder.add_u64_field(name::SIZE, fast_u64.clone());
    builder.add_u64_field(name::FILES_COUNT, fast_u64.clone());
    builder.add_u64_field(name::SEEDERS, fast_u64);
    builder.add_i64_field(name::PUBLISHED_AT, NumericOptions::default().set_fast());

    builder.build()
}

/// Register the path ngram tokenizer on a freshly opened index.
///
/// Tokenizers are runtime state and are not persisted in Tantivy's meta files.
/// Every open/create path must call this before reading or writing.
pub fn register_tokenizer(index: &Index) -> tantivy::Result<()> {
    let analyzer = TextAnalyzer::builder(NgramTokenizer::new(2, 3, false)?)
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(PATH_TOKENIZER, analyzer);
    Ok(())
}

/// Resolved field handles for the pathsearch schema.
#[derive(Debug, Clone, Copy)]
pub struct Fields {
    pub path: Field,
    pub info_hash: Field,
    pub size: Field,
    pub files_count: Field,
    pub seeders: Field,
    pub published_at: Field,
}

impl Fields {
    /// Resolve all field handles against `schema`.
    ///
    /// # Errors
    /// Returns the Tantivy schema error if any required field is missing.
    pub fn from_schema(schema: &Schema) -> tantivy::Result<Self> {
        Ok(Self {
            path: schema.get_field(name::PATH)?,
            info_hash: schema.get_field(name::INFO_HASH)?,
            size: schema.get_field(name::SIZE)?,
            files_count: schema.get_field(name::FILES_COUNT)?,
            seeders: schema.get_field(name::SEEDERS)?,
            published_at: schema.get_field(name::PUBLISHED_AT)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{build_schema, register_tokenizer, Fields, FIELD_NAMES, PATH_TOKENIZER};
    use tantivy::Index;

    #[test]
    fn schema_contains_expected_fields() {
        let schema = build_schema();
        for name in FIELD_NAMES {
            assert!(schema.get_field(name).is_ok(), "missing field {name}");
        }
        Fields::from_schema(&schema).expect("fields resolve");
    }

    #[test]
    fn tokenizer_registers() {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index).expect("tokenizer registers");
        assert!(index.tokenizers().get(PATH_TOKENIZER).is_some());
    }
}
