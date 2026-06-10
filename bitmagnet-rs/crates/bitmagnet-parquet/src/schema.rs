//! Arrow schemas for the L2 artifacts.
//!
//! Three Parquet files make up a generation:
//! * **fact** — one row per file: `(info_hash, file_index, path, extension, size)`,
//!   sorted by `(extension, size)` so row-group min/max zone-maps prune
//!   range/count queries (ARCH-C: exact-count 1024→17 ms). This is the only
//!   v1 schema — denorm columns (`content_type`/`published_at`) are deferred
//!   because they go stale against the `updated_at` watermark (L2 spec rev).
//! * **agg_ext** — per-extension global rollup: `(extension, file_count,
//!   total_size, max_size)`. The `<3 ms` facet/group-by lever.
//! * **agg_torrent_ext** — per-`(info_hash, extension)` rollup: the DuckDB-side
//!   mirror of the PG DROP-gate table; serves distinct-torrent collapse + the
//!   file-type facet without scanning the fact.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

/// Column name constants shared by the writers, rollups and the sidecar's SQL
/// (so a rename is a single edit).
pub mod col {
    pub const INFO_HASH: &str = "info_hash";
    pub const FILE_INDEX: &str = "file_index";
    pub const PATH: &str = "path";
    pub const EXTENSION: &str = "extension";
    pub const SIZE: &str = "size";
    pub const FILE_COUNT: &str = "file_count";
    pub const TOTAL_SIZE: &str = "total_size";
    pub const MAX_SIZE: &str = "max_size";
}

/// The fact schema. `info_hash` is stored as the 40-char hex string (DuckDB
/// joins/point-lookups on it; the bench measured this as fine). `extension` is
/// nullable (G1 `None` = SQL NULL).
pub fn fact_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(col::INFO_HASH, DataType::Utf8, false),
        Field::new(col::FILE_INDEX, DataType::UInt32, false),
        Field::new(col::PATH, DataType::Utf8, false),
        Field::new(col::EXTENSION, DataType::Utf8, true),
        Field::new(col::SIZE, DataType::UInt64, false),
    ]))
}

/// Per-extension global rollup schema.
pub fn agg_ext_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(col::EXTENSION, DataType::Utf8, true),
        Field::new(col::FILE_COUNT, DataType::UInt64, false),
        Field::new(col::TOTAL_SIZE, DataType::UInt64, false),
        Field::new(col::MAX_SIZE, DataType::UInt64, false),
    ]))
}

/// Per-`(info_hash, extension)` rollup schema (mirror of the PG DROP-gate table).
pub fn agg_torrent_ext_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(col::INFO_HASH, DataType::Utf8, false),
        Field::new(col::EXTENSION, DataType::Utf8, true),
        Field::new(col::FILE_COUNT, DataType::UInt64, false),
        Field::new(col::TOTAL_SIZE, DataType::UInt64, false),
        Field::new(col::MAX_SIZE, DataType::UInt64, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fact_columns_in_order() {
        let s = fact_schema();
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["info_hash", "file_index", "path", "extension", "size"]
        );
        // extension is the only nullable column (G1 None => SQL NULL).
        assert!(s.field_with_name("extension").unwrap().is_nullable());
        assert!(!s.field_with_name("size").unwrap().is_nullable());
    }

    #[test]
    fn agg_torrent_ext_keyed_by_hash_and_ext() {
        let s = agg_torrent_ext_schema();
        assert!(s.field_with_name("info_hash").is_ok());
        assert!(s.field_with_name("max_size").is_ok());
    }
}
