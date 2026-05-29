//! Row → proto [`TorrentDocument`] transform for the backfill (`bin/backfill.rs`).
//!
//! The pure, database-free half of the backfill: given one [`TorrentForIndex`]
//! row (a `torrent_contents` row joined to its torrent and, when classified, its
//! `content`) plus its decoded file blob, it builds the proto document the
//! sidecar indexes. It operates on the already-fetched DTO and owned
//! [`BlobFile`]s — no live database — so the whole mapping is unit-testable here
//! and the binary stays a thin I/O loop.
//!
//! ## One document per torrent_content
//!
//! The index keys each document by a composite `doc_id`
//! (`hex(info_hash):content_type:content_source:content_id`, derived from these
//! same fields in [`crate::indexer`]). So a torrent classified as several
//! contents becomes several distinct documents — exactly the rows bitmagnet's
//! Postgres `tsv @@ tsquery` search returns. An unclassified `torrent_content`
//! (every content field `None`) still yields one document, searchable by name,
//! info hash and file paths; its content fields are simply empty.
//!
//! ## Field sources & parity notes
//!
//! * `file_paths` (weight D, never stored) and `file_extensions` (facet) come
//!   straight from the decoded blob — every non-empty path, and the distinct
//!   (sorted) non-empty extensions the blob already records per file.
//! * `content_type` maps the canonical Postgres string back to the proto enum
//!   int via [`ContentType`].
//! * `video_resolution` (`V1080p`…) and `video_3d` (`V3D` / `V3DSBS` / `V3DOU`)
//!   are stored with a leading `V`; we send Go's `Label()` (that `V` stripped,
//!   e.g. `"1080p"` / `"3D"`) so the indexed text, facet and filter values all
//!   match Go's weight-C tsvector + GraphQL facets, which use the same label.
//! * `audio_languages` (proto field 22) has **no** Postgres source — bitmagnet
//!   only stores `languages` — so it is deliberately left empty here.

use std::collections::BTreeSet;

use bitmagnet_db::TorrentForIndex;
use bitmagnet_model::{BlobFile, ContentType};

use crate::proto::TorrentDocument;

/// Build a proto [`TorrentDocument`] from a [`TorrentForIndex`] row plus its
/// decoded file blob.
///
/// `files` is the already-deserialized blob (empty for torrents with no file
/// data); only the file *paths* and their recorded extensions are used — the
/// blob's own size/index fields are not indexed. `row.files_data` is therefore
/// ignored here; the binary decodes it once and passes the result in.
///
/// Empty / absent optional values become the proto defaults (empty string, `0`);
/// [`crate::indexer::document_to_tantivy`] then skips those, so this need not
/// pre-filter them.
#[must_use]
pub fn build_document(row: &TorrentForIndex, files: &[BlobFile]) -> TorrentDocument {
    // File paths feed weight-D relevance; extensions are a facet. Both come only
    // from the blob: every non-empty path, and the distinct (sorted) non-empty
    // extensions the blob already records per file.
    let file_paths: Vec<String> = files
        .iter()
        .filter(|f| !f.path.is_empty())
        .map(|f| f.path.clone())
        .collect();
    let file_extensions: Vec<String> = files
        .iter()
        .filter(|f| !f.extension.is_empty())
        .map(|f| f.extension.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    TorrentDocument {
        info_hash: row.info_hash.as_slice().to_vec(),
        torrent_name: row.torrent_name.clone(),
        content_title: opt_string(&row.content_title),
        original_title: opt_string(&row.original_title),
        release_year: row.release_year.map_or(0, to_u32),
        video_resolution: row
            .video_resolution
            .as_deref()
            .map(strip_v_prefix)
            .unwrap_or_default(),
        video_source: opt_string(&row.video_source),
        video_codec: opt_string(&row.video_codec),
        genres: row.genres.clone(),
        file_paths,
        content_type: content_type_to_proto(row.content_type.as_deref()),
        seeders: row.seeders.map_or(0, to_u32),
        leechers: row.leechers.map_or(0, to_u32),
        // Mirrors the denormalized tc.files_count column for PG parity (absent
        // → 0); the blob length is deliberately not substituted.
        files_count: row.files_count.map_or(0, to_u32),
        size: to_u64(row.size),
        published_at: row.published_at,
        languages: row.languages.clone(),
        file_extensions,
        video_3d: row
            .video_3d
            .as_deref()
            .map(strip_v_prefix)
            .unwrap_or_default(),
        video_modifier: opt_string(&row.video_modifier),
        release_group: opt_string(&row.release_group),
        // No Postgres source for audio languages — see the module docs.
        audio_languages: Vec::new(),
        content_source: opt_string(&row.content_source),
        content_id: opt_string(&row.content_id),
    }
}

/// `Option<String>` → owned `String`, mapping `None` to the empty string the
/// indexer treats as "absent".
fn opt_string(value: &Option<String>) -> String {
    value.clone().unwrap_or_default()
}

/// Saturating `i64` → `u32` (negatives and overflow clamp to `0`); the source
/// columns are non-negative counts/years, so the clamp is just defence.
fn to_u32(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(0)
}

/// Saturating `i64` → `u64` (negative `size` clamps to `0`).
fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// Map a canonical Postgres content-type string to its proto enum int, or `0`
/// (`CONTENT_TYPE_UNKNOWN`) when absent or unrecognised.
fn content_type_to_proto(value: Option<&str>) -> i32 {
    value
        .and_then(|s| s.parse::<ContentType>().ok())
        .map_or(0, ContentType::to_proto_value)
}

/// Mirrors Go's `.Label()` for the `V`-prefixed video enums — `VideoResolution`
/// (`V1080p` → `1080p`) and `Video3D` (`V3D` → `3D`) — i.e. `String()[1:]`, the
/// leading `V` dropped. `VideoSource` / `VideoCodec` / `VideoModifier` are not
/// `V`-prefixed (`Label() == String()`), so those still pass through raw.
fn strip_v_prefix(raw: &str) -> String {
    raw.strip_prefix('V').unwrap_or(raw).to_owned()
}

#[cfg(test)]
mod tests {
    use super::{build_document, strip_v_prefix};
    use crate::indexer::document_to_tantivy;
    use crate::proto::ContentType;
    use crate::schema::{build_schema, Fields};
    use bitmagnet_db::TorrentForIndex;
    use bitmagnet_model::{BlobFile, InfoHash};
    use tantivy::schema::Value;

    fn info_hash(byte: u8) -> InfoHash {
        InfoHash::from_slice(&[byte; 20]).unwrap()
    }

    fn blob_file(index: u32, path: &str, ext: &str, size: u64) -> BlobFile {
        BlobFile {
            index,
            path: path.to_owned(),
            extension: ext.to_owned(),
            size,
        }
    }

    /// A fully-classified movie `torrent_contents` row (with a two-file torrent).
    fn classified_row() -> TorrentForIndex {
        TorrentForIndex {
            id: format!("{}:movie:tmdb:603", "ab".repeat(20)),
            info_hash: info_hash(0xAB),
            torrent_name: "The Matrix 1999 1080p BluRay x265-GROUP".to_owned(),
            content_type: Some("movie".to_owned()),
            content_source: Some("tmdb".to_owned()),
            content_id: Some("603".to_owned()),
            content_title: Some("The Matrix".to_owned()),
            original_title: Some("The Matrix".to_owned()),
            release_year: Some(1999),
            video_resolution: Some("V1080p".to_owned()),
            video_source: Some("BluRay".to_owned()),
            video_codec: Some("x265".to_owned()),
            video_3d: Some("V3D".to_owned()),
            video_modifier: Some("REMUX".to_owned()),
            release_group: Some("GROUP".to_owned()),
            seeders: Some(123),
            leechers: Some(4),
            size: 9_000_000_000,
            files_count: Some(2),
            published_at: 1_700_000_000,
            languages: vec!["en".to_owned(), "fr".to_owned()],
            genres: vec!["action".to_owned(), "sci-fi".to_owned()],
            // build_document takes the decoded files separately; this is unused.
            files_data: None,
        }
    }

    /// An unclassified `torrent_content`: every content field empty, no blob.
    fn unclassified_row() -> TorrentForIndex {
        TorrentForIndex {
            id: format!("{}:?:?:?", "01".repeat(20)),
            info_hash: info_hash(0x01),
            torrent_name: "ubuntu-24.04-desktop-amd64.iso".to_owned(),
            content_type: None,
            content_source: None,
            content_id: None,
            content_title: None,
            original_title: None,
            release_year: None,
            video_resolution: None,
            video_source: None,
            video_codec: None,
            video_3d: None,
            video_modifier: None,
            release_group: None,
            seeders: None,
            leechers: None,
            size: 6_000_000_000,
            files_count: None,
            published_at: 1_600_000_000,
            languages: Vec::new(),
            genres: Vec::new(),
            files_data: None,
        }
    }

    fn classified_files() -> Vec<BlobFile> {
        vec![
            blob_file(0, "The.Matrix.1999.1080p.mkv", "mkv", 8_900_000_000),
            blob_file(1, "The.Matrix.1999.1080p.srt", "srt", 50_000),
            // A path with no extension contributes a path but no file extension.
            blob_file(2, "readme", "", 100),
        ]
    }

    #[test]
    fn maps_all_proto_fields_from_a_classified_row() {
        let doc = build_document(&classified_row(), &classified_files());

        assert_eq!(doc.info_hash, vec![0xAB; 20]);
        assert_eq!(doc.torrent_name, "The Matrix 1999 1080p BluRay x265-GROUP");
        assert_eq!(doc.content_title, "The Matrix");
        assert_eq!(doc.original_title, "The Matrix");
        assert_eq!(doc.release_year, 1999);
        assert_eq!(doc.video_resolution, "1080p");
        assert_eq!(doc.video_source, "BluRay");
        assert_eq!(doc.video_codec, "x265");
        assert_eq!(doc.genres, vec!["action", "sci-fi"]);
        assert_eq!(doc.content_type, ContentType::Movie as i32);
        assert_eq!(doc.seeders, 123);
        assert_eq!(doc.leechers, 4);
        assert_eq!(doc.files_count, 2);
        assert_eq!(doc.size, 9_000_000_000);
        assert_eq!(doc.published_at, 1_700_000_000);
        assert_eq!(doc.languages, vec!["en", "fr"]);
        assert_eq!(doc.video_modifier, "REMUX");
        assert_eq!(doc.release_group, "GROUP");
        assert_eq!(doc.content_source, "tmdb");
        assert_eq!(doc.content_id, "603");
    }

    #[test]
    fn file_paths_and_extensions_come_from_the_blob() {
        let doc = build_document(&classified_row(), &classified_files());

        // Every non-empty path is kept (including the extensionless one).
        assert_eq!(
            doc.file_paths,
            vec![
                "The.Matrix.1999.1080p.mkv",
                "The.Matrix.1999.1080p.srt",
                "readme",
            ]
        );
        // Extensions are unique + sorted and re-derived from the path; "readme"
        // contributes none.
        assert_eq!(doc.file_extensions, vec!["mkv", "srt"]);
    }

    #[test]
    fn video_v_prefixed_fields_use_gos_label() {
        // VideoResolution + Video3D both drop the leading V (Go's Label()).
        assert_eq!(strip_v_prefix("V1080p"), "1080p");
        assert_eq!(strip_v_prefix("V3D"), "3D");
        assert_eq!(strip_v_prefix("V3DSBS"), "3DSBS");
        assert_eq!(strip_v_prefix("V3DOU"), "3DOU");
        // Already-label / unexpected input is passed through unchanged.
        assert_eq!(strip_v_prefix("3D"), "3D");

        let doc = build_document(&classified_row(), &classified_files());
        assert_eq!(doc.video_resolution, "1080p");
        assert_eq!(doc.video_3d, "3D");
    }

    #[test]
    fn audio_languages_is_always_empty() {
        // No Postgres source — must never be populated, even when languages are.
        let doc = build_document(&classified_row(), &classified_files());
        assert!(doc.audio_languages.is_empty());
        assert!(!doc.languages.is_empty());
    }

    #[test]
    fn unclassified_torrent_yields_name_only_document() {
        // A torrent_content the classifier has not matched: it must still
        // produce a usable, name-searchable document with empty content fields.
        let doc = build_document(&unclassified_row(), &[]);

        assert_eq!(doc.torrent_name, "ubuntu-24.04-desktop-amd64.iso");
        assert_eq!(doc.size, 6_000_000_000);
        assert_eq!(doc.published_at, 1_600_000_000);
        assert_eq!(doc.content_type, 0);
        assert_eq!(doc.release_year, 0);
        assert!(doc.content_title.is_empty());
        assert!(doc.content_source.is_empty());
        assert!(doc.video_3d.is_empty());
        assert!(doc.genres.is_empty());
        assert!(doc.languages.is_empty());
        assert!(doc.file_paths.is_empty());
        assert!(doc.file_extensions.is_empty());
        // No blob and no column → files_count falls back to 0.
        assert_eq!(doc.files_count, 0);
    }

    #[test]
    fn files_count_uses_column_and_defaults_to_zero() {
        // files_count mirrors tc.files_count for PG parity: the column when set,
        // else 0 — the blob's length is NOT substituted (a 3-file blob is passed
        // in both cases, so 2 and 0 prove there is no fallback).
        assert_eq!(
            build_document(&classified_row(), &classified_files()).files_count,
            2
        );
        let mut none = classified_row();
        none.files_count = None;
        assert_eq!(build_document(&none, &classified_files()).files_count, 0);
    }

    #[test]
    fn negative_size_clamps_to_zero() {
        let mut row = classified_row();
        row.size = -1;
        let doc = build_document(&row, &[]);
        assert_eq!(doc.size, 0);
    }

    #[test]
    fn document_feeds_the_indexer_end_to_end() {
        // The transform's whole point is to feed `document_to_tantivy`; prove the
        // produced doc maps onto the real schema, that the composite doc_id is
        // formed from the content key, and that stored fields round-trip.
        let fields = Fields::from_schema(&build_schema()).unwrap();
        let row = classified_row();
        let doc = build_document(&row, &classified_files());
        let td = document_to_tantivy(&fields, &doc);

        // The indexer derives doc_id from the same content key the DB used to
        // generate `tc.id`, so it must reproduce the source row id exactly.
        assert_eq!(
            td.get_first(fields.doc_id).and_then(|v| v.as_str()),
            Some(row.id.as_str())
        );
        assert_eq!(
            td.get_first(fields.torrent_name).and_then(|v| v.as_str()),
            Some("The Matrix 1999 1080p BluRay x265-GROUP")
        );
        assert_eq!(
            td.get_first(fields.content_type).and_then(|v| v.as_str()),
            Some("movie")
        );
        assert_eq!(
            td.get_first(fields.release_year).and_then(|v| v.as_u64()),
            Some(1999)
        );
        let exts: Vec<_> = td
            .get_all(fields.file_extensions)
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(exts, vec!["mkv", "srt"]);
    }

    #[test]
    fn derived_doc_id_equals_source_row_id() {
        // Locks the cross-system invariant: the indexer's composite doc_id is
        // byte-identical to the PG `torrent_contents.id` the backfill paginates
        // on — so re-runs / resumes upsert the same document, never a duplicate.
        // Holds for classified rows and the all-`?` unclassified case alike.
        let fields = Fields::from_schema(&build_schema()).unwrap();
        for row in [classified_row(), unclassified_row()] {
            let td = document_to_tantivy(&fields, &build_document(&row, &[]));
            assert_eq!(
                td.get_first(fields.doc_id).and_then(|v| v.as_str()),
                Some(row.id.as_str()),
                "derived doc_id must equal tc.id ({:?})",
                row.id
            );
        }
    }

    #[test]
    fn distinct_classifications_share_info_hash_but_differ_by_content() {
        // Two classifications of the same torrent: same info_hash + name, but
        // different content_id — so `indexer::doc_id` (hence the upsert key)
        // differs and they coexist as two documents (asserted in indexer.rs).
        let a = classified_row();
        let mut b = classified_row();
        b.content_id = Some("604".to_owned());

        let doc_a = build_document(&a, &[]);
        let doc_b = build_document(&b, &[]);

        assert_eq!(doc_a.info_hash, doc_b.info_hash);
        assert_eq!(doc_a.torrent_name, doc_b.torrent_name);
        assert_ne!(doc_a.content_id, doc_b.content_id);
    }
}
