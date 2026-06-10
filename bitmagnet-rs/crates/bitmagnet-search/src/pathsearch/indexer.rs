//! Build a per-torrent path-bag [`TantivyDocument`] and the upsert / delete
//! primitives the writer (backfill + follow loop + push RPC) drives.
//!
//! ## One document per torrent, keyed by `info_hash`
//!
//! Unlike the main search index (keyed by the composite `doc_id` so a torrent's
//! several content classifications coexist), the path-bag index is **per
//! torrent**: one doc per `info_hash`, holding *all* the torrent's file paths.
//! Supersession is therefore a single `delete_term(info_hash)` + a single
//! re-add (PS-T3 §2.1) — the re-crawl replaces the whole fileset.
//!
//! ## The path-bag, file-boundary-safe
//!
//! Every file path is added as a **separate value** of the `path_grams` field
//! (`add_text` called once per path), so no ngram window straddles two files'
//! paths — a "boundary gram" would create false substring matches. A single-file
//! torrent stores no per-file blob, so when the blob yields no paths the torrent
//! **name** is indexed as the lone path value (mirrors the extension fallback in
//! the main backfill's `transform`) — otherwise single-file torrents would be
//! un-typeahead-able.

use tantivy::{IndexWriter, TantivyDocument, Term};

use super::schema::PathFields;

/// The fields needed to build one path-bag document, decoupled from any
/// particular source DTO (the DB follow/backfill row, or the push proto doc).
pub struct PathDoc<'a> {
    /// 20-byte v1 info hash (the delete/upsert key + hit identity).
    pub info_hash: &'a [u8],
    /// Every file path in the torrent. Each becomes a separate `path_grams`
    /// value (no cross-file boundary grams).
    pub file_paths: &'a [String],
    /// Max seeders (rank key).
    pub seeders: u64,
    /// Total torrent size in bytes.
    pub size: u64,
    /// File count (shortest-path proxy / display).
    pub files_count: u64,
    /// Fallback path value (the torrent name) used only when `file_paths` is
    /// empty — keeps single-file / blob-less torrents findable. Empty to skip.
    pub name_fallback: &'a str,
}

/// Build the Tantivy path-bag document for one torrent.
#[must_use]
pub fn build_document(fields: &PathFields, doc: &PathDoc<'_>) -> TantivyDocument {
    let mut td = TantivyDocument::new();

    // Identity: indexed bytes (delete key) + stored (returned in the hit).
    td.add_bytes(fields.info_hash, doc.info_hash);

    // The path-bag: one field value per file path (no boundary grams). When the
    // blob produced no paths, fall back to the torrent name so the torrent is
    // still findable by its name's substrings.
    let mut any_path = false;
    for path in doc.file_paths {
        if !path.is_empty() {
            td.add_text(fields.path_grams, path);
            any_path = true;
        }
    }
    if !any_path && !doc.name_fallback.is_empty() {
        td.add_text(fields.path_grams, doc.name_fallback);
    }

    // Ranking / display signals (FAST).
    td.add_u64(fields.seeders, doc.seeders);
    td.add_u64(fields.size, doc.size);
    td.add_u64(fields.files_count, doc.files_count);

    td
}

/// Upsert one torrent's path-bag: `delete_term(info_hash)` then add the rebuilt
/// document. Tantivy applies the delete to lower-opstamp docs, so the freshly
/// added document survives — a correct replace. No-op identity for an empty
/// hash (it would index an undeletable doc). Visible after the next `commit()`.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the document cannot be added.
pub fn upsert(
    writer: &IndexWriter,
    fields: &PathFields,
    doc: &PathDoc<'_>,
) -> tantivy::Result<()> {
    if doc.info_hash.is_empty() {
        return Ok(());
    }
    writer.delete_term(Term::from_field_bytes(fields.info_hash, doc.info_hash));
    writer.add_document(build_document(fields, doc))?;
    Ok(())
}

/// Delete a torrent's path-bag doc by info hash (supersession / removal). No-op
/// for an empty hash. Takes effect on the next `commit()`.
pub fn delete(writer: &IndexWriter, fields: &PathFields, info_hash: &[u8]) {
    if !info_hash.is_empty() {
        writer.delete_term(Term::from_field_bytes(fields.info_hash, info_hash));
    }
}

#[cfg(test)]
mod tests {
    use super::{build_document, delete, upsert, PathDoc};
    use crate::pathsearch::index::{path_reader, path_writer, register_path_index};
    use crate::pathsearch::schema::{build_path_schema, PathFields};
    use tantivy::schema::Value;
    use tantivy::Index;

    fn doc<'a>(hash: &'a [u8], paths: &'a [String], name: &'a str) -> PathDoc<'a> {
        PathDoc {
            info_hash: hash,
            file_paths: paths,
            seeders: 10,
            size: 1_000,
            files_count: paths.len() as u64,
            name_fallback: name,
        }
    }

    #[test]
    fn build_adds_one_grams_value_per_path() {
        let fields = PathFields::from_schema(&build_path_schema()).unwrap();
        let paths = vec!["a/b.mkv".to_owned(), "a/c.srt".to_owned()];
        let td = build_document(&fields, &doc(&[0xAB; 20], &paths, "ignored"));
        let grams: Vec<_> = td
            .get_all(fields.path_grams)
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(grams, vec!["a/b.mkv", "a/c.srt"]);
    }

    #[test]
    fn build_falls_back_to_name_when_no_paths() {
        let fields = PathFields::from_schema(&build_path_schema()).unwrap();
        let td = build_document(&fields, &doc(&[0x01; 20], &[], "Ubuntu.2024.iso"));
        let grams: Vec<_> = td
            .get_all(fields.path_grams)
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(grams, vec!["Ubuntu.2024.iso"]);
    }

    #[test]
    fn upsert_replaces_by_info_hash_and_delete_removes() {
        let index = Index::create_in_ram(build_path_schema());
        register_path_index(&index);
        let fields = PathFields::from_schema(&index.schema()).unwrap();
        let mut w = path_writer(&index).unwrap();

        let paths = vec!["movie.mkv".to_owned()];
        upsert(&w, &fields, &doc(&[0x01; 20], &paths, "")).unwrap();
        upsert(&w, &fields, &doc(&[0x01; 20], &paths, "")).unwrap(); // same hash → replace
        upsert(&w, &fields, &doc(&[0x02; 20], &paths, "")).unwrap();
        w.commit().unwrap();
        let reader = path_reader(&index).unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2, "upsert replaces per info_hash");

        delete(&w, &fields, &[0x01; 20]);
        w.commit().unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1, "delete removes the torrent");
    }

    #[test]
    fn empty_info_hash_is_a_noop_upsert() {
        let index = Index::create_in_ram(build_path_schema());
        register_path_index(&index);
        let fields = PathFields::from_schema(&index.schema()).unwrap();
        let mut w = path_writer(&index).unwrap();
        upsert(&w, &fields, &doc(&[], &["x.mkv".to_owned()], "")).unwrap();
        w.commit().unwrap();
        let reader = path_reader(&index).unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);
    }
}
