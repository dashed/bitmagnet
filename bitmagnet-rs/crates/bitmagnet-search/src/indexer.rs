//! Mapping the proto [`TorrentDocument`] onto a Tantivy document, plus the
//! upsert / delete primitives the write RPCs call.
//!
//! The relevance text is split across the four weight tiers exactly as Go's
//! `UpdateTsv` does (see [`crate::schema`] for the tier table). Everything the
//! read path needs to rebuild a hit is written to the stored / fast keyword and
//! numeric fields. `file_paths` is the deliberate exception: it feeds `text_d`
//! for relevance but is never stored — storing raw paths is the cost the blob
//! migration removed.

use tantivy::schema::Field;
use tantivy::{IndexWriter, TantivyDocument, Term};

use crate::proto::TorrentDocument;
use crate::schema::Fields;

/// Build a Tantivy document from a proto [`TorrentDocument`].
#[must_use]
pub fn document_to_tantivy(fields: &Fields, doc: &TorrentDocument) -> TantivyDocument {
    let mut td = TantivyDocument::new();

    // Composite identity = the upsert key (see `doc_id`).
    td.add_text(fields.doc_id, doc_id(doc));

    // --- Identity: bytes term (DeleteDocument key) + hex into the weight-A tier
    if !doc.info_hash.is_empty() {
        td.add_bytes(fields.info_hash, &doc.info_hash);
        td.add_text(fields.text_a, to_hex_lower(&doc.info_hash));
    }

    // --- Weight A: torrent name, content title, original title --------------
    // Stored (display) + tokenized into text_a (relevance), to match Go.
    add_stored_and_tier(
        &mut td,
        fields.torrent_name,
        fields.text_a,
        &doc.torrent_name,
    );
    add_stored_and_tier(
        &mut td,
        fields.content_title,
        fields.text_a,
        &doc.content_title,
    );
    add_stored_and_tier(
        &mut td,
        fields.original_title,
        fields.text_a,
        &doc.original_title,
    );

    // --- Weight B: release year (numeric facet/sort + text relevance) -------
    // 0 means "no year" on the proto wire, so skip it entirely (matches Go,
    // which only adds the year when present).
    if doc.release_year != 0 {
        td.add_u64(fields.release_year, u64::from(doc.release_year));
        td.add_text(fields.text_b, doc.release_year.to_string());
    }

    // --- Weight C: video resolution / source / codec ------------------------
    // Keyword (facet/filter) + tokenized into text_c (relevance).
    add_keyword_and_tier(
        &mut td,
        fields.video_resolution,
        fields.text_c,
        &doc.video_resolution,
    );
    add_keyword_and_tier(
        &mut td,
        fields.video_source,
        fields.text_c,
        &doc.video_source,
    );
    add_keyword_and_tier(&mut td, fields.video_codec, fields.text_c, &doc.video_codec);
    add_keyword_and_tier(&mut td, fields.video_3d, fields.text_c, &doc.video_3d);
    add_keyword_and_tier(
        &mut td,
        fields.video_modifier,
        fields.text_c,
        &doc.video_modifier,
    );
    add_keyword_and_tier(
        &mut td,
        fields.release_group,
        fields.text_c,
        &doc.release_group,
    );

    // --- Content classification key -----------------------------------------
    // content_source is facet/filter only; content_id is also weight-D
    // relevance (Go indexes external id values at weight D). The tmdb_id facet
    // aggregates content_id where content_source == "tmdb".
    if !doc.content_source.is_empty() {
        td.add_text(fields.content_source, &doc.content_source);
    }
    add_keyword_and_tier(&mut td, fields.content_id, fields.text_d, &doc.content_id);

    // --- Content type: facet/filter only (enum int -> canonical string) -----
    if let Some(ct) = bitmagnet_model::ContentType::from_proto_value(doc.content_type) {
        td.add_text(fields.content_type, ct.as_str());
    }

    // --- Numerics: sort / range filter (stored + fast) ----------------------
    td.add_u64(fields.size, doc.size);
    td.add_u64(fields.seeders, u64::from(doc.seeders));
    td.add_u64(fields.leechers, u64::from(doc.leechers));
    td.add_u64(fields.files_count, u64::from(doc.files_count));
    td.add_i64(fields.published_at, doc.published_at);

    // --- Multi-valued keyword facets ---------------------------------------
    // Genres are also weight-D relevance text; languages/extensions are
    // facet/filter only.
    for genre in &doc.genres {
        if !genre.is_empty() {
            td.add_text(fields.genres, genre);
            td.add_text(fields.text_d, genre);
        }
    }
    for language in &doc.languages {
        if !language.is_empty() {
            td.add_text(fields.languages, language);
        }
    }
    for language in &doc.audio_languages {
        if !language.is_empty() {
            td.add_text(fields.audio_languages, language);
        }
    }
    for extension in &doc.file_extensions {
        if !extension.is_empty() {
            td.add_text(fields.file_extensions, extension);
        }
    }

    // --- Weight D: file paths (relevance only, never stored) ----------------
    for path in &doc.file_paths {
        if !path.is_empty() {
            td.add_text(fields.text_d, path);
        }
    }

    td
}

/// Upsert `doc`: delete any existing document with the same composite
/// [`doc_id`], then add the new one. Tantivy applies the delete to documents
/// added before it (lower opstamp), so the freshly added document survives — a
/// correct replace. Keying on the composite id (not info_hash) is what lets a
/// torrent's multiple content classifications coexist as distinct documents.
///
/// The change is not visible to readers until the writer is committed.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the document cannot be added.
pub fn upsert(writer: &IndexWriter, fields: &Fields, doc: &TorrentDocument) -> tantivy::Result<()> {
    // Replace by composite id, NOT info_hash: one torrent can have many
    // torrent_content rows, so keying on info_hash would clobber siblings.
    writer.delete_term(Term::from_field_text(fields.doc_id, &doc_id(doc)));
    writer.add_document(document_to_tantivy(fields, doc))?;
    Ok(())
}

/// Delete the document with the given info hash, if present. No-op for an empty
/// hash. Takes effect on the next commit.
pub fn delete(writer: &IndexWriter, fields: &Fields, info_hash: &[u8]) {
    if !info_hash.is_empty() {
        writer.delete_term(Term::from_field_bytes(fields.info_hash, info_hash));
    }
}

/// Add `value` to a stored display field and tokenize it into a relevance tier,
/// skipping empty strings (proto defaults).
fn add_stored_and_tier(td: &mut TantivyDocument, stored: Field, tier: Field, value: &str) {
    if !value.is_empty() {
        td.add_text(stored, value);
        td.add_text(tier, value);
    }
}

/// Add `value` to a keyword facet/filter field and tokenize it into a relevance
/// tier, skipping empty strings.
fn add_keyword_and_tier(td: &mut TantivyDocument, keyword: Field, tier: Field, value: &str) {
    if !value.is_empty() {
        td.add_text(keyword, value);
        td.add_text(tier, value);
    }
}

/// The canonical cross-system row id — byte-identical to the PG
/// `torrent_contents.id` generated column and Go `TorrentContent.InferID()`:
/// `hex(info_hash):content_type:content_source:content_id`, each segment `?`
/// when absent. This (not info_hash) is the upsert identity, because one torrent
/// can be classified as many content rows.
fn doc_id(doc: &TorrentDocument) -> String {
    let content_type = bitmagnet_model::ContentType::from_proto_value(doc.content_type)
        .map_or("?", |ct| ct.as_str());
    let content_source = if doc.content_source.is_empty() {
        "?"
    } else {
        doc.content_source.as_str()
    };
    let content_id = if doc.content_id.is_empty() {
        "?"
    } else {
        doc.content_id.as_str()
    };
    format!(
        "{}:{content_type}:{content_source}:{content_id}",
        to_hex_lower(&doc.info_hash)
    )
}

/// Lower-case hex encoding of `bytes`, matching Go's `InfoHash.String()` so a
/// full-hash search tokenizes to the same single lexeme on both sides.
fn to_hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a String is infallible.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{delete, doc_id, document_to_tantivy, to_hex_lower, upsert};
    use crate::index::{register_tokenizer, writer};
    use crate::proto::{ContentType, TorrentDocument};
    use crate::schema::{build_schema, Fields};
    use tantivy::schema::Value;
    use tantivy::{Index, TantivyDocument};

    fn sample() -> TorrentDocument {
        TorrentDocument {
            info_hash: vec![0xAB; 20],
            torrent_name: "Ubuntu 22.04 LTS".to_owned(),
            content_title: "Ubuntu".to_owned(),
            original_title: String::new(),
            release_year: 2022,
            video_resolution: "1080p".to_owned(),
            video_source: "WEB-DL".to_owned(),
            video_codec: "x265".to_owned(),
            genres: vec!["linux".to_owned(), String::new()],
            file_paths: vec!["ubuntu.iso".to_owned()],
            content_type: ContentType::Software as i32,
            seeders: 100,
            leechers: 5,
            files_count: 1,
            size: 4_000_000_000,
            published_at: 1_700_000_000,
            languages: vec!["en".to_owned()],
            file_extensions: vec!["iso".to_owned()],
            video_3d: String::new(),
            video_modifier: "REMUX".to_owned(),
            release_group: "GROUP".to_owned(),
            audio_languages: vec!["en".to_owned(), "fr".to_owned()],
            content_source: "tmdb".to_owned(),
            content_id: "603".to_owned(),
        }
    }

    #[test]
    fn maps_core_fields_to_stored_values() {
        let fields = Fields::from_schema(&build_schema()).unwrap();
        let td = document_to_tantivy(&fields, &sample());

        // Stored display fields round-trip.
        let name = td
            .get_first(fields.torrent_name)
            .and_then(|v| v.as_str())
            .unwrap();
        assert_eq!(name, "Ubuntu 22.04 LTS");
        // Numerics are present.
        assert_eq!(
            td.get_first(fields.size).and_then(|v| v.as_u64()),
            Some(4_000_000_000)
        );
        assert_eq!(
            td.get_first(fields.published_at).and_then(|v| v.as_i64()),
            Some(1_700_000_000)
        );
        // Content type maps enum int -> canonical string.
        assert_eq!(
            td.get_first(fields.content_type).and_then(|v| v.as_str()),
            Some("software")
        );
        // Empty genre is skipped: exactly one genre value indexed.
        let genres: Vec<_> = td
            .get_all(fields.genres)
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(genres, vec!["linux"]);

        // Extended facet fields map through.
        assert_eq!(
            td.get_first(fields.content_source).and_then(|v| v.as_str()),
            Some("tmdb")
        );
        assert_eq!(
            td.get_first(fields.content_id).and_then(|v| v.as_str()),
            Some("603")
        );
        assert_eq!(
            td.get_first(fields.video_modifier).and_then(|v| v.as_str()),
            Some("REMUX")
        );
        let audio: Vec<_> = td
            .get_all(fields.audio_languages)
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(audio, vec!["en", "fr"]);
        // Empty video_3d is skipped.
        assert!(td.get_first(fields.video_3d).is_none());
        // Composite doc_id is stored.
        assert_eq!(
            td.get_first(fields.doc_id).and_then(|v| v.as_str()),
            Some(format!("{}:software:tmdb:603", "ab".repeat(20))).as_deref()
        );
    }

    #[test]
    fn release_year_zero_is_omitted() {
        let fields = Fields::from_schema(&build_schema()).unwrap();
        let mut doc = sample();
        doc.release_year = 0;
        let td = document_to_tantivy(&fields, &doc);
        assert!(td.get_first(fields.release_year).is_none());
    }

    #[test]
    fn hex_encoding_matches_go_lowercase() {
        assert_eq!(to_hex_lower(&[0x00, 0x0f, 0xab, 0xff]), "000fabff");
    }

    #[test]
    fn doc_id_matches_pg_infer_id_format() {
        // Byte-identical to Go `TorrentContent.InferID()` / the PG generated
        // `torrent_contents.id` column.
        let s = sample(); // 0xAB*20, Software, "tmdb", "603"
        assert_eq!(doc_id(&s), format!("{}:software:tmdb:603", "ab".repeat(20)));

        // Unclassified torrent -> every absent segment becomes "?".
        let mut u = sample();
        u.content_type = ContentType::Unknown as i32;
        u.content_source = String::new();
        u.content_id = String::new();
        assert_eq!(doc_id(&u), format!("{}:?:?:?", "ab".repeat(20)));
    }

    #[test]
    fn upsert_replaces_same_doc_id() {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index);
        let fields = Fields::from_schema(&index.schema()).unwrap();
        let mut w = writer(&index).unwrap();

        upsert(&w, &fields, &sample()).unwrap();
        upsert(&w, &fields, &sample()).unwrap(); // same doc_id -> replace
        w.commit().unwrap();

        let reader = crate::index::reader(&index).unwrap();
        reader.reload().unwrap();
        // Build a doc by hand to prove the type alias matches the writer's.
        let _: TantivyDocument = document_to_tantivy(&fields, &sample());
        assert_eq!(
            reader.searcher().num_docs(),
            1,
            "upsert must replace, not duplicate"
        );
    }

    #[test]
    fn distinct_classifications_of_one_torrent_coexist() {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index);
        let fields = Fields::from_schema(&index.schema()).unwrap();
        let mut w = writer(&index).unwrap();

        // Same torrent (info_hash), two content classifications => two docs.
        let a = sample(); // content_id "603"
        let mut b = sample();
        b.content_id = "604".to_owned(); // different doc_id, same info_hash
        assert_eq!(a.info_hash, b.info_hash);
        assert_ne!(doc_id(&a), doc_id(&b));

        upsert(&w, &fields, &a).unwrap();
        upsert(&w, &fields, &b).unwrap();
        w.commit().unwrap();
        let reader = crate::index::reader(&index).unwrap();
        reader.reload().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            2,
            "distinct classifications of one torrent must coexist"
        );

        // Re-upserting one classification replaces only it.
        upsert(&w, &fields, &a).unwrap();
        w.commit().unwrap();
        reader.reload().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            2,
            "re-upsert of one classification must not duplicate"
        );

        // DeleteDocument(info_hash) clears ALL of the torrent's documents
        // (mirrors PG's on-delete cascade from torrents -> torrent_contents).
        delete(&w, &fields, &a.info_hash);
        w.commit().unwrap();
        reader.reload().unwrap();
        assert_eq!(
            reader.searcher().num_docs(),
            0,
            "delete-by-info_hash removes every document for the torrent"
        );
    }
}
