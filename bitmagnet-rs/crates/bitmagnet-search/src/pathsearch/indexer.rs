//! Write-path mapping for pathsearch documents.

use tantivy::{IndexWriter, TantivyDocument, Term};

use crate::pathsearch::document::PathDocument;
use crate::pathsearch::schema::Fields;

/// Build a Tantivy document from a pathsearch [`PathDocument`].
#[must_use]
pub fn document_to_tantivy(fields: &Fields, doc: &PathDocument) -> TantivyDocument {
    let mut td = TantivyDocument::new();
    for path in &doc.paths {
        if !path.is_empty() {
            td.add_text(fields.path, path);
        }
    }
    if !doc.name.is_empty() {
        td.add_text(fields.name, &doc.name);
    }
    td.add_bytes(fields.info_hash, &doc.info_hash);
    td.add_u64(fields.size, doc.size);
    td.add_u64(fields.files_count, doc.files_count);
    td.add_u64(fields.seeders, doc.seeders);
    td.add_i64(fields.published_at, doc.published_at);
    td
}

/// Replace any existing path-bag doc for this torrent.
///
/// The change becomes visible after `commit` + reader reload.
///
/// # Errors
/// Returns Tantivy document-add failures.
pub fn upsert(writer: &IndexWriter, fields: &Fields, doc: &PathDocument) -> tantivy::Result<()> {
    delete(writer, fields, &doc.info_hash);
    writer.add_document(document_to_tantivy(fields, doc))?;
    Ok(())
}

/// Tombstone any path-bag doc with `info_hash`.
pub fn delete(writer: &IndexWriter, fields: &Fields, info_hash: &[u8]) {
    if !info_hash.is_empty() {
        writer.delete_term(Term::from_field_bytes(fields.info_hash, info_hash));
    }
}

#[cfg(test)]
mod tests {
    use super::{delete, upsert};
    use crate::pathsearch::document::PathDocument;
    use crate::pathsearch::index::{reader, writer};
    use crate::pathsearch::schema::{build_schema, register_tokenizer, Fields};
    use tantivy::Index;

    fn doc(byte: u8, path: &str) -> PathDocument {
        PathDocument {
            info_hash: vec![byte; 20],
            name: String::new(),
            paths: vec![path.to_owned()],
            size: 10,
            files_count: 1,
            seeders: 0,
            published_at: 1,
        }
    }

    #[test]
    fn upsert_replaces_by_info_hash() {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index).unwrap();
        let fields = Fields::from_schema(&index.schema()).unwrap();
        let reader = reader(&index).unwrap();
        let mut w = writer(&index, 256 * 1024 * 1024, 1).unwrap();

        upsert(&w, &fields, &doc(1, "old.mkv")).unwrap();
        w.commit().unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1);

        upsert(&w, &fields, &doc(1, "new.mkv")).unwrap();
        w.commit().unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1);

        delete(&w, &fields, &[1; 20]);
        w.commit().unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);
    }
}
