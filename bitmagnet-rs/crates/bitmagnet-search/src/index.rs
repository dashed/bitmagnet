//! Index lifecycle: opening or creating the on-disk Tantivy index.
//!
//! Phase 3 fills in the bodies; these signatures fix the module's shape so the
//! server and ingest paths can be written against a stable surface.

use std::path::Path;

use tantivy::{Index, IndexReader, IndexWriter};

/// Open the Tantivy index at `path`, creating it from
/// [`crate::schema::build_schema`] when the directory is empty.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the directory cannot be opened, or if
/// an existing on-disk schema does not match [`crate::schema::build_schema`].
///
/// # Panics
/// Always panics — not implemented until Phase 3.
pub fn open_or_create(_path: &Path) -> tantivy::Result<Index> {
    unimplemented!("Phase 3: open or create the on-disk Tantivy index")
}

/// Build a reader with bitmagnet's reload policy.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the reader cannot be constructed.
///
/// # Panics
/// Always panics — not implemented until Phase 3.
pub fn reader(_index: &Index) -> tantivy::Result<IndexReader> {
    unimplemented!("Phase 3: build the index reader")
}

/// Allocate a writer sized for bitmagnet's ingest path.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the writer cannot be allocated.
///
/// # Panics
/// Always panics — not implemented until Phase 3.
pub fn writer(_index: &Index) -> tantivy::Result<IndexWriter> {
    unimplemented!("Phase 3: allocate the index writer")
}
