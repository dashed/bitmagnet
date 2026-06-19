//! Index lifecycle: opening/creating the index, and the reader/writer handles
//! the [`crate::server::SearchServer`] runs on.

use std::path::Path;

use anyhow::Context;
use tantivy::directory::MmapDirectory;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};

use crate::schema::build_schema;
use crate::tokenizer::{analyzer, TOKENIZER_NAME};

/// Writer heap, split across the writer's worker threads. 256 MiB is Tantivy's
/// usual sweet spot for sustained ingest without over-frequent segment flushes.
const WRITER_HEAP_BYTES: usize = 256 * 1024 * 1024;

/// Open the Tantivy index at `path`, creating it from [`build_schema`] when the
/// directory has no index yet, and register the bitmagnet tokenizer on it.
///
/// If an index already exists, its on-disk schema must match [`build_schema`]
/// exactly (Tantivy's `open_or_create` enforces this and errors otherwise),
/// which guards against silently reading an index written by an older schema.
///
/// # Errors
/// Returns an error if `path` cannot be created/opened, or if an existing index
/// has a schema that does not match [`build_schema`].
pub fn open_or_create(path: &Path) -> anyhow::Result<Index> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("creating index directory {}", path.display()))?;
    let dir = MmapDirectory::open(path)
        .with_context(|| format!("opening index directory {}", path.display()))?;
    let index = Index::open_or_create(dir, build_schema())
        .with_context(|| format!("opening or creating index at {}", path.display()))?;
    register_tokenizer(&index);
    Ok(index)
}

/// Register the bitmagnet tokenizer (the `TokenizeFlat` port) under
/// [`TOKENIZER_NAME`] on `index`. Tokenizers are runtime state, not persisted,
/// so this must run on every freshly opened/created index before it is read
/// from or written to. The query parser (read path) registers the same
/// tokenizer under the same name, so the writer and reader tokenize text via
/// one shared path — the parity that shadow mode depends on.
pub fn register_tokenizer(index: &Index) {
    index.tokenizers().register(TOKENIZER_NAME, analyzer());
}

/// Build a reader that refreshes shortly after each commit
/// ([`ReloadPolicy::OnCommitWithDelay`]).
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the reader cannot be constructed.
pub fn reader(index: &Index) -> tantivy::Result<IndexReader> {
    index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
}

/// Allocate the single index writer with a [`WRITER_HEAP_BYTES`] heap.
///
/// Tantivy permits exactly one writer per index; the server keeps this behind a
/// mutex so all ingest is serialized through it.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the writer cannot be allocated (e.g.
/// the index lock is already held).
pub fn writer(index: &Index) -> tantivy::Result<IndexWriter> {
    index.writer(WRITER_HEAP_BYTES)
}

#[cfg(test)]
mod tests {
    use super::{reader, register_tokenizer, writer};
    use crate::schema::build_schema;
    use tantivy::Index;

    #[test]
    fn reader_and_writer_build_on_ram_index() {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index);
        let _writer = writer(&index).expect("writer allocates");
        let reader = reader(&index).expect("reader builds");
        assert_eq!(reader.searcher().num_docs(), 0);
    }
}
