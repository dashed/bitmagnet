//! Index lifecycle helpers for the L3 pathsearch index.

use std::path::Path;

use anyhow::Context;
use tantivy::directory::MmapDirectory;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};

use crate::pathsearch::schema::{build_schema, register_tokenizer};

/// Default writer heap for production path-bag ingest.
pub const DEFAULT_WRITER_HEAP_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// Open or create a pathsearch index at `path`.
///
/// # Errors
/// Returns an error if the directory cannot be created/opened or if an existing
/// index schema is incompatible with the pathsearch schema.
pub fn open_or_create(path: &Path) -> anyhow::Result<Index> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("creating pathsearch index dir {}", path.display()))?;
    let dir = MmapDirectory::open(path)
        .with_context(|| format!("opening pathsearch index dir {}", path.display()))?;
    let index = Index::open_or_create(dir, build_schema())
        .with_context(|| format!("opening or creating pathsearch index at {}", path.display()))?;
    register_tokenizer(&index).context("registering pathsearch tokenizer")?;
    Ok(index)
}

/// Build a reader that notices commits shortly after they land.
///
/// # Errors
/// Returns the Tantivy reader construction error.
pub fn reader(index: &Index) -> tantivy::Result<IndexReader> {
    index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
}

/// Allocate the single pathsearch writer.
///
/// # Errors
/// Returns the Tantivy writer allocation error, including writer-lock conflicts.
pub fn writer(index: &Index, heap_bytes: usize, threads: usize) -> tantivy::Result<IndexWriter> {
    index.writer_with_num_threads(threads.max(1), heap_bytes.max(256 * 1024 * 1024))
}

#[cfg(test)]
mod tests {
    use super::{reader, writer};
    use crate::pathsearch::schema::{build_schema, register_tokenizer};
    use tantivy::Index;

    #[test]
    fn reader_and_writer_build_on_ram_index() {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index).expect("tokenizer registers");
        let _writer = writer(&index, 256 * 1024 * 1024, 1).expect("writer allocates");
        let reader = reader(&index).expect("reader builds");
        assert_eq!(reader.searcher().num_docs(), 0);
    }
}
