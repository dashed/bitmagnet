//! Path-bag index lifecycle: open/create, register the ngram analyzer, and the
//! **single-thread, large-arena** writer the ngram workload requires.
//!
//! # The single-thread + ≥2 GiB-arena invariant (measured, load-bearing)
//!
//! EXP-D found the default multi-thread 256 MiB writer **crashes** ("index
//! writer killed") on the ngram token explosion — each worker's ~32 MiB arena
//! starves. The fix is one writer thread with a ≥2 GiB arena
//! ([`PATH_WRITER_HEAP_BYTES`]). This is not a tuning preference; it is the
//! crash-avoidance configuration the backfill and the follow loop both use.

use std::path::Path;

use anyhow::Context;
use tantivy::directory::MmapDirectory;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};

use super::schema::build_path_schema;
use super::tokenizer::register_path_tokenizer;

/// Writer arena for the single ngram writer thread. 2 GiB — the ≥2 GiB floor
/// EXP-D established (the multi-thread 256 MiB default starves and is killed).
pub const PATH_WRITER_HEAP_BYTES: usize = 2 * 1024 * 1024 * 1024;

// Compile-time guard: the ngram writer crashes below ~2 GiB (EXP-D).
const _: () = assert!(PATH_WRITER_HEAP_BYTES >= 2 * 1024 * 1024 * 1024);

/// Open the path-bag Tantivy index at `path`, creating it from
/// [`build_path_schema`] when absent, and register the ngram analyzer.
///
/// An existing index's on-disk schema must match [`build_path_schema`] exactly
/// (`open_or_create` enforces this), guarding against reading an index written
/// by an older schema/tokenizer.
///
/// # Errors
/// Returns an error if `path` cannot be created/opened, or an existing index's
/// schema does not match.
pub fn open_or_create_path(path: &Path) -> anyhow::Result<Index> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("creating path index directory {}", path.display()))?;
    let dir = MmapDirectory::open(path)
        .with_context(|| format!("opening path index directory {}", path.display()))?;
    let index = Index::open_or_create(dir, build_path_schema())
        .with_context(|| format!("opening or creating path index at {}", path.display()))?;
    register_path_tokenizer(&index);
    Ok(index)
}

/// Register the ngram analyzer on `index`. Runtime state (not persisted), so it
/// must run on every freshly opened/created index before read or write.
pub fn register_path_index(index: &Index) {
    register_path_tokenizer(index);
}

/// Build a reader that refreshes shortly after each commit
/// ([`ReloadPolicy::OnCommitWithDelay`]). The follow loop additionally calls
/// `reload()` explicitly after each commit for prompt freshness.
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the reader cannot be constructed.
pub fn path_reader(index: &Index) -> tantivy::Result<IndexReader> {
    index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
}

/// Allocate the SOLE path-index writer: **one** thread, [`PATH_WRITER_HEAP_BYTES`]
/// arena (the ngram crash-avoidance config). Tantivy permits exactly one writer
/// per directory; the serving pod holds it for the lifetime of the process
/// (it is also the follow-loop writer).
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] if the writer cannot be allocated (e.g.
/// the directory lock is already held by another process — the single-writer
/// backstop).
pub fn path_writer(index: &Index) -> tantivy::Result<IndexWriter> {
    index.writer_with_num_threads(1, PATH_WRITER_HEAP_BYTES)
}

#[cfg(test)]
mod tests {
    use super::{path_reader, path_writer, register_path_index};
    use crate::pathsearch::schema::build_path_schema;
    use tantivy::Index;

    #[test]
    fn reader_and_single_thread_writer_build() {
        let index = Index::create_in_ram(build_path_schema());
        register_path_index(&index);
        // NOTE: in-RAM tests use the default heap (a 2 GiB arena per test is
        // wasteful); production uses `path_writer`. This proves the single-thread
        // allocation path compiles + runs.
        let _w: tantivy::IndexWriter = index
            .writer_with_num_threads(1, 15_000_000)
            .expect("1-thread writer");
        let reader = path_reader(&index).expect("reader builds");
        assert_eq!(reader.searcher().num_docs(), 0);
    }

    #[test]
    fn path_writer_constructor_typechecks() {
        // The ≥2 GiB floor is enforced at compile time (see the const _ assert);
        // here we only ensure the single-thread constructor's signature holds.
        let _ = path_writer as fn(&Index) -> tantivy::Result<tantivy::IndexWriter>;
    }
}
