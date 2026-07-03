//! `bitmagnet-backfill` — bulk-index existing PostgreSQL torrents into Tantivy.
//!
//! Phase 3, task #4. Reads `torrent_contents` (each joined to its torrent and,
//! when classified, its `content`) in keyset-paginated pages, turns every row
//! into a proto [`TorrentDocument`] via [`bitmagnet_search::transform`], and
//! upserts it into the Tantivy index the sidecar serves. Now that file lists
//! live in the per-torrent `files_data` blob, the source is ~16 GB rather than
//! the 273 GB of `torrent_files` rows, so a full backfill is comparatively
//! cheap.
//!
//! ## Design
//!
//! * **One document per `torrent_content`.** The index keys every document by a
//!   composite `doc_id` (`hex(info_hash):content_type:content_source:content_id`),
//!   so a torrent classified as several contents becomes several distinct
//!   documents — mirroring the rows bitmagnet's Postgres `tsv @@ tsquery` search
//!   returns. (Torrents with no `torrent_contents` row are not indexed, so the
//!   Tantivy corpus matches Postgres rather than being a superset.)
//! * **Keyset pagination by `tc.id`.** Each page asks for rows strictly after
//!   the last id seen, an O(n) scan over the composite primary-key index with no
//!   deepening `OFFSET`. That id is logged on every commit, and `--after-id`
//!   resumes from it.
//! * **Idempotent.** Documents are *upserted* (delete-by-`doc_id` then add), so
//!   re-running — or resuming over an already-indexed tail after a crash between
//!   commits — replaces rather than duplicates.
//! * **Resilient.** A single undecodable file blob is logged and skipped (the
//!   torrent_content is still indexed, just without file paths); it never aborts
//!   the run.

use std::path::PathBuf;

use anyhow::Context;
use bitmagnet_db::{connect, stream_torrents_for_index, DbConfig, TorrentForIndex};
use bitmagnet_model::BlobFile;
use bitmagnet_search::index::{open_or_create, writer};
use bitmagnet_search::indexer::upsert;
use bitmagnet_search::schema::Fields;
use bitmagnet_search::transform::build_document;
use clap::Parser;
use tracing::{info, warn};

/// `bitmagnet-backfill` — index existing PostgreSQL torrents into Tantivy.
#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-backfill",
    about = "Backfill the bitmagnet Tantivy search index from PostgreSQL"
)]
struct Args {
    /// Directory holding the Tantivy index (created if it does not exist).
    #[arg(
        long,
        env = "BITMAGNET_SEARCH_INDEX",
        default_value = "/var/lib/bitmagnet/search"
    )]
    index_path: PathBuf,

    /// PostgreSQL DSN (`postgres://user:pass@host:port/db`). When empty, the
    /// connection is built from the `BITMAGNET_POSTGRES_*` environment variables
    /// (including `BITMAGNET_POSTGRES_DSN`).
    #[arg(long, default_value = "")]
    postgres_dsn: String,

    /// Number of rows fetched per keyset page.
    #[arg(long, default_value_t = 1000)]
    batch_size: i64,

    /// Stop after indexing this many documents (default: index everything).
    #[arg(long)]
    limit: Option<u64>,

    /// Commit to disk (making the new documents searchable) every N documents.
    #[arg(long, default_value_t = 10_000)]
    commit_interval: u64,

    /// Resume: only index `torrent_contents` whose `id` is strictly greater than
    /// this composite id (the `hex:type:source:id` value a prior run logged).
    #[arg(long)]
    after_id: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bitmagnet_common::init_tracing();
    run(Args::parse()).await
}

/// Drive the backfill: connect, open the index, then page through PostgreSQL
/// upserting documents and committing every `commit_interval` rows.
async fn run(args: Args) -> anyhow::Result<()> {
    anyhow::ensure!(args.batch_size > 0, "--batch-size must be positive");
    anyhow::ensure!(
        args.commit_interval > 0,
        "--commit-interval must be positive"
    );

    // DSN flag takes precedence; otherwise the BITMAGNET_POSTGRES_* env vars.
    let mut cfg = DbConfig::from_env().context("reading postgres config from env")?;
    if !args.postgres_dsn.is_empty() {
        cfg.dsn = args.postgres_dsn.clone();
    }
    let pool = connect(&cfg).await.context("connecting to postgres")?;

    info!(index_path = %args.index_path.display(), "opening Tantivy index");
    let index = open_or_create(&args.index_path)?;
    let fields = Fields::from_schema(&index.schema()).context("resolving schema fields")?;
    let mut index_writer = writer(&index).context("allocating index writer")?;

    let mut cursor: Option<String> = args.after_id.clone();

    info!(
        batch_size = args.batch_size,
        commit_interval = args.commit_interval,
        limit = ?args.limit,
        start_after = cursor.as_deref(),
        "backfill starting"
    );

    let mut indexed: u64 = 0;
    let mut since_commit: u64 = 0;
    let mut blob_errors: u64 = 0;
    let mut info_hash_decode_skips: u64 = 0;

    'pages: loop {
        let page = stream_torrents_for_index(&pool, cursor.as_deref(), args.batch_size)
            .await
            .context("reading torrent_contents page from postgres")?;
        info_hash_decode_skips += page.skipped_info_hash_decodes;
        if page.is_empty() {
            if let Some(last_seen_id) = page.last_seen_id {
                cursor = Some(last_seen_id);
                continue;
            }
            break;
        }

        for row in &page {
            let files = decode_files(row, &mut blob_errors);
            let doc = build_document(row, &files);
            upsert(&index_writer, &fields, &doc).context("indexing document")?;

            cursor = Some(row.id.clone());
            indexed += 1;
            since_commit += 1;

            if since_commit >= args.commit_interval {
                index_writer.commit().context("committing index")?;
                since_commit = 0;
                info!(indexed, last_id = %row.id, "committed");
            }
            if args.limit.is_some_and(|limit| indexed >= limit) {
                info!(limit = indexed, "document limit reached");
                break 'pages;
            }
        }
        cursor = page.last_seen_id;
    }

    index_writer.commit().context("final index commit")?;
    info!(
        indexed,
        blob_errors,
        info_hash_decode_skips,
        last_id = cursor.as_deref(),
        "backfill complete"
    );
    Ok(())
}

/// Decode a torrent_content's compressed `files_data` blob, returning an empty
/// list and counting the failure when the blob is corrupt. An absent blob is not
/// an error (it yields no files). A bad blob must not abort a multi-million-row
/// run, so it is logged and the row is indexed without file paths.
fn decode_files(row: &TorrentForIndex, blob_errors: &mut u64) -> Vec<BlobFile> {
    match row.files() {
        Ok(files) => files,
        Err(error) => {
            *blob_errors += 1;
            warn!(info_hash = %row.info_hash, %error, "skipping undecodable file blob");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run, Args};

    /// End-to-end against a live PostgreSQL: a small capped backfill must open a
    /// fresh index, page through `torrent_contents`, and leave searchable docs.
    /// Ignored by default (there is no DB in CI / `cargo test`); run it against a
    /// populated server with:
    ///
    /// ```sh
    /// BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
    ///   cargo test -p bitmagnet-search --bin backfill -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
    async fn capped_backfill_indexes_documents() {
        let dir =
            std::env::temp_dir().join(format!("bitmagnet-backfill-it-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        run(Args {
            index_path: dir.clone(),
            postgres_dsn: String::new(), // BITMAGNET_POSTGRES_* from the environment
            batch_size: 100,
            limit: Some(500),
            commit_interval: 100,
            after_id: None,
        })
        .await
        .expect("backfill run");

        // Re-open the committed index and confirm it holds documents.
        let index = bitmagnet_search::index::open_or_create(&dir).expect("reopen index");
        let reader = bitmagnet_search::index::reader(&index).expect("build reader");
        reader.reload().unwrap();
        assert!(
            reader.searcher().num_docs() > 0,
            "a capped backfill of a populated database indexes some documents"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
