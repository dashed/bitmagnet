//! The `--follow` PG-tail watermark mode (the required fork addition, PS-T4 §3.2
//! source (b)) and the shared DB-row → path-bag-doc glue the backfill reuses.
//!
//! The serving pod is the permanent sole writer (PS-T4 §3): a background task
//! polls Postgres on a `(updated_at_micros, info_hash)` watermark, decodes each
//! changed torrent's blob, and `delete_term(info_hash)` + re-adds its path-bag
//! doc, committing at a cadence. CB-validated params (commit ~13–17 ms sustains
//! 50 t/s with headroom; single writer thread + ≥2 GiB arena; default
//! `LogMergePolicy`; sub-ms fresh-lag) are realised by [`super::index::path_writer`]
//! + the per-batch commit here.
//!
//! ## Watermark persistence — a sidecar file, not the index meta
//!
//! The cursor lives in a small text file next to the index
//! ([`watermark_path`]: `<index_dir>/../.pathsearch-watermark`), written
//! atomically (temp + rename). Rationale: Tantivy's index meta is owned by the
//! segment lifecycle (merges/commits rewrite it) and is not an app-cursor store;
//! coupling our watermark to it risks loss on a merge and fights Tantivy's
//! atomicity. A standalone rename-on-write file is crash-safe, independent of the
//! segment lifecycle, and trivially inspectable. A missing file = start from
//! epoch `(0, [])` (the follow loop then sweeps the whole corpus — which also
//! makes it a self-healing gap-closer after a backfill, PS-T4 §6).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bitmagnet_db::{stream_torrents_for_pathsearch_since, PgPool, TorrentForPathIndex};
use tantivy::{IndexReader, IndexWriter};
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::indexer::{upsert, PathDoc};
use super::schema::PathFields;

/// The watermark file name, placed beside the index directory (its parent).
const WATERMARK_FILE: &str = ".pathsearch-watermark";

/// The follow-loop cursor: the largest `(updated_at_micros, info_hash)` indexed.
#[derive(Debug, Clone, Default)]
pub struct Watermark {
    /// `updated_at` of the last indexed torrent, in Unix microseconds.
    pub updated_at_micros: i64,
    /// Raw 20-byte info hash of the last indexed torrent (the keyset tiebreak).
    pub info_hash: Vec<u8>,
}

impl Watermark {
    /// Serialize as `"<micros>\n<hex>\n"` (hex of the info hash; empty on epoch).
    fn serialize(&self) -> String {
        let mut hex = String::with_capacity(self.info_hash.len() * 2);
        for b in &self.info_hash {
            use std::fmt::Write as _;
            let _ = write!(hex, "{b:02x}");
        }
        format!("{}\n{}\n", self.updated_at_micros, hex)
    }

    /// Parse the two-line form; any malformed content yields the epoch default
    /// (safe: the loop just re-sweeps from the start).
    fn parse(text: &str) -> Self {
        let mut lines = text.lines();
        let micros = lines
            .next()
            .and_then(|l| l.trim().parse::<i64>().ok())
            .unwrap_or(0);
        let info_hash = lines
            .next()
            .map(str::trim)
            .filter(|h| !h.is_empty() && h.len() % 2 == 0)
            .and_then(|h| {
                (0..h.len() / 2)
                    .map(|i| u8::from_str_radix(&h[i * 2..i * 2 + 2], 16).ok())
                    .collect::<Option<Vec<u8>>>()
            })
            .unwrap_or_default();
        Self {
            updated_at_micros: micros,
            info_hash,
        }
    }
}

/// Resolve the watermark file path for a given index directory: a sibling of the
/// index dir (its parent), so it survives an index wipe/rebuild only if intended
/// — callers that rebuild from scratch should also remove it.
#[must_use]
pub fn watermark_path(index_path: &Path) -> PathBuf {
    let parent = index_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(WATERMARK_FILE)
}

/// Load the persisted watermark, or the epoch default when absent/unreadable.
#[must_use]
pub fn load_watermark(index_path: &Path) -> Watermark {
    let path = watermark_path(index_path);
    match std::fs::read_to_string(&path) {
        Ok(text) => Watermark::parse(&text),
        Err(_) => Watermark::default(),
    }
}

/// Persist the watermark atomically (temp file + rename).
///
/// # Errors
/// Returns an I/O error if the temp write or rename fails.
pub fn save_watermark(index_path: &Path, wm: &Watermark) -> anyhow::Result<()> {
    let path = watermark_path(index_path);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, wm.serialize())
        .with_context(|| format!("writing watermark temp {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("renaming watermark into place {}", path.display()))?;
    Ok(())
}

/// Index one source row into the path-bag (decode blob → paths → upsert). Shared
/// by the follow loop and the backfill bin so they build identical docs. A bad
/// blob is logged and the torrent is indexed with the name fallback (resilient).
///
/// # Errors
/// Returns a [`tantivy::TantivyError`] only if the Tantivy add fails.
pub fn index_torrent_row(
    writer: &IndexWriter,
    fields: &PathFields,
    row: &TorrentForPathIndex,
) -> tantivy::Result<()> {
    let paths: Vec<String> = match row.files() {
        Ok(files) => files
            .into_iter()
            .filter(|f| !f.path.is_empty())
            .map(|f| f.path)
            .collect(),
        Err(error) => {
            warn!(info_hash = %row.info_hash, %error, "pathsearch: undecodable blob, indexing name only");
            Vec::new()
        }
    };
    let doc = PathDoc {
        info_hash: row.info_hash.as_slice(),
        file_paths: &paths,
        seeders: u64::try_from(row.seeders).unwrap_or(0),
        size: u64::try_from(row.size).unwrap_or(0),
        files_count: row.files_count.and_then(|c| u64::try_from(c).ok()).unwrap_or(0),
        name_fallback: &row.name,
    };
    upsert(writer, fields, &doc)
}

/// Tunables for [`run_follow_loop`].
#[derive(Debug, Clone)]
pub struct FollowConfig {
    /// Index directory (used to locate the watermark sibling file).
    pub index_path: PathBuf,
    /// Rows per keyset page (also the commit cadence — one commit per page).
    pub batch_size: i64,
    /// Sleep between polls when the tail is caught up (no new rows).
    pub poll_interval: Duration,
}

/// Run the PG-tail follow loop until cancelled. Shares the SOLE writer with the
/// serving pod (`Arc<Mutex<IndexWriter>>`), so there is never a second writer.
///
/// Each iteration: read a page after the watermark; if empty, sleep and retry;
/// otherwise upsert every row, commit once, reload the reader, and persist the
/// advanced watermark. The watermark advances monotonically over
/// `(updated_at_micros, info_hash)`.
///
/// # Errors
/// Propagates a fatal DB or Tantivy error. Per-row blob errors are non-fatal
/// (logged, name-only indexed). Designed to be `tokio::spawn`-ed and to run for
/// the process lifetime.
pub async fn run_follow_loop(
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
    fields: PathFields,
    pool: PgPool,
    config: FollowConfig,
) -> anyhow::Result<()> {
    let mut wm = load_watermark(&config.index_path);
    info!(
        start_micros = wm.updated_at_micros,
        batch_size = config.batch_size,
        poll_ms = config.poll_interval.as_millis(),
        "pathsearch follow loop starting"
    );

    loop {
        let page = stream_torrents_for_pathsearch_since(
            &pool,
            wm.updated_at_micros,
            &wm.info_hash,
            config.batch_size,
        )
        .await
        .context("follow: reading torrents page")?;

        if page.is_empty() {
            tokio::time::sleep(config.poll_interval).await;
            continue;
        }

        {
            let mut w = writer.lock().await;
            for row in &page {
                index_torrent_row(&w, &fields, row).context("follow: indexing row")?;
                wm = Watermark {
                    updated_at_micros: row.updated_at_micros,
                    info_hash: row.info_hash.as_slice().to_vec(),
                };
            }
            w.commit().context("follow: committing batch")?;
        }
        reader.reload().context("follow: reloading reader")?;
        save_watermark(&config.index_path, &wm).context("follow: persisting watermark")?;
        info!(
            indexed = page.len(),
            watermark_micros = wm.updated_at_micros,
            "pathsearch follow: batch committed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{load_watermark, save_watermark, watermark_path, Watermark};

    #[test]
    fn watermark_round_trips() {
        let wm = Watermark {
            updated_at_micros: 1_700_000_123_456_789,
            info_hash: vec![0xAB, 0x01, 0xff, 0x00],
        };
        let text = wm.serialize();
        let back = Watermark::parse(&text);
        assert_eq!(back.updated_at_micros, wm.updated_at_micros);
        assert_eq!(back.info_hash, wm.info_hash);
    }

    #[test]
    fn malformed_watermark_is_epoch() {
        let wm = Watermark::parse("not-a-number\nzzzz\n");
        assert_eq!(wm.updated_at_micros, 0);
        assert!(wm.info_hash.is_empty());
        // Odd-length hex is rejected → empty.
        assert!(Watermark::parse("5\nabc\n").info_hash.is_empty());
    }

    #[test]
    fn save_then_load_via_sidecar_file() {
        let dir = std::env::temp_dir().join(format!("pathwm-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let index_path = dir.join("search-files");
        std::fs::create_dir_all(&index_path).unwrap();

        // Default when absent.
        assert_eq!(load_watermark(&index_path).updated_at_micros, 0);

        let wm = Watermark {
            updated_at_micros: 42,
            info_hash: vec![0x12; 20],
        };
        save_watermark(&index_path, &wm).unwrap();
        // Sidecar is the PARENT of the index dir, not inside it.
        assert_eq!(watermark_path(&index_path), dir.join(".pathsearch-watermark"));
        let back = load_watermark(&index_path);
        assert_eq!(back.updated_at_micros, 42);
        assert_eq!(back.info_hash, vec![0x12; 20]);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
