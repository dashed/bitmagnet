//! `verify` — the L2 DROP-gate parity checker (Job A).
//!
//! Proves, per torrent, that the **blob** (`files_data`, the post-DROP source
//! of per-file truth) agrees with the live **`torrent_files`** table (the
//! retiring source) at the per-`(torrent, extension)` aggregate grain:
//!
//! * **expected** — recomputed here from the decoded blob through the SAME G1
//!   path-derivation the export uses ([`crate::decode`]): `ext -> max(size)`,
//!   empty path-derived extensions skipped;
//! * **actual** — `torrent_files GROUP BY info_hash, extension` with
//!   `extension IS NOT NULL` ([`bitmagnet_db::batch_torrent_files_ext_agg`]) —
//!   the PG generated column is the same G1 expression, so both sides see
//!   valid extensions only (L2-P0 §7 null/empty symmetry).
//!
//! Divergence is **structurally zero** (L2-P0 §8: the blob mirrors
//! `torrent_files` at all three write sites), so ANY mismatch is a bug
//! (G1/decode/build), never an accepted loss — the gate is `mismatched == 0`.
//!
//! Job B (the durable `agg ⟺ blob` invariant) holds by construction here: the
//! export's rollups are computed from the blob by this same code path, and the
//! Go LiveChecker maintains `blob ⟺ torrent_files` continuously — composing
//! the loop the L2-P0 spec describes.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bitmagnet_db::{batch_torrent_files_ext_agg, stream_torrents_with_files, PgPool};
use bitmagnet_model::{file_extension_from_path, BlobFile, InfoHash};

/// Per-torrent expected aggregate: valid extension → max file size.
pub type ExtAgg = BTreeMap<String, u64>;

/// Recompute the expected `(extension -> max_size)` aggregate from a decoded
/// blob. G1: the extension is ALWAYS path-derived (never the stored `e`, which
/// is empty for crawl-path torrents); files with no path-derived extension are
/// skipped — mirroring both `torrent_files.extension` (generated column, NULL
/// then) and the reader's `IS NOT NULL` filter.
pub fn agg_from_files(files: &[BlobFile]) -> ExtAgg {
    let mut agg = ExtAgg::new();
    for f in files {
        if let Some(ext) = file_extension_from_path(&f.path) {
            let max = agg.entry(ext).or_insert(0);
            *max = (*max).max(f.size);
        }
    }
    agg
}

/// Compare one torrent's expected (blob) vs actual (`torrent_files`) aggregate.
/// `None` = exact; `Some(detail)` = a human-readable first-differences summary.
pub fn compare_torrent(expected: &ExtAgg, actual: &ExtAgg) -> Option<String> {
    if expected == actual {
        return None;
    }
    let mut diffs = Vec::new();
    for (ext, exp_max) in expected {
        match actual.get(ext) {
            None => diffs.push(format!("blob-only ext '{ext}' (max {exp_max})")),
            Some(act_max) if act_max != exp_max => {
                diffs.push(format!(
                    "ext '{ext}': blob max {exp_max} != tf max {act_max}"
                ));
            }
            _ => {}
        }
    }
    for ext in actual.keys() {
        if !expected.contains_key(ext) {
            diffs.push(format!("tf-only ext '{ext}' (max {})", actual[ext]));
        }
    }
    diffs.truncate(5); // first differences are enough to debug
    Some(diffs.join("; "))
}

/// Totals for one verify run. The gate: `is_clean()` ⇔ zero mismatches AND
/// zero blob decode errors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyStats {
    /// Torrents compared (including trivially-empty exact matches).
    pub torrents_checked: u64,
    /// Exact per-(ext, max_size) agreement.
    pub exact: u64,
    /// ANY difference — a bug to fix, never an accepted loss.
    pub mismatched: u64,
    /// Blobs that failed to decode (also V3-relevant; counted, not fatal).
    pub decode_errors: u64,
}

impl VerifyStats {
    pub fn is_clean(&self) -> bool {
        self.mismatched == 0 && self.decode_errors == 0
    }
}

/// Options for [`run_verify`].
#[derive(Debug, Clone)]
pub struct VerifyOpts {
    /// Stop after this many torrents (`None` = full corpus).
    pub sample_size: Option<u64>,
    /// Resume/start cursor (exclusive lower bound on `info_hash`).
    pub after: Option<InfoHash>,
    /// Torrents per page / per `= ANY(...)` batch.
    pub batch_size: i64,
    /// Print at most this many mismatch details (all are still counted).
    pub max_mismatch_print: u64,
}

impl Default for VerifyOpts {
    fn default() -> Self {
        Self {
            sample_size: None,
            after: None,
            batch_size: 1_000,
            max_mismatch_print: 20,
        }
    }
}

/// Run Job A: keyset-walk torrents, recompute the expected aggregate from each
/// blob, batch-read the actual `torrent_files` aggregate, compare per torrent.
/// Read-only on both sides.
pub async fn run_verify(pool: &PgPool, opts: &VerifyOpts) -> Result<VerifyStats> {
    let mut stats = VerifyStats::default();
    let mut cursor = opts.after;
    let mut printed = 0u64;

    loop {
        let page = stream_torrents_with_files(pool, cursor.as_ref(), opts.batch_size)
            .await
            .context("streaming verify page")?;
        if page.is_empty() {
            break;
        }

        // Actual side, one batched read for the whole page.
        let keys: Vec<InfoHash> = page.iter().map(|r| r.info_hash).collect();
        let actual_rows = batch_torrent_files_ext_agg(pool, &keys)
            .await
            .context("reading torrent_files aggregate batch")?;
        let mut actual: BTreeMap<InfoHash, ExtAgg> = BTreeMap::new();
        for row in actual_rows {
            // torrent_files.size is bigint and non-negative; widen to u64.
            let max = u64::try_from(row.max_size).unwrap_or(0);
            actual
                .entry(row.info_hash)
                .or_default()
                .insert(row.extension, max);
        }

        for row in &page {
            stats.torrents_checked += 1;
            let expected = match row.files() {
                Ok(files) => agg_from_files(&files),
                Err(e) => {
                    stats.decode_errors += 1;
                    tracing::warn!(info_hash = %row.info_hash, error = %e, "blob decode error");
                    continue;
                }
            };
            let actual_agg = actual.remove(&row.info_hash).unwrap_or_default();
            match compare_torrent(&expected, &actual_agg) {
                None => stats.exact += 1,
                Some(detail) => {
                    stats.mismatched += 1;
                    if printed < opts.max_mismatch_print {
                        printed += 1;
                        tracing::warn!(info_hash = %row.info_hash, %detail, "agg parity MISMATCH");
                    }
                }
            }
        }

        cursor = page.last().map(|r| r.info_hash);
        if stats.torrents_checked.is_multiple_of(100_000) {
            tracing::info!(
                checked = stats.torrents_checked,
                exact = stats.exact,
                mismatched = stats.mismatched,
                decode_errors = stats.decode_errors,
                cursor = %cursor.as_ref().map(ToString::to_string).unwrap_or_default(),
                "verify progress"
            );
        }
        if let Some(n) = opts.sample_size {
            if stats.torrents_checked >= n {
                break;
            }
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, size: u64) -> BlobFile {
        BlobFile {
            index: 0,
            path: path.to_owned(),
            // Intentionally wrong stored `e` — G1 must ignore it.
            extension: "WRONG".to_owned(),
            size,
        }
    }

    #[test]
    fn agg_is_g1_path_derived_max_per_ext() {
        let agg = agg_from_files(&[
            file("Movie/video.MKV", 100), // ext lowercased from PATH
            file("Movie/video2.mkv", 300),
            file("Movie/sub.srt", 5),
            file("Movie/readme", 9), // no extension -> skipped
        ]);
        assert_eq!(agg.len(), 2);
        assert_eq!(agg["mkv"], 300);
        assert_eq!(agg["srt"], 5);
    }

    #[test]
    fn compare_exact_is_none() {
        let a = ExtAgg::from([("mkv".into(), 10u64)]);
        assert!(compare_torrent(&a, &a.clone()).is_none());
        // both empty (no-files torrent on both sides) is exact too
        assert!(compare_torrent(&ExtAgg::new(), &ExtAgg::new()).is_none());
    }

    #[test]
    fn compare_reports_missing_extra_and_size_diffs() {
        let expected = ExtAgg::from([("mkv".into(), 10u64), ("srt".into(), 5u64)]);
        let actual = ExtAgg::from([("mkv".into(), 11u64), ("avi".into(), 7u64)]);
        let detail = compare_torrent(&expected, &actual).unwrap();
        assert!(detail.contains("ext 'mkv': blob max 10 != tf max 11"));
        assert!(detail.contains("blob-only ext 'srt'"));
        assert!(detail.contains("tf-only ext 'avi'"));
    }

    #[test]
    fn stats_gate_requires_zero_mismatch_and_zero_decode_errors() {
        let mut s = VerifyStats {
            torrents_checked: 10,
            exact: 10,
            ..Default::default()
        };
        assert!(s.is_clean());
        s.mismatched = 1;
        assert!(!s.is_clean());
        s.mismatched = 0;
        s.decode_errors = 1;
        assert!(!s.is_clean());
    }
}
