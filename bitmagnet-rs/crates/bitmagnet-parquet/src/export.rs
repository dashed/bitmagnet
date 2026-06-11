//! Export orchestration — fans one blob stream into the fact + both rollups
//! (+ the delta tombstone), and drives the base / delta / compaction jobs.
//!
//! The [`Sinks`] fan-out is pure and unit-testable (feed it decoded torrents,
//! read the Parquet back). The `run_*` async functions wire it to the
//! `bitmagnet-db` keyset readers — the production path (needs a live DB).

use std::path::Path;

use anyhow::{Context, Result};
use bitmagnet_db::{stream_changed_torrents, stream_torrents_with_files, PgPool};
use bitmagnet_model::{BlobError, BlobFile};

use crate::decode::{rows_from_files, DecodeStats};
use crate::delta::TombstoneWriter;
use crate::fact::{FactWriter, SortMode};
use crate::generation::{artifact, Kind, Layout};
use crate::rollup::{AggExt, AggTorrentExt};

/// Totals for one build (base or delta).
#[derive(Debug, Clone, Default)]
pub struct BuildStats {
    pub decode: DecodeStats,
    pub fact_rows: u64,
    pub agg_ext_rows: u64,
    pub agg_torrent_ext_rows: u64,
    pub tombstones: u64,
}

impl BuildStats {
    /// V3: the first production base export must report **zero** decode errors.
    pub fn is_clean(&self) -> bool {
        self.decode.decode_errors == 0
    }
}

/// The fan-out: one torrent in, fact + both rollups (+ tombstone) updated.
pub struct Sinks {
    fact: FactWriter,
    agg_ext: AggExt,
    agg_torrent_ext: AggTorrentExt,
    tombstone: Option<TombstoneWriter>,
    stats: DecodeStats,
}

impl Sinks {
    /// Create the artifact writers inside generation dir `dir`. `with_tombstone`
    /// adds the delta supersession key set.
    pub fn create(dir: &Path, sort: SortMode, with_tombstone: bool) -> Result<Self> {
        let fact = FactWriter::create(&dir.join(artifact::FACT), sort)?;
        let agg_torrent_ext = AggTorrentExt::create(&dir.join(artifact::AGG_TORRENT_EXT))?;
        let tombstone = if with_tombstone {
            Some(TombstoneWriter::create(&dir.join(artifact::TOMBSTONES))?)
        } else {
            None
        };
        Ok(Self {
            fact,
            agg_ext: AggExt::default(),
            agg_torrent_ext,
            tombstone,
            stats: DecodeStats::default(),
        })
    }

    /// Feed one torrent. `decoded` is the blob-decode outcome; on a delta build
    /// the hash is tombstoned regardless of decode result (a changed torrent is
    /// always superseded — its rows, if any, come from the delta fact).
    pub fn push_torrent(
        &mut self,
        info_hash_hex: &str,
        decoded: Result<Vec<BlobFile>, BlobError>,
    ) -> Result<()> {
        if let Some(t) = self.tombstone.as_mut() {
            t.add(info_hash_hex)?;
        }
        match decoded {
            Ok(files) => {
                let rows = rows_from_files(info_hash_hex, &files);
                self.stats.torrents_ok += 1;
                self.stats.file_rows += rows.len() as u64;
                // Rollups are the DEFAULT-served aggregates — padding files
                // (alignment filler, 3.74% of the corpus) are excluded here so
                // facets/collapse/counts are clean without losing the rows:
                // the fact keeps them, flagged, behind `include_padding`.
                let content: Vec<_> = rows.iter().filter(|r| !r.is_padding).cloned().collect();
                self.stats.padding_rows += (rows.len() - content.len()) as u64;
                for r in &content {
                    self.agg_ext.add_file(&r.extension, r.size);
                }
                self.agg_torrent_ext.push_torrent(info_hash_hex, &content)?;
                self.fact.push_rows(rows)?;
            }
            Err(_) => self.stats.decode_errors += 1,
        }
        Ok(())
    }

    /// Tombstone a deleted torrent (delta builds only): key set entry, no fact
    /// rows — the anti-join makes it vanish.
    pub fn push_deleted(&mut self, info_hash_hex: &str) -> Result<()> {
        match self.tombstone.as_mut() {
            Some(t) => t.add(info_hash_hex),
            None => anyhow::bail!("push_deleted called on a non-delta build"),
        }
    }

    /// Close all writers, returning the totals. Writes `agg_ext.parquet`.
    pub fn finish(self, dir: &Path) -> Result<BuildStats> {
        let fact_rows = self.fact.finish()?;
        let agg_ext_rows = self.agg_ext.write(&dir.join(artifact::AGG_EXT))?;
        let agg_torrent_ext_rows = self.agg_torrent_ext.finish()?;
        let tombstones = match self.tombstone {
            Some(t) => t.finish()?,
            None => 0,
        };
        Ok(BuildStats {
            decode: self.stats,
            fact_rows,
            agg_ext_rows,
            agg_torrent_ext_rows,
            tombstones,
        })
    }
}

/// Full base export: stream every torrent → sorted fact + rollups → atomic
/// base swap. V3: the returned [`BuildStats::is_clean`] must hold on the first
/// production run.
pub async fn run_base(
    pool: &PgPool,
    layout: &Layout,
    version: &str,
    sort: SortMode,
    page_size: i64,
) -> Result<BuildStats> {
    layout.ensure_dirs()?;
    let dir = layout.new_version_dir(Kind::Base, version)?;
    let mut sinks = Sinks::create(&dir, sort, false)?;

    let mut cursor = None;
    loop {
        let page = stream_torrents_with_files(pool, cursor.as_ref(), page_size)
            .await
            .context("streaming base page")?;
        if page.is_empty() {
            break;
        }
        for row in &page {
            sinks.push_torrent(&row.info_hash.to_string(), row.files())?;
        }
        cursor = page.last().map(|r| r.info_hash.clone());
    }

    let stats = sinks.finish(&dir)?;
    layout.publish(Kind::Base, &dir)?;
    Ok(stats)
}

/// Commit-visibility lag for the delta carve window (seconds). The carve reads
/// `(watermark, now − CARVE_LAG_SECS]` and persists the window END as the new
/// watermark: rows whose transaction commits late (after the carve ran) still
/// have `updated_at > window_end`, so the NEXT run picks them up — nothing can
/// fall between runs as long as writer transactions are shorter than the lag.
pub const CARVE_LAG_SECS: i64 = 30;

/// Minute delta: carve torrents changed in `(layout.read_watermark(),
/// new_watermark]`, plus the supplied `deleted` hashes (from the deletion audit
/// source), into a fresh delta generation; then advance the watermark to
/// `new_watermark` and swap.
///
/// `new_watermark` is BOTH the carve window end and the persisted cursor — pass
/// a lagged now (`now_epoch() − CARVE_LAG_SECS`), never a raw `now`.
///
/// `deleted` is the set of hard-deleted info_hashes since the last run — see the
/// build notes for the audit-source wiring (a delete trigger / audit table).
pub async fn run_delta(
    pool: &PgPool,
    layout: &Layout,
    version: &str,
    new_watermark: i64,
    deleted: &[String],
    page_size: i64,
) -> Result<BuildStats> {
    layout.ensure_dirs()?;
    let since = layout.read_watermark();
    let dir = layout.new_version_dir(Kind::Delta, version)?;
    // Delta is small → in-memory sort keeps it (extension, size)-ordered too.
    let mut sinks = Sinks::create(&dir, SortMode::InMemory, true)?;

    let mut cursor = None;
    loop {
        let page = stream_changed_torrents(pool, since, new_watermark, cursor.as_ref(), page_size)
            .await
            .context("streaming delta page")?;
        if page.is_empty() {
            break;
        }
        for row in &page {
            sinks.push_torrent(&row.info_hash.to_string(), row.files())?;
        }
        cursor = page.last().map(|r| r.info_hash.clone());
    }
    for ih in deleted {
        sinks.push_deleted(ih)?;
    }

    let stats = sinks.finish(&dir)?;
    layout.publish(Kind::Delta, &dir)?;
    // Advance the watermark only after a successful publish.
    layout.write_watermark(new_watermark)?;
    Ok(stats)
}

/// Compaction: full base rebuild, then reset the delta to an EMPTY generation
/// (everything up to `new_watermark` is now in the base). Returns the base
/// build stats.
pub async fn run_compaction(
    pool: &PgPool,
    layout: &Layout,
    version: &str,
    new_watermark: i64,
    sort: SortMode,
    page_size: i64,
) -> Result<BuildStats> {
    let stats = run_base(pool, layout, version, sort, page_size).await?;
    publish_empty_delta(layout, version)?;
    layout.write_watermark(new_watermark)?;
    Ok(stats)
}

/// Publish an empty delta generation (used after compaction).
pub fn publish_empty_delta(layout: &Layout, version: &str) -> Result<()> {
    layout.ensure_dirs()?;
    let dir = layout.new_version_dir(Kind::Delta, &format!("{version}-empty"))?;
    let sinks = Sinks::create(&dir, SortMode::None, true)?;
    sinks.finish(&dir)?;
    layout.publish(Kind::Delta, &dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitmagnet_model::serialize_files;

    fn files(exts: &[(&str, u64)]) -> Vec<BlobFile> {
        exts.iter()
            .enumerate()
            .map(|(i, (path, size))| BlobFile {
                index: i as u32,
                path: (*path).to_owned(),
                extension: "ignored".to_owned(),
                size: *size,
            })
            .collect()
    }

    fn dir(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("bmp-export-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn base_sinks_produce_all_artifacts_and_count() {
        let d = dir("base");
        let mut s = Sinks::create(&d, SortMode::InMemory, false).unwrap();
        s.push_torrent("aa", Ok(files(&[("a.mkv", 10), ("b.mkv", 20), ("c.srt", 1)])))
            .unwrap();
        s.push_torrent("bb", Ok(files(&[("d.avi", 5)]))).unwrap();
        let stats = s.finish(&d).unwrap();
        assert_eq!(stats.decode.torrents_ok, 2);
        assert_eq!(stats.fact_rows, 4);
        // distinct extensions: mkv, srt, avi
        assert_eq!(stats.agg_ext_rows, 3);
        // per-(torrent,ext): aa->{mkv,srt}=2, bb->{avi}=1 => 3
        assert_eq!(stats.agg_torrent_ext_rows, 3);
        assert_eq!(stats.tombstones, 0);
        assert!(stats.is_clean());
        assert!(d.join(artifact::FACT).exists());
        assert!(d.join(artifact::AGG_EXT).exists());
        assert!(d.join(artifact::AGG_TORRENT_EXT).exists());
        assert!(!d.join(artifact::TOMBSTONES).exists());
    }

    #[test]
    fn decode_error_is_counted_not_fatal() {
        let d = dir("err");
        let mut s = Sinks::create(&d, SortMode::None, false).unwrap();
        s.push_torrent("aa", Ok(files(&[("a.mkv", 1)]))).unwrap();
        s.push_torrent(
            "bb",
            bitmagnet_model::deserialize_files(b"garbage").map_err(|e| e),
        )
        .unwrap();
        let stats = s.finish(&d).unwrap();
        assert_eq!(stats.decode.torrents_ok, 1);
        assert_eq!(stats.decode.decode_errors, 1);
        assert!(!stats.is_clean()); // V3 would FAIL — surfaced, not hidden
    }

    #[test]
    fn delta_tombstones_changed_and_deleted() {
        let d = dir("delta");
        let mut s = Sinks::create(&d, SortMode::InMemory, true).unwrap();
        // changed torrent: tombstone + fact rows
        s.push_torrent("aa", Ok(files(&[("a.mkv", 10)]))).unwrap();
        // deleted torrent: tombstone only, no fact rows
        s.push_deleted("bb").unwrap();
        let stats = s.finish(&d).unwrap();
        assert_eq!(stats.fact_rows, 1);
        assert_eq!(stats.tombstones, 2);
        assert!(d.join(artifact::TOMBSTONES).exists());
    }

    #[test]
    fn blob_round_trip_through_sinks() {
        // Tie the real blob serializer to the export path.
        let blob = serialize_files(&files(&[("x.mkv", 7)])).unwrap();
        let decoded = bitmagnet_model::deserialize_files(&blob);
        let d = dir("blob");
        let mut s = Sinks::create(&d, SortMode::None, false).unwrap();
        s.push_torrent("aa", decoded).unwrap();
        let stats = s.finish(&d).unwrap();
        assert_eq!(stats.fact_rows, 1);
    }
}
