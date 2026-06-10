//! Rollup builders — the `<3 ms` facet/collapse lever (ARCH-C).
//!
//! Both rollups are computed **in Rust during the single blob stream** (no
//! DuckDB needed to build them; the CB campaign found serving rollup *Parquet*
//! beats a native DuckDB table by 100–1000×, so Parquet is also the build
//! target):
//! * [`AggExt`] — per-extension global aggregate. ~47 k distinct extensions, so
//!   it fits in a `HashMap` and is written once at the end.
//! * [`AggTorrentExt`] — per-`(info_hash, extension)`. Rows arrive grouped one
//!   torrent at a time, so each torrent's per-extension aggregate is computed
//!   and streamed out immediately — memory stays bounded regardless of corpus
//!   size. This file mirrors the PG `agg_torrent_ext` DROP-gate table.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringBuilder, UInt64Builder};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::decode::FileRow;
use crate::schema::{agg_ext_schema, agg_torrent_ext_schema};

/// One `(count, total_size, max_size)` triple.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Agg {
    file_count: u64,
    total_size: u64,
    max_size: u64,
}

impl Agg {
    fn add(&mut self, size: u64) {
        self.file_count += 1;
        self.total_size = self.total_size.saturating_add(size);
        self.max_size = self.max_size.max(size);
    }
}

/// Order extensions ascending with the NULL bucket last (shared by both rollups).
fn opt_ext_cmp(a: &Option<String>, b: &Option<String>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(x), Some(y)) => x.cmp(y),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn rollup_props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_dictionary_enabled(true)
        .build())
}

/// Per-extension global rollup (held in memory, written once).
#[derive(Debug, Default)]
pub struct AggExt {
    /// Key `None` = files with no path-derived extension.
    map: HashMap<Option<String>, Agg>,
}

impl AggExt {
    /// Fold one file's `(extension, size)` in.
    pub fn add_file(&mut self, ext: &Option<String>, size: u64) {
        self.map.entry(ext.clone()).or_default().add(size);
    }

    /// Number of distinct extensions (incl. the NULL bucket if present).
    pub fn distinct_extensions(&self) -> usize {
        self.map.len()
    }

    /// Write the rollup to `out` (extension-sorted for stable output).
    pub fn write(&self, out: &Path) -> Result<u64> {
        let file = File::create(out).with_context(|| format!("creating {}", out.display()))?;
        let schema = agg_ext_schema();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(rollup_props()?))?;

        let mut ext = StringBuilder::new();
        let mut file_count = UInt64Builder::new();
        let mut total_size = UInt64Builder::new();
        let mut max_size = UInt64Builder::new();

        let mut keys: Vec<&Option<String>> = self.map.keys().collect();
        keys.sort_by(|a, b| opt_ext_cmp(a, b));
        for k in &keys {
            let a = &self.map[*k];
            match k {
                Some(e) => ext.append_value(e),
                None => ext.append_null(),
            }
            file_count.append_value(a.file_count);
            total_size.append_value(a.total_size);
            max_size.append_value(a.max_size);
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(ext.finish()),
            Arc::new(file_count.finish()),
            Arc::new(total_size.finish()),
            Arc::new(max_size.finish()),
        ];
        let n = keys.len() as u64;
        writer.write(&RecordBatch::try_new(schema, cols)?)?;
        writer.close()?;
        Ok(n)
    }
}

/// Per-`(info_hash, extension)` rollup, streamed one torrent at a time.
pub struct AggTorrentExt {
    writer: ArrowWriter<File>,
    info_hash: StringBuilder,
    extension: StringBuilder,
    file_count: UInt64Builder,
    total_size: UInt64Builder,
    max_size: UInt64Builder,
    pending: usize,
    rows_written: u64,
}

const ROLLUP_BATCH_ROWS: usize = 200_000;

impl AggTorrentExt {
    /// Create the streaming rollup writer at `out`.
    pub fn create(out: &Path) -> Result<Self> {
        let file = File::create(out).with_context(|| format!("creating {}", out.display()))?;
        let writer = ArrowWriter::try_new(file, agg_torrent_ext_schema(), Some(rollup_props()?))?;
        Ok(Self {
            writer,
            info_hash: StringBuilder::new(),
            extension: StringBuilder::new(),
            file_count: UInt64Builder::new(),
            total_size: UInt64Builder::new(),
            max_size: UInt64Builder::new(),
            pending: 0,
            rows_written: 0,
        })
    }

    /// Aggregate one torrent's files by extension and append the resulting rows.
    /// `info_hash_hex` keys all rows; an empty `rows` writes nothing.
    pub fn push_torrent(&mut self, info_hash_hex: &str, rows: &[FileRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut per_ext: HashMap<&Option<String>, Agg> = HashMap::new();
        for r in rows {
            per_ext.entry(&r.extension).or_default().add(r.size);
        }
        // Deterministic per-torrent order (extension, NULL last).
        let mut keys: Vec<&&Option<String>> = per_ext.keys().collect();
        keys.sort_by(|a, b| opt_ext_cmp(**a, **b));
        for k in keys {
            let a = &per_ext[*k];
            self.info_hash.append_value(info_hash_hex);
            match **k {
                Some(ref e) => self.extension.append_value(e),
                None => self.extension.append_null(),
            }
            self.file_count.append_value(a.file_count);
            self.total_size.append_value(a.total_size);
            self.max_size.append_value(a.max_size);
            self.pending += 1;
            self.rows_written += 1;
        }
        if self.pending >= ROLLUP_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.info_hash.finish()),
            Arc::new(self.extension.finish()),
            Arc::new(self.file_count.finish()),
            Arc::new(self.total_size.finish()),
            Arc::new(self.max_size.finish()),
        ];
        let batch = RecordBatch::try_new(agg_torrent_ext_schema(), cols)?;
        self.writer.write(&batch)?;
        self.pending = 0;
        Ok(())
    }

    /// Flush and close, returning rows written.
    pub fn finish(mut self) -> Result<u64> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.rows_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ih: &str, ext: Option<&str>, size: u64) -> FileRow {
        FileRow {
            info_hash_hex: ih.to_owned(),
            file_index: 0,
            path: "p".to_owned(),
            extension: ext.map(str::to_owned),
            size,
        }
    }

    #[test]
    fn agg_ext_accumulates_count_total_max() {
        let mut a = AggExt::default();
        a.add_file(&Some("mkv".to_owned()), 10);
        a.add_file(&Some("mkv".to_owned()), 30);
        a.add_file(&None, 5);
        assert_eq!(a.distinct_extensions(), 2);
        let mkv = a.map[&Some("mkv".to_owned())];
        assert_eq!(mkv.file_count, 2);
        assert_eq!(mkv.total_size, 40);
        assert_eq!(mkv.max_size, 30);
    }

    #[test]
    fn agg_torrent_ext_collapses_per_torrent() {
        let dir = std::env::temp_dir().join(format!("bmp-rollup-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("att.parquet");
        let mut w = AggTorrentExt::create(&p).unwrap();
        // torrent aa: two mkv + one srt => 2 agg rows
        w.push_torrent(
            "aa",
            &[
                row("aa", Some("mkv"), 10),
                row("aa", Some("mkv"), 20),
                row("aa", Some("srt"), 1),
            ],
        )
        .unwrap();
        // empty torrent writes nothing
        w.push_torrent("bb", &[]).unwrap();
        let n = w.finish().unwrap();
        assert_eq!(n, 2);
    }
}
