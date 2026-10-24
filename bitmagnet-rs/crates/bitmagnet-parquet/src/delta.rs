//! Delta tombstones — the FB-B1a supersession key set.
//!
//! A delta carries two things: a small **fact** Parquet of the *currently
//! existing* changed torrents' files, and a **tombstone** Parquet listing every
//! `info_hash` that changed — **including deletes**. The read-time view
//! anti-joins the base against the tombstone set, then UNIONs the delta fact:
//!
//! * re-crawl  → tombstone present  + delta fact rows  ⇒ base hidden, delta wins
//! * **delete**→ tombstone present  + NO delta fact rows ⇒ base hidden, nothing
//!   replaces it ⇒ the torrent vanishes (the whole point of carrying deletes in
//!   the tombstone rather than the fact).
//!
//! Supersession is **TORRENT-granular** (a re-crawl replaces a torrent's *whole*
//! fileset). EXP-B proved a per-row `row_number() PARTITION BY info_hash = 1` is
//! WRONG (keeps one file per torrent) and window-max is 80× slower — the
//! tombstone anti-join is the correct, fast shape.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Schema of a tombstone file: one `info_hash` column (40-char hex).
pub fn tombstone_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "info_hash",
        DataType::Utf8,
        false,
    )]))
}

/// Writes the tombstone key set. De-duplicates within the run so a hash that is
/// both re-crawled and (later) appears in the deleted list is written once.
pub struct TombstoneWriter {
    writer: ArrowWriter<File>,
    builder: StringBuilder,
    seen: std::collections::HashSet<String>,
    pending: usize,
    rows_written: u64,
}

const TOMBSTONE_BATCH_ROWS: usize = 100_000;

impl TombstoneWriter {
    pub fn create(out: &Path) -> Result<Self> {
        let file = File::create(out).with_context(|| format!("creating {}", out.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .build();
        let writer = ArrowWriter::try_new(file, tombstone_schema(), Some(props))?;
        Ok(Self {
            writer,
            builder: StringBuilder::new(),
            seen: std::collections::HashSet::new(),
            pending: 0,
            rows_written: 0,
        })
    }

    /// Record a changed/deleted info hash (idempotent within the run).
    pub fn add(&mut self, info_hash_hex: &str) -> Result<()> {
        if !self.seen.insert(info_hash_hex.to_owned()) {
            return Ok(());
        }
        self.builder.append_value(info_hash_hex);
        self.pending += 1;
        self.rows_written += 1;
        if self.pending >= TOMBSTONE_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = vec![Arc::new(self.builder.finish())];
        self.writer
            .write(&RecordBatch::try_new(tombstone_schema(), cols)?)?;
        self.pending = 0;
        Ok(())
    }

    /// Number of distinct tombstoned hashes so far.
    pub fn len(&self) -> u64 {
        self.rows_written
    }

    pub fn is_empty(&self) -> bool {
        self.rows_written == 0
    }

    pub fn finish(mut self) -> Result<u64> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.rows_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_dedups_and_counts() {
        let dir = std::env::temp_dir().join(format!("bmp-tomb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("tombstones.parquet");
        let mut w = TombstoneWriter::create(&p).unwrap();
        w.add("aa").unwrap();
        w.add("bb").unwrap();
        w.add("aa").unwrap(); // dup (re-crawled then also deleted) -> once
        assert_eq!(w.len(), 2);
        let n = w.finish().unwrap();
        assert_eq!(n, 2);
        assert!(p.exists());
    }
}
