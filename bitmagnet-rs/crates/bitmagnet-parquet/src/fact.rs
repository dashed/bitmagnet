//! The fact-Parquet writer.
//!
//! Streams [`FileRow`]s into a ZSTD Parquet with `row_group = 1M` and **bloom
//! filters OFF** (the file is sorted by `(extension, size)`, so row-group
//! min/max zone-maps already prune equality + range — a bloom would be dead
//! weight; ARCH-C). Column statistics stay ON (they ARE the zone-maps).
//!
//! ## Sort
//! ARCH-C's latency wins depend on the global `(extension, size)` order. Two
//! strategies, chosen by the caller:
//! * [`SortMode::None`] — write rows in arrival (info_hash-keyset) order. Used
//!   for the *delta* (tiny) and when a downstream DuckDB `COPY … ORDER BY` will
//!   do the global sort.
//! * [`SortMode::InMemory`] — buffer every row, sort by `(extension, size)`,
//!   then write. Correct for the delta, tests, and `--limit` runs; for the full
//!   ~856 M-row base it needs a spilling external sort — see [`crate::export`]
//!   and the build notes (the deploy compaction job runs the sort in DuckDB).

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringBuilder, UInt32Builder, UInt64Builder};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::decode::FileRow;
use crate::schema::fact_schema;

/// Rows per Parquet row group. ARCH-C found 1 M the best pruning granularity.
pub const ROW_GROUP_ROWS: usize = 1_000_000;
/// Rows accumulated in Arrow builders before a `RecordBatch` is flushed to the
/// writer (independent of the row-group size, which the writer enforces).
const BATCH_ROWS: usize = 200_000;

/// How a [`FactWriter`] orders rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    /// Write in arrival order (no buffering).
    None,
    /// Buffer all rows, sort by `(extension, size)`, then write.
    InMemory,
}

/// ZSTD level mirroring the blob serializer / `bench/blob_export`.
fn writer_props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_dictionary_enabled(true)
        .set_max_row_group_size(ROW_GROUP_ROWS)
        // Bloom filters intentionally OFF: sorted file => min/max prunes.
        .set_bloom_filter_enabled(false)
        .build())
}

/// Stable sort key: extension (NULLs last), then size ascending.
fn sort_key(r: &FileRow) -> (bool, &str, u64) {
    match &r.extension {
        Some(e) => (false, e.as_str(), r.size),
        None => (true, "", r.size),
    }
}

/// Sort a row vector by `(extension, size)` in place (NULL extensions last).
pub fn sort_rows(rows: &mut [FileRow]) {
    rows.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
}

/// Writes [`FileRow`]s to a fact Parquet file.
pub struct FactWriter {
    writer: ArrowWriter<File>,
    mode: SortMode,
    /// Buffer for [`SortMode::InMemory`].
    buffer: Vec<FileRow>,
    // Arrow builders for the streaming path.
    info_hash: StringBuilder,
    file_index: UInt32Builder,
    path: StringBuilder,
    extension: StringBuilder,
    size: UInt64Builder,
    pending: usize,
    rows_written: u64,
}

impl FactWriter {
    /// Create a fact writer at `out`.
    pub fn create(out: &Path, mode: SortMode) -> Result<Self> {
        let file = File::create(out).with_context(|| format!("creating {}", out.display()))?;
        let writer = ArrowWriter::try_new(file, fact_schema(), Some(writer_props()?))?;
        Ok(Self {
            writer,
            mode,
            buffer: Vec::new(),
            info_hash: StringBuilder::new(),
            file_index: UInt32Builder::new(),
            path: StringBuilder::new(),
            extension: StringBuilder::new(),
            size: UInt64Builder::new(),
            pending: 0,
            rows_written: 0,
        })
    }

    /// Append one file row.
    pub fn push(&mut self, row: FileRow) -> Result<()> {
        match self.mode {
            SortMode::None => self.append_now(&row)?,
            SortMode::InMemory => self.buffer.push(row),
        }
        Ok(())
    }

    /// Append every file of one torrent.
    pub fn push_rows(&mut self, rows: Vec<FileRow>) -> Result<()> {
        for r in rows {
            self.push(r)?;
        }
        Ok(())
    }

    fn append_now(&mut self, row: &FileRow) -> Result<()> {
        self.info_hash.append_value(&row.info_hash_hex);
        self.file_index.append_value(row.file_index);
        self.path.append_value(&row.path);
        match &row.extension {
            Some(e) => self.extension.append_value(e),
            None => self.extension.append_null(),
        }
        self.size.append_value(row.size);
        self.pending += 1;
        self.rows_written += 1;
        if self.pending >= BATCH_ROWS {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.info_hash.finish()),
            Arc::new(self.file_index.finish()),
            Arc::new(self.path.finish()),
            Arc::new(self.extension.finish()),
            Arc::new(self.size.finish()),
        ];
        let batch = RecordBatch::try_new(fact_schema(), cols)?;
        self.writer.write(&batch)?;
        self.pending = 0;
        Ok(())
    }

    /// Flush buffered rows (sorting first in [`SortMode::InMemory`]) and close
    /// the Parquet file. Returns the total rows written.
    pub fn finish(mut self) -> Result<u64> {
        if self.mode == SortMode::InMemory {
            let mut buffer = std::mem::take(&mut self.buffer);
            sort_rows(&mut buffer);
            for r in &buffer {
                self.append_now(r)?;
            }
        }
        self.flush_batch()?;
        self.writer.close()?;
        Ok(self.rows_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::col;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn row(ih: &str, idx: u32, ext: Option<&str>, size: u64) -> FileRow {
        FileRow {
            info_hash_hex: ih.to_owned(),
            file_index: idx,
            path: format!("{ih}/{idx}"),
            extension: ext.map(str::to_owned),
            size,
        }
    }

    fn read_back(path: &Path) -> Vec<(Option<String>, u64)> {
        use arrow::array::{Array, StringArray, UInt64Array};
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let ext = batch
                .column_by_name(col::EXTENSION)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let size = batch
                .column_by_name(col::SIZE)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let e = if ext.is_null(i) {
                    None
                } else {
                    Some(ext.value(i).to_owned())
                };
                out.push((e, size.value(i)));
            }
        }
        out
    }

    #[test]
    fn streaming_preserves_order_and_roundtrips() {
        let dir = tempdir();
        let p = dir.join("fact.parquet");
        let mut w = FactWriter::create(&p, SortMode::None).unwrap();
        w.push(row("aa", 0, Some("mkv"), 5)).unwrap();
        w.push(row("aa", 1, None, 1)).unwrap();
        let n = w.finish().unwrap();
        assert_eq!(n, 2);
        assert_eq!(
            read_back(&p),
            vec![(Some("mkv".to_owned()), 5), (None, 1)]
        );
    }

    #[test]
    fn in_memory_sorts_by_ext_then_size_nulls_last() {
        let dir = tempdir();
        let p = dir.join("fact.parquet");
        let mut w = FactWriter::create(&p, SortMode::InMemory).unwrap();
        w.push(row("aa", 0, None, 99)).unwrap();
        w.push(row("bb", 0, Some("mkv"), 9)).unwrap();
        w.push(row("cc", 0, Some("mkv"), 2)).unwrap();
        w.push(row("dd", 0, Some("avi"), 7)).unwrap();
        w.finish().unwrap();
        assert_eq!(
            read_back(&p),
            vec![
                (Some("avi".to_owned()), 7),
                (Some("mkv".to_owned()), 2),
                (Some("mkv".to_owned()), 9),
                (None, 99), // NULL extension sorts last
            ]
        );
    }

    /// Minimal unique temp dir without pulling in the `tempfile` crate.
    fn tempdir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "bmp-fact-{}-{:p}",
            std::process::id(),
            &p as *const _
        );
        p.push(uniq);
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
