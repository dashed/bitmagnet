//! blob_export — THROWAWAY bench tool (team bitmagnet-bench, RUN-0a).
//!
//! Decodes `torrents.files_data` (zstd(msgpack[{i,p,e,s}]), see
//! `bitmagnet-model/src/blob.rs`) into a columnar Parquet of one row per file:
//! `(info_hash, file_index, path, extension, size)`. Feeds the DuckDB-on-blobs
//! latency benchmark (`docs/dev/duckdb-on-blobs-benchmark-spec.md`).
//!
//! 🚨 G1: `extension` is derived from the PATH via
//! `bitmagnet_model::file_extension_from_path`, NOT the blob's `e` field (empty
//! for crawl-path torrents). This matches the live PG semantics and is the
//! correct, uniform basis for `WHERE extension='mkv'`.
//!
//! Two input modes:
//!   * `--dsn <postgres-url>`  — stream the real corpus via the fork's
//!     `stream_torrents_with_files` keyset reader (used on the HEL1 restore).
//!   * `--from-hex <psv>`      — offline smoke: read `info_hash|files_count|hex`
//!     lines and run the identical decode→ext→Parquet pipeline with NO database.
//!
//! `--slim` drops the `path` column (the small analytics Parquet, ~3–5 GB);
//! omit it for the full Parquet (adds `path`, ~18–25 GB, for path-FTS).

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringBuilder, UInt32Builder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bitmagnet_db::{stream_torrents_with_files, PgPool};
use bitmagnet_model::{deserialize_files, file_extension_from_path, BlobFile, InfoHash};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// CLI options (hand-parsed to avoid a clap dependency in throwaway tooling).
struct Opts {
    dsn: Option<String>,
    from_hex: Option<String>,
    out: String,
    slim: bool,
    page_size: i64,
    limit: Option<u64>,
    /// Rows buffered before a Parquet RecordBatch is flushed.
    batch_rows: usize,
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  \
         blob_export --dsn <postgres-url> --out <file.parquet> [--slim] [--page-size N] [--limit T]\n  \
         blob_export --from-hex <sample.psv> --out <file.parquet> [--slim]\n\n\
         --slim       drop the `path` column (small analytics Parquet)\n\
         --page-size  torrents per keyset page (default 20000)\n\
         --limit      stop after T torrents (smoke/partial runs)\n"
    );
    std::process::exit(2);
}

fn parse_opts() -> Opts {
    let mut o = Opts {
        dsn: None,
        from_hex: None,
        out: String::new(),
        slim: false,
        page_size: 20_000,
        limit: None,
        batch_rows: 1_000_000,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dsn" => o.dsn = Some(args.next().unwrap_or_else(|| usage())),
            "--from-hex" => o.from_hex = Some(args.next().unwrap_or_else(|| usage())),
            "--out" => o.out = args.next().unwrap_or_else(|| usage()),
            "--slim" => o.slim = true,
            "--page-size" => {
                o.page_size = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "--limit" => {
                o.limit = Some(
                    args.next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                )
            }
            "--batch-rows" => {
                o.batch_rows = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown arg: {other}");
                usage();
            }
        }
    }
    if o.out.is_empty() || (o.dsn.is_none() == o.from_hex.is_none()) {
        usage();
    }
    o
}

/// Arrow schema for the export. `path` is present only in the non-slim file.
fn schema(slim: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("info_hash", DataType::Utf8, false),
        Field::new("file_index", DataType::UInt32, false),
    ];
    if !slim {
        fields.push(Field::new("path", DataType::Utf8, false));
    }
    fields.push(Field::new("extension", DataType::Utf8, true)); // null = no ext
    fields.push(Field::new("size", DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

/// Accumulates rows and flushes RecordBatches into a Parquet file.
struct ParquetSink {
    writer: ArrowWriter<std::fs::File>,
    schema: Arc<Schema>,
    slim: bool,
    batch_rows: usize,
    info_hash: StringBuilder,
    file_index: UInt32Builder,
    path: StringBuilder,
    extension: StringBuilder,
    size: UInt64Builder,
    buffered: usize,
    total_rows: u64,
}

impl ParquetSink {
    fn create(out: &str, slim: bool, batch_rows: usize) -> Result<Self> {
        let schema = schema(slim);
        let file = std::fs::File::create(out).with_context(|| format!("creating {out}"))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_dictionary_enabled(true)
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        Ok(Self {
            writer,
            schema,
            slim,
            batch_rows,
            info_hash: StringBuilder::new(),
            file_index: UInt32Builder::new(),
            path: StringBuilder::new(),
            extension: StringBuilder::new(),
            size: UInt64Builder::new(),
            buffered: 0,
            total_rows: 0,
        })
    }

    /// Append every file of one torrent. `ih_hex` is the 40-char hex info hash.
    fn push_torrent(&mut self, ih_hex: &str, files: &[BlobFile]) -> Result<()> {
        for f in files {
            self.info_hash.append_value(ih_hex);
            self.file_index.append_value(f.index);
            if !self.slim {
                self.path.append_value(&f.path);
            }
            // G1: ALWAYS path-derive; ignore the blob `e`.
            match file_extension_from_path(&f.path) {
                Some(ext) => self.extension.append_value(ext),
                None => self.extension.append_null(),
            }
            self.size.append_value(f.size);
            self.buffered += 1;
            self.total_rows += 1;
        }
        if self.buffered >= self.batch_rows {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffered == 0 {
            return Ok(());
        }
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(self.info_hash.finish()),
            Arc::new(self.file_index.finish()),
        ];
        if !self.slim {
            cols.push(Arc::new(self.path.finish()));
        }
        cols.push(Arc::new(self.extension.finish()));
        cols.push(Arc::new(self.size.finish()));
        let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
        self.writer.write(&batch)?;
        self.buffered = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        self.flush()?;
        self.writer.close()?;
        Ok(self.total_rows)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = parse_opts();
    let mut sink = ParquetSink::create(&opts.out, opts.slim, opts.batch_rows)?;
    let started = Instant::now();
    let mut torrents: u64 = 0;
    let mut blob_errors: u64 = 0;

    if let Some(path) = &opts.from_hex {
        run_from_hex(path, &mut sink, &mut torrents, &mut blob_errors)?;
    } else {
        let dsn = opts.dsn.as_ref().expect("validated");
        run_from_db(dsn, &opts, &mut sink, &mut torrents, &mut blob_errors).await?;
    }

    let rows = sink.finish()?;
    let secs = started.elapsed().as_secs_f64();
    eprintln!(
        "DONE: {torrents} torrents, {rows} file-rows, {blob_errors} blob errors -> {}\n\
         {:.1}s  ({:.0} torrents/s, {:.2} M files/s)",
        opts.out,
        secs,
        torrents as f64 / secs,
        rows as f64 / secs / 1e6,
    );
    Ok(())
}

/// Offline smoke: `info_hash_hex|files_count|blob_hex` lines, no DB.
fn run_from_hex(
    path: &str,
    sink: &mut ParquetSink,
    torrents: &mut u64,
    blob_errors: &mut u64,
) -> Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?;
    for line in text.lines() {
        let mut parts = line.splitn(3, '|');
        let ih_hex = parts.next().unwrap_or_default();
        let _count = parts.next().unwrap_or_default();
        let blob_hex = match parts.next() {
            Some(h) if !h.is_empty() => h,
            _ => continue,
        };
        // Validate the hash is real 20-byte hex (mirrors the DB path).
        let _ih: InfoHash = ih_hex.parse().with_context(|| format!("bad info_hash {ih_hex}"))?;
        let blob = hex::decode(blob_hex).context("hex-decoding blob")?;
        match deserialize_files(&blob) {
            Ok(files) => sink.push_torrent(ih_hex, &files)?,
            Err(e) => {
                *blob_errors += 1;
                eprintln!("blob decode error for {ih_hex}: {e}");
            }
        }
        *torrents += 1;
    }
    Ok(())
}

/// Stream the real corpus via the fork's keyset reader.
async fn run_from_db(
    dsn: &str,
    opts: &Opts,
    sink: &mut ParquetSink,
    torrents: &mut u64,
    blob_errors: &mut u64,
) -> Result<()> {
    let pool = PgPool::connect(dsn).await.context("connecting to postgres")?;
    let mut cursor: Option<InfoHash> = None;
    loop {
        let page = stream_torrents_with_files(&pool, cursor.as_ref(), opts.page_size)
            .await
            .context("streaming torrents page")?;
        if page.is_empty() {
            break;
        }
        for row in &page {
            let ih_hex = row.info_hash.to_string();
            match row.files() {
                Ok(files) => sink.push_torrent(&ih_hex, &files)?,
                Err(e) => {
                    *blob_errors += 1;
                    eprintln!("blob decode error for {ih_hex}: {e}");
                }
            }
            *torrents += 1;
            if opts.limit.is_some_and(|l| *torrents >= l) {
                return Ok(());
            }
        }
        cursor = page.last().map(|r| r.info_hash.clone());
        if *torrents % 1_000_000 < page.len() as u64 {
            eprintln!("  …{torrents} torrents streamed");
        }
    }
    if *blob_errors > 0 {
        eprintln!("warning: {blob_errors} torrents had undecodable blobs");
    }
    Ok(())
}
