//! blob_encode_rs — THROWAWAY bench tool (team bitmagnet-bench, PSX-D1 / R5).
//!
//! Re-creates `torrents.files_data` on the bench restore from `torrent_files`,
//! using the EXACT production blob format (`bitmagnet_model::serialize_files` =
//! zstd(msgpack[{i,p,e,s}])). This closes the L2 measurement gap: every prior
//! DuckDB/Parquet number was sourced from `torrent_files`; this lets the
//! UNMODIFIED `blob_export` then run the real decode→ext→Parquet pipeline over
//! actual blob bytes at full scale, and proves blob↔torrent_files parity.
//!
//! 🚨 RUST-ENCODER FALLBACK (no Go toolchain on HEL1): the inner msgpack is
//! byte-identical to Go's `blobmigration.SerializeFiles` (proven in
//! `bitmagnet-model/tests/blob_fixture.rs`); only the outer zstd frame differs
//! between libzstd and klauspost — mutually decodable, so immaterial to
//! `blob_export`'s decoder. (Spec §1.2.)
//!
//! Mirrors the BACKFILL write path (`handler.processBatch`): serialises ALL
//! `torrent_files` rows for a hash (NO @100 cap), so the decoded fileset ===
//! `torrent_files` for that hash → exact Stage-3 parity.
//!
//! WRITE STRATEGY: one ordered streaming scan over the `unique(info_hash,index)`
//! index (no sort), group per info_hash, INSERT (info_hash, blob) into a session
//! TEMP table in batches, then ONE final indexed UPDATE joining the temp table
//! to `torrents` (a single pass — NOT a per-batch seq scan). Writes ONLY the
//! throwaway bench DB.
//!
//! usage:
//!   blob_encode_rs --dsn <url> [--limit T] [--batch N]
//!     --limit T   stop after T torrents (smoke gate G3; default = all)
//!     --batch N   torrents per temp-INSERT round-trip (default 20000)

use std::time::Instant;

use anyhow::{Context, Result};
use bitmagnet_model::{serialize_files, BlobFile};
use futures::TryStreamExt;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Row};

struct Opts {
    dsn: String,
    limit: Option<u64>,
    batch: usize,
}

fn parse_opts() -> Opts {
    let mut dsn = String::new();
    let mut limit: Option<u64> = None;
    let mut batch: usize = 20000;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dsn" => dsn = args.next().unwrap_or_default(),
            "--limit" => limit = args.next().and_then(|v| v.parse().ok()),
            "--batch" => batch = args.next().and_then(|v| v.parse().ok()).unwrap_or(20000),
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    if dsn.is_empty() {
        eprintln!("usage: blob_encode_rs --dsn <url> [--limit T] [--batch N]");
        std::process::exit(2);
    }
    Opts { dsn, limit, batch }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = parse_opts();
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&opts.dsn)
        .await
        .context("connect bench postgres")?;

    // Dedicated writer connection: holds the session TEMP table for the whole
    // run (temp tables are per-connection) + runs the final UPDATE.
    let mut writer = pool.acquire().await.context("acquire writer conn")?;
    writer
        .execute("CREATE TEMP TABLE enc (info_hash bytea NOT NULL, blob bytea NOT NULL) ON COMMIT PRESERVE ROWS")
        .await
        .context("create temp table enc")?;

    let started = Instant::now();
    let mut torrents: u64 = 0;
    let mut files: u64 = 0;
    let mut bytes_written: u64 = 0;
    let mut encode_ns: u128 = 0;

    let mut b_hashes: Vec<Vec<u8>> = Vec::with_capacity(opts.batch);
    let mut b_blobs: Vec<Vec<u8>> = Vec::with_capacity(opts.batch);

    let mut cur_hash: Option<Vec<u8>> = None;
    let mut cur_files: Vec<BlobFile> = Vec::new();

    let mut reader = pool.acquire().await.context("acquire reader conn")?;
    let mut rows = sqlx::query(
        r#"SELECT info_hash, "index", path, extension, size
           FROM torrent_files
           ORDER BY info_hash, "index""#,
    )
    .fetch(&mut *reader);

    while let Some(row) = rows.try_next().await.context("stream torrent_files")? {
        let ih: Vec<u8> = row.try_get("info_hash")?;
        let index: i32 = row.try_get("index")?;
        let path: String = row.try_get("path")?;
        let extension: Option<String> = row.try_get("extension")?;
        let size: i64 = row.try_get("size")?;

        match &cur_hash {
            Some(h) if h == &ih => {}
            _ => {
                if let Some(prev) = cur_hash.take() {
                    let t = Instant::now();
                    let blob = serialize_files(&cur_files).context("serialize_files")?;
                    encode_ns += t.elapsed().as_nanos();
                    files += cur_files.len() as u64;
                    bytes_written += blob.len() as u64;
                    b_hashes.push(prev);
                    b_blobs.push(blob);
                    cur_files.clear();
                    torrents += 1;

                    if b_hashes.len() >= opts.batch {
                        insert_batch(&mut writer, &mut b_hashes, &mut b_blobs).await?;
                    }
                    if opts.limit.is_some_and(|l| torrents >= l) {
                        cur_hash = None;
                        break;
                    }
                }
                cur_hash = Some(ih.clone());
            }
        }
        cur_files.push(BlobFile {
            index: index as u32,
            path,
            extension: extension.unwrap_or_default(),
            size: size as u64,
        });
    }

    if let Some(prev) = cur_hash.take() {
        let t = Instant::now();
        let blob = serialize_files(&cur_files).context("serialize_files")?;
        encode_ns += t.elapsed().as_nanos();
        files += cur_files.len() as u64;
        bytes_written += blob.len() as u64;
        b_hashes.push(prev);
        b_blobs.push(blob);
        torrents += 1;
    }
    drop(rows);
    drop(reader);
    insert_batch(&mut writer, &mut b_hashes, &mut b_blobs).await?;

    let read_encode_secs = started.elapsed().as_secs_f64();
    eprintln!(
        "read+encode+temp-insert done: {torrents} torrents in {read_encode_secs:.1}s; applying final UPDATE…"
    );

    // ONE indexed pass: index the temp table then merge/hash-join to torrents.
    let apply_start = Instant::now();
    writer
        .execute("CREATE INDEX enc_ih_idx ON enc (info_hash)")
        .await
        .context("index enc")?;
    writer.execute("ANALYZE enc").await.ok();
    let res = writer
        .execute(
            "UPDATE torrents t SET files_data = e.blob FROM enc e WHERE t.info_hash = e.info_hash",
        )
        .await
        .context("final UPDATE torrents.files_data")?;
    let updated = res.rows_affected();
    writer.execute("DROP TABLE enc").await.ok();
    let apply_secs = apply_start.elapsed().as_secs_f64();

    let secs = started.elapsed().as_secs_f64();
    let us_per_file = if files > 0 {
        (encode_ns as f64 / 1000.0) / files as f64
    } else {
        0.0
    };
    println!(
        "DONE: {torrents} torrents, {files} files encoded, {updated} torrents UPDATEd\n  \
         encode µs/file (pure serialize_files): {us_per_file:.4}\n  \
         files_data bytes written: {bytes_written} ({:.2} GiB)\n  \
         read+encode+temp-insert: {read_encode_secs:.1}s | final apply (index+UPDATE): {apply_secs:.1}s\n  \
         wall: {secs:.1}s  ({:.0} torrents/s, {:.2} M files/s)",
        bytes_written as f64 / (1024.0 * 1024.0 * 1024.0),
        torrents as f64 / secs,
        files as f64 / secs / 1e6,
    );
    Ok(())
}

/// Append a batch of (info_hash, blob) into the session TEMP table via parallel
/// UNNEST (no join → cheap). Clears the batch.
async fn insert_batch(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    hashes: &mut Vec<Vec<u8>>,
    blobs: &mut Vec<Vec<u8>>,
) -> Result<()> {
    if hashes.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO enc (info_hash, blob) \
         SELECT unnest($1::bytea[]), unnest($2::bytea[])",
    )
    .bind(&hashes[..])
    .bind(&blobs[..])
    .execute(&mut **conn)
    .await
    .context("INSERT INTO enc")?;
    hashes.clear();
    blobs.clear();
    Ok(())
}
