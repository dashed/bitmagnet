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
//! Reads `torrent_files` via one ordered streaming scan over the
//! `unique(info_hash, index)` index (no sort); groups per info_hash; batches
//! `UPDATE torrents SET files_data` via parallel UNNEST. Writes ONLY the
//! throwaway bench DB.
//!
//! usage:
//!   blob_encode_rs --dsn <url> [--limit T] [--batch N]
//!     --limit T   stop after T torrents (smoke gate G3; default = all)
//!     --batch N   torrents per UPDATE round-trip (default 5000)

use std::time::Instant;

use anyhow::{Context, Result};
use bitmagnet_model::{serialize_files, BlobFile};
use futures::TryStreamExt;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

struct Opts {
    dsn: String,
    limit: Option<u64>,
    batch: usize,
}

fn parse_opts() -> Opts {
    let mut dsn = String::new();
    let mut limit: Option<u64> = None;
    let mut batch: usize = 5000;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dsn" => dsn = args.next().unwrap_or_default(),
            "--limit" => limit = args.next().and_then(|v| v.parse().ok()),
            "--batch" => batch = args.next().and_then(|v| v.parse().ok()).unwrap_or(5000),
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
    // Pool sized for: 1 long-lived streaming reader + update connections.
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&opts.dsn)
        .await
        .context("connect bench postgres")?;

    let started = Instant::now();
    let mut torrents: u64 = 0;
    let mut files: u64 = 0;
    let mut bytes_written: u64 = 0;
    let mut encode_ns: u128 = 0;

    // Pending UPDATE batch (parallel arrays).
    let mut b_hashes: Vec<Vec<u8>> = Vec::with_capacity(opts.batch);
    let mut b_blobs: Vec<Vec<u8>> = Vec::with_capacity(opts.batch);

    // Current torrent accumulator.
    let mut cur_hash: Option<Vec<u8>> = None;
    let mut cur_files: Vec<BlobFile> = Vec::new();

    // Dedicated streaming connection (ordered scan over unique(info_hash,index)).
    let mut reader = pool.acquire().await.context("acquire reader conn")?;
    let mut rows = sqlx::query(
        r#"SELECT info_hash, "index", path, extension, size
           FROM torrent_files
           ORDER BY info_hash, "index""#,
    )
    .fetch(&mut *reader);

    // Flush helper closure can't borrow pool+batches cleanly across await, so
    // inline the flush via a labeled block using a fn taking the pieces.
    while let Some(row) = rows.try_next().await.context("stream torrent_files")? {
        let ih: Vec<u8> = row.try_get("info_hash")?;
        let index: i32 = row.try_get("index")?;
        let path: String = row.try_get("path")?;
        let extension: Option<String> = row.try_get("extension")?;
        let size: i64 = row.try_get("size")?;

        match &cur_hash {
            Some(h) if h == &ih => {}
            _ => {
                // info_hash boundary: finalise the previous torrent.
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
                        flush(&pool, &mut b_hashes, &mut b_blobs).await?;
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

    // Finalise the trailing torrent (only if we didn't hit --limit).
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
    // Drop the streaming reader before the final flush so the pool has capacity.
    drop(rows);
    drop(reader);
    flush(&pool, &mut b_hashes, &mut b_blobs).await?;

    let secs = started.elapsed().as_secs_f64();
    let us_per_file = if files > 0 {
        (encode_ns as f64 / 1000.0) / files as f64
    } else {
        0.0
    };
    println!(
        "DONE: {torrents} torrents, {files} files encoded\n  \
         encode µs/file (pure serialize_files): {us_per_file:.4}\n  \
         files_data bytes written: {bytes_written} ({:.2} GiB)\n  \
         wall: {secs:.1}s  ({:.0} torrents/s, {:.2} M files/s)",
        bytes_written as f64 / (1024.0 * 1024.0 * 1024.0),
        torrents as f64 / secs,
        files as f64 / secs / 1e6,
    );
    Ok(())
}

/// Batched parallel-UNNEST UPDATE of `torrents.files_data`. Clears the batch.
async fn flush(
    pool: &sqlx::PgPool,
    hashes: &mut Vec<Vec<u8>>,
    blobs: &mut Vec<Vec<u8>>,
) -> Result<()> {
    if hashes.is_empty() {
        return Ok(());
    }
    sqlx::query(
        r#"UPDATE torrents t
           SET files_data = d.blob
           FROM (SELECT unnest($1::bytea[]) AS info_hash,
                        unnest($2::bytea[]) AS blob) d
           WHERE t.info_hash = d.info_hash"#,
    )
    .bind(&hashes[..])
    .bind(&blobs[..])
    .execute(pool)
    .await
    .context("batched UPDATE torrents.files_data")?;
    hashes.clear();
    blobs.clear();
    Ok(())
}
