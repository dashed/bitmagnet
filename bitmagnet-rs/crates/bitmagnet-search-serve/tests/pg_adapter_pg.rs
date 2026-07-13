//! Opt-in live PostgreSQL checks for the composer adapter.
//!
//! Set `BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL` to a disposable PostgreSQL
//! database. The test uses one connection and temporary tables only.

use bitmagnet_model::InfoHash;
use bitmagnet_search_serve::{PgSearch, PgSearchBackend, SearchBuildConfig};
use sqlx::postgres::PgPoolOptions;

fn info_hash(byte: u8) -> InfoHash {
    InfoHash::new([byte; bitmagnet_model::INFO_HASH_LEN])
}

#[tokio::test]
async fn file_counts_prefers_summary_then_falls_back_without_blob_columns() {
    let Ok(dsn) = std::env::var("BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL") else {
        eprintln!("skipping: BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect to disposable PostgreSQL");

    sqlx::query(
        "CREATE TEMP TABLE torrent_file_summary (\
         info_hash bytea PRIMARY KEY, file_count integer NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create temporary summary table");
    sqlx::query(
        "CREATE TEMP TABLE torrents (\
         info_hash bytea PRIMARY KEY, files_count integer NULL)",
    )
    .execute(&pool)
    .await
    .expect("create temporary torrent table");

    let summary = info_hash(1);
    let fallback = info_hash(2);
    let unknown = info_hash(3);
    sqlx::query("INSERT INTO torrent_file_summary (info_hash, file_count) VALUES ($1, 11)")
        .bind(summary.as_slice())
        .execute(&pool)
        .await
        .expect("insert summary count");
    sqlx::query(
        "INSERT INTO torrents (info_hash, files_count) VALUES \
         ($1, 99), ($2, 22), ($3, NULL)",
    )
    .bind(summary.as_slice())
    .bind(fallback.as_slice())
    .bind(unknown.as_slice())
    .execute(&pool)
    .await
    .expect("insert fallback counts");

    let counts = PgSearch::new(pool, SearchBuildConfig::default())
        .file_counts(&[summary, fallback, unknown])
        .await
        .expect("read two-step counts");

    assert_eq!(counts.get(&summary), Some(&11), "summary must win");
    assert_eq!(counts.get(&fallback), Some(&22), "torrent fallback");
    assert!(!counts.contains_key(&unknown), "NULL remains unknown");
}
