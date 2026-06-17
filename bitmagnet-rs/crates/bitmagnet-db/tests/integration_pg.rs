//! DB-gated smoke test for the public path. Ignored by default (there is no
//! live database in CI / `cargo test`); run it against a real server with:
//!
//! ```sh
//! BITMAGNET_POSTGRES_DSN=postgres://postgres@localhost/bitmagnet \
//!   cargo test -p bitmagnet-db -- --ignored
//! ```
//!
//! It exercises connect → ping → first page of `stream_torrents_with_files` →
//! blob decode, and checks that keyset pagination advances past the last hash.

use bitmagnet_db::{connect, ping, stream_torrents_with_files, DbConfig};

#[tokio::test]
#[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
async fn connect_ping_and_stream() {
    let cfg = DbConfig::from_env().expect("config from env");
    let pool = connect(&cfg).await.expect("connect");
    ping(&pool).await.expect("ping");

    let page = stream_torrents_with_files(&pool, None, 5)
        .await
        .expect("stream first page");

    for row in &page {
        // A present blob must decode; an absent one yields no files.
        let files = row.files().expect("decode files blob");
        if row.files_data.is_none() {
            assert!(files.is_empty());
        }
    }

    // Keyset pagination: the next page must start strictly after the last hash.
    if let Some(last) = page.last() {
        let next = stream_torrents_with_files(&pool, Some(&last.info_hash), 5)
            .await
            .expect("stream second page");
        if let Some(first_next) = next.first() {
            assert!(first_next.info_hash > last.info_hash);
        }
    }
}
