//! Live-PostgreSQL parity for the composer-only candidate hydration executor.
//!
//! The optimized executor must return the same fully hydrated rows as the
//! established multi-statement path for the same bounded candidate set. Run
//! this test against a disposable Bitmagnet database:
//!
//! ```text
//! BITMAGNET_POSTGRES_DSN=postgres://... cargo test -p bitmagnet-search-query \
//!   --test candidate_hydration_pg -- --ignored --nocapture
//! ```

use bitmagnet_search_query::{
    search, search_candidates, Criteria, HydrateOptions, InfoHash, SearchBuildConfig, SearchOptions,
};
use sqlx::PgPool;

#[test]
#[ignore = "requires disposable PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
fn candidate_hydration_matches_established_path() {
    let dsn = match std::env::var("BITMAGNET_POSTGRES_DSN") {
        Ok(dsn) if !dsn.is_empty() => dsn,
        _ => {
            eprintln!("BITMAGNET_POSTGRES_DSN not set; skipping candidate hydration live test");
            return;
        }
    };
    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    runtime.block_on(async {
        let pool = PgPool::connect(&dsn)
            .await
            .expect("connect to disposable Bitmagnet PostgreSQL");
        let raw_hashes = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT info_hash FROM torrent_contents ORDER BY id LIMIT 20",
        )
        .fetch_all(&pool)
        .await
        .expect("load candidate hashes");
        assert!(
            !raw_hashes.is_empty(),
            "candidate fixture database is empty"
        );
        let hashes = raw_hashes
            .iter()
            .map(|raw| InfoHash::from_slice(raw).expect("valid candidate info hash"))
            .collect::<Vec<_>>();
        let options = SearchOptions::new()
            .with_limit(None)
            .with_filter(Criteria::torrent_content_info_hash_in(hashes));
        let config = SearchBuildConfig::default();

        for hydrate in [
            HydrateOptions {
                files_data: true,
                max_files_data_bytes: Some(128 * 1024 * 1024),
            },
            HydrateOptions {
                files_data: true,
                max_files_data_bytes: Some(0),
            },
        ] {
            let mut established = search(&pool, &options, &config, hydrate)
                .await
                .expect("execute established hydration path");
            let mut candidate = search_candidates(&pool, &options, &config, hydrate)
                .await
                .expect("execute optimized candidate hydration path");

            established
                .items
                .sort_by(|left, right| left.torrent_content.id.cmp(&right.torrent_content.id));
            candidate
                .items
                .sort_by(|left, right| left.torrent_content.id.cmp(&right.torrent_content.id));
            assert_eq!(candidate.items, established.items);
            assert_eq!(candidate.total_count, 0);
            assert!(!candidate.total_count_is_estimate);
            assert!(!candidate.has_next_page);
            assert!(candidate.aggregations.is_empty());
        }
    });
}
