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
        const ORDER_PROBE: &str = "__candidate_hydration_order_probe__";
        sqlx::query("DELETE FROM torrent_contents WHERE release_group = $1")
            .bind(ORDER_PROBE)
            .execute(&pool)
            .await
            .expect("remove a stale candidate-order probe");
        let raw_hash = sqlx::query_scalar::<_, Vec<u8>>(
            "INSERT INTO torrent_contents (\
                 info_hash, content_type, content_source, content_id, languages, episodes, \
                 video_resolution, video_source, video_codec, video_3d, video_modifier, \
                 release_group, created_at, updated_at, tsv, seeders, leechers, published_at, \
                 size, files_count) \
             SELECT info_hash, 'audiobook', NULL, NULL, languages, '{}'::jsonb, \
                    NULL, NULL, NULL, NULL, NULL, $1, created_at, updated_at, tsv, seeders, \
                    leechers, published_at, size, files_count \
             FROM torrent_contents original \
             WHERE NOT EXISTS (\
                 SELECT 1 FROM torrent_contents sibling \
                 WHERE sibling.info_hash = original.info_hash \
                   AND sibling.content_type = 'audiobook'\
             ) \
             ORDER BY original.id \
             LIMIT 1 \
             RETURNING info_hash",
        )
        .bind(ORDER_PROBE)
        .fetch_one(&pool)
        .await
        .expect("insert same-info-hash candidate-order probe");
        let hash = InfoHash::from_slice(&raw_hash).expect("valid candidate info hash");
        let options = SearchOptions::new()
            .with_limit(None)
            .with_filter(Criteria::torrent_content_info_hash_in([hash]));
        let config = SearchBuildConfig::default();

        let mut results = Vec::new();
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
            let established = search(&pool, &options, &config, hydrate)
                .await
                .expect("execute established hydration path");
            let candidate = search_candidates(&pool, &options, &config, hydrate)
                .await
                .expect("execute optimized candidate hydration path");
            results.push((established, candidate));
        }

        sqlx::query("DELETE FROM torrent_contents WHERE release_group = $1")
            .bind(ORDER_PROBE)
            .execute(&pool)
            .await
            .expect("remove candidate-order probe");

        for (established, candidate) in results {
            assert!(
                established.items.len() >= 2,
                "order fixture must include two content rows for one info hash"
            );
            assert_eq!(candidate.items, established.items);
            assert_eq!(candidate.total_count, 0);
            assert!(!candidate.total_count_is_estimate);
            assert!(!candidate.has_next_page);
            assert!(candidate.aggregations.is_empty());
        }
    });
}
