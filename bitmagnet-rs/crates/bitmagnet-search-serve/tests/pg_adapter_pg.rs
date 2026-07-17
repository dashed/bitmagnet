//! Opt-in live PostgreSQL checks for the composer adapter.
//!
//! Set `BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL` to a disposable PostgreSQL
//! database. The test uses one connection and temporary tables only.

use bitmagnet_model::InfoHash;
use bitmagnet_search_query::{
    fetch_aggregations, fetch_aggregations_grouped_for_candidates, Criteria, FacetRequest,
    SearchOptions, TorrentContentFacet,
};
use bitmagnet_search_serve::{PgSearch, PgSearchBackend, SearchBuildConfig};
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeSet;

fn info_hash(byte: u8) -> InfoHash {
    InfoHash::new([byte; bitmagnet_model::INFO_HASH_LEN])
}

#[tokio::test]
async fn refine_metadata_prefers_summary_and_falls_back_only_for_misses() {
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
         info_hash bytea PRIMARY KEY, file_count integer NOT NULL, compressed_bytes bigint NULL)",
    )
    .execute(&pool)
    .await
    .expect("create temporary summary table");
    sqlx::query(
        "CREATE TEMP TABLE torrents (\
         info_hash bytea PRIMARY KEY, files_count integer NULL, files_data bytea NULL)",
    )
    .execute(&pool)
    .await
    .expect("create temporary torrent table");

    // covered: summary supplies both count and bytes -> NOT in the miss set.
    let covered = info_hash(1);
    // null_bytes: summary row exists but compressed_bytes is NULL -> miss.
    let null_bytes = info_hash(2);
    // no_summary: absent from summary -> miss.
    let no_summary = info_hash(4);
    // unknown: a miss whose torrents row carries no data either.
    let unknown = info_hash(3);

    sqlx::query(
        "INSERT INTO torrent_file_summary (info_hash, file_count, compressed_bytes) VALUES \
         ($1, 11, 3), ($2, 22, NULL)",
    )
    .bind(covered.as_slice())
    .bind(null_bytes.as_slice())
    .execute(&pool)
    .await
    .expect("insert summary rows");
    // `covered` also has a DIFFERENT torrents row (99 / 4 bytes): the miss-set query
    // must never read it, proving the summary wins AND the torrents probe is scoped
    // to only the miss ids.
    sqlx::query(
        "INSERT INTO torrents (info_hash, files_count, files_data) VALUES \
         ($1, 99, decode('01020304', 'hex')), \
         ($2, 99, decode('0102', 'hex')), \
         ($3, 33, decode('010203', 'hex')), \
         ($4, NULL, NULL)",
    )
    .bind(covered.as_slice())
    .bind(null_bytes.as_slice())
    .bind(no_summary.as_slice())
    .bind(unknown.as_slice())
    .execute(&pool)
    .await
    .expect("insert torrents rows");

    let metadata = PgSearch::new(pool, SearchBuildConfig::default())
        .refine_metadata(&[covered, null_bytes, no_summary, unknown])
        .await
        .expect("read summary-first refine metadata");

    // covered: summary wins for both, torrents row (99/4) never consulted.
    assert_eq!(
        metadata[&covered].file_count,
        Some(11),
        "summary count wins"
    );
    assert_eq!(
        metadata[&covered].compressed_bytes,
        Some(3),
        "summary bytes win; torrents miss-set query excludes covered ids"
    );
    // null_bytes: summary count wins, torrents fills the NULL bytes.
    assert_eq!(
        metadata[&null_bytes].file_count,
        Some(22),
        "summary count wins"
    );
    assert_eq!(
        metadata[&null_bytes].compressed_bytes,
        Some(2),
        "torrents fills the NULL compressed_bytes"
    );
    // no_summary: torrents fills both.
    assert_eq!(
        metadata[&no_summary].file_count,
        Some(33),
        "torrents fallback count"
    );
    assert_eq!(metadata[&no_summary].compressed_bytes, Some(3));
    // unknown: a miss with no data anywhere.
    assert_eq!(metadata[&unknown].file_count, None, "NULL remains unknown");
    assert_eq!(metadata[&unknown].compressed_bytes, None);
}

/// A fully covered candidate set must never touch the torrents probe. The
/// torrents relation is deliberately NOT created: if `refine_metadata` still ran
/// `TORRENT_REFINE_METADATA_SQL`, it would error with "relation torrents does not
/// exist"; a clean `Ok` with correct metadata proves the skip.
#[tokio::test]
async fn refine_metadata_skips_torrents_probe_when_fully_covered() {
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
         info_hash bytea PRIMARY KEY, file_count integer NOT NULL, compressed_bytes bigint NULL)",
    )
    .execute(&pool)
    .await
    .expect("create temporary summary table");

    let a = info_hash(1);
    let b = info_hash(2);
    sqlx::query(
        "INSERT INTO torrent_file_summary (info_hash, file_count, compressed_bytes) VALUES \
         ($1, 7, 700), ($2, 9, 900)",
    )
    .bind(a.as_slice())
    .bind(b.as_slice())
    .execute(&pool)
    .await
    .expect("insert covered summary rows");

    let metadata = PgSearch::new(pool, SearchBuildConfig::default())
        .refine_metadata(&[a, b])
        .await
        .expect("fully covered set must not query the (absent) torrents relation");

    assert_eq!(metadata[&a].file_count, Some(7));
    assert_eq!(metadata[&a].compressed_bytes, Some(700));
    assert_eq!(metadata[&b].file_count, Some(9));
    assert_eq!(metadata[&b].compressed_bytes, Some(900));
}

/// Regression: a pre-00026 schema (rolling deploy / pre-backfill window) whose
/// `torrent_file_summary` has NO `compressed_bytes` column at all must still
/// serve — the summary probe reads `file_count` only and every candidate falls
/// through to the torrents `octet_length(files_data)` probe for its size. This
/// mirrors the goose-25 RSS gate harness schema. Before the fix the summary SQL
/// unconditionally selected `compressed_bytes`, so this errored with
/// `column "compressed_bytes" does not exist`, which the composer surfaced as an
/// empty result set (item_count=0, total_count=0, estimate=false).
#[tokio::test]
async fn refine_metadata_falls_back_when_summary_lacks_compressed_bytes_column() {
    let Ok(dsn) = std::env::var("BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL") else {
        eprintln!("skipping: BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect to disposable PostgreSQL");

    // Pre-00026 summary table: file_count present, compressed_bytes column absent.
    sqlx::query(
        "CREATE TEMP TABLE torrent_file_summary (\
         info_hash bytea PRIMARY KEY, file_count integer NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create pre-00026 summary table");
    sqlx::query(
        "CREATE TEMP TABLE torrents (\
         info_hash bytea PRIMARY KEY, files_count integer NULL, files_data bytea NULL)",
    )
    .execute(&pool)
    .await
    .expect("create temporary torrent table");

    // has_summary: summary supplies file_count; torrents supplies the size.
    let has_summary = info_hash(1);
    // no_summary: absent from summary; torrents supplies both count and size.
    let no_summary = info_hash(2);

    sqlx::query("INSERT INTO torrent_file_summary (info_hash, file_count) VALUES ($1, 11)")
        .bind(has_summary.as_slice())
        .execute(&pool)
        .await
        .expect("insert pre-00026 summary row");
    sqlx::query(
        "INSERT INTO torrents (info_hash, files_count, files_data) VALUES \
         ($1, 99, decode('01020304', 'hex')), \
         ($2, 33, decode('010203', 'hex'))",
    )
    .bind(has_summary.as_slice())
    .bind(no_summary.as_slice())
    .execute(&pool)
    .await
    .expect("insert torrents rows");

    let metadata = PgSearch::new(pool, SearchBuildConfig::default())
        .refine_metadata(&[has_summary, no_summary])
        .await
        .expect("pre-00026 schema must serve via the torrents fallback, not error");

    // has_summary: authoritative file_count from the summary, size from torrents.
    assert_eq!(
        metadata[&has_summary].file_count,
        Some(11),
        "summary file_count wins even without the compressed_bytes column"
    );
    assert_eq!(
        metadata[&has_summary].compressed_bytes,
        Some(4),
        "torrents octet_length fills the size when the summary column is absent"
    );
    // no_summary: torrents fills both.
    assert_eq!(
        metadata[&no_summary].file_count,
        Some(33),
        "torrents fallback count"
    );
    assert_eq!(metadata[&no_summary].compressed_bytes, Some(3));
}

/// The grouped fast path must reproduce the per-value aggregation map exactly,
/// including a zero-count filter-selected value and a NULL bucket. A disposable
/// PostgreSQL is provided via `BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL`; this
/// test seeds only session-local temporary tables and never touches the seeded
/// corpus. `aggregation_budget = 0` makes the per-value path use plain
/// `count(*)` subqueries, so it needs no `budgeted_count` function and both
/// paths are exact.
#[tokio::test]
async fn grouped_aggregations_match_per_value_on_scalar_facets() {
    let Ok(dsn) = std::env::var("BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL") else {
        eprintln!("skipping: BITMAGNET_SEARCH_SERVE_TEST_DATABASE_URL is not set");
        return;
    };
    // A single connection keeps every query in the same session, so the
    // session-local temp table is visible to the concurrent facet fan-out
    // (which simply serialises on the one connection here).
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect to disposable PostgreSQL");

    // `torrent_contents` as a session-local temp table shadows the seeded one
    // for this connection pool only; content_type/video_source as text lets the
    // enum `IN`/`IS NULL`/`::text` SQL work unchanged.
    sqlx::query(
        "CREATE TEMP TABLE torrent_contents (\
         info_hash bytea PRIMARY KEY, content_type text NULL, video_source text NULL)",
    )
    .execute(&pool)
    .await
    .expect("create temp torrent_contents");

    // ih1..ih4 are the refined candidate set; ih5 is present but excluded by the
    // info-hash IN list, proving the grouped GROUP BY still honours it.
    let rows: [(InfoHash, Option<&str>, Option<&str>); 5] = [
        (info_hash(1), Some("movie"), Some("BluRay")),
        (info_hash(2), Some("movie"), Some("WEBDL")),
        (info_hash(3), Some("tv_show"), Some("BluRay")),
        (info_hash(4), None, None),
        (info_hash(5), Some("music"), Some("CAM")),
    ];
    for (ih, content_type, video_source) in rows {
        sqlx::query(
            "INSERT INTO torrent_contents (info_hash, content_type, video_source) \
             VALUES ($1, $2, $3)",
        )
        .bind(ih.as_slice())
        .bind(content_type)
        .bind(video_source)
        .execute(&pool)
        .await
        .expect("seed temp torrent_contents row");
    }

    let refined = [info_hash(1), info_hash(2), info_hash(3), info_hash(4)];
    let config = SearchBuildConfig::default();
    let now = chrono::Utc::now();

    let parity = |options: SearchOptions| {
        let pool = pool.clone();
        async move {
            let per_value = fetch_aggregations(&pool, &options, &config, now)
                .await
                .expect("per-value aggregations");
            let grouped = fetch_aggregations_grouped_for_candidates(&pool, &options, &config, now)
                .await
                .expect("grouped aggregations");
            assert_eq!(
                grouped, per_value,
                "grouped fast path must equal the per-value map byte-for-byte"
            );
            grouped
        }
    };

    let base = SearchOptions::new()
        .with_filter(Criteria::torrent_content_info_hash_in(refined))
        .with_aggregation_budget(0.0);

    // Content type alone: a filter-selected value absent from the refined set
    // keeps a count-0 bucket, and the NULL content_type row keeps a "null"
    // bucket (that value is in the content_type vocabulary).
    let content_type_only = base.clone().with_facets([FacetRequest {
        facet: TorrentContentFacet::ContentType,
        aggregate: true,
        logic: None,
        filter: BTreeSet::from(["software".to_owned()]),
    }]);
    let grouped = parity(content_type_only).await;
    let content_type = &grouped["content_type"].items;
    assert_eq!(
        content_type.keys().cloned().collect::<Vec<_>>(),
        vec![
            "movie".to_owned(),
            "null".to_owned(),
            "software".to_owned(),
            "tv_show".to_owned()
        ]
    );
    assert_eq!(content_type["movie"].count, 2);
    assert_eq!(content_type["tv_show"].count, 1);
    assert_eq!(
        content_type["null"].count, 1,
        "NULL content_type bucket kept"
    );
    assert_eq!(
        content_type["software"].count, 0,
        "filter-selected zero-count bucket kept"
    );
    assert!(content_type.values().all(|item| !item.is_estimate));

    // Video source alone: its vocabulary omits "null", so the NULL video_source
    // row must be dropped on both paths (the per-value path never queries it).
    let video_source_only = base.clone().with_facets([FacetRequest {
        facet: TorrentContentFacet::VideoSource,
        aggregate: true,
        logic: None,
        filter: BTreeSet::new(),
    }]);
    let grouped = parity(video_source_only).await;
    let video_source = &grouped["video_source"].items;
    assert_eq!(
        video_source.keys().cloned().collect::<Vec<_>>(),
        vec!["BluRay".to_owned(), "WEBDL".to_owned()],
        "video_source vocabulary omits null, so the NULL row is dropped"
    );
    assert_eq!(video_source["BluRay"].count, 2);
    assert_eq!(video_source["WEBDL"].count, 1);

    // Both scalar facets in one call: cross-facet predicates interact, but the
    // grouped fan-out must still match the per-value map exactly.
    let both = base.with_facets([
        FacetRequest {
            facet: TorrentContentFacet::ContentType,
            aggregate: true,
            logic: None,
            filter: BTreeSet::from(["movie".to_owned()]),
        },
        FacetRequest {
            facet: TorrentContentFacet::VideoSource,
            aggregate: true,
            logic: None,
            filter: BTreeSet::new(),
        },
    ]);
    let _ = parity(both).await;
}
