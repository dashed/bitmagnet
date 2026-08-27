//! Disposable-PostgreSQL gate for the disconnected writer snapshot loader.
//!
//! The ignored test requires an exact disposable database-name sentinel before
//! it truncates or seeds any row. It never targets the production database.

use std::collections::BTreeMap;

use bitmagnet_processor::{
    load_writer_plan, load_writer_torrents, project_unattached_persistence, Materializer,
    TorrentContentWrite, WriterLoadError, WriterLoadedTorrent,
};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use serde_json::Value;
use sqlx::{PgPool, Row};

const TEST_DATABASE_URL: &str = "BITMAGNET_PROCESSOR_WRITER_LOAD_TEST_DATABASE_URL";
const TEST_DATABASE_NAME: &str = "bitmagnet_processor_writer_load_test";
const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const HASH_C: &str = "cccccccccccccccccccccccccccccccccccccccc";
const HASH_D: &str = "dddddddddddddddddddddddddddddddddddddddd";
const HASH_E: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

async fn connect_disposable_database() -> PgPool {
    let database_url = std::env::var(TEST_DATABASE_URL)
        .unwrap_or_else(|_| panic!("{TEST_DATABASE_URL} must be set for ignored gate"));
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect disposable processor PostgreSQL");
    let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("read disposable database sentinel");
    assert_eq!(
        database_name, TEST_DATABASE_NAME,
        "ignored writer-loader gate refuses a database without its exact disposable-name sentinel"
    );
    pool
}

fn content_row(torrent: &WriterLoadedTorrent) -> TorrentContentWrite {
    TorrentContentWrite {
        id: format!("{}:?:?:?", torrent.loaded.info_hash),
        info_hash: torrent.loaded.info_hash.clone(),
        content_type: None,
        content_source: None,
        content_id: None,
        languages: Vec::new(),
        episodes: String::new(),
        video_resolution: None,
        video_source: None,
        video_codec: None,
        video_3d: None,
        video_modifier: None,
        release_group: None,
        size: torrent.loaded.classifier_input.size,
        files_count: torrent.loaded.classifier_input.files_count.map(u64::from),
    }
}

fn supported_params(info_hashes: Vec<ProtocolId>) -> ProcessTorrentParams {
    ProcessTorrentParams {
        info_hashes,
        classifier_workflow: "default".to_owned(),
        classifier_flags: Some(BTreeMap::from([
            ("apis_enabled".to_owned(), Value::Bool(false)),
            ("local_search_enabled".to_owned(), Value::Bool(false)),
            ("tmdb_enabled".to_owned(), Value::Bool(false)),
        ])),
        ..ProcessTorrentParams::default()
    }
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_WRITER_LOAD_TEST_DATABASE_URL pointing at disposable Goose-34 PostgreSQL"]
async fn raw_snapshots_share_the_loaded_keyset_and_preserve_database_values() {
    let pool = connect_disposable_database().await;
    sqlx::query("TRUNCATE torrent_tags, torrent_contents, torrents CASCADE")
        .execute(&pool)
        .await
        .expect("reset processor-owned fixture rows");
    sqlx::query(
        "INSERT INTO torrents \
         (info_hash, name, size, private, created_at, updated_at) \
         VALUES \
         (decode($1, 'hex'), 'writer loader counts', 42, false, \
          '1969-12-31 23:59:59.123456+00', NOW()), \
         (decode($2, 'hex'), 'writer loader no sources', 43, false, \
          '2011-01-01 00:00:00.222333+00', NOW()), \
         (decode($3, 'hex'), 'writer loader cutoff', 44, false, \
          '2010-01-01 00:00:00.444555+00', NOW())",
    )
    .bind(HASH_A)
    .bind(HASH_B)
    .bind(HASH_D)
    .execute(&pool)
    .await
    .expect("seed torrent fixtures");
    sqlx::query(
        "INSERT INTO torrents_torrent_sources \
         (source, info_hash, seeders, leechers, published_at, created_at, updated_at) \
         VALUES \
         ('dht', decode($1, 'hex'), 0, 9, \
          '2001-01-01 00:00:00.654321+00', '1999-01-01 00:00:00.111222+00', NOW()), \
         ('rarbg', decode($1, 'hex'), 8, 1, \
          NULL, '2002-01-01 00:00:00.333444+00', NOW()), \
         ('dht', decode($2, 'hex'), NULL, NULL, \
          '2000-01-01 00:00:00+00', '2005-01-01 00:00:00.555666+00', NOW()), \
         ('rarbg', decode($2, 'hex'), NULL, NULL, \
          '2000-01-01 00:00:00.000001+00', '2006-01-01 00:00:00.777888+00', NOW())",
    )
    .bind(HASH_A)
    .bind(HASH_D)
    .execute(&pool)
    .await
    .expect("seed raw source fixtures");

    let id_a = ProtocolId::from_hex(HASH_A).expect("fixture hash A");
    let id_b = ProtocolId::from_hex(HASH_B).expect("fixture hash B");
    let id_c = ProtocolId::from_hex(HASH_C).expect("fixture hash C");
    let id_d = ProtocolId::from_hex(HASH_D).expect("fixture hash D");
    let loaded = load_writer_torrents(
        &pool,
        &ProcessTorrentParams {
            // The public request occurrence limit precedes deduplication; the
            // accepted duplicate still reaches PostgreSQL only once.
            info_hashes: vec![id_d, id_a, id_c, id_b, id_a],
            ..ProcessTorrentParams::default()
        },
    )
    .await
    .expect("load classifier and writer snapshots");

    assert_eq!(
        loaded.len(),
        3,
        "the missing requested hash is omitted and the duplicate is associated once"
    );
    assert_eq!(loaded[0].loaded.info_hash, HASH_A);
    assert_eq!(loaded[0].loaded.classifier_input.id, HASH_A);
    assert_eq!(loaded[0].torrent_snapshot.created_at_micros, -876_544);
    assert_eq!(loaded[0].source_snapshots.len(), 2);
    assert_eq!(loaded[0].source_snapshots[0].seeders, Some(0));
    assert_eq!(loaded[0].source_snapshots[0].leechers, Some(9));
    assert_eq!(
        loaded[0].source_snapshots[0].published_at_micros,
        Some(978_307_200_654_321)
    );
    assert_eq!(
        loaded[0].source_snapshots[0].created_at_micros,
        915_148_800_111_222
    );
    assert_eq!(loaded[0].source_snapshots[1].seeders, Some(8));
    assert_eq!(loaded[0].source_snapshots[1].leechers, Some(1));
    assert_eq!(loaded[0].source_snapshots[1].published_at_micros, None);
    assert_eq!(
        loaded[0].source_snapshots[1].created_at_micros,
        1_009_843_200_333_444
    );
    assert_eq!(loaded[1].loaded.info_hash, HASH_B);
    assert!(
        loaded[1].source_snapshots.is_empty(),
        "a present torrent with no source rows retains an empty raw image"
    );
    assert_eq!(loaded[2].loaded.info_hash, HASH_D);
    assert_eq!(
        loaded[2].source_snapshots[0].published_at_micros,
        Some(946_684_800_000_000),
        "the exact cutoff remains raw"
    );
    assert_eq!(
        loaded[2].source_snapshots[1].published_at_micros,
        Some(946_684_800_000_001),
        "one microsecond after the cutoff remains exact"
    );

    let counts = project_unattached_persistence(
        &content_row(&loaded[0]),
        &loaded[0].loaded.classifier_input,
        loaded[0].torrent_snapshot,
        &loaded[0].source_snapshots,
    )
    .expect("project independently loaded maxima");
    assert_eq!(counts.seeders, Some(8));
    assert_eq!(counts.leechers, Some(9));

    let cutoff = project_unattached_persistence(
        &content_row(&loaded[2]),
        &loaded[2].loaded.classifier_input,
        loaded[2].torrent_snapshot,
        &loaded[2].source_snapshots,
    )
    .expect("project exact cutoff semantics");
    assert_eq!(
        cutoff.published_at_micros, 946_684_800_000_001,
        "exact cutoff falls back to created_at while cutoff + 1 is accepted"
    );

    let materializer = Materializer::from_core().expect("compile core classifier");
    let plan = load_writer_plan(&pool, &materializer, &supported_params(vec![id_a]))
        .await
        .expect("compose one disconnected writer plan from PostgreSQL");
    assert_eq!(plan.write_set().torrent_contents.len(), 1);
    assert!(plan.retry_info_hashes().is_empty());
    let planned_row = &plan.write_set().torrent_contents[0];
    let planned_metadata = plan
        .persistence()
        .get(&planned_row.id)
        .expect("materialized row has exact persistence metadata");
    assert_eq!(planned_metadata.seeders, Some(8));
    assert_eq!(planned_metadata.leechers, Some(9));
    assert_eq!(planned_metadata.published_at_micros, -876_544);

    let persisted_counts = sqlx::query(
        "SELECT \
           (SELECT count(*)::bigint FROM torrent_contents) AS torrent_contents, \
           (SELECT count(*)::bigint FROM torrent_tags) AS torrent_tags",
    )
    .fetch_one(&pool)
    .await
    .expect("count writer-owned rows after disconnected planning");
    assert_eq!(
        persisted_counts
            .try_get::<i64, _>("torrent_contents")
            .expect("decode torrent_contents count"),
        0,
        "writer planning must not persist torrent_contents"
    );
    assert_eq!(
        persisted_counts
            .try_get::<i64, _>("torrent_tags")
            .expect("decode torrent_tags count"),
        0,
        "writer planning must not persist torrent_tags"
    );

    sqlx::query(
        "UPDATE torrents_torrent_sources SET seeders = -1 \
         WHERE source = 'dht' AND info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .execute(&pool)
    .await
    .expect("seed negative count guard");
    let error = load_writer_torrents(
        &pool,
        &ProcessTorrentParams {
            info_hashes: vec![id_a],
            ..ProcessTorrentParams::default()
        },
    )
    .await
    .expect_err("negative source counts fail closed");
    assert!(matches!(
        error,
        WriterLoadError::NegativeSourceCount {
            ref info_hash,
            field: "seeders",
            value: -1
        } if info_hash == HASH_A
    ));

    sqlx::query(
        "INSERT INTO torrent_sources (key, name, created_at, updated_at) \
         SELECT 'writer-load-' || lpad(i::text, 4, '0'), \
                'writer loader cardinality fixture', NOW(), NOW() \
         FROM generate_series(0, 1024) AS i \
         ON CONFLICT (key) DO NOTHING",
    )
    .execute(&pool)
    .await
    .expect("seed bounded source keys");
    sqlx::query(
        "INSERT INTO torrents \
         (info_hash, name, size, private, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'writer loader source bound', 45, false, NOW(), NOW())",
    )
    .bind(HASH_E)
    .execute(&pool)
    .await
    .expect("seed source-bound torrent");
    sqlx::query(
        "INSERT INTO torrents_torrent_sources \
         (source, info_hash, seeders, leechers, published_at, created_at, updated_at) \
         SELECT key, decode($1, 'hex'), NULL, NULL, NULL, NOW(), NOW() \
         FROM torrent_sources \
         WHERE key LIKE 'writer-load-%' \
         ORDER BY key \
         LIMIT 1025",
    )
    .bind(HASH_E)
    .execute(&pool)
    .await
    .expect("seed one-over per-torrent source cardinality");
    let source_limit_error = load_writer_torrents(
        &pool,
        &ProcessTorrentParams {
            info_hashes: vec![ProtocolId::from_hex(HASH_E).expect("fixture hash E")],
            ..ProcessTorrentParams::default()
        },
    )
    .await
    .expect_err("1025th raw source row fails closed");
    assert!(matches!(
        source_limit_error,
        WriterLoadError::TooManySourcesForTorrent {
            ref info_hash,
            actual: 1025,
            limit: 1024
        } if info_hash == HASH_E
    ));

    // The wrapper committed only reads: the fixture row count remains exact.
    let source_count = sqlx::query(
        "SELECT count(*)::bigint AS count FROM torrents_torrent_sources \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .fetch_one(&pool)
    .await
    .expect("count unchanged source fixtures")
    .try_get::<i64, _>("count")
    .expect("decode source count");
    assert_eq!(source_count, 2);
}
