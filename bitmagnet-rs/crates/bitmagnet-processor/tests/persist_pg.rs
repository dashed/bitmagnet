//! Live-PostgreSQL gate for the Lane P transaction kernel.
//!
//! Set `BITMAGNET_PROCESSOR_TEST_DATABASE_URL` to a disposable goose-29
//! database. The test truncates processor-owned rows and must never target a
//! production database.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use bitmagnet_processor::{
    load_torrents, persist_write_set, read_live_snapshot, BlockingManager, BoxError,
    LiveTorrentState, ShadowRuntime, TorrentContentPersistence, TorrentContentWrite, WriteSet,
};
use bitmagnet_queue::{
    DequeuedJob, ProcessTorrentParams, ProtocolId, QueueJobStatus, PROCESS_TORRENT_SHADOW,
};
use futures::future::BoxFuture;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};

const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const HASH_C: &str = "cccccccccccccccccccccccccccccccccccccccc";

struct RecordingBlocker {
    pool: PgPool,
    calls: Arc<Mutex<Vec<Vec<String>>>>,
    observed_before_delete: Arc<Mutex<bool>>,
}

impl BlockingManager for RecordingBlocker {
    fn block<'a>(&'a self, info_hashes: &'a [String]) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("blocker calls mutex")
                .push(info_hashes.to_vec());
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM torrents WHERE info_hash = decode($1, 'hex')",
            )
            .bind(HASH_C)
            .fetch_one(&self.pool)
            .await?;
            *self
                .observed_before_delete
                .lock()
                .expect("blocker observation mutex") = count == 1;
            Ok(())
        })
    }
}

fn torrent_content(info_hash: &str, content_type: &str, languages: &[&str]) -> TorrentContentWrite {
    TorrentContentWrite {
        id: format!("{info_hash}:{content_type}:?:?"),
        info_hash: info_hash.to_owned(),
        content_type: Some(content_type.to_owned()),
        content_source: None,
        content_id: None,
        languages: languages.iter().map(|value| (*value).to_owned()).collect(),
        episodes: "S01E01-02".to_owned(),
        video_resolution: Some("V1080p".to_owned()),
        video_source: Some("BluRay".to_owned()),
        video_codec: Some("x265".to_owned()),
        video_3d: None,
        video_modifier: None,
        release_group: Some("group".to_owned()),
        size: 42,
        files_count: Some(2),
    }
}

fn metadata(rows: &[TorrentContentWrite]) -> BTreeMap<String, TorrentContentPersistence> {
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            (
                row.id.clone(),
                TorrentContentPersistence {
                    seeders: Some(10 + index as u64),
                    leechers: Some(2 + index as u64),
                    published_at_micros: 1_700_000_000_123_456 + index as i64,
                    tsv: "'fixture':1A".to_owned(),
                },
            )
        })
        .collect()
}

async fn seed_torrent(pool: &PgPool, info_hash: &str, name: &str) {
    sqlx::query(
        "INSERT INTO torrents \
         (info_hash, name, size, private, created_at, updated_at) \
         VALUES (decode($1, 'hex'), $2, 42, false, NOW(), NOW())",
    )
    .bind(info_hash)
    .bind(name)
    .execute(pool)
    .await
    .expect("seed torrent");
}

fn assert_insufficient_privilege(error: sqlx::Error) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL permission error, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some("42501"));
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable goose-29 PostgreSQL"]
async fn transaction_order_upserts_tags_deletes_and_rolls_back() {
    let Ok(database_url) = std::env::var("BITMAGNET_PROCESSOR_TEST_DATABASE_URL") else {
        eprintln!("skipping: BITMAGNET_PROCESSOR_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect disposable PostgreSQL");

    sqlx::query("TRUNCATE torrent_tags, torrent_contents, torrents CASCADE")
        .execute(&pool)
        .await
        .expect("reset processor tables");
    seed_torrent(&pool, HASH_A, "A").await;
    seed_torrent(&pool, HASH_B, "B").await;
    seed_torrent(&pool, HASH_C, "C").await;

    sqlx::query(
        "INSERT INTO torrent_contents \
         (info_hash, content_type, languages, episodes, created_at, updated_at, tsv, \
          published_at, size) \
         VALUES \
         (decode($1, 'hex'), NULL, '[]'::jsonb, '{}'::jsonb, NOW(), NOW(), \
          ''::tsvector, NOW(), 1), \
         (decode($2, 'hex'), 'music', '[]'::jsonb, '{}'::jsonb, NOW(), NOW(), \
          ''::tsvector, NOW(), 1)",
    )
    .bind(HASH_A)
    .bind(HASH_B)
    .execute(&pool)
    .await
    .expect("seed existing contents");

    let rows = vec![
        torrent_content(HASH_A, "movie", &["en"]),
        torrent_content(HASH_B, "music", &["fr", "en"]),
    ];
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed_before_delete = Arc::new(Mutex::new(false));
    let blocker = RecordingBlocker {
        pool: pool.clone(),
        calls: Arc::clone(&calls),
        observed_before_delete: Arc::clone(&observed_before_delete),
    };
    let write_set = WriteSet {
        torrent_contents: rows.clone(),
        delete_ids: vec![format!("{HASH_A}:?:?:?")],
        delete_info_hashes: vec![HASH_C.to_owned()],
        add_tags: BTreeMap::from([(
            HASH_A.to_owned(),
            vec!["trusted".to_owned(), "trusted".to_owned()],
        )]),
        ..WriteSet::default()
    };

    persist_write_set(&pool, &write_set, &metadata(&rows), &blocker)
        .await
        .expect("persist supported write set");

    assert_eq!(
        calls.lock().expect("calls mutex").as_slice(),
        &[vec![HASH_C.to_owned()]]
    );
    assert!(*observed_before_delete.lock().expect("observation mutex"));

    let live = sqlx::query(
        "SELECT id, languages, episodes, seeders, leechers, \
         (EXTRACT(EPOCH FROM published_at) * 1000000)::bigint AS published_at_micros, \
         size, files_count, tsv::text AS tsv \
         FROM torrent_contents ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("read persisted contents");
    assert_eq!(live.len(), 2);
    assert_eq!(live[0].try_get::<String, _>("id").unwrap(), rows[0].id);
    assert_eq!(
        live[0].try_get::<serde_json::Value, _>("episodes").unwrap(),
        serde_json::json!({"1": {"1": {}, "2": {}}})
    );
    assert_eq!(live[0].try_get::<i32, _>("seeders").unwrap(), 10);
    assert_eq!(
        live[0].try_get::<i64, _>("published_at_micros").unwrap(),
        1_700_000_000_123_456
    );
    assert_eq!(live[0].try_get::<String, _>("tsv").unwrap(), "'fixture':1A");
    assert_eq!(live[1].try_get::<i64, _>("size").unwrap(), 42);
    assert_eq!(live[1].try_get::<i32, _>("files_count").unwrap(), 2);

    let tag_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM torrent_tags WHERE name = 'trusted'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(tag_count, 1);
    let deleted_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM torrents WHERE info_hash = decode($1, 'hex')")
            .bind(HASH_C)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(deleted_count, 0);

    let live_snapshot = read_live_snapshot(
        &pool,
        &[HASH_A.to_owned(), HASH_B.to_owned(), HASH_C.to_owned()],
    )
    .await
    .expect("read non-locking live snapshot");
    let LiveTorrentState::Present(live_a) = &live_snapshot[HASH_A] else {
        panic!("hash A should be present");
    };
    assert_eq!(live_a.torrent_contents, vec![rows[0].clone()]);
    assert_eq!(live_a.tags, vec!["trusted"]);
    assert!(matches!(
        live_snapshot[HASH_C],
        LiveTorrentState::LiveAbsent
    ));

    sqlx::query(
        "CREATE OR REPLACE FUNCTION reject_processor_test_tag() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN \
         IF NEW.name = 'force-rollback' THEN RAISE EXCEPTION 'forced rollback'; END IF; \
         RETURN NEW; END $$",
    )
    .execute(&pool)
    .await
    .expect("install rollback-test trigger function");
    sqlx::query("DROP TRIGGER IF EXISTS reject_processor_test_tag ON torrent_tags")
        .execute(&pool)
        .await
        .expect("drop prior rollback-test trigger");
    sqlx::query(
        "CREATE TRIGGER reject_processor_test_tag BEFORE INSERT ON torrent_tags \
         FOR EACH ROW EXECUTE FUNCTION reject_processor_test_tag()",
    )
    .execute(&pool)
    .await
    .expect("install rollback-test trigger");

    let rollback_row = torrent_content(HASH_A, "ebook", &[]);
    let rollback = WriteSet {
        torrent_contents: vec![rollback_row.clone()],
        delete_ids: vec![rows[0].id.clone()],
        add_tags: BTreeMap::from([(HASH_A.to_owned(), vec!["force-rollback".to_owned()])]),
        ..WriteSet::default()
    };
    let error = persist_write_set(
        &pool,
        &rollback,
        &metadata(std::slice::from_ref(&rollback_row)),
        &blocker,
    )
    .await
    .expect_err("invalid tag must roll back the transaction");
    assert!(error.to_string().contains("database"));

    let surviving_ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM torrent_contents ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        surviving_ids,
        vec![rows[0].id.clone(), rows[1].id.clone()],
        "the stale delete and preceding upsert must roll back with the tag failure"
    );

    sqlx::query(
        "DO $$ BEGIN \
         IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'bitmagnet_shadow_test') THEN \
           EXECUTE 'DROP OWNED BY bitmagnet_shadow_test'; \
           EXECUTE 'DROP ROLE bitmagnet_shadow_test'; \
         END IF; END $$",
    )
    .execute(&pool)
    .await
    .expect("remove prior shadow test role");
    sqlx::query(
        "CREATE ROLE bitmagnet_shadow_test LOGIN PASSWORD 'shadow-test-password' \
         NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
    )
    .execute(&pool)
    .await
    .expect("create shadow test role");
    sqlx::query("GRANT USAGE ON SCHEMA public TO bitmagnet_shadow_test")
        .execute(&pool)
        .await
        .expect("grant schema usage");
    sqlx::query(
        "GRANT SELECT ON content, torrent_contents, torrent_tags, torrents, torrent_hints, \
           queue_jobs \
         TO bitmagnet_shadow_test",
    )
    .execute(&pool)
    .await
    .expect("grant bounded shadow reads");
    sqlx::query(
        "GRANT EXECUTE ON FUNCTION \
           public.ingest_shadow_lock_cursor(boolean, timestamptz, text), \
           public.ingest_shadow_advance_cursor(timestamptz, text), \
           public.ingest_shadow_enqueue_job(text, jsonb, bigint, bigint, bigint, integer), \
           public.ingest_shadow_claim_job(), \
           public.ingest_shadow_settle_processed(text, bigint), \
           public.ingest_shadow_settle_retry(text, bigint, text, bigint), \
           public.ingest_shadow_settle_failed(text, bigint, text) \
         TO bitmagnet_shadow_test",
    )
    .execute(&pool)
    .await
    .expect("grant reviewed shadow queue capabilities");

    let shadow_options = PgConnectOptions::from_str(&database_url)
        .expect("parse PostgreSQL URL")
        .username("bitmagnet_shadow_test")
        .password("shadow-test-password");
    let shadow_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(shadow_options)
        .await
        .expect("connect as SELECT-only shadow role");

    let role_snapshot = read_live_snapshot(
        &shadow_pool,
        &[HASH_A.to_owned(), HASH_B.to_owned(), HASH_C.to_owned()],
    )
    .await
    .expect("shadow role can read the stable live image");
    assert_eq!(role_snapshot, live_snapshot);
    let loaded = load_torrents(
        &shadow_pool,
        &ProcessTorrentParams {
            info_hashes: vec![
                ProtocolId::from_hex(HASH_A).unwrap(),
                ProtocolId::from_hex(HASH_B).unwrap(),
                ProtocolId::from_hex(HASH_C).unwrap(),
            ],
            ..ProcessTorrentParams::default()
        },
    )
    .await
    .expect("shadow role can hydrate the classifier image");
    assert_eq!(loaded.len(), 2, "the deleted hash remains a missing input");

    sqlx::query(
        "UPDATE torrents \
         SET name = 'Synthetic Story.m4b', size = 60000000, files_status = 'single' \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .execute(&pool)
    .await
    .expect("prepare a classifier-supported live shadow input");
    let params = ProcessTorrentParams {
        classifier_workflow: "default".into(),
        classifier_flags: Some(BTreeMap::from([
            ("local_search_enabled".into(), serde_json::json!(false)),
            ("apis_enabled".into(), serde_json::json!(false)),
            ("tmdb_enabled".into(), serde_json::json!(false)),
        ])),
        info_hashes: vec![ProtocolId::from_hex(HASH_A).unwrap()],
        ..ProcessTorrentParams::default()
    };
    let digest =
        bitmagnet_classifier::core_config_digest().expect("digest embedded classifier config");
    let runtime = ShadowRuntime::from_core(shadow_pool.clone(), Some(&digest))
        .expect("compile shadow runtime");
    let comparison = runtime
        .process_job(&DequeuedJob {
            id: "shadow-runtime-test".into(),
            fingerprint: "shadow-runtime-test".into(),
            queue: PROCESS_TORRENT_SHADOW.into(),
            original_status: QueueJobStatus::Pending,
            payload: serde_json::to_string(&params).unwrap(),
            retries: 0,
            max_retries: 2,
            priority: 0,
        })
        .await
        .expect("run the read-only shadow pipeline end to end");
    assert_eq!(comparison.torrents.len(), 1);
    assert_eq!(comparison.mismatch_count(), 1);

    let live_write_error =
        sqlx::query("UPDATE torrents SET name = name WHERE info_hash = decode($1, 'hex')")
            .bind(HASH_A)
            .execute(&shadow_pool)
            .await
            .expect_err("shadow role must not mutate live torrents");
    assert_insufficient_privilege(live_write_error);

    let attributes_read_error = sqlx::query("SELECT count(*) FROM content_attributes")
        .execute(&shadow_pool)
        .await
        .expect_err("frozen role must not gain undeclared live-table access");
    assert_insufficient_privilege(attributes_read_error);

    shadow_pool.close().await;
    sqlx::query("DROP OWNED BY bitmagnet_shadow_test")
        .execute(&pool)
        .await
        .expect("remove shadow test grants");
    sqlx::query("DROP ROLE bitmagnet_shadow_test")
        .execute(&pool)
        .await
        .expect("drop shadow test role");
}
