//! Live-PostgreSQL gate for the Lane P transaction kernel.
//!
//! Set `BITMAGNET_PROCESSOR_TEST_DATABASE_URL` to the exact disposable
//! Goose-34 database named `bitmagnet_processor_writer_load_test`. The test
//! truncates processor-owned rows and refuses every other database name.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use bitmagnet_processor::{
    load_torrents, load_writer_plan, persist_write_set, read_live_snapshot, BlockingManager,
    BoxError, LiveTorrentState, Materializer, ShadowRuntime, TorrentContentPersistence,
    TorrentContentWrite, WriteSet, WriterDriftField,
};
use bitmagnet_queue::{
    DequeuedJob, ProcessTorrentParams, ProtocolId, QueueJobStatus, ShadowJobEnvelopeV1,
    PROCESS_TORRENT_SHADOW,
};
use futures::future::BoxFuture;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};

const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const HASH_C: &str = "cccccccccccccccccccccccccccccccccccccccc";
const TEST_DATABASE_NAME: &str = "bitmagnet_processor_writer_load_test";

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
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at the exact disposable Goose-34 database"]
async fn transaction_order_upserts_tags_deletes_and_rolls_back() {
    let Ok(database_url) = std::env::var("BITMAGNET_PROCESSOR_TEST_DATABASE_URL") else {
        eprintln!("skipping: BITMAGNET_PROCESSOR_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect disposable PostgreSQL");
    let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("read disposable database sentinel");
    assert_eq!(
        database_name, TEST_DATABASE_NAME,
        "ignored persistence gate refuses a database without its exact disposable-name sentinel"
    );

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
           torrents_torrent_sources, queue_jobs \
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
    let runtime_materializer = Materializer::from_core().expect("compile runtime classifier");
    let runtime_plan = load_writer_plan(&pool, &runtime_materializer, &params)
        .await
        .expect("compose exact runtime writer plan for the PostgreSQL fixture");
    assert_eq!(runtime_plan.persistence().len(), 1);
    let (writer_id, writer_metadata) = runtime_plan
        .persistence()
        .first_key_value()
        .map(|(id, metadata)| (id.clone(), metadata.clone()))
        .expect("one runtime writer row");
    let runtime_calls = Arc::new(Mutex::new(Vec::new()));
    let runtime_observed = Arc::new(Mutex::new(false));
    let runtime_blocker = RecordingBlocker {
        pool: pool.clone(),
        calls: Arc::clone(&runtime_calls),
        observed_before_delete: Arc::clone(&runtime_observed),
    };
    let writer_row_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT FROM torrent_contents WHERE id = $1)")
            .bind(&writer_id)
            .fetch_one(&pool)
            .await
            .expect("check the missing-row insert precondition");
    assert!(
        !writer_row_exists,
        "writer projection starts as a missing row"
    );
    persist_write_set(
        &pool,
        runtime_plan.write_set(),
        runtime_plan.persistence(),
        &runtime_blocker,
    )
    .await
    .expect("seed the exact live writer projection");
    let inserted_published_at_micros: i64 = sqlx::query_scalar(
        "SELECT (EXTRACT(EPOCH FROM published_at) * 1000000)::bigint \
         FROM torrent_contents WHERE id = $1",
    )
    .bind(&writer_id)
    .fetch_one(&pool)
    .await
    .expect("read the missing-row insert projection");
    assert_eq!(
        inserted_published_at_micros, writer_metadata.published_at_micros,
        "a missing row receives the projected published_at"
    );

    let preserved_published_at_micros = writer_metadata.published_at_micros + 31_536_000_000_000;
    sqlx::query(
        "UPDATE torrent_contents \
         SET published_at = $2 * interval '1 microsecond' + timestamptz 'epoch' \
         WHERE id = $1",
    )
    .bind(&writer_id)
    .bind(preserved_published_at_micros)
    .execute(&pool)
    .await
    .expect("seed a pre-existing published_at sentinel");
    persist_write_set(
        &pool,
        runtime_plan.write_set(),
        runtime_plan.persistence(),
        &runtime_blocker,
    )
    .await
    .expect("exercise the conflict-update path over the sentinel");
    let conflicted_published_at_micros: i64 = sqlx::query_scalar(
        "SELECT (EXTRACT(EPOCH FROM published_at) * 1000000)::bigint \
         FROM torrent_contents WHERE id = $1",
    )
    .bind(&writer_id)
    .fetch_one(&pool)
    .await
    .expect("read the conflict-preserved published_at sentinel");
    assert_eq!(
        conflicted_published_at_micros, preserved_published_at_micros,
        "conflicts preserve the existing published_at like Go/GORM UpdateAll"
    );

    let stale_write_set = WriteSet {
        torrent_contents: vec![rows[0].clone()],
        ..WriteSet::default()
    };
    persist_write_set(
        &pool,
        &stale_write_set,
        &metadata(&stale_write_set.torrent_contents),
        &runtime_blocker,
    )
    .await
    .expect("restore one stale stable row outside writer-comparison ownership");

    let digest =
        bitmagnet_classifier::core_config_digest().expect("digest embedded classifier config");
    let source_payload = serde_json::to_value(&params).expect("encode source payload");
    let source_ran_at: String = sqlx::query_scalar(
        "INSERT INTO queue_jobs \
           (id, fingerprint, queue, status, payload, retries, max_retries, run_after, \
            ran_at, archival_duration, created_at, priority) \
         VALUES \
           ('shadow-runtime-source', 'shadow-runtime-source', 'process_torrent', \
            'processed', $1::jsonb, 0, 2, clock_timestamp(), clock_timestamp(), \
            interval '1 hour', clock_timestamp(), 0) \
         RETURNING ran_at::text",
    )
    .bind(serde_json::to_string(&source_payload).expect("serialize source payload"))
    .fetch_one(&pool)
    .await
    .expect("seed exact runtime source job");
    let envelope = ShadowJobEnvelopeV1::new(
        "shadow-runtime-source".to_owned(),
        source_ran_at.clone(),
        source_payload,
    );
    let runtime = ShadowRuntime::from_core(shadow_pool.clone(), Some(&digest))
        .expect("compile shadow runtime");
    let shadow_job = DequeuedJob {
        id: "shadow-runtime-test".into(),
        fingerprint: "shadow-runtime-test".into(),
        queue: PROCESS_TORRENT_SHADOW.into(),
        original_status: QueueJobStatus::Pending,
        payload: serde_json::to_string(&envelope).unwrap(),
        retries: 0,
        max_retries: 2,
        priority: 0,
    };
    let comparison = runtime
        .process_job(&shadow_job)
        .await
        .expect("run the read-only shadow pipeline end to end");
    assert_eq!(comparison.source_job_id, envelope.source_job_id);
    assert_eq!(comparison.source_ran_at, source_ran_at);
    assert_eq!(comparison.stable.torrents.len(), 1);
    assert_eq!(comparison.stable.mismatch_count(), 1);
    assert!(comparison.writer.is_match());
    assert_eq!(comparison.writer.rows[0].id, writer_id);
    assert!(writer_metadata.seeders.is_none());
    assert!(writer_metadata.leechers.is_none());
    let exact_writer_row = sqlx::query(
        "SELECT seeders, leechers, \
           (EXTRACT(EPOCH FROM published_at) * 1000000)::bigint \
             AS published_at_micros, \
           tsv::text AS tsv_text, $2::tsvector::text AS expected_tsv_text, \
           tsv = $2::tsvector AS tsv_matches \
         FROM torrent_contents WHERE id = $1",
    )
    .bind(&writer_id)
    .bind(&writer_metadata.tsv)
    .fetch_one(&pool)
    .await
    .expect("read the exact persisted writer row");
    assert!(exact_writer_row
        .try_get::<Option<i32>, _>("seeders")
        .unwrap()
        .is_none());
    assert!(exact_writer_row
        .try_get::<Option<i32>, _>("leechers")
        .unwrap()
        .is_none());
    assert_eq!(
        exact_writer_row
            .try_get::<i64, _>("published_at_micros")
            .unwrap(),
        preserved_published_at_micros
    );
    assert_eq!(
        exact_writer_row.try_get::<String, _>("tsv_text").unwrap(),
        exact_writer_row
            .try_get::<String, _>("expected_tsv_text")
            .unwrap()
    );
    assert!(exact_writer_row.try_get::<bool, _>("tsv_matches").unwrap());

    sqlx::query(
        "UPDATE torrent_contents \
         SET seeders = coalesce(seeders, 0) + 1, \
             leechers = coalesce(leechers, 0) + 1, \
             published_at = published_at + interval '1 microsecond', \
             tsv = ''::tsvector, updated_at = $2::timestamptz \
         WHERE id = $1",
    )
    .bind(&writer_id)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("drift comparable writer fields and published_at within the causal timestamp");
    let writer_drift = runtime
        .process_job(&shadow_job)
        .await
        .expect("writer-field drift remains comparable");
    assert_eq!(
        writer_drift.writer.rows[0].drift_fields,
        vec![
            WriterDriftField::Seeders,
            WriterDriftField::Leechers,
            WriterDriftField::Tsv,
        ]
    );
    sqlx::query(
        "UPDATE torrent_contents \
         SET seeders = $2, leechers = $3, \
             tsv = $4::tsvector, updated_at = $5::timestamptz \
         WHERE id = $1",
    )
    .bind(&writer_id)
    .bind(
        writer_metadata
            .seeders
            .map(|value| i32::try_from(value).expect("validated seeders fit i32")),
    )
    .bind(
        writer_metadata
            .leechers
            .map(|value| i32::try_from(value).expect("validated leechers fit i32")),
    )
    .bind(&writer_metadata.tsv)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("restore exact writer fields through the persistence cast contract");
    sqlx::query("DELETE FROM torrent_contents WHERE id = $1")
        .bind(&writer_id)
        .execute(&pool)
        .await
        .expect("remove the expected writer row");
    let missing_writer = runtime
        .process_job(&shadow_job)
        .await
        .expect("a deleted expected row remains comparable live-state evidence");
    assert_eq!(
        missing_writer.writer.rows[0].drift_fields,
        vec![WriterDriftField::RowPresence]
    );

    let mut missing_source = envelope.clone();
    missing_source.source_job_id = "missing-source".to_owned();
    let mut missing_job = shadow_job.clone();
    missing_job.payload = serde_json::to_string(&missing_source).unwrap();
    let missing_error = runtime
        .process_job(&missing_job)
        .await
        .expect_err("an envelope without its exact source row is non-comparable");
    assert!(matches!(
        missing_error,
        bitmagnet_processor::ShadowRuntimeError::SourceJobChangedOrMissing
    ));

    sqlx::query(
        "UPDATE torrents SET updated_at = $2::timestamptz + interval '1 microsecond' \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("make the source torrent newer than its captured run");
    let changed_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("post-run torrent changes are non-comparable");
    assert!(matches!(
        changed_error,
        bitmagnet_processor::ShadowRuntimeError::SourceTorrentUpdatedAfterRun
    ));
    sqlx::query(
        "UPDATE torrents SET updated_at = $2::timestamptz \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("restore the captured source boundary");

    let hinted_content_type = "xxx";
    sqlx::query(
        "INSERT INTO torrent_hints \
           (info_hash, content_type, created_at, updated_at) \
         VALUES (decode($1, 'hex'), $2, \
                 $3::timestamptz - interval '1 microsecond', \
                 $3::timestamptz - interval '1 microsecond')",
    )
    .bind(HASH_A)
    .bind(hinted_content_type)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("seed unchanged bare type-only hint");
    let hinted_comparison = runtime
        .process_job(&shadow_job)
        .await
        .expect("an unchanged bare type-only hint is comparable end to end");
    assert_eq!(
        hinted_comparison.writer.rows[0].id,
        format!("{HASH_A}:xxx:?:?"),
        "the explicit bare hint must override the fixture's inferred movie type"
    );
    assert_eq!(
        hinted_comparison.writer.rows[0].drift_fields,
        vec![WriterDriftField::RowPresence],
        "the hint-derived writer ID is intentionally absent from the live fixture"
    );
    sqlx::query(
        "UPDATE torrent_hints \
         SET updated_at=$2::timestamptz + interval '1 microsecond' \
         WHERE info_hash=decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("move the bare hint past source settlement");
    let changed_hint_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("a post-source bare hint is non-comparable");
    assert!(matches!(
        changed_hint_error,
        bitmagnet_processor::ShadowRuntimeError::SourceHintUpdatedAfterRun
    ));
    sqlx::query(
        "UPDATE torrent_hints \
         SET release_group='ENRICHED', updated_at=$2::timestamptz \
         WHERE info_hash=decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("enrich the explicit hint within the causal timestamp");
    let enriched_hint_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("an enriched hint remains outside the supported subset");
    assert!(matches!(
        enriched_hint_error,
        bitmagnet_processor::ShadowRuntimeError::SourceHintUnsupported
    ));
    sqlx::query(
        "UPDATE torrent_hints \
         SET release_group=NULL, content_source='tmdb', content_id='603', \
             updated_at=$2::timestamptz \
         WHERE info_hash=decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("source the explicit hint within the causal timestamp");
    let sourced_hint_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("a sourced hint remains outside the supported subset");
    assert!(matches!(
        sourced_hint_error,
        bitmagnet_processor::ShadowRuntimeError::SourceHintUnsupported
    ));
    sqlx::query("DELETE FROM torrent_hints WHERE info_hash=decode($1, 'hex')")
        .bind(HASH_A)
        .execute(&pool)
        .await
        .expect("remove explicit-hint causal fixtures");

    sqlx::query(
        "INSERT INTO torrents_torrent_sources \
           (source, info_hash, seeders, leechers, created_at, updated_at) \
         VALUES \
           ('dht', decode($1, 'hex'), 1, 2, $2::timestamptz, \
            $2::timestamptz + interval '1 microsecond')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("seed a source row newer than the captured run");
    let source_changed_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("post-run source-row changes are non-comparable");
    assert!(matches!(
        source_changed_error,
        bitmagnet_processor::ShadowRuntimeError::SourceRowsUpdatedAfterRun
    ));
    sqlx::query(
        "DELETE FROM torrents_torrent_sources \
         WHERE info_hash = decode($1, 'hex') AND source = 'dht'",
    )
    .bind(HASH_A)
    .execute(&pool)
    .await
    .expect("remove the post-run source-row fixture");

    sqlx::query(
        "UPDATE torrent_contents \
         SET updated_at = $2::timestamptz + interval '1 microsecond' \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("make the content row newer than its captured run");
    let content_changed_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("post-run content-row changes are non-comparable");
    assert!(matches!(
        content_changed_error,
        bitmagnet_processor::ShadowRuntimeError::TorrentContentUpdatedAfterRun
    ));
    sqlx::query(
        "UPDATE torrent_contents SET updated_at = $2::timestamptz \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("restore the captured content-row boundary");

    sqlx::query(
        "UPDATE torrent_tags \
         SET updated_at = $2::timestamptz + interval '1 microsecond' \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("make the tag row newer than its captured run");
    let tag_changed_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("post-run tag-row changes are non-comparable");
    assert!(matches!(
        tag_changed_error,
        bitmagnet_processor::ShadowRuntimeError::TorrentTagUpdatedAfterRun
    ));
    sqlx::query(
        "UPDATE torrent_tags SET updated_at = $2::timestamptz \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_A)
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("restore the captured tag-row boundary");

    sqlx::query(
        "INSERT INTO queue_jobs \
           (id, fingerprint, queue, status, payload, retries, max_retries, run_after, \
            ran_at, archival_duration, created_at, priority) \
         VALUES \
           ('shadow-runtime-later', 'shadow-runtime-later', 'process_torrent', \
            'failed', $1::jsonb, 0, 2, clock_timestamp(), $2::timestamptz, \
            interval '1 hour', clock_timestamp(), 0)",
    )
    .bind(serde_json::to_string(&envelope.source_payload).unwrap())
    .bind(&source_ran_at)
    .execute(&pool)
    .await
    .expect("seed a same-timestamp overlapping source attempt");
    let overlap_error = runtime
        .process_job(&shadow_job)
        .await
        .expect_err("a same-timestamp overlapping attempt is non-comparable");
    assert!(matches!(
        overlap_error,
        bitmagnet_processor::ShadowRuntimeError::LaterOverlappingSourceAttempt
    ));
    for status in ["pending", "retry"] {
        sqlx::query(
            "UPDATE queue_jobs \
             SET status = $1::queue_job_status, ran_at = NULL, \
                 run_after = clock_timestamp() + interval '1 year' \
             WHERE id = 'shadow-runtime-later'",
        )
        .bind(status)
        .execute(&pool)
        .await
        .expect("make the overlapping attempt nonterminal and not yet due");
        let active_overlap_error = runtime
            .process_job(&shadow_job)
            .await
            .expect_err("an overlapping nonterminal attempt is non-comparable");
        assert!(matches!(
            active_overlap_error,
            bitmagnet_processor::ShadowRuntimeError::LaterOverlappingSourceAttempt
        ));
    }

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
