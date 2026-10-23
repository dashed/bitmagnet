//! Disposable-PostgreSQL gate for the persisting `process_torrent` worker.
//!
//! Every test truncates queue, torrent, and content tables. Each requires the
//! exact disposable database name `bitmagnet_processor_writer_load_test` and
//! explicit invocation with `--ignored --test-threads=1`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bitmagnet_blocking::BlockingManager as PersistentBlockingManager;
use bitmagnet_classifier::resolver::tmdb;
use bitmagnet_classifier::{ContentResolver, ContentResultItem, ResolveError};
use bitmagnet_content_search::PgContentSearch;
use bitmagnet_model::{Content, ContentType, InfoHash};
use bitmagnet_processor::{
    load_writer_plan, persist_writer_plan, Materializer, ProcessTorrentWorker,
    ProcessTorrentWorkerConfig, WriterPlanError,
};
use bitmagnet_queue::{
    backoff, process_torrent_job, AdmittedConsumeOutcome, ConsumeOutcome, JobVerdict,
    ProcessTorrentAdmission, ProcessTorrentParams, ProtocolId, QueueJobOptions, QueueStore,
    ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL, ADMITTED_PROCESS_TORRENT_CLAIM_SQL,
    PROCESS_TORRENT,
};
use bitmagnet_resolver::LiveContentResolver;
use bitmagnet_tmdb::{HttpResponse, RetryPolicy, TmdbClient, TmdbConfig, Transport};
use reqwest::Url;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const TEST_DATABASE_URL: &str = "BITMAGNET_PROCESSOR_TEST_DATABASE_URL";
const TEST_DATABASE_NAME: &str = "bitmagnet_processor_writer_load_test";
const HASH_A: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const HASH_B: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";
const HASH_MISSING: &str = "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0";

async fn connect() -> PgPool {
    let database_url = std::env::var(TEST_DATABASE_URL)
        .unwrap_or_else(|_| panic!("{TEST_DATABASE_URL} must be set for ignored gate"));
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .expect("connect disposable processor PostgreSQL");
    let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("read disposable database sentinel");
    assert_eq!(
        database_name, TEST_DATABASE_NAME,
        "ignored worker gate refuses a database without its exact disposable-name sentinel"
    );
    pool
}

async fn reset(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE queue_jobs, torrent_tags, torrent_contents, torrents, content, \
         content_collections, bloom_filters CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset worker fixture tables");
}

/// Seed one torrent with deterministic timestamps and one source row, so the
/// writer projection (seeders/leechers/published_at) is reproducible.
async fn seed_torrent(pool: &PgPool, info_hash: &str, name: &str, size: i64) {
    sqlx::query(
        "INSERT INTO torrents \
         (info_hash, name, size, private, files_status, created_at, updated_at) \
         VALUES (decode($1, 'hex'), $2, $3, false, 'single', \
                 '2024-01-02 03:04:05.123456+00', '2024-01-02 03:04:05.123456+00')",
    )
    .bind(info_hash)
    .bind(name)
    .bind(size)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed torrent {info_hash}: {error}"));
    sqlx::query(
        "INSERT INTO torrents_torrent_sources \
         (source, info_hash, seeders, leechers, published_at, created_at, updated_at) \
         VALUES ('dht', decode($1, 'hex'), 7, 3, NULL, \
                 '2024-01-02 03:04:05.123456+00', '2024-01-02 03:04:05.123456+00')",
    )
    .bind(info_hash)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed torrent source {info_hash}: {error}"));
}

async fn seed_fixture_torrents(pool: &PgPool) {
    seed_torrent(pool, HASH_A, "Synthetic Story.m4b", 60_000_000).await;
    seed_torrent(
        pool,
        HASH_B,
        "Some.Show.S02E05.1080p.WEB-DL.x264-GROUP.mkv",
        1_400_000_000,
    )
    .await;
}

fn supported_params(info_hashes: &[&str]) -> ProcessTorrentParams {
    ProcessTorrentParams {
        classifier_workflow: "default".to_owned(),
        classifier_flags: Some(BTreeMap::from([
            ("apis_enabled".to_owned(), Value::Bool(false)),
            ("local_search_enabled".to_owned(), Value::Bool(false)),
            ("tmdb_enabled".to_owned(), Value::Bool(false)),
        ])),
        info_hashes: info_hashes
            .iter()
            .map(|hash| ProtocolId::from_hex(hash).expect("fixture hash"))
            .collect(),
        ..ProcessTorrentParams::default()
    }
}

/// Insert a job exactly as Go's `processor.NewQueueJob` would build it.
async fn enqueue(store: &QueueStore, params: &ProcessTorrentParams) -> String {
    let job = process_torrent_job(params, QueueJobOptions::default()).expect("build queue job");
    match store
        .insert_job_idempotent(&job)
        .await
        .expect("insert queue job")
    {
        bitmagnet_queue::ActiveJobInsertReceipt::Inserted { id } => id,
        other => panic!("fixture job unexpectedly deduplicated: {other:?}"),
    }
}

async fn enqueue_raw(pool: &PgPool, payload: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, max_retries, \
         run_after, archival_duration, created_at, priority) \
         VALUES (md5($1), 'process_torrent', 'pending', $1::jsonb, 0, 2, \
                 clock_timestamp() - interval '1 second', interval '168:00:00', \
                 clock_timestamp(), 0) \
         RETURNING id",
    )
    .bind(payload)
    .fetch_one(pool)
    .await
    .expect("insert raw queue job")
}

fn worker(pool: &PgPool, config: ProcessTorrentWorkerConfig) -> ProcessTorrentWorker {
    ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core().expect("compile embedded classifier"),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        config,
    )
}

fn fast_config() -> ProcessTorrentWorkerConfig {
    ProcessTorrentWorkerConfig {
        check_interval: Duration::from_millis(50),
        ..ProcessTorrentWorkerConfig::default()
    }
}

#[derive(Debug, PartialEq)]
struct JobRow {
    status: String,
    retries: i32,
    error: Option<String>,
    ran_at_set: bool,
    run_after_minus_ran_at_seconds: Option<f64>,
    run_after: String,
}

async fn job_row(pool: &PgPool, id: &str) -> JobRow {
    let row = sqlx::query(
        "SELECT status::text AS status, retries, error, ran_at IS NOT NULL AS ran_at_set, \
                EXTRACT(EPOCH FROM run_after - ran_at)::float8 AS delta, \
                run_after::text AS run_after \
         FROM queue_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read queue job");
    JobRow {
        status: row.get("status"),
        retries: row.get("retries"),
        error: row.get("error"),
        ran_at_set: row.get("ran_at_set"),
        run_after_minus_ran_at_seconds: row.get("delta"),
        run_after: row.get("run_after"),
    }
}

async fn make_ready(pool: &PgPool, id: &str) {
    sqlx::query(
        "UPDATE queue_jobs SET run_after = clock_timestamp() - interval '1 second' WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("make job ready");
}

/// The processor-owned live image, without the per-write timestamps.
async fn live_image(pool: &PgPool) -> Vec<String> {
    let mut image = Vec::new();
    for sql in [
        "SELECT 'tc ' || (to_jsonb(t) - 'created_at' - 'updated_at')::text \
         FROM torrent_contents t ORDER BY id",
        "SELECT 'tag ' || encode(info_hash, 'hex') || ' ' || name \
         FROM torrent_tags ORDER BY info_hash, name",
        "SELECT 'content ' || (to_jsonb(c) - 'created_at' - 'updated_at')::text \
         FROM content c ORDER BY type, source, id",
        "SELECT 'torrent ' || encode(info_hash, 'hex') FROM torrents ORDER BY info_hash",
    ] {
        image.extend(
            sqlx::query_scalar::<_, String>(sql)
                .fetch_all(pool)
                .await
                .unwrap_or_else(|error| panic!("read live image `{sql}`: {error}")),
        );
    }
    image
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn success_persists_exactly_what_the_direct_writer_path_persists() {
    let pool = connect().await;
    let store = QueueStore::new(pool.clone());
    let params = supported_params(&[HASH_A, HASH_B]);

    // Reference: the existing load + persist path, called directly.
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let materializer = Materializer::from_core().expect("compile embedded classifier");
    let plan = load_writer_plan(&pool, &materializer, &params)
        .await
        .expect("compose writer plan");
    assert_eq!(plan.write_set().torrent_contents.len(), 2);
    persist_writer_plan(
        &pool,
        &plan,
        &store,
        &PersistentBlockingManager::new(pool.clone()),
    )
    .await
    .expect("persist writer plan directly");
    let direct = live_image(&pool).await;
    assert!(direct.iter().filter(|row| row.starts_with("tc ")).count() == 2);

    // Same seed, same job, through the worker.
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let id = enqueue(&store, &params).await;
    let worker = worker(&pool, fast_config());
    let outcome = worker.run_once().await.expect("run worker once");
    let AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) = outcome else {
        panic!("expected a processed job, got {outcome:?}");
    };
    assert_eq!(job.id, id);
    assert_eq!(live_image(&pool).await, direct);
    let row = job_row(&pool, &id).await;
    assert_eq!(row.status, "processed");
    assert_eq!(row.retries, 0);
    assert_eq!(row.error, None);
    assert!(row.ran_at_set);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "a full success publishes no retry job"
    );

    // Reprocessing the same input is idempotent on the stable image.
    let again = enqueue(&store, &params).await;
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == again
    ));
    assert_eq!(live_image(&pool).await, direct);
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn non_admitted_payloads_are_never_claimed_and_stay_available_to_go() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let hashes = format!("[\"{HASH_A}\"]");
    let payloads = [
        // The organic DHT crawler shape: Go resolves workflow and flags from
        // its configuration, where all three attach flags are true.
        format!("{{\"InfoHashes\":{hashes}}}"),
        format!("{{\"ClassifierWorkflow\":\"default\",\"InfoHashes\":{hashes}}}"),
        format!(
            "{{\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":true}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":\"false\"}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"custom\",\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}},\"InfoHashes\":{hashes}}}"
        ),
        // Go's json.Unmarshal also binds case-insensitive key matches, and
        // jsonb orders the lower-case key last, so Go runs workflow `custom`
        // and then merges `local_search_enabled: true` into the flags. serde
        // ignores both keys and would classify with the supported subset.
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"classifierWorkflow\":\"custom\",\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}},\"classifierFlags\":{{\"local_search_enabled\":true}},\"InfoHashes\":{hashes}}}"
        ),
        // Admission is a jsonb containment: arrays wrapping the expected
        // values must not match.
        format!(
            "{{\"ClassifierWorkflow\":[\"default\"],\"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":[{{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}}],\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":[false],\"local_search_enabled\":false,\"tmdb_enabled\":false}},\"InfoHashes\":{hashes}}}"
        ),
    ];
    let mut ids = Vec::new();
    for payload in &payloads {
        ids.push(enqueue_raw(&pool, payload).await);
    }
    let mut before = Vec::new();
    for id in &ids {
        before.push(job_row(&pool, id).await);
    }
    let worker = worker(&pool, fast_config());
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    for (id, before) in ids.iter().zip(&before) {
        assert_eq!(&job_row(&pool, id).await, before, "job {id} was touched");
    }
    assert!(live_image(&pool)
        .await
        .iter()
        .all(|row| !row.starts_with("tc ")));

    // Go's generic claim still sees every one of them, in Go's order.
    let go = QueueStore::new(pool.clone());
    let mut claimed = Vec::new();
    for _ in 0..payloads.len() {
        match go
            .consume_one(PROCESS_TORRENT, |_| async { Ok::<_, String>(()) })
            .await
            .unwrap()
        {
            ConsumeOutcome::Processed { job } => claimed.push(job.id),
            other => panic!("Go stand-in expected a job, got {other:?}"),
        }
    }
    assert_eq!(
        claimed, ids,
        "Go claims every non-admitted job in FIFO order"
    );
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn admitted_claim_order_matches_go() {
    let pool = connect().await;
    reset(&pool).await;
    let store = QueueStore::new(pool.clone());
    // Distinct payloads so fingerprints differ; none is executed for real.
    let mk = |hash: &str| supported_params(&[hash]);
    let retry_early = enqueue(&store, &mk(&"01".repeat(20))).await;
    let pending_low_priority = enqueue(&store, &mk(&"02".repeat(20))).await;
    let pending_late = enqueue(&store, &mk(&"03".repeat(20))).await;
    let pending_early = enqueue(&store, &mk(&"04".repeat(20))).await;
    let future = enqueue(&store, &mk(&"05".repeat(20))).await;
    for (id, status, priority, offset_seconds) in [
        (&retry_early, "retry", 0, -100),
        (&pending_low_priority, "pending", 5, -90),
        (&pending_late, "pending", 0, -10),
        (&pending_early, "pending", 0, -50),
        (&future, "pending", 0, 3_600),
    ] {
        sqlx::query(
            "UPDATE queue_jobs SET status = $2::queue_job_status, priority = $3, \
             run_after = clock_timestamp() + make_interval(secs => $4) WHERE id = $1",
        )
        .bind(id)
        .bind(status)
        .bind(priority)
        .bind(f64::from(offset_seconds))
        .execute(&pool)
        .await
        .expect("arrange claim order");
    }
    let mut order = Vec::new();
    loop {
        match store
            .consume_admitted_process_torrent(&[], |job| async move {
                assert_eq!(job.queue, PROCESS_TORRENT);
                JobVerdict::Complete
            })
            .await
            .unwrap()
        {
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) => {
                order.push(job.id);
            }
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty) => break,
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    assert_eq!(
        order,
        vec![
            pending_early,
            pending_late,
            pending_low_priority,
            retry_early
        ],
        "pending before retry, then priority, then run_after; future rows wait"
    );
    let retry = job_row(&pool, &order[3]).await;
    assert_eq!(
        retry.retries, 1,
        "a retry-status job increments retries when run"
    );
}

const ADMITTED_CLAIM_INDEX: &str = "queue_jobs_admitted_process_torrent_claim_idx";
const ADMITTED_FLAGS_ON_CLAIM_INDEX: &str =
    "queue_jobs_admitted_flags_on_process_torrent_claim_idx";

async fn explain_admitted_claim(pool: &PgPool, generic: bool) -> String {
    explain_claim(pool, ADMITTED_PROCESS_TORRENT_CLAIM_SQL, generic).await
}

async fn explain_claim(pool: &PgPool, claim_sql: &str, generic: bool) -> String {
    let mut tx = pool.begin().await.expect("begin plan transaction");
    let plan: Vec<String> = if generic {
        sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
            .execute(&mut *tx)
            .await
            .expect("force a generic plan");
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "PREPARE admitted_claim(text[]) AS {claim_sql}"
        )))
        .execute(&mut *tx)
        .await
        .expect("prepare the admitted claim");
        sqlx::query_scalar("EXPLAIN (COSTS OFF) EXECUTE admitted_claim('{}')")
            .fetch_all(&mut *tx)
            .await
            .expect("explain the generic admitted claim")
    } else {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "EXPLAIN (COSTS OFF) {claim_sql}"
        )))
        .bind(Vec::<String>::new())
        .fetch_all(&mut *tx)
        .await
        .expect("explain the custom admitted claim")
    };
    tx.rollback().await.expect("roll back plan transaction");
    plan.join("\n")
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn admitted_claim_reads_only_admitted_rows_behind_an_organic_backlog() {
    let pool = connect().await;
    reset(&pool).await;
    // An organic backlog ahead of every admitted row in Go's claim order.
    sqlx::query(
        "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, max_retries, \
         run_after, archival_duration, created_at, priority) \
         SELECT md5(n::text), 'process_torrent', 'pending', \
                jsonb_build_object('InfoHashes', jsonb_build_array(lpad(to_hex(n), 40, '0'))), \
                0, 2, clock_timestamp() - interval '1 hour', interval '168:00:00', \
                clock_timestamp(), 0 \
         FROM generate_series(1, 20000) AS n",
    )
    .execute(&pool)
    .await
    .expect("seed organic backlog");
    let store = QueueStore::new(pool.clone());
    let admitted = [
        enqueue(&store, &supported_params(&[HASH_A])).await,
        enqueue(&store, &supported_params(&[HASH_B])).await,
    ];
    for id in &admitted {
        make_ready(&pool, id).await;
    }
    sqlx::query("ANALYZE queue_jobs")
        .execute(&pool)
        .await
        .expect("analyze queue_jobs");

    for generic in [false, true] {
        let plan = explain_admitted_claim(&pool, generic).await;
        assert!(
            plan.contains(ADMITTED_CLAIM_INDEX),
            "the {} admitted claim must scan {ADMITTED_CLAIM_INDEX}; the claim and \
             migration 00035 predicates have drifted apart:\n{plan}",
            if generic { "generic" } else { "custom" },
        );
    }
    // The claim still takes the admitted rows, in Go's order, and leaves the
    // organic backlog to Go.
    let mut claimed = Vec::new();
    while let AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) = store
        .consume_admitted_process_torrent(&[], |_| async { JobVerdict::Complete })
        .await
        .unwrap()
    {
        claimed.push(job.id);
    }
    assert_eq!(claimed, admitted);
    let organic_pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM queue_jobs WHERE status = 'pending'")
            .fetch_one(&pool)
            .await
            .expect("count organic backlog");
    assert_eq!(organic_pending, 20_000);
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn failures_retry_with_go_backoff_and_fail_at_max_retries() {
    let pool = connect().await;
    reset(&pool).await;
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &supported_params(&[HASH_MISSING])).await;
    let worker = worker(&pool, fast_config());
    let expected_error = "job failed to process: missing 1 info hashes";

    // Pending run: retries stays 0, backoff(0) = 0^4 + 15 + 0 + 1 = 16s.
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::RetryScheduled { .. })
    ));
    let first = job_row(&pool, &id).await;
    assert_eq!(first.status, "retry");
    assert_eq!(first.retries, 0);
    assert_eq!(first.error.as_deref(), Some(expected_error));
    let delta = first.run_after_minus_ran_at_seconds.expect("retry delay");
    assert!((delta - 16.0).abs() < 0.5, "backoff(0) is 16s, got {delta}");
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));

    // First retry run: retries 1, backoff(1) in [17, 46]s.
    make_ready(&pool, &id).await;
    worker.run_once().await.unwrap();
    let second = job_row(&pool, &id).await;
    assert_eq!(second.status, "retry");
    assert_eq!(second.retries, 1);
    let envelope = backoff::envelope(1);
    let delta = second.run_after_minus_ran_at_seconds.expect("retry delay");
    assert!(
        delta >= envelope.min_seconds() as f64 - 0.5
            && delta <= envelope.max_seconds() as f64 + 0.5,
        "backoff(1) in {envelope:?}, got {delta}"
    );

    // Second retry run reaches max_retries (2): failed, no further claim.
    make_ready(&pool, &id).await;
    let outcome = worker.run_once().await.unwrap();
    let AdmittedConsumeOutcome::Settled(ConsumeOutcome::Failed { error, .. }) = outcome else {
        panic!("expected a terminal failure, got {outcome:?}");
    };
    assert_eq!(error, expected_error);
    let last = job_row(&pool, &id).await;
    assert_eq!(last.status, "failed");
    assert_eq!(last.retries, 2);
    assert_eq!(last.error.as_deref(), Some(expected_error));
    make_ready(&pool, &id).await;
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "an all-failed job publishes no replacement, like Go"
    );
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn partial_failure_publishes_gos_retry_job_and_persists_the_rest() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let store = QueueStore::new(pool.clone());
    let params = supported_params(&[HASH_MISSING, HASH_A]);
    let id = enqueue(&store, &params).await;
    let worker = worker(&pool, fast_config());
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { .. })
    ));
    assert_eq!(job_row(&pool, &id).await.status, "processed");
    let retry_params = supported_params(&[HASH_MISSING]);
    let expected = process_torrent_job(&retry_params, QueueJobOptions::default()).unwrap();
    let row = sqlx::query(
        "SELECT fingerprint, payload::text AS payload, status::text AS status, retries, \
                max_retries, priority \
         FROM queue_jobs WHERE id <> $1",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .expect("exactly one retry job");
    assert_eq!(row.get::<String, _>("fingerprint"), expected.fingerprint);
    let payload: Value = serde_json::from_str(&row.get::<String, _>("payload")).unwrap();
    let expected_payload: Value = serde_json::from_str(&expected.payload).unwrap();
    assert_eq!(payload, expected_payload);
    assert_eq!(row.get::<String, _>("status"), "pending");
    assert_eq!(row.get::<i32, _>("retries"), 0);
    assert_eq!(row.get::<i32, _>("max_retries"), 2);
    let image = live_image(&pool).await;
    assert_eq!(image.iter().filter(|row| row.starts_with("tc ")).count(), 1);
    assert!(image.iter().any(|row| row.contains(HASH_A)));
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn abstained_jobs_are_left_untouched_for_go() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    // An enriched explicit hint is outside the Rust kernel's supported path.
    sqlx::query(
        "INSERT INTO torrent_hints (info_hash, content_type, release_group, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', 'ENRICHED', NOW(), NOW())",
    )
    .bind(HASH_A)
    .execute(&pool)
    .await
    .expect("seed enriched hint");
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &supported_params(&[HASH_A])).await;
    sqlx::query("UPDATE queue_jobs SET status = 'retry', retries = 1 WHERE id = $1")
        .bind(&id)
        .execute(&pool)
        .await
        .unwrap();
    let before = job_row(&pool, &id).await;
    let worker = worker(&pool, fast_config());
    let outcome = worker.run_once().await.unwrap();
    let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
        panic!("expected an abstention, got {outcome:?}");
    };
    assert_eq!(job.id, id);
    assert_eq!(job.retries, 1, "the reported row is the unchanged row");
    assert!(reason.contains("hint"), "{reason}");
    assert_eq!(
        job_row(&pool, &id).await,
        before,
        "abstention changed the row"
    );
    assert!(live_image(&pool)
        .await
        .iter()
        .all(|row| !row.starts_with("tc ")));
    // The abstained job is excluded from this worker's next claims...
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    // ...but its lock was released, so Go can claim it.
    let go = QueueStore::new(pool.clone());
    assert!(matches!(
        go.consume_one(PROCESS_TORRENT, |_| async { Ok::<_, String>(()) })
            .await
            .unwrap(),
        ConsumeOutcome::Processed { job } if job.id == id
    ));
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn payloads_only_go_can_decode_are_abstained_not_failed() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let admitted = "\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":\
        {\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}";
    // Go's json.Unmarshal accepts both: protocol.ID trims one `0x` before
    // decoding the hex, and a JSON null leaves the slice nil (an empty job Go
    // settles as processed). serde rejects both, which must not consume a
    // retry that Go would never have spent.
    let payloads = [
        format!("{{{admitted},\"InfoHashes\":[\"0x{HASH_A}\"]}}"),
        format!("{{{admitted},\"InfoHashes\":null}}"),
    ];
    let mut before = BTreeMap::new();
    for payload in &payloads {
        let id = enqueue_raw(&pool, payload).await;
        before.insert(id.clone(), job_row(&pool, &id).await);
    }
    let worker = worker(&pool, fast_config());
    let mut abstained = BTreeSet::new();
    for _ in &payloads {
        match worker.run_once().await.unwrap() {
            AdmittedConsumeOutcome::Abstained { job, .. } => {
                abstained.insert(job.id);
            }
            other => panic!("expected an abstention, got {other:?}"),
        }
    }
    assert_eq!(abstained, before.keys().cloned().collect::<BTreeSet<_>>());
    for (id, row) in &before {
        assert_eq!(&job_row(&pool, id).await, row, "job {id} was touched");
    }
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    assert!(live_image(&pool)
        .await
        .iter()
        .all(|row| !row.starts_with("tc ")));
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn deadline_and_timeout_failures_use_go_text() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let store = QueueStore::new(pool.clone());
    let expired = enqueue(&store, &supported_params(&[HASH_A])).await;
    sqlx::query(
        "UPDATE queue_jobs SET deadline = clock_timestamp() - interval '1 second' WHERE id = $1",
    )
    .bind(&expired)
    .execute(&pool)
    .await
    .unwrap();
    let worker = worker(&pool, fast_config());
    worker.run_once().await.unwrap();
    let row = job_row(&pool, &expired).await;
    assert_eq!(row.status, "retry");
    assert_eq!(row.retries, 0);
    assert_eq!(
        row.error.as_deref(),
        Some("the job did not complete before its deadline")
    );
    assert!(
        live_image(&pool)
            .await
            .iter()
            .all(|row| !row.starts_with("tc ")),
        "an expired job never runs its handler"
    );

    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let slow = enqueue(&store, &supported_params(&[HASH_A])).await;
    let impatient = ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core().unwrap(),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        ProcessTorrentWorkerConfig {
            job_timeout: Duration::from_nanos(1),
            ..fast_config()
        },
    );
    impatient.run_once().await.unwrap();
    let row = job_row(&pool, &slow).await;
    assert_eq!(row.status, "retry");
    assert_eq!(
        row.error.as_deref(),
        Some("job exceeded its 1ns timeout: context deadline exceeded")
    );
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn concurrent_rust_workers_and_gos_claim_never_double_claim() {
    let pool = connect().await;
    reset(&pool).await;
    let store = QueueStore::new(pool.clone());
    let mut expected = BTreeSet::new();
    for index in 0..60_u32 {
        let hash = format!("{index:040x}");
        seed_torrent(
            &pool,
            &hash,
            &format!("Synthetic Story {index}.m4b"),
            60_000_000,
        )
        .await;
        expected.insert(enqueue(&store, &supported_params(&[hash.as_str()])).await);
    }

    // Two Rust workers on separate pools plus Go's exact generic claim, all
    // draining the same queue concurrently.
    let rust_a = worker(&connect().await, fast_config());
    let rust_b = worker(&connect().await, fast_config());
    let go = QueueStore::new(connect().await);
    let drain_rust = |worker: ProcessTorrentWorker| async move {
        let mut claimed = Vec::new();
        loop {
            match worker.run_once().await.expect("rust worker") {
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) => {
                    claimed.push(job.id);
                }
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty) => return claimed,
                other => panic!("unexpected outcome {other:?}"),
            }
        }
    };
    let drain_go = async {
        let mut claimed = Vec::new();
        loop {
            match go
                .consume_one(PROCESS_TORRENT, |_| async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Ok::<_, String>(())
                })
                .await
                .expect("go stand-in")
            {
                ConsumeOutcome::Processed { job } => claimed.push(job.id),
                ConsumeOutcome::Empty => return claimed,
                other => panic!("unexpected outcome {other:?}"),
            }
        }
    };
    let (a, b, g) = tokio::join!(drain_rust(rust_a), drain_rust(rust_b), drain_go);
    let mut all = Vec::new();
    all.extend(a.iter().cloned());
    all.extend(b.iter().cloned());
    all.extend(g.iter().cloned());
    let unique = all.iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(all.len(), unique.len(), "a job was claimed twice");
    assert_eq!(unique, expected, "every job was claimed exactly once");
    assert!(
        !a.is_empty() && !b.is_empty() && !g.is_empty(),
        "all three consumers participated: a={} b={} go={}",
        a.len(),
        b.len(),
        g.len()
    );
    let processed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM queue_jobs WHERE status = 'processed'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(processed, 60);
    let rust_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM torrent_contents")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        rust_rows as usize,
        a.len() + b.len(),
        "each Rust-claimed job persisted its row; Go stand-in jobs wrote nothing"
    );
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn persisted_json_columns_use_gos_encoding() {
    // Each expectation was observed in rows the Go worker wrote during the
    // Go-vs-Rust differential (tools/parity/process_torrent_worker).
    const HASH_EMPTY_RANGE: &str = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3";
    const HASH_LANGUAGES: &str = "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4";
    let pool = connect().await;
    reset(&pool).await;
    seed_torrent(&pool, HASH_A, "Synthetic Story.m4b", 60_000_000).await;
    // `S01E79 - 9` is the backwards episode range 79..=9: Go allocates the
    // episodes map and adds nothing, so GORM stores `{}` rather than NULL.
    seed_torrent(
        &pool,
        HASH_EMPTY_RANGE,
        "79. The Late Late Show With James Corden S01E79 - 9-21-15 Monday.flv",
        399_111_392,
    )
    .await;
    // Go stores `Languages.Slice()` order (natsort by name): Italian, Spanish.
    seed_torrent(
        &pool,
        HASH_LANGUAGES,
        "Some Movie 2012 ITA SPA 1080p BluRay x264.mkv",
        8_000_000_000,
    )
    .await;
    let store = QueueStore::new(pool.clone());
    enqueue(
        &store,
        &supported_params(&[HASH_A, HASH_EMPTY_RANGE, HASH_LANGUAGES]),
    )
    .await;
    assert!(matches!(
        worker(&pool, fast_config()).run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { .. })
    ));
    let rows = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, episodes::text AS episodes, \
                languages::text AS languages \
         FROM torrent_contents",
    )
    .fetch_all(&pool)
    .await
    .expect("read persisted rows");
    let image = rows
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("info_hash"),
                (
                    row.get::<Option<String>, _>("episodes"),
                    row.get::<String, _>("languages"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(image[HASH_A], (None, "[]".to_owned()));
    assert_eq!(image[HASH_EMPTY_RANGE].0.as_deref(), Some("{}"));
    assert_eq!(image[HASH_LANGUAGES].1, r#"["it", "es"]"#);
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn an_abstained_job_never_blocks_or_spins_and_its_exclusion_is_bounded() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    const HASH_ENRICHED_2: &str = "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5";
    seed_torrent(&pool, HASH_ENRICHED_2, "Another Enriched.mkv", 1_000_000).await;
    for hash in [HASH_A, HASH_ENRICHED_2] {
        sqlx::query(
            "INSERT INTO torrent_hints \
             (info_hash, content_type, release_group, created_at, updated_at) \
             VALUES (decode($1, 'hex'), 'movie', 'ENRICHED', NOW(), NOW())",
        )
        .bind(hash)
        .execute(&pool)
        .await
        .expect("seed enriched hint");
    }
    let store = QueueStore::new(pool.clone());
    // Claim order: the abstaining job first, then an admitted one behind it.
    let abstaining = enqueue(&store, &supported_params(&[HASH_A])).await;
    let admitted = enqueue(&store, &supported_params(&[HASH_B])).await;
    for (id, offset) in [(&abstaining, -20.0), (&admitted, -10.0)] {
        sqlx::query(
            "UPDATE queue_jobs SET run_after = clock_timestamp() + make_interval(secs => $2) \
             WHERE id = $1",
        )
        .bind(id)
        .bind(offset)
        .execute(&pool)
        .await
        .unwrap();
    }

    let registry = prometheus::Registry::new();
    let metrics = bitmagnet_processor::register_worker_metrics(&registry).unwrap();
    let worker = ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core().unwrap(),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        ProcessTorrentWorkerConfig {
            check_interval: Duration::from_millis(20),
            abstain_cooloff: Duration::from_secs(3_600),
            ..ProcessTorrentWorkerConfig::default()
        },
    )
    .with_metrics(metrics.clone());

    // Poll 1 abstains, poll 2 takes the admitted job behind it, poll 3 is idle.
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Abstained { job, .. } if job.id == abstaining
    ));
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == admitted
    ));
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));

    // No spin: half a second of polling (25 idle intervals) claims nothing.
    let _ = tokio::time::timeout(
        Duration::from_millis(500),
        worker.run_until(std::future::pending()),
    )
    .await;
    assert_eq!(metrics.with_label_values(&["abstained"]).get(), 1);
    assert_eq!(metrics.with_label_values(&["processed"]).get(), 1);

    // Expiry: once the cool-off lapses the job is reconsidered (and, still
    // unsupported, abstained again) rather than excluded forever.
    let expiring = ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core().unwrap(),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        ProcessTorrentWorkerConfig {
            check_interval: Duration::from_millis(20),
            abstain_cooloff: Duration::from_millis(100),
            ..ProcessTorrentWorkerConfig::default()
        },
    );
    assert!(matches!(
        expiring.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Abstained { .. }
    ));
    assert!(matches!(
        expiring.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(matches!(
        expiring.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Abstained { job, .. } if job.id == abstaining
    ));

    // Cap: with room for one exclusion, a second abstention evicts the first.
    let second = enqueue(&store, &supported_params(&[HASH_ENRICHED_2])).await;
    let capped = ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core().unwrap(),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        ProcessTorrentWorkerConfig {
            abstain_cooloff: Duration::from_secs(3_600),
            max_excluded: 1,
            ..ProcessTorrentWorkerConfig::default()
        },
    );
    let mut seen = Vec::new();
    for _ in 0..3 {
        match capped.run_once().await.unwrap() {
            AdmittedConsumeOutcome::Abstained { job, .. } => seen.push(job.id),
            other => panic!("expected an abstention, got {other:?}"),
        }
    }
    assert_eq!(seen, vec![abstaining.clone(), second, abstaining]);
    let unchanged: (String, i32) =
        sqlx::query_as("SELECT status::text, retries FROM queue_jobs WHERE id = $1")
            .bind(&seen[0])
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unchanged, ("pending".to_owned(), 0));
}

/// Hold `torrent_contents` exclusively, so the next job the worker claims
/// stalls inside its handler until the returned transaction ends.
async fn stall_handlers(pool: &PgPool) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut lock = pool.begin().await.expect("begin lock holder");
    sqlx::query("LOCK TABLE torrent_contents IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .expect("lock torrent_contents");
    lock
}

async fn wait_until_a_handler_is_stalled(pool: &PgPool) {
    for _ in 0..400 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await
        .expect("read lock waiters");
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the worker never stalled on the held torrent_contents lock");
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn shutdown_lets_an_in_flight_job_settle_within_the_grace() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &supported_params(&[HASH_A])).await;
    let lock = stall_handlers(&pool).await;
    let worker = worker(
        &pool,
        ProcessTorrentWorkerConfig {
            shutdown_grace: Duration::from_secs(30),
            ..fast_config()
        },
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let release = async {
        wait_until_a_handler_is_stalled(&pool).await;
        stop.send(()).expect("worker is running");
        tokio::time::sleep(Duration::from_millis(200)).await;
        lock.rollback().await.expect("release torrent_contents");
    };
    let ((), ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(
            worker.run_until(async {
                let _ = stopped.await;
            }),
            release
        )
    })
    .await
    .expect("the worker stops once its in-flight job settles");

    let row = job_row(&pool, &id).await;
    assert_eq!((row.status.as_str(), row.retries), ("processed", 0));
    assert!(
        live_image(&pool)
            .await
            .iter()
            .any(|row| row.starts_with("tc ")),
        "the drained job persisted its contents"
    );
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn shutdown_abandons_a_job_still_running_after_the_grace() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &supported_params(&[HASH_A])).await;
    let before = job_row(&pool, &id).await;
    let image_before = live_image(&pool).await;
    let lock = stall_handlers(&pool).await;
    let stopping = worker(
        &pool,
        ProcessTorrentWorkerConfig {
            shutdown_grace: Duration::from_millis(200),
            ..fast_config()
        },
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let signal = async {
        wait_until_a_handler_is_stalled(&pool).await;
        stop.send(()).expect("worker is running");
    };
    // The job timeout is ten minutes; only the grace can end this in time.
    let ((), ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            stopping.run_until(async {
                let _ = stopped.await;
            }),
            signal
        )
    })
    .await
    .expect("shutdown is bounded by the grace, not the job timeout");
    drop(stopping);
    lock.rollback().await.expect("release torrent_contents");

    assert_eq!(
        job_row(&pool, &id).await,
        before,
        "the abandoned claim rolled back, leaving the row for the next claim"
    );
    assert_eq!(live_image(&pool).await, image_before);
    let reclaimed = worker(&pool, fast_config()).run_once().await.unwrap();
    assert!(
        matches!(
            reclaimed,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { ref job }) if job.id == id
        ),
        "the abandoned job is claimable again: {reclaimed:?}"
    );
}

async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..400 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn database_errors_are_counted_and_the_worker_keeps_polling() {
    let pool = connect().await;
    reset(&pool).await;
    seed_fixture_torrents(&pool).await;
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &supported_params(&[HASH_A])).await;

    // A worker pool the test can exhaust: while it holds both connections,
    // every poll fails with PoolTimedOut, as when PostgreSQL is unreachable.
    let worker_pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_millis(100))
        .connect(&std::env::var(TEST_DATABASE_URL).unwrap())
        .await
        .expect("connect worker pool");
    let held = (
        worker_pool.acquire().await.unwrap(),
        worker_pool.acquire().await.unwrap(),
    );
    let registry = prometheus::Registry::new();
    let metrics = bitmagnet_processor::register_worker_metrics(&registry).unwrap();
    let worker = worker(&worker_pool, fast_config()).with_metrics(metrics.clone());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let recover = async {
        eventually("two failed polls", || {
            metrics.with_label_values(&["error"]).get() >= 2
        })
        .await;
        drop(held);
        eventually("the job to be processed", || {
            metrics.with_label_values(&["processed"]).get() == 1
        })
        .await;
        stop.send(()).expect("worker is still running");
    };
    let ((), ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(
            worker.run_until(async {
                let _ = stopped.await;
            }),
            recover
        )
    })
    .await
    .expect("the worker outlives database errors and then processes the job");

    let row = job_row(&pool, &id).await;
    assert_eq!((row.status.as_str(), row.retries), ("processed", 0));
}

/// The statement both Go and Rust blocking managers take before reading the
/// shared filter.
const BLOOM_FILTER_LOCK_SQL: &str = "SELECT pg_catalog.pg_advisory_xact_lock(\
    pg_catalog.hashtextextended('bloom_filters:blocked_torrents', 0))";

async fn wait_for_advisory_lock_waiters(pool: &PgPool, waiters: i64) {
    for _ in 0..400 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event = 'advisory'",
        )
        .fetch_one(pool)
        .await
        .expect("read advisory lock waiters");
        if waiting >= waiters {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{waiters} blocking flushes never waited for the bloom-filter lock");
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn concurrent_blocking_flushes_keep_every_writers_hashes() {
    let pool = connect().await;
    reset(&pool).await;
    let first: InfoHash = HASH_A.parse().unwrap();
    let second: InfoHash = HASH_B.parse().unwrap();
    // Persist a filter first, so both flushes read, extend, and rewrite it.
    PersistentBlockingManager::new(pool.clone())
        .block(&[HASH_MISSING.parse().unwrap()], true)
        .await
        .expect("seed the persisted filter");

    // Hold the lock as another process's in-progress flush would. Both
    // flushes must queue behind it, then apply their hashes one at a time
    // rather than each rewriting the filter it read.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query(BLOOM_FILTER_LOCK_SQL)
        .execute(&mut *holder)
        .await
        .unwrap();
    let (one, two) = (
        PersistentBlockingManager::new(pool.clone()),
        PersistentBlockingManager::new(pool.clone()),
    );
    let release = async {
        wait_for_advisory_lock_waiters(&pool, 2).await;
        holder.commit().await.unwrap();
    };
    let (batch_one, batch_two) = ([first], [second]);
    let (flushed_one, flushed_two, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(
            one.block(&batch_one, true),
            two.block(&batch_two, true),
            release
        )
    })
    .await
    .expect("both flushes finish once the lock is released");
    flushed_one.expect("first flush");
    flushed_two.expect("second flush");

    let reloaded = PersistentBlockingManager::new(pool.clone());
    assert_eq!(
        reloaded.filter(&[first, second]).await.unwrap(),
        Vec::<InfoHash>::new(),
        "the persisted filter keeps both concurrent writers' hashes"
    );
}

// --- The flags-on switch ---------------------------------------------------

const FLAGS_ON: &str =
    "{\"apis_enabled\":true,\"local_search_enabled\":true,\"tmdb_enabled\":true}";
const FLAGS_OFF: &str =
    "{\"apis_enabled\":false,\"local_search_enabled\":false,\"tmdb_enabled\":false}";
const HASH_LOCAL: &str = "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3";
const HASH_UNKNOWN: &str = "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4";

fn flags_on_params(info_hashes: &[&str]) -> ProcessTorrentParams {
    let mut params = supported_params(info_hashes);
    for flag in params
        .classifier_flags
        .as_mut()
        .expect("flags")
        .values_mut()
    {
        *flag = Value::Bool(true);
    }
    params
}

/// Local search against the database, every TMDB call timing out, so a
/// classification that reaches TMDB abstains the job.
struct LocalSearchOnly {
    local: PgContentSearch,
}

impl LocalSearchOnly {
    fn tmdb<T>() -> Result<T, ResolveError> {
        Err(ResolveError::Tmdb("operation timed out".to_owned()))
    }
}

#[async_trait]
impl ContentResolver for LocalSearchOnly {
    async fn content_by_id(
        &self,
        content_type: ContentType,
        source: &str,
        id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        self.local.content_by_id(content_type, source, id).await
    }

    async fn content_by_search(
        &self,
        content_type: ContentType,
        base_title: &str,
        year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        self.local
            .content_by_search(content_type, base_title, year)
            .await
    }

    async fn tmdb_find_by_external_id(
        &self,
        _request: &tmdb::FindByIdRequest,
    ) -> Result<tmdb::FindByIdResponse, ResolveError> {
        Self::tmdb()
    }

    async fn tmdb_movie_details(
        &self,
        _request: &tmdb::MovieDetailsRequest,
    ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError> {
        Self::tmdb()
    }

    async fn tmdb_tv_details(
        &self,
        _request: &tmdb::TvDetailsRequest,
    ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError> {
        Self::tmdb()
    }

    async fn tmdb_search_movie(
        &self,
        _request: &tmdb::SearchMovieRequest,
    ) -> Result<tmdb::SearchMovieResponse, ResolveError> {
        Self::tmdb()
    }

    async fn tmdb_search_tv(
        &self,
        _request: &tmdb::SearchTvRequest,
    ) -> Result<tmdb::SearchTvResponse, ResolveError> {
        Self::tmdb()
    }
}

/// A worker started with the flags-on switch, over [`LocalSearchOnly`].
fn flags_on_worker(pool: &PgPool) -> ProcessTorrentWorker {
    let resolver = LocalSearchOnly {
        local: PgContentSearch::with_associations(pool.clone()),
    };
    ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core_with_resolver(
            Arc::new(resolver),
            tokio::runtime::Handle::current(),
        )
        .expect("compile embedded classifier"),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        fast_config(),
    )
}

/// One local movie, `tmdb:42` "Reusable Movie" (1999), with associations
/// whose order under an en_US-like collation (glibc) differs from their
/// bytewise order: attribute sources `imdb` < `TVDB` and keys
/// `éxtra` < `f-key` < `id`; collection ids `10` < `9` < `a7` < `B8`. A
/// bytewise collation (musl, as in CI's `postgres:16-alpine`) orders them as
/// `COLLATE "C"` does.
async fn seed_local_movie(pool: &PgPool) {
    let mut content = Content {
        content_type: ContentType::Movie,
        source: "tmdb".into(),
        id: "42".into(),
        title: "Reusable Movie".into(),
        release_date: None,
        release_year: Some(1999),
        adult: None,
        original_language: None,
        original_title: None,
        overview: None,
        runtime: None,
        popularity: None,
        vote_average: None,
        vote_count: None,
        created_at: None,
        updated_at: None,
        tsv: Default::default(),
        collections: Vec::new(),
        attributes: Vec::new(),
    };
    content.update_tsv();
    sqlx::query(
        "INSERT INTO content \
         (type, source, id, title, release_date, release_year, tsv, created_at, updated_at) \
         VALUES ('movie', 'tmdb', '42', 'Reusable Movie', '1999-06-01', 1999, $1::tsvector, \
                 '2024-01-02 03:04:05+00', '2024-01-02 03:04:05+00')",
    )
    .bind(content.tsv.to_string())
    .execute(pool)
    .await
    .expect("seed local movie");
    // An upper-case source sorts between `imdb` and `tmdb` under the
    // collation, but before both bytewise. Tests share the database and this
    // table is never reset.
    sqlx::query(
        "INSERT INTO metadata_sources (key, name, created_at, updated_at) \
         VALUES ('TVDB', 'Upper-case TVDB', NOW(), NOW()) \
         ON CONFLICT (key) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("seed upper-case metadata source");
    for (source, key, value) in [
        ("TVDB", "id", "yankeevalue"),
        ("imdb", "id", "zuluvalue"),
        ("imdb", "f-key", "fvalue"),
        ("imdb", "éxtra", "evalue"),
    ] {
        sqlx::query(
            "INSERT INTO content_attributes \
             (content_type, content_source, content_id, source, key, value, created_at, updated_at) \
             VALUES ('movie', 'tmdb', '42', $1, $2, $3, NOW(), NOW())",
        )
        .bind(source)
        .bind(key)
        .bind(value)
        .execute(pool)
        .await
        .expect("seed local movie attribute");
    }
    for (id, name) in [
        ("B8", "Bravo"),
        ("a7", "Alpha"),
        ("9", "Nine"),
        ("10", "Ten"),
    ] {
        sqlx::query(
            "INSERT INTO content_collections (type, source, id, name, created_at, updated_at) \
             VALUES ('genre', 'tmdb', $1, $2, NOW(), NOW())",
        )
        .bind(id)
        .bind(name)
        .execute(pool)
        .await
        .expect("seed local movie collection");
        sqlx::query(
            "INSERT INTO content_collections_content \
             (content_type, content_source, content_id, content_collection_type, \
              content_collection_source, content_collection_id) \
             VALUES ('movie', 'tmdb', '42', 'genre', 'tmdb', $1)",
        )
        .bind(id)
        .execute(pool)
        .await
        .expect("seed local movie collection link");
    }
}

/// The seeded movie's attribute `(source, key, value)`s by `(source, key)` and
/// genre `(id, name)`s by collection key, as the database orders them: under
/// its default collation, or bytewise (`COLLATE "C"`).
async fn seeded_associations(
    pool: &PgPool,
    bytewise: bool,
) -> (Vec<(String, String, String)>, Vec<(String, String)>) {
    let (attributes_sql, collections_sql) = if bytewise {
        (
            "SELECT source, key, value FROM content_attributes \
             WHERE content_type = 'movie' AND content_source = 'tmdb' AND content_id = '42' \
             ORDER BY source COLLATE \"C\", key COLLATE \"C\"",
            "SELECT c.id, c.name FROM content_collections_content l \
             JOIN content_collections c ON (c.type, c.source, c.id) = \
                  (l.content_collection_type, l.content_collection_source, l.content_collection_id) \
             WHERE l.content_type = 'movie' AND l.content_source = 'tmdb' AND l.content_id = '42' \
             ORDER BY l.content_collection_type COLLATE \"C\", \
                      l.content_collection_source COLLATE \"C\", \
                      l.content_collection_id COLLATE \"C\"",
        )
    } else {
        (
            "SELECT source, key, value FROM content_attributes \
             WHERE content_type = 'movie' AND content_source = 'tmdb' AND content_id = '42' \
             ORDER BY source, key",
            "SELECT c.id, c.name FROM content_collections_content l \
             JOIN content_collections c ON (c.type, c.source, c.id) = \
                  (l.content_collection_type, l.content_collection_source, l.content_collection_id) \
             WHERE l.content_type = 'movie' AND l.content_source = 'tmdb' AND l.content_id = '42' \
             ORDER BY l.content_collection_type, l.content_collection_source, l.content_collection_id",
        )
    };
    let attributes = sqlx::query_as(attributes_sql)
        .fetch_all(pool)
        .await
        .expect("seeded attribute order");
    let collections = sqlx::query_as(collections_sql)
        .fetch_all(pool)
        .await
        .expect("seeded collection order");
    (attributes, collections)
}

/// Assert that `tsv` carries `UpdateTsv`'s weight-D lexemes for the seeded
/// movie in the order it appends them: the genre names in `collections`'
/// order, then the `id` attribute values in `attributes`' order.
fn assert_association_lexeme_order(
    tsv: &str,
    attributes: &[(String, String, String)],
    collections: &[(String, String)],
) {
    let lexemes: Vec<String> = collections
        .iter()
        .map(|(_, name)| name.to_lowercase())
        .chain(
            attributes
                .iter()
                .filter(|(_, key, _)| key == "id")
                .map(|(_, _, value)| value.clone()),
        )
        .collect();
    let positions: Vec<u32> = lexemes
        .iter()
        .map(|lexeme| {
            *lexeme_positions(tsv, lexeme)
                .first()
                .unwrap_or_else(|| panic!("{lexeme} missing from {tsv}"))
        })
        .collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "association lexemes {lexemes:?} are out of order at {positions:?} in {tsv}"
    );
}

/// The lexeme positions of `lexeme` in a `tsvector`'s text form.
fn lexeme_positions(tsv: &str, lexeme: &str) -> Vec<u32> {
    let quoted = format!("'{lexeme}':");
    let Some(start) = tsv.find(&quoted) else {
        return Vec::new();
    };
    tsv[start + quoted.len()..]
        .split(' ')
        .next()
        .expect("position list")
        .split(',')
        .map(|label| {
            label
                .trim_end_matches(char::is_alphabetic)
                .parse()
                .expect("lexeme position")
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn local_search_hydrates_associations_in_database_collation_order() {
    let pool = connect().await;
    reset(&pool).await;
    seed_local_movie(&pool).await;
    // The contract: hydration keeps the order the database's default
    // collation gives, and never re-sorts it.
    let (attributes, collections) = seeded_associations(&pool, false).await;
    // Whether this database can tell that order from a bytewise re-sort.
    let (bytewise_attributes, bytewise_collections) = seeded_associations(&pool, true).await;
    let attributes_discriminate = attributes != bytewise_attributes;
    let collections_discriminate = collections != bytewise_collections;

    let hydrating = PgContentSearch::with_associations(pool.clone());
    let by_id = hydrating
        .content_by_id(ContentType::Movie, "tmdb", "42")
        .await
        .expect("local by id")
        .expect("seeded movie");
    let by_alternative_id = hydrating
        .content_by_id(ContentType::Movie, "imdb", "zuluvalue")
        .await
        .expect("local by alternative id")
        .expect("seeded movie by imdb id");
    let by_search = hydrating
        .content_by_search(ContentType::Movie, "Reusable Movie", Some(1999))
        .await
        .expect("local search");
    assert_eq!(by_search.len(), 1, "{by_search:?}");
    for content in [&by_id, &by_alternative_id, &by_search[0].content] {
        let hydrated_attributes: Vec<_> = content
            .attributes
            .iter()
            .map(|a| (a.source.clone(), a.key.clone(), a.value.clone()))
            .collect();
        let hydrated_collections: Vec<_> = content
            .collections
            .iter()
            .map(|c| (c.id.clone(), c.name.clone()))
            .collect();
        assert_eq!(hydrated_attributes, attributes);
        assert_eq!(hydrated_collections, collections);
        assert!(content
            .attributes
            .iter()
            .all(|a| a.content_type == ContentType::Movie
                && a.content_source == "tmdb"
                && a.content_id == "42"));
        assert!(content
            .collections
            .iter()
            .all(|c| c.collection_type == "genre" && c.source == "tmdb"));
        if attributes_discriminate {
            assert_ne!(
                hydrated_attributes, bytewise_attributes,
                "attributes were re-sorted bytewise"
            );
        }
        if collections_discriminate {
            assert_ne!(
                hydrated_collections, bytewise_collections,
                "collections were re-sorted bytewise"
            );
        }
    }
    if !(attributes_discriminate && collections_discriminate) {
        // Written past libtest's output capture, so a passing run still
        // shows that half of this gate did not run here.
        let _ = writeln!(
            std::io::stderr(),
            "worker_pg: bytewise-vs-collation check NOT exercised for the seeded {}: \
             this database's default collation orders them as COLLATE \"C\" does",
            match (attributes_discriminate, collections_discriminate) {
                (false, false) => "attributes and collections",
                (false, true) => "attributes",
                _ => "collections",
            }
        );
    }
    // So the tsv the processor derives carries the associations in that order.
    let mut content = by_id.clone();
    content.update_tsv();
    assert_association_lexeme_order(&content.tsv.to_string(), &attributes, &collections);

    // Without the opt-in, rows stay scalar-only for the restricted shadow role.
    let scalar = PgContentSearch::new(pool.clone())
        .content_by_id(ContentType::Movie, "tmdb", "42")
        .await
        .expect("scalar by id")
        .expect("seeded movie");
    assert!(scalar.attributes.is_empty() && scalar.collections.is_empty());
    assert_eq!(
        Content {
            attributes: Vec::new(),
            collections: Vec::new(),
            ..by_id
        },
        scalar
    );
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn flags_on_admission_claims_both_explicit_subsets_and_nothing_else() {
    let pool = connect().await;
    reset(&pool).await;
    let hashes = format!("[\"{HASH_A}\"]");
    let flags_off = enqueue_raw(
        &pool,
        &format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_OFF},\"InfoHashes\":{hashes}}}"
        ),
    )
    .await;
    let flags_on = enqueue_raw(
        &pool,
        &format!(
            "{{\"ClassifyMode\":\"rematch\",\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_ON},\"InfoHashes\":{hashes}}}"
        ),
    )
    .await;
    let rejected_payloads = [
        // Flags partly true, partly omitted, or not JSON booleans.
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":true,\"local_search_enabled\":true,\"tmdb_enabled\":false}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":true,\"local_search_enabled\":true}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":true,\"local_search_enabled\":true,\"tmdb_enabled\":\"true\"}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":1,\"local_search_enabled\":true,\"tmdb_enabled\":true}},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"apis_enabled\":[true],\"local_search_enabled\":true,\"tmdb_enabled\":true}},\"InfoHashes\":{hashes}}}"
        ),
        // An empty, missing, or other workflow.
        format!("{{\"ClassifierWorkflow\":\"\",\"ClassifierFlags\":{FLAGS_ON},\"InfoHashes\":{hashes}}}"),
        format!("{{\"ClassifierFlags\":{FLAGS_ON},\"InfoHashes\":{hashes}}}"),
        format!(
            "{{\"ClassifierWorkflow\":\"custom\",\"ClassifierFlags\":{FLAGS_ON},\"InfoHashes\":{hashes}}}"
        ),
        // Extra top-level keys, including Go's case-insensitive aliases.
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_ON},\"classifierFlags\":{FLAGS_OFF},\"InfoHashes\":{hashes}}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_ON},\"InfoHashes\":{hashes},\"Extra\":1}}"
        ),
        // The organic shape.
        format!("{{\"InfoHashes\":{hashes}}}"),
    ];
    let mut rejected = Vec::new();
    for payload in &rejected_payloads {
        rejected.push(enqueue_raw(&pool, payload).await);
    }
    let mut before = Vec::new();
    for id in &rejected {
        before.push(job_row(&pool, id).await);
    }

    let store = QueueStore::new(pool.clone());
    let claim_all = |admission| {
        let store = &store;
        async move {
            let mut claimed = Vec::new();
            while let AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) = store
                .consume_admitted_process_torrent_with(admission, &[], |_| async {
                    JobVerdict::Complete
                })
                .await
                .unwrap()
            {
                claimed.push(job.id);
            }
            claimed
        }
    };
    // Without the switch only the flags-off payload is claimed; with it the
    // flags-on payload too (the flags-off one is processed by then).
    assert_eq!(
        claim_all(ProcessTorrentAdmission::FlagsOff).await,
        std::slice::from_ref(&flags_off)
    );
    assert_eq!(
        claim_all(ProcessTorrentAdmission::FlagsOffOrOn).await,
        std::slice::from_ref(&flags_on)
    );
    for id in [&flags_off, &flags_on] {
        assert_eq!(job_row(&pool, id).await.status, "processed");
    }
    for (id, before) in rejected.iter().zip(&before) {
        assert_eq!(&job_row(&pool, id).await, before, "job {id} was touched");
    }

    // The worker's admission follows its materializer.
    assert_eq!(
        worker(&pool, fast_config()).admission(),
        ProcessTorrentAdmission::FlagsOff
    );
    assert_eq!(
        flags_on_worker(&pool).admission(),
        ProcessTorrentAdmission::FlagsOffOrOn
    );
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn flags_on_claim_reads_only_admitted_rows_behind_an_organic_backlog() {
    let pool = connect().await;
    reset(&pool).await;
    sqlx::query(
        "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, max_retries, \
         run_after, archival_duration, created_at, priority) \
         SELECT md5(n::text), 'process_torrent', 'pending', \
                jsonb_build_object('InfoHashes', jsonb_build_array(lpad(to_hex(n), 40, '0'))), \
                0, 2, clock_timestamp() - interval '1 hour', interval '168:00:00', \
                clock_timestamp(), 0 \
         FROM generate_series(1, 20000) AS n",
    )
    .execute(&pool)
    .await
    .expect("seed organic backlog");
    let store = QueueStore::new(pool.clone());
    let admitted = [
        enqueue(&store, &flags_on_params(&[HASH_A])).await,
        enqueue(&store, &supported_params(&[HASH_B])).await,
    ];
    for id in &admitted {
        make_ready(&pool, id).await;
    }
    sqlx::query("ANALYZE queue_jobs")
        .execute(&pool)
        .await
        .expect("analyze queue_jobs");

    for generic in [false, true] {
        let plan = explain_claim(&pool, ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL, generic).await;
        assert!(
            plan.contains(ADMITTED_FLAGS_ON_CLAIM_INDEX),
            "the {} flags-on claim must scan {ADMITTED_FLAGS_ON_CLAIM_INDEX}; the claim and \
             migration 00036 predicates have drifted apart:\n{plan}",
            if generic { "generic" } else { "custom" },
        );
        // The flags-off claim keeps its own index.
        let plan = explain_admitted_claim(&pool, generic).await;
        assert!(plan.contains(ADMITTED_CLAIM_INDEX), "{plan}");
    }
    let mut claimed = Vec::new();
    while let AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) = store
        .consume_admitted_process_torrent_with(
            ProcessTorrentAdmission::FlagsOffOrOn,
            &[],
            |_| async { JobVerdict::Complete },
        )
        .await
        .unwrap()
    {
        claimed.push(job.id);
    }
    assert_eq!(claimed, admitted);
    let organic_pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM queue_jobs WHERE status = 'pending'")
            .fetch_one(&pool)
            .await
            .expect("count organic backlog");
    assert_eq!(organic_pending, 20_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn flags_on_job_attaches_local_content_with_its_ordered_associations() {
    let pool = connect().await;
    reset(&pool).await;
    seed_local_movie(&pool).await;
    seed_torrent(
        &pool,
        HASH_LOCAL,
        "Reusable Movie 1999 1080p BluRay x264-GRP.mkv",
        2_000_000_000,
    )
    .await;
    let content_before = live_image(&pool).await;
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &flags_on_params(&[HASH_LOCAL])).await;
    let worker = flags_on_worker(&pool);
    // TMDB fails every call, so processing proves local search attached the
    // movie (which skips TMDB) and nothing reached TMDB.
    let outcome = worker.run_once().await.expect("run worker once");
    let AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) = outcome else {
        panic!("expected a processed job, got {outcome:?}");
    };
    assert_eq!(job.id, id);
    let row = sqlx::query(
        "SELECT content_type::text AS content_type, content_source, content_id, tsv::text AS tsv \
         FROM torrent_contents WHERE info_hash = decode($1, 'hex')",
    )
    .bind(HASH_LOCAL)
    .fetch_one(&pool)
    .await
    .expect("one torrent_contents row");
    assert_eq!(row.get::<String, _>("content_type"), "movie");
    assert_eq!(
        row.get::<Option<String>, _>("content_source").as_deref(),
        Some("tmdb")
    );
    assert_eq!(
        row.get::<Option<String>, _>("content_id").as_deref(),
        Some("42")
    );
    // The torrent's tsv carries the hydrated genres and `id` attributes in
    // the database's order, whatever its collation.
    let (attributes, collections) = seeded_associations(&pool, false).await;
    assert_association_lexeme_order(&row.get::<String, _>("tsv"), &attributes, &collections);
    // An existing content row is reused, not rewritten (Go upserts only
    // content TMDB just produced).
    let content_after: Vec<_> = live_image(&pool)
        .await
        .into_iter()
        .filter(|row| row.starts_with("content "))
        .collect();
    let content_before: Vec<_> = content_before
        .into_iter()
        .filter(|row| row.starts_with("content "))
        .collect();
    assert_eq!(content_after, content_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_flags_on_resolver_failure_abstains_with_zero_writes() {
    let pool = connect().await;
    reset(&pool).await;
    seed_local_movie(&pool).await;
    seed_torrent(
        &pool,
        HASH_LOCAL,
        "Reusable Movie 1999 1080p BluRay x264-GRP.mkv",
        2_000_000_000,
    )
    .await;
    seed_torrent(
        &pool,
        HASH_UNKNOWN,
        "Unknown Picture 2004 720p WEB-DL x264-GRP.mkv",
        1_000_000_000,
    )
    .await;
    let store = QueueStore::new(pool.clone());
    // The first torrent attaches locally; the second reaches TMDB, which fails.
    let id = enqueue(&store, &flags_on_params(&[HASH_LOCAL, HASH_UNKNOWN])).await;
    sqlx::query("UPDATE queue_jobs SET status = 'retry', retries = 1 WHERE id = $1")
        .bind(&id)
        .execute(&pool)
        .await
        .unwrap();
    let before = job_row(&pool, &id).await;
    let image_before = live_image(&pool).await;
    let worker = flags_on_worker(&pool);
    let outcome = worker.run_once().await.unwrap();
    let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
        panic!("expected an abstention, got {outcome:?}");
    };
    assert_eq!(job.id, id);
    assert_eq!(job.retries, 1, "the reported row is the unchanged row");
    assert!(
        reason.contains("content resolver")
            && reason.contains(HASH_UNKNOWN)
            && reason.contains("operation timed out"),
        "{reason}"
    );
    assert_eq!(
        job_row(&pool, &id).await,
        before,
        "abstention changed the row"
    );
    assert_eq!(
        live_image(&pool).await,
        image_before,
        "abstention wrote rows"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "an abstention publishes no retry job"
    );
    // Excluded from this worker's next claim, and left unlocked for Go.
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    let go = QueueStore::new(pool.clone());
    assert!(matches!(
        go.consume_one(PROCESS_TORRENT, |_| async { Ok::<_, String>(()) })
            .await
            .unwrap(),
        ConsumeOutcome::Processed { job } if job.id == id
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_flags_on_worker_processes_flags_off_jobs_exactly_as_without_the_switch() {
    let pool = connect().await;
    let store = QueueStore::new(pool.clone());
    let params = supported_params(&[HASH_A, HASH_B]);
    let mut images = Vec::new();
    for switched_on in [false, true] {
        reset(&pool).await;
        seed_fixture_torrents(&pool).await;
        let id = enqueue(&store, &params).await;
        let worker = if switched_on {
            flags_on_worker(&pool)
        } else {
            worker(&pool, fast_config())
        };
        assert!(matches!(
            worker.run_once().await.unwrap(),
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
        ));
        images.push(live_image(&pool).await);
    }
    assert_eq!(
        images[0]
            .iter()
            .filter(|row| row.starts_with("tc "))
            .count(),
        2
    );
    assert_eq!(images[1], images[0]);
}

/// Counts the calls that reach it and, when `stall_first`, holds the first
/// one until released. Local search answers nothing and TMDB times out.
#[derive(Default)]
struct CountingResolver {
    calls: AtomicUsize,
    stall_first: bool,
    release: tokio::sync::Notify,
    released: AtomicBool,
}

impl CountingResolver {
    async fn enter(&self) {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 && self.stall_first {
            self.release.notified().await;
            self.released.store(true, Ordering::SeqCst);
        }
    }

    async fn tmdb<T>(&self) -> Result<T, ResolveError> {
        self.enter().await;
        Err(ResolveError::Tmdb("operation timed out".to_owned()))
    }
}

#[async_trait]
impl ContentResolver for CountingResolver {
    async fn content_by_id(
        &self,
        _content_type: ContentType,
        _source: &str,
        _id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        self.enter().await;
        Ok(None)
    }

    async fn content_by_search(
        &self,
        _content_type: ContentType,
        _base_title: &str,
        _year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        self.enter().await;
        Ok(Vec::new())
    }

    async fn tmdb_find_by_external_id(
        &self,
        _request: &tmdb::FindByIdRequest,
    ) -> Result<tmdb::FindByIdResponse, ResolveError> {
        self.tmdb().await
    }

    async fn tmdb_movie_details(
        &self,
        _request: &tmdb::MovieDetailsRequest,
    ) -> Result<Option<tmdb::MovieDetailsResponse>, ResolveError> {
        self.tmdb().await
    }

    async fn tmdb_tv_details(
        &self,
        _request: &tmdb::TvDetailsRequest,
    ) -> Result<Option<tmdb::TvDetailsResponse>, ResolveError> {
        self.tmdb().await
    }

    async fn tmdb_search_movie(
        &self,
        _request: &tmdb::SearchMovieRequest,
    ) -> Result<tmdb::SearchMovieResponse, ResolveError> {
        self.tmdb().await
    }

    async fn tmdb_search_tv(
        &self,
        _request: &tmdb::SearchTvRequest,
    ) -> Result<tmdb::SearchTvResponse, ResolveError> {
        self.tmdb().await
    }
}

/// A worker started with the flags-on switch, over `resolver`.
fn counting_worker(
    pool: &PgPool,
    resolver: &Arc<CountingResolver>,
    config: ProcessTorrentWorkerConfig,
) -> ProcessTorrentWorker {
    ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core_with_resolver(
            Arc::clone(resolver) as Arc<dyn ContentResolver>,
            tokio::runtime::Handle::current(),
        )
        .expect("compile embedded classifier"),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        config,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_flags_on_job_abandoned_at_its_timeout_makes_no_further_resolver_calls() {
    let pool = connect().await;
    reset(&pool).await;
    seed_torrent(
        &pool,
        HASH_UNKNOWN,
        "Unknown Picture 2004 720p WEB-DL x264-GRP.mkv",
        1_000_000_000,
    )
    .await;
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &flags_on_params(&[HASH_UNKNOWN])).await;
    let image_before = live_image(&pool).await;
    let resolver = Arc::new(CountingResolver {
        stall_first: true,
        ..CountingResolver::default()
    });
    let worker = counting_worker(
        &pool,
        &resolver,
        ProcessTorrentWorkerConfig {
            job_timeout: Duration::from_secs(2),
            ..fast_config()
        },
    );

    // The first resolver call never returns on its own, so the job times out
    // and is settled while its composition is still blocked in that call.
    worker.run_once().await.unwrap();
    assert_eq!(
        job_row(&pool, &id).await.error.as_deref(),
        Some("job exceeded its 2s timeout: context deadline exceeded")
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);

    // Unblocked, the abandoned composition goes on classifying, but its
    // scope was stopped when the job was dropped: nothing reaches TMDB.
    resolver.release.notify_one();
    eventually("the stalled call to return", || {
        resolver.released.load(Ordering::SeqCst)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        resolver.calls.load(Ordering::SeqCst),
        1,
        "an abandoned composition made another resolver call"
    );
    assert_eq!(live_image(&pool).await, image_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_flags_on_job_with_a_sourced_hint_abstains_before_any_resolver_call() {
    let pool = connect().await;
    reset(&pool).await;
    seed_local_movie(&pool).await;
    seed_torrent(
        &pool,
        HASH_LOCAL,
        "Reusable Movie 1999 1080p BluRay x264-GRP.mkv",
        2_000_000_000,
    )
    .await;
    seed_torrent(
        &pool,
        HASH_UNKNOWN,
        "Unknown Picture 2004 720p WEB-DL x264-GRP.mkv",
        1_000_000_000,
    )
    .await;
    // Go would look this hint up by ID (local, then TMDB): M3, so Rust
    // leaves the whole job to Go before classifying any of its torrents.
    sqlx::query(
        "INSERT INTO torrent_hints \
         (info_hash, content_type, content_source, content_id, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', 'tmdb', '42', NOW(), NOW())",
    )
    .bind(HASH_UNKNOWN)
    .execute(&pool)
    .await
    .expect("seed sourced hint");
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &flags_on_params(&[HASH_LOCAL, HASH_UNKNOWN])).await;
    let before = job_row(&pool, &id).await;
    let image_before = live_image(&pool).await;
    let resolver = Arc::new(CountingResolver::default());
    let worker = counting_worker(&pool, &resolver, fast_config());

    let outcome = worker.run_once().await.unwrap();
    let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
        panic!("expected an abstention, got {outcome:?}");
    };
    assert_eq!(job.id, id);
    assert_eq!(
        reason,
        WriterPlanError::AttachHintUnsupported {
            info_hash: HASH_UNKNOWN.to_owned()
        }
        .to_string()
    );
    assert_eq!(
        resolver.calls.load(Ordering::SeqCst),
        0,
        "a hint-abstained job reached the resolver"
    );
    assert_eq!(
        job_row(&pool, &id).await,
        before,
        "abstention changed the row"
    );
    assert_eq!(
        live_image(&pool).await,
        image_before,
        "abstention wrote rows"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "an abstention publishes no retry job"
    );
}

// -- TMDB status failures fail only their torrent, as in Go ----------------

const HASH_FOUND: &str = "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5";
/// Go's path for the `find_match` that searches by title.
const BY_SEARCH: &str = "runtime error at Path workflows.default.[5].if_else.if_action.find_match";
const FOUND_SEARCH: &str = r#"{"page":1,"results":[{"id":777,"title":"Found Film","original_title":"Found Film","release_date":"2010-05-01"}],"total_pages":1,"total_results":1}"#;
const FOUND_DETAILS: &str = r#"{"id":777,"title":"Found Film","original_title":"Found Film","original_language":"en","release_date":"2010-05-01","imdb_id":"tt0000777","genres":[{"id":18,"name":"Drama"}]}"#;

type Answer = Result<HttpResponse, String>;

/// An HTTP answer; `reason_phrase` is a non-canonical one, as hyper reports it.
fn answer(status: u16, reason_phrase: Option<&str>, body: &str) -> Answer {
    Ok(HttpResponse {
        status,
        reason_phrase: reason_phrase.map(|phrase| phrase.as_bytes().to_vec()),
        content_type: Some("application/json".to_owned()),
        body: body.as_bytes().to_vec(),
    })
}

/// TMDB behind the real `TmdbClient`. A request is answered by the first
/// answer whose key occurs in its `path?query` (without the API key), and
/// otherwise with an empty search page; every request is recorded, so a test
/// can check the request multiset.
#[derive(Default)]
struct FakeTmdb {
    answers: Vec<(&'static str, Answer)>,
    asked: Mutex<Vec<String>>,
}

impl FakeTmdb {
    fn new(answers: Vec<(&'static str, Answer)>) -> Arc<Self> {
        Arc::new(Self {
            answers,
            ..Self::default()
        })
    }

    fn asked(&self) -> Vec<String> {
        self.asked.lock().expect("asked lock").clone()
    }

    /// How many requests contained `key`.
    fn count(&self, key: &str) -> usize {
        self.asked()
            .iter()
            .filter(|request| request.contains(key))
            .count()
    }
}

#[async_trait]
impl Transport for FakeTmdb {
    async fn execute(&self, url: &Url) -> Result<HttpResponse, String> {
        let query: Vec<String> = url
            .query_pairs()
            .filter(|(key, _)| key != "api_key")
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        let request = format!("{}?{}", url.path(), query.join("&"));
        self.asked.lock().expect("asked lock").push(request.clone());
        self.answers
            .iter()
            .find(|(key, _)| request.contains(key))
            .map_or_else(
                || {
                    answer(
                        200,
                        None,
                        r#"{"page":1,"results":[],"total_pages":0,"total_results":0}"#,
                    )
                },
                |(_, answer)| answer.clone(),
            )
    }
}

/// A worker started with the flags-on switch over Go's resolver stack: the
/// database's local search and the real TMDB client, talking to `tmdb` with
/// Go's three transport retries (without the waits).
fn tmdb_worker(pool: &PgPool, tmdb: &Arc<FakeTmdb>, parallelism: usize) -> ProcessTorrentWorker {
    let config = TmdbConfig {
        base_url: "http://tmdb.invalid/3".to_owned(),
        rate_limit: Duration::ZERO,
        retry: RetryPolicy {
            count: 3,
            wait: Duration::ZERO,
            max_wait: Duration::ZERO,
        },
        ..TmdbConfig::new("test-key")
    };
    let resolver = LiveContentResolver::new(
        Arc::new(PgContentSearch::with_associations(pool.clone())),
        Arc::new(TmdbClient::with_transport(config, Arc::clone(tmdb))),
    );
    ProcessTorrentWorker::new(
        QueueStore::new(pool.clone()),
        Materializer::from_core_with_resolver(
            Arc::new(resolver),
            tokio::runtime::Handle::current(),
        )
        .expect("compile embedded classifier")
        .with_classify_parallelism(NonZeroUsize::new(parallelism).expect("nonzero"))
        .expect("classify pool"),
        Arc::new(PersistentBlockingManager::new(pool.clone())),
        fast_config(),
    )
}

/// The local movie and three torrents: one that attaches it, one TMDB has
/// never heard of, and one TMDB finds.
async fn seed_tmdb_fixture(pool: &PgPool) {
    reset(pool).await;
    seed_local_movie(pool).await;
    seed_torrent(
        pool,
        HASH_LOCAL,
        "Reusable Movie 1999 1080p BluRay x264-GRP.mkv",
        2_000_000_000,
    )
    .await;
    seed_torrent(
        pool,
        HASH_UNKNOWN,
        "Unknown Picture 2004 720p WEB-DL x264-GRP.mkv",
        1_000_000_000,
    )
    .await;
    seed_torrent(
        pool,
        HASH_FOUND,
        "Found Film 2010 1080p BluRay x264-GRP.mkv",
        3_000_000_000,
    )
    .await;
}

/// The one queue job other than `id`, as `(payload, status, retries,
/// max_retries)`; the payload is the stored JSON text.
async fn other_job(pool: &PgPool, id: &str) -> (String, String, i32, i32) {
    let row = sqlx::query(
        "SELECT payload::text AS payload, status::text AS status, retries, max_retries \
         FROM queue_jobs WHERE id <> $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("exactly one retry job");
    (
        row.get("payload"),
        row.get("status"),
        row.get("retries"),
        row.get("max_retries"),
    )
}

/// Go's retry message for `info_hashes`, as it stores it: the flags-on
/// params with the resolved workflow.
fn flags_on_retry_payload(info_hashes: &[&str]) -> Value {
    let job = process_torrent_job(&flags_on_params(info_hashes), QueueJobOptions::default())
        .expect("build retry job");
    serde_json::from_str(&job.payload).expect("retry payload")
}

async fn queue_job_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM queue_jobs")
        .fetch_one(pool)
        .await
        .expect("count queue jobs")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_tmdb_status_failure_fails_only_its_torrent_and_the_others_still_resolve() {
    let pool = connect().await;
    // Serial, and on Go's default pool width.
    for parallelism in [1, 4] {
        seed_tmdb_fixture(&pool).await;
        let tmdb = FakeTmdb::new(vec![
            ("query=Unknown", answer(500, None, "{}")),
            ("query=Found", answer(200, None, FOUND_SEARCH)),
            ("/movie/777", answer(200, None, FOUND_DETAILS)),
        ]);
        let store = QueueStore::new(pool.clone());
        // The failing torrent comes first, so a serial composition reaches the
        // others only after the failure.
        let id = enqueue(
            &store,
            &flags_on_params(&[HASH_UNKNOWN, HASH_LOCAL, HASH_FOUND]),
        )
        .await;
        let worker = tmdb_worker(&pool, &tmdb, parallelism);

        // Go persists the rest, publishes the failed torrent, and returns nil.
        let outcome = worker.run_once().await.unwrap();
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "parallelism {parallelism}: {outcome:?}"
        );
        let source = job_row(&pool, &id).await;
        assert_eq!(
            (source.status.as_str(), source.error.as_deref()),
            ("processed", None)
        );
        let (payload, status, retries, max_retries) = other_job(&pool, &id).await;
        assert_eq!(
            serde_json::from_str::<Value>(&payload).unwrap(),
            flags_on_retry_payload(&[HASH_UNKNOWN])
        );
        assert_eq!((status.as_str(), retries, max_retries), ("pending", 0, 2));

        // Both other torrents resolved: one locally, one from TMDB.
        let image = live_image(&pool).await;
        let classified: Vec<&String> = image.iter().filter(|row| row.starts_with("tc ")).collect();
        assert_eq!(classified.len(), 2, "{image:#?}");
        assert!(!image
            .iter()
            .any(|row| row.starts_with("tc ") && row.contains(HASH_UNKNOWN)));
        assert!(
            classified
                .iter()
                .any(|row| row.contains(HASH_LOCAL) && row.contains("\"content_id\": \"42\"")),
            "{classified:#?}"
        );
        assert!(
            classified
                .iter()
                .any(|row| row.contains(HASH_FOUND) && row.contains("\"content_id\": \"777\"")),
            "{classified:#?}"
        );
        assert!(
            image
                .iter()
                .any(|row| row.starts_with("content ") && row.contains("\"id\": \"777\"")),
            "{image:#?}"
        );

        // One request per call, none retried; the failing torrent stopped at
        // its failed search.
        assert_eq!(tmdb.count("/authentication"), 1, "{:?}", tmdb.asked());
        assert_eq!(tmdb.count("query=Unknown"), 1, "{:?}", tmdb.asked());
        assert_eq!(tmdb.count("query=Found"), 1, "{:?}", tmdb.asked());
        assert_eq!(tmdb.count("/movie/777"), 1, "{:?}", tmdb.asked());
        assert_eq!(tmdb.asked().len(), 4, "{:?}", tmdb.asked());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn missing_hashes_lead_the_retry_job_ahead_of_a_tmdb_failure() {
    let pool = connect().await;
    seed_tmdb_fixture(&pool).await;
    let tmdb = FakeTmdb::new(vec![("query=Unknown", answer(404, None, "{}"))]);
    let store = QueueStore::new(pool.clone());
    let id = enqueue(
        &store,
        &flags_on_params(&[HASH_UNKNOWN, HASH_LOCAL, HASH_MISSING]),
    )
    .await;
    let worker = tmdb_worker(&pool, &tmdb, 1);
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
    ));
    // Go's `failedHashes` starts with the missing hashes, then the torrents
    // whose classification failed.
    let (payload, ..) = other_job(&pool, &id).await;
    assert_eq!(
        serde_json::from_str::<Value>(&payload).unwrap(),
        flags_on_retry_payload(&[HASH_MISSING, HASH_UNKNOWN])
    );
    let image = live_image(&pool).await;
    assert_eq!(image.iter().filter(|row| row.starts_with("tc ")).count(), 1);
    assert_eq!(tmdb.count("query=Unknown"), 1, "{:?}", tmdb.asked());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn an_all_failed_flags_on_job_stores_gos_error_and_fails_at_max_retries() {
    let pool = connect().await;
    seed_tmdb_fixture(&pool).await;
    // The server's own reason phrase reaches the stored error, as in Go.
    let tmdb = FakeTmdb::new(vec![(
        "query=Unknown",
        answer(503, Some("Slow Down"), "{}"),
    )]);
    let store = QueueStore::new(pool.clone());
    let id = enqueue(&store, &flags_on_params(&[HASH_MISSING, HASH_UNKNOWN])).await;
    let image_before = live_image(&pool).await;
    let worker = tmdb_worker(&pool, &tmdb, 1);
    let expected_error = format!(
        "job failed to process: missing 1 info hashes\n\
         {BY_SEARCH}: TMDB request failed: 503 Slow Down"
    );

    // Pending run, then two retry runs; the second reaches max_retries (2).
    for (run, (status, retries)) in [("retry", 0), ("retry", 1), ("failed", 2)]
        .into_iter()
        .enumerate()
    {
        make_ready(&pool, &id).await;
        let outcome = worker.run_once().await.unwrap();
        match (status, &outcome) {
            ("retry", AdmittedConsumeOutcome::Settled(ConsumeOutcome::RetryScheduled { .. })) => {}
            ("failed", AdmittedConsumeOutcome::Settled(ConsumeOutcome::Failed { error, .. })) => {
                assert_eq!(error, &expected_error)
            }
            _ => panic!("run {run}: expected {status}, got {outcome:?}"),
        }
        let row = job_row(&pool, &id).await;
        assert_eq!(
            (row.status.as_str(), row.retries, row.error.as_deref()),
            (status, retries, Some(expected_error.as_str())),
            "run {run}"
        );
        // One search per run, none retried.
        assert_eq!(tmdb.count("query=Unknown"), run + 1, "{:?}", tmdb.asked());
    }
    assert_eq!(
        queue_job_count(&pool).await,
        1,
        "an all-failed job publishes no retry job"
    );
    assert_eq!(
        live_image(&pool).await,
        image_before,
        "an all-failed job wrote rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn abstaining_tmdb_failures_leave_zero_writes_and_make_no_later_calls() {
    let pool = connect().await;
    for (name, failing, failure, attempts, requests) in [
        ("401", "query=Unknown", answer(401, None, "{}"), 1, 2),
        // Go's three retries: four attempts, then the job abstains.
        (
            "transport",
            "query=Unknown",
            Err("operation timed out".to_owned()),
            4,
            5,
        ),
        // The latched validation fails the first search without a request.
        (
            "key validation 500",
            "/authentication",
            answer(500, None, "{}"),
            1,
            1,
        ),
    ] {
        seed_tmdb_fixture(&pool).await;
        let tmdb = FakeTmdb::new(vec![
            (failing, failure),
            ("query=Found", answer(200, None, FOUND_SEARCH)),
            ("/movie/777", answer(200, None, FOUND_DETAILS)),
        ]);
        let store = QueueStore::new(pool.clone());
        let id = enqueue(&store, &flags_on_params(&[HASH_UNKNOWN, HASH_FOUND])).await;
        let before = job_row(&pool, &id).await;
        let image_before = live_image(&pool).await;
        let worker = tmdb_worker(&pool, &tmdb, 1);

        let outcome = worker.run_once().await.unwrap();
        let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
            panic!("{name}: expected an abstention, got {outcome:?}");
        };
        assert_eq!(job.id, id, "{name}");
        assert!(
            reason.contains(HASH_UNKNOWN) && reason.contains("tmdb_search_movie"),
            "{name}: {reason}"
        );
        assert_eq!(
            job_row(&pool, &id).await,
            before,
            "{name}: abstention changed the row"
        );
        assert_eq!(
            live_image(&pool).await,
            image_before,
            "{name}: abstention wrote rows"
        );
        assert_eq!(
            queue_job_count(&pool).await,
            1,
            "{name}: abstention published a job"
        );
        // The failed call's attempts, and nothing after it: the next torrent
        // never reached TMDB.
        assert_eq!(tmdb.count(failing), attempts, "{name}: {:?}", tmdb.asked());
        assert_eq!(
            tmdb.asked().len(),
            requests,
            "{name}: a call after the abstaining failure: {:?}",
            tmdb.asked()
        );
    }
}
