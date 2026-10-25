//! Disposable-PostgreSQL gate for the persisting `process_torrent` worker.
//!
//! Every test truncates queue, torrent, and content tables. Each requires the
//! exact disposable database name `bitmagnet_processor_writer_load_test` and
//! explicit invocation with `--ignored --test-threads=1`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::num::NonZeroUsize;
use std::str::FromStr;
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
    check_payload_key_set, load_writer_plan, persist_writer_plan, validate_supported_params,
    Materializer, ProcessTorrentWorker, ProcessTorrentWorkerConfig, WriterPlanError,
};
use bitmagnet_queue::{
    backoff, process_torrent_job, AdmittedConsumeOutcome, ConsumeOutcome, JobVerdict,
    ProcessTorrentAdmission, ProcessTorrentParams, ProtocolId, QueueJobOptions, QueueStore,
    ADMITTED_FLAGS_ON_PROCESS_TORRENT_CLAIM_SQL, ADMITTED_PROCESS_TORRENT_CLAIM_SQL,
    ORGANIC_PAYLOAD_PREDICATE, ORGANIC_PROCESS_TORRENT_CLAIM_SQL, PROCESS_TORRENT,
};
use bitmagnet_resolver::LiveContentResolver;
use bitmagnet_tmdb::{HttpResponse, RetryPolicy, TmdbClient, TmdbConfig, Transport};
use reqwest::{Url, Version};
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
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
    enqueue_raw_at(pool, payload, 0).await
}

/// [`enqueue_raw`] at `priority`.
async fn enqueue_raw_at(pool: &PgPool, payload: &str, priority: i32) -> String {
    sqlx::query_scalar(
        "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, max_retries, \
         run_after, archival_duration, created_at, priority) \
         VALUES (md5($1), 'process_torrent', 'pending', $1::jsonb, 0, 2, \
                 clock_timestamp() - interval '1 second', interval '168:00:00', \
                 clock_timestamp(), $2) \
         RETURNING id",
    )
    .bind(payload)
    .bind(priority)
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
    // A sourced explicit hint needs a live resolver, which this worker lacks.
    sqlx::query(
        "INSERT INTO torrent_hints \
         (info_hash, content_type, content_source, content_id, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', 'imdb', 'tt0133093', NOW(), NOW())",
    )
    .bind(HASH_A)
    .execute(&pool)
    .await
    .expect("seed sourced hint");
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
    const HASH_SOURCED_2: &str = "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5";
    seed_torrent(&pool, HASH_SOURCED_2, "Another Sourced.mkv", 1_000_000).await;
    // Sourced hints, which this worker (without a live resolver) abstains on.
    for hash in [HASH_A, HASH_SOURCED_2] {
        sqlx::query(
            "INSERT INTO torrent_hints \
             (info_hash, content_type, content_source, content_id, created_at, updated_at) \
             VALUES (decode($1, 'hex'), 'movie', 'imdb', 'tt0133093', NOW(), NOW())",
        )
        .bind(hash)
        .execute(&pool)
        .await
        .expect("seed sourced hint");
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
    assert_eq!(metrics.with_label_values(&["abstained", "hint"]).get(), 1);
    assert_eq!(metrics.with_label_values(&["processed", ""]).get(), 1);

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
    let second = enqueue(&store, &supported_params(&[HASH_SOURCED_2])).await;
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
            metrics.with_label_values(&["error", ""]).get() >= 2
        })
        .await;
        drop(held);
        eventually("the job to be processed", || {
            metrics.with_label_values(&["processed", ""]).get() == 1
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

// --- Blocking failures under a least-privilege role ------------------------

/// The repository's synthetic banned-keyword fixture (the classifier smoke
/// test's): the core workflow deletes it. Go acknowledges a deletion-only batch
/// without blocking, so its job also carries a torrent that classifies.
const BANNED_NAME: &str = "Synthetic paedo marker.txt";
const PROBE_ROLE: &str = "bitmagnet_blocking_probe";
const BLOCKING_FAILURES: &str = "bitmagnet_process_torrent_worker_blocking_failures_total";

/// A writer the log capture appends to.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("capture lock")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Capture {
    /// The captured blocking-failure lines, level first.
    fn blocking_lines(&self) -> Vec<String> {
        let text = String::from_utf8(self.0.lock().expect("capture lock").clone()).unwrap();
        text.lines()
            .filter(|line| line.contains("blocking whole-torrent deletes failed"))
            .map(|line| line.trim_start().to_owned())
            .collect()
    }
}

/// The blocking-failure series as scraped, by label set.
fn scraped_blocking_failures(registry: &prometheus::Registry) -> BTreeMap<String, u64> {
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&registry.gather())
        .expect("encode metrics");
    text.lines()
        .filter_map(|line| line.strip_prefix(BLOCKING_FAILURES)?.strip_prefix('{'))
        .map(|series| {
            let (labels, value) = series.split_once("} ").expect("series value");
            (labels.to_owned(), value.parse().expect("integer count"))
        })
        .collect()
}

/// A login role with DML on every public table but `deleted_torrents`, and no
/// large-object privilege: a worker role missing both blocking grants.
async fn probe_role_pool(pool: &PgPool) -> PgPool {
    for sql in [
        "DO $$ BEGIN \
         IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'bitmagnet_blocking_probe') THEN \
           EXECUTE 'DROP OWNED BY bitmagnet_blocking_probe'; \
           EXECUTE 'DROP ROLE bitmagnet_blocking_probe'; \
         END IF; END $$",
        "CREATE ROLE bitmagnet_blocking_probe LOGIN PASSWORD 'blocking-probe-password' \
         NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
        "GRANT USAGE ON SCHEMA public TO bitmagnet_blocking_probe",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public \
         TO bitmagnet_blocking_probe",
        "GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO bitmagnet_blocking_probe",
        "REVOKE ALL ON deleted_torrents FROM bitmagnet_blocking_probe",
    ] {
        sqlx::query(sql)
            .execute(pool)
            .await
            .unwrap_or_else(|error| panic!("`{sql}`: {error}"));
    }
    let options = PgConnectOptions::from_str(&std::env::var(TEST_DATABASE_URL).unwrap())
        .expect("parse PostgreSQL URL")
        .username(PROBE_ROLE)
        .password("blocking-probe-password");
    PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("connect as the probe role")
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_blocking_failure_logs_and_counts_its_cause_and_keeps_gos_job_text() {
    let pool = connect().await;
    reset(&pool).await;
    seed_torrent(&pool, HASH_A, BANNED_NAME, 1_000).await;
    seed_torrent(&pool, HASH_B, "Synthetic Story.m4b", 60_000_000).await;
    let id = enqueue(
        &QueueStore::new(pool.clone()),
        &supported_params(&[HASH_A, HASH_B]),
    )
    .await;
    let probe = probe_role_pool(&pool).await;
    let registry = prometheus::Registry::new();
    let failures = bitmagnet_processor::register_blocking_failure_metrics(&registry).unwrap();
    let capture = Capture::default();
    let sink = capture.clone();
    let _logs = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || sink.clone())
            .finish(),
    );
    let probe_worker =
        || worker(&probe, fast_config()).with_blocking_failure_metrics(failures.clone());
    let go_text = "job failed to process: blocking whole-torrent deletes failed";
    let line = |op: &str, error: &str| {
        format!(
            "ERROR bitmagnet_processor::worker: blocking whole-torrent deletes failed \
             job_id={id} op=\"{op}\" sqlstate=\"42501\" error=atomic blocking-store commit failed: \
             {error}"
        )
    };

    // Every series exists, at zero, before the first job.
    let scraped = scraped_blocking_failures(&registry);
    assert_eq!(scraped.len(), 4 * 8, "{scraped:?}");
    assert!(scraped.values().all(|count| *count == 0), "{scraped:?}");

    // Without a grant on deleted_torrents, its audit trigger refuses the delete.
    let outcome = probe_worker().run_once().await.unwrap();
    assert!(
        matches!(
            outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::RetryScheduled { .. })
        ),
        "{outcome:?}"
    );
    assert_eq!(job_row(&pool, &id).await.error.as_deref(), Some(go_text));
    // The server's message, without the source line SQLx's text appends.
    let delete_line = line(
        "delete_torrents",
        "failed to delete blocked torrents: permission denied for table deleted_torrents",
    );
    assert_eq!(capture.blocking_lines(), [delete_line.as_str()]);

    // With it, the filter is a large object another role owns.
    sqlx::query("GRANT SELECT, INSERT, UPDATE ON deleted_torrents TO bitmagnet_blocking_probe")
        .execute(&pool)
        .await
        .expect("grant deleted_torrents");
    PersistentBlockingManager::new(pool.clone())
        .block(&[HASH_MISSING.parse().unwrap()], true)
        .await
        .expect("the owner persists the filter");
    let oid: i64 =
        sqlx::query_scalar("SELECT oid::bigint FROM bloom_filters WHERE key = 'blocked_torrents'")
            .fetch_one(&pool)
            .await
            .expect("read the filter's large object");
    make_ready(&pool, &id).await;
    let outcome = probe_worker().run_once().await.unwrap();
    assert!(
        matches!(
            outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::RetryScheduled { .. })
        ),
        "{outcome:?}"
    );
    assert_eq!(job_row(&pool, &id).await.error.as_deref(), Some(go_text));
    let filter_line = line(
        "bloom_filter",
        &format!(
            "failed to read current bloom-filter large object: \
             permission denied for large object {oid}"
        ),
    );
    assert_eq!(
        capture.blocking_lines(),
        [delete_line.as_str(), filter_line.as_str()]
    );
    assert!(live_image(&pool)
        .await
        .contains(&format!("torrent {HASH_A}")));

    // With both grants the same job blocks and deletes the torrent.
    sqlx::query(
        "DO $$ BEGIN \
         EXECUTE format('GRANT SELECT, UPDATE ON LARGE OBJECT %s TO bitmagnet_blocking_probe', \
           (SELECT oid FROM bloom_filters WHERE key = 'blocked_torrents')); \
         END $$",
    )
    .execute(&pool)
    .await
    .expect("grant the filter's large object");
    make_ready(&pool, &id).await;
    let outcome = probe_worker().run_once().await.unwrap();
    assert!(
        matches!(
            outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { .. })
        ),
        "{outcome:?}"
    );
    let image = live_image(&pool).await;
    assert!(!image.contains(&format!("torrent {HASH_A}")), "{image:#?}");
    assert!(image.contains(&format!("torrent {HASH_B}")), "{image:#?}");
    assert_eq!(capture.blocking_lines().len(), 2);

    let scraped = scraped_blocking_failures(&registry);
    let counted: BTreeMap<&str, u64> = scraped
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(labels, count)| (labels.as_str(), *count))
        .collect();
    assert_eq!(
        counted,
        BTreeMap::from([
            ("op=\"bloom_filter\",sqlstate_class=\"42501\"", 1),
            ("op=\"delete_torrents\",sqlstate_class=\"42501\"", 1),
        ])
    );

    probe.close().await;
    for sql in [
        "DROP OWNED BY bitmagnet_blocking_probe",
        "DROP ROLE bitmagnet_blocking_probe",
    ] {
        sqlx::query(sql)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("`{sql}`: {error}"));
    }
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
        version: Version::HTTP_11,
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
    tmdb_worker_with(pool, tmdb, parallelism, fast_config())
}

/// [`tmdb_worker`] with its worker configuration.
fn tmdb_worker_with(
    pool: &PgPool,
    tmdb: &Arc<FakeTmdb>,
    parallelism: usize,
    worker_config: ProcessTorrentWorkerConfig,
) -> ProcessTorrentWorker {
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
        worker_config,
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

// -- Sourced, enriched and ambiguous hints (M3b) ---------------------------

/// TMDB's `/find` and details for the local movie's id, `tmdb:42`. The details
/// differ from the local row, so a job holding both images conflicts.
const FIND_42: &str = r#"{"movie_results":[{"id":42,"title":"Reusable Movie"}],"tv_results":[]}"#;
const DETAILS_42: &str = r#"{"id":42,"title":"Reusable Movie","original_title":"Reusable Movie","original_language":"en","release_date":"1999-06-01","imdb_id":"tt0000042","genres":[]}"#;
const FIND_777: &str = r#"{"movie_results":[{"id":777,"title":"Found Film"}],"tv_results":[]}"#;
const UNSUPPORTED_HINT: &str =
    "writer plan cannot reproduce the complete Go hint/enrichment path for";

/// Insert `info_hash`'s explicit hint: `content_type`, then each
/// `(column, SQL literal)` in `extra`.
async fn seed_hint(pool: &PgPool, info_hash: &str, content_type: &str, extra: &[(&str, &str)]) {
    let columns: String = extra
        .iter()
        .map(|(column, _)| format!(", {column}"))
        .collect();
    let values: String = extra
        .iter()
        .map(|(_, value)| format!(", {value}"))
        .collect();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO torrent_hints (info_hash, content_type{columns}, created_at, updated_at) \
         VALUES (decode($1, 'hex'), $2{values}, NOW(), NOW())"
    )))
    .bind(info_hash)
    .bind(content_type)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed hint for {info_hash}: {error}"));
}

/// A `movie/tmdb/<id>` content row titled `title`, with an `imdb` id
/// attribute when `imdb` is given.
async fn seed_movie(pool: &PgPool, id: &str, title: &str, imdb: Option<&str>) {
    sqlx::query(
        "INSERT INTO content (type, source, id, title, release_year, tsv, created_at, updated_at) \
         VALUES ('movie', 'tmdb', $1, $2, 1999, ''::tsvector, \
                 '2024-01-02 03:04:05+00', '2024-01-02 03:04:05+00')",
    )
    .bind(id)
    .bind(title)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed movie {id}: {error}"));
    if let Some(imdb) = imdb {
        sqlx::query(
            "INSERT INTO content_attributes \
             (content_type, content_source, content_id, source, key, value, created_at, updated_at) \
             VALUES ('movie', 'tmdb', $1, 'imdb', 'id', $2, NOW(), NOW())",
        )
        .bind(id)
        .bind(imdb)
        .execute(pool)
        .await
        .unwrap_or_else(|error| panic!("seed movie {id}'s imdb id: {error}"));
    }
}

/// An existing `torrent_contents` association, as an earlier run left it.
async fn seed_association(
    pool: &PgPool,
    info_hash: &str,
    content_type: &str,
    source: Option<&str>,
    id: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO torrent_contents \
         (info_hash, content_type, content_source, content_id, languages, episodes, \
          created_at, updated_at, tsv, published_at, size) \
         VALUES (decode($1, 'hex'), $2, $3, $4, '[]'::jsonb, NULL, \
                 NOW(), NOW(), ''::tsvector, NOW(), 1)",
    )
    .bind(info_hash)
    .bind(content_type)
    .bind(source)
    .bind(id)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed association for {info_hash}: {error}"));
}

/// `info_hash`'s `torrent_contents` rows, as `(content_id, row)` JSON.
async fn associations(pool: &PgPool, info_hash: &str) -> Vec<(Option<String>, Value)> {
    sqlx::query(
        "SELECT content_id, (to_jsonb(t) - 'created_at' - 'updated_at') AS row \
         FROM torrent_contents t WHERE info_hash = decode($1, 'hex') ORDER BY id",
    )
    .bind(info_hash)
    .fetch_all(pool)
    .await
    .expect("read associations")
    .into_iter()
    .map(|row| (row.get("content_id"), row.get("row")))
    .collect()
}

/// Under the switch, a sourced hint is looked up locally whatever the flags,
/// as Go's `attach_local_content_by_id` is ungated: by primary key for
/// `tmdb`, and by an `imdb` attribute in identity order, so of two content
/// rows carrying the id the first in `(type, source, id)` order wins. Without
/// the switch the job is left to Go with the same text as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_sourced_hint_attaches_by_canonical_and_alternative_id() {
    let pool = connect().await;
    let hashes = [HASH_UNKNOWN, HASH_FOUND];
    for flags_on in [false, true] {
        seed_tmdb_fixture(&pool).await;
        // Inserted after `tmdb:42`, which also carries `zuluvalue`, but
        // first in identity order.
        seed_movie(&pool, "41", "Twin Movie", Some("zuluvalue")).await;
        seed_hint(
            &pool,
            HASH_UNKNOWN,
            "movie",
            &[("content_source", "'tmdb'"), ("content_id", "'42'")],
        )
        .await;
        seed_hint(
            &pool,
            HASH_FOUND,
            "movie",
            &[("content_source", "'imdb'"), ("content_id", "'zuluvalue'")],
        )
        .await;
        let tmdb = FakeTmdb::new(found_answers());
        let params = if flags_on {
            flags_on_params(&hashes)
        } else {
            supported_params(&hashes)
        };
        let id = enqueue(&QueueStore::new(pool.clone()), &params).await;
        let outcome = tmdb_worker(&pool, &tmdb, 1).run_once().await.unwrap();
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "flags_on={flags_on}: {outcome:?}"
        );
        let image = live_image(&pool).await;
        assert!(attached(&image, HASH_UNKNOWN, "\"42\""), "{image:#?}");
        assert!(attached(&image, HASH_FOUND, "\"41\""), "{image:#?}");
        assert_eq!(image.iter().filter(|row| row.starts_with("tc ")).count(), 2);
        // Attached by ID, so neither torrent was searched for.
        assert!(
            tmdb.asked().is_empty(),
            "flags_on={flags_on}: {:?}",
            tmdb.asked()
        );
    }

    seed_tmdb_fixture(&pool).await;
    seed_hint(
        &pool,
        HASH_UNKNOWN,
        "movie",
        &[("content_source", "'tmdb'"), ("content_id", "'42'")],
    )
    .await;
    let id = enqueue(
        &QueueStore::new(pool.clone()),
        &supported_params(&[HASH_UNKNOWN]),
    )
    .await;
    let before = job_row(&pool, &id).await;
    let image_before = live_image(&pool).await;
    let outcome = worker(&pool, fast_config()).run_once().await.unwrap();
    let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
        panic!("expected an abstention, got {outcome:?}");
    };
    assert_eq!(job.id, id);
    assert_eq!(reason, format!("{UNSUPPORTED_HINT} {HASH_UNKNOWN}"));
    assert_eq!(job_row(&pool, &id).await, before);
    assert_eq!(live_image(&pool).await, image_before);
}

/// A sourced hint TMDB has to find: `/find`, the first `movie_results` entry,
/// then its details, ahead of (and instead of) any title search.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_sourced_hint_finds_tmdb_content_by_its_external_id() {
    let pool = connect().await;
    seed_tmdb_fixture(&pool).await;
    seed_hint(
        &pool,
        HASH_UNKNOWN,
        "movie",
        &[("content_source", "'imdb'"), ("content_id", "'tt0000777'")],
    )
    .await;
    let tmdb = FakeTmdb::new(vec![
        ("/find/tt0000777", answer(200, None, FIND_777)),
        ("/movie/777", answer(200, None, FOUND_DETAILS)),
    ]);
    let id = enqueue(
        &QueueStore::new(pool.clone()),
        &flags_on_params(&[HASH_UNKNOWN]),
    )
    .await;
    let outcome = tmdb_worker(&pool, &tmdb, 1).run_once().await.unwrap();
    assert!(
        matches!(
            &outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
        ),
        "{outcome:?}"
    );
    let image = live_image(&pool).await;
    assert!(attached(&image, HASH_UNKNOWN, "\"777\""), "{image:#?}");
    assert!(
        image
            .iter()
            .any(|row| row.starts_with("content ") && row.contains("\"id\": \"777\"")),
        "{image:#?}"
    );
    assert_eq!(tmdb.count("/authentication"), 1, "{:?}", tmdb.asked());
    assert_eq!(
        tmdb.count("/find/tt0000777?external_source=imdb_id"),
        1,
        "{:?}",
        tmdb.asked()
    );
    assert_eq!(tmdb.count("/movie/777"), 1, "{:?}", tmdb.asked());
    assert_eq!(tmdb.asked().len(), 3, "{:?}", tmdb.asked());
    assert_eq!(queue_job_count(&pool).await, 1);
}

/// `/find` failing with a 404 fails that torrent, as any non-2xx other than
/// 401 does in Go, while a details 404 is Go's `ErrNotFound`: unmatched, and
/// the classification goes on to search by title.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_find_404_fails_its_torrent_and_a_details_404_is_unmatched() {
    let pool = connect().await;
    seed_tmdb_fixture(&pool).await;
    seed_hint(
        &pool,
        HASH_UNKNOWN,
        "movie",
        &[("content_source", "'imdb'"), ("content_id", "'tt0000404'")],
    )
    .await;
    let mut answers = vec![("/find/tt0000404", answer(404, None, "{}"))];
    answers.extend(found_answers());
    let tmdb = FakeTmdb::new(answers);
    let id = enqueue(
        &QueueStore::new(pool.clone()),
        &flags_on_params(&[HASH_UNKNOWN, HASH_FOUND]),
    )
    .await;
    let outcome = tmdb_worker(&pool, &tmdb, 1).run_once().await.unwrap();
    assert!(
        matches!(
            &outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
        ),
        "{outcome:?}"
    );
    let (payload, status, ..) = other_job(&pool, &id).await;
    assert_eq!(
        serde_json::from_str::<Value>(&payload).unwrap(),
        flags_on_retry_payload(&[HASH_UNKNOWN])
    );
    assert_eq!(status, "pending");
    let image = live_image(&pool).await;
    assert!(attached(&image, HASH_FOUND, "\"777\""), "{image:#?}");
    assert!(associations(&pool, HASH_UNKNOWN).await.is_empty());
    assert_eq!(tmdb.count("/find/tt0000404"), 1, "{:?}", tmdb.asked());
    assert_eq!(tmdb.count("query=Unknown"), 0, "{:?}", tmdb.asked());

    seed_tmdb_fixture(&pool).await;
    seed_hint(
        &pool,
        HASH_UNKNOWN,
        "movie",
        &[("content_source", "'imdb'"), ("content_id", "'tt0000777'")],
    )
    .await;
    let tmdb = FakeTmdb::new(vec![
        ("/find/tt0000777", answer(200, None, FIND_777)),
        ("/movie/777", answer(404, None, "{}")),
    ]);
    let id = enqueue(
        &QueueStore::new(pool.clone()),
        &flags_on_params(&[HASH_UNKNOWN]),
    )
    .await;
    let outcome = tmdb_worker(&pool, &tmdb, 1).run_once().await.unwrap();
    assert!(
        matches!(
            &outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
        ),
        "{outcome:?}"
    );
    assert_eq!(
        queue_job_count(&pool).await,
        1,
        "an unmatched torrent is not retried"
    );
    let unmatched = associations(&pool, HASH_UNKNOWN).await;
    assert_eq!(unmatched.len(), 1, "{unmatched:?}");
    assert_eq!(unmatched[0].0, None, "{unmatched:?}");
    assert_eq!(tmdb.count("/find/tt0000777"), 1, "{:?}", tmdb.asked());
    assert_eq!(tmdb.count("/movie/777"), 1, "{:?}", tmdb.asked());
    assert_eq!(tmdb.count("query=Unknown"), 1, "{:?}", tmdb.asked());
}

/// A source-less hint with an id is not a lookup: Go's `hasHintedContentId`
/// needs a source. One with a source is, even on a flags-off job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn only_a_sourced_hint_reaches_the_resolver_on_a_flags_off_job() {
    let pool = connect().await;
    for (source, calls) in [("NULL", 0), ("'imdb'", 1)] {
        reset(&pool).await;
        seed_torrent(
            &pool,
            HASH_UNKNOWN,
            "Unknown Picture 2004 720p WEB-DL x264-GRP.mkv",
            1_000_000_000,
        )
        .await;
        seed_hint(
            &pool,
            HASH_UNKNOWN,
            "movie",
            &[("content_source", source), ("content_id", "'tt0133093'")],
        )
        .await;
        let id = enqueue(
            &QueueStore::new(pool.clone()),
            &supported_params(&[HASH_UNKNOWN]),
        )
        .await;
        let resolver = Arc::new(CountingResolver::default());
        let outcome = counting_worker(&pool, &resolver, fast_config())
            .run_once()
            .await
            .unwrap();
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "source {source}: {outcome:?}"
        );
        assert_eq!(
            resolver.calls.load(Ordering::SeqCst),
            calls,
            "source {source}"
        );
    }
}

/// A source-less enriched hint reuses the torrent's association: Go takes the
/// association's type, source and id, and keeps every attribute the hint
/// carries, which then outrank what the name parses to.
#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn an_enriched_hint_keeps_its_attributes_through_content_reuse() {
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
    seed_association(&pool, HASH_LOCAL, "movie", Some("tmdb"), Some("42")).await;
    seed_hint(
        &pool,
        HASH_LOCAL,
        "movie",
        &[
            ("languages", "'[\"fr\"]'::jsonb"),
            ("episodes", "'{\"1\": {\"2\": {}}}'::jsonb"),
            ("video_resolution", "'V2160p'"),
            ("video_source", "'webdl'"),
            ("video_codec", "'xvid'"),
            ("release_group", "''"),
        ],
    )
    .await;
    let id = enqueue(
        &QueueStore::new(pool.clone()),
        &supported_params(&[HASH_LOCAL]),
    )
    .await;
    let outcome = worker(&pool, fast_config()).run_once().await.unwrap();
    assert!(
        matches!(
            &outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
        ),
        "{outcome:?}"
    );
    let rows = associations(&pool, HASH_LOCAL).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let (content_id, row) = &rows[0];
    assert_eq!(content_id.as_deref(), Some("42"));
    for (column, value) in [
        ("languages", serde_json::json!(["fr"])),
        ("episodes", serde_json::json!({"1": {"2": {}}})),
        ("video_resolution", serde_json::json!("V2160p")),
        ("video_source", serde_json::json!("WEBDL")),
        ("video_codec", serde_json::json!("XviD")),
        ("release_group", serde_json::json!("")),
    ] {
        assert_eq!(row[column], value, "{column}: {row}");
    }
}

/// Rematch reuses nothing: the classification searches again, attaches what
/// it finds, and the stale association is deleted. The default mode reuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn rematch_searches_again_where_the_default_mode_reuses() {
    let pool = connect().await;
    for (classify_mode, kept) in [
        (bitmagnet_queue::CLASSIFY_MODE_DEFAULT, "41"),
        (bitmagnet_queue::CLASSIFY_MODE_REMATCH, "42"),
    ] {
        seed_tmdb_fixture(&pool).await;
        seed_movie(&pool, "41", "Stale Movie", None).await;
        seed_association(&pool, HASH_LOCAL, "movie", Some("tmdb"), Some("41")).await;
        let tmdb = FakeTmdb::new(found_answers());
        let params = ProcessTorrentParams {
            classify_mode,
            ..flags_on_params(&[HASH_LOCAL])
        };
        let id = enqueue(&QueueStore::new(pool.clone()), &params).await;
        let outcome = tmdb_worker(&pool, &tmdb, 1).run_once().await.unwrap();
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "mode {classify_mode}: {outcome:?}"
        );
        let rows = associations(&pool, HASH_LOCAL).await;
        assert_eq!(
            rows.iter().map(|(id, _)| id.as_deref()).collect::<Vec<_>>(),
            [Some(kept)],
            "mode {classify_mode}"
        );
        assert!(tmdb.asked().is_empty(), "{:?}", tmdb.asked());
    }
}

/// An empty episode map is not applied (`len > 0` in Go), so nothing is
/// written for it; one season is.
#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn an_empty_episode_hint_writes_null_episodes() {
    let pool = connect().await;
    for (episodes, persisted) in [
        ("'{}'::jsonb", None),
        ("'{\"1\": {}}'::jsonb", Some("{\"1\": {}}")),
    ] {
        reset(&pool).await;
        seed_fixture_torrents(&pool).await;
        seed_hint(
            &pool,
            HASH_A,
            "music",
            &[("episodes", episodes), ("release_group", "''")],
        )
        .await;
        let id = enqueue(&QueueStore::new(pool.clone()), &supported_params(&[HASH_A])).await;
        let outcome = worker(&pool, fast_config()).run_once().await.unwrap();
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "{episodes}: {outcome:?}"
        );
        let row: (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT episodes::text, release_group FROM torrent_contents \
             WHERE info_hash = decode($1, 'hex')",
        )
        .bind(HASH_A)
        .fetch_one(&pool)
        .await
        .expect("one association");
        assert_eq!(
            (row.0.as_deref(), row.1.as_deref()),
            (persisted, Some("")),
            "{episodes}"
        );
    }
}

/// Where Go's pick among the associations depends on row order, the job is
/// left to Go and counted as `ambiguous`: two reusable associations, or, with
/// no file list and no hint, source-less ones of two types.
#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn order_dependent_associations_abstain_as_ambiguous() {
    let pool = connect().await;
    reset(&pool).await;
    seed_local_movie(&pool).await;
    seed_movie(&pool, "41", "Stale Movie", None).await;
    seed_fixture_torrents(&pool).await;
    for id in ["41", "42"] {
        seed_association(&pool, HASH_A, "movie", Some("tmdb"), Some(id)).await;
    }
    sqlx::query("UPDATE torrents SET files_status = 'no_info' WHERE info_hash = decode($1, 'hex')")
        .bind(HASH_B)
        .execute(&pool)
        .await
        .expect("drop the file list");
    for content_type in ["xxx", "comic"] {
        seed_association(&pool, HASH_B, content_type, None, None).await;
    }
    let store = QueueStore::new(pool.clone());
    let reuse = enqueue(&store, &supported_params(&[HASH_A])).await;
    let preserved = enqueue(&store, &supported_params(&[HASH_B])).await;
    let image_before = live_image(&pool).await;
    let registry = prometheus::Registry::new();
    let metrics = bitmagnet_processor::register_worker_metrics(&registry).unwrap();
    let worker = worker(&pool, fast_config()).with_metrics(metrics.clone());
    let mut reasons = BTreeMap::new();
    for _ in 0..2 {
        let outcome = worker.run_once().await.unwrap();
        let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
            panic!("expected an abstention, got {outcome:?}");
        };
        reasons.insert(job.id, reason);
    }
    assert_eq!(
        reasons,
        BTreeMap::from([
            (
                reuse,
                WriterPlanError::AmbiguousContentReuse {
                    info_hash: HASH_A.to_owned()
                }
                .to_string()
            ),
            (
                preserved,
                WriterPlanError::AmbiguousPreservedContentType {
                    info_hash: HASH_B.to_owned()
                }
                .to_string()
            ),
        ])
    );
    assert_eq!(
        metrics.with_label_values(&["abstained", "ambiguous"]).get(),
        2
    );
    assert_eq!(metrics.with_label_values(&["abstained", "hint"]).get(), 0);
    assert_eq!(live_image(&pool).await, image_before);
}

/// A sourced hint TMDB finds as content the job also attaches from the local
/// table: Go writes both images, Rust leaves the job to Go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_found_hint_conflicting_with_a_local_attach_abstains() {
    let pool = connect().await;
    seed_tmdb_fixture(&pool).await;
    seed_hint(
        &pool,
        HASH_UNKNOWN,
        "movie",
        &[("content_source", "'imdb'"), ("content_id", "'tt0000042'")],
    )
    .await;
    let tmdb = FakeTmdb::new(vec![
        ("/find/tt0000042", answer(200, None, FIND_42)),
        ("/movie/42", answer(200, None, DETAILS_42)),
    ]);
    let id = enqueue(
        &QueueStore::new(pool.clone()),
        &flags_on_params(&[HASH_LOCAL, HASH_UNKNOWN]),
    )
    .await;
    let before = job_row(&pool, &id).await;
    let image_before = live_image(&pool).await;
    let outcome = tmdb_worker(&pool, &tmdb, 1).run_once().await.unwrap();
    let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
        panic!("expected an abstention, got {outcome:?}");
    };
    assert_eq!(job.id, id);
    assert!(
        reason.contains("conflicting attached content images for movie:tmdb:42"),
        "{reason}"
    );
    assert_eq!(job_row(&pool, &id).await, before);
    assert_eq!(live_image(&pool).await, image_before);
    assert_eq!(queue_job_count(&pool).await, 1);
}

/// Every row of Go's decode fixture, stored and loaded back through the
/// processor's own query, decodes exactly as the fixture's text does.
#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn the_go_decode_fixture_round_trips_through_the_loader() {
    use bitmagnet_processor::load::{effective_hint, load_torrents, HintAdmission, StoredHint};

    let pool = connect().await;
    reset(&pool).await;
    let text = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/torrent-hint-decode/torrent_hint_decode.jsonl"
    ));
    let text_of = |row: &Value, field: &str| row[field].as_str().map(str::to_owned);
    let mut fixtures = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let fixture: Value = serde_json::from_str(line).expect("fixture line");
        let row = &fixture["input"]["row"];
        let info_hash = format!("{:040x}", index + 1);
        seed_torrent(&pool, &info_hash, "Hint Fixture.mkv", 1_000_000).await;
        if let Some(source) = row["contentSource"].as_str() {
            sqlx::query(
                "INSERT INTO metadata_sources (key, name, created_at, updated_at) \
                 VALUES ($1, $1, NOW(), NOW()) ON CONFLICT (key) DO NOTHING",
            )
            .bind(source)
            .execute(&pool)
            .await
            .expect("seed metadata source");
        }
        sqlx::query(
            "INSERT INTO torrent_hints \
             (info_hash, content_type, content_source, content_id, title, release_year, \
              languages, episodes, video_resolution, video_source, video_codec, video_3d, \
              video_modifier, release_group, created_at, updated_at) \
             VALUES (decode($1, 'hex'), $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, \
                     $9, $10, $11, $12, $13, $14, NOW(), NOW())",
        )
        .bind(&info_hash)
        .bind(text_of(row, "contentType"))
        .bind(text_of(row, "contentSource"))
        .bind(text_of(row, "contentId"))
        .bind(text_of(row, "title"))
        .bind(
            row["releaseYear"]
                .as_i64()
                .map(|year| i32::try_from(year).expect("year")),
        )
        .bind(text_of(row, "languages"))
        .bind(text_of(row, "episodes"))
        .bind(text_of(row, "videoResolution"))
        .bind(text_of(row, "videoSource"))
        .bind(text_of(row, "videoCodec"))
        .bind(text_of(row, "video3d"))
        .bind(text_of(row, "videoModifier"))
        .bind(text_of(row, "releaseGroup"))
        .execute(&pool)
        .await
        .unwrap_or_else(|error| panic!("{}: {error}", fixture["id"]));
        let stored = StoredHint {
            content_type: text_of(row, "contentType").expect("content type"),
            content_source: text_of(row, "contentSource"),
            content_id: text_of(row, "contentId"),
            languages: text_of(row, "languages"),
            episodes: text_of(row, "episodes"),
            video_resolution: text_of(row, "videoResolution"),
            video_source: text_of(row, "videoSource"),
            video_codec: text_of(row, "videoCodec"),
            video_3d: text_of(row, "video3d"),
            video_modifier: text_of(row, "videoModifier"),
            release_group: text_of(row, "releaseGroup"),
        };
        fixtures.push((
            fixture["id"].as_str().expect("id").to_owned(),
            info_hash,
            stored,
        ));
    }
    assert!(fixtures.len() >= 140, "{}", fixtures.len());

    let (mut supported, mut sourced, mut refused) = (0, 0, 0);
    for chunk in fixtures.chunks(50) {
        let hashes: Vec<&str> = chunk.iter().map(|(_, hash, _)| hash.as_str()).collect();
        let loaded = load_torrents(&pool, &supported_params(&hashes))
            .await
            .expect("load the fixture hints");
        let loaded: BTreeMap<&str, _> = loaded
            .iter()
            .map(|torrent| (torrent.info_hash.as_str(), torrent))
            .collect();
        for (id, info_hash, stored) in chunk {
            let torrent = loaded[info_hash.as_str()];
            match stored.clone().decode() {
                Err(detail) => {
                    assert_eq!(
                        torrent.hint_admission,
                        HintAdmission::Unsupported { detail },
                        "{id}"
                    );
                    refused += 1;
                }
                Ok(hint) => {
                    let admission = if hint.content_source.is_some() {
                        sourced += 1;
                        HintAdmission::NeedsLiveResolver
                    } else {
                        supported += 1;
                        HintAdmission::Supported
                    };
                    assert_eq!(torrent.hint_admission, admission, "{id}");
                    assert_eq!(
                        format!("{:?}", torrent.classifier_input.hint),
                        format!("{:?}", effective_hint(Some(hint), &[], true, true)),
                        "{id}"
                    );
                }
            }
        }
    }
    assert!(
        supported > 0 && sourced > 0 && refused > 0,
        "{supported} {sourced} {refused}"
    );
}

// -- Organic admission (M4) ------------------------------------------------

/// A worker configuration admitting organic payloads up to `max_priority`.
fn organic_config(max_priority: i32) -> ProcessTorrentWorkerConfig {
    ProcessTorrentWorkerConfig {
        organic_max_priority: Some(max_priority),
        ..fast_config()
    }
}

/// The ids `admission` claims, in claim order, leaving every row as it was:
/// each claimed job is abstained, then excluded from the next claim.
async fn probe_claims(pool: &PgPool, admission: ProcessTorrentAdmission) -> Vec<String> {
    let store = QueueStore::new(pool.clone());
    let mut claimed: Vec<String> = Vec::new();
    loop {
        match store
            .consume_admitted_process_torrent_with(admission, &claimed, |_| async {
                JobVerdict::Abstain("probe".to_owned())
            })
            .await
            .expect("probe claim")
        {
            AdmittedConsumeOutcome::Abstained { job, .. } => {
                assert!(
                    !claimed.contains(&job.id),
                    "{admission:?} claimed the excluded job {}",
                    job.id
                );
                claimed.push(job.id);
            }
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty) => return claimed,
            other => panic!("{admission:?}: unexpected probe outcome {other:?}"),
        }
    }
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn organic_admission_claims_each_producer_shape_within_its_priority_bound() {
    let pool = connect().await;
    reset(&pool).await;
    let hashes = |n: u8| format!("[\"{}\"]", format!("{n:02x}").repeat(20));
    // (payload, priority, organic), inserted in this order, which within a
    // priority is Go's claim order.
    let jobs = [
        // The DHT crawler, and every processor retry: priority 0.
        (format!("{{\"InfoHashes\":{}}}", hashes(1)), 0, true),
        (
            format!(
                "{{\"ClassifierWorkflow\":\"default\",\"InfoHashes\":{}}}",
                hashes(2)
            ),
            0,
            true,
        ),
        (
            format!(
                "{{\"ClassifyMode\":1,\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{{\"tmdb_enabled\":false}},\"InfoHashes\":{}}}",
                hashes(3)
            ),
            0,
            true,
        ),
        // The explicit subsets are organic shapes too.
        (
            format!(
                "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_OFF},\"InfoHashes\":{}}}",
                hashes(4)
            ),
            0,
            true,
        ),
        // Batch children with the APIs off: priority 4.
        (
            format!(
                "{{\"ClassifierFlags\":{{\"apis_enabled\":false}},\"InfoHashes\":{}}}",
                hashes(5)
            ),
            4,
            true,
        ),
        // Batch children: priority 10.
        (
            format!("{{\"ClassifyMode\":1,\"InfoHashes\":{}}}", hashes(6)),
            10,
            true,
        ),
        (
            format!(
                "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_ON},\"InfoHashes\":{}}}",
                hashes(7)
            ),
            10,
            true,
        ),
        // The importer: priority 20.
        (format!("{{\"InfoHashes\":{}}}", hashes(8)), 20, true),
        // Payloads only Go runs, within every bound.
        (
            format!(
                "{{\"ClassifierWorkflow\":\"custom\",\"InfoHashes\":{}}}",
                hashes(9)
            ),
            0,
            false,
        ),
        (
            format!(
                "{{\"ClassifierFlags\":{{\"delete_xxx\":true}},\"InfoHashes\":{}}}",
                hashes(10)
            ),
            0,
            false,
        ),
        (
            format!(
                "{{\"ClassifierFlags\":{{\"tmdb_enabled\":null}},\"InfoHashes\":{}}}",
                hashes(11)
            ),
            0,
            false,
        ),
        (format!("{{\"infoHashes\":{}}}", hashes(12)), 0, false),
        (
            format!("{{\"InfoHashes\":[\"0x{}\"]}}", "0d".repeat(20)),
            0,
            false,
        ),
        (
            format!("{{\"InfoHashes\":{},\"Extra\":1}}", hashes(14)),
            0,
            false,
        ),
    ];
    let mut ids = Vec::new();
    for (payload, priority, _) in &jobs {
        ids.push(enqueue_raw_at(&pool, payload, *priority).await);
    }
    let mut before = Vec::new();
    for id in &ids {
        before.push(job_row(&pool, id).await);
    }

    for max_priority in [0, 3, 4, 9, 10, 19, 20] {
        let mut expected: Vec<(i32, String)> = jobs
            .iter()
            .zip(&ids)
            .filter(|((_, priority, organic), _)| *organic && *priority <= max_priority)
            .map(|((_, priority, _), id)| (*priority, id.clone()))
            .collect();
        expected.sort_by_key(|(priority, _)| *priority);
        let expected: Vec<String> = expected.into_iter().map(|(_, id)| id).collect();
        assert_eq!(
            probe_claims(&pool, ProcessTorrentAdmission::Organic { max_priority }).await,
            expected,
            "bound {max_priority}"
        );
    }
    // Not a superset of flags-on: the explicit flags-on job above the bound
    // stays with Go.
    assert_eq!(
        probe_claims(&pool, ProcessTorrentAdmission::FlagsOffOrOn).await,
        vec![ids[3].clone(), ids[6].clone()]
    );
    for (id, before) in ids.iter().zip(&before) {
        assert_eq!(&job_row(&pool, id).await, before, "job {id} was touched");
    }

    // The worker's admission follows its configuration, given a live
    // materializer.
    let tmdb = FakeTmdb::new(Vec::new());
    assert_eq!(
        tmdb_worker_with(&pool, &tmdb, 1, organic_config(4)).admission(),
        ProcessTorrentAdmission::Organic { max_priority: 4 }
    );
    assert_eq!(
        tmdb_worker(&pool, &tmdb, 1).admission(),
        ProcessTorrentAdmission::FlagsOffOrOn
    );
    assert_eq!(
        worker(&pool, organic_config(4)).admission(),
        ProcessTorrentAdmission::FlagsOff
    );
}

const DHT_PAYLOAD_SQL: &str =
    "jsonb_build_object('InfoHashes', jsonb_build_array(lpad(to_hex(n), 40, '0')))";
const BATCH_PAYLOAD_SQL: &str = "jsonb_build_object('ClassifierFlags', \
     jsonb_build_object('apis_enabled', true), \
     'InfoHashes', jsonb_build_array(lpad(to_hex(n), 40, '0')))";

/// Insert `count` ready jobs at `priority`. `payload_sql` builds each payload
/// from `n`; `tag` keeps the fingerprints of separate calls apart.
async fn seed_ready_jobs(pool: &PgPool, tag: &str, count: i32, priority: i32, payload_sql: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO queue_jobs (fingerprint, queue, status, payload, retries, max_retries, \
         run_after, archival_duration, created_at, priority) \
         SELECT md5($1 || n::text), 'process_torrent', 'pending', {payload_sql}, \
                0, 2, clock_timestamp() - interval '1 hour', interval '168:00:00', \
                clock_timestamp(), $2 \
         FROM generate_series(1, $3) AS n"
    )))
    .bind(tag)
    .bind(priority)
    .bind(count)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed {tag} jobs: {error}"));
    sqlx::raw_sql("VACUUM ANALYZE queue_jobs")
        .execute(pool)
        .await
        .expect("vacuum queue_jobs");
}

/// `EXPLAIN (<options>)` of the organic claim with `excluded` and
/// `max_priority`: planned for those values, or prepared and generic.
async fn explain_organic_claim(
    pool: &PgPool,
    options: &str,
    excluded: &[String],
    max_priority: i32,
    generic: bool,
) -> String {
    let mut tx = pool.begin().await.expect("begin plan transaction");
    let plan: Vec<String> = if generic {
        sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
            .execute(&mut *tx)
            .await
            .expect("force a generic plan");
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "PREPARE organic_claim(text[], integer) AS {ORGANIC_PROCESS_TORRENT_CLAIM_SQL}"
        )))
        .execute(&mut *tx)
        .await
        .expect("prepare the organic claim");
        // EXPLAIN EXECUTE takes no bind parameters: spell the values out.
        let excluded = excluded
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");
        let plan = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "EXPLAIN ({options}) EXECUTE organic_claim(ARRAY[{excluded}]::text[], {max_priority})"
        )))
        .fetch_all(&mut *tx)
        .await
        .expect("explain the generic organic claim");
        // A prepared statement outlives the transaction.
        sqlx::raw_sql("DEALLOCATE organic_claim")
            .execute(&mut *tx)
            .await
            .expect("deallocate the organic claim");
        plan
    } else {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "EXPLAIN ({options}) {ORGANIC_PROCESS_TORRENT_CLAIM_SQL}"
        )))
        .bind(excluded)
        .bind(max_priority)
        .fetch_all(&mut *tx)
        .await
        .expect("explain the custom organic claim")
    };
    tx.rollback().await.expect("roll back plan transaction");
    plan.join("\n")
}

/// The organic claim walks Go's `queue_jobs_claim_idx` in claim order and
/// checks its bound on the index tuples.
fn assert_organic_claim_plan(plan: &str, generic: bool) {
    let kind = if generic { "generic" } else { "custom" };
    assert!(
        plan.contains("Index Scan using queue_jobs_claim_idx on queue_jobs"),
        "the {kind} organic claim must scan Go's queue_jobs_claim_idx:\n{plan}"
    );
    let index_cond = plan
        .lines()
        .find(|line| line.trim_start().starts_with("Index Cond:"))
        .unwrap_or_else(|| panic!("the {kind} organic claim has no Index Cond:\n{plan}"));
    assert!(
        index_cond.contains("priority <="),
        "the {kind} organic claim must check its bound on the index tuples:\n{plan}"
    );
    for index in [ADMITTED_CLAIM_INDEX, ADMITTED_FLAGS_ON_CLAIM_INDEX] {
        assert!(!plan.contains(index), "{plan}");
    }
    assert!(!plan.contains("Sort"), "{plan}");
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn organic_claim_steps_over_higher_bands_on_gos_claim_index() {
    let pool = connect().await;
    reset(&pool).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("server version");
    let report = |case: &str, generic: bool, plan: &str| {
        // Written past libtest's output capture, for the gate report.
        let _ = writeln!(
            std::io::stderr(),
            "worker_pg: organic claim on PostgreSQL {version}, {case}, {} plan:\n{plan}\n",
            if generic { "generic" } else { "custom" }
        );
    };
    // A whole-database batch backlog above the bound.
    seed_ready_jobs(&pool, "batch", 100_000, 10, BATCH_PAYLOAD_SQL).await;

    // Worst case (a): nothing within the bound.
    for generic in [false, true] {
        let _warm = explain_organic_claim(&pool, "ANALYZE, BUFFERS", &[], 0, generic).await;
        let plan = explain_organic_claim(&pool, "ANALYZE, BUFFERS", &[], 0, generic).await;
        assert_organic_claim_plan(&plan, generic);
        assert!(
            plan.lines()
                .next()
                .is_some_and(|line| line.contains("rows=0")),
            "{plan}"
        );
        report(
            "(a) no row within the bound behind 100k at priority 10",
            generic,
            &plan,
        );
    }

    // Worst case (b): 1,024 rows within the bound, every one excluded.
    seed_ready_jobs(&pool, "excluded", 1_024, 0, DHT_PAYLOAD_SQL).await;
    let excluded: Vec<String> = sqlx::query_scalar("SELECT id FROM queue_jobs WHERE priority = 0")
        .fetch_all(&pool)
        .await
        .expect("read the excluded ids");
    assert_eq!(excluded.len(), 1_024);
    for generic in [false, true] {
        let _warm = explain_organic_claim(&pool, "ANALYZE, BUFFERS", &excluded, 0, generic).await;
        let plan = explain_organic_claim(&pool, "ANALYZE, BUFFERS", &excluded, 0, generic).await;
        assert_organic_claim_plan(&plan, generic);
        assert!(
            plan.lines()
                .next()
                .is_some_and(|line| line.contains("rows=0")),
            "{plan}"
        );
        report(
            "(b) as (a) plus 1,024 excluded rows within the bound",
            generic,
            &plan,
        );
    }

    // An organic DHT backlog within the bound.
    seed_ready_jobs(&pool, "dht", 20_000, 0, DHT_PAYLOAD_SQL).await;
    for generic in [false, true] {
        let plan = explain_organic_claim(&pool, "COSTS OFF", &[], 0, generic).await;
        assert_organic_claim_plan(&plan, generic);
        report("20k DHT rows within the bound", generic, &plan);
    }
    let store = QueueStore::new(pool.clone());
    let outcome = store
        .consume_admitted_process_torrent_with(
            ProcessTorrentAdmission::Organic { max_priority: 0 },
            &excluded,
            |_| async { JobVerdict::Abstain("probe".to_owned()) },
        )
        .await
        .unwrap();
    let AdmittedConsumeOutcome::Abstained { job, .. } = outcome else {
        panic!("expected a claim, got {outcome:?}");
    };
    assert_eq!(job.priority, 0);
    assert!(!excluded.contains(&job.id));
}

/// An independent statement of the keys every admission's worker accepts:
/// only `MessageParams`' four, and in `ClassifierFlags`, if present, only the
/// attach flags as booleans.
fn key_set_reference(payload: &Value) -> bool {
    const KEYS: [&str; 4] = [
        "ClassifyMode",
        "ClassifierWorkflow",
        "ClassifierFlags",
        "InfoHashes",
    ];
    const FLAGS: [&str; 3] = ["local_search_enabled", "apis_enabled", "tmdb_enabled"];
    let Value::Object(fields) = payload else {
        return false;
    };
    fields.keys().all(|key| KEYS.contains(&key.as_str()))
        && fields.get("ClassifierFlags").is_none_or(|flags| {
            flags.as_object().is_some_and(|flags| {
                flags
                    .iter()
                    .all(|(name, value)| FLAGS.contains(&name.as_str()) && value.is_boolean())
            })
        })
}

/// An independent statement of the payloads organic admission claims, over
/// the payload as PostgreSQL stores it (numbers normalized, duplicate keys
/// dropped), which is also the text both decoders read.
fn organic_reference(payload: &Value) -> bool {
    let is_hash = |hash: &Value| {
        hash.as_str().is_some_and(|hash| {
            hash.len() == 40
                && hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    };
    let Value::Object(fields) = payload else {
        return false;
    };
    key_set_reference(payload)
        && fields
            .get("ClassifyMode")
            .is_none_or(|mode| mode.as_u64().is_some_and(|mode| mode <= 1))
        && fields
            .get("ClassifierWorkflow")
            .is_none_or(|workflow| matches!(workflow.as_str(), Some("" | "default")))
        && fields
            .get("InfoHashes")
            .and_then(Value::as_array)
            .is_some_and(|hashes| (1..=100).contains(&hashes.len()) && hashes.iter().all(is_hash))
}

/// Payloads around the organic shapes, as raw text: the producer shapes, each
/// field missing, null, retyped or renamed, and the mode, workflow, hash and
/// flag edges, some of which only raw text can write (`1e0`, duplicate keys).
fn organic_payload_variants() -> Vec<String> {
    use serde_json::json;
    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
    let hashes = |count: usize| -> Value {
        (0..count)
            .map(|n| Value::from(format!("{n:040x}")))
            .collect()
    };
    let base = json!({
        "ClassifyMode": 1,
        "ClassifierWorkflow": "default",
        "ClassifierFlags": {
            "local_search_enabled": true,
            "apis_enabled": false,
            "tmdb_enabled": true
        },
        "InfoHashes": [HASH],
    });
    let with = |key: &str, value: Option<Value>| {
        let mut payload = base.clone();
        let fields = payload.as_object_mut().expect("base is an object");
        match value {
            Some(value) => fields.insert(key.to_owned(), value),
            None => fields.remove(key),
        };
        payload.to_string()
    };
    let with_flag = |name: &str, value: Option<Value>| {
        let mut flags = base["ClassifierFlags"].clone();
        let fields = flags.as_object_mut().expect("base flags are an object");
        match value {
            Some(value) => fields.insert(name.to_owned(), value),
            None => fields.remove(name),
        };
        with("ClassifierFlags", Some(flags))
    };
    let mut variants = vec![
        // DHT and importer jobs, retries, and batch and CLI children.
        json!({"InfoHashes": [HASH]}).to_string(),
        json!({"ClassifierWorkflow": "default", "InfoHashes": [HASH]}).to_string(),
        json!({
            "ClassifyMode": 1,
            "ClassifierWorkflow": "default",
            "ClassifierFlags": {"tmdb_enabled": false},
            "InfoHashes": hashes(3)
        })
        .to_string(),
        json!({"ClassifierFlags": {"apis_enabled": false}, "InfoHashes": hashes(100)}).to_string(),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_OFF},\"InfoHashes\":[\"{HASH}\"]}}"
        ),
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_ON},\"InfoHashes\":[\"{HASH}\"]}}"
        ),
        base.to_string(),
        // Not an object.
        "null".to_owned(),
        "true".to_owned(),
        "1".to_owned(),
        "\"InfoHashes\"".to_owned(),
        "[]".to_owned(),
        "{}".to_owned(),
        format!("[{{\"InfoHashes\":[\"{HASH}\"]}}]"),
    ];
    // Each field missing, null or retyped, and Go's case-insensitive aliases
    // beside it and instead of it.
    let retyped = [
        Value::Null,
        json!(false),
        json!(true),
        json!(0),
        json!(1),
        json!(""),
        json!("default"),
        json!([]),
        json!([HASH]),
        json!({}),
        json!({"tmdb_enabled": true}),
    ];
    for key in [
        "ClassifyMode",
        "ClassifierWorkflow",
        "ClassifierFlags",
        "InfoHashes",
    ] {
        variants.push(with(key, None));
        variants.extend(retyped.iter().map(|value| with(key, Some(value.clone()))));
        let lower_first = format!("{}{}", key[..1].to_lowercase(), &key[1..]);
        for alias in [lower_first, key.to_uppercase()] {
            let mut beside = base.clone();
            beside[alias.as_str()] = base[key].clone();
            variants.push(beside.to_string());
            let mut instead = base.clone();
            let value = instead
                .as_object_mut()
                .expect("base is an object")
                .remove(key)
                .expect("base field");
            instead[alias.as_str()] = value;
            variants.push(instead.to_string());
        }
    }
    // Keys MessageParams does not have.
    for key in ["Extra", "delete_xxx", "ClassifierFlag", "InfoHash", ""] {
        let mut payload = base.clone();
        payload[key] = json!(true);
        variants.push(payload.to_string());
    }
    // ClassifyMode as written; jsonb normalizes the number (`1e0` is `1`).
    for mode in [
        "0",
        "1",
        "2",
        "-1",
        "-0",
        "0.0",
        "1.0",
        "1e0",
        "1E+0",
        "10e-1",
        "0.5",
        "\"1\"",
        "4294967297",
    ] {
        variants.push(format!(
            "{{\"ClassifyMode\":{mode},\"InfoHashes\":[\"{HASH}\"]}}"
        ));
    }
    for workflow in [
        "",
        "default",
        "Default",
        "DEFAULT",
        " default",
        "default ",
        "default\n",
        "custom",
        "d\u{e9}fault",
    ] {
        variants.push(with("ClassifierWorkflow", Some(json!(workflow))));
    }
    // Duplicate keys: jsonb keeps the last.
    for (first, last) in [("custom", "default"), ("default", "custom")] {
        variants.push(format!(
            "{{\"ClassifierWorkflow\":\"{first}\",\"ClassifierWorkflow\":\"{last}\",\"InfoHashes\":[\"{HASH}\"]}}"
        ));
    }
    for (first, last) in [("1", "false"), ("false", "1")] {
        variants.push(format!(
            "{{\"ClassifierFlags\":{{\"tmdb_enabled\":{first},\"tmdb_enabled\":{last}}},\"InfoHashes\":[\"{HASH}\"]}}"
        ));
    }
    // Hash counts around the loader's bound.
    for count in [0, 1, 2, 99, 100, 101, 250] {
        variants.push(with("InfoHashes", Some(hashes(count))));
    }
    // Hash elements, alone and after a good one.
    let upper = HASH.to_uppercase();
    for element in [
        json!(HASH),
        json!(upper),
        json!(format!("{}{}", &upper[..20], &HASH[20..])),
        json!(&HASH[..39]),
        json!(format!("{HASH}8")),
        json!(format!("0x{HASH}")),
        json!(format!("0x{}", &HASH[2..])),
        json!(format!(" {}", &HASH[1..])),
        json!(format!("{HASH}\n")),
        json!(format!("{}\n", &HASH[..39])),
        json!(format!("g{}", &HASH[1..])),
        json!("\u{e9}".repeat(40)),
        json!(""),
        json!(1),
        Value::Null,
        json!(true),
        json!([HASH]),
        json!({"hash": HASH}),
    ] {
        variants.push(with("InfoHashes", Some(json!([element]))));
        variants.push(with("InfoHashes", Some(json!([HASH, element]))));
    }
    // A unicode-escaped hash, which jsonb stores decoded.
    variants.push(format!("{{\"InfoHashes\":[\"\\u0030{}\"]}}", &HASH[1..]));
    // Each flag missing or of every JSON type, and keys beside them.
    for name in ["local_search_enabled", "apis_enabled", "tmdb_enabled"] {
        variants.push(with_flag(name, None));
        for value in [
            json!(true),
            json!(false),
            Value::Null,
            json!(0),
            json!(1),
            json!("true"),
            json!("false"),
            json!([true]),
            json!({}),
        ] {
            variants.push(with_flag(name, Some(value)));
        }
    }
    for (name, value) in [
        ("delete_xxx", json!(true)),
        ("delete_xxx", json!(false)),
        ("delete_content_types", json!(["movie"])),
        ("delete_content_types", json!([])),
        ("unknown_flag", json!(true)),
        ("TMDB_enabled", json!(true)),
        ("Tmdb_Enabled", json!(false)),
        ("", json!(true)),
    ] {
        variants.push(with_flag(name, Some(value)));
    }
    let mut seen = BTreeSet::new();
    variants.retain(|payload| seen.insert(payload.clone()));
    variants
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn organic_sql_admission_agrees_with_an_independent_reference() {
    let pool = connect().await;
    reset(&pool).await;
    let variants = organic_payload_variants();
    let mut ids = Vec::new();
    for payload in &variants {
        ids.push(enqueue_raw(&pool, payload).await);
    }
    // The predicate over the stored column, beside the text the worker reads.
    let evaluated: BTreeMap<String, (String, Option<bool>)> =
        sqlx::query_as::<_, (String, String, Option<bool>)>(sqlx::AssertSqlSafe(format!(
            "SELECT id, payload::text, {ORGANIC_PAYLOAD_PREDICATE} FROM queue_jobs"
        )))
        .fetch_all(&pool)
        .await
        .expect("evaluate the organic predicate")
        .into_iter()
        .map(|(id, stored, admitted)| (id, (stored, admitted)))
        .collect();
    let mut admitted = BTreeSet::new();
    let mut outside_key_set = 0;
    for (payload, id) in variants.iter().zip(&ids) {
        let (stored, sql) = &evaluated[id];
        let parsed: Value = serde_json::from_str(stored).expect("stored payload is JSON");
        let reference = organic_reference(&parsed);
        assert_eq!(
            *sql,
            Some(reference),
            "payload {payload} (stored as {stored})"
        );
        // The worker's own key check agrees with the reference's, so it never
        // refuses an admitted payload.
        let key_set = key_set_reference(&parsed);
        assert_eq!(
            check_payload_key_set(stored).is_ok(),
            key_set,
            "payload {payload} (stored as {stored})"
        );
        if !key_set {
            outside_key_set += 1;
        }
        if reference {
            admitted.insert(id.clone());
            // Whatever the claim admits, the worker decodes and accepts.
            let params: ProcessTorrentParams = serde_json::from_str(stored)
                .unwrap_or_else(|error| panic!("{stored} does not decode: {error}"));
            validate_supported_params(
                &params,
                ProcessTorrentAdmission::Organic { max_priority: 0 },
            )
            .unwrap_or_else(|error| panic!("{stored}: {error}"));
            assert!((1..=100).contains(&params.info_hashes.len()), "{stored}");
        }
    }
    let _ = writeln!(
        std::io::stderr(),
        "worker_pg: organic reference agreement over {} payloads, {} admitted, {} outside the key set",
        variants.len(),
        admitted.len(),
        outside_key_set
    );
    assert!(
        admitted.len() >= 20 && variants.len() - admitted.len() >= 100,
        "{} of {} admitted",
        admitted.len(),
        variants.len()
    );
    assert!(
        outside_key_set >= 20,
        "{outside_key_set} outside the key set"
    );
    // The claim takes exactly those rows.
    let claimed: BTreeSet<String> =
        probe_claims(&pool, ProcessTorrentAdmission::Organic { max_priority: 0 })
            .await
            .into_iter()
            .collect();
    assert_eq!(claimed, admitted);
}

/// Answers for [`seed_tmdb_fixture`]'s torrent TMDB finds.
fn found_answers() -> Vec<(&'static str, Answer)> {
    vec![
        ("query=Found", answer(200, None, FOUND_SEARCH)),
        ("/movie/777", answer(200, None, FOUND_DETAILS)),
    ]
}

/// Whether `image` holds a `torrent_contents` row for `info_hash` whose
/// `content_id` is `content_id` (a JSON value, e.g. `"42"` or `null`).
fn attached(image: &[String], info_hash: &str, content_id: &str) -> bool {
    image.iter().any(|row| {
        row.starts_with("tc ")
            && row.contains(info_hash)
            && row.contains(&format!("\"content_id\": {content_id}"))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn an_organic_dht_job_runs_with_every_attach_flag_on() {
    let pool = connect().await;
    let hashes = [HASH_LOCAL, HASH_UNKNOWN, HASH_FOUND];
    let mut runs = Vec::new();
    for organic in [false, true] {
        seed_tmdb_fixture(&pool).await;
        let tmdb = FakeTmdb::new(found_answers());
        let (id, worker) = if organic {
            let payload = format!(
                "{{\"InfoHashes\":[\"{HASH_LOCAL}\",\"{HASH_UNKNOWN}\",\"{HASH_FOUND}\"]}}"
            );
            (
                enqueue_raw(&pool, &payload).await,
                tmdb_worker_with(&pool, &tmdb, 1, organic_config(0)),
            )
        } else {
            (
                enqueue(&QueueStore::new(pool.clone()), &flags_on_params(&hashes)).await,
                tmdb_worker(&pool, &tmdb, 1),
            )
        };
        let outcome = worker.run_once().await.unwrap();
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "organic={organic}: {outcome:?}"
        );
        assert_eq!(queue_job_count(&pool).await, 1, "organic={organic}");
        let mut asked = tmdb.asked();
        asked.sort();
        runs.push((live_image(&pool).await, asked));
    }
    // The core defaults turned local search and TMDB on.
    let (image, asked) = &runs[1];
    assert!(attached(image, HASH_LOCAL, "\"42\""), "{image:#?}");
    assert!(attached(image, HASH_FOUND, "\"777\""), "{image:#?}");
    assert!(
        asked
            .iter()
            .any(|request| request.contains("query=Unknown")),
        "{asked:?}"
    );
    assert_eq!(
        runs[1], runs[0],
        "an organic DHT job is not the explicit flags-on job"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn explicit_organic_flags_override_the_core_defaults() {
    let pool = connect().await;

    // TMDB off; local search left to its default, on.
    seed_tmdb_fixture(&pool).await;
    let tmdb = FakeTmdb::new(found_answers());
    let id = enqueue_raw(
        &pool,
        &format!(
            "{{\"ClassifierFlags\":{{\"tmdb_enabled\":false}},\"InfoHashes\":[\"{HASH_LOCAL}\",\"{HASH_FOUND}\"]}}"
        ),
    )
    .await;
    let worker = tmdb_worker_with(&pool, &tmdb, 1, organic_config(0));
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
    ));
    assert_eq!(
        tmdb.asked(),
        Vec::<String>::new(),
        "tmdb_enabled=false reached TMDB"
    );
    let image = live_image(&pool).await;
    assert!(attached(&image, HASH_LOCAL, "\"42\""), "{image:#?}");
    assert!(attached(&image, HASH_FOUND, "null"), "{image:#?}");

    // Local search off; TMDB left to its default, on: the local movie is
    // searched for on TMDB, which does not know it.
    seed_tmdb_fixture(&pool).await;
    let tmdb = FakeTmdb::new(Vec::new());
    let id = enqueue_raw(
        &pool,
        &format!(
            "{{\"ClassifierFlags\":{{\"local_search_enabled\":false}},\"InfoHashes\":[\"{HASH_LOCAL}\"]}}"
        ),
    )
    .await;
    let worker = tmdb_worker_with(&pool, &tmdb, 1, organic_config(0));
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
    ));
    assert!(tmdb.count("query=Reusable") >= 1, "{:?}", tmdb.asked());
    let image = live_image(&pool).await;
    assert!(attached(&image, HASH_LOCAL, "null"), "{image:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn organic_retry_jobs_carry_the_resolved_workflow_and_the_mode_and_flags() {
    let pool = connect().await;
    let failing = format!("[\"{HASH_UNKNOWN}\",\"{HASH_LOCAL}\"]");
    let dht_retry = serde_json::json!({
        "ClassifierWorkflow": "default",
        "InfoHashes": [HASH_UNKNOWN]
    });
    for (payload, priority, retry) in [
        // A DHT job: the workflow resolved, the flags still omitted.
        (
            format!("{{\"InfoHashes\":{failing}}}"),
            0,
            dht_retry.clone(),
        ),
        // An explicit empty workflow, and flags Go's `omitempty` drops.
        (
            format!(
                "{{\"ClassifierWorkflow\":\"\",\"ClassifierFlags\":{{}},\"InfoHashes\":{failing}}}"
            ),
            0,
            dht_retry,
        ),
        // A rematch batch child: its mode and flags kept.
        (
            format!(
                "{{\"ClassifyMode\":1,\"ClassifierFlags\":{{\"apis_enabled\":true}},\"InfoHashes\":{failing}}}"
            ),
            10,
            serde_json::json!({
                "ClassifyMode": 1,
                "ClassifierWorkflow": "default",
                "ClassifierFlags": {"apis_enabled": true},
                "InfoHashes": [HASH_UNKNOWN]
            }),
        ),
    ] {
        seed_tmdb_fixture(&pool).await;
        let tmdb = FakeTmdb::new(vec![("query=Unknown", answer(500, None, "{}"))]);
        let id = enqueue_raw_at(&pool, &payload, priority).await;
        let worker = tmdb_worker_with(&pool, &tmdb, 1, organic_config(10));
        let outcome = worker.run_once().await.unwrap();
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "{payload}: {outcome:?}"
        );
        let (stored, status, retries, max_retries) = other_job(&pool, &id).await;
        assert_eq!(
            serde_json::from_str::<Value>(&stored).unwrap(),
            retry,
            "{payload}"
        );
        assert_eq!(
            (status.as_str(), retries, max_retries),
            ("pending", 0, 2),
            "{payload}"
        );
        // Go publishes it at the default priority, so it is an organic job
        // within the tightest bound.
        let (retry_id, retry_priority): (String, i32) =
            sqlx::query_as("SELECT id, priority FROM queue_jobs WHERE id <> $1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .expect("the retry job");
        assert_eq!(retry_priority, 0, "{payload}");
        make_ready(&pool, &retry_id).await;
        assert_eq!(
            probe_claims(&pool, ProcessTorrentAdmission::Organic { max_priority: 0 }).await,
            vec![retry_id],
            "{payload}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn an_abstained_organic_job_is_excluded_and_counted_by_its_reason() {
    let pool = connect().await;
    seed_tmdb_fixture(&pool).await;
    // A video source Go's scan rejects; Go fails the load, Rust leaves it to Go.
    sqlx::query(
        "INSERT INTO torrent_hints \
         (info_hash, content_type, video_source, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', 'blu-ray', NOW(), NOW())",
    )
    .bind(HASH_UNKNOWN)
    .execute(&pool)
    .await
    .expect("seed undecodable hint");
    // Claim order: the hinted DHT job first, then one behind it.
    let hinted = enqueue_raw(
        &pool,
        &format!("{{\"InfoHashes\":[\"{HASH_LOCAL}\",\"{HASH_UNKNOWN}\"]}}"),
    )
    .await;
    let behind = enqueue_raw(&pool, &format!("{{\"InfoHashes\":[\"{HASH_FOUND}\"]}}")).await;
    let before = job_row(&pool, &hinted).await;
    let tmdb = FakeTmdb::new(found_answers());
    let registry = prometheus::Registry::new();
    let metrics = bitmagnet_processor::register_worker_metrics(&registry).unwrap();
    let worker = tmdb_worker_with(
        &pool,
        &tmdb,
        1,
        ProcessTorrentWorkerConfig {
            abstain_cooloff: Duration::from_secs(3_600),
            ..organic_config(0)
        },
    )
    .with_metrics(metrics.clone());

    // Poll 1 abstains, poll 2 takes the job behind it, poll 3 is idle.
    let outcome = worker.run_once().await.unwrap();
    let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
        panic!("expected an abstention, got {outcome:?}");
    };
    assert_eq!(job.id, hinted);
    assert_eq!(
        reason,
        WriterPlanError::AttachHintUnsupported {
            info_hash: HASH_UNKNOWN.to_owned(),
            detail: "video_source",
        }
        .to_string()
    );
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == behind
    ));
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    assert_eq!(metrics.with_label_values(&["abstained", "hint"]).get(), 1);
    assert_eq!(
        metrics.with_label_values(&["abstained", "payload"]).get(),
        0
    );
    assert_eq!(metrics.with_label_values(&["processed", ""]).get(), 1);
    // Only the job behind it reached TMDB.
    assert!(
        tmdb.asked().iter().all(
            |request| !request.contains("query=Unknown") && !request.contains("query=Reusable")
        ),
        "{:?}",
        tmdb.asked()
    );
    assert_eq!(tmdb.count("query=Found"), 1, "{:?}", tmdb.asked());
    // The abstained row is untouched, and Go claims it.
    assert_eq!(job_row(&pool, &hinted).await, before);
    let go = QueueStore::new(pool.clone());
    assert!(matches!(
        go.consume_one(PROCESS_TORRENT, |_| async { Ok::<_, String>(()) })
            .await
            .unwrap(),
        ConsumeOutcome::Processed { job } if job.id == hinted
    ));
}

#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn a_type_only_hint_with_empty_languages_and_null_episodes_is_bare() {
    let pool = connect().await;
    // No hint, or a type-only hint with `languages` (SQL NULL when `None`).
    let run = |hint: Option<Option<&'static str>>| {
        let pool = pool.clone();
        async move {
            reset(&pool).await;
            seed_fixture_torrents(&pool).await;
            if let Some(languages) = hint {
                sqlx::query(
                    "INSERT INTO torrent_hints \
                     (info_hash, content_type, languages, episodes, created_at, updated_at) \
                     VALUES (decode($1, 'hex'), 'movie', $2::jsonb, NULL, NOW(), NOW())",
                )
                .bind(HASH_B)
                .bind(languages)
                .execute(&pool)
                .await
                .expect("seed type-only hint");
            }
            let id = enqueue(&QueueStore::new(pool.clone()), &supported_params(&[HASH_B])).await;
            let outcome = worker(&pool, fast_config()).run_once().await.unwrap();
            (id, outcome, live_image(&pool).await)
        }
    };
    let (_, _, unhinted) = run(None).await;
    // Every prod hint stores `languages = '[]'` and SQL NULL episodes.
    let mut images = Vec::new();
    for languages in [None, Some("[]")] {
        let (id, outcome, image) = run(Some(languages)).await;
        assert!(
            matches!(
                &outcome,
                AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
            ),
            "languages {languages:?}: {outcome:?}"
        );
        images.push(image);
    }
    assert_eq!(images[1], images[0]);
    assert_ne!(images[0], unhinted, "the type-only hint was not used");
    // A language Go writes is applied, as Go applies it.
    let (id, outcome, image) = run(Some(Some("[\"en\"]"))).await;
    assert!(
        matches!(
            &outcome,
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == id
        ),
        "{outcome:?}"
    );
    assert!(
        image
            .iter()
            .any(|row| row.starts_with("tc ") && row.contains("\"languages\": [\"en\"]")),
        "{image:#?}"
    );
    // One Go cannot scan is left to Go, which fails the load.
    let (id, outcome, _) = run(Some(Some("[\"xx\"]"))).await;
    assert!(
        matches!(
            &outcome,
            AdmittedConsumeOutcome::Abstained { job, reason }
                if job.id == id
                    && *reason == WriterPlanError::AttachHintUnsupported {
                        info_hash: HASH_B.to_owned(),
                        detail: "languages",
                    }
                    .to_string()
        ),
        "{outcome:?}"
    );
}

// --- The payload key set (Milestone 11) -------------------------------------

/// The explicit claims match `ClassifierFlags` by containment, so they take a
/// flag beside the three attach flags, which Go would read. The worker leaves
/// the job to Go untouched and takes the one behind it.
#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn an_extra_flag_under_an_explicit_admission_is_left_to_go() {
    let pool = connect().await;
    for (live, flags, name, value) in [
        (false, FLAGS_OFF, "delete_xxx", false),
        (true, FLAGS_ON, "unknown_flag", true),
    ] {
        reset(&pool).await;
        seed_fixture_torrents(&pool).await;
        let refused = enqueue_raw(
            &pool,
            &format!(
                "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{},\"InfoHashes\":[\"{HASH_A}\"]}}",
                flags.replace('}', &format!(",\"{name}\":{value}}}"))
            ),
        )
        .await;
        let behind = enqueue(&QueueStore::new(pool.clone()), &supported_params(&[HASH_B])).await;
        let before = job_row(&pool, &refused).await;
        let registry = prometheus::Registry::new();
        let metrics = bitmagnet_processor::register_worker_metrics(&registry).unwrap();
        let worker = if live {
            flags_on_worker(&pool)
        } else {
            worker(&pool, fast_config())
        }
        .with_metrics(metrics.clone());
        assert_eq!(
            worker.admission(),
            if live {
                ProcessTorrentAdmission::FlagsOffOrOn
            } else {
                ProcessTorrentAdmission::FlagsOff
            }
        );

        let outcome = worker.run_once().await.unwrap();
        let AdmittedConsumeOutcome::Abstained { job, reason } = outcome else {
            panic!("live={live}: expected an abstention, got {outcome:?}");
        };
        assert_eq!(job.id, refused);
        assert_eq!(
            reason,
            format!("payload: classifier flag \"{name}\" is not an attach flag")
        );
        assert!(matches!(
            worker.run_once().await.unwrap(),
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Processed { job }) if job.id == behind
        ));
        assert!(matches!(
            worker.run_once().await.unwrap(),
            AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
        ));
        assert_eq!(
            metrics.with_label_values(&["abstained", "payload"]).get(),
            1
        );
        assert_eq!(metrics.with_label_values(&["processed", ""]).get(), 1);
        assert_eq!(job_row(&pool, &refused).await, before, "live={live}");
        let go = QueueStore::new(pool.clone());
        assert!(matches!(
            go.consume_one(PROCESS_TORRENT, |_| async { Ok::<_, String>(()) })
                .await
                .unwrap(),
            ConsumeOutcome::Processed { job } if job.id == refused
        ));
    }
}

/// A case-variant or an extra top-level key is outside every claim, so no Rust
/// worker sees it; the worker's own check is its unit test's.
#[tokio::test]
#[ignore = "requires BITMAGNET_PROCESSOR_TEST_DATABASE_URL pointing at disposable migrated PostgreSQL"]
async fn case_variant_and_extra_keys_are_claimed_under_no_admission() {
    let pool = connect().await;
    seed_tmdb_fixture(&pool).await;
    let mut before = BTreeMap::new();
    for payload in [
        // Go reads "default", then "custom", and runs `custom`.
        format!(
            "{{\"ClassifierWorkflow\":\"default\",\"ClassifierFlags\":{FLAGS_OFF},\
             \"InfoHashes\":[\"{HASH_FOUND}\"],\"classifierworkflow\":\"custom\"}}"
        ),
        format!("{{\"InfoHashes\":[\"{HASH_FOUND}\"],\"Extra\":true}}"),
        format!(
            "{{\"InfoHashes\":[\"{HASH_FOUND}\"],\"classifierflags\":{{\"tmdb_enabled\":false}}}}"
        ),
    ] {
        let id = enqueue_raw(&pool, &payload).await;
        before.insert(id.clone(), job_row(&pool, &id).await);
    }
    for admission in [
        ProcessTorrentAdmission::FlagsOff,
        ProcessTorrentAdmission::FlagsOffOrOn,
        ProcessTorrentAdmission::Organic { max_priority: 0 },
    ] {
        assert_eq!(probe_claims(&pool, admission).await, Vec::<String>::new());
    }
    let tmdb = FakeTmdb::new(found_answers());
    let worker = tmdb_worker_with(&pool, &tmdb, 1, organic_config(0));
    assert!(matches!(
        worker.run_once().await.unwrap(),
        AdmittedConsumeOutcome::Settled(ConsumeOutcome::Empty)
    ));
    assert!(tmdb.asked().is_empty(), "{:?}", tmdb.asked());
    for (id, row) in &before {
        assert_eq!(&job_row(&pool, id).await, row, "job {id} was touched");
    }
}
