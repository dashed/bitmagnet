//! Disposable-PostgreSQL gate for the persisting `process_torrent` worker.
//!
//! Every test truncates queue, torrent, and content tables. Each requires the
//! exact disposable database name `bitmagnet_processor_writer_load_test` and
//! explicit invocation with `--ignored --test-threads=1`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bitmagnet_blocking::BlockingManager as PersistentBlockingManager;
use bitmagnet_model::InfoHash;
use bitmagnet_processor::{
    load_writer_plan, persist_writer_plan, Materializer, ProcessTorrentWorker,
    ProcessTorrentWorkerConfig,
};
use bitmagnet_queue::{
    backoff, process_torrent_job, AdmittedConsumeOutcome, ConsumeOutcome, JobVerdict,
    ProcessTorrentParams, ProtocolId, QueueJobOptions, QueueStore, PROCESS_TORRENT,
};
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
