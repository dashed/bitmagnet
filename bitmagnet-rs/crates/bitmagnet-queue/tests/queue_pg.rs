//! Migration-backed PostgreSQL gate for the Lane Q runtime.
//!
//! This test truncates queue, torrent, and content tables. It requires the exact
//! disposable Goose-34 database name `bitmagnet_processor_writer_load_test`
//! and explicit invocation with `--ignored --test-threads=1`.

use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bitmagnet_queue::pg::MirrorBootstrap;
use bitmagnet_queue::{
    fingerprint, new_queue_job, ActiveJobInsertReceipt, ConsumeOutcome, Consumer, ConsumerConfig,
    MirrorConfig, MirrorIneligibleReason, PreparedQueueJob, ProtocolId, QueueJobOptions,
    QueueJobStatus, QueuePgError, QueueStore, ShadowJobEnvelopeV1, PROCESS_TORRENT,
    PROCESS_TORRENT_BATCH, PROCESS_TORRENT_SHADOW, SHADOW_JOB_ENVELOPE_VERSION,
};
use chrono::Utc;
use prometheus::Encoder as _;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use tokio::sync::{oneshot, Mutex};

const TEST_DATABASE_NAME: &str = "bitmagnet_processor_writer_load_test";

async fn reset(pool: &PgPool) {
    sqlx::query(
        "ALTER TABLE queue_mirror_cursors \
         DROP CONSTRAINT IF EXISTS queue_mirror_test_no_advance",
    )
    .execute(pool)
    .await
    .expect("drop test-only cursor constraint");
    sqlx::query("TRUNCATE queue_jobs, queue_mirror_cursors, torrents, content CASCADE")
        .execute(pool)
        .await
        .expect("truncate queue runtime and source tables");
}

#[allow(clippy::too_many_arguments)]
async fn seed(
    pool: &PgPool,
    id: &str,
    queue: &str,
    status: &str,
    priority: i32,
    run_after_offset: i32,
    retries: i32,
    max_retries: i32,
    payload: &str,
) {
    sqlx::query(
        "INSERT INTO queue_jobs \
         (id, fingerprint, queue, status, payload, retries, max_retries, run_after, \
          archival_duration, created_at, priority) \
         VALUES ($1, $2, $3, $4::queue_job_status, $5::jsonb, $6, $7, \
                 clock_timestamp() + make_interval(secs => $8), \
                 make_interval(secs => 604800), \
                 clock_timestamp(), $9)",
    )
    .bind(id)
    .bind(format!("fp-{id}"))
    .bind(queue)
    .bind(status)
    .bind(payload)
    .bind(retries)
    .bind(max_retries)
    .bind(run_after_offset)
    .bind(priority)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed {id}: {error}"));
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

async fn seed_torrent(pool: &PgPool, info_hash: &str) {
    sqlx::query(
        "INSERT INTO torrents \
         (info_hash, name, size, private, created_at, updated_at) \
         VALUES (decode($1, 'hex'), $1, 42, false, \
                 clock_timestamp() - interval '1 day', \
                 clock_timestamp() - interval '1 day')",
    )
    .bind(info_hash)
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed torrent {info_hash}: {error}"));
}

fn assert_insufficient_privilege(error: sqlx::Error) {
    assert_database_code(error, "42501");
}

fn assert_database_code(error: sqlx::Error, expected: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL database error, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some(expected));
}

fn assert_queue_database_code(error: QueuePgError, expected: &str) {
    let QueuePgError::Database(error) = error else {
        panic!("expected a PostgreSQL queue error, got {error}");
    };
    assert_database_code(error, expected);
}

#[tokio::test]
#[ignore = "requires BITMAGNET_QUEUE_TEST_DATABASE_URL pointing at disposable goose-34 PostgreSQL"]
async fn queue_runtime_matches_go_contract() {
    let database_url = std::env::var("BITMAGNET_QUEUE_TEST_DATABASE_URL")
        .expect("BITMAGNET_QUEUE_TEST_DATABASE_URL must be set for ignored gate");
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect disposable PostgreSQL");
    let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("read disposable database sentinel");
    assert_eq!(
        database_name, TEST_DATABASE_NAME,
        "ignored queue gate refuses a database without its exact disposable-name sentinel"
    );
    let store = QueueStore::new(pool.clone());

    // Terminal-row cleanup uses Go's strict status, cutoff, and null
    // semantics across the shared queue table.
    reset(&pool).await;
    let gc_cutoff = chrono::DateTime::parse_from_rfc3339("2026-08-12T07:15:00Z")
        .expect("fixed cleanup cutoff")
        .with_timezone(&Utc);
    sqlx::query(
        "INSERT INTO queue_jobs \
         (id, fingerprint, queue, status, payload, run_after, ran_at, \
          archival_duration, created_at, priority) VALUES \
         ('gc-processed-expired', 'gc-fp-1', 'gc-a', 'processed', '{}', $1, $1 - interval '2 hours', interval '1 hour', $1, 0), \
         ('gc-failed-expired', 'gc-fp-2', 'gc-b', 'failed', '{}', $1, $1 - interval '61 minutes', interval '1 hour', $1, 0), \
         ('gc-processed-boundary', 'gc-fp-3', 'gc-a', 'processed', '{}', $1, $1 - interval '1 hour', interval '1 hour', $1, 0), \
         ('gc-failed-future', 'gc-fp-4', 'gc-b', 'failed', '{}', $1, $1 - interval '59 minutes', interval '1 hour', $1, 0), \
         ('gc-pending-expired', 'gc-fp-5', 'gc-a', 'pending', '{}', $1, $1 - interval '2 hours', interval '1 hour', $1, 0), \
         ('gc-retry-expired', 'gc-fp-6', 'gc-b', 'retry', '{}', $1, $1 - interval '2 hours', interval '1 hour', $1, 0), \
         ('gc-processed-null', 'gc-fp-7', 'gc-a', 'processed', '{}', $1, NULL, interval '1 hour', $1, 0)",
    )
    .bind(gc_cutoff)
    .execute(&pool)
    .await
    .expect("seed terminal cleanup contract");
    assert_eq!(
        store
            .delete_expired_terminal_jobs(gc_cutoff)
            .await
            .expect("delete expired terminal jobs"),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT array_agg(id ORDER BY id) FROM queue_jobs WHERE queue LIKE 'gc-%'",
        )
        .fetch_one(&pool)
        .await
        .expect("read retained cleanup rows"),
        vec![
            "gc-failed-future".to_owned(),
            "gc-pending-expired".to_owned(),
            "gc-processed-boundary".to_owned(),
            "gc-processed-null".to_owned(),
            "gc-retry-expired".to_owned(),
        ]
    );
    assert_eq!(
        store
            .delete_expired_terminal_jobs(gc_cutoff)
            .await
            .expect("repeat terminal cleanup is empty"),
        0
    );

    // Batch-worker metric snapshots include only nonempty status groups for
    // their fixed queue. Other queues are invisible and zero-valued batch
    // statuses are not synthesized.
    reset(&pool).await;
    for (id, queue, status) in [
        ("metric-batch-pending-1", PROCESS_TORRENT_BATCH, "pending"),
        ("metric-batch-pending-2", PROCESS_TORRENT_BATCH, "pending"),
        ("metric-batch-processed", PROCESS_TORRENT_BATCH, "processed"),
        ("metric-batch-retry-1", PROCESS_TORRENT_BATCH, "retry"),
        ("metric-batch-retry-2", PROCESS_TORRENT_BATCH, "retry"),
        ("metric-batch-retry-3", PROCESS_TORRENT_BATCH, "retry"),
        ("metric-batch-failed", PROCESS_TORRENT_BATCH, "failed"),
        ("metric-other-retry", PROCESS_TORRENT, "retry"),
        ("metric-other-failed", "other-queue", "failed"),
    ] {
        seed(&pool, id, queue, status, 0, -1, 0, 2, "{}").await;
    }
    let mut status_counts = store
        .process_torrent_batch_status_counts()
        .await
        .expect("read queue status counts");
    status_counts.sort_by(|left, right| {
        (left.queue.as_str(), left.status.as_str())
            .cmp(&(right.queue.as_str(), right.status.as_str()))
    });
    assert_eq!(
        status_counts
            .iter()
            .map(|item| (item.queue.as_str(), item.status.as_str(), item.count))
            .collect::<Vec<_>>(),
        vec![
            (PROCESS_TORRENT_BATCH, "failed", 1),
            (PROCESS_TORRENT_BATCH, "pending", 2),
            (PROCESS_TORRENT_BATCH, "processed", 1),
            (PROCESS_TORRENT_BATCH, "retry", 3),
        ],
        "other queues and empty batch statuses must not appear"
    );
    let families = store
        .status_metric_families()
        .await
        .expect("build queue metric family");
    let mut metrics_text = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&families, &mut metrics_text)
        .expect("encode queue metric family");
    let metrics_text = String::from_utf8(metrics_text).expect("metric text is UTF-8");
    assert!(metrics_text.contains(
        "# HELP bitmagnet_queue_jobs_total Number of tasks enqueued; broken down by queue and status."
    ));
    assert!(metrics_text.contains("# TYPE bitmagnet_queue_jobs_total gauge"));
    assert!(metrics_text.contains(
        "bitmagnet_queue_jobs_total{queue=\"process_torrent_batch\",status=\"pending\"} 2"
    ));
    assert!(!metrics_text.contains("other-queue"));
    reset(&pool).await;
    assert!(store
        .status_metric_families()
        .await
        .expect("empty metric snapshot")
        .is_empty());

    // Strict producer insertion is a single all-or-nothing statement. It
    // preserves the constructor fingerprint across JSONB normalization and
    // preserves Go's per-job run_after plus one shared application created_at.
    reset(&pool).await;
    store
        .insert_jobs_strict(&[])
        .await
        .expect("empty producer insert");
    let materialized_at = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
        .expect("current timestamp fits PostgreSQL precision")
        - chrono::TimeDelta::seconds(2);
    let immediate = new_queue_job(
        "producer_contract",
        &serde_json::json!({"kind": "immediate", "nested": {"value": 1}}),
        QueueJobOptions::default()
            .with_max_retries(2)
            .with_priority(-7),
    )
    .expect("construct immediate producer job");
    let delayed = new_queue_job(
        "producer_contract",
        &serde_json::json!({"kind": "delayed"}),
        QueueJobOptions::default()
            .with_max_retries(3)
            .with_priority(9)
            .with_delay(Duration::from_micros(2_500_001)),
    )
    .expect("construct delayed producer job");
    let immediate_prepared = PreparedQueueJob::materialize_at(immediate.clone(), materialized_at)
        .expect("prepare immediate producer job");
    let later_page_at = materialized_at + chrono::TimeDelta::seconds(1);
    let delayed_prepared = PreparedQueueJob::materialize_at(delayed.clone(), later_page_at)
        .expect("prepare delayed producer job");
    store
        .insert_jobs_strict(&[immediate_prepared, delayed_prepared])
        .await
        .expect("insert strict producer batch");
    let immediate_row = sqlx::query(
        "SELECT id <> '' AS has_id, fingerprint, payload::text AS payload, \
                status::text AS status, retries, max_retries, priority, \
                ran_at IS NULL AS ran_at_null, error IS NULL AS error_null, \
                deadline IS NULL AS deadline_null, \
                archival_duration = interval '7 days' AS archive_seven_days, \
                run_after <= created_at AS ready_immediately \
         FROM queue_jobs WHERE fingerprint = $1",
    )
    .bind(&immediate.fingerprint)
    .fetch_one(&pool)
    .await
    .expect("read immediate producer row");
    assert!(immediate_row.try_get::<bool, _>("has_id").expect("has id"));
    assert_eq!(
        immediate_row
            .try_get::<String, _>("fingerprint")
            .expect("stored fingerprint"),
        immediate.fingerprint
    );
    let normalized_payload = immediate_row
        .try_get::<String, _>("payload")
        .expect("normalized payload");
    assert_ne!(normalized_payload, immediate.payload);
    assert_ne!(
        fingerprint(&immediate.queue, &normalized_payload),
        immediate.fingerprint,
        "producer must not fingerprint PostgreSQL-normalized JSONB text"
    );
    assert_eq!(
        immediate_row
            .try_get::<String, _>("status")
            .expect("status"),
        "pending"
    );
    assert_eq!(immediate_row.try_get::<i32, _>("retries").unwrap(), 0);
    assert_eq!(immediate_row.try_get::<i32, _>("max_retries").unwrap(), 2);
    assert_eq!(immediate_row.try_get::<i32, _>("priority").unwrap(), -7);
    for field in [
        "ran_at_null",
        "error_null",
        "deadline_null",
        "archive_seven_days",
        "ready_immediately",
    ] {
        assert!(immediate_row.try_get::<bool, _>(field).unwrap(), "{field}");
    }
    let timing = sqlx::query(
        "SELECT count(DISTINCT created_at)::bigint AS created_at_count, \
                min(run_after) AS minimum_run_after, max(run_after) AS maximum_run_after \
         FROM queue_jobs WHERE queue = 'producer_contract'",
    )
    .fetch_one(&pool)
    .await
    .expect("read producer timestamps");
    assert_eq!(
        timing.try_get::<i64, _>("created_at_count").unwrap(),
        1,
        "one insert statement must share one created_at base"
    );
    assert_eq!(
        timing
            .try_get::<chrono::DateTime<Utc>, _>("minimum_run_after")
            .unwrap(),
        materialized_at
    );
    assert_eq!(
        timing
            .try_get::<chrono::DateTime<Utc>, _>("maximum_run_after")
            .unwrap(),
        later_page_at + chrono::TimeDelta::microseconds(2_500_001),
        "later page materialization and per-job delay must both be preserved"
    );

    // Every application-side validation runs before the insert statement.
    for invalid in {
        let mut malformed = immediate.clone();
        malformed.payload = "{".to_owned();
        let mut wrong_status = immediate.clone();
        wrong_status.status = QueueJobStatus::Processed;
        let mut wrong_archive = immediate.clone();
        wrong_archive.archival_duration = Duration::from_secs(1);
        let mut wrong_fingerprint = immediate.clone();
        wrong_fingerprint.fingerprint = "0".repeat(64);
        let mut max_retries_overflow = immediate.clone();
        max_retries_overflow.max_retries = u32::MAX;
        [
            malformed,
            wrong_status,
            wrong_archive,
            wrong_fingerprint,
            max_retries_overflow,
        ]
    } {
        let before: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .expect("count before invalid producer job");
        let invalid = PreparedQueueJob::materialize_at(invalid, materialized_at)
            .expect("non-delay invalid jobs still materialize");
        assert!(store.insert_jobs_strict(&[invalid]).await.is_err());
        let after: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .expect("count after invalid producer job");
        assert_eq!(after, before, "invalid input must insert no row");
    }
    let mut delay_overflow = immediate.clone();
    delay_overflow.delay = Duration::MAX;
    assert!(PreparedQueueJob::materialize_at(delay_overflow, materialized_at).is_err());

    // The active-fingerprint unique index is strict. A duplicate anywhere in
    // the statement returns 23505 and rolls back every otherwise-valid row.
    reset(&pool).await;
    let duplicate = new_queue_job(
        "producer_contract",
        &serde_json::json!({"duplicate": true}),
        QueueJobOptions::default(),
    )
    .expect("construct duplicate job");
    assert_queue_database_code(
        store
            .insert_jobs_strict(&[
                PreparedQueueJob::materialize_at(duplicate.clone(), materialized_at).unwrap(),
                PreparedQueueJob::materialize_at(duplicate.clone(), materialized_at).unwrap(),
            ])
            .await
            .expect_err("duplicate request must fail"),
        "23505",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    store
        .insert_jobs_strict(&[
            PreparedQueueJob::materialize_at(duplicate.clone(), materialized_at).unwrap(),
        ])
        .await
        .expect("insert active fingerprint");
    let unique = new_queue_job(
        "producer_contract",
        &serde_json::json!({"unique": true}),
        QueueJobOptions::default(),
    )
    .expect("construct unique job");
    assert_queue_database_code(
        store
            .insert_jobs_strict(&[
                PreparedQueueJob::materialize_at(unique.clone(), materialized_at).unwrap(),
                PreparedQueueJob::materialize_at(duplicate.clone(), materialized_at).unwrap(),
            ])
            .await
            .expect_err("active collision must fail whole statement"),
        "23505",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM queue_jobs WHERE fingerprint = $1",
        )
        .bind(&unique.fingerprint)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "the non-conflicting row must roll back with its batch"
    );
    sqlx::query("UPDATE queue_jobs SET status = 'retry' WHERE fingerprint = $1")
        .bind(&duplicate.fingerprint)
        .execute(&pool)
        .await
        .expect("make active row retry");
    assert_queue_database_code(
        store
            .insert_jobs_strict(&[PreparedQueueJob::materialize_at(
                duplicate.clone(),
                materialized_at,
            )
            .unwrap()])
            .await
            .expect_err("retry collision must fail"),
        "23505",
    );
    sqlx::query("UPDATE queue_jobs SET status = 'processed' WHERE fingerprint = $1")
        .bind(&duplicate.fingerprint)
        .execute(&pool)
        .await
        .expect("settle active fingerprint");
    store
        .insert_jobs_strict(&[
            PreparedQueueJob::materialize_at(duplicate.clone(), materialized_at).unwrap(),
        ])
        .await
        .expect("reuse fingerprint after processed");
    sqlx::query(
        "UPDATE queue_jobs SET status = 'failed' \
         WHERE fingerprint = $1 AND status = 'pending'",
    )
    .bind(&duplicate.fingerprint)
    .execute(&pool)
    .await
    .expect("fail second fingerprint row");
    store
        .insert_jobs_strict(&[
            PreparedQueueJob::materialize_at(duplicate.clone(), materialized_at).unwrap(),
        ])
        .await
        .expect("reuse fingerprint after failed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM queue_jobs WHERE fingerprint = $1",
        )
        .bind(&duplicate.fingerprint)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );

    // The processor retry producer accepts the production schema's active-
    // fingerprint collision and otherwise exposes a durable one-row insertion.
    // The typed receipt distinguishes those two successful outcomes without a
    // racy follow-up SELECT.
    reset(&pool).await;
    let idempotent = new_queue_job(
        PROCESS_TORRENT,
        &serde_json::json!({"InfoHashes": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}),
        QueueJobOptions::default().with_max_retries(2),
    )
    .expect("construct idempotent producer job");
    let inserted_id = match store
        .insert_job_idempotent(&idempotent)
        .await
        .expect("insert first idempotent job")
    {
        ActiveJobInsertReceipt::Inserted { id } => id,
        ActiveJobInsertReceipt::ExistingActiveFingerprint => {
            panic!("first idempotent insert unexpectedly conflicted")
        }
    };
    assert!(!inserted_id.is_empty());
    let idempotent_row = sqlx::query(
        "SELECT id, fingerprint, queue, payload::text AS payload, \
                status::text AS status, retries, max_retries, priority, \
                run_after <= created_at AS ready_immediately, \
                archival_duration = make_interval(secs => 604800) AS archive_exact \
         FROM queue_jobs WHERE fingerprint = $1",
    )
    .bind(&idempotent.fingerprint)
    .fetch_one(&pool)
    .await
    .expect("read idempotent producer row");
    assert_eq!(
        idempotent_row.try_get::<String, _>("id").unwrap(),
        inserted_id
    );
    assert_eq!(
        idempotent_row.try_get::<String, _>("fingerprint").unwrap(),
        idempotent.fingerprint
    );
    assert_eq!(
        idempotent_row.try_get::<String, _>("queue").unwrap(),
        PROCESS_TORRENT
    );
    assert_eq!(
        idempotent_row.try_get::<String, _>("status").unwrap(),
        "pending"
    );
    assert_eq!(idempotent_row.try_get::<i32, _>("retries").unwrap(), 0);
    assert_eq!(idempotent_row.try_get::<i32, _>("max_retries").unwrap(), 2);
    assert_eq!(idempotent_row.try_get::<i32, _>("priority").unwrap(), 0);
    assert!(idempotent_row
        .try_get::<bool, _>("ready_immediately")
        .unwrap());
    assert!(idempotent_row.try_get::<bool, _>("archive_exact").unwrap());
    let normalized_idempotent_payload = idempotent_row
        .try_get::<String, _>("payload")
        .expect("normalized idempotent payload");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&normalized_idempotent_payload).unwrap(),
        serde_json::from_str::<serde_json::Value>(&idempotent.payload).unwrap()
    );
    assert_ne!(
        fingerprint(PROCESS_TORRENT, &normalized_idempotent_payload),
        idempotent.fingerprint,
        "the stored fingerprint must retain the raw constructor bytes"
    );
    assert_eq!(
        store.insert_job_idempotent(&idempotent).await.unwrap(),
        ActiveJobInsertReceipt::ExistingActiveFingerprint
    );
    sqlx::query("UPDATE queue_jobs SET status = 'retry' WHERE id = $1")
        .bind(&inserted_id)
        .execute(&pool)
        .await
        .expect("make idempotent row retry");
    assert_eq!(
        store.insert_job_idempotent(&idempotent).await.unwrap(),
        ActiveJobInsertReceipt::ExistingActiveFingerprint
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM queue_jobs WHERE fingerprint = $1",
        )
        .bind(&idempotent.fingerprint)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    sqlx::query("UPDATE queue_jobs SET status = 'processed' WHERE id = $1")
        .bind(&inserted_id)
        .execute(&pool)
        .await
        .expect("settle idempotent row");
    assert!(matches!(
        store.insert_job_idempotent(&idempotent).await.unwrap(),
        ActiveJobInsertReceipt::Inserted { .. }
    ));
    sqlx::query(
        "UPDATE queue_jobs SET status = 'failed' \
         WHERE fingerprint = $1 AND status = 'pending'",
    )
    .bind(&idempotent.fingerprint)
    .execute(&pool)
    .await
    .expect("fail second idempotent row");
    assert!(matches!(
        store.insert_job_idempotent(&idempotent).await.unwrap(),
        ActiveJobInsertReceipt::Inserted { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM queue_jobs WHERE fingerprint = $1",
        )
        .bind(&idempotent.fingerprint)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );

    reset(&pool).await;
    let concurrent = new_queue_job(
        PROCESS_TORRENT,
        &serde_json::json!({"InfoHashes": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]}),
        QueueJobOptions::default().with_max_retries(2),
    )
    .expect("construct concurrent idempotent job");
    let (left, right) = tokio::join!(
        store.insert_job_idempotent(&concurrent),
        store.insert_job_idempotent(&concurrent),
    );
    let receipts = [left.unwrap(), right.unwrap()];
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| matches!(receipt, ActiveJobInsertReceipt::Inserted { .. }))
            .count(),
        1
    );
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| {
                matches!(receipt, ActiveJobInsertReceipt::ExistingActiveFingerprint)
            })
            .count(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM queue_jobs WHERE fingerprint = $1",
        )
        .bind(&concurrent.fingerprint)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );

    // Go's batch handler inserts children through its base DAO while the queue
    // server retains the parent row transaction. Freeze that independent
    // commit boundary: rolling back a simulated parent transaction does not
    // roll back an already committed child, and retrying collides strictly.
    reset(&pool).await;
    let parent = pool
        .begin()
        .await
        .expect("begin simulated parent transaction");
    let child = new_queue_job(
        "producer_contract",
        &serde_json::json!({"child": "commits-first"}),
        QueueJobOptions::default(),
    )
    .expect("construct independently committed child");
    let child_prepared = PreparedQueueJob::materialize_at(child.clone(), materialized_at).unwrap();
    store
        .insert_jobs_strict(&[child_prepared])
        .await
        .expect("commit child outside parent transaction");
    parent
        .rollback()
        .await
        .expect("roll back simulated parent settlement");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM queue_jobs WHERE fingerprint = $1",
        )
        .bind(&child.fingerprint)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_queue_database_code(
        store
            .insert_jobs_strict(
                &[PreparedQueueJob::materialize_at(child, materialized_at).unwrap()],
            )
            .await
            .expect_err("parent retry must encounter committed child"),
        "23505",
    );

    // Callable batch orchestration timestamps each child at its page boundary,
    // timestamps the continuation last, then inserts the complete plan once.
    reset(&pool).await;
    for value in 1_u8..=5 {
        seed_torrent(&pool, &format!("{value:040x}")).await;
    }
    let batch_payload = serde_json::json!({
        "InfoHashGreaterThan": "0000000000000000000000000000000000000000",
        "UpdatedBefore": "2100-01-01T00:00:00Z",
        "ChunkSize": 3,
        "BatchSize": 2
    })
    .to_string();
    let child_one_at = materialized_at;
    let child_two_at = materialized_at + chrono::TimeDelta::seconds(1);
    let continuation_at = materialized_at + chrono::TimeDelta::seconds(2);
    let mut clock = VecDeque::from([child_one_at, child_two_at, continuation_at]);
    let batch_report = store
        .handle_process_torrent_batch_payload_with_clock(&batch_payload, || {
            clock.pop_front().expect("one clock tick per planned job")
        })
        .await
        .expect("handle batch payload");
    assert!(clock.is_empty(), "handler must consume exactly three ticks");
    assert_eq!(batch_report.selected, 4);
    assert_eq!(batch_report.child_jobs, 2);
    assert!(batch_report.continuation_inserted);
    assert!(!batch_report.done);
    assert_eq!(
        batch_report.max_info_hash,
        ProtocolId::from_hex("0000000000000000000000000000000000000004").unwrap()
    );
    let planned = sqlx::query(
        "SELECT queue, payload::text AS payload, fingerprint, priority, run_after \
         FROM queue_jobs ORDER BY run_after",
    )
    .fetch_all(&pool)
    .await
    .expect("read planned batch jobs");
    assert_eq!(planned.len(), 3);
    let planned_times = planned
        .iter()
        .map(|row| {
            row.try_get::<chrono::DateTime<Utc>, _>("run_after")
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(planned_times, [child_one_at, child_two_at, continuation_at]);
    let planned_queues = planned
        .iter()
        .map(|row| row.try_get::<String, _>("queue").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        planned_queues,
        [PROCESS_TORRENT, PROCESS_TORRENT, PROCESS_TORRENT_BATCH]
    );
    let planned_priorities = planned
        .iter()
        .map(|row| row.try_get::<i32, _>("priority").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(planned_priorities, [10, 10, 0]);
    let planned_payloads = planned
        .iter()
        .map(|row| {
            serde_json::from_str::<serde_json::Value>(&row.try_get::<String, _>("payload").unwrap())
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        planned_payloads[0]["InfoHashes"],
        serde_json::json!([
            "0000000000000000000000000000000000000001",
            "0000000000000000000000000000000000000002"
        ])
    );
    assert_eq!(
        planned_payloads[1]["InfoHashes"],
        serde_json::json!([
            "0000000000000000000000000000000000000003",
            "0000000000000000000000000000000000000004"
        ])
    );
    assert_eq!(
        planned_payloads[2]["InfoHashGreaterThan"],
        "0000000000000000000000000000000000000004"
    );
    for (row, payload) in planned.iter().zip(&planned_payloads) {
        let queue = row.try_get::<String, _>("queue").unwrap();
        let stored_fingerprint = row.try_get::<String, _>("fingerprint").unwrap();
        let logical_job = if queue == PROCESS_TORRENT_BATCH {
            bitmagnet_queue::process_torrent_batch_job(
                &serde_json::from_value(payload.clone()).unwrap(),
                QueueJobOptions::default(),
            )
            .unwrap()
        } else {
            bitmagnet_queue::process_torrent_job(
                &serde_json::from_value(payload.clone()).unwrap(),
                QueueJobOptions::default().with_priority(10),
            )
            .unwrap()
        };
        assert_eq!(stored_fingerprint, logical_job.fingerprint);
    }
    let count_before_retry: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM queue_jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut retry_clock = VecDeque::from([child_one_at, child_two_at, continuation_at]);
    let retry_error = store
        .handle_process_torrent_batch_payload_with_clock(&batch_payload, || {
            retry_clock.pop_front().expect("retry clock tick")
        })
        .await
        .expect_err("active child fingerprints must reject parent retry");
    let bitmagnet_queue::BatchHandleError::Queue(retry_error) = retry_error else {
        panic!("expected strict producer failure");
    };
    assert_queue_database_code(retry_error, "23505");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        count_before_retry,
        "retry conflict must insert no siblings"
    );

    reset(&pool).await;
    let mut no_match_clock_calls = 0_u8;
    let no_match = store
        .handle_process_torrent_batch_payload_with_clock(&batch_payload, || {
            no_match_clock_calls += 1;
            materialized_at
        })
        .await
        .expect("empty batch payload");
    assert_eq!(no_match.selected, 0);
    assert_eq!(no_match.child_jobs, 0);
    assert!(!no_match.continuation_inserted);
    assert!(no_match.done);
    assert_eq!(no_match_clock_calls, 0, "empty pages create no jobs");
    for invalid in [
        "{".to_owned(),
        serde_json::json!({
            "InfoHashGreaterThan": "0000000000000000000000000000000000000000",
            "UpdatedBefore": "2100-01-01T00:00:00Z",
            "ChunkSize": 3,
            "BatchSize": 0
        })
        .to_string(),
        serde_json::json!({
            "InfoHashGreaterThan": "0000000000000000000000000000000000000000",
            "UpdatedBefore": "2100-01-01T00:00:00Z",
            "ChunkSize": 0,
            "BatchSize": 2
        })
        .to_string(),
    ] {
        assert!(store
            .handle_process_torrent_batch_payload_with_clock(&invalid, || materialized_at)
            .await
            .is_err());
    }

    // A shutdown observed before a claim must not start a fresh job.
    reset(&pool).await;
    seed(
        &pool,
        "idle-shutdown-parent",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -1,
        0,
        2,
        "{}",
    )
    .await;
    let idle_handler_called = Arc::new(AtomicBool::new(false));
    let idle_handler_called_for_run = Arc::clone(&idle_handler_called);
    let mut idle_config = ConsumerConfig::new(PROCESS_TORRENT_BATCH);
    idle_config.check_interval = Duration::from_secs(60);
    let idle_consumer = Consumer::new(store.clone(), idle_config);
    idle_consumer
        .run_until(
            move |_| {
                idle_handler_called_for_run.store(true, Ordering::SeqCst);
                std::future::ready(Ok::<(), QueuePgError>(()))
            },
            std::future::ready(()),
        )
        .await
        .expect("pre-signaled shutdown succeeds");
    assert!(!idle_handler_called.load(Ordering::SeqCst));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM queue_jobs WHERE id = 'idle-shutdown-parent'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "pending"
    );

    let lifecycle_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("connect two-connection lifecycle pool");
    let lifecycle_store = QueueStore::new(lifecycle_pool);

    // Graceful shutdown stops after the current job: it must not cancel an
    // in-flight handler or roll back its retained parent transaction.
    reset(&pool).await;
    seed(
        &pool,
        "drain-parent",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -1,
        0,
        2,
        "{}",
    )
    .await;
    seed(
        &pool,
        "drain-parent-next",
        PROCESS_TORRENT_BATCH,
        "pending",
        1,
        -1,
        0,
        2,
        "{}",
    )
    .await;
    let drain_child = new_queue_job(
        "producer_contract",
        &serde_json::json!({"child": "graceful-drain"}),
        QueueJobOptions::default(),
    )
    .expect("construct graceful-drain child");
    let drain_child = PreparedQueueJob::materialize_at(drain_child, materialized_at).unwrap();
    let (drain_started_tx, drain_started_rx) = oneshot::channel();
    let drain_started_tx = Arc::new(Mutex::new(Some(drain_started_tx)));
    let (drain_release_tx, drain_release_rx) = oneshot::channel();
    let drain_release_rx = Arc::new(Mutex::new(Some(drain_release_rx)));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut drain_config = ConsumerConfig::new(PROCESS_TORRENT_BATCH);
    drain_config.check_interval = Duration::from_secs(60);
    drain_config.job_timeout = Duration::from_secs(5);
    let drain_consumer = Consumer::new(lifecycle_store.clone(), drain_config);
    let drain_store = lifecycle_store.clone();
    let drain_task = tokio::spawn(async move {
        drain_consumer
            .run_until(
                move |_| {
                    let drain_store = drain_store.clone();
                    let drain_child = drain_child.clone();
                    let drain_started_tx = Arc::clone(&drain_started_tx);
                    let drain_release_rx = Arc::clone(&drain_release_rx);
                    async move {
                        drain_store.insert_jobs_strict(&[drain_child]).await?;
                        drain_started_tx
                            .lock()
                            .await
                            .take()
                            .expect("one drain notification")
                            .send(())
                            .expect("notify drain handler start");
                        drain_release_rx
                            .lock()
                            .await
                            .take()
                            .expect("one drain release")
                            .await
                            .expect("release drain handler");
                        Ok::<(), QueuePgError>(())
                    }
                },
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
    });
    drain_started_rx.await.expect("drain handler started");
    shutdown_tx.send(()).expect("signal graceful shutdown");
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !drain_task.is_finished(),
        "shutdown must await the in-flight handler"
    );
    drain_release_tx.send(()).expect("release drained handler");
    drain_task
        .await
        .expect("join drained consumer")
        .expect("drained consumer succeeds");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM queue_jobs WHERE id = 'drain-parent'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "processed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM queue_jobs WHERE id = 'drain-parent-next'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "pending"
    );

    // A handler timeout after an independently committed child settles the
    // parent to retry. Reprocessing then surfaces the strict 23505 collision
    // and reaches the parent's terminal failure at max_retries=1.
    reset(&pool).await;
    seed(
        &pool,
        "timeout-parent",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -1,
        0,
        1,
        "{}",
    )
    .await;
    let timeout_child = new_queue_job(
        "producer_contract",
        &serde_json::json!({"child": "timeout-survivor"}),
        QueueJobOptions::default(),
    )
    .expect("construct timeout child");
    let timeout_child = PreparedQueueJob::materialize_at(timeout_child, materialized_at).unwrap();
    let (timeout_inserted_tx, timeout_inserted_rx) = oneshot::channel();
    let timeout_inserted_tx = Arc::new(Mutex::new(Some(timeout_inserted_tx)));
    let (timeout_shutdown_tx, timeout_shutdown_rx) = oneshot::channel();
    let mut timeout_config = ConsumerConfig::new(PROCESS_TORRENT_BATCH);
    timeout_config.check_interval = Duration::from_secs(60);
    timeout_config.job_timeout = Duration::from_millis(25);
    let timeout_consumer = Consumer::new(lifecycle_store.clone(), timeout_config);
    let timeout_store = lifecycle_store.clone();
    let timeout_task = tokio::spawn(async move {
        timeout_consumer
            .run_until(
                move |_| {
                    let timeout_store = timeout_store.clone();
                    let timeout_child = timeout_child.clone();
                    let timeout_inserted_tx = Arc::clone(&timeout_inserted_tx);
                    async move {
                        timeout_store.insert_jobs_strict(&[timeout_child]).await?;
                        timeout_inserted_tx
                            .lock()
                            .await
                            .take()
                            .expect("one timeout notification")
                            .send(())
                            .expect("notify timeout child insert");
                        std::future::pending::<Result<(), QueuePgError>>().await
                    }
                },
                async {
                    let _ = timeout_shutdown_rx.await;
                },
            )
            .await
    });
    timeout_inserted_rx.await.expect("timeout child committed");
    timeout_shutdown_tx
        .send(())
        .expect("stop after timed-out parent settles");
    timeout_task
        .await
        .expect("join timeout consumer")
        .expect("timeout settlement succeeds");
    let timeout_parent = sqlx::query(
        "SELECT status::text AS status, retries, error \
         FROM queue_jobs WHERE id = 'timeout-parent'",
    )
    .fetch_one(&pool)
    .await
    .expect("read timed-out parent");
    assert_eq!(
        timeout_parent.try_get::<String, _>("status").unwrap(),
        "retry"
    );
    assert_eq!(timeout_parent.try_get::<i32, _>("retries").unwrap(), 0);
    assert!(timeout_parent
        .try_get::<String, _>("error")
        .unwrap()
        .contains("job exceeded its 25ms timeout"));
    make_ready(&pool, "timeout-parent").await;
    let retry_child = new_queue_job(
        "producer_contract",
        &serde_json::json!({"child": "timeout-survivor"}),
        QueueJobOptions::default(),
    )
    .expect("reconstruct timeout child");
    let retry_child = PreparedQueueJob::materialize_at(retry_child, materialized_at).unwrap();
    let retry_store = lifecycle_store.clone();
    let (collision_code_tx, collision_code_rx) = oneshot::channel();
    let retry_outcome = lifecycle_store
        .consume_one(PROCESS_TORRENT_BATCH, move |_| {
            let retry_store = retry_store.clone();
            async move {
                let result = retry_store.insert_jobs_strict(&[retry_child]).await;
                let code = match &result {
                    Err(QueuePgError::Database(sqlx::Error::Database(error))) => {
                        error.code().map(|value| value.into_owned())
                    }
                    _ => None,
                };
                collision_code_tx
                    .send(code)
                    .expect("report retry collision SQLSTATE");
                result
            }
        })
        .await
        .expect("settle strict retry collision");
    assert_eq!(
        collision_code_rx.await.expect("receive collision SQLSTATE"),
        Some("23505".to_owned())
    );
    let ConsumeOutcome::Failed { job, error } = retry_outcome else {
        panic!("retry collision must fail terminally");
    };
    assert_eq!(job.id, "timeout-parent");
    assert_eq!(job.retries, 1);
    assert!(error.contains("duplicate key value violates unique constraint"));
    let failed_parent = sqlx::query(
        "SELECT status::text AS status, retries, ran_at IS NOT NULL AS has_ran_at, error \
         FROM queue_jobs WHERE id = 'timeout-parent'",
    )
    .fetch_one(&pool)
    .await
    .expect("read terminal retry collision");
    assert_eq!(
        failed_parent.try_get::<String, _>("status").unwrap(),
        "failed"
    );
    assert_eq!(failed_parent.try_get::<i32, _>("retries").unwrap(), 1);
    assert!(failed_parent.try_get::<bool, _>("has_ran_at").unwrap());
    assert_eq!(failed_parent.try_get::<String, _>("error").unwrap(), error);

    // A minimally granted batch-consumer role can claim and settle only the
    // fixed process_torrent_batch queue through migration-30 capabilities.
    // PUBLIC has no implicit EXECUTE, and the caller transaction retains the
    // claimed row lock so a two-connection pool skips to the next batch job.
    reset(&pool).await;
    seed(
        &pool,
        "batch-boundary-live",
        PROCESS_TORRENT,
        "pending",
        -100,
        -100,
        0,
        0,
        "{}",
    )
    .await;
    seed(
        &pool,
        "batch-boundary-a",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -2,
        0,
        0,
        "{}",
    )
    .await;
    seed(
        &pool,
        "batch-boundary-b",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -1,
        0,
        0,
        "{}",
    )
    .await;
    sqlx::query(
        "DO $$ BEGIN \
         IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'bitmagnet_queue_batch_test') THEN \
           EXECUTE 'DROP OWNED BY bitmagnet_queue_batch_test'; \
           EXECUTE 'DROP ROLE bitmagnet_queue_batch_test'; \
         END IF; END $$",
    )
    .execute(&pool)
    .await
    .expect("remove prior queue batch role");
    sqlx::query(
        "CREATE ROLE bitmagnet_queue_batch_test LOGIN PASSWORD 'queue-batch-test-password' \
         NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
    )
    .execute(&pool)
    .await
    .expect("create queue batch role");
    sqlx::query("GRANT USAGE ON SCHEMA public TO bitmagnet_queue_batch_test")
        .execute(&pool)
        .await
        .expect("grant public schema usage to batch role");

    let capability_catalog = sqlx::query(
        "SELECT p.oid::regprocedure::text AS signature, \
                p.prosecdef, p.proconfig, \
                pg_catalog.pg_get_userbyid(p.proowner) AS owner, \
                NOT EXISTS (\
                  SELECT 1 \
                  FROM pg_catalog.aclexplode(\
                    COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))\
                  ) AS acl \
                  WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE'\
                ) AS public_execute_revoked \
         FROM pg_catalog.pg_proc AS p \
         WHERE p.oid = ANY(ARRAY[\
           'public.process_torrent_batch_claim_job()'::regprocedure, \
           'public.process_torrent_batch_settle_processed(text,bigint)'::regprocedure, \
           'public.process_torrent_batch_settle_retry(text,bigint,text,bigint)'::regprocedure, \
           'public.process_torrent_batch_settle_failed(text,bigint,text)'::regprocedure\
         ]) \
         ORDER BY signature",
    )
    .fetch_all(&pool)
    .await
    .expect("inspect batch capability catalog contract");
    assert_eq!(capability_catalog.len(), 4);
    for row in capability_catalog {
        let signature = row.try_get::<String, _>("signature").unwrap();
        assert!(
            row.try_get::<bool, _>("prosecdef").unwrap(),
            "{signature} must be SECURITY DEFINER"
        );
        assert_eq!(
            row.try_get::<Option<Vec<String>>, _>("proconfig").unwrap(),
            Some(vec!["search_path=pg_catalog, pg_temp".to_owned()]),
            "{signature} must pin its search_path"
        );
        assert!(
            row.try_get::<bool, _>("public_execute_revoked").unwrap(),
            "{signature} must revoke PUBLIC EXECUTE"
        );
        assert_ne!(
            row.try_get::<String, _>("owner").unwrap(),
            "bitmagnet_queue_batch_test",
            "runtime ownership transfer is not part of migration 30"
        );
    }

    let batch_options = PgConnectOptions::from_str(&database_url)
        .expect("parse PostgreSQL URL")
        .username("bitmagnet_queue_batch_test")
        .password("queue-batch-test-password");
    let batch_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(batch_options)
        .await
        .expect("connect minimally granted queue batch role");
    let public_claim = sqlx::query("SELECT * FROM public.process_torrent_batch_claim_job()")
        .execute(&batch_pool)
        .await
        .expect_err("PUBLIC must not inherit batch claim execution");
    assert_insufficient_privilege(public_claim);
    let enqueue_catalog = sqlx::query(
        "SELECT p.prosecdef, p.proconfig, \
                pg_catalog.pg_get_userbyid(p.proowner) AS owner, \
                NOT EXISTS (\
                  SELECT 1 \
                  FROM pg_catalog.aclexplode(\
                    COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))\
                  ) AS acl \
                  WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE'\
                ) AS public_execute_revoked \
         FROM pg_catalog.pg_proc AS p \
         WHERE p.oid = 'public.process_torrent_batch_enqueue_plan(\
           text[],timestamp with time zone[],integer[],text,\
           timestamp with time zone,timestamp with time zone\
         )'::regprocedure",
    )
    .fetch_one(&pool)
    .await
    .expect("inspect batch enqueue capability catalog contract");
    assert!(enqueue_catalog.try_get::<bool, _>("prosecdef").unwrap());
    assert_eq!(
        enqueue_catalog
            .try_get::<Option<Vec<String>>, _>("proconfig")
            .unwrap(),
        Some(vec!["search_path=pg_catalog, pg_temp".to_owned()])
    );
    assert!(enqueue_catalog
        .try_get::<bool, _>("public_execute_revoked")
        .unwrap());
    assert_ne!(
        enqueue_catalog.try_get::<String, _>("owner").unwrap(),
        "bitmagnet_queue_batch_test",
        "runtime ownership transfer is not part of migration 32"
    );
    let public_enqueue = sqlx::query(
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[]::text[], ARRAY[]::timestamptz[], ARRAY[]::integer[], \
           NULL::text, NULL::timestamptz, clock_timestamp()\
         )",
    )
    .execute(&batch_pool)
    .await
    .expect_err("PUBLIC must not inherit batch enqueue execution");
    assert_insufficient_privilege(public_enqueue);
    let status_catalog = sqlx::query(
        "SELECT p.prosecdef, p.proconfig, \
                pg_catalog.pg_get_userbyid(p.proowner) AS owner, \
                NOT EXISTS (\
                  SELECT 1 \
                  FROM pg_catalog.aclexplode(\
                    COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))\
                  ) AS acl \
                  WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE'\
                ) AS public_execute_revoked \
         FROM pg_catalog.pg_proc AS p \
         WHERE p.oid = 'public.process_torrent_batch_status_counts()'::regprocedure",
    )
    .fetch_one(&pool)
    .await
    .expect("inspect batch status capability catalog contract");
    assert!(status_catalog.try_get::<bool, _>("prosecdef").unwrap());
    assert_eq!(
        status_catalog
            .try_get::<Option<Vec<String>>, _>("proconfig")
            .unwrap(),
        Some(vec!["search_path=pg_catalog, pg_temp".to_owned()])
    );
    assert!(status_catalog
        .try_get::<bool, _>("public_execute_revoked")
        .unwrap());
    assert_ne!(
        status_catalog.try_get::<String, _>("owner").unwrap(),
        "bitmagnet_queue_batch_test",
        "runtime ownership transfer is not part of migration 33"
    );
    let public_status = sqlx::query("SELECT * FROM public.process_torrent_batch_status_counts()")
        .execute(&batch_pool)
        .await
        .expect_err("PUBLIC must not inherit batch status execution");
    assert_insufficient_privilege(public_status);

    sqlx::query(
        "GRANT EXECUTE ON FUNCTION \
           public.process_torrent_batch_claim_job(), \
           public.process_torrent_batch_settle_processed(text, bigint), \
           public.process_torrent_batch_settle_retry(text, bigint, text, bigint), \
           public.process_torrent_batch_settle_failed(text, bigint, text), \
           public.process_torrent_batch_enqueue_plan(\
             text[], timestamptz[], integer[], text, timestamptz, timestamptz\
           ), \
           public.process_torrent_batch_status_counts() \
         TO bitmagnet_queue_batch_test",
    )
    .execute(&pool)
    .await
    .expect("grant reviewed batch queue capabilities");
    let cross_queue_settle = sqlx::query(
        "SELECT public.process_torrent_batch_settle_failed(\
           $1::text, $2::bigint, $3::text\
         )",
    )
    .bind("batch-boundary-live")
    .bind(0_i64)
    .bind("forbidden cross-queue settlement")
    .execute(&batch_pool)
    .await
    .expect_err("batch settle capability must reject another queue");
    assert_database_code(cross_queue_settle, "P0002");
    let direct_settle =
        sqlx::query("UPDATE queue_jobs SET status = 'failed' WHERE id = 'batch-boundary-a'")
            .execute(&batch_pool)
            .await
            .expect_err("batch role must not directly settle queue rows");
    assert_insufficient_privilege(direct_settle);
    let direct_insert = sqlx::query(
        "INSERT INTO queue_jobs \
         (fingerprint, queue, payload, run_after, archival_duration, created_at) \
         VALUES ('forbidden-batch-insert', 'process_torrent', '{}'::jsonb, \
                 clock_timestamp(), make_interval(secs => 604800), clock_timestamp())",
    )
    .execute(&batch_pool)
    .await
    .expect_err("batch role must not directly insert queue rows");
    assert_insufficient_privilege(direct_insert);
    let direct_status_select = sqlx::query("SELECT status FROM queue_jobs LIMIT 1")
        .execute(&batch_pool)
        .await
        .expect_err("batch role must not directly read queue status rows");
    assert_insufficient_privilege(direct_status_select);

    reset(&pool).await;
    for (id, queue, status) in [
        ("bounded-status-pending-a", PROCESS_TORRENT_BATCH, "pending"),
        ("bounded-status-pending-b", PROCESS_TORRENT_BATCH, "pending"),
        (
            "bounded-status-processed",
            PROCESS_TORRENT_BATCH,
            "processed",
        ),
        ("bounded-status-retry-a", PROCESS_TORRENT_BATCH, "retry"),
        ("bounded-status-retry-b", PROCESS_TORRENT_BATCH, "retry"),
        ("bounded-status-failed", PROCESS_TORRENT_BATCH, "failed"),
        ("hidden-live-processed", PROCESS_TORRENT, "processed"),
        ("hidden-other-retry", "unrelated-queue", "retry"),
    ] {
        seed(&pool, id, queue, status, 0, -1, 0, 2, "{}").await;
    }
    let bounded_status_store = QueueStore::new(batch_pool.clone());
    let mut bounded_counts = bounded_status_store
        .process_torrent_batch_status_counts()
        .await
        .expect("read status through fixed batch capability");
    bounded_counts.sort_by(|left, right| left.status.as_str().cmp(right.status.as_str()));
    assert_eq!(
        bounded_counts
            .iter()
            .map(|item| (item.queue.as_str(), item.status.as_str(), item.count))
            .collect::<Vec<_>>(),
        vec![
            (PROCESS_TORRENT_BATCH, "failed", 1),
            (PROCESS_TORRENT_BATCH, "pending", 2),
            (PROCESS_TORRENT_BATCH, "processed", 1),
            (PROCESS_TORRENT_BATCH, "retry", 2),
        ],
        "minimal role must see only nonempty fixed-label batch groups"
    );

    // The enqueue capability preserves raw-text fingerprints across JSONB
    // normalization, hardcodes the two allowed queues and fixed row fields,
    // and shares one exact application created_at across all planned rows.
    reset(&pool).await;
    let raw_child_a = r#"{"z":1, "a":"child-a"}"#;
    let raw_child_b = r#"{"a":"child-b","escaped":"a\\tb"}"#;
    let raw_continuation = r#"{"BatchSize":2, "ChunkSize":3}"#;
    let enqueue_created_at = chrono::DateTime::parse_from_rfc3339("2026-08-12T08:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let enqueue_run_a = enqueue_created_at + chrono::TimeDelta::seconds(1);
    let enqueue_run_b = enqueue_created_at + chrono::TimeDelta::seconds(2);
    let enqueue_run_continuation = enqueue_created_at + chrono::TimeDelta::seconds(3);
    let inserted: i64 = sqlx::query_scalar(
        "SELECT public.process_torrent_batch_enqueue_plan(\
           $1::text[], $2::timestamptz[], $3::integer[], \
           $4::text, $5::timestamptz, $6::timestamptz\
         )",
    )
    .bind(vec![raw_child_a, raw_child_b])
    .bind(vec![enqueue_run_a, enqueue_run_b])
    .bind(vec![4_i32, 10_i32])
    .bind(raw_continuation)
    .bind(enqueue_run_continuation)
    .bind(enqueue_created_at)
    .fetch_one(&batch_pool)
    .await
    .expect("enqueue fixed batch plan capability");
    assert_eq!(inserted, 3);
    let enqueued = sqlx::query(
        "SELECT fingerprint, queue, payload::text AS payload, \
                status::text AS status, retries, max_retries, priority, \
                run_after, created_at, ran_at IS NULL AS ran_at_null, \
                error IS NULL AS error_null, deadline IS NULL AS deadline_null, \
                archival_duration = interval '7 days' AS archive_seven_days, \
                EXTRACT(day FROM archival_duration)::bigint AS archive_days, \
                EXTRACT(epoch FROM archival_duration)::bigint AS archive_seconds \
         FROM queue_jobs ORDER BY run_after",
    )
    .fetch_all(&pool)
    .await
    .expect("read capability-enqueued batch plan");
    assert_eq!(enqueued.len(), 3);
    for (index, (raw, queue, priority, run_after)) in [
        (raw_child_a, PROCESS_TORRENT, 4, enqueue_run_a),
        (raw_child_b, PROCESS_TORRENT, 10, enqueue_run_b),
        (
            raw_continuation,
            PROCESS_TORRENT_BATCH,
            0,
            enqueue_run_continuation,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let row = &enqueued[index];
        assert_eq!(row.try_get::<String, _>("queue").unwrap(), queue);
        assert_eq!(
            row.try_get::<String, _>("fingerprint").unwrap(),
            fingerprint(queue, raw),
            "fingerprint must use the raw payload text"
        );
        assert_ne!(
            row.try_get::<String, _>("payload").unwrap(),
            raw,
            "fixture must observe PostgreSQL JSONB normalization"
        );
        assert_eq!(row.try_get::<String, _>("status").unwrap(), "pending");
        assert_eq!(row.try_get::<i32, _>("retries").unwrap(), 0);
        assert_eq!(row.try_get::<i32, _>("max_retries").unwrap(), 2);
        assert_eq!(row.try_get::<i32, _>("priority").unwrap(), priority);
        assert_eq!(
            row.try_get::<chrono::DateTime<Utc>, _>("run_after")
                .unwrap(),
            run_after
        );
        assert_eq!(
            row.try_get::<chrono::DateTime<Utc>, _>("created_at")
                .unwrap(),
            enqueue_created_at
        );
        for field in [
            "ran_at_null",
            "error_null",
            "deadline_null",
            "archive_seven_days",
        ] {
            assert!(row.try_get::<bool, _>(field).unwrap(), "{field}");
        }
        assert_eq!(row.try_get::<i64, _>("archive_days").unwrap(), 0);
        assert_eq!(
            row.try_get::<i64, _>("archive_seconds").unwrap(),
            7 * 24 * 60 * 60
        );
    }

    // Empty plan is an explicit successful no-op. Malformed arrays, payloads,
    // priorities, continuation pairs, and timestamps fail closed before the
    // single INSERT can affect any row.
    reset(&pool).await;
    let empty_inserted: i64 = sqlx::query_scalar(
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[]::text[], ARRAY[]::timestamptz[], ARRAY[]::integer[], \
           NULL::text, NULL::timestamptz, clock_timestamp()\
         )",
    )
    .fetch_one(&batch_pool)
    .await
    .expect("empty batch plan is a no-op");
    assert_eq!(empty_inserted, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    for invalid in [
        "SELECT public.process_torrent_batch_enqueue_plan(\
           NULL::text[], ARRAY[]::timestamptz[], ARRAY[]::integer[], \
           NULL, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[['{}']]::text[], ARRAY[clock_timestamp()]::timestamptz[], \
           ARRAY[4]::integer[], NULL, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY['{}']::text[], ARRAY[]::timestamptz[], ARRAY[4]::integer[], \
           NULL, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[NULL]::text[], ARRAY[clock_timestamp()]::timestamptz[], \
           ARRAY[4]::integer[], NULL, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY['[]']::text[], ARRAY[clock_timestamp()]::timestamptz[], \
           ARRAY[4]::integer[], NULL, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY['{']::text[], ARRAY[clock_timestamp()]::timestamptz[], \
           ARRAY[4]::integer[], NULL, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY['{}']::text[], ARRAY[clock_timestamp()]::timestamptz[], \
           ARRAY[0]::integer[], NULL, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[]::text[], ARRAY[]::timestamptz[], ARRAY[]::integer[], \
           '{}'::text, NULL, clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[]::text[], ARRAY[]::timestamptz[], ARRAY[]::integer[], \
           '[]'::text, clock_timestamp(), clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[]::text[], ARRAY[]::timestamptz[], ARRAY[]::integer[], \
           '{}'::text, clock_timestamp(), clock_timestamp())",
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[]::text[], ARRAY[]::timestamptz[], ARRAY[]::integer[], \
           NULL, NULL, NULL)",
    ] {
        let error = sqlx::query(invalid)
            .execute(&batch_pool)
            .await
            .expect_err("invalid batch plan must fail closed");
        assert_database_code(error, "22023");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM queue_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );

    // One native 23505 aborts every sibling in the capability's single INSERT.
    let collision_payload = r#"{"raw": "collision"}"#;
    sqlx::query(
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[$1]::text[], ARRAY[$2]::timestamptz[], ARRAY[4]::integer[], \
           NULL, NULL, $2::timestamptz\
         )",
    )
    .bind(collision_payload)
    .bind(enqueue_created_at)
    .execute(&batch_pool)
    .await
    .expect("seed active capability fingerprint");
    let unique_payload = r#"{"raw":"unique-sibling"}"#;
    let collision = sqlx::query(
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[$1, $2]::text[], ARRAY[$3, $3]::timestamptz[], \
           ARRAY[10, 4]::integer[], NULL, NULL, $3::timestamptz\
         )",
    )
    .bind(unique_payload)
    .bind(collision_payload)
    .bind(enqueue_created_at)
    .execute(&batch_pool)
    .await
    .expect_err("active fingerprint collision must abort every sibling");
    assert_database_code(collision, "23505");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)::bigint FROM queue_jobs WHERE fingerprint = $1",
        )
        .bind(fingerprint(PROCESS_TORRENT, unique_payload))
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    sqlx::query("UPDATE queue_jobs SET status = 'retry'")
        .execute(&pool)
        .await
        .expect("make capability collision row retry");
    let retry_collision = sqlx::query(
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[$1]::text[], ARRAY[$2]::timestamptz[], ARRAY[4]::integer[], \
           NULL, NULL, $2::timestamptz\
         )",
    )
    .bind(collision_payload)
    .bind(enqueue_created_at)
    .execute(&batch_pool)
    .await
    .expect_err("retry fingerprint remains active");
    assert_database_code(retry_collision, "23505");
    sqlx::query("UPDATE queue_jobs SET status = 'processed'")
        .execute(&pool)
        .await
        .expect("settle capability collision row");
    sqlx::query(
        "SELECT public.process_torrent_batch_enqueue_plan(\
           ARRAY[$1]::text[], ARRAY[$2]::timestamptz[], ARRAY[4]::integer[], \
           NULL, NULL, $2::timestamptz\
         )",
    )
    .bind(collision_payload)
    .bind(enqueue_created_at)
    .execute(&batch_pool)
    .await
    .expect("processed fingerprint may be reused");

    reset(&pool).await;
    seed(
        &pool,
        "batch-boundary-live",
        PROCESS_TORRENT,
        "pending",
        -100,
        -100,
        0,
        0,
        "{}",
    )
    .await;
    seed(
        &pool,
        "batch-boundary-a",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -2,
        0,
        0,
        "{}",
    )
    .await;
    seed(
        &pool,
        "batch-boundary-b",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -1,
        0,
        0,
        "{}",
    )
    .await;

    let batch_store = QueueStore::new(batch_pool.clone());
    let (batch_claimed_tx, batch_claimed_rx) = oneshot::channel();
    let (batch_release_tx, batch_release_rx) = oneshot::channel();
    let batch_claimed_tx = Arc::new(Mutex::new(Some(batch_claimed_tx)));
    let first_batch_store = batch_store.clone();
    let first_batch = tokio::spawn(async move {
        first_batch_store
            .consume_one(PROCESS_TORRENT_BATCH, move |job| {
                let batch_claimed_tx = Arc::clone(&batch_claimed_tx);
                async move {
                    batch_claimed_tx
                        .lock()
                        .await
                        .take()
                        .expect("batch claim notifier")
                        .send(job.id)
                        .expect("send claimed batch id");
                    batch_release_rx.await.expect("release first batch handler");
                    Ok::<(), String>(())
                }
            })
            .await
    });
    assert_eq!(
        batch_claimed_rx.await.expect("first batch claim"),
        "batch-boundary-a"
    );
    let second_batch = batch_store
        .consume_one(PROCESS_TORRENT_BATCH, |_| async { Ok::<(), String>(()) })
        .await
        .expect("second capability-mediated batch consume");
    assert!(matches!(
        second_batch,
        ConsumeOutcome::Processed { ref job } if job.id == "batch-boundary-b"
    ));
    batch_release_tx
        .send(())
        .expect("release first batch handler");
    assert!(matches!(
        first_batch
            .await
            .expect("join first batch")
            .expect("first batch consume"),
        ConsumeOutcome::Processed { ref job } if job.id == "batch-boundary-a"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM queue_jobs WHERE id = 'batch-boundary-live'",
        )
        .fetch_one(&pool)
        .await
        .expect("read isolated live queue row"),
        "pending"
    );
    let repeat_settle = sqlx::query(
        "SELECT public.process_torrent_batch_settle_processed(\
           $1::text, $2::bigint\
         )",
    )
    .bind("batch-boundary-a")
    .bind(0_i64)
    .execute(&batch_pool)
    .await
    .expect_err("terminal batch row must not settle twice");
    assert_database_code(repeat_settle, "P0002");

    reset(&pool).await;
    seed(
        &pool,
        "batch-boundary-retry",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -1,
        0,
        2,
        "{}",
    )
    .await;
    assert!(matches!(
        batch_store
            .consume_one(PROCESS_TORRENT_BATCH, |_| async {
                Err::<(), _>("capability retry")
            })
            .await
            .expect("settle batch retry through capability"),
        ConsumeOutcome::RetryScheduled { ref job, .. }
            if job.id == "batch-boundary-retry"
    ));
    reset(&pool).await;
    seed(
        &pool,
        "batch-boundary-failed",
        PROCESS_TORRENT_BATCH,
        "pending",
        0,
        -1,
        0,
        0,
        "{}",
    )
    .await;
    assert!(matches!(
        batch_store
            .consume_one(PROCESS_TORRENT_BATCH, |_| async {
                Err::<(), _>("capability failed")
            })
            .await
            .expect("settle batch failure through capability"),
        ConsumeOutcome::Failed { ref job, .. }
            if job.id == "batch-boundary-failed"
    ));
    batch_pool.close().await;
    sqlx::query("DROP OWNED BY bitmagnet_queue_batch_test")
        .execute(&pool)
        .await
        .expect("drop queue batch role grants");
    sqlx::query("DROP ROLE bitmagnet_queue_batch_test")
        .execute(&pool)
        .await
        .expect("drop queue batch role");

    // Frozen dequeue order: pending before retry, then priority and run_after.
    reset(&pool).await;
    for (id, queue, status, priority, offset) in [
        ("dead", PROCESS_TORRENT, "failed", -100, -300),
        ("done", PROCESS_TORRENT, "processed", -100, -300),
        ("future", PROCESS_TORRENT, "pending", -100, 3600),
        ("other", "process_torrent_batch", "pending", -100, -300),
        ("p-a", PROCESS_TORRENT, "pending", 0, -60),
        ("p-b", PROCESS_TORRENT, "pending", 0, -30),
        ("p-c", PROCESS_TORRENT, "pending", 5, -120),
        ("p-d", PROCESS_TORRENT, "pending", -10, -10),
        ("r-a", PROCESS_TORRENT, "retry", 0, -90),
        ("r-b", PROCESS_TORRENT, "retry", 0, -45),
        ("r-c", PROCESS_TORRENT, "retry", 3, -200),
    ] {
        seed(&pool, id, queue, status, priority, offset, 0, 3, "{}").await;
    }
    let mut order = Vec::new();
    loop {
        match store
            .consume_one(PROCESS_TORRENT, |_| async { Ok::<(), String>(()) })
            .await
            .expect("consume ordered job")
        {
            ConsumeOutcome::Processed { job } => order.push(job.id),
            ConsumeOutcome::Empty => break,
            other => panic!("unexpected ordering outcome: {other:?}"),
        }
    }
    assert_eq!(order, ["p-d", "p-a", "p-b", "p-c", "r-a", "r-b", "r-c"]);

    // SKIP LOCKED: a second consumer claims the next row while the first handler
    // deliberately holds its transaction and row lock open.
    reset(&pool).await;
    seed(
        &pool,
        "lock-a",
        PROCESS_TORRENT,
        "pending",
        0,
        -2,
        0,
        0,
        "{}",
    )
    .await;
    seed(
        &pool,
        "lock-b",
        PROCESS_TORRENT,
        "pending",
        0,
        -1,
        0,
        0,
        "{}",
    )
    .await;
    let (claimed_tx, claimed_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let claimed_tx = Arc::new(Mutex::new(Some(claimed_tx)));
    let first_store = store.clone();
    let first = tokio::spawn(async move {
        first_store
            .consume_one(PROCESS_TORRENT, move |job| {
                let claimed_tx = Arc::clone(&claimed_tx);
                async move {
                    claimed_tx
                        .lock()
                        .await
                        .take()
                        .expect("claim notifier")
                        .send(job.id)
                        .expect("send claimed id");
                    release_rx.await.expect("release first handler");
                    Ok::<(), String>(())
                }
            })
            .await
    });
    assert_eq!(claimed_rx.await.expect("first claim"), "lock-a");
    let second = store
        .consume_one(PROCESS_TORRENT, |_| async { Ok::<(), String>(()) })
        .await
        .expect("second concurrent consume");
    assert!(matches!(
        second,
        ConsumeOutcome::Processed { ref job } if job.id == "lock-b"
    ));
    release_tx.send(()).expect("release first");
    assert!(matches!(
        first.await.expect("join first").expect("first consume"),
        ConsumeOutcome::Processed { ref job } if job.id == "lock-a"
    ));

    // Retry accounting: pending failure uses retry count zero (exactly 16s),
    // retry claims increment before handling, and max_retries=2 gives 3 attempts.
    reset(&pool).await;
    seed(
        &pool,
        "retry",
        PROCESS_TORRENT,
        "pending",
        0,
        -1,
        0,
        2,
        "{}",
    )
    .await;
    let first_failure = store
        .consume_one(PROCESS_TORRENT, |_| async { Err::<(), _>("first") })
        .await
        .expect("settle first failure");
    assert!(matches!(
        first_failure,
        ConsumeOutcome::RetryScheduled { ref job, delay }
            if job.retries == 0 && delay == Duration::from_secs(16)
    ));
    make_ready(&pool, "retry").await;
    let second_failure = store
        .consume_one(PROCESS_TORRENT, |_| async { Err::<(), _>("second") })
        .await
        .expect("settle second failure");
    assert!(matches!(
        second_failure,
        ConsumeOutcome::RetryScheduled { ref job, delay }
            if job.retries == 1
                && (Duration::from_secs(17)..=Duration::from_secs(46)).contains(&delay)
    ));
    make_ready(&pool, "retry").await;
    assert!(matches!(
        store
            .consume_one(PROCESS_TORRENT, |_| async { Err::<(), _>("third") })
            .await
            .expect("settle terminal failure"),
        ConsumeOutcome::Failed { ref job, ref error }
            if job.retries == 2 && error == "third"
    ));

    // Pin the Go deadline quirk: expiry is checked before a retry increment and
    // the handler is not called.
    seed(
        &pool,
        "deadline",
        PROCESS_TORRENT,
        "retry",
        0,
        -1,
        1,
        3,
        "{}",
    )
    .await;
    sqlx::query("UPDATE queue_jobs SET deadline = clock_timestamp() - interval '1 second' WHERE id = 'deadline'")
        .execute(&pool)
        .await
        .expect("expire deadline");
    assert!(matches!(
        store
            .consume_one(PROCESS_TORRENT, |_| async {
                panic!("expired handler must not run");
                #[allow(unreachable_code)]
                Ok::<(), String>(())
            })
            .await
            .expect("settle expired job"),
        ConsumeOutcome::RetryScheduled { ref job, .. } if job.retries == 1
    ));

    // Go recovers handler panics; Rust must also settle them instead of
    // crash-looping forever on the same locked row.
    seed(
        &pool,
        "panic",
        PROCESS_TORRENT,
        "pending",
        0,
        -1,
        0,
        0,
        "{}",
    )
    .await;
    assert!(matches!(
        store
            .consume_one(PROCESS_TORRENT, |_| async {
                panic!("poison payload");
                #[allow(unreachable_code)]
                Ok::<(), String>(())
            })
            .await
            .expect("settle panicking handler"),
        ConsumeOutcome::Failed { ref job, ref error }
            if job.id == "panic" && error.contains("poison payload")
    ));

    // The production-safe first run starts at a deliberate database-time
    // watermark and never silently replays retained archive history.
    reset(&pool).await;
    let eligible_payload = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["dddddddddddddddddddddddddddddddddddddddd"]}"#;
    seed(
        &pool,
        "historical-source",
        PROCESS_TORRENT,
        "processed",
        0,
        -1,
        0,
        2,
        eligible_payload,
    )
    .await;
    seed_torrent(&pool, "dddddddddddddddddddddddddddddddddddddddd").await;
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = clock_timestamp() - interval '1 second' \
         WHERE id = 'historical-source'",
    )
    .execute(&pool)
    .await
    .expect("settle historical source");
    let latest = store
        .mirror_processed_page(&MirrorConfig {
            sample_basis_points: 10_000,
            ..MirrorConfig::default()
        })
        .await
        .expect("bootstrap at latest");
    assert_eq!(latest.scanned, 0);
    assert_eq!(latest.inserted, 0);
    assert!(latest.cursor.is_some());
    assert_eq!(
        latest.cursor.as_ref().expect("latest cursor").id,
        "",
        "latest bootstrap uses the server-time lower bound"
    );

    // Scratch insertion and durable cursor advancement are one transaction.
    // Force the final cursor update to fail and prove that neither write
    // survives.
    reset(&pool).await;
    seed(
        &pool,
        "atomic-source",
        PROCESS_TORRENT,
        "processed",
        0,
        -1,
        0,
        2,
        eligible_payload,
    )
    .await;
    seed_torrent(&pool, "dddddddddddddddddddddddddddddddddddddddd").await;
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = clock_timestamp() - interval '1 second' \
         WHERE id = 'atomic-source'",
    )
    .execute(&pool)
    .await
    .expect("settle atomic source");
    sqlx::query(
        "ALTER TABLE queue_mirror_cursors \
         ADD CONSTRAINT queue_mirror_test_no_advance CHECK (source_job_id IS NULL)",
    )
    .execute(&pool)
    .await
    .expect("install test-only cursor constraint");
    let atomic_config = MirrorConfig {
        bootstrap: MirrorBootstrap::ArchiveStart,
        sample_basis_points: 10_000,
        ..MirrorConfig::default()
    };
    assert!(
        store.mirror_processed_page(&atomic_config).await.is_err(),
        "forced cursor failure must abort the mirror transaction"
    );
    sqlx::query(
        "ALTER TABLE queue_mirror_cursors \
         DROP CONSTRAINT queue_mirror_test_no_advance",
    )
    .execute(&pool)
    .await
    .expect("remove test-only cursor constraint");
    let atomic_rows: i64 = sqlx::query_scalar(
        "SELECT \
           (SELECT count(*) FROM queue_mirror_cursors) + \
           (SELECT count(*) FROM queue_jobs WHERE queue = $1)",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .fetch_one(&pool)
    .await
    .expect("count rolled-back mirror writes");
    assert_eq!(atomic_rows, 0);

    // Mirror only settled source rows, preserve the stored JSONB value, and
    // stop without advancing past a sampled row when the active cap is full.
    reset(&pool).await;
    let payload_a = r#"{"ClassifyMode":1,"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}"#;
    let payload_b = r#"{"ClassifyMode":0,"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]}"#;
    let payload_default = r#"{"InfoHashes":["cccccccccccccccccccccccccccccccccccccccc"]}"#;
    let payload_hinted = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"]}"#;
    let payload_content = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["ffffffffffffffffffffffffffffffffffffffff"]}"#;
    let payload_deleted = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["9999999999999999999999999999999999999999"]}"#;
    seed(
        &pool,
        "source-a",
        PROCESS_TORRENT,
        "processed",
        4,
        -1,
        0,
        2,
        payload_a,
    )
    .await;
    seed_torrent(&pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").await;
    seed(
        &pool,
        "source-default",
        PROCESS_TORRENT,
        "processed",
        4,
        -1,
        0,
        2,
        payload_default,
    )
    .await;
    seed_torrent(&pool, "cccccccccccccccccccccccccccccccccccccccc").await;
    seed(
        &pool,
        "source-hinted",
        PROCESS_TORRENT,
        "processed",
        4,
        -1,
        0,
        2,
        payload_hinted,
    )
    .await;
    seed_torrent(&pool, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").await;
    sqlx::query(
        "INSERT INTO torrent_hints \
         (info_hash, content_type, release_group, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', 'ENRICHED', \
                 clock_timestamp(), clock_timestamp())",
    )
    .bind("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
    .execute(&pool)
    .await
    .expect("seed type-only explicit hint");
    seed(
        &pool,
        "source-content",
        PROCESS_TORRENT,
        "processed",
        4,
        -1,
        0,
        2,
        payload_content,
    )
    .await;
    seed_torrent(&pool, "ffffffffffffffffffffffffffffffffffffffff").await;
    sqlx::query(
        "INSERT INTO content \
           (type, source, id, title, created_at, updated_at) \
         VALUES ('movie', 'tmdb', '603', 'fixture', \
                 clock_timestamp(), clock_timestamp())",
    )
    .execute(&pool)
    .await
    .expect("seed source-backed content metadata");
    sqlx::query(
        "INSERT INTO torrent_contents \
           (info_hash, content_type, content_source, content_id, languages, episodes, \
            created_at, updated_at, tsv, published_at, size) \
         VALUES (decode($1, 'hex'), 'movie', 'tmdb', '603', '[]'::jsonb, '{}'::jsonb, \
                 clock_timestamp(), clock_timestamp(), ''::tsvector, \
                 clock_timestamp(), 42)",
    )
    .bind("ffffffffffffffffffffffffffffffffffffffff")
    .execute(&pool)
    .await
    .expect("seed source-backed content");
    seed(
        &pool,
        "source-deleted",
        PROCESS_TORRENT,
        "processed",
        4,
        -1,
        0,
        2,
        payload_deleted,
    )
    .await;
    seed(
        &pool,
        "source-b",
        PROCESS_TORRENT,
        "processed",
        5,
        -1,
        0,
        2,
        payload_b,
    )
    .await;
    seed_torrent(&pool, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").await;
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = clock_timestamp() + \
         CASE id WHEN 'source-a' THEN interval '-2 seconds' \
                 WHEN 'source-default' THEN interval '-1800 milliseconds' \
                 WHEN 'source-hinted' THEN interval '-1600 milliseconds' \
                 WHEN 'source-content' THEN interval '-1400 milliseconds' \
                 WHEN 'source-deleted' THEN interval '-1200 milliseconds' \
                 ELSE interval '-1 second' END \
         WHERE id IN ('source-a','source-default','source-hinted',\
                      'source-content','source-deleted','source-b')",
    )
    .execute(&pool)
    .await
    .expect("settle mirror sources");
    let unsafe_config = MirrorConfig {
        shadow_queue: PROCESS_TORRENT.to_owned(),
        sample_basis_points: 10_000,
        ..MirrorConfig::default()
    };
    assert!(
        store.mirror_processed_page(&unsafe_config).await.is_err(),
        "mirror must reject the live queue as its target"
    );
    let live_active: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM queue_jobs \
         WHERE queue = $1 AND status IN ('pending','retry')",
    )
    .bind(PROCESS_TORRENT)
    .fetch_one(&pool)
    .await
    .expect("count live active jobs");
    assert_eq!(live_active, 0, "unsafe mirror attempt must insert nothing");

    let config = MirrorConfig {
        bootstrap: MirrorBootstrap::ArchiveStart,
        sample_basis_points: 10_000,
        active_depth_cap: 1,
        delay: Duration::from_secs(30),
        ..MirrorConfig::default()
    };
    let first_page = store
        .mirror_processed_page(&config)
        .await
        .expect("mirror first page");
    assert_eq!(first_page.scanned, 5);
    assert_eq!(first_page.inserted, 1);
    assert!(first_page.capped);
    // The admission funnel is attributable: every scanned candidate is either
    // sampled or charged to exactly one supported-subset conjunct. Without this
    // breakdown a mirror that scans steadily while admitting nothing is
    // indistinguishable from a mirror with nothing to do.
    assert_eq!(first_page.sampled, 1);
    assert_eq!(
        first_page.ineligible,
        BTreeMap::from([
            (MirrorIneligibleReason::PayloadShape, 1),
            (MirrorIneligibleReason::TorrentMissing, 1),
            (MirrorIneligibleReason::UnsupportedHint, 1),
            (MirrorIneligibleReason::HasContentSource, 1),
        ]),
        "each refusal is attributed to its first failing conjunct"
    );
    assert_eq!(
        first_page.sampled + first_page.ineligible.values().sum::<u32>(),
        first_page.scanned,
        "scanned candidates are fully partitioned into sampled and ineligible"
    );
    let scratch = sqlx::query(
        "SELECT fingerprint, payload::text AS payload, \
                (SELECT ran_at::text FROM queue_jobs WHERE id = 'source-a') \
                  AS source_ran_at, \
                run_after > created_at AS delayed \
         FROM queue_jobs WHERE queue = $1",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .fetch_one(&pool)
    .await
    .expect("read scratch row");
    let scratch_payload: String = scratch.try_get("payload").expect("scratch payload");
    let envelope: ShadowJobEnvelopeV1 =
        serde_json::from_str(&scratch_payload).expect("decode exact-source envelope");
    assert_eq!(envelope.schema_version, SHADOW_JOB_ENVELOPE_VERSION);
    assert_eq!(envelope.source_job_id, "source-a");
    assert_eq!(
        envelope.source_payload,
        serde_json::from_str::<serde_json::Value>(payload_a).unwrap()
    );
    assert_eq!(
        envelope.source_ran_at,
        scratch
            .try_get::<String, _>("source_ran_at")
            .expect("source timestamp")
    );
    assert!(scratch.try_get::<bool, _>("delayed").expect("delayed"));
    assert_eq!(
        scratch
            .try_get::<String, _>("fingerprint")
            .expect("fingerprint"),
        fingerprint(
            PROCESS_TORRENT_SHADOW,
            &serde_json::to_string(&envelope).expect("re-encode exact-source envelope")
        )
    );
    let capped = store
        .mirror_processed_page(&config)
        .await
        .expect("observe cap");
    assert_eq!(capped.scanned, 0);
    assert_eq!(capped.cursor, first_page.cursor);

    let durable_cursor = sqlx::query(
        "SELECT ran_at::text AS ran_at, source_job_id \
         FROM queue_mirror_cursors \
         WHERE source_queue = $1 AND shadow_queue = $2",
    )
    .bind(PROCESS_TORRENT)
    .bind(PROCESS_TORRENT_SHADOW)
    .fetch_one(&pool)
    .await
    .expect("read durable cursor");
    assert_eq!(
        durable_cursor
            .try_get::<String, _>("source_job_id")
            .expect("cursor source job"),
        "source-deleted"
    );
    assert_eq!(
        durable_cursor
            .try_get::<String, _>("ran_at")
            .expect("cursor timestamp"),
        first_page.cursor.as_ref().expect("first cursor").ran_at
    );

    // Terminal scratch rows free capacity. A fresh QueueStore (standing in for
    // a restarted/stale replica) resumes from the durable position and mirrors
    // the deferred source exactly once.
    sqlx::query(
        "UPDATE queue_jobs SET status = 'processed', ran_at = clock_timestamp() \
         WHERE queue = $1 AND status IN ('pending','retry')",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .execute(&pool)
    .await
    .expect("settle first scratch row");
    let restarted = QueueStore::new(pool.clone());
    let second_page = restarted
        .mirror_processed_page(&config)
        .await
        .expect("resume durable mirror");
    assert_eq!(second_page.scanned, 1);
    assert_eq!(second_page.inserted, 1);
    assert_eq!(second_page.sampled, 1);
    assert!(second_page.ineligible.is_empty());
    assert_eq!(
        second_page.cursor.as_ref().expect("second cursor").id,
        "source-b"
    );
    let complete = store
        .mirror_processed_page(&config)
        .await
        .expect("stale replica rereads durable mirror cursor");
    assert_eq!(complete.scanned, 0);
    assert_eq!(complete.inserted, 0);
    assert_eq!(complete.cursor, second_page.cursor);
    let unsupported_scratch: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM queue_jobs \
         WHERE queue = $1 \
           AND payload->'sourcePayload' IN ($2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb)",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .bind(payload_default)
    .bind(payload_hinted)
    .bind(payload_content)
    .bind(payload_deleted)
    .fetch_one(&pool)
    .await
    .expect("count unsupported scratch rows");
    assert_eq!(
        unsupported_scratch, 0,
        "unsupported default, explicit-hint, source-backed, and deleted inputs must fail closed"
    );

    // A physically bare type-only hint is reproducible and may enter the
    // scratch queue, but its exact row must predate source settlement. This
    // keeps mirror admission and the processor's repeatable-read causal fence
    // on the same supported subset.
    reset(&pool).await;
    let payload_bare_hint = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["1111111111111111111111111111111111111111"]}"#;
    let payload_changed_hint = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["2222222222222222222222222222222222222222"]}"#;
    for (id, payload, hash) in [
        (
            "source-bare-hint",
            payload_bare_hint,
            "1111111111111111111111111111111111111111",
        ),
        (
            "source-changed-hint",
            payload_changed_hint,
            "2222222222222222222222222222222222222222",
        ),
    ] {
        seed(
            &pool,
            id,
            PROCESS_TORRENT,
            "processed",
            0,
            -1,
            0,
            2,
            payload,
        )
        .await;
        seed_torrent(&pool, hash).await;
        sqlx::query(
            "INSERT INTO torrent_hints \
             (info_hash, content_type, created_at, updated_at) \
             VALUES (decode($1, 'hex'), 'movie', \
                     '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')",
        )
        .bind(hash)
        .execute(&pool)
        .await
        .expect("seed bare type-only mirror hint");
    }
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = CASE id \
           WHEN 'source-bare-hint' THEN '2026-01-01T00:00:00Z'::timestamptz \
           ELSE '2026-01-01T00:00:01Z'::timestamptz END \
         WHERE id IN ('source-bare-hint','source-changed-hint')",
    )
    .execute(&pool)
    .await
    .expect("settle bare-hint mirror sources");
    sqlx::query(
        "UPDATE torrents SET updated_at='2025-01-01T00:00:00Z'::timestamptz \
         WHERE info_hash IN (decode('1111111111111111111111111111111111111111','hex'), \
                             decode('2222222222222222222222222222222222222222','hex'))",
    )
    .execute(&pool)
    .await
    .expect("make bare-hint torrent images older than source settlement");
    sqlx::query(
        "UPDATE torrent_hints SET updated_at='2026-01-01T00:00:02Z'::timestamptz \
         WHERE info_hash=decode('2222222222222222222222222222222222222222','hex')",
    )
    .execute(&pool)
    .await
    .expect("make one bare hint newer than source settlement");
    let bare_hint_report = store
        .mirror_processed_page(&MirrorConfig {
            bootstrap: MirrorBootstrap::ArchiveStart,
            sample_basis_points: 10_000,
            ..MirrorConfig::default()
        })
        .await
        .expect("mirror the unchanged bare type-only hint");
    assert_eq!(bare_hint_report.scanned, 2);
    assert_eq!(bare_hint_report.sampled, 1);
    assert_eq!(bare_hint_report.inserted, 1);
    assert_eq!(
        bare_hint_report.ineligible,
        BTreeMap::from([(MirrorIneligibleReason::HintUpdatedAfterRanAt, 1)])
    );
    let bare_source_id: String = sqlx::query_scalar(
        "SELECT payload->>'sourceJobId' FROM queue_jobs \
         WHERE queue=$1 AND payload->>'sourceJobId'='source-bare-hint'",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .fetch_one(&pool)
    .await
    .expect("read admitted bare-hint envelope");
    assert_eq!(bare_source_id, "source-bare-hint");

    // Admission is all-or-nothing across a multi-hash payload, malformed wire
    // shapes fail closed without aborting the page, and an explicit cursor is
    // used only when the durable mirror identity is first created.
    reset(&pool).await;
    let payload_mixed = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"]}"#;
    let payload_invalid_hex = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":["not-an-info-hash"]}"#;
    let payload_non_array = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#;
    let payload_non_string = r#"{"ClassifierWorkflow":"default","ClassifierFlags":{"local_search_enabled":false,"apis_enabled":false,"tmdb_enabled":false},"InfoHashes":[42]}"#;
    for (id, payload) in [
        ("cursor-before", payload_a),
        ("mixed-hint", payload_mixed),
        ("invalid-hex", payload_invalid_hex),
        ("non-array", payload_non_array),
        ("non-string", payload_non_string),
        ("cursor-eligible", payload_b),
    ] {
        seed(
            &pool,
            id,
            PROCESS_TORRENT,
            "processed",
            0,
            -1,
            0,
            2,
            payload,
        )
        .await;
    }
    seed_torrent(&pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").await;
    seed_torrent(&pool, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").await;
    seed_torrent(&pool, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").await;
    sqlx::query(
        "INSERT INTO torrent_hints \
         (info_hash, content_type, release_group, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', 'ENRICHED', \
                 clock_timestamp(), clock_timestamp())",
    )
    .bind("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
    .execute(&pool)
    .await
    .expect("seed mixed-payload explicit hint");
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = CASE id \
           WHEN 'cursor-before' THEN '2026-01-01T00:00:00Z'::timestamptz \
           WHEN 'mixed-hint' THEN '2026-01-01T00:00:02Z'::timestamptz \
           WHEN 'invalid-hex' THEN '2026-01-01T00:00:03Z'::timestamptz \
           WHEN 'non-array' THEN '2026-01-01T00:00:04Z'::timestamptz \
           WHEN 'non-string' THEN '2026-01-01T00:00:05Z'::timestamptz \
           ELSE '2026-01-01T00:00:06Z'::timestamptz END \
         WHERE id IN ('cursor-before','mixed-hint','invalid-hex','non-array',\
                      'non-string','cursor-eligible')",
    )
    .execute(&pool)
    .await
    .expect("position explicit-cursor sources");
    sqlx::query("UPDATE torrents SET updated_at = '2025-12-31T00:00:00Z'::timestamptz")
        .execute(&pool)
        .await
        .expect("make explicit-cursor source images older than their jobs");
    let cursor_config = MirrorConfig {
        bootstrap: MirrorBootstrap::Cursor(bitmagnet_queue::MirrorCursor {
            ran_at: "2026-01-01T00:00:01Z".into(),
            id: String::new(),
        }),
        sample_basis_points: 10_000,
        ..MirrorConfig::default()
    };
    let cursor_report = store
        .mirror_processed_page(&cursor_config)
        .await
        .expect("mirror from explicit cursor");
    assert_eq!(cursor_report.scanned, 5);
    assert_eq!(cursor_report.inserted, 1);
    assert_eq!(
        cursor_report.cursor.as_ref().expect("cursor report").id,
        "cursor-eligible"
    );
    let scratch_payload_count: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM queue_jobs \
         WHERE queue = $1 \
           AND payload->>'sourceJobId' = 'cursor-eligible' \
           AND payload->'sourcePayload' = $2::jsonb",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .bind(payload_b)
    .fetch_one(&pool)
    .await
    .expect("count explicit-cursor scratch payload");
    assert_eq!(scratch_payload_count, 1);

    let durable_wins = store
        .mirror_processed_page(&MirrorConfig {
            bootstrap: MirrorBootstrap::ArchiveStart,
            sample_basis_points: 10_000,
            ..MirrorConfig::default()
        })
        .await
        .expect("retain existing durable cursor");
    assert_eq!(durable_wins.scanned, 0);
    assert_eq!(durable_wins.cursor, cursor_report.cursor);

    // Concurrent replicas serialize on the database-owned checkpoint. Even
    // though both begin with no process-local cursor, page_size=1 makes them
    // claim consecutive source positions without replay or omission.
    reset(&pool).await;
    for id in ["concurrent-a", "concurrent-b"] {
        seed(
            &pool,
            id,
            PROCESS_TORRENT,
            "processed",
            0,
            -1,
            0,
            2,
            payload_a,
        )
        .await;
    }
    seed_torrent(&pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").await;
    seed_torrent(&pool, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").await;
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = clock_timestamp() + \
         CASE id WHEN 'concurrent-a' THEN interval '-2 seconds' \
                 ELSE interval '-1 second' END \
         WHERE id IN ('concurrent-a','concurrent-b')",
    )
    .execute(&pool)
    .await
    .expect("settle concurrent mirror sources");
    let concurrent_config = MirrorConfig {
        bootstrap: MirrorBootstrap::ArchiveStart,
        sample_basis_points: 10_000,
        page_size: 1,
        active_depth_cap: 10,
        ..MirrorConfig::default()
    };
    let replica_a = QueueStore::new(pool.clone());
    let replica_b = QueueStore::new(pool.clone());
    let (report_a, report_b) = tokio::join!(
        replica_a.mirror_processed_page(&concurrent_config),
        replica_b.mirror_processed_page(&concurrent_config)
    );
    let report_a = report_a.expect("first concurrent mirror");
    let report_b = report_b.expect("second concurrent mirror");
    assert_eq!(report_a.scanned + report_b.scanned, 2);
    assert_eq!(report_a.inserted + report_b.inserted, 2);
    let concurrent_scratch: (i64, i64) = sqlx::query_as(
        "SELECT count(*)::bigint, count(DISTINCT fingerprint)::bigint FROM queue_jobs \
         WHERE queue = $1 AND status IN ('pending','retry')",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .fetch_one(&pool)
    .await
    .expect("count concurrent scratch rows");
    assert_eq!(concurrent_scratch, (2, 2));

    // A minimally granted runtime role can use the reviewed shadow
    // capabilities, but it cannot directly write either queue or any cursor
    // identity. Queue names and the cursor identity are hardcoded inside the
    // SECURITY DEFINER functions.
    reset(&pool).await;
    seed(
        &pool,
        "boundary-source",
        PROCESS_TORRENT,
        "processed",
        0,
        -1,
        0,
        2,
        payload_b,
    )
    .await;
    seed_torrent(&pool, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").await;
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = clock_timestamp() - interval '1 second' \
         WHERE id = 'boundary-source'",
    )
    .execute(&pool)
    .await
    .expect("settle boundary source");
    sqlx::query(
        "UPDATE torrents SET updated_at = clock_timestamp() - interval '2 seconds' \
         WHERE info_hash = decode($1, 'hex')",
    )
    .bind("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    .execute(&pool)
    .await
    .expect("prepare boundary source");
    sqlx::query(
        "DO $$ BEGIN \
         IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'bitmagnet_queue_shadow_test') THEN \
           EXECUTE 'DROP OWNED BY bitmagnet_queue_shadow_test'; \
           EXECUTE 'DROP ROLE bitmagnet_queue_shadow_test'; \
         END IF; END $$",
    )
    .execute(&pool)
    .await
    .expect("remove prior queue shadow role");
    sqlx::query(
        "CREATE ROLE bitmagnet_queue_shadow_test LOGIN PASSWORD 'queue-shadow-test-password' \
         NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
    )
    .execute(&pool)
    .await
    .expect("create queue shadow role");
    sqlx::query("GRANT USAGE ON SCHEMA public TO bitmagnet_queue_shadow_test")
        .execute(&pool)
        .await
        .expect("grant public schema usage");
    sqlx::query(
        "GRANT SELECT ON goose_db_version, queue_jobs, torrents, torrent_hints, torrent_contents \
         TO bitmagnet_queue_shadow_test",
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
         TO bitmagnet_queue_shadow_test",
    )
    .execute(&pool)
    .await
    .expect("grant reviewed shadow capabilities");

    let shadow_options = PgConnectOptions::from_str(&database_url)
        .expect("parse PostgreSQL URL")
        .username("bitmagnet_queue_shadow_test")
        .password("queue-shadow-test-password");
    let shadow_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(shadow_options)
        .await
        .expect("connect minimally granted queue shadow role");
    let null_bootstrap = sqlx::query(
        "SELECT * FROM public.ingest_shadow_lock_cursor(\
           NULL::boolean, NULL::timestamptz, NULL::text\
         )",
    )
    .execute(&shadow_pool)
    .await
    .expect_err("shadow cursor bootstrap mode must be explicit");
    assert_database_code(null_bootstrap, "22023");
    let direct_live_insert = sqlx::query(
        "INSERT INTO queue_jobs \
         (fingerprint, queue, payload, run_after, archival_duration, created_at) \
         VALUES ('forbidden-live', 'process_torrent', '{}'::jsonb, \
                 clock_timestamp(), interval '1 hour', clock_timestamp())",
    )
    .execute(&shadow_pool)
    .await
    .expect_err("shadow role must not directly insert into the live queue");
    assert_insufficient_privilege(direct_live_insert);
    let direct_scratch_insert = sqlx::query(
        "INSERT INTO queue_jobs \
         (fingerprint, queue, payload, run_after, archival_duration, created_at) \
         VALUES ('forbidden-scratch', 'process_torrent_shadow', '{}'::jsonb, \
                 clock_timestamp(), interval '1 hour', clock_timestamp())",
    )
    .execute(&shadow_pool)
    .await
    .expect_err("shadow role must not bypass the scratch enqueue capability");
    assert_insufficient_privilege(direct_scratch_insert);
    let direct_live_update =
        sqlx::query("UPDATE queue_jobs SET status = 'failed' WHERE id = 'boundary-source'")
            .execute(&shadow_pool)
            .await
            .expect_err("shadow role must not directly settle a live queue job");
    assert_insufficient_privilege(direct_live_update);
    let capability_live_settle = sqlx::query(
        "SELECT public.ingest_shadow_settle_failed(\
           'boundary-source', 0, 'forbidden live settlement'\
         )",
    )
    .execute(&shadow_pool)
    .await
    .expect_err("shadow settle capability must reject a live queue job ID");
    assert_database_code(capability_live_settle, "P0002");
    let live_status: String =
        sqlx::query_scalar("SELECT status::text FROM queue_jobs WHERE id = 'boundary-source'")
            .fetch_one(&pool)
            .await
            .expect("read live source status after rejected capability call");
    assert_eq!(live_status, "processed");
    let caller_selected_enqueue = sqlx::query(
        "SELECT public.ingest_shadow_enqueue_job(\
           'process_torrent', 'forbidden-fingerprint', '{}'::jsonb, 0, 0, 3600, 0\
         )",
    )
    .execute(&shadow_pool)
    .await
    .expect_err("enqueue capability must expose no caller-selected queue argument");
    assert_database_code(caller_selected_enqueue, "42883");
    let direct_cursor_insert = sqlx::query(
        "INSERT INTO queue_mirror_cursors (source_queue, shadow_queue) \
         VALUES ('arbitrary_source', 'arbitrary_shadow')",
    )
    .execute(&shadow_pool)
    .await
    .expect_err("shadow role must not create an arbitrary cursor identity");
    assert_insufficient_privilege(direct_cursor_insert);

    let shadow_store = QueueStore::new(shadow_pool.clone());
    let boundary_report = shadow_store
        .mirror_processed_page(&MirrorConfig {
            bootstrap: MirrorBootstrap::ArchiveStart,
            sample_basis_points: 10_000,
            delay: Duration::ZERO,
            ..MirrorConfig::default()
        })
        .await
        .expect("mirror through row-scoped capabilities");
    assert_eq!(boundary_report.scanned, 1);
    assert_eq!(boundary_report.inserted, 1);
    let status = shadow_store
        .ingest_shadow_status_snapshot()
        .await
        .expect("read fixed shadow status through existing bounded SELECT grants");
    assert_eq!(status.goose_version, 34);
    assert_eq!(status.pending, 1);
    assert_eq!(status.retry, 0);
    assert_eq!(status.failed, 0);
    let families = shadow_store
        .ingest_shadow_status_metric_families()
        .await
        .expect("collect fresh shadow metrics through the minimally granted role");
    let mut encoded = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&families, &mut encoded)
        .expect("encode minimally granted shadow metrics");
    let metrics_text = String::from_utf8(encoded).expect("Prometheus text is UTF-8");
    assert!(metrics_text.contains("bitmagnet_ingest_shadow_goose_version 34"));
    assert!(metrics_text.contains("bitmagnet_ingest_shadow_scratch_jobs{status=\"pending\"} 1"));
    assert!(metrics_text.contains("bitmagnet_ingest_shadow_scratch_jobs{status=\"retry\"} 0"));
    assert!(metrics_text.contains("bitmagnet_ingest_shadow_scratch_jobs{status=\"failed\"} 0"));
    let direct_cursor_update = sqlx::query(
        "UPDATE queue_mirror_cursors SET source_job_id = 'forbidden' \
         WHERE source_queue = 'process_torrent' \
           AND shadow_queue = 'process_torrent_shadow'",
    )
    .execute(&shadow_pool)
    .await
    .expect_err("shadow role must not directly advance the fixed cursor");
    assert_insufficient_privilege(direct_cursor_update);

    let consumed = shadow_store
        .consume_one(PROCESS_TORRENT_SHADOW, |_| async { Ok::<(), String>(()) })
        .await
        .expect("claim and settle through row-scoped capabilities");
    assert!(matches!(
        consumed,
        ConsumeOutcome::Processed { ref job }
            if job.queue == PROCESS_TORRENT_SHADOW
    ));
    let boundary_status: String = sqlx::query_scalar(
        "SELECT status::text FROM queue_jobs \
         WHERE queue = 'process_torrent_shadow'",
    )
    .fetch_one(&pool)
    .await
    .expect("read function-settled scratch status");
    assert_eq!(boundary_status, "processed");

    shadow_pool.close().await;
    sqlx::query("DROP OWNED BY bitmagnet_queue_shadow_test")
        .execute(&pool)
        .await
        .expect("remove queue shadow grants");
    sqlx::query("DROP ROLE bitmagnet_queue_shadow_test")
        .execute(&pool)
        .await
        .expect("drop queue shadow role");

    pool.close().await;
}
