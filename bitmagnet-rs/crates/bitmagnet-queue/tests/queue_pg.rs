//! Migration-backed PostgreSQL gate for the Lane Q runtime.
//!
//! This test truncates `queue_jobs`; point it only at a disposable goose-27
//! database and invoke it explicitly with `--ignored --test-threads=1`.

use std::sync::Arc;
use std::time::Duration;

use bitmagnet_queue::{
    fingerprint, ConsumeOutcome, MirrorConfig, QueueStore, PROCESS_TORRENT, PROCESS_TORRENT_SHADOW,
};
use sqlx::{PgPool, Row};
use tokio::sync::{oneshot, Mutex};

async fn reset(pool: &PgPool) {
    sqlx::query("TRUNCATE queue_jobs")
        .execute(pool)
        .await
        .expect("truncate queue_jobs");
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
                 clock_timestamp() + make_interval(secs => $8), interval '7 days', \
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

#[tokio::test]
#[ignore = "requires BITMAGNET_QUEUE_TEST_DATABASE_URL pointing at disposable goose-27 PostgreSQL"]
async fn queue_runtime_matches_go_contract() {
    let database_url = std::env::var("BITMAGNET_QUEUE_TEST_DATABASE_URL")
        .expect("BITMAGNET_QUEUE_TEST_DATABASE_URL must be set for ignored gate");
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect disposable PostgreSQL");
    let store = QueueStore::new(pool.clone());

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

    // Mirror only settled source rows, preserve the stored JSONB value, and
    // stop without advancing past a sampled row when the active cap is full.
    reset(&pool).await;
    let payload_a = r#"{"ClassifyMode":1,"ClassifierWorkflow":"custom","ClassifierFlags":{"contentType":true,"contentSource":false},"InfoHashes":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}"#;
    let payload_b =
        r#"{"ClassifyMode":0,"InfoHashes":["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]}"#;
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
    sqlx::query(
        "UPDATE queue_jobs SET ran_at = clock_timestamp() + \
         CASE id WHEN 'source-a' THEN interval '-2 seconds' ELSE interval '-1 second' END \
         WHERE id IN ('source-a','source-b')",
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
        store
            .mirror_processed_page(&unsafe_config, None)
            .await
            .is_err(),
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
        sample_basis_points: 10_000,
        active_depth_cap: 1,
        delay: Duration::from_secs(30),
        ..MirrorConfig::default()
    };
    let first_page = store
        .mirror_processed_page(&config, None)
        .await
        .expect("mirror first page");
    assert_eq!(first_page.scanned, 1);
    assert_eq!(first_page.inserted, 1);
    assert!(first_page.capped);
    let scratch = sqlx::query(
        "SELECT fingerprint, payload::text AS payload, \
                payload = (SELECT payload FROM queue_jobs WHERE id = 'source-a') AS same_payload, \
                run_after > created_at AS delayed \
         FROM queue_jobs WHERE queue = $1",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .fetch_one(&pool)
    .await
    .expect("read scratch row");
    let scratch_payload: String = scratch.try_get("payload").expect("scratch payload");
    assert!(scratch
        .try_get::<bool, _>("same_payload")
        .expect("same payload"));
    assert!(scratch.try_get::<bool, _>("delayed").expect("delayed"));
    assert_eq!(
        scratch
            .try_get::<String, _>("fingerprint")
            .expect("fingerprint"),
        fingerprint(PROCESS_TORRENT_SHADOW, &scratch_payload)
    );
    let capped = store
        .mirror_processed_page(&config, first_page.cursor.as_ref())
        .await
        .expect("observe cap");
    assert_eq!(capped.scanned, 0);
    assert_eq!(capped.cursor, first_page.cursor);

    pool.close().await;
}
