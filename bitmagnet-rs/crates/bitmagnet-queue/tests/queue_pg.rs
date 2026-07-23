//! Migration-backed PostgreSQL gate for the Lane Q runtime.
//!
//! This test truncates queue, torrent, and content tables; point it only at a
//! disposable goose-28 database and invoke it explicitly with
//! `--ignored --test-threads=1`.

use std::sync::Arc;
use std::time::Duration;

use bitmagnet_queue::pg::MirrorBootstrap;
use bitmagnet_queue::{
    fingerprint, ConsumeOutcome, MirrorConfig, QueueStore, PROCESS_TORRENT, PROCESS_TORRENT_SHADOW,
};
use sqlx::{PgPool, Row};
use tokio::sync::{oneshot, Mutex};

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

#[tokio::test]
#[ignore = "requires BITMAGNET_QUEUE_TEST_DATABASE_URL pointing at disposable goose-28 PostgreSQL"]
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
         (info_hash, content_type, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', \
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
           AND payload IN ($2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb)",
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
         (info_hash, content_type, created_at, updated_at) \
         VALUES (decode($1, 'hex'), 'movie', clock_timestamp(), clock_timestamp())",
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
         WHERE queue = $1 AND payload = $2::jsonb",
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
    for (id, payload) in [("concurrent-a", payload_a), ("concurrent-b", payload_b)] {
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
    let concurrent_scratch: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM queue_jobs \
         WHERE queue = $1 AND status IN ('pending','retry')",
    )
    .bind(PROCESS_TORRENT_SHADOW)
    .fetch_one(&pool)
    .await
    .expect("count concurrent scratch rows");
    assert_eq!(concurrent_scratch, 2);

    pool.close().await;
}
