//! Migration-backed admission, lifecycle, and atomic-write gates for the DHT
//! crawler graph.
//!
//! The graph-admission gates deliberately stop before any worker is polled. The
//! writer gates exercise the concrete six-stage transaction and delete their
//! keyed fixtures on success. Point this suite only at a disposable Goose-33
//! database and invoke it explicitly with `--ignored --test-threads=1`.

use std::future::ready;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use bitmagnet_blocking::BlockingFinalizeOutcome;
use bitmagnet_db::{GooseHeadMismatch, PgPool};
use bitmagnet_dht::{DhtRuntimeExit, Id20};
use bitmagnet_dht_crawler::{
    DhtCrawlerAppConfig, DhtCrawlerAppProjection, DhtCrawlerPipelineExit,
    DhtCrawlerPipelineStartError, DhtCrawlerPipelineSupervisor, DhtTorrentBatchWriteError,
    DhtTorrentBatchWriter, DhtTorrentFileSummaryWrite, DhtTorrentFileWrite, DhtTorrentPiecesWrite,
    DhtTorrentSourceLinkWrite, DhtTorrentTransactionPlan, DhtTorrentWrite,
    PgDhtTorrentBatchWriteStage, PgDhtTorrentBatchWriter, PgDhtTorrentBatchWriterError,
};
use bitmagnet_model::{FilesStatus, InfoHash, TorrentFileSummary};
use bitmagnet_queue::{
    new_queue_job, ProcessTorrentParams, ProtocolId, QueueJob, QueueJobOptions,
    PROCESS_TORRENT_SHADOW,
};
use clap::{CommandFactory, FromArgMatches};
use sqlx::Row;

const TEST_DATABASE_URL: &str = "BITMAGNET_DHT_CRAWLER_TEST_DATABASE_URL";
const TEST_DATABASE_NAME: &str = "bitmagnet_writer_gate";
static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq)]
struct WriterTableCounts {
    torrents: i64,
    torrent_files: i64,
    torrent_file_summary: i64,
    torrents_torrent_sources: i64,
    torrent_pieces: i64,
    queue_jobs: i64,
    bloom_filters: i64,
}

async fn connect_disposable_database() -> PgPool {
    let database_url = std::env::var(TEST_DATABASE_URL)
        .unwrap_or_else(|_| panic!("{TEST_DATABASE_URL} must be set for ignored gate"));
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect disposable Goose-33 PostgreSQL");
    let database_name = sqlx::query_scalar::<_, String>("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("read disposable database sentinel");
    assert_eq!(
        database_name, TEST_DATABASE_NAME,
        "ignored writer gate refuses a database without its exact disposable-name sentinel"
    );
    pool
}

fn writer_projection(expected_goose_version: i64) -> DhtCrawlerAppProjection {
    let args = vec![
        "bitmagnet-dht-crawler".to_owned(),
        "--dht-server-port".to_owned(),
        "0".to_owned(),
        "--expected-goose-version".to_owned(),
        expected_goose_version.to_string(),
        "--classifier-queue".to_owned(),
        "shadow".to_owned(),
        "--dht-crawler-scaling-factor".to_owned(),
        "1".to_owned(),
        "--dht-crawler-bootstrap-nodes".to_owned(),
        "127.0.0.1:9".to_owned(),
    ];
    let matches = DhtCrawlerAppConfig::command()
        .mut_args(|argument| argument.env(None::<&str>))
        .try_get_matches_from(args)
        .expect("explicit writer test configuration parses");
    DhtCrawlerAppConfig::from_arg_matches(&matches)
        .expect("explicit writer test configuration is typed")
        .projection()
        .expect("explicit writer test configuration projects")
}

async fn writer_table_counts(pool: &PgPool) -> WriterTableCounts {
    let row = sqlx::query(
        "SELECT \
            (SELECT count(*) FROM torrents) AS torrents, \
            (SELECT count(*) FROM torrent_files) AS torrent_files, \
            (SELECT count(*) FROM torrent_file_summary) AS torrent_file_summary, \
            (SELECT count(*) FROM torrents_torrent_sources) AS torrents_torrent_sources, \
            (SELECT count(*) FROM torrent_pieces) AS torrent_pieces, \
            (SELECT count(*) FROM queue_jobs) AS queue_jobs, \
            (SELECT count(*) FROM bloom_filters) AS bloom_filters",
    )
    .fetch_one(pool)
    .await
    .expect("read all writer-owned table counts");

    WriterTableCounts {
        torrents: row.get("torrents"),
        torrent_files: row.get("torrent_files"),
        torrent_file_summary: row.get("torrent_file_summary"),
        torrents_torrent_sources: row.get("torrents_torrent_sources"),
        torrent_pieces: row.get("torrent_pieces"),
        queue_jobs: row.get("queue_jobs"),
        bloom_filters: row.get("bloom_filters"),
    }
}

fn unique_fixture_id() -> Id20 {
    let mut bytes = [0_u8; 20];
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test fixture clock is after the Unix epoch")
        .as_nanos() as u64;
    bytes[..8].copy_from_slice(&unix_nanos.to_be_bytes());
    bytes[8..16].copy_from_slice(
        &FIXTURE_SEQUENCE
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes(),
    );
    bytes[16..].copy_from_slice(&std::process::id().to_be_bytes());
    Id20::from_slice(&bytes).expect("twenty-byte fixture ID")
}

fn shadow_job(info_hash: Id20) -> QueueJob {
    new_queue_job(
        PROCESS_TORRENT_SHADOW,
        &ProcessTorrentParams {
            info_hashes: vec![ProtocolId::from_bytes(*info_hash.as_bytes())],
            ..ProcessTorrentParams::default()
        },
        QueueJobOptions::default()
            .with_max_retries(2)
            .with_delay(Duration::from_secs(60)),
    )
    .expect("fixed process_torrent_shadow payload serializes")
}

fn six_stage_plan(info_hash: Id20, queue_job: QueueJob) -> DhtTorrentTransactionPlan {
    DhtTorrentTransactionPlan {
        torrents: vec![DhtTorrentWrite {
            info_hash,
            info_hash_v1: Some(info_hash),
            info_hash_v2: None,
            meta_version: 1,
            name: format!("rust-writer-pg-{}", info_hash.as_bytes()[19]),
            size: 12_345,
            private: false,
            files_status: FilesStatus::Multi,
            files_count: Some(1),
            files_data: Some(vec![0x01, 0x02, 0x03]),
            file_extensions: Some(vec!["mkv".to_owned()]),
        }],
        files: vec![DhtTorrentFileWrite {
            info_hash,
            index: 0,
            path: "video/fixture.mkv".to_owned(),
            size: 12_345,
        }],
        file_summaries: vec![DhtTorrentFileSummaryWrite {
            summary: TorrentFileSummary {
                info_hash: InfoHash::new(*info_hash.as_bytes()),
                file_count: 1,
                total_size: 12_345,
                largest_file_size: 12_345,
                extensions: vec!["mkv".to_owned()],
                has_video: true,
                has_subtitle: false,
                has_audio: false,
            },
            compressed_bytes: Some(3),
        }],
        sources: vec![DhtTorrentSourceLinkWrite {
            source: "dht".to_owned(),
            info_hash,
        }],
        pieces: vec![DhtTorrentPiecesWrite {
            info_hash,
            piece_length: 16_384,
            pieces: vec![0x7a; 20],
        }],
        queue_jobs: vec![queue_job],
    }
}

async fn delete_writer_fixture(pool: &PgPool, info_hash: Id20, fingerprint: &str) {
    sqlx::query("DELETE FROM queue_jobs WHERE fingerprint = $1")
        .bind(fingerprint)
        .execute(pool)
        .await
        .expect("delete keyed queue fixture");
    sqlx::query("DELETE FROM torrents WHERE info_hash = $1")
        .bind(info_hash.as_bytes().as_slice())
        .execute(pool)
        .await
        .expect("delete keyed torrent fixture and cascading children");
}

async fn keyed_count(pool: &PgPool, table: &str, info_hash: Id20) -> i64 {
    let sql = match table {
        "torrents" => "SELECT count(*) FROM torrents WHERE info_hash = $1",
        "torrent_files" => "SELECT count(*) FROM torrent_files WHERE info_hash = $1",
        "torrent_file_summary" => "SELECT count(*) FROM torrent_file_summary WHERE info_hash = $1",
        "torrents_torrent_sources" => {
            "SELECT count(*) FROM torrents_torrent_sources WHERE info_hash = $1"
        }
        "torrent_pieces" => "SELECT count(*) FROM torrent_pieces WHERE info_hash = $1",
        _ => panic!("unknown fixed writer table {table}"),
    };
    sqlx::query_scalar(sql)
        .bind(info_hash.as_bytes().as_slice())
        .fetch_one(pool)
        .await
        .expect("count keyed writer rows")
}

#[tokio::test]
#[ignore = "requires BITMAGNET_DHT_CRAWLER_TEST_DATABASE_URL pointing at disposable Goose-33 PostgreSQL"]
async fn writer_graph_admits_goose_33_and_drains_without_writes() {
    let pool = connect_disposable_database().await;
    let before = writer_table_counts(&pool).await;

    let (supervisor, handles) = DhtCrawlerPipelineSupervisor::start(writer_projection(33), &pool)
        .await
        .expect("Goose-33 admits the complete taskless writer graph");
    let local_addr = supervisor.local_addr();
    drop(handles);

    let exit = tokio::time::timeout(Duration::from_secs(10), supervisor.run(ready(())))
        .await
        .expect("pre-ready writer shutdown completes within the test deadline");
    let DhtCrawlerPipelineExit::ShutdownBeforeStart { runtime, blocking } = exit else {
        panic!("ready shutdown must win before any writer worker starts: {exit:?}");
    };
    assert!(matches!(runtime, Ok(DhtRuntimeExit::Shutdown)));
    assert!(matches!(
        blocking,
        Ok(Ok(BlockingFinalizeOutcome::NothingPending))
    ));

    let rebound = tokio::net::UdpSocket::bind(SocketAddr::V4(local_addr))
        .await
        .expect("clean runtime drain releases its ephemeral UDP address");
    drop(rebound);

    assert_eq!(writer_table_counts(&pool).await, before);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires BITMAGNET_DHT_CRAWLER_TEST_DATABASE_URL pointing at disposable Goose-33 PostgreSQL"]
async fn writer_graph_rejects_goose_32_without_writes_or_a_bound_runtime() {
    let pool = connect_disposable_database().await;
    let before = writer_table_counts(&pool).await;

    let error = match DhtCrawlerPipelineSupervisor::start(writer_projection(32), &pool).await {
        Ok((supervisor, handles)) => {
            drop((supervisor, handles));
            panic!("Goose-32 requirement must not admit a Goose-33 database");
        }
        Err(error) => error,
    };
    assert!(matches!(
        &error,
        DhtCrawlerPipelineStartError::GooseHead(GooseHeadMismatch::Unexpected {
            required: 32,
            actual: 33,
        })
    ));
    assert_eq!(error.bound_addr(), None);
    assert!(error.runtime_cleanup().is_none());
    assert_eq!(writer_table_counts(&pool).await, before);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires BITMAGNET_DHT_CRAWLER_TEST_DATABASE_URL pointing at disposable Goose-33 PostgreSQL"]
async fn torrent_writer_commits_all_six_stages_with_shadow_queue_identity() {
    let pool = connect_disposable_database().await;
    let info_hash = unique_fixture_id();
    let queue_job = shadow_job(info_hash);

    let writer = PgDhtTorrentBatchWriter::new(pool.clone());
    writer
        .write_batch(&six_stage_plan(info_hash, queue_job.clone()))
        .await
        .expect("one transaction commits all six writer stages");

    for table in [
        "torrents",
        "torrent_files",
        "torrent_file_summary",
        "torrents_torrent_sources",
        "torrent_pieces",
    ] {
        assert_eq!(keyed_count(&pool, table, info_hash).await, 1, "{table}");
    }

    let queue_row = sqlx::query(
        "SELECT queue, status::text AS status, payload, retries, max_retries, priority, \
                run_after - created_at AS delay \
         FROM queue_jobs WHERE fingerprint = $1",
    )
    .bind(&queue_job.fingerprint)
    .fetch_one(&pool)
    .await
    .expect("read committed shadow queue row");
    assert_eq!(queue_row.get::<String, _>("queue"), PROCESS_TORRENT_SHADOW);
    assert_eq!(queue_row.get::<String, _>("status"), "pending");
    assert_eq!(
        queue_row.get::<serde_json::Value, _>("payload"),
        serde_json::from_str::<serde_json::Value>(&queue_job.payload)
            .expect("fixture queue payload is JSON")
    );
    assert_eq!(queue_row.get::<i32, _>("retries"), 0);
    assert_eq!(queue_row.get::<i32, _>("max_retries"), 2);
    assert_eq!(queue_row.get::<i32, _>("priority"), 0);
    let delay = queue_row.get::<sqlx::postgres::types::PgInterval, _>("delay");
    assert_eq!(delay.months, 0);
    assert_eq!(delay.days, 0);
    assert_eq!(delay.microseconds, 60_000_000);

    delete_writer_fixture(&pool, info_hash, &queue_job.fingerprint).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires BITMAGNET_DHT_CRAWLER_TEST_DATABASE_URL pointing at disposable Goose-33 PostgreSQL"]
async fn shadow_fingerprint_collision_rolls_back_every_preceding_stage() {
    let pool = connect_disposable_database().await;
    let info_hash = unique_fixture_id();
    let queue_job = shadow_job(info_hash);

    let writer = PgDhtTorrentBatchWriter::new(pool.clone());
    writer
        .write_batch(&DhtTorrentTransactionPlan {
            queue_jobs: vec![queue_job.clone()],
            ..DhtTorrentTransactionPlan::default()
        })
        .await
        .expect("seed one active shadow fingerprint");

    let error = writer
        .write_batch(&six_stage_plan(info_hash, queue_job.clone()))
        .await
        .expect_err("duplicate active fingerprint rejects the transaction");
    let DhtTorrentBatchWriteError::Rejected { source } = error else {
        panic!("a rolled-back queue collision must not be outcome-unknown");
    };
    let typed = source
        .downcast_ref::<PgDhtTorrentBatchWriterError>()
        .expect("writer retains typed PostgreSQL stage context");
    let PgDhtTorrentBatchWriterError::ExecuteRolledBack {
        stage: PgDhtTorrentBatchWriteStage::QueueJobs,
        chunk_index: 0,
        row_offset: 0,
        row_count: 1,
        source,
    } = typed
    else {
        panic!("active fingerprint must fail in the first queue chunk: {typed}");
    };
    let database_error = source
        .as_database_error()
        .expect("queue collision preserves a PostgreSQL database error");
    assert_eq!(database_error.code().as_deref(), Some("23505"));
    assert_eq!(
        database_error.constraint(),
        Some("queue_jobs_fingerprint_idx")
    );

    for table in [
        "torrents",
        "torrent_files",
        "torrent_file_summary",
        "torrents_torrent_sources",
        "torrent_pieces",
    ] {
        assert_eq!(keyed_count(&pool, table, info_hash).await, 0, "{table}");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM queue_jobs WHERE fingerprint = $1")
            .bind(&queue_job.fingerprint)
            .fetch_one(&pool)
            .await
            .expect("count preserved active shadow fingerprint"),
        1
    );

    delete_writer_fixture(&pool, info_hash, &queue_job.fingerprint).await;
    pool.close().await;
}
