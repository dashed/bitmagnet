//! Migration-backed admission and lifecycle gate for the complete DHT crawler graph.
//!
//! This gate deliberately stops before any worker is polled. Point it only at a
//! disposable Goose-33 database and invoke it explicitly with
//! `--ignored --test-threads=1`.

use std::future::ready;
use std::net::SocketAddr;
use std::time::Duration;

use bitmagnet_blocking::BlockingFinalizeOutcome;
use bitmagnet_db::{GooseHeadMismatch, PgPool};
use bitmagnet_dht::DhtRuntimeExit;
use bitmagnet_dht_crawler::{
    DhtCrawlerAppConfig, DhtCrawlerAppProjection, DhtCrawlerPipelineExit,
    DhtCrawlerPipelineStartError, DhtCrawlerPipelineSupervisor,
};
use clap::{CommandFactory, FromArgMatches};
use sqlx::Row;

const TEST_DATABASE_URL: &str = "BITMAGNET_DHT_CRAWLER_TEST_DATABASE_URL";

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
    PgPool::connect(&database_url)
        .await
        .expect("connect disposable Goose-33 PostgreSQL")
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
