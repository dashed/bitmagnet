//! PostgreSQL differential for the read-only `process_torrent_batch` selector.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use bitmagnet_queue::{BatchSelection, ProtocolId, QueuePgError, QueueStore};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

#[derive(Deserialize)]
struct Fixture {
    subsystem: String,
    input: Input,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Seed {
    info_hash: String,
    updated_at: String,
    content_types: Vec<Option<String>>,
}

#[derive(Deserialize)]
struct Scenario {
    id: String,
    selection: BatchSelection,
}

#[derive(Deserialize)]
struct Input {
    seed: Vec<Seed>,
    cases: Vec<Scenario>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultFixture {
    id: String,
    info_hashes: Vec<ProtocolId>,
}

#[derive(Deserialize)]
struct Expected {
    results: Vec<ResultFixture>,
}

fn fixture() -> Fixture {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/queue/process_torrent_batch_selection.jsonl");
    let file = File::open(&path).unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    let lines = BufReader::new(file)
        .lines()
        .map(|line| line.expect("read fixture line"))
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "expected one batch-selection fixture");
    serde_json::from_str(&lines[0]).expect("decode batch-selection fixture")
}

async fn reset(pool: &PgPool) {
    for statement in [
        "DROP SCHEMA IF EXISTS phase3_queue_batch_selection CASCADE",
        "CREATE SCHEMA phase3_queue_batch_selection",
        "SET search_path TO phase3_queue_batch_selection",
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .expect("reset private batch-selection schema");
    }
    sqlx::query(
        "CREATE TABLE torrents (info_hash bytea PRIMARY KEY, updated_at timestamptz NOT NULL)",
    )
    .execute(pool)
    .await
    .expect("create torrents fixture table");
    sqlx::query("CREATE TABLE torrent_contents (info_hash bytea NOT NULL, content_type text NULL)")
        .execute(pool)
        .await
        .expect("create torrent_contents fixture table");
}

#[tokio::test]
#[ignore = "requires BITMAGNET_QUEUE_TEST_DATABASE_URL pointing at disposable PostgreSQL"]
async fn batch_selection_matches_go_postgres_oracle() {
    let database_url = std::env::var("BITMAGNET_QUEUE_TEST_DATABASE_URL")
        .expect("BITMAGNET_QUEUE_TEST_DATABASE_URL must be set for ignored gate");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect disposable PostgreSQL");
    reset(&pool).await;
    let fixture = fixture();
    assert_eq!(fixture.subsystem, "process_torrent_batch_selection_pg");
    for seed in fixture.input.seed {
        sqlx::query("INSERT INTO torrents (info_hash, updated_at) VALUES (decode($1, 'hex'), $2)")
            .bind(&seed.info_hash)
            .bind(
                chrono::DateTime::parse_from_rfc3339(&seed.updated_at)
                    .expect("parse fixture updatedAt"),
            )
            .execute(&pool)
            .await
            .expect("insert torrent fixture");
        for content_type in seed.content_types {
            sqlx::query(
                "INSERT INTO torrent_contents (info_hash, content_type) \
                 VALUES (decode($1, 'hex'), $2)",
            )
            .bind(&seed.info_hash)
            .bind(content_type)
            .execute(&pool)
            .await
            .expect("insert torrent content fixture");
        }
    }

    assert_eq!(fixture.input.cases.len(), fixture.expected.results.len());
    let store = QueueStore::new(pool.clone());
    for (scenario, expected) in fixture.input.cases.iter().zip(&fixture.expected.results) {
        assert_eq!(scenario.id, expected.id);
        let actual = store
            .select_process_torrent_batch_page(&scenario.selection)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", scenario.id));
        assert_eq!(actual, expected.info_hashes, "{}", scenario.id);
    }

    let mut invalid_order = fixture.input.cases[0].selection.clone();
    invalid_order.order_by = "info_hash_desc".to_string();
    assert!(matches!(
        store
            .select_process_torrent_batch_page(&invalid_order)
            .await,
        Err(QueuePgError::InvalidBatchSelection(
            "order_by must be info_hash_asc"
        ))
    ));
    let mut zero_limit = fixture.input.cases[0].selection.clone();
    zero_limit.limit = 0;
    assert!(matches!(
        store.select_process_torrent_batch_page(&zero_limit).await,
        Err(QueuePgError::InvalidBatchSelection(
            "limit must be positive"
        ))
    ));

    sqlx::query("TRUNCATE torrents, torrent_contents")
        .execute(&pool)
        .await
        .expect("clear selection fixtures");
    sqlx::query(
        "INSERT INTO torrents (info_hash, updated_at) \
         VALUES ('\\x01'::bytea, '2026-08-11T00:00:00Z'::timestamptz)",
    )
    .execute(&pool)
    .await
    .expect("insert malformed database info hash");
    assert!(matches!(
        store
            .select_process_torrent_batch_page(&fixture.input.cases[0].selection)
            .await,
        Err(QueuePgError::InvalidInfoHashLength(1))
    ));

    pool.close().await;
}
