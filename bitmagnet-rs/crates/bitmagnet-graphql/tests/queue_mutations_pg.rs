//! Disposable-PostgreSQL differential proof for queue mutations.
//!
//! The Go production manager validates the shared corpus first. This test
//! replays the same cases through Rust GraphQL with a distinct least-privilege
//! writer and proves purge-plus-enqueue rollback on a forced insert failure.

use std::fs;
use std::path::PathBuf;

use async_graphql::{EmptySubscription, Request, Variables};
use bitmagnet_db::PgPool;
use bitmagnet_graphql::{admit_pg, Mutation, Query, QueueMutationsRuntimeData};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;

const WRITER_ROLE: &str = "bitmagnet_graphql_queue_writer_ci";
const EXPECTED_GOOSE_VERSION: i64 = 34;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueMutationCase {
    id: String,
    operation: String,
    input: Option<Value>,
    updated_before: String,
    #[serde(default)]
    preexisting_enqueue: bool,
    error_contains: Option<String>,
    expected: Vec<QueueMutationRow>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueMutationRow {
    fingerprint: String,
    queue: String,
    status: String,
    payload: Value,
    max_retries: i32,
    priority: i32,
}

fn baseline_rows() -> Vec<QueueMutationRow> {
    vec![
        QueueMutationRow {
            fingerprint: "1af3b5592f64dbd81b6318ab821e1dc563af4575fdce97ea5132be773eca07a9"
                .to_owned(),
            queue: "alpha".to_owned(),
            status: "pending".to_owned(),
            payload: json!({ "case": "a" }),
            max_retries: 0,
            priority: 0,
        },
        QueueMutationRow {
            fingerprint: "f95b096cef0c64647322fe57954a4c8c8e0d37ce88d7e8ce2c545255a19f0545"
                .to_owned(),
            queue: "alpha".to_owned(),
            status: "processed".to_owned(),
            payload: json!({ "case": "b" }),
            max_retries: 0,
            priority: 0,
        },
        QueueMutationRow {
            fingerprint: "67bbaa4cefba59fdc89e8e245ef26250e03b308e8ff56c0abb5cb732a05bc50c"
                .to_owned(),
            queue: "beta".to_owned(),
            status: "retry".to_owned(),
            payload: json!({ "case": "c" }),
            max_retries: 0,
            priority: 0,
        },
        QueueMutationRow {
            fingerprint: "dfabeb2343e7d584d5b72149a03c6b428ee4f5fee1f2553591170187781e5b46"
                .to_owned(),
            queue: "gamma".to_owned(),
            status: "failed".to_owned(),
            payload: json!({ "case": "d" }),
            max_retries: 0,
            priority: 0,
        },
    ]
}

#[tokio::test]
#[ignore = "requires a Go-verified disposable PostgreSQL queue mutation database"]
async fn real_pg_queue_mutations_match_go_with_isolated_writer() {
    let Some(admin_dsn) = test_dsn("BITMAGNET_GRAPHQL_QUEUE_MUTATION_TEST_ADMIN_DATABASE_URL")
    else {
        eprintln!("queue mutation parity admin DSN is not set; skipping");
        return;
    };
    let writer_dsn = test_dsn("BITMAGNET_GRAPHQL_QUEUE_MUTATION_TEST_DATABASE_URL")
        .expect("queue mutation parity writer DSN is required with the admin DSN");

    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_dsn)
        .await
        .expect("connect queue mutation parity admin");
    let writer_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&writer_dsn)
        .await
        .expect("connect isolated queue mutation writer");
    let admin_identity = postgres_identity(&admin_pool).await;
    assert_eq!(
        admin_identity.0, "bitmagnet_graphql_queue_mutation_test",
        "ignored queue mutation gate refuses a database without its exact disposable-name sentinel"
    );
    assert_eq!(
        postgres_identity(&writer_pool).await,
        admin_identity,
        "queue mutation reader and writer must target the same PostgreSQL system and database"
    );
    assert_exact_writer_authority(&writer_pool).await;
    let goose_head = admit_pg(&writer_pool, EXPECTED_GOOSE_VERSION)
        .await
        .expect("the isolated queue writer can admit the exact Goose head");
    assert_eq!(goose_head.version, EXPECTED_GOOSE_VERSION);

    for oracle in load_cases() {
        seed_baseline(&admin_pool).await;
        if oracle.preexisting_enqueue {
            let row = oracle
                .expected
                .iter()
                .find(|row| row.queue == "process_torrent_batch")
                .expect("conflict case carries the preexisting batch row");
            insert_row(&admin_pool, row).await;
        }

        let fixed = parse_time(&oracle.updated_before);
        let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(QueueMutationsRuntimeData::pg_fixed(
                writer_pool.clone(),
                fixed,
                fixed,
            ))
            .finish();
        let response = execute_case(&schema, &oracle).await;
        match oracle.error_contains.as_deref() {
            None => assert!(
                response.errors.is_empty(),
                "oracle case {:?} returned errors: {:?}",
                oracle.id,
                response.errors
            ),
            Some(expected) => {
                assert_eq!(response.errors.len(), 1, "oracle case {:?}", oracle.id);
                assert!(
                    response.errors[0].message.contains(expected),
                    "oracle case {:?}: expected error containing {:?}, got {:?}",
                    oracle.id,
                    expected,
                    response.errors
                );
            }
        }
        assert_eq!(
            read_rows(&admin_pool).await,
            sorted_rows(oracle.expected),
            "{:?}",
            oracle.id
        );
    }

    prove_enqueue_purge_rollback(&admin_pool, &writer_pool).await;

    writer_pool.close().await;
    admin_pool.close().await;
}

async fn execute_case(
    schema: &bitmagnet_graphql::Schema,
    oracle: &QueueMutationCase,
) -> async_graphql::Response {
    let (query, input) = match oracle.operation.as_str() {
        "purge" => (
            "mutation Purge($input: QueuePurgeJobsInput!) { queue { purgeJobs(input: $input) } }",
            oracle.input.clone().expect("purge input is present"),
        ),
        "enqueue" => (
            "mutation Enqueue($input: QueueEnqueueReprocessTorrentsBatchInput) { \
             queue { enqueueReprocessTorrentsBatch(input: $input) } }",
            oracle.input.clone().unwrap_or(Value::Null),
        ),
        other => panic!("unknown operation {other:?}"),
    };
    schema
        .execute(Request::new(query).variables(Variables::from_json(json!({ "input": input }))))
        .await
}

async fn prove_enqueue_purge_rollback(admin_pool: &PgPool, writer_pool: &PgPool) {
    seed_baseline(admin_pool).await;
    sqlx::query(
        "ALTER TABLE queue_jobs ADD CONSTRAINT queue_mutation_reject_batch \
         CHECK (queue <> 'process_torrent_batch') NOT VALID",
    )
    .execute(admin_pool)
    .await
    .expect("install forced batch rejection");

    let fixed = parse_time("2026-08-27T20:15:16Z");
    let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(QueueMutationsRuntimeData::pg_fixed(
            writer_pool.clone(),
            fixed,
            fixed,
        ))
        .finish();
    let response = schema
        .execute("mutation { queue { enqueueReprocessTorrentsBatch(input: { purge: true }) } }")
        .await;
    assert_eq!(response.errors.len(), 1);
    assert!(response.errors[0]
        .message
        .contains("queue_mutation_reject_batch"));

    sqlx::query("ALTER TABLE queue_jobs DROP CONSTRAINT queue_mutation_reject_batch")
        .execute(admin_pool)
        .await
        .expect("drop forced batch rejection");
    assert_eq!(read_rows(admin_pool).await, sorted_rows(baseline_rows()));
}

async fn postgres_identity(pool: &PgPool) -> (String, String) {
    sqlx::query_as::<_, (String, String)>(
        "SELECT current_database()::text, system_identifier::text FROM pg_control_system()",
    )
    .fetch_one(pool)
    .await
    .expect("read queue mutation PostgreSQL identity")
}

async fn assert_exact_writer_authority(pool: &PgPool) {
    let attributes = sqlx::query_as::<_, (String, bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT current_user::text, rolcanlogin, rolinherit, rolsuper, rolcreatedb, \
         rolcreaterole, rolreplication, rolbypassrls \
         FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(pool)
    .await
    .expect("read queue writer role attributes");
    assert_eq!(
        attributes,
        (
            WRITER_ROLE.to_owned(),
            true,
            false,
            false,
            false,
            false,
            false,
            false,
        )
    );

    let grants = sqlx::query_as::<_, (String, String)>(
        "SELECT table_name, privilege_type \
         FROM information_schema.role_table_grants \
         WHERE grantee = current_user \
         ORDER BY table_name, privilege_type",
    )
    .fetch_all(pool)
    .await
    .expect("read queue writer grants");
    assert_eq!(
        grants,
        [
            ("goose_db_version".to_owned(), "SELECT".to_owned()),
            ("queue_jobs".to_owned(), "DELETE".to_owned()),
            ("queue_jobs".to_owned(), "INSERT".to_owned()),
            ("queue_jobs".to_owned(), "TRUNCATE".to_owned()),
        ]
    );

    let selected_columns = sqlx::query_scalar::<_, String>(
        "SELECT column_name FROM information_schema.column_privileges \
         WHERE grantee = current_user AND table_schema = 'public' \
           AND table_name = 'queue_jobs' AND privilege_type = 'SELECT' \
         ORDER BY column_name",
    )
    .fetch_all(pool)
    .await
    .expect("read queue writer selected columns");
    assert_eq!(selected_columns, ["queue", "status"]);

    sqlx::query("SELECT queue, status::text FROM queue_jobs LIMIT 0")
        .execute(pool)
        .await
        .expect("queue writer can read only its filter columns");
    let payload_read = sqlx::query("SELECT payload FROM queue_jobs LIMIT 0")
        .execute(pool)
        .await
        .expect_err("queue writer must not read payload");
    assert_sqlstate_42501(payload_read, "queue_jobs payload SELECT");
    let update_error = sqlx::query("UPDATE queue_jobs SET priority = priority WHERE FALSE")
        .execute(pool)
        .await
        .expect_err("queue writer must not hold UPDATE");
    assert_sqlstate_42501(update_error, "queue_jobs UPDATE");
    let unrelated_read = sqlx::query("SELECT count(*) FROM torrents")
        .fetch_one(pool)
        .await
        .expect_err("queue writer must not read torrents");
    assert_sqlstate_42501(unrelated_read, "torrents SELECT");
}

fn assert_sqlstate_42501(error: sqlx::Error, operation: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL permission error for {operation}, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some("42501"), "{operation}");
}

async fn seed_baseline(pool: &PgPool) {
    sqlx::query("TRUNCATE queue_jobs")
        .execute(pool)
        .await
        .expect("truncate queue mutation baseline");
    for row in baseline_rows() {
        insert_row(pool, &row).await;
    }
}

async fn insert_row(pool: &PgPool, row: &QueueMutationRow) {
    let created_at = parse_time("2024-06-01T00:00:00Z");
    sqlx::query(
        "INSERT INTO queue_jobs \
         (fingerprint, queue, status, payload, retries, max_retries, run_after, \
          ran_at, error, deadline, archival_duration, created_at, priority) \
         VALUES ($1, $2, $3::queue_job_status, $4::jsonb, 0, $5, $6, \
                 NULL, NULL, NULL, interval '7 days', $6, $7)",
    )
    .bind(&row.fingerprint)
    .bind(&row.queue)
    .bind(&row.status)
    .bind(row.payload.to_string())
    .bind(row.max_retries)
    .bind(created_at)
    .bind(row.priority)
    .execute(pool)
    .await
    .expect("seed queue mutation row");
}

async fn read_rows(pool: &PgPool) -> Vec<QueueMutationRow> {
    let raw = sqlx::query_as::<_, (String, String, String, String, i32, i32)>(
        "SELECT fingerprint, queue, status::text, payload::text, max_retries, priority \
         FROM queue_jobs ORDER BY queue, status::text, fingerprint",
    )
    .fetch_all(pool)
    .await
    .expect("read queue mutation rows");
    sorted_rows(
        raw.into_iter()
            .map(
                |(fingerprint, queue, status, payload, max_retries, priority)| QueueMutationRow {
                    fingerprint,
                    queue,
                    status,
                    payload: serde_json::from_str(&payload)
                        .expect("decode PostgreSQL queue payload"),
                    max_retries,
                    priority,
                },
            )
            .collect(),
    )
}

fn sorted_rows(mut rows: Vec<QueueMutationRow>) -> Vec<QueueMutationRow> {
    rows.sort_by(|left, right| {
        (&left.queue, &left.status, &left.fingerprint).cmp(&(
            &right.queue,
            &right.status,
            &right.fingerprint,
        ))
    });
    rows
}

fn parse_time(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap_or_else(|error| panic!("parse queue mutation time {value:?}: {error}"))
        .with_timezone(&Utc)
}

fn load_cases() -> Vec<QueueMutationCase> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/graphql-queue-mutations/corpus.jsonl");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read queue mutation parity corpus {path:?}: {error}"));
    let cases = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode queue mutation parity case"))
        .collect::<Vec<_>>();
    assert!(!cases.is_empty(), "queue mutation parity corpus is empty");
    cases
}

fn test_dsn(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|dsn| !dsn.is_empty())
}
