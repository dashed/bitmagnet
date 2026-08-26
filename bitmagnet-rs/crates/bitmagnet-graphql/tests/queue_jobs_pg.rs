//! Disposable-PostgreSQL parity and admission for `queue.jobs`.
//!
//! The Go fixture generator owns all mutations. This test authenticates the
//! production schema as the shared current direct-reader SELECT-only role,
//! fingerprints every public table through a separate read-only admin pool,
//! and proves the full oracle replay is nonmutating. This exact six-table union
//! is not the later search-reader activation contract.

use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Output;

use async_graphql::{Request, Variables};
use bitmagnet_db::{GooseHeadMismatch, PgPool};
use bitmagnet_graphql::{
    admit_pg, build_runtime_schema, PgAdmissionError, RuntimeConfig, MAX_QUEUE_JOBS_LIMIT,
};
use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;

const EXPECTED_GOOSE_VERSION: i64 = 33;
const READER_ROLE: &str = "bitmagnet_graphql_reader_ci";

#[derive(Debug, Deserialize)]
struct OracleCase {
    id: String,
    input: Value,
    expected: Value,
}

#[tokio::test]
#[ignore = "requires Go-seeded disposable PostgreSQL 16"]
async fn real_pg_queue_jobs_matches_go_without_mutation() {
    let Some(reader_dsn) = test_dsn("BITMAGNET_GRAPHQL_TEST_DATABASE_URL") else {
        eprintln!("BITMAGNET_GRAPHQL_TEST_DATABASE_URL not set; skipping queue.jobs PG parity");
        return;
    };
    let admin_dsn = test_dsn("POSTGRES_DSN")
        .expect("POSTGRES_DSN is required to fingerprint the disposable database");

    assert_exact_select_only_reader(&reader_dsn).await;
    let pool = readonly_pool(&reader_dsn).await;
    let admin_pool = readonly_pool(&admin_dsn).await;
    let before = table_fingerprints(&admin_pool).await;

    let head = admit_pg(&pool, EXPECTED_GOOSE_VERSION)
        .await
        .expect("Goose 33 database is admitted");
    assert_eq!(head.version, EXPECTED_GOOSE_VERSION);
    assert!(matches!(
        admit_pg(&pool, EXPECTED_GOOSE_VERSION - 1).await,
        Err(PgAdmissionError::Head(GooseHeadMismatch::Unexpected {
            required: 32,
            actual: 33,
        }))
    ));
    assert_goose_mismatch_precedes_listener_bind(&reader_dsn).await;

    let schema = build_runtime_schema(
        "queue-pg-parity".to_owned(),
        pool.clone(),
        RuntimeConfig::default(),
    );
    for case in load_cases(&parity_dir().join("corpus.jsonl")) {
        let response = schema
            .execute(
                Request::new(
                    "query QueueJobsParity($input: QueueJobsQueryInput!) {\
                     queue { jobs(input: $input) {\
                     totalCount hasNextPage items { id queue status payload priority retries \
                     maxRetries runAfter ranAt error createdAt } aggregations {\
                     queue { value label count } status { value label count } } } } }",
                )
                .variables(Variables::from_json(
                    serde_json::json!({ "input": case.input }),
                )),
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "oracle case {:?} returned errors: {:?}",
            case.id,
            response.errors
        );
        assert_eq!(
            serde_json::to_value(response.data).expect("encode GraphQL response data"),
            serde_json::json!({ "queue": { "jobs": case.expected } }),
            "oracle case {:?}",
            case.id
        );
    }
    assert_input_bounds_fail_closed(&schema).await;

    let after = table_fingerprints(&admin_pool).await;
    assert_eq!(
        after, before,
        "queue.jobs replay must not mutate PostgreSQL"
    );

    drop(schema);
    pool.close().await;
    admin_pool.close().await;
}

fn test_dsn(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|dsn| !dsn.is_empty())
}

async fn readonly_pool(dsn: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(dsn)
        .await
        .expect("connect to Go-seeded disposable PostgreSQL")
}

async fn assert_exact_select_only_reader(dsn: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(dsn)
        .await
        .expect("connect as the shared direct GraphQL reader");
    let attributes = sqlx::query_as::<_, (String, bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT current_user::text, rolcanlogin, rolinherit, rolsuper, rolcreatedb, \
         rolcreaterole, rolreplication, rolbypassrls \
         FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(&pool)
    .await
    .expect("read queue GraphQL reader role attributes");
    assert_eq!(
        attributes,
        (
            READER_ROLE.to_owned(),
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
    .fetch_all(&pool)
    .await
    .expect("read queue GraphQL reader table grants");
    assert_eq!(
        grants,
        [
            ("goose_db_version".to_owned(), "SELECT".to_owned()),
            ("queue_jobs".to_owned(), "SELECT".to_owned()),
            ("torrent_file_summary".to_owned(), "SELECT".to_owned()),
            ("torrent_sources".to_owned(), "SELECT".to_owned()),
            ("torrent_tags".to_owned(), "SELECT".to_owned()),
            ("torrents".to_owned(), "SELECT".to_owned()),
        ]
    );

    let write_error = sqlx::query("UPDATE queue_jobs SET priority = priority WHERE FALSE")
        .execute(&pool)
        .await
        .expect_err("the direct GraphQL reader must not hold UPDATE");
    assert_sqlstate_42501(write_error, "queue_jobs UPDATE");
    let unrelated_read = sqlx::query("SELECT count(*) FROM content")
        .fetch_one(&pool)
        .await
        .expect_err("the direct GraphQL reader must not read unrelated tables");
    assert_sqlstate_42501(unrelated_read, "content SELECT");

    pool.close().await;
}

fn assert_sqlstate_42501(error: sqlx::Error, operation: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL permission error for {operation}, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some("42501"), "{operation}");
}

async fn assert_input_bounds_fail_closed(schema: &bitmagnet_graphql::Schema) {
    let oversized = schema
        .execute(format!(
            "{{ queue {{ jobs(input: {{ limit: {} }}) {{ totalCount }} }} }}",
            MAX_QUEUE_JOBS_LIMIT + 1
        ))
        .await;
    assert_eq!(oversized.errors.len(), 1);
    assert!(oversized.errors[0]
        .message
        .contains("limit must be between"));

    let negative = schema
        .execute("{ queue { jobs(input: { page: -1 }) { totalCount } } }")
        .await;
    assert_eq!(negative.errors.len(), 1);
    assert!(negative.errors[0]
        .message
        .contains("page and offset must be nonnegative"));
}

async fn table_fingerprints(pool: &PgPool) -> BTreeMap<String, (i64, String)> {
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
    )
    .fetch_all(pool)
    .await
    .expect("list public tables for fingerprinting");
    assert!(!tables.is_empty(), "migrated schema has public tables");

    let mut fingerprints = BTreeMap::new();
    for table in tables {
        assert!(
            table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "unexpected public table identifier {table:?}"
        );
        let statement = format!(
            "SELECT count(*)::bigint, \
             md5(COALESCE(string_agg(to_jsonb(row_data)::text, E'\\n' \
             ORDER BY to_jsonb(row_data)::text), '')) \
             FROM \"{table}\" AS row_data"
        );
        let fingerprint = sqlx::query_as::<_, (i64, String)>(sqlx::AssertSqlSafe(statement))
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| panic!("fingerprint {table}: {error}"));
        fingerprints.insert(table, fingerprint);
    }
    fingerprints
}

async fn assert_goose_mismatch_precedes_listener_bind(dsn: &str) {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback listener");
    let listen_addr = occupied.local_addr().expect("reserved listener address");
    let output = mismatch_process(dsn, &listen_addr.to_string()).await;
    assert!(!output.status.success(), "Goose-32 process must fail");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Goose migration head is 33; required version 32"),
        "typed Goose mismatch must be the process failure: {stderr}"
    );
    assert!(
        !stderr.contains("Address already in use"),
        "HTTP bind must not run before Goose admission: {stderr}"
    );
    drop(occupied);
}

async fn mismatch_process(dsn: &str, listen_addr: &str) -> Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_bitmagnet-graphql"))
        .env_clear()
        .env("BITMAGNET_POSTGRES_DSN", dsn)
        .env("BITMAGNET_POSTGRES_MAX_CONNECTIONS", "1")
        .arg("--listen-addr")
        .arg(listen_addr)
        .arg("--expected-goose-version")
        .arg((EXPECTED_GOOSE_VERSION - 1).to_string())
        .output()
        .await
        .expect("run GraphQL binary against Goose 33")
}

fn load_cases(path: &Path) -> Vec<OracleCase> {
    fs::read_to_string(path)
        .expect("read queue.jobs corpus")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode queue.jobs oracle line"))
        .collect()
}

fn parity_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("testdata/parity/graphql-queue-jobs")
}
