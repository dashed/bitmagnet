//! Disposable-PostgreSQL parity and admission for bounded torrent reads.
//!
//! The Go fixture generator owns all mutations. This Rust test authenticates
//! the production schema as the shared current direct-reader SELECT-only role,
//! fingerprints every public table through a separate read-only admin pool,
//! and proves replay is nonmutating. This exact six-table union is not the
//! later search-reader activation contract.

use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Output;

use async_graphql::{Request, Variables};
use bitmagnet_db::{GooseHeadMismatch, PgPool};
use bitmagnet_graphql::{
    admit_pg, build_runtime_schema, PgAdmissionError, RuntimeConfig, TorrentFilesLimits,
    TORRENT_FILES_SQL,
};
use bitmagnet_model::InfoHash;
use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

const EXPECTED_GOOSE_VERSION: i64 = 34;
const READER_ROLE: &str = "bitmagnet_graphql_reader_ci";
const ZERO_BLOB_HASH: &str = "2222222222222222222222222222222222222222";
const MISSING_SUMMARY_HASH: &str = "3333333333333333333333333333333333333333";
const MISMATCHED_BYTES_HASH: &str = "4444444444444444444444444444444444444444";

#[derive(Debug, Deserialize)]
struct OracleCase {
    id: String,
    input: Value,
    expected: Value,
}

#[derive(Debug, Deserialize)]
struct TagOracleCase {
    id: String,
    input: Value,
    expected: Value,
}

#[tokio::test]
#[ignore = "requires Go-seeded disposable PostgreSQL"]
async fn real_pg_graphql_reads_match_go_without_mutation() {
    let Some(reader_dsn) = test_dsn("BITMAGNET_GRAPHQL_TEST_DATABASE_URL") else {
        eprintln!("BITMAGNET_GRAPHQL_TEST_DATABASE_URL not set; skipping GraphQL PG parity");
        return;
    };
    let admin_dsn = test_dsn("POSTGRES_DSN")
        .expect("POSTGRES_DSN is required to fingerprint the disposable database");

    assert_select_only_reader(&reader_dsn).await;
    let pool = readonly_pool(&reader_dsn).await;
    let admin_pool = readonly_pool(&admin_dsn).await;
    let before = table_fingerprints(&admin_pool).await;

    let head = admit_pg(&pool, EXPECTED_GOOSE_VERSION)
        .await
        .expect("Goose 34 database is admitted");
    assert_eq!(head.version, EXPECTED_GOOSE_VERSION);
    assert!(matches!(
        admit_pg(&pool, EXPECTED_GOOSE_VERSION - 1).await,
        Err(PgAdmissionError::Head(GooseHeadMismatch::Unexpected {
            required: 33,
            actual: 34,
        }))
    ));
    assert_goose_mismatch_precedes_listener_bind(&reader_dsn).await;
    assert_primary_key_bounded_plan(&pool).await;

    let schema = build_runtime_schema(
        "pg-parity".to_owned(),
        pool.clone(),
        RuntimeConfig::default(),
    );
    for case in load_cases(&parity_dir().join("corpus.jsonl")) {
        let response = schema
            .execute(
                Request::new(
                    "query TorrentFilesParity($input: TorrentFilesQueryInput!) {\
                     torrent { files(input: $input) {\
                     totalCount hasNextPage items {\
                     infoHash index path extension fileType size createdAt updatedAt\
                     } } } }",
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
            serde_json::json!({ "torrent": { "files": case.expected } }),
            "oracle case {:?}",
            case.id
        );
    }

    assert_source_list_matches_go(&schema).await;
    assert_tag_suggestions_match_go(&schema).await;
    assert_empty_blob_with_zero_summary(&schema).await;
    assert_metadata_error(&schema, MISSING_SUMMARY_HASH, "file_count is NULL").await;
    assert_metadata_error(&schema, MISMATCHED_BYTES_HASH, "compressed-byte mismatch").await;

    let after = table_fingerprints(&admin_pool).await;
    assert_eq!(after, before, "GraphQL replay must not mutate PostgreSQL");

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

async fn assert_select_only_reader(dsn: &str) {
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
    .expect("read GraphQL reader role attributes");
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
    .expect("read GraphQL reader table grants");
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

    let write_error = sqlx::query("UPDATE torrents SET name = name WHERE FALSE")
        .execute(&pool)
        .await
        .expect_err("the direct GraphQL reader must not hold UPDATE");
    let sqlx::Error::Database(write_error) = write_error else {
        panic!("expected a PostgreSQL permission error, got {write_error}");
    };
    assert_eq!(write_error.code().as_deref(), Some("42501"));

    let unrelated_read = sqlx::query("SELECT count(*) FROM content")
        .fetch_one(&pool)
        .await
        .expect_err("the direct GraphQL reader must not read unrelated tables");
    let sqlx::Error::Database(unrelated_read) = unrelated_read else {
        panic!("expected a PostgreSQL permission error, got {unrelated_read}");
    };
    assert_eq!(unrelated_read.code().as_deref(), Some("42501"));

    pool.close().await;
}

async fn assert_tag_suggestions_match_go(schema: &bitmagnet_graphql::Schema) {
    for case in load_tag_cases(&tags_parity_dir().join("corpus.jsonl")) {
        let response = schema
            .execute(
                Request::new(
                    "query TorrentTagsParity($input: SuggestTagsQueryInput) {\
                     torrent { suggestTags(input: $input) { suggestions { name count } } } }",
                )
                .variables(Variables::from_json(
                    serde_json::json!({ "input": case.input }),
                )),
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "tag oracle case {:?} returned errors: {:?}",
            case.id,
            response.errors
        );
        assert_eq!(
            serde_json::to_value(response.data).expect("encode tag response data"),
            serde_json::json!({ "torrent": { "suggestTags": case.expected } }),
            "tag oracle case {:?}",
            case.id
        );
    }
}

async fn assert_source_list_matches_go(schema: &bitmagnet_graphql::Schema) {
    let response = schema
        .execute("{ torrent { listSources { sources { key name } } } }")
        .await;
    assert!(
        response.errors.is_empty(),
        "torrent.listSources errors: {:?}",
        response.errors
    );
    assert_eq!(
        serde_json::to_value(response.data).expect("encode source-list response"),
        serde_json::json!({
            "torrent": { "listSources": { "sources": [
                { "key": "dht", "name": "DHT" },
                { "key": "rarbg", "name": "RARBG" },
            ] } }
        })
    );
}

async fn assert_primary_key_bounded_plan(pool: &PgPool) {
    let limits = TorrentFilesLimits::default();
    let hash: InfoHash = "0123456789abcdef0123456789abcdef01234567"
        .parse()
        .expect("valid fixture hash");
    let mut transaction = pool
        .begin()
        .await
        .expect("start read-only plan transaction");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await
        .expect("prefer the available primary-key path");
    let explain = format!("EXPLAIN (COSTS OFF) {TORRENT_FILES_SQL}");
    let rows = sqlx::query(sqlx::AssertSqlSafe(explain))
        .bind(vec![hash.as_slice().to_vec()])
        .bind(limit_i64(limits.max_files_per_blob))
        .bind(limit_i64(limits.max_files_per_request))
        .bind(limit_i64(limits.max_compressed_bytes_per_blob))
        .bind(limit_i64(limits.max_compressed_bytes_per_request))
        .fetch_all(&mut *transaction)
        .await
        .expect("explain production torrent.files query");
    let plan = rows
        .into_iter()
        .map(|row| row.get::<String, _>("QUERY PLAN"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("torrents_pkey") && plan.contains("ANY"),
        "production query must use the info_hash primary-key selector:\n{plan}"
    );
    transaction
        .rollback()
        .await
        .expect("rollback read-only plan transaction");
}

fn limit_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

async fn assert_empty_blob_with_zero_summary(schema: &bitmagnet_graphql::Schema) {
    let response = schema
        .execute(format!(
            "{{ torrent {{ files(input: {{ infoHashes: [\"{ZERO_BLOB_HASH}\"], \
             totalCount: true, hasNextPage: true }}) {{ totalCount hasNextPage items {{ path }} }} }} }}"
        ))
        .await;
    assert!(
        response.errors.is_empty(),
        "NULL blob errors: {:?}",
        response.errors
    );
    assert_eq!(
        serde_json::to_value(response.data).expect("encode NULL-blob response"),
        serde_json::json!({
            "torrent": { "files": { "totalCount": 0, "hasNextPage": false, "items": [] } }
        })
    );
}

async fn assert_metadata_error(schema: &bitmagnet_graphql::Schema, hash: &str, message: &str) {
    let response = schema
        .execute(format!(
            "{{ torrent {{ files(input: {{ infoHashes: [\"{hash}\"] }}) {{ totalCount }} }} }}"
        ))
        .await;
    assert_eq!(response.errors.len(), 1);
    assert!(
        response.errors[0].message.contains(message),
        "expected {message:?}, got {:?}",
        response.errors
    );
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
    assert!(!output.status.success(), "Goose-33 process must fail");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Goose migration head is 34; required version 33"),
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
        .expect("run GraphQL binary against Goose 34")
}

fn load_cases(path: &Path) -> Vec<OracleCase> {
    fs::read_to_string(path)
        .expect("read torrent.files corpus")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode torrent.files oracle line"))
        .collect()
}

fn load_tag_cases(path: &Path) -> Vec<TagOracleCase> {
    fs::read_to_string(path)
        .expect("read torrent.suggestTags corpus")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode torrent.suggestTags oracle line"))
        .collect()
}

fn parity_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("testdata/parity/graphql-torrent-files")
}

fn tags_parity_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("testdata/parity/graphql-torrent-tags")
}
