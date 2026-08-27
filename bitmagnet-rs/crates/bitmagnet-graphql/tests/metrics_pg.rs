//! Disposable-PostgreSQL production-Go parity for GraphQL metrics.
//!
//! The Go generator owns all fixture mutations. Rust authenticates a dedicated
//! three-table SELECT-only role, replays the exact GraphQL corpus, and proves
//! that the complete replay leaves every public table unchanged.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use async_graphql::{Request, Variables};
use bitmagnet_db::PgPool;
use bitmagnet_graphql::{admit_pg, build_runtime_schema, RuntimeConfig};
use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;

const EXPECTED_GOOSE_VERSION: i64 = 34;
const READER_ROLE: &str = "bitmagnet_graphql_metrics_reader_ci";
const TABLES: [&str; 3] = ["goose_db_version", "queue_jobs", "torrents_torrent_sources"];

#[derive(Debug, Deserialize)]
struct OracleCase {
    id: String,
    surface: String,
    input: Value,
    expected: Value,
}

#[tokio::test]
#[ignore = "requires production-Go-seeded disposable PostgreSQL 16"]
async fn real_pg_metrics_match_go_without_mutation() {
    let Some(reader_dsn) = test_dsn("BITMAGNET_GRAPHQL_METRICS_TEST_DATABASE_URL") else {
        eprintln!(
            "BITMAGNET_GRAPHQL_METRICS_TEST_DATABASE_URL not set; skipping metrics PG parity"
        );
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
        .expect("Goose 34 database is admitted");
    assert_eq!(head.version, EXPECTED_GOOSE_VERSION);

    let schema = build_runtime_schema(
        "metrics-pg-parity".to_owned(),
        pool.clone(),
        RuntimeConfig::default(),
    );
    for case in load_cases(&parity_dir().join("corpus.jsonl")) {
        let (query, expected) = match case.surface.as_str() {
            "queue" => (
                "query MetricsParity($input: QueueMetricsQueryInput!) { queue { metrics(input: $input) { buckets { queue status createdAtBucket ranAtBucket count latency } } } }",
                serde_json::json!({ "queue": { "metrics": case.expected } }),
            ),
            "torrent" => (
                "query MetricsParity($input: TorrentMetricsQueryInput!) { torrent { metrics(input: $input) { buckets { source bucket updated count } } } }",
                serde_json::json!({ "torrent": { "metrics": case.expected } }),
            ),
            surface => panic!("unknown metrics oracle surface {surface:?}"),
        };
        let response = schema
            .execute(
                Request::new(query).variables(Variables::from_json(serde_json::json!({
                    "input": case.input,
                }))),
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
            expected,
            "oracle case {:?}",
            case.id
        );
    }

    let after = table_fingerprints(&admin_pool).await;
    assert_eq!(
        after, before,
        "GraphQL metrics replay must not mutate PostgreSQL"
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
        .expect("connect to production-Go-seeded disposable PostgreSQL")
}

async fn assert_exact_select_only_reader(dsn: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(dsn)
        .await
        .expect("connect as the GraphQL metrics reader");
    let attributes = sqlx::query_as::<_, (String, bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT current_user::text, rolcanlogin, rolinherit, rolsuper, rolcreatedb, \
         rolcreaterole, rolreplication, rolbypassrls \
         FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(&pool)
    .await
    .expect("read GraphQL metrics reader role attributes");
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
         WHERE grantee = current_user AND table_schema = 'public' \
         ORDER BY table_name, privilege_type",
    )
    .fetch_all(&pool)
    .await
    .expect("read GraphQL metrics reader table grants");
    let expected = TABLES
        .iter()
        .map(|table| ((*table).to_owned(), "SELECT".to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(grants, expected);

    let schema_grants = sqlx::query_as::<_, (String, String)>(
        "SELECT n.nspname::text, acl.privilege_type::text \
         FROM pg_namespace n \
         CROSS JOIN LATERAL aclexplode(n.nspacl) acl \
         WHERE acl.grantee = (SELECT oid FROM pg_roles WHERE rolname = current_user) \
         ORDER BY n.nspname, acl.privilege_type",
    )
    .fetch_all(&pool)
    .await
    .expect("read GraphQL metrics reader schema grants");
    assert_eq!(schema_grants, [("public".to_owned(), "USAGE".to_owned())]);

    let sequence_grants = sqlx::query_as::<_, (String, String)>(
        "SELECT c.relname::text, acl.privilege_type::text \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         CROSS JOIN LATERAL aclexplode(c.relacl) acl \
         WHERE n.nspname = 'public' AND c.relkind = 'S' \
         AND acl.grantee = (SELECT oid FROM pg_roles WHERE rolname = current_user) \
         ORDER BY c.relname, acl.privilege_type",
    )
    .fetch_all(&pool)
    .await
    .expect("read GraphQL metrics reader sequence grants");
    assert!(
        sequence_grants.is_empty(),
        "unexpected sequence grants: {sequence_grants:?}"
    );

    let memberships = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_userbyid(roleid) FROM pg_auth_members \
         WHERE member = (SELECT oid FROM pg_roles WHERE rolname = current_user)",
    )
    .fetch_all(&pool)
    .await
    .expect("read GraphQL metrics reader memberships");
    assert!(
        memberships.is_empty(),
        "unexpected memberships: {memberships:?}"
    );

    let executable_routines = sqlx::query_as::<_, (String, bool)>(
        "SELECT p.oid::regprocedure::text, \
         has_function_privilege(current_user, p.oid, 'EXECUTE') \
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'public' ORDER BY p.oid::regprocedure::text",
    )
    .fetch_all(&pool)
    .await
    .expect("enumerate GraphQL metrics reader routine authority");
    assert!(
        executable_routines
            .iter()
            .all(|(_, can_execute)| !can_execute),
        "metrics reader can execute public routines: {executable_routines:?}"
    );

    let write_error = sqlx::query("UPDATE queue_jobs SET priority = priority WHERE FALSE")
        .execute(&pool)
        .await
        .expect_err("GraphQL metrics reader must not hold UPDATE");
    assert_sqlstate_42501(write_error, "queue_jobs UPDATE");
    let unrelated_read = sqlx::query("SELECT count(*) FROM torrents")
        .fetch_one(&pool)
        .await
        .expect_err("GraphQL metrics reader must not read unrelated tables");
    assert_sqlstate_42501(unrelated_read, "torrents SELECT");

    pool.close().await;
}

fn assert_sqlstate_42501(error: sqlx::Error, operation: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL permission error for {operation}, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some("42501"), "{operation}");
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

fn load_cases(path: &Path) -> Vec<OracleCase> {
    fs::read_to_string(path)
        .expect("read GraphQL metrics corpus")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode GraphQL metrics oracle line"))
        .collect()
}

fn parity_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("testdata/parity/graphql-metrics")
}
