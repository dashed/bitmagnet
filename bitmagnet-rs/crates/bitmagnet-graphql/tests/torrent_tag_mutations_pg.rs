//! Disposable-PostgreSQL differential proof for torrent-tag mutations.
//!
//! The Go DAO test validates the shared corpus first. This test replays those
//! cases through the Rust GraphQL surface using a separate least-privilege
//! writer role and compares the resulting table state.

use std::fs;
use std::path::PathBuf;

use async_graphql::{EmptySubscription, Request, Variables};
use bitmagnet_db::PgPool;
use bitmagnet_graphql::{admit_pg, Mutation, Query, TorrentTagMutationsRuntimeData};
use bitmagnet_model::InfoHash;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;

const WRITER_ROLE: &str = "bitmagnet_graphql_tag_writer_ci";
const EXPECTED_GOOSE_VERSION: i64 = 34;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TagMutationCase {
    id: String,
    operation: String,
    info_hashes: Option<Vec<String>>,
    tag_names: Option<Vec<String>>,
    error_contains: Option<String>,
    expected: Vec<TagRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TagRow {
    info_hash: String,
    name: String,
}

fn baseline_rows() -> Vec<TagRow> {
    vec![
        TagRow {
            info_hash: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            name: "alpha".to_owned(),
        },
        TagRow {
            info_hash: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            name: "beta".to_owned(),
        },
        TagRow {
            info_hash: "1111111111111111111111111111111111111111".to_owned(),
            name: "beta".to_owned(),
        },
        TagRow {
            info_hash: "1111111111111111111111111111111111111111".to_owned(),
            name: "gamma".to_owned(),
        },
    ]
}

#[tokio::test]
#[ignore = "requires a Go-verified disposable PostgreSQL mutation database"]
async fn real_pg_torrent_tag_mutations_match_go_with_isolated_writer() {
    let Some(admin_dsn) = test_dsn("BITMAGNET_GRAPHQL_MUTATION_TEST_ADMIN_DATABASE_URL") else {
        eprintln!("mutation parity admin DSN is not set; skipping");
        return;
    };
    let writer_dsn = test_dsn("BITMAGNET_GRAPHQL_MUTATION_TEST_DATABASE_URL")
        .expect("mutation parity writer DSN is required with the admin DSN");

    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_dsn)
        .await
        .expect("connect mutation parity admin");
    let writer_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&writer_dsn)
        .await
        .expect("connect isolated mutation writer");
    let admin_identity = postgres_identity(&admin_pool).await;
    assert_eq!(
        admin_identity.0, "bitmagnet_graphql_mutation_test",
        "ignored mutation gate refuses a database without its exact disposable-name sentinel"
    );
    assert_eq!(
        postgres_identity(&writer_pool).await,
        admin_identity,
        "mutation test reader and writer must target the same PostgreSQL system and database"
    );
    assert_exact_writer_authority(&writer_pool).await;
    let goose_head = admit_pg(&writer_pool, EXPECTED_GOOSE_VERSION)
        .await
        .expect("the isolated writer can admit the exact Goose head");
    assert_eq!(goose_head.version, EXPECTED_GOOSE_VERSION);

    let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(TorrentTagMutationsRuntimeData::pg(writer_pool.clone()))
        .finish();

    for oracle in load_cases() {
        seed_baseline(&admin_pool).await;
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
            oracle.expected,
            "{:?}",
            oracle.id
        );
    }

    drop(schema);
    writer_pool.close().await;
    admin_pool.close().await;
}

async fn postgres_identity(pool: &PgPool) -> (String, String) {
    sqlx::query_as::<_, (String, String)>(
        "SELECT current_database()::text, system_identifier::text \
         FROM pg_control_system()",
    )
    .fetch_one(pool)
    .await
    .expect("read mutation parity PostgreSQL identity")
}

async fn execute_case(
    schema: &bitmagnet_graphql::Schema,
    oracle: &TagMutationCase,
) -> async_graphql::Response {
    let hashes = oracle.info_hashes.clone().unwrap_or_default();
    let tags = oracle.tag_names.clone().unwrap_or_default();
    let (query, variables) = match oracle.operation.as_str() {
        "put" => (
            "mutation Put($hashes: [Hash20!]!, $tags: [String!]!) { \
             torrent { putTags(infoHashes: $hashes, tagNames: $tags) } }",
            serde_json::json!({ "hashes": hashes, "tags": tags }),
        ),
        "set" => (
            "mutation Set($hashes: [Hash20!]!, $tags: [String!]!) { \
             torrent { setTags(infoHashes: $hashes, tagNames: $tags) } }",
            serde_json::json!({ "hashes": hashes, "tags": tags }),
        ),
        "delete" => (
            "mutation Delete($hashes: [Hash20!], $tags: [String!]) { \
             torrent { deleteTags(infoHashes: $hashes, tagNames: $tags) } }",
            serde_json::json!({
                "hashes": oracle.info_hashes,
                "tags": oracle.tag_names,
            }),
        ),
        other => panic!("unknown operation {other:?}"),
    };
    schema
        .execute(Request::new(query).variables(Variables::from_json(variables)))
        .await
}

async fn assert_exact_writer_authority(pool: &PgPool) {
    let attributes = sqlx::query_as::<_, (String, bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT current_user::text, rolcanlogin, rolinherit, rolsuper, rolcreatedb, \
         rolcreaterole, rolreplication, rolbypassrls \
         FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(pool)
    .await
    .expect("read mutation writer role attributes");
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
    .expect("read mutation writer grants");
    assert_eq!(
        grants,
        [
            ("goose_db_version".to_owned(), "SELECT".to_owned()),
            ("torrent_tags".to_owned(), "DELETE".to_owned()),
            ("torrent_tags".to_owned(), "INSERT".to_owned()),
            ("torrent_tags".to_owned(), "SELECT".to_owned()),
        ]
    );

    let update_error = sqlx::query("UPDATE torrent_tags SET name = name WHERE FALSE")
        .execute(pool)
        .await
        .expect_err("mutation writer must not hold UPDATE");
    assert_sqlstate_42501(update_error, "torrent_tags UPDATE");
    let unrelated_read = sqlx::query("SELECT count(*) FROM torrents")
        .fetch_one(pool)
        .await
        .expect_err("mutation writer must not read torrents");
    assert_sqlstate_42501(unrelated_read, "torrents SELECT");
}

fn assert_sqlstate_42501(error: sqlx::Error, operation: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL permission error for {operation}, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some("42501"), "{operation}");
}

async fn seed_baseline(pool: &PgPool) {
    sqlx::query("TRUNCATE torrent_tags, torrents CASCADE")
        .execute(pool)
        .await
        .expect("truncate mutation parity baseline");
    let mut hashes = baseline_rows()
        .into_iter()
        .map(|row| row.info_hash)
        .collect::<Vec<_>>();
    hashes.sort();
    hashes.dedup();
    for raw_hash in hashes {
        let hash: InfoHash = raw_hash.parse().expect("baseline hash");
        sqlx::query(
            "INSERT INTO torrents \
             (info_hash, name, size, private, created_at, updated_at) \
             VALUES ($1, $2, 0, false, TIMESTAMPTZ '2024-06-01 00:00:00Z', \
                     TIMESTAMPTZ '2024-06-01 00:00:00Z')",
        )
        .bind(hash.as_slice())
        .bind(raw_hash)
        .execute(pool)
        .await
        .expect("seed baseline torrent");
    }
    for row in baseline_rows() {
        let hash: InfoHash = row.info_hash.parse().expect("baseline tag hash");
        sqlx::query(
            "INSERT INTO torrent_tags (info_hash, name, created_at, updated_at) \
             VALUES ($1, $2, TIMESTAMPTZ '2024-06-01 00:00:00Z', \
                     TIMESTAMPTZ '2024-06-01 00:00:00Z')",
        )
        .bind(hash.as_slice())
        .bind(row.name)
        .execute(pool)
        .await
        .expect("seed baseline tag");
    }
}

async fn read_rows(pool: &PgPool) -> Vec<TagRow> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT encode(info_hash, 'hex'), name \
         FROM torrent_tags ORDER BY info_hash, name",
    )
    .fetch_all(pool)
    .await
    .expect("read mutation parity rows")
    .into_iter()
    .map(|(info_hash, name)| TagRow { info_hash, name })
    .collect()
}

fn load_cases() -> Vec<TagMutationCase> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/graphql-torrent-tag-mutations/corpus.jsonl");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read mutation parity corpus {path:?}: {error}"));
    let cases = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode mutation parity case"))
        .collect::<Vec<_>>();
    assert!(!cases.is_empty(), "mutation parity corpus is empty");
    cases
}

fn test_dsn(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|dsn| !dsn.is_empty())
}
