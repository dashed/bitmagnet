//! Disposable-PostgreSQL authority gate for the complete GraphQL read router.
//!
//! The production Go search oracle resets and seeds the disposable database.
//! This Rust test then authenticates one exact least-privilege role, gives the
//! complete runtime schema one read-only pool, and exercises search, facet,
//! hydration, direct-torrent, and queue reads without changing any public row.
//!
//! This is an authority contract, not production identity provisioning. A
//! production activation must separately audit and revoke PUBLIC execution on
//! every public routine before granting only `budgeted_count`; granting the
//! reader alone leaves SECURITY DEFINER mutation routines publicly executable.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::process::Output;
use std::sync::Arc;

use async_graphql::{Request, Variables};
use bitmagnet_db::{GooseHeadMismatch, PgPool};
use bitmagnet_graphql::schema::search::{SearchBuildConfig, SearchFeatures};
use bitmagnet_graphql::{
    admit_pg, build_runtime_search_schema, DisabledFileSearchBackend, PgAdmissionError,
    PgL2SearchRuntime, RuntimeConfig, SearchRuntime, SqlxLaneSSearchBackend,
};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;

const EXPECTED_GOOSE_VERSION: i64 = 34;
const READER_ROLE: &str = "bitmagnet_graphql_full_reader_ci";
const HASH_ONE: &str = "0000000000000000000000000000000000000001";
const HASH_TWO: &str = "0000000000000000000000000000000000000002";
const ABSENT_HASH: &str = "ffffffffffffffffffffffffffffffffffffffff";
const TABLES: [&str; 13] = [
    "content",
    "content_attributes",
    "content_collections",
    "content_collections_content",
    "goose_db_version",
    "queue_jobs",
    "torrent_contents",
    "torrent_file_summary",
    "torrent_files",
    "torrent_sources",
    "torrent_tags",
    "torrents",
    "torrents_torrent_sources",
];

#[tokio::test]
#[ignore = "requires production-Go-seeded disposable PostgreSQL 16"]
async fn full_router_uses_one_exact_reader_without_mutation() {
    let Some(reader_dsn) = test_dsn("BITMAGNET_GRAPHQL_FULL_READER_TEST_DATABASE_URL") else {
        eprintln!(
            "BITMAGNET_GRAPHQL_FULL_READER_TEST_DATABASE_URL not set; skipping full-reader gate"
        );
        return;
    };
    let admin_dsn = test_dsn("POSTGRES_DSN")
        .expect("POSTGRES_DSN is required to fingerprint the disposable database");

    let admin_pool = readonly_pool(&admin_dsn).await;
    let before = table_fingerprints(&admin_pool).await;
    assert_exact_reader_authority(&reader_dsn).await;
    let pool = readonly_pool(&reader_dsn).await;

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

    let search: Arc<dyn SearchRuntime> = Arc::new(PgL2SearchRuntime::from_backends(
        SqlxLaneSSearchBackend::new(pool.clone()),
        DisabledFileSearchBackend,
        SearchFeatures::default(),
        SearchBuildConfig::default(),
    ));
    let schema = build_runtime_search_schema(
        "full-reader-pg".to_owned(),
        pool.clone(),
        RuntimeConfig::default(),
        search,
    );

    assert_search_facets_and_hydration(&schema).await;
    assert_direct_and_queue_reads(&schema).await;

    let after = table_fingerprints(&admin_pool).await;
    assert_eq!(
        after, before,
        "full GraphQL reader replay must not mutate any public table"
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

async fn assert_exact_reader_authority(dsn: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(dsn)
        .await
        .expect("connect as the full GraphQL reader");

    let attributes = sqlx::query_as::<_, (String, bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT current_user::text, rolcanlogin, rolinherit, rolsuper, rolcreatedb, \
         rolcreaterole, rolreplication, rolbypassrls \
         FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(&pool)
    .await
    .expect("read full GraphQL reader role attributes");
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
    .expect("read full GraphQL reader table grants");
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
    .expect("read full GraphQL reader schema grants");
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
    .expect("read full GraphQL reader sequence grants");
    assert!(
        sequence_grants.is_empty(),
        "reader must not hold sequence privileges: {sequence_grants:?}"
    );

    let memberships = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_userbyid(roleid) \
         FROM pg_auth_members \
         WHERE member = (SELECT oid FROM pg_roles WHERE rolname = current_user) \
         ORDER BY roleid",
    )
    .fetch_all(&pool)
    .await
    .expect("read full GraphQL reader memberships");
    assert!(
        memberships.is_empty(),
        "reader must not inherit or SET ROLE into another authority: {memberships:?}"
    );

    let routine_grants = sqlx::query_as::<_, (String, String)>(
        "SELECT routine_name, privilege_type \
         FROM information_schema.routine_privileges \
         WHERE grantee = current_user AND routine_schema = 'public' \
         ORDER BY routine_name, privilege_type",
    )
    .fetch_all(&pool)
    .await
    .expect("read full GraphQL reader routine grants");
    assert_eq!(
        routine_grants,
        [("budgeted_count".to_owned(), "EXECUTE".to_owned())]
    );
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT has_function_privilege(\
             current_user, 'public.budgeted_count(text,double precision)', 'EXECUTE')",
    )
    .fetch_one(&pool)
    .await
    .expect("check budgeted_count authority"));

    let executable_routines = sqlx::query_as::<_, (String, String, bool)>(
        "SELECT p.oid::regprocedure::text, p.prokind::text, \
         has_function_privilege(current_user, p.oid, 'EXECUTE') \
         FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'public' \
         ORDER BY p.oid::regprocedure::text",
    )
    .fetch_all(&pool)
    .await
    .expect("enumerate public routine authority");
    assert!(
        executable_routines
            .iter()
            .any(|(signature, _, _)| signature == "budgeted_count(text,double precision)"),
        "Goose 33 exposes budgeted_count"
    );
    for (signature, kind, can_execute) in executable_routines {
        assert_eq!(
            can_execute,
            signature == "budgeted_count(text,double precision)",
            "unexpected EXECUTE authority on public routine {signature} (prokind={kind})"
        );
    }

    for table in TABLES {
        let statement = format!("SELECT 1 FROM \"{table}\" LIMIT 0");
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("reader cannot SELECT {table}: {error}"));
    }
    sqlx::query("SELECT count, budget_exceeded FROM budgeted_count('SELECT 1', 5000.0)")
        .fetch_one(&pool)
        .await
        .expect("reader can execute budgeted_count");

    let write_error = sqlx::query("UPDATE torrents SET name = name WHERE FALSE")
        .execute(&pool)
        .await
        .expect_err("the full GraphQL reader must not hold UPDATE");
    assert_sqlstate_42501(write_error, "torrents UPDATE");

    let unrelated_read = sqlx::query("SELECT count(*) FROM metadata_sources")
        .fetch_one(&pool)
        .await
        .expect_err("the full GraphQL reader must not read unrelated tables");
    assert_sqlstate_42501(unrelated_read, "metadata_sources SELECT");

    let mutating_routine = sqlx::query("SELECT * FROM ingest_shadow_claim_job()")
        .fetch_one(&pool)
        .await
        .expect_err("the full GraphQL reader must not execute queue mutation routines");
    assert_sqlstate_42501(mutating_routine, "ingest_shadow_claim_job EXECUTE");

    pool.close().await;
}

fn assert_sqlstate_42501(error: sqlx::Error, operation: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL permission error for {operation}, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some("42501"), "{operation}");
}

async fn assert_search_facets_and_hydration(schema: &bitmagnet_graphql::Schema) {
    let input = serde_json::json!({
        "infoHashes": [HASH_ONE, HASH_TWO],
        "limit": 5,
        "totalCount": true,
        "hasNextPage": true,
        "aggregationBudget": 5_000.0,
        "orderBy": [{ "field": "info_hash", "descending": false }],
        "facets": {
            "contentType": { "aggregate": true },
            "torrentSource": { "aggregate": true },
            "torrentTag": { "aggregate": true },
            "torrentFileType": { "aggregate": true },
            "language": { "aggregate": true },
            "genre": { "aggregate": true },
            "releaseYear": { "aggregate": true },
            "videoResolution": { "aggregate": true },
            "videoSource": { "aggregate": true }
        }
    });
    let response = schema
        .execute(
            Request::new(
                r#"query FullReaderSearch($input: TorrentContentSearchQueryInput!) {
                  torrentContent {
                    search(input: $input) {
                      totalCount
                      totalCountIsEstimate
                      hasNextPage
                      items {
                        id
                        infoHash
                        title
                        contentType
                        contentSource
                        contentId
                        content { type source id title releaseYear originalLanguage { id name } }
                        torrent {
                          infoHash
                          name
                          filesCount
                          fileExtensions
                          tagNames
                          sources {
                            key
                            name
                            importId
                            seeders
                            leechers
                            seenCount
                            firstSeenAt
                            lastSeenAt
                          }
                        }
                      }
                      aggregations {
                        contentType { value count isEstimate }
                        torrentSource { value count isEstimate }
                        torrentTag { value count isEstimate }
                        torrentFileType { value count isEstimate }
                        language { value count isEstimate }
                        genre { value count isEstimate }
                        releaseYear { value count isEstimate }
                        videoResolution { value count isEstimate }
                        videoSource { value count isEstimate }
                      }
                    }
                  }
                }"#,
            )
            .variables(Variables::from_json(serde_json::json!({ "input": input }))),
        )
        .await;
    assert!(
        response.errors.is_empty(),
        "full-reader search returned errors: {:?}",
        response.errors
    );
    let data = serde_json::to_value(response.data).expect("encode search response");
    let search = &data["torrentContent"]["search"];
    assert_eq!(search["totalCount"], 2);
    assert_eq!(search["totalCountIsEstimate"], false);
    assert_eq!(search["hasNextPage"], false);
    let items = search["items"].as_array().expect("search items array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["infoHash"], HASH_ONE);
    assert_eq!(items[0]["content"]["title"], "The Matrix");
    assert!(items[0]["torrent"]["sources"]
        .as_array()
        .is_some_and(|sources| !sources.is_empty()));
    assert!(items[0]["torrent"]["tagNames"]
        .as_array()
        .is_some_and(|tags| !tags.is_empty()));

    let aggregations = search["aggregations"]
        .as_object()
        .expect("search aggregations object");
    for facet in [
        "contentType",
        "torrentSource",
        "torrentTag",
        "torrentFileType",
        "language",
        "genre",
        "releaseYear",
        "videoResolution",
        "videoSource",
    ] {
        assert!(
            aggregations.get(facet).is_some_and(Value::is_array),
            "missing {facet} aggregation: {aggregations:?}"
        );
    }
}

async fn assert_direct_and_queue_reads(schema: &bitmagnet_graphql::Schema) {
    let response = schema
        .execute(format!(
            r#"{{
              torrent {{
                listSources {{ sources {{ key name }} }}
                suggestTags(input: {{ prefix: "parity" }}) {{ suggestions {{ name count }} }}
                files(input: {{
                  infoHashes: ["{ABSENT_HASH}"]
                  limit: 1
                  totalCount: true
                  hasNextPage: true
                }}) {{
                  totalCount
                  hasNextPage
                  items {{ infoHash }}
                }}
              }}
              queue {{
                jobs(input: {{
                  limit: 1
                  totalCount: true
                  hasNextPage: true
                  facets: {{
                    queue: {{ aggregate: true }}
                    status: {{ aggregate: true }}
                  }}
                }}) {{
                  totalCount
                  hasNextPage
                  items {{ id }}
                  aggregations {{
                    queue {{ value count }}
                    status {{ value count }}
                  }}
                }}
              }}
            }}"#
        ))
        .await;
    assert!(
        response.errors.is_empty(),
        "full-reader direct queries returned errors: {:?}",
        response.errors
    );
    let data = serde_json::to_value(response.data).expect("encode direct response");
    assert!(data["torrent"]["listSources"]["sources"]
        .as_array()
        .is_some_and(|sources| sources.iter().any(|source| source["key"] == "dht")));
    assert!(data["torrent"]["suggestTags"]["suggestions"]
        .as_array()
        .is_some_and(|suggestions| !suggestions.is_empty()));
    assert_eq!(data["torrent"]["files"]["totalCount"], 0);
    assert_eq!(data["torrent"]["files"]["hasNextPage"], false);
    assert_eq!(data["queue"]["jobs"]["totalCount"], 0);
    assert_eq!(data["queue"]["jobs"]["hasNextPage"], false);
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
