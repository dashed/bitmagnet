//! Same-database Go -> Rust GraphQL -> Go torrent-delete differential.

use async_graphql::{EmptySubscription, Request, Variables};
use bitmagnet_db::PgPool;
use bitmagnet_graphql::{
    admit_pg, admit_torrent_delete_writer_authority, Mutation, Query,
    TorrentDeleteMutationsRuntimeData,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::postgres::{types::Oid, PgPoolOptions};

const DATABASE_NAME: &str = "bitmagnet_graphql_torrent_delete_test";
const WRITER_ROLE: &str = "bitmagnet_graphql_torrent_delete_writer_ci";
const EXPECTED_GOOSE_VERSION: i64 = 34;
const FILTER_BYTES: usize = 25_000_091;

const HASH_A: &str = "00000000000000000000000000000000000000a1";
const HASH_B: &str = "00000000000000000000000000000000000000b2";
const HASH_C: &str = "00000000000000000000000000000000000000c3";
const HASH_D: &str = "00000000000000000000000000000000000000d4";
const HASH_E: &str = "00000000000000000000000000000000000000e5";

#[derive(Debug, Clone, PartialEq, Eq)]
struct TorrentDeleteSnapshot {
    torrents: Vec<String>,
    tags: Vec<String>,
    deleted: Vec<String>,
    bloom_oid: u32,
    bloom_owner: String,
    bloom_created_at: DateTime<Utc>,
    bloom_updated_at: DateTime<Utc>,
    bloom_encoded: Vec<u8>,
}

#[tokio::test]
#[ignore = "requires a Go-seeded disposable PostgreSQL torrent-delete database"]
async fn real_pg_torrent_delete_round_trip_matches_go_with_rollback() {
    let Some(admin_dsn) = test_dsn("BITMAGNET_GRAPHQL_TORRENT_DELETE_TEST_ADMIN_DATABASE_URL")
    else {
        eprintln!("torrent-delete parity admin DSN is not set; skipping");
        return;
    };
    let writer_dsn = test_dsn("BITMAGNET_GRAPHQL_TORRENT_DELETE_TEST_DATABASE_URL")
        .expect("torrent-delete parity writer DSN is required with the admin DSN");

    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_dsn)
        .await
        .expect("connect torrent-delete parity admin");
    let writer_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&writer_dsn)
        .await
        .expect("connect isolated torrent-delete writer");

    let admin_identity = postgres_identity(&admin_pool).await;
    assert_eq!(
        admin_identity.0, DATABASE_NAME,
        "ignored torrent-delete gate refuses a database without its exact disposable-name sentinel"
    );
    let writer_identity = postgres_identity(&writer_pool).await;
    assert_eq!(writer_identity.0, admin_identity.0);
    assert_eq!(writer_identity.1, admin_identity.1);
    assert_eq!(writer_identity.2, WRITER_ROLE);
    assert_ne!(writer_identity.2, admin_identity.2);

    let goose_head = admit_pg(&writer_pool, EXPECTED_GOOSE_VERSION)
        .await
        .expect("the isolated torrent-delete writer can admit the exact Goose head");
    assert_eq!(goose_head.version, EXPECTED_GOOSE_VERSION);
    let admission = admit_torrent_delete_writer_authority(&writer_pool)
        .await
        .expect("admit exact torrent-delete writer authority over the Go-owned object");
    assert_ne!(admission.blocked_torrents_oid, 0);
    assert_negative_writer_authority(&writer_pool, admission.blocked_torrents_oid).await;

    let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(TorrentDeleteMutationsRuntimeData::pg(writer_pool.clone()))
        .finish();

    let go_seed = read_torrent_delete_snapshot(&admin_pool).await;
    assert_rows(
        &go_seed,
        &[HASH_C, HASH_D, HASH_E],
        &[HASH_C, HASH_D, HASH_E],
        &[HASH_A, HASH_B],
    );
    assert_eq!(go_seed.bloom_oid, admission.blocked_torrents_oid);
    assert_eq!(go_seed.bloom_encoded.len(), FILTER_BYTES);
    bitmagnet_blocking::validate_go_blocked_torrents_filter(&go_seed.bloom_encoded)
        .expect("Go seed strictly decodes with the Rust production geometry");

    let delete_c = execute_delete(&schema, vec![HASH_C.to_owned(), HASH_C.to_owned()]).await;
    assert_delete_success(delete_c);
    let after_c = read_torrent_delete_snapshot(&admin_pool).await;
    assert_rows(
        &after_c,
        &[HASH_D, HASH_E],
        &[HASH_D, HASH_E],
        &[HASH_A, HASH_B, HASH_C],
    );
    assert_eq!(after_c.bloom_oid, go_seed.bloom_oid);
    assert_eq!(after_c.bloom_owner, go_seed.bloom_owner);
    assert_eq!(after_c.bloom_created_at, go_seed.bloom_created_at);
    assert_eq!(after_c.bloom_updated_at, go_seed.bloom_updated_at);
    assert_eq!(after_c.bloom_encoded.len(), FILTER_BYTES);
    assert_ne!(after_c.bloom_encoded, go_seed.bloom_encoded);
    bitmagnet_blocking::validate_go_blocked_torrents_filter(&after_c.bloom_encoded)
        .expect("Rust C checkpoint strictly decodes with the production geometry");

    set_large_object_update(&admin_pool, after_c.bloom_oid, false).await;
    let failed_d = execute_delete(&schema, vec![HASH_D.to_owned()]).await;
    set_large_object_update(&admin_pool, after_c.bloom_oid, true).await;
    assert_delete_failure(failed_d, "torrent delete blocking-store write failed");
    assert_eq!(
        read_torrent_delete_snapshot(&admin_pool).await,
        after_c,
        "failed lo_put must roll back torrent deletion, audit row, and Bloom bytes"
    );

    let readmitted = admit_torrent_delete_writer_authority(&writer_pool)
        .await
        .expect("restored exact large-object ACL is admitted");
    assert_eq!(readmitted, admission);
    let retry_d = execute_delete(&schema, Vec::new()).await;
    assert_delete_success(retry_d);
    let after_d = read_torrent_delete_snapshot(&admin_pool).await;
    assert_rows(
        &after_d,
        &[HASH_E],
        &[HASH_E],
        &[HASH_A, HASH_B, HASH_C, HASH_D],
    );
    assert_eq!(after_d.bloom_oid, go_seed.bloom_oid);
    assert_eq!(after_d.bloom_owner, go_seed.bloom_owner);
    assert_eq!(after_d.bloom_created_at, go_seed.bloom_created_at);
    assert_eq!(after_d.bloom_updated_at, go_seed.bloom_updated_at);
    assert_eq!(after_d.bloom_encoded.len(), FILTER_BYTES);
    assert_ne!(after_d.bloom_encoded, after_c.bloom_encoded);
    bitmagnet_blocking::validate_go_blocked_torrents_filter(&after_d.bloom_encoded)
        .expect("Rust retained-buffer retry strictly decodes with the production geometry");

    let invalid = execute_delete(&schema, vec!["not-a-hash".to_owned()]).await;
    assert!(
        !invalid.errors.is_empty(),
        "invalid Hash20 must fail resolver normalization"
    );
    let oversized = execute_delete(&schema, vec![HASH_E.to_owned(); 10_001]).await;
    assert_delete_failure(oversized, "has more than 10000 entries");
    assert_eq!(
        read_torrent_delete_snapshot(&admin_pool).await,
        after_d,
        "validation failures must not reach PostgreSQL"
    );

    drop(schema);
    writer_pool.close().await;
    admin_pool.close().await;
}

fn test_dsn(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

async fn postgres_identity(pool: &PgPool) -> (String, String, String) {
    sqlx::query_as::<_, (String, String, String)>(
        "SELECT current_database()::text, system_identifier::text, current_user::text \
         FROM pg_catalog.pg_control_system()",
    )
    .fetch_one(pool)
    .await
    .expect("read torrent-delete PostgreSQL identity")
}

async fn execute_delete(
    schema: &bitmagnet_graphql::Schema,
    hashes: Vec<String>,
) -> async_graphql::Response {
    schema
        .execute(
            Request::new(
                "mutation Delete($hashes: [Hash20!]!) { \
                 torrent { delete(infoHashes: $hashes) } }",
            )
            .variables(Variables::from_json(json!({ "hashes": hashes }))),
        )
        .await
}

fn assert_delete_success(response: async_graphql::Response) {
    assert!(
        response.errors.is_empty(),
        "torrent.delete returned errors: {:?}",
        response.errors
    );
    assert_eq!(
        response
            .data
            .into_json()
            .expect("serialize torrent.delete response data"),
        json!({ "torrent": { "delete": null } })
    );
}

fn assert_delete_failure(response: async_graphql::Response, expected: &str) {
    assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
    assert!(
        response.errors[0].message.contains(expected),
        "expected error containing {expected:?}, got {:?}",
        response.errors
    );
}

async fn read_torrent_delete_snapshot(pool: &PgPool) -> TorrentDeleteSnapshot {
    let (oid, owner, created_at, updated_at, encoded) =
        sqlx::query_as::<_, (Oid, String, DateTime<Utc>, DateTime<Utc>, Vec<u8>)>(
            "SELECT bf.oid, r.rolname::text, bf.created_at, bf.updated_at, \
                pg_catalog.lo_get(bf.oid, 0::bigint, $2::integer) \
         FROM public.bloom_filters bf \
         JOIN pg_catalog.pg_largeobject_metadata lom ON lom.oid = bf.oid \
         JOIN pg_catalog.pg_roles r ON r.oid = lom.lomowner \
         WHERE bf.key = $1",
        )
        .bind("blocked_torrents")
        .bind((FILTER_BYTES + 1) as i32)
        .fetch_one(pool)
        .await
        .expect("read torrent-delete Bloom snapshot");
    TorrentDeleteSnapshot {
        torrents: read_hashes(
            pool,
            "SELECT encode(info_hash, 'hex') FROM public.torrents ORDER BY info_hash",
        )
        .await,
        tags: read_hashes(
            pool,
            "SELECT encode(info_hash, 'hex') FROM public.torrent_tags ORDER BY info_hash",
        )
        .await,
        deleted: read_hashes(
            pool,
            "SELECT encode(info_hash, 'hex') FROM public.deleted_torrents ORDER BY info_hash",
        )
        .await,
        bloom_oid: oid.0,
        bloom_owner: owner,
        bloom_created_at: created_at,
        bloom_updated_at: updated_at,
        bloom_encoded: encoded,
    }
}

async fn read_hashes(pool: &PgPool, query: &'static str) -> Vec<String> {
    sqlx::query_scalar::<_, String>(query)
        .fetch_all(pool)
        .await
        .expect("read torrent-delete hash rows")
}

fn assert_rows(
    snapshot: &TorrentDeleteSnapshot,
    torrents: &[&str],
    tags: &[&str],
    deleted: &[&str],
) {
    assert_eq!(snapshot.torrents, torrents);
    assert_eq!(snapshot.tags, tags);
    assert_eq!(snapshot.deleted, deleted);
}

async fn set_large_object_update(pool: &PgPool, oid: u32, grant: bool) {
    let operation = if grant { "GRANT" } else { "REVOKE" };
    let direction = if grant { "TO" } else { "FROM" };
    // Audited dynamic SQL: operation/direction/role are closed constants and
    // oid is the u32 read from the sentinel database's blocking metadata.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "{operation} UPDATE ON LARGE OBJECT {oid} {direction} {WRITER_ROLE}"
    )))
    .execute(pool)
    .await
    .expect("change torrent-delete writer large-object UPDATE authority");
}

async fn assert_negative_writer_authority(pool: &PgPool, oid: u32) {
    assert_permission_denied(
        sqlx::query(
            "INSERT INTO public.torrents \
             (info_hash, name, size, private, created_at, updated_at) \
             VALUES (decode('1111111111111111111111111111111111111111', 'hex'), \
                     'forbidden', 0, false, now(), now())",
        )
        .execute(pool)
        .await
        .expect_err("delete writer must not insert torrents"),
        "torrents INSERT",
    );
    assert_permission_denied(
        sqlx::query("UPDATE public.torrents SET name = name WHERE false")
            .execute(pool)
            .await
            .expect_err("delete writer must not update torrents"),
        "torrents UPDATE",
    );
    assert_permission_denied(
        sqlx::query("SELECT name FROM public.torrents LIMIT 0")
            .execute(pool)
            .await
            .expect_err("delete writer must not read torrent names"),
        "torrents name SELECT",
    );
    assert_permission_denied(
        sqlx::query("SELECT count(*) FROM public.queue_jobs")
            .execute(pool)
            .await
            .expect_err("delete writer must not read unrelated tables"),
        "queue_jobs SELECT",
    );
    assert_permission_denied(
        sqlx::query("SELECT pg_catalog.lo_create(0::oid)")
            .execute(pool)
            .await
            .expect_err("delete writer must not create large objects"),
        "lo_create EXECUTE",
    );
    assert_permission_denied(
        sqlx::query("SELECT pg_catalog.lo_unlink($1::oid)")
            .bind(Oid(oid))
            .execute(pool)
            .await
            .expect_err("delete writer must not unlink the rollback object"),
        "lo_unlink EXECUTE",
    );
}

fn assert_permission_denied(error: sqlx::Error, operation: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL permission error for {operation}, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some("42501"), "{operation}");
}
