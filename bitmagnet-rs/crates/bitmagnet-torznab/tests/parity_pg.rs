//! Disposable-PostgreSQL admission and real-router Torznab parity.
//!
//! Seed the Goose-33 database with the Go fixture generator first:
//!
//! ```text
//! POSTGRES_DSN=postgres://... go test -tags integration -count=1 \
//!   ./internal/torznab -run '^TestGenerateTorznabParityPgFixtures$'
//! # Provision the fixed SELECT-only reader shown in `.github/workflows/rust.yml`.
//! BITMAGNET_TORZNAB_TEST_DATABASE_URL=postgres://bitmagnet_torznab_reader_ci:... cargo test \
//!   -p bitmagnet-torznab --test parity_pg -- --ignored --test-threads=1
//! ```
//!
//! The Go step mutates only that disposable database. Rust admission and HTTP
//! serving are read-only, and this test fingerprints every table the Torznab
//! query path can read before and after replaying the complete shared corpus.

use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener;
use std::process::Output;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use bitmagnet_db::{GooseHeadMismatch, PgPool};
use bitmagnet_torznab::{admit_pg, pg_router, Config, PgAdmissionError};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

mod parity_support;

use parity_support::{first_diff, goldens_dir, load_corpus, normalize};

const EXPECTED_GOOSE_VERSION: i64 = 33;
const READER_ROLE: &str = "bitmagnet_torznab_reader_ci";
macro_rules! fingerprint_sql {
    ($table:literal) => {
        concat!(
            "SELECT count(*)::bigint, ",
            "md5(COALESCE(string_agg(to_jsonb(row_data)::text, E'\\n' ",
            "ORDER BY to_jsonb(row_data)::text), '')) ",
            "FROM ",
            $table,
            " AS row_data"
        )
    };
}

const FINGERPRINT_QUERIES: [(&str, &str); 10] = [
    ("goose_db_version", fingerprint_sql!("goose_db_version")),
    ("content", fingerprint_sql!("content")),
    ("content_attributes", fingerprint_sql!("content_attributes")),
    (
        "content_collections_content",
        fingerprint_sql!("content_collections_content"),
    ),
    ("torrent_contents", fingerprint_sql!("torrent_contents")),
    ("torrents", fingerprint_sql!("torrents")),
    (
        "torrent_file_summary",
        fingerprint_sql!("torrent_file_summary"),
    ),
    ("torrent_sources", fingerprint_sql!("torrent_sources")),
    (
        "torrents_torrent_sources",
        fingerprint_sql!("torrents_torrent_sources"),
    ),
    ("torrent_tags", fingerprint_sql!("torrent_tags")),
];

#[tokio::test]
#[ignore = "requires Go-seeded disposable PostgreSQL (set BITMAGNET_TORZNAB_TEST_DATABASE_URL)"]
async fn real_pg_router_matches_go_goldens_without_mutation() {
    let Some(reader_dsn) = test_dsn() else {
        eprintln!("BITMAGNET_TORZNAB_TEST_DATABASE_URL not set; skipping Torznab PG parity");
        return;
    };

    assert_select_only_reader(&reader_dsn).await;

    let pool = readonly_pool(&reader_dsn).await;
    let read_only: String = sqlx::query_scalar("SHOW default_transaction_read_only")
        .fetch_one(&pool)
        .await
        .expect("read-only session setting is queryable");
    assert_eq!(read_only, "on");

    let before = table_fingerprints(&pool).await;
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

    let app = pg_router(Config::default().merge_defaults(), pool.clone());
    let dir = goldens_dir();
    let corpus = load_corpus(&dir.join("corpus.jsonl"));
    assert_eq!(corpus.len(), 67, "the complete shared corpus is replayed");
    assert_eq!(
        corpus.iter().filter(|query| query.kind == "search").count(),
        64,
        "all search queries traverse the real PostgreSQL client"
    );
    assert_eq!(
        corpus
            .iter()
            .filter(|query| query.expect_ids.is_some())
            .count(),
        64,
        "every search query carries the Go oracle's expected fixture IDs"
    );

    let mut failures = Vec::new();
    for query in &corpus {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&query.path)
                    .body(Body::empty())
                    .expect("corpus request builds"),
            )
            .await
            .expect("production router responds");
        if response.status() != StatusCode::OK {
            failures.push(format!(
                "{}: HTTP status is {}, expected 200",
                query.id,
                response.status()
            ));
            continue;
        }
        if response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/xml; charset=utf-8")
        {
            failures.push(format!("{}: response is not Torznab XML", query.id));
            continue;
        }

        let raw = to_bytes(response.into_body(), 16 * 1024 * 1024)
            .await
            .expect("response body is readable");
        let actual = normalize(&raw);
        let golden_path = dir.join(query.golden_name());
        let expected = fs::read(&golden_path).expect("Go-generated golden is readable");
        if actual != expected {
            failures.push(format!(
                "{}: normalized output != {}\n{}",
                query.id,
                golden_path.display(),
                first_diff(&actual, &expected)
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "real PostgreSQL Torznab parity failed for {}/{} queries:\n\n{}",
        failures.len(),
        corpus.len(),
        failures.join("\n\n")
    );

    let after = table_fingerprints(&pool).await;
    assert_eq!(after, before, "Torznab replay must not mutate PostgreSQL");

    drop(app);
    pool.close().await;
}

fn test_dsn() -> Option<String> {
    std::env::var("BITMAGNET_TORZNAB_TEST_DATABASE_URL")
        .ok()
        .filter(|dsn| !dsn.is_empty())
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
        .expect("connect as the dedicated Torznab reader");

    let attributes = sqlx::query_as::<_, (String, bool, bool, bool, bool, bool, bool, bool)>(
        "SELECT current_user::text, rolcanlogin, rolinherit, rolsuper, rolcreatedb, \
         rolcreaterole, rolreplication, rolbypassrls \
         FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(&pool)
    .await
    .expect("read the Torznab reader's role attributes");
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
        ),
        "the Torznab test DSN must authenticate as the dedicated unprivileged reader"
    );

    let table_privileges = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT privilege_type \
         FROM information_schema.role_table_grants \
         WHERE grantee = current_user \
         ORDER BY privilege_type",
    )
    .fetch_all(&pool)
    .await
    .expect("read effective table grants for the Torznab reader");
    assert_eq!(table_privileges, ["SELECT"]);

    let write_error = sqlx::query("UPDATE torrents SET name = name WHERE FALSE")
        .execute(&pool)
        .await
        .expect_err("the dedicated Torznab reader must not hold UPDATE");
    let sqlx::Error::Database(write_error) = write_error else {
        panic!("expected a PostgreSQL permission error, got {write_error}");
    };
    assert_eq!(write_error.code().as_deref(), Some("42501"));

    pool.close().await;
}

async fn table_fingerprints(pool: &PgPool) -> BTreeMap<&'static str, (i64, String)> {
    let mut fingerprints = BTreeMap::new();
    for (table, statement) in FINGERPRINT_QUERIES {
        let fingerprint = sqlx::query_as::<_, (i64, String)>(statement)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| panic!("fingerprint {table}: {error}"));
        fingerprints.insert(table, fingerprint);
    }
    fingerprints
}

async fn assert_goose_mismatch_precedes_listener_bind(dsn: &str) {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback listener");
    let listen_addr = occupied
        .local_addr()
        .expect("reserved listener has an address");
    let output = mismatch_process(dsn, &listen_addr.to_string()).await;
    assert!(!output.status.success(), "Goose-32 process must fail");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Goose migration head is 33; required version 32"),
        "typed Goose mismatch must be the process failure: {stderr}"
    );
    assert!(
        !stderr.contains("Address already in use"),
        "the HTTP bind must not run before Goose admission: {stderr}"
    );

    drop(occupied);
}

async fn mismatch_process(dsn: &str, listen_addr: &str) -> Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_bitmagnet-torznab"))
        .env_clear()
        .env("BITMAGNET_POSTGRES_DSN", dsn)
        .env("BITMAGNET_POSTGRES_MAX_CONNECTIONS", "1")
        .arg("--listen-addr")
        .arg(listen_addr)
        .arg("--expected-goose-version")
        .arg((EXPECTED_GOOSE_VERSION - 1).to_string())
        .output()
        .await
        .expect("run Torznab binary against Goose 33")
}
