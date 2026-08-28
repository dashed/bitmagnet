//! PostgreSQL differential for the read-only `process_torrent_batch` selector.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

use bitmagnet_queue::{BatchSelection, ProtocolId, QueuePgError, QueueStore};
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};

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
    sqlx::query("TRUNCATE queue_jobs, queue_mirror_cursors, torrents, content CASCADE")
        .execute(pool)
        .await
        .expect("truncate batch-selection source tables");
}

fn assert_database_code(error: sqlx::Error, expected: &str) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected a PostgreSQL database error, got {error}");
    };
    assert_eq!(error.code().as_deref(), Some(expected));
}

#[tokio::test]
#[ignore = "requires BITMAGNET_QUEUE_TEST_DATABASE_URL pointing at disposable PostgreSQL"]
async fn batch_selection_matches_go_postgres_oracle() {
    let database_url = std::env::var("BITMAGNET_QUEUE_TEST_DATABASE_URL")
        .expect("BITMAGNET_QUEUE_TEST_DATABASE_URL must be set for ignored gate");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("connect disposable PostgreSQL");
    reset(&pool).await;
    let fixture = fixture();
    assert_eq!(fixture.subsystem, "process_torrent_batch_selection_pg");
    for seed in fixture.input.seed {
        sqlx::query(
            "INSERT INTO torrents \
             (info_hash, name, size, private, created_at, updated_at) \
             VALUES (decode($1, 'hex'), $1, 42, false, $2, $2)",
        )
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
                "INSERT INTO torrent_contents \
                 (info_hash, content_type, created_at, updated_at) \
                 VALUES (decode($1, 'hex'), $2, clock_timestamp(), clock_timestamp())",
            )
            .bind(&seed.info_hash)
            .bind(content_type)
            .execute(&pool)
            .await
            .expect("insert torrent content fixture");
        }
    }

    sqlx::query(
        "DO $$ BEGIN \
         IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'bitmagnet_queue_batch_select_test') THEN \
           EXECUTE 'DROP OWNED BY bitmagnet_queue_batch_select_test'; \
           EXECUTE 'DROP ROLE bitmagnet_queue_batch_select_test'; \
         END IF; END $$",
    )
    .execute(&pool)
    .await
    .expect("remove prior batch selector role");
    sqlx::query(
        "CREATE ROLE bitmagnet_queue_batch_select_test LOGIN \
         PASSWORD 'queue-batch-select-test-password' \
         NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
    )
    .execute(&pool)
    .await
    .expect("create batch selector role");
    sqlx::query("GRANT USAGE ON SCHEMA public TO bitmagnet_queue_batch_select_test")
        .execute(&pool)
        .await
        .expect("grant public schema usage");

    let catalog = sqlx::query(
        "SELECT p.prosecdef, p.proconfig, \
                pg_catalog.pg_get_userbyid(p.proowner) AS owner, \
                NOT EXISTS (\
                  SELECT 1 \
                  FROM pg_catalog.aclexplode(\
                    COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))\
                  ) AS acl \
                  WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE'\
                ) AS public_execute_revoked \
         FROM pg_catalog.pg_proc AS p \
         WHERE p.oid = 'public.process_torrent_batch_select_page(\
           bytea,timestamp with time zone,text[],boolean,boolean,bigint\
         )'::regprocedure",
    )
    .fetch_one(&pool)
    .await
    .expect("inspect selector capability catalog contract");
    assert!(catalog.try_get::<bool, _>("prosecdef").unwrap());
    assert_eq!(
        catalog
            .try_get::<Option<Vec<String>>, _>("proconfig")
            .unwrap(),
        Some(vec!["search_path=pg_catalog, pg_temp".to_owned()])
    );
    assert!(catalog
        .try_get::<bool, _>("public_execute_revoked")
        .unwrap());
    assert_ne!(
        catalog.try_get::<String, _>("owner").unwrap(),
        "bitmagnet_queue_batch_select_test",
        "runtime ownership transfer is deferred to deployment automation"
    );

    let selector_options = PgConnectOptions::from_str(&database_url)
        .expect("parse PostgreSQL URL")
        .username("bitmagnet_queue_batch_select_test")
        .password("queue-batch-select-test-password");
    let selector_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(selector_options)
        .await
        .expect("connect minimally granted batch selector role");
    let public_call = sqlx::query(
        "SELECT * FROM public.process_torrent_batch_select_page(\
           decode(repeat('00', 20), 'hex')::bytea, \
           '2026-08-12T00:00:00Z'::timestamptz, ARRAY[]::text[], \
           false::boolean, false::boolean, 1::bigint\
         )",
    )
    .execute(&selector_pool)
    .await
    .expect_err("PUBLIC must not inherit selector execution");
    assert_database_code(public_call, "42501");
    let direct_select = sqlx::query("SELECT info_hash FROM public.torrents LIMIT 1")
        .execute(&selector_pool)
        .await
        .expect_err("selector role must not directly read source tables");
    assert_database_code(direct_select, "42501");
    let direct_content_select =
        sqlx::query("SELECT info_hash FROM public.torrent_contents LIMIT 1")
            .execute(&selector_pool)
            .await
            .expect_err("selector role must not directly read content filters");
    assert_database_code(direct_content_select, "42501");
    sqlx::query(
        "GRANT EXECUTE ON FUNCTION public.process_torrent_batch_select_page(\
           bytea, timestamptz, text[], boolean, boolean, bigint\
         ) TO bitmagnet_queue_batch_select_test",
    )
    .execute(&pool)
    .await
    .expect("grant reviewed selector capability");

    assert_eq!(fixture.input.cases.len(), fixture.expected.results.len());
    let store = QueueStore::new(selector_pool.clone());
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

    for statement in [
        "SELECT * FROM public.process_torrent_batch_select_page(\
           '\\x01'::bytea, clock_timestamp(), ARRAY[]::text[], false, false, 1\
         )",
        "SELECT * FROM public.process_torrent_batch_select_page(\
           decode(repeat('00', 20), 'hex'), NULL::timestamptz, \
           ARRAY[]::text[], false, false, 1\
         )",
        "SELECT * FROM public.process_torrent_batch_select_page(\
           decode(repeat('00', 20), 'hex'), clock_timestamp(), \
           ARRAY[NULL]::text[], false, false, 1\
         )",
        "SELECT * FROM public.process_torrent_batch_select_page(\
           decode(repeat('00', 20), 'hex'), clock_timestamp(), \
           ARRAY['not-a-content-type']::text[], false, false, 1\
         )",
        "SELECT * FROM public.process_torrent_batch_select_page(\
           decode(repeat('00', 20), 'hex'), clock_timestamp(), \
           ARRAY[['movie'],['tv_show']]::text[], false, false, 1\
         )",
        "SELECT * FROM public.process_torrent_batch_select_page(\
           decode(repeat('00', 20), 'hex'), clock_timestamp(), \
           ARRAY[]::text[], NULL::boolean, false, 1\
         )",
        "SELECT * FROM public.process_torrent_batch_select_page(\
           decode(repeat('00', 20), 'hex'), clock_timestamp(), \
           ARRAY[]::text[], false, false, 0\
         )",
    ] {
        let error = sqlx::query(statement)
            .execute(&selector_pool)
            .await
            .expect_err("invalid selector capability argument must fail closed");
        assert_database_code(error, "22023");
    }

    sqlx::query("TRUNCATE torrents CASCADE")
        .execute(&pool)
        .await
        .expect("clear selection fixtures");
    sqlx::query(
        "INSERT INTO torrents \
         (info_hash, name, size, private, created_at, updated_at) \
         VALUES ('\\x01'::bytea, 'malformed', 42, false, \
                 '2026-08-11T00:00:00Z'::timestamptz, \
                 '2026-08-11T00:00:00Z'::timestamptz)",
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

    reset(&pool).await;

    selector_pool.close().await;
    sqlx::query("DROP OWNED BY bitmagnet_queue_batch_select_test")
        .execute(&pool)
        .await
        .expect("drop batch selector role grants");
    sqlx::query("DROP ROLE bitmagnet_queue_batch_select_test")
        .execute(&pool)
        .await
        .expect("drop batch selector role");
    pool.close().await;
}
