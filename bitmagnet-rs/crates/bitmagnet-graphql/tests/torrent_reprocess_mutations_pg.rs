//! Disposable-PostgreSQL differential proof for `torrent.reprocess`.
//!
//! `TestTorrentReprocessParityGo` runs every corpus case through the production
//! gqlgen schema and the real Go processor and commits the response plus a
//! normalized database image as `expected.jsonl`. This test replays the same
//! seed and requests through the Rust schema with a distinct least-privilege
//! writer and must reproduce that oracle column for column. Cases marked
//! `rustRefuses` need content search, TMDB, or Go's attach-by-ID hint path;
//! Rust must refuse them before any write.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use async_graphql::{EmptySubscription, Request, Variables};
use bitmagnet_db::PgPool;
use bitmagnet_graphql::{
    admit_pg, admit_torrent_reprocess_writer_authority, Mutation, Query,
    TorrentReprocessMutationsRuntimeData,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;

const EXPECTED_GOOSE_VERSION: i64 = 36;
const DATABASE: &str = "bitmagnet_graphql_torrent_reprocess_test";
/// Every fail-closed refusal: unsupported flags, or a plan the kernel rejects
/// before writing (for example an explicit hint naming a content ID).
const REFUSAL_PREFIXES: [&str; 2] = [
    "torrent reprocess is not supported by this Rust runtime: ",
    "torrent reprocess plan failed: ",
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReprocessCase {
    id: String,
    query: String,
    #[serde(default)]
    variables: Option<Value>,
    #[serde(default)]
    repeat: usize,
    #[serde(default)]
    rust_refuses: bool,
}

#[derive(Debug, Deserialize)]
struct ReprocessOutcome {
    id: String,
    responses: Vec<Value>,
    state: Value,
}

#[tokio::test]
#[ignore = "requires the Go-verified disposable PostgreSQL torrent-reprocess database"]
async fn real_pg_torrent_reprocess_matches_go_with_isolated_writer() {
    let Some(admin_dsn) = test_dsn("BITMAGNET_GRAPHQL_TORRENT_REPROCESS_TEST_ADMIN_DATABASE_URL")
    else {
        eprintln!("torrent reprocess parity admin DSN is not set; skipping");
        return;
    };
    let writer_dsn = test_dsn("BITMAGNET_GRAPHQL_TORRENT_REPROCESS_TEST_DATABASE_URL")
        .expect("torrent reprocess parity writer DSN is required with the admin DSN");

    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_dsn)
        .await
        .expect("connect torrent reprocess parity admin");
    let writer_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&writer_dsn)
        .await
        .expect("connect isolated torrent reprocess writer");
    let admin_identity = postgres_identity(&admin_pool).await;
    assert_eq!(
        admin_identity.0, DATABASE,
        "ignored torrent reprocess gate refuses a database without its exact disposable-name sentinel"
    );
    let writer_identity = postgres_identity(&writer_pool).await;
    assert_eq!(
        (&writer_identity.0, &writer_identity.1),
        (&admin_identity.0, &admin_identity.1),
        "admin and writer must target the same PostgreSQL system and database"
    );
    assert_ne!(writer_identity.2, admin_identity.2);
    admit_torrent_reprocess_writer_authority(&writer_pool)
        .await
        .expect("the CI writer role has exactly the admitted authority");
    let goose_head = admit_pg(&writer_pool, EXPECTED_GOOSE_VERSION)
        .await
        .expect("the isolated writer can admit the exact Goose head");
    assert_eq!(goose_head.version, EXPECTED_GOOSE_VERSION);

    let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(
            TorrentReprocessMutationsRuntimeData::pg(writer_pool.clone())
                .expect("compile the core classifier"),
        )
        .finish();
    let seed = read_corpus_file("seed.sql");
    let dump = read_corpus_file("dump.sql");
    let mut expected = load_outcomes()
        .into_iter()
        .map(|outcome| (outcome.id.clone(), outcome))
        .collect::<BTreeMap<_, _>>();

    let mut failures = Vec::new();
    let mut checked = 0;
    for oracle in load_cases() {
        // Committed corpus files, not caller input.
        sqlx::raw_sql(sqlx::AssertSqlSafe(seed.clone()))
            .execute(&admin_pool)
            .await
            .unwrap_or_else(|error| panic!("{}: seed: {error}", oracle.id));
        let seeded = dump_state(&admin_pool, &dump).await;

        let mut responses = Vec::new();
        for _ in 0..oracle.repeat.max(1) {
            let mut request = Request::new(oracle.query.as_str());
            if let Some(variables) = &oracle.variables {
                request = request.variables(Variables::from_json(variables.clone()));
            }
            responses.push(project_response(
                serde_json::to_value(schema.execute(request).await).expect("response JSON"),
            ));
        }
        let state = dump_state(&admin_pool, &dump).await;

        if oracle.rust_refuses {
            assert!(
                !expected.contains_key(&oracle.id),
                "{}: refused case has a Go oracle",
                oracle.id
            );
            for response in &responses {
                let message = response["errors"][0]["message"]
                    .as_str()
                    .unwrap_or_default();
                if !REFUSAL_PREFIXES
                    .iter()
                    .any(|prefix| message.starts_with(prefix))
                    || response["data"] != json!({ "torrent": { "reprocess": null } })
                {
                    failures.push(format!("{}: expected refusal, got {response}", oracle.id));
                }
            }
            diff_json(
                &format!("{}: state", oracle.id),
                &seeded,
                &state,
                &mut failures,
            );
            checked += 1;
            continue;
        }

        let go = expected
            .remove(&oracle.id)
            .unwrap_or_else(|| panic!("{}: no Go oracle outcome", oracle.id));
        assert_eq!(go.responses.len(), responses.len(), "{}", oracle.id);
        for (index, (go_response, rust_response)) in go.responses.iter().zip(&responses).enumerate()
        {
            diff_json(
                &format!("{}: response[{index}]", oracle.id),
                go_response,
                rust_response,
                &mut failures,
            );
        }
        diff_json(
            &format!("{}: state", oracle.id),
            &go.state,
            &state,
            &mut failures,
        );
        checked += 1;
    }
    assert!(
        expected.is_empty(),
        "Go oracle outcomes without a corpus case: {:?}",
        expected.keys().collect::<Vec<_>>()
    );
    assert!(
        failures.is_empty(),
        "{} parity failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    eprintln!("torrent reprocess parity: {checked} cases match");
}

/// The core workflow's first rule deletes any torrent whose name or file paths
/// match `keywords.banned`, whatever the flags. Go blocks and deletes it inside
/// `Process`; this runtime has no blocking-store authority, so a plan that
/// both deletes and persists must be refused before its first write — the
/// partial-failure retry publication included.
#[tokio::test]
#[ignore = "requires the Go-verified disposable PostgreSQL torrent-reprocess database"]
async fn real_pg_torrent_reprocess_refuses_banned_deletions_before_any_write() {
    const BANNED: &str = "b00000000000000000000000000000000000000b";
    let Some(admin_dsn) = test_dsn("BITMAGNET_GRAPHQL_TORRENT_REPROCESS_TEST_ADMIN_DATABASE_URL")
    else {
        eprintln!("torrent reprocess parity admin DSN is not set; skipping");
        return;
    };
    let writer_dsn = test_dsn("BITMAGNET_GRAPHQL_TORRENT_REPROCESS_TEST_DATABASE_URL")
        .expect("torrent reprocess parity writer DSN is required with the admin DSN");
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_dsn)
        .await
        .expect("connect torrent reprocess parity admin");
    assert_eq!(postgres_identity(&admin_pool).await.0, DATABASE);
    let writer_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&writer_dsn)
        .await
        .expect("connect isolated torrent reprocess writer");
    let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(
            TorrentReprocessMutationsRuntimeData::pg(writer_pool)
                .expect("compile the core classifier"),
        )
        .finish();

    sqlx::raw_sql(sqlx::AssertSqlSafe(read_corpus_file("seed.sql")))
        .execute(&admin_pool)
        .await
        .expect("seed");
    sqlx::query(
        "INSERT INTO torrents (info_hash, name, size, private, files_status, created_at, \
         updated_at) VALUES (decode($1, 'hex'), 'pthc sample clip.avi', 1000, false, \
         'single', '2015-10-01 00:00:00+00', '2015-10-02 00:00:00+00')",
    )
    .bind(BANNED)
    .execute(&admin_pool)
    .await
    .expect("seed a banned-keyword torrent");
    let dump = read_corpus_file("dump.sql");
    let seeded = dump_state(&admin_pool, &dump).await;

    // A classified torrent, the banned one, and a missing hash: Go publishes
    // a retry for the missing hash, blocks and deletes the banned torrent,
    // and persists the classified one.
    let request = Request::new(
        "mutation TorrentReprocess($input: TorrentReprocessInput!) \
         { torrent { reprocess(input: $input) } }",
    )
    .variables(Variables::from_json(json!({
        "input": {
            "infoHashes": [
                "1000000000000000000000000000000000000001",
                BANNED,
                "e000000000000000000000000000000000000001",
            ],
            "apisDisabled": true,
            "localSearchDisabled": true,
        }
    })));
    let response = project_response(
        serde_json::to_value(schema.execute(request).await).expect("response JSON"),
    );
    let message = response["errors"][0]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(
        message.starts_with(REFUSAL_PREFIXES[0]),
        "expected a refusal, got {response}"
    );
    assert_eq!(
        response["data"],
        json!({ "torrent": { "reprocess": null } })
    );
    let mut failures = Vec::new();
    diff_json(
        "state",
        &seeded,
        &dump_state(&admin_pool, &dump).await,
        &mut failures,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Keep only what the Go oracle records: `data` and the whole error objects.
fn project_response(response: Value) -> Value {
    json!({
        "data": response["data"],
        "errors": response.get("errors").cloned().unwrap_or_else(|| json!([])),
    })
}

/// Record every leaf difference, keyed by table row and column.
fn diff_json(path: &str, expected: &Value, actual: &Value, failures: &mut Vec<String>) {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            for (key, value) in expected {
                let child = format!("{path}.{key}");
                match actual.get(key) {
                    Some(actual_value) => diff_json(&child, value, actual_value, failures),
                    None => failures.push(format!("{child}: missing in Rust (Go {value})")),
                }
            }
            for (key, value) in actual {
                if !expected.contains_key(key) {
                    failures.push(format!("{path}.{key}: extra in Rust ({value})"));
                }
            }
        }
        (Value::Array(expected), Value::Array(actual)) => {
            if expected.len() != actual.len() {
                failures.push(format!(
                    "{path}: Go has {} entries, Rust has {}\n  Go:   {}\n  Rust: {}",
                    expected.len(),
                    actual.len(),
                    Value::Array(expected.clone()),
                    Value::Array(actual.clone())
                ));
                return;
            }
            for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
                diff_json(&format!("{path}[{index}]"), expected, actual, failures);
            }
        }
        _ if expected == actual => {}
        _ => failures.push(format!("{path}: Go {expected} != Rust {actual}")),
    }
}

async fn dump_state(pool: &PgPool, dump: &str) -> Value {
    let state = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(dump.to_owned()))
        .fetch_one(pool)
        .await
        .expect("dump normalized state");
    serde_json::from_str(&state).expect("decode normalized state")
}

async fn postgres_identity(pool: &PgPool) -> (String, String, String) {
    sqlx::query_as::<_, (String, String, String)>(
        "SELECT current_database()::text, system_identifier::text, current_user::text \
         FROM pg_control_system()",
    )
    .fetch_one(pool)
    .await
    .expect("read PostgreSQL identity")
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/parity/graphql-torrent-reprocess")
}

fn read_corpus_file(name: &str) -> String {
    let path = corpus_dir().join(name);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}"))
}

fn load_cases() -> Vec<ReprocessCase> {
    let cases = read_corpus_file("cases.jsonl")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode torrent reprocess case"))
        .collect::<Vec<_>>();
    assert!(!cases.is_empty(), "torrent reprocess corpus is empty");
    cases
}

fn load_outcomes() -> Vec<ReprocessOutcome> {
    read_corpus_file("expected.jsonl")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode torrent reprocess Go outcome"))
        .collect()
}

fn test_dsn(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|dsn| !dsn.is_empty())
}
