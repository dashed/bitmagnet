//! Live-PostgreSQL differential parity for the Torznab search-query builder.
//!
//! Generate and leave the deterministic database rows in place with the Go
//! integration generator first, then run this ignored test against that same
//! database:
//!
//! ```text
//! POSTGRES_DSN=postgres://... go test -tags integration \
//!   ./internal/parity -run TestGenerateSearchQueryParityFixtures
//! BITMAGNET_POSTGRES_DSN=postgres://... cargo test \
//!   -p bitmagnet-search-query --test parity_pg -- --ignored
//! ```

use anyhow::Result;
use bitmagnet_diff::{
    driver::Driver,
    fixture::load_file,
    runner::{run, Options},
};
use bitmagnet_search_query::TorznabSearchParams;
use serde_json::Value;
use sqlx::PgPool;
use tokio::runtime::Handle;

struct SearchQueryDriver {
    pool: PgPool,
    handle: Handle,
}

impl Driver for SearchQueryDriver {
    fn subsystem(&self) -> &str {
        "searchquery"
    }

    fn run(&self, input: &Value) -> Result<Value> {
        let params: TorznabSearchParams = serde_json::from_value(input.clone())?;
        let query = bitmagnet_search_query::build_query(&params)?;
        let hashes = self.handle.block_on(query.fetch_info_hashes(&self.pool))?;

        Ok(serde_json::to_value(
            hashes.iter().map(ToString::to_string).collect::<Vec<_>>(),
        )?)
    }
}

#[test]
#[ignore = "requires Go-seeded live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
fn search_query_parity_via_live_postgres() {
    let dsn = match std::env::var("BITMAGNET_POSTGRES_DSN") {
        Ok(dsn) if !dsn.is_empty() => dsn,
        _ => {
            eprintln!("BITMAGNET_POSTGRES_DSN not set; skipping live search-query parity test");
            return;
        }
    };

    let fixtures = load_file(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/searchquery/torznab_search.jsonl"
    ))
    .expect("load search-query parity corpus");

    // Driver::run is synchronous. Keep its Handle::block_on calls on this
    // plain test thread, outside an entered Tokio runtime, to avoid nesting.
    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    let pool = runtime
        .block_on(PgPool::connect(&dsn))
        .expect("connect to fixture PostgreSQL");
    let driver = SearchQueryDriver {
        pool,
        handle: runtime.handle().clone(),
    };

    let report = run(&fixtures, &driver, Options::default());
    assert!(report.ran >= 15, "corpus too small: {report}");
    assert!(report.ok(), "search-query parity diverged:\n{report}");
}
