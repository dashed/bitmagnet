//! Phase-3 base-prep: harness plumbing for the classifier parity pair.
//!
//! The real Rust classifier is Lane C. This stub wires the flags-off classifier
//! corpus (`testdata/parity/classifier/corpus.golden.jsonl`, 330 fixtures) into
//! the shared differential harness so that fixture loading + the `Driver`/`run`
//! plumbing compile and execute, and every fixture round-trips the canonical
//! normalizer. Lane C only has to replace `ClassifierDriver::run`'s body with
//! the real port and flip the pending-implementation assertion.
//!
//! The corpus is the flags-off oracle (local_search_enabled / apis_enabled /
//! tmdb_enabled all false); see `internal/classifier/corpus_test.go` and
//! `docs/dev/rust-rewrite/phase3-contracts.md §2`.

use anyhow::{bail, Result};
use bitmagnet_diff::{
    canonical,
    driver::Driver,
    fixture::load_file,
    runner::{run, Options},
    Fixture,
};
use serde_json::Value;

const CLASSIFIER_SUBSYSTEM: &str = "classifier";
const EXPECTED_FIXTURES: usize = 330;

fn corpus_path() -> String {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/classifier/corpus.golden.jsonl"
    )
    .to_string()
}

/// Placeholder for the Lane C Rust classifier. Until that lands, every fixture
/// deliberately errors — the harness plumbing is exercised, but no parity claim
/// is made.
struct ClassifierDriver;

impl Driver for ClassifierDriver {
    fn subsystem(&self) -> &str {
        CLASSIFIER_SUBSYSTEM
    }

    fn run(&self, _input: &Value) -> Result<Value> {
        bail!("bitmagnet classifier Rust port not yet implemented (Phase-3 Lane C)");
    }
}

fn load_corpus() -> Vec<Fixture> {
    load_file(corpus_path()).expect("load classifier corpus")
}

#[test]
fn classifier_corpus_loads_with_expected_shape() {
    let fixtures = load_corpus();
    assert_eq!(
        fixtures.len(),
        EXPECTED_FIXTURES,
        "classifier corpus size drifted"
    );

    for fixture in &fixtures {
        assert_eq!(
            fixture.subsystem, CLASSIFIER_SUBSYSTEM,
            "fixture {} has unexpected subsystem",
            fixture.id
        );
        // Every expected record is a classifierExpected object with an outcome
        // and a contentType key (the frozen output schema, §2).
        let expected = fixture
            .expected
            .as_object()
            .unwrap_or_else(|| panic!("fixture {} expected is not an object", fixture.id));
        assert!(
            expected.contains_key("outcome"),
            "fixture {} missing outcome",
            fixture.id
        );
        assert!(
            expected.contains_key("contentType"),
            "fixture {} missing contentType",
            fixture.id
        );
    }
}

#[test]
fn classifier_corpus_round_trips_the_normalizer() {
    let fixtures = load_corpus();
    for fixture in &fixtures {
        let once = canonical(&fixture.expected);
        let twice = canonical(&once);
        assert_eq!(
            once, twice,
            "canonical normalization is not idempotent for fixture {}",
            fixture.id
        );
    }
}

#[test]
fn classifier_harness_runs_every_fixture_pending_lane_c() {
    let fixtures = load_corpus();
    let report = run(&fixtures, &ClassifierDriver, Options::default());

    // The plumbing must see all 330 classifier fixtures.
    assert_eq!(
        report.ran, EXPECTED_FIXTURES,
        "harness did not run every classifier fixture: {report}"
    );
    // Until Lane C lands, the stub errors on every fixture: nothing matched,
    // nothing mismatched, all errored. When the real driver is wired, replace
    // this with `assert!(report.ok(), ...)`.
    assert_eq!(report.matched, 0, "unexpected matches from the stub driver");
    assert_eq!(
        report.mismatched, 0,
        "unexpected mismatches from the stub driver"
    );
    assert_eq!(
        report.errored, EXPECTED_FIXTURES,
        "stub driver should error on every fixture until Lane C: {report}"
    );
}
