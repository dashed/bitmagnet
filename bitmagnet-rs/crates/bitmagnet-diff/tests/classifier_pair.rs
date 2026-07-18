//! Phase-3 Lane C: the classifier parity pair.
//!
//! Wires the flags-off classifier corpus
//! (`testdata/parity/classifier/corpus.golden.jsonl`, 330 fixtures) into the
//! shared differential harness, driving the real Lane C `Classifier` over each
//! input and comparing to the frozen golden.
//!
//! The corpus is the flags-off oracle (local_search_enabled / apis_enabled /
//! tmdb_enabled all false); see `internal/classifier/corpus_test.go` and
//! `docs/dev/rust-rewrite/phase3-contracts.md §2`.
//!
//! ✅ The Rust classifier matches all 330 golden fixtures exactly (the CEL
//! engine + content-type classification + the date parser + Lane R's
//! `parse_video_content` title/year/episode/language/video parsers).

use anyhow::Result;
use bitmagnet_classifier::{Classifier, ClassifierInput};
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

/// Drives the real Lane C classifier over the flags-off corpus.
struct ClassifierDriver {
    classifier: Classifier,
}

impl ClassifierDriver {
    fn new() -> Self {
        ClassifierDriver {
            classifier: Classifier::from_core().expect("compile classifier.core.yml"),
        }
    }
}

impl Driver for ClassifierDriver {
    fn subsystem(&self) -> &str {
        CLASSIFIER_SUBSYSTEM
    }

    fn run(&self, input: &Value) -> Result<Value> {
        let parsed: ClassifierInput = serde_json::from_value(input.clone())?;
        Ok(self
            .classifier
            .run("default", &Classifier::flags_off(), &parsed))
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
fn classifier_matches_the_full_corpus() {
    // Full flags-off parity gate: the Rust classifier must reproduce every one
    // of the 330 golden fixtures exactly (contract §2.5).
    let fixtures = load_corpus();
    let report = run(&fixtures, &ClassifierDriver::new(), Options::default());

    assert_eq!(
        report.ran, EXPECTED_FIXTURES,
        "harness did not run every classifier fixture: {report}"
    );
    assert!(
        report.ok(),
        "classifier diverged from the flags-off corpus golden: {report}"
    );
    assert_eq!(
        report.matched, EXPECTED_FIXTURES,
        "not every fixture matched"
    );
}
