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
//! 🚧 Milestone 1 status: the CEL engine + content-type classification + the
//! date parser are landed, so the content-type-only and `deleted` fixtures pass.
//! The movie/tv fixtures exercise `parse_video_content`, whose title/year
//! extraction (plus `InferLanguages` / `InferVideo3D` / `InferVideoModifier`) is
//! Lane-R-pending — those fixtures mismatch until R lands the parsers. See the
//! `classifier_engine_never_errors` gate + the printed match summary.

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
#[ignore = "diagnostic: prints which fields drive each mismatch (Lane-R attribution)"]
fn classifier_mismatch_field_attribution() {
    let classifier = Classifier::from_core().expect("compile");
    let fixtures = load_corpus();
    let flags = Classifier::flags_off();
    let mut field_tally: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    let mut mismatches = 0usize;

    for fixture in &fixtures {
        let parsed: ClassifierInput = serde_json::from_value(fixture.input.clone()).unwrap();
        let got = canonical(&classifier.run("default", &flags, &parsed));
        let want = canonical(&fixture.expected);
        if got == want {
            continue;
        }
        mismatches += 1;
        let (go, gw) = (got.as_object().unwrap(), want.as_object().unwrap());
        let mut differing: Vec<String> = Vec::new();
        for key in gw.keys() {
            if go.get(key) != gw.get(key) {
                differing.push(key.clone());
            }
        }
        *field_tally.entry(differing.join("+")).or_default() += 1;
    }

    eprintln!("mismatches={mismatches}; differing-field-sets:");
    let mut pairs: Vec<_> = field_tally.into_iter().collect();
    pairs.sort_by_key(|p| std::cmp::Reverse(p.1));
    for (fields, n) in pairs {
        eprintln!("  {n:>4}  {fields}");
    }
}

#[test]
fn classifier_engine_never_errors() {
    // The engine must run every fixture to a terminal outcome without the
    // driver returning `Err` — an errored fixture means a CEL/compile bug, not
    // a parity mismatch. Mismatches (Lane-R-pending video path) are allowed and
    // reported separately.
    let fixtures = load_corpus();
    let report = run(&fixtures, &ClassifierDriver::new(), Options::default());

    eprintln!(
        "Lane C classifier corpus: ran={} matched={} mismatched={} errored={} ({:.1}% match)",
        report.ran,
        report.matched,
        report.mismatched,
        report.errored,
        100.0 * report.matched as f64 / report.ran.max(1) as f64,
    );
    for diff in report.diffs.iter().take(8) {
        eprintln!("  mismatch/err id={}", diff.id);
    }

    assert_eq!(
        report.ran, EXPECTED_FIXTURES,
        "harness did not run every classifier fixture: {report}"
    );
    assert_eq!(
        report.errored, 0,
        "classifier driver errored on some fixtures (engine bug, not a parity gap): {report}"
    );
}
