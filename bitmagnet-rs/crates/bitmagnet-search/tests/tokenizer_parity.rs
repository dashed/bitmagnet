//! Ground-truth parity test for the [`tokenize_flat`] port.
//!
//! Every case in `fixtures/tokenizer_fixtures.json` was produced by running the
//! *real* Go `internal/database/fts.TokenizeFlat` over an adversarial corpus
//! (see `fixtures/README.md`). This test asserts the Rust port reproduces each
//! one exactly. Combined with the by-construction parity of the embedded tables
//! (also generated from Go), this is what guarantees the shadow-mode index
//! tokenizes identically to production.

use bitmagnet_search::tokenizer::tokenize_flat;

const FIXTURES_JSON: &str = include_str!("fixtures/tokenizer_fixtures.json");

#[test]
fn matches_go_tokenizeflat_for_every_fixture() {
    let cases: serde_json::Value =
        serde_json::from_str(FIXTURES_JSON).expect("fixtures JSON should deserialize");
    let cases = cases.as_array().expect("fixtures are a JSON array");
    assert!(
        cases.len() > 1000,
        "expected a large corpus, found only {}",
        cases.len()
    );

    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let input = case["input"].as_str().expect("`input` is a string");
        let expected: Vec<&str> = case["tokens"]
            .as_array()
            .expect("`tokens` is an array")
            .iter()
            .map(|t| t.as_str().expect("each token is a string"))
            .collect();

        let got = tokenize_flat(input);
        if got != expected {
            failures.push(format!(
                "input {:?} (code points {:04X?}): expected {:?}, got {:?}",
                input,
                input.chars().map(|c| c as u32).collect::<Vec<_>>(),
                expected,
                got,
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} fixtures diverged from Go:\n{}",
        failures.len(),
        cases.len(),
        failures
            .iter()
            .take(40)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n"),
    );
}
