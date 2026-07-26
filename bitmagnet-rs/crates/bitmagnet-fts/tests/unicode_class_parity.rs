//! Behavioural parity for the Go-pinned word-char class, against results
//! captured from the production Go binary.
//!
//! `testdata/parity/unicode/go-oracle.jsonl` is the output of
//! `fts.AppQueryToTsquery` / `fts.TokenizeFlat` (plus the classifier/release
//! parsers other crates check) over 348 probe strings. The probes are built
//! from the code points where Rust's Unicode predicates are wider than Go's,
//! wedged between operands where the difference actually changes behaviour:
//! a wider word-char class merges two operands into one run, which flips the
//! tsquery operator from `&` to `<->`. `<->` is strictly narrower, so the old
//! `is_alphanumeric()` implementation silently returned a SUBSET of Go's
//! results — `Test²Case` produced `test <-> case` instead of `test & case`.

use std::collections::BTreeMap;

fn oracle_path() -> String {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/unicode/go-oracle.jsonl"
    )
    .to_string()
}

fn load() -> Vec<BTreeMap<String, serde_json::Value>> {
    let raw = std::fs::read_to_string(oracle_path()).expect("read go-oracle.jsonl");
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid oracle json"))
        .collect()
}

#[test]
fn app_query_to_tsquery_matches_go_on_every_probe() {
    let cases = load();
    assert!(cases.len() > 300, "oracle looks truncated");

    let mut failures = Vec::new();
    for case in &cases {
        let input = case["input"].as_str().expect("input");
        let want = case["tsquery"].as_str().expect("tsquery");
        let got = bitmagnet_fts::app_query_to_tsquery(input);
        if got != want {
            failures.push(format!("{input:?}: want {want:?}, got {got:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} probes diverge from Go:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

#[test]
fn tokenize_flat_matches_go_on_every_probe() {
    let mut failures = Vec::new();
    for case in load() {
        let input = case["input"].as_str().expect("input").to_string();
        let want: Vec<String> = case["lexemes"]
            .as_array()
            .expect("lexemes")
            .iter()
            .map(|v| v.as_str().expect("lexeme").to_string())
            .collect();
        let got = bitmagnet_fts::tokenize_flat(&input);
        if got != want {
            failures.push(format!("{input:?}: want {want:?}, got {got:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The four cases the audit named, spelled out so a regression is legible
/// without decoding the fixture.
#[test]
fn named_audit_cases_produce_the_conjunction_operator() {
    for (input, want) in [
        ("Test²Case", "test & case"),
        ("8½Women", "8 & women"),
        ("Alien³Resurrection", "alien & resurrection"),
        // Transliterated by go-unidecode; the load-bearing part is the `&`.
        ("第①話", "Di & Hua"),
    ] {
        assert_eq!(
            bitmagnet_fts::app_query_to_tsquery(input),
            want,
            "input = {input:?}"
        );
    }
}

/// Controls: inputs with no divergent character must be untouched by the fix.
#[test]
fn controls_still_pass() {
    for (input, want) in [
        // Latin-1 letter: one word run, transliterated.
        ("AlphaéBeta", "alphaebeta"),
        // CJK is a word char in BOTH engines, so it stays one run — and the
        // `<->` here is CORRECT, which is exactly why the operator flip on the
        // divergent characters was so easy to overlook.
        ("Alpha中Beta", "alpha <-> Zhong <-> beta"),
        // Arabic-Indic digit: category Nd, so Go's IsDigit accepts it too.
        ("1٣2", "132"),
        ("The Movie 2019", "the & movie & 2019"),
    ] {
        assert_eq!(
            bitmagnet_fts::app_query_to_tsquery(input),
            want,
            "input = {input:?}"
        );
    }
}
