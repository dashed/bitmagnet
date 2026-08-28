//! Behavioural parity for the Go-pinned word-char class, against results
//! captured from the production Go binary.
//!
//! `testdata/parity/unicode/go-oracle.jsonl` is the output of
//! `fts.AppQueryToTsquery` / `fts.TokenizeFlat` (plus the classifier/release
//! parsers other crates check) over 728 probe strings. The probes are built
//! from the code points where Rust's Unicode predicates are wider than Go's,
//! wedged between operands where the difference actually changes behaviour,
//! in BOTH query shapes:
//!
//! * **Unquoted** (the app-query path): a wider word-char class merges two
//!   operands into one run, which flips the tsquery operator from `&` to
//!   `<->`. `<->` is strictly narrower, so the old `is_alphanumeric()`
//!   implementation silently returned a SUBSET of Go's results — `Test²Case`
//!   produced `test <-> case` instead of `test & case`.
//! * **Quoted** (the classifier path — `ContentBySearch` issues
//!   `fmt.Sprintf("\"%s\"", baseTitle)`): the phrase lexes as ONE quoted token
//!   before any word run, so the operator is Go-correctly `<->` and what must
//!   match is the LEXEME BOUNDARIES inside the phrase — `"Test²Case"` must be
//!   `test <-> case`, exactly what Go produces.

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
        // Go marshals a nil slice as `null`; that is the empty lexeme list
        // (e.g. `"½"` tokenizes to nothing).
        let want: Vec<String> = case["lexemes"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|v| v.as_str().expect("lexeme").to_string())
                    .collect()
            })
            .unwrap_or_default();
        let got = bitmagnet_fts::tokenize_flat(&input);
        if got != want {
            failures.push(format!("{input:?}: want {want:?}, got {got:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The classifier-path shape, spelled out so a regression is legible without
/// decoding the fixture. Quoted input is ONE operand, so `<->` (phrase
/// adjacency) is the CORRECT operator here — what Go checks is that the lexeme
/// boundaries inside the phrase fall where Go's word-char class puts them.
/// The same string unquoted must produce `&` (see
/// [`named_audit_cases_produce_the_conjunction_operator`]); that contrast is
/// the two-manifestation proof.
#[test]
fn quoted_classifier_path_preserves_go_lexeme_boundaries() {
    for (input, want) in [
        (r#""Test²Case""#, "test <-> case"),
        (r#""8½Women""#, "8 <-> women"),
        (r#""Alien³Resurrection""#, "alien <-> resurrection"),
        (r#""第①話""#, "Di <-> Hua"),
        // A quoted operand next to unquoted ones: the quoted token keeps its
        // internal `<->` while the separate operands join with `&`.
        (
            r#"Alien "8½Women" Resurrection"#,
            "alien & 8 <-> women & resurrection",
        ),
        // A phrase whose content tokenizes to nothing contributes no operand.
        (r#""½""#, ""),
    ] {
        assert_eq!(
            bitmagnet_fts::app_query_to_tsquery(input),
            want,
            "input = {input:?}"
        );
    }
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
