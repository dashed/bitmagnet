//! Behavioural parity for this crate's copy of the app-query lexer.
//!
//! `bitmagnet-search` carries its own `app_query_to_tsquery` (Tantivy-side)
//! alongside `bitmagnet-fts`'s (Postgres-side). Both had the same
//! `is_alphanumeric()` bug and both are fixed; this asserts the Tantivy-side
//! copy against the same Go oracle so the two can never drift apart.
//! See `bitmagnet-fts/tests/unicode_class_parity.rs` for the full rationale.

use std::collections::BTreeMap;

fn load() -> Vec<BTreeMap<String, serde_json::Value>> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/unicode/go-oracle.jsonl"
    );
    let raw = std::fs::read_to_string(path).expect("read go-oracle.jsonl");
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
        let got = bitmagnet_search::query::app_query_to_tsquery(input);
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
        let got = bitmagnet_search::tokenizer::tokenize_flat(&input);
        if got != want {
            failures.push(format!("{input:?}: want {want:?}, got {got:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The classifier-path shape: quoted input is ONE operand, so `<->` is the
/// CORRECT operator — what must match Go is the lexeme boundaries inside the
/// phrase. See `bitmagnet-fts/tests/unicode_class_parity.rs` for the full
/// two-manifestation rationale.
#[test]
fn quoted_classifier_path_preserves_go_lexeme_boundaries() {
    for (input, want) in [
        (r#""Test²Case""#, "test <-> case"),
        (r#""8½Women""#, "8 <-> women"),
        (r#""Alien³Resurrection""#, "alien <-> resurrection"),
        (r#""第①話""#, "Di <-> Hua"),
        (
            r#"Alien "8½Women" Resurrection"#,
            "alien & 8 <-> women & resurrection",
        ),
        (r#""½""#, ""),
    ] {
        assert_eq!(
            bitmagnet_search::query::app_query_to_tsquery(input),
            want,
            "input = {input:?}"
        );
    }
}

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
            bitmagnet_search::query::app_query_to_tsquery(input),
            want,
            "input = {input:?}"
        );
    }
}
