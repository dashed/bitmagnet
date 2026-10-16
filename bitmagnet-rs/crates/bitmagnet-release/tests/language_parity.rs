//! Behavioral parity for language detection: replay frozen fixtures and assert
//! `infer_languages(name)` matches Go's `InferLanguages(name).Slice()` (alpha2,
//! natsort-by-name order). Oracle: `testdata/parity/release/languages.jsonl`.

use bitmagnet_release::infer_languages;
use serde::Deserialize;

#[derive(Deserialize)]
struct LangFixture {
    id: String,
    name: String,
    languages: Vec<String>,
}

#[test]
fn languages_match_go() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/release/languages.jsonl"
    );
    let raw = std::fs::read_to_string(path).expect("read languages.jsonl");
    let fixtures: Vec<LangFixture> = raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid fixture json"))
        .collect();
    assert!(fixtures.len() >= 25, "expected the full language corpus");

    for f in &fixtures {
        assert_eq!(
            infer_languages(&f.name),
            f.languages,
            "language mismatch for {}",
            f.id
        );
    }
}
