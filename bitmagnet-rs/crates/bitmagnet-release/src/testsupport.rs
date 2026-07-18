//! Test-only helpers: load the Go regex oracle (`testdata/parity/release/
//! patterns.jsonl`, dumped from the Go `keywords`/`model` packages) and apply
//! the single documented ASCII adaptation that separates Go's `regexp` dialect
//! from the Rust `regex` crate.

use std::collections::HashMap;

/// Translate a raw Go `rex` pattern into the Rust-`regex` equivalent by making
/// the digit class ASCII (Go `\d` == `[0-9]`; Rust `\d` == Unicode). `\p{L}`
/// is identical in both engines and is left untouched. The three ordered
/// substitutions cover exactly where the emitter diverges:
///
/// * `[\p{L}\d]`  (word char)      -> `[\p{L}0-9]`
/// * `[^\p{L}\d]` (non-word char)  -> `[^\p{L}0-9]`
/// * `\s`  (whitespace, Go ASCII)  -> `[\t\n\f\r ]`
/// * remaining bare `\d` (from `#` / `\d{1,2}`) -> `[0-9]`
///
/// Go's `regexp` `\s` is `[\t\n\f\r ]` (RE2, no vertical tab); the Rust crate's
/// `\s` is Unicode. The explicit ASCII class matches Go exactly (and avoids the
/// `\v` that `(?-u:\s)` would add).
pub(crate) fn adapt_go_pattern(go: &str) -> String {
    go.replace(r"[\p{L}\d]", "[\\p{L}0-9]")
        .replace(r"[^\p{L}\d]", "[^\\p{L}0-9]")
        .replace(r"\s", r"[\t\n\f\r ]")
        .replace(r"\d", "[0-9]")
}

fn patterns_path() -> String {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/release/patterns.jsonl"
    )
    .to_string()
}

/// Map `id -> raw Go pattern` from the oracle file.
pub(crate) fn load_go_patterns() -> HashMap<String, String> {
    let raw = std::fs::read_to_string(patterns_path()).expect("read patterns.jsonl");
    let mut out = HashMap::new();
    for line in raw.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid pattern json");
        let id = v["id"].as_str().expect("id").to_string();
        let pattern = v["go_pattern"].as_str().expect("go_pattern").to_string();
        out.insert(id, pattern);
    }
    out
}

/// Map `id -> (keywords, raw Go pattern)` for the DSL cases (which carry an
/// explicit `keywords` array).
pub(crate) fn load_go_dsl_cases() -> Vec<(String, Vec<String>, String)> {
    let raw = std::fs::read_to_string(patterns_path()).expect("read patterns.jsonl");
    let mut out = Vec::new();
    for line in raw.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid pattern json");
        let kws = match v.get("keywords").and_then(|k| k.as_array()) {
            Some(arr) => arr
                .iter()
                .map(|k| k.as_str().expect("keyword str").to_string())
                .collect::<Vec<_>>(),
            None => continue,
        };
        let id = v["id"].as_str().expect("id").to_string();
        let pattern = v["go_pattern"].as_str().expect("go_pattern").to_string();
        out.push((id, kws, pattern));
    }
    out
}
