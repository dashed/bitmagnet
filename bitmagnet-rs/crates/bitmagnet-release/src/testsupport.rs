//! Test-only helpers: load the Go regex oracle (`testdata/parity/release/
//! patterns.jsonl`, dumped from the Go `keywords`/`model` packages) and apply
//! the single documented ASCII adaptation that separates Go's `regexp` dialect
//! from the Rust `regex` crate.

use std::collections::HashMap;

use crate::goclass;

/// Translate a raw Go `rex` pattern into the Rust-`regex` equivalent. Every
/// shorthand Go's RE2 and the Rust `regex` crate disagree about is replaced by
/// an explicit class that means what Go means:
///
/// * `[\p{L}\d]`  (word char)      -> `[<letters>0-9]`
/// * `[^\p{L}\d]` (non-word char)  -> `[^<letters>0-9]`
/// * `[^\w]` (non-ASCII-word char) -> `[^0-9A-Za-z_]`
/// * `\w`   (ASCII word char)      -> `[0-9A-Za-z_]`
/// * `\s`   (whitespace, Go ASCII) -> `[\t\n\f\r ]`
/// * remaining bare `\d` (from `#` / `\d{1,2}`) -> `[0-9]`
/// * remaining bare `\p{L}`        -> `<letters>`
///
/// where `<letters>` is [`goclass::LETTER_CLASS_BODY`].
///
/// Go's `regexp` `\w`/`\d`/`\s` are ASCII; the Rust crate's are Unicode. The
/// explicit ASCII classes match Go exactly (Go `\s` is `[\t\n\f\r ]` — RE2, no
/// vertical tab).
///
/// 🚨 `\p{L}` is NOT identical in the two engines, and pretending it was is why
/// the divergence survived review: this function used to pass `\p{L}` through
/// untouched, so both sides of every `assert_eq!(PATTERN, adapt_go_pattern(go))`
/// contained the same literal and the assert passed while the compiled
/// behaviour differed by 4,924 code points. Substituting the pinned class here
/// is what makes those byte-equality tests mean something — the Rust side is
/// pinned too, so the comparison is between two fully-expanded patterns.
///
/// `[[:upper:]]` is identical in both engines and left untouched. Bracketed
/// composite forms are replaced before their bare shorthands so no nested
/// classes form.
pub(crate) fn adapt_go_pattern(go: &str) -> String {
    let letters = goclass::LETTER_CLASS_BODY;
    go.replace(r"[\p{L}\d]", &format!("[{letters}0-9]"))
        .replace(r"[^\p{L}\d]", &format!("[^{letters}0-9]"))
        .replace(r"[^\w]", "[^0-9A-Za-z_]")
        .replace(r"\w", "[0-9A-Za-z_]")
        .replace(r"\s", r"[\t\n\f\r ]")
        .replace(r"\d", "[0-9]")
        .replace(r"\p{L}", letters)
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
