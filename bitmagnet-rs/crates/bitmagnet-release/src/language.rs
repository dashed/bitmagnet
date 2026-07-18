//! Port of `internal/model/language.go` + `languages.csv` — language detection
//! from a release name.
//!
//! The CSV (62 rows) is embedded (`include_str!`) and parsed once. For each
//! language the regex keyword set is `alpha2+"dub"`, `alpha3`, lowercased
//! `name`, and lowercased aliases — built in **CSV order** exactly as Go's
//! `newLanguagesRegex`. Unlike the video tables this list is NOT sorted
//! longest-first: it is already deterministic (a slice, not map iteration), and
//! matches are word-bounded so token order can't change which text matches.
//!
//! Determinism note: Go's `ParseLanguage` iterates `languagesMap` (random map
//! order) for the alpha3/name/alias lookup. A collision (one token → two
//! languages) would make that observable, but there are **none** in the frozen
//! CSV (verified), so resolving in CSV order is both deterministic and
//! Go-identical. `Slice()` order (the corpus's output order) is natsort by
//! name — ported faithfully in `natsort`.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use regex::Regex;

use crate::keywords::regex_pattern_from_keywords;
use crate::natsort::nat_less;

struct LanguageInfo {
    alpha2: String,
    alpha3: String,
    /// Original-case name (what Go's `natsort` compares in `Slice()`).
    name: String,
    lower_name: String,
    /// Lowercased aliases in **CSV `|`-split order** — Go's `aliases []string`.
    /// Order is load-bearing for byte-identical regex construction.
    lower_aliases: Vec<String>,
}

const LANGUAGES_CSV: &str = include_str!("languages.csv");

static LANGUAGES: LazyLock<Vec<LanguageInfo>> = LazyLock::new(|| {
    // Skip the header row; mirror Go's Split("\n")[1:] + skip-empty.
    LANGUAGES_CSV
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            let aliases: Vec<String> = parts[3]
                .split('|')
                .map(str::trim)
                .filter(|a| !a.is_empty())
                .map(str::to_lowercase)
                .collect();
            LanguageInfo {
                alpha2: parts[0].to_string(),
                // Go takes the first 3 bytes; all alpha3 codes are ASCII.
                alpha3: parts[1].to_string(),
                name: parts[2].to_string(),
                lower_name: parts[2].to_lowercase(),
                lower_aliases: aliases,
            }
        })
        .collect()
});

static ALPHA2_SET: LazyLock<BTreeSet<&'static str>> =
    LazyLock::new(|| LANGUAGES.iter().map(|l| l.alpha2.as_str()).collect());

/// Build the languages regex pattern (CSV order), byte-identical to Go's
/// `newLanguagesRegex` output after the ASCII adaptation.
pub(crate) fn languages_pattern() -> String {
    let mut tokens: Vec<String> = Vec::with_capacity(LANGUAGES.len() * 4);
    for lang in LANGUAGES.iter() {
        tokens.push(format!("{}dub", lang.alpha2));
        tokens.push(lang.alpha3.clone());
        tokens.push(lang.lower_name.clone());
        for alias in &lang.lower_aliases {
            tokens.push(alias.clone());
        }
    }
    let refs: Vec<&str> = tokens.iter().map(String::as_str).collect();
    regex_pattern_from_keywords(&refs).expect("language keywords compile")
}

static LANGUAGES_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&languages_pattern()).expect("languages regex compiles"));

/// Port of `ParseLanguage` → the language's alpha2 code, or `None`.
pub fn parse_language(name: &str) -> Option<String> {
    let name = name.to_lowercase();
    // Go: `len(name) == 2` is a byte length.
    if name.len() == 2 && ALPHA2_SET.contains(name.as_str()) {
        return Some(name);
    }
    for lang in LANGUAGES.iter() {
        if name == lang.alpha3
            || name == lang.lower_name
            || lang.lower_aliases.iter().any(|a| a == &name)
        {
            return Some(lang.alpha2.clone());
        }
    }
    None
}

/// Port of `InferLanguages` → the detected languages as alpha2 codes in
/// `Languages.Slice()` order (natsort by language name), which is the order the
/// classifier corpus serializes. Empty ⇒ no languages (Go returns nil).
pub fn infer_languages(input: &str) -> Vec<String> {
    let mut found: BTreeSet<String> = BTreeSet::new();
    let mut rest = input;

    while let Some(caps) = LANGUAGES_RE.captures(rest) {
        let whole = caps.get(0).expect("group 0");
        let substr = caps.get(1).map_or("", |m| m.as_str());
        // TrimSuffix "dub" (one occurrence).
        let substr = substr.strip_suffix("dub").unwrap_or(substr);
        if let Some(alpha2) = parse_language(substr) {
            found.insert(alpha2);
        }
        let next = whole.end();
        rest = &rest[next..];
    }

    // Order by Languages.Slice(): natsort on the language NAME.
    let mut langs: Vec<&LanguageInfo> = found
        .iter()
        .filter_map(|a2| LANGUAGES.iter().find(|l| &l.alpha2 == a2))
        .collect();
    // Derive Ordering from Go's Less predicate (natsort.Compare on the name),
    // exactly as Go's sort.Slice does.
    langs.sort_by(|a, b| {
        if nat_less(&a.name, &b.name) {
            std::cmp::Ordering::Less
        } else if nat_less(&b.name, &a.name) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    langs.into_iter().map(|l| l.alpha2.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testsupport::{adapt_go_pattern, load_go_patterns};

    #[test]
    fn languages_pattern_matches_go() {
        let go = load_go_patterns();
        assert_eq!(languages_pattern(), adapt_go_pattern(&go["languages"]));
        LazyLock::force(&LANGUAGES_RE);
    }

    #[test]
    fn csv_matches_go_oracle() {
        // Drift guard: the embedded CSV must equal the Go oracle byte-for-byte.
        let oracle = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../internal/model/languages.csv"
        );
        let oracle = std::fs::read_to_string(oracle).expect("read go languages.csv");
        assert_eq!(
            LANGUAGES_CSV, oracle,
            "embedded languages.csv drifted from Go"
        );
    }

    #[test]
    fn parses_62_languages() {
        assert_eq!(LANGUAGES.len(), 62);
    }
}
