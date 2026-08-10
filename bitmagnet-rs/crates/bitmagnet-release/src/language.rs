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

    slice_order(found)
}

/// Go `Languages.Slice()` (`internal/model/language.go:180`) — the set's members
/// ordered by **natsort on the language NAME**.
///
/// 🚨 This is NOT a sort by alpha-2 code, and the two genuinely disagree:
/// `[de, en]` by code is German/English, which by name is `[en, de]`. Every
/// serialisation of a language list goes through this order, so sorting by code
/// anywhere is a bug — one that hid for a long time because it only shows up
/// once a list has two languages whose code and name orders differ, which in
/// practice means once `AttachContent` folds a content row's original language
/// into an inferred list.
///
/// Duplicates collapse: Go holds languages in a `map`, so the input is a set.
///
/// An unrecognised code is KEPT, not dropped — Go's `Language.Name()` is a map
/// lookup returning the zero value, so an unknown language sorts under the empty
/// name (i.e. first) rather than disappearing. Ties are broken by the code:
/// Go's `sort.Slice` is unstable, so tied names have no defined order there and
/// any consistent choice is faithful.
#[must_use]
pub fn slice_order<I, S>(codes: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let unique: BTreeSet<String> = codes
        .into_iter()
        .map(|code| code.as_ref().to_owned())
        .collect();

    let mut ordered: Vec<(String, String)> = unique
        .into_iter()
        .map(|code| {
            let name = LANGUAGES
                .iter()
                .find(|lang| lang.alpha2 == code)
                .map_or(String::new(), |lang| lang.name.clone());
            (name, code)
        })
        .collect();

    // Go derives the ordering from a Less predicate (natsort.Compare on the
    // name), exactly as sort.Slice does.
    ordered.sort_by(|a, b| {
        if nat_less(&a.0, &b.0) {
            std::cmp::Ordering::Less
        } else if nat_less(&b.0, &a.0) {
            std::cmp::Ordering::Greater
        } else {
            a.1.cmp(&b.1)
        }
    });

    ordered.into_iter().map(|(_, code)| code).collect()
}

#[cfg(test)]
mod slice_order_tests {
    use super::*;

    /// The bug this function exists to prevent: code order and name order are
    /// genuinely different, so sorting by alpha-2 is not a shortcut for
    /// `Languages.Slice()`.
    #[test]
    fn name_order_is_not_code_order() {
        // de = German, en = English -> by NAME English comes first.
        assert_eq!(slice_order(["de", "en"]), vec!["en", "de"]);
        // ...and the input order does not matter, because it is a set.
        assert_eq!(slice_order(["en", "de"]), vec!["en", "de"]);
    }

    #[test]
    fn duplicates_collapse_because_go_holds_a_map() {
        assert_eq!(slice_order(["en", "en", "ko"]), vec!["en", "ko"]);
    }

    /// The load-bearing property is that an unrecognised code is KEPT — dropping
    /// it would silently lose data. Its POSITION is deliberately not asserted:
    /// the empty name ties under `nat_less`, and Go's `sort.Slice` is unstable,
    /// so neither side defines an order for it.
    #[test]
    fn an_unknown_code_is_kept_not_dropped() {
        let ordered = slice_order(["en", "zz"]);
        assert_eq!(ordered.len(), 2);
        assert!(ordered.contains(&"zz".to_owned()));
        assert!(ordered.contains(&"en".to_owned()));
    }

    #[test]
    fn empty_stays_empty() {
        assert!(slice_order(Vec::<String>::new()).is_empty());
    }

    /// `infer_languages` already produced this order; extracting it must not
    /// have changed what it produces.
    #[test]
    fn infer_languages_still_emits_slice_order() {
        let inferred = infer_languages("Movie.2011.German.English.1080p");
        assert_eq!(inferred, slice_order(inferred.clone()));
    }
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

    // 🔑 Case-mapping drift guard. The keyword compiler uses Rust's *full*
    // Unicode case mapping (`char::to_lowercase`/`to_uppercase`, which can yield
    // multiple chars — e.g. `ß`→`SS`, `İ`→`i̇`), whereas Go's rex/compiler use
    // *simple* 1:1 mapping (`strings.ToLower`/`ToUpper` over runes,
    // `unicode.To{Lower,Upper}`). They diverge ONLY for chars whose full mapping
    // is multi-char ( or has locale-special rules). None exist in the current
    // `languages.csv` (á/ñ/CJK are all 1:1 case-mapping-safe), so the compiled
    // regex matches Go. This test asserts every CSV char stays 1:1 in both
    // directions, so a future ß/İ-class alias fires here instead of silently
    // drifting the regex away from Go.
    #[test]
    fn csv_chars_have_simple_case_mapping() {
        for c in LANGUAGES_CSV.chars() {
            let lower_len = c.to_lowercase().count();
            let upper_len = c.to_uppercase().count();
            assert_eq!(
                lower_len, 1,
                "char {c:?} (U+{:04X}) has multi-char lowercase (full != simple case mapping); \
                 the keyword compiler would diverge from Go — handle it explicitly",
                c as u32
            );
            assert_eq!(
                upper_len, 1,
                "char {c:?} (U+{:04X}) has multi-char uppercase (full != simple case mapping); \
                 the keyword compiler would diverge from Go — handle it explicitly",
                c as u32
            );
        }
    }
}
