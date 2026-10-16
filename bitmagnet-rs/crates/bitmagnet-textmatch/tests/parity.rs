//! Differential parity tests for `bitmagnet-textmatch`.
//!
//! Every expected value in `fixtures/textmatch_fixtures.json` was produced by
//! calling the **real Go** functions — `levenshteinFindBestMatch`,
//! `levenshteinFindMinDistance`, `levenshteinNormalizeString`,
//! `regex.NormalizeString`, `unidecode.Unidecode` and
//! `levenshtein.ComputeDistance` — not by hand. See `fixtures/README.md`.

use std::collections::BTreeSet;

use bitmagnet_textmatch as tm;
use serde::{Deserialize, Deserializer};

const FIXTURES_JSON: &str = include_str!("fixtures/textmatch_fixtures.json");

/// Go marshals a `nil` slice as JSON `null`. Rust draws no distinction between
/// a nil and an empty slice, and neither does the algorithm (`range` over nil
/// is a zero-iteration loop), so both decode to an empty `Vec`.
fn null_as_empty<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(d)?.unwrap_or_default())
}

#[derive(Deserialize)]
struct StrCase {
    #[serde(rename = "in")]
    input: String,
    out: String,
}

#[derive(Deserialize)]
struct DistCase {
    a: String,
    b: String,
    d: usize,
}

#[derive(Deserialize)]
struct MinDistCase {
    target: String,
    #[serde(deserialize_with = "null_as_empty")]
    candidates: Vec<String>,
    /// `-1` is Go's "no candidates" sentinel.
    d: i64,
}

#[derive(Deserialize)]
struct BestMatchCase {
    name: String,
    target: String,
    #[serde(deserialize_with = "null_as_empty")]
    items: Vec<Vec<String>>,
    /// `-1` is Go's `ok == false`.
    index: i64,
}

#[derive(Deserialize)]
struct Fixtures {
    word_token_pattern: String,
    word_char_class: String,
    threshold: usize,
    letter_digit_ranges: Vec<(u32, u32)>,
    unidecode: Vec<StrCase>,
    normalize: Vec<StrCase>,
    lev_normalize: Vec<StrCase>,
    distance: Vec<DistCase>,
    min_distance: Vec<MinDistCase>,
    best_match: Vec<BestMatchCase>,
}

fn fixtures() -> Fixtures {
    serde_json::from_str(FIXTURES_JSON).expect("fixtures parse")
}

/// Guard: the pinned pattern is byte-identical to what the Go `rex` combinators
/// actually compile to. If `internal/regex/util.go` is ever edited, this fails.
#[test]
fn go_word_token_pattern_is_pinned() {
    assert_eq!(fixtures().word_token_pattern, tm::GO_WORD_TOKEN_PATTERN);
}

/// Guard: the pinned word-char class in `src/word_char_class.rs` is still the
/// one the generator read out of Go.
#[test]
fn word_char_class_is_pinned() {
    assert_eq!(fixtures().word_char_class, tm::WORD_CHAR_CLASS);
}

/// Guard: the Rust pattern differs from the Go one *only* by the substitution
/// of the word-char class.
#[test]
fn rust_pattern_differs_only_in_the_word_char_class() {
    assert_eq!(
        tm::GO_WORD_TOKEN_PATTERN.replace(r"[\p{L}\d]", tm::WORD_CHAR_CLASS),
        tm::word_token_pattern(),
    );
    // ...and that substitution really did happen (two occurrences).
    assert_eq!(tm::GO_WORD_TOKEN_PATTERN.matches(r"[\p{L}\d]").count(), 2);
    assert!(!tm::word_token_pattern().contains(r"\p{L}"));
    assert!(!tm::word_token_pattern().contains(r"\d"));
}

#[test]
fn threshold_matches_go() {
    assert_eq!(fixtures().threshold, tm::LEVENSHTEIN_THRESHOLD);
}

/// The `[\p{L}\d]` word-char class, proved equal over **every** Unicode scalar
/// value. This is the ASCII-vs-Unicode trap made non-negotiable: Rust's `\d`
/// would add all of `Nd`, and a blanket `(?-u)` would have gutted `\p{L}`.
#[test]
fn word_char_class_matches_go_over_all_scalar_values() {
    let fx = fixtures();
    let go: BTreeSet<u32> = fx
        .letter_digit_ranges
        .iter()
        .flat_map(|&(lo, hi)| lo..=hi)
        .collect();

    let re = regex::Regex::new(&format!("^(?:{})$", tm::word_token_pattern()))
        .expect("single-char probe compiles");

    let mut mismatches = Vec::new();
    let mut checked = 0usize;
    for cp in 0..=0x0010_FFFFu32 {
        let Some(c) = char::from_u32(cp) else {
            continue; // surrogates are not scalar values
        };
        checked += 1;
        let mut buf = [0u8; 4];
        // A single word char is a complete `WordToken` match; nothing else in
        // the production can match a lone character.
        let rust = re.is_match(c.encode_utf8(&mut buf));
        if rust != go.contains(&cp) {
            mismatches.push(cp);
        }
    }

    assert_eq!(checked, 1_112_064, "all scalar values were probed");
    assert!(
        mismatches.is_empty(),
        "word-char class diverges from Go at {} code points, first: {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(16)],
    );
    // Sanity: Go really is ASCII-only for digits.
    assert!(go.contains(&u32::from('9')));
    assert!(
        !go.contains(&0x0663),
        "Arabic-Indic 3 must NOT be a word char"
    );
    assert!(go.contains(&0x4E2D), "CJK must be a word char");
}

#[test]
fn unidecode_matches_go() {
    let fx = fixtures();
    assert!(fx.unidecode.len() > 4000, "corpus size");
    for case in &fx.unidecode {
        assert_eq!(
            tm::unidecode(&case.input),
            case.out,
            "unidecode({:?})",
            case.input,
        );
    }
}

#[test]
fn normalize_string_matches_go() {
    let fx = fixtures();
    assert!(fx.normalize.len() > 8000, "corpus size");
    for case in &fx.normalize {
        assert_eq!(
            tm::normalize_string(&case.input),
            case.out,
            "normalize_string({:?})",
            case.input,
        );
    }
}

#[test]
fn levenshtein_normalize_string_matches_go() {
    let fx = fixtures();
    for case in &fx.lev_normalize {
        assert_eq!(
            tm::levenshtein_normalize_string(&case.input),
            case.out,
            "levenshtein_normalize_string({:?})",
            case.input,
        );
    }
}

#[test]
fn compute_distance_matches_go() {
    let fx = fixtures();
    assert!(fx.distance.len() > 2000, "corpus size");
    for case in &fx.distance {
        assert_eq!(
            tm::compute_distance(&case.a, &case.b),
            case.d,
            "compute_distance({:?}, {:?})",
            case.a,
            case.b,
        );
    }
}

#[test]
fn find_min_distance_matches_go() {
    let fx = fixtures();
    for case in &fx.min_distance {
        let expected = usize::try_from(case.d).ok();
        assert_eq!(
            tm::find_min_distance(&case.target, &case.candidates),
            expected,
            "find_min_distance({:?}, {:?})",
            case.target,
            case.candidates,
        );
    }
}

#[test]
fn find_best_match_matches_go() {
    let fx = fixtures();
    assert!(fx.best_match.len() > 900, "corpus size");
    for case in &fx.best_match {
        let expected = usize::try_from(case.index).ok();
        let got = tm::find_best_match_index(&case.target, &case.items, Clone::clone);
        assert_eq!(
            got, expected,
            "[{}] find_best_match({:?}, {:?})",
            case.name, case.target, case.items,
        );

        // The by-reference wrapper must agree with the index wrapper.
        let by_ref = tm::find_best_match(&case.target, &case.items, Clone::clone);
        assert_eq!(by_ref, expected.map(|i| &case.items[i]), "[{}]", case.name);
    }
}

/// The named cases the ledger calls out explicitly, asserted by name so a
/// regression names itself instead of hiding in the bulk corpus.
#[test]
fn named_semantic_cases_are_present_and_pass() {
    let fx = fixtures();
    let required = [
        ("tie-first-wins", Some(0usize)),
        ("tie-first-wins-distance-5", Some(0)),
        ("threshold-5-accepted", Some(0)),
        ("threshold-6-rejected", None),
        ("threshold-6-then-5", Some(1)),
        ("exact-first", Some(0)),
        ("exact-later-wins-over-near", Some(1)),
        ("empty-candidate-list-skipped", Some(1)),
        ("multi-string-per-item-second-wins", Some(0)),
        ("multi-string-min-taken", Some(0)),
        ("duplicate-normalized-candidates", Some(0)),
        ("all-above-threshold", None),
        ("unidecode-path", Some(0)),
        ("empty-target", Some(2)),
        ("whitespace-target", Some(0)),
        ("punctuation-only-candidates", Some(1)),
        ("empty-items", None),
        ("nil-items", None),
    ];

    for (name, expected) in required {
        let case = fx
            .best_match
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("fixture case {name:?} is missing"));
        // The Go oracle agrees with the ledger's stated semantics...
        assert_eq!(
            usize::try_from(case.index).ok(),
            expected,
            "Go oracle for {name:?}",
        );
        // ...and so does the Rust port.
        assert_eq!(
            tm::find_best_match_index(&case.target, &case.items, Clone::clone),
            expected,
            "Rust port for {name:?}",
        );
    }
}

/// Early exit is not observable from the return value alone (a later exact
/// match would win anyway), so prove it by counting `get_candidates` calls.
#[test]
fn distance_zero_short_circuits_the_scan() {
    let items = vec![
        vec!["Blade Runnerx".to_owned()],
        vec!["Blade Runner".to_owned()],
        vec!["Blade Runner".to_owned()],
        vec!["Blade Runner".to_owned()],
    ];

    let mut calls = 0usize;
    let got = tm::find_best_match_index("Blade Runner", &items, |item: &Vec<String>| {
        calls += 1;
        item.clone()
    });

    assert_eq!(got, Some(1));
    assert_eq!(calls, 2, "the scan must stop at the first distance-0 item");
}

/// `char::to_lowercase` is the full (one-to-many) Unicode mapping; Go's
/// `strings.ToLower` is the simple one. `İ` is the canonical divergence, and
/// title metadata really does contain it (Turkish releases).
#[test]
fn turkish_dotted_capital_i_uses_gos_simple_lowering() {
    assert_eq!(
        'İ'.to_lowercase().collect::<String>(),
        "i\u{307}",
        "precondition: Rust's mapping is one-to-many",
    );
    // Go: strings.ToLower("İ") == "i"; NFC leaves it alone; the token is "i".
    assert_eq!(tm::normalize_string("İ"), "i");
    // And via the classifier's real entry point (unidecode maps İ -> "I").
    assert_eq!(tm::levenshtein_normalize_string("İstanbul"), "istanbul");
}

/// `ß` is the other classic full-vs-simple case-mapping trap; both engines
/// leave it alone at the lowering step, and unidecode expands it to `ss`.
#[test]
fn sharp_s_round_trips_like_go() {
    assert_eq!(tm::normalize_string("Straße"), "straße");
    assert_eq!(tm::levenshtein_normalize_string("Straße"), "strasse");
    assert_eq!(tm::levenshtein_normalize_string("STRASSE"), "strasse");
}
