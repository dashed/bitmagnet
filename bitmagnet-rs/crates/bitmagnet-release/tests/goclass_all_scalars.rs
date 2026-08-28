//! All-scalar equality assertions for the Go-pinned Unicode classes.
//!
//! Every check below sweeps all 1,112,064 Unicode scalar values. The point is
//! that a `regex`-crate or rustc bump must FAIL LOUDLY here rather than drift:
//! the previous test methodology compared pattern *strings* that contained the
//! same literal `\p{L}` on both sides, so it passed while the two engines
//! matched different character sets.

use bitmagnet_release::goclass::{
    is_letter, is_word_char, LETTER_CLASS_BODY, LETTER_RANGES, WORD_CHAR_RANGES,
};

const TOTAL_SCALARS: usize = 1_112_064;

fn scalars() -> impl Iterator<Item = char> {
    (0..=0x10FFFF_u32).filter_map(char::from_u32)
}

fn in_ranges(ranges: &[(u32, u32)], c: char) -> bool {
    let cp = c as u32;
    let idx = ranges.partition_point(|&(lo, _)| lo <= cp);
    idx > 0 && cp <= ranges[idx - 1].1
}

#[test]
fn scalar_count_is_what_we_think_it_is() {
    assert_eq!(scalars().count(), TOTAL_SCALARS);
}

/// The pinned class string and the generated Go oracle must agree on EVERY
/// scalar. This is the assertion that makes the pinning trustworthy; if the
/// generator is ever re-run against a different Go toolchain and only one of
/// the two artifacts is refreshed, this fails.
#[test]
fn pinned_letter_class_equals_go_letter_ranges_on_every_scalar() {
    let re = regex::Regex::new(&format!("^[{LETTER_CLASS_BODY}]$")).expect("pinned class compiles");
    let mut mismatches = Vec::new();
    for c in scalars() {
        if re.is_match(&c.to_string()) != in_ranges(LETTER_RANGES, c) {
            mismatches.push(c);
            if mismatches.len() > 8 {
                break;
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "pinned letter class disagrees with the Go oracle at {mismatches:?}"
    );
}

/// The whole reason the pinned class exists: the `regex` crate's own `\p{L}` is
/// NOT Go's. Asserting the direction (Rust wider, Go never wider) rather than a
/// frozen count keeps this meaningful across `regex` bumps — but the divergence
/// must stay non-empty, otherwise the assertion is vacuous and someone could
/// "simplify" the pinned class back to `\p{L}` without failing anything.
#[test]
fn regex_crate_letter_class_is_strictly_wider_than_go() {
    let rust = regex::Regex::new(r"^\p{L}$").expect("\\p{L} compiles");
    let mut rust_wider = 0usize;
    let mut go_wider = Vec::new();
    for c in scalars() {
        match (rust.is_match(&c.to_string()), is_letter(c)) {
            (true, false) => rust_wider += 1,
            (false, true) => go_wider.push(c),
            _ => {}
        }
    }
    assert!(
        go_wider.is_empty(),
        "Go accepts letters the regex crate rejects at {go_wider:?} — the port \
         would now be NARROWER than production, which the subset argument \
         everywhere else assumes cannot happen"
    );
    assert!(
        rust_wider > 0,
        "regex's \\p{{L}} no longer differs from Go's; the pinned class may be \
         stale or the oracle was regenerated against the wrong toolchain"
    );
    // Measured at the time of writing: 4,924 (regex 1.13 vs Go go1.23.6).
    assert_eq!(rust_wider, 4_924, "letter-class divergence moved");
}

/// Same shape for the word-char predicate: `is_word_char` must be exactly the
/// generated table, and `char::is_alphanumeric()` must be strictly wider.
#[test]
fn word_char_predicate_equals_table_and_std_is_strictly_wider() {
    let mut std_wider = 0usize;
    let mut go_wider = Vec::new();
    for c in scalars() {
        assert_eq!(
            is_word_char(c),
            in_ranges(WORD_CHAR_RANGES, c),
            "is_word_char disagrees with its own table at {c:?}"
        );
        match (c.is_alphanumeric(), is_word_char(c)) {
            (true, false) => std_wider += 1,
            (false, true) => go_wider.push(c),
            _ => {}
        }
    }
    assert!(
        go_wider.is_empty(),
        "Go accepts word chars Rust rejects at {go_wider:?}"
    );
    // Measured at the time of writing: 12,322 (rustc 1.97.1 vs Go go1.23.6).
    assert_eq!(std_wider, 12_322, "word-char divergence moved");
}

/// `is_alphanumeric()` and `is_alphabetic() || is_numeric()` are the same
/// predicate — both were used in the shipped code, both were equally wrong.
#[test]
fn the_two_rust_spellings_are_the_same_wrong_predicate() {
    for c in scalars() {
        assert_eq!(c.is_alphanumeric(), c.is_alphabetic() || c.is_numeric());
    }
}

/// The bare shorthands Go and Rust disagree about, measured so the sweep for
/// them elsewhere in the workspace has a number attached. Go's `regexp` `\w`
/// and `\d` are ASCII; the `regex` crate's are Unicode.
#[test]
fn bare_shorthands_diverge_and_the_ascii_forms_do_not() {
    let rust_w = regex::Regex::new(r"^\w$").expect("\\w compiles");
    let rust_d = regex::Regex::new(r"^\d$").expect("\\d compiles");
    let ascii_w = regex::Regex::new(r"^[0-9A-Za-z_]$").expect("ascii \\w compiles");
    let ascii_d = regex::Regex::new(r"^[0-9]$").expect("ascii \\d compiles");

    let mut w_diff = 0usize;
    let mut d_diff = 0usize;
    for c in scalars() {
        let s = c.to_string();
        // The explicit ASCII classes ARE Go's `\w`/`\d`, on every scalar.
        assert_eq!(ascii_w.is_match(&s), c.is_ascii_alphanumeric() || c == '_');
        assert_eq!(ascii_d.is_match(&s), c.is_ascii_digit());
        if rust_w.is_match(&s) != ascii_w.is_match(&s) {
            w_diff += 1;
        }
        if rust_d.is_match(&s) != ascii_d.is_match(&s) {
            d_diff += 1;
        }
    }
    // Measured: a bare `\w` in a unicode-mode Rust regex is 144,604 code points
    // wider than Go's, and a bare `\d` is 750 wider.
    assert_eq!(w_diff, 144_604, "\\w divergence moved");
    assert_eq!(d_diff, 750, "\\d divergence moved");
}

/// `[[:upper:]]` appears verbatim in the title patterns on the claim that it is
/// identical in both engines. That claim was never measured either — measure
/// it, because if it were ever Unicode-aware in Rust the acronym branch of
/// `cleanTitle` would diverge. Go's RE2 matches exactly the 26 ASCII letters.
#[test]
fn posix_upper_is_ascii_in_both_engines() {
    let re = regex::Regex::new(r"^[[:upper:]]$").expect("[[:upper:]] compiles");
    let matched: Vec<char> = scalars().filter(|c| re.is_match(&c.to_string())).collect();
    assert_eq!(
        matched.len(),
        26,
        "Rust's [[:upper:]] is no longer ASCII-only"
    );
    assert_eq!(matched, ('A'..='Z').collect::<Vec<_>>());
}
