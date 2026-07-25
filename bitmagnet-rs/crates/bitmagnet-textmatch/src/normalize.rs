//! Port of Go `regex.NormalizeString` (`internal/regex/util.go:60-73`) and the
//! `wordTokenRegex` it tokenizes with (`internal/regex/util.go:14-54`).
//!
//! Go pipeline, in order:
//!
//! ```go
//! input = strings.ToLower(input)
//! input, _, _ = transform.String(transform.Chain(norm.NFD, norm.NFC), input)
//! // join every non-empty wordTokenRegex match with a single space
//! ```
//!
//! # The regex
//!
//! `wordTokenRegex` is assembled from `rex` combinators, so the authoritative
//! form is what `rex.New(WordToken()).MustCompile()` actually produces. Read
//! straight out of the running Go program it is [`GO_WORD_TOKEN_PATTERN`]; it
//! is pinned as a constant here and asserted against the generated fixture, so
//! a change to the `rex` combinators fails this crate's tests.
//!
//! # ASCII vs Unicode — the one adaptation
//!
//! [`word_token_pattern`] is [`GO_WORD_TOKEN_PATTERN`] with the single
//! substring `[\p{L}\d]` replaced by [`WORD_CHAR_CLASS`], and that is the
//! **only** difference. Neither half of that swap is cosmetic:
//!
//! * Go's RE2 `\d` is ASCII `[0-9]`; Rust's `regex` with Unicode enabled makes
//!   `\d` the whole `Nd` category (Arabic-Indic `٣`, Devanagari `१`, …). Left
//!   alone it would silently widen the token class.
//! * `\p{L}` *is* genuinely Unicode in both engines — a blanket `(?-u)` would
//!   have been badly wrong, gutting every non-Latin title. But the two engines
//!   disagree on **which** code points are letters, because Rust's `regex`
//!   ships newer Unicode tables than the Go toolchain bitmagnet is built with:
//!   a literal `[\p{L}0-9]` diverges from Go at ~4.9k code points (U+1C89,
//!   U+A7CB…, U+10600…), measured, not assumed. So the class is **pinned** to
//!   the ranges the real Go regex accepts (see `word_char_class.rs`), and
//!   `tests/parity.rs` re-proves the equality over all 1,112,064 scalar values.
//! * `[[:upper:]]` is ASCII-only in RE2 *and* in Rust's `regex`, so it needs no
//!   adaptation. It is also unreachable from `normalize_string`, whose input
//!   has already been lower-cased; it is kept because it is part of the shared
//!   `WordToken()` production.
//! * Go's RE2 `\s` (which excludes `\v`) never appears in this production, so
//!   the usual `\s` trap does not apply here.
//!
//! Both engines use leftmost-first (Perl-style, not POSIX-leftmost-longest)
//! alternation, so `find_iter` enumerates the same matches as Go's
//! `FindAllStringSubmatch`.
//!
//! # Lower-casing
//!
//! Rust's `char::to_lowercase` is the *full* Unicode mapping and is one-to-many
//! (`İ` U+0130 → `i` + U+0307), whereas Go's `strings.ToLower` applies the
//! *simple*, single-rune `unicode.ToLower` (`İ` → `i`). The port therefore uses
//! the generated `LOWER_MAP` table rather than `char::to_lowercase`; see
//! [`to_lower`].

use std::sync::LazyLock;

use regex::Regex;
use unicode_normalization::UnicodeNormalization;

pub use crate::word_char_class::WORD_CHAR_CLASS;

/// `rex.New(regex.WordToken()).MustCompile().String()`, read from the real Go
/// program. Asserted against the generated fixture in `tests/parity.rs`.
pub const GO_WORD_TOKEN_PATTERN: &str = r#"(?:[\('"]*(?:(?:[[:upper:]]\.){2,}|(?:[\p{L}\d]+(?:['\x2D]+[\p{L}\d]+)*))[,;:\?!\x2D\)'"]*)"#;

/// The `[\p{L}\d]` sub-expression of [`GO_WORD_TOKEN_PATTERN`], the one part
/// that cannot be reused verbatim.
const GO_WORD_CHAR_CLASS: &str = r"[\p{L}\d]";

static WORD_TOKEN_PATTERN: LazyLock<String> =
    LazyLock::new(|| GO_WORD_TOKEN_PATTERN.replace(GO_WORD_CHAR_CLASS, WORD_CHAR_CLASS));

static WORD_TOKEN_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&WORD_TOKEN_PATTERN).expect("wordTokenRegex is a valid pattern"));

/// [`GO_WORD_TOKEN_PATTERN`] adapted for Rust's `regex` crate: `[\p{L}\d]` is
/// replaced by the pinned [`WORD_CHAR_CLASS`]. Nothing else changes.
#[must_use]
pub fn word_token_pattern() -> &'static str {
    &WORD_TOKEN_PATTERN
}

/// The compiled port of Go's `regex.WordTokenRegex()`.
#[must_use]
pub fn word_token_regex() -> &'static Regex {
    &WORD_TOKEN_REGEX
}

/// Go's single-rune `unicode.ToLower` (what `strings.ToLower` maps with),
/// backed by the generated `LOWER_MAP`. Falls back to identity when there is no
/// mapping — which is what Go does too.
fn to_lower(c: char) -> char {
    let cp = c as u32;
    match crate::tables::LOWER_MAP.binary_search_by_key(&cp, |&(src, _)| src) {
        Ok(i) => char::from_u32(crate::tables::LOWER_MAP[i].1).unwrap_or(c),
        Err(_) => c,
    }
}

/// Port of Go `regex.NormalizeString`.
///
/// Lower-cases, applies `NFD` then `NFC`, then joins every non-empty
/// `wordTokenRegex` match with a single space.
#[must_use]
pub fn normalize_string(input: &str) -> String {
    let lowered: String = input.chars().map(to_lower).collect();
    // `transform.Chain(norm.NFD, norm.NFC)` — transcribed as the same two
    // passes rather than collapsed into a single `.nfc()`.
    let normalized: String = lowered.nfd().collect::<String>().nfc().collect();

    let mut tokens: Vec<&str> = Vec::new();
    for m in WORD_TOKEN_REGEX.find_iter(&normalized) {
        // Go guards `len(match[0]) >= 1`; this production cannot match empty,
        // but the guard is transcribed anyway.
        if !m.as_str().is_empty() {
            tokens.push(m.as_str());
        }
    }

    tokens.join(" ")
}
