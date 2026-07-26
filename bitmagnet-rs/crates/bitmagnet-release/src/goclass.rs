//! The Go-pinned Unicode character classes every port in the workspace must
//! use instead of Rust's own predicates.
//!
//! Rust's `char::is_alphanumeric` / `is_alphabetic` / `is_numeric` and the
//! `regex` crate's `\p{L}` are all STRICTLY WIDER than the Go equivalents
//! bitmagnet is built against — Rust accepts characters production rejects, and
//! Go is never the wider of the two. See [`tables`] for the measured counts.
//!
//! This module is the single source of truth; `tests/goclass_all_scalars.rs`
//! re-proves it against the generated Go oracle over every one of the 1,112,064
//! Unicode scalar values, so a `regex` or rustc bump fails loudly rather than
//! drifting silently.

mod tables;

pub use tables::{LETTER_CLASS_BODY, LETTER_RANGES, WORD_CHAR_RANGES};

/// Is `cp` inside one of the (sorted, non-overlapping) inclusive `ranges`?
fn in_ranges(ranges: &[(u32, u32)], cp: u32) -> bool {
    // Index of the first range whose `lo` is greater than `cp`; the candidate
    // range that could contain `cp` is therefore the one just before it.
    let idx = ranges.partition_point(|&(lo, _)| lo <= cp);
    idx > 0 && cp <= ranges[idx - 1].1
}

/// Go `lexer.IsWordChar` — `unicode.IsLetter(r) || unicode.IsDigit(r)`.
///
/// Use this, never `char::is_alphanumeric()` (12,322 code points wider) and
/// never `is_alphabetic() || is_numeric()` (the same 12,322 — the two Rust
/// spellings are equivalent).
#[must_use]
pub fn is_word_char(c: char) -> bool {
    in_ranges(WORD_CHAR_RANGES, c as u32)
}

/// Go `unicode.IsLetter`. Use this, never `char::is_alphabetic()` (11,317 code
/// points wider).
#[must_use]
pub fn is_letter(c: char) -> bool {
    in_ranges(LETTER_RANGES, c as u32)
}

/// Substitute every literal `\p{L}` in `pattern` for [`LETTER_CLASS_BODY`], so
/// the compiled regex matches the letter set Go's RE2 matches rather than the
/// (4,924 code points wider) set the `regex` crate ships.
///
/// The body carries no brackets, so this works for both the positive
/// (`[\p{L}0-9]`) and negated (`[^\p{L}0-9]`) forms the Go emitter produces.
#[must_use]
pub fn pin_letter_class(pattern: &str) -> String {
    pattern.replace(r"\p{L}", LETTER_CLASS_BODY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn word_char_rejects_what_rust_would_accept() {
        // The audit's headline divergences: Rust calls each of these
        // alphanumeric, Go does not.
        for c in ['²', '³', '¹', '¼', '½', '¾', '①', '②', '③', 'Ⅷ'] {
            assert!(c.is_alphanumeric(), "premise: Rust accepts {c:?}");
            assert!(!is_word_char(c), "Go rejects {c:?}");
        }
        // Controls that must keep matching: ASCII, CJK, Cyrillic, fullwidth
        // katakana, and Arabic-Indic digits (Nd — Go's IsDigit DOES accept).
        for c in ['a', 'Z', '7', '第', 'ж', 'ｱ', '٣'] {
            assert!(is_word_char(c), "Go accepts {c:?}");
        }
    }

    #[test]
    fn letter_matches_go_not_rust() {
        // `Ⅷ` (Nl) is `is_alphabetic` in Rust but not a Go letter.
        assert!('Ⅷ'.is_alphabetic());
        assert!(!is_letter('Ⅷ'));
        assert!(is_letter('a') && is_letter('第') && is_letter('ж'));
        // A digit is never a letter, in either engine.
        assert!(!is_letter('7'));
    }

    #[test]
    fn pin_letter_class_rewrites_both_polarities() {
        assert_eq!(
            pin_letter_class(r"[\p{L}0-9]"),
            format!("[{LETTER_CLASS_BODY}0-9]")
        );
        assert_eq!(
            pin_letter_class(r"[^\p{L}0-9]"),
            format!("[^{LETTER_CLASS_BODY}0-9]")
        );
        assert_eq!(pin_letter_class("[0-9]"), "[0-9]");
    }
}
