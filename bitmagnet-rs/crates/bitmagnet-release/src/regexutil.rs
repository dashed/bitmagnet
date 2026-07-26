//! Regex-fragment helpers mirroring `internal/regex/util.go` and the subset of
//! `hedhyw/rex`'s `Chars` builders the keyword compiler relies on.
//!
//! Unicode-parity note (the #1 Lane R trap): NONE of Go's regex shorthands mean
//! what the Rust `regex` crate means by them.
//!
//! * Go's `\d`/`\w`/`\s` are ASCII, Rust's are Unicode — so every digit class
//!   here is emitted as `[0-9]`, never `\d`.
//! * Go's `\p{L}` is 4,924 code points NARROWER than Rust's, because `regex`
//!   1.13 ships newer Unicode tables than Go 1.23.6 — so `\p{L}` is spliced out
//!   for [`goclass::LETTER_CLASS_BODY`], the class Go's RE2 actually applies.
//!
//! See `docs/dev/rust-rewrite/phase3-contracts.md §3/§2.4`.

use std::sync::LazyLock;

use crate::goclass;

/// `regex.AnyWordChar()` — Go emits `[\p{L}\d]`; we emit the ASCII-digit form
/// with the Go-pinned letter class spliced in.
static ANY_WORD_CHAR: LazyLock<String> =
    LazyLock::new(|| format!("[{}0-9]", goclass::LETTER_CLASS_BODY));

/// `regex.AnyNonWordChar()` — Go emits `[^\p{L}\d]`.
static ANY_NON_WORD_CHAR: LazyLock<String> =
    LazyLock::new(|| format!("[^{}0-9]", goclass::LETTER_CLASS_BODY));

/// `regex.AnyWordChar()` — see [`ANY_WORD_CHAR`].
pub(crate) fn any_word_char() -> &'static str {
    &ANY_WORD_CHAR
}

/// `regex.AnyNonWordChar()` — see [`ANY_NON_WORD_CHAR`].
pub(crate) fn any_non_word_char() -> &'static str {
    &ANY_NON_WORD_CHAR
}

/// `rex.Chars.Digits()` — Go emits `\d` (ASCII); we emit `[0-9]`.
pub(crate) fn digits() -> String {
    "[0-9]".to_string()
}

/// Regex metacharacters escaped by Go's `regexp.QuoteMeta` (exactly the bytes
/// in `\.+*?()|[]{}^$`).
fn is_quote_meta(c: char) -> bool {
    matches!(
        c,
        '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
    )
}

/// Faithful port of `hedhyw/rex`'s `CharsBaseDialect.Runes`: a character class
/// whose members are each emitted via `single_char` (so non-ASCII runes are
/// hex-escaped, e.g. `Áá` -> `[\xC1\xE1]`, matching Go). Brackets are dropped
/// for a single rune, as Go does.
pub(crate) fn runes_class(s: &str) -> String {
    let members: String = s.chars().map(single_char).collect();
    if s.chars().count() <= 1 {
        members
    } else {
        format!("[{members}]")
    }
}

/// Faithful port of `hedhyw/rex`'s `CharsBaseDialect.Single`: printable-ASCII
/// runes (except `-` and `%`) are emitted via `regexp.QuoteMeta`; everything
/// else is hex-escaped (`\xHH` for two hex digits, `\x{…}` otherwise). `-` and
/// `%` are always hex-escaped (Go excludes them from the QuoteMeta path).
pub(crate) fn single_char(c: char) -> String {
    let code = c as u32;
    if (0x20..0x7F).contains(&code) && c != '-' && c != '%' {
        if is_quote_meta(c) {
            return format!("\\{c}");
        }
        return c.to_string();
    }

    let hex = format!("{code:X}");
    if hex.len() == 2 {
        format!("\\x{hex}")
    } else {
        format!("\\x{{{hex}}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_char_matches_go_rex() {
        // Printable ASCII, non-special -> as-is.
        assert_eq!(single_char('a'), "a");
        assert_eq!(single_char('1'), "1");
        // QuoteMeta specials.
        assert_eq!(single_char('.'), "\\.");
        assert_eq!(single_char('$'), "\\$");
        assert_eq!(single_char('('), "\\(");
        assert_eq!(single_char('\\'), "\\\\");
        // `-` and `%` always hex.
        assert_eq!(single_char('-'), "\\x2D");
        assert_eq!(single_char('%'), "\\x25");
        // Non-ASCII -> `\x{...}`.
        assert_eq!(single_char('【'), "\\x{3010}");
    }
}
