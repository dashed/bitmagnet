//! Regex-fragment helpers mirroring `internal/regex/util.go` and the subset of
//! `hedhyw/rex`'s `Chars` builders the keyword compiler relies on.
//!
//! ASCII-parity note (the #1 Lane R trap): Go's `regexp` treats `\d` as ASCII
//! `[0-9]`, but the Rust `regex` crate treats `\d` as Unicode. So every digit
//! class here is emitted as `[0-9]`, never `\d`, while `\p{L}` (identical in
//! both engines) stays. See `docs/dev/rust-rewrite/phase3-contracts.md §3/§2.4`.

/// `regex.AnyWordChar()` — Go emits `[\p{L}\d]`; we emit the ASCII-digit form.
pub(crate) fn any_word_char() -> &'static str {
    "[\\p{L}0-9]"
}

/// `regex.AnyNonWordChar()` — Go emits `[^\p{L}\d]`.
pub(crate) fn any_non_word_char() -> &'static str {
    "[^\\p{L}0-9]"
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
