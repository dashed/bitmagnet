//! Port of `internal/keywords/parser.go` — the keyword-glob DSL → regex
//! compiler. Go builds regexes with the `hedhyw/rex` token builder; this port
//! emits the *same regex strings* `rex` would, with two deliberate
//! ASCII-parity adaptations (see `regexutil`):
//!
//! * Go's `rex.Chars.Digits()` / `\d` (ASCII in Go's `regexp`) is emitted as
//!   `[0-9]`, because the Rust `regex` crate's `\d` is Unicode by default.
//! * The word-char class `[\p{L}\d]` is emitted as `[\p{L}0-9]` for the same
//!   reason; `\p{L}` stays Unicode (identical in Go and Rust).
//!
//! Every other byte of the emitted pattern is identical to Go's `rex` output
//! (verified byte-for-byte against a Go dump in the crate tests). The DSL:
//!
//! | token   | meaning                                   | emitted            |
//! |---------|-------------------------------------------|--------------------|
//! | `a`     | case-insensitive letter                   | `[Aa]`             |
//! | `1`     | digit / caseless char (literal)           | `1`                |
//! | `*`     | zero-or-more word chars                    | `[\p{L}0-9]*`      |
//! | `#`     | a single ASCII digit                      | `[0-9]`            |
//! | ` `     | a single non-word char                    | `[^\p{L}0-9]`      |
//! | `x?`    | preceding token optional                  | `…?`               |
//! | `x+`    | preceding token one-or-more               | `…+`               |
//! | `(…)`   | group; `?` after → optional               | `(?:…)` / `(?:…)?` |
//! | `|`     | alternation within a group                | `…|…`              |
//! | `\x`    | literal `x`                               | escaped `x`        |

use crate::lexer::{is_word_char, Lexer};
use crate::regexutil::{any_non_word_char, any_word_char, digits, runes_class, single_char};

/// Errors from compiling a keyword. Mirrors the sentinels in
/// `internal/keywords/parser.go` (`ErrEOF` is an internal control signal and is
/// not surfaced).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeywordError {
    #[error("no keywords provided")]
    NoKeywords,
    #[error("error in keyword '{keyword}' at position {pos}: unexpected EOF")]
    UnexpectedEof { keyword: String, pos: usize },
    #[error("error in keyword '{keyword}' at position {pos}: unexpected character")]
    UnexpectedChar { keyword: String, pos: usize },
}

/// The reserved DSL metacharacters (mirrors `reservedChars`).
fn is_reserved(c: char) -> bool {
    matches!(c, '(' | ')' | '|' | '*' | '?' | '+' | '#' | ' ')
}

/// Outcome of `lex_class_token`: a token pattern, or the EOF control signal
/// (Go's `ErrEOF`, which is flow control, not a real error).
enum ClassOutcome {
    Token(String),
    Eof,
}

/// Internal per-keyword compiler state.
struct KeywordsLexer {
    lexer: Lexer,
    keyword: String,
}

impl KeywordsLexer {
    fn new(keyword: &str) -> Self {
        KeywordsLexer {
            lexer: Lexer::new(keyword),
            keyword: keyword.to_string(),
        }
    }

    fn err_eof(&self) -> KeywordError {
        KeywordError::UnexpectedEof {
            keyword: self.keyword.clone(),
            pos: self.lexer.pos(),
        }
    }

    fn err_char(&self) -> KeywordError {
        KeywordError::UnexpectedChar {
            keyword: self.keyword.clone(),
            pos: self.lexer.pos(),
        }
    }

    /// Port of `lexGroupToken`. Returns the `(?:…)` group pattern.
    fn lex_group_token(&mut self, parens: bool) -> Result<String, KeywordError> {
        let mut group_tokens: Vec<String> = Vec::new();

        'outer: loop {
            let mut tokens: Vec<String> = Vec::new();

            macro_rules! add_group {
                () => {
                    if !tokens.is_empty() {
                        // rex.Group.NonCaptured(tokens...) => "(?:" + concat + ")"
                        group_tokens.push(format!("(?:{})", tokens.concat()));
                    }
                    tokens.clear();
                };
            }

            loop {
                if parens {
                    if self.lexer.read_char('(') {
                        self.lexer.backup();
                        return Err(self.err_char());
                    }
                    if self.lexer.read_char(')') {
                        add_group!();
                        break 'outer;
                    }
                } else if self.lexer.read_char('(') {
                    let group = self.lex_group_token(true)?;
                    if self.lexer.read_char('?') {
                        tokens.push(format!("{group}?"));
                    } else {
                        tokens.push(group);
                    }
                    continue;
                }

                if self.lexer.read_char('|') {
                    if tokens.is_empty() {
                        self.lexer.backup();
                        return Err(self.err_char());
                    }
                    add_group!();
                    continue 'outer;
                }

                match self.lex_class_with_modifier_token()? {
                    ClassOutcome::Eof => {
                        if parens {
                            return Err(self.err_eof());
                        }
                        add_group!();
                        break 'outer;
                    }
                    ClassOutcome::Token(tk) => {
                        tokens.push(tk);
                        continue;
                    }
                }
            }
        }

        if group_tokens.is_empty() {
            return Err(self.err_eof());
        }

        // rex.Group.Composite(groupTokens...).NonCaptured()
        if group_tokens.len() == 1 {
            Ok(format!("(?:{})", group_tokens[0]))
        } else {
            Ok(format!("(?:{})", group_tokens.join("|")))
        }
    }

    /// Port of `lexClassWithModifierToken`.
    fn lex_class_with_modifier_token(&mut self) -> Result<ClassOutcome, KeywordError> {
        if self.lexer.read_char('*') {
            // regex.AnyWordChar().Repeat().ZeroOrMore()
            return Ok(ClassOutcome::Token(format!("{}*", any_word_char())));
        }

        let tk = match self.lex_class_token()? {
            ClassOutcome::Eof => return Ok(ClassOutcome::Eof),
            ClassOutcome::Token(tk) => tk,
        };

        match self.lexer.read() {
            None => Ok(ClassOutcome::Token(tk)),
            Some('?') => Ok(ClassOutcome::Token(format!("{tk}?"))),
            Some('+') => Ok(ClassOutcome::Token(format!("{tk}+"))),
            Some(_) => {
                self.lexer.backup();
                Ok(ClassOutcome::Token(tk))
            }
        }
    }

    /// Port of `lexClassToken`.
    fn lex_class_token(&mut self) -> Result<ClassOutcome, KeywordError> {
        let ch = match self.lexer.read() {
            None => return Ok(ClassOutcome::Eof),
            Some(c) => c,
        };

        match ch {
            '\\' => {
                let exact = self.lexer.read().ok_or_else(|| self.err_eof())?;
                Ok(ClassOutcome::Token(single_char(exact)))
            }
            _ if is_word_char(ch) => {
                let lc = ch.to_lowercase().to_string();
                if ch.to_string() != lc {
                    // Uppercase keyword source is rejected (Go errors here).
                    return Err(self.err_char());
                }
                let uc = ch.to_uppercase().to_string();
                if lc == uc {
                    // Caseless (digits, symbols-as-letters): a literal single char.
                    Ok(ClassOutcome::Token(single_char(ch)))
                } else {
                    // rex.Chars.Runes(ucChar + lcChar) => "[Uu]" (non-ASCII
                    // members hex-escaped, e.g. accented letters -> [\xC1\xE1]).
                    Ok(ClassOutcome::Token(runes_class(&format!("{uc}{lc}"))))
                }
            }
            '#' => Ok(ClassOutcome::Token(digits())),
            ' ' => Ok(ClassOutcome::Token(any_non_word_char().to_string())),
            _ => {
                if is_reserved(ch) {
                    self.lexer.backup();
                    Err(self.err_char())
                } else {
                    Ok(ClassOutcome::Token(single_char(ch)))
                }
            }
        }
    }
}

/// Port of `NewRexTokensFromKeywords`: compile each keyword to its group
/// pattern, de-duplicating repeated keywords (Go's `usedKeywords` set) while
/// preserving first-seen order.
pub fn rex_tokens_from_keywords(kws: &[&str]) -> Result<Vec<String>, KeywordError> {
    if kws.is_empty() {
        return Err(KeywordError::NoKeywords);
    }

    let mut seen: Vec<&str> = Vec::new();
    let mut tokens = Vec::with_capacity(kws.len());
    for &kw in kws {
        if seen.contains(&kw) {
            continue;
        }
        seen.push(kw);
        let mut l = KeywordsLexer::new(kw);
        tokens.push(l.lex_group_token(false)?);
    }

    Ok(tokens)
}

/// Port of `NewRegexFromKeywords`: wrap the keyword alternation in the
/// word-boundary context and return the full pattern string.
///
/// `(?:^|[^\p{L}0-9]+)(<alt>)(?:$|[^\p{L}0-9]+)`
///
/// The middle group is *captured* (group 1 = the matched keyword text).
pub fn regex_pattern_from_keywords(kws: &[&str]) -> Result<String, KeywordError> {
    let tokens = rex_tokens_from_keywords(kws)?;
    Ok(format!(
        "(?:^|{nw}+){cap}(?:$|{nw}+)",
        nw = any_non_word_char(),
        cap = capture_alternation(&tokens),
    ))
}

/// Build the captured alternation group `(<tok1>|<tok2>|…)` — mirrors
/// `rex.Group.Composite(tokens...)` (a captured group; `len==1` drops the `|`).
pub(crate) fn capture_alternation(tokens: &[String]) -> String {
    if tokens.len() == 1 {
        format!("({})", tokens[0])
    } else {
        format!("({})", tokens.join("|"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testsupport::{adapt_go_pattern, load_go_dsl_cases};

    // Every keyword-DSL case (glob `*`, `#`, ` `, `?`, `+`, `(…)`, `|`, `\x`,
    // hyphen) must compile byte-identically to Go's `rex` output after the ASCII
    // adaptation. Oracle: testdata/parity/release/patterns.jsonl.
    #[test]
    fn dsl_compiler_matches_go() {
        let cases = load_go_dsl_cases();
        assert!(!cases.is_empty(), "no DSL cases loaded");
        for (id, kws, go_pattern) in cases {
            let refs: Vec<&str> = kws.iter().map(String::as_str).collect();
            let got = regex_pattern_from_keywords(&refs)
                .unwrap_or_else(|e| panic!("{id}: compile failed: {e}"));
            assert_eq!(got, adapt_go_pattern(&go_pattern), "DSL mismatch for {id}");
            // And the emitted pattern must be a valid Rust regex.
            regex::Regex::new(&got).unwrap_or_else(|e| panic!("{id}: invalid regex: {e}"));
        }
    }

    #[test]
    fn empty_keywords_errors() {
        assert_eq!(
            regex_pattern_from_keywords(&[]).unwrap_err(),
            KeywordError::NoKeywords
        );
    }

    #[test]
    fn deduplicates_repeated_keywords() {
        // Go's usedKeywords set skips repeats; two "ab" collapse to one token.
        let tokens = rex_tokens_from_keywords(&["ab", "ab", "cd"]).unwrap();
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn unbalanced_paren_errors() {
        // Unterminated group -> unexpected EOF (mirrors lexGroupToken parens).
        assert!(matches!(
            regex_pattern_from_keywords(&["a(b"]),
            Err(KeywordError::UnexpectedEof { .. })
        ));
    }
}
