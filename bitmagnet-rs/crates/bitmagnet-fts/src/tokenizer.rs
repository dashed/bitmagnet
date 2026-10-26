//! Ported verbatim from the Go `internal/database/fts` tokenizer
//! (Go `src/query.rs` `app_query_to_tsquery` + `src/tokenizer.rs` +
//! `src/tokenizer/tables.rs`, go-unidecode v0.2.0). This crate is the single,
//! Tantivy-free home for the port, shared by every FTS consumer.
//!
//! The std-only tokenizer core that replicates bitmagnet's Go
//! `fts.TokenizeFlat()` byte-for-byte, so this crate tokenizes identically to
//! the current Postgres-backed full-text search.
//!
//! # The real algorithm (NOT accent-stripping + dedupe)
//!
//! `TokenizeFlat` is a rune-by-rune lexer (see
//! `internal/database/fts/tokenizer.go` + `internal/lexer/lexer.go`). It builds
//! up a current token ("lexeme") and flushes it on a word boundary. There is
//! **no** NFD normalization and **no** de-duplication — tokens are emitted in
//! order, with repeats. For each rune of the input:
//!
//! 1. A *word char* is `unicode.IsLetter(r) || unicode.IsDigit(r)` (Unicode
//!    category `L*` or `Nd`). A non-word char flushes the current lexeme (a
//!    word boundary). Note this is narrower than Rust's `char::is_alphabetic`
//!    (which also accepts `Nl`, e.g. roman numerals `Ⅷ`, and `Other_Alphabetic`).
//! 2. A word char is lower-cased with Go's single-rune `unicode.ToLower` (which
//!    differs from Rust's one-to-many `char::to_lowercase`, e.g. `İ` → `i`).
//! 3. If the lower-cased rune is ASCII (`< 0x7F`) it is appended verbatim.
//! 4. Otherwise it is transliterated through go-unidecode's tables:
//!    * runes `> U+1FFF` are treated as "non-breaking-language" (CJK, etc.):
//!      the lexeme is flushed *before* the rune and again *after* it, so each
//!      such rune becomes its own token;
//!    * the table substitution has `'` → `_sq_`, `\` → `_bs_` applied and is
//!      then trimmed (the substitution itself is **not** lower-cased — that is
//!      why CJK tokens keep their capitals, e.g. `北` → `Bei`);
//!    * an empty substitution also flushes the lexeme.
//!
//! Worked examples (verified against the Go implementation — see the fixtures
//! under `tests/fixtures/`):
//!
//! | Input        | Tokens                  | Why                                   |
//! |--------------|-------------------------|---------------------------------------|
//! | `Pokémon`    | `[pokemon]`             | `é` → `e` via transliteration         |
//! | `S.W.A.T`    | `[s, w, a, t]`          | `.` is a word boundary                |
//! | `Spider-Man` | `[spider, man]`         | `-` is a word boundary                |
//! | `straße`     | `[strasse]`             | `ß` → `ss`                            |
//! | `北京`        | `[Bei, Jing]`           | CJK: one token per rune, not lowered  |
//! | `½ cup`      | `[cup]`                 | `½` is not a letter/digit             |
//! | `it's`       | `[it, s]`               | a literal `'` is a word boundary      |
//! | `ŉ`          | `[_sq_n]`               | `'` *inside* a substitution → `_sq_`  |
//!

use std::borrow::Cow;

#[path = "tables.rs"]
mod tables;

/// `unicode.MaxASCII` from Go: runes `< 0x7F` take the verbatim-append path.
const MAX_ASCII: u32 = 0x7F;
/// Cutoff above which a rune is a "non-breaking language" rune (one token each).
const NON_BREAKING_CUTOFF: u32 = 0x1FFF;

/// Is `c` a word char per Go's `lexer.IsWordChar` (`IsLetter || IsDigit`)?
///
/// The Go-generated table is the only correct source: `char::is_alphanumeric()`
/// is 12,322 code points wider (`² ³ ¼ ½ ①②③`, …) and `is_alphabetic()` is
/// 11,317 wider than `unicode.IsLetter`. Shared with the app-query lexer, which
/// must classify identically or it emits the wrong tsquery operator.
pub(crate) fn is_go_word_char(c: char) -> bool {
    let cp = c as u32;
    let ranges = tables::WORD_CHAR_RANGES;
    // Index of the first range whose `lo` is greater than `cp`; the candidate
    // range that could contain `cp` is therefore the one just before it.
    let idx = ranges.partition_point(|&(lo, _)| lo <= cp);
    idx > 0 && cp <= ranges[idx - 1].1
}

/// Go's single-rune `unicode.ToLower`. Falls back to identity when there is no
/// mapping (which is what Go does too).
fn to_lower(c: char) -> char {
    let cp = c as u32;
    match tables::LOWER_MAP.binary_search_by_key(&cp, |&(src, _)| src) {
        Ok(i) => char::from_u32(tables::LOWER_MAP[i].1).unwrap_or(c),
        Err(_) => c,
    }
}

/// Look up the raw (pre-processing) go-unidecode substitution for `c`, or
/// `None` when the rune's section/position has no table entry — exactly the
/// `table.Tables[section]` + `len(tb) > position` guards in the Go code.
fn translit_lookup(c: char) -> Option<&'static str> {
    let cp = c as u32;
    let section = cp >> 8;
    let position = (cp & 0xFF) as usize;
    let i = tables::TRANSLIT_SECTIONS
        .binary_search_by_key(&section, |&(s, _)| s)
        .ok()?;
    let table = tables::TRANSLIT_SECTIONS[i].1;
    table.get(position).copied()
}

/// Apply the Go substitution post-processing: `ReplaceAll("'", "_sq_")`,
/// `ReplaceAll("\\", "_bs_")`, then `TrimSpace`. Borrows when no replacement is
/// needed (the common case — most substitutions are plain ASCII words).
fn process_subst(raw: &'static str) -> Cow<'static, str> {
    if raw.contains('\'') || raw.contains('\\') {
        let replaced = raw.replace('\'', "_sq_").replace('\\', "_bs_");
        Cow::Owned(replaced.trim().to_owned())
    } else {
        Cow::Borrowed(raw.trim())
    }
}

/// One emitted token together with the byte range of the input that produced
/// it. The text is transliterated, so it need not equal the input slice
/// `input[offset_from..offset_to]`; the range is the span of input runes that
/// contributed to the token (useful for highlighting/positions).
#[allow(dead_code)] // Offsets are consumed only by the excluded Tantivy adapter.
struct TokenSpan {
    text: String,
    offset_from: usize,
    offset_to: usize,
}

/// Accumulator mirroring the Go `readPhrase` closures (`breakWord`/`appendStr`)
/// but flattened across phrases — the flat token sequence is exactly the
/// sequence of `breakWord` flushes, so phrase grouping is irrelevant here.
#[derive(Default)]
struct Accumulator {
    out: Vec<TokenSpan>,
    lexeme: String,
    /// Byte offset in the input where the current lexeme started.
    start: usize,
    /// Byte offset in the input just past the last rune that fed the lexeme.
    end: usize,
}

impl Accumulator {
    /// `breakWord`: flush the current lexeme as a token, if non-empty.
    fn break_word(&mut self) {
        if !self.lexeme.is_empty() {
            self.out.push(TokenSpan {
                text: std::mem::take(&mut self.lexeme),
                offset_from: self.start,
                offset_to: self.end,
            });
        }
    }

    /// Append the verbatim ASCII rune `ch` (from input bytes `cs..ce`).
    fn push_ascii(&mut self, ch: char, cs: usize, ce: usize) {
        if self.lexeme.is_empty() {
            self.start = cs;
        }
        self.lexeme.push(ch);
        self.end = ce;
    }

    /// Append a (non-empty) substitution `s` contributed by input bytes `cs..ce`.
    fn push_subst(&mut self, s: &str, cs: usize, ce: usize) {
        if s.is_empty() {
            return; // Empty substitution contributes no bytes (matches `appendStr("")`).
        }
        if self.lexeme.is_empty() {
            self.start = cs;
        }
        self.lexeme.push_str(s);
        self.end = ce;
    }
}

/// Core tokenizer: produce tokens with their input byte spans, replicating
/// `fts.TokenizeFlat` exactly.
fn tokenize_spans(input: &str) -> Vec<TokenSpan> {
    let mut acc = Accumulator::default();

    for (cs, raw_ch) in input.char_indices() {
        let ce = cs + raw_ch.len_utf8();

        if !is_go_word_char(raw_ch) {
            acc.break_word();
            continue;
        }

        let ch = to_lower(raw_ch);
        let cp = ch as u32;

        if cp < MAX_ASCII {
            acc.push_ascii(ch, cs, ce);
            continue;
        }

        let non_breaking = cp > NON_BREAKING_CUTOFF;
        if non_breaking {
            acc.break_word();
        }

        if let Some(raw) = translit_lookup(ch) {
            let subst = process_subst(raw);
            acc.push_subst(&subst, cs, ce);
            // `subst` is already trimmed, so `ends_with(' ')` is always false;
            // it is kept to mirror the Go condition line-for-line.
            if non_breaking || subst.is_empty() || subst.ends_with(' ') {
                acc.break_word();
            }
        }
        // No table entry: contribute nothing. If this rune is non-breaking the
        // preceding lexeme was already flushed above.
    }

    acc.break_word(); // EOF flush.
    acc.out
}

/// Tokenize `input` exactly as Go's `fts.TokenizeFlat()` does: a flat,
/// order-preserving (non-deduplicated) list of tokens.
///
/// See the [module documentation](self) for the algorithm and worked examples.
#[must_use]
pub fn tokenize_flat(input: &str) -> Vec<String> {
    tokenize_spans(input)
        .into_iter()
        .map(|span| span.text)
        .collect()
}
