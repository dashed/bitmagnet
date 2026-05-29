//! A Tantivy tokenizer that replicates bitmagnet's Go `fts.TokenizeFlat()`
//! byte-for-byte, so the Rust index tokenizes identically to the current
//! Postgres-backed full-text search. In shadow mode the two indexes are
//! compared directly, so *exact* parity here is the whole ballgame.
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
//! The same [`BitmagnetTokenizer`] is registered (under [`TOKENIZER_NAME`]) for
//! both the index writer and the query parser, so they share one tokenization
//! path.

use std::borrow::Cow;

use tantivy::tokenizer::{TextAnalyzer, Token, TokenStream, Tokenizer};

mod tables;

/// Name under which [`BitmagnetTokenizer`] is registered in the index's
/// `TokenizerManager`. The writer and the query parser must use the same name.
pub const TOKENIZER_NAME: &str = "bitmagnet";

/// `unicode.MaxASCII` from Go: runes `< 0x7F` take the verbatim-append path.
const MAX_ASCII: u32 = 0x7F;
/// Cutoff above which a rune is a "non-breaking language" rune (one token each).
const NON_BREAKING_CUTOFF: u32 = 0x1FFF;

/// Is `c` a word char per Go's `lexer.IsWordChar` (`IsLetter || IsDigit`)?
fn is_word_char(c: char) -> bool {
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

        if !is_word_char(raw_ch) {
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

/// The Tantivy [`Tokenizer`] implementing the `TokenizeFlat` port. Prefer
/// registering the prebuilt [`analyzer()`] under [`TOKENIZER_NAME`] over using
/// this type directly:
/// ```no_run
/// # use bitmagnet_search::tokenizer::{analyzer, TOKENIZER_NAME};
/// # let index: tantivy::Index = unimplemented!();
/// index.tokenizers().register(TOKENIZER_NAME, analyzer());
/// ```
#[derive(Clone, Default)]
pub struct BitmagnetTokenizer;

/// Token stream produced by [`BitmagnetTokenizer`]. Tokens are computed eagerly
/// (the algorithm is a single forward pass) and then replayed.
pub struct BitmagnetTokenStream {
    tokens: std::vec::IntoIter<Token>,
    current: Token,
}

impl Tokenizer for BitmagnetTokenizer {
    type TokenStream<'a> = BitmagnetTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let tokens: Vec<Token> = tokenize_spans(text)
            .into_iter()
            .enumerate()
            .map(|(position, span)| Token {
                offset_from: span.offset_from,
                offset_to: span.offset_to,
                position,
                text: span.text,
                position_length: 1,
            })
            .collect();
        BitmagnetTokenStream {
            tokens: tokens.into_iter(),
            current: Token::default(),
        }
    }
}

impl TokenStream for BitmagnetTokenStream {
    fn advance(&mut self) -> bool {
        match self.tokens.next() {
            Some(token) => {
                self.current = token;
                true
            }
            None => false,
        }
    }

    fn token(&self) -> &Token {
        &self.current
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.current
    }
}

/// The [`TextAnalyzer`] the index writer and query layer register under
/// [`TOKENIZER_NAME`] (the single source of truth for tokenizer identity).
///
/// It is the bare [`BitmagnetTokenizer`] with **no** token filters: the port
/// already lower-cases the input (via Go's `unicode.ToLower`) and folds inside
/// the tokenizer itself, so stacking e.g. a `LowerCaser` would double-process
/// and break parity — transliterated tokens keep their case on purpose
/// (`北` → `Bei`, never `bei`).
#[must_use]
pub fn analyzer() -> TextAnalyzer {
    TextAnalyzer::from(BitmagnetTokenizer)
}

#[cfg(test)]
mod tests {
    use super::{is_word_char, tokenize_flat, BitmagnetTokenizer, Token, TokenStream, Tokenizer};

    fn flat(input: &str) -> Vec<String> {
        tokenize_flat(input)
    }

    #[test]
    fn worked_examples_from_module_docs() {
        assert_eq!(flat("Pokémon"), ["pokemon"]);
        assert_eq!(flat("POKÉMON"), ["pokemon"]);
        assert_eq!(flat("S.W.A.T"), ["s", "w", "a", "t"]);
        assert_eq!(flat("Spider-Man"), ["spider", "man"]);
        assert_eq!(flat("straße"), ["strasse"]);
        assert_eq!(flat("北京"), ["Bei", "Jing"]);
        assert_eq!(flat("½ cup"), ["cup"]);
        assert_eq!(flat("it's"), ["it", "s"]);
        assert_eq!(flat("ŉ"), ["_sq_n"]);
    }

    #[test]
    fn no_dedupe_and_order_preserved() {
        assert_eq!(flat("a a b a"), ["a", "a", "b", "a"]);
        assert_eq!(flat("the the"), ["the", "the"]);
    }

    #[test]
    fn empty_and_separator_only() {
        assert!(flat("").is_empty());
        assert!(flat("   ").is_empty());
        assert!(flat("...---...").is_empty());
        assert!(flat("😀👍🏽").is_empty());
    }

    #[test]
    fn word_char_excludes_nl_and_marks() {
        // Roman numerals are category Nl — letters to Rust's `is_alphabetic`,
        // but NOT word chars in Go. This is the key divergence the embedded
        // table protects against.
        assert!(!is_word_char('Ⅷ'));
        assert!('Ⅷ'.is_alphabetic()); // sanity: Rust std would wrongly accept it
        assert_eq!(flat("Ⅷ Ⅻ ⅸ"), Vec::<String>::new());
        // Combining marks (Mn) and connector punctuation are not word chars.
        assert_eq!(flat("e\u{0301}"), ["e"]); // e + combining acute
        assert_eq!(flat("snake_case"), ["snake", "case"]);
        // Letters and ASCII digits are word chars.
        assert!(is_word_char('a'));
        assert!(is_word_char('9'));
        assert!(is_word_char('中'));
    }

    #[test]
    fn turkish_dotted_i_uses_single_rune_lowercase() {
        // Go `ToLower('İ') == 'i'` (single rune); Rust's `to_lowercase` would
        // yield "i\u{307}" (two chars) and break parity.
        assert_eq!(flat("İstanbul"), ["istanbul"]);
        assert_eq!('İ'.to_lowercase().count(), 2); // sanity: std diverges
    }

    #[test]
    fn non_ascii_digits_accrete_into_one_token() {
        // Arabic-Indic digits are < U+1FFF (breaking), so they accumulate.
        assert_eq!(flat("٠١٢٣٤٥٦٧٨٩"), ["0123456789"]);
    }

    #[test]
    fn math_alphanumerics_are_per_rune_tokens() {
        // Each math-bold letter is > U+1FFF → its own token, case preserved.
        assert_eq!(flat("𝐇𝐞𝐥𝐥𝐨"), ["H", "e", "l", "l", "o"]);
    }

    #[test]
    fn offsets_point_at_the_contributing_input_bytes() {
        let spans = super::tokenize_spans("Spider-Man");
        let got: Vec<(&str, usize, usize)> = spans
            .iter()
            .map(|s| (s.text.as_str(), s.offset_from, s.offset_to))
            .collect();
        assert_eq!(got, [("spider", 0, 6), ("man", 7, 10)]);

        // `é` occupies bytes 3..5, so the single token spans the whole input.
        let spans = super::tokenize_spans("Pokémon");
        let got: Vec<(&str, usize, usize)> = spans
            .iter()
            .map(|s| (s.text.as_str(), s.offset_from, s.offset_to))
            .collect();
        assert_eq!(got, [("pokemon", 0, 8)]);

        // CJK runes are 3 bytes each and become one token apiece.
        let spans = super::tokenize_spans("a中b");
        let got: Vec<(&str, usize, usize)> = spans
            .iter()
            .map(|s| (s.text.as_str(), s.offset_from, s.offset_to))
            .collect();
        assert_eq!(got, [("a", 0, 1), ("Zhong", 1, 4), ("b", 4, 5)]);
    }

    #[test]
    fn tantivy_token_stream_matches_tokenize_flat() {
        let input = "Spider-Man 北京 café";
        let mut tokenizer = BitmagnetTokenizer;
        let mut stream = tokenizer.token_stream(input);

        let mut tokens: Vec<Token> = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().clone());
        }

        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, tokenize_flat(input));

        // Positions are dense and ascending from 0.
        let positions: Vec<usize> = tokens.iter().map(|t| t.position).collect();
        assert_eq!(positions, (0..tokens.len()).collect::<Vec<_>>());

        // Offsets are within bounds and non-decreasing.
        for token in &tokens {
            assert!(token.offset_from <= token.offset_to);
            assert!(token.offset_to <= input.len());
        }
    }

    #[test]
    fn analyzer_is_unfiltered_and_matches_tokenize_flat() {
        let mut a = super::analyzer();
        let input = "Spider-Man 北京 café STRASSE";
        let mut stream = a.token_stream(input);
        let mut texts: Vec<String> = Vec::new();
        while stream.advance() {
            texts.push(stream.token().text.clone());
        }
        assert_eq!(texts, tokenize_flat(input));
        // No stacked LowerCaser: a transliterated capital survives. (A naive
        // `.filter(LowerCaser)` would turn this into "bei" and break parity.)
        assert!(texts.contains(&"Bei".to_string()));
    }
}
