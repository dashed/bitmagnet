//! The **one** char-ngram analyzer the path-FTS index uses for BOTH the writer
//! and the query parser — the CJK-correctness invariant (PS-T3).
//!
//! # Why char-ngram, not the bitmagnet word tokenizer
//!
//! The main search index ([`crate::tokenizer`]) ports Go's word-level
//! `TokenizeFlat`, which makes a mid-run CJK substring un-findable (a CJK run is
//! one token → recall 0.0037, measured EXP-D). The path typeahead must answer
//! arbitrary substrings in any script, so it tokenizes the path into overlapping
//! **character** ngrams of width 2..=3 (`NgramTokenizer::new(2, 3, false)`), lower-
//! cased. A substring query is then a **conjunction** of its own grams
//! (`build_path_query`), which matches CJK at recall 1.0 (EXP-D).
//!
//! # The single-tokenizer invariant
//!
//! Both the writer (when indexing each file path) and the query side (when
//! tokenizing the user's substring) MUST run the *identical* analyzer, or the
//! query grams won't equal the indexed grams. The writer registers
//! [`register_path_tokenizer`] on the index; the query side calls
//! [`path_grams`], which constructs the same analyzer via [`path_analyzer`].
//! There is exactly one constructor, so they cannot drift.
//!
//! # Positions are dead weight
//!
//! Every ngram token carries `position = 0` (`ngram_tokenizer.rs:168`), so the
//! path field is indexed `WithFreqs` (NOT `WithFreqsAndPositions`) — positions
//! were 83.5 % of the index and the conjunction query never reads them
//! (PS-T3 §2.2, validated).

use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer, TokenStream};
use tantivy::Index;

/// Name the path ngram analyzer is registered under, and the name the
/// `path_grams` field binds to. The writer and query parser share it.
pub const PATH_TOKENIZER_NAME: &str = "path_ngram";

/// Smallest ngram width. `min = 2` means a 1-char query produces **no** gram
/// (`EmptyQuery`) — the broadest-possible firing query is 2 chars = one bigram,
/// which the server guard + client min-chars=3 keep off the hot path (PS-T3 §1).
pub const NGRAM_MIN: usize = 2;
/// Largest ngram width. `max = 3` gives the bi/tri-gram shape the 14.0 GiB keyed
/// index was measured at; the `max_gram` term enforces contiguity so a ≥3-char
/// query's gram-conjunction is far more selective than any single bigram.
pub const NGRAM_MAX: usize = 3;

// Compile-time sanity: a valid (non-empty, non-inverted) ngram window.
const _: () = assert!(NGRAM_MIN >= 1 && NGRAM_MIN <= NGRAM_MAX);

/// Build the path ngram [`TextAnalyzer`]: `NgramTokenizer(2, 3, prefix_only=false)`
/// then a `LowerCaser` filter (case-insensitive substring matching). Lower-casing
/// happens in the filter, so the indexed grams and the query grams fold identically.
///
/// # Panics
/// Never in practice: `NgramTokenizer::new(2, 3, false)` only errors on an
/// invalid `min/max` window, and these are compile-time constants with `2 <= 3`.
#[must_use]
pub fn path_analyzer() -> TextAnalyzer {
    let ngram = NgramTokenizer::new(NGRAM_MIN, NGRAM_MAX, false)
        .expect("NGRAM_MIN <= NGRAM_MAX and both > 0");
    TextAnalyzer::builder(ngram).filter(LowerCaser).build()
}

/// Register [`path_analyzer`] under [`PATH_TOKENIZER_NAME`] on `index`.
///
/// Tokenizers are runtime state (not persisted in the index meta), so this must
/// run on every freshly opened/created index before it is read or written — the
/// same idiom as [`crate::index::register_tokenizer`].
pub fn register_path_tokenizer(index: &Index) {
    index
        .tokenizers()
        .register(PATH_TOKENIZER_NAME, path_analyzer());
}

/// Tokenize `text` through the path ngram analyzer and return its **distinct**
/// grams in first-seen order. Used by the query side so the per-keystroke
/// substring is reduced to exactly the grams the writer indexed.
///
/// Distinctness matters: a substring like `aaa` yields `aa, aa, aaa`; the query
/// is a conjunction, so the duplicate `aa` clause is redundant and dropped.
#[must_use]
pub fn path_grams(text: &str) -> Vec<String> {
    let mut analyzer = path_analyzer();
    let mut stream = analyzer.token_stream(text);
    let mut seen: Vec<String> = Vec::new();
    while stream.advance() {
        let gram = stream.token().text.clone();
        if !seen.iter().any(|g| g == &gram) {
            seen.push(gram);
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::path_grams;

    #[test]
    fn one_char_yields_no_gram() {
        // min_gram = 2 → a single char produces nothing (EmptyQuery upstream).
        assert!(path_grams("a").is_empty());
    }

    #[test]
    fn two_chars_yield_one_bigram() {
        assert_eq!(path_grams("ab"), vec!["ab"]);
    }

    #[test]
    fn three_chars_yield_a_distinct_gram_conjunction() {
        // bi+tri grams over "abc", in NgramTokenizer emission order (by start
        // offset then length): ab, abc (offset 0), then bc (offset 1).
        assert_eq!(path_grams("abc"), vec!["ab", "abc", "bc"]);
    }

    #[test]
    fn lowercases_so_query_folds_like_the_index() {
        assert_eq!(path_grams("AB"), path_grams("ab"));
    }

    #[test]
    fn duplicate_grams_are_deduplicated() {
        // "aaaa": bigrams all "aa"; trigrams all "aaa" → only two distinct grams.
        assert_eq!(path_grams("aaaa"), vec!["aa", "aaa"]);
    }

    #[test]
    fn cjk_runs_produce_char_ngrams_not_one_token() {
        // The whole point: a CJK run becomes overlapping char-grams (recall 1.0),
        // unlike the word tokenizer which would emit one token per run.
        let grams = path_grams("北京市");
        assert!(grams.contains(&"北京".to_string()));
        assert!(grams.contains(&"京市".to_string()));
        assert!(grams.contains(&"北京市".to_string()));
    }

}
