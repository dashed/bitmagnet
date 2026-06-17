//! A Tantivy tokenizer replicating bitmagnet's Go `TokenizeFlat()` so the Rust
//! index tokenizes identically to the current Postgres-backed search.
//!
//! `TokenizeFlat` produces a flat, order-preserving, de-duplicated token list
//! with Postgres `simple`-dictionary semantics: no stemming and no stop-words.
//! The Phase 3 port must reproduce these steps exactly:
//!
//! 1. Lower-case the input (Unicode-aware).
//! 2. Strip accents/diacritics: normalize to NFD and drop combining marks, so
//!    `Pokémon` becomes `pokemon`.
//! 3. Replace every non-alphanumeric character with a separator, so `S.W.A.T`
//!    becomes `s w a t` and `Spider-Man` becomes `spider man`.
//! 4. Split on whitespace and discard empty tokens.
//! 5. De-duplicate while preserving first-seen order.
//!
//! Worked examples the Phase 3 implementation and its tests must satisfy:
//!
//! | Input        | Tokens           |
//! |--------------|------------------|
//! | `Pokémon`    | `[pokemon]`      |
//! | `S.W.A.T`    | `[s, w, a, t]`   |
//! | `Spider-Man` | `[spider, man]`  |
//!
//! Phase 3 will expose this as a [`tantivy::tokenizer::Tokenizer`] registered on
//! the index so the writer and the query parser share one tokenization path.

/// Tokenize `input` the way the Go `TokenizeFlat()` does.
///
/// See the module documentation for the exact algorithm and worked examples.
///
/// # Panics
/// Always panics — not implemented until Phase 3.
#[must_use]
pub fn tokenize_flat(_input: &str) -> Vec<String> {
    unimplemented!("Phase 3: port Go TokenizeFlat (lower-case, strip accents, split, dedupe)")
}
