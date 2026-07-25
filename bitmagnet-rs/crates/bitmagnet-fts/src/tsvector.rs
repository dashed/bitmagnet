//! Port of Go `internal/database/fts/tsvector.go` — the in-memory `tsvector`
//! builder the ingest path uses to populate the `content.tsv` / `torrent.tsv`
//! columns.
//!
//! Only the write side is ported here (the B′-0 seam needs
//! `model.Content.UpdateTsv`): the map representation, [`Tsvector::add_text`],
//! and the `String()` rendering. `ParseTsvector` and the GORM `Value()` glue
//! have no Rust consumer yet.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::tokenize_flat;

/// The largest single lexeme PostgreSQL accepts in a `tsvector` (Go
/// `fts.MaxLexemeBytes`). Longer lexemes are silently dropped rather than
/// failing the whole insert.
pub const MAX_LEXEME_BYTES: usize = 2046;

/// A lexeme's position label weight (Go `fts.TsvectorWeight`, a rune).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TsvectorWeight {
    A,
    B,
    C,
    D,
}

impl TsvectorWeight {
    /// The PostgreSQL label character.
    #[must_use]
    pub fn as_char(self) -> char {
        match self {
            TsvectorWeight::A => 'A',
            TsvectorWeight::B => 'B',
            TsvectorWeight::C => 'C',
            TsvectorWeight::D => 'D',
        }
    }
}

/// Go `fts.Tsvector` — `map[lexeme]map[position]weight`.
///
/// Go's map iteration order is randomised, but every observable output
/// (`String()`) sorts by lexeme then position, so a [`BTreeMap`] is
/// order-equivalent *and* makes the Rust rendering deterministic by
/// construction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Tsvector(BTreeMap<String, BTreeMap<i32, TsvectorWeight>>);

impl Tsvector {
    /// An empty vector.
    #[must_use]
    pub fn new() -> Self {
        Tsvector(BTreeMap::new())
    }

    /// Whether the vector holds no lexemes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of distinct lexemes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The raw lexeme → (position → weight) map.
    #[must_use]
    pub fn labels(&self) -> &BTreeMap<String, BTreeMap<i32, TsvectorWeight>> {
        &self.0
    }

    /// Go `Tsvector.AddText` — tokenize `text` and append its lexemes at
    /// increasing positions with `weight`.
    ///
    /// The position rule is copied verbatim: `next_pos` starts one past the
    /// highest position already present, and when the vector is non-empty an
    /// **extra** position is skipped, leaving a one-slot gap between texts so a
    /// phrase (`<->`) match cannot straddle two different fields.
    pub fn add_text(&mut self, text: &str, weight: TsvectorWeight) {
        let mut next_pos = 1;
        for labels in self.0.values() {
            for pos in labels.keys() {
                if *pos >= next_pos {
                    next_pos = *pos + 1;
                }
            }
        }
        if next_pos > 1 {
            next_pos += 1;
        }

        for lexeme in tokenize_flat(text) {
            if lexeme.len() > MAX_LEXEME_BYTES {
                continue;
            }
            self.0.entry(lexeme).or_default().insert(next_pos, weight);
            next_pos += 1;
        }
    }
}

impl fmt::Display for Tsvector {
    /// Go `Tsvector.String()`: lexemes sorted ascending, each rendered as a
    /// force-quoted literal followed by its `:`-joined position labels. Position
    /// `0` labels are dropped, and a `D` weight is left implicit (PostgreSQL's
    /// default), exactly as Go does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (lexeme, labels_map) in &self.0 {
            if !first {
                f.write_str(" ")?;
            }
            first = false;

            f.write_str(&quote_lexeme(lexeme))?;

            // BTreeMap iterates positions ascending, matching Go's explicit
            // sort by position.
            let mut label_first = true;
            for (pos, weight) in labels_map {
                if *pos == 0 {
                    continue;
                }
                f.write_str(if label_first { ":" } else { "," })?;
                label_first = false;
                write!(f, "{pos}")?;
                if *weight != TsvectorWeight::D {
                    f.write_str(&weight.as_char().to_string())?;
                }
            }
        }
        Ok(())
    }
}

/// Go `quoteLexeme(str, true)` — always quote, doubling embedded `'`.
fn quote_lexeme(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_text_assigns_ascending_positions() {
        let mut tsv = Tsvector::new();
        tsv.add_text("The Matrix", TsvectorWeight::A);
        assert_eq!(tsv.to_string(), "'matrix':2A 'the':1A");
    }

    /// The gap rule: a second `add_text` starts at `max + 2`, not `max + 1`.
    #[test]
    fn add_text_leaves_a_positional_gap_between_texts() {
        let mut tsv = Tsvector::new();
        tsv.add_text("matrix", TsvectorWeight::A);
        tsv.add_text("1999", TsvectorWeight::B);
        assert_eq!(tsv.to_string(), "'1999':3B 'matrix':1A");
    }

    /// A `D` weight renders bare (PostgreSQL's implicit default).
    #[test]
    fn weight_d_label_is_implicit() {
        let mut tsv = Tsvector::new();
        tsv.add_text("action", TsvectorWeight::D);
        assert_eq!(tsv.to_string(), "'action':1");
    }

    #[test]
    fn repeated_lexeme_accumulates_positions() {
        let mut tsv = Tsvector::new();
        tsv.add_text("matrix", TsvectorWeight::A);
        tsv.add_text("matrix", TsvectorWeight::D);
        assert_eq!(tsv.to_string(), "'matrix':1A,3");
    }

    #[test]
    fn oversized_lexemes_are_dropped() {
        let mut tsv = Tsvector::new();
        let oversized = "a".repeat(MAX_LEXEME_BYTES + 1);
        tsv.add_text(&oversized, TsvectorWeight::A);
        assert!(tsv.is_empty(), "oversized lexeme must be skipped");

        let at_limit = "b".repeat(MAX_LEXEME_BYTES);
        tsv.add_text(&at_limit, TsvectorWeight::A);
        assert_eq!(tsv.len(), 1, "a lexeme exactly at the limit is kept");
    }

    #[test]
    fn apostrophes_are_doubled_in_the_rendering() {
        assert_eq!(quote_lexeme("dD'Dd'"), "'dD''Dd'''");
    }
}
