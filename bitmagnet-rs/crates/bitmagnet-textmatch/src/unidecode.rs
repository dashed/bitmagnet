//! Port of `github.com/mozillazg/go-unidecode` v0.2.0 `Unidecode`, which Go's
//! `levenshteinNormalizeString` applies *before* `regex.NormalizeString`
//! (`internal/classifier/util.go:58-60`).
//!
//! # Table provenance — reused, not re-ported
//!
//! The transliteration tables are **not** copied here. The crate includes the
//! already-in-tree generated table module from `bitmagnet-fts` verbatim via a
//! `#[path]` `mod` in [`crate`], so there is exactly one copy of
//! `go-unidecode`'s `table.Tables` reachable from this workspace and it stays
//! in lockstep with the FTS tokenizer. That file is machine-generated from the
//! real Go package (Go 1.23.6 / Unicode 15.0.0 / go-unidecode v0.2.0) and
//! stores the **raw**, pre-processing substitution strings — which is exactly
//! what plain `Unidecode` needs. (`bitmagnet-fts`'s own `' -> _sq_`,
//! `\ -> _bs_`, trim post-processing is FTS-tokenizer-specific and is applied
//! by that crate, not by `Unidecode`; it is deliberately not applied here.)

/// `unicode.MaxASCII` from Go. Note the Go comparison is `r < unicode.MaxASCII`
/// — **strictly** less than `0x7F` — so `DEL` (U+007F) itself takes the table
/// path rather than the verbatim path. It maps to an empty table entry and is
/// therefore dropped.
const MAX_ASCII: u32 = 0x7F;

/// Go drops every rune above this before touching the tables.
const MAX_TABLE_RUNE: u32 = 0x000E_FFFF;

/// The raw go-unidecode substitution for `c`, or `None` when the rune's
/// section is absent or its position is past the end of that section's
/// slice — the `table.Tables[section]` + `len(tb) > position` guards in Go.
fn lookup(c: char) -> Option<&'static str> {
    let cp = c as u32;
    let section = cp >> 8;
    let position = (cp & 0xFF) as usize;
    let i = crate::tables::TRANSLIT_SECTIONS
        .binary_search_by_key(&section, |&(s, _)| s)
        .ok()?;
    crate::tables::TRANSLIT_SECTIONS[i].1.get(position).copied()
}

/// Port of Go `unidecode.Unidecode`: transliterate Unicode text into plain
/// 7-bit ASCII.
#[must_use]
pub fn unidecode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());

    for c in input.chars() {
        let cp = c as u32;
        if cp < MAX_ASCII {
            out.push(c);
            continue;
        }

        if cp > MAX_TABLE_RUNE {
            continue;
        }

        if let Some(subst) = lookup(c) {
            out.push_str(subst);
        }
    }

    out
}
