//! Fuzzy title matching for the B′ enrichment-parity lanes.
//!
//! A byte-faithful port of Go's `internal/classifier/util.go` —
//! `levenshteinFindBestMatch`, `levenshteinFindMinDistance` and
//! `levenshteinNormalizeString` — plus everything they stand on:
//! `regex.NormalizeString` ([`normalize_string`]), go-unidecode
//! ([`unidecode`]) and `agnivade/levenshtein` ([`compute_distance`]).
//!
//! This decides which content attaches to a torrent, so every observable
//! behaviour of the Go original is reproduced, not approximated:
//!
//! * **Threshold 5, inclusive.** Go seeds `minDistance` with
//!   `levenshteinThreshold + 1 == 6` and accepts on `distance < minDistance`,
//!   so a distance of exactly 5 matches and 6 does not
//!   ([`LEVENSHTEIN_THRESHOLD`]).
//! * **Strictly first-wins.** The comparison is `<`, never `<=`, so among
//!   candidates at equal distance the **first in input order** wins and no
//!   later candidate can displace it. The caller's ordering is therefore
//!   semantically load-bearing (see the note on candidate order below).
//! * **Early exit at distance 0.** The scan `break`s the moment a candidate
//!   normalizes to the target, so items after it are never evaluated.
//! * **Per-item minimum over multiple strings.** Go passes a
//!   `func(T) []string` returning e.g. `[Title, OriginalTitle]`; each item's
//!   score is the minimum distance over its own candidate strings, computed
//!   *before* it is compared against the running best. An item with an empty
//!   candidate list scores `-1` in Go (here: [`None`]) and is skipped
//!   entirely — it can never become the best match, even at index 0.
//!
//! 🔑 Levenshtein selection runs on the **Rust** side, over the ordered
//! candidate list returned by
//! `bitmagnet_classifier::ContentResolver::content_by_search`. Go's candidate
//! *ordering* is a nondeterministic PostgreSQL observation and is therefore
//! recorded by the parity tape; the first-wins tie-break over that ordering is
//! deterministic logic and belongs here.

// The generated go-unidecode / `unicode.ToLower` / word-char tables, included
// verbatim from `bitmagnet-fts` rather than copied, so this workspace holds
// exactly one transcription of `go-unidecode`'s `table.Tables`. See
// `src/unidecode.rs` for the rationale and `crates/bitmagnet-fts/tests/
// fixtures/README.md` for the generator + provenance.
//
// `WORD_CHAR_RANGES` is `IsLetter || IsDigit` (Go's FTS lexer rule), which is
// *not* the `[\p{L}\d]` class this crate's regex needs, so it goes unused here
// — hence the `dead_code` allowance.
#[allow(dead_code)]
#[path = "../../bitmagnet-fts/src/tables.rs"]
mod tables;

mod levenshtein;
mod normalize;
mod unidecode;
mod word_char_class;

use std::collections::HashSet;

pub use self::levenshtein::compute_distance;
pub use self::normalize::{
    normalize_string, word_token_pattern, word_token_regex, GO_WORD_TOKEN_PATTERN, WORD_CHAR_CLASS,
};
pub use self::unidecode::unidecode;

/// Go `const levenshteinThreshold = 5` (`internal/classifier/util.go:9`).
///
/// **Inclusive**: a distance of 5 is a match, 6 is not.
pub const LEVENSHTEIN_THRESHOLD: usize = 5;

/// Port of Go `levenshteinNormalizeString`: `regex.NormalizeString` applied to
/// `unidecode.Unidecode(str)`.
#[must_use]
pub fn levenshtein_normalize_string(input: &str) -> String {
    normalize_string(&unidecode(input))
}

/// Port of Go `levenshteinFindMinDistance`.
///
/// Returns the smallest normalized edit distance between `target` and any of
/// `candidates`, or `None` when there are no candidates (Go's `-1` sentinel).
/// Note this is **not** threshold-filtered — the threshold lives in
/// [`find_best_match_index`].
pub fn find_min_distance<I, S>(target: &str, candidates: I) -> Option<usize>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    find_min_distance_normalized(&levenshtein_normalize_string(target), candidates)
}

/// [`find_min_distance`] with the target already normalized.
///
/// Go re-normalizes the target once per item; hoisting it out of the loop is
/// the only deviation in this file and is observationally identical.
fn find_min_distance_normalized<I, S>(norm_target: &str, candidates: I) -> Option<usize>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    // Go memoizes on the *normalized* candidate. Distance is a pure function of
    // it, so this only skips repeated work — but it is transcribed to keep the
    // port line-for-line with the original.
    let mut tried: HashSet<String> = HashSet::new();
    let mut min_distance: Option<usize> = None;

    for candidate in candidates {
        let norm_candidate = levenshtein_normalize_string(candidate.as_ref());
        if tried.contains(&norm_candidate) {
            continue;
        }

        let distance = compute_distance(norm_target, &norm_candidate);
        if min_distance.is_none_or(|current| distance < current) {
            min_distance = Some(distance);
        }

        tried.insert(norm_candidate);
    }

    min_distance
}

/// Port of Go `levenshteinFindBestMatch`, returning the winning **index** into
/// `items`, or `None` when nothing scores at or below
/// [`LEVENSHTEIN_THRESHOLD`].
///
/// `get_candidates` is Go's `func(T) []string`: the strings to score item `i`
/// against (e.g. `[Title, OriginalTitle]`). The item's score is the minimum
/// over them.
///
/// Scanning is strictly in `items` order, first-wins on ties, with an early
/// exit on an exact (distance-0) match — so `get_candidates` is not
/// necessarily called for every item.
pub fn find_best_match_index<T, C, S>(
    target: &str,
    items: &[T],
    mut get_candidates: impl FnMut(&T) -> C,
) -> Option<usize>
where
    C: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let norm_target = levenshtein_normalize_string(target);

    // Go: `minDistance := levenshteinThreshold + 1`, compared with `<`.
    let mut min_distance = LEVENSHTEIN_THRESHOLD + 1;
    let mut best_match: Option<usize> = None;

    for (i, item) in items.iter().enumerate() {
        let candidates = get_candidates(item);

        // Go's `distance >= 0` guard: an item with no candidates is skipped.
        let Some(distance) = find_min_distance_normalized(&norm_target, candidates) else {
            continue;
        };

        // Strictly `<`: the first item at a given distance keeps the slot.
        if distance < min_distance {
            min_distance = distance;
            best_match = Some(i);

            if distance == 0 {
                break;
            }
        }
    }

    best_match
}

/// [`find_best_match_index`] returning the matched item itself — the shape of
/// Go's `(t T, ok bool)` return.
pub fn find_best_match<'a, T, C, S>(
    target: &str,
    items: &'a [T],
    get_candidates: impl FnMut(&T) -> C,
) -> Option<&'a T>
where
    C: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    find_best_match_index(target, items, get_candidates).map(|i| &items[i])
}
