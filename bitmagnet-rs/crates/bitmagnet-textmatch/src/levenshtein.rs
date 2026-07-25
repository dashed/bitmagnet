//! Verbatim port of `github.com/agnivade/levenshtein` v1.2.1
//! `ComputeDistance` — the exact edit-distance function Go's classifier calls.
//!
//! # Why a hand-port and not `strsim`
//!
//! `strsim::levenshtein` is *probably* the same metric, but "probably" is not a
//! parity contract: edit-distance crates differ on transpositions
//! (Damerau/OSA), on whether they count bytes or `char`s, and on Unicode
//! normalization. This function decides which content attaches to a torrent, so
//! the Go implementation is transcribed line for line instead — including its
//! prefix/suffix trimming and its `uint16` accumulator (see below).
//!
//! # Faithfulness notes
//!
//! * Go compares **runes**, not bytes (`[]rune(a)`), so the port collects
//!   `char`s.
//! * The `len(a) == 0` / `len(b) == 0` short-circuits are on **byte** length in
//!   Go, but a string is byte-empty iff it is rune-empty, so `is_empty()` is
//!   equivalent.
//! * The DP row is `uint16` in Go and therefore wraps silently past 65535. The
//!   port uses `u16` with `wrapping_add` so it reproduces that (unreachable for
//!   any realistic title, but a Rust `u16` would *panic* in debug builds rather
//!   than diverge quietly, which would be a different kind of wrong).

/// Port of Go `levenshtein.ComputeDistance(a, b)`.
///
/// Operates on Unicode code points and does **not** normalize its inputs —
/// callers normalize first via
/// [`levenshtein_normalize_string`](crate::levenshtein_normalize_string),
/// exactly as Go's `levenshteinFindMinDistance` does.
#[must_use]
pub fn compute_distance(a: &str, b: &str) -> usize {
    if a.is_empty() {
        return b.chars().count();
    }

    if b.is_empty() {
        return a.chars().count();
    }

    if a == b {
        return 0;
    }

    let mut s1: Vec<char> = a.chars().collect();
    let mut s2: Vec<char> = b.chars().collect();

    // Swap to save some memory: O(min(a, b)) instead of O(a).
    if s1.len() > s2.len() {
        std::mem::swap(&mut s1, &mut s2);
    }

    // Remove trailing identical runes. Note Go breaks out of the loop at the
    // *first* difference and only then truncates, so a fully-shared suffix
    // (loop runs to completion) truncates nothing.
    for i in 0..s1.len() {
        if s1[s1.len() - 1 - i] != s2[s2.len() - 1 - i] {
            let (n1, n2) = (s1.len() - i, s2.len() - i);
            s1.truncate(n1);
            s2.truncate(n2);
            break;
        }
    }

    // Remove leading identical runes.
    for i in 0..s1.len() {
        if s1[i] != s2[i] {
            s1.drain(..i);
            s2.drain(..i);
            break;
        }
    }

    let len_s1 = s1.len();
    let len_s2 = s2.len();

    // Init the row. (Go's `minLengthThreshold` small-string optimization is a
    // pure allocation trick with no effect on the result, so it is dropped.)
    let mut x: Vec<u16> = vec![0; len_s1 + 1];
    for (i, cell) in x.iter_mut().enumerate().skip(1) {
        *cell = i as u16;
    }

    for i in 1..=len_s2 {
        let mut prev = i as u16;

        for j in 1..=len_s1 {
            let mut current = x[j - 1]; // match
            if s2[i - 1] != s1[j - 1] {
                current = x[j - 1]
                    .wrapping_add(1)
                    .min(prev.wrapping_add(1))
                    .min(x[j].wrapping_add(1));
            }

            x[j - 1] = prev;
            prev = current;
        }

        x[len_s1] = prev;
    }

    usize::from(x[len_s1])
}
