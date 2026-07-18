//! Faithful port of `github.com/facette/natsort` `Compare` — the natural-order
//! string comparison Go uses to sort `Languages.Slice()`. Chunkifies each
//! string into maximal runs of ASCII digits (`\d+`) vs non-digits (`\D+`) and
//! compares chunk-by-chunk: numeric chunks numerically, others lexicographically
//! (Go byte order). Language names carry no digits, so this reduces to plain
//! string comparison there, but the general algorithm is ported for fidelity.

/// Split into runs of ASCII digits and non-digits (Go's `(\d+|\D+)`).
fn chunkify(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut chunks = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let digit = bytes[i].is_ascii_digit();
        while i < bytes.len() && bytes[i].is_ascii_digit() == digit {
            i += 1;
        }
        chunks.push(&s[start..i]);
    }
    chunks
}

/// Go `strconv.Atoi` semantics for a chunk (only whole-digit runs parse).
fn atoi(chunk: &str) -> Option<i64> {
    chunk.parse::<i64>().ok()
}

/// Port of `natsort.Compare(a, b)` → `a < b` in natural order.
pub(crate) fn nat_less(a: &str, b: &str) -> bool {
    let chunks_a = chunkify(a);
    let chunks_b = chunkify(b);
    let n_a = chunks_a.len();
    let n_b = chunks_b.len();

    for (i, &chunk_a) in chunks_a.iter().enumerate() {
        if i >= n_b {
            return false;
        }
        let chunk_b = chunks_b[i];
        let a_int = atoi(chunk_a);
        let b_int = atoi(chunk_b);

        if let (Some(ai), Some(bi)) = (a_int, b_int) {
            if ai == bi {
                if i == n_a - 1 {
                    return true;
                } else if i == n_b - 1 {
                    return false;
                }
                continue;
            }
            return ai < bi;
        }

        if chunk_a == chunk_b {
            if i == n_a - 1 {
                return true;
            } else if i == n_b - 1 {
                return false;
            }
            continue;
        }

        return chunk_a < chunk_b;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_lexicographic_for_names() {
        assert!(nat_less("English", "French"));
        assert!(nat_less("French", "German"));
        assert!(!nat_less("German", "English"));
    }

    #[test]
    fn numeric_chunks_compared_as_numbers() {
        // "a2" < "a10" numerically (not lexically).
        assert!(nat_less("a2", "a10"));
        assert!(!nat_less("a10", "a2"));
    }
}
