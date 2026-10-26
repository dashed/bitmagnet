//! Go's `net/http.StatusText`, which is what Go stores for a TMDB error
//! answered over HTTP/2.
//!
//! HTTP/2 has no reason phrase. Go's HTTP/2 client builds `res.Status` from the
//! `:status` value, a space and `StatusText(code)` (`h2_bundle.go:9641` in
//! go1.23.6), so a code Go has no text for is stored as `"599 "`. The table is
//! Go's own, not hyper's: `http`'s canonical reasons name the same 62 codes but
//! word 203, 413, 414 and 416 differently (`413 Payload Too Large` against Go's
//! `413 Request Entity Too Large`).
//!
//! `go_status_text.tsv` is written by Go, with the `go.mod` toolchain the prod
//! images build with:
//!
//! ```text
//! go test ./internal/tmdb/ -run TestStatusTextFixture -update
//! ```
//!
//! It has one row per code from 100 to 999: the code, then a tab and the text,
//! or the code alone where Go has no text. The same Go package drives the
//! production resty client over HTTP/2 and checks `res.Status()` against these
//! rows (`TestStatusTextOverHTTP2`), so both sides pin one table.

const TABLE: &str = include_str!("go_status_text.tsv");

/// Go's `http.StatusText(status)`: `Some("")` for a code Go's table does not
/// name, and `None` outside 100–999, which no HTTP/2 response can carry.
pub(crate) fn go_status_text(status: u16) -> Option<&'static str> {
    TABLE
        .lines()
        .filter(|line| !line.starts_with('#'))
        .find_map(|line| {
            let (code, text) = line.split_once('\t').unwrap_or((line, ""));
            (code.parse() == Ok(status)).then_some(text)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const GO_MOD: &str = include_str!("../../../../go.mod");

    /// The table is the go.mod toolchain's, which the prod images build with;
    /// the Go side refuses to write it from any other.
    #[test]
    fn the_table_comes_from_the_go_mod_toolchain() {
        let go = GO_MOD
            .lines()
            .find_map(|line| line.strip_prefix("go "))
            .expect("go.mod has a go directive")
            .trim();

        assert_eq!(
            TABLE.lines().next(),
            Some(
                format!(
                    "# net/http.StatusText for every status code from 100 to 999, from go{go}."
                )
                .as_str()
            )
        );
    }

    /// One row per code, in order, so a lookup can neither miss a code nor
    /// find two. A row's text is never empty after its tab: an empty text is
    /// the code alone.
    #[test]
    fn the_table_has_one_row_per_code() {
        let rows: Vec<&str> = TABLE
            .lines()
            .filter(|line| !line.starts_with('#'))
            .collect();
        assert_eq!(rows.len(), 900);

        for (row, code) in rows.iter().zip(100_u16..=999) {
            let (row_code, text) = row.split_once('\t').unwrap_or((row, ""));
            assert_eq!(row_code, code.to_string(), "{row:?}");
            assert!(!text.contains('\t'), "{row:?}");
            assert_eq!(row.contains('\t'), !text.is_empty(), "{row:?}");
        }
    }

    #[test]
    fn lookups_return_gos_text() {
        assert_eq!(go_status_text(100), Some("Continue"));
        assert_eq!(go_status_text(413), Some("Request Entity Too Large"));
        assert_eq!(go_status_text(422), Some("Unprocessable Entity"));
        assert_eq!(go_status_text(599), Some(""));
        assert_eq!(go_status_text(999), Some(""));
        assert_eq!(go_status_text(99), None);
        assert_eq!(go_status_text(1000), None);
    }
}
