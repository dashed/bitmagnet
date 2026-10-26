//! Behavioural parity for the Go-pinned letter class in the title regexes,
//! against results captured from the production Go binary.
//!
//! The title/year/episode patterns embed `[\p{L}0-9]` and `[^\p{L}0-9]`. A
//! literal `\p{L}` in the Rust `regex` crate is 4,924 code points wider than
//! Go's, so a title carrying a code point Go 1.23.6 does not yet know as a
//! letter split differently. `goclass::pin_letter_class` removes that; this
//! test is the behavioural proof, over the same 728-probe oracle the fts crate
//! uses.

use std::collections::BTreeMap;

fn load() -> Vec<BTreeMap<String, serde_json::Value>> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/unicode/go-oracle.jsonl"
    );
    let raw = std::fs::read_to_string(path).expect("read go-oracle.jsonl");
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid oracle json"))
        .collect()
}

#[test]
fn title_year_episodes_matches_go_on_every_probe() {
    let cases = load();
    assert!(cases.len() > 300, "oracle looks truncated");

    let mut failures = Vec::new();
    for case in &cases {
        let input = case["input"].as_str().expect("input");
        let go_matched = case["matched"].as_bool().expect("matched");
        let got = bitmagnet_release::parse_title_year_episodes_dispatch(None, input);

        match (go_matched, &got) {
            (false, None) => {}
            (false, Some(r)) => failures.push(format!("{input:?}: Go unmatched, Rust {r:?}")),
            (true, None) => failures.push(format!("{input:?}: Go matched, Rust unmatched")),
            (true, Some((title, year, _episodes, rest))) => {
                let want_title = case["title"].as_str().expect("title");
                let want_year = u16::try_from(case["year"].as_u64().expect("year")).expect("u16");
                let want_rest = case["rest"].as_str().expect("rest");
                if title != want_title || *year != want_year || rest != want_rest {
                    failures.push(format!(
                        "{input:?}: want ({want_title:?}, {want_year}, {want_rest:?}), \
                         got ({title:?}, {year}, {rest:?})"
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} probes diverge from Go:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}
