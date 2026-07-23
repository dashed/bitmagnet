//! Behavioral parity for the episode parser: replay frozen fixtures and assert
//! `parse_episodes(name).to_string()` matches Go's `ParseEpisodes(name).String()`
//! — including the reproduced Go bugs (S01E01 -> S01, S01-03 -> S00-01, dead
//! x-format branch). Oracle: `testdata/parity/release/episodes.jsonl`.

use bitmagnet_release::parse_episodes;
use serde::Deserialize;

#[derive(Deserialize)]
struct EpisodeFixture {
    id: String,
    name: String,
    episodes: String,
}

#[test]
fn episodes_match_go() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/release/episodes.jsonl"
    );
    let raw = std::fs::read_to_string(path).expect("read episodes.jsonl");
    let fixtures: Vec<EpisodeFixture> = raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid fixture json"))
        .collect();
    assert!(fixtures.len() >= 25, "expected the full episode corpus");

    for f in &fixtures {
        let got = parse_episodes(&f.name).to_string();
        assert_eq!(got, f.episodes, "episodes mismatch for {}", f.id);
    }
}
