//! Behavioral parity for title/year/episode extraction and `ParseVideoContent`.
//! Oracles: `testdata/parity/release/{title_year_episodes,video_content}.jsonl`.

use bitmagnet_release::{parse_title_year_episodes_dispatch, parse_video_content, ContentType};
use serde::Deserialize;

fn ct(hint: &str) -> Option<ContentType> {
    if hint.is_empty() {
        None
    } else {
        Some(ContentType::parse_ci(hint).expect("valid hint"))
    }
}

#[derive(Deserialize)]
struct TyeFixture {
    id: String,
    name: String,
    hint: String,
    matched: bool,
    title: String,
    year: u16,
    episodes: String,
    rest: String,
}

#[test]
fn title_year_episodes_match_go() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/release/title_year_episodes.jsonl"
    );
    let raw = std::fs::read_to_string(path).expect("read fixture");
    let fixtures: Vec<TyeFixture> = raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    assert!(fixtures.len() >= 14);

    for f in &fixtures {
        match parse_title_year_episodes_dispatch(ct(&f.hint), &f.name) {
            Some((title, year, episodes, rest)) => {
                assert!(f.matched, "{}: expected no match", f.id);
                assert_eq!(title, f.title, "{}: title", f.id);
                assert_eq!(year, f.year, "{}: year", f.id);
                assert_eq!(episodes.to_string(), f.episodes, "{}: episodes", f.id);
                assert_eq!(rest, f.rest, "{}: rest", f.id);
            }
            None => assert!(!f.matched, "{}: expected a match", f.id),
        }
    }
}

#[derive(Deserialize)]
struct CvFixture {
    id: String,
    name: String,
    hint: String,
    date_valid: bool,
    ok: bool,
    content_type: Option<String>,
    base_title: Option<String>,
    year: u16,
    episodes: String,
    languages: Vec<String>,
    language_multi: bool,
    video_resolution: Option<String>,
    video_source: Option<String>,
    video_codec: Option<String>,
    video_3d: Option<String>,
    video_modifier: Option<String>,
    release_group: Option<String>,
}

#[test]
fn video_content_match_go() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/release/video_content.jsonl"
    );
    let raw = std::fs::read_to_string(path).expect("read fixture");
    let fixtures: Vec<CvFixture> = raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    assert!(fixtures.len() >= 12);

    for f in &fixtures {
        let attrs = parse_video_content(&f.name, ct(&f.hint), f.date_valid);
        if !f.ok {
            assert!(attrs.is_none(), "{}: expected error", f.id);
            continue;
        }
        let a = attrs.unwrap_or_else(|| panic!("{}: expected attrs", f.id));
        assert_eq!(
            a.content_type.map(|c| c.as_str().to_string()),
            f.content_type,
            "{}: content_type",
            f.id
        );
        assert_eq!(a.base_title, f.base_title, "{}: base_title", f.id);
        assert_eq!(a.year, f.year, "{}: year", f.id);
        assert_eq!(a.episodes.to_string(), f.episodes, "{}: episodes", f.id);
        assert_eq!(a.languages, f.languages, "{}: languages", f.id);
        assert_eq!(
            a.language_multi, f.language_multi,
            "{}: language_multi",
            f.id
        );
        assert_eq!(
            a.video_resolution.map(|v| v.as_str().to_string()),
            f.video_resolution,
            "{}: resolution",
            f.id
        );
        assert_eq!(
            a.video_source.map(|v| v.as_str().to_string()),
            f.video_source,
            "{}: source",
            f.id
        );
        assert_eq!(
            a.video_codec.map(|v| v.as_str().to_string()),
            f.video_codec,
            "{}: codec",
            f.id
        );
        assert_eq!(
            a.video_3d.map(|v| v.as_str().to_string()),
            f.video_3d,
            "{}: video_3d",
            f.id
        );
        assert_eq!(
            a.video_modifier.map(|v| v.as_str().to_string()),
            f.video_modifier,
            "{}: modifier",
            f.id
        );
        assert_eq!(a.release_group, f.release_group, "{}: release_group", f.id);
    }
}
