//! Behavioral parity for the alias-less video tables (`video_modifier`,
//! `video_3d`). Oracle: `testdata/parity/release/video_extra.jsonl`.

use bitmagnet_release::{infer_video_3d, infer_video_modifier};
use serde::Deserialize;

#[derive(Deserialize)]
struct ExtraFixture {
    id: String,
    name: String,
    video_modifier: Option<String>,
    video_3d: Option<String>,
}

#[test]
fn video_extra_match_go() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/release/video_extra.jsonl"
    );
    let raw = std::fs::read_to_string(path).expect("read video_extra.jsonl");
    let fixtures: Vec<ExtraFixture> = raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid fixture json"))
        .collect();
    assert!(fixtures.len() >= 12, "expected the full extra corpus");

    for f in &fixtures {
        let modifier = infer_video_modifier(&f.name).map(|v| v.as_str().to_string());
        assert_eq!(modifier, f.video_modifier, "modifier mismatch for {}", f.id);

        let three_d = infer_video_3d(&f.name).map(|v| v.as_str().to_string());
        assert_eq!(three_d, f.video_3d, "video_3d mismatch for {}", f.id);
    }
}
