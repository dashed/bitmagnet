use std::collections::{BTreeMap, BTreeSet};

use bitmagnet_classifier::{Classification, ClassifierInput, InputHint, Outcome};
use bitmagnet_processor::{LoadedTorrent, Materializer};

const INFO_HASH: &str = "0123456789abcdef0123456789abcdef01234567";

#[test]
fn materialize_replayed_builds_the_canonical_same_state_image() {
    let materializer = Materializer::from_core().expect("compile core classifier");
    let input = ClassifierInput {
        id: INFO_HASH.to_owned(),
        name: "Example.Movie.2024.1080p.BluRay.x264-GRP.mkv".to_owned(),
        size: 1234,
        files_status: "single".to_owned(),
        extension: Some("mkv".to_owned()),
        files_count: None,
        files: Vec::new(),
        contents: Vec::new(),
        hint: None,
    };
    let mut result = Classification::default();
    result.apply_hint(&InputHint {
        content_type: "movie".to_owned(),
        languages: vec!["fr".to_owned(), "de".to_owned()],
        video_resolution: Some("V1080p".to_owned()),
        video_source: Some("BluRay".to_owned()),
        video_codec: Some("x264".to_owned()),
        release_group: Some("GRP".to_owned()),
        ..InputHint::default()
    });
    result.tags = BTreeSet::from(["z".to_owned(), "a".to_owned()]);

    let write_set = materializer
        .materialize_replayed(
            LoadedTorrent {
                info_hash: INFO_HASH.to_owned(),
                classifier_input: input,
                existing_content_ids: vec![
                    "stale:b".to_owned(),
                    format!("{INFO_HASH}:movie:?:?"),
                    "stale:a".to_owned(),
                ],
                attach_hint_unsupported: false,
            },
            result,
            Outcome::Classified,
        )
        .expect("materialize replayed classification");

    assert_eq!(write_set.delete_ids, ["stale:a", "stale:b"]);
    assert_eq!(
        write_set.add_tags,
        BTreeMap::from([(INFO_HASH.to_owned(), vec!["a".to_owned(), "z".to_owned()])])
    );
    assert_eq!(write_set.torrent_contents.len(), 1);
    let torrent_content = &write_set.torrent_contents[0];
    assert_eq!(torrent_content.id, format!("{INFO_HASH}:movie:?:?"));
    assert_eq!(torrent_content.languages, ["de", "fr"]);
    assert_eq!(torrent_content.files_count, Some(1));
    assert_eq!(torrent_content.content_type.as_deref(), Some("movie"));
    assert_eq!(torrent_content.video_resolution.as_deref(), Some("V1080p"));
    assert_eq!(torrent_content.video_source.as_deref(), Some("BluRay"));
    assert_eq!(torrent_content.video_codec.as_deref(), Some("x264"));
    assert_eq!(torrent_content.release_group.as_deref(), Some("GRP"));
}

#[test]
fn materialize_replayed_maps_deterministic_terminal_outcomes() {
    let materializer = Materializer::from_core().expect("compile core classifier");

    for (outcome, deleted, failed) in [
        (Outcome::Deleted("delete".to_owned()), 1, 0),
        (Outcome::Unmatched("unmatched".to_owned()), 0, 1),
    ] {
        let write_set = materializer
            .materialize_replayed(
                LoadedTorrent {
                    info_hash: INFO_HASH.to_owned(),
                    classifier_input: ClassifierInput {
                        id: INFO_HASH.to_owned(),
                        name: "fixture".to_owned(),
                        size: 1,
                        files_status: "no_info".to_owned(),
                        extension: None,
                        files_count: None,
                        files: Vec::new(),
                        contents: Vec::new(),
                        hint: None,
                    },
                    existing_content_ids: Vec::new(),
                    attach_hint_unsupported: false,
                },
                Classification::default(),
                outcome,
            )
            .expect("materialize terminal outcome");

        assert_eq!(write_set.delete_info_hashes.len(), deleted);
        assert_eq!(write_set.failed_info_hashes.len(), failed);
    }
}
