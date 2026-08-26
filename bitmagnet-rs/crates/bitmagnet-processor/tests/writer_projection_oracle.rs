use bitmagnet_classifier::{ClassifierInput, InputFile};
use bitmagnet_processor::{
    project_unattached_persistence, TorrentContentWrite, TorrentSnapshot, TorrentSourceSnapshot,
};
use serde::Deserialize;

const FIXTURES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/processor-writer-projection/fixtures.json"
));

const FIXTURE_IDS: [&str; 12] = [
    "no-sources",
    "source-zero-is-present",
    "source-maxima-and-null-fallback",
    "source-maxima-permuted",
    "published-at-exact-cutoff-falls-back",
    "published-at-cutoff-plus-one-microsecond",
    "video-v1080p-and-v3dsbs",
    "video-v3dou-literal",
    "paths-ascii-reduction",
    "paths-utf8-prefix-split",
    "paths-utf8-suffix-split",
    "paths-overlong-lexeme-then-normal",
];

#[derive(Debug, Deserialize)]
struct Fixture {
    id: String,
    input: FixtureInput,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct FixtureInput {
    torrent: TorrentInput,
    classification: ClassificationInput,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TorrentInput {
    info_hash: String,
    name: String,
    created_at_micros: i64,
    files: Vec<String>,
    sources: Vec<SourceInput>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourceInput {
    seeders: Option<u64>,
    leechers: Option<u64>,
    published_at_micros: Option<i64>,
    created_at_micros: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClassificationInput {
    content_type: String,
    video_resolution: Option<String>,
    video_source: Option<String>,
    video_codec: Option<String>,
    #[serde(rename = "video3d")]
    video_3d: Option<String>,
    video_modifier: Option<String>,
    release_group: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    seeders: Option<u64>,
    leechers: Option<u64>,
    published_at_micros: i64,
    tsv: String,
}

#[test]
fn pure_writer_projection_matches_go_new_torrent_content_oracle() {
    let fixtures: Vec<Fixture> = serde_json::from_str(FIXTURES).expect("decode writer fixtures");
    assert_eq!(fixtures.len(), FIXTURE_IDS.len());

    for (index, fixture) in fixtures.into_iter().enumerate() {
        assert_eq!(fixture.id, FIXTURE_IDS[index]);
        let file_count = fixture.input.torrent.files.len();
        let classifier_input = ClassifierInput {
            id: fixture.input.torrent.info_hash.clone(),
            name: fixture.input.torrent.name,
            size: 1,
            files_status: if file_count == 0 { "no_info" } else { "multi" }.to_owned(),
            extension: None,
            files_count: (file_count != 0)
                .then(|| u32::try_from(file_count).expect("fixture file count fits u32")),
            files: fixture
                .input
                .torrent
                .files
                .into_iter()
                .enumerate()
                .map(|(index, path)| InputFile {
                    index: u32::try_from(index).expect("fixture file index fits u32"),
                    path,
                    extension: String::new(),
                    size: 1,
                })
                .collect(),
            hint: None,
            contents: Vec::new(),
        };
        let row = TorrentContentWrite {
            id: format!(
                "{}:{}:?:?",
                classifier_input.id, fixture.input.classification.content_type
            ),
            info_hash: classifier_input.id.clone(),
            content_type: Some(fixture.input.classification.content_type),
            content_source: None,
            content_id: None,
            languages: Vec::new(),
            episodes: "[]".to_owned(),
            video_resolution: fixture.input.classification.video_resolution,
            video_source: fixture.input.classification.video_source,
            video_codec: fixture.input.classification.video_codec,
            video_3d: fixture.input.classification.video_3d,
            video_modifier: fixture.input.classification.video_modifier,
            release_group: fixture.input.classification.release_group,
            size: 1,
            files_count: (file_count != 0).then_some(file_count as u64),
        };
        let sources = fixture
            .input
            .torrent
            .sources
            .into_iter()
            .map(|source| TorrentSourceSnapshot {
                seeders: source.seeders,
                leechers: source.leechers,
                published_at_micros: source.published_at_micros,
                created_at_micros: source.created_at_micros,
            })
            .collect::<Vec<_>>();

        let actual = project_unattached_persistence(
            &row,
            &classifier_input,
            TorrentSnapshot {
                created_at_micros: fixture.input.torrent.created_at_micros,
            },
            &sources,
        )
        .unwrap_or_else(|error| panic!("fixture {} failed: {error}", fixture.id));

        assert_eq!(actual.seeders, fixture.expected.seeders, "{}", fixture.id);
        assert_eq!(actual.leechers, fixture.expected.leechers, "{}", fixture.id);
        assert_eq!(
            actual.published_at_micros, fixture.expected.published_at_micros,
            "{}",
            fixture.id
        );
        assert_eq!(actual.tsv, fixture.expected.tsv, "{}", fixture.id);
    }
}
