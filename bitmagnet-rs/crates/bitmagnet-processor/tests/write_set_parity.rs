use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use bitmagnet_classifier::ClassifierInput;
use bitmagnet_processor::{LoadedTorrent, Materializer, WriteSet};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Fixture {
    id: String,
    subsystem: String,
    input: FixtureInput,
    expected: WriteSet,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureInput {
    info_hash: String,
    classifier: ClassifierInput,
    existing_content_ids: Vec<String>,
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/write_set.golden.jsonl")
}

#[test]
fn go_write_set_oracle_matches_all_classifier_corpus_torrents() {
    let materializer = Materializer::from_core().expect("compile core classifier");
    let reader = BufReader::new(File::open(fixture_path()).expect("open write-set golden"));
    let mut count = 0;

    for (line_index, line) in reader.lines().enumerate() {
        let line = line.unwrap_or_else(|err| panic!("read fixture line {}: {err}", line_index + 1));
        let fixture: Fixture = serde_json::from_str(&line)
            .unwrap_or_else(|err| panic!("decode fixture line {}: {err}", line_index + 1));
        assert_eq!(fixture.subsystem, "processor-write-set");

        let info_hash = ProtocolId::from_hex(&fixture.input.info_hash)
            .unwrap_or_else(|err| panic!("{}: parse info hash: {err}", fixture.id));
        let params = ProcessTorrentParams {
            info_hashes: vec![info_hash],
            ..ProcessTorrentParams::default()
        };
        let actual = materializer
            .materialize(
                &params,
                vec![LoadedTorrent {
                    info_hash: fixture.input.info_hash,
                    classifier_input: fixture.input.classifier,
                    existing_content_ids: fixture.input.existing_content_ids,
                    attach_hint_unsupported: false,
                    source_backed_content_present: false,
                }],
            )
            .unwrap_or_else(|err| panic!("{}: materialize: {err}", fixture.id));

        assert_eq!(actual, fixture.expected, "fixture {}", fixture.id);
        count += 1;
    }

    assert_eq!(count, 330, "expected the full frozen classifier corpus");
}
