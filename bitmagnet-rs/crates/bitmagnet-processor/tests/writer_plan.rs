use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use bitmagnet_classifier::ClassifierInput;
use bitmagnet_processor::{
    compose_writer_plan, project_unattached_persistence, LoadedTorrent, Materializer,
    TorrentSnapshot, TorrentSourceSnapshot, WriteSet, WriterLoadedTorrent,
};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId};
use serde::Deserialize;
use serde_json::Value;

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

fn supported_params(info_hashes: Vec<ProtocolId>) -> ProcessTorrentParams {
    ProcessTorrentParams {
        info_hashes,
        classifier_workflow: "default".to_owned(),
        classifier_flags: Some(BTreeMap::from([
            ("apis_enabled".to_owned(), Value::Bool(false)),
            ("local_search_enabled".to_owned(), Value::Bool(false)),
            ("tmdb_enabled".to_owned(), Value::Bool(false)),
        ])),
        ..ProcessTorrentParams::default()
    }
}

#[test]
fn writer_plan_composes_every_go_write_set_fixture_without_cloning_inputs() {
    let materializer = Materializer::from_core().expect("compile core classifier");
    let reader = BufReader::new(File::open(fixture_path()).expect("open write-set golden"));
    let mut count = 0;

    for (line_index, line) in reader.lines().enumerate() {
        let line =
            line.unwrap_or_else(|error| panic!("read fixture line {}: {error}", line_index + 1));
        let fixture: Fixture = serde_json::from_str(&line)
            .unwrap_or_else(|error| panic!("decode fixture line {}: {error}", line_index + 1));
        assert_eq!(fixture.subsystem, "processor-write-set");
        assert!(
            fixture.expected.contents.is_empty(),
            "{} unexpectedly entered attached-content scope",
            fixture.id
        );

        let info_hash = ProtocolId::from_hex(&fixture.input.info_hash)
            .unwrap_or_else(|error| panic!("{}: parse info hash: {error}", fixture.id));
        let params = supported_params(vec![info_hash]);
        let mut classifier = fixture.input.classifier;
        classifier.id.clone_from(&fixture.input.info_hash);
        let loaded = vec![WriterLoadedTorrent {
            loaded: LoadedTorrent {
                info_hash: fixture.input.info_hash,
                classifier_input: classifier,
                existing_content_ids: fixture.input.existing_content_ids,
                attach_hint_unsupported: false,
            },
            torrent_snapshot: TorrentSnapshot {
                created_at_micros: 1_700_000_000_123_456,
            },
            source_snapshots: vec![TorrentSourceSnapshot {
                seeders: Some(7),
                leechers: Some(11),
                published_at_micros: Some(1_600_000_000_654_321),
                created_at_micros: 1_500_000_000_111_222,
            }],
        }];

        let plan = compose_writer_plan(&materializer, &params, &loaded)
            .unwrap_or_else(|error| panic!("{}: compose writer plan: {error}", fixture.id));
        assert_eq!(
            plan.write_set(),
            &fixture.expected,
            "fixture {}",
            fixture.id
        );
        assert_eq!(
            plan.persistence().len(),
            plan.write_set().torrent_contents.len(),
            "fixture {} persistence keyset",
            fixture.id
        );
        for row in &plan.write_set().torrent_contents {
            let expected = project_unattached_persistence(
                row,
                &loaded[0].loaded.classifier_input,
                loaded[0].torrent_snapshot,
                &loaded[0].source_snapshots,
            )
            .unwrap_or_else(|error| panic!("{}: direct projection: {error}", fixture.id));
            assert_eq!(
                plan.persistence().get(&row.id),
                Some(&expected),
                "fixture {} metadata {}",
                fixture.id,
                row.id
            );
        }
        count += 1;
    }

    assert_eq!(count, 330, "expected the full frozen classifier corpus");
}

#[test]
fn missing_requested_torrent_remains_retryable_without_persistence_intent() {
    let materializer = Materializer::from_core().expect("compile core classifier");
    let info_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let params = supported_params(vec![ProtocolId::from_hex(info_hash).expect("fixture hash")]);

    let plan = compose_writer_plan(&materializer, &params, &[])
        .expect("missing source row produces a retryable plan");

    assert_eq!(plan.retry_info_hashes(), &[info_hash.to_owned()]);
    assert!(plan.write_set().torrent_contents.is_empty());
    assert!(plan.persistence().is_empty());
}
