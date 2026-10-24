//! Lane G Torznab golden byte-parity.
//!
//! For every query in `testdata/parity/torznab/corpus.jsonl` this reconstructs
//! the response Lane T's serializer produces — reusing the *real* request parser
//! ([`parse`]), category mapping ([`to_search_params`]) and result mapping
//! ([`item_from_fixture_fields`] → `map_fields`), with `corpus.expectIds` standing
//! in for Lane Q's SQL execution (see the crate lane contract) — then normalises
//! it to the shared canonical form and asserts a byte match against the golden.
//!
//! The production serializer emits Go's raw `encoding/xml` shape (XML
//! declaration, expanded empty elements, `&#34;`/`&#x9;`-style escapes,
//! struct-order attributes) so the deployed service is byte-identical to the Go
//! endpoint. The goldens are the *normalised* form, so both sides pass through
//! the same normalisation — a Rust port of `internal/parity/torznab_xml.go`
//! (`NormalizeTorznabXML`) — before comparison.
//!
//! Skips (with a printed notice) when the Lane G goldens are not present, so it
//! is a no-op on the Lane T branch until the phase-1 integration merges Lane G.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use bitmagnet_search_query::{ContentType, Episodes, VideoResolution};
use bitmagnet_torznab::{
    item_from_fixture_fields, parse, to_search_params, Channel, FixtureItemFields, Profile,
    Response, SearchResult, TorznabError,
};
use serde::Deserialize;
use sha1::{Digest, Sha1};

mod parity_support;

use parity_support::{first_diff, goldens_dir, load_corpus, normalize, read_jsonl, CorpusQuery};

/// Seconds from the Unix epoch to 2020-01-01T00:00:00Z — the base Lane G's seed
/// adds `pub * 24h` to for each fixture's `published_at`.
const PUBLISHED_AT_BASE: i64 = 1_577_836_800;

#[test]
fn torznab_goldens_are_byte_exact_after_normalization() {
    let dir = goldens_dir();
    let corpus_path = dir.join("corpus.jsonl");
    let fixtures_path = dir.join("fixtures.jsonl");
    if !corpus_path.is_file() || !fixtures_path.is_file() {
        eprintln!(
            "skipping Torznab goldens: {} not present",
            corpus_path.display()
        );
        return;
    }

    let fixtures = load_fixtures(&fixtures_path);
    let corpus = load_corpus(&corpus_path);
    let profile = Profile::default_profile();

    let mut passed = 0usize;
    let mut failures: Vec<String> = Vec::new();

    for query in &corpus {
        let golden_path = dir.join(query.golden_name());
        let golden = match fs::read(&golden_path) {
            Ok(bytes) => bytes,
            Err(error) => {
                failures.push(format!("{}: missing golden ({error})", query.id));
                continue;
            }
        };

        let raw = render(query, &profile, &fixtures);
        let actual = normalize(&raw);

        if actual == golden {
            passed += 1;
        } else {
            failures.push(format!(
                "{}: normalized output != {}\n{}",
                query.id,
                golden_path.display(),
                first_diff(&actual, &golden),
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{passed}/{} Torznab goldens matched; {} failed:\n\n{}",
        corpus.len(),
        failures.len(),
        failures.join("\n\n"),
    );
    assert_eq!(passed, corpus.len(), "every corpus query has a golden");
    eprintln!("Torznab goldens: {passed}/{} byte-exact", corpus.len());
}

/// Reproduce `service.rs::handle` dispatch, with `expectIds` fixtures replacing
/// the live search backend.
fn render(query: &CorpusQuery, profile: &Profile, fixtures: &BTreeMap<String, Fixture>) -> Vec<u8> {
    if query.kind == "caps" {
        return profile.caps().to_xml().expect("caps XML renders");
    }

    let request = parse(form_urlencoded::parse(query_string(&query.path).as_bytes()));
    if request.type_.is_empty() {
        return TorznabError {
            code: 200,
            description: "missing parameter (t)".to_owned(),
        }
        .to_xml()
        .expect("error XML renders");
    }

    match to_search_params(&request, profile) {
        Err(error) => error.to_xml().expect("error XML renders"),
        Ok(_params) => {
            let items = query
                .expect_ids
                .clone()
                .unwrap_or_default()
                .iter()
                .map(|id| {
                    let fixture = fixtures
                        .get(id)
                        .unwrap_or_else(|| panic!("{}: unknown fixture {id}", query.id));
                    item_from_fixture_fields(&fixture.to_item_fields())
                })
                .collect();
            SearchResult {
                channel: Channel {
                    title: Some(profile.title.clone()),
                    response: Response {
                        offset: request.offset.unwrap_or_default(),
                        total: 0,
                    },
                    items,
                    ..Channel::default()
                },
            }
            .to_xml()
            .expect("search XML renders")
        }
    }
}

fn query_string(path: &str) -> String {
    path.split_once('?')
        .map(|(_, query)| query.to_owned())
        .unwrap_or_default()
}

// ---- fixtures ---------------------------------------------------------------

/// One `fixtures.jsonl` row. Keys mirror Lane G's `torznabFixtureJSON`.
#[derive(Debug, Deserialize)]
struct Fixture {
    id: String,
    #[serde(default, rename = "pub")]
    pub_index: i64,
    #[serde(default, rename = "contentType")]
    content_type: String,
    #[serde(default)]
    tmdb: String,
    #[serde(default)]
    imdb: String,
    #[serde(default)]
    year: i32,
    title: String,
    size: u64,
    #[serde(default, rename = "videoResolution")]
    video_resolution: String,
    #[serde(default, rename = "videoCodec")]
    video_codec: String,
    #[serde(default, rename = "releaseGroup")]
    release_group: String,
    #[serde(default)]
    seeders: Option<u32>,
    #[serde(default)]
    leechers: Option<u32>,
    #[serde(default)]
    episodes: BTreeMap<String, Vec<i32>>,
    #[serde(default)]
    files: Vec<String>,
}

impl Fixture {
    fn to_item_fields(&self) -> FixtureItemFields {
        let content_type = if self.content_type.is_empty() {
            None
        } else {
            Some(
                self.content_type
                    .parse::<ContentType>()
                    .unwrap_or_else(|_| {
                        panic!("{}: bad contentType {}", self.id, self.content_type)
                    }),
            )
        };

        let mut episodes = Episodes::new();
        for (season, list) in &self.episodes {
            let season: i32 = season.parse().expect("numeric season key");
            if list.is_empty() {
                episodes = episodes.add_season(season);
            } else {
                for episode in list {
                    episodes = episodes.add_episode(season, *episode);
                }
            }
        }

        FixtureItemFields {
            info_hash: fixture_info_hash(&self.id),
            info_hash_v1: None,
            info_hash_v2: None,
            name: self.title.clone(),
            size: self.size,
            content_type,
            published_at: PUBLISHED_AT_BASE + self.pub_index * 86_400,
            seeders: self.seeders,
            leechers: self.leechers,
            // The Torznab `files` attr mirrors live Go's `len(Torrent.Files)`,
            // which Go derives from the `files_data` blob — NOT the fixture's
            // `filesCount` (`torrent_contents.files_count`, which Lane G seeds
            // independently to exercise the divergence). Every fixture seeds a
            // `files_data` blob, so the presence-gated summary count equals the
            // enumerated file-list length; an empty list yields `Some(0)`, which
            // the `>0` guard omits, matching Go's single-file behavior.
            files_attr_count: Some(self.files.len() as u32),
            video_resolution: video_resolution(&self.video_resolution),
            video_codec: none_if_empty(&self.video_codec),
            release_group: none_if_empty(&self.release_group),
            episodes,
            // The `year`/`tmdb`/`imdb` attrs come from the content join, which
            // Lane G seeds only when the fixture has a TMDB id.
            release_year: (!self.tmdb.is_empty()).then_some(self.year),
            imdb_id: none_if_empty(&self.imdb),
            tmdb_id: none_if_empty(&self.tmdb),
        }
    }
}

fn fixture_info_hash(id: &str) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(id.as_bytes());
    hasher.finalize().into()
}

fn none_if_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn video_resolution(value: &str) -> Option<VideoResolution> {
    Some(match value {
        "360p" => VideoResolution::V360p,
        "480p" => VideoResolution::V480p,
        "540p" => VideoResolution::V540p,
        "576p" => VideoResolution::V576p,
        "720p" => VideoResolution::V720p,
        "1080p" => VideoResolution::V1080p,
        "1440p" => VideoResolution::V1440p,
        "2160p" => VideoResolution::V2160p,
        "4320p" => VideoResolution::V4320p,
        _ => return None,
    })
}

fn load_fixtures(path: &Path) -> BTreeMap<String, Fixture> {
    read_jsonl::<Fixture>(path)
        .into_iter()
        .map(|fixture| (fixture.id.clone(), fixture))
        .collect()
}
