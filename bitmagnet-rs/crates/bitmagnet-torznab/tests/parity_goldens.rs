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
use std::path::{Path, PathBuf};

use bitmagnet_search_query::{ContentType, Episodes, VideoResolution};
use bitmagnet_torznab::{
    item_from_fixture_fields, parse, to_search_params, Channel, FixtureItemFields, Profile,
    Response, SearchResult, TorznabError,
};
use serde::Deserialize;
use sha1::{Digest, Sha1};

/// Seconds from the Unix epoch to 2020-01-01T00:00:00Z — the base Lane G's seed
/// adds `pub * 24h` to for each fixture's `published_at`.
const PUBLISHED_AT_BASE: i64 = 1_577_836_800;

fn goldens_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/torznab")
}

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
            files_count: Some(self.files.len() as u32),
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

fn load_corpus(path: &Path) -> Vec<CorpusQuery> {
    read_jsonl::<CorpusQuery>(path)
}

fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &Path) -> Vec<T> {
    let text = fs::read_to_string(path).expect("jsonl is readable");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl line parses"))
        .collect()
}

#[derive(Debug, Deserialize)]
struct CorpusQuery {
    id: String,
    kind: String,
    path: String,
    #[serde(default, rename = "expectIds")]
    expect_ids: Option<Vec<String>>,
}

impl CorpusQuery {
    fn golden_name(&self) -> String {
        if self.id == "caps" {
            "caps.golden.xml".to_owned()
        } else {
            format!("q-{}.golden.xml", self.id)
        }
    }
}

// ---- canonical XML normalizer (port of internal/parity/torznab_xml.go) ------

#[derive(Debug)]
enum Child {
    Element(Element),
    Text(String),
}

#[derive(Debug)]
struct Element {
    name: String,
    attrs: Vec<(String, String)>,
    children: Vec<Child>,
}

fn normalize(raw: &[u8]) -> Vec<u8> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_reader(raw);
    let mut buf = Vec::new();
    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;

    loop {
        match reader.read_event_into(&mut buf).expect("valid XML") {
            Event::Eof => break,
            Event::Start(start) => stack.push(element_from(&start)),
            Event::Empty(empty) => {
                let element = element_from(&empty);
                attach(&mut stack, &mut root, element);
            }
            Event::End(_) => {
                let element = stack.pop().expect("balanced end tag");
                attach(&mut stack, &mut root, element);
            }
            Event::Text(text) => {
                let value = unescape_bytes(&text.into_inner());
                if value.trim().is_empty() {
                    continue;
                }
                let parent = stack.last_mut().expect("text within an element");
                parent.children.push(Child::Text(value));
            }
            Event::CData(cdata) => {
                let value =
                    String::from_utf8(cdata.into_inner().into_owned()).expect("utf-8 cdata");
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(Child::Text(value));
                }
            }
            // XML declaration, comments, processing instructions, doctype: the
            // canonical tree carries only elements and text.
            _ => {}
        }
        buf.clear();
    }

    let root = root.expect("document has a root element");
    let mut out = String::new();
    write_element(&mut out, &root, 0);
    out.into_bytes()
}

fn element_from(start: &quick_xml::events::BytesStart<'_>) -> Element {
    let name = String::from_utf8(start.name().as_ref().to_vec()).expect("utf-8 element name");
    let attrs = start
        .attributes()
        .map(|attr| {
            let attr = attr.expect("well-formed attribute");
            let key = String::from_utf8(attr.key.as_ref().to_vec()).expect("utf-8 attr name");
            let value = unescape_bytes(&attr.value);
            (key, value)
        })
        .collect();
    Element {
        name,
        attrs,
        children: Vec::new(),
    }
}

/// Decode an escaped XML byte run to its text. `quick_xml::escape::unescape`
/// resolves the five predefined entities plus decimal/hex numeric character
/// references (`&#34;`, `&#x9;`) — exactly the forms Go's `encoding/xml` writes.
fn unescape_bytes(bytes: &[u8]) -> String {
    let raw = std::str::from_utf8(bytes).expect("utf-8 xml text");
    quick_xml::escape::unescape(raw)
        .expect("valid xml escapes")
        .into_owned()
}

fn attach(stack: &mut [Element], root: &mut Option<Element>, element: Element) {
    match stack.last_mut() {
        Some(parent) => parent.children.push(Child::Element(element)),
        None => *root = Some(element),
    }
}

fn write_element(out: &mut String, element: &Element, depth: usize) {
    let indent = "  ".repeat(depth);
    out.push_str(&indent);
    out.push('<');
    out.push_str(&element.name);

    let mut attrs = element.attrs.clone();
    attrs.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, value) in &attrs {
        out.push(' ');
        out.push_str(name);
        out.push_str("=\"");
        push_escaped(out, value, true);
        out.push('"');
    }

    if element.children.is_empty() {
        out.push_str("/>\n");
        return;
    }

    let has_text = element
        .children
        .iter()
        .any(|child| matches!(child, Child::Text(_)));
    let has_element = element
        .children
        .iter()
        .any(|child| matches!(child, Child::Element(_)));
    assert!(
        !(has_text && has_element),
        "mixed content in <{}> is unsupported",
        element.name
    );

    if has_text {
        out.push('>');
        for child in &element.children {
            if let Child::Text(text) = child {
                push_escaped(out, text, false);
            }
        }
        out.push_str("</");
        out.push_str(&element.name);
        out.push_str(">\n");
        return;
    }

    out.push_str(">\n");
    for child in &element.children {
        if let Child::Element(element) = child {
            write_element(out, element, depth + 1);
        }
    }
    out.push_str(&indent);
    out.push_str("</");
    out.push_str(&element.name);
    out.push_str(">\n");
}

/// Canonical escaping (Go `writeCanonicalXMLEscaped`): `& < >` always; `"` only
/// in attribute values; everything else — including `'`, tab and newline — is
/// written verbatim.
fn push_escaped(out: &mut String, value: &str, attribute: bool) {
    for character in value.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' if attribute => out.push_str("&quot;"),
            _ => out.push(character),
        }
    }
}

fn first_diff(actual: &[u8], expected: &[u8]) -> String {
    let actual = String::from_utf8_lossy(actual);
    let expected = String::from_utf8_lossy(expected);
    for (index, (a, e)) in actual.lines().zip(expected.lines()).enumerate() {
        if a != e {
            return format!(
                "  first diff at line {}:\n  actual:   {a:?}\n  expected: {e:?}",
                index + 1
            );
        }
    }
    if actual.lines().count() != expected.lines().count() {
        return format!(
            "  line count differs: actual {} vs expected {}",
            actual.lines().count(),
            expected.lines().count(),
        );
    }
    "  (bytes differ only in trailing whitespace)".to_owned()
}
