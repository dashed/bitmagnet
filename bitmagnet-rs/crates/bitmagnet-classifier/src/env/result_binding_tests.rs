//! The `result` binding [`execute`] makes, held to the one it replaced.
//!
//! Until `result` resolved its fields on access, every condition bound it as
//! the whole object, serialized from a struct. [`frozen`] keeps that
//! construction verbatim as the oracle. Every expression below — each field,
//! presence tests, unknown keys, operations through a field, the object as a
//! whole, and every condition in `classifier.core.yml` — is evaluated over a
//! spread of classifications, torrents and flags with `result` bound both ways,
//! and the two `ResolveResult`s, errors included, must be equal.

use std::collections::BTreeMap;
use std::sync::Arc;

use bitmagnet_model::Content;
use bitmagnet_release::{
    Episodes, Video3D, VideoCodec, VideoModifier, VideoResolution, VideoSource,
};
use cel::common::types::{CelBool, CelDouble, CelInt, CelNull, CelString, CelUInt};
use cel::common::value::Val;
use cel::{to_value, Context, ResolveResult};
use serde_json::json;
use serde_yaml::Value as Yaml;

use super::{execute, flags_value, CelExpression, Env, ResultUse, RESULT};
use crate::cel_value::{build_cel_torrent, LazyResult};
use crate::model::{ClassifierInput, ContentType, Date, InputHint};
use crate::result::Classification;
use crate::source::{FlagValue, Source};

/// How `result` was built before it resolved fields on access, verbatim.
mod frozen {
    use serde::Serialize;

    use crate::model::ContentType;
    use crate::result::Classification;

    #[derive(Serialize)]
    pub(super) struct CelClassification {
        #[serde(rename = "contentType")]
        content_type: i32,
        #[serde(rename = "hasAttachedContent")]
        has_attached_content: bool,
        #[serde(rename = "hasBaseTitle")]
        has_base_title: bool,
        languages: Vec<String>,
        episodes: Vec<String>,
        #[serde(rename = "videoResolution")]
        video_resolution: String,
        #[serde(rename = "videoSource")]
        video_source: String,
        #[serde(rename = "videoCodec")]
        video_codec: String,
        #[serde(rename = "releaseGroup")]
        release_group: String,
        #[serde(rename = "contentId")]
        content_id: String,
        #[serde(rename = "contentSource")]
        content_source: String,
    }

    pub(super) fn build_cel_classification(c: &Classification) -> CelClassification {
        CelClassification {
            content_type: c.content_type.map_or(0, ContentType::proto_i32),
            has_attached_content: c.content_attached(),
            has_base_title: c.base_title.is_some(),
            languages: c.languages.clone(),
            episodes: Vec::new(),
            video_resolution: c
                .video_resolution
                .map(|r| r.as_str().to_string())
                .unwrap_or_default(),
            video_source: c
                .video_source
                .map(|s| s.as_str().to_string())
                .unwrap_or_default(),
            video_codec: c
                .video_codec
                .map(|c| c.as_str().to_string())
                .unwrap_or_default(),
            release_group: c.release_group.clone().unwrap_or_default(),
            content_id: c.content.as_ref().map(|x| x.id.clone()).unwrap_or_default(),
            content_source: c
                .content
                .as_ref()
                .map(|x| x.source.clone())
                .unwrap_or_default(),
        }
    }
}

/// The map the old binding bound.
fn frozen_map(c: &Classification) -> Box<dyn Val> {
    to_value(frozen::build_cel_classification(c))
        .expect("serialize result")
        .try_into()
        .expect("convert result")
}

/// `expr` evaluated with `result` bound the old way.
fn frozen_execute(scope: &Context<'_>, expr: &CelExpression, c: &Classification) -> ResolveResult {
    let mut ctx = scope.new_inner_scope();
    ctx.add_variable_from_value(
        RESULT,
        to_value(frozen::build_cel_classification(c)).expect("serialize result"),
    );
    expr.program.execute(&ctx)
}

/// The distinct ways `classifier.core.yml` reads `result`.
const CORE_RESULT_CONDITIONS: [&str; 7] = [
    "result.contentType == contentType.unknown",
    "torrent.hasHintedContentId && !result.hasAttachedContent",
    "result.contentType in [contentType.movie, contentType.tv_show, contentType.xxx]",
    "result.contentType in [contentType.movie, contentType.tv_show]",
    "!result.hasAttachedContent && result.hasBaseTitle",
    "result.contentType in flags.delete_content_types",
    "flags.delete_xxx && result.contentType == contentType.xxx",
];

/// Every field name, and names that are not fields: the two proto fields the
/// transformer leaves out, a near miss on case and spelling, and the empty key.
const KEYS: [&str; 17] = [
    "contentType",
    "hasAttachedContent",
    "hasBaseTitle",
    "languages",
    "episodes",
    "videoResolution",
    "videoSource",
    "videoCodec",
    "releaseGroup",
    "contentId",
    "contentSource",
    "year",
    "video3d",
    "ContentType",
    "content_type",
    "nope",
    "",
];

/// Expressions beyond the per-key selections and presence tests.
const EXPRESSIONS: &[&str] = &[
    // Through a field.
    "size(result.languages)",
    "result.languages.size()",
    "'en' in result.languages",
    "result.languages.exists(l, l == 'fr')",
    "result.languages.all(l, size(l) == 2)",
    "result.languages.map(l, l + '!')",
    "result.languages.filter(l, l != 'en')",
    "result.languages.join('|')",
    "result.languages[0]",
    "result.languages[1]",
    "result.languages['x']",
    "result.languages.nope",
    "has(result.languages.nope)",
    "size(result.episodes)",
    "result.episodes.exists(e, true)",
    "result.episodes == []",
    "result.releaseGroup.matches('^GR')",
    "result.videoResolution.matches('1080')",
    "result.videoSource.matches('(?i)bluray')",
    "result.videoCodec.matches('(')",
    "result.contentId.matches('^[0-9]+$')",
    "result.contentSource.matches('tmdb')",
    "result.contentType.matches('1')",
    "result.contentType == contentType.movie",
    "result.contentType == contentType.unknown",
    "result.contentType != contentType.tv_show",
    "result.contentType > 0",
    "result.contentType + 1",
    "-result.contentType",
    "result.contentType / 0",
    "result.contentType == 1u",
    "result.contentType == 1.0",
    "result.contentType + 'x'",
    "result.contentType.nope",
    "has(result.contentType.nope)",
    "result.hasBaseTitle && !result.hasAttachedContent",
    "result.hasAttachedContent || result.hasBaseTitle",
    "!result.hasBaseTitle",
    "result.hasBaseTitle ? result.releaseGroup : 'none'",
    "result.releaseGroup + result.contentId",
    "result.releaseGroup == ''",
    "size(result.releaseGroup)",
    "result.videoResolution < result.videoSource",
    "has(result.year) || result.contentType == 0",
    "[result.contentType, result.hasBaseTitle, result.languages]",
    "{'type': result.contentType, 'langs': result.languages}",
    "{'type': result.contentType}.type == result.contentType",
    // Inside comprehensions, and shadowed by one.
    "[1, 2].all(x, result.contentType >= 0)",
    "torrent.files.exists(f, result.hasBaseTitle)",
    "torrent.files.map(f, result.contentType)",
    "[{'contentType': 5}].exists(result, result.contentType == 5)",
    "[1].map(result, result + 1)",
    // The object as a whole.
    "result",
    "size(result)",
    "result.size()",
    "result == result",
    "result != result",
    "'contentType' in result",
    "'year' in result",
    "1 in result",
    "result['contentType']",
    "result['year']",
    "result[1]",
    "result[true]",
    "result[1.5]",
    "result.all(k, k != '')",
    "result.exists(k, k == 'languages')",
    "result.exists_one(k, k == 'episodes')",
    "size(result.map(k, k))",
    "[result]",
    "[result][0].contentType",
    "{'r': result}.r.hasBaseTitle",
    "result + 1",
    "result == {}",
    "{} == result",
    "result == {'contentType': 0, 'hasAttachedContent': false, 'hasBaseTitle': false, \
     'languages': [], 'episodes': [], 'videoResolution': '', 'videoSource': '', \
     'videoCodec': '', 'releaseGroup': '', 'contentId': '', 'contentSource': ''}",
    "{'contentType': 0, 'hasAttachedContent': false, 'hasBaseTitle': false, \
     'languages': [], 'episodes': [], 'videoResolution': '', 'videoSource': '', \
     'videoCodec': '', 'releaseGroup': '', 'contentId': '', 'contentSource': ''} == result",
    "type(result)",
    "dyn(result).contentType",
    "result.contentType == 0 && size(result) == 11",
    "has(result.contentType) && 'languages' in result",
    // Never names it.
    "true",
    "torrent.baseName != ''",
    "flags.delete_xxx",
    "'result' == 'result'",
];

/// Every condition string in `classifier.core.yml`.
fn core_conditions() -> Vec<String> {
    fn strings(yaml: &Yaml, out: &mut Vec<String>) {
        match yaml {
            Yaml::String(s) => out.push(s.clone()),
            Yaml::Sequence(items) => items.iter().for_each(|item| strings(item, out)),
            Yaml::Mapping(map) => map.values().for_each(|value| strings(value, out)),
            _ => {}
        }
    }
    fn conditions(yaml: &Yaml, out: &mut Vec<String>) {
        match yaml {
            Yaml::Sequence(items) => items.iter().for_each(|item| conditions(item, out)),
            Yaml::Mapping(map) => {
                for (key, value) in map {
                    if key.as_str() == Some("condition") {
                        strings(value, out);
                    } else {
                        conditions(value, out);
                    }
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    for workflow in Source::load_core().expect("core source").workflows.values() {
        conditions(workflow, &mut out);
    }
    out.sort();
    out.dedup();
    out
}

/// Every expression the battery evaluates.
fn expressions() -> Vec<String> {
    let mut all: Vec<String> = KEYS
        .iter()
        .flat_map(|key| [format!("result.{key}"), format!("has(result.{key})")])
        // `result.` and `has(result.)` do not parse.
        .filter(|source| !source.ends_with('.') && !source.ends_with(".)"))
        .collect();
    all.extend(EXPRESSIONS.iter().map(|source| (*source).to_owned()));
    all.extend(core_conditions());
    all
}

fn content(content_type: bitmagnet_model::ContentType, original_language: Option<&str>) -> Content {
    Content {
        content_type,
        source: "tmdb".to_owned(),
        id: "77117".to_owned(),
        title: "Sunny".to_owned(),
        release_date: None,
        release_year: Some(2011),
        adult: None,
        original_language: original_language.map(str::to_owned),
        original_title: None,
        overview: None,
        runtime: None,
        popularity: None,
        vote_average: None,
        vote_count: None,
        created_at: None,
        updated_at: None,
        tsv: bitmagnet_fts::Tsvector::default(),
        collections: Vec::new(),
        attributes: Vec::new(),
    }
}

fn classifications() -> Vec<(String, Classification)> {
    let mut episodes = Episodes::default();
    episodes.add_episode(2, 3);
    let every_field = Classification {
        content_type: Some(ContentType::TvShow),
        base_title: Some("Example Show".to_owned()),
        date: Date {
            year: 2024,
            month: 5,
            day: 6,
        },
        languages: vec!["en".to_owned(), "fr".to_owned()],
        language_multi: true,
        episodes: episodes.clone(),
        video_resolution: Some(VideoResolution::V1080p),
        video_source: Some(VideoSource::Bluray),
        video_codec: Some(VideoCodec::X265),
        video_3d: Some(Video3D::V3DSbs),
        video_modifier: Some(VideoModifier::Remux),
        release_group: Some("GROUP".to_owned()),
        content: Some(content(bitmagnet_model::ContentType::TvShow, None)),
        tags: ["a-tag".to_owned()].into(),
    };

    let mut attached = Classification::default();
    attached.attach_content(content(bitmagnet_model::ContentType::Movie, Some("ja")));

    let mut hinted = Classification::default();
    hinted.apply_hint(&InputHint {
        content_type: "tv_show".to_owned(),
        content_source: "tvdb".to_owned(),
        content_id: "81189".to_owned(),
        episodes: bitmagnet_release::parse_episodes("S01E02"),
        languages: vec!["de".to_owned()],
        video_resolution: Some(VideoResolution::V720p.as_str().to_owned()),
        video_source: Some(VideoSource::Tv.as_str().to_owned()),
        video_codec: Some(VideoCodec::H264.as_str().to_owned()),
        video_3d: Some(Video3D::V3DOu.as_str().to_owned()),
        video_modifier: Some(VideoModifier::Screener.as_str().to_owned()),
        release_group: Some("GRP".to_owned()),
    });
    assert!(hinted.video_codec.is_some() && hinted.video_3d.is_some());

    let mut all = vec![
        ("default".to_owned(), Classification::default()),
        ("every field set".to_owned(), every_field),
        (
            "allocated empty episodes".to_owned(),
            Classification {
                episodes: Episodes::allocated(),
                ..Classification::default()
            },
        ),
        (
            "episodes".to_owned(),
            Classification {
                content_type: Some(ContentType::TvShow),
                episodes,
                ..Classification::default()
            },
        ),
        (
            "one language".to_owned(),
            Classification {
                languages: vec!["en".to_owned()],
                ..Classification::default()
            },
        ),
        (
            "several languages".to_owned(),
            Classification {
                languages: vec!["en".to_owned(), "fr".to_owned(), "de".to_owned()],
                language_multi: true,
                ..Classification::default()
            },
        ),
        ("attached content".to_owned(), attached),
        ("hint applied".to_owned(), hinted),
        (
            "base title only".to_owned(),
            Classification {
                base_title: Some(String::new()),
                ..Classification::default()
            },
        ),
        (
            "odd strings".to_owned(),
            Classification {
                release_group: Some("a\"b ü 日本 \\".to_owned()),
                languages: vec![String::new()],
                ..Classification::default()
            },
        ),
    ];
    all.extend(ContentType::all().iter().map(|content_type| {
        (
            content_type.as_str().to_owned(),
            Classification {
                content_type: Some(*content_type),
                ..Classification::default()
            },
        )
    }));
    all
}

fn torrents() -> Vec<ClassifierInput> {
    [
        json!({
            "name": "Example.Show.S02E03.1080p.BluRay.x265-GROUP.mkv",
            "size": 2_000_000_000_u64,
            "filesStatus": "single",
            "extension": "mkv",
        }),
        json!({
            "name": "Some Album",
            "size": 31_000_000,
            "filesStatus": "multi",
            "filesCount": 2,
            "files": [
                {"index": 0, "path": "Some Album/01.flac", "extension": "flac", "size": 30_000_000},
                {"index": 1, "path": "Some Album/cover.jpg", "extension": "jpg", "size": 1_000_000},
            ],
        }),
        json!({
            "name": "Hinted Movie 2011",
            "size": 700_000_000,
            "filesStatus": "no_info",
            "hint": {"contentType": "movie", "contentSource": "tmdb", "contentId": "77117"},
        }),
    ]
    .into_iter()
    .map(|input| serde_json::from_value(input).expect("classifier input"))
    .collect()
}

fn flag_sets() -> Vec<BTreeMap<String, FlagValue>> {
    let defaults = Source::load_core().expect("core source").flags;
    let mut deleting = defaults.clone();
    deleting.insert(
        "delete_content_types".to_owned(),
        FlagValue::ContentTypeList(vec![ContentType::Movie, ContentType::Xxx]),
    );
    deleting.insert("delete_xxx".to_owned(), FlagValue::Bool(true));
    vec![defaults, deleting]
}

#[test]
fn core_conditions_read_result_through_fields_alone() {
    let conditions = core_conditions();
    assert_eq!(conditions.len(), 22, "{conditions:#?}");
    let fields: Vec<&str> = conditions
        .iter()
        .filter_map(|source| {
            let expr = CelExpression::compile(source).expect("core condition compiles");
            match expr.result {
                ResultUse::Unused => None,
                ResultUse::Fields => Some(source.as_str()),
                ResultUse::Whole => panic!("{source} uses `result` as a whole"),
            }
        })
        .collect();
    let mut expected = CORE_RESULT_CONDITIONS.to_vec();
    expected.sort_unstable();
    assert_eq!(fields, expected);
}

#[test]
fn how_result_is_read_decides_its_binding() {
    for (source, expected) in [
        ("result.contentType == 0", ResultUse::Fields),
        ("has(result.year)", ResultUse::Fields),
        ("result.languages.exists(l, l == 'en')", ResultUse::Fields),
        ("[1].all(x, result.hasBaseTitle)", ResultUse::Fields),
        ("{'a': result.contentType}", ResultUse::Fields),
        (
            "[{'contentType': 5}].exists(result, result.contentType == 5)",
            ResultUse::Fields,
        ),
        ("result", ResultUse::Whole),
        ("size(result)", ResultUse::Whole),
        ("result.size()", ResultUse::Whole),
        ("result == result", ResultUse::Whole),
        ("'contentType' in result", ResultUse::Whole),
        ("result['contentType']", ResultUse::Whole),
        ("result.all(k, k != '')", ResultUse::Whole),
        ("[result]", ResultUse::Whole),
        ("{'r': result}", ResultUse::Whole),
        (
            "result.contentType == 0 && size(result) > 0",
            ResultUse::Whole,
        ),
        ("[1].map(result, result + 1)", ResultUse::Whole),
        ("torrent.name != ''", ResultUse::Unused),
        ("[1].exists(r, r == 1)", ResultUse::Unused),
        ("'result' == 'result'", ResultUse::Unused),
    ] {
        assert_eq!(
            CelExpression::compile(source).expect("compiles").result,
            expected,
            "{source}"
        );
    }
}

#[test]
fn every_expression_resolves_exactly_as_with_the_eager_binding() {
    let env = Env::build(&Source::load_core().expect("core source")).expect("env");
    let expressions: Vec<(String, CelExpression)> = expressions()
        .into_iter()
        .map(|source| {
            let expr = CelExpression::compile(&source)
                .unwrap_or_else(|err| panic!("compile {source}: {err}"));
            (source, expr)
        })
        .collect();
    let torrents = torrents();
    let flag_sets = flag_sets();

    let mut evaluated = BTreeMap::<ResultUse, usize>::new();
    for (name, classification) in classifications() {
        let shared = Arc::new(classification);
        for torrent in &torrents {
            for flags in &flag_sets {
                let torrent = to_value(build_cel_torrent(torrent)).expect("serialize torrent");
                let scope = env.run_scope(torrent, flags_value(flags));
                for (source, expr) in &expressions {
                    assert_eq!(
                        execute(&scope, expr, &shared),
                        frozen_execute(&scope, expr, &shared),
                        "{source} over {name}"
                    );
                    // Nothing it bound outlives the evaluation, so the engine
                    // gets the classification back without a copy.
                    assert_eq!(Arc::strong_count(&shared), 1, "{source} over {name}");
                    *evaluated.entry(expr.result).or_default() += 1;
                }
            }
        }
    }
    // Each binding is exercised, the lazy one the most.
    assert!(evaluated[&ResultUse::Unused] > 0 && evaluated[&ResultUse::Whole] > 0);
    assert!(evaluated[&ResultUse::Fields] > evaluated[&ResultUse::Whole]);
}

#[test]
fn lazy_result_answers_every_map_operation_as_the_eager_map_does() {
    let mut keys: Vec<Box<dyn Val>> = KEYS
        .iter()
        .map(|key| Box::new(CelString::from(*key)) as Box<dyn Val>)
        .collect();
    keys.extend([
        Box::new(CelInt::from(0)) as Box<dyn Val>,
        Box::new(CelUInt::from(1)),
        Box::new(CelBool::from(true)),
        Box::new(CelDouble::from(1.5)),
        Box::new(CelNull),
    ]);
    let classifications = classifications();

    for (name, classification) in &classifications {
        let eager = frozen_map(classification);
        let lazy = LazyResult::new(Arc::new(classification.clone()));

        assert_eq!(lazy.get_type(), eager.get_type(), "{name}");
        for key in &keys {
            let (lazy_get, eager_get) = (
                lazy.as_indexer().expect("indexer").get(key.as_ref()),
                eager.as_indexer().expect("indexer").get(key.as_ref()),
            );
            assert_eq!(lazy_get, eager_get, "get {key:?} over {name}");
            assert_eq!(
                lazy.clone_as_boxed()
                    .into_indexer()
                    .expect("indexer")
                    .steal(key.as_ref()),
                eager
                    .clone_as_boxed()
                    .into_indexer()
                    .expect("indexer")
                    .steal(key.as_ref()),
                "steal {key:?} over {name}"
            );
            assert_eq!(
                lazy.as_container()
                    .expect("container")
                    .contains(key.as_ref()),
                eager
                    .as_container()
                    .expect("container")
                    .contains(key.as_ref()),
                "contains {key:?} over {name}"
            );
        }

        let size = |map: &dyn Val| map.as_sizer().map(|sizer| *sizer.size().inner());
        assert_eq!(size(&lazy), size(eager.as_ref()), "{name}");
        let zero = |map: &dyn Val| map.as_zeroer().map(|zeroer| zeroer.is_zero_value());
        assert_eq!(zero(&lazy), zero(eager.as_ref()), "{name}");
        let keys_of = |map: &dyn Val| {
            let mut iter = map.as_iterable().expect("iterable").iter();
            let mut keys = Vec::new();
            while let Some(key) = iter.next() {
                keys.push(
                    key.downcast_ref::<CelString>()
                        .expect("string key")
                        .inner()
                        .to_owned(),
                );
            }
            keys.sort();
            keys
        };
        assert_eq!(keys_of(&lazy), keys_of(eager.as_ref()), "{name}");
        for (other_name, other) in &classifications {
            let other_lazy = LazyResult::new(Arc::new(other.clone()));
            let other_eager = frozen_map(other);
            let want = eager.equals(other_eager.as_ref());
            assert_eq!(lazy.equals(&other_lazy), want, "{name} == {other_name}");
            assert_eq!(
                lazy.equals(other_eager.as_ref()),
                want,
                "{name} == {other_name}"
            );
        }
        for (lazy_trait, eager_trait) in [
            (lazy.as_adder().is_some(), eager.as_adder().is_some()),
            (lazy.as_comparer().is_some(), eager.as_comparer().is_some()),
            (lazy.as_negator().is_some(), eager.as_negator().is_some()),
            (
                lazy.as_subtractor().is_some(),
                eager.as_subtractor().is_some(),
            ),
        ] {
            assert_eq!(lazy_trait, eager_trait, "{name}");
        }
    }
}
