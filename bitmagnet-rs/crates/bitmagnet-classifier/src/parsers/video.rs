//! Port of `internal/classifier/parsers/video.go ParseVideoContent`.
//!
//! 🚧 Lane-R dependency status (contract §3): the video-attribute inference
//! (`video_resolution` / `video_source` / `video_codec` + `release_group`) is
//! LANDED in Lane R and wired here. The following are Lane-R-PENDING and
//! stubbed — every fixture whose golden needs one of these is blocked on R:
//!
//! * **title/year extraction** (`titleRegex` / `titleYearRegex` /
//!   `titleEpisodesRegex` in `parsers/video.go`) — the linchpin: without it
//!   `ParseTitleYearEpisodes` cannot run, so `base_title`/`year`/the
//!   movie-vs-tv content-type inference are unavailable. Stubbed to "unmatched".
//! * **`InferLanguages`** (`languages.csv`) — stubbed to empty.
//! * **`InferVideo3D`** / **`InferVideoModifier`** — stubbed to `None`.
//!
//! When R exports these, swap the three stub fns for R calls; the orchestration
//! below is already the faithful shape.

use std::sync::OnceLock;

use bitmagnet_release::{
    infer_video_codec_and_release_group, infer_video_resolution, infer_video_source, Episodes,
};
use regex::Regex;

use crate::errors::FlowError;
use crate::model::{ClassifierInput, ContentType, Date};
use crate::result::Classification;

/// `multiRegex` — `keywords.MustNewRegexFromKeywords("multi", "dual")`, built
/// with Lane R's keyword-glob compiler.
fn multi_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        let pattern = bitmagnet_release::regex_pattern_from_keywords(&["multi", "dual"])
            .expect("compile multi/dual keyword regex");
        Regex::new(&pattern).expect("valid multi/dual regex")
    })
}

struct TitleYearEpisodes {
    title: String,
    year: u16,
    episodes: Episodes,
    #[allow(dead_code)]
    rest: String,
}

/// 🚧 Lane-R-PENDING: `ParseTitleYearEpisodes`. Always returns "unmatched"
/// until R lands the title/year extraction regexes.
fn parse_title_year_episodes(
    _content_type: Option<ContentType>,
    _input: &str,
) -> Result<TitleYearEpisodes, FlowError> {
    Err(FlowError::Unmatched)
}

/// 🚧 Lane-R-PENDING: `InferLanguages`. Empty until R lands `languages.csv`.
fn infer_languages(_input: &str) -> Vec<String> {
    Vec::new()
}

/// 🚧 Lane-R-PENDING: `InferVideo3D`.
fn infer_video_3d(_input: &str) -> Option<String> {
    None
}

/// 🚧 Lane-R-PENDING: `InferVideoModifier`.
fn infer_video_modifier(_input: &str) -> Option<String> {
    None
}

/// Port of `ParseVideoContent`. Returns `(Some(attrs), None)` on success (the
/// attributes to `merge` into the result), or `(None, Some(err))` when
/// `ParseTitleYearEpisodes` fails and no content type is set (the `ErrUnmatched`
/// early return).
pub(crate) fn parse_video_content(
    input: &ClassifierInput,
    result: &Classification,
) -> (Option<Classification>, Option<FlowError>) {
    let name = &input.name;

    let (mut title, year, mut episodes, mut rest) =
        match parse_title_year_episodes(result.content_type, name) {
            Ok(t) => (t.title, t.year, t.episodes, t.rest),
            Err(_) => {
                if result.content_type.is_none() {
                    return (None, Some(FlowError::Unmatched));
                }
                (String::new(), 0u16, Episodes::new(), name.clone())
            }
        };

    let ct: Option<ContentType> = if result.content_type.is_some() {
        result.content_type
    } else if !episodes.is_empty() || result.date.is_valid() {
        Some(ContentType::TvShow)
    } else if year != 0 {
        Some(ContentType::Movie)
    } else {
        None
    };

    if ct != Some(ContentType::TvShow) {
        episodes = Episodes::new();
        if year == 0 {
            title = String::new();
            rest = name.clone();
        }
    }

    let (codec, release_group) = infer_video_codec_and_release_group(&rest);
    let attrs = Classification {
        content_type: ct,
        base_title: if title.is_empty() { None } else { Some(title) },
        date: Date {
            year,
            ..Date::default()
        },
        episodes,
        languages: infer_languages(&rest),
        language_multi: multi_regex().is_match(&rest),
        video_resolution: infer_video_resolution(&rest),
        video_source: infer_video_source(&rest),
        video_codec: codec,
        video_3d: infer_video_3d(&rest),
        video_modifier: infer_video_modifier(&rest),
        release_group,
        content_attached: false,
    };

    (Some(attrs), None)
}
