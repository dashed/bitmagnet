//! Port of `internal/classifier/parsers/video.go` — title/year/episode
//! extraction and the top-level `ParseVideoContent` orchestration.
//!
//! The title regexes are hardcoded as *templates* that mirror Go's `rex` output
//! after the ASCII adaptation (`\d`/`\w`/`\s` → explicit ASCII classes,
//! `[[:upper:]]` kept). [`goclass::pin_letter_class`] then splices out every
//! `\p{L}` for the class Go's RE2 actually applies — `regex` 1.13's `\p{L}` is
//! 4,924 code points wider — and it is the PINNED result, not the template,
//! that is byte-equality tested against the Go oracle in
//! `testdata/parity/release/patterns.jsonl`.
//!
//! 🔑 Episode-group offset: `titleEpisodesRegex` puts the title in group 1, so
//! `EpisodesToken`'s groups shift to 2.., and Go passes `match[2:]` to
//! `EpisodesMatchToEpisodes`. That +1 shift **cancels** the extra-capture bug
//! that makes standalone `ParseEpisodes` wrong (see `episodes`), so episodes
//! parsed through a title are *correct* (`S01E01` → `S01E01`). The same
//! index-based `episodes_match_to_episodes` serves both call sites; only the
//! slice offset differs.

use std::sync::LazyLock;

use regex::{Captures, Regex};

use crate::content_type::ContentType;
use crate::episodes::{episodes_match_to_episodes, Episodes};
use crate::goclass;
use crate::keywords::regex_pattern_from_keywords;
use crate::language::infer_languages;
use crate::video::{
    infer_video_3d, infer_video_codec_and_release_group, infer_video_modifier,
    infer_video_resolution, infer_video_source, Video3D, VideoCodec, VideoModifier,
    VideoResolution, VideoSource,
};

const TITLE_PATTERN_TEMPLATE: &str =
    r#"^(?:((?:(?:[\p{L}0-9]+(?:\x2D[\p{L}0-9]+)*)|[^\p{L}0-9]+)+(?:[^\p{L}0-9]+|$)))"#;

const TITLE_YEAR_PATTERN_TEMPLATE: &str = r#"^(?:(?:((?:(?:[\p{L}0-9]+(?:\x2D[\p{L}0-9]+)*)|[^\p{L}0-9]+)+(?:[^\p{L}0-9]+|$)))(?:(?:[^0-9A-Za-z_]*)((?:18|19|20)[0-9]{2})(?:[^0-9A-Za-z_]|$)))"#;

const TITLE_EPISODES_PATTERN_TEMPLATE: &str = r#"^(?:(?:((?:(?:[\p{L}0-9]+(?:\x2D[\p{L}0-9]+)*)|[^\p{L}0-9]+)+(?:[^\p{L}0-9]+|$)))((?:(?:(?:(?:(?:[Ss][Ee][Aa][Ss][Oo][Nn]))|(?:(?:[Ss])))[\t\n\f\r ]?(([0-9]{1,2})(?:(?:[\t\n\f\r ]?\x2D[\t\n\f\r ]?(?:[sS]?[\t\n\f\r ]?)?([0-9]{1,2}))|(?:[\t\n\f\r ]?,[\t\n\f\r ]?(?:[sS]?[\t\n\f\r ]?)?([0-9]{1,2})[\t\n\f\r ]?)+)?)[\t\n\f\r ]?)(?:(?:(?:(?:[Ee][Pp][Ii][Ss][Oo][Dd][Ee]))|(?:(?:[Ee][Pp]))|(?:(?:[Ee])))[\t\n\f\r ]?(([0-9]{1,2})(?:(?:[\t\n\f\r ]?\x2D[\t\n\f\r ]?(?:[eE]?[\t\n\f\r ]?)?([0-9]{1,2}))|(?:[\t\n\f\r ]?,[\t\n\f\r ]?(?:[eE]?[\t\n\f\r ]?)?([0-9]{1,2})[\t\n\f\r ]?)+)?))?)|(?:(?:([0-9]{1,2})[xX]([0-9]{1,2}))(?:[\t\n\f\r ]?\x2D[\t\n\f\r ]?([0-9]{1,2}))?)))"#;

const TITLE_PART_PATTERN_TEMPLATE: &str = r#"[ \._]?((?:[\('"]*(?:(?:[[:upper:]]\.){2,}|(?:[\p{L}0-9]+(?:['\x2D]+[\p{L}0-9]+)*))[,;:\?!\x2D\)'"]*))[ \._]?"#;

const TRIM_TITLE_PATTERN_TEMPLATE: &str = r#"^(?:(?:\[[^\]]+\])|(?:\x{3010}[^\x{3011}]+\x{3011}))?[^\p{L}0-9]*((?:[\('"]*(?:(?:[[:upper:]]\.){2,}|(?:[\p{L}0-9]+(?:['\x2D]+[\p{L}0-9]+)*))[,;:\?!\x2D\)'"]*)(?:.(?:[\('"]*(?:(?:[[:upper:]]\.){2,}|(?:[\p{L}0-9]+(?:['\x2D]+[\p{L}0-9]+)*))[,;:\?!\x2D\)'"]*))*)[^\p{L}0-9]*$"#;

/// The five templates above with every `\p{L}` replaced by the Go-pinned
/// letter class. These — not the templates — are what gets compiled, and what
/// `title_patterns_match_go` byte-compares against the Go oracle.
static TITLE_PATTERN: LazyLock<String> =
    LazyLock::new(|| goclass::pin_letter_class(TITLE_PATTERN_TEMPLATE));
static TITLE_YEAR_PATTERN: LazyLock<String> =
    LazyLock::new(|| goclass::pin_letter_class(TITLE_YEAR_PATTERN_TEMPLATE));
static TITLE_EPISODES_PATTERN: LazyLock<String> =
    LazyLock::new(|| goclass::pin_letter_class(TITLE_EPISODES_PATTERN_TEMPLATE));
static TITLE_PART_PATTERN: LazyLock<String> =
    LazyLock::new(|| goclass::pin_letter_class(TITLE_PART_PATTERN_TEMPLATE));
static TRIM_TITLE_PATTERN: LazyLock<String> =
    LazyLock::new(|| goclass::pin_letter_class(TRIM_TITLE_PATTERN_TEMPLATE));

static TITLE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(&TITLE_PATTERN).expect("title re"));
static TITLE_YEAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&TITLE_YEAR_PATTERN).expect("title_year re"));
static TITLE_EPISODES_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&TITLE_EPISODES_PATTERN).expect("title_episodes re"));
static TITLE_PART_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&TITLE_PART_PATTERN).expect("title_part re"));
static TRIM_TITLE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&TRIM_TITLE_PATTERN).expect("trim_title re"));

static MULTI_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&regex_pattern_from_keywords(&["multi", "dual"]).expect("multi keywords"))
        .expect("multi re")
});

/// Port of `cleanTitle`.
fn clean_title(title: &str) -> String {
    // Replace each title-part match with its group 1 + " ".
    let replaced = TITLE_PART_RE.replace_all(title, |caps: &Captures<'_>| match caps.get(1) {
        Some(m) => format!("{} ", m.as_str()),
        None => String::new(),
    });
    // trimTitle: anchored, replace the whole match with group 1.
    TRIM_TITLE_RE.replace_all(&replaced, "${1}").into_owned()
}

/// Port of `parseTitleYear` → `(title, year, rest)`.
fn parse_title_year(input: &str) -> Option<(String, u16, String)> {
    let caps = TITLE_YEAR_RE.captures(input)?;
    // Go strconv.ParseUint(match[2], 10, 16); the group is exactly 4 digits.
    let year = caps
        .get(2)
        .and_then(|m| m.as_str().parse::<u16>().ok())
        .unwrap_or(0);
    let title = clean_title(caps.get(1).map_or("", |m| m.as_str()));
    if title.is_empty() {
        return None;
    }
    let rest = input[caps.get(0).unwrap().end()..].to_string();
    Some((title, year, rest))
}

/// Port of `parseTitle` → `(title, rest)`.
fn parse_title(input: &str) -> Option<(String, String)> {
    let caps = TITLE_RE.captures(input)?;
    let title = clean_title(caps.get(1).map_or("", |m| m.as_str()));
    if title.is_empty() {
        return None;
    }
    let rest = input[caps.get(0).unwrap().end()..].to_string();
    Some((title, rest))
}

/// Port of `parseTitleYearEpisodes` → `(title, year, episodes, rest)`.
fn parse_title_year_episodes(input: &str) -> Option<(String, u16, Episodes, String)> {
    let caps = TITLE_EPISODES_RE.captures(input)?;
    let raw_title = caps.get(1).map_or("", |m| m.as_str());
    let (title, year) = match parse_title_year(raw_title) {
        Some((t, y, _)) => (t, y),
        None => (clean_title(raw_title), 0),
    };
    // EpisodesMatchToEpisodes(match[2:]): groups from index 2 onward.
    let groups: Vec<&str> = (2..caps.len())
        .map(|i| caps.get(i).map_or("", |m| m.as_str()))
        .collect();
    let episodes = episodes_match_to_episodes(&groups);
    let rest = input[caps.get(0).unwrap().end()..].to_string();
    Some((title, year, episodes, rest))
}

/// Port of `ParseTitleYearEpisodes`. Returns `None` for `ErrUnmatched`.
pub fn parse_title_year_episodes_dispatch(
    content_type: Option<ContentType>,
    input: &str,
) -> Option<(String, u16, Episodes, String)> {
    if content_type.is_none() || content_type == Some(ContentType::TvShow) {
        if let Some(r) = parse_title_year_episodes(input) {
            return Some(r);
        }
    }
    if let Some((title, year, rest)) = parse_title_year(input) {
        return Some((title, year, Episodes::new(), rest));
    }
    if let Some((title, rest)) = parse_title(input) {
        return Some((title, 0, Episodes::new(), rest));
    }
    None
}

/// The parsed video-content attributes — the release-crate analog of Go's
/// `classification.ContentAttributes` (the fields `ParseVideoContent` sets).
///
/// 🚨 `video_3d`/`year` live here (the ContentAttributes surface) exactly as Go.
/// The proto transformer's omission of them (contracts §0 #2) is Lane C's job,
/// not this crate's.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContentAttributes {
    pub content_type: Option<ContentType>,
    pub base_title: Option<String>,
    /// `Date.Year` (0 == nil). Month/day are not set by `ParseVideoContent`.
    pub year: u16,
    pub episodes: Episodes,
    /// alpha2 codes in `Languages.Slice()` (natsort-by-name) order.
    pub languages: Vec<String>,
    pub language_multi: bool,
    pub video_resolution: Option<VideoResolution>,
    pub video_source: Option<VideoSource>,
    pub video_codec: Option<VideoCodec>,
    pub video_3d: Option<Video3D>,
    pub video_modifier: Option<VideoModifier>,
    pub release_group: Option<String>,
}

/// Port of `ParseVideoContent`. `content_type` and `date_valid` come from the
/// classifier's `result` (its `ContentType` hint and whether `result.Date`
/// `IsValid()`). Returns `None` only for the error case Go returns — an
/// unmatched title AND no content-type hint.
pub fn parse_video_content(
    name: &str,
    content_type: Option<ContentType>,
    date_valid: bool,
) -> Option<ContentAttributes> {
    let (mut title, year, mut episodes, mut rest) =
        match parse_title_year_episodes_dispatch(content_type, name) {
            Some(t) => t,
            None => {
                // ErrUnmatched: fail only if there is no content-type hint.
                content_type?;
                (String::new(), 0, Episodes::new(), name.to_string())
            }
        };

    let ct = if content_type.is_some() {
        content_type
    } else if !episodes.is_empty() || date_valid {
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
            rest = name.to_string();
        }
    }

    let (video_codec, release_group) = infer_video_codec_and_release_group(&rest);
    Some(ContentAttributes {
        content_type: ct,
        base_title: if title.is_empty() { None } else { Some(title) },
        year,
        episodes,
        languages: infer_languages(&rest),
        language_multi: MULTI_RE.is_match(&rest),
        video_resolution: infer_video_resolution(&rest),
        video_source: infer_video_source(&rest),
        video_codec,
        video_3d: infer_video_3d(&rest),
        video_modifier: infer_video_modifier(&rest),
        release_group,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testsupport::{adapt_go_pattern, load_go_patterns};

    #[test]
    fn title_patterns_match_go() {
        let go = load_go_patterns();
        assert_eq!(*TITLE_PATTERN, adapt_go_pattern(&go["title"]));
        assert_eq!(*TITLE_YEAR_PATTERN, adapt_go_pattern(&go["title_year"]));
        assert_eq!(
            *TITLE_EPISODES_PATTERN,
            adapt_go_pattern(&go["title_episodes"])
        );
        assert_eq!(*TITLE_PART_PATTERN, adapt_go_pattern(&go["title_part"]));
        assert_eq!(*TRIM_TITLE_PATTERN, adapt_go_pattern(&go["trim_title"]));
        // The templates are NOT what ships: assert the pinning actually
        // happened, so this test can never pass on a literal `\p{L}` again.
        for pinned in [
            &*TITLE_PATTERN,
            &*TITLE_YEAR_PATTERN,
            &*TITLE_EPISODES_PATTERN,
            &*TITLE_PART_PATTERN,
            &*TRIM_TITLE_PATTERN,
        ] {
            assert!(
                !pinned.contains(r"\p{L}"),
                "pattern still carries a literal \\p{{L}}"
            );
        }
        // All compile.
        LazyLock::force(&TITLE_RE);
        LazyLock::force(&TITLE_YEAR_RE);
        LazyLock::force(&TITLE_EPISODES_RE);
        LazyLock::force(&TITLE_PART_RE);
        LazyLock::force(&TRIM_TITLE_RE);
        LazyLock::force(&MULTI_RE);
    }

    // `cleanTitle` is the highest-churn surface; lock its exact behavior against
    // the Go oracle, including its quirks (multi-dot artifacts, only-first
    // bracket stripped, preserved acronyms/apostrophes/accents).
    #[derive(serde::Deserialize)]
    struct CleanFixture {
        id: String,
        input: String,
        output: String,
    }

    #[test]
    fn clean_title_matches_go() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../testdata/parity/release/clean_title.jsonl"
        );
        let raw = std::fs::read_to_string(path).expect("read clean_title.jsonl");
        let fixtures: Vec<CleanFixture> = raw
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("json"))
            .collect();
        assert!(fixtures.len() >= 20);
        for f in &fixtures {
            assert_eq!(
                clean_title(&f.input),
                f.output,
                "clean_title mismatch: {}",
                f.id
            );
        }
    }
}
