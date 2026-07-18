//! Video-attribute parsing — the port of `internal/model/video_resolution.go`,
//! `video_codec.go`, and `video_source.go` plus their generated enums.
//!
//! Each parser compiles a keyword regex from `[enum names] ++ [aliases]` and
//! runs it over a release name. Two frozen contracts (phase3-contracts §3.3):
//!
//! * **Longest-first alias ordering (alpha tiebreak) for EVERY table.** Go only
//!   sorts `video_source`'s aliases (commit 998ebfc6); `video_resolution` and
//!   `video_codec` append aliases in Go's *randomized* map order. This port
//!   sorts every alias table longest-first so e.g. `web-dl` can never be
//!   shadowed by `web`. Enum names keep their declaration order and come first
//!   (matching Go, and safe because no enum name is a prefix-word of an alias).
//! * **ASCII regex mode** for the digit class — handled in `regexutil`.

use std::sync::LazyLock;

use regex::Regex;

use crate::keywords::{capture_alternation, regex_pattern_from_keywords, rex_tokens_from_keywords};
use crate::regexutil::any_non_word_char;

/// Video resolution. Canonical `as_str` values match Go's enum `String()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoResolution {
    V360p,
    V480p,
    V540p,
    V576p,
    V720p,
    V1080p,
    V1440p,
    V2160p,
    V4320p,
}

impl VideoResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V360p => "V360p",
            Self::V480p => "V480p",
            Self::V540p => "V540p",
            Self::V576p => "V576p",
            Self::V720p => "V720p",
            Self::V1080p => "V1080p",
            Self::V1440p => "V1440p",
            Self::V2160p => "V2160p",
            Self::V4320p => "V4320p",
        }
    }

    /// Case-insensitive parse against the canonical names — mirrors go-enum's
    /// `ParseVideoResolution` (exact then lowercased lookup).
    fn parse_ci(s: &str) -> Option<Self> {
        let lower = s.to_lowercase();
        VARIANTS_RESOLUTION
            .iter()
            .copied()
            .find(|v| v.as_str().to_lowercase() == lower)
    }
}

const VARIANTS_RESOLUTION: [VideoResolution; 9] = [
    VideoResolution::V360p,
    VideoResolution::V480p,
    VideoResolution::V540p,
    VideoResolution::V576p,
    VideoResolution::V720p,
    VideoResolution::V1080p,
    VideoResolution::V1440p,
    VideoResolution::V2160p,
    VideoResolution::V4320p,
];

/// `videoResolutionAliases`. The enum-name keywords used to build the regex are
/// the `as_str` values with the leading `V` stripped and lowercased.
const RESOLUTION_ALIASES: [(&str, VideoResolution); 10] = [
    ("1080i", VideoResolution::V1080p),
    ("1920x1080", VideoResolution::V1080p),
    ("3840x2160", VideoResolution::V2160p),
    ("2k", VideoResolution::V1080p),
    ("4k", VideoResolution::V2160p),
    ("8k", VideoResolution::V4320p),
    ("sd", VideoResolution::V480p),
    ("hd", VideoResolution::V720p),
    ("fhd", VideoResolution::V1080p),
    ("uhd", VideoResolution::V2160p),
];

/// Video codec. Canonical `as_str` matches Go's enum `String()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCodec {
    H264,
    X264,
    X265,
    XviD,
    DivX,
    Mpeg2,
    Mpeg4,
}

impl VideoCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::H264 => "H264",
            Self::X264 => "x264",
            Self::X265 => "x265",
            Self::XviD => "XviD",
            Self::DivX => "DivX",
            Self::Mpeg2 => "MPEG2",
            Self::Mpeg4 => "MPEG4",
        }
    }

    fn parse_ci(s: &str) -> Option<Self> {
        let lower = s.to_lowercase();
        VARIANTS_CODEC
            .iter()
            .copied()
            .find(|v| v.as_str().to_lowercase() == lower)
    }
}

const VARIANTS_CODEC: [VideoCodec; 7] = [
    VideoCodec::H264,
    VideoCodec::X264,
    VideoCodec::X265,
    VideoCodec::XviD,
    VideoCodec::DivX,
    VideoCodec::Mpeg2,
    VideoCodec::Mpeg4,
];

const CODEC_ALIASES: [(&str, VideoCodec); 1] = [("avc", VideoCodec::H264)];

/// Video source. Canonical `as_str` matches Go's enum `String()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoSource {
    Cam,
    Telesync,
    Telecine,
    Workprint,
    Dvd,
    Tv,
    Webdl,
    Webrip,
    Bluray,
}

impl VideoSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cam => "CAM",
            Self::Telesync => "TELESYNC",
            Self::Telecine => "TELECINE",
            Self::Workprint => "WORKPRINT",
            Self::Dvd => "DVD",
            Self::Tv => "TV",
            Self::Webdl => "WEBDL",
            Self::Webrip => "WEBRip",
            Self::Bluray => "BluRay",
        }
    }

    fn parse_ci(s: &str) -> Option<Self> {
        let lower = s.to_lowercase();
        VARIANTS_SOURCE
            .iter()
            .copied()
            .find(|v| v.as_str().to_lowercase() == lower)
    }
}

const VARIANTS_SOURCE: [VideoSource; 9] = [
    VideoSource::Cam,
    VideoSource::Telesync,
    VideoSource::Telecine,
    VideoSource::Workprint,
    VideoSource::Dvd,
    VideoSource::Tv,
    VideoSource::Webdl,
    VideoSource::Webrip,
    VideoSource::Bluray,
];

const SOURCE_ALIASES: [(&str, VideoSource); 13] = [
    ("bdremux", VideoSource::Bluray),
    ("bdrip", VideoSource::Bluray),
    ("blu-ray", VideoSource::Bluray),
    ("brrip", VideoSource::Bluray),
    ("dvd5", VideoSource::Dvd),
    ("dvd9", VideoSource::Dvd),
    ("dvdrip", VideoSource::Dvd),
    ("hdtv", VideoSource::Tv),
    ("iptvrip", VideoSource::Tv),
    ("satrip", VideoSource::Tv),
    ("web", VideoSource::Webrip),
    ("web-dl", VideoSource::Webdl),
    ("web-rip", VideoSource::Webrip),
];

/// Sort alias keys longest-first, alphabetical tiebreak — the frozen ordering
/// (`video_source.go:41-46`), applied to *every* table for determinism.
fn sorted_alias_keys<V: Copy>(aliases: &[(&str, V)]) -> Vec<String> {
    let mut keys: Vec<String> = aliases.iter().map(|(k, _)| (*k).to_string()).collect();
    keys.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    keys
}

fn alias_lookup<V: Copy>(aliases: &[(&str, V)], key: &str) -> Option<V> {
    aliases.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
}

// --- Resolution ------------------------------------------------------------

fn resolution_keywords() -> Vec<String> {
    // enum names: strip leading 'V', lowercase; declaration order first.
    let mut kws: Vec<String> = VARIANTS_RESOLUTION
        .iter()
        .map(|v| v.as_str()[1..].to_lowercase())
        .collect();
    kws.extend(sorted_alias_keys(&RESOLUTION_ALIASES));
    kws
}

pub(crate) fn resolution_pattern() -> String {
    let kws = resolution_keywords();
    let refs: Vec<&str> = kws.iter().map(String::as_str).collect();
    regex_pattern_from_keywords(&refs).expect("resolution keywords compile")
}

static RESOLUTION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&resolution_pattern()).expect("resolution regex compiles"));

/// Port of `InferVideoResolution`.
pub fn infer_video_resolution(input: &str) -> Option<VideoResolution> {
    let caps = RESOLUTION_RE.captures(input)?;
    let m = caps.get(1)?.as_str();
    if let Some(parsed) = VideoResolution::parse_ci(&format!("V{m}")) {
        return Some(parsed);
    }
    alias_lookup(&RESOLUTION_ALIASES, &m.to_lowercase())
}

// --- Source ----------------------------------------------------------------

fn source_keywords() -> Vec<String> {
    let mut kws: Vec<String> = VARIANTS_SOURCE
        .iter()
        .map(|v| v.as_str().to_lowercase())
        .collect();
    kws.extend(sorted_alias_keys(&SOURCE_ALIASES));
    kws
}

pub(crate) fn source_pattern() -> String {
    let kws = source_keywords();
    let refs: Vec<&str> = kws.iter().map(String::as_str).collect();
    regex_pattern_from_keywords(&refs).expect("source keywords compile")
}

static SOURCE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&source_pattern()).expect("source regex compiles"));

/// Port of `InferVideoSource`.
pub fn infer_video_source(input: &str) -> Option<VideoSource> {
    let caps = SOURCE_RE.captures(input)?;
    let m = caps.get(1)?.as_str();
    if let Some(parsed) = VideoSource::parse_ci(m) {
        return Some(parsed);
    }
    alias_lookup(&SOURCE_ALIASES, &m.to_lowercase())
}

// --- Codec (+ optional release group) --------------------------------------

fn codec_keywords() -> Vec<String> {
    let mut kws: Vec<String> = VARIANTS_CODEC
        .iter()
        .map(|v| v.as_str().to_lowercase())
        .collect();
    kws.extend(sorted_alias_keys(&CODEC_ALIASES));
    kws
}

/// Port of `createVideoCodecAndOptionalReleaseGroupRegex`. The keyword
/// alternation is group 1 (codec); the trailing `-<word>` (if present) is group
/// 2 (release group). Suffix mirrors the raw `rex` construction:
/// `(?:$|(?:\x2D([\p{L}0-9]+))|[^\p{L}0-9]+)`.
pub(crate) fn codec_pattern() -> String {
    let kws = codec_keywords();
    let refs: Vec<&str> = kws.iter().map(String::as_str).collect();
    let tokens = rex_tokens_from_keywords(&refs).expect("codec keywords compile");
    let nw = any_non_word_char();
    format!(
        "(?:^|{nw}+){cap}(?:$|(?:\\x2D([\\p{{L}}0-9]+))|{nw}+)",
        cap = capture_alternation(&tokens),
    )
}

static CODEC_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&codec_pattern()).expect("codec regex compiles"));

/// Port of `InferVideoCodecAndReleaseGroup`. Returns `(codec, release_group)`;
/// the release group is only surfaced when a codec actually resolves (matching
/// Go, which discards `match[2]` on the no-codec fall-through).
pub fn infer_video_codec_and_release_group(input: &str) -> (Option<VideoCodec>, Option<String>) {
    if let Some(caps) = CODEC_RE.captures(input) {
        let m = caps.get(1).map(|g| g.as_str()).unwrap_or_default();
        let release_group = caps.get(2).map(|g| g.as_str().to_string());
        if let Some(parsed) = VideoCodec::parse_ci(m) {
            return (Some(parsed), release_group);
        }
        if let Some(aliased) = alias_lookup(&CODEC_ALIASES, &m.to_lowercase()) {
            return (Some(aliased), release_group);
        }
    }
    (None, None)
}

// --- Modifier (alias-less enum) --------------------------------------------

/// Video modifier. Canonical `as_str` matches Go's enum `String()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoModifier {
    Regional,
    Screener,
    RawHd,
    BrDisk,
    Remux,
}

impl VideoModifier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Regional => "REGIONAL",
            Self::Screener => "SCREENER",
            Self::RawHd => "RAWHD",
            Self::BrDisk => "BRDISK",
            Self::Remux => "REMUX",
        }
    }

    fn parse_ci(s: &str) -> Option<Self> {
        let lower = s.to_lowercase();
        VARIANTS_MODIFIER
            .iter()
            .copied()
            .find(|v| v.as_str().to_lowercase() == lower)
    }
}

const VARIANTS_MODIFIER: [VideoModifier; 5] = [
    VideoModifier::Regional,
    VideoModifier::Screener,
    VideoModifier::RawHd,
    VideoModifier::BrDisk,
    VideoModifier::Remux,
];

pub(crate) fn modifier_pattern() -> String {
    let kws: Vec<String> = VARIANTS_MODIFIER
        .iter()
        .map(|v| v.as_str().to_lowercase())
        .collect();
    let refs: Vec<&str> = kws.iter().map(String::as_str).collect();
    regex_pattern_from_keywords(&refs).expect("modifier keywords compile")
}

static MODIFIER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&modifier_pattern()).expect("modifier regex compiles"));

/// Port of `InferVideoModifier` (no alias table).
pub fn infer_video_modifier(input: &str) -> Option<VideoModifier> {
    let caps = MODIFIER_RE.captures(input)?;
    VideoModifier::parse_ci(caps.get(1)?.as_str())
}

// --- 3D (alias-less enum, `V` prefix like resolution) ----------------------

/// Video 3D type. Canonical `as_str` matches Go's enum `String()`.
///
/// 🚨 Parity note (contracts §0 correction #2): the proto `Classification`
/// transformer DROPS `video3d` (and `year`), so the CEL `result` never sees it.
/// This value lives on the ContentAttributes surface only — exactly as Go. The
/// omission belongs in Lane C's proto transformer; the parser here still
/// produces it (Go's `InferVideo3D` does too).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Video3D {
    V3D,
    V3DSbs,
    V3DOu,
}

impl Video3D {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V3D => "V3D",
            Self::V3DSbs => "V3DSBS",
            Self::V3DOu => "V3DOU",
        }
    }

    fn parse_ci(s: &str) -> Option<Self> {
        let lower = s.to_lowercase();
        VARIANTS_3D
            .iter()
            .copied()
            .find(|v| v.as_str().to_lowercase() == lower)
    }
}

const VARIANTS_3D: [Video3D; 3] = [Video3D::V3D, Video3D::V3DSbs, Video3D::V3DOu];

pub(crate) fn video3d_pattern() -> String {
    // enum names, `V`-prefix stripped + lowercased (like resolution).
    let kws: Vec<String> = VARIANTS_3D
        .iter()
        .map(|v| v.as_str()[1..].to_lowercase())
        .collect();
    let refs: Vec<&str> = kws.iter().map(String::as_str).collect();
    regex_pattern_from_keywords(&refs).expect("video3d keywords compile")
}

static VIDEO3D_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&video3d_pattern()).expect("video3d regex compiles"));

/// Port of `InferVideo3D` (prepends `V` before parsing, like resolution).
pub fn infer_video_3d(input: &str) -> Option<Video3D> {
    let caps = VIDEO3D_RE.captures(input)?;
    Video3D::parse_ci(&format!("V{}", caps.get(1)?.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testsupport::{adapt_go_pattern, load_go_patterns};

    // The video regexes must be byte-identical to Go's `rex` output after the
    // one documented ASCII adaptation (`\d` -> `[0-9]`).
    #[test]
    fn video_patterns_match_go() {
        let go = load_go_patterns();
        assert_eq!(
            resolution_pattern(),
            adapt_go_pattern(&go["video_resolution"])
        );
        assert_eq!(source_pattern(), adapt_go_pattern(&go["video_source"]));
        assert_eq!(codec_pattern(), adapt_go_pattern(&go["video_codec"]));
        assert_eq!(modifier_pattern(), adapt_go_pattern(&go["video_modifier"]));
        assert_eq!(video3d_pattern(), adapt_go_pattern(&go["video_3d"]));
    }

    // Sorted-alias determinism proof (frozen longest-first, alpha tiebreak).
    #[test]
    fn source_aliases_sorted_longest_first() {
        assert_eq!(
            sorted_alias_keys(&SOURCE_ALIASES),
            vec![
                "bdremux", "blu-ray", "iptvrip", "web-rip", // len 7
                "dvdrip", "satrip", "web-dl", // len 6
                "bdrip", "brrip", // len 5
                "dvd5", "dvd9", "hdtv", // len 4
                "web",  // len 3
            ]
        );
    }

    #[test]
    fn resolution_aliases_sorted_longest_first() {
        assert_eq!(
            sorted_alias_keys(&RESOLUTION_ALIASES),
            vec![
                "1920x1080",
                "3840x2160", // len 9
                "1080i",     // len 5
                "fhd",
                "uhd", // len 3
                "2k",
                "4k",
                "8k",
                "hd",
                "sd", // len 2
            ]
        );
    }
}
