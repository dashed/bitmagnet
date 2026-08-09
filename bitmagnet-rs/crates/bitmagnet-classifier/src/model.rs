//! Input-side model types mirroring the Go `internal/model` surface the
//! classifier consumes, plus the extension/file-type derivation the CEL
//! `torrent` transformer needs.
//!
//! These are deliberately a *subset* of Go's `model.Torrent` — only the fields
//! the classifier reads (`corpus_test.go toTorrent` builds exactly this shape).

use std::collections::BTreeMap;

use regex::Regex;
use serde::Deserialize;
use std::sync::OnceLock;

/// Content type — mirrors `model.ContentType` (`content_type_enum.go`). The
/// integer discriminant is sourced from the shared proto (frozen contract
/// §0.5) via [`ContentType::proto_i32`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ContentType {
    Movie,
    TvShow,
    Music,
    Ebook,
    Comic,
    Audiobook,
    Game,
    Software,
    Xxx,
}

impl ContentType {
    /// Matches `model.ContentType.String()`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ContentType::Movie => "movie",
            ContentType::TvShow => "tv_show",
            ContentType::Music => "music",
            ContentType::Ebook => "ebook",
            ContentType::Comic => "comic",
            ContentType::Audiobook => "audiobook",
            ContentType::Game => "game",
            ContentType::Software => "software",
            ContentType::Xxx => "xxx",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "movie" => ContentType::Movie,
            "tv_show" => ContentType::TvShow,
            "music" => ContentType::Music,
            "ebook" => ContentType::Ebook,
            "comic" => ContentType::Comic,
            "audiobook" => ContentType::Audiobook,
            "game" => ContentType::Game,
            "software" => ContentType::Software,
            "xxx" => ContentType::Xxx,
            _ => return None,
        })
    }

    /// The proto `bitmagnet.Classification.ContentType` discriminant. Unknown
    /// (an invalid `NullContentType`) is `0` and has no enum member here.
    #[must_use]
    pub fn proto_i32(self) -> i32 {
        use bitmagnet_proto::ContentType as P;
        let p = match self {
            ContentType::Movie => P::Movie,
            ContentType::TvShow => P::TvShow,
            ContentType::Music => P::Music,
            ContentType::Ebook => P::Ebook,
            ContentType::Comic => P::Comic,
            ContentType::Audiobook => P::Audiobook,
            ContentType::Game => P::Game,
            ContentType::Software => P::Software,
            ContentType::Xxx => P::Xxx,
        };
        p as i32
    }

    /// All content types in declaration order (for the CEL `contentType`
    /// namespace map).
    #[must_use]
    pub fn all() -> &'static [ContentType] {
        &[
            ContentType::Movie,
            ContentType::TvShow,
            ContentType::Music,
            ContentType::Ebook,
            ContentType::Comic,
            ContentType::Audiobook,
            ContentType::Game,
            ContentType::Software,
            ContentType::Xxx,
        ]
    }
}

/// File type — mirrors `model.FileType` (`file_type.go`). Only used by the CEL
/// `fileType` namespace / `file.fileType` field, which `classifier.core.yml`
/// does not exercise; ported for env fidelity + user `classifier.yml`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FileType {
    Archive,
    Audio,
    Data,
    Document,
    Image,
    Software,
    Subtitles,
    Video,
}

impl FileType {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            FileType::Archive => "archive",
            FileType::Audio => "audio",
            FileType::Data => "data",
            FileType::Document => "document",
            FileType::Image => "image",
            FileType::Software => "software",
            FileType::Subtitles => "subtitles",
            FileType::Video => "video",
        }
    }

    #[must_use]
    pub(crate) fn proto_i32(self) -> i32 {
        use bitmagnet_proto::FileType as P;
        let p = match self {
            FileType::Archive => P::Archive,
            FileType::Audio => P::Audio,
            FileType::Data => P::Data,
            FileType::Document => P::Document,
            FileType::Image => P::Image,
            FileType::Software => P::Software,
            FileType::Subtitles => P::Subtitles,
            FileType::Video => P::Video,
        };
        p as i32
    }

    #[must_use]
    pub(crate) fn all() -> &'static [FileType] {
        &[
            FileType::Archive,
            FileType::Audio,
            FileType::Data,
            FileType::Document,
            FileType::Image,
            FileType::Software,
            FileType::Subtitles,
            FileType::Video,
        ]
    }
}

/// `model.Date` — `Year` is a `u16`, `Month` is 1..=12, `Day` is 1..=31.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Date {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl Date {
    /// `Date.IsNil()` — the zero value.
    #[must_use]
    pub fn is_nil(self) -> bool {
        self == Date::default()
    }

    /// `Date.IsValid()` (`date.go:59`).
    #[must_use]
    pub fn is_valid(self) -> bool {
        self.year >= 1000
            && self.year <= 9999
            && self.month >= 1
            && self.month <= 12
            && self.day >= 1
            && self.day <= num_days_in_month(self.year, self.month)
    }
}

/// Port of `numDaysInMonth` (`date.go:125`). NOTE: reproduces Go's simplified
/// leap rule `year%4 == 0` (no century correction) verbatim.
#[must_use]
pub(crate) fn num_days_in_month(year: u16, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year.is_multiple_of(4) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// `model.FilesStatus`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FilesStatus {
    NoInfo,
    Single,
    Multi,
    OverThreshold,
}

impl FilesStatus {
    #[must_use]
    pub(crate) fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "no_info" => FilesStatus::NoInfo,
            "single" => FilesStatus::Single,
            "multi" => FilesStatus::Multi,
            "over_threshold" => FilesStatus::OverThreshold,
            _ => return None,
        })
    }
}

/// One input file — mirrors the `classifierInputFile` corpus shape.
#[derive(Clone, Debug, Deserialize)]
pub struct InputFile {
    #[serde(default)]
    pub index: u32,
    pub path: String,
    #[serde(default)]
    pub extension: String,
    #[serde(default)]
    pub size: u64,
}

/// A content hint — mirrors `classifierInputHint`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct InputHint {
    #[serde(rename = "contentType")]
    pub content_type: String,
    #[serde(rename = "contentSource", default)]
    pub content_source: String,
    #[serde(rename = "contentId", default)]
    pub content_id: String,

    // --- T9 -----------------------------------------------------------------
    //
    // Go's `ContentAttributes.ApplyHint` (`classification/result.go:93`) copies
    // these across too, and a `torrent_hints` row can carry every one of them.
    // Rust previously read only the content type, so a hinted episode list or
    // video attribute was silently dropped — invisible under the flags-off
    // corpus, whose hints carry a content type and nothing else.
    //
    // All optional and defaulted, so a fixture written before T9 still parses.
    /// Canonical form, e.g. `"S07E10"`.
    #[serde(default)]
    pub episodes: Option<String>,
    /// Alpha-2 codes, in `model.Languages` set order.
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(rename = "videoResolution", default)]
    pub video_resolution: Option<String>,
    #[serde(rename = "videoSource", default)]
    pub video_source: Option<String>,
    #[serde(rename = "videoCodec", default)]
    pub video_codec: Option<String>,
    #[serde(rename = "video3d", default)]
    pub video_3d: Option<String>,
    #[serde(rename = "videoModifier", default)]
    pub video_modifier: Option<String>,
    #[serde(rename = "releaseGroup", default)]
    pub release_group: Option<String>,
}

/// An existing `torrent_contents` association — Go `model.TorrentContent`, as
/// much of it as the pre-attach reads.
///
/// 🚨 This is the T9 input Rust previously had no way to represent. Go's
/// `runner.Run` attaches an already-known content row **before the workflow
/// runs**, so `result.hasAttachedContent` is already true and the enrichment
/// branch never fires. Without these rows a port re-derives content the original
/// classification simply reused, which is both a different write set and a
/// different set of dependency calls.
#[derive(Clone, Debug, Deserialize)]
pub struct InputContent {
    #[serde(rename = "contentType", default)]
    pub content_type: String,
    #[serde(rename = "contentSource", default)]
    pub content_source: String,
    #[serde(rename = "contentId", default)]
    pub content_id: String,
    /// The hydrated `content` row. Go guards on
    /// `tc.Content.Source == tc.ContentSource`, which is how it detects an
    /// association whose content was never loaded; an absent value here is the
    /// same condition.
    #[serde(default)]
    pub content: Option<bitmagnet_model::Content>,
}

/// The classifier input — mirrors `classifierInput` (`corpus_test.go`).
#[derive(Clone, Debug, Deserialize)]
pub struct ClassifierInput {
    #[serde(default)]
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub size: u64,
    #[serde(rename = "filesStatus")]
    pub files_status: String,
    #[serde(default)]
    pub extension: Option<String>,
    #[serde(rename = "filesCount", default)]
    pub files_count: Option<u32>,
    #[serde(default)]
    pub files: Vec<InputFile>,
    #[serde(default)]
    pub hint: Option<InputHint>,
    /// Existing content associations, for the T9 pre-attach. See
    /// [`InputContent`]. Empty for a torrent with nothing attached yet, which is
    /// every subject of the flags-off corpus.
    #[serde(default)]
    pub contents: Vec<InputContent>,
}

fn file_extension_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Mirrors `fileExtensionRegex` (`torrent_files.go:33`). Applied to the
    // lowercased path.
    RE.get_or_init(|| Regex::new(r"[^/.]\.([a-z0-9]+)$").unwrap())
}

/// Port of `model.FileExtensionFromPath` — the single-file / per-file
/// extension derivation. Returns the lowercase extension without the dot.
#[must_use]
pub(crate) fn file_extension_from_path(path: &str) -> Option<String> {
    let lower = path.to_lowercase();
    file_extension_regex()
        .captures(&lower)
        .map(|c| c[1].to_string())
}

fn extension_to_file_type() -> &'static BTreeMap<&'static str, FileType> {
    static MAP: OnceLock<BTreeMap<&'static str, FileType>> = OnceLock::new();
    MAP.get_or_init(|| {
        use FileType::{Archive, Audio, Data, Document, Image, Software, Subtitles, Video};
        // Verbatim from `extensionToFileTypeMap` (`file_type.go:21-110`).
        [
            ("zip", Archive),
            ("rar", Archive),
            ("tar", Archive),
            ("gz", Archive),
            ("7z", Archive),
            ("iso", Archive),
            ("bz2", Archive),
            ("mp3", Audio),
            ("wav", Audio),
            ("flac", Audio),
            ("aac", Audio),
            ("ogg", Audio),
            ("m4a", Audio),
            ("m4b", Audio),
            ("mid", Audio),
            ("dsf", Audio),
            ("csv", Data),
            ("json", Data),
            ("xml", Data),
            ("xls", Data),
            ("xlsx", Data),
            ("pdf", Document),
            ("doc", Document),
            ("docx", Document),
            ("otf", Document),
            ("ppt", Document),
            ("pptx", Document),
            ("html", Document),
            ("htm", Document),
            ("epub", Document),
            ("mobi", Document),
            ("azw", Document),
            ("azw3", Document),
            ("rtf", Document),
            ("txt", Document),
            ("md", Document),
            ("nfo", Document),
            ("djvu", Document),
            ("jpg", Image),
            ("jpeg", Image),
            ("png", Image),
            ("gif", Image),
            ("bmp", Image),
            ("svg", Image),
            ("dds", Image),
            ("psd", Image),
            ("tif", Image),
            ("tiff", Image),
            ("ico", Image),
            ("exe", Software),
            ("bin", Software),
            ("sh", Software),
            ("bat", Software),
            ("msi", Software),
            ("apk", Software),
            ("dmg", Software),
            ("pkg", Software),
            ("deb", Software),
            ("rpm", Software),
            ("jar", Software),
            ("dll", Software),
            ("lua", Software),
            ("package", Software),
            ("srt", Subtitles),
            ("sub", Subtitles),
            ("vtt", Subtitles),
            ("mp4", Video),
            ("mkv", Video),
            ("avi", Video),
            ("mov", Video),
            ("wmv", Video),
            ("flv", Video),
            ("m4v", Video),
            ("mpg", Video),
            ("mpeg", Video),
            ("ts", Video),
            ("vob", Video),
        ]
        .into_iter()
        .collect()
    })
}

/// Port of `model.FileTypeFromExtension`.
#[must_use]
pub(crate) fn file_type_from_extension(ext: &str) -> Option<FileType> {
    extension_to_file_type().get(ext).copied()
}
