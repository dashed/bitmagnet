//! Domain enumerations mirroring the Go string enums in `internal/model/`
//! (`content_type_enum.go`, `file_type_enum.go`, `files_status_enum.go`) and
//! the extension/file-type helpers from `file_type.go` / `torrent_files.go`.
//!
//! Each variant (de)serialises to the exact lowercase / snake_case string the
//! Go code and the PostgreSQL columns use (e.g. `ContentType::TvShow` ⇄
//! `"tv_show"`). The integer mappings ([`ContentType::to_proto_value`],
//! [`FileType::to_proto_value`]) match the proto enums in
//! `bitmagnet-rs/proto/bitmagnet/common.proto` (and Go's
//! `internal/protobuf/bitmagnet.proto`) — those integers are what travel on the
//! gRPC wire to the Tantivy sidecar. Proto value `0` is the `*_UNKNOWN`
//! sentinel, which has no domain variant.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Error returned when a string does not name a valid enum variant.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("{value:?} is not a valid {kind}")]
pub struct ParseEnumError {
    /// The enum type that failed to parse (e.g. `"ContentType"`).
    pub kind: &'static str,
    /// The offending input string.
    pub value: String,
}

/// Classification of a torrent's content. Mirrors Go `model.ContentType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// The canonical lowercase string used in PostgreSQL and JSON.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Movie => "movie",
            Self::TvShow => "tv_show",
            Self::Music => "music",
            Self::Ebook => "ebook",
            Self::Comic => "comic",
            Self::Audiobook => "audiobook",
            Self::Game => "game",
            Self::Software => "software",
            Self::Xxx => "xxx",
        }
    }

    /// The proto/gRPC integer value (see `common.proto`).
    pub const fn to_proto_value(self) -> i32 {
        match self {
            Self::Movie => 1,
            Self::TvShow => 2,
            Self::Music => 3,
            Self::Ebook => 4,
            Self::Comic => 5,
            Self::Audiobook => 6,
            Self::Game => 7,
            Self::Software => 8,
            Self::Xxx => 9,
        }
    }

    /// Maps a proto/gRPC integer value back to a variant. Returns `None` for
    /// the `CONTENT_TYPE_UNKNOWN` sentinel (`0`) and any unknown value.
    pub const fn from_proto_value(value: i32) -> Option<Self> {
        Some(match value {
            1 => Self::Movie,
            2 => Self::TvShow,
            3 => Self::Music,
            4 => Self::Ebook,
            5 => Self::Comic,
            6 => Self::Audiobook,
            7 => Self::Game,
            8 => Self::Software,
            9 => Self::Xxx,
            _ => return None,
        })
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ContentType {
    type Err = ParseEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "movie" => Self::Movie,
            "tv_show" => Self::TvShow,
            "music" => Self::Music,
            "ebook" => Self::Ebook,
            "comic" => Self::Comic,
            "audiobook" => Self::Audiobook,
            "game" => Self::Game,
            "software" => Self::Software,
            "xxx" => Self::Xxx,
            _ => {
                return Err(ParseEnumError {
                    kind: "ContentType",
                    value: s.to_owned(),
                })
            }
        })
    }
}

/// General classification of a file by its extension. Mirrors Go
/// `model.FileType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
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
    /// The canonical lowercase string used in PostgreSQL and JSON.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Audio => "audio",
            Self::Data => "data",
            Self::Document => "document",
            Self::Image => "image",
            Self::Software => "software",
            Self::Subtitles => "subtitles",
            Self::Video => "video",
        }
    }

    /// The proto/gRPC integer value (see `common.proto`).
    pub const fn to_proto_value(self) -> i32 {
        match self {
            Self::Archive => 1,
            Self::Audio => 2,
            Self::Data => 3,
            Self::Document => 4,
            Self::Image => 5,
            Self::Software => 6,
            Self::Subtitles => 7,
            Self::Video => 8,
        }
    }

    /// Maps a proto/gRPC integer value back to a variant. Returns `None` for
    /// the `FILE_TYPE_UNKNOWN` sentinel (`0`) and any unknown value.
    pub const fn from_proto_value(value: i32) -> Option<Self> {
        Some(match value {
            1 => Self::Archive,
            2 => Self::Audio,
            3 => Self::Data,
            4 => Self::Document,
            5 => Self::Image,
            6 => Self::Software,
            7 => Self::Subtitles,
            8 => Self::Video,
            _ => return None,
        })
    }

    /// Classifies a (lowercased, dot-less) extension, mirroring Go's
    /// `extensionToFileTypeMap` / `FileTypeFromExtension`.
    pub fn from_extension(ext: &str) -> Option<Self> {
        Some(match ext {
            "zip" | "rar" | "tar" | "gz" | "7z" | "iso" | "bz2" => Self::Archive,
            "mp3" | "wav" | "flac" | "aac" | "ogg" | "m4a" | "m4b" | "mid" | "dsf" => Self::Audio,
            "csv" | "json" | "xml" | "xls" | "xlsx" => Self::Data,
            "pdf" | "doc" | "docx" | "otf" | "ppt" | "pptx" | "html" | "htm" | "epub" | "mobi"
            | "azw" | "azw3" | "rtf" | "txt" | "md" | "nfo" | "djvu" => Self::Document,
            "jpg" | "jpeg" | "png" | "gif" | "bmp" | "svg" | "dds" | "psd" | "tif" | "tiff"
            | "ico" => Self::Image,
            "exe" | "bin" | "sh" | "bat" | "msi" | "apk" | "dmg" | "pkg" | "deb" | "rpm"
            | "jar" | "dll" | "lua" | "package" => Self::Software,
            "srt" | "sub" | "vtt" => Self::Subtitles,
            "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "m4v" | "mpg" | "mpeg" | "ts"
            | "vob" => Self::Video,
            _ => return None,
        })
    }

    /// Classifies a file path by its extension (mirrors Go `fileTypeFromPath`).
    pub fn from_path(path: &str) -> Option<Self> {
        file_extension_from_path(path).and_then(|ext| Self::from_extension(&ext))
    }
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for FileType {
    type Err = ParseEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "archive" => Self::Archive,
            "audio" => Self::Audio,
            "data" => Self::Data,
            "document" => Self::Document,
            "image" => Self::Image,
            "software" => Self::Software,
            "subtitles" => Self::Subtitles,
            "video" => Self::Video,
            _ => {
                return Err(ParseEnumError {
                    kind: "FileType",
                    value: s.to_owned(),
                })
            }
        })
    }
}

/// Whether and how a torrent's file list is known. Mirrors Go
/// `model.FilesStatus` (PostgreSQL `files_status` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesStatus {
    NoInfo,
    Single,
    Multi,
    OverThreshold,
}

impl FilesStatus {
    /// The canonical lowercase string used in PostgreSQL and JSON.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoInfo => "no_info",
            Self::Single => "single",
            Self::Multi => "multi",
            Self::OverThreshold => "over_threshold",
        }
    }
}

impl fmt::Display for FilesStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for FilesStatus {
    type Err = ParseEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "no_info" => Self::NoInfo,
            "single" => Self::Single,
            "multi" => Self::Multi,
            "over_threshold" => Self::OverThreshold,
            _ => {
                return Err(ParseEnumError {
                    kind: "FilesStatus",
                    value: s.to_owned(),
                })
            }
        })
    }
}

/// Extracts the lowercase extension (without the dot) from a file path,
/// mirroring Go's `model.FileExtensionFromPath` and its regex
/// `[^/.]\.([a-z0-9]+)$` applied to the lowercased path.
///
/// Returns `None` when there is no extension: no dot, a dot at the start of a
/// path segment (e.g. `.gitignore`, `dir/.x`), a dot directly preceded by
/// another dot, or a trailing component that is not purely `[a-z0-9]`.
pub fn file_extension_from_path(path: &str) -> Option<String> {
    let lower = path.to_lowercase();
    let dot = lower.rfind('.')?;
    // Need a character before the dot that is neither '/' nor '.'.
    let before = lower[..dot].chars().next_back()?;
    if before == '/' || before == '.' {
        return None;
    }
    let ext = &lower[dot + 1..];
    if ext.is_empty() || !ext.bytes().all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9')) {
        return None;
    }
    Some(ext.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_proto_values_match_proto() {
        // Must equal common.proto CONTENT_TYPE_* (movie=1 .. xxx=9).
        let expected = [
            (ContentType::Movie, 1),
            (ContentType::TvShow, 2),
            (ContentType::Music, 3),
            (ContentType::Ebook, 4),
            (ContentType::Comic, 5),
            (ContentType::Audiobook, 6),
            (ContentType::Game, 7),
            (ContentType::Software, 8),
            (ContentType::Xxx, 9),
        ];
        for (ct, v) in expected {
            assert_eq!(ct.to_proto_value(), v);
            assert_eq!(ContentType::from_proto_value(v), Some(ct));
        }
        assert_eq!(ContentType::from_proto_value(0), None);
        assert_eq!(ContentType::from_proto_value(10), None);
    }

    #[test]
    fn file_type_proto_values_match_proto() {
        // Must equal common.proto FILE_TYPE_* (archive=1 .. video=8).
        let expected = [
            (FileType::Archive, 1),
            (FileType::Audio, 2),
            (FileType::Data, 3),
            (FileType::Document, 4),
            (FileType::Image, 5),
            (FileType::Software, 6),
            (FileType::Subtitles, 7),
            (FileType::Video, 8),
        ];
        for (ft, v) in expected {
            assert_eq!(ft.to_proto_value(), v);
            assert_eq!(FileType::from_proto_value(v), Some(ft));
        }
        assert_eq!(FileType::from_proto_value(0), None);
    }

    #[test]
    fn canonical_strings_match_go() {
        // `as_str` / `FromStr` are the canonical mapping used for the PG text
        // columns; they must reproduce the exact Go/PostgreSQL strings.
        for (ct, s) in [
            (ContentType::Movie, "movie"),
            (ContentType::TvShow, "tv_show"),
            (ContentType::Xxx, "xxx"),
            (ContentType::Audiobook, "audiobook"),
        ] {
            assert_eq!(ct.as_str(), s);
            assert_eq!(s.parse::<ContentType>().unwrap(), ct);
            assert_eq!(ct.to_string(), s);
        }
        assert_eq!(FilesStatus::OverThreshold.as_str(), "over_threshold");
        assert_eq!(FilesStatus::NoInfo.as_str(), "no_info");
        assert_eq!(
            "over_threshold".parse::<FilesStatus>().unwrap(),
            FilesStatus::OverThreshold
        );
        assert!("not_a_type".parse::<ContentType>().is_err());
    }

    #[test]
    fn enum_serde_round_trips() {
        // Representation is an internal detail (enums are not on any wire-
        // critical path); we only require a stable round-trip.
        for ct in [ContentType::Movie, ContentType::TvShow, ContentType::Xxx] {
            let bytes = rmp_serde::to_vec(&ct).unwrap();
            assert_eq!(rmp_serde::from_slice::<ContentType>(&bytes).unwrap(), ct);
        }
        let bytes = rmp_serde::to_vec(&FileType::Video).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<FileType>(&bytes).unwrap(),
            FileType::Video
        );
    }

    #[test]
    fn from_extension_matches_go_map() {
        assert_eq!(FileType::from_extension("mkv"), Some(FileType::Video));
        assert_eq!(FileType::from_extension("flac"), Some(FileType::Audio));
        assert_eq!(FileType::from_extension("srt"), Some(FileType::Subtitles));
        assert_eq!(FileType::from_extension("iso"), Some(FileType::Archive));
        assert_eq!(FileType::from_extension("epub"), Some(FileType::Document));
        assert_eq!(FileType::from_extension("png"), Some(FileType::Image));
        assert_eq!(FileType::from_extension("exe"), Some(FileType::Software));
        assert_eq!(FileType::from_extension("zzz"), None);
    }

    #[test]
    fn extension_from_path_matches_go_regex() {
        assert_eq!(
            file_extension_from_path("Movie.2024.1080p.MKV").as_deref(),
            Some("mkv")
        );
        assert_eq!(
            file_extension_from_path("a/b/c.tar.gz").as_deref(),
            Some("gz")
        );
        assert_eq!(
            file_extension_from_path("音楽/曲.flac").as_deref(),
            Some("flac")
        );
        // No extension cases (mirrors the Go regex rejecting these).
        assert_eq!(file_extension_from_path("README"), None);
        assert_eq!(file_extension_from_path(".gitignore"), None);
        assert_eq!(file_extension_from_path("dir/.hidden"), None);
        assert_eq!(file_extension_from_path("trailing."), None);
        assert_eq!(file_extension_from_path("weird..x").as_deref(), None);
        assert_eq!(file_extension_from_path("file.tar.").as_deref(), None);
    }

    #[test]
    fn file_type_from_path() {
        assert_eq!(FileType::from_path("S01/ep1.mkv"), Some(FileType::Video));
        assert_eq!(FileType::from_path("readme"), None);
    }
}
