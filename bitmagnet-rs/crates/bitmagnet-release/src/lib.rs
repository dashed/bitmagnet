//! Phase-3 Lane R — release-name parsing.
//!
//! Ports the Go release parsers (`internal/keywords`, `internal/regex`,
//! `internal/lexer`, `internal/model/video_*`) so release names parse
//! byte-identically to Go. Contract: `docs/dev/rust-rewrite/phase3-contracts.md`
//! §3 (release-parse output shape).
//!
//! Milestone 1 (this commit): the video-attribute alias tables + parsers
//! (`video_resolution`, `video_codec`, `video_source`) plus the keyword-glob
//! DSL → regex compiler they build on. Still pending for the full lane:
//! language parsing (`languages.csv`), episode parsing (`EpisodesToken` /
//! `ParseEpisodes`), the `video3d` / `video_modifier` tables, and the
//! title/year extraction in `parsers/video.go`.

mod keywords;
mod lexer;
mod regexutil;
mod video;

#[cfg(test)]
mod testsupport;

pub use keywords::{regex_pattern_from_keywords, rex_tokens_from_keywords, KeywordError};
pub use video::{
    infer_video_codec_and_release_group, infer_video_resolution, infer_video_source, VideoCodec,
    VideoResolution, VideoSource,
};
