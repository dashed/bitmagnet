//! Phase-3 Lane R — release-name parsing.
//!
//! Ports the Go release parsers (`internal/keywords`, `internal/regex`,
//! `internal/lexer`, `internal/model/*`) so release names parse
//! byte-identically to Go. Contract: `docs/dev/rust-rewrite/phase3-contracts.md`
//! §3 (release-parse output shape).
//!
//! Landed:
//! * M1 — the keyword-glob DSL → regex compiler + the video-attribute parsers
//!   (`video_resolution`, `video_codec`, `video_source`).
//! * M2 — the episode parser (`ParseEpisodes` / `Episodes`), reproducing Go's
//!   group-index bugs verbatim (see `episodes`).
//!
//! Still pending: language parsing (`languages.csv`), the `video3d` /
//! `video_modifier` tables, and title/year extraction in `parsers/video.go`.

mod episodes;
mod keywords;
mod lexer;
mod regexutil;
mod video;

#[cfg(test)]
mod testsupport;

pub use episodes::{parse_episodes, Episodes};
pub use keywords::{regex_pattern_from_keywords, rex_tokens_from_keywords, KeywordError};
pub use video::{
    infer_video_codec_and_release_group, infer_video_resolution, infer_video_source, VideoCodec,
    VideoResolution, VideoSource,
};
