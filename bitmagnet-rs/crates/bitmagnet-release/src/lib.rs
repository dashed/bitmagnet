//! Phase-3 Lane R — release-name parsing.
//!
//! Ports the Go release parsers (`internal/keywords`, `internal/regex`,
//! `internal/lexer`, `internal/model/*`) so release names parse
//! byte-identically to Go. Contract: `docs/dev/rust-rewrite/phase3-contracts.md`
//! §3 (release-parse output shape).
//!
//! Landed (Lane R COMPLETE):
//! * M1 — the keyword-glob DSL → regex compiler + the video-attribute parsers
//!   (`video_resolution`, `video_codec`, `video_source`).
//! * M2 — the episode parser (`parse_episodes` / `Episodes`), reproducing Go's
//!   group-index bugs verbatim (see `episodes`).
//! * M3 — language detection (`languages.csv`, `infer_languages` /
//!   `parse_language`, natsort order) + the `video3d` / `video_modifier` tables.
//! * M4 — title/year extraction (`cleanTitle`, the title regexes,
//!   `parse_title_year_episodes_dispatch`) + the top-level `parse_video_content`
//!   orchestration producing `ContentAttributes` (see `title`).
//!
//! Every parser is gated by Go-oracle fixtures under
//! `testdata/parity/release/**` (byte-identical compiled regexes + behavioral
//! replay). The one output surface deliberately NOT produced here is the proto
//! `Classification` (which drops `year`/`video3d`, contracts §0 #2) — that
//! transform belongs to Lane C's classifier.

mod content_type;
mod episodes;
pub mod goclass;
mod keywords;
mod language;
mod lexer;
mod natsort;
mod regexutil;
mod title;
mod video;

#[cfg(test)]
mod testsupport;

pub use content_type::ContentType;
pub use episodes::{parse_episodes, Episodes};
pub use keywords::{regex_pattern_from_keywords, rex_tokens_from_keywords, KeywordError};
pub use language::{infer_languages, parse_language, slice_order};
pub use title::{parse_title_year_episodes_dispatch, parse_video_content, ContentAttributes};
pub use video::{
    infer_video_3d, infer_video_codec_and_release_group, infer_video_modifier,
    infer_video_resolution, infer_video_source, Video3D, VideoCodec, VideoModifier,
    VideoResolution, VideoSource,
};
