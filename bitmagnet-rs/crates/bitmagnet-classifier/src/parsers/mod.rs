//! Classifier-owned parsers: the date lexer (fully ported) and the
//! video-content orchestration (Lane-R-backed, with the title/year extraction
//! still pending on R — see `video`).

mod date;
mod video;

pub(crate) use date::parse_date;
pub(crate) use video::parse_video_content;
