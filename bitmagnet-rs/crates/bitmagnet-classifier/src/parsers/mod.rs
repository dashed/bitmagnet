//! Classifier-owned parsers: the date lexer (fully ported here) and the
//! `parse_video_content` adapter over Lane R's parser (see `video`).

mod date;
mod video;

pub(crate) use date::parse_date;
pub(crate) use video::parse_video_content;
