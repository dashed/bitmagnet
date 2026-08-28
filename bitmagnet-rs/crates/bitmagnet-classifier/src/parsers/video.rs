//! `parse_video_content` — a thin adapter over Lane R's `parse_video_content`,
//! which is the full port of Go `parsers/video.go ParseVideoContent` (title/year
//! extraction, episode + language inference, and the video-attribute parsers).
//!
//! 🔑 Episodes come out correct here because R routes through the title path
//! (`titleEpisodesRegex`), which cancels the standalone-episode-parser index bug
//! (contract note from Lane R). year/video3d live on R's `ContentAttributes`;
//! the CEL `result` transformer is the layer that drops them (contract §0.2), so
//! Lane C simply omits them from `CelClassification`, not here.

use bitmagnet_release::{
    parse_video_content as r_parse_video_content, ContentType as RContentType,
};

use crate::errors::FlowError;
use crate::model::{ClassifierInput, ContentType, Date};
use crate::result::Classification;

fn to_release_content_type(ct: ContentType) -> RContentType {
    match ct {
        ContentType::Movie => RContentType::Movie,
        ContentType::TvShow => RContentType::TvShow,
        ContentType::Music => RContentType::Music,
        ContentType::Ebook => RContentType::Ebook,
        ContentType::Comic => RContentType::Comic,
        ContentType::Audiobook => RContentType::Audiobook,
        ContentType::Game => RContentType::Game,
        ContentType::Software => RContentType::Software,
        ContentType::Xxx => RContentType::Xxx,
    }
}

fn from_release_content_type(ct: RContentType) -> ContentType {
    match ct {
        RContentType::Movie => ContentType::Movie,
        RContentType::TvShow => ContentType::TvShow,
        RContentType::Music => ContentType::Music,
        RContentType::Ebook => ContentType::Ebook,
        RContentType::Comic => ContentType::Comic,
        RContentType::Audiobook => ContentType::Audiobook,
        RContentType::Game => ContentType::Game,
        RContentType::Software => ContentType::Software,
        RContentType::Xxx => ContentType::Xxx,
    }
}

/// Port of the `parse_video_content` action body. Returns `(Some(attrs), None)`
/// with the attributes to `merge`, or `(None, Some(ErrUnmatched))` when R's
/// parser returns `None` (the `ParseTitleYearEpisodes`-failed + no-content-type
/// early return in Go `ParseVideoContent`).
pub(crate) fn parse_video_content(
    input: &ClassifierInput,
    result: &Classification,
) -> (Option<Classification>, Option<FlowError>) {
    let content_type = result.content_type.map(to_release_content_type);
    match r_parse_video_content(&input.name, content_type, result.date.is_valid()) {
        Some(attrs) => {
            let cls = Classification {
                content_type: attrs.content_type.map(from_release_content_type),
                base_title: attrs.base_title,
                date: Date {
                    year: attrs.year,
                    ..Date::default()
                },
                languages: attrs.languages,
                language_multi: attrs.language_multi,
                episodes: attrs.episodes,
                video_resolution: attrs.video_resolution,
                video_source: attrs.video_source,
                video_codec: attrs.video_codec,
                video_3d: attrs.video_3d,
                video_modifier: attrs.video_modifier,
                release_group: attrs.release_group,
                // `parse_video_content` produces a `ContentAttributes`, which in
                // Go does not carry the attached `*model.Content` at all; it is
                // merged into the threaded result, and `Merge` never touches
                // `Content`.
                content: None,
                // Tags live on the outer classification result, not on the
                // parsed ContentAttributes merged into it.
                tags: Default::default(),
            };
            (Some(cls), None)
        }
        None => (None, Some(FlowError::Unmatched)),
    }
}
