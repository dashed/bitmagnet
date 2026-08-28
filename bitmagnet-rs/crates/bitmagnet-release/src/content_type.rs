//! Port of `internal/model/content_type_enum.go` — the content-type vocabulary.
//! Only the string values are needed here (as the classifier hint fed to
//! `parse_title_year_episodes` / `parse_video_content` and echoed on output).

/// Content type. Canonical `as_str` matches Go's enum `String()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn as_str(self) -> &'static str {
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

    /// Case-insensitive parse against the canonical names (go-enum semantics).
    pub fn parse_ci(s: &str) -> Option<Self> {
        let lower = s.to_lowercase();
        [
            Self::Movie,
            Self::TvShow,
            Self::Music,
            Self::Ebook,
            Self::Comic,
            Self::Audiobook,
            Self::Game,
            Self::Software,
            Self::Xxx,
        ]
        .into_iter()
        .find(|v| v.as_str() == lower)
    }
}
