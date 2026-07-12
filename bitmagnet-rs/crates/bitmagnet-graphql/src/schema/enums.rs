#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ContentType {
    #[graphql(name = "audiobook")]
    Audiobook,
    #[graphql(name = "comic")]
    Comic,
    #[graphql(name = "ebook")]
    Ebook,
    #[graphql(name = "game")]
    Game,
    #[graphql(name = "movie")]
    Movie,
    #[graphql(name = "music")]
    Music,
    #[graphql(name = "software")]
    Software,
    #[graphql(name = "tv_show")]
    TvShow,
    #[graphql(name = "xxx")]
    Xxx,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum FacetLogic {
    #[graphql(name = "and")]
    And,
    #[graphql(name = "or")]
    Or,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum FileFacetField {
    #[graphql(name = "extension")]
    Extension,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum FileType {
    #[graphql(name = "archive")]
    Archive,
    #[graphql(name = "audio")]
    Audio,
    #[graphql(name = "data")]
    Data,
    #[graphql(name = "document")]
    Document,
    #[graphql(name = "image")]
    Image,
    #[graphql(name = "software")]
    Software,
    #[graphql(name = "subtitles")]
    Subtitles,
    #[graphql(name = "video")]
    Video,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum FilesStatus {
    #[graphql(name = "multi")]
    Multi,
    #[graphql(name = "no_info")]
    NoInfo,
    #[graphql(name = "over_threshold")]
    OverThreshold,
    #[graphql(name = "single")]
    Single,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum HealthStatus {
    #[graphql(name = "down")]
    Down,
    #[graphql(name = "inactive")]
    Inactive,
    #[graphql(name = "unknown")]
    Unknown,
    #[graphql(name = "up")]
    Up,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum Language {
    #[graphql(name = "af")]
    Af,
    #[graphql(name = "ar")]
    Ar,
    #[graphql(name = "az")]
    Az,
    #[graphql(name = "be")]
    Be,
    #[graphql(name = "bg")]
    Bg,
    #[graphql(name = "bs")]
    Bs,
    #[graphql(name = "ca")]
    Ca,
    #[graphql(name = "ce")]
    Ce,
    #[graphql(name = "co")]
    Co,
    #[graphql(name = "cs")]
    Cs,
    #[graphql(name = "cy")]
    Cy,
    #[graphql(name = "da")]
    Da,
    #[graphql(name = "de")]
    De,
    #[graphql(name = "el")]
    El,
    #[graphql(name = "en")]
    En,
    #[graphql(name = "es")]
    Es,
    #[graphql(name = "et")]
    Et,
    #[graphql(name = "eu")]
    Eu,
    #[graphql(name = "fa")]
    Fa,
    #[graphql(name = "fi")]
    Fi,
    #[graphql(name = "fr")]
    Fr,
    #[graphql(name = "he")]
    He,
    #[graphql(name = "hi")]
    Hi,
    #[graphql(name = "hr")]
    Hr,
    #[graphql(name = "hu")]
    Hu,
    #[graphql(name = "hy")]
    Hy,
    #[graphql(name = "id")]
    Id,
    #[graphql(name = "is")]
    Is,
    #[graphql(name = "it")]
    It,
    #[graphql(name = "ja")]
    Ja,
    #[graphql(name = "ka")]
    Ka,
    #[graphql(name = "ko")]
    Ko,
    #[graphql(name = "ku")]
    Ku,
    #[graphql(name = "lt")]
    Lt,
    #[graphql(name = "lv")]
    Lv,
    #[graphql(name = "mi")]
    Mi,
    #[graphql(name = "mk")]
    Mk,
    #[graphql(name = "ml")]
    Ml,
    #[graphql(name = "mn")]
    Mn,
    #[graphql(name = "ms")]
    Ms,
    #[graphql(name = "mt")]
    Mt,
    #[graphql(name = "nl")]
    Nl,
    #[graphql(name = "no")]
    No,
    #[graphql(name = "pl")]
    Pl,
    #[graphql(name = "pt")]
    Pt,
    #[graphql(name = "ro")]
    Ro,
    #[graphql(name = "ru")]
    Ru,
    #[graphql(name = "sa")]
    Sa,
    #[graphql(name = "sk")]
    Sk,
    #[graphql(name = "sl")]
    Sl,
    #[graphql(name = "sm")]
    Sm,
    #[graphql(name = "so")]
    So,
    #[graphql(name = "sr")]
    Sr,
    #[graphql(name = "sv")]
    Sv,
    #[graphql(name = "ta")]
    Ta,
    #[graphql(name = "th")]
    Th,
    #[graphql(name = "tr")]
    Tr,
    #[graphql(name = "uk")]
    Uk,
    #[graphql(name = "vi")]
    Vi,
    #[graphql(name = "yi")]
    Yi,
    #[graphql(name = "zh")]
    Zh,
    #[graphql(name = "zu")]
    Zu,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum MetricsBucketDuration {
    #[graphql(name = "day")]
    Day,
    #[graphql(name = "hour")]
    Hour,
    #[graphql(name = "minute")]
    Minute,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum QueueJobStatus {
    #[graphql(name = "failed")]
    Failed,
    #[graphql(name = "pending")]
    Pending,
    #[graphql(name = "processed")]
    Processed,
    #[graphql(name = "retry")]
    Retry,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum QueueJobsOrderByField {
    #[graphql(name = "created_at")]
    CreatedAt,
    #[graphql(name = "priority")]
    Priority,
    #[graphql(name = "ran_at")]
    RanAt,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum TorrentContentOrderByField {
    #[graphql(name = "files_count")]
    FilesCount,
    #[graphql(name = "info_hash")]
    InfoHash,
    #[graphql(name = "leechers")]
    Leechers,
    #[graphql(name = "name")]
    Name,
    #[graphql(name = "published_at")]
    PublishedAt,
    #[graphql(name = "relevance")]
    Relevance,
    #[graphql(name = "seeders")]
    Seeders,
    #[graphql(name = "size")]
    Size,
    #[graphql(name = "updated_at")]
    UpdatedAt,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum TorrentFilesOrderByField {
    #[graphql(name = "extension")]
    Extension,
    #[graphql(name = "index")]
    Index,
    #[graphql(name = "path")]
    Path,
    #[graphql(name = "size")]
    Size,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum Video3D {
    #[graphql(name = "V3D")]
    Standard,
    #[graphql(name = "V3DOU")]
    OverUnder,
    #[graphql(name = "V3DSBS")]
    SideBySide,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum VideoCodec {
    #[graphql(name = "DivX")]
    Divx,
    #[graphql(name = "H264")]
    H264,
    #[graphql(name = "MPEG2")]
    Mpeg2,
    #[graphql(name = "MPEG4")]
    Mpeg4,
    #[graphql(name = "XviD")]
    Xvid,
    #[graphql(name = "x264")]
    X264,
    #[graphql(name = "x265")]
    X265,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum VideoModifier {
    #[graphql(name = "BRDISK")]
    Brdisk,
    #[graphql(name = "RAWHD")]
    Rawhd,
    #[graphql(name = "REGIONAL")]
    Regional,
    #[graphql(name = "REMUX")]
    Remux,
    #[graphql(name = "SCREENER")]
    Screener,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum VideoResolution {
    #[graphql(name = "V1080p")]
    P1080,
    #[graphql(name = "V1440p")]
    P1440,
    #[graphql(name = "V2160p")]
    P2160,
    #[graphql(name = "V360p")]
    P360,
    #[graphql(name = "V4320p")]
    P4320,
    #[graphql(name = "V480p")]
    P480,
    #[graphql(name = "V540p")]
    P540,
    #[graphql(name = "V576p")]
    P576,
    #[graphql(name = "V720p")]
    P720,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub(crate) enum VideoSource {
    #[graphql(name = "BluRay")]
    Bluray,
    #[graphql(name = "CAM")]
    Cam,
    #[graphql(name = "DVD")]
    Dvd,
    #[graphql(name = "TELECINE")]
    Telecine,
    #[graphql(name = "TELESYNC")]
    Telesync,
    #[graphql(name = "TV")]
    Tv,
    #[graphql(name = "WEBDL")]
    Webdl,
    #[graphql(name = "WEBRip")]
    Webrip,
    #[graphql(name = "WORKPRINT")]
    Workprint,
}
