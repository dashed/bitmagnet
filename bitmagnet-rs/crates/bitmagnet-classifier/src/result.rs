//! The classification result + the flags-off output normalization that mirrors
//! `corpus_test.go normalizeClassifierResult` (the frozen corpus `expected`
//! schema, contract §2.1/§2.3).

use bitmagnet_model::Content;
use bitmagnet_release::{
    Episodes, Video3D, VideoCodec, VideoModifier, VideoResolution, VideoSource,
};
use serde_json::{json, Value};

use crate::model::{ContentType, Date};

/// `classification.ContentAttributes` + `Content`/`Tags` (`result.go`). The
/// video/title/language attributes are produced by Lane R's `parse_video_content`
/// and merged in by the `parse_video_content` action.
#[derive(Clone, Debug, Default)]
pub struct Classification {
    pub content_type: Option<ContentType>,
    pub base_title: Option<String>,
    pub date: Date,
    /// `l.String()` display forms (alpha2 codes) in `model.Languages` set order.
    pub languages: Vec<String>,
    pub language_multi: bool,
    pub episodes: Episodes,
    pub video_resolution: Option<VideoResolution>,
    pub video_source: Option<VideoSource>,
    pub video_codec: Option<VideoCodec>,
    pub video_3d: Option<Video3D>,
    pub video_modifier: Option<VideoModifier>,
    pub release_group: Option<String>,
    /// The attached content row — Go `classification.Result.Content`
    /// (`result.go:7`), a `*model.Content` that is nil until one of the four
    /// `attach_*` actions succeeds.
    ///
    /// Note this sits on `Result`, **not** on `ContentAttributes`, which is why
    /// [`Classification::merge`] (the port of `ContentAttributes.Merge`) leaves
    /// it alone.
    ///
    /// Always `None` on the flags-off path: with the
    /// [`crate::NullContentResolver`] every lookup misses and the `attach_*`
    /// actions all raise `unmatched`.
    pub content: Option<Content>,
}

impl Classification {
    /// Whether `AttachContent` ran — the corpus `contentAttached` field.
    #[must_use]
    pub fn content_attached(&self) -> bool {
        self.content.is_some()
    }

    /// Port of Go `classification.Result.AttachContent` (`result.go:18-29`):
    /// attach the row, adopt its content type, and fold in its original
    /// language when the result has no languages yet (or is multi-language).
    ///
    /// Unused in this lane — with a null resolver no `attach_*` action ever
    /// reaches it — but it is the single place lane B′-4's four real attach
    /// actions converge on, so the semantics are pinned here rather than
    /// re-derived four times.
    ///
    /// ⚠️ Go appends into a `model.Languages` **set** (a map), so the resulting
    /// display order is that set's order, not append order. The flags-off gates
    /// cannot exercise this, so lane B′-4 must confirm the ordering against a
    /// flags-ON oracle before relying on it.
    pub fn attach_content(&mut self, content: Content) {
        self.content_type = ContentType::parse(content.content_type.as_str());

        if let Some(language) = content.original_language.as_deref() {
            if (self.languages.is_empty() || self.language_multi)
                && !self.languages.iter().any(|l| l == language)
            {
                self.languages.push(language.to_owned());
            }
        }

        self.content = Some(content);
    }

    /// `ContentAttributes.Merge` — fill any unset field from `other`
    /// (`result.go:46`). Only the video-path attributes participate; the CEL
    /// `result` fields (content_type/base_title) are handled by the actions.
    pub fn merge(&mut self, other: Classification) {
        if self.content_type.is_none() {
            self.content_type = other.content_type;
        }
        if self.base_title.is_none() {
            self.base_title = other.base_title;
        }
        if self.date.is_nil() {
            self.date = other.date;
        }
        if self.languages.is_empty() {
            self.languages = other.languages;
        }
        self.language_multi = self.language_multi || other.language_multi;
        if self.episodes.is_empty() {
            self.episodes = other.episodes;
        }
        if self.video_resolution.is_none() {
            self.video_resolution = other.video_resolution;
        }
        if self.video_source.is_none() {
            self.video_source = other.video_source;
        }
        if self.video_codec.is_none() {
            self.video_codec = other.video_codec;
        }
        if self.video_3d.is_none() {
            self.video_3d = other.video_3d;
        }
        if self.video_modifier.is_none() {
            self.video_modifier = other.video_modifier;
        }
        if self.release_group.is_none() {
            self.release_group = other.release_group;
        }
    }
}

/// Renders the corpus `expected` object for this result + terminal outcome.
/// Field values (not order) match `normalizeClassifierResult`; the diff harness
/// canonicalizes keys so declaration order is irrelevant.
#[must_use]
pub(crate) fn to_expected_json(result: &Classification, outcome: &Outcome) -> Value {
    // On any terminal error Go's list-runner returns the zero Result, so the
    // attributes are empty for deleted/unmatched/error outcomes.
    let attrs = match outcome {
        Outcome::Classified => result,
        _ => &Classification::default(),
    };

    let date = if attrs.date.is_nil() {
        Value::Null
    } else {
        json!({"year": attrs.date.year, "month": attrs.date.month, "day": attrs.date.day})
    };

    let mut obj = json!({
        "contentType": attrs.content_type.map_or("", ContentType::as_str),
        "baseTitle": attrs.base_title.clone().map_or(Value::Null, Value::String),
        "date": date,
        "languages": attrs.languages.clone(),
        "languageMulti": attrs.language_multi,
        "episodes": attrs.episodes.to_string(),
        "videoResolution": opt_str(attrs.video_resolution.map(VideoResolution::as_str)),
        "videoSource": opt_str(attrs.video_source.map(VideoSource::as_str)),
        "videoCodec": opt_str(attrs.video_codec.map(VideoCodec::as_str)),
        "video3d": opt_str(attrs.video_3d.map(Video3D::as_str)),
        "videoModifier": opt_str(attrs.video_modifier.map(VideoModifier::as_str)),
        "releaseGroup": attrs.release_group.clone().map_or(Value::Null, Value::String),
        "contentAttached": attrs.content_attached(),
        "outcome": outcome.tag(),
    });

    if let Some(err) = outcome.error_string() {
        obj.as_object_mut()
            .unwrap()
            .insert("error".to_string(), Value::String(err));
    }
    obj
}

fn opt_str(v: Option<&str>) -> Value {
    v.map_or(Value::Null, |s| Value::String(s.to_string()))
}

/// The terminal outcome of a workflow run, mirroring the `outcome`/`error`
/// mapping in `normalizeClassifierResult`.
#[derive(Clone, Debug)]
pub enum Outcome {
    Classified,
    Deleted(String),
    Unmatched(String),
    Error(String),
}

impl Outcome {
    #[must_use]
    pub fn tag(&self) -> &'static str {
        match self {
            Outcome::Classified => "classified",
            Outcome::Deleted(_) => "deleted",
            Outcome::Unmatched(_) => "unmatched",
            Outcome::Error(_) => "error",
        }
    }

    #[must_use]
    pub fn error_string(&self) -> Option<String> {
        match self {
            Outcome::Classified => None,
            Outcome::Deleted(e) | Outcome::Unmatched(e) | Outcome::Error(e) => Some(e.clone()),
        }
    }
}
