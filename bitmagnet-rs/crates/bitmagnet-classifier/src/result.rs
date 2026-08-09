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

    /// Port of Go `classification.Result.ApplyHint` (`result.go:11-14`), which
    /// is the content type plus `ContentAttributes.ApplyHint` (`result.go:93`).
    ///
    /// Every field is **overwrite-if-the-hint-has-one**, not fill-if-empty:
    /// a hint is a stored decision about this torrent and outranks whatever the
    /// name parser would say. That is the opposite of [`Self::merge`], and the
    /// asymmetry is Go's.
    ///
    /// 🚨 Applied BEFORE the workflow runs, so `parse_video_content` can still
    /// overwrite these later via `merge` — which only fills unset fields, so in
    /// practice the hint wins. Note it does NOT carry a base title or a date:
    /// Go's `ApplyHint` leaves both to the parser.
    ///
    /// An attribute the hint spells in a way this build does not recognise is
    /// left unset rather than guessed, matching a Go `Null*` that never
    /// validated.
    pub fn apply_hint(&mut self, hint: &crate::model::InputHint) {
        // `Result.ApplyHint` assigns the content type unconditionally; Go's
        // caller guards on the hint not being nil, and an unparseable type
        // cannot occur there because it comes out of an enum column.
        if let Some(content_type) = ContentType::parse(&hint.content_type) {
            self.content_type = Some(content_type);
        }

        if let Some(episodes) = hint.episodes.as_deref().filter(|e| !e.is_empty()) {
            let parsed = bitmagnet_release::parse_episodes(episodes);
            // Go tests `len(h.Episodes) > 0`, i.e. the parsed set, not the text.
            if !parsed.is_empty() {
                self.episodes = parsed;
            }
        }

        if !hint.languages.is_empty() {
            self.languages = hint.languages.clone();
        }

        if let Some(value) = parse_attribute(hint.video_resolution.as_deref()) {
            self.video_resolution = Some(value);
        }

        if let Some(value) = parse_attribute(hint.video_source.as_deref()) {
            self.video_source = Some(value);
        }

        if let Some(value) = parse_attribute(hint.video_codec.as_deref()) {
            self.video_codec = Some(value);
        }

        if let Some(value) = parse_attribute(hint.video_3d.as_deref()) {
            self.video_3d = Some(value);
        }

        if let Some(value) = parse_attribute(hint.video_modifier.as_deref()) {
            self.video_modifier = Some(value);
        }

        if let Some(group) = hint.release_group.as_deref().filter(|g| !g.is_empty()) {
            self.release_group = Some(group.to_owned());
        }
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

/// Parses an attribute the hint stored, ignoring an absent or unrecognised one.
///
/// A value this build cannot parse is dropped rather than guessed: Go stores
/// these as `Null*` enums that only ever hold a value the writer validated, so
/// an unparseable string means the two sides disagree about the vocabulary, and
/// inventing a variant would hide that.
fn parse_attribute<T: std::str::FromStr>(value: Option<&str>) -> Option<T> {
    value.filter(|v| !v.is_empty())?.parse().ok()
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
