//! The classification result + the flags-off output normalization that mirrors
//! `corpus_test.go normalizeClassifierResult` (the frozen corpus `expected`
//! schema, contract §2.1/§2.3).

use std::collections::BTreeSet;

use bitmagnet_model::Content;
use bitmagnet_release::{
    Episodes, Video3D, VideoCodec, VideoModifier, VideoResolution, VideoSource,
};
use serde::Serialize;
use serde_json::Value;

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
    /// Torrent tags accumulated by `add_tag`. A sorted set matches Go's set
    /// semantics while making the processor write-set deterministic.
    pub tags: BTreeSet<String>,
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
    /// 🚨 Go inserts into a `model.Languages` **map**, so the rendered order is
    /// `Languages.Slice()` — natsort by language NAME — and NOT the order things
    /// were added. Appending and leaving it is therefore wrong the moment the
    /// list has more than one member.
    ///
    /// The flags-off gates cannot exercise this (nothing ever attaches), and the
    /// B′ notes flagged it as needing a flags-ON oracle to confirm. The oracle
    /// confirmed it: it was the sole remaining drift in the write-set gate's
    /// enrichment-dependent bucket, 31 of 231 subjects.
    pub fn attach_content(&mut self, content: Content) {
        self.content_type = ContentType::parse(content.content_type.as_str());

        if let Some(language) = content.original_language.as_deref() {
            if (self.languages.is_empty() || self.language_multi)
                && !self.languages.iter().any(|l| l == language)
            {
                self.languages.push(language.to_owned());
                // Re-derive the set's order; see above.
                self.languages = bitmagnet_release::slice_order(&self.languages);
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

/// Stable, complete classifier boundary used by parity evidence.
///
/// This deliberately mirrors the Go frozen-corpus result field order and
/// nullable/string projections. It contains attributes such as `baseTitle`,
/// `date`, and `languageMulti` that are not all represented in the processor
/// write set, so a same-input rerun cannot hide classifier drift behind an
/// unchanged persistence image.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedClassifierResult {
    pub content_type: String,
    pub base_title: Option<String>,
    pub date: Option<NormalizedClassifierDate>,
    pub languages: Vec<String>,
    pub language_multi: bool,
    pub episodes: String,
    pub video_resolution: Option<String>,
    pub video_source: Option<String>,
    pub video_codec: Option<String>,
    #[serde(rename = "video3d")]
    pub video_3d: Option<String>,
    pub video_modifier: Option<String>,
    pub release_group: Option<String>,
    pub content_attached: bool,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// JSON projection of Go `model.Date` for [`NormalizedClassifierResult`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct NormalizedClassifierDate {
    pub year: i32,
    pub month: i32,
    pub day: i32,
}

impl NormalizedClassifierResult {
    /// Normalize a classification exactly as the Go runner exposes it.
    ///
    /// A terminal workflow error returns Go's zero `classification.Result`, so
    /// deleted, unmatched, and failed results must not retain attributes that
    /// were accumulated before the terminal action. Applying that rule here as
    /// well makes the evidence DTO safe even if a caller supplies a partial
    /// Rust result alongside a terminal outcome.
    #[must_use]
    pub fn from_result(result: &Classification, outcome: &Outcome) -> Self {
        let empty = Classification::default();
        let result = if matches!(outcome, Outcome::Classified) {
            result
        } else {
            &empty
        };

        Self {
            content_type: result
                .content_type
                .map_or_else(String::new, |content_type| content_type.as_str().to_owned()),
            base_title: result.base_title.clone(),
            date: (!result.date.is_nil()).then_some(NormalizedClassifierDate {
                year: i32::from(result.date.year),
                month: i32::from(result.date.month),
                day: i32::from(result.date.day),
            }),
            languages: result.languages.clone(),
            language_multi: result.language_multi,
            episodes: result.episodes.to_string(),
            video_resolution: result
                .video_resolution
                .map(|value| value.as_str().to_owned()),
            video_source: result.video_source.map(|value| value.as_str().to_owned()),
            video_codec: result.video_codec.map(|value| value.as_str().to_owned()),
            video_3d: result.video_3d.map(|value| value.as_str().to_owned()),
            video_modifier: result.video_modifier.map(|value| value.as_str().to_owned()),
            release_group: result.release_group.clone(),
            content_attached: result.content_attached(),
            outcome: outcome.tag().to_owned(),
            error: outcome.error_string(),
        }
    }
}

/// Renders the corpus `expected` object for this result + terminal outcome.
/// Field values (not order) match `normalizeClassifierResult`; the diff harness
/// canonicalizes keys so declaration order is irrelevant.
#[must_use]
pub(crate) fn to_expected_json(result: &Classification, outcome: &Outcome) -> Value {
    serde_json::to_value(NormalizedClassifierResult::from_result(result, outcome))
        .expect("normalized classifier result is JSON-serializable")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn populated_classification() -> Classification {
        let mut episodes = Episodes::default();
        episodes.add_episode(2, 3);

        Classification {
            content_type: Some(ContentType::TvShow),
            base_title: Some("Example Show".to_owned()),
            date: Date {
                year: 2024,
                month: 5,
                day: 6,
            },
            languages: vec!["en".to_owned(), "fr".to_owned()],
            language_multi: true,
            episodes,
            video_resolution: Some(VideoResolution::V1080p),
            video_source: Some(VideoSource::Bluray),
            release_group: Some("GROUP".to_owned()),
            ..Classification::default()
        }
    }

    #[test]
    fn normalized_result_preserves_full_classifier_only_fields_and_order() {
        let normalized = NormalizedClassifierResult::from_result(
            &populated_classification(),
            &Outcome::Classified,
        );

        assert_eq!(normalized.base_title.as_deref(), Some("Example Show"));
        assert_eq!(
            normalized.date,
            Some(NormalizedClassifierDate {
                year: 2024,
                month: 5,
                day: 6,
            })
        );
        assert!(normalized.language_multi);
        assert_eq!(normalized.episodes, "S02E03");

        let encoded = serde_json::to_string(&normalized).expect("serialize normalized result");
        assert_eq!(
            encoded,
            r#"{"contentType":"tv_show","baseTitle":"Example Show","date":{"year":2024,"month":5,"day":6},"languages":["en","fr"],"languageMulti":true,"episodes":"S02E03","videoResolution":"V1080p","videoSource":"BluRay","videoCodec":null,"video3d":null,"videoModifier":null,"releaseGroup":"GROUP","contentAttached":false,"outcome":"classified"}"#
        );
    }

    #[test]
    fn normalized_result_zeros_attributes_for_terminal_outcomes() {
        let populated = populated_classification();
        for outcome in [
            Outcome::Deleted("deleted".to_owned()),
            Outcome::Unmatched("unmatched".to_owned()),
            Outcome::Error("failed".to_owned()),
        ] {
            let normalized = NormalizedClassifierResult::from_result(&populated, &outcome);
            assert_eq!(normalized.content_type, "");
            assert_eq!(normalized.base_title, None);
            assert_eq!(normalized.date, None);
            assert!(normalized.languages.is_empty());
            assert!(!normalized.language_multi);
            assert_eq!(normalized.episodes, "");
            assert_eq!(normalized.outcome, outcome.tag());
            assert_eq!(normalized.error, outcome.error_string());
        }
    }
}
