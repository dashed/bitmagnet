//! Read-only hydration for the processor shadow.
//!
//! This loader intentionally stays inside the frozen shadow-role boundary:
//! `torrents` supplies the classifier image, `torrent_hints` supplies the
//! explicit hint, and `torrent_contents` supplies both stale IDs and the
//! reusable effective hint used by Go's default mode.

use std::collections::{BTreeMap, BTreeSet};

use bitmagnet_classifier::{ClassifierInput, ContentType, InputContent, InputFile, InputHint};
use bitmagnet_model::Content;
use bitmagnet_model::{deserialize_files_bounded, BlobError};
use bitmagnet_queue::{ProcessTorrentParams, CLASSIFY_MODE_REMATCH};
use bitmagnet_release::{
    Episodes, Video3D, VideoCodec, VideoModifier, VideoResolution, VideoSource,
};
use futures::TryStreamExt;
use sqlx::{PgConnection, PgPool, Row};

use super::LoadedTorrent;

const MAX_COMPRESSED_OR_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const MAX_FILES_PER_TORRENT: usize = 300_000;
const MAX_JOB_COMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const MAX_JOB_DECODED_BYTES: usize = 128 * 1024 * 1024;
const MAX_JOB_FILES: usize = 300_000;

/// Hydrate the requested torrents without locking or mutating live rows.
///
/// Missing hashes are intentionally omitted. The materializer turns those
/// omissions into its `failed_info_hashes` image, matching the Go republish
/// boundary.
pub async fn load_torrents(
    pool: &PgPool,
    params: &ProcessTorrentParams,
) -> Result<Vec<LoadedTorrent>, LoadError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let loaded = load_torrents_in(&mut tx, params).await?;
    tx.commit().await?;
    Ok(loaded)
}

pub(crate) async fn load_torrents_in(
    connection: &mut PgConnection,
    params: &ProcessTorrentParams,
) -> Result<Vec<LoadedTorrent>, LoadError> {
    let requested = params
        .info_hashes
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    let content_rows = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, id, \
                content_type::text AS content_type, content_source, content_id \
         FROM torrent_contents \
         WHERE info_hash = ANY($1::bytea[]) \
         ORDER BY info_hash, id",
    )
    .bind(&requested)
    .fetch_all(&mut *connection)
    .await?;
    let mut current = BTreeMap::<String, Vec<CurrentContent>>::new();
    for row in content_rows {
        current
            .entry(row.try_get("info_hash")?)
            .or_default()
            .push(CurrentContent {
                id: row.try_get("id")?,
                content_type: row.try_get("content_type")?,
                content_source: row.try_get("content_source")?,
                content_id: row.try_get("content_id")?,
            });
    }

    let hydrated = hydrate_contents(&mut *connection, &current).await?;

    // `title` and `release_year` are not read: nothing in Go's classification
    // does, which the torrent hint decode fixture proves.
    let hint_rows = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, content_type, \
                content_source, content_id, languages::text AS languages, \
                episodes::text AS episodes, video_resolution, video_source, \
                video_codec, video_3d, video_modifier, release_group \
         FROM torrent_hints \
         WHERE info_hash = ANY($1::bytea[]) \
         ORDER BY info_hash",
    )
    .bind(&requested)
    .fetch_all(&mut *connection)
    .await?;
    let mut explicit_hints = BTreeMap::<String, Result<CurrentHint, &'static str>>::new();
    for row in hint_rows {
        let stored = StoredHint {
            content_type: row.try_get("content_type")?,
            content_source: row.try_get("content_source")?,
            content_id: row.try_get("content_id")?,
            languages: row.try_get("languages")?,
            episodes: row.try_get("episodes")?,
            video_resolution: row.try_get("video_resolution")?,
            video_source: row.try_get("video_source")?,
            video_codec: row.try_get("video_codec")?,
            video_3d: row.try_get("video_3d")?,
            video_modifier: row.try_get("video_modifier")?,
            release_group: row.try_get("release_group")?,
        };
        explicit_hints.insert(row.try_get("info_hash")?, stored.decode());
    }

    let mut rows = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, name, size, \
                files_status::text AS files_status, extension, files_count, \
                octet_length(files_data)::bigint AS files_data_bytes, \
                CASE WHEN octet_length(files_data) <= $2 THEN files_data END AS files_data \
         FROM torrents \
         WHERE info_hash = ANY($1::bytea[]) \
         ORDER BY info_hash",
    )
    .bind(&requested)
    .bind(i64::try_from(MAX_COMPRESSED_OR_DECOMPRESSED_BYTES).expect("64 MiB fits i64"))
    .fetch(&mut *connection);

    let mut loaded = Vec::new();
    let mut job_compressed_bytes = 0_usize;
    let mut job_decoded_bytes = 0_usize;
    let mut job_files = 0_usize;
    while let Some(row) = rows.try_next().await? {
        let info_hash: String = row.try_get("info_hash")?;
        let size = nonnegative_u64("size", row.try_get("size")?)?;
        let files_count = row
            .try_get::<Option<i32>, _>("files_count")?
            .map(|value| nonnegative_u32("files_count", value))
            .transpose()?;
        let compressed_bytes = row
            .try_get::<Option<i64>, _>("files_data_bytes")?
            .map(|bytes| nonnegative_usize("files_data_bytes", bytes))
            .transpose()?
            .unwrap_or_default();
        if compressed_bytes > MAX_COMPRESSED_OR_DECOMPRESSED_BYTES {
            return Err(LoadError::CompressedBlobTooLarge {
                bytes: compressed_bytes,
                limit: MAX_COMPRESSED_OR_DECOMPRESSED_BYTES,
            });
        }
        job_compressed_bytes = job_compressed_bytes.saturating_add(compressed_bytes);
        enforce_job_budget(
            "compressed bytes",
            job_compressed_bytes,
            MAX_JOB_COMPRESSED_BYTES,
        )?;
        let mut files = match row.try_get::<Option<Vec<u8>>, _>("files_data")? {
            Some(blob) => {
                if blob.len() > MAX_COMPRESSED_OR_DECOMPRESSED_BYTES {
                    return Err(LoadError::CompressedBlobTooLarge {
                        bytes: blob.len(),
                        limit: MAX_COMPRESSED_OR_DECOMPRESSED_BYTES,
                    });
                }
                let decoded = deserialize_files_bounded(
                    &blob,
                    MAX_COMPRESSED_OR_DECOMPRESSED_BYTES,
                    MAX_FILES_PER_TORRENT,
                )?;
                job_decoded_bytes = job_decoded_bytes
                    .saturating_add(decoded.decompressed_bytes)
                    .saturating_add(decoded.owned_string_bytes);
                enforce_job_budget(
                    "decoded and retained string bytes",
                    job_decoded_bytes,
                    MAX_JOB_DECODED_BYTES,
                )?;
                job_files = job_files.saturating_add(decoded.files.len());
                enforce_job_budget("files", job_files, MAX_JOB_FILES)?;
                decoded.files
            }
            None => Vec::new(),
        };
        // Go's Torrent.AfterFind orders the hydrated blob image by path before
        // classification; retain the original file index within that ordering.
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let files = files
            .into_iter()
            .map(|file| InputFile {
                index: file.index,
                path: file.path,
                extension: file.extension,
                size: file.size,
            })
            .collect();

        let existing = current.remove(&info_hash).unwrap_or_default();
        let allow_content_reuse = params.classify_mode != CLASSIFY_MODE_REMATCH;
        let files_status: String = row.try_get("files_status")?;
        let stored_file_list = has_stored_file_list(&files_status);
        // An explicit hint that cannot be reproduced leaves the torrent to Go;
        // the rest of its hydration is then never used.
        let (explicit, hint_admission) = match explicit_hints.remove(&info_hash) {
            None => (None, HintAdmission::Supported),
            Some(Err(detail)) => (None, HintAdmission::Unsupported { detail }),
            Some(Ok(hint)) => {
                let admission = hint.admission();
                (Some(hint), admission)
            }
        };
        let content_ambiguity = content_ambiguity(
            explicit.as_ref(),
            &existing,
            allow_content_reuse,
            stored_file_list,
        );
        let hint = effective_hint(explicit, &existing, allow_content_reuse, stored_file_list);
        let source_backed_content_present = hint.as_ref().is_some_and(|hint| {
            !hint.content_source.is_empty()
                && existing.iter().any(|content| {
                    content.content_type.as_deref() == Some(hint.content_type.as_str())
                        && content.content_source.as_deref() == Some(hint.content_source.as_str())
                        && content.content_id.as_deref().unwrap_or_default() == hint.content_id
                })
        });
        loaded.push(LoadedTorrent {
            info_hash: info_hash.clone(),
            classifier_input: ClassifierInput {
                id: info_hash,
                name: row.try_get("name")?,
                size,
                files_status: files_status.clone(),
                extension: row.try_get("extension")?,
                files_count,
                files,
                hint,
                // T9: the associations the classifier pre-attaches from. An
                // association whose `content` row could not be hydrated is
                // carried with `content: None`, which the pre-attach treats
                // exactly as Go treats an unloaded association -- it skips it
                // rather than attaching a blank row.
                contents: existing
                    .iter()
                    .map(|content| content.to_input(&hydrated))
                    .collect(),
            },
            existing_content_ids: existing.iter().map(|content| content.id.clone()).collect(),
            hint_admission,
            content_ambiguity,
            source_backed_content_present,
        });
    }
    Ok(loaded)
}

/// The `(type, source, id)` key of a `content` row.
type ContentKey = (String, String, String);

/// Loads the `content` rows the associations point at, keyed by their primary
/// key.
///
/// A separate query rather than a join, because `torrent_contents` and `content`
/// both have an `id` column and aliasing one of them away would mean the shared
/// [`bitmagnet_content_search::CONTENT_COLUMNS`] select list could not be used
/// verbatim. Reusing that list is the point: the live `ContentResolver` will
/// hydrate the same row, and a second hand-written copy could drift.
///
/// A referenced row that does not come back is simply absent from the map. That
/// is not an error — an association can outlive the content it points at — and
/// the caller turns it into `content: None`, which the pre-attach skips.
async fn hydrate_contents(
    connection: &mut PgConnection,
    current: &BTreeMap<String, Vec<CurrentContent>>,
) -> Result<BTreeMap<ContentKey, Content>, LoadError> {
    let mut types = Vec::new();
    let mut sources = Vec::new();
    let mut ids = Vec::new();
    let mut seen = std::collections::BTreeSet::new();

    for content in current.values().flatten() {
        let Some(key) = content.key() else { continue };
        if seen.insert(key.clone()) {
            types.push(key.0);
            sources.push(key.1);
            ids.push(key.2);
        }
    }

    if types.is_empty() {
        return Ok(BTreeMap::new());
    }

    // The only interpolation is a compile-time constant select list; every
    // value is bound. The keys go in as three parallel arrays rather than a
    // generated IN-list, so the statement text is fixed regardless of how many
    // rows are requested -- which also keeps it plan-cacheable.
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {} FROM content \
         WHERE (type::text, source, id) \
               IN (SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[]))",
        bitmagnet_content_search::CONTENT_COLUMNS
    )))
    .bind(&types)
    .bind(&sources)
    .bind(&ids)
    .fetch_all(&mut *connection)
    .await?;

    let mut hydrated = BTreeMap::new();
    for row in rows {
        let content = bitmagnet_content_search::content_from_row(&row)?;
        hydrated.insert(
            (
                content.content_type.as_str().to_owned(),
                content.source.clone(),
                content.id.clone(),
            ),
            content,
        );
    }

    Ok(hydrated)
}

/// A `torrent_hints` row as PostgreSQL returns it, before Go's GORM scan.
///
/// The jsonb columns are their text rendering; `title` and `release_year` are
/// absent because Go never reads them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredHint {
    pub content_type: String,
    pub content_source: Option<String>,
    pub content_id: Option<String>,
    pub languages: Option<String>,
    pub episodes: Option<String>,
    pub video_resolution: Option<String>,
    pub video_source: Option<String>,
    pub video_codec: Option<String>,
    pub video_3d: Option<String>,
    pub video_modifier: Option<String>,
    pub release_group: Option<String>,
}

impl StoredHint {
    /// The hint as Go's `model.TorrentHint` scan reads it, or the column the
    /// writer cannot reproduce it from.
    ///
    /// Go fails the whole job's load on every value it rejects, so refusing
    /// one leaves the job to Go, which fails it as before. A few values Go
    /// accepts are refused too, because Rust would not write them back the way
    /// Go does or does not need to read them at all. The
    /// `testdata/parity/torrent-hint-decode` fixture, generated from Go's own
    /// scan, pins both sets.
    ///
    /// # Errors
    ///
    /// The name of the first column whose value is refused.
    pub fn decode(self) -> Result<CurrentHint, &'static str> {
        // Go's go-enum scan folds case; only the canonical spelling is taken.
        if ContentType::parse(&self.content_type).is_none() {
            return Err("content_type");
        }
        match (self.content_source.as_deref(), self.content_id.as_deref()) {
            // A non-NULL empty source is valid in Go (`hasHintedContentId`) and
            // indistinguishable from no source in Rust.
            (Some(""), _) => return Err("content_source"),
            (Some(_), id) if !id.is_some_and(is_reproducible_content_id) => {
                return Err("content_id");
            }
            _ => {}
        }
        let attributes = HintAttributes {
            episodes: decode_episodes(self.episodes.as_deref())?,
            languages: decode_languages(self.languages.as_deref())?,
            video_resolution: decode_video(
                self.video_resolution,
                VideoResolution::ALL,
                VideoResolution::as_str,
                "video_resolution",
            )?,
            video_source: decode_video(
                self.video_source,
                VideoSource::ALL,
                VideoSource::as_str,
                "video_source",
            )?,
            video_codec: decode_video(
                self.video_codec,
                VideoCodec::ALL,
                VideoCodec::as_str,
                "video_codec",
            )?,
            video_3d: decode_video(self.video_3d, Video3D::ALL, Video3D::as_str, "video_3d")?,
            video_modifier: decode_video(
                self.video_modifier,
                VideoModifier::ALL,
                VideoModifier::as_str,
                "video_modifier",
            )?,
            // Go applies a valid empty group too.
            release_group: self.release_group,
        };
        Ok(CurrentHint {
            content_type: self.content_type,
            content_source: self.content_source,
            content_id: self.content_id,
            attributes,
        })
    }
}

/// A sourced hint's id must be safe to put in a TMDB path as it stands.
///
/// Go concatenates `"/find/" + id` raw, while Rust's client parses the URL, so
/// an id with a separator, an escape or a leading dot (`..`) would not reach
/// TMDB as the same request. Prod's ids are all `tt` + digits.
fn is_reproducible_content_id(id: &str) -> bool {
    let mut bytes = id.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// `languages` as Go's `Languages.UnmarshalJSON` reads it, limited to what
/// Go's `MarshalJSON` writes: lowercase alpha-2 codes. Go also takes alpha-3
/// codes, names and aliases in any case, and picks among ambiguous aliases by
/// map order.
fn decode_languages(text: Option<&str>) -> Result<Vec<String>, &'static str> {
    let Some(text) = text else {
        return Ok(Vec::new());
    };
    let codes = serde_json::from_str::<Option<Vec<String>>>(text)
        .map_err(|_| "languages")?
        .unwrap_or_default();
    if codes
        .iter()
        .any(|code| bitmagnet_release::parse_language(code).as_deref() != Some(code.as_str()))
    {
        return Err("languages");
    }
    // Go's set, rendered in `Languages.Slice()` order.
    Ok(bitmagnet_release::slice_order(codes))
}

/// `episodes` as Go's JSON decode into `map[int]map[int]struct{}` reads it,
/// limited to the shape Go's encoder writes: seasons with canonical decimal
/// keys, each an object of episodes whose values are `{}`.
///
/// 🚨 `{"1": null}` decodes in Go to a nil episode map, which Go then persists
/// as `{"1":null}`; Rust's set cannot carry that, so it is refused. So are
/// `"01"`, `"+1"` and `"-1"` keys, numbers above `u16`, and episode values
/// other than `{}`.
fn decode_episodes(text: Option<&str>) -> Result<Episodes, &'static str> {
    let Some(text) = text else {
        return Ok(Episodes::new());
    };
    let Some(seasons) =
        serde_json::from_str::<Option<BTreeMap<String, BTreeMap<String, serde_json::Value>>>>(text)
            .map_err(|_| "episodes")?
    else {
        return Ok(Episodes::new());
    };
    let mut episodes = Episodes::allocated();
    for (season, numbers) in seasons {
        let season = canonical_number(&season).ok_or("episodes")?;
        if numbers.is_empty() {
            episodes.add_season(season);
        }
        for (episode, value) in numbers {
            if !value.as_object().is_some_and(serde_json::Map::is_empty) {
                return Err("episodes");
            }
            episodes.add_episode(season, canonical_number(&episode).ok_or("episodes")?);
        }
    }
    Ok(episodes)
}

/// A JSON object key as Go's encoder writes an `int`: no sign, no leading zero.
///
/// Bounded to `u16`, because the writer persists episodes through their
/// `S01E02` rendering and parses each number back as one; Go takes any `int`.
fn canonical_number(key: &str) -> Option<i64> {
    let canonical = key == "0"
        || (!key.is_empty()
            && !key.starts_with('0')
            && key.bytes().all(|byte| byte.is_ascii_digit()));
    canonical
        .then(|| key.parse::<u16>().ok())
        .flatten()
        .map(i64::from)
}

/// A video attribute as Go's go-enum scan reads it: the canonical spelling,
/// matched ignoring ASCII case. Go lowercases with Unicode rules, so it also
/// takes a few non-ASCII spellings (`WOR\u{212A}PRINT`), which are refused.
fn decode_video<T: Copy>(
    value: Option<String>,
    all: &[T],
    as_str: fn(T) -> &'static str,
    column: &'static str,
) -> Result<Option<String>, &'static str> {
    value
        .map(|value| {
            all.iter()
                .map(|variant| as_str(*variant))
                .find(|canonical| canonical.eq_ignore_ascii_case(&value))
                .map(str::to_owned)
                .ok_or(column)
        })
        .transpose()
}

/// A decoded `torrent_hints` row — Go's explicit hint, before the processor
/// decides what the classifier actually sees.
#[derive(Debug, Clone, Default)]
pub struct CurrentHint {
    pub content_type: String,
    pub content_source: Option<String>,
    pub content_id: Option<String>,
    pub attributes: HintAttributes,
}

/// What Go's `ContentAttributes.ApplyHint` copies from a hint, decoded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HintAttributes {
    pub episodes: Episodes,
    /// Alpha-2 codes in `Languages.Slice()` order.
    pub languages: Vec<String>,
    /// Canonical spellings.
    pub video_resolution: Option<String>,
    pub video_source: Option<String>,
    pub video_codec: Option<String>,
    pub video_3d: Option<String>,
    pub video_modifier: Option<String>,
    pub release_group: Option<String>,
}

impl CurrentHint {
    /// A sourced hint reaches Go's pre-attach or its by-ID lookups, which only
    /// a live resolver can reproduce.
    fn admission(&self) -> HintAdmission {
        if self.content_source.is_some() {
            HintAdmission::NeedsLiveResolver
        } else {
            HintAdmission::Supported
        }
    }

    /// The classifier input, or `None` for Go's nil hint (no content type).
    fn into_input(self) -> Option<InputHint> {
        if self.content_type.is_empty() {
            return None;
        }
        let HintAttributes {
            episodes,
            languages,
            video_resolution,
            video_source,
            video_codec,
            video_3d,
            video_modifier,
            release_group,
        } = self.attributes;
        Some(InputHint {
            content_type: self.content_type,
            content_source: self.content_source.unwrap_or_default(),
            content_id: self.content_id.unwrap_or_default(),
            episodes,
            languages,
            video_resolution,
            video_source,
            video_codec,
            video_3d,
            video_modifier,
            release_group,
        })
    }
}

/// How far the writer can reproduce Go's handling of a torrent's explicit hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintAdmission {
    /// No explicit hint, or a source-less one any writer reproduces.
    Supported,
    /// A sourced hint, which needs a live resolver.
    NeedsLiveResolver,
    /// A hint the writer cannot reproduce; `detail` names the column.
    Unsupported { detail: &'static str },
}

/// Where Go's choice among a torrent's existing associations depends on row
/// order.
///
/// Go's processor takes the FIRST eligible association for content reuse
/// (`processor.go:123-132`) and the FIRST source-less one for the preserved
/// type (`preservedRuleDerivedContentType`), from a preload without
/// `ORDER BY`. With two candidates that differ, Go's pick is not
/// reproducible, so the job is left to Go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentAmbiguity {
    /// More than one sourced association is eligible for reuse.
    Reuse,
    /// A torrent with no file list and no hint has source-less associations of
    /// more than one type.
    PreservedType,
}

/// The [`ContentAmbiguity`] of a torrent, if any, under the same rules as
/// [`effective_hint`].
pub fn content_ambiguity(
    explicit: Option<&CurrentHint>,
    existing: &[CurrentContent],
    allow_content_reuse: bool,
    has_stored_file_list: bool,
) -> Option<ContentAmbiguity> {
    // A sourced explicit hint is never overwritten, and is not nil.
    if explicit.is_some_and(|hint| hint.content_source.is_some()) {
        return None;
    }
    let explicit_type = explicit.map(|hint| hint.content_type.as_str());
    let reusable = if allow_content_reuse {
        existing
            .iter()
            .filter(|content| content.is_reuse_candidate(explicit_type))
            .count()
    } else {
        0
    };
    if reusable > 1 {
        return Some(ContentAmbiguity::Reuse);
    }
    if reusable == 0 && explicit.is_none() && !has_stored_file_list {
        let preserved_types = existing
            .iter()
            .filter(|content| content.content_source.is_none())
            .filter_map(|content| content.content_type.as_deref())
            .collect::<BTreeSet<_>>();
        if preserved_types.len() > 1 {
            return Some(ContentAmbiguity::PreservedType);
        }
    }
    None
}

/// A `torrent_contents` association, as much of it as the hint synthesis reads.
#[derive(Debug, Clone)]
pub struct CurrentContent {
    pub id: String,
    pub content_type: Option<String>,
    pub content_source: Option<String>,
    pub content_id: Option<String>,
}

impl CurrentContent {
    /// The `content` primary key this association points at, when it points at
    /// one at all.
    fn key(&self) -> Option<ContentKey> {
        Some((
            self.content_type.clone()?,
            self.content_source.clone()?,
            self.content_id.clone()?,
        ))
    }

    /// The classifier-side view, with the `content` row attached when it was
    /// hydrated.
    fn to_input(&self, hydrated: &BTreeMap<ContentKey, Content>) -> InputContent {
        InputContent {
            content_type: self.content_type.clone().unwrap_or_default(),
            content_source: self.content_source.clone().unwrap_or_default(),
            content_id: self.content_id.clone().unwrap_or_default(),
            content: self.key().and_then(|key| hydrated.get(&key)).cloned(),
        }
    }

    /// Go's reuse condition, given the explicit hint's type when there is one.
    fn is_reuse_candidate(&self, explicit_type: Option<&str>) -> bool {
        self.content_type.is_some()
            && self.content_source.is_some()
            && explicit_type
                .is_none_or(|content_type| self.content_type.as_deref() == Some(content_type))
    }

    /// Go's reuse (`processor.go:129-131`): this association's type, source and
    /// id overwrite the explicit hint's, and everything else the hint carries
    /// stays.
    fn reused_into(&self, explicit: Option<InputHint>) -> InputHint {
        let mut hint = explicit.unwrap_or_default();
        hint.content_type = self.content_type.clone().unwrap_or_default();
        hint.content_source = self.content_source.clone().unwrap_or_default();
        hint.content_id = self.content_id.clone().unwrap_or_default();
        hint
    }
}

/// Mirrors Go's `model.FilesStatus.HasStoredFileList`. A `no_info` /
/// `over_threshold` torrent has no stored file list BY NATURE, so classifier
/// rules over `torrent.files` cannot fire for it at all.
pub fn has_stored_file_list(files_status: &str) -> bool {
    files_status != "no_info" && files_status != "over_threshold"
}

pub fn effective_hint(
    explicit: Option<CurrentHint>,
    existing: &[CurrentContent],
    allow_content_reuse: bool,
    has_stored_file_list: bool,
) -> Option<InputHint> {
    let explicit = explicit.and_then(CurrentHint::into_input);
    if explicit
        .as_ref()
        .is_some_and(|hint| !hint.content_source.is_empty())
    {
        return explicit;
    }
    let explicit_type = explicit.as_ref().map(|hint| hint.content_type.clone());
    // Go reuses the first candidate; `content_ambiguity` refuses a torrent
    // with more than one, so which one this is cannot matter.
    let reused = existing.iter().find(|content| {
        allow_content_reuse && content.is_reuse_candidate(explicit_type.as_deref())
    });
    let resolved = match reused {
        Some(content) => Some(content.reused_into(explicit)),
        None => explicit,
    };
    if resolved.is_some() || has_stored_file_list {
        return resolved;
    }
    // Mirrors the Go processor: the reuse above is source-backed only, so a
    // rule-derived content type (`xxx`, `comic`, ...) was never carried over. With
    // no stored file list the rules cannot re-derive it, so clearing it is data
    // loss rather than a fresh verdict. Type only — never source/ID — and
    // deliberately NOT gated on classify mode, matching Go.
    existing.iter().find_map(|content| {
        let content_type = content.content_type.clone()?;
        content.content_source.is_none().then_some(InputHint {
            content_type,
            content_source: String::new(),
            content_id: String::new(),
            ..InputHint::default()
        })
    })
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("compressed files_data blob is {bytes} bytes, above the {limit}-byte shadow limit")]
    CompressedBlobTooLarge { bytes: usize, limit: usize },
    #[error("shadow job {resource} budget exceeded: {actual} > {limit}")]
    JobBudgetExceeded {
        resource: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("negative {field} value in live row: {value}")]
    NegativeInteger { field: &'static str, value: i64 },
}

fn nonnegative_u64(field: &'static str, value: i64) -> Result<u64, LoadError> {
    u64::try_from(value).map_err(|_| LoadError::NegativeInteger { field, value })
}

fn nonnegative_u32(field: &'static str, value: i32) -> Result<u32, LoadError> {
    u32::try_from(value).map_err(|_| LoadError::NegativeInteger {
        field,
        value: i64::from(value),
    })
}

fn nonnegative_usize(field: &'static str, value: i64) -> Result<usize, LoadError> {
    usize::try_from(value).map_err(|_| LoadError::NegativeInteger { field, value })
}

fn enforce_job_budget(
    resource: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), LoadError> {
    if actual > limit {
        Err(LoadError::JobBudgetExceeded {
            resource,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        content_ambiguity, effective_hint, has_stored_file_list, ContentAmbiguity, CurrentContent,
        CurrentHint, HintAdmission, HintAttributes, StoredHint,
    };
    use bitmagnet_classifier::{Classification, InputHint, NormalizedClassifierResult, Outcome};
    use bitmagnet_model::Content;
    use std::collections::BTreeMap;

    fn tc(
        id: &str,
        content_type: Option<&str>,
        source: Option<&str>,
        content_id: Option<&str>,
    ) -> CurrentContent {
        CurrentContent {
            id: id.into(),
            content_type: content_type.map(Into::into),
            content_source: source.map(Into::into),
            content_id: content_id.map(Into::into),
        }
    }

    fn hint(content_type: &str, source: Option<&str>, id: Option<&str>) -> CurrentHint {
        CurrentHint {
            content_type: content_type.into(),
            content_source: source.map(Into::into),
            content_id: id.map(Into::into),
            attributes: HintAttributes::default(),
        }
    }

    fn enriched(content_type: &str) -> CurrentHint {
        CurrentHint {
            attributes: HintAttributes {
                episodes: bitmagnet_release::parse_episodes("S01E02"),
                languages: vec!["en".into()],
                video_resolution: Some("V1080p".into()),
                release_group: Some(String::new()),
                ..HintAttributes::default()
            },
            ..hint(content_type, None, None)
        }
    }

    /// A stable rendering of an effective hint, for equality.
    fn render(hint: Option<&InputHint>) -> String {
        format!("{hint:?}")
    }

    #[test]
    fn reuse_takes_type_and_source_and_keeps_the_explicit_attributes() {
        let complete = tc("tc", Some("movie"), Some("tmdb"), Some("603"));
        assert!(complete.is_reuse_candidate(None));
        assert!(complete.is_reuse_candidate(Some("movie")));
        assert!(!complete.is_reuse_candidate(Some("tv_show")));
        assert!(!tc("tc", Some("movie"), None, Some("603")).is_reuse_candidate(None));
        assert!(!tc("tc", None, Some("tmdb"), Some("603")).is_reuse_candidate(None));
        // A non-NULL empty source is valid in Go.
        assert!(tc("tc", Some("movie"), Some(""), None).is_reuse_candidate(None));

        let explicit = enriched("movie").into_input();
        let reused = complete.reused_into(explicit.clone());
        assert_eq!(
            (
                reused.content_type.as_str(),
                reused.content_source.as_str(),
                reused.content_id.as_str()
            ),
            ("movie", "tmdb", "603")
        );
        let explicit = explicit.expect("an explicit hint");
        assert_eq!(reused.episodes, explicit.episodes);
        assert_eq!(reused.languages, explicit.languages);
        assert_eq!(reused.video_resolution, explicit.video_resolution);
        assert_eq!(reused.release_group, Some(String::new()));
        assert!(complete.reused_into(None).episodes.is_empty());
    }

    #[test]
    fn an_association_hydrates_into_the_classifier_input() {
        let association = CurrentContent {
            id: "tc".into(),
            content_type: Some("movie".into()),
            content_source: Some("tmdb".into()),
            content_id: Some("77117".into()),
        };
        let key = association.key().expect("a fully-keyed association");
        let hydrated = BTreeMap::from([(
            key,
            Content {
                content_type: bitmagnet_model::ContentType::Movie,
                source: "tmdb".into(),
                id: "77117".into(),
                title: "Sunny".into(),
                release_date: None,
                release_year: Some(2011),
                adult: None,
                original_language: None,
                original_title: None,
                overview: None,
                runtime: None,
                popularity: None,
                vote_average: None,
                vote_count: None,
                created_at: Some(1),
                updated_at: None,
                tsv: Default::default(),
                collections: Vec::new(),
                attributes: Vec::new(),
            },
        )]);

        let input = association.to_input(&hydrated);

        assert_eq!(input.content_type, "movie");
        assert_eq!(input.content_source, "tmdb");
        assert_eq!(input.content_id, "77117");
        assert_eq!(
            input.content.as_ref().map(|content| content.title.as_str()),
            Some("Sunny"),
            "the pre-attach needs the hydrated row, not just the ids"
        );
    }

    /// An association whose content row is gone must arrive as `None`. The
    /// classifier's pre-attach then skips it, matching Go's guard on an unloaded
    /// association — attaching a blank row would be worse than not attaching.
    #[test]
    fn a_missing_content_row_hydrates_to_none() {
        let association = CurrentContent {
            id: "tc".into(),
            content_type: Some("movie".into()),
            content_source: Some("tmdb".into()),
            content_id: Some("77117".into()),
        };

        let input = association.to_input(&BTreeMap::new());

        assert_eq!(input.content_id, "77117");
        assert!(input.content.is_none());
    }

    /// A partially-keyed association points at no content row at all, so it can
    /// never be hydrated and must not be looked up.
    #[test]
    fn a_partially_keyed_association_has_no_content_key() {
        let association = CurrentContent {
            id: "tc".into(),
            content_type: Some("movie".into()),
            content_source: None,
            content_id: Some("77117".into()),
        };

        assert!(association.key().is_none());
        assert!(association.to_input(&BTreeMap::new()).content.is_none());
    }

    /// Go's processor (`processor.go:115-152`), transcribed loop for loop: the
    /// reuse takes the FIRST eligible association in the order given, and
    /// overwrites the explicit hint's type, source and id only.
    fn go_effective_hint(
        explicit: Option<&CurrentHint>,
        existing: &[&CurrentContent],
        rematch: bool,
        has_stored_file_list: bool,
    ) -> Option<InputHint> {
        let mut hint = explicit.cloned().and_then(CurrentHint::into_input);
        let mut found = false;
        for content in existing {
            let source_valid = hint
                .as_ref()
                .is_some_and(|hint| !hint.content_source.is_empty());
            if !found
                && !source_valid
                && !rematch
                && content.content_type.is_some()
                && content.content_source.is_some()
                && hint.as_ref().is_none_or(|hint| {
                    content.content_type.as_deref() == Some(hint.content_type.as_str())
                })
            {
                let mut reused = hint.unwrap_or_default();
                reused.content_type = content.content_type.clone().unwrap();
                reused.content_source = content.content_source.clone().unwrap();
                reused.content_id = content.content_id.clone().unwrap_or_default();
                hint = Some(reused);
                found = true;
            }
        }
        if !found && hint.is_none() && !has_stored_file_list {
            hint = existing.iter().find_map(|content| {
                (content.content_type.is_some() && content.content_source.is_none()).then(|| {
                    InputHint {
                        content_type: content.content_type.clone().unwrap(),
                        ..InputHint::default()
                    }
                })
            });
        }
        hint
    }

    fn permutations<'a>(items: &[&'a CurrentContent]) -> Vec<Vec<&'a CurrentContent>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut all = Vec::new();
        for (index, item) in items.iter().enumerate() {
            let mut rest = items.to_vec();
            rest.remove(index);
            for mut tail in permutations(&rest) {
                tail.insert(0, item);
                all.push(tail);
            }
        }
        all
    }

    /// The precedence table: every explicit hint shape against every set of
    /// associations, in both modes, with and without a file list.
    ///
    /// Where `content_ambiguity` is `None`, `effective_hint` must equal Go's
    /// verdict under EVERY row order Go could see. Where it is `Some`, at least
    /// two orders must give Go different verdicts, so the refusal is never
    /// spurious.
    #[test]
    fn effective_hint_is_go_under_every_row_order_unless_ambiguous() {
        let explicit_hints = [
            None,
            Some(hint("movie", None, None)),
            Some(enriched("movie")),
            Some(hint("movie", None, Some("tt0133093"))),
            Some(hint("movie", Some("imdb"), Some("tt0133093"))),
            Some(enriched("tv_show")),
        ];
        let movie_603 = tc("a", Some("movie"), Some("tmdb"), Some("603"));
        let movie_604 = tc("b", Some("movie"), Some("tmdb"), Some("604"));
        let tv = tc("c", Some("tv_show"), Some("tvdb"), Some("81189"));
        let xxx = tc("d", Some("xxx"), None, None);
        let comic = tc("e", Some("comic"), None, None);
        let xxx_again = tc("f", Some("xxx"), None, None);
        let untyped = tc("g", None, Some("tmdb"), Some("1"));
        let association_sets: Vec<Vec<&CurrentContent>> = vec![
            vec![],
            vec![&movie_603],
            vec![&tv],
            vec![&movie_603, &tv],
            vec![&movie_603, &movie_604],
            vec![&movie_603, &movie_604, &tv],
            vec![&xxx],
            vec![&xxx, &comic],
            vec![&xxx, &xxx_again],
            vec![&movie_603, &xxx, &comic],
            vec![&untyped, &xxx],
        ];

        let (mut agreed, mut ambiguous) = (0, 0);
        for explicit in &explicit_hints {
            for associations in &association_sets {
                for rematch in [false, true] {
                    for files in [true, false] {
                        let owned = associations
                            .iter()
                            .map(|content| (*content).clone())
                            .collect::<Vec<_>>();
                        let ambiguity =
                            content_ambiguity(explicit.as_ref(), &owned, !rematch, files);
                        let go_verdicts = permutations(associations)
                            .iter()
                            .map(|order| {
                                render(
                                    go_effective_hint(explicit.as_ref(), order, rematch, files)
                                        .as_ref(),
                                )
                            })
                            .collect::<std::collections::BTreeSet<_>>();
                        let case = format!(
                            "{explicit:?} {associations:?} rematch={rematch} files={files}"
                        );
                        match ambiguity {
                            None => {
                                assert_eq!(go_verdicts.len(), 1, "Go depends on row order: {case}");
                                for order in permutations(associations) {
                                    let owned = order
                                        .iter()
                                        .map(|content| (*content).clone())
                                        .collect::<Vec<_>>();
                                    let rust =
                                        effective_hint(explicit.clone(), &owned, !rematch, files);
                                    assert_eq!(
                                        Some(&render(rust.as_ref())),
                                        go_verdicts.first(),
                                        "{case}"
                                    );
                                }
                                agreed += 1;
                            }
                            Some(_) => {
                                assert!(go_verdicts.len() > 1, "a spurious refusal: {case}");
                                ambiguous += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(agreed + ambiguous, 6 * 11 * 4);
        assert!(ambiguous > 0);
    }

    #[test]
    fn ambiguity_is_reported_by_kind() {
        let movie_603 = tc("a", Some("movie"), Some("tmdb"), Some("603"));
        let movie_604 = tc("b", Some("movie"), Some("tmdb"), Some("604"));
        let xxx = tc("d", Some("xxx"), None, None);
        let comic = tc("e", Some("comic"), None, None);
        let two_movies = [movie_603.clone(), movie_604];
        assert_eq!(
            content_ambiguity(None, &two_movies, true, true),
            Some(ContentAmbiguity::Reuse)
        );
        // Rematch reuses nothing, and a sourced hint is never overwritten.
        assert_eq!(content_ambiguity(None, &two_movies, false, true), None);
        let sourced = hint("movie", Some("imdb"), Some("tt1"));
        assert_eq!(
            content_ambiguity(Some(&sourced), &two_movies, true, true),
            None
        );
        // A typed hint narrows the candidates to its type.
        assert_eq!(
            content_ambiguity(Some(&hint("tv_show", None, None)), &two_movies, true, true),
            None
        );

        let preserved = [xxx, comic];
        assert_eq!(
            content_ambiguity(None, &preserved, true, false),
            Some(ContentAmbiguity::PreservedType)
        );
        // Not consulted with a file list, a hint, or a reuse.
        assert_eq!(content_ambiguity(None, &preserved, true, true), None);
        assert_eq!(
            content_ambiguity(Some(&hint("movie", None, None)), &preserved, true, false),
            None
        );
        let mut with_reuse = preserved.to_vec();
        with_reuse.push(movie_603);
        assert_eq!(content_ambiguity(None, &with_reuse, true, false), None);
        assert_eq!(
            content_ambiguity(None, &with_reuse, false, false),
            Some(ContentAmbiguity::PreservedType)
        );
    }

    #[test]
    fn a_sourced_hint_needs_the_live_resolver_and_a_null_source_one_does_not() {
        assert_eq!(
            hint("movie", Some("imdb"), Some("tt1")).admission(),
            HintAdmission::NeedsLiveResolver
        );
        assert_eq!(
            hint("movie", None, Some("tt1")).admission(),
            HintAdmission::Supported
        );
        assert_eq!(enriched("movie").admission(), HintAdmission::Supported);
    }

    fn stored(content_type: &str) -> StoredHint {
        StoredHint {
            content_type: content_type.into(),
            ..StoredHint::default()
        }
    }

    #[test]
    fn decode_bounds_that_the_fixture_does_not_reach() {
        let episodes = |text: &str| {
            StoredHint {
                episodes: Some(text.into()),
                ..stored("tv_show")
            }
            .decode()
            .map(|hint| hint.attributes.episodes.to_string())
        };
        // The writer persists episodes through their rendering, as u16.
        assert_eq!(
            episodes(r#"{"65535": {"65535": {}}}"#),
            Ok("S65535E65535".into())
        );
        assert_eq!(episodes(r#"{"65536": {}}"#), Err("episodes"));
        assert_eq!(episodes(r#"{"1": {"65536": {}}}"#), Err("episodes"));
        assert_eq!(episodes(r#"{"": {}}"#), Err("episodes"));
        // `[]` would decode as an empty struct in serde, not in Go.
        assert_eq!(episodes(r#"{"1": {"2": []}}"#), Err("episodes"));

        let id = |id: &str| {
            StoredHint {
                content_source: Some("imdb".into()),
                content_id: Some(id.into()),
                ..stored("movie")
            }
            .decode()
            .map(|hint| hint.content_id)
        };
        assert_eq!(id("tt0133093"), Ok(Some("tt0133093".into())));
        assert_eq!(id("1.2_3-4"), Ok(Some("1.2_3-4".into())));
        for refused in [
            "", ".", "..", "-1", "_x", "tt%2F", "tt/1", "tt 1", "tt\u{e9}",
        ] {
            assert_eq!(id(refused), Err("content_id"), "{refused}");
        }
        // The id of a source-less hint is never sent anywhere.
        assert!(StoredHint {
            content_id: Some("tt 1".into()),
            ..stored("movie")
        }
        .decode()
        .is_ok());
    }

    /// Go accepts these; Rust refuses them, so the job is left to Go.
    const RUST_STRICTER_THAN_GO: &[&str] = &[
        "type_upper",
        "sourced_empty_source",
        "sourced_empty_id",
        "sourced_id_space",
        "sourced_id_slash",
        "sourced_id_dotdot",
        "sourced_id_dot_lead",
        "languages_upper",
        "languages_alpha3",
        "languages_name",
        "episodes_season_i64_max",
        "episodes_null_season",
        "episodes_null_episode",
        "episodes_leading_zero",
        "episodes_negative",
        "episodes_plus",
        "episodes_episode_fields",
        r#"video_source_"WOR\u212aPRINT""#,
        r#"video_source_"WEBR\u0130P""#,
    ];

    #[derive(serde::Deserialize)]
    struct DecodeFixture {
        id: String,
        input: DecodeInput,
        expected: serde_json::Value,
    }

    #[derive(serde::Deserialize)]
    struct DecodeInput {
        row: DecodeRow,
    }

    /// Go's row, less `title` and `releaseYear`, which the loader never reads.
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct DecodeRow {
        content_type: String,
        content_source: Option<String>,
        content_id: Option<String>,
        languages: Option<String>,
        episodes: Option<String>,
        video_resolution: Option<String>,
        video_source: Option<String>,
        video_codec: Option<String>,
        #[serde(rename = "video3d")]
        video_3d: Option<String>,
        video_modifier: Option<String>,
        release_group: Option<String>,
    }

    fn decode_fixtures() -> Vec<(String, StoredHint, serde_json::Value)> {
        let text = include_str!(
            "../../../../testdata/parity/torrent-hint-decode/torrent_hint_decode.jsonl"
        );
        text.lines()
            .map(|line| {
                let fixture: DecodeFixture = serde_json::from_str(line).expect("fixture line");
                let row = fixture.input.row;
                let stored = StoredHint {
                    content_type: row.content_type,
                    content_source: row.content_source,
                    content_id: row.content_id,
                    languages: row.languages,
                    episodes: row.episodes,
                    video_resolution: row.video_resolution,
                    video_source: row.video_source,
                    video_codec: row.video_codec,
                    video_3d: row.video_3d,
                    video_modifier: row.video_modifier,
                    release_group: row.release_group,
                };
                (fixture.id, stored, fixture.expected)
            })
            .collect()
    }

    /// Go's GORM scan and `ApplyHint` over every fixture row
    /// (`internal/parity/torrent_hint_decode_pg_gen_test.go`): where Go fails
    /// the load, Rust refuses; where Go succeeds, Rust classifies and persists
    /// exactly what Go does, or the case is on the reviewed list of refusals.
    #[test]
    fn decode_matches_go_scan_and_apply_hint() {
        let fixtures = decode_fixtures();
        assert!(fixtures.len() >= 140, "{}", fixtures.len());
        let mut stricter = Vec::new();
        for (id, stored, expected) in fixtures {
            let go_failed = expected.get("loadError").is_some();
            let decoded = stored.clone().decode();
            if go_failed {
                assert!(
                    decoded.is_err(),
                    "{id}: Go fails the load, Rust took {decoded:?}"
                );
                continue;
            }
            if RUST_STRICTER_THAN_GO.contains(&id.as_str()) {
                assert!(
                    decoded.is_err(),
                    "{id}: listed as refused, Rust took {decoded:?}"
                );
                stricter.push(id);
                continue;
            }
            let hint =
                decoded.unwrap_or_else(|column| panic!("{id}: Go loads it, Rust refused {column}"));

            assert_eq!(
                serde_json::to_value(&hint.content_source).unwrap(),
                expected
                    .get("contentSource")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                "{id}: content source"
            );
            assert_eq!(
                serde_json::to_value(&hint.content_id).unwrap(),
                expected
                    .get("contentId")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                "{id}: content id"
            );
            let admission = hint.admission();
            let input = hint.into_input().expect("a typed hint");
            assert_eq!(
                !input.content_type.is_empty() && !input.content_source.is_empty(),
                expected["hasHintedContentId"] == serde_json::Value::Bool(true),
                "{id}: hasHintedContentId"
            );
            assert_eq!(
                admission == HintAdmission::NeedsLiveResolver,
                expected["hasHintedContentId"] == serde_json::Value::Bool(true),
                "{id}: admission"
            );

            let mut applied = Classification::default();
            applied.apply_hint(&input);
            assert_eq!(
                serde_json::to_value(NormalizedClassifierResult::from_result(
                    &applied,
                    &Outcome::Classified
                ))
                .unwrap(),
                expected["applied"],
                "{id}: applied"
            );
            // As the writer persists them: an allocated empty map is `{}`.
            let persisted = if applied.episodes.is_empty() && !applied.episodes.is_nil() {
                serde_json::json!({})
            } else {
                crate::persist::episodes_json(&applied.episodes.to_string())
                    .expect("persistable episodes")
                    .unwrap_or(serde_json::Value::Null)
            };
            let go_persisted: serde_json::Value = serde_json::from_str(
                expected["appliedEpisodesJson"]
                    .as_str()
                    .expect("episodes json"),
            )
            .unwrap();
            assert_eq!(persisted, go_persisted, "{id}: persisted episodes");
        }
        stricter.sort();
        let mut listed = RUST_STRICTER_THAN_GO
            .iter()
            .map(|id| (*id).to_owned())
            .collect::<Vec<_>>();
        listed.sort();
        assert_eq!(stricter, listed, "every listed refusal is a row Go accepts");
    }

    #[test]
    fn rematch_preserves_explicit_hint_but_skips_content_reuse() {
        let existing = vec![tc("tc", Some("movie"), Some("tmdb"), Some("603"))];
        let explicit = hint("tv_show", None, None);

        let hint = effective_hint(Some(explicit), &existing, false, true).expect("explicit hint");
        assert_eq!(hint.content_type, "tv_show");
        assert!(hint.content_source.is_empty());
        assert!(effective_hint(None, &existing, false, true).is_none());
    }

    /// The `xxx` data-loss regression: a source-less (rule-derived) content type
    /// on a torrent with no stored file list must survive a reprocess, because
    /// the rules that would re-derive it cannot fire without files.
    fn source_less_xxx() -> Vec<CurrentContent> {
        vec![CurrentContent {
            id: "tc".into(),
            content_type: Some("xxx".into()),
            content_source: None,
            content_id: None,
        }]
    }

    #[test]
    fn source_less_type_is_preserved_when_no_stored_file_list() {
        let existing = source_less_xxx();
        for allow_content_reuse in [true, false] {
            let hint = effective_hint(None, &existing, allow_content_reuse, false)
                .expect("source-less xxx must be preserved when files are unavailable");
            assert_eq!(hint.content_type, "xxx");
            // Type only — never resurrect a content match.
            assert!(hint.content_source.is_empty());
            assert!(hint.content_id.is_empty());
        }
    }

    #[test]
    fn source_less_type_is_not_preserved_when_files_are_available() {
        // Files present ⇒ the rules CAN evaluate, so a not-xxx verdict is a
        // legitimate reclassification and must still be allowed through.
        let existing = source_less_xxx();
        assert!(effective_hint(None, &existing, true, true).is_none());
        assert!(effective_hint(None, &existing, false, true).is_none());
    }

    #[test]
    fn source_backed_reuse_still_wins_over_the_preservation_fallback() {
        let existing = vec![
            CurrentContent {
                id: "a".into(),
                content_type: Some("movie".into()),
                content_source: Some("tmdb".into()),
                content_id: Some("603".into()),
            },
            CurrentContent {
                id: "b".into(),
                content_type: Some("xxx".into()),
                content_source: None,
                content_id: None,
            },
        ];
        let hint = effective_hint(None, &existing, true, false).expect("sourced reuse");
        assert_eq!(hint.content_type, "movie");
        assert_eq!(hint.content_source, "tmdb");
    }

    #[test]
    fn has_stored_file_list_matches_go_enum() {
        assert!(!has_stored_file_list("no_info"));
        assert!(!has_stored_file_list("over_threshold"));
        assert!(has_stored_file_list("single"));
        assert!(has_stored_file_list("multi"));
    }
}
