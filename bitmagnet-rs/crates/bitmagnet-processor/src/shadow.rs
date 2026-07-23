//! Non-locking live-row projection for the dark processor shadow.
//!
//! The reader uses plain `SELECT`s over the four live tables allowed by the
//! frozen shadow-role contract. Missing torrents are represented explicitly so
//! Go's delete outcome is comparable instead of being treated as a read error.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row};

use super::{validate_info_hash, ContentWrite, TorrentContentWrite};

/// Canonical live image keyed by lowercase hexadecimal info hash.
pub type LiveSnapshot = BTreeMap<String, LiveTorrentState>;

/// A settled live-row outcome for one requested info hash.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", content = "snapshot", rename_all = "snake_case")]
pub enum LiveTorrentState {
    LiveAbsent,
    Present(LiveTorrentSnapshot),
}

/// Stable projection of the rows written by the Go processor.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveTorrentSnapshot {
    pub contents: Vec<ContentWrite>,
    pub torrent_contents: Vec<TorrentContentWrite>,
    pub tags: Vec<String>,
}

/// Read the current live image without row locks.
///
/// The queries are intentionally plain `SELECT`s; there is no transaction,
/// `FOR UPDATE`, or mutation. `content.identifiers` remains empty because the
/// normalized M1 image does not expose attached content and the frozen shadow
/// role has no `content_attributes` grant.
pub async fn read_live_snapshot(
    pool: &PgPool,
    info_hashes: &[String],
) -> Result<LiveSnapshot, ShadowReadError> {
    let mut snapshot = LiveSnapshot::new();
    let mut decoded = Vec::new();
    for info_hash in info_hashes {
        validate_info_hash(info_hash)?;
        if snapshot
            .insert(info_hash.clone(), LiveTorrentState::LiveAbsent)
            .is_none()
        {
            decoded.push(hex::decode(info_hash).expect("validated hexadecimal info hash"));
        }
    }
    if decoded.is_empty() {
        return Ok(snapshot);
    }

    let present = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash \
         FROM torrents WHERE info_hash = ANY($1::bytea[])",
    )
    .bind(&decoded)
    .fetch_all(pool)
    .await?;
    for row in present {
        let info_hash = row.try_get::<String, _>("info_hash")?;
        snapshot.insert(
            info_hash,
            LiveTorrentState::Present(LiveTorrentSnapshot::default()),
        );
    }

    let content_rows = sqlx::query(
        "SELECT tc.id, encode(tc.info_hash, 'hex') AS info_hash, \
         tc.content_type, tc.content_source, tc.content_id, \
         COALESCE(tc.languages, '[]'::jsonb) AS languages, \
         COALESCE(tc.episodes, '{}'::jsonb) AS episodes, \
         tc.video_resolution, tc.video_source, tc.video_codec, tc.video_3d, \
         tc.video_modifier, tc.release_group, tc.size, tc.files_count, \
         c.title AS attached_title, c.release_year AS attached_release_year \
         FROM torrent_contents tc \
         LEFT JOIN content c ON tc.content_type = c.type \
           AND tc.content_source = c.source AND tc.content_id = c.id \
         WHERE tc.info_hash = ANY($1::bytea[]) \
         ORDER BY tc.info_hash, tc.id",
    )
    .bind(&decoded)
    .fetch_all(pool)
    .await?;

    for row in content_rows {
        let info_hash = row.try_get::<String, _>("info_hash")?;
        let state = present_state_mut(&mut snapshot, &info_hash)?;
        let content_type = row.try_get::<Option<String>, _>("content_type")?;
        let content_source = row.try_get::<Option<String>, _>("content_source")?;
        let content_id = row.try_get::<Option<String>, _>("content_id")?;
        let mut languages = row
            .try_get::<sqlx::types::Json<Vec<String>>, _>("languages")?
            .0;
        languages.sort();
        languages.dedup();
        let episodes = row.try_get::<sqlx::types::Json<Value>, _>("episodes")?.0;
        let size = nonnegative_u64("size", row.try_get::<i64, _>("size")?)?;
        let files_count = row
            .try_get::<Option<i32>, _>("files_count")?
            .map(|value| nonnegative_u64("files_count", i64::from(value)))
            .transpose()?;

        state.torrent_contents.push(TorrentContentWrite {
            id: row.try_get("id")?,
            info_hash: info_hash.clone(),
            content_type: content_type.clone(),
            content_source: content_source.clone(),
            content_id: content_id.clone(),
            languages,
            episodes: episodes_string(&episodes)?,
            video_resolution: row.try_get("video_resolution")?,
            video_source: row.try_get("video_source")?,
            video_codec: row.try_get("video_codec")?,
            video_3d: row.try_get("video_3d")?,
            video_modifier: row.try_get("video_modifier")?,
            release_group: row.try_get("release_group")?,
            size,
            files_count,
        });

        if let (Some(content_type), Some(source), Some(id)) =
            (content_type, content_source, content_id)
        {
            let content = ContentWrite {
                content_type,
                source,
                id,
                title: row
                    .try_get::<Option<String>, _>("attached_title")?
                    .ok_or_else(|| ShadowReadError::MissingAttachedContent {
                        info_hash: info_hash.clone(),
                    })?,
                release_year: row
                    .try_get::<Option<i32>, _>("attached_release_year")?
                    .map(|year| {
                        u16::try_from(year).map_err(|_| ShadowReadError::InvalidReleaseYear(year))
                    })
                    .transpose()?,
                identifiers: BTreeMap::new(),
            };
            if !state.contents.contains(&content) {
                state.contents.push(content);
            }
        }
    }

    let tags = sqlx::query(
        "SELECT encode(info_hash, 'hex') AS info_hash, name \
         FROM torrent_tags WHERE info_hash = ANY($1::bytea[]) \
         ORDER BY info_hash, name",
    )
    .bind(&decoded)
    .fetch_all(pool)
    .await?;
    for row in tags {
        let info_hash = row.try_get::<String, _>("info_hash")?;
        present_state_mut(&mut snapshot, &info_hash)?
            .tags
            .push(row.try_get("name")?);
    }

    for state in snapshot.values_mut() {
        if let LiveTorrentState::Present(state) = state {
            state.contents.sort();
            state.torrent_contents.sort_by(|left, right| {
                (&left.info_hash, &left.id).cmp(&(&right.info_hash, &right.id))
            });
            state.tags.sort();
            state.tags.dedup();
        }
    }

    Ok(snapshot)
}

#[derive(Debug, thiserror::Error)]
pub enum ShadowReadError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    InvalidInfoHash(#[from] super::MaterializeError),
    #[error("live torrent '{0}' disappeared between shadow SELECTs")]
    ChangedDuringRead(String),
    #[error("live torrent '{info_hash}' references a missing content row")]
    MissingAttachedContent { info_hash: String },
    #[error("invalid episodes JSONB image: {0}")]
    InvalidEpisodes(Value),
    #[error("negative {field} value in live row: {value}")]
    NegativeInteger { field: &'static str, value: i64 },
    #[error("invalid attached-content release year: {0}")]
    InvalidReleaseYear(i32),
}

fn present_state_mut<'a>(
    snapshot: &'a mut LiveSnapshot,
    info_hash: &str,
) -> Result<&'a mut LiveTorrentSnapshot, ShadowReadError> {
    match snapshot.get_mut(info_hash) {
        Some(LiveTorrentState::Present(state)) => Ok(state),
        _ => Err(ShadowReadError::ChangedDuringRead(info_hash.to_owned())),
    }
}

fn nonnegative_u64(field: &'static str, value: i64) -> Result<u64, ShadowReadError> {
    u64::try_from(value).map_err(|_| ShadowReadError::NegativeInteger { field, value })
}

fn episodes_string(value: &Value) -> Result<String, ShadowReadError> {
    if value.is_null() {
        return Ok(String::new());
    }
    let object = value
        .as_object()
        .ok_or_else(|| ShadowReadError::InvalidEpisodes(value.clone()))?;
    let mut seasons = BTreeMap::<i64, BTreeSet<i64>>::new();
    for (season, episodes) in object {
        let season = parse_json_number(season, value)?;
        let episodes = episodes
            .as_object()
            .ok_or_else(|| ShadowReadError::InvalidEpisodes(value.clone()))?;
        let mut episode_set = BTreeSet::new();
        for episode in episodes.keys() {
            episode_set.insert(parse_json_number(episode, value)?);
        }
        seasons.insert(season, episode_set);
    }

    let whole_seasons = seasons
        .iter()
        .filter_map(|(season, episodes)| episodes.is_empty().then_some(*season))
        .collect::<Vec<_>>();
    let mut parts = BTreeMap::<i64, String>::new();
    for (start, end) in contiguous_ranges(&whole_seasons) {
        parts.insert(start, format!("S{}", format_range(start, end)));
    }
    for (season, episodes) in seasons {
        if episodes.is_empty() {
            continue;
        }
        let episode_values = episodes.into_iter().collect::<Vec<_>>();
        let episode_parts = contiguous_ranges(&episode_values)
            .into_iter()
            .map(|(start, end)| format!("E{}", format_range(start, end)))
            .collect::<Vec<_>>()
            .join(",");
        parts.insert(season, format!("S{season:02}{episode_parts}"));
    }
    Ok(parts.into_values().collect::<Vec<_>>().join(", "))
}

fn parse_json_number(raw: &str, image: &Value) -> Result<i64, ShadowReadError> {
    raw.parse()
        .map_err(|_| ShadowReadError::InvalidEpisodes(image.clone()))
}

fn contiguous_ranges(values: &[i64]) -> Vec<(i64, i64)> {
    let Some(&first) = values.first() else {
        return Vec::new();
    };
    let mut ranges = Vec::new();
    let mut start = first;
    let mut end = first;
    for &value in &values[1..] {
        if value == end + 1 {
            end = value;
        } else {
            ranges.push((start, end));
            start = value;
            end = value;
        }
    }
    ranges.push((start, end));
    ranges
}

fn format_range(start: i64, end: i64) -> String {
    if start == end {
        format!("{start:02}")
    } else {
        format!("{start:02}-{end:02}")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{episodes_string, LiveTorrentSnapshot, LiveTorrentState};

    #[test]
    fn episodes_jsonb_converts_to_go_string_shape() {
        assert_eq!(episodes_string(&json!(null)).unwrap(), "");
        assert_eq!(episodes_string(&json!({})).unwrap(), "");
        assert_eq!(episodes_string(&json!({"1": {}})).unwrap(), "S01");
        assert_eq!(
            episodes_string(&json!({"1": {}, "2": {}, "3": {}})).unwrap(),
            "S01-03"
        );
        assert_eq!(
            episodes_string(&json!({
                "1": {"1": {}, "2": {}, "3": {}, "5": {}},
                "2": {"7": {}}
            }))
            .unwrap(),
            "S01E01-03,E05, S02E07"
        );
    }

    #[test]
    fn live_absent_is_a_first_class_serialized_outcome() {
        assert_eq!(
            serde_json::to_value(LiveTorrentState::LiveAbsent).unwrap(),
            json!({"outcome": "live_absent"})
        );
        assert_eq!(
            serde_json::to_value(LiveTorrentState::Present(LiveTorrentSnapshot::default()))
                .unwrap(),
            json!({
                "outcome": "present",
                "snapshot": {"contents": [], "torrentContents": [], "tags": []}
            })
        );
    }
}
