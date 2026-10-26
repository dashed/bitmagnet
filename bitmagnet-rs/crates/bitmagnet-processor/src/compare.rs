//! Pure comparison of a materialized processor write-set with settled live rows.
//!
//! Both sides are projected onto the stable fields frozen by contract §5.2(c).
//! The result uses a closed set of drift labels so callers can safely expose
//! counters without turning row values into metric-label cardinality.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    validate_info_hash, ContentWrite, LiveSnapshot, LiveTorrentSnapshot, LiveTorrentState,
    MaterializeError, WriteSet,
};

/// The comparison result for a complete materialized write-set.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowComparison {
    pub torrents: Vec<TorrentComparison>,
}

impl ShadowComparison {
    /// Number of hashes whose stable image matches the settled live image.
    pub fn match_count(&self) -> usize {
        self.torrents
            .iter()
            .filter(|comparison| comparison.verdict == ComparisonVerdict::Match)
            .count()
    }

    /// Number of hashes with at least one stable-field drift.
    pub fn mismatch_count(&self) -> usize {
        self.torrents.len() - self.match_count()
    }

    /// True when every comparable hash matches.
    pub fn is_match(&self) -> bool {
        self.mismatch_count() == 0
    }
}

/// A bounded-cardinality result suitable for match/mismatch and drift metrics.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentComparison {
    pub info_hash: String,
    pub content_type: Option<String>,
    pub verdict: ComparisonVerdict,
    pub drift_fields: Vec<DriftField>,
}

/// Whether one torrent's complete stable image matches.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonVerdict {
    Match,
    Mismatch,
}

/// Stable, bounded labels for the fields in the §5.2(c) comparison image.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftField {
    DeleteSignal,
    ContentRows,
    ContentType,
    ContentSource,
    ContentId,
    ContentTitle,
    ContentReleaseYear,
    ContentIdentifiers,
    TorrentContentRows,
    TorrentContentInferId,
    TorrentContentType,
    TorrentContentSource,
    TorrentContentId,
    TorrentContentLanguages,
    TorrentContentEpisodes,
    TorrentContentVideoResolution,
    TorrentContentVideoSource,
    TorrentContentVideoCodec,
    TorrentContentVideo3d,
    TorrentContentVideoModifier,
    TorrentContentReleaseGroup,
    TorrentContentSize,
    TorrentContentFilesCount,
    TorrentTags,
}

impl DriftField {
    /// Low-cardinality label value for `bitmagnet_ingest_shadow_*` metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeleteSignal => "delete_signal",
            Self::ContentRows => "content.rows",
            Self::ContentType => "content.type",
            Self::ContentSource => "content.source",
            Self::ContentId => "content.id",
            Self::ContentTitle => "content.title",
            Self::ContentReleaseYear => "content.release_year",
            Self::ContentIdentifiers => "content.identifiers",
            Self::TorrentContentRows => "torrent_content.rows",
            Self::TorrentContentInferId => "torrent_content.infer_id",
            Self::TorrentContentType => "torrent_content.content_type",
            Self::TorrentContentSource => "torrent_content.content_source",
            Self::TorrentContentId => "torrent_content.content_id",
            Self::TorrentContentLanguages => "torrent_content.languages",
            Self::TorrentContentEpisodes => "torrent_content.episodes",
            Self::TorrentContentVideoResolution => "torrent_content.video_resolution",
            Self::TorrentContentVideoSource => "torrent_content.video_source",
            Self::TorrentContentVideoCodec => "torrent_content.video_codec",
            Self::TorrentContentVideo3d => "torrent_content.video_3d",
            Self::TorrentContentVideoModifier => "torrent_content.video_modifier",
            Self::TorrentContentReleaseGroup => "torrent_content.release_group",
            Self::TorrentContentSize => "torrent_content.size",
            Self::TorrentContentFilesCount => "torrent_content.files_count",
            Self::TorrentTags => "torrent_tags",
        }
    }
}

/// Invalid or incomplete inputs that cannot produce a trustworthy comparison.
#[derive(Debug, thiserror::Error)]
pub enum CompareError {
    #[error(transparent)]
    InvalidInfoHash(#[from] MaterializeError),
    #[error("failed hash '{0}' has no materialized outcome to compare")]
    FailedHash(String),
    #[error("hash '{0}' has both a delete signal and a present write image")]
    ConflictingOutcome(String),
    #[error("materialized hash '{0}' is absent from the live snapshot")]
    MissingLiveState(String),
    #[error("live snapshot hash '{0}' has no materialized outcome")]
    MissingWriteOutcome(String),
    #[error(
        "content row {content_type}/{content_source}/{id} is not referenced by any torrent write"
    )]
    OrphanContent {
        content_type: String,
        content_source: String,
        id: String,
    },
}

/// Compare a canonical materialized write-set against the settled live rows.
///
/// Failed hashes are rejected because retryable classifier/loader failures have
/// no would-be persisted image. Every successful materialized hash must have an
/// explicit live state, including [`LiveTorrentState::LiveAbsent`].
pub fn compare_write_set(
    write_set: &WriteSet,
    live: &LiveSnapshot,
) -> Result<ShadowComparison, CompareError> {
    let failed = write_set
        .failed_info_hashes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if let Some(info_hash) = failed.first() {
        validate_info_hash(info_hash)?;
        return Err(CompareError::FailedHash(info_hash.clone()));
    }

    let mut expected = BTreeMap::<String, LiveTorrentSnapshot>::new();
    for row in &write_set.torrent_contents {
        validate_info_hash(&row.info_hash)?;
        expected
            .entry(row.info_hash.clone())
            .or_default()
            .torrent_contents
            .push(row.clone());
    }
    for (info_hash, tags) in &write_set.add_tags {
        validate_info_hash(info_hash)?;
        expected
            .entry(info_hash.clone())
            .or_default()
            .tags
            .extend(tags.iter().cloned());
    }

    let deleted = write_set
        .delete_info_hashes
        .iter()
        .map(|info_hash| {
            validate_info_hash(info_hash)?;
            Ok(info_hash.clone())
        })
        .collect::<Result<BTreeSet<_>, MaterializeError>>()?;
    if let Some(info_hash) = deleted
        .iter()
        .find(|info_hash| expected.contains_key(*info_hash))
    {
        return Err(CompareError::ConflictingOutcome(info_hash.clone()));
    }

    attach_expected_contents(&mut expected, &write_set.contents)?;

    let expected_hashes = expected
        .keys()
        .chain(deleted.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    for info_hash in &expected_hashes {
        if !live.contains_key(info_hash) {
            return Err(CompareError::MissingLiveState(info_hash.clone()));
        }
    }
    if let Some(info_hash) = live
        .keys()
        .find(|info_hash| !expected_hashes.contains(*info_hash))
    {
        validate_info_hash(info_hash)?;
        return Err(CompareError::MissingWriteOutcome(info_hash.clone()));
    }

    let mut torrents = Vec::with_capacity(expected_hashes.len());
    for info_hash in expected_hashes {
        let live_state = &live[&info_hash];
        let expected_state = if deleted.contains(&info_hash) {
            LiveTorrentState::LiveAbsent
        } else {
            LiveTorrentState::Present(
                expected
                    .remove(&info_hash)
                    .expect("expected hash was assembled above"),
            )
        };
        torrents.push(compare_torrent(
            info_hash,
            expected_state,
            live_state.clone(),
        ));
    }
    Ok(ShadowComparison { torrents })
}

fn attach_expected_contents(
    expected: &mut BTreeMap<String, LiveTorrentSnapshot>,
    contents: &[ContentWrite],
) -> Result<(), CompareError> {
    for content in contents {
        let mut referenced = false;
        for snapshot in expected.values_mut() {
            if snapshot.torrent_contents.iter().any(|row| {
                row.content_type.as_deref() == Some(&content.content_type)
                    && row.content_source.as_deref() == Some(&content.source)
                    && row.content_id.as_deref() == Some(&content.id)
            }) {
                snapshot.contents.push(content.clone());
                referenced = true;
            }
        }
        if !referenced {
            return Err(CompareError::OrphanContent {
                content_type: content.content_type.clone(),
                content_source: content.source.clone(),
                id: content.id.clone(),
            });
        }
    }
    Ok(())
}

fn compare_torrent(
    info_hash: String,
    expected: LiveTorrentState,
    actual: LiveTorrentState,
) -> TorrentComparison {
    let content_type = content_type_label(&expected).or_else(|| content_type_label(&actual));
    let mut drift = BTreeSet::new();
    match (expected, actual) {
        (LiveTorrentState::LiveAbsent, LiveTorrentState::LiveAbsent) => {}
        (LiveTorrentState::LiveAbsent, LiveTorrentState::Present(_))
        | (LiveTorrentState::Present(_), LiveTorrentState::LiveAbsent) => {
            drift.insert(DriftField::DeleteSignal);
        }
        (LiveTorrentState::Present(mut expected), LiveTorrentState::Present(mut actual)) => {
            canonicalize_snapshot(&mut expected);
            canonicalize_snapshot(&mut actual);
            compare_present(&expected, &actual, &mut drift);
        }
    }
    TorrentComparison {
        info_hash,
        content_type,
        verdict: if drift.is_empty() {
            ComparisonVerdict::Match
        } else {
            ComparisonVerdict::Mismatch
        },
        drift_fields: drift.into_iter().collect(),
    }
}

fn content_type_label(state: &LiveTorrentState) -> Option<String> {
    let LiveTorrentState::Present(snapshot) = state else {
        return None;
    };
    snapshot
        .torrent_contents
        .iter()
        .find_map(|row| row.content_type.clone())
        .or_else(|| {
            snapshot
                .contents
                .first()
                .map(|row| row.content_type.clone())
        })
}

fn canonicalize_snapshot(snapshot: &mut LiveTorrentSnapshot) {
    snapshot.contents.sort();
    for row in &mut snapshot.torrent_contents {
        row.languages.sort();
        row.languages.dedup();
    }
    snapshot
        .torrent_contents
        .sort_by(|left, right| (&left.info_hash, &left.id).cmp(&(&right.info_hash, &right.id)));
    snapshot.tags.sort();
    snapshot.tags.dedup();
}

fn compare_present(
    expected: &LiveTorrentSnapshot,
    actual: &LiveTorrentSnapshot,
    drift: &mut BTreeSet<DriftField>,
) {
    if expected.contents != actual.contents {
        drift.insert(DriftField::ContentRows);
    }
    compare_projection(
        &expected.contents,
        &actual.contents,
        |row| row.content_type.clone(),
        DriftField::ContentType,
        drift,
    );
    compare_projection(
        &expected.contents,
        &actual.contents,
        |row| row.source.clone(),
        DriftField::ContentSource,
        drift,
    );
    compare_projection(
        &expected.contents,
        &actual.contents,
        |row| row.id.clone(),
        DriftField::ContentId,
        drift,
    );
    compare_projection(
        &expected.contents,
        &actual.contents,
        |row| row.title.clone(),
        DriftField::ContentTitle,
        drift,
    );
    compare_projection(
        &expected.contents,
        &actual.contents,
        |row| row.release_year,
        DriftField::ContentReleaseYear,
        drift,
    );
    compare_projection(
        &expected.contents,
        &actual.contents,
        |row| row.identifiers.clone(),
        DriftField::ContentIdentifiers,
        drift,
    );

    if expected.torrent_contents != actual.torrent_contents {
        drift.insert(DriftField::TorrentContentRows);
    }
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.id.clone(),
        DriftField::TorrentContentInferId,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.content_type.clone(),
        DriftField::TorrentContentType,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.content_source.clone(),
        DriftField::TorrentContentSource,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.content_id.clone(),
        DriftField::TorrentContentId,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.languages.clone(),
        DriftField::TorrentContentLanguages,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.episodes.clone(),
        DriftField::TorrentContentEpisodes,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.video_resolution.clone(),
        DriftField::TorrentContentVideoResolution,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.video_source.clone(),
        DriftField::TorrentContentVideoSource,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.video_codec.clone(),
        DriftField::TorrentContentVideoCodec,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.video_3d.clone(),
        DriftField::TorrentContentVideo3d,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.video_modifier.clone(),
        DriftField::TorrentContentVideoModifier,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.release_group.clone(),
        DriftField::TorrentContentReleaseGroup,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.size,
        DriftField::TorrentContentSize,
        drift,
    );
    compare_projection(
        &expected.torrent_contents,
        &actual.torrent_contents,
        |row| row.files_count,
        DriftField::TorrentContentFilesCount,
        drift,
    );
    // Go's processor only inserts tags with ON CONFLICT DO NOTHING; it does
    // not replace or delete the torrent's pre-existing tag set. Therefore the
    // job's additions must be present, while unrelated live tags are valid.
    if expected
        .tags
        .iter()
        .any(|tag| actual.tags.binary_search(tag).is_err())
    {
        drift.insert(DriftField::TorrentTags);
    }
}

fn compare_projection<Row, Value, Project>(
    expected: &[Row],
    actual: &[Row],
    project: Project,
    field: DriftField,
    drift: &mut BTreeSet<DriftField>,
) where
    Value: Ord,
    Project: Fn(&Row) -> Value,
{
    let mut expected = expected.iter().map(&project).collect::<Vec<_>>();
    let mut actual = actual.iter().map(project).collect::<Vec<_>>();
    expected.sort();
    actual.sort();
    if expected != actual {
        drift.insert(field);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        compare_write_set, CompareError, ComparisonVerdict, DriftField, LiveSnapshot,
        LiveTorrentSnapshot, LiveTorrentState,
    };
    use crate::{ContentWrite, TorrentContentWrite, WriteSet};

    const HASH_A: &str = "1111111111111111111111111111111111111111";
    const HASH_B: &str = "2222222222222222222222222222222222222222";

    #[test]
    fn canonical_order_and_sets_match() {
        let row = torrent_content(HASH_A);
        let mut write_set = WriteSet {
            torrent_contents: vec![row.clone()],
            add_tags: BTreeMap::from([(
                HASH_A.to_owned(),
                vec!["z".to_owned(), "a".to_owned(), "a".to_owned()],
            )]),
            ..WriteSet::default()
        };
        write_set.torrent_contents[0].languages = vec!["ja".into(), "en".into(), "en".into()];

        let mut live_row = row;
        live_row.languages = vec!["en".into(), "ja".into()];
        let live = LiveSnapshot::from([(
            HASH_A.to_owned(),
            LiveTorrentState::Present(LiveTorrentSnapshot {
                torrent_contents: vec![live_row],
                tags: vec!["a".into(), "pre-existing".into(), "z".into()],
                ..LiveTorrentSnapshot::default()
            }),
        )]);

        let comparison = compare_write_set(&write_set, &live).unwrap();
        assert!(comparison.is_match());
        assert_eq!(comparison.match_count(), 1);
        assert_eq!(comparison.mismatch_count(), 0);
        assert_eq!(
            comparison.torrents[0].content_type.as_deref(),
            Some("movie")
        );
    }

    #[test]
    fn delete_signal_compares_against_live_absence() {
        let write_set = WriteSet {
            delete_info_hashes: vec![HASH_A.to_owned(), HASH_B.to_owned()],
            ..WriteSet::default()
        };
        let live = LiveSnapshot::from([
            (HASH_A.to_owned(), LiveTorrentState::LiveAbsent),
            (
                HASH_B.to_owned(),
                LiveTorrentState::Present(LiveTorrentSnapshot::default()),
            ),
        ]);

        let comparison = compare_write_set(&write_set, &live).unwrap();
        assert_eq!(comparison.match_count(), 1);
        assert_eq!(comparison.mismatch_count(), 1);
        assert_eq!(
            comparison.torrents[1],
            super::TorrentComparison {
                info_hash: HASH_B.to_owned(),
                content_type: None,
                verdict: ComparisonVerdict::Mismatch,
                drift_fields: vec![DriftField::DeleteSignal],
            }
        );
    }

    #[test]
    fn present_write_against_live_absence_is_delete_drift() {
        let write_set = WriteSet {
            torrent_contents: vec![torrent_content(HASH_A)],
            ..WriteSet::default()
        };
        let live = LiveSnapshot::from([(HASH_A.to_owned(), LiveTorrentState::LiveAbsent)]);

        let comparison = compare_write_set(&write_set, &live).unwrap();
        assert_eq!(
            comparison.torrents[0].drift_fields,
            [DriftField::DeleteSignal]
        );
    }

    #[test]
    fn reports_stable_field_drift_with_bounded_labels() {
        let content = ContentWrite {
            content_type: "movie".into(),
            source: "tmdb".into(),
            id: "42".into(),
            title: "Expected".into(),
            release_year: Some(2024),
            identifiers: BTreeMap::from([("imdb".into(), "tt42".into())]),
        };
        let mut expected_row = torrent_content(HASH_A);
        expected_row.content_source = Some("tmdb".into());
        expected_row.content_id = Some("42".into());
        let write_set = WriteSet {
            contents: vec![content.clone()],
            torrent_contents: vec![expected_row.clone()],
            add_tags: BTreeMap::from([(HASH_A.to_owned(), vec!["action".into()])]),
            ..WriteSet::default()
        };

        let mut actual_content = content;
        actual_content.title = "Actual".into();
        let mut actual_row = expected_row;
        actual_row.languages = vec!["fr".into()];
        actual_row.size += 1;
        let live = LiveSnapshot::from([(
            HASH_A.to_owned(),
            LiveTorrentState::Present(LiveTorrentSnapshot {
                contents: vec![actual_content],
                torrent_contents: vec![actual_row],
                tags: vec!["drama".into()],
            }),
        )]);

        let comparison = compare_write_set(&write_set, &live).unwrap();
        let torrent = &comparison.torrents[0];
        assert_eq!(torrent.verdict, ComparisonVerdict::Mismatch);
        assert_eq!(
            torrent.drift_fields,
            [
                DriftField::ContentRows,
                DriftField::ContentTitle,
                DriftField::TorrentContentRows,
                DriftField::TorrentContentLanguages,
                DriftField::TorrentContentSize,
                DriftField::TorrentTags,
            ]
        );
        assert_eq!(
            DriftField::TorrentContentSize.as_str(),
            "torrent_content.size"
        );
    }

    #[test]
    fn rejects_inputs_without_a_comparable_outcome() {
        let failed = WriteSet {
            failed_info_hashes: vec![HASH_A.to_owned()],
            ..WriteSet::default()
        };
        assert!(matches!(
            compare_write_set(&failed, &LiveSnapshot::new()),
            Err(CompareError::FailedHash(hash)) if hash == HASH_A
        ));

        let conflicting = WriteSet {
            torrent_contents: vec![torrent_content(HASH_A)],
            delete_info_hashes: vec![HASH_A.to_owned()],
            ..WriteSet::default()
        };
        let live = LiveSnapshot::from([(HASH_A.to_owned(), LiveTorrentState::LiveAbsent)]);
        assert!(matches!(
            compare_write_set(&conflicting, &live),
            Err(CompareError::ConflictingOutcome(hash)) if hash == HASH_A
        ));
    }

    #[test]
    fn rejects_orphan_content_and_incomplete_snapshot() {
        let orphan = WriteSet {
            contents: vec![ContentWrite {
                content_type: "movie".into(),
                source: "tmdb".into(),
                id: "42".into(),
                title: "Title".into(),
                release_year: None,
                identifiers: BTreeMap::new(),
            }],
            ..WriteSet::default()
        };
        assert!(matches!(
            compare_write_set(&orphan, &LiveSnapshot::new()),
            Err(CompareError::OrphanContent { .. })
        ));

        let missing = WriteSet {
            torrent_contents: vec![torrent_content(HASH_A)],
            ..WriteSet::default()
        };
        assert!(matches!(
            compare_write_set(&missing, &LiveSnapshot::new()),
            Err(CompareError::MissingLiveState(hash)) if hash == HASH_A
        ));
    }

    fn torrent_content(info_hash: &str) -> TorrentContentWrite {
        TorrentContentWrite {
            id: format!("{info_hash}:movie:?:?"),
            info_hash: info_hash.to_owned(),
            content_type: Some("movie".into()),
            content_source: None,
            content_id: None,
            languages: vec!["en".into()],
            episodes: String::new(),
            video_resolution: Some("1080p".into()),
            video_source: Some("web".into()),
            video_codec: Some("h264".into()),
            video_3d: None,
            video_modifier: None,
            release_group: Some("group".into()),
            size: 123,
            files_count: Some(1),
        }
    }
}
