//! Read-only comparison of a writer plan's volatile persistence image.
//!
//! The stable comparator owns classification rows, deletes, tags, and stale or
//! unexpected live rows. This comparator considers only the exact
//! `torrent_contents.id` keys in [`crate::WriterPlan::persistence`] and compares
//! the volatile fields that [`crate::persist_write_set`] would upsert.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Row};

use crate::{ComparisonVerdict, TorrentContentPersistence};

const MAX_WRITER_COMPARISON_ROWS: usize = 100;

const WRITER_LIVE_SQL: &str = "\
WITH expected(id, tsv) AS ( \
  SELECT * FROM unnest($1::text[], $2::text[]) \
) \
SELECT expected.id, \
       tc.id IS NOT NULL AS present, \
       tc.seeders, tc.leechers, \
       (EXTRACT(EPOCH FROM tc.published_at) * 1000000)::bigint \
         AS published_at_micros, \
       CASE WHEN tc.id IS NULL THEN NULL \
            ELSE tc.tsv = expected.tsv::tsvector END AS tsv_matches \
FROM expected \
LEFT JOIN torrent_contents AS tc ON tc.id = expected.id \
ORDER BY expected.id";

/// Volatile writer comparison for every exact ID in the writer plan.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriterComparison {
    pub rows: Vec<WriterRowComparison>,
}

impl WriterComparison {
    /// Number of expected rows whose writer image exactly matches live.
    pub fn match_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.verdict == ComparisonVerdict::Match)
            .count()
    }

    /// Number of expected rows with presence or volatile-field drift.
    pub fn mismatch_count(&self) -> usize {
        self.rows.len() - self.match_count()
    }

    /// True when every expected writer row matches.
    pub fn is_match(&self) -> bool {
        self.mismatch_count() == 0
    }
}

/// One exact `torrent_contents.id` writer comparison.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriterRowComparison {
    pub id: String,
    pub verdict: ComparisonVerdict,
    pub drift_fields: Vec<WriterDriftField>,
}

/// Closed metric labels for the volatile writer image.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterDriftField {
    RowPresence,
    Seeders,
    Leechers,
    PublishedAt,
    Tsv,
}

impl WriterDriftField {
    /// Complete bounded label vocabulary.
    pub const ALL: [Self; 5] = [
        Self::RowPresence,
        Self::Seeders,
        Self::Leechers,
        Self::PublishedAt,
        Self::Tsv,
    ];

    /// Low-cardinality label for writer drift metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RowPresence => "row_presence",
            Self::Seeders => "seeders",
            Self::Leechers => "leechers",
            Self::PublishedAt => "published_at",
            Self::Tsv => "tsv",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LiveWriterRow {
    id: String,
    present: bool,
    seeders: Option<i32>,
    leechers: Option<i32>,
    published_at_micros: Option<i64>,
    tsv_matches: Option<bool>,
}

/// Read and compare only the writer plan's expected IDs in the caller's
/// existing transaction.
pub(crate) async fn compare_writer_persistence_in(
    connection: &mut PgConnection,
    expected: &BTreeMap<String, TorrentContentPersistence>,
) -> Result<WriterComparison, WriterCompareError> {
    if expected.len() > MAX_WRITER_COMPARISON_ROWS {
        return Err(WriterCompareError::TooManyRows {
            actual: expected.len(),
            limit: MAX_WRITER_COMPARISON_ROWS,
        });
    }
    if expected.is_empty() {
        return Ok(WriterComparison::default());
    }

    let ids = expected.keys().cloned().collect::<Vec<_>>();
    let tsvs = expected
        .values()
        .map(|metadata| metadata.tsv.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(WRITER_LIVE_SQL)
        .bind(&ids)
        .bind(&tsvs)
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .map(|row| {
            Ok(LiveWriterRow {
                id: row.try_get("id")?,
                present: row.try_get("present")?,
                seeders: row.try_get("seeders")?,
                leechers: row.try_get("leechers")?,
                published_at_micros: row.try_get("published_at_micros")?,
                tsv_matches: row.try_get("tsv_matches")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    if rows.len() != ids.len()
        || rows
            .iter()
            .map(|row| row.id.as_str())
            .ne(ids.iter().map(String::as_str))
    {
        return Err(WriterCompareError::LiveKeysetMismatch);
    }

    Ok(compare_writer_rows(expected, &rows))
}

fn compare_writer_rows(
    expected: &BTreeMap<String, TorrentContentPersistence>,
    live: &[LiveWriterRow],
) -> WriterComparison {
    let rows = live
        .iter()
        .map(|actual| {
            let expected = &expected[&actual.id];
            let mut drift_fields = Vec::new();
            if !actual.present {
                drift_fields.push(WriterDriftField::RowPresence);
            } else {
                if actual.seeders.map(i64::from)
                    != expected.seeders.map(|value| {
                        i64::try_from(value).expect("validated persistence count fits i64")
                    })
                {
                    drift_fields.push(WriterDriftField::Seeders);
                }
                if actual.leechers.map(i64::from)
                    != expected.leechers.map(|value| {
                        i64::try_from(value).expect("validated persistence count fits i64")
                    })
                {
                    drift_fields.push(WriterDriftField::Leechers);
                }
                if actual.published_at_micros != Some(expected.published_at_micros) {
                    drift_fields.push(WriterDriftField::PublishedAt);
                }
                if actual.tsv_matches != Some(true) {
                    drift_fields.push(WriterDriftField::Tsv);
                }
            }
            WriterRowComparison {
                id: actual.id.clone(),
                verdict: if drift_fields.is_empty() {
                    ComparisonVerdict::Match
                } else {
                    ComparisonVerdict::Mismatch
                },
                drift_fields,
            }
        })
        .collect();
    WriterComparison { rows }
}

/// Fail-closed errors from the bounded writer comparison.
#[derive(Debug, thiserror::Error)]
pub enum WriterCompareError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("writer comparison has {actual} rows, above the {limit}-row limit")]
    TooManyRows { actual: usize, limit: usize },
    #[error("writer comparison query returned an incomplete or reordered expected keyset")]
    LiveKeysetMismatch,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        compare_writer_rows, LiveWriterRow, WriterDriftField, MAX_WRITER_COMPARISON_ROWS,
        WRITER_LIVE_SQL,
    };
    use crate::{ComparisonVerdict, TorrentContentPersistence};

    fn persistence(seed: u64) -> TorrentContentPersistence {
        TorrentContentPersistence {
            seeders: Some(seed),
            leechers: Some(7),
            published_at_micros: 1_700_000_000_123_456,
            tsv: "'fixture':1A".to_owned(),
        }
    }

    #[test]
    fn exact_rows_match_and_the_query_is_expected_id_keyed() {
        let expected = BTreeMap::from([("expected".to_owned(), persistence(5))]);
        let live = [LiveWriterRow {
            id: "expected".to_owned(),
            present: true,
            seeders: Some(5),
            leechers: Some(7),
            published_at_micros: Some(1_700_000_000_123_456),
            tsv_matches: Some(true),
        }];
        let comparison = compare_writer_rows(&expected, &live);
        assert!(comparison.is_match());
        assert_eq!(comparison.rows[0].verdict, ComparisonVerdict::Match);
        assert_eq!(MAX_WRITER_COMPARISON_ROWS, 100);
        assert!(WRITER_LIVE_SQL.contains("FROM expected"));
        assert!(WRITER_LIVE_SQL.contains("tc.tsv = expected.tsv::tsvector"));
        assert!(!WRITER_LIVE_SQL.contains("to_tsvector"));
    }

    #[test]
    fn missing_row_and_each_volatile_field_have_closed_drift_labels() {
        let expected = BTreeMap::from([
            ("changed".to_owned(), persistence(5)),
            ("missing".to_owned(), persistence(6)),
        ]);
        let live = [
            LiveWriterRow {
                id: "changed".to_owned(),
                present: true,
                seeders: Some(4),
                leechers: Some(8),
                published_at_micros: Some(1_700_000_000_123_455),
                tsv_matches: Some(false),
            },
            LiveWriterRow {
                id: "missing".to_owned(),
                present: false,
                seeders: None,
                leechers: None,
                published_at_micros: None,
                tsv_matches: None,
            },
        ];
        let comparison = compare_writer_rows(&expected, &live);
        assert_eq!(comparison.mismatch_count(), 2);
        assert_eq!(
            comparison.rows[0].drift_fields,
            vec![
                WriterDriftField::Seeders,
                WriterDriftField::Leechers,
                WriterDriftField::PublishedAt,
                WriterDriftField::Tsv,
            ]
        );
        assert_eq!(
            comparison.rows[1].drift_fields,
            vec![WriterDriftField::RowPresence]
        );
        assert_eq!(WriterDriftField::RowPresence.as_str(), "row_presence");
        assert_eq!(WriterDriftField::PublishedAt.as_str(), "published_at");
        assert_eq!(
            WriterDriftField::ALL.map(WriterDriftField::as_str),
            ["row_presence", "seeders", "leechers", "published_at", "tsv"]
        );
    }
}
