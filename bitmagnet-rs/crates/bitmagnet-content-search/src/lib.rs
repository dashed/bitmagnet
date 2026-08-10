//! Local content lookup for the B′ enrichment-parity lanes (Go
//! `internal/classifier/search.go` `localSearch`).
//!
//! Placeholder registered by the B′-0 classifier-dependency-seam lane so the
//! follow-on lane can land the PostgreSQL implementation of
//! [`bitmagnet_classifier::ContentResolver`]'s `content_by_id` /
//! `content_by_search` without editing the workspace manifest.
//!
//! 🚨 `content_by_search` must return the **ordered, pre-Levenshtein** candidate
//! list (Go's `query.Limit(10)` + `OrderByQueryStringRank`), never a single
//! winner — the tie-break belongs to `bitmagnet-textmatch`.

use bitmagnet_model::{Content, ContentType, Date};
use sqlx::postgres::PgRow;
use sqlx::Row;

/// The `content` select list, aliased to the names [`content_from_row`] reads.
///
/// 🔑 One list, one decoder, one place to change. The processor's loader and the
/// (still to come) live `ContentResolver` both hydrate the same row, and two
/// hand-written copies of this would be free to drift into disagreeing about
/// what a content row is — which is precisely the class of bug the parity work
/// exists to catch, so it should not be introduced by the parity work itself.
///
/// Deliberately scalar-only. The workspace's `sqlx` carries no `chrono`/`time`
/// feature, so `date` and `timestamptz` are rendered in SQL rather than decoded
/// through a date type: `to_char` is immune to the session `DateStyle`, where
/// `release_date::text` would not be.
pub const CONTENT_COLUMNS: &str = "\
    type::text AS content_type, \
    source, \
    id, \
    title, \
    to_char(release_date, 'YYYY-MM-DD') AS release_date, \
    release_year, \
    adult, \
    original_language, \
    original_title, \
    overview, \
    runtime, \
    popularity, \
    vote_average, \
    vote_count, \
    EXTRACT(EPOCH FROM created_at)::bigint AS created_at, \
    EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at";

/// Decodes a `content` row selected with [`CONTENT_COLUMNS`].
///
/// # What is deliberately NOT hydrated
///
/// * `tsv` — the search vector is derived, large, and no consumer of a hydrated
///   row reads it.
/// * `collections` / `attributes` — the shadow role holds no SELECT grant on
///   `content_collections`, `content_collections_content` or
///   `content_attributes`. They stay empty rather than being faked, and a caller
///   that needs them must say so and get the grants first.
///
/// 🚨 `created_at` IS hydrated, and is not cosmetic: Go's `persist.go` upserts an
/// attached content row only when `Content.CreatedAt.IsZero()`, which is true
/// only for a row TMDB just produced. A row read back from the database has a
/// non-zero timestamp and is therefore NOT re-upserted. Dropping it here would
/// silently turn a reused row into a rewritten one and change the write set.
///
/// # Errors
///
/// If a column is missing or has an unexpected type, or the content type is not
/// one this build knows.
pub fn content_from_row(row: &PgRow) -> Result<Content, sqlx::Error> {
    let content_type: String = row.try_get("content_type")?;
    let content_type: ContentType = content_type.parse().map_err(|_| {
        sqlx::Error::Decode(format!("unknown content type {content_type:?}").into())
    })?;

    let release_date: Option<String> = row.try_get("release_date")?;

    Ok(Content {
        content_type,
        source: row.try_get("source")?,
        id: row.try_get("id")?,
        title: row.try_get("title")?,
        release_date: release_date.as_deref().and_then(parse_iso_date),
        release_year: row
            .try_get::<Option<i32>, _>("release_year")?
            .and_then(|year| u32::try_from(year).ok()),
        adult: row.try_get("adult")?,
        original_language: row.try_get("original_language")?,
        original_title: row.try_get("original_title")?,
        overview: row.try_get("overview")?,
        runtime: row
            .try_get::<Option<i32>, _>("runtime")?
            .and_then(|runtime| u32::try_from(runtime).ok()),
        // `popularity` / `vote_average` are `double precision` in PostgreSQL but
        // `f32` in the model, matching Go's `NullFloat32`.
        popularity: row
            .try_get::<Option<f64>, _>("popularity")?
            .map(|value| value as f32),
        vote_average: row
            .try_get::<Option<f64>, _>("vote_average")?
            .map(|value| value as f32),
        vote_count: row
            .try_get::<Option<i64>, _>("vote_count")?
            .and_then(|count| u32::try_from(count).ok()),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        tsv: bitmagnet_fts::Tsvector::default(),
        collections: Vec::new(),
        attributes: Vec::new(),
    })
}

/// `YYYY-MM-DD`, as rendered by [`CONTENT_COLUMNS`]. A value that is not that
/// shape is dropped rather than guessed — the column is a `date`, so anything
/// else means the select list and this decoder have diverged.
fn parse_iso_date(value: &str) -> Option<Date> {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }

    Some(Date {
        year: value[0..4].parse().ok()?,
        month: value[5..7].parse().ok()?,
        day: value[8..10].parse().ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The select list and the decoder have to agree on every alias, or a
    /// hydrated row silently loses a field.
    #[test]
    fn every_decoded_column_is_selected() {
        for column in [
            "content_type",
            "source",
            "id",
            "title",
            "release_date",
            "release_year",
            "adult",
            "original_language",
            "original_title",
            "overview",
            "runtime",
            "popularity",
            "vote_average",
            "vote_count",
            "created_at",
            "updated_at",
        ] {
            assert!(
                CONTENT_COLUMNS.contains(column),
                "{column} is decoded but not selected"
            );
        }
    }

    #[test]
    fn parses_the_rendered_date_form() {
        assert_eq!(
            parse_iso_date("2011-07-15"),
            Some(Date {
                year: 2011,
                month: 7,
                day: 15
            })
        );
        assert_eq!(parse_iso_date("2011-7-15"), None);
        assert_eq!(parse_iso_date(""), None);
    }
}
