//! The live PostgreSQL local content lookup — Go `internal/classifier/search.go`
//! `localSearch`, wrapped in `search_semaphore.go`'s `localSearchSemaphore`.
//!
//! This is the other half of the [`bitmagnet_classifier::ContentResolver`] pair
//! whose recorded half lives in
//! `bitmagnet-classifier/src/resolver/tape.rs`. The tape answers from what Go
//! observed; this answers from the database Go observed it in. Both must ask the
//! *same question*, so the query-shape constants below are the same constants
//! Go keeps at the top of `search.go` for exactly that reason.
//!
//! # Why an inherent struct rather than a `ContentResolver` impl
//!
//! [`bitmagnet_classifier::ContentResolver`] also declares five TMDB methods.
//! Local search and TMDB are two different backends with two different failure
//! modes, and Go injects them as two collaborators (`dependencies.search` and
//! `dependencies.tmdbClient`). Keeping them apart here and composing them behind
//! one trait object elsewhere preserves that separation; the signatures and the
//! error type match the trait exactly, so the composite is a two-line forward.
//!
//! # Read-only
//!
//! Every statement in this module is a `SELECT`. There is no code path that
//! writes, and there must never be one: the classifier's write set is produced
//! by the processor, and a stray write here would contaminate the very parity
//! measurement this crate exists to support.

use bitmagnet_classifier::{ContentResultItem, ResolveError};
use bitmagnet_model::{Content, ContentType};
use sqlx::{PgPool, Row};
use tokio::sync::Semaphore;

use crate::{content_from_row, CONTENT_COLUMNS};

/// Go `contentBySearchLimit`. Inlined into the SQL text, never bound — see
/// [`content_by_search_sql`].
const CONTENT_BY_SEARCH_LIMIT: u32 = 10;

/// Go `contentByIDLimit`.
const CONTENT_BY_ID_LIMIT: u32 = 1;

/// Go `canonicalIdentifierSource`. The one source whose id is the `content`
/// primary key rather than a `content_attributes` value.
const CANONICAL_IDENTIFIER_SOURCE: &str = "tmdb";

/// Go's `contentIdentityColumns` (`order_content.go`), in primary-key order.
///
/// 🚨 These are `text` columns, so PostgreSQL orders them by the database
/// collation (`en_US.utf8` in production), NOT bytewise — `"10"` sorts before
/// `"9"`. That is why the ordering is left to PostgreSQL and the rows are used
/// in the order they arrive. Re-sorting them in Rust would silently pick a
/// different winner from a tied window than Go does.
const CONTENT_IDENTITY_ORDER_BY: &str = "content.type, content.source, content.id";

/// The alias the relevance rank is projected under, and ordered by. Go names it
/// `_order_0`, because its builder aliases every ordering column positionally;
/// the name is internal to the statement either way.
const QUERY_STRING_RANK_ALIAS: &str = "query_string_rank";

/// PostgreSQL-backed local content lookup.
///
/// # Concurrency
///
/// Go does **not** wire `localSearch` in directly: `factory.go` wraps it in
/// `localSearchSemaphore` with a buffer of 1, so at most one local search runs
/// at a time even though the classification workflow runs at concurrency 10.
/// That bound is reproduced here rather than left to the caller, because it is
/// part of how the queries behave under load — a content search is a full-text
/// scan, and ten of them at once is a different database than one of them at a
/// time. Callers get Go's wiring by construction instead of having to know
/// about it.
pub struct PgContentSearch {
    pool: PgPool,
    /// Go's `semaphore chan struct{}` of capacity 1. A cancelled caller simply
    /// drops the `acquire` future, which is Go's `case <-ctx.Done()` arm.
    slot: Semaphore,
}

impl PgContentSearch {
    /// Wraps a pool. The pool may be shared; the concurrency bound above is
    /// per-[`PgContentSearch`], matching Go's one-semaphore-per-classifier.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            slot: Semaphore::new(1),
        }
    }

    /// Go `LocalSearch.ContentByID` — look a content row up by an identifier.
    ///
    /// The dispatch on `source` is Go's, and the two branches are genuinely
    /// different queries; see [`content_by_id_sql`].
    ///
    /// `Ok(None)` is Go's `classification.ErrUnmatched`: the query ran and
    /// matched nothing. A failure to run it is an [`Err`]. Conflating the two
    /// would turn a dead database into a stream of confident "no such content"
    /// answers.
    ///
    /// # Errors
    ///
    /// [`ResolveError::LocalSearch`] if the query or the row decode fails.
    pub async fn content_by_id(
        &self,
        content_type: ContentType,
        source: &str,
        id: &str,
    ) -> Result<Option<Content>, ResolveError> {
        let canonical = source == CANONICAL_IDENTIFIER_SOURCE;
        let sql = content_by_id_sql(canonical);

        let _permit = self.acquire().await;

        // Only compile-time constants are interpolated into `sql` (the shared
        // select list, the identity ordering, the literal limit); all three
        // values below are bound. See `content_by_id_sql`.
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(content_type.as_str())
            .bind(source)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(local_search_error)?;

        row.as_ref()
            .map(content_from_row)
            .transpose()
            .map_err(local_search_error)
    }

    /// Go `LocalSearch.ContentBySearch`, **stopping before its tie-break**.
    ///
    /// 🚨 The return value is the ordered, pre-Levenshtein candidate window —
    /// Go's `LIMIT 10` ordered by relevance then identity. It deliberately does
    /// NOT pick a winner: `levenshteinFindBestMatch` is first-wins over this
    /// list, so the *order* is the load-bearing output and collapsing the window
    /// here would move Go's tie-break onto the unobservable side of the seam.
    ///
    /// An empty vector is Go's `ErrUnmatched`.
    ///
    /// # Errors
    ///
    /// [`ResolveError::LocalSearch`] if the query or a row decode fails.
    pub async fn content_by_search(
        &self,
        content_type: ContentType,
        base_title: &str,
        year: Option<u16>,
    ) -> Result<Vec<ContentResultItem>, ResolveError> {
        // Go hands the query builder `contentSearchString(baseTitle)` — the base
        // title wrapped in double quotes, i.e. a phrase query — and the builder
        // compiles that with `fts.AppQueryToTsquery`. Both steps are reproduced
        // rather than approximated: a port that quotes differently, or that
        // derives a different tsquery from the same string, is asking a
        // different question. (The tape cannot catch the second of those — it
        // records the search string, not the tsquery — so it has to be right by
        // construction here.)
        let tsquery = bitmagnet_fts::app_query_to_tsquery(&content_search_string(base_title));

        // Go's `model.Year` zero value is nil (`Year.IsNil()`), and
        // `contentBySearchOptions` adds the date filter only for a non-nil year.
        // `Some(0)` therefore has to mean "no year", not "the year 0".
        let range = year.filter(|y| *y != 0).map(date_range_from_year);

        let sql = content_by_search_sql(!tsquery.is_empty(), range.is_some());

        let _permit = self.acquire().await;

        // Only compile-time constants are interpolated into `sql`: the shared
        // select list, the rank expression, the identity ordering and the
        // literal limit. Every value — content type, tsquery, both date bounds —
        // is bound below, in the order `content_by_search_sql` numbered them.
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(content_type.as_str());
        if !tsquery.is_empty() {
            query = query.bind(&tsquery);
        }
        if let Some((start, end)) = &range {
            query = query.bind(start).bind(end);
        }

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(local_search_error)?;

        rows.iter()
            .map(|row| {
                Ok(ContentResultItem {
                    content: content_from_row(row).map_err(local_search_error)?,
                    query_string_rank: row
                        .try_get(QUERY_STRING_RANK_ALIAS)
                        .map_err(local_search_error)?,
                })
            })
            .collect()
    }

    /// Go's `localSearchSemaphore` gate. Held for the duration of the query and
    /// released by the guard's drop, exactly as Go's `defer func() { <-s.semaphore }()`.
    async fn acquire(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.slot
            .acquire()
            .await
            .expect("local search semaphore is never closed")
    }
}

/// Go's `contentSearchString`: the base title as a **phrase** query.
fn content_search_string(base_title: &str) -> String {
    format!("\"{base_title}\"")
}

/// Go's `model.NewDateRangeFromYear` as the two `date` bounds the criteria
/// compares against: `[YYYY-01-01, (YYYY+1)-01-01)`.
///
/// Rendered as ISO strings and cast in SQL rather than bound as a date type,
/// because the workspace's `sqlx` carries no `chrono`/`time` feature — the same
/// reasoning as [`CONTENT_COLUMNS`]'s `to_char`.
fn date_range_from_year(year: u16) -> (String, String) {
    (
        format!("{year}-01-01"),
        format!("{}-01-01", year.saturating_add(1)),
    )
}

/// The two shapes of Go's `contentByIDOptions`.
///
/// **Canonical** (`source == "tmdb"`): `ContentCanonicalIdentifierCriteria`
/// matches the `content` primary key. Go imposes no ordering here and neither
/// does this — `(type, source, id)` is `content_pkey`, so `LIMIT 1` can only
/// ever admit the one row that exists, and inventing an `ORDER BY` would suggest
/// a choice is being made where there is none.
///
/// **Alternative** (any other source): `ContentAlternativeIdentifierCriteria` is
/// an `EXISTS` over `content_attributes`, joined on the content key, whose
/// `source` is the ref's source and whose `value` is the ref's id.
///
/// 🚨 The attribute `key` is **not** constrained, and that is deliberate in Go —
/// `q.ContentAttribute` conditions list content_type/source/value and nothing
/// else. Adding `AND key = 'id'` would look like a tightening and would silently
/// drop matches Go accepts.
///
/// Unlike the canonical branch this one is not unique — several content rows can
/// carry the same attribute value — so Go orders by the canonical identity to
/// make the `LIMIT 1` pick reproducible, and so does this.
///
/// 🚨 The `LIMIT` is inlined into the statement text, never bound. A bound
/// `LIMIT $n` makes PostgreSQL build a *generic* plan, and a generic plan for a
/// query whose ordering has ties reshuffles which tied row surfaces. That has
/// already bitten this project once; the literal keeps the plan specific.
fn content_by_id_sql(canonical: bool) -> String {
    if canonical {
        return format!(
            "SELECT {CONTENT_COLUMNS} \
             FROM content \
             WHERE content.type = $1 AND content.source = $2 AND content.id = $3 \
             LIMIT {CONTENT_BY_ID_LIMIT}"
        );
    }

    // `content_attributes.content_type = $1` is redundant with the outer
    // `content.type = $1` plus the `EqCol` join, but Go emits it (the criteria
    // adds the type condition on the attribute table before the join columns)
    // and it lets the planner filter inside the subquery. Kept for both reasons.
    format!(
        "SELECT {CONTENT_COLUMNS} \
         FROM content \
         WHERE content.type = $1 \
           AND EXISTS ( \
             SELECT 1 FROM content_attributes \
             WHERE content_attributes.content_type = $1 \
               AND content_attributes.content_type = content.type \
               AND content_attributes.content_source = content.source \
               AND content_attributes.content_id = content.id \
               AND content_attributes.source = $2 \
               AND content_attributes.value = $3 \
           ) \
         ORDER BY {CONTENT_IDENTITY_ORDER_BY} \
         LIMIT {CONTENT_BY_ID_LIMIT}"
    )
}

/// Go's `contentBySearchOptions`, as SQL.
///
/// Placeholders are numbered in bind order: `$1` content type, then the tsquery
/// if there is one, then the two date bounds if there is a year.
///
/// # The empty-tsquery branch is not a corner case to skip
///
/// Go's builder guards every tsquery use with `b.tsquery != ""`: with an empty
/// tsquery it adds **no** `tsv @@` filter and projects the literal `0` as the
/// rank. A base title with no word characters at all compiles to an empty
/// tsquery, and Go then returns the first 10 rows of the content type in
/// identity order — not zero rows. Emitting `tsv @@ ''::tsquery` instead would
/// match nothing and diverge silently, so the branch is reproduced.
///
/// # Why the rank is cast
///
/// `ts_rank_cd` returns `real`. Go's `database/sql` widens that to `float64` on
/// scan; `sqlx` decodes by exact type, so the cast to `float8` happens in SQL.
/// Widening `float4` → `float8` is exact and monotonic, so neither the recorded
/// rank values nor the ordering move. The literal `0` is cast for the same
/// reason (it would otherwise be `int4`).
///
/// 🚨 `ORDER BY` is relevance **then identity**, and the identity tiebreak is
/// not optional: `ts_rank_cd` is degenerate for these single-phrase queries and
/// commonly returns an identical rank for every candidate, so without a total
/// order both the contents of the `LIMIT 10` window and the order within it are
/// decided by whichever plan the planner picked.
///
/// 🚨 The `LIMIT` is inlined, for the generic-plan reason in
/// [`content_by_id_sql`] — which bites hardest exactly here, where the ties are.
fn content_by_search_sql(with_tsquery: bool, with_year: bool) -> String {
    let mut next_placeholder = 2;

    let (rank, tsv_filter) = if with_tsquery {
        let placeholder = next_placeholder;
        next_placeholder += 1;
        (
            format!("ts_rank_cd(content.tsv, ${placeholder}::tsquery)::float8"),
            format!(" AND content.tsv @@ ${placeholder}::tsquery"),
        )
    } else {
        ("0::float8".to_owned(), String::new())
    };

    let release_date_filter = if with_year {
        let start = next_placeholder;
        let end = next_placeholder + 1;
        format!(
            " AND content.release_date >= ${start}::date AND content.release_date < ${end}::date"
        )
    } else {
        String::new()
    };

    format!(
        "SELECT {CONTENT_COLUMNS}, {rank} AS {QUERY_STRING_RANK_ALIAS} \
         FROM content \
         WHERE content.type = $1{tsv_filter}{release_date_filter} \
         ORDER BY {QUERY_STRING_RANK_ALIAS} DESC, {CONTENT_IDENTITY_ORDER_BY} \
         LIMIT {CONTENT_BY_SEARCH_LIMIT}"
    )
}

/// A backend failure is an error, never an empty answer. See the nullability
/// convention on [`bitmagnet_classifier::ContentResolver`].
fn local_search_error(err: sqlx::Error) -> ResolveError {
    ResolveError::LocalSearch(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical branch matches the primary key, so it needs no ordering —
    /// and must not acquire one, because an `ORDER BY` here would imply the
    /// `LIMIT 1` is choosing between rows when `content_pkey` guarantees it is
    /// not (Go `contentByIDOptions`, the `ref.Source == canonicalIdentifierSource`
    /// arm).
    #[test]
    fn canonical_by_id_matches_the_primary_key_with_no_ordering() {
        let sql = content_by_id_sql(true);

        assert_eq!(
            sql,
            format!(
                "SELECT {CONTENT_COLUMNS} \
                 FROM content \
                 WHERE content.type = $1 AND content.source = $2 AND content.id = $3 \
                 LIMIT 1"
            )
        );
        assert!(!sql.contains("ORDER BY"), "canonical: {sql}");
        assert!(!sql.contains("content_attributes"), "canonical: {sql}");
    }

    /// The alternative branch is Go's `ContentAlternativeIdentifierCriteria`: an
    /// `EXISTS` over `content_attributes` joined on the content key, matching the
    /// ref's source against the attribute `source` and the ref's id against the
    /// attribute `value`.
    #[test]
    fn alternative_by_id_is_an_exists_over_content_attributes() {
        assert_eq!(
            content_by_id_sql(false),
            format!(
                "SELECT {CONTENT_COLUMNS} \
                 FROM content \
                 WHERE content.type = $1 \
                   AND EXISTS ( \
                     SELECT 1 FROM content_attributes \
                     WHERE content_attributes.content_type = $1 \
                       AND content_attributes.content_type = content.type \
                       AND content_attributes.content_source = content.source \
                       AND content_attributes.content_id = content.id \
                       AND content_attributes.source = $2 \
                       AND content_attributes.value = $3 \
                   ) \
                 ORDER BY content.type, content.source, content.id \
                 LIMIT 1"
            )
        );
    }

    /// 🚨 Go constrains the attribute `source` and `value` and pointedly NOT the
    /// `key` — `contentAlternativeIdentifierCriteria` lists content_type, the
    /// three `EqCol` join columns, `Source.Eq` and `Value.In`, and stops. A
    /// `key = 'id'` that looked like a harmless tightening would drop every
    /// match Go finds under any other key, and the loss would show up as a
    /// classification that quietly failed to attach.
    #[test]
    fn the_alternative_branch_does_not_constrain_the_attribute_key() {
        let sql = content_by_id_sql(false);

        assert!(
            !sql.contains("content_attributes.key"),
            "the attribute key must stay unconstrained: {sql}"
        );
    }

    /// 🚨 A bound `LIMIT $n` makes PostgreSQL build a generic plan, and a generic
    /// plan for an ordering with ties reshuffles which tied row surfaces. Both
    /// limits are Go's constants (`contentBySearchLimit` / `contentByIDLimit`)
    /// and both must appear as literals in the statement text.
    #[test]
    fn every_limit_is_inline_and_never_bound() {
        for sql in [
            content_by_id_sql(true),
            content_by_id_sql(false),
            content_by_search_sql(true, true),
            content_by_search_sql(true, false),
            content_by_search_sql(false, false),
        ] {
            assert!(
                !sql.contains("LIMIT $"),
                "the limit must not be a bind parameter: {sql}"
            );
        }

        assert!(content_by_search_sql(true, true).ends_with("LIMIT 10"));
        assert!(content_by_id_sql(true).ends_with("LIMIT 1"));
    }

    /// A year expands into the release-date range and nothing else. Go's
    /// `contentBySearchOptions` adds exactly one option for a non-nil year,
    /// `ContentReleaseDateCriteria(NewDateRangeFromYear(year))`, which is
    /// `release_date >= start AND release_date < end` — half-open, so the
    /// following January 1st is excluded. There is no separate `release_year`
    /// predicate; the `year` in the taped request is the input, not a filter.
    #[test]
    fn a_year_becomes_a_half_open_release_date_range() {
        let sql = content_by_search_sql(true, true);

        assert_eq!(
            sql,
            format!(
                "SELECT {CONTENT_COLUMNS}, ts_rank_cd(content.tsv, $2::tsquery)::float8 AS query_string_rank \
                 FROM content \
                 WHERE content.type = $1 AND content.tsv @@ $2::tsquery \
                 AND content.release_date >= $3::date AND content.release_date < $4::date \
                 ORDER BY query_string_rank DESC, content.type, content.source, content.id \
                 LIMIT 10"
            )
        );
        // `release_year` is in the select list, so look only at the predicates.
        let (_, predicates) = sql.split_once(" WHERE ").expect("the query filters");
        assert!(
            !predicates.contains("release_year"),
            "the year is not a release_year predicate: {predicates}"
        );
    }

    /// Without a year there is no date predicate at all — and, crucially, no
    /// `release_date IS NULL` allowance either. The filtered form excludes rows
    /// with a null release date (a comparison against NULL is not true), so the
    /// two shapes are not the same query with a wider range.
    #[test]
    fn no_year_means_no_date_predicate() {
        let sql = content_by_search_sql(true, false);

        assert_eq!(
            sql,
            format!(
                "SELECT {CONTENT_COLUMNS}, ts_rank_cd(content.tsv, $2::tsquery)::float8 AS query_string_rank \
                 FROM content \
                 WHERE content.type = $1 AND content.tsv @@ $2::tsquery \
                 ORDER BY query_string_rank DESC, content.type, content.source, content.id \
                 LIMIT 10"
            )
        );
    }

    /// Go's builder guards every tsquery use with `b.tsquery != ""`. With an
    /// empty tsquery it drops the `tsv @@` filter and projects a literal `0`
    /// rank, so the query degenerates to "the first 10 rows of this content type
    /// in identity order". Emitting `tsv @@ ''::tsquery` instead would return
    /// zero rows and desync without any error to notice.
    #[test]
    fn an_empty_tsquery_drops_the_filter_and_ranks_zero() {
        let sql = content_by_search_sql(false, false);

        assert_eq!(
            sql,
            format!(
                "SELECT {CONTENT_COLUMNS}, 0::float8 AS query_string_rank \
                 FROM content \
                 WHERE content.type = $1 \
                 ORDER BY query_string_rank DESC, content.type, content.source, content.id \
                 LIMIT 10"
            )
        );
        assert!(!sql.contains("tsquery"), "empty tsquery: {sql}");
    }

    /// 🚨 The identity tiebreak is what makes the `LIMIT 10` window
    /// deterministic. `ts_rank_cd` is degenerate for these single-phrase queries
    /// — real ones come back with every candidate ranked identically — so
    /// relevance alone leaves both the window's membership and its order to the
    /// planner, and the classifier's first-wins Levenshtein then picks whichever
    /// row the plan happened to surface first.
    #[test]
    fn relevance_is_always_followed_by_the_identity_tiebreak() {
        for sql in [
            content_by_search_sql(true, true),
            content_by_search_sql(true, false),
            content_by_search_sql(false, false),
        ] {
            assert!(
                sql.contains(
                    "ORDER BY query_string_rank DESC, content.type, content.source, content.id"
                ),
                "{sql}"
            );
        }
    }

    /// Go's `contentSearchString` wraps the base title in double quotes, making
    /// it a phrase query. The quoting is the whole difference between "these
    /// words, in this order" and "these words, anywhere": `AppQueryToTsquery`
    /// compiles a quoted run with `<->` and an unquoted one with `&`.
    #[test]
    fn the_search_string_is_the_base_title_as_a_phrase() {
        assert_eq!(content_search_string("Cinderella"), "\"Cinderella\"");
        assert_eq!(
            bitmagnet_fts::app_query_to_tsquery(&content_search_string("The Wire")),
            bitmagnet_fts::app_query_to_tsquery("\"The Wire\"")
        );
    }

    /// A title with no word characters compiles to an empty tsquery, which is
    /// the branch above — this pins that the branch is reachable from a real
    /// base title and not merely defensive.
    #[test]
    fn a_word_free_title_compiles_to_an_empty_tsquery() {
        assert!(bitmagnet_fts::app_query_to_tsquery(&content_search_string("...")).is_empty());
    }

    /// Go's `NewDateRangeFromYear` is `[Jan 1 of year, Jan 1 of year+1)`, and
    /// the tape records both bounds formatted `2006-01-02`. The same two strings
    /// are what get bound here, so a port that expanded the year differently
    /// would be visible in both places at once.
    #[test]
    fn the_year_expands_to_januarys() {
        assert_eq!(
            date_range_from_year(2011),
            ("2011-01-01".to_owned(), "2012-01-01".to_owned())
        );
    }

    /// Read-only is a property of this module, not a convention to remember. If
    /// a write ever appears in a statement built here, this fails.
    #[test]
    fn every_statement_is_a_select() {
        for sql in [
            content_by_id_sql(true),
            content_by_id_sql(false),
            content_by_search_sql(true, true),
            content_by_search_sql(true, false),
            content_by_search_sql(false, false),
        ] {
            assert!(sql.starts_with("SELECT "), "{sql}");
            for write in ["INSERT", "UPDATE", "DELETE", "MERGE", "FOR UPDATE"] {
                assert!(!sql.contains(write), "{write} in a read-only query: {sql}");
            }
        }
    }
}
