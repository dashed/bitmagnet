//! Go-compatible count-per-value facet aggregation execution.
//!
//! This ports `internal/database/query/facets.go`, the torrent-content facet
//! builders in `internal/database/search/facet_*.go`, and the presentation
//! ordering in `internal/gql/gqlmodel/facet.go`.

use crate::aggregations::{AggregationGroup, AggregationItem, Aggregations, FacetLogic};
use crate::criteria::{
    ContentCollectionRef, Criteria, TorrentContentAttribute, Video3D, VideoCodec, VideoModifier,
    VideoResolution, VideoSource,
};
use crate::options::{FacetRequest, SearchBuildConfig, SearchOptions, TorrentContentFacet};
use crate::query::{
    criteria_sql, Bind, BuildState, CriteriaCtx, RequiredJoins, Result, SearchQueryError,
};
use crate::{ContentType, FileType};
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use sqlx::{PgPool, Row};
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

// Production currently gives the GraphQL process an eight-connection pool.
// Reserve headroom for the concurrent membership/count branches and for other
// requests instead of reproducing Go's unbounded goroutine fan-out. This is
// one request-wide cap shared by all facet groups.
const FACET_DB_CONCURRENCY: usize = 4;

pub(crate) const BASE_SELECT: &str = "SELECT torrent_contents.info_hash\nFROM torrent_contents";
const BUDGETED_COUNT_SQL: &str = "SELECT count, budget_exceeded FROM budgeted_count($1, $2)";
const TORRENT_SOURCE_VALUES_SQL: &str = "SELECT key, name FROM torrent_sources";
const TORRENT_TAG_VALUES_SQL: &str = "SELECT DISTINCT name FROM torrent_tags";
const CONTENT_GENRE_VALUES_SQL: &str =
    "SELECT source, id, name FROM content_collections WHERE type = 'genre'";
const RELEASE_YEAR_VALUES_SQL: &str =
    "SELECT DISTINCT release_year FROM content WHERE release_year >= 1000 AND release_year <= 9999";

const CONTENT_TYPE_VALUES: &[(&str, &str)] = &[
    ("null", "Unknown"),
    ("movie", "movie"),
    ("tv_show", "tv_show"),
    ("music", "music"),
    ("ebook", "ebook"),
    ("comic", "comic"),
    ("audiobook", "audiobook"),
    ("game", "game"),
    ("software", "software"),
    ("xxx", "xxx"),
];

const FILE_TYPE_VALUES: &[(&str, &str)] = &[
    ("archive", "Archive"),
    ("audio", "Audio"),
    ("data", "Data"),
    ("document", "Document"),
    ("image", "Image"),
    ("software", "Software"),
    ("subtitles", "Subtitles"),
    ("video", "Video"),
];

// `internal/model/languages.csv`, loaded by `LanguageValues()`/`Name()` in Go.
const LANGUAGE_VALUES: &[(&str, &str)] = &[
    ("af", "Afrikaans"),
    ("ar", "Arabic"),
    ("az", "Azerbaijani"),
    ("be", "Belarusian"),
    ("bg", "Bulgarian"),
    ("bs", "Bosnian"),
    ("ca", "Catalan"),
    ("ce", "Chechen"),
    ("co", "Corsican"),
    ("cs", "Czech"),
    ("cy", "Welsh"),
    ("da", "Danish"),
    ("de", "German"),
    ("el", "Greek"),
    ("en", "English"),
    ("es", "Spanish"),
    ("et", "Estonian"),
    ("eu", "Basque"),
    ("fa", "Persian"),
    ("fi", "Finnish"),
    ("fr", "French"),
    ("he", "Hebrew"),
    ("hi", "Hindi"),
    ("hr", "Croatian"),
    ("hu", "Hungarian"),
    ("hy", "Armenian"),
    ("id", "Indonesian"),
    ("is", "Icelandic"),
    ("it", "Italian"),
    ("ja", "Japanese"),
    ("ka", "Georgian"),
    ("ko", "Korean"),
    ("ku", "Kurdish"),
    ("lt", "Lithuanian"),
    ("lv", "Latvian"),
    ("mi", "Maori"),
    ("mk", "Macedonian"),
    ("ml", "Malayalam"),
    ("mn", "Mongolian"),
    ("ms", "Malay"),
    ("mt", "Maltese"),
    ("nl", "Dutch"),
    ("no", "Norwegian"),
    ("pl", "Polish"),
    ("pt", "Portuguese"),
    ("ro", "Romanian"),
    ("ru", "Russian"),
    ("sa", "Sanskrit"),
    ("sk", "Slovak"),
    ("sl", "Slovenian"),
    ("sm", "Samoan"),
    ("so", "Somali"),
    ("sr", "Serbian"),
    ("sv", "Swedish"),
    ("ta", "Tamil"),
    ("th", "Thai"),
    ("tr", "Turkish"),
    ("uk", "Ukrainian"),
    ("vi", "Vietnamese"),
    ("yi", "Yiddish"),
    ("zh", "Chinese"),
    ("zu", "Zulu"),
];

const VIDEO_RESOLUTION_VALUES: &[(&str, &str)] = &[
    ("V360p", "360p"),
    ("V480p", "480p"),
    ("V540p", "540p"),
    ("V576p", "576p"),
    ("V720p", "720p"),
    ("V1080p", "1080p"),
    ("V1440p", "1440p"),
    ("V2160p", "2160p"),
    ("V4320p", "4320p"),
];

const VIDEO_SOURCE_VALUES: &[(&str, &str)] = &[
    ("CAM", "CAM"),
    ("TELESYNC", "TELESYNC"),
    ("TELECINE", "TELECINE"),
    ("WORKPRINT", "WORKPRINT"),
    ("DVD", "DVD"),
    ("TV", "TV"),
    ("WEBDL", "WEBDL"),
    ("WEBRip", "WEBRip"),
    ("BluRay", "BluRay"),
];

const VIDEO_3D_VALUES: &[(&str, &str)] = &[("V3D", "3D"), ("V3DSBS", "3DSBS"), ("V3DOU", "3DOU")];

const VIDEO_CODEC_VALUES: &[(&str, &str)] = &[
    ("H264", "H264"),
    ("x264", "x264"),
    ("x265", "x265"),
    ("XviD", "XviD"),
    ("DivX", "DivX"),
    ("MPEG2", "MPEG2"),
    ("MPEG4", "MPEG4"),
];

const VIDEO_MODIFIER_VALUES: &[(&str, &str)] = &[
    ("REGIONAL", "REGIONAL"),
    ("SCREENER", "SCREENER"),
    ("RAWHD", "RAWHD"),
    ("BRDISK", "BRDISK"),
    ("REMUX", "REMUX"),
];

/// Execute Go-compatible count-per-value aggregations for the requested facets.
///
/// Each retained bucket comes from one `budgeted_count` call over the GraphQL
/// base predicate plus that bucket's criterion. This ports
/// `internal/database/query/facets.go` `calculateAggregations`.
pub async fn fetch_aggregations(
    pool: &PgPool,
    options: &SearchOptions,
    config: &SearchBuildConfig,
    now: DateTime<Utc>,
) -> Result<Aggregations> {
    let ctx = CriteriaCtx::new(config, now);
    let facet_preparations = options
        .facets
        .iter()
        .filter(|facet| facet.aggregate)
        .enumerate()
        .map(|(order, facet)| [(order, Arc::new(facet.clone()))]);
    let mut prepared = try_collect_facet_work(facet_preparations, |(order, facet)| {
        prepare_facet_group(pool, order, facet)
    })
    .await?;
    prepared.sort_by_key(|facet| facet.order);

    let mut groups = Vec::with_capacity(prepared.len());
    let item_work = prepared
        .into_iter()
        .enumerate()
        .map(|(group_index, prepared)| {
            groups.push((
                prepared.facet.facet.key().to_owned(),
                AggregationGroup {
                    label: facet_label(prepared.facet.facet).to_owned(),
                    logic: prepared.logic,
                    items: BTreeMap::new(),
                },
            ));

            prepared
                .values
                .into_iter()
                .map(move |(value, label)| FacetItemWork {
                    group_index,
                    facet: Arc::clone(&prepared.facet),
                    logic: prepared.logic,
                    current_facet: prepared.current_facet,
                    value,
                    label,
                })
        })
        .collect::<Vec<_>>();

    // Interleave groups before applying one request-wide cap. This lets
    // independent facet groups overlap without multiplying per-group limits;
    // buffer_unordered retains at most FACET_DB_CONCURRENCY pending futures.
    let ctx = &ctx;
    let entries = try_collect_facet_work(item_work, |work| async move {
        let item = fetch_facet_item(
            pool,
            options,
            ctx,
            &work.facet,
            work.logic,
            work.current_facet,
            work.value,
            work.label,
        )
        .await?;
        Ok((work.group_index, item))
    })
    .await?;

    for (group_index, item) in entries {
        if let Some((value, item)) = item {
            groups
                .get_mut(group_index)
                .expect("prepared facet group must exist")
                .1
                .items
                .insert(value, item);
        }
    }

    let mut aggregations = Aggregations::new();
    for (key, group) in groups {
        // Match the previous sequential assembly if malformed options contain
        // the same facet twice: the later complete group replaces the first.
        aggregations.insert(key, group);
    }

    Ok(aggregations)
}

/// Grouped-facet fast path for an already-refined candidate set.
///
/// This is a byte-for-byte-count-equivalent replacement for
/// [`fetch_aggregations`] used only by the L3 composer's refined-set
/// re-aggregation. Instead of one `budgeted_count` statement per facet value,
/// each *scalar single-valued `torrent_contents` column* facet
/// ([`grouped_facet_column`]) is aggregated with a single
/// `SELECT <col>, count(*) ... GROUP BY <col>` over the identical base query
/// (`build_base_query`), so joins, cross-facet predicates from other facets,
/// current-facet self-exclusion, and the inlined refined-id `IN` list are
/// unchanged. Because the base membership query never fans rows out (all facet
/// predicates are `EXISTS` subqueries; the only joins are 1:1), the grouped
/// count for each value equals the per-value `count(*)` exactly, so every count
/// is exact (`is_estimate = false`).
///
/// A value is retained iff its grouped count is `> 0` or it is in that facet's
/// filter selection (zero-count filter-selected values keep an explicit count-0
/// entry), matching [`fetch_facet_item`]. Grouped rows whose column value is not
/// in the facet's known vocabulary — including the NULL bucket for facets whose
/// vocabulary omits `"null"` — are dropped, exactly as the per-value path never
/// queries them. Facets that are not scalar-eligible fall back to the unchanged
/// per-value [`fetch_facet_item`] path, and both kinds of work share the one
/// request-wide [`FACET_DB_CONCURRENCY`] cap.
pub async fn fetch_aggregations_grouped_for_candidates(
    pool: &PgPool,
    options: &SearchOptions,
    config: &SearchBuildConfig,
    now: DateTime<Utc>,
) -> Result<Aggregations> {
    let ctx = CriteriaCtx::new(config, now);
    let facet_preparations = options
        .facets
        .iter()
        .filter(|facet| facet.aggregate)
        .enumerate()
        .map(|(order, facet)| [(order, Arc::new(facet.clone()))]);
    let mut prepared = try_collect_facet_work(facet_preparations, |(order, facet)| {
        prepare_facet_group(pool, order, facet)
    })
    .await?;
    prepared.sort_by_key(|facet| facet.order);

    let mut groups = Vec::with_capacity(prepared.len());
    let mut work_by_group: Vec<Vec<GroupedFacetWork>> = Vec::with_capacity(prepared.len());
    for (group_index, prepared) in prepared.into_iter().enumerate() {
        groups.push((
            prepared.facet.facet.key().to_owned(),
            AggregationGroup {
                label: facet_label(prepared.facet.facet).to_owned(),
                logic: prepared.logic,
                items: BTreeMap::new(),
            },
        ));

        if let Some(column) = grouped_facet_column(prepared.facet.facet) {
            work_by_group.push(vec![GroupedFacetWork::Grouped {
                group_index,
                facet: Arc::clone(&prepared.facet),
                current_facet: prepared.current_facet,
                column,
                values: Arc::new(prepared.values),
            }]);
        } else {
            let PreparedFacet {
                facet,
                logic,
                current_facet,
                values,
                ..
            } = prepared;
            work_by_group.push(
                values
                    .into_iter()
                    .map(|(value, label)| {
                        GroupedFacetWork::PerValue(FacetItemWork {
                            group_index,
                            facet: Arc::clone(&facet),
                            logic,
                            current_facet,
                            value,
                            label,
                        })
                    })
                    .collect(),
            );
        }
    }

    let ctx = &ctx;
    let entries = try_collect_facet_work(work_by_group, |work| async move {
        match work {
            GroupedFacetWork::Grouped {
                group_index,
                facet,
                current_facet,
                column,
                values,
            } => {
                let items = fetch_facet_group_grouped(
                    pool,
                    options,
                    ctx,
                    &facet,
                    current_facet,
                    column,
                    &values,
                )
                .await?;
                Ok((group_index, items))
            }
            GroupedFacetWork::PerValue(work) => {
                let item = fetch_facet_item(
                    pool,
                    options,
                    ctx,
                    &work.facet,
                    work.logic,
                    work.current_facet,
                    work.value,
                    work.label,
                )
                .await?;
                Ok((work.group_index, item.into_iter().collect::<Vec<_>>()))
            }
        }
    })
    .await?;

    for (group_index, items) in entries {
        let group = &mut groups
            .get_mut(group_index)
            .expect("prepared facet group must exist")
            .1;
        for (value, item) in items {
            group.items.insert(value, item);
        }
    }

    let mut aggregations = Aggregations::new();
    for (key, group) in groups {
        aggregations.insert(key, group);
    }

    Ok(aggregations)
}

enum GroupedFacetWork {
    Grouped {
        group_index: usize,
        facet: Arc<FacetRequest>,
        current_facet: Option<&'static str>,
        column: &'static str,
        values: Arc<BTreeMap<String, String>>,
    },
    PerValue(FacetItemWork),
}

/// The single-valued scalar `torrent_contents` column a facet aggregates over,
/// or `None` when the facet must use the per-value fallback.
///
/// Only facets backed by exactly one scalar `torrent_contents` column are
/// eligible: their per-value criterion is a plain `<col> IN (v)` / `<col> IS
/// NULL`, so partitioning the base query with `GROUP BY <col>` reproduces every
/// per-value count. `release_year` is deliberately excluded — it lives on the
/// joined `content` table (`content.release_year`), not on `torrent_contents` —
/// and so is `video_3d`, which is outside this change's documented extension
/// set. Multi-valued or join/EXISTS-backed facets (`language`, `torrent_tag`,
/// `file_type`, `content_genre`, `torrent_source`) can never be grouped this
/// way.
const fn grouped_facet_column(facet: TorrentContentFacet) -> Option<&'static str> {
    match facet {
        TorrentContentFacet::ContentType => Some("torrent_contents.content_type"),
        TorrentContentFacet::VideoResolution => Some("torrent_contents.video_resolution"),
        TorrentContentFacet::VideoSource => Some("torrent_contents.video_source"),
        TorrentContentFacet::VideoCodec => Some("torrent_contents.video_codec"),
        TorrentContentFacet::VideoModifier => Some("torrent_contents.video_modifier"),
        TorrentContentFacet::Video3D
        | TorrentContentFacet::ReleaseYear
        | TorrentContentFacet::TorrentSource
        | TorrentContentFacet::TorrentTag
        | TorrentContentFacet::FileType
        | TorrentContentFacet::Language
        | TorrentContentFacet::ContentGenre => None,
    }
}

/// Wrap [`build_base_query`]'s membership SQL as a grouped count over `column`.
///
/// The base query is reused verbatim; only its lean `info_hash` projection is
/// swapped for `<column>::text, count(*)` and a trailing `GROUP BY 1`. The
/// `::text` cast matches the item query's enum-to-text projection so the grouped
/// keys line up with the facet vocabulary.
fn grouped_facet_sql(column: &str, base_sql: &str) -> String {
    let remainder = base_sql
        .strip_prefix(BASE_SELECT)
        .expect("grouped facet base query must start with the canonical base select");
    format!(
        "SELECT {column}::text AS facet_value, count(*) AS count\nFROM torrent_contents{remainder}\nGROUP BY 1"
    )
}

async fn fetch_facet_group_grouped(
    pool: &PgPool,
    options: &SearchOptions,
    ctx: &CriteriaCtx<'_>,
    facet: &FacetRequest,
    current_facet: Option<&str>,
    column: &str,
    values: &BTreeMap<String, String>,
) -> Result<Vec<(String, AggregationItem)>> {
    let mut state = BuildState::default();
    let base_sql = build_base_query(options, ctx, current_facet, None, false, &mut state)?;
    let base_sql = inline_binds(&base_sql, state.binds());
    let grouped_sql = grouped_facet_sql(column, &base_sql);

    let rows = sqlx::query(sqlx::AssertSqlSafe(grouped_sql))
        .fetch_all(pool)
        .await?;
    let mut counts = BTreeMap::new();
    for row in rows {
        // A NULL column value maps to the same `"null"` bucket key the per-value
        // path uses; unknown non-null values are ignored below.
        let value: Option<String> = row.try_get("facet_value")?;
        let count: i64 = row.try_get("count")?;
        counts.insert(value.unwrap_or_else(|| "null".to_owned()), nonnegative_count(count)?);
    }

    let mut items = Vec::new();
    for (value, label) in values {
        let count = counts.get(value).copied().unwrap_or(0);
        if count > 0 || facet.filter.contains(value) {
            items.push((
                value.clone(),
                AggregationItem {
                    label: label.clone(),
                    count,
                    is_estimate: false,
                },
            ));
        }
    }
    Ok(items)
}

struct PreparedFacet {
    order: usize,
    facet: Arc<FacetRequest>,
    logic: FacetLogic,
    current_facet: Option<&'static str>,
    values: BTreeMap<String, String>,
}

struct FacetItemWork {
    group_index: usize,
    facet: Arc<FacetRequest>,
    logic: FacetLogic,
    current_facet: Option<&'static str>,
    value: String,
    label: String,
}

async fn prepare_facet_group(
    pool: &PgPool,
    order: usize,
    facet: Arc<FacetRequest>,
) -> Result<PreparedFacet> {
    let values = facet_values(pool, facet.facet).await?;
    let logic = effective_logic(&facet);
    let current_facet = (logic == FacetLogic::Or).then(|| facet.facet.key());

    Ok(PreparedFacet {
        order,
        facet,
        logic,
        current_facet,
        values,
    })
}

async fn try_collect_facet_work<Groups, Group, Work, Run, Fut, T>(
    groups: Groups,
    run: Run,
) -> Result<Vec<T>>
where
    Groups: IntoIterator<Item = Group>,
    Group: IntoIterator<Item = Work>,
    Run: FnMut(Work) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut groups = groups
        .into_iter()
        .map(IntoIterator::into_iter)
        .collect::<VecDeque<_>>();
    let round_robin = std::iter::from_fn(move || loop {
        let mut group = groups.pop_front()?;
        if let Some(work) = group.next() {
            groups.push_back(group);
            return Some(work);
        }
    });

    stream::iter(round_robin)
        .map(run)
        .buffer_unordered(FACET_DB_CONCURRENCY)
        .try_collect()
        .await
}

#[allow(clippy::too_many_arguments)]
async fn fetch_facet_item(
    pool: &PgPool,
    options: &SearchOptions,
    ctx: &CriteriaCtx<'_>,
    facet: &FacetRequest,
    logic: FacetLogic,
    current_facet: Option<&str>,
    value: String,
    label: String,
) -> Result<Option<(String, AggregationItem)>> {
    let Some(value_criterion) = facet_value_criterion(facet.facet, &value)? else {
        return Ok(None);
    };
    let extra = match logic {
        FacetLogic::And => Criteria::And(vec![value_criterion]),
        FacetLogic::Or => Criteria::Or(vec![value_criterion]),
    };
    let mut state = BuildState::default();
    let inner_sql = build_base_query(options, ctx, current_facet, Some(&extra), false, &mut state)?;
    let inner_sql = inline_binds(&inner_sql, state.binds());
    let (count, budget_exceeded) =
        budgeted_count(pool, &inner_sql, options.aggregation_budget).await?;

    Ok(
        (count > 0 || budget_exceeded || facet.filter.contains(&value)).then_some((
            value,
            AggregationItem {
                label,
                count,
                is_estimate: budget_exceeded,
            },
        )),
    )
}

impl AggregationGroup {
    /// Return buckets in Go GraphQL presentation order: natural label order,
    /// with the `"null"` bucket appended last.
    ///
    /// This mirrors `internal/gql/gqlmodel/facet.go` `aggs`. Ordering is only a
    /// presentation concern; the parity-gated quantity remains each item's
    /// count.
    pub fn sorted_items(&self) -> Vec<(&String, &AggregationItem)> {
        let mut items = self
            .items
            .iter()
            .filter(|(value, _)| value.as_str() != "null")
            .collect::<Vec<_>>();
        items.sort_by(|(left_value, left), (right_value, right)| {
            natural_cmp(&left.label, &right.label).then_with(|| left_value.cmp(right_value))
        });
        if let Some(null) = self.items.get_key_value("null") {
            items.push(null);
        }
        items
    }
}

/// Produce the shared GraphQL joins + predicate query, with no order or limit.
///
/// S3 uses this for facet counts; S4 reuses the same base predicate for item
/// membership so the two execution paths cannot drift.
pub(crate) fn build_base_query(
    options: &SearchOptions,
    ctx: &CriteriaCtx<'_>,
    current_facet: Option<&str>,
    extra: Option<&Criteria>,
    require_torrents_for_order: bool,
    state: &mut BuildState,
) -> Result<String> {
    let mut required_joins = RequiredJoins::default();
    if require_torrents_for_order {
        required_joins.require_torrents();
    }
    let mut where_conditions = Vec::new();

    if let Some(query) = options.query.as_deref().filter(|query| !query.is_empty()) {
        let placeholder =
            state.push_bind(Bind::Tsquery(bitmagnet_fts::app_query_to_tsquery(query)));
        where_conditions.push(format!("torrent_contents.tsv @@ {placeholder}::tsquery"));
    }

    if let Some(filter) = &options.filter {
        required_joins.extend(RequiredJoins::from_criteria(filter));
        where_conditions.push(criteria_sql(filter, state, ctx)?);
    }

    let mut cross_facet = Vec::new();
    for facet in options
        .facets
        .iter()
        .filter(|facet| !facet.filter.is_empty())
    {
        if effective_logic(facet) == FacetLogic::Or && current_facet == Some(facet.facet.key()) {
            continue;
        }
        let criteria = facet_criteria(facet)?;
        required_joins.extend(RequiredJoins::from_criteria(&criteria));
        cross_facet.push(criteria);
    }
    if !cross_facet.is_empty() {
        let criteria = Criteria::And(cross_facet);
        where_conditions.push(criteria_sql(&criteria, state, ctx)?);
    }

    if let Some(extra) = extra {
        required_joins.extend(RequiredJoins::from_criteria(extra));
        where_conditions.push(criteria_sql(extra, state, ctx)?);
    }

    let mut sql = String::from(BASE_SELECT);
    required_joins.append_sql(&mut sql);
    if !where_conditions.is_empty() {
        sql.push_str("\nWHERE ");
        sql.push_str(&where_conditions.join("\n  AND "));
    }
    Ok(sql)
}

/// Execute Go's `doCount` query for the fully filtered result set.
///
/// Ordering-only joins are deliberately absent: Go calls `newSubQuery` with
/// `withOrder=false` for counts, so a `name` sort must not widen the count SQL.
pub(crate) async fn fetch_total_count(
    pool: &PgPool,
    options: &SearchOptions,
    config: &SearchBuildConfig,
    now: DateTime<Utc>,
) -> Result<(u64, bool)> {
    let ctx = CriteriaCtx::new(config, now);
    let mut state = BuildState::default();
    let inner_sql = build_base_query(options, &ctx, None, None, false, &mut state)?;
    let inner_sql = inline_binds(&inner_sql, state.binds());
    budgeted_count(pool, &inner_sql, options.aggregation_budget).await
}

fn facet_criteria(facet: &FacetRequest) -> Result<Criteria> {
    if facet.filter.is_empty() {
        return Ok(Criteria::And(Vec::new()));
    }

    if effective_logic(facet) == FacetLogic::And {
        let mut criteria = Vec::with_capacity(facet.filter.len());
        for value in &facet.filter {
            if let Some(criterion) = facet_value_criterion(facet.facet, value)? {
                criteria.push(criterion);
            }
        }
        return Ok(Criteria::And(criteria));
    }

    match facet.facet {
        TorrentContentFacet::ContentType => {
            let has_null = facet.filter.contains("null");
            let values = facet
                .filter
                .iter()
                .filter(|value| value.as_str() != "null")
                .map(|value| parse_content_type(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(nullable_or_criteria(
                (!values.is_empty()).then_some(Criteria::ContentTypeIn(values)),
                has_null,
                TorrentContentAttribute::ContentType,
            ))
        }
        TorrentContentFacet::TorrentSource => Ok(Criteria::TorrentSourceIn(
            facet.filter.iter().cloned().collect(),
        )),
        TorrentContentFacet::TorrentTag => Ok(Criteria::Or(
            facet
                .filter
                .iter()
                .cloned()
                .map(|value| Criteria::TorrentTag(vec![value]))
                .collect(),
        )),
        TorrentContentFacet::FileType => Ok(Criteria::TorrentFileTypeIn(
            facet
                .filter
                .iter()
                .map(|value| parse_file_type(value))
                .collect::<Result<Vec<_>>>()?,
        )),
        TorrentContentFacet::Language => {
            Ok(Criteria::LanguageIn(facet.filter.iter().cloned().collect()))
        }
        TorrentContentFacet::ContentGenre => {
            let refs = facet
                .filter
                .iter()
                .filter_map(|value| genre_ref(value))
                .collect::<Vec<_>>();
            Ok(if refs.is_empty() {
                Criteria::And(Vec::new())
            } else {
                Criteria::ContentGenre(refs)
            })
        }
        TorrentContentFacet::ReleaseYear => {
            let has_null = facet.filter.contains("null");
            let years = facet
                .filter
                .iter()
                .filter(|value| value.as_str() != "null")
                .map(|value| parse_release_year(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(nullable_or_criteria(
                (!years.is_empty()).then_some(Criteria::ReleaseYearIn(years)),
                has_null,
                TorrentContentAttribute::ReleaseYear,
            ))
        }
        TorrentContentFacet::VideoResolution => {
            let has_null = facet.filter.contains("null");
            let values = facet
                .filter
                .iter()
                .filter(|value| value.as_str() != "null")
                .map(|value| parse_video_resolution(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(nullable_or_criteria(
                (!values.is_empty()).then_some(Criteria::VideoResolutionIn(values)),
                has_null,
                TorrentContentAttribute::VideoResolution,
            ))
        }
        TorrentContentFacet::VideoSource => {
            let has_null = facet.filter.contains("null");
            let values = facet
                .filter
                .iter()
                .filter(|value| value.as_str() != "null")
                .map(|value| parse_video_source(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(nullable_or_criteria(
                (!values.is_empty()).then_some(Criteria::VideoSourceIn(values)),
                has_null,
                TorrentContentAttribute::VideoSource,
            ))
        }
        TorrentContentFacet::Video3D => {
            let has_null = facet.filter.contains("null");
            let values = facet
                .filter
                .iter()
                .filter(|value| value.as_str() != "null")
                .map(|value| parse_video_3d(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(nullable_or_criteria(
                (!values.is_empty()).then_some(Criteria::Video3DIn(values)),
                has_null,
                TorrentContentAttribute::Video3D,
            ))
        }
        TorrentContentFacet::VideoCodec => {
            let has_null = facet.filter.contains("null");
            let values = facet
                .filter
                .iter()
                .filter(|value| value.as_str() != "null")
                .map(|value| parse_video_codec(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(nullable_or_criteria(
                (!values.is_empty()).then_some(Criteria::VideoCodecIn(values)),
                has_null,
                TorrentContentAttribute::VideoCodec,
            ))
        }
        TorrentContentFacet::VideoModifier => {
            let has_null = facet.filter.contains("null");
            let values = facet
                .filter
                .iter()
                .filter(|value| value.as_str() != "null")
                .map(|value| parse_video_modifier(value))
                .collect::<Result<Vec<_>>>()?;
            Ok(nullable_or_criteria(
                (!values.is_empty()).then_some(Criteria::VideoModifierIn(values)),
                has_null,
                TorrentContentAttribute::VideoModifier,
            ))
        }
    }
}

fn facet_value_criterion(facet: TorrentContentFacet, value: &str) -> Result<Option<Criteria>> {
    Ok(Some(match facet {
        TorrentContentFacet::ContentType if value == "null" => {
            Criteria::IsNull(TorrentContentAttribute::ContentType)
        }
        TorrentContentFacet::ContentType => {
            Criteria::ContentTypeIn(vec![parse_content_type(value)?])
        }
        TorrentContentFacet::TorrentSource => Criteria::TorrentSourceIn(vec![value.to_owned()]),
        TorrentContentFacet::TorrentTag => Criteria::TorrentTag(vec![value.to_owned()]),
        TorrentContentFacet::FileType => Criteria::TorrentFileTypeIn(vec![parse_file_type(value)?]),
        TorrentContentFacet::Language => Criteria::LanguageIn(vec![value.to_owned()]),
        TorrentContentFacet::ContentGenre => {
            let Some(reference) = genre_ref(value) else {
                return Ok(None);
            };
            Criteria::ContentGenre(vec![reference])
        }
        TorrentContentFacet::ReleaseYear if value == "null" => {
            Criteria::IsNull(TorrentContentAttribute::ReleaseYear)
        }
        TorrentContentFacet::ReleaseYear => {
            Criteria::ReleaseYearIn(vec![parse_release_year(value)?])
        }
        TorrentContentFacet::VideoResolution if value == "null" => {
            Criteria::IsNull(TorrentContentAttribute::VideoResolution)
        }
        TorrentContentFacet::VideoResolution => {
            Criteria::VideoResolutionIn(vec![parse_video_resolution(value)?])
        }
        TorrentContentFacet::VideoSource if value == "null" => {
            Criteria::IsNull(TorrentContentAttribute::VideoSource)
        }
        TorrentContentFacet::VideoSource => {
            Criteria::VideoSourceIn(vec![parse_video_source(value)?])
        }
        TorrentContentFacet::Video3D if value == "null" => {
            Criteria::IsNull(TorrentContentAttribute::Video3D)
        }
        TorrentContentFacet::Video3D => Criteria::Video3DIn(vec![parse_video_3d(value)?]),
        TorrentContentFacet::VideoCodec if value == "null" => {
            Criteria::IsNull(TorrentContentAttribute::VideoCodec)
        }
        TorrentContentFacet::VideoCodec => Criteria::VideoCodecIn(vec![parse_video_codec(value)?]),
        TorrentContentFacet::VideoModifier if value == "null" => {
            Criteria::IsNull(TorrentContentAttribute::VideoModifier)
        }
        TorrentContentFacet::VideoModifier => {
            Criteria::VideoModifierIn(vec![parse_video_modifier(value)?])
        }
    }))
}

fn nullable_or_criteria(
    non_null: Option<Criteria>,
    has_null: bool,
    attribute: TorrentContentAttribute,
) -> Criteria {
    if has_null {
        let mut criteria = non_null.into_iter().collect::<Vec<_>>();
        criteria.push(Criteria::IsNull(attribute));
        Criteria::Or(criteria)
    } else {
        non_null.unwrap_or_else(|| Criteria::And(Vec::new()))
    }
}

fn genre_ref(value: &str) -> Option<ContentCollectionRef> {
    let mut parts = value.split(':');
    let source = parts.next()?;
    let id = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some(ContentCollectionRef {
        collection_type: Some("genre".to_owned()),
        source: source.to_owned(),
        id: id.to_owned(),
    })
}

fn parse_content_type(value: &str) -> Result<ContentType> {
    value
        .parse()
        .map_err(|_| invalid_facet_value(TorrentContentFacet::ContentType, value))
}

fn parse_file_type(value: &str) -> Result<FileType> {
    value
        .parse()
        .map_err(|_| invalid_facet_value(TorrentContentFacet::FileType, value))
}

fn parse_release_year(value: &str) -> Result<u16> {
    value
        .parse::<u16>()
        .ok()
        .filter(|year| (1000..=9999).contains(year))
        .ok_or_else(|| invalid_facet_value(TorrentContentFacet::ReleaseYear, value))
}

fn parse_video_resolution(value: &str) -> Result<VideoResolution> {
    match value {
        "V360p" => Ok(VideoResolution::V360p),
        "V480p" => Ok(VideoResolution::V480p),
        "V540p" => Ok(VideoResolution::V540p),
        "V576p" => Ok(VideoResolution::V576p),
        "V720p" => Ok(VideoResolution::V720p),
        "V1080p" => Ok(VideoResolution::V1080p),
        "V1440p" => Ok(VideoResolution::V1440p),
        "V2160p" => Ok(VideoResolution::V2160p),
        "V4320p" => Ok(VideoResolution::V4320p),
        _ => Err(invalid_facet_value(
            TorrentContentFacet::VideoResolution,
            value,
        )),
    }
}

fn parse_video_source(value: &str) -> Result<VideoSource> {
    match value {
        "CAM" => Ok(VideoSource::Cam),
        "TELESYNC" => Ok(VideoSource::Telesync),
        "TELECINE" => Ok(VideoSource::Telecine),
        "WORKPRINT" => Ok(VideoSource::Workprint),
        "DVD" => Ok(VideoSource::Dvd),
        "TV" => Ok(VideoSource::Tv),
        "WEBDL" => Ok(VideoSource::WebDl),
        "WEBRip" => Ok(VideoSource::WebRip),
        "BluRay" => Ok(VideoSource::BluRay),
        _ => Err(invalid_facet_value(TorrentContentFacet::VideoSource, value)),
    }
}

fn parse_video_3d(value: &str) -> Result<Video3D> {
    match value {
        "V3D" => Ok(Video3D::V3D),
        "V3DSBS" => Ok(Video3D::V3DSBS),
        "V3DOU" => Ok(Video3D::V3DOU),
        _ => Err(invalid_facet_value(TorrentContentFacet::Video3D, value)),
    }
}

fn parse_video_codec(value: &str) -> Result<VideoCodec> {
    match value {
        "H264" => Ok(VideoCodec::H264),
        "x264" => Ok(VideoCodec::X264),
        "x265" => Ok(VideoCodec::X265),
        "XviD" => Ok(VideoCodec::XviD),
        "DivX" => Ok(VideoCodec::DivX),
        "MPEG2" => Ok(VideoCodec::Mpeg2),
        "MPEG4" => Ok(VideoCodec::Mpeg4),
        _ => Err(invalid_facet_value(TorrentContentFacet::VideoCodec, value)),
    }
}

fn parse_video_modifier(value: &str) -> Result<VideoModifier> {
    match value {
        "REGIONAL" => Ok(VideoModifier::Regional),
        "SCREENER" => Ok(VideoModifier::Screener),
        "RAWHD" => Ok(VideoModifier::RawHd),
        "BRDISK" => Ok(VideoModifier::BrDisk),
        "REMUX" => Ok(VideoModifier::Remux),
        _ => Err(invalid_facet_value(
            TorrentContentFacet::VideoModifier,
            value,
        )),
    }
}

fn invalid_facet_value(facet: TorrentContentFacet, value: &str) -> SearchQueryError {
    SearchQueryError::InvalidParams(format!("invalid {} facet value: {value:?}", facet.key()))
}

fn effective_logic(facet: &FacetRequest) -> FacetLogic {
    facet.logic.unwrap_or_else(|| default_logic(facet.facet))
}

const fn default_logic(facet: TorrentContentFacet) -> FacetLogic {
    match facet {
        TorrentContentFacet::TorrentTag
        | TorrentContentFacet::ContentGenre
        | TorrentContentFacet::FileType => FacetLogic::And,
        _ => FacetLogic::Or,
    }
}

const fn facet_label(facet: TorrentContentFacet) -> &'static str {
    match facet {
        TorrentContentFacet::ContentType => "Content Type",
        TorrentContentFacet::TorrentSource => "Torrent Source",
        TorrentContentFacet::TorrentTag => "Torrent Tag",
        TorrentContentFacet::FileType => "File Type",
        TorrentContentFacet::Language => "Language",
        TorrentContentFacet::ContentGenre => "Genre",
        TorrentContentFacet::ReleaseYear => "Release Year",
        TorrentContentFacet::VideoResolution => "Video Resolution",
        TorrentContentFacet::VideoSource => "Video Source",
        TorrentContentFacet::Video3D => "Video 3D",
        TorrentContentFacet::VideoCodec => "Video Codec",
        TorrentContentFacet::VideoModifier => "Video Modifier",
    }
}

async fn facet_values(
    pool: &PgPool,
    facet: TorrentContentFacet,
) -> Result<BTreeMap<String, String>> {
    match facet {
        TorrentContentFacet::ContentType => Ok(static_values(CONTENT_TYPE_VALUES)),
        TorrentContentFacet::FileType => Ok(static_values(FILE_TYPE_VALUES)),
        TorrentContentFacet::Language => Ok(static_values(LANGUAGE_VALUES)),
        TorrentContentFacet::VideoResolution => Ok(static_values(VIDEO_RESOLUTION_VALUES)),
        TorrentContentFacet::VideoSource => Ok(static_values(VIDEO_SOURCE_VALUES)),
        TorrentContentFacet::Video3D => Ok(static_values(VIDEO_3D_VALUES)),
        TorrentContentFacet::VideoCodec => Ok(static_values(VIDEO_CODEC_VALUES)),
        TorrentContentFacet::VideoModifier => Ok(static_values(VIDEO_MODIFIER_VALUES)),
        TorrentContentFacet::TorrentSource => {
            let rows = sqlx::query(TORRENT_SOURCE_VALUES_SQL)
                .fetch_all(pool)
                .await?;
            let mut values = BTreeMap::new();
            for row in rows {
                values.insert(row.try_get("key")?, row.try_get("name")?);
            }
            Ok(values)
        }
        TorrentContentFacet::TorrentTag => {
            let rows = sqlx::query(TORRENT_TAG_VALUES_SQL).fetch_all(pool).await?;
            let mut values = BTreeMap::new();
            for row in rows {
                let name: String = row.try_get("name")?;
                values.insert(name.clone(), name);
            }
            Ok(values)
        }
        TorrentContentFacet::ContentGenre => {
            let rows = sqlx::query(CONTENT_GENRE_VALUES_SQL)
                .fetch_all(pool)
                .await?;
            let mut values = BTreeMap::new();
            for row in rows {
                let source: String = row.try_get("source")?;
                let id: String = row.try_get("id")?;
                let name: String = row.try_get("name")?;
                values.insert(format!("{source}:{id}"), name);
            }
            Ok(values)
        }
        TorrentContentFacet::ReleaseYear => {
            let rows = sqlx::query(RELEASE_YEAR_VALUES_SQL).fetch_all(pool).await?;
            let mut values = BTreeMap::from([("null".to_owned(), "Unknown".to_owned())]);
            for row in rows {
                let year: i32 = row.try_get("release_year")?;
                let year = year.to_string();
                values.insert(year.clone(), year);
            }
            Ok(values)
        }
    }
}

fn static_values(values: &[(&str, &str)]) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(value, label)| ((*value).to_owned(), (*label).to_owned()))
        .collect()
}

async fn budgeted_count(pool: &PgPool, inner_sql: &str, budget: f64) -> Result<(u64, bool)> {
    if budget > 0.0 {
        let row = sqlx::query(BUDGETED_COUNT_SQL)
            .bind(inner_sql)
            .bind(budget)
            .fetch_one(pool)
            .await?;
        let count: i32 = row.try_get("count")?;
        let budget_exceeded: bool = row.try_get("budget_exceeded")?;
        return Ok((nonnegative_count(i64::from(count))?, budget_exceeded));
    }

    let sql = format!("SELECT count(*) AS count FROM ({inner_sql}) t");
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_one(pool)
        .await?;
    let count: i64 = row.try_get("count")?;
    Ok((nonnegative_count(count)?, false))
}

fn nonnegative_count(count: i64) -> Result<u64> {
    u64::try_from(count).map_err(|_| {
        SearchQueryError::InvalidParams("database returned a negative facet count".to_owned())
    })
}

fn inline_binds(sql: &str, binds: &[Bind]) -> String {
    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut cursor = 0;

    while let Some(relative) = sql[cursor..].find('$') {
        let dollar = cursor + relative;
        output.push_str(&sql[cursor..dollar]);
        let mut end = dollar + 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }

        if end == dollar + 1 {
            output.push('$');
            cursor = end;
            continue;
        }

        let index = sql[dollar + 1..end].parse::<usize>().ok();
        if let Some(bind) = index
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| binds.get(index))
        {
            output.push_str(&bind_literal(bind));
        } else {
            output.push_str(&sql[dollar..end]);
        }
        cursor = end;
    }

    output.push_str(&sql[cursor..]);
    output
}

fn bind_literal(bind: &Bind) -> String {
    match bind {
        Bind::Text(value) => quote_literal(value),
        Bind::Tsquery(value) => format!("{}::tsquery", quote_literal(value)),
        Bind::Timestamp(value) => format!("{}::timestamptz", quote_literal(value)),
        Bind::TextArray(values) => format!(
            "array[{}]::text[]",
            values
                .iter()
                .map(|value| quote_literal(value))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Bind::Bytea(bytes) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut value = String::with_capacity(bytes.len() * 2);
            for byte in bytes {
                value.push(HEX[usize::from(byte >> 4)] as char);
                value.push(HEX[usize::from(byte & 0x0f)] as char);
            }
            format!("'\\x{value}'::bytea")
        }
    }
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn natural_cmp(left: &str, right: &str) -> Ordering {
    let mut left = left;
    let mut right = right;

    while !left.is_empty() && !right.is_empty() {
        let (left_run, left_rest, left_is_digit) = next_run(left);
        let (right_run, right_rest, right_is_digit) = next_run(right);
        let ordering = if left_is_digit && right_is_digit {
            numeric_run_cmp(left_run, right_run)
        } else {
            left_run.cmp(right_run)
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
        left = left_rest;
        right = right_rest;
    }

    left.len().cmp(&right.len())
}

fn next_run(value: &str) -> (&str, &str, bool) {
    let mut chars = value.char_indices();
    let (_, first) = chars.next().expect("next_run requires a non-empty string");
    let is_digit = first.is_ascii_digit();
    for (index, character) in chars {
        if character.is_ascii_digit() != is_digit {
            return (&value[..index], &value[index..], is_digit);
        }
    }
    (value, "", is_digit)
}

fn numeric_run_cmp(left: &str, right: &str) -> Ordering {
    let left = left.trim_start_matches('0');
    let right = right.trim_start_matches('0');
    let left = if left.is_empty() { "0" } else { left };
    let right = if right.is_empty() { "0" } else { right };
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    };
    use tokio::sync::Semaphore;

    fn request(facet: TorrentContentFacet, values: &[&str]) -> FacetRequest {
        FacetRequest {
            facet,
            aggregate: true,
            logic: None,
            filter: values.iter().map(|value| (*value).to_owned()).collect(),
        }
    }

    fn fixed_now() -> DateTime<Utc> {
        "2026-07-12T12:00:00Z".parse().unwrap()
    }

    #[tokio::test]
    async fn facet_group_fanout_overlaps_with_one_global_cap() {
        const EXPECTED_CONCURRENCY: usize = 4;
        const GROUPS: usize = 2;
        const ITEMS_PER_GROUP: usize = EXPECTED_CONCURRENCY;
        const WORK_ITEMS: usize = GROUPS * ITEMS_PER_GROUP;

        assert_eq!(FACET_DB_CONCURRENCY, EXPECTED_CONCURRENCY);

        let active = Arc::new(AtomicUsize::new(0));
        let active_by_group =
            Arc::new((0..GROUPS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let work = (0..GROUPS)
            .map(|group| (0..ITEMS_PER_GROUP).map(move |index| (group, index)))
            .collect::<Vec<_>>();
        let run_active = Arc::clone(&active);
        let run_active_by_group = Arc::clone(&active_by_group);
        let run_peak = Arc::clone(&peak);
        let run_started = Arc::clone(&started);
        let run_release = Arc::clone(&release);
        let mut collection = Box::pin(try_collect_facet_work(work, move |(group, index)| {
            let active = Arc::clone(&run_active);
            let active_by_group = Arc::clone(&run_active_by_group);
            let peak = Arc::clone(&run_peak);
            let started = Arc::clone(&run_started);
            let release = Arc::clone(&run_release);
            async move {
                let current = active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                active_by_group[group].fetch_add(1, AtomicOrdering::SeqCst);
                peak.fetch_max(current, AtomicOrdering::SeqCst);
                started.fetch_add(1, AtomicOrdering::SeqCst);

                let permit = release.acquire().await.expect("release semaphore is open");
                permit.forget();
                active_by_group[group].fetch_sub(1, AtomicOrdering::SeqCst);
                active.fetch_sub(1, AtomicOrdering::SeqCst);
                Ok((group, index))
            }
        }));
        assert!(futures::poll!(&mut collection).is_pending());

        assert_eq!(active.load(AtomicOrdering::SeqCst), EXPECTED_CONCURRENCY);
        assert_eq!(peak.load(AtomicOrdering::SeqCst), EXPECTED_CONCURRENCY);
        assert_eq!(started.load(AtomicOrdering::SeqCst), EXPECTED_CONCURRENCY);
        assert_eq!(active_by_group[0].load(AtomicOrdering::SeqCst), 2);
        assert_eq!(active_by_group[1].load(AtomicOrdering::SeqCst), 2);

        release.add_permits(WORK_ITEMS);
        let mut completed = collection.await.unwrap();
        completed.sort_unstable();

        assert_eq!(
            completed,
            (0..GROUPS)
                .flat_map(|group| (0..ITEMS_PER_GROUP).map(move |index| (group, index)))
                .collect::<Vec<_>>()
        );
        assert_eq!(active.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(peak.load(AtomicOrdering::SeqCst), EXPECTED_CONCURRENCY);
    }

    #[tokio::test]
    async fn facet_group_fanout_fails_closed_and_drops_pending_tasks() {
        struct ActiveGuard(Arc<AtomicUsize>);

        impl Drop for ActiveGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, AtomicOrdering::SeqCst);
            }
        }

        const WORK_ITEMS: usize = FACET_DB_CONCURRENCY * 3;
        let active = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let never_release = Arc::new(Semaphore::new(0));
        let work = (0..2)
            .map(|group| (0..WORK_ITEMS / 2).map(move |index| (group, index)))
            .collect::<Vec<_>>();
        let run_active = Arc::clone(&active);
        let run_started = Arc::clone(&started);
        let run_never_release = Arc::clone(&never_release);

        let error = try_collect_facet_work(work, move |(group, index)| {
            let active = Arc::clone(&run_active);
            let started = Arc::clone(&run_started);
            let never_release = Arc::clone(&run_never_release);
            async move {
                active.fetch_add(1, AtomicOrdering::SeqCst);
                started.fetch_add(1, AtomicOrdering::SeqCst);
                let _active = ActiveGuard(active);

                if group == 1 && index == 0 {
                    return Err(SearchQueryError::InvalidParams(
                        "synthetic facet failure".to_owned(),
                    ));
                }

                let permit = never_release
                    .acquire()
                    .await
                    .expect("test semaphore remains open");
                permit.forget();
                Ok((group, index))
            }
        })
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid search params: synthetic facet failure"
        );
        assert_eq!(active.load(AtomicOrdering::SeqCst), 0);
        // Dropping the Rust futures prevents new work from starting. SQLx may
        // still have to drain already-issued PostgreSQL queries before their
        // pooled connections become reusable.
        assert!(started.load(AtomicOrdering::SeqCst) <= FACET_DB_CONCURRENCY);
        assert!(started.load(AtomicOrdering::SeqCst) < WORK_ITEMS);
    }

    #[test]
    fn inline_binds_renders_every_bind_variant() {
        let sql = "SELECT $1, $2, $3, $4, $5, $6, $7";
        let binds = vec![
            Bind::Text("O'Brien".to_owned()),
            Bind::Tsquery("foo'bar".to_owned()),
            Bind::Timestamp("2026-07-12T12:00:00Z".to_owned()),
            Bind::TextArray(vec!["alpha".to_owned(), "b'eta".to_owned()]),
            Bind::TextArray(Vec::new()),
            Bind::Bytea(vec![0x00, 0xab, 0xff]),
            Bind::Text("last".to_owned()),
        ];

        assert_eq!(
            inline_binds(sql, &binds),
            "SELECT 'O''Brien', 'foo''bar'::tsquery, '2026-07-12T12:00:00Z'::timestamptz, array['alpha','b''eta']::text[], array[]::text[], '\\x00abff'::bytea, 'last'"
        );
    }

    #[test]
    fn inline_binds_scans_the_whole_placeholder_number() {
        let binds = (1..=10)
            .map(|index| Bind::Text(format!("value-{index}")))
            .collect::<Vec<_>>();
        assert_eq!(
            inline_binds("SELECT $1, $10, $2", &binds),
            "SELECT 'value-1', 'value-10', 'value-2'"
        );
    }

    #[test]
    fn facet_criteria_cover_every_facet_and_null_bucket() {
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::ContentType,
                &["movie", "null"]
            ))
            .unwrap(),
            Criteria::Or(vec![
                Criteria::ContentTypeIn(vec![ContentType::Movie]),
                Criteria::IsNull(TorrentContentAttribute::ContentType),
            ])
        );
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::TorrentSource,
                &["dht", "rss"]
            ))
            .unwrap(),
            Criteria::TorrentSourceIn(vec!["dht".to_owned(), "rss".to_owned()])
        );
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::TorrentTag,
                &["approved", "trusted"]
            ))
            .unwrap(),
            Criteria::And(vec![
                Criteria::TorrentTag(vec!["approved".to_owned()]),
                Criteria::TorrentTag(vec!["trusted".to_owned()]),
            ])
        );
        assert_eq!(
            facet_criteria(&request(TorrentContentFacet::FileType, &["audio", "video"])).unwrap(),
            Criteria::And(vec![
                Criteria::TorrentFileTypeIn(vec![FileType::Audio]),
                Criteria::TorrentFileTypeIn(vec![FileType::Video]),
            ])
        );
        assert_eq!(
            facet_criteria(&request(TorrentContentFacet::Language, &["en", "fr"])).unwrap(),
            Criteria::LanguageIn(vec!["en".to_owned(), "fr".to_owned()])
        );
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::ContentGenre,
                &["imdb:action", "tmdb:18"]
            ))
            .unwrap(),
            Criteria::And(vec![
                Criteria::ContentGenre(vec![genre_ref("imdb:action").unwrap()]),
                Criteria::ContentGenre(vec![genre_ref("tmdb:18").unwrap()]),
            ])
        );
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::ReleaseYear,
                &["1999", "null"]
            ))
            .unwrap(),
            Criteria::Or(vec![
                Criteria::ReleaseYearIn(vec![1999]),
                Criteria::IsNull(TorrentContentAttribute::ReleaseYear),
            ])
        );
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::VideoResolution,
                &["V1080p", "null"]
            ))
            .unwrap(),
            Criteria::Or(vec![
                Criteria::VideoResolutionIn(vec![VideoResolution::V1080p]),
                Criteria::IsNull(TorrentContentAttribute::VideoResolution),
            ])
        );
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::VideoSource,
                &["BluRay", "null"]
            ))
            .unwrap(),
            Criteria::Or(vec![
                Criteria::VideoSourceIn(vec![VideoSource::BluRay]),
                Criteria::IsNull(TorrentContentAttribute::VideoSource),
            ])
        );
        assert_eq!(
            facet_criteria(&request(TorrentContentFacet::Video3D, &["V3DSBS", "null"])).unwrap(),
            Criteria::Or(vec![
                Criteria::Video3DIn(vec![Video3D::V3DSBS]),
                Criteria::IsNull(TorrentContentAttribute::Video3D),
            ])
        );
        assert_eq!(
            facet_criteria(&request(TorrentContentFacet::VideoCodec, &["H264", "null"])).unwrap(),
            Criteria::Or(vec![
                Criteria::VideoCodecIn(vec![VideoCodec::H264]),
                Criteria::IsNull(TorrentContentAttribute::VideoCodec),
            ])
        );
        assert_eq!(
            facet_criteria(&request(
                TorrentContentFacet::VideoModifier,
                &["REMUX", "null"]
            ))
            .unwrap(),
            Criteria::Or(vec![
                Criteria::VideoModifierIn(vec![VideoModifier::Remux]),
                Criteria::IsNull(TorrentContentAttribute::VideoModifier),
            ])
        );
    }

    #[test]
    fn logic_override_changes_per_value_grouping() {
        let mut facet = request(TorrentContentFacet::TorrentSource, &["dht", "rss"]);
        facet.logic = Some(FacetLogic::And);
        assert_eq!(
            facet_criteria(&facet).unwrap(),
            Criteria::And(vec![
                Criteria::TorrentSourceIn(vec!["dht".to_owned()]),
                Criteria::TorrentSourceIn(vec!["rss".to_owned()]),
            ])
        );

        let mut facet = request(TorrentContentFacet::FileType, &["audio", "video"]);
        facet.logic = Some(FacetLogic::Or);
        assert_eq!(
            facet_criteria(&facet).unwrap(),
            Criteria::TorrentFileTypeIn(vec![FileType::Audio, FileType::Video])
        );
    }

    #[test]
    fn base_query_drops_only_the_current_or_facet() {
        let options = SearchOptions::new()
            .with_query("foo")
            .with_filter(Criteria::SizeRange {
                min: Some(42),
                max: None,
            })
            .with_facets([
                request(TorrentContentFacet::ContentType, &["movie"]),
                request(TorrentContentFacet::TorrentTag, &["trusted"]),
            ]);
        let config = SearchBuildConfig::default();
        let ctx = CriteriaCtx::new(&config, fixed_now());

        let mut state = BuildState::default();
        let sql = build_base_query(&options, &ctx, None, None, false, &mut state).unwrap();
        let sql = inline_binds(&sql, state.binds());
        assert_eq!(
            sql,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nINNER JOIN torrents ON torrent_contents.info_hash = torrents.info_hash\nWHERE torrent_contents.tsv @@ 'foo'::tsquery::tsquery\n  AND torrent_contents.size >= 42\n  AND (torrent_contents.content_type IN ('movie') AND (EXISTS (SELECT 1 FROM torrent_tags WHERE torrent_tags.info_hash = torrents.info_hash AND torrent_tags.name IN ('trusted'))))"
        );

        let mut state = BuildState::default();
        let sql = build_base_query(
            &options,
            &ctx,
            Some(TorrentContentFacet::ContentType.key()),
            None,
            false,
            &mut state,
        )
        .unwrap();
        let sql = inline_binds(&sql, state.binds());
        assert!(!sql.contains("content_type IN"));
        assert!(sql.contains("torrent_tags.name IN ('trusted')"));
        assert!(sql.contains("torrent_contents.tsv @@ 'foo'::tsquery::tsquery"));
        assert!(sql.contains("torrent_contents.size >= 42"));
    }

    #[test]
    fn count_inner_query_is_fully_inlined() {
        let options = SearchOptions::new()
            .with_query("matrix")
            .with_facets([request(TorrentContentFacet::ContentType, &["tv_show"])]);
        let config = SearchBuildConfig::default();
        let ctx = CriteriaCtx::new(&config, fixed_now());
        let extra = Criteria::Or(vec![Criteria::ContentTypeIn(vec![ContentType::Movie])]);
        let mut state = BuildState::default();
        let sql = build_base_query(
            &options,
            &ctx,
            Some(TorrentContentFacet::ContentType.key()),
            Some(&extra),
            false,
            &mut state,
        )
        .unwrap();
        let sql = inline_binds(&sql, state.binds());

        assert!(!sql.contains('$'));
        assert!(sql.contains("torrent_contents.tsv @@ 'matrix'::tsquery::tsquery"));
        assert!(sql.contains("(torrent_contents.content_type IN ('movie'))"));
    }

    #[test]
    fn values_queries_match_the_go_sources() {
        assert_eq!(
            TORRENT_SOURCE_VALUES_SQL,
            "SELECT key, name FROM torrent_sources"
        );
        assert_eq!(
            TORRENT_TAG_VALUES_SQL,
            "SELECT DISTINCT name FROM torrent_tags"
        );
        assert_eq!(
            CONTENT_GENRE_VALUES_SQL,
            "SELECT source, id, name FROM content_collections WHERE type = 'genre'"
        );
        assert_eq!(
            RELEASE_YEAR_VALUES_SQL,
            "SELECT DISTINCT release_year FROM content WHERE release_year >= 1000 AND release_year <= 9999"
        );
        assert_eq!(static_values(LANGUAGE_VALUES).len(), 62);
    }

    #[test]
    fn sorted_items_use_natural_labels_and_put_null_last() {
        let group = AggregationGroup {
            label: "Video Resolution".to_owned(),
            logic: FacetLogic::Or,
            items: BTreeMap::from([
                (
                    "V1080p".to_owned(),
                    AggregationItem {
                        label: "1080p".to_owned(),
                        count: 1,
                        is_estimate: false,
                    },
                ),
                (
                    "V720p".to_owned(),
                    AggregationItem {
                        label: "720p".to_owned(),
                        count: 1,
                        is_estimate: false,
                    },
                ),
                (
                    "null".to_owned(),
                    AggregationItem {
                        label: "Unknown".to_owned(),
                        count: 1,
                        is_estimate: false,
                    },
                ),
            ]),
        };

        assert_eq!(
            group
                .sorted_items()
                .into_iter()
                .map(|(value, _)| value.as_str())
                .collect::<Vec<_>>(),
            vec!["V720p", "V1080p", "null"]
        );
    }

    #[test]
    fn empty_filter_is_a_noop() {
        let facet = FacetRequest {
            facet: TorrentContentFacet::ContentType,
            aggregate: true,
            logic: None,
            filter: BTreeSet::new(),
        };
        assert_eq!(facet_criteria(&facet).unwrap(), Criteria::And(Vec::new()));
    }
}
