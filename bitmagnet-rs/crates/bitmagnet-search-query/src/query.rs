//! SQL construction + execution: [`build_query`] turns a
//! [`TorznabSearchParams`] into a [`SearchQuery`] (a `$N`-parameterised SQL
//! string plus its ordered binds), and [`SearchQuery`] runs it against a
//! `PgPool`.
//!
//! House style (matching `bitmagnet-db`): the runtime `sqlx::query` API with
//! `$N` placeholders and explicit binds — NO compile-time `query!` macros — so
//! the crate builds and unit-tests green without a live database or
//! `DATABASE_URL`. Only the DB-gated integration test (Q3, `#[ignore]`) needs a
//! server.
//!
//! Q2 implements `build_query` and the fetch methods; Q1 fixes the signatures
//! and the bind model so Lane T can code against them today.

use crate::criteria::{ContentRef, Criteria, Episodes, Video3D, VideoResolution};
use crate::order::{OrderDirection, TorrentContentOrderField};
use crate::params::TorznabSearchParams;
use crate::result::SearchResultItem;
use bitmagnet_model::{ContentType, InfoHash};
use serde::Deserialize;
use sqlx::{types::Json, PgPool, Row};
use std::collections::BTreeMap;

/// Errors from building or running a search query.
#[derive(Debug, thiserror::Error)]
pub enum SearchQueryError {
    /// The params could not be lowered to SQL (e.g. an identifier criterion
    /// with no source). Mirrors the Go builder's option/criteria errors.
    #[error("invalid search params: {0}")]
    InvalidParams(String),
    /// A database / execution error.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

/// A `SearchQueryError` result alias.
pub type Result<T> = std::result::Result<T, SearchQueryError>;

/// A single positional bind value for a `$N` placeholder, in declaration order.
///
/// The variants cover exactly the column/parameter types the Torznab subset
/// needs. `Tsquery` is bound as text and cast `::tsquery` in the SQL (Go binds
/// the pre-tokenised tsquery string the same way — see
/// `internal/database/query/query.go` `applyPre`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    /// A `bytea` value (info hash / content id bytes).
    Bytea(Vec<u8>),
    /// A `text` value (content type, resolution, source, id, tag ...).
    Text(String),
    /// A pre-tokenised tsquery string, cast `::tsquery` at the placeholder.
    Tsquery(String),
}

/// A parameterised SQL statement ready to run: the `$N`-placeholder SQL text
/// plus its positional [`Bind`]s.
///
/// Exposing the SQL and binds (rather than only executing) lets Q2's unit tests
/// assert SQL *shape* without a database, and lets Lane G's shadow harness log
/// the exact query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQuery {
    sql: String,
    binds: Vec<Bind>,
}

struct OrderedRow {
    id: String,
    info_hash: InfoHash,
    size: u64,
    content_type: Option<ContentType>,
    published_at: i64,
    files_count: Option<u32>,
    video_resolution: Option<VideoResolution>,
    video_3d: Option<Video3D>,
    video_codec: Option<String>,
    release_group: Option<String>,
    episodes: Episodes,
}

#[derive(Default)]
struct Hydration {
    name: Option<String>,
    info_hash_v1: Option<[u8; 20]>,
    info_hash_v2: Option<[u8; 32]>,
    release_year: Option<i32>,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    seeders: Option<u32>,
    leechers: Option<u32>,
}

impl SearchQuery {
    /// Construct from raw parts (used by [`build_query`] and by shape tests).
    pub fn new(sql: impl Into<String>, binds: Vec<Bind>) -> Self {
        Self {
            sql: sql.into(),
            binds,
        }
    }

    /// The `$N`-placeholder SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The positional binds, in `$1..$N` order.
    pub fn binds(&self) -> &[Bind] {
        &self.binds
    }

    /// Execute and return the ordered info-hash list — the Q3 parity output.
    pub async fn fetch_info_hashes(&self, _pool: &PgPool) -> Result<Vec<InfoHash>> {
        // The builder binds every user-supplied string; only typed integers and
        // fixed SQL tokens are inlined, so the generated statement is audited.
        let mut query = sqlx::query(sqlx::AssertSqlSafe(self.sql()));
        for bind in self.binds() {
            query = match bind {
                Bind::Bytea(value) => query.bind(value),
                Bind::Text(value) | Bind::Tsquery(value) => query.bind(value),
            };
        }

        let rows = query.fetch_all(_pool).await?;
        let mut info_hashes = Vec::with_capacity(rows.len());
        for row in rows {
            let raw: Vec<u8> = row.try_get("info_hash")?;
            info_hashes.push(decode_info_hash(&raw)?);
        }
        Ok(info_hashes)
    }

    /// Execute and return fully-hydrated result rows for Lane T's XML.
    pub async fn fetch(&self, pool: &PgPool) -> Result<Vec<SearchResultItem>> {
        // Query 1 — lean, ordered membership. NO hydration joins/subselects, so the
        // planner stays serial (a joined+subselect projection tips PG into a parallel
        // Gather Merge whose tie order is nondeterministic). See CONTRACT.md "Deviations".
        let ordering_sql = self.ordering_sql()?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(ordering_sql));
        for bind in self.binds() {
            query = match bind {
                Bind::Bytea(value) => query.bind(value),
                Bind::Text(value) | Bind::Tsquery(value) => query.bind(value),
            };
        }
        let ordered_rows = query.fetch_all(pool).await?;

        let mut ordered: Vec<OrderedRow> = Vec::with_capacity(ordered_rows.len());
        for row in ordered_rows {
            let raw_info_hash: Vec<u8> = row.try_get("info_hash")?;
            let content_type: Option<String> = row.try_get("content_type")?;
            let video_resolution: Option<String> = row.try_get("video_resolution")?;
            let video_3d: Option<String> = row.try_get("video_3d")?;
            let Json(db_episodes): Json<DbEpisodes> = row.try_get("episodes")?;

            ordered.push(OrderedRow {
                id: row.try_get("id")?,
                info_hash: decode_info_hash(&raw_info_hash)?,
                size: decode_u64("size", row.try_get("size")?)?,
                content_type: decode_content_type(content_type)?,
                published_at: row.try_get("published_at")?,
                // DEVIATION: `files` attr derives from denormalized files_count; live Go
                // omits it under the D1 torrent_files read-disable. See CONTRACT.md.
                files_count: decode_optional_u32("files_count", row.try_get("files_count")?)?,
                video_resolution: decode_video_resolution(video_resolution)?,
                video_3d: decode_video_3d(video_3d)?,
                video_codec: row.try_get("video_codec")?,
                release_group: row.try_get("release_group")?,
                episodes: db_episodes.into(),
            });
        }
        if ordered.is_empty() {
            return Ok(Vec::new());
        }

        // Query 2 — hydration keyed by the exact per-row identity (tc.id). No ORDER
        // BY/LIMIT, so its (possibly parallel) plan cannot affect membership or order.
        let ids: Vec<String> = ordered.iter().map(|row| row.id.clone()).collect();
        let hydration_rows = sqlx::query(sqlx::AssertSqlSafe(HYDRATION_BY_ID_SQL))
            .bind(&ids)
            .fetch_all(pool)
            .await?;
        let mut hydration: std::collections::HashMap<String, Hydration> =
            std::collections::HashMap::with_capacity(hydration_rows.len());
        for row in hydration_rows {
            let id: String = row.try_get("id")?;
            let info_hash_v1: Option<Vec<u8>> = row.try_get("info_hash_v1")?;
            let info_hash_v2: Option<Vec<u8>> = row.try_get("info_hash_v2")?;
            let release_year: Option<i32> = row.try_get("release_year")?;
            hydration.insert(
                id,
                Hydration {
                    name: row.try_get("name")?,
                    info_hash_v1: decode_fixed_hash::<20>("info_hash_v1", info_hash_v1)?,
                    info_hash_v2: decode_fixed_hash::<32>("info_hash_v2", info_hash_v2)?,
                    release_year: release_year.filter(|year| *year != 0),
                    imdb_id: row.try_get("imdb_id")?,
                    tmdb_id: row.try_get("tmdb_id")?,
                    seeders: decode_optional_u32("seeders", row.try_get("seeders")?)?,
                    leechers: decode_optional_u32("leechers", row.try_get("leechers")?)?,
                },
            );
        }

        // Merge preserving query-1 order.
        let mut items = Vec::with_capacity(ordered.len());
        for row in ordered {
            let h = hydration.remove(&row.id).unwrap_or_default();
            items.push(SearchResultItem {
                info_hash: row.info_hash,
                name: h.name.unwrap_or_default(),
                size: row.size,
                content_type: row.content_type,
                published_at: row.published_at,
                seeders: h.seeders,
                leechers: h.leechers,
                files_count: row.files_count,
                video_resolution: row.video_resolution,
                video_3d: row.video_3d,
                video_codec: row.video_codec,
                release_group: row.release_group,
                episodes: row.episodes,
                release_year: h.release_year,
                imdb_id: h.imdb_id,
                tmdb_id: h.tmdb_id,
                info_hash_v1: h.info_hash_v1,
                info_hash_v2: h.info_hash_v2,
            });
        }
        Ok(items)
    }

    fn ordering_sql(&self) -> Result<String> {
        let suffix = self.sql.strip_prefix(INFO_HASH_SELECT).ok_or_else(|| {
            SearchQueryError::InvalidParams(
                "hydration requires SQL produced by build_query".to_owned(),
            )
        })?;
        Ok(format!("{ORDERING_SELECT}{suffix}"))
    }
}

/// Lower a [`TorznabSearchParams`] to a [`SearchQuery`].
///
/// This is the single entry point Lane T calls. It ports the resolved-option →
/// SQL path of `internal/database/search` + `internal/database/query`:
/// `SELECT` (with order-alias columns), the dynamically-required joins
/// (`torrents`/`content` only when a criterion or ordering needs them), the
/// `tsv @@ $::tsquery` predicate, the filter tree, `ORDER BY`, `LIMIT`,
/// `OFFSET`. See `CONTRACT.md` for the full mapping. Implemented in Q2.
pub fn build_query(_params: &TorznabSearchParams) -> Result<SearchQuery> {
    let required_joins = _params
        .filter
        .as_ref()
        .map(RequiredJoins::from_criteria)
        .unwrap_or_default();
    let mut state = BuildState::default();
    let tsquery_placeholder = _params.query.as_ref().and_then(|raw| {
        (!raw.is_empty())
            .then(|| state.push_bind(Bind::Tsquery(crate::fts::app_query_to_tsquery(raw))))
    });

    let mut where_conditions = Vec::new();
    if let Some(placeholder) = &tsquery_placeholder {
        where_conditions.push(format!("torrent_contents.tsv @@ {placeholder}::tsquery"));
    }
    if let Some(criteria) = &_params.filter {
        where_conditions.push(criteria_sql(criteria, &mut state));
    }

    let mut sql = String::from(INFO_HASH_SELECT);
    if required_joins.torrents {
        sql.push_str(INNER_JOIN_TORRENTS);
    }
    if required_joins.content {
        sql.push_str(LEFT_JOIN_CONTENT);
    }
    if !where_conditions.is_empty() {
        sql.push_str("\nWHERE ");
        sql.push_str(&where_conditions.join("\n  AND "));
    }

    sql.push_str("\nORDER BY ");
    match _params.order {
        None => sql.push_str("torrent_contents.published_at DESC"),
        Some(order) => {
            let direction = direction_sql(order.direction);
            match order.field {
                TorrentContentOrderField::Relevance => {
                    // Go projects this expression as `_order_0`; ordering by the
                    // expression directly is result-equivalent and avoids widening
                    // the parity SELECT list.
                    if let Some(placeholder) = &tsquery_placeholder {
                        sql.push_str(&format!(
                            "ts_rank_cd(torrent_contents.tsv, {placeholder}::tsquery) {direction}"
                        ));
                    } else {
                        // A cast keeps PostgreSQL from interpreting bare `0` as an
                        // invalid positional ORDER BY reference.
                        sql.push_str(&format!("0::bigint {direction}"));
                    }
                }
                TorrentContentOrderField::PublishedAt => {
                    sql.push_str(&format!(
                        "torrent_contents.published_at {direction}, torrent_contents.info_hash {direction}"
                    ));
                }
            }
        }
    }

    // LIMIT/OFFSET are rendered as integer literals (not binds): a parameterised
    // LIMIT drives PG to a generic parallel Gather-Merge plan whose worker merge
    // order shuffles ties between identical requests. GORM inlines them as
    // literals for the same reason. `limit`/`offset` are validated u32s — no
    // injection surface. All other values remain bound.
    sql.push_str(&format!("\nLIMIT {}", _params.limit));
    if let Some(offset) = _params.offset {
        sql.push_str(&format!("\nOFFSET {}", offset));
    }

    Ok(SearchQuery::new(sql, state.binds))
}

const INFO_HASH_SELECT: &str = "SELECT torrent_contents.info_hash\nFROM torrent_contents";
const INNER_JOIN_TORRENTS: &str =
    "\nINNER JOIN torrents ON torrent_contents.info_hash = torrents.info_hash";
const LEFT_JOIN_CONTENT: &str = "\nLEFT JOIN content ON torrent_contents.content_type = content.type AND torrent_contents.content_source = content.source AND torrent_contents.content_id = content.id";
const ORDERING_SELECT: &str = "SELECT torrent_contents.id AS id,\n       torrent_contents.info_hash AS info_hash,\n       torrent_contents.size AS size,\n       torrent_contents.content_type AS content_type,\n       floor(EXTRACT(EPOCH FROM torrent_contents.published_at))::bigint AS published_at,\n       torrent_contents.files_count::bigint AS files_count,\n       torrent_contents.video_resolution AS video_resolution,\n       torrent_contents.video_3d AS video_3d,\n       torrent_contents.video_codec AS video_codec,\n       torrent_contents.release_group AS release_group,\n       COALESCE(torrent_contents.episodes, '{}'::jsonb) AS episodes\nFROM torrent_contents";
const HYDRATION_BY_ID_SQL: &str = "SELECT torrent_contents.id AS id,\n       torrents.name AS name,\n       torrents.info_hash_v1 AS info_hash_v1,\n       torrents.info_hash_v2 AS info_hash_v2,\n       NULLIF(content.release_year, 0) AS release_year,\n       CASE WHEN content.source = 'imdb' THEN content.id ELSE ca_imdb.value END AS imdb_id,\n       CASE WHEN content.source = 'tmdb' THEN content.id ELSE ca_tmdb.value END AS tmdb_id,\n       (SELECT max(s.seeders)::bigint FROM torrents_torrent_sources s WHERE s.info_hash = torrent_contents.info_hash) AS seeders,\n       (SELECT max(s.leechers)::bigint FROM torrents_torrent_sources s WHERE s.info_hash = torrent_contents.info_hash) AS leechers\nFROM torrent_contents\nLEFT JOIN torrents ON torrent_contents.info_hash = torrents.info_hash\nLEFT JOIN content ON torrent_contents.content_type = content.type AND torrent_contents.content_source = content.source AND torrent_contents.content_id = content.id\nLEFT JOIN content_attributes ca_imdb ON ca_imdb.content_type = content.type AND ca_imdb.content_source = content.source AND ca_imdb.content_id = content.id AND ca_imdb.source = 'imdb' AND ca_imdb.key = 'id'\nLEFT JOIN content_attributes ca_tmdb ON ca_tmdb.content_type = content.type AND ca_tmdb.content_source = content.source AND ca_tmdb.content_id = content.id AND ca_tmdb.source = 'tmdb' AND ca_tmdb.key = 'id'\nWHERE torrent_contents.id = ANY($1::text[])";

#[derive(Default)]
struct BuildState {
    binds: Vec<Bind>,
}

impl BuildState {
    fn push_bind(&mut self, bind: Bind) -> String {
        self.binds.push(bind);
        format!("${}", self.binds.len())
    }
}

#[derive(Default)]
struct RequiredJoins {
    torrents: bool,
    content: bool,
}

impl RequiredJoins {
    fn from_criteria(criteria: &Criteria) -> Self {
        let mut joins = Self::default();
        joins.visit(criteria);
        joins
    }

    fn visit(&mut self, criteria: &Criteria) {
        match criteria {
            Criteria::And(children) | Criteria::Or(children) => {
                for child in children {
                    self.visit(child);
                }
            }
            Criteria::Not(child) => self.visit(child),
            Criteria::CanonicalIdentifier(_) | Criteria::AlternativeIdentifier(_) => {
                self.content = true;
            }
            Criteria::TorrentTag(_) => self.torrents = true,
            Criteria::ContentTypeIn(_)
            | Criteria::VideoResolutionIn(_)
            | Criteria::Video3DIn(_)
            | Criteria::Episodes(_) => {}
        }
    }
}

fn criteria_sql(criteria: &Criteria, state: &mut BuildState) -> String {
    match criteria {
        Criteria::And(children) => boolean_group(children, "AND", "TRUE", state),
        Criteria::Or(children) => boolean_group(children, "OR", "FALSE", state),
        Criteria::Not(child) => format!("NOT ({})", criteria_sql(child, state)),
        Criteria::ContentTypeIn(types) => in_predicate(
            "torrent_contents.content_type",
            types
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        ),
        Criteria::VideoResolutionIn(values) => in_predicate(
            "torrent_contents.video_resolution",
            values
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        ),
        Criteria::Video3DIn(values) => in_predicate(
            "torrent_contents.video_3d",
            values
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        ),
        Criteria::Episodes(episodes) => episodes_sql(episodes),
        Criteria::CanonicalIdentifier(refs) => canonical_identifier_sql(refs, state),
        Criteria::AlternativeIdentifier(refs) => alternative_identifier_sql(refs, state),
        Criteria::TorrentTag(names) => {
            if names.is_empty() {
                return "FALSE".to_owned();
            }
            let placeholders = names
                .iter()
                .map(|name| state.push_bind(Bind::Text(name.clone())))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "EXISTS (SELECT 1 FROM torrent_tags WHERE torrent_tags.info_hash = torrents.info_hash AND torrent_tags.name IN ({placeholders}))"
            )
        }
    }
}

fn boolean_group(
    children: &[Criteria],
    operator: &str,
    empty: &str,
    state: &mut BuildState,
) -> String {
    if children.is_empty() {
        return empty.to_owned();
    }
    let parts = children
        .iter()
        .map(|child| criteria_sql(child, state))
        .collect::<Vec<_>>();
    format!("({})", parts.join(&format!(" {operator} ")))
}

fn in_predicate(
    column: &str,
    binds: impl IntoIterator<Item = Bind>,
    state: &mut BuildState,
) -> String {
    let placeholders = binds
        .into_iter()
        .map(|bind| state.push_bind(bind))
        .collect::<Vec<_>>();
    if placeholders.is_empty() {
        "FALSE".to_owned()
    } else {
        format!("{column} IN ({})", placeholders.join(", "))
    }
}

fn episodes_sql(episodes: &Episodes) -> String {
    let predicates = episodes
        .0
        .iter()
        .map(|(season, episodes)| {
            if episodes.is_empty() {
                format!("torrent_contents.episodes #> '{{{season}}}' = '{{}}'::jsonb")
            } else {
                let mut episodes = episodes.clone();
                episodes.sort_unstable();
                episodes.dedup();
                let keys = episodes
                    .iter()
                    .map(|episode| format!("\"{episode}\":{{}}"))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("torrent_contents.episodes #> '{{{season}}}' @> '{{{keys}}}'::jsonb")
            }
        })
        .collect::<Vec<_>>();

    match predicates.as_slice() {
        [] => "TRUE".to_owned(),
        [predicate] => predicate.clone(),
        _ => format!("({})", predicates.join(" AND ")),
    }
}

struct ContentRefGroup<'a> {
    content_type: Option<ContentType>,
    source: &'a str,
    ids: Vec<&'a str>,
}

fn group_content_refs(refs: &[ContentRef]) -> Vec<ContentRefGroup<'_>> {
    let mut groups: Vec<ContentRefGroup<'_>> = Vec::new();
    for content_ref in refs {
        if let Some(group) = groups.iter_mut().find(|group| {
            group.content_type == content_ref.content_type && group.source == content_ref.source
        }) {
            if !group.ids.contains(&content_ref.id.as_str()) {
                group.ids.push(&content_ref.id);
            }
        } else {
            groups.push(ContentRefGroup {
                content_type: content_ref.content_type,
                source: &content_ref.source,
                ids: vec![&content_ref.id],
            });
        }
    }
    groups
}

fn canonical_identifier_sql(refs: &[ContentRef], state: &mut BuildState) -> String {
    let branches = group_content_refs(refs)
        .into_iter()
        .map(|group| {
            let mut predicates = Vec::new();
            if let Some(content_type) = group.content_type {
                let placeholder = state.push_bind(Bind::Text(content_type.as_str().to_owned()));
                predicates.push(format!("content.type = {placeholder}"));
            }
            let source = state.push_bind(Bind::Text(group.source.to_owned()));
            predicates.push(format!("content.source = {source}"));
            let ids = group
                .ids
                .into_iter()
                .map(|id| state.push_bind(Bind::Text(id.to_owned())))
                .collect::<Vec<_>>()
                .join(", ");
            predicates.push(format!("content.id IN ({ids})"));
            predicates.join(" AND ")
        })
        .collect::<Vec<_>>();
    or_branches(branches)
}

fn alternative_identifier_sql(refs: &[ContentRef], state: &mut BuildState) -> String {
    let branches = group_content_refs(refs)
        .into_iter()
        .map(|group| {
            let source = state.push_bind(Bind::Text(group.source.to_owned()));
            let ids = group
                .ids
                .into_iter()
                .map(|id| state.push_bind(Bind::Text(id.to_owned())))
                .collect::<Vec<_>>()
                .join(", ");
            let mut sql = format!(
                "EXISTS (SELECT 1 FROM content_attributes WHERE content_attributes.content_type = content.type AND content_attributes.content_source = content.source AND content_attributes.content_id = content.id AND content_attributes.source = {source} AND content_attributes.value IN ({ids})"
            );
            if let Some(content_type) = group.content_type {
                let placeholder =
                    state.push_bind(Bind::Text(content_type.as_str().to_owned()));
                sql.push_str(&format!(
                    " AND content_attributes.content_type = {placeholder}"
                ));
            }
            sql.push(')');
            sql
        })
        .collect::<Vec<_>>();
    or_branches(branches)
}

fn or_branches(branches: Vec<String>) -> String {
    match branches.as_slice() {
        [] => "FALSE".to_owned(),
        [branch] => branch.clone(),
        _ => format!("({})", branches.join(" OR ")),
    }
}

const fn direction_sql(direction: OrderDirection) -> &'static str {
    match direction {
        OrderDirection::Ascending => "ASC",
        OrderDirection::Descending => "DESC",
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
struct DbEpisodes(BTreeMap<i32, BTreeMap<i32, serde::de::IgnoredAny>>);

impl From<DbEpisodes> for Episodes {
    fn from(value: DbEpisodes) -> Self {
        Self(
            value
                .0
                .into_iter()
                .map(|(season, episodes)| (season, episodes.into_keys().collect()))
                .collect(),
        )
    }
}

fn decode_info_hash(raw: &[u8]) -> Result<InfoHash> {
    InfoHash::from_slice(raw).map_err(|error| decode_error(format!("info_hash: {error}")))
}

fn decode_fixed_hash<const N: usize>(
    column: &str,
    value: Option<Vec<u8>>,
) -> Result<Option<[u8; N]>> {
    value
        .map(|bytes| {
            <[u8; N]>::try_from(bytes.as_slice()).map_err(|_| {
                decode_error(format!("{column}: expected {N} bytes, got {}", bytes.len()))
            })
        })
        .transpose()
}

fn decode_content_type(value: Option<String>) -> Result<Option<ContentType>> {
    value
        .map(|value| {
            value
                .parse::<ContentType>()
                .map_err(|error| decode_error(format!("content_type: {error}")))
        })
        .transpose()
}

fn decode_video_resolution(value: Option<String>) -> Result<Option<VideoResolution>> {
    value
        .map(|value| match value.as_str() {
            "V360p" => Ok(VideoResolution::V360p),
            "V480p" => Ok(VideoResolution::V480p),
            "V540p" => Ok(VideoResolution::V540p),
            "V576p" => Ok(VideoResolution::V576p),
            "V720p" => Ok(VideoResolution::V720p),
            "V1080p" => Ok(VideoResolution::V1080p),
            "V1440p" => Ok(VideoResolution::V1440p),
            "V2160p" => Ok(VideoResolution::V2160p),
            "V4320p" => Ok(VideoResolution::V4320p),
            _ => Err(decode_error(format!(
                "video_resolution: invalid value {value:?}"
            ))),
        })
        .transpose()
}

fn decode_video_3d(value: Option<String>) -> Result<Option<Video3D>> {
    value
        .map(|value| match value.as_str() {
            "V3D" => Ok(Video3D::V3D),
            "V3DSBS" => Ok(Video3D::V3DSBS),
            "V3DOU" => Ok(Video3D::V3DOU),
            _ => Err(decode_error(format!("video_3d: invalid value {value:?}"))),
        })
        .transpose()
}

fn decode_u64(column: &str, value: i64) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| decode_error(format!("{column}: negative bigint value {value}")))
}

fn decode_optional_u32(column: &str, value: Option<i64>) -> Result<Option<u32>> {
    value
        .map(|value| {
            u32::try_from(value)
                .map_err(|_| decode_error(format!("{column}: out-of-range bigint value {value}")))
        })
        .transpose()
}

fn decode_error(message: String) -> SearchQueryError {
    SearchQueryError::Db(sqlx::Error::Decode(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::TorrentContentOrder;

    fn assert_query(params: TorznabSearchParams, sql: &str, binds: &[Bind]) {
        let query = build_query(&params).unwrap();
        assert_eq!(query.sql(), sql);
        assert_eq!(query.binds(), binds);
    }

    fn content_ref(content_type: Option<ContentType>, source: &str, id: &str) -> ContentRef {
        ContentRef {
            content_type,
            source: source.to_owned(),
            id: id.to_owned(),
        }
    }

    #[test]
    fn builds_browse_query_without_joins() {
        let params = TorznabSearchParams::new(100)
            .with_filter(Criteria::ContentTypeIn(vec![ContentType::Movie]));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nWHERE torrent_contents.content_type IN ($1)\nORDER BY torrent_contents.published_at DESC\nLIMIT 100",
            &[Bind::Text("movie".to_owned())],
        );
    }

    #[test]
    fn binds_tsquery_once_and_reuses_it_for_relevance() {
        let params = TorznabSearchParams::new(50)
            .with_query("foo")
            .with_order(TorrentContentOrder::relevance_desc());

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nWHERE torrent_contents.tsv @@ $1::tsquery\nORDER BY ts_rank_cd(torrent_contents.tsv, $1::tsquery) DESC\nLIMIT 50",
            &[Bind::Tsquery("foo".to_owned())],
        );
    }

    #[test]
    fn builds_canonical_identifier_with_content_join() {
        let params = TorznabSearchParams::new(25).with_filter(Criteria::CanonicalIdentifier(vec![
            content_ref(Some(ContentType::Movie), "tmdb", "603"),
        ]));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nLEFT JOIN content ON torrent_contents.content_type = content.type AND torrent_contents.content_source = content.source AND torrent_contents.content_id = content.id\nWHERE content.type = $1 AND content.source = $2 AND content.id IN ($3)\nORDER BY torrent_contents.published_at DESC\nLIMIT 25",
            &[
                Bind::Text("movie".to_owned()),
                Bind::Text("tmdb".to_owned()),
                Bind::Text("603".to_owned()),
            ],
        );
    }

    #[test]
    fn builds_alternative_identifier_with_content_attributes_exists() {
        let params =
            TorznabSearchParams::new(25).with_filter(Criteria::AlternativeIdentifier(vec![
                content_ref(Some(ContentType::Movie), "imdb", "tt0133093"),
            ]));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nLEFT JOIN content ON torrent_contents.content_type = content.type AND torrent_contents.content_source = content.source AND torrent_contents.content_id = content.id\nWHERE EXISTS (SELECT 1 FROM content_attributes WHERE content_attributes.content_type = content.type AND content_attributes.content_source = content.source AND content_attributes.content_id = content.id AND content_attributes.source = $1 AND content_attributes.value IN ($2) AND content_attributes.content_type = $3)\nORDER BY torrent_contents.published_at DESC\nLIMIT 25",
            &[
                Bind::Text("imdb".to_owned()),
                Bind::Text("tt0133093".to_owned()),
                Bind::Text("movie".to_owned()),
            ],
        );
    }

    #[test]
    fn builds_torrent_tag_with_torrents_join() {
        let params =
            TorznabSearchParams::new(10).with_filter(Criteria::TorrentTag(vec!["x".to_owned()]));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nINNER JOIN torrents ON torrent_contents.info_hash = torrents.info_hash\nWHERE EXISTS (SELECT 1 FROM torrent_tags WHERE torrent_tags.info_hash = torrents.info_hash AND torrent_tags.name IN ($1))\nORDER BY torrent_contents.published_at DESC\nLIMIT 10",
            &[Bind::Text("x".to_owned())],
        );
    }

    #[test]
    fn builds_season_only_episode_predicate() {
        let params = TorznabSearchParams::new(10)
            .with_filter(Criteria::Episodes(Episodes::new().add_season(1)));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nWHERE torrent_contents.episodes #> '{1}' = '{}'::jsonb\nORDER BY torrent_contents.published_at DESC\nLIMIT 10",
            &[],
        );
    }

    #[test]
    fn builds_season_and_episode_containment_predicate() {
        let params = TorznabSearchParams::new(10).with_filter(Criteria::Episodes(
            Episodes::new().add_episode(2, 4).add_episode(2, 3),
        ));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nWHERE torrent_contents.episodes #> '{2}' @> '{\"3\":{},\"4\":{}}'::jsonb\nORDER BY torrent_contents.published_at DESC\nLIMIT 10",
            &[],
        );
    }

    #[test]
    fn emits_zero_limit_and_zero_offset() {
        let params = TorznabSearchParams::new(0).with_offset(0);

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nORDER BY torrent_contents.published_at DESC\nLIMIT 0\nOFFSET 0",
            &[],
        );
    }

    #[test]
    fn empty_and_is_true_and_empty_or_is_false() {
        assert_query(
            TorznabSearchParams::new(1).with_filter(Criteria::And(Vec::new())),
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nWHERE TRUE\nORDER BY torrent_contents.published_at DESC\nLIMIT 1",
            &[],
        );
        assert_query(
            TorznabSearchParams::new(1).with_filter(Criteria::Or(Vec::new())),
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nWHERE FALSE\nORDER BY torrent_contents.published_at DESC\nLIMIT 1",
            &[],
        );
    }

    #[test]
    fn relevance_without_query_uses_non_positional_constant() {
        let params = TorznabSearchParams::new(5).with_order(TorrentContentOrder::relevance_desc());

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nORDER BY 0::bigint DESC\nLIMIT 5",
            &[],
        );
    }

    #[test]
    fn ordering_sql_is_lean_single_table() {
        let query = build_query(&TorznabSearchParams::new(1)).unwrap();
        let sql = query.ordering_sql().unwrap();

        assert!(sql.contains("torrent_contents.id AS id"));
        assert!(sql.contains("FROM torrent_contents"));
        assert!(!sql.contains("LEFT JOIN content"));
        assert!(!sql.contains("content_attributes"));
        assert!(!sql.contains("torrents_torrent_sources"));
        assert!(!sql.contains("LEFT JOIN torrents"));
        assert!(!sql.contains("SELECT max("));
        assert!(sql.contains("\nLIMIT 1"));
    }

    #[test]
    fn ordering_sql_keeps_filter_joins() {
        let query =
            build_query(
                &TorznabSearchParams::new(1).with_filter(Criteria::CanonicalIdentifier(vec![
                    content_ref(Some(ContentType::Movie), "tmdb", "603"),
                ])),
            )
            .unwrap();
        let sql = query.ordering_sql().unwrap();

        assert!(sql.contains("LEFT JOIN content ON"));
        assert!(!sql.contains("content_attributes"));
        assert!(!sql.contains("SELECT max("));
        assert!(!sql.contains("torrents.name"));
    }

    #[test]
    fn hydration_by_id_sql_shape() {
        assert!(HYDRATION_BY_ID_SQL.contains("LEFT JOIN torrents"));
        assert!(HYDRATION_BY_ID_SQL.contains("LEFT JOIN content ON"));
        assert!(HYDRATION_BY_ID_SQL.contains("content_attributes ca_imdb"));
        assert!(HYDRATION_BY_ID_SQL.contains("content_attributes ca_tmdb"));
        assert!(HYDRATION_BY_ID_SQL.contains("SELECT max(s.seeders)"));
        assert!(HYDRATION_BY_ID_SQL.contains("SELECT max(s.leechers)"));
        assert!(HYDRATION_BY_ID_SQL.contains("torrent_contents.id = ANY($1::text[])"));
        assert!(!HYDRATION_BY_ID_SQL.contains("ORDER BY"));
        assert!(!HYDRATION_BY_ID_SQL.contains("LIMIT"));
    }

    #[test]
    fn hydrates_database_episode_json_shape() {
        let db_episodes: DbEpisodes =
            serde_json::from_str(r#"{"1":{},"2":{"3":{},"4":{}}}"#).unwrap();
        let episodes = Episodes::from(db_episodes);

        assert_eq!(
            episodes,
            Episodes::new()
                .add_season(1)
                .add_episode(2, 3)
                .add_episode(2, 4)
        );
    }
}
