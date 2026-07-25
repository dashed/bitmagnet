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

use crate::criteria::{
    ContentCollectionRef, ContentRef, Criteria, Episodes, TorrentContentAttribute, Video3D,
    VideoResolution,
};
use crate::facets::natural_cmp;
use crate::order::{OrderDirection, TorrentContentOrderField};
use crate::params::TorznabSearchParams;
use crate::result::{derive_title, dht_seen_stats, SearchResultItem, TorrentSourceInfo};
use bitmagnet_model::{
    Content, ContentType, FileType, FilesStatus, InfoHash, Torrent, TorrentContent,
};
use chrono::{DateTime, Datelike, Duration, Months, NaiveDate, NaiveDateTime, SecondsFormat};
use chrono::{Timelike, Utc};
use serde::Deserialize;
use sqlx::{types::Json, PgPool, Row};
use std::collections::BTreeMap;

/// Controls optional, heavyweight result hydration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HydrateOptions {
    /// Load `torrents.files_data`. Off by default so the blob never reaches
    /// the ordered membership path or consumers that do not need file refine.
    pub files_data: bool,
    /// Optional hard ceiling for one compressed blob selected from PostgreSQL.
    /// Oversized values are projected as NULL so SQLx never materialises them.
    pub max_files_data_bytes: Option<u64>,
}

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
/// `Tsquery` and `Timestamp` are bound as text and cast to `::tsquery` and
/// `::timestamptz` respectively at their placeholders. `TextArray` is bound as
/// a PostgreSQL `text[]` and likewise carries an explicit `::text[]` cast in
/// SQL. Go binds the pre-tokenised tsquery string the same way — see
/// `internal/database/query/query.go` `applyPre`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    /// A `bytea` value (info hash / content id bytes).
    Bytea(Vec<u8>),
    /// A `text` value (content type, resolution, source, id, tag ...).
    Text(String),
    /// A pre-tokenised tsquery string, cast `::tsquery` at the placeholder.
    Tsquery(String),
    /// An RFC3339 UTC timestamp string, cast `::timestamptz` at the placeholder.
    Timestamp(String),
    /// A PostgreSQL text array, cast `::text[]` at the placeholder.
    TextArray(Vec<String>),
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
    query_string_rank: f64,
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
    files_attr_count: Option<u32>,
    torrent_content: Option<TorrentContent>,
    torrent_content_video_modifier: Option<String>,
    torrent_content_created_at: i64,
    torrent_content_updated_at: i64,
    torrent: Option<Torrent>,
    torrent_created_at: i64,
    torrent_updated_at: i64,
    torrent_meta_version: Option<u16>,
    content: Option<Content>,
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
                Bind::Text(value) | Bind::Tsquery(value) | Bind::Timestamp(value) => {
                    query.bind(value)
                }
                Bind::TextArray(value) => query.bind(value),
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
        self.fetch_with(pool, HydrateOptions::default()).await
    }

    /// Execute and return fully-hydrated result rows, optionally loading the
    /// heavyweight `torrents.files_data` blob in the id-keyed hydration pass.
    pub async fn fetch_with(
        &self,
        pool: &PgPool,
        options: HydrateOptions,
    ) -> Result<Vec<SearchResultItem>> {
        // Query 1 — lean, ordered membership. NO hydration joins/subselects, so the
        // planner stays serial (a joined+subselect projection tips PG into a parallel
        // Gather Merge whose tie order is nondeterministic). See CONTRACT.md "Deviations".
        let ordering_sql = self.ordering_sql()?;
        let has_query_string_rank = self.query_string_rank_placeholder().is_some();
        let mut query = sqlx::query(sqlx::AssertSqlSafe(ordering_sql));
        for bind in self.binds() {
            query = match bind {
                Bind::Bytea(value) => query.bind(value),
                Bind::Text(value) | Bind::Tsquery(value) | Bind::Timestamp(value) => {
                    query.bind(value)
                }
                Bind::TextArray(value) => query.bind(value),
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
                query_string_rank: if has_query_string_rank {
                    f64::from(row.try_get::<f32, _>("query_string_rank")?)
                } else {
                    0.0
                },
            });
        }
        if ordered.is_empty() {
            return Ok(Vec::new());
        }

        // Query 2 — hydration keyed by the exact per-row identity (tc.id). No ORDER
        // BY/LIMIT, so its (possibly parallel) plan cannot affect membership or order.
        let ids: Vec<String> = ordered.iter().map(|row| row.id.clone()).collect();
        let hydration_sql = hydration_by_id_sql(options);
        let hydration_rows = sqlx::query(sqlx::AssertSqlSafe(hydration_sql))
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
            let raw_tc_info_hash: Vec<u8> = row.try_get("tc_info_hash")?;
            let tc_info_hash = decode_info_hash(&raw_tc_info_hash)?;
            let tc_content_type = decode_content_type(row.try_get("tc_content_type")?)?;
            let tc_size = decode_u64("tc_size", row.try_get("tc_size")?)?;
            let tc_seeders = decode_optional_u32("tc_seeders", row.try_get("tc_seeders")?)?;
            let tc_leechers = decode_optional_u32("tc_leechers", row.try_get("tc_leechers")?)?;
            let tc_files_count =
                decode_optional_u32("tc_files_count", row.try_get("tc_files_count")?)?;
            let Json(tc_languages): Json<Vec<String>> = row.try_get("tc_languages")?;
            let torrent_content = TorrentContent {
                id: id.clone(),
                info_hash: tc_info_hash,
                content_type: tc_content_type,
                content_source: row.try_get("tc_content_source")?,
                content_id: row.try_get("tc_content_id")?,
                languages: tc_languages,
                video_resolution: row.try_get("tc_video_resolution")?,
                video_source: row.try_get("tc_video_source")?,
                video_codec: row.try_get("tc_video_codec")?,
                release_group: row.try_get("tc_release_group")?,
                seeders: tc_seeders,
                leechers: tc_leechers,
                published_at: row.try_get("tc_published_at")?,
                size: tc_size,
                files_count: tc_files_count,
            };

            let torrent_name: Option<String> = row.try_get("name")?;
            let torrent = if let Some(name) = torrent_name.clone() {
                let raw_files_status: String = row.try_get("torrent_files_status")?;
                let files_status = raw_files_status
                    .parse::<FilesStatus>()
                    .map_err(|error| decode_error(format!("torrent_files_status: {error}")))?;
                let Json(file_extensions): Json<Vec<String>> =
                    row.try_get("torrent_file_extensions")?;
                Some(Torrent {
                    info_hash: tc_info_hash,
                    name,
                    size: decode_u64("torrent_size", row.try_get("torrent_size")?)?,
                    private: row.try_get("torrent_private")?,
                    files_status,
                    extension: row.try_get("torrent_extension")?,
                    files_count: decode_optional_u32(
                        "torrent_files_count",
                        row.try_get("torrent_files_count")?,
                    )?,
                    files_data: if options.files_data {
                        row.try_get("torrent_files_data")?
                    } else {
                        None
                    },
                    file_extensions,
                })
            } else {
                None
            };

            let content_type = decode_content_type(row.try_get("content_type_value")?)?;
            let content_source: Option<String> = row.try_get("content_source_value")?;
            let content_id: Option<String> = row.try_get("content_id_value")?;
            let content_title: Option<String> = row.try_get("content_title")?;
            let content = match (content_type, content_source, content_id, content_title) {
                (Some(content_type), Some(source), Some(id), Some(title)) if !id.is_empty() => {
                    Some(Content {
                        content_type,
                        source,
                        id,
                        title,
                        release_year: decode_optional_u32(
                            "content_release_year",
                            row.try_get("content_release_year")?,
                        )?,
                        original_language: row.try_get("content_original_language")?,
                        original_title: row.try_get("content_original_title")?,
                        overview: row.try_get("content_overview")?,
                        runtime: decode_optional_u32(
                            "content_runtime",
                            row.try_get("content_runtime")?,
                        )?,
                        popularity: row.try_get("content_popularity")?,
                        vote_average: row.try_get("content_vote_average")?,
                        vote_count: decode_optional_u32(
                            "content_vote_count",
                            row.try_get("content_vote_count")?,
                        )?,
                        // The search projection selects only the columns the
                        // result mapper reads; the remaining `model.Content`
                        // columns (release_date, adult, timestamps, tsv) and the
                        // collection/attribute associations are deliberately not
                        // hydrated here, exactly as before the B′-0 seam widened
                        // the struct.
                        release_date: None,
                        adult: None,
                        created_at: None,
                        updated_at: None,
                        tsv: Default::default(),
                        collections: Vec::new(),
                        attributes: Vec::new(),
                    })
                }
                _ => None,
            };
            hydration.insert(
                id,
                Hydration {
                    name: torrent_name,
                    info_hash_v1: decode_fixed_hash::<20>("info_hash_v1", info_hash_v1)?,
                    info_hash_v2: decode_fixed_hash::<32>("info_hash_v2", info_hash_v2)?,
                    release_year: release_year.filter(|year| *year != 0),
                    imdb_id: row.try_get("imdb_id")?,
                    tmdb_id: row.try_get("tmdb_id")?,
                    seeders: decode_optional_u32("seeders", row.try_get("seeders")?)?,
                    leechers: decode_optional_u32("leechers", row.try_get("leechers")?)?,
                    files_attr_count: decode_optional_u32(
                        "files_attr_count",
                        row.try_get("files_attr_count")?,
                    )?,
                    torrent_content: Some(torrent_content),
                    torrent_content_video_modifier: row.try_get("tc_video_modifier")?,
                    torrent_content_created_at: row.try_get("tc_created_at")?,
                    torrent_content_updated_at: row.try_get("tc_updated_at")?,
                    torrent,
                    torrent_created_at: row
                        .try_get::<Option<i64>, _>("torrent_created_at")?
                        .unwrap_or(0),
                    torrent_updated_at: row
                        .try_get::<Option<i64>, _>("torrent_updated_at")?
                        .unwrap_or(0),
                    torrent_meta_version: decode_optional_u16(
                        "torrent_meta_version",
                        row.try_get("torrent_meta_version")?,
                    )?,
                    content,
                },
            );
        }

        let info_hashes: Vec<Vec<u8>> = ordered
            .iter()
            .map(|row| row.info_hash.as_slice().to_vec())
            .collect();
        let source_rows = sqlx::query(sqlx::AssertSqlSafe(TORRENT_SOURCES_BY_INFO_HASH_SQL))
            .bind(&info_hashes)
            .fetch_all(pool)
            .await?;
        let mut sources_by_hash: std::collections::HashMap<InfoHash, Vec<TorrentSourceInfo>> =
            std::collections::HashMap::new();
        for source_row in source_rows {
            let raw_info_hash: Vec<u8> = source_row.try_get("info_hash")?;
            let info_hash = decode_info_hash(&raw_info_hash)?;
            sources_by_hash
                .entry(info_hash)
                .or_default()
                .push(TorrentSourceInfo {
                    key: source_row.try_get("source")?,
                    name: source_row
                        .try_get::<Option<String>, _>("source_name")?
                        .unwrap_or_default(),
                    import_id: source_row.try_get("import_id")?,
                    seeders: decode_optional_u32(
                        "source_seeders",
                        source_row.try_get("source_seeders")?,
                    )?,
                    leechers: decode_optional_u32(
                        "source_leechers",
                        source_row.try_get("source_leechers")?,
                    )?,
                    published_at: source_row.try_get("source_published_at")?,
                    seen_count: decode_u32(
                        "source_seen_count",
                        source_row.try_get("source_seen_count")?,
                    )?,
                    first_seen_at: source_row.try_get("source_created_at")?,
                    last_seen_at: source_row.try_get("source_updated_at")?,
                });
        }

        let tag_rows = sqlx::query(sqlx::AssertSqlSafe(TORRENT_TAGS_BY_INFO_HASH_SQL))
            .bind(&info_hashes)
            .fetch_all(pool)
            .await?;
        let mut tags_by_hash: std::collections::HashMap<InfoHash, Vec<String>> =
            std::collections::HashMap::new();
        for tag_row in tag_rows {
            let raw_info_hash: Vec<u8> = tag_row.try_get("info_hash")?;
            tags_by_hash
                .entry(decode_info_hash(&raw_info_hash)?)
                .or_default()
                .push(tag_row.try_get("name")?);
        }
        for tags in tags_by_hash.values_mut() {
            tags.sort_by(|left, right| natural_cmp(left, right));
        }

        // Merge preserving query-1 order.
        let mut items = Vec::with_capacity(ordered.len());
        for row in ordered {
            let h = hydration.remove(&row.id).unwrap_or_default();
            let name = h.name.clone().unwrap_or_default();
            // A torrent can have multiple torrent_content rows, so every row
            // sharing an info hash gets the same keyed association preload.
            let torrent_sources = sources_by_hash
                .get(&row.info_hash)
                .cloned()
                .unwrap_or_default();
            let torrent_tags = tags_by_hash
                .get(&row.info_hash)
                .cloned()
                .unwrap_or_default();
            let (dht_seen_count, dht_first_seen_at, dht_last_seen_at) =
                dht_seen_stats(&torrent_sources);
            let content = h.content;
            let title = derive_title(&name, content.as_ref(), &row.episodes);
            let torrent_content = h.torrent_content.unwrap_or_else(|| TorrentContent {
                id: row.id.clone(),
                info_hash: row.info_hash,
                content_type: row.content_type,
                content_source: None,
                content_id: None,
                languages: Vec::new(),
                video_resolution: row.video_resolution.map(|value| value.as_str().to_owned()),
                video_source: None,
                video_codec: row.video_codec.clone(),
                release_group: row.release_group.clone(),
                seeders: None,
                leechers: None,
                published_at: row.published_at,
                size: row.size,
                files_count: row.files_count,
            });
            let torrent = h.torrent.unwrap_or_else(|| Torrent {
                info_hash: row.info_hash,
                name: name.clone(),
                size: row.size,
                private: false,
                files_status: FilesStatus::NoInfo,
                extension: None,
                files_count: row.files_count,
                files_data: None,
                file_extensions: Vec::new(),
            });
            items.push(SearchResultItem {
                info_hash: row.info_hash,
                name,
                size: row.size,
                content_type: row.content_type,
                published_at: row.published_at,
                seeders: h.seeders,
                leechers: h.leechers,
                files_count: row.files_count,
                files_attr_count: h.files_attr_count,
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
                torrent_content,
                torrent_content_video_modifier: h.torrent_content_video_modifier,
                torrent_content_created_at: h.torrent_content_created_at,
                torrent_content_updated_at: h.torrent_content_updated_at,
                torrent,
                refine_files: Vec::new(),
                torrent_created_at: h.torrent_created_at,
                torrent_updated_at: h.torrent_updated_at,
                torrent_meta_version: h.torrent_meta_version,
                torrent_sources,
                torrent_tags,
                content,
                title,
                dht_seen_count,
                dht_first_seen_at,
                dht_last_seen_at,
                query_string_rank: row.query_string_rank,
            });
        }
        Ok(items)
    }

    fn ordering_sql(&self) -> Result<String> {
        let sql_after_projection =
            self.sql.strip_prefix(INFO_HASH_PROJECTION).ok_or_else(|| {
                SearchQueryError::InvalidParams(
                    "hydration requires SQL produced by build_query".to_owned(),
                )
            })?;
        let from_offset = sql_after_projection.find(BASE_FROM).ok_or_else(|| {
            SearchQueryError::InvalidParams(
                "hydration query is missing the torrent_contents base relation".to_owned(),
            )
        })?;
        let order_projections = &sql_after_projection[..from_offset];
        let suffix = &sql_after_projection[from_offset + BASE_FROM.len()..];
        let mut ordering_select = self.query_string_rank_placeholder().map_or_else(
            || ORDERING_SELECT.to_owned(),
            |placeholder| {
                ORDERING_SELECT.replacen(
                    "\nFROM torrent_contents",
                    &format!(
                        ",\n       ts_rank_cd(torrent_contents.tsv, ${placeholder}::tsquery) AS query_string_rank\nFROM torrent_contents"
                    ),
                    1,
                )
            },
        );
        if !order_projections.is_empty() {
            ordering_select =
                ordering_select.replacen(BASE_FROM, &format!("{order_projections}{BASE_FROM}"), 1);
        }
        Ok(format!("{ordering_select}{suffix}"))
    }

    fn query_string_rank_placeholder(&self) -> Option<usize> {
        self.binds
            .iter()
            .position(|bind| matches!(bind, Bind::Tsquery(_)))
            .map(|index| index + 1)
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
    let config = crate::SearchBuildConfig::default();
    let criteria_ctx = CriteriaCtx {
        config: &config,
        now: Utc::now(),
    };
    let tsquery_placeholder = _params.query.as_ref().and_then(|raw| {
        (!raw.is_empty())
            .then(|| state.push_bind(Bind::Tsquery(bitmagnet_fts::app_query_to_tsquery(raw))))
    });

    let mut where_conditions = Vec::new();
    if let Some(placeholder) = &tsquery_placeholder {
        where_conditions.push(format!("torrent_contents.tsv @@ {placeholder}::tsquery"));
    }
    if let Some(criteria) = &_params.filter {
        where_conditions.push(criteria_sql(criteria, &mut state, &criteria_ctx)?);
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
                _ => {
                    return Err(SearchQueryError::InvalidParams(format!(
                        "order field not yet supported by build_query: {:?} (Lane S S4)",
                        order.field
                    )));
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

const INFO_HASH_PROJECTION: &str = "SELECT torrent_contents.info_hash";
const BASE_FROM: &str = "\nFROM torrent_contents";
const INFO_HASH_SELECT: &str = "SELECT torrent_contents.info_hash\nFROM torrent_contents";
const INNER_JOIN_TORRENTS: &str =
    "\nINNER JOIN torrents ON torrent_contents.info_hash = torrents.info_hash";
const LEFT_JOIN_CONTENT: &str = "\nLEFT JOIN content ON torrent_contents.content_type = content.type AND torrent_contents.content_source = content.source AND torrent_contents.content_id = content.id";
const ORDERING_SELECT: &str = "SELECT torrent_contents.id AS id,\n       torrent_contents.info_hash AS info_hash,\n       torrent_contents.size AS size,\n       torrent_contents.content_type AS content_type,\n       floor(EXTRACT(EPOCH FROM torrent_contents.published_at))::bigint AS published_at,\n       torrent_contents.files_count::bigint AS files_count,\n       torrent_contents.video_resolution AS video_resolution,\n       torrent_contents.video_3d AS video_3d,\n       torrent_contents.video_codec AS video_codec,\n       torrent_contents.release_group AS release_group,\n       COALESCE(torrent_contents.episodes, '{}'::jsonb) AS episodes\nFROM torrent_contents";
const HYDRATION_SELECT: &str = r#"SELECT torrent_contents.id AS id,
       torrent_contents.info_hash AS tc_info_hash,
       torrent_contents.content_type::text AS tc_content_type,
       torrent_contents.content_source AS tc_content_source,
       torrent_contents.content_id AS tc_content_id,
       COALESCE(torrent_contents.languages, '[]'::jsonb) AS tc_languages,
       torrent_contents.video_resolution::text AS tc_video_resolution,
       torrent_contents.video_source::text AS tc_video_source,
       torrent_contents.video_codec::text AS tc_video_codec,
       torrent_contents.video_modifier::text AS tc_video_modifier,
       torrent_contents.release_group AS tc_release_group,
       floor(EXTRACT(EPOCH FROM torrent_contents.created_at))::bigint AS tc_created_at,
       floor(EXTRACT(EPOCH FROM torrent_contents.updated_at))::bigint AS tc_updated_at,
       torrent_contents.seeders::bigint AS tc_seeders,
       torrent_contents.leechers::bigint AS tc_leechers,
       floor(EXTRACT(EPOCH FROM torrent_contents.published_at))::bigint AS tc_published_at,
       torrent_contents.size::bigint AS tc_size,
       torrent_contents.files_count::bigint AS tc_files_count,
       torrents.name AS name,
       torrents.size::bigint AS torrent_size,
       torrents.private AS torrent_private,
       floor(EXTRACT(EPOCH FROM torrents.created_at))::bigint AS torrent_created_at,
       floor(EXTRACT(EPOCH FROM torrents.updated_at))::bigint AS torrent_updated_at,
       torrents.files_status::text AS torrent_files_status,
       torrents.extension AS torrent_extension,
       torrents.files_count::bigint AS torrent_files_count,
       CASE WHEN octet_length(torrents.files_data) > 0 THEN torrent_file_summary.file_count::bigint ELSE NULL END AS files_attr_count,
       COALESCE(torrents.file_extensions, '[]'::jsonb) AS torrent_file_extensions,
       torrents.info_hash_v1 AS info_hash_v1,
       torrents.info_hash_v2 AS info_hash_v2,
       torrents.meta_version::bigint AS torrent_meta_version,
       content.type::text AS content_type_value,
       content.source AS content_source_value,
       content.id AS content_id_value,
       content.title AS content_title,
       NULLIF(content.release_year, 0)::bigint AS content_release_year,
       content.original_language::text AS content_original_language,
       content.original_title AS content_original_title,
       content.overview AS content_overview,
       content.runtime::bigint AS content_runtime,
       content.popularity::real AS content_popularity,
       content.vote_average::real AS content_vote_average,
       content.vote_count::bigint AS content_vote_count,
       NULLIF(content.release_year, 0) AS release_year,
       CASE WHEN content.source = 'imdb' THEN content.id ELSE ca_imdb.value END AS imdb_id,
       CASE WHEN content.source = 'tmdb' THEN content.id ELSE ca_tmdb.value END AS tmdb_id,
       (SELECT max(s.seeders)::bigint FROM torrents_torrent_sources s WHERE s.info_hash = torrent_contents.info_hash) AS seeders,
       (SELECT max(s.leechers)::bigint FROM torrents_torrent_sources s WHERE s.info_hash = torrent_contents.info_hash) AS leechers"#;
const HYDRATION_FROM: &str = r#"
FROM torrent_contents
LEFT JOIN torrents ON torrent_contents.info_hash = torrents.info_hash
LEFT JOIN torrent_file_summary ON torrent_file_summary.info_hash = torrents.info_hash
LEFT JOIN content ON torrent_contents.content_type = content.type AND torrent_contents.content_source = content.source AND torrent_contents.content_id = content.id
LEFT JOIN content_attributes ca_imdb ON ca_imdb.content_type = content.type AND ca_imdb.content_source = content.source AND ca_imdb.content_id = content.id AND ca_imdb.source = 'imdb' AND ca_imdb.key = 'id'
LEFT JOIN content_attributes ca_tmdb ON ca_tmdb.content_type = content.type AND ca_tmdb.content_source = content.source AND ca_tmdb.content_id = content.id AND ca_tmdb.source = 'tmdb' AND ca_tmdb.key = 'id'
WHERE torrent_contents.id = ANY($1::text[])"#;
const TORRENT_SOURCES_BY_INFO_HASH_SQL: &str = r#"SELECT s.source AS source,
       s.info_hash AS info_hash,
       s.import_id AS import_id,
       s.seeders::bigint AS source_seeders,
       s.leechers::bigint AS source_leechers,
       floor(EXTRACT(EPOCH FROM s.published_at))::bigint AS source_published_at,
       floor(EXTRACT(EPOCH FROM s.created_at))::bigint AS source_created_at,
       floor(EXTRACT(EPOCH FROM s.updated_at))::bigint AS source_updated_at,
       s.seen_count::bigint AS source_seen_count,
       torrent_sources.name AS source_name
FROM torrents_torrent_sources s
LEFT JOIN torrent_sources ON torrent_sources.key = s.source
WHERE s.info_hash = ANY($1::bytea[])
ORDER BY s.info_hash, s.source"#;

const TORRENT_TAGS_BY_INFO_HASH_SQL: &str = r#"SELECT info_hash, name
FROM torrent_tags
WHERE info_hash = ANY($1::bytea[])
ORDER BY info_hash, name"#;

fn hydration_by_id_sql(options: HydrateOptions) -> String {
    let files_column = if options.files_data {
        match options.max_files_data_bytes {
            Some(limit) => {
                format!(
                    ",\n       CASE WHEN octet_length(torrents.files_data) <= {limit} \
                     THEN torrents.files_data ELSE NULL END AS torrent_files_data"
                )
            }
            None => ",\n       torrents.files_data AS torrent_files_data".to_owned(),
        }
    } else {
        String::new()
    };
    format!("{HYDRATION_SELECT}{files_column}{HYDRATION_FROM}")
}

#[derive(Default)]
pub(crate) struct BuildState {
    binds: Vec<Bind>,
}

impl BuildState {
    pub(crate) fn push_bind(&mut self, bind: Bind) -> String {
        self.binds.push(bind);
        format!("${}", self.binds.len())
    }

    pub(crate) fn binds(&self) -> &[Bind] {
        &self.binds
    }
}

pub(crate) struct CriteriaCtx<'a> {
    config: &'a crate::SearchBuildConfig,
    now: DateTime<Utc>,
}

impl<'a> CriteriaCtx<'a> {
    pub(crate) const fn new(config: &'a crate::SearchBuildConfig, now: DateTime<Utc>) -> Self {
        Self { config, now }
    }
}

#[derive(Default)]
pub(crate) struct RequiredJoins {
    torrents: bool,
    content: bool,
}

impl RequiredJoins {
    pub(crate) fn require_torrents(&mut self) {
        self.torrents = true;
    }

    pub(crate) fn from_criteria(criteria: &Criteria) -> Self {
        let mut joins = Self::default();
        joins.visit(criteria);
        joins
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.torrents |= other.torrents;
        self.content |= other.content;
    }

    pub(crate) fn append_sql(&self, sql: &mut String) {
        if self.torrents {
            sql.push_str(INNER_JOIN_TORRENTS);
        }
        if self.content {
            sql.push_str(LEFT_JOIN_CONTENT);
        }
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
            Criteria::ReleaseYearIn(_) | Criteria::IsNull(TorrentContentAttribute::ReleaseYear) => {
                self.content = true;
            }
            Criteria::TorrentTag(_) => self.torrents = true,
            Criteria::TorrentFileTypeIn(_) | Criteria::FileExtensionIn(_) => {
                self.torrents = true;
            }
            Criteria::ContentTypeIn(_)
            | Criteria::TorrentSourceIn(_)
            | Criteria::LanguageIn(_)
            | Criteria::ContentGenre(_)
            | Criteria::ContentCollection(_)
            | Criteria::VideoResolutionIn(_)
            | Criteria::VideoSourceIn(_)
            | Criteria::VideoCodecIn(_)
            | Criteria::Video3DIn(_)
            | Criteria::VideoModifierIn(_)
            | Criteria::SizeRange { .. }
            | Criteria::PublishedAt(_)
            | Criteria::TorrentContentInfoHashIn(_)
            | Criteria::IsNull(_)
            | Criteria::Episodes(_) => {}
        }
    }
}

pub(crate) fn criteria_sql(
    criteria: &Criteria,
    state: &mut BuildState,
    ctx: &CriteriaCtx<'_>,
) -> Result<String> {
    match criteria {
        Criteria::And(children) => boolean_group(children, "AND", "TRUE", state, ctx),
        Criteria::Or(children) => boolean_group(children, "OR", "FALSE", state, ctx),
        Criteria::Not(child) => Ok(format!("NOT ({})", criteria_sql(child, state, ctx)?)),
        Criteria::ContentTypeIn(types) => Ok(in_predicate(
            "torrent_contents.content_type",
            types
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        )),
        Criteria::VideoResolutionIn(values) => Ok(in_predicate(
            "torrent_contents.video_resolution",
            values
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        )),
        Criteria::Video3DIn(values) => Ok(in_predicate(
            "torrent_contents.video_3d",
            values
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        )),
        Criteria::Episodes(episodes) => Ok(episodes_sql(episodes)),
        Criteria::CanonicalIdentifier(refs) => Ok(canonical_identifier_sql(refs, state)),
        Criteria::AlternativeIdentifier(refs) => Ok(alternative_identifier_sql(refs, state)),
        Criteria::TorrentTag(names) => {
            if names.is_empty() {
                return Ok("FALSE".to_owned());
            }
            let placeholders = names
                .iter()
                .map(|name| state.push_bind(Bind::Text(name.clone())))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "EXISTS (SELECT 1 FROM torrent_tags WHERE torrent_tags.info_hash = torrents.info_hash AND torrent_tags.name IN ({placeholders}))"
            ))
        }
        Criteria::TorrentSourceIn(keys) => {
            if keys.is_empty() {
                return Ok("FALSE".to_owned());
            }
            let placeholders = keys
                .iter()
                .map(|key| state.push_bind(Bind::Text(key.clone())))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "EXISTS (SELECT 1 FROM torrents_torrent_sources WHERE torrents_torrent_sources.info_hash = torrent_contents.info_hash AND torrents_torrent_sources.source IN ({placeholders}))"
            ))
        }
        Criteria::TorrentFileTypeIn(file_types) => {
            let extensions = file_types
                .iter()
                .flat_map(|file_type| file_type_extensions(*file_type))
                .map(|extension| (*extension).to_owned())
                .collect::<Vec<_>>();
            Ok(file_extension_sql(&extensions, state, ctx))
        }
        Criteria::FileExtensionIn(extensions) => Ok(file_extension_sql(extensions, state, ctx)),
        Criteria::LanguageIn(ids) => {
            if ids.is_empty() {
                return Ok("TRUE".to_owned());
            }
            // Go interpolates array['id', ...]. A bound text[] has the same
            // match set while preserving this crate's no-user-string-inlining rule.
            let placeholder = state.push_bind(Bind::TextArray(ids.clone()));
            Ok(format!(
                "torrent_contents.languages ?| {placeholder}::text[]"
            ))
        }
        Criteria::ContentGenre(refs) | Criteria::ContentCollection(refs) => {
            Ok(content_collection_sql(refs, state))
        }
        Criteria::ReleaseYearIn(years) => {
            if years.is_empty() {
                Ok("FALSE".to_owned())
            } else {
                Ok(format!(
                    "content.release_year IN ({})",
                    years
                        .iter()
                        .map(u16::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
        }
        Criteria::VideoSourceIn(values) => Ok(in_predicate(
            "torrent_contents.video_source",
            values
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        )),
        Criteria::VideoCodecIn(values) => Ok(in_predicate(
            "torrent_contents.video_codec",
            values
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        )),
        Criteria::VideoModifierIn(values) => Ok(in_predicate(
            "torrent_contents.video_modifier",
            values
                .iter()
                .map(|value| Bind::Text(value.as_str().to_owned())),
            state,
        )),
        Criteria::SizeRange { min, max } => Ok(size_range_sql(*min, *max)),
        Criteria::PublishedAt(time_frame) => published_at_sql(time_frame, state, ctx.now),
        Criteria::TorrentContentInfoHashIn(hashes) => {
            // Go emits DECODE('<hex>', 'hex') literals. Bound bytea values are
            // result-equivalent and avoid inlining caller-provided hash text.
            Ok(in_predicate(
                "torrent_contents.info_hash",
                hashes
                    .iter()
                    .map(|hash| Bind::Bytea(hash.as_bytes().to_vec())),
                state,
            ))
        }
        Criteria::IsNull(attribute) => Ok(format!("{} IS NULL", nullable_column(*attribute))),
    }
}

fn boolean_group(
    children: &[Criteria],
    operator: &str,
    empty: &str,
    state: &mut BuildState,
    ctx: &CriteriaCtx<'_>,
) -> Result<String> {
    if children.is_empty() {
        return Ok(empty.to_owned());
    }
    let parts = children
        .iter()
        .map(|child| criteria_sql(child, state, ctx))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("({})", parts.join(&format!(" {operator} "))))
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

fn file_extension_sql(
    extensions: &[String],
    state: &mut BuildState,
    ctx: &CriteriaCtx<'_>,
) -> String {
    if extensions.is_empty() {
        return "FALSE".to_owned();
    }

    let single_file = in_predicate(
        "torrents.extension",
        extensions.iter().cloned().map(Bind::Text),
        state,
    );
    let multi_file = if ctx.config.file_extensions_jsonb {
        let clauses = extensions
            .iter()
            .map(|extension| {
                let json = serde_json::to_string(&[extension])
                    .expect("serializing a string array cannot fail");
                let placeholder = state.push_bind(Bind::Text(json));
                format!("torrents.file_extensions @> {placeholder}::jsonb")
            })
            .collect::<Vec<_>>();
        format!("({})", clauses.join(" OR "))
    } else {
        let predicate = in_predicate(
            "torrent_files.extension",
            extensions.iter().cloned().map(Bind::Text),
            state,
        );
        format!(
            "EXISTS (SELECT 1 FROM torrent_files WHERE torrent_files.info_hash = torrents.info_hash AND {predicate})"
        )
    };

    format!("({single_file} OR {multi_file})")
}

const fn file_type_extensions(file_type: FileType) -> &'static [&'static str] {
    match file_type {
        FileType::Archive => &["7z", "bz2", "gz", "iso", "rar", "tar", "zip"],
        FileType::Audio => &[
            "aac", "dsf", "flac", "m4a", "m4b", "mid", "mp3", "ogg", "wav",
        ],
        FileType::Data => &["csv", "json", "xls", "xlsx", "xml"],
        FileType::Document => &[
            "azw", "azw3", "djvu", "doc", "docx", "epub", "htm", "html", "md", "mobi", "nfo",
            "otf", "pdf", "ppt", "pptx", "rtf", "txt",
        ],
        FileType::Image => &[
            "bmp", "dds", "gif", "ico", "jpeg", "jpg", "png", "psd", "svg", "tif", "tiff",
        ],
        FileType::Software => &[
            "apk", "bat", "bin", "deb", "dll", "dmg", "exe", "jar", "lua", "msi", "package", "pkg",
            "rpm", "sh",
        ],
        FileType::Subtitles => &["srt", "sub", "vtt"],
        FileType::Video => &[
            "avi", "flv", "m4v", "mkv", "mov", "mp4", "mpeg", "mpg", "ts", "vob", "wmv",
        ],
    }
}

struct ContentCollectionRefGroup<'a> {
    collection_type: Option<&'a str>,
    source: &'a str,
    ids: Vec<&'a str>,
}

fn group_content_collection_refs(
    refs: &[ContentCollectionRef],
) -> Vec<ContentCollectionRefGroup<'_>> {
    let mut groups: Vec<ContentCollectionRefGroup<'_>> = Vec::new();
    for collection_ref in refs {
        if let Some(group) = groups.iter_mut().find(|group| {
            group.collection_type == collection_ref.collection_type.as_deref()
                && group.source == collection_ref.source
        }) {
            if !group.ids.contains(&collection_ref.id.as_str()) {
                group.ids.push(&collection_ref.id);
            }
        } else {
            groups.push(ContentCollectionRefGroup {
                collection_type: collection_ref.collection_type.as_deref(),
                source: &collection_ref.source,
                ids: vec![&collection_ref.id],
            });
        }
    }
    groups
}

fn content_collection_sql(refs: &[ContentCollectionRef], state: &mut BuildState) -> String {
    let branches = group_content_collection_refs(refs)
        .into_iter()
        .map(|group| {
            let collection_type = state.push_bind(Bind::Text(
                group.collection_type.unwrap_or_default().to_owned(),
            ));
            let source = state.push_bind(Bind::Text(group.source.to_owned()));
            let ids = group
                .ids
                .into_iter()
                .map(|id| state.push_bind(Bind::Text(id.to_owned())))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "EXISTS (SELECT 1 FROM content_collections_content WHERE content_collections_content.content_type = torrent_contents.content_type AND content_collections_content.content_source = torrent_contents.content_source AND content_collections_content.content_id = torrent_contents.content_id AND content_collections_content.content_collection_type = {collection_type} AND content_collections_content.content_collection_source = {source} AND content_collections_content.content_collection_id IN ({ids}))"
            )
        })
        .collect::<Vec<_>>();

    if branches.is_empty() {
        "FALSE".to_owned()
    } else {
        format!(
            "(torrent_contents.content_id IS NOT NULL AND {})",
            or_branches(branches)
        )
    }
}

fn size_range_sql(min: Option<i64>, max: Option<i64>) -> String {
    let mut predicates = Vec::with_capacity(2);
    if let Some(min) = min {
        predicates.push(format!("torrent_contents.size >= {min}"));
    }
    if let Some(max) = max {
        predicates.push(format!("torrent_contents.size <= {max}"));
    }

    match predicates.as_slice() {
        [] => "TRUE".to_owned(),
        [predicate] => predicate.clone(),
        _ => format!("({})", predicates.join(" AND ")),
    }
}

fn published_at_sql(
    time_frame: &str,
    state: &mut BuildState,
    now: DateTime<Utc>,
) -> Result<String> {
    let Some((start, end)) = parse_time_frame(time_frame, now)? else {
        return Ok("TRUE".to_owned());
    };
    let start = state.push_bind(Bind::Timestamp(format_timestamp(start)));
    let end = state.push_bind(Bind::Timestamp(format_timestamp(end)));
    Ok(format!(
        "(torrent_contents.published_at >= {start}::timestamptz AND torrent_contents.published_at <= {end}::timestamptz)"
    ))
}

fn parse_time_frame(
    time_frame: &str,
    now: DateTime<Utc>,
) -> Result<Option<(DateTime<Utc>, DateTime<Utc>)>> {
    let time_frame = time_frame.trim();
    if time_frame.is_empty() {
        return Ok(None);
    }

    if is_relative_time(time_frame) {
        let duration = parse_relative_time(time_frame)?;
        let start = now.checked_sub_signed(duration).ok_or_else(|| {
            invalid_published_at("relative time is outside chrono's supported range")
        })?;
        return Ok(Some((start, now)));
    }

    let keyword_range = match time_frame {
        "today" => Some((start_of_day(now.date_naive()), now)),
        "yesterday" => {
            let date = now.date_naive().pred_opt().ok_or_else(|| {
                invalid_published_at("yesterday is outside chrono's supported range")
            })?;
            Some((start_of_day(date), end_of_day(date)))
        }
        "this week" => {
            let date = start_of_week(now.date_naive())?;
            Some((start_of_day(date), now))
        }
        "last week" => {
            let this_week = start_of_week(now.date_naive())?;
            let last_week = this_week
                .checked_sub_signed(Duration::days(7))
                .ok_or_else(|| invalid_published_at("last week is out of range"))?;
            let end = start_of_day(this_week)
                .checked_sub_signed(Duration::seconds(1))
                .ok_or_else(|| invalid_published_at("last week is out of range"))?;
            Some((start_of_day(last_week), end))
        }
        "this month" => {
            let date = NaiveDate::from_ymd_opt(now.year(), now.month(), 1)
                .ok_or_else(|| invalid_published_at("this month is out of range"))?;
            Some((start_of_day(date), now))
        }
        "last month" => {
            let this_month = NaiveDate::from_ymd_opt(now.year(), now.month(), 1)
                .ok_or_else(|| invalid_published_at("this month is out of range"))?;
            let last_month = this_month
                .checked_sub_months(Months::new(1))
                .ok_or_else(|| invalid_published_at("last month is out of range"))?;
            let end = start_of_day(this_month)
                .checked_sub_signed(Duration::seconds(1))
                .ok_or_else(|| invalid_published_at("last month is out of range"))?;
            Some((start_of_day(last_month), end))
        }
        "this year" => {
            let date = NaiveDate::from_ymd_opt(now.year(), 1, 1)
                .ok_or_else(|| invalid_published_at("this year is out of range"))?;
            Some((start_of_day(date), now))
        }
        "last year" => {
            let this_year = NaiveDate::from_ymd_opt(now.year(), 1, 1)
                .ok_or_else(|| invalid_published_at("this year is out of range"))?;
            let last_year = this_year
                .checked_sub_months(Months::new(12))
                .ok_or_else(|| invalid_published_at("last year is out of range"))?;
            let end = start_of_day(this_year)
                .checked_sub_signed(Duration::seconds(1))
                .ok_or_else(|| invalid_published_at("last year is out of range"))?;
            Some((start_of_day(last_year), end))
        }
        _ => None,
    };
    if keyword_range.is_some() {
        return Ok(keyword_range);
    }

    if time_frame.contains(" to ") {
        let parts = time_frame.split(" to ").collect::<Vec<_>>();
        if parts.len() != 2 {
            return Err(invalid_published_at(
                "invalid date range format; expected 'start to end'",
            ));
        }
        let start = parse_date_string(parts[0].trim())
            .ok_or_else(|| invalid_published_at("could not parse date string"))?;
        let mut end = parse_date_string(parts[1].trim())
            .ok_or_else(|| invalid_published_at("could not parse date string"))?;
        if end.hour() == 0 && end.minute() == 0 && end.second() == 0 {
            end = end_of_day(end.date_naive());
        }
        return Ok(Some((start, end)));
    }

    if let Some(start) = parse_date_string(time_frame) {
        return Ok(Some((start, end_of_day(start.date_naive()))));
    }

    Err(invalid_published_at("could not parse time frame"))
}

fn is_relative_time(value: &str) -> bool {
    let Some((&unit, digits)) = value.as_bytes().split_last() else {
        return false;
    };
    !digits.is_empty()
        && digits.iter().all(u8::is_ascii_digit)
        && matches!(unit, b's' | b'm' | b'h' | b'd' | b'w' | b'M' | b'y')
}

fn parse_relative_time(value: &str) -> Result<Duration> {
    let (&unit, digits) = value
        .as_bytes()
        .split_last()
        .ok_or_else(|| invalid_published_at("invalid relative time format"))?;
    let digits = std::str::from_utf8(digits)
        .map_err(|_| invalid_published_at("invalid relative time format"))?;
    let value = digits
        .parse::<i64>()
        .map_err(|_| invalid_published_at("invalid relative time value"))?;
    let seconds_per_unit = match unit {
        b's' => 1,
        b'm' => 60,
        b'h' => 60 * 60,
        b'd' => 24 * 60 * 60,
        b'w' => 7 * 24 * 60 * 60,
        b'M' => 30 * 24 * 60 * 60,
        b'y' => 365 * 24 * 60 * 60,
        _ => return Err(invalid_published_at("unknown relative time unit")),
    };
    let seconds = value
        .checked_mul(seconds_per_unit)
        .ok_or_else(|| invalid_published_at("relative time is out of range"))?;
    Duration::try_seconds(seconds)
        .ok_or_else(|| invalid_published_at("relative time is out of range"))
}

fn parse_date_string(value: &str) -> Option<DateTime<Utc>> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Some(start_of_day(date));
    }
    if let Ok(date_time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%SZ") {
        return Some(DateTime::from_naive_utc_and_offset(date_time, Utc));
    }
    if let Ok(date_time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Some(DateTime::from_naive_utc_and_offset(date_time, Utc));
    }
    for format in ["%Y/%m/%d", "%m/%d/%Y", "%-d-%b-%Y", "%b %-d, %Y"] {
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            return Some(start_of_day(date));
        }
    }
    None
}

fn start_of_week(date: NaiveDate) -> Result<NaiveDate> {
    date.checked_sub_signed(Duration::days(i64::from(
        date.weekday().num_days_from_monday(),
    )))
    .ok_or_else(|| invalid_published_at("week start is out of range"))
}

fn start_of_day(date: NaiveDate) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(
        date.and_hms_micro_opt(0, 0, 0, 0)
            .expect("midnight is always valid"),
        Utc,
    )
}

fn end_of_day(date: NaiveDate) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(
        date.and_hms_micro_opt(23, 59, 59, 999_999)
            .expect("end of day is always valid"),
        Utc,
    )
}

fn format_timestamp(value: DateTime<Utc>) -> String {
    // The Go pg driver truncates time.Time nanoseconds to PostgreSQL's
    // microsecond precision. Keep this explicit for the S5 parity harness:
    // truncate, never round, and always render exactly six fractional digits.
    let nanos = value.nanosecond() / 1_000 * 1_000;
    value
        .with_nanosecond(nanos)
        .expect("a truncated nanosecond is valid")
        .to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn invalid_published_at(message: &str) -> SearchQueryError {
    SearchQueryError::InvalidParams(format!("published_at: {message}"))
}

const fn nullable_column(attribute: TorrentContentAttribute) -> &'static str {
    match attribute {
        TorrentContentAttribute::ContentType => "torrent_contents.content_type",
        TorrentContentAttribute::VideoResolution => "torrent_contents.video_resolution",
        TorrentContentAttribute::VideoSource => "torrent_contents.video_source",
        TorrentContentAttribute::VideoCodec => "torrent_contents.video_codec",
        TorrentContentAttribute::Video3D => "torrent_contents.video_3d",
        TorrentContentAttribute::VideoModifier => "torrent_contents.video_modifier",
        TorrentContentAttribute::ReleaseYear => "content.release_year",
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

fn decode_u32(column: &str, value: i64) -> Result<u32> {
    u32::try_from(value)
        .map_err(|_| decode_error(format!("{column}: out-of-range bigint value {value}")))
}

fn decode_optional_u16(column: &str, value: Option<i64>) -> Result<Option<u16>> {
    value
        .map(|value| {
            u16::try_from(value)
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
    use crate::criteria::{VideoCodec, VideoModifier, VideoSource};
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

    fn collection_ref(
        collection_type: Option<&str>,
        source: &str,
        id: &str,
    ) -> ContentCollectionRef {
        ContentCollectionRef {
            collection_type: collection_type.map(str::to_owned),
            source: source.to_owned(),
            id: id.to_owned(),
        }
    }

    fn fixed_now() -> DateTime<Utc> {
        "2026-07-12T12:00:00Z".parse().unwrap()
    }

    fn assert_criteria_sql(
        criteria: Criteria,
        config: crate::SearchBuildConfig,
        sql: &str,
        binds: &[Bind],
    ) {
        assert_criteria_sql_at(criteria, config, fixed_now(), sql, binds);
    }

    fn assert_criteria_sql_at(
        criteria: Criteria,
        config: crate::SearchBuildConfig,
        now: DateTime<Utc>,
        sql: &str,
        binds: &[Bind],
    ) {
        let mut state = BuildState::default();
        let ctx = CriteriaCtx {
            config: &config,
            now,
        };
        assert_eq!(criteria_sql(&criteria, &mut state, &ctx).unwrap(), sql);
        assert_eq!(state.binds, binds);
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
    fn content_type_or_null_admits_null_rows_and_excludes_other_types() {
        // Torznab's F3 widening renders `content_type IN (...) OR content_type IS
        // NULL`: an unclassified (NULL) row passes via the second branch, while a
        // row classified as a *different* type satisfies neither branch and is
        // still excluded.
        let params = TorznabSearchParams::new(100).with_filter(Criteria::Or(vec![
            Criteria::ContentTypeIn(vec![ContentType::Movie]),
            Criteria::IsNull(TorrentContentAttribute::ContentType),
        ]));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nWHERE (torrent_contents.content_type IN ($1) OR torrent_contents.content_type IS NULL)\nORDER BY torrent_contents.published_at DESC\nLIMIT 100",
            &[Bind::Text("movie".to_owned())],
        );
    }

    #[test]
    fn alternative_identifier_without_content_type_omits_the_type_constraint() {
        // F6: an imdb lookup issued via t=search carries no content type, so the
        // EXISTS sub-select matches the identifier regardless of the content's
        // type (no trailing `content_attributes.content_type = $` predicate).
        let params =
            TorznabSearchParams::new(25).with_filter(Criteria::AlternativeIdentifier(vec![
                content_ref(None, "imdb", "tt0133093"),
            ]));

        assert_query(
            params,
            "SELECT torrent_contents.info_hash\nFROM torrent_contents\nLEFT JOIN content ON torrent_contents.content_type = content.type AND torrent_contents.content_source = content.source AND torrent_contents.content_id = content.id\nWHERE EXISTS (SELECT 1 FROM content_attributes WHERE content_attributes.content_type = content.type AND content_attributes.content_source = content.source AND content_attributes.content_id = content.id AND content_attributes.source = $1 AND content_attributes.value IN ($2))\nORDER BY torrent_contents.published_at DESC\nLIMIT 25",
            &[
                Bind::Text("imdb".to_owned()),
                Bind::Text("tt0133093".to_owned()),
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
        assert!(!sql.contains("query_string_rank"));
    }

    #[test]
    fn ordering_sql_projects_rank_only_for_tsquery() {
        let query = build_query(
            &TorznabSearchParams::new(1)
                .with_query("matrix")
                .with_order(TorrentContentOrder::relevance_desc()),
        )
        .unwrap();
        let sql = query.ordering_sql().unwrap();

        assert!(sql.contains("ts_rank_cd(torrent_contents.tsv, $1::tsquery) AS query_string_rank"));
        assert!(!sql.contains("LEFT JOIN torrents"));
        assert!(!sql.contains("LEFT JOIN content"));
        assert!(!sql.contains("SELECT max("));
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
        let sql = hydration_by_id_sql(HydrateOptions::default());
        assert!(sql.contains("LEFT JOIN torrents"));
        assert!(sql.contains(
            "LEFT JOIN torrent_file_summary ON torrent_file_summary.info_hash = torrents.info_hash"
        ));
        assert!(sql.contains(
            "CASE WHEN octet_length(torrents.files_data) > 0 THEN torrent_file_summary.file_count::bigint ELSE NULL END AS files_attr_count"
        ));
        assert!(sql.contains("LEFT JOIN content ON"));
        assert!(sql.contains("content_attributes ca_imdb"));
        assert!(sql.contains("content_attributes ca_tmdb"));
        assert!(sql.contains("SELECT max(s.seeders)"));
        assert!(sql.contains("SELECT max(s.leechers)"));
        assert!(sql.contains("torrent_contents.id = ANY($1::text[])"));
        assert!(sql.contains("torrent_contents.languages"));
        assert!(sql.contains("torrent_contents.video_source"));
        assert!(sql.contains("torrent_contents.video_modifier"));
        assert!(sql.contains("torrents.file_extensions"));
        assert!(sql.contains("torrents.meta_version"));
        assert!(sql.contains("content.original_language"));
        assert!(sql.contains("content.popularity::real"));
        assert!(sql.contains("content.vote_average::real"));
        assert!(sql.contains("content.vote_average"));
        assert!(!sql.contains("torrent_files_data"));
        assert!(!sql.contains("ORDER BY"));
        assert!(!sql.contains("LIMIT"));
    }

    #[test]
    fn files_data_is_gated_to_id_keyed_hydration() {
        let default_sql = hydration_by_id_sql(HydrateOptions::default());
        let files_sql = hydration_by_id_sql(HydrateOptions {
            files_data: true,
            max_files_data_bytes: None,
        });
        let bounded_files_sql = hydration_by_id_sql(HydrateOptions {
            files_data: true,
            max_files_data_bytes: Some(67_108_864),
        });

        // The heavy blob is never PROJECTED by default. Its only default-path
        // reference is the presence gate for the Torznab `files` attr, whose
        // octet_length reads the bytea length header and never materialises
        // (detoasts) the blob.
        assert!(!default_sql.contains("AS torrent_files_data"));
        assert!(default_sql.contains(
            "CASE WHEN octet_length(torrents.files_data) > 0 THEN torrent_file_summary.file_count"
        ));
        assert!(files_sql.contains("torrents.files_data AS torrent_files_data"));
        assert!(bounded_files_sql.contains("octet_length(torrents.files_data) <= 67108864"));
        assert!(bounded_files_sql.contains("THEN torrents.files_data ELSE NULL"));
        assert!(!ORDERING_SELECT.contains("files_data"));
        assert!(!TORRENT_SOURCES_BY_INFO_HASH_SQL.contains("files_data"));
        assert!(!TORRENT_TAGS_BY_INFO_HASH_SQL.contains("files_data"));
    }

    #[test]
    fn source_and_tag_hydration_are_info_hash_keyed() {
        assert!(TORRENT_SOURCES_BY_INFO_HASH_SQL.contains("s.info_hash = ANY($1::bytea[])"));
        assert!(TORRENT_SOURCES_BY_INFO_HASH_SQL.contains("LEFT JOIN torrent_sources"));
        assert!(TORRENT_TAGS_BY_INFO_HASH_SQL.contains("info_hash = ANY($1::bytea[])"));
    }

    #[test]
    fn tag_names_follow_go_natural_order() {
        let mut tags = ["tag10", "tag2", "tag1"];
        tags.sort_by(|left, right| natural_cmp(left, right));
        assert_eq!(tags, ["tag1", "tag2", "tag10"]);
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

    #[test]
    fn builds_torrent_source_exists_criteria() {
        assert_criteria_sql(
            Criteria::TorrentSourceIn(vec!["nyaa".to_owned(), "dht".to_owned()]),
            crate::SearchBuildConfig::default(),
            "EXISTS (SELECT 1 FROM torrents_torrent_sources WHERE torrents_torrent_sources.info_hash = torrent_contents.info_hash AND torrents_torrent_sources.source IN ($1, $2))",
            &[
                Bind::Text("nyaa".to_owned()),
                Bind::Text("dht".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::TorrentSourceIn(Vec::new()),
            crate::SearchBuildConfig::default(),
            "FALSE",
            &[],
        );
    }

    #[test]
    fn builds_legacy_file_extension_criteria() {
        assert_criteria_sql(
            Criteria::FileExtensionIn(vec!["mkv".to_owned(), "mp4".to_owned()]),
            crate::SearchBuildConfig::default(),
            "(torrents.extension IN ($1, $2) OR EXISTS (SELECT 1 FROM torrent_files WHERE torrent_files.info_hash = torrents.info_hash AND torrent_files.extension IN ($3, $4)))",
            &[
                Bind::Text("mkv".to_owned()),
                Bind::Text("mp4".to_owned()),
                Bind::Text("mkv".to_owned()),
                Bind::Text("mp4".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::FileExtensionIn(Vec::new()),
            crate::SearchBuildConfig::default(),
            "FALSE",
            &[],
        );
    }

    #[test]
    fn builds_jsonb_file_extension_criteria() {
        let config = crate::SearchBuildConfig {
            file_extensions_jsonb: true,
            ..crate::SearchBuildConfig::default()
        };
        assert_criteria_sql(
            Criteria::FileExtensionIn(vec!["mkv".to_owned(), "mp4".to_owned()]),
            config,
            "(torrents.extension IN ($1, $2) OR (torrents.file_extensions @> $3::jsonb OR torrents.file_extensions @> $4::jsonb))",
            &[
                Bind::Text("mkv".to_owned()),
                Bind::Text("mp4".to_owned()),
                Bind::Text(r#"["mkv"]"#.to_owned()),
                Bind::Text(r#"["mp4"]"#.to_owned()),
            ],
        );
    }

    #[test]
    fn expands_file_types_to_go_extension_lists() {
        assert_criteria_sql(
            Criteria::TorrentFileTypeIn(vec![FileType::Subtitles]),
            crate::SearchBuildConfig::default(),
            "(torrents.extension IN ($1, $2, $3) OR EXISTS (SELECT 1 FROM torrent_files WHERE torrent_files.info_hash = torrents.info_hash AND torrent_files.extension IN ($4, $5, $6)))",
            &[
                Bind::Text("srt".to_owned()),
                Bind::Text("sub".to_owned()),
                Bind::Text("vtt".to_owned()),
                Bind::Text("srt".to_owned()),
                Bind::Text("sub".to_owned()),
                Bind::Text("vtt".to_owned()),
            ],
        );
        assert_eq!(
            file_type_extensions(FileType::Video),
            &["avi", "flv", "m4v", "mkv", "mov", "mp4", "mpeg", "mpg", "ts", "vob", "wmv"]
        );
    }

    #[test]
    fn binds_language_ids_as_text_array() {
        assert_criteria_sql(
            Criteria::LanguageIn(vec!["en".to_owned(), "fr".to_owned()]),
            crate::SearchBuildConfig::default(),
            "torrent_contents.languages ?| $1::text[]",
            &[Bind::TextArray(vec!["en".to_owned(), "fr".to_owned()])],
        );
        assert_criteria_sql(
            Criteria::LanguageIn(Vec::new()),
            crate::SearchBuildConfig::default(),
            "TRUE",
            &[],
        );
    }

    #[test]
    fn groups_content_collection_exists_criteria_deterministically() {
        assert_criteria_sql(
            Criteria::ContentCollection(vec![
                collection_ref(Some("series"), "tmdb", "10"),
                collection_ref(Some("genre"), "imdb", "thriller"),
                collection_ref(Some("series"), "tmdb", "11"),
                collection_ref(Some("series"), "tmdb", "10"),
            ]),
            crate::SearchBuildConfig::default(),
            "(torrent_contents.content_id IS NOT NULL AND (EXISTS (SELECT 1 FROM content_collections_content WHERE content_collections_content.content_type = torrent_contents.content_type AND content_collections_content.content_source = torrent_contents.content_source AND content_collections_content.content_id = torrent_contents.content_id AND content_collections_content.content_collection_type = $1 AND content_collections_content.content_collection_source = $2 AND content_collections_content.content_collection_id IN ($3, $4)) OR EXISTS (SELECT 1 FROM content_collections_content WHERE content_collections_content.content_type = torrent_contents.content_type AND content_collections_content.content_source = torrent_contents.content_source AND content_collections_content.content_id = torrent_contents.content_id AND content_collections_content.content_collection_type = $5 AND content_collections_content.content_collection_source = $6 AND content_collections_content.content_collection_id IN ($7))))",
            &[
                Bind::Text("series".to_owned()),
                Bind::Text("tmdb".to_owned()),
                Bind::Text("10".to_owned()),
                Bind::Text("11".to_owned()),
                Bind::Text("genre".to_owned()),
                Bind::Text("imdb".to_owned()),
                Bind::Text("thriller".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::ContentCollection(Vec::new()),
            crate::SearchBuildConfig::default(),
            "FALSE",
            &[],
        );
    }

    #[test]
    fn content_genre_uses_content_collection_sql() {
        assert_criteria_sql(
            Criteria::ContentGenre(vec![collection_ref(Some("genre"), "tmdb", "28")]),
            crate::SearchBuildConfig::default(),
            "(torrent_contents.content_id IS NOT NULL AND EXISTS (SELECT 1 FROM content_collections_content WHERE content_collections_content.content_type = torrent_contents.content_type AND content_collections_content.content_source = torrent_contents.content_source AND content_collections_content.content_id = torrent_contents.content_id AND content_collections_content.content_collection_type = $1 AND content_collections_content.content_collection_source = $2 AND content_collections_content.content_collection_id IN ($3)))",
            &[
                Bind::Text("genre".to_owned()),
                Bind::Text("tmdb".to_owned()),
                Bind::Text("28".to_owned()),
            ],
        );
    }

    #[test]
    fn inlines_release_year_literals() {
        assert_criteria_sql(
            Criteria::ReleaseYearIn(vec![1999, 2024]),
            crate::SearchBuildConfig::default(),
            "content.release_year IN (1999, 2024)",
            &[],
        );
        assert_criteria_sql(
            Criteria::ReleaseYearIn(Vec::new()),
            crate::SearchBuildConfig::default(),
            "FALSE",
            &[],
        );
    }

    #[test]
    fn builds_new_video_attribute_criteria() {
        assert_criteria_sql(
            Criteria::VideoSourceIn(vec![VideoSource::WebDl, VideoSource::BluRay]),
            crate::SearchBuildConfig::default(),
            "torrent_contents.video_source IN ($1, $2)",
            &[
                Bind::Text("WEBDL".to_owned()),
                Bind::Text("BluRay".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::VideoCodecIn(vec![VideoCodec::H264, VideoCodec::X265]),
            crate::SearchBuildConfig::default(),
            "torrent_contents.video_codec IN ($1, $2)",
            &[Bind::Text("H264".to_owned()), Bind::Text("x265".to_owned())],
        );
        assert_criteria_sql(
            Criteria::VideoModifierIn(vec![VideoModifier::Remux, VideoModifier::RawHd]),
            crate::SearchBuildConfig::default(),
            "torrent_contents.video_modifier IN ($1, $2)",
            &[
                Bind::Text("REMUX".to_owned()),
                Bind::Text("RAWHD".to_owned()),
            ],
        );
    }

    #[test]
    fn builds_all_size_range_shapes() {
        assert_criteria_sql(
            Criteria::SizeRange {
                min: Some(100),
                max: None,
            },
            crate::SearchBuildConfig::default(),
            "torrent_contents.size >= 100",
            &[],
        );
        assert_criteria_sql(
            Criteria::SizeRange {
                min: None,
                max: Some(200),
            },
            crate::SearchBuildConfig::default(),
            "torrent_contents.size <= 200",
            &[],
        );
        assert_criteria_sql(
            Criteria::SizeRange {
                min: Some(100),
                max: Some(200),
            },
            crate::SearchBuildConfig::default(),
            "(torrent_contents.size >= 100 AND torrent_contents.size <= 200)",
            &[],
        );
        assert_criteria_sql(
            Criteria::SizeRange {
                min: None,
                max: None,
            },
            crate::SearchBuildConfig::default(),
            "TRUE",
            &[],
        );
    }

    #[test]
    fn builds_required_published_at_shapes() {
        let sql = "(torrent_contents.published_at >= $1::timestamptz AND torrent_contents.published_at <= $2::timestamptz)";
        assert_criteria_sql(
            Criteria::PublishedAt("2023-01-15".to_owned()),
            crate::SearchBuildConfig::default(),
            sql,
            &[
                Bind::Timestamp("2023-01-15T00:00:00.000000Z".to_owned()),
                Bind::Timestamp("2023-01-15T23:59:59.999999Z".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::PublishedAt("2023-01-15 to 2023-01-20".to_owned()),
            crate::SearchBuildConfig::default(),
            sql,
            &[
                Bind::Timestamp("2023-01-15T00:00:00.000000Z".to_owned()),
                Bind::Timestamp("2023-01-20T23:59:59.999999Z".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::PublishedAt("7d".to_owned()),
            crate::SearchBuildConfig::default(),
            sql,
            &[
                Bind::Timestamp("2026-07-05T12:00:00.000000Z".to_owned()),
                Bind::Timestamp("2026-07-12T12:00:00.000000Z".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::PublishedAt("today".to_owned()),
            crate::SearchBuildConfig::default(),
            sql,
            &[
                Bind::Timestamp("2026-07-12T00:00:00.000000Z".to_owned()),
                Bind::Timestamp("2026-07-12T12:00:00.000000Z".to_owned()),
            ],
        );
        assert_criteria_sql(
            Criteria::PublishedAt(String::new()),
            crate::SearchBuildConfig::default(),
            "TRUE",
            &[],
        );
    }

    #[test]
    fn supports_all_go_published_at_keywords() {
        let expected = [
            (
                "yesterday",
                "2026-07-11T00:00:00.000000Z",
                "2026-07-11T23:59:59.999999Z",
            ),
            (
                "this week",
                "2026-07-06T00:00:00.000000Z",
                "2026-07-12T12:00:00.000000Z",
            ),
            (
                "last week",
                "2026-06-29T00:00:00.000000Z",
                "2026-07-05T23:59:59.000000Z",
            ),
            (
                "this month",
                "2026-07-01T00:00:00.000000Z",
                "2026-07-12T12:00:00.000000Z",
            ),
            (
                "last month",
                "2026-06-01T00:00:00.000000Z",
                "2026-06-30T23:59:59.000000Z",
            ),
            (
                "this year",
                "2026-01-01T00:00:00.000000Z",
                "2026-07-12T12:00:00.000000Z",
            ),
            (
                "last year",
                "2025-01-01T00:00:00.000000Z",
                "2025-12-31T23:59:59.000000Z",
            ),
        ];
        let sql = "(torrent_contents.published_at >= $1::timestamptz AND torrent_contents.published_at <= $2::timestamptz)";
        for (keyword, start, end) in expected {
            assert_criteria_sql(
                Criteria::PublishedAt(keyword.to_owned()),
                crate::SearchBuildConfig::default(),
                sql,
                &[
                    Bind::Timestamp(start.to_owned()),
                    Bind::Timestamp(end.to_owned()),
                ],
            );
        }
    }

    #[test]
    fn supports_all_go_published_at_date_formats_and_microsecond_truncation() {
        let cases = [
            ("2023-01-15", "2023-01-15T00:00:00.000000Z"),
            ("2023-01-15T12:34:56Z", "2023-01-15T12:34:56.000000Z"),
            ("2023-01-15 12:34:56", "2023-01-15T12:34:56.000000Z"),
            ("2023/01/15", "2023-01-15T00:00:00.000000Z"),
            ("01/15/2023", "2023-01-15T00:00:00.000000Z"),
            ("15-Jan-2023", "2023-01-15T00:00:00.000000Z"),
            ("Jan 15, 2023", "2023-01-15T00:00:00.000000Z"),
        ];
        let sql = "(torrent_contents.published_at >= $1::timestamptz AND torrent_contents.published_at <= $2::timestamptz)";
        for (time_frame, start) in cases {
            assert_criteria_sql(
                Criteria::PublishedAt(time_frame.to_owned()),
                crate::SearchBuildConfig::default(),
                sql,
                &[
                    Bind::Timestamp(start.to_owned()),
                    Bind::Timestamp("2023-01-15T23:59:59.999999Z".to_owned()),
                ],
            );
        }

        let now = "2026-07-12T12:00:00.123456789Z".parse().unwrap();
        assert_criteria_sql_at(
            Criteria::PublishedAt("0s".to_owned()),
            crate::SearchBuildConfig::default(),
            now,
            sql,
            &[
                Bind::Timestamp("2026-07-12T12:00:00.123456Z".to_owned()),
                Bind::Timestamp("2026-07-12T12:00:00.123456Z".to_owned()),
            ],
        );
    }

    #[test]
    fn supports_all_go_relative_time_units_and_rejects_invalid_frames() {
        assert_eq!(parse_relative_time("1s").unwrap(), Duration::seconds(1));
        assert_eq!(parse_relative_time("1m").unwrap(), Duration::minutes(1));
        assert_eq!(parse_relative_time("1h").unwrap(), Duration::hours(1));
        assert_eq!(parse_relative_time("1d").unwrap(), Duration::days(1));
        assert_eq!(parse_relative_time("1w").unwrap(), Duration::days(7));
        assert_eq!(parse_relative_time("1M").unwrap(), Duration::days(30));
        assert_eq!(parse_relative_time("1y").unwrap(), Duration::days(365));

        let mut state = BuildState::default();
        let config = crate::SearchBuildConfig::default();
        let ctx = CriteriaCtx {
            config: &config,
            now: fixed_now(),
        };
        assert!(matches!(
            criteria_sql(
                &Criteria::PublishedAt("not a date".to_owned()),
                &mut state,
                &ctx
            ),
            Err(SearchQueryError::InvalidParams(_))
        ));
    }

    #[test]
    fn binds_torrent_content_info_hashes_as_bytea() {
        assert_criteria_sql(
            Criteria::TorrentContentInfoHashIn(vec![
                InfoHash::new([0x11; 20]),
                InfoHash::new([0x22; 20]),
            ]),
            crate::SearchBuildConfig::default(),
            "torrent_contents.info_hash IN ($1, $2)",
            &[Bind::Bytea(vec![0x11; 20]), Bind::Bytea(vec![0x22; 20])],
        );
        assert_criteria_sql(
            Criteria::TorrentContentInfoHashIn(Vec::new()),
            crate::SearchBuildConfig::default(),
            "FALSE",
            &[],
        );
    }

    #[test]
    fn builds_null_bucket_criteria() {
        assert_criteria_sql(
            Criteria::IsNull(TorrentContentAttribute::ContentType),
            crate::SearchBuildConfig::default(),
            "torrent_contents.content_type IS NULL",
            &[],
        );
        assert_criteria_sql(
            Criteria::IsNull(TorrentContentAttribute::VideoCodec),
            crate::SearchBuildConfig::default(),
            "torrent_contents.video_codec IS NULL",
            &[],
        );
        assert_criteria_sql(
            Criteria::IsNull(TorrentContentAttribute::ReleaseYear),
            crate::SearchBuildConfig::default(),
            "content.release_year IS NULL",
            &[],
        );
    }

    #[test]
    fn required_joins_cover_s2_leaves() {
        let joins = RequiredJoins::from_criteria(&Criteria::And(vec![
            Criteria::TorrentFileTypeIn(vec![FileType::Video]),
            Criteria::ReleaseYearIn(vec![2026]),
        ]));
        assert!(joins.torrents);
        assert!(joins.content);

        let joins = RequiredJoins::from_criteria(&Criteria::And(vec![
            Criteria::TorrentSourceIn(vec!["dht".to_owned()]),
            Criteria::LanguageIn(vec!["en".to_owned()]),
            Criteria::PublishedAt("today".to_owned()),
        ]));
        assert!(!joins.torrents);
        assert!(!joins.content);
    }
}
