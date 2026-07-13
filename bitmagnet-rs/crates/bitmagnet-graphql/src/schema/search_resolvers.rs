//! Resolver routing, input shaping, and result transformation.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};

use async_graphql::{Context, Error, MaybeUndefined, Result, ID};
use bitmagnet_model::{file_extension_from_path, ContentType as ModelContentType};
use chrono::{DateTime as ChronoDateTime, SecondsFormat, Utc};

use super::enums::{
    ContentType, FacetLogic as GraphqlFacetLogic, FileFacetField, FileType, FilesStatus, Language,
    TorrentContentOrderByField, Video3D, VideoCodec, VideoModifier, VideoResolution, VideoSource,
};
use super::inputs::{
    FileSearchFacetsInput, FileSearchInput, PathTypeaheadInput, TorrentContentCollapsePathsInput,
    TorrentContentFacetsInput, TorrentContentSearchQueryInput,
};
use super::objects::{
    Content, ContentAttribute, ContentCollection, ContentTypeAgg, Episodes, ExternalLink,
    FileFacetAgg, FileFacetBucketAgg, FileSearchFacetsResult, FileSearchItem, FileSearchResult,
    GenreAgg, LanguageAgg, LanguageInfo, MetadataSource, PathTypeaheadResult, ReleaseYearAgg,
    Season, Torrent, TorrentContent, TorrentContentAggregations, TorrentContentCollapsePathsResult,
    TorrentContentPathGroup, TorrentContentSearchResult, TorrentFile, TorrentFileTypeAgg,
    TorrentSourceAgg, TorrentSourceInfo, TorrentTagAgg, VideoResolutionAgg, VideoSourceAgg,
};
use super::scalars::{DateTime, Hash20, Hash32, Year};
use super::search::{
    self, Criteria, FacetRequest, FileFacetsRequest, FilePathTypeaheadRequest, FileRowSort,
    FileRowsResult, FileSearchRequest, Filters, HydrateOptions, OrderDirection, QueryOptions,
    SearchBuildConfig, SearchRequest, SearchResult, SearchResultItem, SearchRuntime,
    SearchRuntimeData, TorrentContentFacet, TorrentContentOrder, TorrentContentOrderField,
};

const DEFAULT_PAGE_SIZE: u32 = 10;
const DEFAULT_AGGREGATION_BUDGET: f64 = 5_000.0;
const MAX_PATH_SEARCH_LIMIT: u32 = 200;
const FILE_SEARCH_DEFAULT_LIMIT: u32 = 20;
const FILE_SEARCH_MAX_LIMIT: u32 = 100;
const TYPEAHEAD_DEFAULT_LIMIT: u32 = 10;
const TYPEAHEAD_MAX_LIMIT: u32 = 25;
const FILE_SEARCH_MAX_QUERY_CHARS: usize = 256;
const TYPEAHEAD_MAX_PREFIX_CHARS: usize = 128;
const TYPEAHEAD_MIN_PREFIX_CHARS: usize = 2;
const MAX_EXTENSIONS: usize = 64;
const MAX_EXTENSION_CHARS: usize = 32;
const ZERO_DATETIME: &str = "0001-01-01T00:00:00Z";

struct SearchPlan {
    has_query_string: bool,
    query: String,
    order_is_pathsearch_eligible: bool,
    filters: Filters,
    pg: SearchRequest,
    composer: QueryOptions,
    path_limit: u32,
    offset: u32,
}

pub(super) async fn search(
    ctx: &Context<'_>,
    input: TorrentContentSearchQueryInput,
) -> Result<TorrentContentSearchResult> {
    let runtime = runtime(ctx)?;
    search_with_runtime(runtime.as_ref(), input).await
}

async fn search_with_runtime(
    runtime: &dyn SearchRuntime,
    input: TorrentContentSearchQueryInput,
) -> Result<TorrentContentSearchResult> {
    let plan = build_search_plan(&input, runtime.search_build_config())?;

    if runtime.typeahead_enabled()
        && plan.has_query_string
        && runtime.eligible(&plan.query)
        && plan.order_is_pathsearch_eligible
        && runtime.healthy()
    {
        let (result, served) = runtime
            .torrent_content(
                plan.filters,
                plan.composer,
                plan.path_limit,
                plan.offset,
                Vec::new(),
            )
            .await
            .map_err(backend_error)?;
        if served {
            return map_search_result(result);
        }
    }

    let result = runtime
        .pg_torrent_content(plan.pg)
        .await
        .map_err(backend_error)?;
    map_search_result(result)
}

pub(super) async fn collapse_paths(
    ctx: &Context<'_>,
    input: TorrentContentCollapsePathsInput,
) -> Result<TorrentContentCollapsePathsResult> {
    let runtime = runtime(ctx)?;
    collapse_paths_with_runtime(runtime.as_ref(), input).await
}

async fn collapse_paths_with_runtime(
    runtime: &dyn SearchRuntime,
    input: TorrentContentCollapsePathsInput,
) -> Result<TorrentContentCollapsePathsResult> {
    if !runtime.collapse_enabled() || !runtime.healthy() {
        return Err(Error::new("path collapse unavailable"));
    }
    if !runtime.eligible(&input.query_string) {
        return Err(Error::new("path collapse query too short"));
    }

    let raw_limit = optional_nonnegative_u32(&input.limit, "limit")?.unwrap_or(0);
    let limit = if raw_limit == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        raw_limit.min(MAX_PATH_SEARCH_LIMIT)
    };
    let offset = optional_nonnegative_u32(&input.offset, "offset")?.unwrap_or(0);
    let options = refine_only_query_options(runtime.search_build_config());
    let (groups, served) = runtime
        .collapse_paths(
            Filters {
                query: input.query_string,
                ..Filters::default()
            },
            options,
            limit,
            offset,
            Vec::new(),
        )
        .await
        .map_err(backend_error)?;
    if !served {
        return Err(Error::new("path collapse unavailable"));
    }

    Ok(TorrentContentCollapsePathsResult {
        groups: groups
            .into_iter()
            .map(|group| TorrentContentPathGroup {
                path: group.path,
                info_hashes: group
                    .info_hashes
                    .into_iter()
                    .map(|hash| Hash20(hash.to_string()))
                    .collect(),
            })
            .collect(),
    })
}

pub(super) async fn file_search(
    ctx: &Context<'_>,
    input: FileSearchInput,
) -> Result<FileSearchResult> {
    let runtime = runtime(ctx)?;
    file_search_with_runtime(runtime.as_ref(), input).await
}

async fn file_search_with_runtime(
    runtime: &dyn SearchRuntime,
    input: FileSearchInput,
) -> Result<FileSearchResult> {
    if !runtime.features().file_search_enabled {
        return Err(Error::new("file search is not enabled"));
    }

    let request = normalize_file_search_input(input)?;
    let has_torrent_sort = request
        .sort
        .iter()
        .any(|sort| is_torrent_file_sort(&sort.field));
    let should_route = !request.query.trim().is_empty()
        && request.info_hash.is_none()
        && runtime.file_search_route_text_enabled()
        && runtime.healthy()
        && runtime.eligible(&request.query);

    if should_route {
        let filters = Filters {
            query: request.query.clone(),
            extensions: request.extensions.clone(),
            min_size: request.min_size,
            max_size: request.max_size,
        };
        let (result, served) = runtime
            .search_file_rows(
                filters,
                refine_only_query_options(runtime.search_build_config()),
                request.limit,
                request.offset,
                request.sort.clone(),
            )
            .await
            .map_err(backend_error)?;
        if served {
            return map_file_rows_result(result);
        }
    }

    if has_torrent_sort {
        return Err(Error::new(
            "file search torrent-field sorts require the routed text-search path",
        ));
    }

    let result = runtime.file_search(request).await.map_err(backend_error)?;
    map_file_rows_result(result)
}

pub(super) async fn file_search_facets(
    ctx: &Context<'_>,
    input: FileSearchFacetsInput,
) -> Result<FileSearchFacetsResult> {
    let runtime = runtime(ctx)?;
    file_search_facets_with_runtime(runtime.as_ref(), input).await
}

async fn file_search_facets_with_runtime(
    runtime: &dyn SearchRuntime,
    input: FileSearchFacetsInput,
) -> Result<FileSearchFacetsResult> {
    let features = runtime.features();
    if !features.file_search_enabled || !features.file_search_facets_enabled {
        return Err(Error::new("file search facets are not enabled"));
    }

    let request = normalize_file_facets_input(input)?;
    let result = runtime
        .file_search_facets(request)
        .await
        .map_err(backend_error)?;
    Ok(FileSearchFacetsResult {
        facets: result
            .facets
            .into_iter()
            .filter_map(|facet| {
                (facet.field == "extension").then(|| FileFacetAgg {
                    field: FileFacetField::Extension,
                    buckets: facet
                        .buckets
                        .into_iter()
                        .map(|bucket| FileFacetBucketAgg {
                            value: bucket.value,
                            count: bounded_i64(bucket.count),
                            total_size: bounded_i64(bucket.total_size),
                            is_estimate: false,
                        })
                        .collect(),
                })
            })
            .collect(),
    })
}

pub(super) async fn path_typeahead(
    ctx: &Context<'_>,
    input: PathTypeaheadInput,
) -> Result<PathTypeaheadResult> {
    let runtime = runtime(ctx)?;
    path_typeahead_with_runtime(runtime.as_ref(), input).await
}

async fn path_typeahead_with_runtime(
    runtime: &dyn SearchRuntime,
    input: PathTypeaheadInput,
) -> Result<PathTypeaheadResult> {
    if !runtime.features().file_search_enabled {
        return Err(Error::new("file search is not enabled"));
    }

    let request = normalize_typeahead_input(input)?;
    if runtime.features().file_search_typeahead_rpc_enabled {
        if let Ok((suggestions, true)) =
            runtime.suggest(request.prefix.clone(), request.limit).await
        {
            return Ok(PathTypeaheadResult { suggestions });
        }
    }

    if runtime.typeahead_enabled() && runtime.healthy() {
        if !runtime.eligible(&request.prefix) {
            return Err(Error::new("typeahead prefix too short"));
        }
        let (suggestions, served) = runtime
            .path_typeahead(
                request.prefix.clone(),
                refine_only_query_options(runtime.search_build_config()),
                request.limit,
            )
            .await
            .map_err(backend_error)?;
        if served {
            return Ok(PathTypeaheadResult { suggestions });
        }
    }

    let suggestions = runtime
        .file_path_typeahead(request)
        .await
        .map_err(backend_error)?;
    Ok(PathTypeaheadResult { suggestions })
}

fn runtime(ctx: &Context<'_>) -> Result<std::sync::Arc<dyn SearchRuntime>> {
    Ok(ctx.data::<SearchRuntimeData>()?.shared())
}

fn backend_error(error: search::Error) -> Error {
    Error::new(error.to_string())
}

fn build_search_plan(
    input: &TorrentContentSearchQueryInput,
    build: SearchBuildConfig,
) -> Result<SearchPlan> {
    let has_query_string = input.query_string.is_value();
    let query = input.query_string.value().cloned().unwrap_or_default();
    let order = resolved_order(input, has_query_string);
    let order_is_pathsearch_eligible = input
        .order_by
        .as_deref()
        .unwrap_or_default()
        .iter()
        .all(|order| order.field == TorrentContentOrderByField::Relevance);
    let facets = resolved_facets(input.facets.value())?;
    let filter = resolved_filter(input)?;
    let limit_is_explicit = input.limit.is_value();
    let limit = optional_nonnegative_u32(&input.limit, "limit")?.unwrap_or(DEFAULT_PAGE_SIZE);
    let offset = search_page_offset(input, limit_is_explicit, limit)?;
    let aggregation_budget = input
        .aggregation_budget
        .value()
        .copied()
        .unwrap_or(DEFAULT_AGGREGATION_BUDGET);
    let total_count = input.total_count.value().copied().unwrap_or(false);
    let has_next_page = input.has_next_page.value().copied().unwrap_or(false);

    let mut pg_order = order.clone();
    if build.popularity_sort_default
        && has_query_string
        && pg_order.len() == 1
        && pg_order[0].field == TorrentContentOrderField::Relevance
    {
        pg_order = vec![TorrentContentOrder {
            field: TorrentContentOrderField::Seeders,
            direction: OrderDirection::Descending,
        }];
    }

    let pg = SearchRequest {
        options: search::SearchOptions {
            query: has_query_string.then(|| query.clone()),
            filter: filter.clone(),
            order: pg_order,
            facets: facets.clone(),
            limit: Some(limit),
            offset,
            total_count,
            has_next_page,
            aggregation_budget,
        },
        build,
        hydrate: HydrateOptions {
            torrent: true,
            content: true,
            files_data: false,
        },
    };

    let composer = composer_query_options(filter, order, facets, build);
    Ok(SearchPlan {
        has_query_string,
        query: query.clone(),
        order_is_pathsearch_eligible,
        filters: Filters {
            query,
            extensions: pathsearch_extensions(input.facets.value()),
            min_size: 0,
            max_size: 0,
        },
        pg,
        composer,
        path_limit: limit.min(MAX_PATH_SEARCH_LIMIT),
        offset,
    })
}

fn composer_query_options(
    filter: Option<Criteria>,
    order: Vec<TorrentContentOrder>,
    facets: Vec<FacetRequest>,
    build: SearchBuildConfig,
) -> QueryOptions {
    let base = search::SearchOptions {
        query: None,
        filter,
        order,
        facets: facets.clone(),
        limit: None,
        offset: 0,
        total_count: false,
        has_next_page: false,
        aggregation_budget: DEFAULT_AGGREGATION_BUDGET,
    };
    let refine_facets = facets
        .iter()
        .cloned()
        .map(|mut facet| {
            facet.aggregate = false;
            facet
        })
        .collect();

    let combined = SearchRequest {
        options: base.clone(),
        build,
        hydrate: HydrateOptions {
            torrent: true,
            content: true,
            files_data: true,
        },
    };
    let mut refine_options = base.clone();
    refine_options.facets = refine_facets;
    let refine = SearchRequest {
        options: refine_options,
        build,
        hydrate: combined.hydrate,
    };
    let mut agg_options = base;
    agg_options.order = Vec::new();
    let agg = SearchRequest {
        options: agg_options,
        build,
        hydrate: HydrateOptions::default(),
    };
    QueryOptions {
        combined,
        refine: Some(refine),
        agg,
    }
}

fn refine_only_query_options(build: SearchBuildConfig) -> QueryOptions {
    let request = SearchRequest {
        options: search::SearchOptions {
            limit: None,
            ..search::SearchOptions::default()
        },
        build,
        hydrate: HydrateOptions {
            torrent: true,
            content: true,
            files_data: true,
        },
    };
    QueryOptions {
        combined: request.clone(),
        refine: Some(request),
        agg: SearchRequest {
            options: search::SearchOptions {
                limit: None,
                ..search::SearchOptions::default()
            },
            build,
            hydrate: HydrateOptions::default(),
        },
    }
}

fn resolved_order(
    input: &TorrentContentSearchQueryInput,
    has_query_string: bool,
) -> Vec<TorrentContentOrder> {
    input
        .order_by
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|order| {
            if order.field == TorrentContentOrderByField::Relevance && !has_query_string {
                return None;
            }
            Some(TorrentContentOrder {
                field: match order.field {
                    TorrentContentOrderByField::Relevance => TorrentContentOrderField::Relevance,
                    TorrentContentOrderByField::PublishedAt => {
                        TorrentContentOrderField::PublishedAt
                    }
                    TorrentContentOrderByField::UpdatedAt => TorrentContentOrderField::UpdatedAt,
                    TorrentContentOrderByField::Size => TorrentContentOrderField::Size,
                    TorrentContentOrderByField::FilesCount => TorrentContentOrderField::FilesCount,
                    TorrentContentOrderByField::Seeders => TorrentContentOrderField::Seeders,
                    TorrentContentOrderByField::Leechers => TorrentContentOrderField::Leechers,
                    TorrentContentOrderByField::Name => TorrentContentOrderField::Name,
                    TorrentContentOrderByField::InfoHash => TorrentContentOrderField::InfoHash,
                },
                direction: if order.descending.value().copied().unwrap_or(false) {
                    OrderDirection::Descending
                } else {
                    OrderDirection::Ascending
                },
            })
        })
        .collect()
}

fn resolved_filter(input: &TorrentContentSearchQueryInput) -> Result<Option<Criteria>> {
    let mut criteria = Vec::new();
    if let Some(facets) = input.facets.value() {
        if let Some(size) = facets.size_range.value() {
            let min = optional_i64(&size.min);
            let max = optional_i64(&size.max);
            if min.is_some() || max.is_some() {
                criteria.push(Criteria::SizeRange { min, max });
            }
        }
        if let Some(value) = facets
            .published_at
            .value()
            .filter(|value| !value.is_empty())
        {
            criteria.push(Criteria::PublishedAt(value.clone()));
        }
    }
    if let Some(info_hashes) = &input.info_hashes {
        let parsed = info_hashes
            .iter()
            .map(parse_hash20)
            .collect::<Result<Vec<_>>>()?;
        criteria.push(Criteria::TorrentContentInfoHashIn(parsed));
    }
    Ok(match criteria.len() {
        0 => None,
        1 => criteria.pop(),
        _ => Some(Criteria::And(criteria)),
    })
}

fn resolved_facets(input: Option<&TorrentContentFacetsInput>) -> Result<Vec<FacetRequest>> {
    let Some(input) = input else {
        return Ok(Vec::new());
    };
    let mut facets = Vec::new();
    if let Some(value) = input.content_type.value() {
        facets.push(facet_request(
            TorrentContentFacet::ContentType,
            &value.aggregate,
            None,
            value.filter.as_ref().map(|values| {
                values
                    .iter()
                    .map(|value| match value {
                        Some(value) => content_type_str(*value).to_owned(),
                        None => "null".to_owned(),
                    })
                    .collect()
            }),
        ));
    }
    if let Some(value) = input.torrent_source.value() {
        facets.push(facet_request(
            TorrentContentFacet::TorrentSource,
            &value.aggregate,
            facet_logic(&value.logic),
            value.filter.clone(),
        ));
    }
    if let Some(value) = input.torrent_tag.value() {
        facets.push(facet_request(
            TorrentContentFacet::TorrentTag,
            &value.aggregate,
            facet_logic(&value.logic),
            value.filter.clone(),
        ));
    }
    if let Some(value) = input.torrent_file_type.value() {
        facets.push(facet_request(
            TorrentContentFacet::FileType,
            &value.aggregate,
            facet_logic(&value.logic),
            value.filter.as_ref().map(|values| {
                values
                    .iter()
                    .map(|value| file_type_str(*value).to_owned())
                    .collect()
            }),
        ));
    }
    if let Some(value) = input.language.value() {
        facets.push(facet_request(
            TorrentContentFacet::Language,
            &value.aggregate,
            None,
            value.filter.as_ref().map(|values| {
                values
                    .iter()
                    .map(|value| language_str(*value).to_owned())
                    .collect()
            }),
        ));
    }
    if let Some(value) = input.genre.value() {
        facets.push(facet_request(
            TorrentContentFacet::ContentGenre,
            &value.aggregate,
            facet_logic(&value.logic),
            value.filter.clone(),
        ));
    }
    if let Some(value) = input.release_year.value() {
        facets.push(facet_request(
            TorrentContentFacet::ReleaseYear,
            &value.aggregate,
            None,
            value.filter.as_ref().map(|values| {
                values
                    .iter()
                    .map(|value| match value {
                        Some(value) => value.0.clone(),
                        None => "null".to_owned(),
                    })
                    .collect()
            }),
        ));
    }
    if let Some(value) = input.video_resolution.value() {
        facets.push(facet_request(
            TorrentContentFacet::VideoResolution,
            &value.aggregate,
            None,
            value.filter.as_ref().map(|values| {
                values
                    .iter()
                    .map(|value| match value {
                        Some(value) => video_resolution_str(*value).to_owned(),
                        None => "null".to_owned(),
                    })
                    .collect()
            }),
        ));
    }
    if let Some(value) = input.video_source.value() {
        facets.push(facet_request(
            TorrentContentFacet::VideoSource,
            &value.aggregate,
            None,
            value.filter.as_ref().map(|values| {
                values
                    .iter()
                    .map(|value| match value {
                        Some(value) => video_source_str(*value).to_owned(),
                        None => "null".to_owned(),
                    })
                    .collect()
            }),
        ));
    }
    Ok(facets)
}

fn facet_request(
    facet: TorrentContentFacet,
    aggregate: &MaybeUndefined<bool>,
    logic: Option<search::FacetLogic>,
    filter: Option<Vec<String>>,
) -> FacetRequest {
    FacetRequest {
        facet,
        aggregate: aggregate.value().copied().unwrap_or(false),
        logic,
        filter: filter.unwrap_or_default().into_iter().collect(),
    }
}

fn facet_logic(logic: &MaybeUndefined<GraphqlFacetLogic>) -> Option<search::FacetLogic> {
    logic.value().map(|logic| match logic {
        GraphqlFacetLogic::And => search::FacetLogic::And,
        GraphqlFacetLogic::Or => search::FacetLogic::Or,
    })
}

fn pathsearch_extensions(input: Option<&TorrentContentFacetsInput>) -> Vec<String> {
    input
        .and_then(|facets| facets.torrent_file_type.value())
        .and_then(|facet| facet.filter.as_ref())
        .into_iter()
        .flatten()
        .flat_map(|file_type| file_type_extensions(*file_type).iter().copied())
        .map(str::to_owned)
        .collect()
}

fn search_page_offset(
    input: &TorrentContentSearchQueryInput,
    limit_is_explicit: bool,
    limit: u32,
) -> Result<u32> {
    let mut offset = 0_u32;
    if limit_is_explicit {
        if let Some(page) = optional_nonnegative_u32(&input.page, "page")?.filter(|page| *page > 0)
        {
            offset = page
                .checked_sub(1)
                .and_then(|page| page.checked_mul(limit))
                .ok_or_else(|| Error::new("page window exceeds supported range"))?;
        }
    }
    if let Some(explicit_offset) = optional_nonnegative_u32(&input.offset, "offset")? {
        offset = offset
            .checked_add(explicit_offset)
            .ok_or_else(|| Error::new("page window exceeds supported range"))?;
    }
    Ok(offset)
}

fn optional_nonnegative_u32(value: &MaybeUndefined<i32>, field: &str) -> Result<Option<u32>> {
    value
        .value()
        .map(|value| {
            u32::try_from(*value).map_err(|_| Error::new(format!("{field} must be non-negative")))
        })
        .transpose()
}

fn optional_i64(value: &MaybeUndefined<i32>) -> Option<i64> {
    value.value().copied().map(i64::from)
}

fn normalize_file_search_input(input: FileSearchInput) -> Result<FileSearchRequest> {
    let query = cap_chars(
        input
            .query
            .value()
            .map(String::as_str)
            .unwrap_or_default()
            .trim(),
        FILE_SEARCH_MAX_QUERY_CHARS,
    );
    let extensions = normalize_extensions(input.extensions.unwrap_or_default());
    let min_size = optional_nonnegative_u64(&input.min_size, "minSize")?.unwrap_or(0);
    let max_size = optional_nonnegative_u64(&input.max_size, "maxSize")?.unwrap_or(0);
    let info_hash = input.info_hash.value().map(parse_hash20).transpose()?;
    let sort = input
        .sort
        .unwrap_or_default()
        .into_iter()
        .map(|sort| FileRowSort {
            field: sort.field,
            descending: sort.descending.value().copied().unwrap_or(false),
        })
        .collect::<Vec<_>>();
    if query.is_empty() && sort.iter().any(|sort| is_torrent_file_sort(&sort.field)) {
        return Err(Error::new(
            "file search torrent-field sorts require a non-empty text query",
        ));
    }
    if query.is_empty()
        && extensions.is_empty()
        && min_size == 0
        && max_size == 0
        && info_hash.is_none()
    {
        return Err(Error::new(
            "file search requires a query, extension, size bound or info hash",
        ));
    }
    let limit = clamp_limit(
        optional_nonnegative_u32(&input.limit, "limit")?.unwrap_or(0),
        FILE_SEARCH_DEFAULT_LIMIT,
        FILE_SEARCH_MAX_LIMIT,
    );
    let offset = optional_nonnegative_u32(&input.offset, "offset")?.unwrap_or(0);
    Ok(FileSearchRequest {
        query_like_pattern: escape_like(&query),
        query,
        extensions,
        min_size,
        max_size,
        info_hash,
        sort,
        limit,
        offset,
        skip_total_count: input.total_count.value().is_some_and(|value| !value),
    })
}

fn normalize_file_facets_input(input: FileSearchFacetsInput) -> Result<FileFacetsRequest> {
    let query = cap_chars(
        input
            .query
            .value()
            .map(String::as_str)
            .unwrap_or_default()
            .trim(),
        FILE_SEARCH_MAX_QUERY_CHARS,
    );
    let fields = if input.facets.unwrap_or_default().is_empty() {
        Vec::new()
    } else {
        vec!["extension".to_owned()]
    };
    Ok(FileFacetsRequest {
        query_like_pattern: escape_like(&query),
        query,
        extensions: normalize_extensions(input.extensions.unwrap_or_default()),
        min_size: optional_nonnegative_u64(&input.min_size, "minSize")?.unwrap_or(0),
        max_size: optional_nonnegative_u64(&input.max_size, "maxSize")?.unwrap_or(0),
        fields,
    })
}

fn normalize_typeahead_input(input: PathTypeaheadInput) -> Result<FilePathTypeaheadRequest> {
    let prefix = input.prefix.trim();
    if prefix.chars().count() < TYPEAHEAD_MIN_PREFIX_CHARS {
        return Err(Error::new("typeahead prefix too short"));
    }
    let prefix = cap_chars(prefix, TYPEAHEAD_MAX_PREFIX_CHARS);
    let limit = clamp_limit(
        optional_nonnegative_u32(&input.limit, "limit")?.unwrap_or(0),
        TYPEAHEAD_DEFAULT_LIMIT,
        TYPEAHEAD_MAX_LIMIT,
    );
    Ok(FilePathTypeaheadRequest {
        prefix_like_pattern: escape_like(&prefix),
        prefix,
        limit,
    })
}

fn optional_nonnegative_u64(value: &MaybeUndefined<i32>, field: &str) -> Result<Option<u64>> {
    value
        .value()
        .map(|value| {
            u64::try_from(*value).map_err(|_| Error::new(format!("{field} must be non-negative")))
        })
        .transpose()
}

fn normalize_extensions(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter_map(|value| {
            let value = value.trim().to_lowercase();
            let value = cap_chars(
                value.strip_prefix('.').unwrap_or(&value),
                MAX_EXTENSION_CHARS,
            );
            (!value.is_empty() && seen.insert(value.clone())).then_some(value)
        })
        .take(MAX_EXTENSIONS)
        .collect()
}

fn cap_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn clamp_limit(value: u32, default: u32, max: u32) -> u32 {
    if value == 0 {
        default
    } else {
        value.min(max)
    }
}

fn is_torrent_file_sort(field: &str) -> bool {
    let field = field.trim().to_lowercase();
    let field = match field.as_str() {
        "dhtlastseenat" => "dht_last_seen_at",
        value => value,
    };
    matches!(
        field,
        "last_seen" | "dht_last_seen_at" | "seeders" | "published_at" | "updated_at"
    )
}

fn map_file_rows_result(result: FileRowsResult) -> Result<FileSearchResult> {
    Ok(FileSearchResult {
        items: result
            .rows
            .into_iter()
            .map(|row| {
                Ok(FileSearchItem {
                    info_hash: Hash20(row.info_hash.to_string()),
                    index: i64::from(row.index),
                    path: row.path,
                    extension: row.extension,
                    size: bounded_i64(row.size),
                    torrent_content: map_search_item(row.torrent_content)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        total_count: bounded_i64(result.total_count),
        total_count_is_estimate: result.total_count_is_estimate,
        has_next_page: result.has_next_page,
    })
}

fn map_search_result(result: SearchResult) -> Result<TorrentContentSearchResult> {
    Ok(TorrentContentSearchResult {
        total_count: bounded_i64(result.total_count),
        total_count_is_estimate: result.total_count_is_estimate,
        has_next_page: Some(result.has_next_page),
        items: result
            .items
            .into_iter()
            .map(map_search_item)
            .collect::<Result<Vec<_>>>()?,
        aggregations: map_aggregations(&result.aggregations)?,
    })
}

fn map_search_item(item: SearchResultItem) -> Result<TorrentContent> {
    let info_hash = Hash20(item.info_hash.to_string());
    let languages = (!item.torrent_content.languages.is_empty()).then(|| {
        item.torrent_content
            .languages
            .iter()
            .map(|id| LanguageInfo {
                id: id.clone(),
                name: language_name(id).unwrap_or(id).to_owned(),
            })
            .collect()
    });
    let episodes = (!item.episodes.is_empty()).then(|| Episodes {
        label: episodes_label(&item.episodes),
        seasons: item
            .episodes
            .iter()
            .map(|(season, episodes)| Season {
                season: *season,
                episodes: (!episodes.is_empty()).then(|| episodes.clone()),
            })
            .collect(),
    });
    let content = item
        .content
        .as_ref()
        .map(|content| map_content(content, &item))
        .transpose()?;
    let torrent = map_torrent(&item)?;

    Ok(TorrentContent {
        id: ID(item.torrent_content.id.clone()),
        info_hash,
        torrent,
        content_type: item.content_type.map(map_model_content_type),
        content_source: item.torrent_content.content_source.clone(),
        content_id: item.torrent_content.content_id.clone(),
        content,
        title: item.title,
        languages,
        episodes,
        video_resolution: item
            .video_resolution
            .as_deref()
            .map(parse_video_resolution)
            .transpose()?,
        video_source: item
            .torrent_content
            .video_source
            .as_deref()
            .map(parse_video_source)
            .transpose()?,
        video_codec: item
            .video_codec
            .as_deref()
            .map(parse_video_codec)
            .transpose()?,
        video_3d: item.video_3d.as_deref().map(parse_video_3d).transpose()?,
        video_modifier: item
            .torrent_content_video_modifier
            .as_deref()
            .map(parse_video_modifier)
            .transpose()?,
        release_group: item.release_group,
        seeders: item.seeders.map(i64::from),
        leechers: item.leechers.map(i64::from),
        dht_seen_count: i64::from(item.dht_seen_count),
        dht_first_seen_at: item.dht_first_seen_at.map(timestamp).transpose()?,
        dht_last_seen_at: item.dht_last_seen_at.map(timestamp).transpose()?,
        published_at: timestamp(item.published_at)?,
        created_at: timestamp(item.torrent_content_created_at)?,
        updated_at: timestamp(item.torrent_content_updated_at)?,
    })
}

fn map_torrent(item: &SearchResultItem) -> Result<Torrent> {
    let sources = item
        .torrent_sources
        .iter()
        .map(|source| {
            Ok(TorrentSourceInfo {
                key: source.key.clone(),
                name: source.name.clone(),
                import_id: source.import_id.clone(),
                seeders: source.seeders.map(i64::from),
                leechers: source.leechers.map(i64::from),
                seen_count: i64::from(source.seen_count),
                first_seen_at: timestamp(source.first_seen_at)?,
                last_seen_at: timestamp(source.last_seen_at)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let seeders = item
        .torrent_sources
        .iter()
        .filter_map(|source| source.seeders)
        .max();
    let leechers = item
        .torrent_sources
        .iter()
        .filter_map(|source| source.leechers)
        .max();
    let files = (!item.refine_files.is_empty()).then(|| {
        item.refine_files
            .iter()
            .map(|file| {
                let extension = if file.extension.is_empty() {
                    file_extension_from_path(&file.path)
                } else {
                    Some(file.extension.to_lowercase())
                };
                TorrentFile {
                    info_hash: Hash20(item.info_hash.to_string()),
                    index: i64::from(file.index),
                    path: file.path.clone(),
                    file_type: extension
                        .as_deref()
                        .and_then(parse_file_type_from_extension),
                    extension,
                    size: bounded_i64(file.size),
                    created_at: DateTime(ZERO_DATETIME.to_owned()),
                    updated_at: DateTime(ZERO_DATETIME.to_owned()),
                }
            })
            .collect()
    });
    let file_types = unique_file_types(&item.torrent.file_extensions);

    Ok(Torrent {
        info_hash: Hash20(item.info_hash.to_string()),
        info_hash_v2: item.info_hash_v2.map(|hash| Hash32(bytes_hex(&hash))),
        meta_version: item.torrent_meta_version.map(i64::from),
        name: item.torrent.name.clone(),
        size: bounded_i64(item.torrent.size),
        has_files_info: matches!(
            item.torrent.files_status,
            bitmagnet_model::FilesStatus::Single | bitmagnet_model::FilesStatus::Multi
        ) || files.is_some(),
        single_file: Some(item.torrent.files_status == bitmagnet_model::FilesStatus::Single),
        extension: item.torrent.extension.clone(),
        files_status: map_files_status(item.torrent.files_status),
        files_count: item.torrent.files_count.map(i64::from),
        file_type: item
            .torrent
            .extension
            .as_deref()
            .and_then(parse_file_type_from_extension),
        file_types: Some(file_types),
        file_extensions: item.torrent.file_extensions.clone(),
        files,
        sources,
        seeders: seeders.map(i64::from),
        leechers: leechers.map(i64::from),
        tag_names: item.torrent_tags.clone(),
        magnet_uri: magnet_uri(item),
        created_at: timestamp(item.torrent_created_at)?,
        updated_at: timestamp(item.torrent_updated_at)?,
    })
}

fn map_content(content: &bitmagnet_model::Content, item: &SearchResultItem) -> Result<Content> {
    let original_language = content.original_language.as_ref().map(|id| LanguageInfo {
        id: id.clone(),
        name: language_name(id).unwrap_or(id).to_owned(),
    });
    Ok(Content {
        content_type: map_model_content_type(content.content_type),
        source: content.source.clone(),
        id: content.id.clone(),
        title: content.title.clone(),
        release_date: None,
        release_year: content.release_year.map(|year| Year(year.to_string())),
        adult: None,
        original_language,
        original_title: content.original_title.clone(),
        overview: content.overview.clone(),
        runtime: content.runtime.map(i64::from),
        popularity: content.popularity.map(f64::from),
        vote_average: content.vote_average.map(f64::from),
        vote_count: content.vote_count.map(i64::from),
        attributes: Vec::<ContentAttribute>::new(),
        collections: Vec::<ContentCollection>::new(),
        metadata_source: MetadataSource {
            key: content.source.clone(),
            name: content.source.clone(),
        },
        external_links: Vec::<ExternalLink>::new(),
        // The frozen Lane-S item does not yet carry content-table timestamps;
        // use the enclosing association timestamps until the adapter expands.
        created_at: timestamp(item.torrent_content_created_at)?,
        updated_at: timestamp(item.torrent_content_updated_at)?,
    })
}

fn map_aggregations(aggregations: &search::Aggregations) -> Result<TorrentContentAggregations> {
    Ok(TorrentContentAggregations {
        content_type: map_typed_agg(
            aggregations.get(TorrentContentFacet::ContentType.key()),
            |value| parse_content_type(value).map(Some),
            |value, item| ContentTypeAgg {
                value,
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        torrent_source: map_non_null_string_agg(
            aggregations.get(TorrentContentFacet::TorrentSource.key()),
            |value, item| TorrentSourceAgg {
                value,
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        torrent_tag: map_non_null_string_agg(
            aggregations.get(TorrentContentFacet::TorrentTag.key()),
            |value, item| TorrentTagAgg {
                value,
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        torrent_file_type: map_typed_agg(
            aggregations.get(TorrentContentFacet::FileType.key()),
            |value| parse_file_type(value).map(Some),
            |value, item| TorrentFileTypeAgg {
                value: value.expect("file type aggregation is non-null"),
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        language: map_typed_agg(
            aggregations.get(TorrentContentFacet::Language.key()),
            |value| parse_language(value).map(Some),
            |value, item| LanguageAgg {
                value: value.expect("language aggregation is non-null"),
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        genre: map_non_null_string_agg(
            aggregations.get(TorrentContentFacet::ContentGenre.key()),
            |value, item| GenreAgg {
                value,
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        release_year: map_typed_agg(
            aggregations.get(TorrentContentFacet::ReleaseYear.key()),
            |value| Ok(Some(Year(value.to_owned()))),
            |value, item| ReleaseYearAgg {
                value,
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        video_resolution: map_typed_agg(
            aggregations.get(TorrentContentFacet::VideoResolution.key()),
            |value| parse_video_resolution(value).map(Some),
            |value, item| VideoResolutionAgg {
                value,
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
        video_source: map_typed_agg(
            aggregations.get(TorrentContentFacet::VideoSource.key()),
            |value| parse_video_source(value).map(Some),
            |value, item| VideoSourceAgg {
                value,
                label: item.label.clone(),
                count: bounded_i32(item.count),
                is_estimate: item.is_estimate,
            },
        )?,
    })
}

fn map_typed_agg<T, O>(
    group: Option<&search::AggregationGroup>,
    parse: impl Fn(&str) -> Result<Option<T>>,
    build: impl Fn(Option<T>, &search::AggregationItem) -> O,
) -> Result<Option<Vec<O>>> {
    let Some(group) = group else {
        return Ok(None);
    };
    let mut values = group
        .items
        .iter()
        .filter(|(value, _)| value.as_str() != "null")
        .map(|(value, item)| Ok((build(parse(value)?, item), item.label.clone())))
        .collect::<Result<Vec<_>>>()?;
    values.sort_by(|left, right| natural_cmp(&left.1, &right.1));
    let mut out = values
        .into_iter()
        .map(|(value, _)| value)
        .collect::<Vec<_>>();
    if let Some(item) = group.items.get("null") {
        out.push(build(None, item));
    }
    Ok(Some(out))
}

fn map_non_null_string_agg<O>(
    group: Option<&search::AggregationGroup>,
    build: impl Fn(String, &search::AggregationItem) -> O,
) -> Result<Option<Vec<O>>> {
    let Some(group) = group else {
        return Ok(None);
    };
    if group.items.contains_key("null") {
        return Err(Error::new("non-null aggregation contained a null bucket"));
    }
    let mut values = group
        .items
        .iter()
        .map(|(value, item)| (build(value.clone(), item), item.label.clone()))
        .collect::<Vec<_>>();
    values.sort_by(|left, right| natural_cmp(&left.1, &right.1));
    Ok(Some(values.into_iter().map(|(value, _)| value).collect()))
}

fn natural_cmp(left: &str, right: &str) -> Ordering {
    let mut left = left.chars().peekable();
    let mut right = right.chars().peekable();
    loop {
        match (left.peek(), right.peek()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(a), Some(b)) if a.is_ascii_digit() && b.is_ascii_digit() => {
                let a = take_digit_run(&mut left);
                let b = take_digit_run(&mut right);
                let a_trimmed = a.trim_start_matches('0');
                let b_trimmed = b.trim_start_matches('0');
                let order = a_trimmed
                    .len()
                    .cmp(&b_trimmed.len())
                    .then_with(|| a_trimmed.cmp(b_trimmed))
                    .then_with(|| a.len().cmp(&b.len()));
                if order != Ordering::Equal {
                    return order;
                }
            }
            (Some(_), Some(_)) => {
                let a = left.next().expect("peeked character");
                let b = right.next().expect("peeked character");
                let order = a.cmp(&b);
                if order != Ordering::Equal {
                    return order;
                }
            }
        }
    }
}

fn take_digit_run(iter: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut out = String::new();
    while iter.peek().is_some_and(char::is_ascii_digit) {
        out.push(iter.next().expect("peeked digit"));
    }
    out
}

fn timestamp(epoch: i64) -> Result<DateTime> {
    ChronoDateTime::<Utc>::from_timestamp(epoch, 0)
        .map(|value| DateTime(value.to_rfc3339_opts(SecondsFormat::Secs, true)))
        .ok_or_else(|| Error::new(format!("timestamp {epoch} is out of range")))
}

fn parse_hash20(value: &Hash20) -> Result<bitmagnet_model::InfoHash> {
    value
        .0
        .parse()
        .map_err(|error| Error::new(format!("invalid Hash20: {error}")))
}

fn bounded_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn bounded_i32(value: u64) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn bytes_hex<const N: usize>(value: &[u8; N]) -> String {
    use std::fmt::Write;
    value
        .iter()
        .fold(String::with_capacity(N * 2), |mut out, byte| {
            write!(out, "{byte:02x}").expect("writing to String cannot fail");
            out
        })
}

fn magnet_uri(item: &SearchResultItem) -> String {
    let mut topics = Vec::with_capacity(2);
    if let Some(v1) = item.info_hash_v1 {
        topics.push(format!("xt=urn:btih:{}", bytes_hex(&v1)));
    } else if item.info_hash_v2.is_none() {
        topics.push(format!("xt=urn:btih:{}", item.info_hash));
    }
    if let Some(v2) = item.info_hash_v2 {
        topics.push(format!("xt=urn:btmh:1220{}", bytes_hex(&v2)));
    }
    format!(
        "magnet:?{}&dn={}&xl={}",
        topics.join("&"),
        query_escape(&item.torrent.name),
        item.torrent.size
    )
}

fn query_escape(value: &str) -> String {
    use std::fmt::Write;
    value
        .as_bytes()
        .iter()
        .fold(String::new(), |mut out, byte| {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(char::from(*byte));
                }
                b' ' => out.push('+'),
                _ => write!(out, "%{byte:02X}").expect("writing to String cannot fail"),
            }
            out
        })
}

fn unique_file_types(extensions: &[String]) -> Vec<FileType> {
    let mut seen = BTreeSet::new();
    extensions
        .iter()
        .filter_map(|extension| parse_file_type_from_extension(extension))
        .filter(|file_type| seen.insert(file_type_str(*file_type)))
        .collect()
}

fn episodes_label(episodes: &search::Episodes) -> String {
    let whole = episodes
        .iter()
        .filter_map(|(season, values)| values.is_empty().then_some(*season))
        .collect::<Vec<_>>();
    let mut whole_labels = std::collections::BTreeMap::new();
    for (start, end) in contiguous_ranges(&whole) {
        whole_labels.insert(start, format!("S{}", format_range(start, end)));
    }
    episodes
        .iter()
        .filter_map(|(season, values)| {
            if values.is_empty() {
                whole_labels.get(season).cloned()
            } else {
                let mut values = values.clone();
                values.sort_unstable();
                values.dedup();
                let parts = contiguous_ranges(&values)
                    .into_iter()
                    .map(|(start, end)| format!("E{}", format_range(start, end)))
                    .collect::<Vec<_>>()
                    .join(",");
                Some(format!("S{season:02}{parts}"))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn contiguous_ranges(values: &[i32]) -> Vec<(i32, i32)> {
    let Some(&first) = values.first() else {
        return Vec::new();
    };
    let mut ranges = Vec::new();
    let (mut start, mut end) = (first, first);
    for &value in &values[1..] {
        if value == end + 1 {
            end = value;
        } else {
            ranges.push((start, end));
            (start, end) = (value, value);
        }
    }
    ranges.push((start, end));
    ranges
}

fn format_range(start: i32, end: i32) -> String {
    if start == end {
        format!("{start:02}")
    } else {
        format!("{start:02}-{end:02}")
    }
}

fn map_model_content_type(value: ModelContentType) -> ContentType {
    match value {
        ModelContentType::Movie => ContentType::Movie,
        ModelContentType::TvShow => ContentType::TvShow,
        ModelContentType::Music => ContentType::Music,
        ModelContentType::Ebook => ContentType::Ebook,
        ModelContentType::Comic => ContentType::Comic,
        ModelContentType::Audiobook => ContentType::Audiobook,
        ModelContentType::Game => ContentType::Game,
        ModelContentType::Software => ContentType::Software,
        ModelContentType::Xxx => ContentType::Xxx,
    }
}

fn map_files_status(value: bitmagnet_model::FilesStatus) -> FilesStatus {
    match value {
        bitmagnet_model::FilesStatus::NoInfo => FilesStatus::NoInfo,
        bitmagnet_model::FilesStatus::Single => FilesStatus::Single,
        bitmagnet_model::FilesStatus::Multi => FilesStatus::Multi,
        bitmagnet_model::FilesStatus::OverThreshold => FilesStatus::OverThreshold,
    }
}

fn parse_content_type(value: &str) -> Result<ContentType> {
    match value {
        "movie" => Ok(ContentType::Movie),
        "tv_show" => Ok(ContentType::TvShow),
        "music" => Ok(ContentType::Music),
        "ebook" => Ok(ContentType::Ebook),
        "comic" => Ok(ContentType::Comic),
        "audiobook" => Ok(ContentType::Audiobook),
        "game" => Ok(ContentType::Game),
        "software" => Ok(ContentType::Software),
        "xxx" => Ok(ContentType::Xxx),
        _ => Err(Error::new(format!(
            "invalid content type aggregation: {value}"
        ))),
    }
}

fn parse_file_type(value: &str) -> Result<FileType> {
    match value {
        "archive" => Ok(FileType::Archive),
        "audio" => Ok(FileType::Audio),
        "data" => Ok(FileType::Data),
        "document" => Ok(FileType::Document),
        "image" => Ok(FileType::Image),
        "software" => Ok(FileType::Software),
        "subtitles" => Ok(FileType::Subtitles),
        "video" => Ok(FileType::Video),
        _ => Err(Error::new(format!(
            "invalid file type aggregation: {value}"
        ))),
    }
}

fn parse_language(value: &str) -> Result<Language> {
    all_languages()
        .iter()
        .find_map(|(id, language, _)| (*id == value).then_some(*language))
        .ok_or_else(|| Error::new(format!("invalid language aggregation: {value}")))
}

fn parse_video_resolution(value: &str) -> Result<VideoResolution> {
    match value {
        "V360p" => Ok(VideoResolution::P360),
        "V480p" => Ok(VideoResolution::P480),
        "V540p" => Ok(VideoResolution::P540),
        "V576p" => Ok(VideoResolution::P576),
        "V720p" => Ok(VideoResolution::P720),
        "V1080p" => Ok(VideoResolution::P1080),
        "V1440p" => Ok(VideoResolution::P1440),
        "V2160p" => Ok(VideoResolution::P2160),
        "V4320p" => Ok(VideoResolution::P4320),
        _ => Err(Error::new(format!("invalid video resolution: {value}"))),
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
        "WEBDL" => Ok(VideoSource::Webdl),
        "WEBRip" => Ok(VideoSource::Webrip),
        "BluRay" => Ok(VideoSource::Bluray),
        _ => Err(Error::new(format!("invalid video source: {value}"))),
    }
}

fn parse_video_codec(value: &str) -> Result<VideoCodec> {
    match value {
        "H264" => Ok(VideoCodec::H264),
        "x264" => Ok(VideoCodec::X264),
        "x265" => Ok(VideoCodec::X265),
        "XviD" => Ok(VideoCodec::Xvid),
        "DivX" => Ok(VideoCodec::Divx),
        "MPEG2" => Ok(VideoCodec::Mpeg2),
        "MPEG4" => Ok(VideoCodec::Mpeg4),
        _ => Err(Error::new(format!("invalid video codec: {value}"))),
    }
}

fn parse_video_3d(value: &str) -> Result<Video3D> {
    match value {
        "V3D" => Ok(Video3D::Standard),
        "V3DSBS" => Ok(Video3D::SideBySide),
        "V3DOU" => Ok(Video3D::OverUnder),
        _ => Err(Error::new(format!("invalid video 3D value: {value}"))),
    }
}

fn parse_video_modifier(value: &str) -> Result<VideoModifier> {
    match value {
        "REGIONAL" => Ok(VideoModifier::Regional),
        "SCREENER" => Ok(VideoModifier::Screener),
        "RAWHD" => Ok(VideoModifier::Rawhd),
        "BRDISK" => Ok(VideoModifier::Brdisk),
        "REMUX" => Ok(VideoModifier::Remux),
        _ => Err(Error::new(format!("invalid video modifier: {value}"))),
    }
}

fn parse_file_type_from_extension(extension: &str) -> Option<FileType> {
    bitmagnet_model::FileType::from_extension(extension).map(|file_type| match file_type {
        bitmagnet_model::FileType::Archive => FileType::Archive,
        bitmagnet_model::FileType::Audio => FileType::Audio,
        bitmagnet_model::FileType::Data => FileType::Data,
        bitmagnet_model::FileType::Document => FileType::Document,
        bitmagnet_model::FileType::Image => FileType::Image,
        bitmagnet_model::FileType::Software => FileType::Software,
        bitmagnet_model::FileType::Subtitles => FileType::Subtitles,
        bitmagnet_model::FileType::Video => FileType::Video,
    })
}

fn content_type_str(value: ContentType) -> &'static str {
    match value {
        ContentType::Audiobook => "audiobook",
        ContentType::Comic => "comic",
        ContentType::Ebook => "ebook",
        ContentType::Game => "game",
        ContentType::Movie => "movie",
        ContentType::Music => "music",
        ContentType::Software => "software",
        ContentType::TvShow => "tv_show",
        ContentType::Xxx => "xxx",
    }
}

fn file_type_str(value: FileType) -> &'static str {
    match value {
        FileType::Archive => "archive",
        FileType::Audio => "audio",
        FileType::Data => "data",
        FileType::Document => "document",
        FileType::Image => "image",
        FileType::Software => "software",
        FileType::Subtitles => "subtitles",
        FileType::Video => "video",
    }
}

fn language_str(value: Language) -> &'static str {
    all_languages()
        .iter()
        .find_map(|(id, language, _)| (*language == value).then_some(*id))
        .expect("all GraphQL language variants are listed")
}

fn language_name(value: &str) -> Option<&'static str> {
    all_languages()
        .iter()
        .find_map(|(id, _, name)| (*id == value).then_some(*name))
}

fn video_resolution_str(value: VideoResolution) -> &'static str {
    match value {
        VideoResolution::P360 => "V360p",
        VideoResolution::P480 => "V480p",
        VideoResolution::P540 => "V540p",
        VideoResolution::P576 => "V576p",
        VideoResolution::P720 => "V720p",
        VideoResolution::P1080 => "V1080p",
        VideoResolution::P1440 => "V1440p",
        VideoResolution::P2160 => "V2160p",
        VideoResolution::P4320 => "V4320p",
    }
}

fn video_source_str(value: VideoSource) -> &'static str {
    match value {
        VideoSource::Cam => "CAM",
        VideoSource::Telesync => "TELESYNC",
        VideoSource::Telecine => "TELECINE",
        VideoSource::Workprint => "WORKPRINT",
        VideoSource::Dvd => "DVD",
        VideoSource::Tv => "TV",
        VideoSource::Webdl => "WEBDL",
        VideoSource::Webrip => "WEBRip",
        VideoSource::Bluray => "BluRay",
    }
}

fn file_type_extensions(value: FileType) -> &'static [&'static str] {
    match value {
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

fn all_languages() -> &'static [(&'static str, Language, &'static str)] {
    &[
        ("af", Language::Af, "Afrikaans"),
        ("ar", Language::Ar, "Arabic"),
        ("az", Language::Az, "Azerbaijani"),
        ("be", Language::Be, "Belarusian"),
        ("bg", Language::Bg, "Bulgarian"),
        ("bs", Language::Bs, "Bosnian"),
        ("ca", Language::Ca, "Catalan"),
        ("ce", Language::Ce, "Chechen"),
        ("co", Language::Co, "Corsican"),
        ("cs", Language::Cs, "Czech"),
        ("cy", Language::Cy, "Welsh"),
        ("da", Language::Da, "Danish"),
        ("de", Language::De, "German"),
        ("el", Language::El, "Greek"),
        ("en", Language::En, "English"),
        ("es", Language::Es, "Spanish"),
        ("et", Language::Et, "Estonian"),
        ("eu", Language::Eu, "Basque"),
        ("fa", Language::Fa, "Persian"),
        ("fi", Language::Fi, "Finnish"),
        ("fr", Language::Fr, "French"),
        ("he", Language::He, "Hebrew"),
        ("hi", Language::Hi, "Hindi"),
        ("hr", Language::Hr, "Croatian"),
        ("hu", Language::Hu, "Hungarian"),
        ("hy", Language::Hy, "Armenian"),
        ("id", Language::Id, "Indonesian"),
        ("is", Language::Is, "Icelandic"),
        ("it", Language::It, "Italian"),
        ("ja", Language::Ja, "Japanese"),
        ("ka", Language::Ka, "Georgian"),
        ("ko", Language::Ko, "Korean"),
        ("ku", Language::Ku, "Kurdish"),
        ("lt", Language::Lt, "Lithuanian"),
        ("lv", Language::Lv, "Latvian"),
        ("mi", Language::Mi, "Maori"),
        ("mk", Language::Mk, "Macedonian"),
        ("ml", Language::Ml, "Malayalam"),
        ("mn", Language::Mn, "Mongolian"),
        ("ms", Language::Ms, "Malay"),
        ("mt", Language::Mt, "Maltese"),
        ("nl", Language::Nl, "Dutch"),
        ("no", Language::No, "Norwegian"),
        ("pl", Language::Pl, "Polish"),
        ("pt", Language::Pt, "Portuguese"),
        ("ro", Language::Ro, "Romanian"),
        ("ru", Language::Ru, "Russian"),
        ("sa", Language::Sa, "Sanskrit"),
        ("sk", Language::Sk, "Slovak"),
        ("sl", Language::Sl, "Slovenian"),
        ("sm", Language::Sm, "Samoan"),
        ("so", Language::So, "Somali"),
        ("sr", Language::Sr, "Serbian"),
        ("sv", Language::Sv, "Swedish"),
        ("ta", Language::Ta, "Tamil"),
        ("th", Language::Th, "Thai"),
        ("tr", Language::Tr, "Turkish"),
        ("uk", Language::Uk, "Ukrainian"),
        ("vi", Language::Vi, "Vietnamese"),
        ("yi", Language::Yi, "Yiddish"),
        ("zh", Language::Zh, "Chinese"),
        ("zu", Language::Zu, "Zulu"),
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex;

    use bitmagnet_model::{Content as ModelContent, FilesStatus as ModelFilesStatus};

    use super::*;
    use crate::schema::inputs::{
        FileSearchSortInput, SizeRangeInput, TorrentContentOrderByInput, TorrentFileTypeFacetInput,
    };
    use crate::schema::search::{
        AggregationGroup, AggregationItem, FileFacet, FileFacetBucket, FileFacetsResult, PathGroup,
        SearchFeatures, TorrentSourceInfo as RuntimeTorrentSourceInfo,
    };

    #[derive(Default)]
    struct MockRuntime {
        composer_served: bool,
        composer_error: bool,
        file_route_served: bool,
        collapse_served: bool,
        typeahead_served: bool,
        suggest_served: bool,
        eligible: bool,
        healthy: bool,
        typeahead_enabled: bool,
        file_route_enabled: bool,
        collapse_enabled: bool,
        features: SearchFeatures,
        build: SearchBuildConfig,
        composer_result: SearchResult,
        pg_result: SearchResult,
        file_result: FileRowsResult,
        collapse_groups: Vec<PathGroup>,
        typeahead_result: Vec<String>,
        suggest_result: Vec<String>,
        file_facets_result: FileFacetsResult,
        l2_typeahead_result: Vec<String>,
        composer_calls: AtomicUsize,
        pg_calls: AtomicUsize,
        file_route_calls: AtomicUsize,
        l2_file_calls: AtomicUsize,
        collapse_calls: AtomicUsize,
        typeahead_calls: AtomicUsize,
        suggest_calls: AtomicUsize,
        l2_typeahead_calls: AtomicUsize,
        captured_composer: Mutex<Vec<(Filters, QueryOptions, u32, u32)>>,
        captured_pg: Mutex<Vec<SearchRequest>>,
        captured_file: Mutex<Vec<FileSearchRequest>>,
        captured_collapse: Mutex<Vec<(Filters, QueryOptions, u32, u32)>>,
    }

    #[async_trait::async_trait]
    impl SearchRuntime for MockRuntime {
        async fn pg_torrent_content(&self, request: SearchRequest) -> search::Result<SearchResult> {
            self.pg_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.captured_pg.lock().unwrap().push(request);
            Ok(self.pg_result.clone())
        }

        async fn torrent_content(
            &self,
            filters: Filters,
            options: QueryOptions,
            limit: u32,
            offset: u32,
            _sorts: Vec<bitmagnet_proto::v1::SortBy>,
        ) -> search::Result<(SearchResult, bool)> {
            self.composer_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.captured_composer
                .lock()
                .unwrap()
                .push((filters, options, limit, offset));
            if self.composer_error {
                return Err(search::Error::Backend("composer failed".into()));
            }
            Ok((self.composer_result.clone(), self.composer_served))
        }

        async fn collapse_paths(
            &self,
            filters: Filters,
            options: QueryOptions,
            limit: u32,
            offset: u32,
            _sorts: Vec<bitmagnet_proto::v1::SortBy>,
        ) -> search::Result<(Vec<PathGroup>, bool)> {
            self.collapse_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.captured_collapse
                .lock()
                .unwrap()
                .push((filters, options, limit, offset));
            Ok((self.collapse_groups.clone(), self.collapse_served))
        }

        async fn search_file_rows(
            &self,
            _filters: Filters,
            _options: QueryOptions,
            _limit: u32,
            _offset: u32,
            _sort_by: Vec<FileRowSort>,
        ) -> search::Result<(FileRowsResult, bool)> {
            self.file_route_calls.fetch_add(1, AtomicOrdering::Relaxed);
            Ok((self.file_result.clone(), self.file_route_served))
        }

        async fn file_search(&self, request: FileSearchRequest) -> search::Result<FileRowsResult> {
            self.l2_file_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.captured_file.lock().unwrap().push(request);
            Ok(self.file_result.clone())
        }

        async fn file_search_facets(
            &self,
            _request: FileFacetsRequest,
        ) -> search::Result<FileFacetsResult> {
            Ok(self.file_facets_result.clone())
        }

        async fn path_typeahead(
            &self,
            _prefix: String,
            _options: QueryOptions,
            _limit: u32,
        ) -> search::Result<(Vec<String>, bool)> {
            self.typeahead_calls.fetch_add(1, AtomicOrdering::Relaxed);
            Ok((self.typeahead_result.clone(), self.typeahead_served))
        }

        async fn suggest(
            &self,
            _prefix: String,
            _limit: u32,
        ) -> search::Result<(Vec<String>, bool)> {
            self.suggest_calls.fetch_add(1, AtomicOrdering::Relaxed);
            Ok((self.suggest_result.clone(), self.suggest_served))
        }

        async fn file_path_typeahead(
            &self,
            _request: FilePathTypeaheadRequest,
        ) -> search::Result<Vec<String>> {
            self.l2_typeahead_calls
                .fetch_add(1, AtomicOrdering::Relaxed);
            Ok(self.l2_typeahead_result.clone())
        }

        fn eligible(&self, _query: &str) -> bool {
            self.eligible
        }

        fn healthy(&self) -> bool {
            self.healthy
        }

        fn typeahead_enabled(&self) -> bool {
            self.typeahead_enabled
        }

        fn file_search_route_text_enabled(&self) -> bool {
            self.file_route_enabled
        }

        fn collapse_enabled(&self) -> bool {
            self.collapse_enabled
        }

        fn features(&self) -> SearchFeatures {
            self.features
        }

        fn search_build_config(&self) -> SearchBuildConfig {
            self.build
        }
    }

    fn empty_search_input() -> TorrentContentSearchQueryInput {
        TorrentContentSearchQueryInput {
            aggregation_budget: MaybeUndefined::Undefined,
            cached: MaybeUndefined::Undefined,
            facets: MaybeUndefined::Undefined,
            has_next_page: MaybeUndefined::Undefined,
            info_hashes: None,
            limit: MaybeUndefined::Undefined,
            offset: MaybeUndefined::Undefined,
            order_by: None,
            page: MaybeUndefined::Undefined,
            query_string: MaybeUndefined::Undefined,
            total_count: MaybeUndefined::Undefined,
        }
    }

    fn routed_search_input() -> TorrentContentSearchQueryInput {
        let mut input = empty_search_input();
        input.query_string = MaybeUndefined::Value("matrix".into());
        input.order_by = Some(vec![TorrentContentOrderByInput {
            descending: MaybeUndefined::Value(true),
            field: TorrentContentOrderByField::Relevance,
        }]);
        input
    }

    fn routed_runtime(served: bool) -> MockRuntime {
        MockRuntime {
            composer_served: served,
            eligible: true,
            healthy: true,
            typeahead_enabled: true,
            ..MockRuntime::default()
        }
    }

    fn sample_item() -> SearchResultItem {
        let info_hash: bitmagnet_model::InfoHash =
            "0123456789abcdef0123456789abcdef01234567".parse().unwrap();
        SearchResultItem {
            info_hash,
            name: "Movie Name".into(),
            size: 5_000_000_000,
            content_type: Some(ModelContentType::Movie),
            published_at: 1_700_000_000,
            seeders: Some(11),
            leechers: Some(2),
            files_count: Some(1),
            video_resolution: Some("V1080p".into()),
            video_3d: None,
            video_codec: Some("x265".into()),
            release_group: Some("GROUP".into()),
            episodes: search::Episodes::new(),
            release_year: Some(2024),
            imdb_id: Some("tt0133093".into()),
            tmdb_id: Some("603".into()),
            info_hash_v1: Some([0x11; 20]),
            info_hash_v2: Some([0x22; 32]),
            torrent_content: bitmagnet_model::TorrentContent {
                id: "movie:tmdb:1".into(),
                info_hash,
                content_type: Some(ModelContentType::Movie),
                content_source: Some("tmdb".into()),
                content_id: Some("1".into()),
                languages: vec!["en".into()],
                video_resolution: Some("V1080p".into()),
                video_source: Some("BluRay".into()),
                video_codec: Some("x265".into()),
                release_group: Some("GROUP".into()),
                seeders: Some(11),
                leechers: Some(2),
                published_at: 1_700_000_000,
                size: 5_000_000_000,
                files_count: Some(1),
            },
            torrent_content_video_modifier: Some("REMUX".into()),
            torrent_content_created_at: 1_600_000_000,
            torrent_content_updated_at: 1_700_000_001,
            torrent: bitmagnet_model::Torrent {
                info_hash,
                name: "Movie Name".into(),
                size: 5_000_000_000,
                private: false,
                files_status: ModelFilesStatus::Multi,
                extension: None,
                files_count: Some(1),
                files_data: None,
                file_extensions: vec!["mkv".into()],
            },
            refine_files: vec![bitmagnet_model::BlobFile {
                index: 0,
                path: "Movie.Name.MKV".into(),
                extension: String::new(),
                size: 5_000_000_000,
            }],
            torrent_created_at: 1_600_000_000,
            torrent_updated_at: 1_700_000_001,
            torrent_meta_version: Some(2),
            torrent_sources: vec![RuntimeTorrentSourceInfo {
                key: "dht".into(),
                name: "DHT".into(),
                import_id: None,
                seeders: Some(11),
                leechers: Some(2),
                seen_count: 7,
                first_seen_at: 1_600_000_000,
                last_seen_at: 1_700_000_001,
            }],
            torrent_tags: vec!["trusted".into()],
            content: Some(ModelContent {
                content_type: ModelContentType::Movie,
                source: "tmdb".into(),
                id: "1".into(),
                title: "Movie".into(),
                release_year: Some(2024),
                original_language: Some("en".into()),
                original_title: None,
                overview: Some("Overview".into()),
                runtime: Some(120),
                popularity: Some(10.0),
                vote_average: Some(8.0),
                vote_count: Some(100),
            }),
            title: "Movie (2024)".into(),
            dht_seen_count: 7,
            dht_first_seen_at: Some(1_600_000_000),
            dht_last_seen_at: Some(1_700_000_001),
            query_string_rank: 0.75,
        }
    }

    fn aggregation_group(values: &[(&str, &str)]) -> AggregationGroup {
        AggregationGroup {
            label: "test".into(),
            logic: search::FacetLogic::Or,
            items: values
                .iter()
                .map(|(value, label)| {
                    (
                        (*value).to_owned(),
                        AggregationItem {
                            label: (*label).to_owned(),
                            count: 1,
                            is_estimate: false,
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn natural_sort_orders_numeric_labels() {
        let mut values = ["Source 10", "Source 2", "Source 01", "Source 1"];
        values.sort_by(|left, right| natural_cmp(left, right));
        assert_eq!(values, ["Source 1", "Source 01", "Source 2", "Source 10"]);
    }

    #[test]
    fn query_escape_matches_go_form_encoding_shape() {
        assert_eq!(query_escape("a b/c+"), "a+b%2Fc%2B");
    }

    #[test]
    fn extension_normalization_is_bounded_and_stable() {
        assert_eq!(
            normalize_extensions(vec![" .MKV ".into(), "mkv".into(), "SRT".into()]),
            vec!["mkv", "srt"]
        );
    }

    #[test]
    fn search_plan_shapes_find2_and_all_composer_options() {
        let mut input = routed_search_input();
        input.limit = MaybeUndefined::Value(1_000);
        input.page = MaybeUndefined::Value(2);
        input.offset = MaybeUndefined::Value(3);
        input.total_count = MaybeUndefined::Value(true);
        input.has_next_page = MaybeUndefined::Value(true);
        input.aggregation_budget = MaybeUndefined::Value(123.0);
        input.info_hashes = Some(vec![Hash20(
            "0123456789abcdef0123456789abcdef01234567".into(),
        )]);
        input.facets = MaybeUndefined::Value(TorrentContentFacetsInput {
            content_type: MaybeUndefined::Undefined,
            genre: MaybeUndefined::Undefined,
            language: MaybeUndefined::Undefined,
            published_at: MaybeUndefined::Value("P1Y".into()),
            release_year: MaybeUndefined::Undefined,
            size_range: MaybeUndefined::Value(SizeRangeInput {
                min: MaybeUndefined::Value(10),
                max: MaybeUndefined::Value(20),
            }),
            torrent_file_type: MaybeUndefined::Value(TorrentFileTypeFacetInput {
                aggregate: MaybeUndefined::Value(true),
                filter: Some(vec![FileType::Video]),
                logic: MaybeUndefined::Value(GraphqlFacetLogic::Or),
            }),
            torrent_source: MaybeUndefined::Undefined,
            torrent_tag: MaybeUndefined::Undefined,
            video_resolution: MaybeUndefined::Undefined,
            video_source: MaybeUndefined::Undefined,
        });

        let plan = build_search_plan(
            &input,
            SearchBuildConfig {
                file_extensions_jsonb: true,
                popularity_sort_default: true,
            },
        )
        .unwrap();
        assert_eq!(plan.path_limit, MAX_PATH_SEARCH_LIMIT);
        assert_eq!(plan.offset, 1_003);
        assert_eq!(plan.pg.options.limit, Some(1_000));
        assert_eq!(plan.pg.options.aggregation_budget, 123.0);
        assert!(plan.pg.options.total_count);
        assert!(plan.pg.options.has_next_page);
        assert_eq!(plan.pg.options.order.len(), 1);
        assert_eq!(
            plan.pg.options.order[0].field,
            TorrentContentOrderField::Seeders
        );
        assert!(plan.composer.combined.hydrate.files_data);
        assert!(plan.composer.combined.hydrate.content);
        assert_eq!(plan.composer.combined.options.limit, None);
        assert_eq!(plan.composer.combined.options.query, None);
        assert!(!plan.composer.combined.options.total_count);
        assert!(plan.composer.combined.options.facets[0].aggregate);
        let refine = plan.composer.refine.as_ref().unwrap();
        assert!(refine.hydrate.files_data);
        assert!(!refine.options.facets[0].aggregate);
        assert!(!plan.composer.agg.hydrate.files_data);
        assert!(plan.composer.agg.options.facets[0].aggregate);
        assert_eq!(plan.composer.agg.options.limit, None);
        assert!(plan.composer.agg.options.order.is_empty());
        assert!(plan.filters.extensions.contains(&"mkv".to_owned()));
        assert!(matches!(
            plan.pg.options.filter,
            Some(Criteria::And(ref criteria)) if criteria.len() == 3
        ));
    }

    #[test]
    fn omitted_limit_defaults_and_does_not_activate_page_arithmetic() {
        let mut input = empty_search_input();
        input.page = MaybeUndefined::Value(9);
        input.offset = MaybeUndefined::Value(4);
        let plan = build_search_plan(&input, SearchBuildConfig::default()).unwrap();
        assert_eq!(plan.pg.options.limit, Some(DEFAULT_PAGE_SIZE));
        assert_eq!(plan.offset, 4);
        assert!(plan.pg.options.order.is_empty());
    }

    #[tokio::test]
    async fn composer_served_false_falls_back_but_errors_do_not() {
        let runtime = routed_runtime(false);
        search_with_runtime(&runtime, routed_search_input())
            .await
            .unwrap();
        assert_eq!(runtime.composer_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(runtime.pg_calls.load(AtomicOrdering::Relaxed), 1);

        let runtime = MockRuntime {
            composer_error: true,
            ..routed_runtime(false)
        };
        let error = search_with_runtime(&runtime, routed_search_input())
            .await
            .err()
            .expect("composer error should propagate");
        assert!(error.message.contains("composer failed"));
        assert_eq!(runtime.pg_calls.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exact_route_predicate_bypasses_composer_for_structured_order() {
        let runtime = routed_runtime(true);
        let mut input = routed_search_input();
        input.order_by = Some(vec![TorrentContentOrderByInput {
            descending: MaybeUndefined::Value(true),
            field: TorrentContentOrderByField::Seeders,
        }]);
        search_with_runtime(&runtime, input).await.unwrap();
        assert_eq!(runtime.composer_calls.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(runtime.pg_calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn expanded_item_maps_canonical_id_hybrid_magnet_and_refined_files() {
        let mapped = map_search_item(sample_item()).unwrap();
        assert_eq!(mapped.id.as_str(), "movie:tmdb:1");
        assert_eq!(mapped.torrent.size, 5_000_000_000);
        assert_eq!(mapped.torrent.info_hash_v2.as_ref().unwrap().0.len(), 64);
        assert!(mapped.torrent.magnet_uri.contains("xt=urn:btih:"));
        assert!(mapped.torrent.magnet_uri.contains("xt=urn:btmh:1220"));
        let files = mapped.torrent.files.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].extension.as_deref(), Some("mkv"));
        assert_eq!(files[0].size, 5_000_000_000);
        assert_eq!(mapped.languages.unwrap()[0].name, "English");
    }

    #[test]
    fn all_nine_facets_map_with_natural_order_and_null_last() {
        let mut aggregations = search::Aggregations::new();
        aggregations.insert(
            TorrentContentFacet::ContentType.key().into(),
            aggregation_group(&[("movie", "Movie")]),
        );
        aggregations.insert(
            TorrentContentFacet::TorrentSource.key().into(),
            aggregation_group(&[("s10", "Source 10"), ("s2", "Source 2")]),
        );
        aggregations.insert(
            TorrentContentFacet::TorrentTag.key().into(),
            aggregation_group(&[("trusted", "Trusted")]),
        );
        aggregations.insert(
            TorrentContentFacet::FileType.key().into(),
            aggregation_group(&[("video", "Video")]),
        );
        aggregations.insert(
            TorrentContentFacet::Language.key().into(),
            aggregation_group(&[("en", "English")]),
        );
        aggregations.insert(
            TorrentContentFacet::ContentGenre.key().into(),
            aggregation_group(&[("action", "Action")]),
        );
        aggregations.insert(
            TorrentContentFacet::ReleaseYear.key().into(),
            aggregation_group(&[("2024", "2024"), ("null", "Unknown")]),
        );
        aggregations.insert(
            TorrentContentFacet::VideoResolution.key().into(),
            aggregation_group(&[("V1080p", "1080p"), ("null", "Unknown")]),
        );
        aggregations.insert(
            TorrentContentFacet::VideoSource.key().into(),
            aggregation_group(&[("BluRay", "BluRay"), ("null", "Unknown")]),
        );

        let mapped = map_aggregations(&aggregations).unwrap();
        assert!(mapped.content_type.is_some());
        assert!(mapped.torrent_tag.is_some());
        assert!(mapped.torrent_file_type.is_some());
        assert!(mapped.language.is_some());
        assert!(mapped.genre.is_some());
        assert_eq!(mapped.torrent_source.unwrap()[0].value, "s2");
        assert!(mapped.release_year.unwrap().last().unwrap().value.is_none());
        assert!(mapped
            .video_resolution
            .unwrap()
            .last()
            .unwrap()
            .value
            .is_none());
        assert!(mapped.video_source.unwrap().last().unwrap().value.is_none());
    }

    #[tokio::test]
    async fn file_search_routes_text_then_falls_back_to_l2() {
        let runtime = MockRuntime {
            eligible: true,
            healthy: true,
            file_route_enabled: true,
            features: SearchFeatures {
                file_search_enabled: true,
                ..SearchFeatures::default()
            },
            ..MockRuntime::default()
        };
        let input = FileSearchInput {
            extensions: Some(vec![".MKV".into()]),
            info_hash: MaybeUndefined::Undefined,
            limit: MaybeUndefined::Value(500),
            max_size: MaybeUndefined::Undefined,
            min_size: MaybeUndefined::Undefined,
            offset: MaybeUndefined::Undefined,
            query: MaybeUndefined::Value(" movie% ".into()),
            sort: None,
            total_count: MaybeUndefined::Value(false),
        };
        file_search_with_runtime(&runtime, input).await.unwrap();
        assert_eq!(runtime.file_route_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(runtime.l2_file_calls.load(AtomicOrdering::Relaxed), 1);
        let request = runtime.captured_file.lock().unwrap();
        assert_eq!(request[0].query, "movie%");
        assert_eq!(request[0].query_like_pattern, "movie\\%");
        assert_eq!(request[0].extensions, vec!["mkv"]);
        assert_eq!(request[0].limit, FILE_SEARCH_MAX_LIMIT);
        assert!(request[0].skip_total_count);
    }

    #[tokio::test]
    async fn routed_only_file_sort_never_leaks_to_l2() {
        let runtime = MockRuntime {
            eligible: true,
            healthy: true,
            file_route_enabled: true,
            features: SearchFeatures {
                file_search_enabled: true,
                ..SearchFeatures::default()
            },
            ..MockRuntime::default()
        };
        let input = FileSearchInput {
            extensions: None,
            info_hash: MaybeUndefined::Undefined,
            limit: MaybeUndefined::Undefined,
            max_size: MaybeUndefined::Undefined,
            min_size: MaybeUndefined::Undefined,
            offset: MaybeUndefined::Undefined,
            query: MaybeUndefined::Value("movie".into()),
            sort: Some(vec![FileSearchSortInput {
                descending: MaybeUndefined::Value(true),
                field: "seeders".into(),
            }]),
            total_count: MaybeUndefined::Undefined,
        };
        let error = file_search_with_runtime(&runtime, input)
            .await
            .err()
            .expect("routed-only sort should reject L2 fallback");
        assert!(error.message.contains("routed text-search path"));
        assert_eq!(runtime.l2_file_calls.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn collapse_clamps_limit_and_has_no_pg_fallback() {
        let runtime = MockRuntime {
            eligible: true,
            healthy: true,
            collapse_enabled: true,
            collapse_served: false,
            ..MockRuntime::default()
        };
        let error = collapse_paths_with_runtime(
            &runtime,
            TorrentContentCollapsePathsInput {
                limit: MaybeUndefined::Value(10_000),
                offset: MaybeUndefined::Value(4),
                query_string: "movie".into(),
            },
        )
        .await
        .err()
        .expect("unserved collapse should be unavailable");
        assert!(error.message.contains("unavailable"));
        let calls = runtime.captured_collapse.lock().unwrap();
        assert_eq!(calls[0].2, MAX_PATH_SEARCH_LIMIT);
        assert_eq!(calls[0].3, 4);
        assert_eq!(runtime.pg_calls.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn typeahead_falls_through_suggest_and_composer_to_l2() {
        let runtime = MockRuntime {
            eligible: true,
            healthy: true,
            typeahead_enabled: true,
            features: SearchFeatures {
                file_search_enabled: true,
                file_search_typeahead_rpc_enabled: true,
                ..SearchFeatures::default()
            },
            l2_typeahead_result: vec!["Movies/".into()],
            ..MockRuntime::default()
        };
        let result = path_typeahead_with_runtime(
            &runtime,
            PathTypeaheadInput {
                limit: MaybeUndefined::Value(999),
                prefix: " Movies ".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(result.suggestions, vec!["Movies/"]);
        assert_eq!(runtime.suggest_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(runtime.typeahead_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(runtime.l2_typeahead_calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn file_facet_mapping_skips_unknown_fields() {
        let runtime = MockRuntime {
            features: SearchFeatures {
                file_search_enabled: true,
                file_search_facets_enabled: true,
                ..SearchFeatures::default()
            },
            file_facets_result: FileFacetsResult {
                facets: vec![
                    FileFacet {
                        field: "future".into(),
                        buckets: Vec::new(),
                    },
                    FileFacet {
                        field: "extension".into(),
                        buckets: vec![FileFacetBucket {
                            value: "mkv".into(),
                            count: 2,
                            total_size: 10,
                        }],
                    },
                ],
            },
            ..MockRuntime::default()
        };
        let result =
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(file_search_facets_with_runtime(
                    &runtime,
                    FileSearchFacetsInput {
                        extensions: None,
                        facets: None,
                        max_size: MaybeUndefined::Undefined,
                        min_size: MaybeUndefined::Undefined,
                        query: MaybeUndefined::Undefined,
                    },
                ));
        assert_eq!(result.unwrap().facets.len(), 1);
    }
}
