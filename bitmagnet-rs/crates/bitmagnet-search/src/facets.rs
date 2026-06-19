//! The 14 search facets exposed by bitmagnet, mirroring the Go `Facet`
//! implementations, and the `GetFacets` RPC entry point the server delegates to.
//!
//! Each facet is computed with a Tantivy built-in aggregation
//! (`tantivy::aggregation`, no cargo feature in 0.26) over the documents
//! matching `query` + `filters` (the same query [`crate::query`] builds for the
//! `Search` RPC, so counts reflect the active filters). Keyword facets are
//! [`TermsAggregation`]s over the STRING/FAST keyword fields; `files_count` is a
//! [`RangeAggregation`]; `file_type` folds the per-extension term counts into
//! [`bitmagnet_model::FileType`] buckets; and `tmdb_id` is a `content_id` terms
//! aggregation over the sub-set of docs whose `content_source == "tmdb"`.

use std::collections::BTreeMap;

use tantivy::aggregation::agg_req::{Aggregation, AggregationVariants, Aggregations};
use tantivy::aggregation::agg_result::{
    AggregationResult, AggregationResults, BucketEntries, BucketResult,
};
use tantivy::aggregation::bucket::{RangeAggregation, RangeAggregationRange, TermsAggregation};
use tantivy::aggregation::{AggContextParams, AggregationCollector, Key};
use tantivy::query::{BooleanQuery, Occur, QueryClone, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema};
use tantivy::{Index, IndexReader, Term};

use bitmagnet_model::FileType;

use crate::proto::{Facet, FacetBucket, GetFacetsRequest, GetFacetsResponse};
use crate::query::build_search_query;
use crate::schema::Fields;

/// Max distinct buckets requested per keyword facet. Generous enough for the
/// real facets (languages, genres, extensions, release groups, content ids);
/// values beyond this are dropped by Tantivy (reported in `sum_other_doc_count`).
const FACET_TERMS_SIZE: u32 = 1000;

/// `files_count` range buckets as `(label, from-inclusive, to-exclusive)`. There
/// is no Go reference for these (the plan lists `files_count` as sort-only), so
/// they are a sensible default; ranges are half-open `[from, to)`.
const FILES_COUNT_BUCKETS: [(&str, Option<f64>, Option<f64>); 5] = [
    ("1", Some(1.0), Some(2.0)),
    ("2-4", Some(2.0), Some(5.0)),
    ("5-9", Some(5.0), Some(10.0)),
    ("10-99", Some(10.0), Some(100.0)),
    ("100+", Some(100.0), None),
];

/// Aggregation name for the `tmdb_id` facet (run as its own query).
const TMDB_AGG: &str = "tmdb_id";

/// Run faceted aggregation for `request.facet_fields` (all 14 facets when the
/// list is empty) over the documents matching `request.query` +
/// `request.filters`, returning one [`Facet`] per requested field in the
/// canonical [`FacetType::ALL`] order. This is the entry point
/// [`crate::server::SearchServer`] delegates the `GetFacets` RPC to.
///
/// `tmdb_id` aggregates `content_id` over the docs whose `content_source` is
/// `"tmdb"` (a second aggregation pass); `file_type` folds the `file_extensions`
/// term counts into [`FileType`] buckets — note a torrent carrying two
/// extensions of the same type is counted in that type twice (the schema has no
/// dedicated file-type field). Empty matched sets yield empty bucket lists.
///
/// # Errors
/// Returns an error if an aggregation search fails (e.g. the bucket limit is
/// exceeded).
pub fn run_facets(
    index: &Index,
    reader: &IndexReader,
    fields: &Fields,
    request: GetFacetsRequest,
) -> anyhow::Result<GetFacetsResponse> {
    let searcher = reader.searcher();
    let schema = index.schema();
    let base = build_search_query(fields, &request.query, request.filters.as_ref());

    // Which facets to compute, in canonical order.
    let requested: Vec<FacetType> = if request.facet_fields.is_empty() {
        FacetType::ALL.to_vec()
    } else {
        FacetType::ALL
            .into_iter()
            .filter(|f| request.facet_fields.iter().any(|k| k.as_str() == f.key()))
            .collect()
    };

    // One aggregation pass for every facet except tmdb_id (which needs an extra
    // content_source filter and so runs as its own query).
    let mut aggs: Aggregations = Aggregations::default();
    for &facet in &requested {
        if facet != FacetType::TmdbId {
            aggs.insert(
                facet.key().to_owned(),
                build_facet_agg(facet, fields, &schema),
            );
        }
    }
    let results = if aggs.is_empty() {
        None
    } else {
        let collector = AggregationCollector::from_aggs(aggs, AggContextParams::default());
        Some(searcher.search(&*base, &collector)?)
    };

    // tmdb_id: aggregate content_id over base AND content_source == "tmdb".
    let tmdb_results = if requested.contains(&FacetType::TmdbId) {
        let tmdb_query = BooleanQuery::new(vec![
            (Occur::Must, base.box_clone()),
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(fields.content_source, "tmdb"),
                    IndexRecordOption::Basic,
                )),
            ),
        ]);
        let mut tmdb_aggs: Aggregations = Aggregations::default();
        tmdb_aggs.insert(
            TMDB_AGG.to_owned(),
            terms_agg(schema.get_field_name(fields.content_id)),
        );
        let collector = AggregationCollector::from_aggs(tmdb_aggs, AggContextParams::default());
        Some(searcher.search(&tmdb_query, &collector)?)
    } else {
        None
    };

    // Assemble in canonical order.
    let facets = FacetType::ALL
        .into_iter()
        .filter(|f| requested.contains(f))
        .map(|facet| {
            let buckets = match facet {
                FacetType::TmdbId => terms_buckets(tmdb_results.as_ref(), TMDB_AGG),
                FacetType::FilesCount => range_buckets(results.as_ref(), facet.key()),
                FacetType::FileType => {
                    fold_file_types(terms_buckets(results.as_ref(), facet.key()))
                }
                _ => terms_buckets(results.as_ref(), facet.key()),
            };
            Facet {
                field: facet.key().to_owned(),
                buckets,
            }
        })
        .collect();

    Ok(GetFacetsResponse { facets })
}

/// Build the aggregation for one facet: a range aggregation for `files_count`,
/// otherwise a terms aggregation over the facet's backing field.
fn build_facet_agg(facet: FacetType, fields: &Fields, schema: &Schema) -> Aggregation {
    if facet == FacetType::FilesCount {
        Aggregation {
            agg: AggregationVariants::Range(RangeAggregation {
                field: schema.get_field_name(fields.files_count).to_owned(),
                ranges: FILES_COUNT_BUCKETS
                    .iter()
                    .map(|&(label, from, to)| RangeAggregationRange {
                        key: Some(label.to_owned()),
                        from,
                        to,
                    })
                    .collect(),
                keyed: false,
            }),
            sub_aggregation: Aggregations::default(),
        }
    } else {
        terms_agg(schema.get_field_name(facet_field(facet, fields)))
    }
}

/// A terms aggregation over `field_name`, capped at [`FACET_TERMS_SIZE`] buckets.
fn terms_agg(field_name: &str) -> Aggregation {
    Aggregation {
        agg: AggregationVariants::Terms(TermsAggregation {
            field: field_name.to_owned(),
            size: Some(FACET_TERMS_SIZE),
            ..TermsAggregation::default()
        }),
        sub_aggregation: Aggregations::default(),
    }
}

/// The schema field a facet aggregates. `file_type` aggregates `file_extensions`
/// (then folds to types); `tmdb_id` aggregates `content_id`.
fn facet_field(facet: FacetType, fields: &Fields) -> Field {
    match facet {
        FacetType::ContentType => fields.content_type,
        FacetType::FilesCount => fields.files_count,
        FacetType::FileType => fields.file_extensions,
        FacetType::Language => fields.languages,
        FacetType::Genre => fields.genres,
        FacetType::ReleaseYear => fields.release_year,
        FacetType::VideoResolution => fields.video_resolution,
        FacetType::VideoSource => fields.video_source,
        FacetType::VideoCodec => fields.video_codec,
        FacetType::Video3d => fields.video_3d,
        FacetType::VideoModifier => fields.video_modifier,
        FacetType::ReleaseGroup => fields.release_group,
        FacetType::TmdbId => fields.content_id,
        FacetType::AudioLanguage => fields.audio_languages,
    }
}

/// Extract a terms aggregation's buckets, sorted by count descending then value
/// ascending for a deterministic order.
fn terms_buckets(results: Option<&AggregationResults>, name: &str) -> Vec<FacetBucket> {
    let Some(AggregationResult::BucketResult(BucketResult::Terms { buckets, .. })) =
        results.and_then(|r| r.0.get(name))
    else {
        return Vec::new();
    };
    let mut out: Vec<FacetBucket> = buckets
        .iter()
        .map(|b| FacetBucket {
            value: key_to_string(&b.key),
            count: b.doc_count,
        })
        .collect();
    sort_buckets(&mut out);
    out
}

/// Extract a range aggregation's buckets in definition order, dropping empties.
fn range_buckets(results: Option<&AggregationResults>, name: &str) -> Vec<FacetBucket> {
    let Some(AggregationResult::BucketResult(BucketResult::Range { buckets })) =
        results.and_then(|r| r.0.get(name))
    else {
        return Vec::new();
    };
    // `BucketEntries::iter` is private in 0.26; match the variant directly. A
    // non-keyed range aggregation always yields the `Vec` form.
    let BucketEntries::Vec(entries) = buckets else {
        return Vec::new();
    };
    entries
        .iter()
        .filter(|b| b.doc_count > 0)
        .map(|b| FacetBucket {
            value: key_to_string(&b.key),
            count: b.doc_count,
        })
        .collect()
}

/// Fold per-extension buckets into [`FileType`] buckets via
/// [`FileType::from_extension`], summing counts. Extensions with no known type
/// are dropped. (A torrent with several extensions of the same type is counted
/// once per extension — an overcount the schema cannot avoid without a dedicated
/// file-type field.)
fn fold_file_types(ext_buckets: Vec<FacetBucket>) -> Vec<FacetBucket> {
    let mut by_type: BTreeMap<&'static str, u64> = BTreeMap::new();
    for bucket in ext_buckets {
        if let Some(ft) = FileType::from_extension(&bucket.value) {
            *by_type.entry(ft.as_str()).or_insert(0) += bucket.count;
        }
    }
    let mut out: Vec<FacetBucket> = by_type
        .into_iter()
        .map(|(value, count)| FacetBucket {
            value: value.to_owned(),
            count,
        })
        .collect();
    sort_buckets(&mut out);
    out
}

/// Render an aggregation bucket key as the proto facet value. Read directly from
/// the typed results (no JSON round-trip), so a `u64` field surfaces as
/// [`Key::U64`]; the `F64` arm renders whole numbers without a decimal point.
fn key_to_string(key: &Key) -> String {
    match key {
        Key::Str(s) => s.clone(),
        Key::U64(n) => n.to_string(),
        Key::I64(n) => n.to_string(),
        Key::F64(n) if n.fract() == 0.0 => (*n as i64).to_string(),
        Key::F64(n) => n.to_string(),
    }
}

/// Sort facet buckets by count descending, then value ascending (stable order).
fn sort_buckets(buckets: &mut [FacetBucket]) {
    buckets.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
}

/// A search facet: a field whose distinct values are aggregated into counts
/// alongside a result set.
///
/// The ordering matches the Go facet registry so the `GetFacets` gRPC response
/// is stable across the Go and Rust implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FacetType {
    /// Primary content classification (movie, tv show, ...).
    ContentType,
    /// Number of files in the torrent, bucketed.
    FilesCount,
    /// Per-file content type.
    FileType,
    /// Detected content languages.
    Language,
    /// Release genre(s).
    Genre,
    /// Release year.
    ReleaseYear,
    /// Video resolution (1080p, 2160p, ...).
    VideoResolution,
    /// Video source (`BluRay`, `WEB-DL`, ...).
    VideoSource,
    /// Video codec (x264, x265, ...).
    VideoCodec,
    /// Stereoscopic 3D layout, when present.
    Video3d,
    /// Additional video modifiers (`REMUX`, `PROPER`, ...).
    VideoModifier,
    /// Scene / p2p release group.
    ReleaseGroup,
    /// TMDB identifier of the matched title.
    TmdbId,
    /// Audio track languages.
    AudioLanguage,
}

impl FacetType {
    /// Every facet, in the canonical Go ordering.
    pub const ALL: [FacetType; 14] = [
        FacetType::ContentType,
        FacetType::FilesCount,
        FacetType::FileType,
        FacetType::Language,
        FacetType::Genre,
        FacetType::ReleaseYear,
        FacetType::VideoResolution,
        FacetType::VideoSource,
        FacetType::VideoCodec,
        FacetType::Video3d,
        FacetType::VideoModifier,
        FacetType::ReleaseGroup,
        FacetType::TmdbId,
        FacetType::AudioLanguage,
    ];

    /// The stable string key used by the gRPC and GraphQL APIs.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            FacetType::ContentType => "content_type",
            FacetType::FilesCount => "files_count",
            FacetType::FileType => "file_type",
            FacetType::Language => "language",
            FacetType::Genre => "genre",
            FacetType::ReleaseYear => "release_year",
            FacetType::VideoResolution => "video_resolution",
            FacetType::VideoSource => "video_source",
            FacetType::VideoCodec => "video_codec",
            FacetType::Video3d => "video_3d",
            FacetType::VideoModifier => "video_modifier",
            FacetType::ReleaseGroup => "release_group",
            FacetType::TmdbId => "tmdb_id",
            FacetType::AudioLanguage => "audio_language",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run_facets, FacetType};
    use crate::index::{reader, register_tokenizer, writer};
    use crate::proto::{ContentType, GetFacetsRequest, GetFacetsResponse, TorrentDocument};
    use crate::schema::{build_schema, Fields};
    use std::collections::{BTreeMap, HashSet};
    use tantivy::Index;

    #[test]
    fn fourteen_facets_with_unique_keys() {
        assert_eq!(FacetType::ALL.len(), 14);
        let keys: HashSet<&str> = FacetType::ALL.iter().map(|facet| facet.key()).collect();
        assert_eq!(keys.len(), 14, "facet keys must be unique");
    }

    /// A document with sensible defaults; tests override the facet-relevant
    /// fields.
    fn doc(info_hash: u8) -> TorrentDocument {
        TorrentDocument {
            info_hash: vec![info_hash; 20],
            torrent_name: format!("torrent {info_hash}"),
            content_title: String::new(),
            original_title: String::new(),
            release_year: 2020,
            video_resolution: "1080p".to_owned(),
            video_source: "BluRay".to_owned(),
            video_codec: "x264".to_owned(),
            genres: vec!["action".to_owned()],
            file_paths: Vec::new(),
            content_type: ContentType::Movie as i32,
            seeders: 1,
            leechers: 0,
            files_count: 1,
            size: 1,
            published_at: 0,
            languages: vec!["en".to_owned()],
            file_extensions: vec!["mkv".to_owned()],
            video_3d: String::new(),
            video_modifier: String::new(),
            release_group: "GRP".to_owned(),
            audio_languages: vec!["en".to_owned()],
            content_source: "tmdb".to_owned(),
            content_id: "1".to_owned(),
        }
    }

    fn index_docs(docs: &[TorrentDocument]) -> (Index, tantivy::IndexReader, Fields) {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index);
        let fields = Fields::from_schema(&index.schema()).unwrap();
        let mut w = writer(&index).unwrap();
        for d in docs {
            crate::indexer::upsert(&w, &fields, d).unwrap();
        }
        w.commit().unwrap();
        let r = reader(&index).unwrap();
        r.reload().unwrap();
        (index, r, fields)
    }

    fn facets_for(
        index: &Index,
        reader: &tantivy::IndexReader,
        fields: &Fields,
        keys: &[&str],
    ) -> GetFacetsResponse {
        run_facets(
            index,
            reader,
            fields,
            GetFacetsRequest {
                query: String::new(),
                filters: None,
                facet_fields: keys.iter().map(|k| (*k).to_owned()).collect(),
            },
        )
        .unwrap()
    }

    /// Pull one facet's buckets as a value -> count map.
    fn bucket_map(resp: &GetFacetsResponse, field: &str) -> BTreeMap<String, u64> {
        resp.facets
            .iter()
            .find(|f| f.field == field)
            .map(|f| {
                f.buckets
                    .iter()
                    .map(|b| (b.value.clone(), b.count))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn sample_docs() -> Vec<TorrentDocument> {
        let mut d1 = doc(1);
        d1.content_type = ContentType::Movie as i32;
        d1.languages = vec!["en".to_owned()];
        d1.genres = vec!["action".to_owned()];
        d1.release_year = 2020;
        d1.files_count = 1;
        d1.file_extensions = vec!["mkv".to_owned()];
        d1.video_resolution = "1080p".to_owned();
        d1.content_source = "tmdb".to_owned();
        d1.content_id = "100".to_owned();

        let mut d2 = doc(2);
        d2.content_type = ContentType::Movie as i32;
        d2.languages = vec!["en".to_owned(), "fr".to_owned()];
        d2.genres = vec!["action".to_owned(), "drama".to_owned()];
        d2.release_year = 2021;
        d2.files_count = 3;
        d2.file_extensions = vec!["mkv".to_owned(), "srt".to_owned()];
        d2.video_resolution = "1080p".to_owned();
        d2.content_source = "tmdb".to_owned();
        d2.content_id = "200".to_owned();

        let mut d3 = doc(3);
        d3.content_type = ContentType::TvShow as i32;
        d3.languages = vec!["fr".to_owned()];
        d3.genres = vec!["comedy".to_owned()];
        d3.release_year = 2020;
        d3.files_count = 12;
        d3.file_extensions = vec!["mp4".to_owned()];
        d3.video_resolution = "2160p".to_owned();
        d3.content_source = "imdb".to_owned(); // excluded from the tmdb_id facet
        d3.content_id = "300".to_owned();

        vec![d1, d2, d3]
    }

    #[test]
    fn content_type_and_resolution_term_counts() {
        let docs = sample_docs();
        let (index, reader, fields) = index_docs(&docs);
        let resp = facets_for(
            &index,
            &reader,
            &fields,
            &["content_type", "video_resolution"],
        );

        let ct = bucket_map(&resp, "content_type");
        assert_eq!(ct.get("movie"), Some(&2));
        assert_eq!(ct.get("tv_show"), Some(&1));

        let res = bucket_map(&resp, "video_resolution");
        assert_eq!(res.get("1080p"), Some(&2));
        assert_eq!(res.get("2160p"), Some(&1));
    }

    #[test]
    fn multi_valued_language_and_genre_count_each_value() {
        let (index, reader, fields) = index_docs(&sample_docs());
        let resp = facets_for(&index, &reader, &fields, &["language", "genre"]);

        // d1=[en], d2=[en,fr], d3=[fr] -> en in 2 docs, fr in 2 docs.
        let langs = bucket_map(&resp, "language");
        assert_eq!(langs.get("en"), Some(&2));
        assert_eq!(langs.get("fr"), Some(&2));

        // d1=[action], d2=[action,drama], d3=[comedy].
        let genres = bucket_map(&resp, "genre");
        assert_eq!(genres.get("action"), Some(&2));
        assert_eq!(genres.get("drama"), Some(&1));
        assert_eq!(genres.get("comedy"), Some(&1));
    }

    #[test]
    fn release_year_numeric_buckets() {
        let (index, reader, fields) = index_docs(&sample_docs());
        let resp = facets_for(&index, &reader, &fields, &["release_year"]);
        let years = bucket_map(&resp, "release_year");
        assert_eq!(years.get("2020"), Some(&2));
        assert_eq!(years.get("2021"), Some(&1));
    }

    #[test]
    fn files_count_range_buckets() {
        let (index, reader, fields) = index_docs(&sample_docs());
        let resp = facets_for(&index, &reader, &fields, &["files_count"]);
        let buckets = bucket_map(&resp, "files_count");
        // d1=1 -> "1"; d2=3 -> "2-4"; d3=12 -> "10-99". Empty ranges dropped.
        assert_eq!(buckets.get("1"), Some(&1));
        assert_eq!(buckets.get("2-4"), Some(&1));
        assert_eq!(buckets.get("10-99"), Some(&1));
        assert_eq!(buckets.get("5-9"), None);
    }

    #[test]
    fn file_type_folds_extensions_into_types() {
        let (index, reader, fields) = index_docs(&sample_docs());
        let resp = facets_for(&index, &reader, &fields, &["file_type"]);
        let types = bucket_map(&resp, "file_type");
        // mkv (d1,d2) + mp4 (d3) -> video=3; srt (d2) -> subtitles=1.
        assert_eq!(types.get("video"), Some(&3));
        assert_eq!(types.get("subtitles"), Some(&1));
    }

    #[test]
    fn tmdb_id_only_counts_tmdb_sourced_docs() {
        let (index, reader, fields) = index_docs(&sample_docs());
        let resp = facets_for(&index, &reader, &fields, &["tmdb_id"]);
        let ids = bucket_map(&resp, "tmdb_id");
        // d1 (tmdb, 100), d2 (tmdb, 200) counted; d3 (imdb, 300) excluded.
        assert_eq!(ids.get("100"), Some(&1));
        assert_eq!(ids.get("200"), Some(&1));
        assert_eq!(ids.get("300"), None);
    }

    #[test]
    fn empty_request_returns_all_facets_in_canonical_order() {
        let (index, reader, fields) = index_docs(&sample_docs());
        let resp = run_facets(
            &index,
            &reader,
            &fields,
            GetFacetsRequest {
                query: String::new(),
                filters: None,
                facet_fields: Vec::new(),
            },
        )
        .unwrap();

        let returned: Vec<&str> = resp.facets.iter().map(|f| f.field.as_str()).collect();
        let expected: Vec<&str> = FacetType::ALL.iter().map(|f| f.key()).collect();
        assert_eq!(returned, expected);
    }

    #[test]
    fn requested_subset_keeps_canonical_order() {
        let (index, reader, fields) = index_docs(&sample_docs());
        // Request out of order; response must follow FacetType::ALL order.
        let resp = facets_for(&index, &reader, &fields, &["genre", "content_type"]);
        let returned: Vec<&str> = resp.facets.iter().map(|f| f.field.as_str()).collect();
        assert_eq!(returned, vec!["content_type", "genre"]);
    }
}
