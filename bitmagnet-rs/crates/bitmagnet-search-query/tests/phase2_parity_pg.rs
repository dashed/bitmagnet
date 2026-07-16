//! Live-PostgreSQL differential parity for the full Phase-2 search builder.
//!
//! The Go generator resets and seeds a disposable PostgreSQL database, leaves
//! those rows in place, and writes the oracle corpus. Run Rust against that
//! same database:
//!
//! ```text
//! POSTGRES_DSN=postgres://... go test -tags integration ./internal/parity \
//!   -run TestGeneratePhase2SearchQueryParityFixtures
//! BITMAGNET_POSTGRES_DSN=postgres://... cargo test -p bitmagnet-search-query \
//!   --test phase2_parity_pg -- --ignored --nocapture
//! ```

#![recursion_limit = "256"]

use anyhow::Result;
use bitmagnet_diff::{
    driver::Driver,
    fixture::load_file,
    runner::{run, Options},
};
use bitmagnet_search_query::{
    search, Aggregations, HydrateOptions, SearchBuildConfig, SearchOptions, SearchResult,
    SearchResultItem,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::{PgPool, Row};
use std::collections::{BTreeMap, BTreeSet};
use tokio::runtime::Handle;

const SUBSYSTEM: &str = "searchquery_phase2";

#[derive(Deserialize)]
struct FixtureInput {
    options: SearchOptions,
    config: SearchBuildConfig,
}

struct Phase2SearchDriver {
    pool: PgPool,
    handle: Handle,
}

impl Driver for Phase2SearchDriver {
    fn subsystem(&self) -> &str {
        SUBSYSTEM
    }

    fn run(&self, input: &Value) -> Result<Value> {
        let input: FixtureInput = serde_json::from_value(input.clone())?;
        let result = self.handle.block_on(search(
            &self.pool,
            &input.options,
            &input.config,
            HydrateOptions::default(),
        ))?;
        let ids = result
            .items
            .iter()
            .map(|item| item.torrent_content.id.clone())
            .collect::<Vec<_>>();
        let micros = self
            .handle
            .block_on(fetch_published_at_micros(&self.pool, &ids))?;
        Ok(project_result(result, &micros))
    }
}

async fn fetch_published_at_micros(pool: &PgPool, ids: &[String]) -> Result<BTreeMap<String, i64>> {
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(
        "SELECT id, floor(EXTRACT(EPOCH FROM published_at) * 1000000)::bigint AS published_at_micros \
         FROM torrent_contents WHERE id = ANY($1::text[])",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;
    let mut values = BTreeMap::new();
    for row in rows {
        values.insert(
            row.try_get::<String, _>("id")?,
            row.try_get::<i64, _>("published_at_micros")?,
        );
    }
    Ok(values)
}

fn project_result(result: SearchResult, micros: &BTreeMap<String, i64>) -> Value {
    let infer_ids = result.items.iter().map(infer_id).collect::<Vec<_>>();
    let items = result
        .items
        .iter()
        .map(|item| project_item(item, micros))
        .collect::<Vec<_>>();

    json!({
        "total_count": result.total_count,
        "total_count_is_estimate": result.total_count_is_estimate,
        "has_next_page": result.has_next_page,
        "infer_ids": infer_ids,
        "items": items,
        "aggregations": project_aggregations(result.aggregations),
    })
}

fn project_aggregations(aggregations: Aggregations) -> Value {
    let mut output = Map::new();
    for (facet_key, group) in aggregations {
        let mut items = Map::new();
        for (value, item) in group.items {
            items.insert(
                value,
                json!({"count": item.count, "is_estimate": item.is_estimate}),
            );
        }
        output.insert(facet_key, Value::Object(items));
    }
    Value::Object(output)
}

fn project_item(item: &SearchResultItem, micros: &BTreeMap<String, i64>) -> Value {
    let mut sources = item.torrent_sources.clone();
    sources.sort_by(|left, right| left.key.cmp(&right.key));
    let sources = sources
        .into_iter()
        .map(|source| {
            json!({
                "key": source.key,
                "name": source.name,
                "import_id": source.import_id,
                "seeders": source.seeders,
                "leechers": source.leechers,
                "published_at": source.published_at,
                "seen_count": source.seen_count,
                "first_seen_at": source.first_seen_at,
                "last_seen_at": source.last_seen_at,
            })
        })
        .collect::<Vec<_>>();
    let mut tags = item.torrent_tags.clone();
    tags.sort();
    let content = item.content.as_ref().map(|content| {
        json!({
            "type": content.content_type.as_str(),
            "source": content.source,
            "id": content.id,
            "title": content.title,
            "release_year": content.release_year,
            "original_language": content.original_language,
            "original_title": content.original_title,
            "overview": content.overview,
            "runtime": content.runtime,
            // Go's encoding/json formats float32 using its shortest
            // round-trippable decimal. serde_json widens f32 to f64 first,
            // so normalize through the f32 display representation.
            "popularity": normalized_f32(content.popularity),
            "vote_average": normalized_f32(content.vote_average),
            "vote_count": content.vote_count,
        })
    });

    json!({
        "id": item.torrent_content.id,
        "infer_id": infer_id(item),
        "info_hash": item.info_hash.to_string(),
        "name": item.name,
        "title": item.title,
        "size": item.size,
        "content_type": item.content_type.map(|value| value.as_str()),
        "content_source": item.torrent_content.content_source,
        "content_id": item.torrent_content.content_id,
        "languages": item.torrent_content.languages,
        "video_resolution": item.video_resolution.map(|value| value.as_str()),
        "video_source": item.torrent_content.video_source,
        "video_codec": item.video_codec,
        "video_3d": item.video_3d.map(|value| value.as_str()),
        "video_modifier": item.torrent_content_video_modifier,
        "release_group": item.release_group,
        "episodes": item.episodes.0,
        "release_year": item.release_year,
        "imdb_id": item.imdb_id,
        "tmdb_id": item.tmdb_id,
        "seeders": item.seeders,
        "leechers": item.leechers,
        "files_count": item.files_count,
        "info_hash_v1": item.info_hash_v1.as_ref().map(|value| hex_lower(value)),
        "info_hash_v2": item.info_hash_v2.as_ref().map(|value| hex_lower(value)),
        "query_string_rank": normalized_f64(item.query_string_rank),
        "published_at": item.published_at,
        "published_at_micros": micros.get(&item.torrent_content.id),
        "created_at": item.torrent_content_created_at,
        "updated_at": item.torrent_content_updated_at,
        "torrent_content": {
            "id": item.torrent_content.id,
            "info_hash": item.torrent_content.info_hash.to_string(),
            "content_type": item.torrent_content.content_type.map(|value| value.as_str()),
            "content_source": item.torrent_content.content_source,
            "content_id": item.torrent_content.content_id,
            "languages": item.torrent_content.languages,
            "episodes": item.episodes.0,
            "video_resolution": item.torrent_content.video_resolution,
            "video_source": item.torrent_content.video_source,
            "video_codec": item.torrent_content.video_codec,
            "video_3d": item.video_3d.map(|value| value.as_str()),
            "video_modifier": item.torrent_content_video_modifier,
            "release_group": item.torrent_content.release_group,
            "seeders": item.torrent_content.seeders,
            "leechers": item.torrent_content.leechers,
            "published_at": item.torrent_content.published_at,
            "size": item.torrent_content.size,
            "files_count": item.torrent_content.files_count,
            "created_at": item.torrent_content_created_at,
            "updated_at": item.torrent_content_updated_at,
        },
        "torrent": {
            "name": item.torrent.name,
            "size": item.torrent.size,
            "private": item.torrent.private,
            "created_at": item.torrent_created_at,
            "updated_at": item.torrent_updated_at,
            "files_status": item.torrent.files_status.as_str(),
            "extension": item.torrent.extension,
            "files_count": item.torrent.files_count,
            "file_extensions": item.torrent.file_extensions,
            "info_hash_v1": item.info_hash_v1.as_ref().map(|value| hex_lower(value)),
            "info_hash_v2": item.info_hash_v2.as_ref().map(|value| hex_lower(value)),
            "meta_version": item.torrent_meta_version,
        },
        "sources": sources,
        "tags": tags,
        "content": content,
        "dht_seen_count": item.dht_seen_count,
        "dht_first_seen_at": item.dht_first_seen_at,
        "dht_last_seen_at": item.dht_last_seen_at,
    })
}

fn infer_id(item: &SearchResultItem) -> String {
    format!(
        "{}:{}:{}:{}",
        item.info_hash,
        item.torrent_content
            .content_type
            .map_or("?", |value| value.as_str()),
        item.torrent_content
            .content_source
            .as_deref()
            .unwrap_or("?"),
        item.torrent_content.content_id.as_deref().unwrap_or("?"),
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn normalized_f32(value: Option<f32>) -> Option<f64> {
    value.and_then(|value| value.to_string().parse().ok())
}

fn normalized_f64(value: f64) -> Value {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

#[test]
#[ignore = "requires Go-seeded disposable PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]
fn phase2_search_query_parity_via_live_postgres() {
    let dsn = match std::env::var("BITMAGNET_POSTGRES_DSN") {
        Ok(dsn) if !dsn.is_empty() => dsn,
        _ => {
            eprintln!(
                "BITMAGNET_POSTGRES_DSN not set; skipping Phase-2 live search-query parity test"
            );
            return;
        }
    };
    let fixtures = load_file(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../testdata/parity/searchquery/graphql_search.jsonl"
    ))
    .expect("load Phase-2 search-query corpus");
    assert_corpus_coverage(&fixtures);

    let runtime = tokio::runtime::Runtime::new().expect("create Tokio runtime");
    let pool = runtime
        .block_on(PgPool::connect(&dsn))
        .expect("connect to disposable fixture PostgreSQL");
    let driver = Phase2SearchDriver {
        pool,
        handle: runtime.handle().clone(),
    };
    let report = run(&fixtures, &driver, Options::default());
    assert!(report.ran >= 17, "Phase-2 corpus too small: {report}");
    assert!(
        report.ok(),
        "Phase-2 search-query parity diverged:\n{report}"
    );
}

fn assert_corpus_coverage(fixtures: &[bitmagnet_diff::fixture::Fixture]) {
    let phase2 = fixtures
        .iter()
        .filter(|fixture| fixture.subsystem == SUBSYSTEM)
        .collect::<Vec<_>>();
    let ids = phase2
        .iter()
        .map(|fixture| fixture.id.as_str())
        .collect::<BTreeSet<_>>();
    for required in [
        "criteria_and_or",
        "facets_all_exact",
        "file_extension_jsonb",
        "find2_off",
        "find2_on",
        "paging_limit_plus_one",
        "published_at_microseconds",
        "total_count_estimate",
    ] {
        assert!(
            ids.contains(required),
            "missing required fixture {required}"
        );
    }
    for field in [
        "relevance",
        "published_at",
        "updated_at",
        "size",
        "files_count",
        "seeders",
        "leechers",
        "name",
        "info_hash",
    ] {
        assert!(
            ids.contains(format!("order_{field}").as_str()),
            "missing order fixture for {field}"
        );
    }

    let facet_fixture = phase2
        .iter()
        .find(|fixture| fixture.id == "facets_all_exact")
        .expect("facets fixture");
    let facets = facet_fixture.input["options"]["facets"]
        .as_array()
        .expect("facets input array")
        .iter()
        .filter_map(|facet| facet["facet"].as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        facets,
        BTreeSet::from([
            "content_type",
            "torrent_source",
            "torrent_tag",
            "file_type",
            "language",
            "content_genre",
            "release_year",
            "video_resolution",
            "video_source",
        ])
    );

    let micro_fixture = phase2
        .iter()
        .find(|fixture| fixture.id == "published_at_microseconds")
        .expect("microsecond fixture");
    let has_subsecond = micro_fixture.expected["items"]
        .as_array()
        .expect("microsecond items")
        .iter()
        .any(|item| {
            let micros = item["published_at_micros"].as_i64().unwrap_or_default();
            let seconds = item["published_at"].as_i64().unwrap_or_default();
            micros.rem_euclid(1_000_000) != 0 && micros.div_euclid(1_000_000) == seconds
        });
    assert!(
        has_subsecond,
        "published_at corpus lacks a correctly floored microsecond row"
    );
    assert!(
        phase2
            .iter()
            .find(|fixture| fixture.id == "total_count_estimate")
            .and_then(|fixture| fixture.expected["total_count_is_estimate"].as_bool())
            .unwrap_or(false),
        "estimate fixture did not exercise budget_exceeded"
    );
}
