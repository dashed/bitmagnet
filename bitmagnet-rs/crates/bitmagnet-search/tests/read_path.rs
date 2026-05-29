//! Read-path integration tests: drive `Search` and `GetFacets` **through the
//! server** (the `SearchService` RPC handlers → `query::run_search` /
//! `facets::run_facets`), over an in-RAM index seeded via `IndexDocument`.
//!
//! These exist because the unit tests in `query.rs`/`facets.rs` call the read
//! functions directly, and the smoke test only drives the write RPCs — so the
//! server's read delegation (and `run_facets` itself) was never exercised. A
//! green build alone was a false signal: an `unimplemented!()` in a read RPC
//! only panics when something calls it. These call it.

use bitmagnet_search::proto::search_service_server::SearchService;
use bitmagnet_search::proto::{
    ContentType, GetFacetsRequest, IndexDocumentRequest, Pagination, SearchFilters, SearchRequest,
    SortBy, TorrentDocument,
};
use bitmagnet_search::SearchServer;
use tonic::Request;

/// Build a movie document. `content_id` distinguishes classifications of the
/// same torrent (so two rows with one info_hash get distinct composite ids).
#[allow(clippy::too_many_arguments)]
fn movie(
    info_hash: Vec<u8>,
    name: &str,
    content_id: &str,
    resolution: &str,
    seeders: u32,
    release_year: u32,
) -> TorrentDocument {
    TorrentDocument {
        info_hash,
        torrent_name: name.to_owned(),
        content_title: name.to_owned(),
        original_title: String::new(),
        release_year,
        video_resolution: resolution.to_owned(),
        video_source: "BluRay".to_owned(),
        video_codec: "x264".to_owned(),
        genres: vec!["action".to_owned()],
        file_paths: vec![format!("{name}.mkv")],
        content_type: ContentType::Movie as i32,
        seeders,
        leechers: 1,
        files_count: 1,
        size: 1_000_000,
        published_at: 1_600_000_000,
        languages: vec!["en".to_owned()],
        file_extensions: vec!["mkv".to_owned()],
        video_3d: String::new(),
        video_modifier: String::new(),
        release_group: String::new(),
        audio_languages: vec!["en".to_owned()],
        content_source: "tmdb".to_owned(),
        content_id: content_id.to_owned(),
    }
}

async fn index(server: &SearchServer, doc: TorrentDocument) {
    server
        .index_document(Request::new(IndexDocumentRequest {
            document: Some(doc),
        }))
        .await
        .expect("index_document ok");
}

async fn search(
    server: &SearchServer,
    req: SearchRequest,
) -> bitmagnet_search::proto::SearchResponse {
    server
        .search(Request::new(req))
        .await
        .expect("search ok")
        .into_inner()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn search_matches_ranks_and_filters_through_the_server() {
    let server = SearchServer::in_ram().expect("in-ram server");
    index(
        &server,
        movie(vec![1; 20], "The Matrix", "603", "1080p", 100, 1999),
    )
    .await;
    index(
        &server,
        movie(vec![2; 20], "The Matrix Reloaded", "604", "2160p", 50, 2003),
    )
    .await;
    index(
        &server,
        movie(vec![3; 20], "Inception", "27205", "1080p", 200, 2010),
    )
    .await;

    // Free-text: "matrix" matches the two Matrix titles, not Inception.
    let resp = search(
        &server,
        SearchRequest {
            query: "matrix".to_owned(),
            filters: None,
            pagination: None,
            sort: vec![],
        },
    )
    .await;
    assert_eq!(resp.total_hits, 2, "two titles contain 'matrix'");
    assert_eq!(resp.hits.len(), 2);
    for hit in &resp.hits {
        let doc = hit.document.as_ref().expect("hit carries a document");
        assert!(doc.torrent_name.to_lowercase().contains("matrix"));
        assert!(hit.score > 0.0, "text hits must be scored");
    }

    // Empty query + sort by seeders desc → all three, ordered 200, 100, 50.
    let resp = search(
        &server,
        SearchRequest {
            query: String::new(),
            filters: None,
            pagination: Some(Pagination {
                limit: 10,
                offset: 0,
            }),
            sort: vec![SortBy {
                field: "seeders".to_owned(),
                descending: true,
            }],
        },
    )
    .await;
    assert_eq!(resp.total_hits, 3);
    let seeders: Vec<u32> = resp
        .hits
        .iter()
        .map(|h| h.document.as_ref().unwrap().seeders)
        .collect();
    assert_eq!(seeders, vec![200, 100, 50], "sorted by seeders descending");

    // Structured filter: release_year >= 2005 → only Inception (2010).
    let resp = search(
        &server,
        SearchRequest {
            query: String::new(),
            filters: Some(SearchFilters {
                release_year_min: Some(2005),
                ..Default::default()
            }),
            pagination: None,
            sort: vec![],
        },
    )
    .await;
    assert_eq!(resp.total_hits, 1);
    assert_eq!(resp.hits[0].document.as_ref().unwrap().content_id, "27205");
}

#[tokio::test]
async fn get_facets_counts_buckets_through_the_server() {
    let server = SearchServer::in_ram().expect("in-ram server");
    index(
        &server,
        movie(vec![1; 20], "The Matrix", "603", "1080p", 100, 1999),
    )
    .await;
    index(
        &server,
        movie(vec![2; 20], "The Matrix Reloaded", "604", "2160p", 50, 2003),
    )
    .await;
    index(
        &server,
        movie(vec![3; 20], "Inception", "27205", "1080p", 200, 2010),
    )
    .await;

    let resp = server
        .get_facets(Request::new(GetFacetsRequest {
            query: String::new(),
            filters: None,
            facet_fields: vec![
                "content_type".to_owned(),
                "video_resolution".to_owned(),
                "tmdb_id".to_owned(),
            ],
        }))
        .await
        .expect("get_facets ok")
        .into_inner();

    let facet = |field: &str| {
        resp.facets
            .iter()
            .find(|f| f.field == field)
            .unwrap_or_else(|| panic!("facet {field} present"))
    };
    let bucket = |field: &str, value: &str| -> u64 {
        facet(field)
            .buckets
            .iter()
            .find(|b| b.value == value)
            .map_or(0, |b| b.count)
    };

    // All three are movies.
    assert_eq!(bucket("content_type", "movie"), 3);
    // Two 1080p, one 2160p.
    assert_eq!(bucket("video_resolution", "1080p"), 2);
    assert_eq!(bucket("video_resolution", "2160p"), 1);
    // tmdb_id aggregates content_id over content_source == "tmdb".
    assert_eq!(bucket("tmdb_id", "603"), 1);
    assert_eq!(bucket("tmdb_id", "604"), 1);
    assert_eq!(bucket("tmdb_id", "27205"), 1);

    // Facets reflect the active query: "matrix" narrows to the two Matrix docs.
    let narrowed = server
        .get_facets(Request::new(GetFacetsRequest {
            query: "matrix".to_owned(),
            filters: None,
            facet_fields: vec!["video_resolution".to_owned()],
        }))
        .await
        .expect("get_facets ok")
        .into_inner();
    let res_facet = narrowed
        .facets
        .iter()
        .find(|f| f.field == "video_resolution")
        .expect("video_resolution facet");
    let total: u64 = res_facet.buckets.iter().map(|b| b.count).sum();
    assert_eq!(total, 2, "facet counts honour the active query");
}

#[tokio::test]
async fn multiple_classifications_of_one_info_hash_coexist_in_search() {
    let server = SearchServer::in_ram().expect("in-ram server");

    // One torrent (info_hash 9) classified two ways: distinct composite doc_ids.
    let ih = vec![9u8; 20];
    index(
        &server,
        movie(ih.clone(), "Double Feature", "100", "1080p", 10, 2000),
    )
    .await;
    index(
        &server,
        movie(ih.clone(), "Double Feature", "200", "1080p", 10, 2001),
    )
    .await;
    // An unrelated torrent that should NOT match the query below.
    index(
        &server,
        movie(vec![8; 20], "Unrelated", "300", "720p", 5, 1990),
    )
    .await;

    let resp = search(
        &server,
        SearchRequest {
            query: "double feature".to_owned(),
            filters: None,
            pagination: None,
            sort: vec![],
        },
    )
    .await;

    // Both classifications survive as independent documents and are both returned.
    assert_eq!(resp.total_hits, 2, "distinct classifications coexist");
    let mut got_doc_ids: Vec<String> = resp
        .hits
        .iter()
        .map(|h| {
            let d = h.document.as_ref().unwrap();
            assert_eq!(d.info_hash, ih, "both hits share the torrent's info_hash");
            // Reconstruct the composite doc_id from the hit (hex:type:source:id).
            format!("{}:movie:tmdb:{}", hex(&d.info_hash), d.content_id)
        })
        .collect();
    got_doc_ids.sort();
    assert_eq!(
        got_doc_ids,
        vec![
            format!("{}:movie:tmdb:100", hex(&ih)),
            format!("{}:movie:tmdb:200", hex(&ih)),
        ],
        "each hit reconstructs to its own composite doc_id"
    );
}
