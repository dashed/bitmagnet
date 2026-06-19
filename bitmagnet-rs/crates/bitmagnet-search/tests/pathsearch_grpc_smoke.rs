//! Real (non-ignored) gRPC TCP smoke for the L3 `PathSearchService`, run over
//! the same ephemeral-port tonic client/server stack as `tests/smoke.rs` and
//! complementary to `tests/pathsearch_smoke.rs`. This file exercises behaviour
//! the base smoke does not: a multi-torrent `candidate_total`, fast-field `sort`
//! ordering surfaced through the wire, and the `watermark_epoch` field echoed by
//! `HealthCheck`.

use bitmagnet_search::pathsearch::document::PathDocument;
use bitmagnet_search::pathsearch::PathSearchServer;
use bitmagnet_search::proto::path_search_health::ServingStatus;
use bitmagnet_search::proto::path_search_service_client::PathSearchServiceClient;
use bitmagnet_search::proto::path_search_service_server::PathSearchServiceServer;
use bitmagnet_search::proto::{HealthCheckRequest, PathCandidatesRequest, SortBy};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Boot a (possibly pre-seeded) in-RAM `PathSearchServer` on an ephemeral port
/// and return a connected client. Mirrors `tests/smoke.rs::boot`.
async fn boot(server: PathSearchServer) -> PathSearchServiceClient<tonic::transport::Channel> {
    // Bind first so the OS accepts connections into the backlog before the
    // server task is polled — this removes the client/server startup race.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        Server::builder()
            .add_service(PathSearchServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server runs");
    });

    PathSearchServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("client connects")
}

fn doc(byte: u8, path: &str, seeders: u64) -> PathDocument {
    PathDocument {
        info_hash: vec![byte; 20],
        paths: vec![path.to_owned()],
        size: 8_000_000_000,
        files_count: 1,
        seeders,
        published_at: 1_650_000_000,
    }
}

/// Three torrents share a substring with distinct seeder counts; a
/// `sort=[seeders desc]` request must return all three as candidates, ordered by
/// descending seeders, with the fast-field sort value surfaced on each — proving
/// the sort path round-trips through the real gRPC stack, not just in-process.
#[tokio::test]
async fn candidates_sort_by_seeders_over_grpc() {
    let server = PathSearchServer::in_ram().expect("in-ram server");
    server
        .upsert_document(&doc(1, "Show.S01E01.720p.mkv", 5))
        .await
        .expect("seed low-seeder torrent");
    server
        .upsert_document(&doc(2, "Show.S01E01.1080p.mkv", 99))
        .await
        .expect("seed high-seeder torrent");
    server
        .upsert_document(&doc(3, "Show.S01E01.2160p.mkv", 42))
        .await
        .expect("seed mid-seeder torrent");
    let mut client = boot(server).await;

    let response = client
        .path_candidates(PathCandidatesRequest {
            query: "s01e01".to_owned(),
            limit: 10,
            oversample: 0,
            sort: vec![SortBy {
                field: "seeders".to_owned(),
                descending: true,
            }],
        })
        .await
        .expect("path candidates succeeds")
        .into_inner();

    // candidate_total is a torrent-doc count (all three match) and the response
    // is always estimated — exact file counts are L1/L2's job.
    assert_eq!(response.candidate_total, 3);
    assert_eq!(response.candidates.len(), 3);
    assert!(response.estimated);

    // Descending seeders 99 > 42 > 5 → info_hash lead bytes 2, 3, 1.
    let order: Vec<u8> = response.candidates.iter().map(|c| c.info_hash[0]).collect();
    assert_eq!(order, vec![2, 3, 1]);
    // The fast-field sort value rides along on each candidate.
    assert_eq!(response.candidates[0].sort_value, 99);
}

/// The follow loop publishes its position via `set_watermark_epoch`; the value
/// must travel through `HealthCheck` so the backend can observe sidecar
/// freshness. The base smoke only checks the zero default — this checks a real,
/// non-zero epoch over the wire.
#[tokio::test]
async fn health_check_echoes_watermark_epoch_over_grpc() {
    let server = PathSearchServer::in_ram().expect("in-ram server");
    server.set_watermark_epoch(1_700_000_000);
    let mut client = boot(server).await;

    let response = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check succeeds")
        .into_inner();
    assert_eq!(response.status, ServingStatus::Serving as i32);
    assert!(response.writable);
    assert_eq!(response.doc_count, 0);
    assert_eq!(response.watermark_epoch, 1_700_000_000);
}
