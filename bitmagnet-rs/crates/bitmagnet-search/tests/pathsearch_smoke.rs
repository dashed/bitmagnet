//! End-to-end smoke tests for the L3 `PathSearchService`: boot the gRPC server
//! on an ephemeral TCP port and exercise it through the real tonic
//! client/server stack. Mirrors `tests/smoke.rs` for the main `SearchService`.

use bitmagnet_search::pathsearch::document::PathDocument;
use bitmagnet_search::pathsearch::PathSearchServer;
use bitmagnet_search::proto::path_search_health::ServingStatus;
use bitmagnet_search::proto::path_search_service_client::PathSearchServiceClient;
use bitmagnet_search::proto::path_search_service_server::PathSearchServiceServer;
use bitmagnet_search::proto::{HealthCheckRequest, PathCandidatesRequest};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Boot a (possibly pre-seeded) in-RAM `PathSearchServer` on an ephemeral port
/// and return a connected client.
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

fn doc(byte: u8, paths: &[&str]) -> PathDocument {
    PathDocument {
        info_hash: vec![byte; 20],
        paths: paths.iter().map(|p| (*p).to_owned()).collect(),
        size: 8_000_000_000,
        files_count: paths.len() as u64,
        seeders: 0,
        published_at: 1_650_000_000,
    }
}

#[tokio::test]
async fn health_check_round_trips_over_grpc() {
    let server = PathSearchServer::in_ram().expect("in-ram server");
    let mut client = boot(server).await;

    let response = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check succeeds")
        .into_inner();
    assert_eq!(response.status, ServingStatus::Serving as i32);
    assert_eq!(response.doc_count, 0);
    assert!(response.writable);
    // No follow loop in this smoke, so the watermark stays at its zero default.
    assert_eq!(response.watermark_epoch, 0);
}

#[tokio::test]
async fn path_candidates_round_trips_over_grpc() {
    let server = PathSearchServer::in_ram().expect("in-ram server");
    server
        .upsert_document(&doc(1, &["Show.S01E01.1080p.mkv"]))
        .await
        .expect("seed candidate doc");
    let mut client = boot(server).await;

    // The committed doc is visible to HealthCheck.
    let count = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check succeeds")
        .into_inner()
        .doc_count;
    assert_eq!(count, 1);

    // A real substring returns the torrent as a candidate, and the response is
    // always marked estimated (exact file counts are L1/L2's job).
    let response = client
        .path_candidates(PathCandidatesRequest {
            query: "s01e01".to_owned(),
            limit: 10,
            oversample: 0,
            sort: Vec::new(),
        })
        .await
        .expect("path candidates succeeds")
        .into_inner();
    assert_eq!(response.candidate_total, 1);
    assert_eq!(response.candidates.len(), 1);
    assert_eq!(response.candidates[0].info_hash, vec![1; 20]);
    assert!(response.estimated);

    // A sub-2-char query must not become a full-index scan.
    let response = client
        .path_candidates(PathCandidatesRequest {
            query: "a".to_owned(),
            limit: 10,
            oversample: 0,
            sort: Vec::new(),
        })
        .await
        .expect("short path candidates succeeds")
        .into_inner();
    assert_eq!(response.candidate_total, 0);
    assert!(response.candidates.is_empty());
}
