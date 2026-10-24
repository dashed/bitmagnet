//! End-to-end smoke tests: boot the gRPC server on an ephemeral TCP port and
//! exercise it through the real tonic client/server stack.

use bitmagnet_search::proto::health_check_response::ServingStatus;
use bitmagnet_search::proto::search_service_client::SearchServiceClient;
use bitmagnet_search::proto::search_service_server::SearchServiceServer;
use bitmagnet_search::proto::{
    ContentType, HealthCheckRequest, IndexDocumentRequest, TorrentDocument,
};
use bitmagnet_search::SearchServer;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Boot an in-RAM `SearchServer` on an ephemeral port and return a connected
/// client.
async fn boot() -> SearchServiceClient<tonic::transport::Channel> {
    // Bind first so the OS accepts connections into the backlog before the
    // server task is polled — this removes the client/server startup race.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    let server = SearchServer::in_ram().expect("in-ram server");
    tokio::spawn(async move {
        Server::builder()
            .add_service(SearchServiceServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server runs");
    });

    SearchServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("client connects")
}

fn doc(info_hash: Vec<u8>, name: &str) -> TorrentDocument {
    TorrentDocument {
        info_hash,
        torrent_name: name.to_owned(),
        content_title: name.to_owned(),
        original_title: String::new(),
        release_year: 2021,
        video_resolution: "2160p".to_owned(),
        video_source: "BluRay".to_owned(),
        video_codec: "x265".to_owned(),
        genres: vec!["drama".to_owned()],
        file_paths: vec![format!("{name}.mkv")],
        content_type: ContentType::Movie as i32,
        seeders: 42,
        leechers: 7,
        files_count: 1,
        size: 8_000_000_000,
        published_at: 1_650_000_000,
        languages: vec!["en".to_owned()],
        file_extensions: vec!["mkv".to_owned()],
        video_3d: String::new(),
        video_modifier: String::new(),
        release_group: "GRP".to_owned(),
        audio_languages: vec!["en".to_owned()],
        content_source: "tmdb".to_owned(),
        content_id: "42".to_owned(),
    }
}

#[tokio::test]
async fn health_check_round_trips_over_grpc() {
    let mut client = boot().await;
    let response = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check succeeds");
    let response = response.into_inner();
    assert_eq!(response.status, ServingStatus::Serving as i32);
    assert_eq!(response.doc_count, 0);
}

#[tokio::test]
async fn batch_index_streams_and_counts_over_grpc() {
    let mut client = boot().await;

    let requests = (0u8..3)
        .map(|i| IndexDocumentRequest {
            document: Some(doc(vec![i + 1; 20], &format!("Title {i}"))),
        })
        .collect::<Vec<_>>();

    let response = client
        .batch_index(tokio_stream::iter(requests))
        .await
        .expect("batch_index succeeds")
        .into_inner();
    assert_eq!(response.indexed_count, 3);
    assert_eq!(response.error_count, 0);

    // The committed batch is visible to HealthCheck.
    let count = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check succeeds")
        .into_inner()
        .doc_count;
    assert_eq!(count, 3);
}
