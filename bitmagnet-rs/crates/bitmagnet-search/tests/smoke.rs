//! End-to-end smoke test: boot the gRPC server on an ephemeral TCP port and
//! confirm `HealthCheck` succeeds through the real tonic client/server stack.

use bitmagnet_search::proto::health_check_response::ServingStatus;
use bitmagnet_search::proto::search_service_client::SearchServiceClient;
use bitmagnet_search::proto::search_service_server::SearchServiceServer;
use bitmagnet_search::proto::HealthCheckRequest;
use bitmagnet_search::SearchServer;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn health_check_round_trips_over_grpc() {
    // Bind first so the OS accepts connections into the backlog before the
    // server task is polled — this removes the client/server startup race.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        Server::builder()
            .add_service(SearchServiceServer::new(SearchServer::default()))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server runs");
    });

    let mut client = SearchServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("client connects");
    let response = client
        .health_check(HealthCheckRequest {})
        .await
        .expect("health check succeeds");
    assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);
}
