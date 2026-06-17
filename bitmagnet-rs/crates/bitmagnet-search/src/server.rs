//! The [`SearchServer`]: bitmagnet's `tonic` [`SearchService`] implementation.
//!
//! Every RPC is a Phase 3 stub returning [`tonic::Status::unimplemented`],
//! except [`SearchServer::health_check`], which always reports healthy so the
//! smoke test and container/orchestrator probes have something to hit before the
//! Tantivy backend exists.

use tonic::{Request, Response, Status, Streaming};

use crate::proto::health_check_response::ServingStatus;
use crate::proto::search_service_server::SearchService;
use crate::proto::{
    BatchIndexResponse, DeleteDocumentRequest, DeleteDocumentResponse, GetFacetsRequest,
    GetFacetsResponse, HealthCheckRequest, HealthCheckResponse, IndexDocumentRequest,
    IndexDocumentResponse, SearchRequest, SearchResponse,
};

/// gRPC entry point for the search sidecar.
///
/// Phase 3 gives this real state (a Tantivy index handle, a reader pool and the
/// resolved [`crate::schema`] fields); for now it is unit-like.
#[derive(Debug, Default, Clone)]
pub struct SearchServer {}

#[tonic::async_trait]
impl SearchService for SearchServer {
    async fn index_document(
        &self,
        _request: Request<IndexDocumentRequest>,
    ) -> Result<Response<IndexDocumentResponse>, Status> {
        Err(Status::unimplemented("Phase 3"))
    }

    async fn batch_index(
        &self,
        _request: Request<Streaming<IndexDocumentRequest>>,
    ) -> Result<Response<BatchIndexResponse>, Status> {
        Err(Status::unimplemented("Phase 3"))
    }

    async fn delete_document(
        &self,
        _request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<DeleteDocumentResponse>, Status> {
        Err(Status::unimplemented("Phase 3"))
    }

    async fn search(
        &self,
        _request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Err(Status::unimplemented("Phase 3"))
    }

    async fn get_facets(
        &self,
        _request: Request<GetFacetsRequest>,
    ) -> Result<Response<GetFacetsResponse>, Status> {
        Err(Status::unimplemented("Phase 3"))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
            doc_count: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::SearchServer;
    use crate::proto::health_check_response::ServingStatus;
    use crate::proto::search_service_server::SearchService;
    use crate::proto::HealthCheckRequest;
    use tonic::Request;

    #[tokio::test]
    async fn health_check_reports_serving() {
        let server = SearchServer::default();
        let response = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .expect("health_check should return Ok");
        assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);
    }
}
