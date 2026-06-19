//! The [`SearchServer`]: bitmagnet's `tonic` [`SearchService`] implementation.
//!
//! Write RPCs (`IndexDocument`, `BatchIndex`, `DeleteDocument`) drive the single
//! Tantivy [`IndexWriter`], serialized behind a [`tokio::sync::Mutex`]. Read
//! RPCs (`Search`, `GetFacets`) are delegated to [`crate::query::run_search`]
//! and [`crate::facets::run_facets`], whose bodies the read path fills in.
//! `HealthCheck` reports the live document count from the reader.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status, Streaming};

use tantivy::{Index, IndexReader, IndexWriter};

use crate::index;
use crate::indexer;
use crate::proto::health_check_response::ServingStatus;
use crate::proto::search_service_server::SearchService;
use crate::proto::{
    BatchIndexResponse, DeleteDocumentRequest, DeleteDocumentResponse, GetFacetsRequest,
    GetFacetsResponse, HealthCheckRequest, HealthCheckResponse, IndexDocumentRequest,
    IndexDocumentResponse, SearchRequest, SearchResponse,
};
use crate::schema::{build_schema, Fields};

/// gRPC entry point for the search sidecar.
///
/// Cheap to [`Clone`] (tonic clones the service per connection): the index and
/// reader are `Arc`-backed and the writer is shared behind an `Arc<Mutex<_>>`,
/// so every clone drives the same single writer.
#[derive(Clone)]
pub struct SearchServer {
    index: Index,
    reader: IndexReader,
    fields: Fields,
    /// Tantivy allows one writer per index; all ingest funnels through this.
    writer: Arc<Mutex<IndexWriter>>,
}

impl SearchServer {
    /// Build a server over an already-opened `index` whose tokenizer has been
    /// registered (see [`crate::index::open_or_create`]).
    ///
    /// # Errors
    /// Returns a [`tantivy::TantivyError`] if the reader or writer cannot be
    /// constructed.
    pub fn new(index: Index, fields: Fields) -> tantivy::Result<Self> {
        let reader = index::reader(&index)?;
        let writer = index::writer(&index)?;
        Ok(Self {
            index,
            reader,
            fields,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Open (or create) the on-disk index at `path` and build a server over it.
    ///
    /// # Errors
    /// Returns an error if the index cannot be opened/created or its schema is
    /// incompatible (see [`crate::index::open_or_create`]).
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let index = index::open_or_create(path)?;
        let fields = Fields::from_schema(&index.schema())?;
        Ok(Self::new(index, fields)?)
    }

    /// Build a server backed by a fresh in-RAM index. For tests and ephemeral
    /// instances; on-disk deployments use [`SearchServer::open`].
    ///
    /// # Errors
    /// Returns a [`tantivy::TantivyError`] if the reader or writer cannot be
    /// constructed.
    pub fn in_ram() -> tantivy::Result<Self> {
        let index = Index::create_in_ram(build_schema());
        index::register_tokenizer(&index);
        let fields = Fields::from_schema(&index.schema())?;
        Self::new(index, fields)
    }
}

/// Map a Tantivy error into a gRPC internal-error status.
fn internal<E: std::fmt::Display>(error: E) -> Status {
    Status::internal(error.to_string())
}

#[tonic::async_trait]
impl SearchService for SearchServer {
    async fn index_document(
        &self,
        request: Request<IndexDocumentRequest>,
    ) -> Result<Response<IndexDocumentResponse>, Status> {
        let document = request
            .into_inner()
            .document
            .ok_or_else(|| Status::invalid_argument("index_document: missing `document`"))?;

        {
            let mut writer = self.writer.lock().await;
            indexer::upsert(&writer, &self.fields, &document).map_err(internal)?;
            writer.commit().map_err(internal)?;
        }
        // Make the commit visible immediately (OnCommitWithDelay would lag).
        self.reader.reload().map_err(internal)?;

        Ok(Response::new(IndexDocumentResponse { ok: true }))
    }

    async fn batch_index(
        &self,
        request: Request<Streaming<IndexDocumentRequest>>,
    ) -> Result<Response<BatchIndexResponse>, Status> {
        let mut stream = request.into_inner();
        let mut indexed_count: u64 = 0;
        let mut error_count: u64 = 0;

        {
            // Hold the writer for the whole stream: a backfill is one big batch
            // and there is only one writer anyway. Commit once at the end.
            let mut writer = self.writer.lock().await;
            while let Some(req) = stream.message().await? {
                match req.document {
                    Some(document) => match indexer::upsert(&writer, &self.fields, &document) {
                        Ok(()) => indexed_count += 1,
                        Err(error) => {
                            tracing::warn!(%error, "batch_index: skipping document");
                            error_count += 1;
                        }
                    },
                    None => {
                        tracing::warn!("batch_index: request without a document");
                        error_count += 1;
                    }
                }
            }
            writer.commit().map_err(internal)?;
        }
        self.reader.reload().map_err(internal)?;

        Ok(Response::new(BatchIndexResponse {
            indexed_count,
            error_count,
        }))
    }

    async fn delete_document(
        &self,
        request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<DeleteDocumentResponse>, Status> {
        let info_hash = request.into_inner().info_hash;
        if info_hash.is_empty() {
            return Err(Status::invalid_argument(
                "delete_document: empty `info_hash`",
            ));
        }

        {
            let mut writer = self.writer.lock().await;
            indexer::delete(&writer, &self.fields, &info_hash);
            writer.commit().map_err(internal)?;
        }
        self.reader.reload().map_err(internal)?;

        Ok(Response::new(DeleteDocumentResponse { ok: true }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let response = crate::query::run_search(
            &self.index,
            &self.reader,
            &self.fields,
            request.into_inner(),
        )
        .map_err(internal)?;
        Ok(Response::new(response))
    }

    async fn get_facets(
        &self,
        request: Request<GetFacetsRequest>,
    ) -> Result<Response<GetFacetsResponse>, Status> {
        let response = crate::facets::run_facets(
            &self.index,
            &self.reader,
            &self.fields,
            request.into_inner(),
        )
        .map_err(internal)?;
        Ok(Response::new(response))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let doc_count = self.reader.searcher().num_docs();
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
            doc_count,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::SearchServer;
    use crate::proto::health_check_response::ServingStatus;
    use crate::proto::search_service_server::SearchService;
    use crate::proto::{
        ContentType, DeleteDocumentRequest, HealthCheckRequest, IndexDocumentRequest,
        TorrentDocument,
    };
    use tonic::Request;

    fn doc(info_hash: Vec<u8>, name: &str) -> TorrentDocument {
        TorrentDocument {
            info_hash,
            torrent_name: name.to_owned(),
            content_title: name.to_owned(),
            original_title: String::new(),
            release_year: 2022,
            video_resolution: "1080p".to_owned(),
            video_source: "BluRay".to_owned(),
            video_codec: "x264".to_owned(),
            genres: vec!["action".to_owned()],
            file_paths: vec![format!("{name}.mkv")],
            content_type: ContentType::Movie as i32,
            seeders: 10,
            leechers: 1,
            files_count: 1,
            size: 1_000_000,
            published_at: 1_600_000_000,
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

    async fn count(server: &SearchServer) -> u64 {
        server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .expect("health_check ok")
            .into_inner()
            .doc_count
    }

    #[tokio::test]
    async fn write_path_index_upsert_delete_counts() {
        let server = SearchServer::in_ram().expect("in-ram server");

        // HealthCheck is healthy and starts empty.
        let initial = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .expect("health_check ok")
            .into_inner();
        assert_eq!(initial.status, ServingStatus::Serving as i32);
        assert_eq!(initial.doc_count, 0);

        // Index two distinct documents.
        let hash_a = vec![0x01; 20];
        let hash_b = vec![0x02; 20];
        for d in [doc(hash_a.clone(), "Alpha"), doc(hash_b.clone(), "Beta")] {
            server
                .index_document(Request::new(IndexDocumentRequest { document: Some(d) }))
                .await
                .expect("index_document ok");
        }
        assert_eq!(count(&server).await, 2);

        // Re-indexing the same info hash upserts (replaces), not duplicates.
        server
            .index_document(Request::new(IndexDocumentRequest {
                document: Some(doc(hash_a.clone(), "Alpha Reissue")),
            }))
            .await
            .expect("upsert ok");
        assert_eq!(count(&server).await, 2);

        // Delete one document.
        server
            .delete_document(Request::new(DeleteDocumentRequest { info_hash: hash_a }))
            .await
            .expect("delete_document ok");
        assert_eq!(count(&server).await, 1);
    }

    #[tokio::test]
    async fn index_document_without_document_is_invalid_argument() {
        let server = SearchServer::in_ram().expect("in-ram server");
        let status = server
            .index_document(Request::new(IndexDocumentRequest { document: None }))
            .await
            .expect_err("missing document must error");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn delete_document_empty_hash_is_invalid_argument() {
        let server = SearchServer::in_ram().expect("in-ram server");
        let status = server
            .delete_document(Request::new(DeleteDocumentRequest { info_hash: vec![] }))
            .await
            .expect_err("empty hash must error");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }
}
