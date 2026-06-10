//! [`PathSearchServer`]: the `tonic` [`PathSearchService`] implementation (L3).
//!
//! A distinct engine from [`crate::server::SearchServer`] over a distinct
//! path-bag index. The read RPC ([`PathTypeahead`]) is delegated to
//! [`crate::pathsearch::query`]; the write RPCs drive the SOLE
//! [`IndexWriter`] behind an [`Arc<Mutex<_>>`] — the SAME writer the in-pod
//! follow loop shares, so there is never a second writer (PS-T4 §4).

use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status, Streaming};

use tantivy::{Index, IndexReader, IndexWriter};

use super::index;
use super::indexer::{self, PathDoc};
use super::schema::{build_path_schema, PathFields};
use crate::pathsearch::query::path_typeahead;
use crate::proto::health_check_response::ServingStatus;
use crate::proto::path_search_service_server::PathSearchService;
use crate::proto::{
    DeleteDocumentRequest, DeleteDocumentResponse, HealthCheckRequest, HealthCheckResponse,
    IndexPathsResponse, PathTypeaheadRequest, PathTypeaheadResponse, TorrentPathDocument,
};

/// gRPC entry point for the path-FTS typeahead sidecar.
///
/// Cheap to [`Clone`] (tonic clones per connection): the reader is `Arc`-backed
/// and the writer is shared behind an `Arc<Mutex<_>>`, so every clone — and the
/// follow loop — drives the same single writer.
#[derive(Clone)]
pub struct PathSearchServer {
    reader: IndexReader,
    fields: PathFields,
    /// The SOLE path-index writer (single-thread, ≥2 GiB arena). Shared with the
    /// follow loop.
    writer: Arc<Mutex<IndexWriter>>,
}

impl PathSearchServer {
    /// Build a server over an already-opened path `index` whose ngram analyzer
    /// has been registered (see [`super::index::open_or_create_path`]).
    ///
    /// # Errors
    /// Returns a [`tantivy::TantivyError`] if the reader or writer cannot be
    /// constructed.
    pub fn new(index: Index, fields: PathFields) -> tantivy::Result<Self> {
        let reader = index::path_reader(&index)?;
        let writer = index::path_writer(&index)?;
        Ok(Self {
            reader,
            fields,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Open (or create) the on-disk path index at `path` and build a server over
    /// it.
    ///
    /// # Errors
    /// Returns an error if the index cannot be opened/created or its schema is
    /// incompatible.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let index = index::open_or_create_path(path)?;
        let fields = PathFields::from_schema(&index.schema())?;
        Ok(Self::new(index, fields)?)
    }

    /// Build a server over a fresh in-RAM index (tests / ephemeral).
    ///
    /// # Errors
    /// Returns a [`tantivy::TantivyError`] if the reader or writer cannot be
    /// constructed.
    pub fn in_ram() -> tantivy::Result<Self> {
        let index = Index::create_in_ram(build_path_schema());
        index::register_path_index(&index);
        let fields = PathFields::from_schema(&index.schema())?;
        Self::new(index, fields)
    }

    /// The shared writer handle — handed to the follow loop so it writes through
    /// the same single writer the RPCs use.
    #[must_use]
    pub fn writer_handle(&self) -> Arc<Mutex<IndexWriter>> {
        Arc::clone(&self.writer)
    }

    /// A clone of the index reader (for the follow loop's post-commit reload).
    #[must_use]
    pub fn reader(&self) -> IndexReader {
        self.reader.clone()
    }

    /// The resolved path-bag field handles.
    #[must_use]
    pub fn fields(&self) -> PathFields {
        self.fields
    }
}

fn internal<E: std::fmt::Display>(error: E) -> Status {
    Status::internal(error.to_string())
}

#[tonic::async_trait]
impl PathSearchService for PathSearchServer {
    async fn path_typeahead(
        &self,
        request: Request<PathTypeaheadRequest>,
    ) -> Result<Response<PathTypeaheadResponse>, Status> {
        let response = path_typeahead(&self.reader, &self.fields, &request.into_inner())?;
        Ok(Response::new(response))
    }

    async fn index_torrent_paths(
        &self,
        request: Request<Streaming<TorrentPathDocument>>,
    ) -> Result<Response<IndexPathsResponse>, Status> {
        let mut stream = request.into_inner();
        let mut indexed_count: u64 = 0;
        let mut error_count: u64 = 0;

        {
            let mut writer = self.writer.lock().await;
            while let Some(doc) = stream.message().await? {
                let pd = PathDoc {
                    info_hash: &doc.info_hash,
                    file_paths: &doc.file_paths,
                    seeders: u64::from(doc.seeders),
                    size: doc.size,
                    files_count: u64::from(doc.files_count),
                    name_fallback: "",
                };
                match indexer::upsert(&writer, &self.fields, &pd) {
                    Ok(()) => indexed_count += 1,
                    Err(error) => {
                        tracing::warn!(%error, "index_torrent_paths: skipping document");
                        error_count += 1;
                    }
                }
            }
            writer.commit().map_err(internal)?;
        }
        self.reader.reload().map_err(internal)?;

        Ok(Response::new(IndexPathsResponse {
            indexed_count,
            error_count,
        }))
    }

    async fn delete_torrent(
        &self,
        request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<DeleteDocumentResponse>, Status> {
        let info_hash = request.into_inner().info_hash;
        if info_hash.is_empty() {
            return Err(Status::invalid_argument("delete_torrent: empty `info_hash`"));
        }
        {
            let mut writer = self.writer.lock().await;
            indexer::delete(&writer, &self.fields, &info_hash);
            writer.commit().map_err(internal)?;
        }
        self.reader.reload().map_err(internal)?;
        Ok(Response::new(DeleteDocumentResponse { ok: true }))
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
    use super::PathSearchServer;
    use crate::proto::path_search_service_server::PathSearchService;
    use crate::proto::{
        DeleteDocumentRequest, HealthCheckRequest, Pagination, PathTypeaheadRequest,
    };
    use tonic::Request;

    #[tokio::test]
    async fn typeahead_and_delete_over_inram_server() {
        let server = PathSearchServer::in_ram().expect("in-ram path server");

        // Seed one torrent via the (in-process) writer + commit by reusing the
        // streaming push is overkill; drive the writer directly.
        {
            let w = server.writer_handle();
            let mut writer = w.lock().await;
            let fields = server.fields();
            crate::pathsearch::indexer::upsert(
                &writer,
                &fields,
                &crate::pathsearch::indexer::PathDoc {
                    info_hash: &[0x07; 20],
                    file_paths: &["Movies/Inception.2010.1080p.mkv".to_owned()],
                    seeders: 99,
                    size: 1_000,
                    files_count: 1,
                    name_fallback: "",
                },
            )
            .unwrap();
            writer.commit().unwrap();
        }
        server.reader().reload().unwrap();

        // HealthCheck reflects the one doc.
        let hc = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(hc.doc_count, 1);

        // Typeahead finds it.
        let resp = server
            .path_typeahead(Request::new(PathTypeaheadRequest {
                query: "inception".to_owned(),
                pagination: Some(Pagination { limit: 10, offset: 0 }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].info_hash, vec![0x07; 20]);

        // Too-short query → InvalidArgument.
        let err = server
            .path_typeahead(Request::new(PathTypeaheadRequest {
                query: "in".to_owned(),
                pagination: None,
            }))
            .await
            .expect_err("must reject short query");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // Delete removes it.
        server
            .delete_torrent(Request::new(DeleteDocumentRequest {
                info_hash: vec![0x07; 20],
            }))
            .await
            .unwrap();
        let hc = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(hc.doc_count, 0);
    }
}
