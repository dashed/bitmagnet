//! gRPC server for the L3 [`PathSearchService`](crate::proto::PathSearchService).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use tantivy::{Index, IndexReader, IndexWriter};

use crate::pathsearch::document::PathDocument;
use crate::pathsearch::schema::{build_schema, register_tokenizer, Fields};
use crate::pathsearch::{index, indexer, query};
use crate::proto::path_search_health::ServingStatus;
use crate::proto::path_search_service_server::PathSearchService;
use crate::proto::{
    HealthCheckRequest, PathCandidatesRequest, PathCandidatesResponse, PathSearchHealth,
};

/// Pathsearch gRPC entry point.
#[derive(Clone)]
pub struct PathSearchServer {
    index: Index,
    reader: IndexReader,
    fields: Fields,
    writer: Arc<Mutex<IndexWriter>>,
    index_path: Option<PathBuf>,
    watermark_epoch: Arc<AtomicI64>,
}

impl PathSearchServer {
    /// Build a server over an already-opened pathsearch index.
    ///
    /// # Errors
    /// Returns Tantivy reader/writer setup errors.
    pub fn new(
        index: Index,
        fields: Fields,
        heap_bytes: usize,
        threads: usize,
    ) -> tantivy::Result<Self> {
        let reader = index::reader(&index)?;
        let writer = index::writer(&index, heap_bytes, threads)?;
        Ok(Self {
            index,
            reader,
            fields,
            writer: Arc::new(Mutex::new(writer)),
            index_path: None,
            watermark_epoch: Arc::new(AtomicI64::new(0)),
        })
    }

    /// Open or create an on-disk pathsearch index.
    ///
    /// # Errors
    /// Returns index open/create errors or incompatible schema errors.
    pub fn open(path: &Path, heap_bytes: usize, threads: usize) -> anyhow::Result<Self> {
        let index = index::open_or_create(path)?;
        let fields = Fields::from_schema(&index.schema())?;
        let mut server = Self::new(index, fields, heap_bytes, threads)?;
        server.index_path = Some(path.to_owned());
        Ok(server)
    }

    /// Build an in-RAM server for tests.
    ///
    /// # Errors
    /// Returns Tantivy setup errors.
    pub fn in_ram() -> tantivy::Result<Self> {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index)?;
        let fields = Fields::from_schema(&index.schema())?;
        Self::new(index, fields, 256 * 1024 * 1024, 1)
    }

    /// Upsert one path-bag document and make it visible immediately.
    ///
    /// # Errors
    /// Returns Tantivy write/reload failures.
    pub async fn upsert_document(&self, doc: &PathDocument) -> tantivy::Result<()> {
        {
            let mut writer = self.writer.lock().await;
            indexer::upsert(&writer, &self.fields, doc)?;
            writer.commit()?;
        }
        self.reader.reload()?;
        Ok(())
    }

    /// Delete one path-bag document and make it visible immediately.
    ///
    /// # Errors
    /// Returns Tantivy commit/reload failures.
    pub async fn delete_info_hash(&self, info_hash: &[u8]) -> tantivy::Result<()> {
        {
            let mut writer = self.writer.lock().await;
            indexer::delete(&writer, &self.fields, info_hash);
            writer.commit()?;
        }
        self.reader.reload()?;
        Ok(())
    }

    /// Update the externally reported follow watermark.
    pub fn set_watermark_epoch(&self, watermark_epoch: i64) {
        self.watermark_epoch
            .store(watermark_epoch, Ordering::Relaxed);
    }
}

fn internal<E: std::fmt::Display>(error: E) -> Status {
    Status::internal(error.to_string())
}

fn dir_size(path: Option<&Path>) -> u64 {
    let Some(path) = path else {
        return 0;
    };
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(|meta| meta.is_file())
        .map(|meta| meta.len())
        .sum()
}

#[tonic::async_trait]
impl PathSearchService for PathSearchServer {
    async fn path_candidates(
        &self,
        request: Request<PathCandidatesRequest>,
    ) -> Result<Response<PathCandidatesResponse>, Status> {
        let response = query::run_path_candidates(
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
    ) -> Result<Response<PathSearchHealth>, Status> {
        Ok(Response::new(PathSearchHealth {
            status: ServingStatus::Serving as i32,
            doc_count: self.reader.searcher().num_docs(),
            index_bytes: dir_size(self.index_path.as_deref()),
            watermark_epoch: self.watermark_epoch.load(Ordering::Relaxed),
            writable: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::PathSearchServer;
    use crate::pathsearch::document::PathDocument;
    use crate::proto::path_search_health::ServingStatus;
    use crate::proto::path_search_service_server::PathSearchService;
    use crate::proto::{HealthCheckRequest, PathCandidatesRequest};
    use tonic::Request;

    fn doc(byte: u8, path: &str) -> PathDocument {
        PathDocument {
            info_hash: vec![byte; 20],
            paths: vec![path.to_owned()],
            size: 1,
            files_count: 1,
            seeders: 0,
            published_at: 1,
        }
    }

    #[tokio::test]
    async fn service_indexes_searches_and_deletes() {
        let server = PathSearchServer::in_ram().expect("server");
        server
            .upsert_document(&doc(1, "Show.S01E01.mkv"))
            .await
            .expect("upsert");

        let out = server
            .path_candidates(Request::new(PathCandidatesRequest {
                query: "s01e01".to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            }))
            .await
            .expect("path candidates")
            .into_inner();
        assert_eq!(out.candidates.len(), 1);
        assert_eq!(out.candidates[0].info_hash, vec![1; 20]);

        let health = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .expect("health")
            .into_inner();
        assert_eq!(health.status, ServingStatus::Serving as i32);
        assert_eq!(health.doc_count, 1);

        server.delete_info_hash(&[1; 20]).await.expect("delete");
        let out = server
            .path_candidates(Request::new(PathCandidatesRequest {
                query: "s01e01".to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            }))
            .await
            .expect("path candidates")
            .into_inner();
        assert_eq!(out.candidate_total, 0);
    }
}
