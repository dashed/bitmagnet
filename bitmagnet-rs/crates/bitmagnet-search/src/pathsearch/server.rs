//! gRPC server for the L3 [`PathSearchService`](crate::proto::PathSearchService).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use tantivy::{Index, IndexReader, IndexWriter};

use crate::pathsearch::document::PathDocument;
use crate::pathsearch::prefix::PrefixIndex;
use crate::pathsearch::schema::{build_schema, register_tokenizer, Fields};
use crate::pathsearch::{index, indexer, query};
use crate::proto::path_search_health::ServingStatus;
use crate::proto::path_search_service_server::PathSearchService;
use crate::proto::{
    HealthCheckRequest, PathCandidatesRequest, PathCandidatesResponse, PathSearchHealth,
    SuggestRequest, SuggestResponse, Suggestion as ProtoSuggestion,
};

const DEFAULT_SUGGEST_LIMIT: usize = 10;
const MAX_SUGGEST_LIMIT: usize = 50;

/// Pathsearch gRPC entry point.
#[derive(Clone)]
pub struct PathSearchServer {
    index: Index,
    reader: IndexReader,
    fields: Fields,
    writer: Arc<Mutex<IndexWriter>>,
    index_path: Option<PathBuf>,
    watermark_epoch: Arc<AtomicI64>,
    prefix: Option<Arc<PrefixIndex>>,
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
        prefix: Option<Arc<PrefixIndex>>,
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
            prefix,
        })
    }

    /// Open or create an on-disk pathsearch index.
    ///
    /// # Errors
    /// Returns index open/create errors or incompatible schema errors.
    pub fn open(
        path: &Path,
        heap_bytes: usize,
        threads: usize,
        prefix: Option<Arc<PrefixIndex>>,
    ) -> anyhow::Result<Self> {
        let index = index::open_or_create(path)?;
        let fields = Fields::from_schema(&index.schema())?;
        let mut server = Self::new(index, fields, heap_bytes, threads, prefix)?;
        server.index_path = Some(path.to_owned());
        Ok(server)
    }

    /// Build an in-RAM server for tests.
    ///
    /// # Errors
    /// Returns Tantivy setup errors.
    pub fn in_ram() -> tantivy::Result<Self> {
        Self::in_ram_with_prefix(None)
    }

    /// Build an in-RAM Tantivy server with an optional mmap prefix index.
    ///
    /// # Errors
    /// Returns Tantivy setup errors.
    pub fn in_ram_with_prefix(prefix: Option<Arc<PrefixIndex>>) -> tantivy::Result<Self> {
        let index = Index::create_in_ram(build_schema());
        register_tokenizer(&index)?;
        let fields = Fields::from_schema(&index.schema())?;
        Self::new(index, fields, 256 * 1024 * 1024, 1, prefix)
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

    async fn suggest(
        &self,
        request: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        let prefix = self
            .prefix
            .as_ref()
            .ok_or_else(|| Status::unavailable("suggest index not built"))?;
        let request = request.into_inner();
        let limit = if request.limit == 0 {
            DEFAULT_SUGGEST_LIMIT
        } else {
            (request.limit as usize).min(MAX_SUGGEST_LIMIT)
        };
        let suggestions = prefix
            .suggest(&request.prefix, limit)
            .into_iter()
            .map(|suggestion| ProtoSuggestion {
                value: suggestion.value,
                score: suggestion.score,
            })
            .collect();
        Ok(Response::new(SuggestResponse { suggestions }))
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
            suggest_ready: self.prefix.as_ref().is_some_and(|index| !index.is_empty()),
            suggest_entries: self.prefix.as_ref().map_or(0, |index| index.len() as u64),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::PathSearchServer;
    use crate::pathsearch::document::PathDocument;
    use crate::pathsearch::prefix::{PrefixIndex, PrefixIndexBuilder, PrefixIndexConfig};
    use crate::proto::path_search_health::ServingStatus;
    use crate::proto::path_search_service_server::PathSearchService;
    use crate::proto::{HealthCheckRequest, PathCandidatesRequest, SuggestRequest};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tonic::{Code, Request};

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
        assert!(!health.suggest_ready);
        assert_eq!(health.suggest_entries, 0);

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

    #[tokio::test]
    async fn suggest_is_unavailable_without_prefix_index() {
        let server = PathSearchServer::in_ram().expect("server");
        let error = server
            .suggest(Request::new(SuggestRequest {
                prefix: "show".to_owned(),
                limit: 10,
            }))
            .await
            .expect_err("missing prefix index must be unavailable");
        assert_eq!(error.code(), Code::Unavailable);
        assert_eq!(error.message(), "suggest index not built");
    }

    #[tokio::test]
    async fn suggest_ranks_prefix_index_and_reports_health() {
        let cfg = PrefixIndexConfig {
            max_tracked: 10,
            min_freq: 1,
            max_entries: 10,
            max_scan: 10,
            min_seg_chars: 2,
            max_seg_chars: 32,
        };
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("prefix.fst");
        let mut builder = PrefixIndexBuilder::new(cfg);
        builder.add_paths(&["Alpha".to_owned()]);
        builder.add_paths(&["Alpha".to_owned(), "Alpine".to_owned()]);
        builder.add_paths(&["Albatross".to_owned()]);
        builder.finalize(&path).expect("finalize prefix index");
        let prefix = PrefixIndex::open(&path, cfg)
            .expect("open prefix index")
            .expect("prefix index exists");
        let server = PathSearchServer::in_ram_with_prefix(Some(Arc::new(prefix))).expect("server");

        let response = server
            .suggest(Request::new(SuggestRequest {
                prefix: " AL ".to_owned(),
                limit: 10,
            }))
            .await
            .expect("suggest")
            .into_inner();
        let ranked: Vec<(&str, u64)> = response
            .suggestions
            .iter()
            .map(|suggestion| (suggestion.value.as_str(), suggestion.score))
            .collect();
        assert_eq!(ranked, vec![("alpha", 2), ("albatross", 1), ("alpine", 1)]);

        let health = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .expect("health")
            .into_inner();
        assert!(health.suggest_ready);
        assert_eq!(health.suggest_entries, 3);
    }
}
