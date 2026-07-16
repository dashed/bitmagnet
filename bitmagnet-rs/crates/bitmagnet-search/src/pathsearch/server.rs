//! gRPC server for the L3 [`PathSearchService`](crate::proto::PathSearchService).

use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use tantivy::{Index, IndexReader, IndexWriter, TantivyError};

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

/// One mutation in a commit-visible pathsearch follow batch.
pub enum PathSearchMutation<'a> {
    /// Replace the current path-bag document for this torrent.
    Replace(PathDocument),
    /// Delete the path-bag document for this torrent.
    Delete(&'a [u8]),
}

enum WriterState {
    Writable(Box<IndexWriter>),
    // Tantivy reconstructs its writer during rollback. If that reconstruction
    // fails, the old writer may be killed or lockless and must never be reused.
    Unwritable(TantivyError),
}

fn rollback_or_disable_writer(
    state: &mut WriterState,
    writer_writable: &AtomicBool,
    apply_error: TantivyError,
    rollback: impl FnOnce(&mut IndexWriter) -> tantivy::Result<()>,
) -> TantivyError {
    let rollback_result = match state {
        WriterState::Writable(writer) => rollback(writer),
        WriterState::Unwritable(error) => return error.clone(),
    };

    match rollback_result {
        Ok(()) => apply_error,
        Err(rollback_error) => {
            let fatal_error = TantivyError::InternalError(format!(
                "pathsearch writer transaction failed ({apply_error}); rollback also failed \
                 ({rollback_error}); writer is disabled until restart"
            ));
            *state = WriterState::Unwritable(fatal_error.clone());
            writer_writable.store(false, Ordering::Release);
            fatal_error
        }
    }
}

/// Pathsearch gRPC entry point.
#[derive(Clone)]
pub struct PathSearchServer {
    index: Index,
    reader: IndexReader,
    fields: Fields,
    writer: Arc<Mutex<WriterState>>,
    writer_writable: Arc<AtomicBool>,
    #[cfg(test)]
    explicit_commit_count: Arc<AtomicUsize>,
    #[cfg(test)]
    explicit_reload_count: Arc<AtomicUsize>,
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
            writer: Arc::new(Mutex::new(WriterState::Writable(Box::new(writer)))),
            writer_writable: Arc::new(AtomicBool::new(true)),
            #[cfg(test)]
            explicit_commit_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            explicit_reload_count: Arc::new(AtomicUsize::new(0)),
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
        let committed = self
            .apply_writer_transaction(|writer| {
                indexer::upsert(writer, &self.fields, doc)?;
                Ok(true)
            })
            .await?;
        if committed {
            self.reload_reader()?;
        }
        Ok(())
    }

    async fn apply_writer_transaction(
        &self,
        apply: impl FnOnce(&mut IndexWriter) -> tantivy::Result<bool>,
    ) -> tantivy::Result<bool> {
        let mut state = self.writer.lock().await;
        let apply_result = match &mut *state {
            WriterState::Writable(writer) => (|| {
                let changed = apply(writer)?;
                if changed {
                    writer.commit()?;
                    #[cfg(test)]
                    self.explicit_commit_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(changed)
            })(),
            WriterState::Unwritable(error) => return Err(error.clone()),
        };

        match apply_result {
            Ok(committed) => Ok(committed),
            Err(apply_error) => Err(rollback_or_disable_writer(
                &mut state,
                &self.writer_writable,
                apply_error,
                |writer| writer.rollback().map(|_| ()),
            )),
        }
    }

    fn reload_reader(&self) -> tantivy::Result<()> {
        self.reader.reload()?;
        #[cfg(test)]
        self.explicit_reload_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Delete one path-bag document and make it visible immediately.
    ///
    /// # Errors
    /// Returns Tantivy commit/reload failures.
    pub async fn delete_info_hash(&self, info_hash: &[u8]) -> tantivy::Result<()> {
        let committed = self
            .apply_writer_transaction(|writer| {
                indexer::delete(writer, &self.fields, info_hash);
                Ok(true)
            })
            .await?;
        if committed {
            self.reload_reader()?;
        }
        Ok(())
    }

    /// Apply one incremental follow batch as a single visible index update.
    ///
    /// Replacements are applied before tombstones. The follow loop filters
    /// stale same-window tombstones before calling this method, so a torrent
    /// that was deleted and then re-added remains present. A non-empty batch
    /// performs exactly one commit and one reader reload, regardless of the
    /// number of documents it contains.
    ///
    /// # Errors
    /// Returns Tantivy write/commit/reload failures. If both a transaction and
    /// its rollback fail, the returned error preserves both causes and the
    /// server reports `writable = false` until restart.
    pub async fn apply_follow_batch<'a>(
        &self,
        mutations: impl IntoIterator<Item = PathSearchMutation<'a>>,
    ) -> tantivy::Result<()> {
        let committed = self
            .apply_writer_transaction(|writer| {
                let mut changed = false;
                let mut tombstones = Vec::new();
                for mutation in mutations {
                    match mutation {
                        PathSearchMutation::Replace(document) => {
                            indexer::upsert(writer, &self.fields, &document)?;
                            changed = true;
                        }
                        PathSearchMutation::Delete(info_hash) => tombstones.push(info_hash),
                    }
                }
                for info_hash in tombstones {
                    indexer::delete(writer, &self.fields, info_hash);
                    changed = true;
                }
                Ok(changed)
            })
            .await?;
        if committed {
            self.reload_reader()?;
        }
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
            writable: self.writer_writable.load(Ordering::Acquire),
            suggest_ready: self.prefix.as_ref().is_some_and(|index| !index.is_empty()),
            suggest_entries: self.prefix.as_ref().map_or(0, |index| index.len() as u64),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{rollback_or_disable_writer, PathSearchMutation, PathSearchServer};
    use crate::pathsearch::document::PathDocument;
    use crate::pathsearch::indexer;
    use crate::pathsearch::prefix::{PrefixIndex, PrefixIndexBuilder, PrefixIndexConfig};
    use crate::proto::path_search_health::ServingStatus;
    use crate::proto::path_search_service_server::PathSearchService;
    use crate::proto::{HealthCheckRequest, PathCandidatesRequest, SuggestRequest};
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::sync::Arc;
    use tantivy::TantivyError;
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
    async fn follow_batch_applies_replacements_before_tombstones() {
        let server = PathSearchServer::in_ram().expect("server");
        server
            .upsert_document(&doc(1, "Old.Release.mkv"))
            .await
            .expect("seed replacement");
        server
            .upsert_document(&doc(2, "Deleted.Release.mkv"))
            .await
            .expect("seed tombstone");

        let deleted = [2; 20];
        let transient = [3; 20];
        server
            .apply_follow_batch([
                PathSearchMutation::Delete(&deleted),
                PathSearchMutation::Replace(doc(1, "New.Release.mkv")),
                PathSearchMutation::Replace(doc(3, "Transient.Release.mkv")),
                PathSearchMutation::Delete(&transient),
            ])
            .await
            .expect("apply follow batch");

        let search = |query: &str| {
            server.path_candidates(Request::new(PathCandidatesRequest {
                query: query.to_owned(),
                limit: 10,
                oversample: 0,
                sort: Vec::new(),
            }))
        };
        assert_eq!(
            search("old.release")
                .await
                .expect("old path query")
                .into_inner()
                .candidate_total,
            0
        );
        assert_eq!(
            search("new.release")
                .await
                .expect("new path query")
                .into_inner()
                .candidate_total,
            1
        );
        assert_eq!(
            search("deleted.release")
                .await
                .expect("deleted path query")
                .into_inner()
                .candidate_total,
            0
        );
        assert_eq!(
            search("transient.release")
                .await
                .expect("same-batch tombstone query")
                .into_inner()
                .candidate_total,
            0,
            "the tombstone phase must run after replacements"
        );

        server
            .apply_follow_batch(std::iter::empty())
            .await
            .expect("an empty batch is a no-op");
        assert_eq!(
            search("new.release")
                .await
                .expect("post-no-op query")
                .into_inner()
                .candidate_total,
            1
        );
    }

    #[tokio::test]
    async fn follow_batch_replay_is_idempotent_and_commits_and_reloads_once() {
        let server = PathSearchServer::in_ram().expect("server");
        let tombstoned = [2; 20];
        let mutations = || {
            [
                PathSearchMutation::Replace(doc(1, "Kept.Release.mkv")),
                PathSearchMutation::Replace(doc(2, "Transient.Release.mkv")),
                PathSearchMutation::Delete(&tombstoned),
            ]
        };

        let before_commits = server.explicit_commit_count.load(AtomicOrdering::Relaxed);
        let before_reloads = server.explicit_reload_count.load(AtomicOrdering::Relaxed);
        server
            .apply_follow_batch(mutations())
            .await
            .expect("first batch");
        assert_eq!(
            server.explicit_commit_count.load(AtomicOrdering::Relaxed),
            before_commits + 1,
            "all replacements and tombstones must share one commit"
        );
        assert_eq!(
            server.explicit_reload_count.load(AtomicOrdering::Relaxed),
            before_reloads + 1
        );

        server
            .apply_follow_batch(mutations())
            .await
            .expect("idempotent replay");
        assert_eq!(
            server.explicit_commit_count.load(AtomicOrdering::Relaxed),
            before_commits + 2,
            "replaying the page adds exactly one more commit"
        );
        assert_eq!(
            server.explicit_reload_count.load(AtomicOrdering::Relaxed),
            before_reloads + 2
        );
        assert_eq!(server.reader.searcher().num_docs(), 1);

        server
            .apply_follow_batch(std::iter::empty())
            .await
            .expect("empty batch");
        assert_eq!(
            server.explicit_commit_count.load(AtomicOrdering::Relaxed),
            before_commits + 2,
            "an empty batch must not commit"
        );
        assert_eq!(
            server.explicit_reload_count.load(AtomicOrdering::Relaxed),
            before_reloads + 2,
            "an empty batch must neither commit nor explicitly reload"
        );
    }

    #[tokio::test]
    async fn successful_rollback_discards_partial_work_and_allows_replay() {
        let server = PathSearchServer::in_ram().expect("server");
        let apply_error = TantivyError::SystemError("synthetic apply failure".to_owned());

        let returned = server
            .apply_writer_transaction(|writer| {
                indexer::upsert(writer, &server.fields, &doc(1, "Uncommitted.Release.mkv"))?;
                Err(apply_error.clone())
            })
            .await
            .expect_err("synthetic apply failure must roll back");
        assert_eq!(returned.to_string(), apply_error.to_string());

        let unrelated = [9; 20];
        server
            .apply_follow_batch([PathSearchMutation::Delete(&unrelated)])
            .await
            .expect("writer remains usable after rollback");
        assert_eq!(
            server.reader.searcher().num_docs(),
            0,
            "the later commit must not publish rolled-back partial work"
        );

        server
            .apply_follow_batch([PathSearchMutation::Replace(doc(
                1,
                "Uncommitted.Release.mkv",
            ))])
            .await
            .expect("replay succeeds");
        assert_eq!(server.reader.searcher().num_docs(), 1);
    }

    #[tokio::test]
    async fn rollback_failure_preserves_both_errors_and_permanently_disables_writer() {
        let server = PathSearchServer::in_ram().expect("server");
        let apply_error = TantivyError::SystemError("synthetic apply failure".to_owned());
        let rollback_error = TantivyError::SystemError("synthetic rollback failure".to_owned());

        // Tantivy's failure points require its optional `failpoints` Cargo
        // feature. Inject the rollback result at our recovery boundary instead,
        // so this test remains deterministic without changing workspace features.
        let returned = {
            let mut state = server.writer.lock().await;
            rollback_or_disable_writer(
                &mut state,
                &server.writer_writable,
                apply_error,
                |_writer| Err(rollback_error),
            )
        };
        let message = returned.to_string();
        assert!(message.contains("synthetic apply failure"));
        assert!(message.contains("synthetic rollback failure"));
        assert!(message.contains("writer is disabled until restart"));

        let health = server
            .health_check(Request::new(HealthCheckRequest {}))
            .await
            .expect("health")
            .into_inner();
        assert!(!health.writable);

        let retry = server
            .apply_follow_batch([PathSearchMutation::Replace(doc(1, "Must.Not.Commit.mkv"))])
            .await
            .expect_err("a failed rollback permanently disables this writer");
        assert_eq!(retry.to_string(), message);
        assert_eq!(server.reader.searcher().num_docs(), 0);
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
