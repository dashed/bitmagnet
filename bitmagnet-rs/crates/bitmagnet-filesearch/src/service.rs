//! The gRPC [`FileSearchService`] implementation.
//!
//! Generic over the [`Engine`] so the default build tests it end-to-end against
//! the [`InMemoryEngine`]; production wires the `DuckEngine`. Each RPC:
//! 1. maps the proto request → a validated domain query (clamping limits),
//! 2. pins the current generation ([`GenerationManager::current`]),
//! 3. acquires a concurrency permit (the CB semaphore at the knee, ~4–8) and
//!    runs the (sync) engine call on a `spawn_blocking` thread with a deadline,
//! 4. maps engine rows → the proto response (overfetch → `has_next`).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

use bitmagnet_proto::v1 as proto;

use crate::engine::{Engine, EngineError, FileHitRow, GroupRow};
use crate::generation::{GenerationManager, LoadedGeneration};
use crate::query::{clamp_limit, clamp_preview, CountQuery, FileQuery, Filters, Sort};

/// Tunables for the service surface (CB-measured defaults).
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Concurrent in-flight engine queries (tokio semaphore permits).
    pub max_concurrency: usize,
    /// Per-query deadline (DuckDB interrupt watchdog).
    pub query_deadline: Duration,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 6,
            query_deadline: Duration::from_secs(10),
        }
    }
}

/// The service: a generation manager + an engine + a concurrency gate.
pub struct FileSearchServer<E: Engine> {
    gens: Arc<GenerationManager>,
    engine: Arc<E>,
    sem: Arc<Semaphore>,
    cfg: ServiceConfig,
}

impl<E: Engine + 'static> FileSearchServer<E> {
    pub fn new(gens: Arc<GenerationManager>, engine: Arc<E>, cfg: ServiceConfig) -> Self {
        let sem = Arc::new(Semaphore::new(cfg.max_concurrency));
        Self {
            gens,
            engine,
            sem,
            cfg,
        }
    }

    /// Acquire a permit and run `f` (a sync engine call) on a blocking thread.
    async fn run_blocking<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(Arc<E>, Arc<LoadedGeneration>, Duration) -> anyhow::Result<T> + Send + 'static,
    {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("service shutting down"))?;
        let engine = self.engine.clone();
        let gen = self.gens.current();
        let deadline = self.cfg.query_deadline;
        let out = tokio::task::spawn_blocking(move || {
            let _permit = permit; // held for the query's lifetime
            f(engine, gen, deadline)
        })
        .await
        .map_err(|e| Status::internal(format!("worker join error: {e}")))?;
        out.map_err(status_from_engine_error)
    }
}

fn status_from_engine_error(e: anyhow::Error) -> Status {
    match e.downcast_ref::<EngineError>() {
        Some(EngineError::QueryDeadlineExceeded) => Status::deadline_exceeded(e.to_string()),
        None => Status::internal(e.to_string()),
    }
}

/// Map proto filters → domain. `content_types` is accepted but not yet a filter
/// (denorm column is v2 — see the proto comment).
fn map_filters(f: Option<proto::FileFilters>) -> Filters {
    let f = f.unwrap_or_default();
    Filters {
        extensions: f.extensions,
        size_min: f.size_min,
        size_max: f.size_max,
        path_query: f.path_query.filter(|s| !s.is_empty()),
        include_padding: f.include_padding,
    }
}

fn map_sort(sort: &[proto::FileSortBy]) -> Sort {
    sort.first()
        .map(|s| Sort::from_proto(&s.field, s.descending))
        .unwrap_or_default()
}

fn to_proto_hit(r: FileHitRow) -> proto::FileHit {
    proto::FileHit {
        info_hash: r.info_hash,
        file_index: r.file_index,
        path: r.path,
        extension: r.extension.unwrap_or_default(),
        size: r.size,
    }
}

fn to_proto_group(g: GroupRow, preview: Vec<FileHitRow>) -> proto::TorrentFileGroup {
    proto::TorrentFileGroup {
        info_hash: g.info_hash,
        matching_file_count: g.matching_file_count,
        matching_total_size: g.matching_total_size,
        matching_max_size: g.matching_max_size,
        preview: preview.into_iter().map(to_proto_hit).collect(),
    }
}

#[tonic::async_trait]
impl<E: Engine + 'static> proto::file_search_service_server::FileSearchService
    for FileSearchServer<E>
{
    async fn search_files(
        &self,
        request: Request<proto::SearchFilesRequest>,
    ) -> Result<Response<proto::SearchFilesResponse>, Status> {
        let req = request.into_inner();
        let limit = clamp_limit(req.pagination.as_ref().map_or(0, |p| p.limit));
        let preview_limit = clamp_preview(req.preview_limit);
        let q = FileQuery {
            filters: map_filters(req.filters),
            sort: map_sort(&req.sort),
            limit,
            collapse_to_torrent: req.collapse_to_torrent,
            preview_limit,
        };

        if q.collapse_to_torrent {
            let qc = q.clone();
            let groups = self
                .run_blocking(move |engine, gen, deadline| {
                    let mut groups = engine.collapse(&gen, &qc, deadline)?;
                    let has_more = groups.len() > qc.limit as usize;
                    groups.truncate(qc.limit as usize);
                    // Fill previews in one engine call; the DuckDB engine uses
                    // one fact scan for the whole collapsed page.
                    let info_hashes: Vec<String> =
                        groups.iter().map(|g| g.info_hash.clone()).collect();
                    let mut previews = engine.previews(
                        &gen,
                        &info_hashes,
                        &qc.filters,
                        qc.preview_limit,
                        deadline,
                    )?;
                    let mut out = Vec::with_capacity(groups.len());
                    for g in groups {
                        let preview = previews.remove(&g.info_hash).unwrap_or_default();
                        out.push(to_proto_group(g, preview));
                    }
                    Ok((out, has_more))
                })
                .await?;
            let (groups, has_next) = groups;
            Ok(Response::new(proto::SearchFilesResponse {
                files: Vec::new(),
                groups,
                next_cursor: String::new(), // keyset resumption: see build notes (stub)
                has_next,
            }))
        } else {
            let qf = q.clone();
            let (files, has_next) = self
                .run_blocking(move |engine, gen, deadline| {
                    let mut rows = engine.search_files(&gen, &qf, deadline)?;
                    let has_more = rows.len() > qf.limit as usize;
                    rows.truncate(qf.limit as usize);
                    Ok((rows, has_more))
                })
                .await?;
            Ok(Response::new(proto::SearchFilesResponse {
                files: files.into_iter().map(to_proto_hit).collect(),
                groups: Vec::new(),
                next_cursor: String::new(),
                has_next,
            }))
        }
    }

    async fn count_files(
        &self,
        request: Request<proto::CountFilesRequest>,
    ) -> Result<Response<proto::CountFilesResponse>, Status> {
        let req = request.into_inner();
        let q = CountQuery {
            filters: map_filters(req.filters),
            collapse_to_torrent: req.collapse_to_torrent,
        };
        let (count, estimated) = self
            .run_blocking(move |engine, gen, deadline| engine.count(&gen, &q, deadline))
            .await?;
        Ok(Response::new(proto::CountFilesResponse {
            count,
            estimated,
        }))
    }

    async fn facets(
        &self,
        request: Request<proto::FacetsRequest>,
    ) -> Result<Response<proto::FacetsResponse>, Status> {
        let req = request.into_inner();
        let filters = map_filters(req.filters);
        let want_ext =
            req.facet_fields.is_empty() || req.facet_fields.iter().any(|f| f == "extension");
        let buckets = if want_ext {
            self.run_blocking(move |engine, gen, deadline| {
                engine.facet_ext(&gen, &filters, deadline)
            })
            .await?
        } else {
            Vec::new()
        };
        let facets = if want_ext {
            vec![proto::FileFacet {
                field: "extension".to_owned(),
                buckets: buckets
                    .into_iter()
                    .map(|b| proto::FileFacetBucket {
                        value: b.value.unwrap_or_default(),
                        count: b.count,
                        total_size: b.total_size,
                    })
                    .collect(),
            }]
        } else {
            Vec::new()
        };
        Ok(Response::new(proto::FacetsResponse { facets }))
    }

    async fn reload(
        &self,
        request: Request<proto::ReloadRequest>,
    ) -> Result<Response<proto::ReloadResponse>, Status> {
        let expect = request.into_inner().expect_version;
        let expect = (!expect.is_empty()).then_some(expect);
        let (gen, reloaded) = self
            .gens
            .reload(expect.as_deref())
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::ReloadResponse {
            reloaded,
            base_version: gen.base_version.clone(),
            delta_version: gen.delta_version.clone(),
        }))
    }

    async fn health_check(
        &self,
        _request: Request<proto::FileHealthCheckRequest>,
    ) -> Result<Response<proto::FileHealthCheckResponse>, Status> {
        let gen = self.gens.current();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let delta_age = (now - gen.delta_watermark).max(0);
        Ok(Response::new(proto::FileHealthCheckResponse {
            status: proto::file_health_check_response::ServingStatus::Serving as i32,
            base_version: gen.base_version.clone(),
            delta_version: gen.delta_version.clone(),
            delta_age_seconds: delta_age,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{FileHitRow, InMemoryEngine};
    use bitmagnet_parquet::generation::{artifact, Kind, Layout};

    fn seed_layout(tag: &str) -> Layout {
        let root = std::env::temp_dir().join(format!("bmfs-svc-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let layout = Layout::new(root);
        layout.ensure_dirs().unwrap();
        layout.write_watermark(0).unwrap();
        for (kind, files) in [
            (
                Kind::Base,
                vec![artifact::FACT, artifact::AGG_TORRENT_EXT, artifact::AGG_EXT],
            ),
            (
                Kind::Delta,
                vec![
                    artifact::FACT,
                    artifact::AGG_TORRENT_EXT,
                    artifact::AGG_EXT,
                    artifact::TOMBSTONES,
                ],
            ),
        ] {
            let dir = layout.new_version_dir(kind, "1").unwrap();
            for f in files {
                std::fs::write(dir.join(f), b"").unwrap();
            }
            layout.publish(kind, &dir).unwrap();
        }
        layout
    }

    fn server(tag: &str) -> FileSearchServer<InMemoryEngine> {
        let layout = seed_layout(tag);
        let gens = Arc::new(GenerationManager::open(layout).unwrap());
        let engine = Arc::new(InMemoryEngine::new(vec![
            FileHitRow {
                info_hash: "aa".into(),
                file_index: 0,
                path: "Movie/big.mkv".into(),
                extension: Some("mkv".into()),
                size: 2_000_000_000,
            },
            FileHitRow {
                info_hash: "aa".into(),
                file_index: 1,
                path: "Movie/s.srt".into(),
                extension: Some("srt".into()),
                size: 1,
            },
            FileHitRow {
                info_hash: "bb".into(),
                file_index: 0,
                path: "Show/ep.mkv".into(),
                extension: Some("mkv".into()),
                size: 1_500_000_000,
            },
        ]));
        FileSearchServer::new(gens, engine, ServiceConfig::default())
    }

    use proto::file_search_service_server::FileSearchService as _;

    #[test]
    fn engine_deadline_maps_to_deadline_exceeded_status() {
        let status = status_from_engine_error(EngineError::QueryDeadlineExceeded.into());
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    #[tokio::test]
    async fn search_files_collapsed_default() {
        let s = server("collapse");
        let req = proto::SearchFilesRequest {
            filters: Some(proto::FileFilters {
                extensions: vec!["mkv".into()],
                size_min: Some(1_000_000_000),
                ..Default::default()
            }),
            pagination: Some(proto::FilePagination {
                limit: 10,
                cursor: String::new(),
            }),
            sort: vec![],
            collapse_to_torrent: true,
            preview_limit: 5,
        };
        let resp = s
            .search_files(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.groups.len(), 2);
        assert_eq!(resp.groups[0].info_hash, "aa");
        assert_eq!(resp.groups[0].matching_file_count, 1);
        assert!(!resp.groups[0].preview.is_empty());
        assert!(!resp.has_next);
    }

    #[tokio::test]
    async fn search_files_rows_overfetch_has_next() {
        let s = server("rows");
        let req = proto::SearchFilesRequest {
            filters: Some(proto::FileFilters {
                extensions: vec!["mkv".into()],
                ..Default::default()
            }),
            pagination: Some(proto::FilePagination {
                limit: 1,
                cursor: String::new(),
            }),
            sort: vec![proto::FileSortBy {
                field: "size".into(),
                descending: true,
            }],
            collapse_to_torrent: false,
            preview_limit: 0,
        };
        let resp = s
            .search_files(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.files.len(), 1);
        assert_eq!(resp.files[0].info_hash, "aa"); // 2GB mkv first
        assert!(resp.has_next); // bb mkv overfetched
    }

    #[tokio::test]
    async fn count_and_facets() {
        let s = server("count");
        let c = s
            .count_files(Request::new(proto::CountFilesRequest {
                filters: Some(proto::FileFilters {
                    extensions: vec!["mkv".into()],
                    ..Default::default()
                }),
                collapse_to_torrent: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(c.count, 2);

        let f = s
            .facets(Request::new(proto::FacetsRequest {
                filters: None,
                facet_fields: vec!["extension".into()],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(f.facets.len(), 1);
        assert_eq!(f.facets[0].field, "extension");
    }

    #[tokio::test]
    async fn health_reports_versions() {
        let s = server("health");
        let h = s
            .health_check(Request::new(proto::FileHealthCheckRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(h.base_version, "v1");
        assert_eq!(h.delta_version, "v1");
    }
}
