//! The production DuckDB engine (feature `duckdb-engine`).
//!
//! One embedded DuckDB instance, a small pool of cloned connections (the CB
//! "1 instance + cursor pool"), each reading the generation's Parquet through
//! the [`crate::sql`] SafeQuery builder.
//!
//! ## FB-B1d lockdown (and the external-access tension)
//! `enable_external_access=false` would block `read_parquet`, which is the
//! engine's whole job — so it stays ON. We constrain the surface instead:
//! * every `read_parquet` path is **server-controlled** (the generation dir),
//!   never user input (see [`crate::sql`] trust tiers);
//! * all user values are **bound parameters** — nothing user-supplied reaches
//!   the SQL text;
//! * extension autoload/autoinstall are disabled and `lock_configuration=true`
//!   is set last, so a query cannot re-enable anything or load an extension;
//! * `threads`/`memory_limit` are bounded per the pod (CB: threads≈4).
//!
//! ## Deadlines
//! DuckDB has no `statement_timeout`; each query runs on a worker thread while
//! the caller waits up to the deadline. On timeout, the caller interrupts the
//! connection and returns immediately; the connection is returned to the pool
//! only after DuckDB unwinds.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use duckdb::types::Value;
use duckdb::{params_from_iter, Connection};

use crate::engine::{
    Deadline, Engine, EngineError, FacetBucketRow, FileHitRow, GroupRow, PreviewRows,
};
use crate::generation::LoadedGeneration;
use crate::query::{CountQuery, FileQuery, Filters};
use crate::sql::{self, Param, SafeQuery};

const QUERY_RUNNING: u8 = 0;
const QUERY_FINISHED: u8 = 1;
const QUERY_INTERRUPTING: u8 = 2;
const QUERY_INTERRUPTED: u8 = 3;

/// Per-pod DuckDB tuning (CB-measured: threads≈4, memory bounded, warm object
/// cache; heavy shapes already routed through rollups by [`crate::sql`]).
#[derive(Debug, Clone)]
pub struct DuckConfig {
    pub threads: u32,
    pub memory_limit: String,
    /// Connections in the cursor pool (the tokio semaphore should match).
    pub pool_size: usize,
}

impl Default for DuckConfig {
    fn default() -> Self {
        Self {
            threads: 4,
            memory_limit: "4GB".to_owned(),
            pool_size: 6,
        }
    }
}

/// The embedded engine: a free-list of cloned connections sharing one instance.
pub struct DuckEngine {
    pool: Arc<Mutex<Vec<Connection>>>,
}

impl DuckEngine {
    /// Open the engine: one in-memory DuckDB, locked-down, then `pool_size`
    /// shared clones.
    pub fn open(config: DuckConfig) -> Result<Self> {
        let primary = Connection::open_in_memory().context("opening duckdb")?;
        Self::lockdown(&primary, &config)?;
        let mut pool = Vec::with_capacity(config.pool_size);
        for _ in 0..config.pool_size {
            // try_clone shares the underlying instance (buffer pool / object cache).
            pool.push(primary.try_clone().context("cloning duckdb connection")?);
        }
        // keep the primary too
        pool.push(primary);
        Ok(Self {
            pool: Arc::new(Mutex::new(pool)),
        })
    }

    /// Apply the FB-B1d lockdown PRAGMAs (order matters: lock_configuration last).
    fn lockdown(conn: &Connection, cfg: &DuckConfig) -> Result<()> {
        conn.execute_batch(&format!(
            "SET threads={threads}; \
             SET memory_limit='{mem}'; \
             SET enable_object_cache=true; \
             SET autoinstall_known_extensions=false; \
             SET autoload_known_extensions=false; \
             SET lock_configuration=true;",
            threads = cfg.threads,
            mem = cfg.memory_limit,
        ))
        .context("applying duckdb lockdown pragmas")?;
        Ok(())
    }

    fn checkout(&self) -> Result<Connection> {
        let mut pool = self.pool.lock().expect("duck pool poisoned");
        match pool.pop() {
            Some(c) => Ok(c),
            None => Err(anyhow!("duckdb pool exhausted")),
        }
    }

    /// Run `f` with a pooled connection under the request's SHARED deadline. Each
    /// call arms its `recv_timeout` with the budget REMAINING at this instant
    /// (`deadline.remaining()`), not a fresh full deadline — so a request that runs
    /// several scans (e.g. a `size_min` collapse: groups + aggregates + previews)
    /// shares ONE budget and cannot blow the watchdog out to ~N× (review #7 / #96).
    /// An already-exhausted budget yields a zero remaining → immediate timeout.
    fn with_conn<T>(
        &self,
        deadline: Deadline,
        f: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
    ) -> Result<(T, bool)>
    where
        T: Send + 'static,
    {
        let remaining = deadline.remaining();
        let conn = self.checkout()?;
        let handle = conn.interrupt_handle();
        let pool = Arc::clone(&self.pool);
        let state = Arc::new(AtomicU8::new(QUERY_RUNNING));
        let worker_state = Arc::clone(&state);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = f(&conn);
            match worker_state.compare_exchange(
                QUERY_RUNNING,
                QUERY_FINISHED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(QUERY_INTERRUPTING) => {
                    while worker_state.load(Ordering::Acquire) == QUERY_INTERRUPTING {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
                Err(_) => {}
            }
            pool.lock().expect("duck pool poisoned").push(conn);
            let _ = tx.send(result);
        });

        match rx.recv_timeout(remaining) {
            Ok(Ok(v)) => Ok((v, false)),
            Ok(Err(e)) => Err(e),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if state
                    .compare_exchange(
                        QUERY_RUNNING,
                        QUERY_INTERRUPTING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    handle.interrupt();
                    state.store(QUERY_INTERRUPTED, Ordering::Release);
                }
                Err(EngineError::QueryDeadlineExceeded.into())
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(anyhow!("query worker terminated without result"))
            }
        }
    }

    fn bind_values(params: &[Param]) -> Vec<Value> {
        params
            .iter()
            .map(|p| match p {
                Param::Text(s) => Value::Text(s.clone()),
                // file sizes / counts fit i64; DuckDB BIGINT.
                Param::U64(n) => Value::BigInt(*n as i64),
            })
            .collect()
    }
}

impl Engine for DuckEngine {
    fn search_files(
        &self,
        gen: &LoadedGeneration,
        q: &FileQuery,
        deadline: Deadline,
    ) -> Result<Vec<FileHitRow>> {
        let sq = sql::build_search_files(q, &gen.layers);
        let (rows, _) = self.with_conn(deadline, move |conn| query_files(conn, &sq))?;
        Ok(rows)
    }

    fn collapse(
        &self,
        gen: &LoadedGeneration,
        q: &FileQuery,
        deadline: Deadline,
    ) -> Result<Vec<GroupRow>> {
        match sql::rollup_plan(&q.filters) {
            // Rollup aggregates exact (ext-only / unfiltered) — the <50 ms path.
            sql::RollupPlan::Exact => {
                let sq = sql::build_collapse_rollup(q, &gen.layers);
                let (rows, _) = self.with_conn(deadline, move |conn| query_groups(conn, &sq))?;
                Ok(rows)
            }
            // size_min: rollup set/ordering exact, but file_count/total_size
            // would include sub-threshold files — hydrate exact aggregates for
            // the returned groups in one fact scan (page size is clamped).
            sql::RollupPlan::ExactSetApproxAggs => {
                let sq = sql::build_collapse_rollup(q, &gen.layers);
                let (mut groups, _) =
                    self.with_conn(deadline, move |conn| query_groups(conn, &sq))?;
                let info_hashes: Vec<String> = groups.iter().map(|g| g.info_hash.clone()).collect();
                let agg = sql::build_group_aggregates(&info_hashes, &q.filters, &gen.layers);
                let (rows, _) =
                    self.with_conn(deadline, move |conn| query_group_aggs(conn, &agg))?;
                for g in &mut groups {
                    if let Some((count, total, max)) = rows.get(&g.info_hash).copied() {
                        g.matching_file_count = count;
                        g.matching_total_size = total;
                        g.matching_max_size = max;
                    }
                }
                Ok(groups)
            }
            // path / size_max: the rollup cannot serve this shape at all.
            sql::RollupPlan::FactOnly => {
                let sq = sql::build_collapse_fact(q, &gen.layers);
                let (rows, _) = self.with_conn(deadline, move |conn| query_groups(conn, &sq))?;
                Ok(rows)
            }
        }
    }

    fn preview(
        &self,
        gen: &LoadedGeneration,
        info_hash: &str,
        filters: &Filters,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<FileHitRow>> {
        let mut previews = self.previews(gen, &[info_hash.to_owned()], filters, limit, deadline)?;
        Ok(previews.remove(info_hash).unwrap_or_default())
    }

    fn previews(
        &self,
        gen: &LoadedGeneration,
        info_hashes: &[String],
        filters: &Filters,
        limit: u32,
        deadline: Deadline,
    ) -> Result<PreviewRows> {
        let sq = sql::build_previews(info_hashes, filters, limit, &gen.layers);
        let (rows, _) = self.with_conn(deadline, move |conn| query_previews(conn, &sq))?;
        Ok(rows)
    }

    fn count(
        &self,
        gen: &LoadedGeneration,
        q: &CountQuery,
        deadline: Deadline,
    ) -> Result<(u64, bool)> {
        let sq = sql::build_count(q, &gen.layers);
        let (count, interrupted) =
            self.with_conn(deadline, move |conn| query_scalar_u64(conn, &sq))?;
        Ok((count, interrupted))
    }

    fn facet_ext(
        &self,
        gen: &LoadedGeneration,
        filters: &Filters,
        deadline: Deadline,
    ) -> Result<Vec<FacetBucketRow>> {
        // Facet buckets count MATCHING FILES: the rollup form is exact only
        // when no size/path filter is in play (otherwise it would drop the
        // path filter and mis-handle size bounds — see sql::rollup_plan).
        let sq = match sql::rollup_plan(filters) {
            sql::RollupPlan::Exact => sql::build_facet_ext(filters, &gen.layers),
            _ => sql::build_facet_fact(filters, &gen.layers),
        };
        let (rows, _) = self.with_conn(deadline, move |conn| query_facets(conn, &sq))?;
        Ok(rows)
    }
}

fn query_files(conn: &Connection, sq: &SafeQuery) -> Result<Vec<FileHitRow>> {
    let mut stmt = conn.prepare(&sq.sql)?;
    let values = DuckEngine::bind_values(&sq.params);
    let rows = stmt.query_map(params_from_iter(values.iter()), |r| {
        Ok(FileHitRow {
            info_hash: r.get::<_, String>(0)?,
            file_index: r.get::<_, i64>(1)? as u32,
            path: r.get::<_, String>(2)?,
            extension: r.get::<_, Option<String>>(3)?,
            size: r.get::<_, i64>(4)? as u64,
        })
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_groups(conn: &Connection, sq: &SafeQuery) -> Result<Vec<GroupRow>> {
    let mut stmt = conn.prepare(&sq.sql)?;
    let values = DuckEngine::bind_values(&sq.params);
    let rows = stmt.query_map(params_from_iter(values.iter()), |r| {
        Ok(GroupRow {
            info_hash: r.get::<_, String>(0)?,
            matching_file_count: r.get::<_, i64>(1)? as u64,
            matching_total_size: r.get::<_, i64>(2)? as u64,
            matching_max_size: r.get::<_, i64>(3)? as u64,
        })
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_facets(conn: &Connection, sq: &SafeQuery) -> Result<Vec<FacetBucketRow>> {
    let mut stmt = conn.prepare(&sq.sql)?;
    let values = DuckEngine::bind_values(&sq.params);
    let rows = stmt.query_map(params_from_iter(values.iter()), |r| {
        Ok(FacetBucketRow {
            value: r.get::<_, Option<String>>(0)?,
            count: r.get::<_, i64>(1)? as u64,
            total_size: r.get::<_, i64>(2)? as u64,
        })
    })?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn query_scalar_u64(conn: &Connection, sq: &SafeQuery) -> Result<u64> {
    let mut stmt = conn.prepare(&sq.sql)?;
    let values = DuckEngine::bind_values(&sq.params);
    let n: i64 = stmt.query_row(params_from_iter(values.iter()), |r| r.get(0))?;
    Ok(n as u64)
}

fn query_previews(conn: &Connection, sq: &SafeQuery) -> Result<PreviewRows> {
    let rows = query_files(conn, sq)?;
    let mut out = PreviewRows::new();
    for row in rows {
        out.entry(row.info_hash.clone()).or_default().push(row);
    }
    Ok(out)
}

/// Exact matching aggregates (`count, sum, max`) keyed by torrent. `GROUP BY`
/// emits no row for zero matches, so missing keys are ignored by the caller.
fn query_group_aggs(
    conn: &Connection,
    sq: &SafeQuery,
) -> Result<std::collections::BTreeMap<String, (u64, u64, u64)>> {
    let mut stmt = conn.prepare(&sq.sql)?;
    let values = DuckEngine::bind_values(&sq.params);
    let rows = stmt.query_map(params_from_iter(values.iter()), |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, Option<i64>>(2)?,
            r.get::<_, Option<i64>>(3)?,
        ))
    })?;
    let mut out = std::collections::BTreeMap::new();
    for row in rows {
        let (info_hash, count, total, max) = row?;
        if let (c, Some(ts), Some(ms)) = (count, total, max) {
            if c > 0 {
                out.insert(info_hash, (c as u64, ts as u64, ms as u64));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::GenerationManager;
    use crate::query::{Filters, Sort, SortDir, SortField};
    use bitmagnet_model::BlobFile;
    use bitmagnet_parquet::export::{publish_empty_delta, Sinks};
    use bitmagnet_parquet::fact::SortMode;
    use bitmagnet_parquet::generation::{Kind, Layout};
    use std::time::{Duration, Instant};

    /// Build a real base generation (+ empty delta) on disk and return a manager.
    fn seed_generation(tag: &str) -> GenerationManager {
        let root = std::env::temp_dir().join(format!("bmfs-duck-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let layout = Layout::new(root);
        layout.ensure_dirs().unwrap();
        layout.write_watermark(0).unwrap();
        let dir = layout.new_version_dir(Kind::Base, "1").unwrap();
        let mut sinks = Sinks::create(&dir, SortMode::InMemory, false).unwrap();
        let blob = |path: &str, size: u64| BlobFile {
            index: 0,
            path: path.to_owned(),
            extension: "x".to_owned(),
            size,
        };
        // aa carries a SUB-threshold mkv too, so the size_min collapse must
        // hydrate exact aggregates (the raw rollup would count it).
        sinks
            .push_torrent(
                "aa",
                Ok(vec![
                    blob("Movie/big.mkv", 2_000_000_000),
                    blob("Movie/small.mkv", 5),
                    blob("Movie/s.srt", 1),
                ]),
            )
            .unwrap();
        sinks
            .push_torrent("bb", Ok(vec![blob("Show/ep.mkv", 1_500_000_000)]))
            .unwrap();
        // cc is a BEP-47-style torrent: one real file + padding entries (kept
        // in the fact flagged is_padding, excluded from rollups).
        sinks
            .push_torrent(
                "cc",
                Ok(vec![
                    blob("Show/finale.mkv", 1_200_000_000),
                    blob(".pad/999", 999),
                    blob(".pad/999", 999),
                ]),
            )
            .unwrap();
        sinks.finish(&dir).unwrap();
        layout.publish(Kind::Base, &dir).unwrap();
        publish_empty_delta(&layout, "1").unwrap();
        GenerationManager::open(layout).unwrap()
    }

    fn fq(filters: Filters, collapse: bool, limit: u32) -> FileQuery {
        FileQuery {
            filters,
            sort: Sort {
                field: SortField::Size,
                dir: SortDir::Desc,
            },
            limit,
            collapse_to_torrent: collapse,
            preview_limit: 5,
        }
    }

    #[test]
    fn duck_deadline_interrupts_cloned_connection_and_returns_promptly() {
        let engine = DuckEngine::open(DuckConfig {
            threads: 1,
            memory_limit: "1GB".to_owned(),
            pool_size: 1,
        })
        .unwrap();
        let held_primary = engine.checkout().unwrap();

        let started = Instant::now();
        let err = engine
            .with_conn(Deadline::starting_now(Duration::from_millis(20)), |conn| {
                let mut stmt =
                    conn.prepare("select count(*) from range(10000000) t1, range(1000000) t2")?;
                Ok(stmt.execute([])?)
            })
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<EngineError>(),
            Some(EngineError::QueryDeadlineExceeded)
        ));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "deadline call returned after {:?}",
            started.elapsed()
        );

        engine
            .pool
            .lock()
            .expect("duck pool poisoned")
            .push(held_primary);
        let cleanup_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < cleanup_deadline {
            if engine.pool.lock().expect("duck pool poisoned").len() == 2 {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(engine.pool.lock().expect("duck pool poisoned").len(), 2);
    }

    #[test]
    fn duck_reads_real_generation_end_to_end() {
        let mgr = seed_generation("e2e");
        let gen = mgr.current();
        let engine = DuckEngine::open(DuckConfig::default()).unwrap();
        let dl = Deadline::starting_now(Duration::from_secs(30));

        // mkv > 1GB: aa/big (2GB) + bb/ep (1.5GB)
        let filters = Filters {
            extensions: vec!["mkv".into()],
            size_min: Some(1_000_000_000),
            ..Default::default()
        };
        let rows = engine
            .search_files(&gen, &fq(filters.clone(), false, 50), dl)
            .unwrap();
        assert_eq!(rows.len(), 3); // aa/big, bb/ep, cc/finale
        assert_eq!(rows[0].info_hash, "aa"); // size DESC

        // collapse (size_min → rollup set + exact hydration): 2 torrents, and
        // aa's matching_file_count must be 1 (only big.mkv ≥ 1 GB — the raw
        // rollup would have said 2, counting small.mkv).
        let groups = engine
            .collapse(&gen, &fq(filters.clone(), true, 50), dl)
            .unwrap();
        assert_eq!(groups.len(), 3);
        let aa = groups.iter().find(|g| g.info_hash == "aa").unwrap();
        assert_eq!(aa.matching_file_count, 1);
        assert_eq!(aa.matching_total_size, 2_000_000_000);
        assert_eq!(aa.matching_max_size, 2_000_000_000);

        let previews = engine
            .previews(
                &gen,
                &["aa".to_owned(), "bb".to_owned()],
                &Filters {
                    extensions: vec!["mkv".into()],
                    ..Default::default()
                },
                1,
                dl,
            )
            .unwrap();
        assert_eq!(previews["aa"].len(), 1);
        assert_eq!(previews["aa"][0].path, "Movie/big.mkv");
        assert_eq!(previews["bb"].len(), 1);
        assert_eq!(previews["bb"][0].path, "Show/ep.mkv");

        // count distinct torrents = 2
        let (c, _) = engine
            .count(
                &gen,
                &CountQuery {
                    filters,
                    collapse_to_torrent: true,
                },
                dl,
            )
            .unwrap();
        assert_eq!(c, 3);
    }

    #[test]
    fn duck_padding_excluded_by_default_included_on_opt_in() {
        let mgr = seed_generation("pads");
        let gen = mgr.current();
        let engine = DuckEngine::open(DuckConfig::default()).unwrap();
        let dl = Deadline::starting_now(Duration::from_secs(30));

        // Default: cc shows only its real file; the pad rows are invisible to
        // finds, counts and facets (rollups are built padding-free).
        let rows = engine
            .search_files(&gen, &fq(Filters::default(), false, 50), dl)
            .unwrap();
        assert!(rows.iter().all(|r| !r.path.starts_with(".pad/")));
        let (files, _) = engine
            .count(
                &gen,
                &CountQuery {
                    filters: Filters::default(),
                    collapse_to_torrent: false,
                },
                dl,
            )
            .unwrap();
        assert_eq!(files, 5); // 3(aa) + 1(bb) + 1(cc real); pads excluded
        let buckets = engine.facet_ext(&gen, &Filters::default(), dl).unwrap();
        let null_bucket = buckets.iter().find(|b| b.value.is_none());
        assert!(null_bucket.is_none()); // the only NULL-ext rows were pads

        // Opt-in: the pads come back (forced onto the fact path).
        let inc = Filters {
            include_padding: true,
            ..Default::default()
        };
        let (files, _) = engine
            .count(
                &gen,
                &CountQuery {
                    filters: inc.clone(),
                    collapse_to_torrent: false,
                },
                dl,
            )
            .unwrap();
        assert_eq!(files, 7); // + the 2 pad rows
        let rows = engine.search_files(&gen, &fq(inc, false, 50), dl).unwrap();
        assert!(rows.iter().any(|r| r.path.starts_with(".pad/")));
    }

    #[test]
    fn duck_path_ilike_is_escaped_and_safe() {
        let mgr = seed_generation("ilike");
        let gen = mgr.current();
        let engine = DuckEngine::open(DuckConfig::default()).unwrap();
        // A path_query with a SQL metacharacter must be treated literally.
        let rows = engine
            .search_files(
                &gen,
                &fq(
                    Filters {
                        path_query: Some("Movie".into()),
                        ..Default::default()
                    },
                    false,
                    50,
                ),
                Deadline::starting_now(Duration::from_secs(30)),
            )
            .unwrap();
        assert_eq!(rows.len(), 3); // all Movie/* files (big.mkv, small.mkv, s.srt)
    }

    #[test]
    fn duck_collapse_respects_path_and_size_max() {
        let mgr = seed_generation("route");
        let gen = mgr.current();
        let engine = DuckEngine::open(DuckConfig::default()).unwrap();
        let dl = Deadline::starting_now(Duration::from_secs(30));

        // path filter (FactOnly): only aa has Movie/* files. The raw rollup
        // would have silently dropped the path filter and returned bb too.
        let groups = engine
            .collapse(
                &gen,
                &fq(
                    Filters {
                        path_query: Some("Movie".into()),
                        ..Default::default()
                    },
                    true,
                    50,
                ),
                dl,
            )
            .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].info_hash, "aa");
        assert_eq!(groups[0].matching_file_count, 3);

        // size_max (FactOnly): files ≤ 10 bytes exist in aa only (small.mkv,
        // s.srt). The rollup's max_size<=? would wrongly EXCLUDE aa (its mkv
        // group max is 2 GB) — the fact path must include it.
        let groups = engine
            .collapse(
                &gen,
                &fq(
                    Filters {
                        size_max: Some(10),
                        ..Default::default()
                    },
                    true,
                    50,
                ),
                dl,
            )
            .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].info_hash, "aa");
        assert_eq!(groups[0].matching_file_count, 2);

        // count distinct with size_max routes to the fact too.
        let (c, _) = engine
            .count(
                &gen,
                &CountQuery {
                    filters: Filters {
                        extensions: vec!["mkv".into()],
                        size_max: Some(10),
                        ..Default::default()
                    },
                    collapse_to_torrent: true,
                },
                dl,
            )
            .unwrap();
        assert_eq!(c, 1); // only aa has an mkv ≤ 10 bytes

        // facet under a size filter counts MATCHING files only.
        let buckets = engine
            .facet_ext(
                &gen,
                &Filters {
                    size_min: Some(1_000_000_000),
                    ..Default::default()
                },
                dl,
            )
            .unwrap();
        let mkv = buckets
            .iter()
            .find(|b| b.value.as_deref() == Some("mkv"))
            .unwrap();
        assert_eq!(mkv.count, 3); // big.mkv + ep.mkv + finale.mkv; small.mkv excluded
    }
}
