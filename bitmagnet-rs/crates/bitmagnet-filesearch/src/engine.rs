//! Query engine abstraction.
//!
//! The [`Engine`] trait is what the gRPC service calls; two implementations:
//! * [`InMemoryEngine`] — evaluates the domain [`FileQuery`] over an in-memory
//!   row vector. No DuckDB, no filesystem; it exists so the whole service
//!   (concurrency, proto mapping, pagination, collapse) is unit-testable in the
//!   default build.
//! * `DuckEngine` (feature `duckdb-engine`) — the production engine: one
//!   persistent DuckDB instance reading the generation's Parquet via the
//!   [`crate::sql`] SafeQuery builder, with per-query interrupts for deadlines.
//!
//! Both take a `&LoadedGeneration` so the engine reads whichever generation the
//! caller pinned for the request (an in-flight query keeps its generation even
//! across a reload).

use std::time::{Duration, Instant};

use anyhow::Result;

use crate::generation::LoadedGeneration;
use crate::query::{CountQuery, FileQuery, Filters};

/// A single shared query budget: the absolute [`Instant`] by which all of one
/// request's engine scans must finish, together.
///
/// The watchdog (l2-11) bounds how long ONE request may pin a worker + pooled
/// connection. But a single request can run several sequential scans — a
/// `size_min` collapse does two fact/rollup scans inside [`Engine::collapse`] and
/// then a third in [`Engine::previews`]. Passing a fresh `Duration` deadline to
/// each scan armed every scan with the FULL budget independently, so a slow
/// collapse page could run ~3× the intended wall-clock and defeat the watchdog
/// (review #7 / #96).
///
/// `Deadline` fixes this: it is computed ONCE at request entry and shared (it is
/// `Copy`) across every scan; each scan arms its timeout with [`Deadline::remaining`]
/// — the time left until the shared instant — so the scans together can never
/// exceed one budget.
#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    at: Instant,
}

impl Deadline {
    /// Start a budget that expires `budget` from now. Call this ONCE per request.
    pub fn starting_now(budget: Duration) -> Self {
        Self {
            at: Instant::now() + budget,
        }
    }

    /// Build a deadline from an explicit absolute instant (mainly for tests).
    pub fn at(at: Instant) -> Self {
        Self { at }
    }

    /// Time left until the shared deadline; saturates at zero once elapsed, so an
    /// exhausted budget yields an immediate timeout rather than panicking.
    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }
}

/// Engine-level errors that callers may want to map to transport status codes.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("query exceeded deadline")]
    QueryDeadlineExceeded,
}

/// One file row returned by the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHitRow {
    pub info_hash: String,
    pub file_index: u32,
    pub path: String,
    pub extension: Option<String>,
    pub size: u64,
}

/// One collapsed-to-torrent group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRow {
    pub info_hash: String,
    pub matching_file_count: u64,
    pub matching_total_size: u64,
    pub matching_max_size: u64,
}

/// One facet bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetBucketRow {
    pub value: Option<String>,
    pub count: u64,
    pub total_size: u64,
}

/// Per-torrent previews keyed by info_hash.
pub type PreviewRows = std::collections::BTreeMap<String, Vec<FileHitRow>>;

/// The query engine the service depends on.
pub trait Engine: Send + Sync {
    /// File-rows search (collapse = false). Returns up to `limit + 1` rows so
    /// the caller can detect `has_next`.
    fn search_files(
        &self,
        gen: &LoadedGeneration,
        q: &FileQuery,
        deadline: Deadline,
    ) -> Result<Vec<FileHitRow>>;

    /// Distinct-torrent collapse. Returns up to `limit + 1` groups.
    fn collapse(
        &self,
        gen: &LoadedGeneration,
        q: &FileQuery,
        deadline: Deadline,
    ) -> Result<Vec<GroupRow>>;

    /// The matching files of one torrent (a group's preview), capped at `limit`.
    fn preview(
        &self,
        gen: &LoadedGeneration,
        info_hash: &str,
        filters: &Filters,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<FileHitRow>>;

    /// Matching-file previews for many torrents. The default keeps simple
    /// engines correct; production overrides it to batch the fact scan.
    fn previews(
        &self,
        gen: &LoadedGeneration,
        info_hashes: &[String],
        filters: &Filters,
        limit: u32,
        deadline: Deadline,
    ) -> Result<PreviewRows> {
        let mut out = PreviewRows::new();
        for info_hash in info_hashes {
            out.insert(
                info_hash.clone(),
                self.preview(gen, info_hash, filters, limit, deadline)?,
            );
        }
        Ok(out)
    }

    /// Count files or distinct torrents. The `bool` is `true` if estimated
    /// (e.g. a deadline-capped scan).
    fn count(
        &self,
        gen: &LoadedGeneration,
        q: &CountQuery,
        deadline: Deadline,
    ) -> Result<(u64, bool)>;

    /// Per-extension facet buckets.
    fn facet_ext(
        &self,
        gen: &LoadedGeneration,
        filters: &Filters,
        deadline: Deadline,
    ) -> Result<Vec<FacetBucketRow>>;
}

// ===========================================================================
// InMemoryEngine — the test/reference implementation
// ===========================================================================

/// A simple in-memory engine over `(info_hash, file_index, path, extension,
/// size)` tuples. Mirrors the SQL semantics (latest-wins not modelled — feed it
/// already-reconciled rows).
#[derive(Debug, Default, Clone)]
pub struct InMemoryEngine {
    pub rows: Vec<FileHitRow>,
}

impl InMemoryEngine {
    pub fn new(rows: Vec<FileHitRow>) -> Self {
        Self { rows }
    }

    fn matching<'a>(&'a self, filters: &'a Filters) -> impl Iterator<Item = &'a FileHitRow> + 'a {
        self.rows
            .iter()
            .filter(move |r| filters.matches(&r.extension, r.size, &r.path))
    }

    fn sort_rows(rows: &mut [FileHitRow], q: &FileQuery) {
        use crate::query::{SortDir, SortField};
        rows.sort_by(|a, b| {
            let ord = match q.sort.field {
                SortField::Size => a.size.cmp(&b.size),
                SortField::Path => a.path.cmp(&b.path),
                SortField::InfoHash => a.info_hash.cmp(&b.info_hash),
            }
            .then(a.info_hash.cmp(&b.info_hash))
            .then(a.file_index.cmp(&b.file_index));
            match q.sort.dir {
                SortDir::Asc => ord,
                SortDir::Desc => ord.reverse(),
            }
        });
    }
}

impl Engine for InMemoryEngine {
    fn search_files(
        &self,
        _gen: &LoadedGeneration,
        q: &FileQuery,
        _deadline: Deadline,
    ) -> Result<Vec<FileHitRow>> {
        let mut rows: Vec<FileHitRow> = self.matching(&q.filters).cloned().collect();
        Self::sort_rows(&mut rows, q);
        rows.truncate(q.limit as usize + 1);
        Ok(rows)
    }

    fn collapse(
        &self,
        _gen: &LoadedGeneration,
        q: &FileQuery,
        _deadline: Deadline,
    ) -> Result<Vec<GroupRow>> {
        use std::collections::BTreeMap;
        let mut groups: BTreeMap<String, GroupRow> = BTreeMap::new();
        for r in self.matching(&q.filters) {
            let g = groups.entry(r.info_hash.clone()).or_insert(GroupRow {
                info_hash: r.info_hash.clone(),
                matching_file_count: 0,
                matching_total_size: 0,
                matching_max_size: 0,
            });
            g.matching_file_count += 1;
            g.matching_total_size = g.matching_total_size.saturating_add(r.size);
            g.matching_max_size = g.matching_max_size.max(r.size);
        }
        let mut out: Vec<GroupRow> = groups.into_values().collect();
        // Order by max_size DESC, info_hash ASC (mirror the rollup SQL).
        out.sort_by(|a, b| {
            b.matching_max_size
                .cmp(&a.matching_max_size)
                .then(a.info_hash.cmp(&b.info_hash))
        });
        out.truncate(q.limit as usize + 1);
        Ok(out)
    }

    fn preview(
        &self,
        _gen: &LoadedGeneration,
        info_hash: &str,
        filters: &Filters,
        limit: u32,
        _deadline: Deadline,
    ) -> Result<Vec<FileHitRow>> {
        let mut rows: Vec<FileHitRow> = self
            .matching(filters)
            .filter(|r| r.info_hash == info_hash)
            .cloned()
            .collect();
        rows.sort_by(|a, b| b.size.cmp(&a.size).then(a.file_index.cmp(&b.file_index)));
        rows.truncate(limit as usize);
        Ok(rows)
    }

    fn previews(
        &self,
        _gen: &LoadedGeneration,
        info_hashes: &[String],
        filters: &Filters,
        limit: u32,
        _deadline: Deadline,
    ) -> Result<PreviewRows> {
        use std::collections::BTreeSet;
        let wanted: BTreeSet<&str> = info_hashes.iter().map(String::as_str).collect();
        let mut out = PreviewRows::new();
        for info_hash in info_hashes {
            out.entry(info_hash.clone()).or_default();
        }
        let mut rows: Vec<FileHitRow> = self
            .matching(filters)
            .filter(|r| wanted.contains(r.info_hash.as_str()))
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            a.info_hash
                .cmp(&b.info_hash)
                .then(b.size.cmp(&a.size))
                .then(a.file_index.cmp(&b.file_index))
        });
        for row in rows {
            let bucket = out.entry(row.info_hash.clone()).or_default();
            if bucket.len() < limit as usize {
                bucket.push(row);
            }
        }
        Ok(out)
    }

    fn count(
        &self,
        _gen: &LoadedGeneration,
        q: &CountQuery,
        _deadline: Deadline,
    ) -> Result<(u64, bool)> {
        if q.collapse_to_torrent {
            use std::collections::BTreeSet;
            let set: BTreeSet<&str> = self
                .matching(&q.filters)
                .map(|r| r.info_hash.as_str())
                .collect();
            Ok((set.len() as u64, false))
        } else {
            Ok((self.matching(&q.filters).count() as u64, false))
        }
    }

    fn facet_ext(
        &self,
        _gen: &LoadedGeneration,
        filters: &Filters,
        _deadline: Deadline,
    ) -> Result<Vec<FacetBucketRow>> {
        use std::collections::BTreeMap;
        let mut buckets: BTreeMap<Option<String>, FacetBucketRow> = BTreeMap::new();
        for r in self.matching(filters) {
            let b = buckets
                .entry(r.extension.clone())
                .or_insert(FacetBucketRow {
                    value: r.extension.clone(),
                    count: 0,
                    total_size: 0,
                });
            b.count += 1;
            b.total_size = b.total_size.saturating_add(r.size);
        }
        let mut out: Vec<FacetBucketRow> = buckets.into_values().collect();
        out.sort_by_key(|b| std::cmp::Reverse(b.count));
        Ok(out)
    }
}

#[cfg(feature = "duckdb-engine")]
pub use duck::DuckEngine;

#[cfg(feature = "duckdb-engine")]
pub mod duck;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Filters, Sort, SortDir, SortField};
    use crate::sql::{LayerPaths, LayerSet};

    fn row(ih: &str, idx: u32, path: &str, ext: Option<&str>, size: u64) -> FileHitRow {
        FileHitRow {
            info_hash: ih.to_owned(),
            file_index: idx,
            path: path.to_owned(),
            extension: ext.map(str::to_owned),
            size,
        }
    }

    fn gen() -> LoadedGeneration {
        LoadedGeneration {
            layers: LayerSet::new(vec![
                LayerPaths {
                    fact: "x".into(),
                    agg_torrent_ext: "x".into(),
                    tombstones: None,
                },
                LayerPaths {
                    fact: "x".into(),
                    agg_torrent_ext: "x".into(),
                    tombstones: Some("x".into()),
                },
            ]),
            base_version: "v1".into(),
            manifest_version: 0,
            segment_count: 0,
            delta_version: "v1".into(),
            delta_watermark: 0,
        }
    }

    fn engine() -> InMemoryEngine {
        InMemoryEngine::new(vec![
            row("aa", 0, "Movie/big.mkv", Some("mkv"), 2_000_000_000),
            row("aa", 1, "Movie/small.mkv", Some("mkv"), 5),
            row("aa", 2, "Movie/sub.srt", Some("srt"), 1),
            row("bb", 0, "Show/ep.mkv", Some("mkv"), 1_500_000_000),
            row("cc", 0, "readme", None, 10),
        ])
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
    fn search_files_filters_sorts_and_overfetches() {
        let e = engine();
        let q = fq(
            Filters {
                extensions: vec!["mkv".into()],
                size_min: Some(1_000_000_000),
                ..Default::default()
            },
            false,
            1,
        );
        let rows = e
            .search_files(&gen(), &q, Deadline::starting_now(Duration::from_secs(1)))
            .unwrap();
        // two mkv>1GB (aa/0, bb/0); limit 1 + overfetch 1 = 2 returned, size DESC
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].info_hash, "aa");
        assert_eq!(rows[1].info_hash, "bb");
    }

    #[test]
    fn collapse_groups_per_torrent() {
        let e = engine();
        let q = fq(
            Filters {
                extensions: vec!["mkv".into()],
                ..Default::default()
            },
            true,
            10,
        );
        let groups = e
            .collapse(&gen(), &q, Deadline::starting_now(Duration::from_secs(1)))
            .unwrap();
        assert_eq!(groups.len(), 2); // aa (2 mkv), bb (1 mkv)
        assert_eq!(groups[0].info_hash, "aa");
        assert_eq!(groups[0].matching_file_count, 2);
        assert_eq!(groups[0].matching_max_size, 2_000_000_000);
    }

    #[test]
    fn empty_ext_facet_counts_null_bucket() {
        let e = engine();
        let buckets = e
            .facet_ext(
                &gen(),
                &Filters::default(),
                Deadline::starting_now(Duration::from_secs(1)),
            )
            .unwrap();
        // mkv(3), srt(1), NULL(1)
        let null = buckets.iter().find(|b| b.value.is_none()).unwrap();
        assert_eq!(null.count, 1);
        let mkv = buckets
            .iter()
            .find(|b| b.value.as_deref() == Some("mkv"))
            .unwrap();
        assert_eq!(mkv.count, 3);
    }

    #[test]
    fn count_distinct_vs_files() {
        let e = engine();
        let filters = Filters {
            extensions: vec!["mkv".into()],
            ..Default::default()
        };
        let (files, _) = e
            .count(
                &gen(),
                &CountQuery {
                    filters: filters.clone(),
                    collapse_to_torrent: false,
                },
                Deadline::starting_now(Duration::from_secs(1)),
            )
            .unwrap();
        assert_eq!(files, 3);
        let (torrents, _) = e
            .count(
                &gen(),
                &CountQuery {
                    filters,
                    collapse_to_torrent: true,
                },
                Deadline::starting_now(Duration::from_secs(1)),
            )
            .unwrap();
        assert_eq!(torrents, 2);
    }

    #[test]
    fn preview_returns_matching_files_for_torrent() {
        let e = engine();
        let prev = e
            .preview(
                &gen(),
                "aa",
                &Filters {
                    extensions: vec!["mkv".into()],
                    ..Default::default()
                },
                5,
                Deadline::starting_now(Duration::from_secs(1)),
            )
            .unwrap();
        assert_eq!(prev.len(), 2);
        assert!(prev.iter().all(|r| r.info_hash == "aa"));
    }

    #[test]
    fn previews_returns_limited_matching_files_for_many_torrents() {
        let e = engine();
        let hashes = vec!["aa".to_owned(), "bb".to_owned()];
        let previews = e
            .previews(
                &gen(),
                &hashes,
                &Filters {
                    extensions: vec!["mkv".into()],
                    ..Default::default()
                },
                1,
                Deadline::starting_now(Duration::from_secs(1)),
            )
            .unwrap();
        assert_eq!(previews["aa"].len(), 1);
        assert_eq!(previews["aa"][0].path, "Movie/big.mkv");
        assert_eq!(previews["bb"].len(), 1);
        assert_eq!(previews["bb"][0].path, "Show/ep.mkv");
    }
}
