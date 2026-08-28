//! `bitmagnet-shadow` — the V2 dual-read shadow harness (the L2 DROP gate).
//!
//! For each query-pair *shape*, runs the equivalent **`torrent_files` SQL**
//! (the retiring source of truth, read-only) and the **`FileSearchService`
//! gRPC call** (the sidecar under test), and compares the results **exactly**:
//!
//! | shape           | sidecar RPC                      | compared as                  |
//! |-----------------|----------------------------------|------------------------------|
//! | `find`          | `SearchFiles(collapse=false)`    | ordered file rows            |
//! | `collapse`      | `SearchFiles(collapse=true)`     | ordered torrent groups       |
//! | `count_files`   | `CountFiles(collapse=false)`     | exact count                  |
//! | `count_torrents`| `CountFiles(collapse=true)`      | exact count                  |
//! | `facet`         | `Facets(["extension"])`          | extension → (count, total)   |
//!
//! The PG side mirrors the sidecar's SQL builder (`bitmagnet-filesearch::sql`)
//! predicate-for-predicate: same `IN`/NULL-bucket extension semantics, same
//! size bounds, same `ILIKE`-escaping, same sort + tiebreaks — with
//! `COLLATE "C"` on path ordering (binary, matching DuckDB's UTF-8 code-point
//! order) and `encode(info_hash, 'hex')` (hex preserves bytea order).
//!
//! **GATE:** zero mismatches across the suite, run against the SAME snapshot
//! (a restore + a generation exported from it). Against live prod the
//! comparison is indicative only — `torrent_files` keeps moving while the
//! generation is as fresh as its last delta. Keep `path_query` patterns ASCII:
//! `ILIKE` case-folding of non-ASCII differs between PG (collation) and DuckDB.

use std::collections::BTreeMap;

use serde::Deserialize;

pub mod grpcmap;
pub mod pg;

/// The five query-pair shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Shape {
    Find,
    Collapse,
    CountFiles,
    CountTorrents,
    Facet,
}

/// One query pair: a filter + shape (deserialized from `--pairs` JSON, or from
/// the built-in suite). Field semantics mirror the proto `FileFilters`.
#[derive(Debug, Clone, Deserialize)]
pub struct PairSpec {
    pub shape: Shape,
    #[serde(default)]
    pub label: Option<String>,
    /// `extension IN (...)`; an empty-string entry selects the NULL bucket.
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default)]
    pub size_min: Option<u64>,
    #[serde(default)]
    pub size_max: Option<u64>,
    /// ASCII substring (see the module docs for the non-ASCII ILIKE caveat).
    #[serde(default)]
    pub path_query: Option<String>,
    /// Include BitTorrent padding files (default false, mirroring the server
    /// default — the PG mirror applies the equivalent path-pattern predicate).
    #[serde(default)]
    pub include_padding: bool,
    /// `size` | `path` | `info_hash`; `None` = the server default.
    #[serde(default)]
    pub sort_field: Option<String>,
    #[serde(default)]
    pub sort_desc: bool,
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    100
}

impl PairSpec {
    /// Human-readable filter summary for the CSV.
    pub fn filter_summary(&self) -> String {
        let mut parts = Vec::new();
        if !self.extensions.is_empty() {
            parts.push(format!("ext={}", self.extensions.join("|")));
        }
        if let Some(n) = self.size_min {
            parts.push(format!("min={n}"));
        }
        if let Some(n) = self.size_max {
            parts.push(format!("max={n}"));
        }
        if let Some(q) = &self.path_query {
            parts.push(format!("path~{q}"));
        }
        if self.include_padding {
            parts.push("pads=in".to_owned());
        }
        if let Some(f) = &self.sort_field {
            parts.push(format!(
                "sort={f}:{}",
                if self.sort_desc { "desc" } else { "asc" }
            ));
        }
        parts.push(format!("limit={}", self.limit));
        parts.join(" ")
    }
}

/// The sort the SERVER will actually apply (mirrors
/// `bitmagnet-filesearch::query::{Sort::from_proto, Sort::default}`): no sort
/// → size DESC; unknown field names fall back to size keeping the flag.
pub fn resolved_sort(spec: &PairSpec) -> (SortField, bool) {
    match spec.sort_field.as_deref() {
        None => (SortField::Size, true),
        Some("path") => (SortField::Path, spec.sort_desc),
        Some("info_hash") => (SortField::InfoHash, spec.sort_desc),
        Some(_) => (SortField::Size, spec.sort_desc),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortField {
    Size,
    Path,
    InfoHash,
}

// ===========================================================================
// Normalized results — both sides reduce to these before comparison.
// ===========================================================================

/// One file row, normalized (`ext` empty string = SQL NULL, matching the proto).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRowN {
    pub info_hash: String,
    pub file_index: u32,
    pub path: String,
    pub extension: String,
    pub size: u64,
}

/// One collapsed torrent group, normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupN {
    pub info_hash: String,
    pub matching_file_count: u64,
    pub matching_total_size: u64,
    pub matching_max_size: u64,
}

/// One side's result for a pair, in the shape's comparison domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeResult {
    /// Ordered (the pair's sort is a total order via tiebreaks).
    Rows(Vec<FileRowN>),
    /// Ordered by `(matching_max_size DESC, info_hash ASC)` — total order.
    Groups(Vec<GroupN>),
    /// `estimated` (the sidecar's deadline-capped flag) fails the gate.
    Count { count: u64, estimated: bool },
    /// extension → (count, total_size); `""` = the NULL bucket.
    Facet(BTreeMap<String, (u64, u64)>),
}

impl ShapeResult {
    /// Row/bucket cardinality for the CSV `n` columns.
    pub fn len(&self) -> usize {
        match self {
            Self::Rows(v) => v.len(),
            Self::Groups(v) => v.len(),
            Self::Count { .. } => 1,
            Self::Facet(m) => m.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Compare the PG (expected) and sidecar (actual) results exactly. `None` =
/// equal; `Some(detail)` = a short first-difference description.
pub fn compare(expected: &ShapeResult, actual: &ShapeResult) -> Option<String> {
    use ShapeResult as R;
    match (expected, actual) {
        (R::Rows(e), R::Rows(a)) => {
            if e.len() != a.len() {
                return Some(format!("row count {} != {}", e.len(), a.len()));
            }
            for (i, (x, y)) in e.iter().zip(a).enumerate() {
                if x != y {
                    return Some(format!("row {i}: pg {x:?} != sidecar {y:?}"));
                }
            }
            None
        }
        (R::Groups(e), R::Groups(a)) => {
            if e.len() != a.len() {
                return Some(format!("group count {} != {}", e.len(), a.len()));
            }
            for (i, (x, y)) in e.iter().zip(a).enumerate() {
                if x != y {
                    return Some(format!("group {i}: pg {x:?} != sidecar {y:?}"));
                }
            }
            None
        }
        (
            R::Count {
                count: e,
                estimated: _,
            },
            R::Count {
                count: a,
                estimated,
            },
        ) => {
            if *estimated {
                // A deadline-capped sidecar count cannot prove parity.
                return Some(format!("sidecar count {a} is ESTIMATED (pg {e})"));
            }
            (e != a).then(|| format!("count pg {e} != sidecar {a}"))
        }
        (R::Facet(e), R::Facet(a)) => {
            for (ext, ec) in e {
                match a.get(ext) {
                    None => return Some(format!("facet '{ext}' missing on sidecar (pg {ec:?})")),
                    Some(ac) if ac != ec => {
                        return Some(format!("facet '{ext}': pg {ec:?} != sidecar {ac:?}"))
                    }
                    _ => {}
                }
            }
            for ext in a.keys() {
                if !e.contains_key(ext) {
                    return Some(format!("facet '{ext}' extra on sidecar ({:?})", a[ext]));
                }
            }
            None
        }
        _ => Some("shape kind mismatch (harness bug)".to_owned()),
    }
}

/// The built-in suite: realistic, restore-friendly pairs covering every shape
/// AND every `RollupPlan` routing class of the sidecar (exact / hydrated /
/// fact-only). Extend or replace with `--pairs <json>`.
pub fn default_suite() -> Vec<PairSpec> {
    let p = |shape, label: &str| PairSpec {
        shape,
        label: Some(label.to_owned()),
        extensions: Vec::new(),
        size_min: None,
        size_max: None,
        path_query: None,
        include_padding: false,
        sort_field: None,
        sort_desc: false,
        limit: 100,
    };
    vec![
        // find — the canonical "mkv > 1 GB", server-default sort (size DESC)
        PairSpec {
            extensions: vec!["mkv".into()],
            size_min: Some(1_000_000_000),
            ..p(Shape::Find, "find:mkv>1g")
        },
        // find — multi-ext, two-sided range, size ASC
        PairSpec {
            extensions: vec!["mkv".into(), "avi".into()],
            size_min: Some(100_000_000),
            size_max: Some(2_000_000_000),
            sort_field: Some("size".into()),
            ..p(Shape::Find, "find:range")
        },
        // find — the NULL-extension bucket, info_hash ASC
        PairSpec {
            extensions: vec![String::new()],
            sort_field: Some("info_hash".into()),
            ..p(Shape::Find, "find:nullext")
        },
        // find — ASCII path substring, path ASC (binary collation both sides)
        PairSpec {
            path_query: Some("1080p".into()),
            sort_field: Some("path".into()),
            ..p(Shape::Find, "find:path")
        },
        // collapse — size_min: the rollup-set + exact-hydration path
        PairSpec {
            extensions: vec!["mkv".into()],
            size_min: Some(1_000_000_000),
            limit: 50,
            ..p(Shape::Collapse, "collapse:mkv>1g")
        },
        // collapse — ext-only: the pure-rollup exact path
        PairSpec {
            extensions: vec!["flac".into()],
            limit: 50,
            ..p(Shape::Collapse, "collapse:flac")
        },
        // collapse — size_max: the FactOnly routing (rollup would be WRONG)
        PairSpec {
            extensions: vec!["mkv".into()],
            size_max: Some(10_000_000),
            limit: 50,
            ..p(Shape::Collapse, "collapse:smallmkv")
        },
        // collapse — path filter: FactOnly (rollup has no path)
        PairSpec {
            path_query: Some("S01E01".into()),
            limit: 50,
            ..p(Shape::Collapse, "collapse:path")
        },
        // counts — file + distinct-torrent grains
        PairSpec {
            extensions: vec!["flac".into()],
            ..p(Shape::CountFiles, "count:flac-files")
        },
        PairSpec {
            extensions: vec!["flac".into()],
            ..p(Shape::CountTorrents, "count:flac-torrents")
        },
        PairSpec {
            extensions: vec!["mkv".into()],
            size_min: Some(4_000_000_000),
            ..p(Shape::CountTorrents, "count:mkv>4g-torrents")
        },
        // facet — ext-filtered (rollup path) + size-filtered (fact path)
        PairSpec {
            extensions: vec!["mkv".into(), "mp4".into(), "avi".into(), "flac".into()],
            ..p(Shape::Facet, "facet:video")
        },
        PairSpec {
            size_min: Some(1_000_000_000),
            ..p(Shape::Facet, "facet:>1g")
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ih: &str, idx: u32, size: u64) -> FileRowN {
        FileRowN {
            info_hash: ih.into(),
            file_index: idx,
            path: format!("{ih}/{idx}"),
            extension: "mkv".into(),
            size,
        }
    }

    #[test]
    fn compare_rows_orders_matter() {
        let a = ShapeResult::Rows(vec![row("aa", 0, 10), row("bb", 0, 5)]);
        assert!(compare(&a, &a.clone()).is_none());
        let b = ShapeResult::Rows(vec![row("bb", 0, 5), row("aa", 0, 10)]);
        assert!(compare(&a, &b).unwrap().contains("row 0"));
    }

    #[test]
    fn compare_counts_and_estimated_fails() {
        let e = ShapeResult::Count {
            count: 10,
            estimated: false,
        };
        assert!(compare(
            &e,
            &ShapeResult::Count {
                count: 10,
                estimated: false
            }
        )
        .is_none());
        assert!(compare(
            &e,
            &ShapeResult::Count {
                count: 11,
                estimated: false
            }
        )
        .is_some());
        // An estimated equal count still fails the gate.
        assert!(compare(
            &e,
            &ShapeResult::Count {
                count: 10,
                estimated: true
            }
        )
        .unwrap()
        .contains("ESTIMATED"));
    }

    #[test]
    fn compare_facets_as_maps() {
        let e = ShapeResult::Facet(BTreeMap::from([
            ("mkv".to_owned(), (3u64, 100u64)),
            (String::new(), (1, 5)), // NULL bucket
        ]));
        assert!(compare(&e, &e.clone()).is_none());
        let a = ShapeResult::Facet(BTreeMap::from([("mkv".to_owned(), (3u64, 100u64))]));
        assert!(compare(&e, &a).unwrap().contains("missing on sidecar"));
    }

    #[test]
    fn resolved_sort_mirrors_server_default_and_fallback() {
        let mut s = default_suite().remove(0);
        s.sort_field = None;
        assert_eq!(resolved_sort(&s), (SortField::Size, true)); // default size DESC
        s.sort_field = Some("path".into());
        s.sort_desc = false;
        assert_eq!(resolved_sort(&s), (SortField::Path, false));
        s.sort_field = Some("bogus".into());
        s.sort_desc = true;
        assert_eq!(resolved_sort(&s), (SortField::Size, true)); // unknown → size
    }

    #[test]
    fn default_suite_covers_every_shape() {
        let suite = default_suite();
        for shape in [
            Shape::Find,
            Shape::Collapse,
            Shape::CountFiles,
            Shape::CountTorrents,
            Shape::Facet,
        ] {
            assert!(suite.iter().any(|s| s.shape == shape), "{shape:?} missing");
        }
        // and the FactOnly routing classes are exercised
        assert!(suite
            .iter()
            .any(|s| s.shape == Shape::Collapse && s.size_max.is_some()));
        assert!(suite
            .iter()
            .any(|s| s.shape == Shape::Collapse && s.path_query.is_some()));
    }
}
