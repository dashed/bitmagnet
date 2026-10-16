//! Parity/latency battery helpers for the L2 segmented-store gate.
//!
//! This module stays DuckDB-free: it builds domain queries, executes them
//! through the public [`Engine`] trait, normalizes result rows, compares them,
//! and serializes per-case reports. The `bitmagnet-parity` binary supplies the
//! feature-gated DuckDB engine.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::engine::{Deadline, Engine, FacetBucketRow, FileHitRow, GroupRow};
use crate::generation::LoadedGeneration;
use crate::query::{CountQuery, FileQuery, Filters, Sort, SortDir, SortField};

pub const DEFAULT_REPS: usize = 5;
pub const DEFAULT_LIMIT: u32 = 100;
pub const DEFAULT_PREVIEW_LIMIT: u32 = 5;
pub const DEFAULT_SAMPLE_INFO_HASHES: usize = 8;
pub const DEFAULT_DEADLINE_SECS: u64 = 60;
pub const DEFAULT_EXTENSIONS: &[&str] = &["mkv", "mp4", "srt", "avi", "m4v"];

/// One stable battery case.
#[derive(Debug, Clone)]
pub struct BatteryCase {
    pub id: String,
    pub op: CaseOp,
    pub compare: CompareMode,
}

/// A query shape executed by the parity battery.
#[derive(Debug, Clone)]
pub enum CaseOp {
    SearchFiles(FileQuery),
    Collapse(FileQuery),
    /// Mirrors the collapsed service path: collapse, sample the page's
    /// info_hashes, then batch-preview those torrents under the same deadline.
    CollapseWithPreviews {
        query: FileQuery,
        sample_size: usize,
    },
    Count(CountQuery),
    FacetExt(Filters),
}

/// How result row order should be interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareMode {
    Exact,
    Multiset,
    /// Keep the outer order of sort-key groups, but compare rows inside each
    /// equal-key group as a multiset.
    TieAware {
        sort_column: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub enum Cell {
    Null,
    Text(String),
    U64(u64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Row(pub Vec<Cell>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompareOutcome {
    pub equal: bool,
    pub first_divergence: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseReport {
    pub case_id: String,
    pub equal: bool,
    pub rows_a: usize,
    pub rows_b: usize,
    pub first_divergence: Option<String>,
    pub p50_ms_a: f64,
    pub p50_ms_b: f64,
    pub max_ms_a: f64,
    pub max_ms_b: f64,
}

#[derive(Debug)]
struct TimedRows {
    rows: Vec<Row>,
    p50_ms: f64,
    max_ms: f64,
}

pub fn default_extensions() -> Vec<String> {
    DEFAULT_EXTENSIONS.iter().map(|s| (*s).to_owned()).collect()
}

pub fn default_deadline() -> Duration {
    Duration::from_secs(DEFAULT_DEADLINE_SECS)
}

pub fn build_battery(extensions: &[String]) -> Vec<BatteryCase> {
    let exts = if extensions.is_empty() {
        default_extensions()
    } else {
        extensions.to_vec()
    };

    let search_filters = Filters {
        extensions: exts.clone(),
        size_min: Some(1_000_000),
        size_max: Some(50_000_000_000),
        path_query: Some(".".to_owned()),
        include_padding: false,
    };
    let include_padding_filters = Filters {
        include_padding: true,
        ..search_filters.clone()
    };
    let ext_only = Filters {
        extensions: exts.clone(),
        ..Default::default()
    };
    let size_min = Filters {
        extensions: exts.clone(),
        size_min: Some(1_000_000_000),
        ..Default::default()
    };
    let size_max = Filters {
        extensions: exts.clone(),
        size_max: Some(50_000_000),
        ..Default::default()
    };
    let path_filter = Filters {
        extensions: exts,
        path_query: Some(".".to_owned()),
        ..Default::default()
    };

    let mut cases = Vec::new();
    for (field_name, field, sort_column) in [
        ("size", SortField::Size, 4),
        ("path", SortField::Path, 2),
        ("info_hash", SortField::InfoHash, 0),
    ] {
        for (dir_name, dir) in [("asc", SortDir::Asc), ("desc", SortDir::Desc)] {
            cases.push(BatteryCase {
                id: format!("search_rows_{field_name}_{dir_name}_ext_size_path_pad_excl"),
                op: CaseOp::SearchFiles(file_query(
                    search_filters.clone(),
                    Sort { field, dir },
                    false,
                )),
                compare: CompareMode::TieAware { sort_column },
            });
        }
    }
    cases.push(BatteryCase {
        id: "search_rows_size_desc_ext_size_path_pad_incl".to_owned(),
        op: CaseOp::SearchFiles(file_query(
            include_padding_filters,
            Sort {
                field: SortField::Size,
                dir: SortDir::Desc,
            },
            false,
        )),
        compare: CompareMode::TieAware { sort_column: 4 },
    });

    cases.push(BatteryCase {
        id: "collapse_rollup_exact_ext".to_owned(),
        op: CaseOp::Collapse(file_query(ext_only.clone(), Sort::default(), true)),
        compare: CompareMode::TieAware { sort_column: 3 },
    });
    cases.push(BatteryCase {
        id: "collapse_rollup_exactset_group_aggs_size_min".to_owned(),
        op: CaseOp::Collapse(file_query(size_min.clone(), Sort::default(), true)),
        compare: CompareMode::TieAware { sort_column: 3 },
    });
    cases.push(BatteryCase {
        id: "collapse_fact_path".to_owned(),
        op: CaseOp::Collapse(file_query(path_filter.clone(), Sort::default(), true)),
        compare: CompareMode::TieAware { sort_column: 3 },
    });
    cases.push(BatteryCase {
        id: "collapse_previews_exactset_aggs_sampled".to_owned(),
        op: CaseOp::CollapseWithPreviews {
            query: file_query(size_min.clone(), Sort::default(), true),
            sample_size: DEFAULT_SAMPLE_INFO_HASHES,
        },
        compare: CompareMode::Exact,
    });

    cases.push(BatteryCase {
        id: "count_files_fact_ext_size_path".to_owned(),
        op: CaseOp::Count(CountQuery {
            filters: search_filters,
            collapse_to_torrent: false,
        }),
        compare: CompareMode::Exact,
    });
    cases.push(BatteryCase {
        id: "count_torrents_rollup_exact_ext".to_owned(),
        op: CaseOp::Count(CountQuery {
            filters: ext_only.clone(),
            collapse_to_torrent: true,
        }),
        compare: CompareMode::Exact,
    });
    cases.push(BatteryCase {
        id: "count_torrents_rollup_exactset_size_min".to_owned(),
        op: CaseOp::Count(CountQuery {
            filters: size_min,
            collapse_to_torrent: true,
        }),
        compare: CompareMode::Exact,
    });
    cases.push(BatteryCase {
        id: "count_torrents_fact_size_max".to_owned(),
        op: CaseOp::Count(CountQuery {
            filters: size_max,
            collapse_to_torrent: true,
        }),
        compare: CompareMode::Exact,
    });

    cases.push(BatteryCase {
        id: "facet_ext_rollup_exact".to_owned(),
        op: CaseOp::FacetExt(ext_only),
        compare: CompareMode::TieAware { sort_column: 1 },
    });
    cases.push(BatteryCase {
        id: "facet_ext_fact_path".to_owned(),
        op: CaseOp::FacetExt(path_filter),
        compare: CompareMode::TieAware { sort_column: 1 },
    });

    cases
}

fn file_query(filters: Filters, sort: Sort, collapse_to_torrent: bool) -> FileQuery {
    FileQuery {
        filters,
        sort,
        limit: DEFAULT_LIMIT,
        collapse_to_torrent,
        preview_limit: DEFAULT_PREVIEW_LIMIT,
    }
}

pub fn validate_generation_artifacts(gen: &LoadedGeneration) -> Result<()> {
    for (idx, layer) in gen.layers.layers.iter().enumerate() {
        validate_artifact(idx, "fact", &layer.fact)?;
        validate_artifact(idx, "agg_torrent_ext", &layer.agg_torrent_ext)?;
        if let Some(tombstones) = &layer.tombstones {
            validate_artifact(idx, "tombstones", tombstones)?;
        }
    }
    Ok(())
}

fn validate_artifact(layer_idx: usize, artifact: &str, path: &str) -> Result<()> {
    let meta = std::fs::metadata(path)
        .with_context(|| format!("layer {layer_idx} {artifact} missing at {path}"))?;
    if !meta.is_file() {
        bail!("layer {layer_idx} {artifact} is not a file at {path}");
    }
    if meta.len() == 0 {
        bail!("layer {layer_idx} {artifact} is empty at {path}");
    }
    Ok(())
}

pub fn run_battery_pair<E: Engine>(
    engine: &E,
    gen_a: &LoadedGeneration,
    gen_b: &LoadedGeneration,
    cases: &[BatteryCase],
    reps: usize,
    deadline: Duration,
) -> Result<Vec<CaseReport>> {
    cases
        .iter()
        .map(|case| run_case_pair(engine, gen_a, gen_b, case, reps, deadline))
        .collect()
}

pub fn run_case_pair<E: Engine>(
    engine: &E,
    gen_a: &LoadedGeneration,
    gen_b: &LoadedGeneration,
    case: &BatteryCase,
    reps: usize,
    deadline: Duration,
) -> Result<CaseReport> {
    if reps == 0 {
        bail!("--reps must be at least 1");
    }
    let a = run_timed(engine, gen_a, case, reps, deadline)
        .with_context(|| format!("running {} against root A", case.id))?;
    let b = run_timed(engine, gen_b, case, reps, deadline)
        .with_context(|| format!("running {} against root B", case.id))?;
    let comparison = compare_rows(&a.rows, &b.rows, case.compare);
    Ok(CaseReport {
        case_id: case.id.clone(),
        equal: comparison.equal,
        rows_a: a.rows.len(),
        rows_b: b.rows.len(),
        first_divergence: comparison.first_divergence,
        p50_ms_a: a.p50_ms,
        p50_ms_b: b.p50_ms,
        max_ms_a: a.max_ms,
        max_ms_b: b.max_ms,
    })
}

fn run_timed<E: Engine>(
    engine: &E,
    gen: &LoadedGeneration,
    case: &BatteryCase,
    reps: usize,
    deadline: Duration,
) -> Result<TimedRows> {
    let mut timings = Vec::with_capacity(reps);
    let mut rows = Vec::new();
    for _ in 0..reps {
        let started = Instant::now();
        rows = execute_case(engine, gen, case, deadline)?;
        timings.push(started.elapsed());
    }
    let measured = if timings.len() > 1 {
        &timings[1..]
    } else {
        &timings[..]
    };
    Ok(TimedRows {
        rows,
        p50_ms: p50_ms(measured),
        max_ms: max_ms(measured),
    })
}

fn execute_case<E: Engine>(
    engine: &E,
    gen: &LoadedGeneration,
    case: &BatteryCase,
    deadline: Duration,
) -> Result<Vec<Row>> {
    let deadline = Deadline::starting_now(deadline);
    match &case.op {
        CaseOp::SearchFiles(q) => Ok(engine
            .search_files(gen, q, deadline)?
            .into_iter()
            .map(file_row)
            .collect()),
        CaseOp::Collapse(q) => Ok(engine
            .collapse(gen, q, deadline)?
            .into_iter()
            .map(group_row)
            .collect()),
        CaseOp::CollapseWithPreviews { query, sample_size } => {
            let mut groups = engine.collapse(gen, query, deadline)?;
            groups.truncate(query.limit as usize);
            let info_hashes: Vec<String> = groups
                .iter()
                .take(*sample_size)
                .map(|g| g.info_hash.clone())
                .collect();
            let previews = engine.previews(
                gen,
                &info_hashes,
                &query.filters,
                query.preview_limit,
                deadline,
            )?;
            let mut rows = Vec::new();
            for group in groups.iter().take(*sample_size) {
                rows.push(tagged_group_row(group));
            }
            for info_hash in info_hashes {
                if let Some(files) = previews.get(&info_hash) {
                    for file in files {
                        rows.push(tagged_file_row(file));
                    }
                }
            }
            Ok(rows)
        }
        CaseOp::Count(q) => {
            let (count, estimated) = engine.count(gen, q, deadline)?;
            Ok(vec![Row(vec![Cell::U64(count), Cell::Bool(estimated)])])
        }
        CaseOp::FacetExt(filters) => Ok(engine
            .facet_ext(gen, filters, deadline)?
            .into_iter()
            .map(facet_row)
            .collect()),
    }
}

fn p50_ms(durations: &[Duration]) -> f64 {
    let mut values: Vec<u128> = durations.iter().map(Duration::as_micros).collect();
    values.sort_unstable();
    micros_to_ms(values[values.len() / 2])
}

fn max_ms(durations: &[Duration]) -> f64 {
    micros_to_ms(
        durations
            .iter()
            .map(Duration::as_micros)
            .max()
            .unwrap_or_default(),
    )
}

fn micros_to_ms(us: u128) -> f64 {
    us as f64 / 1000.0
}

fn file_row(row: FileHitRow) -> Row {
    Row(vec![
        Cell::Text(row.info_hash),
        Cell::U64(u64::from(row.file_index)),
        Cell::Text(row.path),
        optional_text(row.extension),
        Cell::U64(row.size),
    ])
}

fn tagged_file_row(row: &FileHitRow) -> Row {
    Row(vec![
        Cell::Text("preview".to_owned()),
        Cell::Text(row.info_hash.clone()),
        Cell::U64(u64::from(row.file_index)),
        Cell::Text(row.path.clone()),
        optional_text(row.extension.clone()),
        Cell::U64(row.size),
    ])
}

fn group_row(row: GroupRow) -> Row {
    Row(vec![
        Cell::Text(row.info_hash),
        Cell::U64(row.matching_file_count),
        Cell::U64(row.matching_total_size),
        Cell::U64(row.matching_max_size),
    ])
}

fn tagged_group_row(row: &GroupRow) -> Row {
    Row(vec![
        Cell::Text("group".to_owned()),
        Cell::Text(row.info_hash.clone()),
        Cell::U64(row.matching_file_count),
        Cell::U64(row.matching_total_size),
        Cell::U64(row.matching_max_size),
    ])
}

fn facet_row(row: FacetBucketRow) -> Row {
    Row(vec![
        optional_text(row.value),
        Cell::U64(row.count),
        Cell::U64(row.total_size),
    ])
}

fn optional_text(value: Option<String>) -> Cell {
    value.map_or(Cell::Null, Cell::Text)
}

pub fn compare_rows(a: &[Row], b: &[Row], mode: CompareMode) -> CompareOutcome {
    match mode {
        CompareMode::Exact => compare_exact(a, b),
        CompareMode::Multiset => compare_multiset(a, b, "rows"),
        CompareMode::TieAware { sort_column } => compare_tie_aware(a, b, sort_column),
    }
}

pub fn compare_tie_aware(a: &[Row], b: &[Row], sort_column: usize) -> CompareOutcome {
    if a.len() != b.len() {
        return divergence(format!("row count differs: a={} b={}", a.len(), b.len()));
    }

    let mut pos = 0;
    while pos < a.len() {
        let Some(a_key) = a[pos].0.get(sort_column) else {
            return divergence(format!("row {pos} missing sort column {sort_column} in A"));
        };
        let Some(b_key) = b[pos].0.get(sort_column) else {
            return divergence(format!("row {pos} missing sort column {sort_column} in B"));
        };
        if a_key != b_key {
            return divergence(format!(
                "row {pos} sort key differs: a={} b={}",
                format_cell(a_key),
                format_cell(b_key)
            ));
        }

        let mut a_end = pos + 1;
        while a_end < a.len() && a[a_end].0.get(sort_column) == Some(a_key) {
            a_end += 1;
        }
        let mut b_end = pos + 1;
        while b_end < b.len() && b[b_end].0.get(sort_column) == Some(b_key) {
            b_end += 1;
        }
        if a_end - pos != b_end - pos {
            return divergence(format!(
                "tie group at row {pos} sort_key={} row count differs: a={} b={}",
                format_cell(a_key),
                a_end - pos,
                b_end - pos
            ));
        }

        let outcome = compare_multiset(
            &a[pos..a_end],
            &b[pos..b_end],
            &format!("tie group at row {pos} sort_key={}", format_cell(a_key)),
        );
        if !outcome.equal {
            return outcome;
        }
        pos = a_end;
    }
    equal()
}

fn compare_exact(a: &[Row], b: &[Row]) -> CompareOutcome {
    for (idx, (ra, rb)) in a.iter().zip(b.iter()).enumerate() {
        if ra != rb {
            return divergence(format!(
                "row {idx}: a={} b={}",
                format_row(ra),
                format_row(rb)
            ));
        }
    }
    if a.len() != b.len() {
        divergence(format!("row count differs: a={} b={}", a.len(), b.len()))
    } else {
        equal()
    }
}

fn compare_multiset(a: &[Row], b: &[Row], label: &str) -> CompareOutcome {
    if a.len() != b.len() {
        return divergence(format!(
            "{label} row count differs: a={} b={}",
            a.len(),
            b.len()
        ));
    }
    let mut sorted_a = a.to_vec();
    let mut sorted_b = b.to_vec();
    sorted_a.sort();
    sorted_b.sort();
    for (idx, (ra, rb)) in sorted_a.iter().zip(sorted_b.iter()).enumerate() {
        if ra != rb {
            return divergence(format!(
                "{label} differs at sorted offset {idx}: a={} b={}",
                format_row(ra),
                format_row(rb)
            ));
        }
    }
    equal()
}

fn equal() -> CompareOutcome {
    CompareOutcome {
        equal: true,
        first_divergence: None,
    }
}

fn divergence(first_divergence: String) -> CompareOutcome {
    CompareOutcome {
        equal: false,
        first_divergence: Some(first_divergence),
    }
}

fn format_row(row: &Row) -> String {
    let cells: Vec<String> = row.0.iter().map(format_cell).collect();
    format!("[{}]", cells.join(", "))
}

fn format_cell(cell: &Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_owned(),
        Cell::Text(s) => format!("{s:?}"),
        Cell::U64(n) => n.to_string(),
        Cell::Bool(v) => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{rollup_plan, RollupPlan};

    fn row(cells: Vec<Cell>) -> Row {
        Row(cells)
    }

    #[test]
    fn battery_construction_covers_rollup_plans_and_expected_ids() {
        let cases = build_battery(&["mkv".to_owned(), "srt".to_owned()]);
        assert!(cases.len() >= 8);
        let ids: std::collections::BTreeSet<&str> =
            cases.iter().map(|case| case.id.as_str()).collect();
        for id in [
            "search_rows_size_asc_ext_size_path_pad_excl",
            "search_rows_size_desc_ext_size_path_pad_excl",
            "search_rows_path_asc_ext_size_path_pad_excl",
            "search_rows_path_desc_ext_size_path_pad_excl",
            "search_rows_info_hash_asc_ext_size_path_pad_excl",
            "search_rows_info_hash_desc_ext_size_path_pad_excl",
            "search_rows_size_desc_ext_size_path_pad_incl",
            "collapse_rollup_exact_ext",
            "collapse_rollup_exactset_group_aggs_size_min",
            "collapse_fact_path",
            "collapse_previews_exactset_aggs_sampled",
            "count_files_fact_ext_size_path",
            "count_torrents_rollup_exact_ext",
            "count_torrents_rollup_exactset_size_min",
            "count_torrents_fact_size_max",
            "facet_ext_rollup_exact",
            "facet_ext_fact_path",
        ] {
            assert!(ids.contains(id), "missing case id {id}");
        }

        let mut plans = Vec::new();
        for case in &cases {
            match &case.op {
                CaseOp::Collapse(q) => {
                    plans.push(rollup_plan(&q.filters));
                }
                CaseOp::CollapseWithPreviews { query, .. } => {
                    plans.push(rollup_plan(&query.filters));
                }
                _ => {}
            }
        }
        assert!(plans.contains(&RollupPlan::Exact));
        assert!(plans.contains(&RollupPlan::ExactSetApproxAggs));
        assert!(plans.contains(&RollupPlan::FactOnly));
    }

    #[test]
    fn tie_aware_comparator_accepts_reordered_rows_inside_tie_group() {
        let a = vec![
            row(vec![Cell::U64(10), Cell::Text("a".to_owned())]),
            row(vec![Cell::U64(10), Cell::Text("b".to_owned())]),
            row(vec![Cell::U64(11), Cell::Text("c".to_owned())]),
        ];
        let b = vec![
            row(vec![Cell::U64(10), Cell::Text("b".to_owned())]),
            row(vec![Cell::U64(10), Cell::Text("a".to_owned())]),
            row(vec![Cell::U64(11), Cell::Text("c".to_owned())]),
        ];
        assert!(compare_tie_aware(&a, &b, 0).equal);
    }

    #[test]
    fn tie_aware_comparator_reports_first_divergence_with_multiset_semantics() {
        let a = vec![
            row(vec![Cell::U64(10), Cell::Text("a".to_owned())]),
            row(vec![Cell::U64(10), Cell::Text("b".to_owned())]),
        ];
        let b = vec![
            row(vec![Cell::U64(10), Cell::Text("a".to_owned())]),
            row(vec![Cell::U64(10), Cell::Text("c".to_owned())]),
        ];
        let outcome = compare_tie_aware(&a, &b, 0);
        assert!(!outcome.equal);
        assert_eq!(
            outcome.first_divergence.as_deref(),
            Some(
                "tie group at row 0 sort_key=10 differs at sorted offset 1: a=[10, \"b\"] b=[10, \"c\"]"
            )
        );
    }

    #[test]
    fn json_report_serialization_round_trips() {
        let reports = vec![CaseReport {
            case_id: "search_rows_size_desc_ext_size_path_pad_excl".to_owned(),
            equal: false,
            rows_a: 2,
            rows_b: 2,
            first_divergence: Some("row 1: a=x b=y".to_owned()),
            p50_ms_a: 1.25,
            p50_ms_b: 2.5,
            max_ms_a: 3.75,
            max_ms_b: 4.0,
        }];
        let json = serde_json::to_string(&reports).unwrap();
        let decoded: Vec<CaseReport> = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, reports);
    }
}
