//! Safe SQL construction (FB-B1d).
//!
//! Two trust tiers, kept strictly separate:
//! * **server-controlled** — the generation's Parquet *paths* and the column /
//!   sort identifiers. These are not user input; paths are single-quote-escaped
//!   ([`quote_literal`]) and sort fields come from a fixed allow-list
//!   ([`SortField`]).
//! * **user-supplied values** — extensions, sizes, the path substring, page
//!   limit. These are **never** interpolated; they become bound `?` parameters
//!   ([`Param`]). The path substring is additionally `ILIKE`-escaped
//!   ([`escape_like`]) so a user `%`/`_`/`\` is a literal, not a wildcard.
//!
//! The result is a [`SafeQuery`] — `sql` text with positional `?` holes and the
//! ordered `params`. The engine binds them; nothing user-supplied reaches the
//! SQL string.

use crate::query::{CountQuery, FileQuery, Filters, SortDir, SortField};

/// A bound parameter value (DuckDB positional `?`).
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    Text(String),
    U64(u64),
}

/// SQL text with positional `?` placeholders and the ordered bound parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct SafeQuery {
    pub sql: String,
    pub params: Vec<Param>,
}

/// Resolved, server-controlled Parquet paths for the current generation pair.
#[derive(Debug, Clone)]
pub struct GenPaths {
    pub base_fact: String,
    pub delta_fact: String,
    pub delta_tombstones: String,
    pub base_agg_torrent_ext: String,
    pub delta_agg_torrent_ext: String,
}

/// Escape an `ILIKE` pattern body so user `\`, `%`, `_` match literally. Pair
/// with `ESCAPE '\'`. (Order matters: escape the escape char first.)
pub fn escape_like(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 8);
    for ch in input.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '%' => out.push_str("\\%"),
            '_' => out.push_str("\\_"),
            other => out.push(other),
        }
    }
    out
}

/// Single-quote-escape a server-controlled string literal (Parquet path). Never
/// used for user input — that path goes through [`Param`].
pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The base+delta `files` CTE — the EXP-B supersession-correct anti-join
/// (TORRENT-granular: a re-crawl/delete hides the *whole* base fileset; the
/// delta fact supplies the survivors). Paths are server-controlled.
pub fn files_cte(paths: &GenPaths) -> String {
    format!(
        "WITH files AS (\
           SELECT info_hash, file_index, path, extension, size, is_padding \
           FROM read_parquet({base}) b \
           WHERE NOT EXISTS (SELECT 1 FROM read_parquet({tomb}) t WHERE t.info_hash = b.info_hash) \
           UNION ALL \
           SELECT info_hash, file_index, path, extension, size, is_padding FROM read_parquet({delta}))",
        base = quote_literal(&paths.base_fact),
        tomb = quote_literal(&paths.delta_tombstones),
        delta = quote_literal(&paths.delta_fact),
    )
}

/// The base+delta `att` CTE over the `agg_torrent_ext` rollups, reconciled the
/// same way (the rollup-served collapse/facet path — the `<50 ms` lever).
fn att_cte(paths: &GenPaths) -> String {
    format!(
        "WITH att AS (\
           SELECT info_hash, extension, file_count, total_size, max_size \
           FROM read_parquet({base}) b \
           WHERE NOT EXISTS (SELECT 1 FROM read_parquet({tomb}) t WHERE t.info_hash = b.info_hash) \
           UNION ALL \
           SELECT info_hash, extension, file_count, total_size, max_size FROM read_parquet({delta}))",
        base = quote_literal(&paths.base_agg_torrent_ext),
        tomb = quote_literal(&paths.delta_tombstones),
        delta = quote_literal(&paths.delta_agg_torrent_ext),
    )
}

/// Append the WHERE predicate for `filters`, pushing bound params. Returns the
/// predicate body (without the leading `WHERE`); empty string = no filter.
fn predicate(filters: &Filters, params: &mut Vec<Param>) -> String {
    let mut clauses: Vec<String> = Vec::new();

    if !filters.extensions.is_empty() {
        // extension IN (?, ?, ...); '' selects the NULL-extension bucket too.
        let mut wants_null = false;
        let mut holes = Vec::new();
        for e in &filters.extensions {
            if e.is_empty() {
                wants_null = true;
            } else {
                holes.push("?");
                params.push(Param::Text(e.clone()));
            }
        }
        let mut parts = Vec::new();
        if !holes.is_empty() {
            parts.push(format!("extension IN ({})", holes.join(", ")));
        }
        if wants_null {
            parts.push("extension IS NULL".to_owned());
        }
        clauses.push(format!("({})", parts.join(" OR ")));
    }
    if let Some(min) = filters.size_min {
        clauses.push("size >= ?".to_owned());
        params.push(Param::U64(min));
    }
    if let Some(max) = filters.size_max {
        clauses.push("size <= ?".to_owned());
        params.push(Param::U64(max));
    }
    if let Some(q) = &filters.path_query {
        clauses.push("path ILIKE ? ESCAPE '\\'".to_owned());
        params.push(Param::Text(format!("%{}%", escape_like(q))));
    }
    // Padding files are excluded BY DEFAULT (the materialized export-time
    // column — no per-row path pattern cost). include_padding=true omits the
    // clause; rollup_plan() routes such queries to the fact (rollups are
    // built padding-free).
    if !filters.include_padding {
        clauses.push("NOT is_padding".to_owned());
    }
    clauses.join(" AND ")
}

fn order_by(field: SortField, dir: SortDir) -> &'static str {
    match (field, dir) {
        (SortField::Size, SortDir::Asc) => "ORDER BY size ASC, info_hash ASC, file_index ASC",
        (SortField::Size, SortDir::Desc) => "ORDER BY size DESC, info_hash ASC, file_index ASC",
        (SortField::Path, SortDir::Asc) => "ORDER BY path ASC, info_hash ASC, file_index ASC",
        (SortField::Path, SortDir::Desc) => "ORDER BY path DESC, info_hash ASC, file_index ASC",
        (SortField::InfoHash, SortDir::Asc) => "ORDER BY info_hash ASC, file_index ASC",
        (SortField::InfoHash, SortDir::Desc) => "ORDER BY info_hash DESC, file_index ASC",
    }
}

/// File-rows search (collapse = false). Always LIMIT-bounded.
pub fn build_search_files(q: &FileQuery, paths: &GenPaths) -> SafeQuery {
    let mut params = Vec::new();
    let pred = predicate(&q.filters, &mut params);
    let where_clause = if pred.is_empty() {
        String::new()
    } else {
        format!(" WHERE {pred}")
    };
    let sql = format!(
        "{cte} SELECT info_hash, file_index, path, extension, size FROM files{where_clause} {order} LIMIT ?",
        cte = files_cte(paths),
        order = order_by(q.sort.field, q.sort.dir),
    );
    params.push(Param::U64(u64::from(q.limit) + 1)); // +1 to detect has_next
    SafeQuery { sql, params }
}

/// Distinct-torrent collapse via the `agg_torrent_ext` rollup — the fast path,
/// valid only when [`rollup_plan`] says so: exact under [`RollupPlan::Exact`];
/// exact SET + max (hydrate count/total via [`build_group_aggregate`]) under
/// [`RollupPlan::ExactSetApproxAggs`]; NEVER for [`RollupPlan::FactOnly`]
/// shapes (use [`build_collapse_fact`]). Returns one row per torrent.
pub fn build_collapse_rollup(q: &FileQuery, paths: &GenPaths) -> SafeQuery {
    let mut params = Vec::new();
    let pred = predicate_rollup(&q.filters, &mut params);
    let where_clause = if pred.is_empty() {
        String::new()
    } else {
        format!(" WHERE {pred}")
    };
    // Aggregate the per-(torrent,ext) rollup rows up to per-torrent.
    let sql = format!(
        "{cte} SELECT info_hash, sum(file_count) AS matching_file_count, \
            sum(total_size) AS matching_total_size, max(max_size) AS matching_max_size \
         FROM att{where_clause} GROUP BY info_hash \
         ORDER BY matching_max_size DESC, info_hash ASC LIMIT ?",
        cte = att_cte(paths),
    );
    params.push(Param::U64(u64::from(q.limit) + 1));
    SafeQuery { sql, params }
}

/// How a collapse / distinct-count / facet may be served, decided purely from
/// the filter shape. The `att` rollup has NO `path` column and only a
/// per-(torrent,ext) `max_size` for size, so:
///
/// * `path_query` present → [`RollupPlan::FactOnly`] — the rollup would
///   silently DROP the path filter (wrong results, not just approximate);
/// * `size_max` present → [`RollupPlan::FactOnly`] — `max_size <= X` means
///   "ALL files of this ext ≤ X", NOT "∃ file ≤ X": wrong set membership;
/// * `size_min` only → [`RollupPlan::ExactSetApproxAggs`] — the torrent SET
///   is exact (`max_size >= X ⟺ ∃ file ≥ X`) and so is the per-torrent
///   matching max (each matching group's `max_size` IS a matching file's
///   size), so ordering/pagination hold — but `file_count`/`total_size`
///   would include sub-threshold files of matching extensions; the caller
///   hydrates exact aggregates per returned group ([`build_group_aggregate`]);
/// * ext-only / unfiltered → [`RollupPlan::Exact`] — rollup aggregates exact.
///
/// The `InMemoryEngine` is the reference semantics this routing must converge
/// to (it always computes matching-file aggregates exactly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupPlan {
    /// Rollup serves the query exactly.
    Exact,
    /// Rollup gives the exact torrent set + max, approximate count/total —
    /// hydrate per-group aggregates from the fact.
    ExactSetApproxAggs,
    /// Rollup cannot serve this filter shape — use the fact CTE.
    FactOnly,
}

/// Decide the [`RollupPlan`] for `filters` (pure; unit-tested without DuckDB).
pub fn rollup_plan(filters: &Filters) -> RollupPlan {
    // include_padding: the rollups are built PADDING-FREE — only the fact
    // (which keeps flagged padding rows) can serve a pads-included query.
    if filters.path_query.is_some() || filters.size_max.is_some() || filters.include_padding {
        RollupPlan::FactOnly
    } else if filters.size_min.is_some() {
        RollupPlan::ExactSetApproxAggs
    } else {
        RollupPlan::Exact
    }
}

/// Rollup predicate: ext/size on the per-(torrent,ext) grain (`max_size` for
/// the size bound). `path_query` is NOT supported here (rollup has no path).
fn predicate_rollup(filters: &Filters, params: &mut Vec<Param>) -> String {
    let mut clauses: Vec<String> = Vec::new();
    if !filters.extensions.is_empty() {
        let mut wants_null = false;
        let mut holes = Vec::new();
        for e in &filters.extensions {
            if e.is_empty() {
                wants_null = true;
            } else {
                holes.push("?");
                params.push(Param::Text(e.clone()));
            }
        }
        let mut parts = Vec::new();
        if !holes.is_empty() {
            parts.push(format!("extension IN ({})", holes.join(", ")));
        }
        if wants_null {
            parts.push("extension IS NULL".to_owned());
        }
        clauses.push(format!("({})", parts.join(" OR ")));
    }
    if let Some(min) = filters.size_min {
        clauses.push("max_size >= ?".to_owned());
        params.push(Param::U64(min));
    }
    if let Some(max) = filters.size_max {
        clauses.push("max_size <= ?".to_owned());
        params.push(Param::U64(max));
    }
    clauses.join(" AND ")
}

/// Distinct-torrent collapse over the fact CTE — the correct (slower) path for
/// filter shapes the rollup cannot serve ([`RollupPlan::FactOnly`]): exact
/// matching-file aggregates per torrent, same ordering contract as the rollup
/// form (`matching_max_size DESC, info_hash ASC`).
pub fn build_collapse_fact(q: &FileQuery, paths: &GenPaths) -> SafeQuery {
    let mut params = Vec::new();
    let pred = predicate(&q.filters, &mut params);
    let where_clause = if pred.is_empty() {
        String::new()
    } else {
        format!(" WHERE {pred}")
    };
    let sql = format!(
        "{cte} SELECT info_hash, count(*) AS matching_file_count, \
            sum(size) AS matching_total_size, max(size) AS matching_max_size \
         FROM files{where_clause} GROUP BY info_hash \
         ORDER BY matching_max_size DESC, info_hash ASC LIMIT ?",
        cte = files_cte(paths),
    );
    params.push(Param::U64(u64::from(q.limit) + 1));
    SafeQuery { sql, params }
}

/// Exact matching-file aggregates for ONE torrent (a bounded point query) —
/// hydrates a rollup-served group's `file_count`/`total_size`/`max_size` under
/// [`RollupPlan::ExactSetApproxAggs`]. `info_hash` comes from an engine result
/// row (server data) but is bound as a parameter anyway.
pub fn build_group_aggregate(info_hash: &str, filters: &Filters, paths: &GenPaths) -> SafeQuery {
    let mut params = vec![Param::Text(info_hash.to_owned())];
    let pred = predicate(filters, &mut params);
    let and_clause = if pred.is_empty() {
        String::new()
    } else {
        format!(" AND {pred}")
    };
    let sql = format!(
        "{cte} SELECT count(*) AS c, sum(size) AS ts, max(size) AS ms \
         FROM files WHERE info_hash = ?{and_clause}",
        cte = files_cte(paths),
    );
    SafeQuery { sql, params }
}

/// Per-extension facet over the fact CTE — the correct path whenever size/path
/// filters are present (facet buckets count MATCHING FILES; the rollup form
/// would drop the path filter and mis-handle size bounds).
pub fn build_facet_fact(filters: &Filters, paths: &GenPaths) -> SafeQuery {
    let mut params = Vec::new();
    let pred = predicate(filters, &mut params);
    let where_clause = if pred.is_empty() {
        String::new()
    } else {
        format!(" WHERE {pred}")
    };
    let sql = format!(
        "{cte} SELECT extension, count(*) AS c, sum(size) AS ts \
         FROM files{where_clause} GROUP BY extension ORDER BY c DESC",
        cte = files_cte(paths),
    );
    SafeQuery { sql, params }
}

/// COUNT — files (collapse=false) or distinct torrents (collapse=true). The
/// distinct-torrent count uses the rollup whenever the plan is not
/// [`RollupPlan::FactOnly`] (set membership stays exact under `size_min`).
pub fn build_count(q: &CountQuery, paths: &GenPaths) -> SafeQuery {
    let mut params = Vec::new();
    if q.collapse_to_torrent && rollup_plan(&q.filters) != RollupPlan::FactOnly {
        let pred = predicate_rollup(&q.filters, &mut params);
        let where_clause = if pred.is_empty() {
            String::new()
        } else {
            format!(" WHERE {pred}")
        };
        let sql = format!(
            "{cte} SELECT count(DISTINCT info_hash) AS c FROM att{where_clause}",
            cte = att_cte(paths),
        );
        SafeQuery { sql, params }
    } else {
        let pred = predicate(&q.filters, &mut params);
        let where_clause = if pred.is_empty() {
            String::new()
        } else {
            format!(" WHERE {pred}")
        };
        let agg = if q.collapse_to_torrent {
            "count(DISTINCT info_hash)"
        } else {
            "count(*)"
        };
        let sql = format!(
            "{cte} SELECT {agg} AS c FROM files{where_clause}",
            cte = files_cte(paths),
        );
        SafeQuery { sql, params }
    }
}

/// Per-extension facet over the rollup (`<3 ms` lever).
pub fn build_facet_ext(filters: &Filters, paths: &GenPaths) -> SafeQuery {
    let mut params = Vec::new();
    let pred = predicate_rollup(filters, &mut params);
    let where_clause = if pred.is_empty() {
        String::new()
    } else {
        format!(" WHERE {pred}")
    };
    let sql = format!(
        "{cte} SELECT extension, sum(file_count) AS c, sum(total_size) AS ts \
         FROM att{where_clause} GROUP BY extension ORDER BY c DESC",
        cte = att_cte(paths),
    );
    SafeQuery { sql, params }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Filters, Sort};

    fn paths() -> GenPaths {
        GenPaths {
            base_fact: "/g/base/v1/fact.parquet".into(),
            delta_fact: "/g/delta/v1/fact.parquet".into(),
            delta_tombstones: "/g/delta/v1/tombstones.parquet".into(),
            base_agg_torrent_ext: "/g/base/v1/agg_torrent_ext.parquet".into(),
            delta_agg_torrent_ext: "/g/delta/v1/agg_torrent_ext.parquet".into(),
        }
    }

    #[test]
    fn escape_like_neutralizes_wildcards() {
        assert_eq!(escape_like("100%_a\\b"), "100\\%\\_a\\\\b");
        assert_eq!(escape_like("plain"), "plain");
    }

    #[test]
    fn quote_literal_doubles_single_quotes() {
        assert_eq!(quote_literal("a'b"), "'a''b'");
    }

    #[test]
    fn search_binds_values_never_interpolates() {
        let q = FileQuery {
            filters: Filters {
                extensions: vec!["mkv".into()],
                size_min: Some(1_000_000_000),
                size_max: None,
                path_query: Some("rm -rf' OR 1=1".into()),
                include_padding: false,
            },
            sort: Sort {
                field: SortField::Size,
                dir: SortDir::Desc,
            },
            limit: 100,
            collapse_to_torrent: false,
            preview_limit: 5,
        };
        let sq = build_search_files(&q, &paths());
        // No user value appears in the SQL text — only `?` holes + ESCAPE.
        assert!(!sq.sql.contains("rm -rf"));
        assert!(!sq.sql.contains("mkv"));
        assert!(sq.sql.contains("extension IN (?)"));
        assert!(sq.sql.contains("size >= ?"));
        assert!(sq.sql.contains("path ILIKE ? ESCAPE '\\'"));
        assert!(sq.sql.contains("LIMIT ?"));
        // Params in order: ext, size_min, path-pattern, limit+1.
        assert_eq!(
            sq.params,
            vec![
                Param::Text("mkv".into()),
                Param::U64(1_000_000_000),
                Param::Text("%rm -rf' OR 1=1%".into()),
                Param::U64(101),
            ]
        );
    }

    #[test]
    fn empty_extension_selects_null_bucket() {
        let mut params = Vec::new();
        let pred = predicate(
            &Filters {
                extensions: vec!["".into(), "mkv".into()],
                ..Default::default()
            },
            &mut params,
        );
        assert!(pred.contains("extension IN (?)"));
        assert!(pred.contains("extension IS NULL"));
        assert_eq!(params, vec![Param::Text("mkv".into())]);
    }

    #[test]
    fn collapse_uses_rollup_cte_and_max_size_bound() {
        let q = FileQuery {
            filters: Filters {
                extensions: vec!["mkv".into()],
                size_min: Some(1),
                size_max: None,
                path_query: None,
                include_padding: false,
            },
            sort: Sort::default(),
            limit: 50,
            collapse_to_torrent: true,
            preview_limit: 5,
        };
        let sq = build_collapse_rollup(&q, &paths());
        assert!(sq.sql.contains("WITH att AS"));
        assert!(sq.sql.contains("GROUP BY info_hash"));
        assert!(sq.sql.contains("max_size >= ?"));
        assert!(sq.sql.contains("agg_torrent_ext.parquet"));
    }

    #[test]
    fn count_distinct_torrents_uses_rollup_without_path() {
        let sq = build_count(
            &CountQuery {
                filters: Filters {
                    extensions: vec!["mkv".into()],
                    ..Default::default()
                },
                collapse_to_torrent: true,
            },
            &paths(),
        );
        assert!(sq.sql.contains("count(DISTINCT info_hash)"));
        assert!(sq.sql.contains("WITH att AS"));
    }

    #[test]
    fn count_files_with_path_uses_fact() {
        let sq = build_count(
            &CountQuery {
                filters: Filters {
                    path_query: Some("foo".into()),
                    ..Default::default()
                },
                collapse_to_torrent: false,
            },
            &paths(),
        );
        assert!(sq.sql.contains("count(*)"));
        assert!(sq.sql.contains("WITH files AS"));
        assert!(sq.sql.contains("ILIKE ? ESCAPE"));
    }

    #[test]
    fn files_cte_is_antijoin_not_rownumber() {
        let cte = files_cte(&paths());
        assert!(cte.contains("NOT EXISTS"));
        assert!(cte.contains("UNION ALL"));
        assert!(!cte.contains("row_number"));
    }

    #[test]
    fn rollup_plan_routes_by_filter_shape() {
        // path filter: the rollup has no path column — must NOT be used.
        assert_eq!(
            rollup_plan(&Filters {
                path_query: Some("x".into()),
                ..Default::default()
            }),
            RollupPlan::FactOnly
        );
        // size_max: max_size <= X is "ALL files <= X", wrong set membership.
        assert_eq!(
            rollup_plan(&Filters {
                size_max: Some(10),
                ..Default::default()
            }),
            RollupPlan::FactOnly
        );
        // size_min only: set exact, aggregates approximate.
        assert_eq!(
            rollup_plan(&Filters {
                extensions: vec!["mkv".into()],
                size_min: Some(1),
                ..Default::default()
            }),
            RollupPlan::ExactSetApproxAggs
        );
        // ext-only / unfiltered: rollup exact.
        assert_eq!(
            rollup_plan(&Filters {
                extensions: vec!["mkv".into()],
                ..Default::default()
            }),
            RollupPlan::Exact
        );
        assert_eq!(rollup_plan(&Filters::default()), RollupPlan::Exact);
        // include_padding: rollups are built padding-free → only the fact can
        // serve a pads-included query.
        assert_eq!(
            rollup_plan(&Filters {
                include_padding: true,
                ..Default::default()
            }),
            RollupPlan::FactOnly
        );
    }

    #[test]
    fn padding_excluded_by_default_in_fact_predicates() {
        let mut params = Vec::new();
        let pred = predicate(&Filters::default(), &mut params);
        assert_eq!(pred, "NOT is_padding");
        assert!(params.is_empty());
        let pred = predicate(
            &Filters {
                include_padding: true,
                ..Default::default()
            },
            &mut params,
        );
        assert!(pred.is_empty()); // opt-in: no padding clause at all
        // and the files CTE carries the column for the predicate to act on
        assert!(files_cte(&paths()).contains("is_padding"));
    }

    #[test]
    fn collapse_fact_groups_with_full_predicate() {
        let q = FileQuery {
            filters: Filters {
                extensions: vec!["mkv".into()],
                size_min: None,
                size_max: Some(100),
                path_query: Some("Movie".into()),
                include_padding: false,
            },
            sort: Sort::default(),
            limit: 50,
            collapse_to_torrent: true,
            preview_limit: 5,
        };
        let sq = build_collapse_fact(&q, &paths());
        // Fact CTE (per-file grain), full predicate incl. path + size_max.
        assert!(sq.sql.contains("WITH files AS"));
        assert!(!sq.sql.contains("WITH att AS"));
        assert!(sq.sql.contains("GROUP BY info_hash"));
        assert!(sq.sql.contains("size <= ?"));
        assert!(sq.sql.contains("ILIKE ? ESCAPE"));
        assert!(sq.sql.contains("ORDER BY matching_max_size DESC, info_hash ASC"));
        // ext, size_max, path-pattern, limit+1 — all bound.
        assert_eq!(
            sq.params,
            vec![
                Param::Text("mkv".into()),
                Param::U64(100),
                Param::Text("%Movie%".into()),
                Param::U64(51),
            ]
        );
    }

    #[test]
    fn group_aggregate_binds_info_hash_first() {
        let f = Filters {
            extensions: vec!["mkv".into()],
            size_min: Some(1_000_000_000),
            ..Default::default()
        };
        let sq = build_group_aggregate("aabb", &f, &paths());
        assert!(sq.sql.contains("WHERE info_hash = ?"));
        assert!(sq.sql.contains("count(*)"));
        assert!(sq.sql.contains("WITH files AS"));
        assert_eq!(
            sq.params,
            vec![
                Param::Text("aabb".into()),
                Param::Text("mkv".into()),
                Param::U64(1_000_000_000),
            ]
        );
    }

    #[test]
    fn facet_fact_counts_matching_files() {
        let f = Filters {
            size_min: Some(1),
            path_query: Some("Movie".into()),
            ..Default::default()
        };
        let sq = build_facet_fact(&f, &paths());
        assert!(sq.sql.contains("WITH files AS"));
        assert!(!sq.sql.contains("WITH att AS"));
        assert!(sq.sql.contains("GROUP BY extension"));
        assert!(sq.sql.contains("ILIKE ? ESCAPE"));
    }

    #[test]
    fn count_torrents_with_size_max_uses_fact() {
        // size_max on the rollup would be wrong set membership — must route to
        // the fact CTE even for a collapse count.
        let sq = build_count(
            &CountQuery {
                filters: Filters {
                    extensions: vec!["mkv".into()],
                    size_max: Some(100),
                    ..Default::default()
                },
                collapse_to_torrent: true,
            },
            &paths(),
        );
        assert!(sq.sql.contains("WITH files AS"));
        assert!(!sq.sql.contains("WITH att AS"));
        assert!(sq.sql.contains("count(DISTINCT info_hash)"));
        assert!(sq.sql.contains("size <= ?"));
    }

    #[test]
    fn count_torrents_with_size_min_keeps_rollup() {
        // size_min set-membership is exact on the rollup (max_size >= X).
        let sq = build_count(
            &CountQuery {
                filters: Filters {
                    extensions: vec!["mkv".into()],
                    size_min: Some(1),
                    ..Default::default()
                },
                collapse_to_torrent: true,
            },
            &paths(),
        );
        assert!(sq.sql.contains("WITH att AS"));
        assert!(sq.sql.contains("max_size >= ?"));
    }
}
