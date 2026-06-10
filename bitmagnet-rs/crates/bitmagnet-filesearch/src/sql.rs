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
           SELECT info_hash, file_index, path, extension, size \
           FROM read_parquet({base}) b \
           WHERE NOT EXISTS (SELECT 1 FROM read_parquet({tomb}) t WHERE t.info_hash = b.info_hash) \
           UNION ALL \
           SELECT info_hash, file_index, path, extension, size FROM read_parquet({delta}))",
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

/// Distinct-torrent collapse via the `agg_torrent_ext` rollup — the fast path
/// when there is no `path_query` (ext/size predicates only). Returns one row
/// per torrent with the matching file_count/total/max.
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

/// COUNT — files (collapse=false) or distinct torrents (collapse=true). The
/// distinct-torrent count uses the rollup when there is no path_query.
pub fn build_count(q: &CountQuery, paths: &GenPaths) -> SafeQuery {
    let mut params = Vec::new();
    if q.collapse_to_torrent && q.filters.path_query.is_none() {
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
}
