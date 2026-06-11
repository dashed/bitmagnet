//! The `torrent_files` side of each pair — raw read-only SQL mirroring the
//! sidecar's SafeQuery builder (`bitmagnet-filesearch::sql`) predicate for
//! predicate.
//!
//! Mirror rules (each one is a place the comparison could silently lie):
//! * `extension IN (...)` + `extension IS NULL` for the `""` bucket — same as
//!   the sidecar's `predicate()`;
//! * the path substring is `ILIKE`-escaped with the SAME escape map and
//!   `ESCAPE '\'`;
//! * path ordering is `COLLATE "C"` (binary), matching DuckDB's UTF-8
//!   code-point order — a locale collation would produce a different (still
//!   correct-looking!) row order;
//! * `info_hash` is compared/ordered as `encode(info_hash, 'hex')` — lowercase
//!   hex is order-isomorphic to the bytea;
//! * `"index"` (int4) and `sum(size)` (numeric) are cast to `bigint` in SQL so
//!   sqlx decodes exact `int8` (sqlx errors on an OID-mismatched `i64` read);
//! * all user values are bound `$n` parameters — never interpolated. The SQL
//!   string itself is assembled only from fixed fragments (hence the audited
//!   `AssertSqlSafe` in the executor).

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::{resolved_sort, FileRowN, GroupN, PairSpec, Shape, ShapeResult, SortField};

/// A bound parameter (PG `$n`).
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Text(String),
    I64(i64),
}

/// SQL text + ordered binds.
#[derive(Debug, Clone, PartialEq)]
pub struct PgQuery {
    pub sql: String,
    pub binds: Vec<Bind>,
}

/// `ILIKE`-escape, byte-identical to `bitmagnet-filesearch::sql::escape_like`.
fn escape_like(input: &str) -> String {
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

/// The WHERE body (no leading `WHERE`), pushing binds; mirrors the sidecar's
/// `predicate()` clause for clause.
fn predicate(spec: &PairSpec, binds: &mut Vec<Bind>) -> String {
    let mut clauses: Vec<String> = Vec::new();

    if !spec.extensions.is_empty() {
        let mut wants_null = false;
        let mut holes = Vec::new();
        for e in &spec.extensions {
            if e.is_empty() {
                wants_null = true;
            } else {
                binds.push(Bind::Text(e.clone()));
                holes.push(format!("${}", binds.len()));
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
    if let Some(min) = spec.size_min {
        binds.push(Bind::I64(min as i64));
        clauses.push(format!("size >= ${}", binds.len()));
    }
    if let Some(max) = spec.size_max {
        binds.push(Bind::I64(max as i64));
        clauses.push(format!("size <= ${}", binds.len()));
    }
    if let Some(q) = &spec.path_query {
        binds.push(Bind::Text(format!("%{}%", escape_like(q))));
        clauses.push(format!("path ILIKE ${} ESCAPE '\\'", binds.len()));
    }
    // Mirror of the sidecar's default `NOT is_padding`: torrent_files has no
    // such column, so the SAME classification the export materializes
    // (bitmagnet_parquet::decode::is_padding_path — BEP-47 `.pad/` prefix +
    // BitComet `_____padding_file` marker) is expressed as exact string ops
    // (left/strpos, NOT LIKE — underscores would be wildcards).
    if !spec.include_padding {
        clauses.push(
            "NOT (left(path, 5) = '.pad/' OR strpos(path, '_____padding_file') > 0 \
             OR strpos(path, '.____padding_file/') > 0)"
                .to_owned(),
        );
    }
    clauses.join(" AND ")
}

fn where_clause(pred: &str) -> String {
    if pred.is_empty() {
        String::new()
    } else {
        format!(" WHERE {pred}")
    }
}

/// Sort mirror of the sidecar's `order_by()`, with `COLLATE "C"` on path.
fn order_by(field: SortField, desc: bool) -> String {
    let dir = if desc { "DESC" } else { "ASC" };
    match field {
        SortField::Size => format!("ORDER BY size {dir}, info_hash ASC, \"index\" ASC"),
        SortField::Path => format!("ORDER BY path COLLATE \"C\" {dir}, info_hash ASC, \"index\" ASC"),
        SortField::InfoHash => format!("ORDER BY info_hash {dir}, \"index\" ASC"),
    }
}

/// Build the mirror query for `spec`. Pure — unit-tested without a database.
pub fn build(spec: &PairSpec) -> PgQuery {
    let mut binds = Vec::new();
    let pred = predicate(spec, &mut binds);
    let wh = where_clause(&pred);
    let sql = match spec.shape {
        Shape::Find => {
            let (field, desc) = resolved_sort(spec);
            binds.push(Bind::I64(i64::from(spec.limit)));
            format!(
                "SELECT encode(info_hash, 'hex') AS ih, \"index\"::bigint AS idx, \
                 path, extension, size FROM torrent_files{wh} {} LIMIT ${}",
                order_by(field, desc),
                binds.len()
            )
        }
        Shape::Collapse => {
            binds.push(Bind::I64(i64::from(spec.limit)));
            format!(
                "SELECT encode(info_hash, 'hex') AS ih, count(*)::bigint AS c, \
                 sum(size)::bigint AS ts, max(size) AS ms \
                 FROM torrent_files{wh} GROUP BY info_hash \
                 ORDER BY ms DESC, info_hash ASC LIMIT ${}",
                binds.len()
            )
        }
        Shape::CountFiles => {
            format!("SELECT count(*)::bigint AS c FROM torrent_files{wh}")
        }
        Shape::CountTorrents => {
            format!("SELECT count(DISTINCT info_hash)::bigint AS c FROM torrent_files{wh}")
        }
        Shape::Facet => {
            format!(
                "SELECT extension, count(*)::bigint AS c, sum(size)::bigint AS ts \
                 FROM torrent_files{wh} GROUP BY extension"
            )
        }
    };
    PgQuery { sql, binds }
}

/// Execute the mirror query, normalizing into the shape's comparison domain.
pub async fn run(pool: &PgPool, spec: &PairSpec) -> Result<ShapeResult> {
    let q = build(spec);
    // The SQL is assembled ONLY from fixed fragments + $n holes (audited
    // above); every user value travels as a bind.
    let mut query = sqlx::query(sqlx::AssertSqlSafe(q.sql.clone()));
    for b in &q.binds {
        query = match b {
            Bind::Text(s) => query.bind(s.clone()),
            Bind::I64(n) => query.bind(*n),
        };
    }
    let rows = query
        .fetch_all(pool)
        .await
        .with_context(|| format!("pg mirror query failed: {}", q.sql))?;

    Ok(match spec.shape {
        Shape::Find => ShapeResult::Rows(
            rows.iter()
                .map(|r| {
                    Ok(FileRowN {
                        info_hash: r.try_get::<String, _>("ih")?,
                        file_index: r.try_get::<i64, _>("idx")? as u32,
                        path: r.try_get("path")?,
                        extension: r
                            .try_get::<Option<String>, _>("extension")?
                            .unwrap_or_default(),
                        size: r.try_get::<i64, _>("size")? as u64,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        Shape::Collapse => ShapeResult::Groups(
            rows.iter()
                .map(|r| {
                    Ok(GroupN {
                        info_hash: r.try_get::<String, _>("ih")?,
                        matching_file_count: r.try_get::<i64, _>("c")? as u64,
                        matching_total_size: r.try_get::<i64, _>("ts")? as u64,
                        matching_max_size: r.try_get::<i64, _>("ms")? as u64,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        Shape::CountFiles | Shape::CountTorrents => ShapeResult::Count {
            count: rows
                .first()
                .map(|r| r.try_get::<i64, _>("c"))
                .transpose()?
                .unwrap_or(0) as u64,
            estimated: false,
        },
        Shape::Facet => {
            let mut m = BTreeMap::new();
            for r in &rows {
                let ext = r
                    .try_get::<Option<String>, _>("extension")?
                    .unwrap_or_default();
                let c = r.try_get::<i64, _>("c")? as u64;
                let ts = r.try_get::<i64, _>("ts")? as u64;
                m.insert(ext, (c, ts));
            }
            ShapeResult::Facet(m)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(shape: Shape) -> PairSpec {
        PairSpec {
            shape,
            label: None,
            extensions: vec!["mkv".into(), String::new()],
            size_min: Some(1_000_000_000),
            size_max: None,
            path_query: Some("100%_a".into()),
            include_padding: false,
            sort_field: Some("path".into()),
            sort_desc: false,
            limit: 50,
        }
    }

    #[test]
    fn find_mirrors_predicate_sort_and_binds() {
        let q = build(&spec(Shape::Find));
        assert!(q.sql.contains("FROM torrent_files"));
        assert!(q.sql.contains("(extension IN ($1) OR extension IS NULL)"));
        assert!(q.sql.contains("size >= $2"));
        assert!(q.sql.contains("path ILIKE $3 ESCAPE '\\'"));
        // binary collation on path order + the standard tiebreaks
        assert!(q
            .sql
            .contains("ORDER BY path COLLATE \"C\" ASC, info_hash ASC, \"index\" ASC"));
        assert!(q.sql.contains("LIMIT $4"));
        // int4 "index" cast to bigint for exact sqlx decode
        assert!(q.sql.contains("\"index\"::bigint"));
        assert_eq!(
            q.binds,
            vec![
                Bind::Text("mkv".into()),
                Bind::I64(1_000_000_000),
                Bind::Text("%100\\%\\_a%".into()), // ILIKE-escaped
                Bind::I64(50),
            ]
        );
    }

    #[test]
    fn collapse_orders_like_the_sidecar() {
        let q = build(&PairSpec {
            path_query: None,
            ..spec(Shape::Collapse)
        });
        assert!(q.sql.contains("GROUP BY info_hash"));
        assert!(q.sql.contains("ORDER BY ms DESC, info_hash ASC"));
        assert!(q.sql.contains("sum(size)::bigint")); // numeric → bigint cast
    }

    #[test]
    fn counts_have_no_limit() {
        let q = build(&PairSpec {
            path_query: None,
            sort_field: None,
            ..spec(Shape::CountTorrents)
        });
        assert!(q.sql.contains("count(DISTINCT info_hash)::bigint"));
        assert!(!q.sql.contains("LIMIT"));
    }

    #[test]
    fn facet_groups_by_extension_unordered() {
        let q = build(&PairSpec {
            extensions: vec![],
            path_query: None,
            ..spec(Shape::Facet)
        });
        assert!(q.sql.contains("GROUP BY extension"));
        assert!(!q.sql.contains("ORDER BY")); // compared as a map
    }
}
