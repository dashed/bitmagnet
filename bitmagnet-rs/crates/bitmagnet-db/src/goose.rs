//! Read-only admission checks for the migration history owned by Goose.
//!
//! Goose identifies history rows by the monotonically increasing `id`, not by
//! timestamp. Historical Goose versions recorded rollback rows with
//! `is_applied = false`, and a later reapply can add another row for the same
//! migration version. Admission must therefore use only the newest row for
//! each version before selecting the first currently-applied row in descending
//! identity order, exactly as `goose.EnsureDBVersionContext` does.

use std::collections::HashSet;

use sqlx::{PgPool, Row};

use crate::error::Result;

/// The exact read-only query used to obtain Goose's ordered version history.
///
/// `id DESC` matches Goose's own PostgreSQL `ListMigrations` contract. The
/// newest-row-per-version projection remains explicit Rust logic so rollback
/// and reapply histories can be tested without a database.
pub const GOOSE_VERSION_HISTORY_SQL: &str = "SELECT id, version_id, is_applied\
\nFROM goose_db_version\
\nORDER BY id DESC";

/// The migration version Goose considers to be the current applied head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GooseAppliedHead {
    /// Goose migration version, such as `33` for `00033_*.sql`.
    pub version: i64,
}

/// A typed failure from an exact migration-head admission assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GooseHeadMismatch {
    /// The version table contained no currently-applied migration row.
    #[error("no applied Goose migration; required version {required}")]
    Missing {
        /// Version required by the caller.
        required: i64,
    },
    /// The database is at a different migration head than the caller requires.
    #[error("Goose migration head is {actual}; required version {required}")]
    Unexpected {
        /// Version required by the caller.
        required: i64,
        /// Version observed in the database.
        actual: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GooseHistoryRow {
    id: i32,
    version: i64,
    is_applied: bool,
}

/// Reads the effective applied migration head from `goose_db_version`.
///
/// This does not create the Goose table and never applies or rolls back a
/// migration. A missing table is therefore returned as the underlying SQLx
/// error, which keeps application admission fail-closed.
pub async fn read_goose_applied_head(pool: &PgPool) -> Result<Option<GooseAppliedHead>> {
    let rows = sqlx::query(GOOSE_VERSION_HISTORY_SQL)
        .fetch_all(pool)
        .await?;
    let history = rows
        .into_iter()
        .map(|row| {
            Ok(GooseHistoryRow {
                id: row.try_get("id")?,
                version: row.try_get("version_id")?,
                is_applied: row.try_get("is_applied")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(project_applied_head(history))
}

/// Requires an exact Goose applied head for fail-closed application admission.
pub fn assert_goose_applied_head(
    actual: Option<GooseAppliedHead>,
    required: i64,
) -> std::result::Result<GooseAppliedHead, GooseHeadMismatch> {
    match actual {
        Some(head) if head.version == required => Ok(head),
        Some(head) => Err(GooseHeadMismatch::Unexpected {
            required,
            actual: head.version,
        }),
        None => Err(GooseHeadMismatch::Missing { required }),
    }
}

fn project_applied_head(
    history: impl IntoIterator<Item = GooseHistoryRow>,
) -> Option<GooseAppliedHead> {
    let mut history = history.into_iter().collect::<Vec<_>>();
    history.sort_unstable_by_key(|row| std::cmp::Reverse(row.id));

    let mut seen_versions = HashSet::new();
    history
        .into_iter()
        .filter(|row| seen_versions.insert(row.version))
        .find(|row| row.is_applied)
        .map(|row| GooseAppliedHead {
            version: row.version,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i32, version: i64, is_applied: bool) -> GooseHistoryRow {
        GooseHistoryRow {
            id,
            version,
            is_applied,
        }
    }

    #[test]
    fn sql_shape_orders_by_goose_row_identity() {
        assert_eq!(
            GOOSE_VERSION_HISTORY_SQL,
            "SELECT id, version_id, is_applied\nFROM goose_db_version\nORDER BY id DESC"
        );
        assert!(!GOOSE_VERSION_HISTORY_SQL.contains("MAX(version_id)"));
        assert!(!GOOSE_VERSION_HISTORY_SQL.contains("tstamp"));
    }

    #[test]
    fn projection_uses_only_the_newest_state_for_each_version() {
        let head = project_applied_head([
            row(1, 0, true),
            row(2, 1, true),
            row(3, 2, true),
            row(4, 3, true),
            row(5, 3, false),
        ]);

        assert_eq!(head, Some(GooseAppliedHead { version: 2 }));
    }

    #[test]
    fn projection_recognizes_a_reapply_after_rollback() {
        let head = project_applied_head([
            row(9, 3, true),
            row(2, 1, true),
            row(8, 3, false),
            row(7, 2, false),
            row(3, 2, true),
            row(4, 3, true),
            row(1, 0, true),
        ]);

        assert_eq!(head, Some(GooseAppliedHead { version: 3 }));
    }

    #[test]
    fn projection_matches_goose_first_applied_not_numeric_max() {
        let head = project_applied_head([row(12, 2, true), row(11, 3, true), row(1, 0, true)]);

        assert_eq!(head, Some(GooseAppliedHead { version: 2 }));
    }

    #[test]
    fn projection_is_independent_of_query_iteration_order() {
        let ascending = [
            row(1, 0, true),
            row(2, 1, true),
            row(3, 2, true),
            row(4, 2, false),
        ];
        let descending = ascending.into_iter().rev().collect::<Vec<_>>();

        assert_eq!(
            project_applied_head(ascending),
            project_applied_head(descending)
        );
        assert_eq!(
            project_applied_head(ascending),
            Some(GooseAppliedHead { version: 1 })
        );
    }

    #[test]
    fn assertion_is_exact_and_typed() {
        let head = GooseAppliedHead { version: 33 };
        assert_eq!(assert_goose_applied_head(Some(head), 33), Ok(head));
        assert_eq!(
            assert_goose_applied_head(Some(head), 34),
            Err(GooseHeadMismatch::Unexpected {
                required: 34,
                actual: 33,
            })
        );
        assert_eq!(
            assert_goose_applied_head(None, 33),
            Err(GooseHeadMismatch::Missing { required: 33 })
        );
    }
}
