//! Live-vs-tape comparison for the **local content search**.
//!
//! # The gap this closes
//!
//! A tape replay proves the classifier asks the right *questions*: it re-issues
//! Go's recorded requests and asserts them byte for byte. It says nothing about
//! whether the live backend *answers* them the way Go's did — the answers come
//! from the recording, not from PostgreSQL. So `bitmagnet-content-search`'s SQL
//! has had no answer-level evidence at all.
//!
//! This binary supplies it, and deliberately does NOT involve the classifier.
//! For every recorded `local.*` observation it takes Go's own request, re-issues
//! it against the live database through [`PgContentSearch`], and compares the
//! result to what Go recorded. One dependency, one question.
//!
//! # 🚨 How to read the output — drift is not defect
//!
//! The `content` table is live and moves. A difference is only evidence about
//! the port if the underlying rows did not change underneath it, so the verdicts
//! separate the cases that mean different things:
//!
//! * `content_by_id` is a primary-key (or alternative-identifier) lookup and is
//!   stable. A mismatch here is a real signal.
//! * `content_by_search` returns a ranked window over a table that gains rows
//!   continuously. A changed *membership* can be legitimate churn. What is NOT
//!   legitimate is the same membership in a different ORDER: that ordering is
//!   fixed by the identity tiebreak precisely so it cannot move, and
//!   [`Verdict::ReorderedWindow`] is therefore the highest-signal outcome this
//!   tool can produce.
//!
//! 🚨 `ts_rank_cd` is degenerate for these single-phrase queries — real corpora
//! show whole result sets tied at exactly 1.0 — so the rank is NOT compared.
//! Comparing it would flag float noise as divergence while telling you nothing;
//! the ORDER is the thing that matters, and the tiebreak is what determines it.
//!
//! # Read-only, by the same three layers as the write-set gate
//!
//! Session `default_transaction_read_only = on`; a startup refusal if the
//! connecting role holds `INSERT`/`UPDATE`/`DELETE` on a touched table; and
//! SELECT-only code. This process cannot write, whatever it does.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use bitmagnet_content_search::PgContentSearch;
use bitmagnet_model::ContentType;
use bitmagnet_tape::{decode_records, Record, OUTCOME_OK};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

/// Without this there is nothing to compare.
const REQUIRED_TABLES: [&str; 1] = ["content"];

/// Read ONLY by `content_by_id`'s alternative-identifier branch.
///
/// 🚨 Optional on purpose. The frozen shadow role has no SELECT on
/// `content_attributes` (the T11 grant gap), and demanding it would withhold a
/// whole run's worth of evidence over a branch the corpus may never exercise —
/// the 2026-08-09 tape holds 72 `local.content_by_search` observations and zero
/// `local.content_by_id`. A gate that refuses to produce evidence it could have
/// produced is worse than one that states its own coverage limit, so the limit
/// travels in the report instead.
const OPTIONAL_TABLES: [&str; 1] = ["content_attributes"];

const KIND_CONTENT_BY_SEARCH: &str = "local.content_by_search";
const KIND_CONTENT_BY_ID: &str = "local.content_by_id";

#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-localsearch-replay",
    about = "Re-issue a tape's recorded local-search requests against live PostgreSQL and diff the answers"
)]
struct Args {
    /// Directory holding `tape.jsonl`.
    #[arg(long)]
    tape_dir: PathBuf,

    #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
    postgres_dsn: String,

    /// Stop after this many observations. 0 means all of them.
    #[arg(long, default_value_t = 0)]
    limit: usize,

    #[arg(long, default_value_t = 4)]
    max_connections: u32,

    #[arg(long, default_value_t = 30)]
    statement_timeout_seconds: u64,

    /// Escape hatch for a role that can write. Never use it against production:
    /// the refusal is a safety layer, not a nuisance.
    #[arg(long, default_value_t = false)]
    allow_write_capable_role: bool,

    /// Cap on how many differing observations are listed in the report.
    #[arg(long, default_value_t = 25)]
    sample: usize,
}

/// Go's `localContentBySearchRequest`, as the tape encodes it.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchRequest {
    content_type: String,
    base_title: String,
    #[serde(default)]
    year: Option<u16>,
}

/// Go's `localContentByIDRequest`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ByIdRequest {
    content_type: String,
    source: String,
    id: String,
}

/// Go's `localContentResponse`. Only the identity of each item is compared —
/// see the module docs on why the rank is not.
#[derive(Debug, Deserialize)]
struct RecordedResponse {
    #[serde(default)]
    items: Vec<RecordedItem>,
}

#[derive(Debug, Deserialize)]
struct RecordedItem {
    content: RecordedContent,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordedContent {
    #[serde(rename = "type")]
    content_type: String,
    source: String,
    id: String,
}

impl RecordedContent {
    fn key(&self) -> String {
        format!("{}/{}/{}", self.content_type, self.source, self.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "verdict")]
enum Verdict {
    /// Live answered exactly what Go recorded, in the same order.
    Identical,
    /// 🚨 Same rows, different order. The identity tiebreak exists to make this
    /// impossible, so this is a real defect in the ordering, not table churn.
    ReorderedWindow {
        recorded: Vec<String>,
        live: Vec<String>,
    },
    /// Membership differs. On a by-id lookup that is a signal; on a search it
    /// may be legitimate churn in a table that grows continuously.
    MembershipChanged {
        recorded: Vec<String>,
        live: Vec<String>,
    },
    /// The live query failed.
    Failed { detail: String },
}

#[derive(Debug, Clone, Serialize)]
struct ObservationReport {
    subject: String,
    kind: String,
    #[serde(flatten)]
    verdict: Verdict,
}

#[derive(Debug, Default, Serialize)]
struct KindSummary {
    compared: usize,
    identical: usize,
    reordered: usize,
    membership_changed: usize,
    failed: usize,
}

#[derive(Debug, Serialize)]
struct Report {
    scope: &'static str,
    /// What this run could NOT cover, so the artifact carries its own limits.
    coverage: Vec<String>,
    tape_dir: String,
    by_kind: BTreeMap<String, KindSummary>,
    differences: Vec<ObservationReport>,
}

const SCOPE: &str = "Live-vs-tape comparison of the LOCAL CONTENT SEARCH only. Each recorded \
request is re-issued against live PostgreSQL and the ANSWER is diffed; the classifier is not \
involved and TMDB is not called. The `content` table moves, so a membership change on a search \
may be ordinary churn -- but a REORDERED window is not: the identity tiebreak exists to make \
the order stable. Ranks are deliberately not compared, because ts_rank_cd is degenerate for \
these single-phrase queries and float noise would swamp the signal.";

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let tape_path = args.tape_dir.join(bitmagnet_tape::TAPE_FILE_NAME);
    let bytes = std::fs::read(&tape_path)
        .with_context(|| format!("reading tape {}", tape_path.display()))?;
    let records = decode_records(&bytes[..]).context("decoding tape")?;

    let timeout_ms = args.statement_timeout_seconds.saturating_mul(1_000);
    let pool = PgPoolOptions::new()
        .max_connections(args.max_connections)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                // Layer 1: PostgreSQL rejects any write this process could issue.
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET application_name = 'bitmagnet-localsearch-replay'")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                    .bind(timeout_ms.to_string())
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&args.postgres_dsn)
        .await
        .context("connecting to PostgreSQL")?;

    let unreadable = preflight(&pool, args.allow_write_capable_role).await?;
    let coverage = unreadable
        .iter()
        .map(|table| {
            format!(
                "no SELECT on {table}: content_by_id's alternative-identifier branch is NOT covered"
            )
        })
        .collect::<Vec<_>>();
    for note in &coverage {
        tracing::warn!("{note}");
    }

    let search = PgContentSearch::new(pool.clone());
    let mut by_kind: BTreeMap<String, KindSummary> = BTreeMap::new();
    let mut differences = Vec::new();
    let mut seen = 0usize;

    for record in &records {
        // An incomplete record's observation list is a prefix, but each
        // observation it DID record is still a real (request, answer) pair, so
        // they are all worth comparing.
        for observation in &record.observations {
            if observation.kind != KIND_CONTENT_BY_SEARCH && observation.kind != KIND_CONTENT_BY_ID
            {
                continue;
            }
            // A recorded FAILURE has no answer to compare against.
            if observation.outcome != OUTCOME_OK {
                continue;
            }
            if args.limit != 0 && seen >= args.limit {
                break;
            }
            seen += 1;

            let verdict = compare(&search, record, observation).await;
            let summary = by_kind.entry(observation.kind.clone()).or_default();
            summary.compared += 1;
            match &verdict {
                Verdict::Identical => summary.identical += 1,
                Verdict::ReorderedWindow { .. } => summary.reordered += 1,
                Verdict::MembershipChanged { .. } => summary.membership_changed += 1,
                Verdict::Failed { .. } => summary.failed += 1,
            }

            if verdict != Verdict::Identical && differences.len() < args.sample {
                differences.push(ObservationReport {
                    subject: record.subject.clone(),
                    kind: observation.kind.clone(),
                    verdict,
                });
            }
        }
    }

    let report = Report {
        scope: SCOPE,
        coverage,
        tape_dir: args.tape_dir.display().to_string(),
        by_kind,
        differences,
    };
    eprintln!("{}", serde_json::to_string_pretty(&report)?);

    Ok(())
}

/// Re-issue one recorded request and diff the answer.
async fn compare(
    search: &PgContentSearch,
    record: &Record,
    observation: &bitmagnet_tape::Observation,
) -> Verdict {
    let Some(response) = observation.response.as_ref() else {
        return Verdict::Failed {
            detail: "recorded observation has no response body".to_owned(),
        };
    };
    let recorded: RecordedResponse = match serde_json::from_str(response.get()) {
        Ok(recorded) => recorded,
        Err(err) => {
            return Verdict::Failed {
                detail: format!("decoding recorded response: {err}"),
            }
        }
    };
    let recorded_keys: Vec<String> = recorded.items.iter().map(|i| i.content.key()).collect();

    let live_keys = match observation.kind.as_str() {
        KIND_CONTENT_BY_ID => {
            let request: ByIdRequest = match serde_json::from_str(observation.request.get()) {
                Ok(request) => request,
                Err(err) => return decode_failure(err),
            };
            let Some(content_type) = parse_content_type(&request.content_type) else {
                return unknown_type(&request.content_type);
            };
            match search
                .content_by_id(content_type, &request.source, &request.id)
                .await
            {
                Ok(found) => found
                    .into_iter()
                    .map(|c| format!("{}/{}/{}", c.content_type.as_str(), c.source, c.id))
                    .collect(),
                Err(err) => {
                    return Verdict::Failed {
                        detail: err.to_string(),
                    }
                }
            }
        }
        _ => {
            let request: SearchRequest = match serde_json::from_str(observation.request.get()) {
                Ok(request) => request,
                Err(err) => return decode_failure(err),
            };
            let Some(content_type) = parse_content_type(&request.content_type) else {
                return unknown_type(&request.content_type);
            };
            match search
                .content_by_search(content_type, &request.base_title, request.year)
                .await
            {
                Ok(items) => items
                    .into_iter()
                    .map(|i| {
                        format!(
                            "{}/{}/{}",
                            i.content.content_type.as_str(),
                            i.content.source,
                            i.content.id
                        )
                    })
                    .collect(),
                Err(err) => {
                    return Verdict::Failed {
                        detail: err.to_string(),
                    }
                }
            }
        }
    };

    let _ = record;
    verdict_for(recorded_keys, live_keys)
}

/// Order first, membership second — the two mean very different things.
fn verdict_for(recorded: Vec<String>, live: Vec<String>) -> Verdict {
    if recorded == live {
        return Verdict::Identical;
    }

    let mut recorded_set = recorded.clone();
    let mut live_set = live.clone();
    recorded_set.sort();
    live_set.sort();

    if recorded_set == live_set {
        // Same rows, different order. The identity tiebreak is supposed to make
        // this impossible, so it is a defect rather than churn.
        Verdict::ReorderedWindow { recorded, live }
    } else {
        Verdict::MembershipChanged { recorded, live }
    }
}

fn parse_content_type(value: &str) -> Option<ContentType> {
    value.parse().ok()
}

fn decode_failure(err: serde_json::Error) -> Verdict {
    Verdict::Failed {
        detail: format!("decoding recorded request: {err}"),
    }
}

fn unknown_type(value: &str) -> Verdict {
    Verdict::Failed {
        detail: format!("recorded content type {value:?} is not one this build knows"),
    }
}

/// Layer 2 of the read-only guarantee: refuse a role that could write.
///
/// Returns the OPTIONAL tables the role cannot read, so the caller can state the
/// resulting coverage limit rather than failing the whole run over it.
async fn preflight(pool: &PgPool, allow_write_capable_role: bool) -> Result<Vec<String>> {
    let read_only: String = sqlx::query_scalar("SHOW default_transaction_read_only")
        .fetch_one(pool)
        .await
        .context("reading default_transaction_read_only")?;
    anyhow::ensure!(
        read_only == "on",
        "session is not read-only (default_transaction_read_only = {read_only})"
    );

    let tables: Vec<String> = REQUIRED_TABLES
        .iter()
        .chain(OPTIONAL_TABLES.iter())
        .map(|t| (*t).to_owned())
        .collect();
    let rows = sqlx::query(
        "SELECT t AS table_name, \
                has_table_privilege(current_user, t, 'SELECT') AS may_select, \
                has_table_privilege(current_user, t, 'INSERT') \
                  OR has_table_privilege(current_user, t, 'UPDATE') \
                  OR has_table_privilege(current_user, t, 'DELETE') AS may_write \
         FROM unnest($1::text[]) AS t",
    )
    .bind(&tables)
    .fetch_all(pool)
    .await
    .context("checking table privileges")?;

    let mut missing_required = Vec::new();
    let mut missing_optional = Vec::new();
    let mut writable = Vec::new();
    for row in rows {
        let table: String = row.try_get("table_name")?;
        if !row.try_get::<bool, _>("may_select")? {
            if REQUIRED_TABLES.contains(&table.as_str()) {
                missing_required.push(table.clone());
            } else {
                missing_optional.push(table.clone());
            }
        }
        // The write check applies to every table, required or not: a role that
        // can write anything this touches is the wrong role.
        if row.try_get::<bool, _>("may_write")? {
            writable.push(table);
        }
    }

    anyhow::ensure!(
        missing_required.is_empty(),
        "role cannot SELECT the tables this compares: {}",
        missing_required.join(", ")
    );

    if !writable.is_empty() {
        anyhow::ensure!(
            allow_write_capable_role,
            "refusing to run: the connecting role can write {}. \
             Use the SELECT-only replay role.",
            writable.join(", ")
        );
        tracing::warn!(tables = ?writable, "running with a write-capable role by explicit opt-in");
    }

    Ok(missing_optional)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_owned()).collect()
    }

    #[test]
    fn identical_windows_are_identical() {
        assert_eq!(
            verdict_for(
                keys(&["movie/tmdb/1", "movie/tmdb/2"]),
                keys(&["movie/tmdb/1", "movie/tmdb/2"])
            ),
            Verdict::Identical
        );
    }

    /// The highest-signal outcome: the same rows came back in a different
    /// order. The identity tiebreak exists to make that impossible, so this
    /// cannot be explained away as the table having moved.
    #[test]
    fn the_same_rows_in_a_different_order_is_a_reorder_not_churn() {
        let verdict = verdict_for(
            keys(&["movie/tmdb/1", "movie/tmdb/2"]),
            keys(&["movie/tmdb/2", "movie/tmdb/1"]),
        );
        assert!(
            matches!(verdict, Verdict::ReorderedWindow { .. }),
            "got {verdict:?}"
        );
    }

    /// A row appearing or vanishing is ordinary churn in a table that grows
    /// continuously, so it must be reported as its own thing rather than as an
    /// ordering defect.
    #[test]
    fn a_changed_row_set_is_reported_separately() {
        let verdict = verdict_for(
            keys(&["movie/tmdb/1"]),
            keys(&["movie/tmdb/1", "movie/tmdb/3"]),
        );
        assert!(
            matches!(verdict, Verdict::MembershipChanged { .. }),
            "got {verdict:?}"
        );
    }

    /// An empty recorded answer is a real answer ("no such content"), so live
    /// returning nothing too is agreement, not an absence of evidence.
    #[test]
    fn two_empty_answers_agree() {
        assert_eq!(verdict_for(Vec::new(), Vec::new()), Verdict::Identical);
    }

    /// Go's recorded response shape, so the decode cannot drift silently.
    #[test]
    fn the_recorded_response_shape_decodes() {
        let decoded: RecordedResponse = serde_json::from_str(
            r#"{"items":[{"queryStringRank":"1","content":{"type":"movie","source":"tmdb","id":"77117","title":"Sunny"}}]}"#,
        )
        .expect("decodes");

        assert_eq!(decoded.items.len(), 1);
        assert_eq!(decoded.items[0].content.key(), "movie/tmdb/77117");
    }
}
