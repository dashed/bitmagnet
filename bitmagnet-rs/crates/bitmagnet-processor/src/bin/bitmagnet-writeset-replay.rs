//! Track E — offline write-set replay harness.
//!
//! The live ingest-shadow pilot proves write-set parity at the rate the crawler
//! happens to enqueue eligible work. This harness proves the same property
//! offline: it iterates *archived* `process_torrent` jobs, re-materializes the
//! Rust write-set in memory, and diffs it against the settled live rows with
//! the **same** comparison code the live shadow uses
//! ([`bitmagnet_processor::compare_write_set`]). It never writes anything, so
//! it needs neither the scratch queue nor the mirror.
//!
//! # Differences from the live shadow consumer (deliberate, documented)
//!
//! 1. **Per-hash outcomes, not whole-job all-or-nothing.** `compare_write_set`
//!    rejects an entire write-set when any hash lacks a comparable outcome
//!    (`compare.rs` `CompareError::FailedHash`), and `ShadowRuntime::process_job`
//!    rejects an entire job when any hash is attach-affected. For offline
//!    evidence we want maximum signal, so this harness projects the canonical
//!    job write-set onto **one hash at a time** and calls the unmodified
//!    `compare_write_set` per hash. A hash that cannot be compared is recorded
//!    as its own `unsupported` outcome and its clean siblings are still
//!    compared. The projection is proven equivalent to the whole-set comparison
//!    by `per_hash_split_matches_whole_set_comparison`.
//! 2. **Per-hash admission.** The mirror's admission predicate
//!    (`bitmagnet-queue/src/pg.rs`) is evaluated over a whole job with
//!    `NOT EXISTS (...)`; here the same four conditions are evaluated per hash
//!    so one attach-affected hash does not discard its 20 siblings.
//! 3. **Replay configuration is supplied, not required from the payload.**
//!    Production `process_torrent` payloads carry only `InfoHashes`; Go resolves
//!    the workflow and flags from deployment configuration. The harness replays
//!    every job under `workflow=default` + `flags-off`
//!    (`local_search_enabled`/`apis_enabled`/`tmdb_enabled` = false), which is
//!    the configuration the live Go processor effectively runs for every hash
//!    that is not attach-affected — and attach-affected hashes are excluded by
//!    admission. The embedded classifier configuration digest must still match
//!    the live Go `effective_config_digest`, exactly as the live consumer
//!    requires.
//!
//! # Read-only safety
//!
//! Writing is blocked at three independent layers:
//!
//! * **Session.** Every pooled connection is opened with
//!   `default_transaction_read_only = on`, so PostgreSQL itself rejects any
//!   `INSERT`/`UPDATE`/`DELETE` this process could ever issue.
//! * **Role.** Startup refuses to proceed if the connecting role holds
//!   `INSERT`/`UPDATE`/`DELETE` on any touched table. The intended role is
//!   `bitmagnet_ingest_shadow_consumer`, which migration 29 grants `SELECT`
//!   only (contract §5.4).
//! * **Code.** The harness issues `SELECT` only, and the reused loaders open
//!   explicit `REPEATABLE READ READ ONLY` transactions. No queue store is
//!   constructed, so no job can be claimed, enqueued, or settled.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use bitmagnet_classifier::core_config_digest;
use bitmagnet_processor::{
    compare_write_set, load_torrents, read_live_snapshot, ComparisonVerdict, LiveSnapshot,
    LoadError, LoadedTorrent, MaterializeError, Materializer, WriteSet,
};
use bitmagnet_queue::{ProcessTorrentParams, ProtocolId, PROCESS_TORRENT};
use clap::Parser;
use futures::StreamExt;
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

/// Tables the harness reads. Startup asserts `SELECT` on all of them and the
/// absence of any write privilege.
const TOUCHED_TABLES: [&str; 6] = [
    "torrents",
    "torrent_contents",
    "torrent_hints",
    "torrent_tags",
    "content",
    "queue_jobs",
];

/// Hard ceiling on concurrency; production PostgreSQL also serves the live
/// crawler and search.
const MAX_CONCURRENCY: usize = 8;

#[derive(Debug, Parser)]
#[command(
    name = "bitmagnet-writeset-replay",
    about = "Offline, read-only Go-vs-Rust write-set parity replay over archived process_torrent jobs."
)]
struct Args {
    #[arg(long, env = "BITMAGNET_POSTGRES_DSN")]
    postgres_dsn: String,
    /// `effective_config_digest` from the live Go processor's
    /// `classifier runner initialized` log line.
    #[arg(
        long,
        env = "BITMAGNET_INGEST_SHADOW_EXPECTED_CLASSIFIER_CONFIG_DIGEST"
    )]
    expected_classifier_config_digest: String,
    /// Maximum archived jobs to replay.
    #[arg(long, default_value_t = 50)]
    limit_jobs: u64,
    /// Archived jobs fetched per keyset page.
    #[arg(long, default_value_t = 25)]
    page_size: u32,
    /// Replay jobs whose `ran_at` is at or after this timestamp (any format
    /// PostgreSQL accepts for `timestamptz`).
    #[arg(long)]
    since: Option<String>,
    /// Replay jobs whose `ran_at` is strictly before this timestamp.
    #[arg(long)]
    until: Option<String>,
    /// Walk the archive oldest-first instead of newest-first.
    #[arg(long, default_value_t = false)]
    oldest_first: bool,
    /// Jobs replayed in parallel.
    #[arg(long, default_value_t = 2)]
    concurrency: usize,
    /// Upper bound on job starts per second across all workers.
    #[arg(long, default_value_t = 4.0)]
    max_jobs_per_second: f64,
    #[arg(long, default_value_t = 4)]
    max_connections: u32,
    /// Server-side `statement_timeout` for every connection.
    #[arg(long, default_value_t = 60)]
    statement_timeout_seconds: u64,
    /// Per-hash JSONL results. Defaults to stdout.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Aggregate summary JSON. Defaults to stderr.
    #[arg(long)]
    summary: Option<PathBuf>,
    /// Re-run the single-hash pipeline for every mismatch. A verdict that
    /// flips is labelled `unconfirmed_live_change` (contract §5.6: the live row
    /// is a moving target).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    recheck_mismatches: bool,
    /// Also re-run matches. A stability probe: it measures how often a verdict
    /// changes between two reads, and it exercises the same recheck path when a
    /// sample happens to contain no mismatch.
    #[arg(long, default_value_t = false)]
    recheck_matches: bool,
    /// Mismatch samples retained in the summary.
    #[arg(long, default_value_t = 25)]
    summary_examples: usize,
    /// Proceed even though the connecting role can write. Intended only for a
    /// disposable local database; never for production.
    #[arg(long, default_value_t = false)]
    allow_write_capable_role: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let args = Args::parse();
    validate_args(&args)?;

    let expected = args.expected_classifier_config_digest.trim().to_owned();
    let actual = core_config_digest().context("computing embedded classifier config digest")?;
    if expected != actual {
        bail!(
            "classifier configuration mismatch: expected live Go digest {expected}, \
             Rust embedded digest {actual}"
        );
    }
    tracing::info!(digest = %actual, "classifier configuration digest verified");

    let timeout_ms = args.statement_timeout_seconds.saturating_mul(1_000);
    let pool = PgPoolOptions::new()
        .max_connections(args.max_connections)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                // Layer 1 of the read-only guarantee: PostgreSQL rejects any
                // write this process could issue, whatever the code does.
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET application_name = 'bitmagnet-writeset-replay'")
                    .execute(&mut *conn)
                    .await?;
                // `set_config` keeps the value a bind parameter; sqlx 0.9
                // rejects dynamically formatted SQL strings outright.
                sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                    .bind(timeout_ms.to_string())
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET idle_in_transaction_session_timeout = 30000")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&args.postgres_dsn)
        .await
        .context("connecting to PostgreSQL")?;

    preflight(&pool, args.allow_write_capable_role).await?;

    let ctx = Arc::new(ReplayContext {
        pool,
        materializer: Materializer::from_core().context("compiling the embedded classifier")?,
        recheck_mismatches: args.recheck_mismatches,
        recheck_matches: args.recheck_matches,
        throttle: Throttle::new(args.max_jobs_per_second),
    });

    let started = Instant::now();
    let mut writer = Writer::open(args.output.as_deref()).await?;
    let mut summary = Summary::default();
    let mut cursor: Option<Cursor> = None;
    let mut remaining = args.limit_jobs;

    while remaining > 0 {
        let page = u32::try_from(remaining.min(u64::from(args.page_size)))
            .unwrap_or(args.page_size)
            .max(1);
        let jobs = fetch_jobs(&ctx.pool, &args, cursor.as_ref(), page).await?;
        if jobs.is_empty() {
            break;
        }
        cursor = jobs.last().map(ArchivedJob::cursor);
        remaining = remaining.saturating_sub(jobs.len() as u64);
        summary.jobs_scanned += jobs.len() as u64;

        let results = futures::stream::iter(jobs.into_iter().map(|job| {
            let ctx = Arc::clone(&ctx);
            async move { replay_job(&ctx, job).await }
        }))
        .buffer_unordered(args.concurrency)
        .collect::<Vec<_>>()
        .await;

        for result in results {
            let records = result?;
            summary.observe(&records, args.summary_examples);
            writer.write_all(&records).await?;
        }
        tracing::info!(
            jobs = summary.jobs_scanned,
            compared = summary.hashes_compared,
            matched = summary.matched,
            mismatched = summary.mismatched,
            "replay progress"
        );
    }

    writer.finish().await?;
    summary.elapsed_seconds = started.elapsed().as_secs_f64();
    summary.emit(args.summary.as_deref()).await?;
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    anyhow::ensure!(args.limit_jobs > 0, "limit-jobs must be positive");
    anyhow::ensure!(args.page_size > 0, "page-size must be positive");
    anyhow::ensure!(
        args.concurrency > 0 && args.concurrency <= MAX_CONCURRENCY,
        "concurrency must be between 1 and {MAX_CONCURRENCY}"
    );
    anyhow::ensure!(
        args.max_jobs_per_second > 0.0,
        "max-jobs-per-second must be positive"
    );
    anyhow::ensure!(
        args.max_connections as usize >= args.concurrency,
        "max-connections must be at least concurrency"
    );
    anyhow::ensure!(
        args.statement_timeout_seconds > 0,
        "statement-timeout-seconds must be positive"
    );
    anyhow::ensure!(
        !args.expected_classifier_config_digest.trim().is_empty(),
        "expected-classifier-config-digest must not be empty"
    );
    Ok(())
}

/// Layer 2 of the read-only guarantee: refuse a role that could write.
async fn preflight(pool: &PgPool, allow_write_capable_role: bool) -> Result<()> {
    let read_only: String = sqlx::query_scalar("SHOW default_transaction_read_only")
        .fetch_one(pool)
        .await
        .context("reading default_transaction_read_only")?;
    anyhow::ensure!(
        read_only == "on",
        "session is not read-only (default_transaction_read_only = {read_only})"
    );

    let identity = sqlx::query("SELECT current_user AS role, current_database() AS db")
        .fetch_one(pool)
        .await
        .context("reading session identity")?;
    let role: String = identity.try_get("role")?;
    let database: String = identity.try_get("db")?;

    let tables = TOUCHED_TABLES.map(str::to_owned).to_vec();
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

    let mut missing_select = Vec::new();
    let mut writable = Vec::new();
    for row in rows {
        let table: String = row.try_get("table_name")?;
        if !row.try_get::<bool, _>("may_select")? {
            missing_select.push(table.clone());
        }
        if row.try_get::<bool, _>("may_write")? {
            writable.push(table);
        }
    }
    anyhow::ensure!(
        missing_select.is_empty(),
        "role {role} lacks SELECT on: {}",
        missing_select.join(", ")
    );
    if !writable.is_empty() && !allow_write_capable_role {
        bail!(
            "role {role} holds write privileges on: {} — connect as the SELECT-only \
             ingest-shadow role (bitmagnet_ingest_shadow_consumer) or pass \
             --allow-write-capable-role for a disposable database",
            writable.join(", ")
        );
    }
    if !writable.is_empty() {
        tracing::warn!(
            %role,
            tables = %writable.join(", "),
            "running with a write-capable role; only the read-only session GUC is blocking writes"
        );
    }

    tracing::info!(%role, %database, "read-only preflight passed");
    Ok(())
}

struct ReplayContext {
    pool: PgPool,
    materializer: Materializer,
    recheck_mismatches: bool,
    recheck_matches: bool,
    throttle: Throttle,
}

/// A global lower bound on the interval between job starts.
struct Throttle {
    min_interval: Duration,
    next: Mutex<Option<Instant>>,
}

impl Throttle {
    fn new(jobs_per_second: f64) -> Self {
        Self {
            min_interval: Duration::from_secs_f64(1.0 / jobs_per_second),
            next: Mutex::new(None),
        }
    }

    async fn acquire(&self) {
        let sleep_for = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            let slot = next.map_or(now, |slot| slot.max(now));
            *next = Some(slot + self.min_interval);
            slot.saturating_duration_since(now)
        };
        if !sleep_for.is_zero() {
            tokio::time::sleep(sleep_for).await;
        }
    }
}

#[derive(Clone, Debug)]
struct ArchivedJob {
    id: String,
    ran_at: String,
    payload: String,
}

impl ArchivedJob {
    fn cursor(&self) -> Cursor {
        Cursor {
            ran_at: self.ran_at.clone(),
            id: self.id.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct Cursor {
    ran_at: String,
    id: String,
}

/// Keyset-paginate the archive so batches stay bounded and resumable.
async fn fetch_jobs(
    pool: &PgPool,
    args: &Args,
    cursor: Option<&Cursor>,
    limit: u32,
) -> Result<Vec<ArchivedJob>> {
    let sql = if args.oldest_first {
        "SELECT id, ran_at::text AS ran_at, payload::text AS payload \
         FROM queue_jobs \
         WHERE queue = $1 AND status = 'processed' AND ran_at IS NOT NULL \
           AND ($2::text IS NULL OR ran_at >= $2::timestamptz) \
           AND ($3::text IS NULL OR ran_at < $3::timestamptz) \
           AND ($4::text IS NULL OR (ran_at, id) > ($4::timestamptz, $5)) \
         ORDER BY ran_at, id LIMIT $6"
    } else {
        "SELECT id, ran_at::text AS ran_at, payload::text AS payload \
         FROM queue_jobs \
         WHERE queue = $1 AND status = 'processed' AND ran_at IS NOT NULL \
           AND ($2::text IS NULL OR ran_at >= $2::timestamptz) \
           AND ($3::text IS NULL OR ran_at < $3::timestamptz) \
           AND ($4::text IS NULL OR (ran_at, id) < ($4::timestamptz, $5)) \
         ORDER BY ran_at DESC, id DESC LIMIT $6"
    };
    let rows = sqlx::query(sql)
        .bind(PROCESS_TORRENT)
        .bind(args.since.as_deref())
        .bind(args.until.as_deref())
        .bind(cursor.map(|c| c.ran_at.as_str()))
        .bind(cursor.map_or("", |c| c.id.as_str()))
        .bind(i64::from(limit))
        .fetch_all(pool)
        .await
        .context("reading archived process_torrent jobs")?;

    rows.into_iter()
        .map(|row| {
            Ok(ArchivedJob {
                id: row.try_get("id")?,
                ran_at: row.try_get("ran_at")?,
                payload: row.try_get("payload")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Match,
    Mismatch,
    Unsupported,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HashRecord {
    job_id: String,
    ran_at: String,
    info_hash: String,
    outcome: Outcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    drift_fields: Vec<&'static str>,
    /// Stable, low-cardinality exclusion label. Values mirror
    /// `ShadowRuntimeError::unsupported_reason` so offline and live evidence
    /// share one vocabulary.
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recheck: Option<&'static str>,
}

impl HashRecord {
    fn unsupported(
        job: &ArchivedJob,
        info_hash: &str,
        reason: &'static str,
        detail: Option<String>,
    ) -> Self {
        Self {
            job_id: job.id.clone(),
            ran_at: job.ran_at.clone(),
            info_hash: info_hash.to_owned(),
            outcome: Outcome::Unsupported,
            content_type: None,
            drift_fields: Vec::new(),
            reason: Some(reason),
            detail,
            recheck: None,
        }
    }
}

/// A whole-group failure that must be attributed to individual hashes.
struct GroupFailure {
    reason: &'static str,
    detail: String,
}

impl GroupFailure {
    fn new(reason: &'static str, detail: impl ToString) -> Self {
        Self {
            reason,
            detail: detail.to_string(),
        }
    }
}

/// Mirrors `ShadowRuntimeError::unsupported_reason` for loader failures.
fn load_reason(error: &LoadError) -> &'static str {
    match error {
        LoadError::CompressedBlobTooLarge { .. } => "compressed_blob_limit",
        LoadError::JobBudgetExceeded { .. } => "job_decode_budget",
        LoadError::Blob(_) => "invalid_or_oversized_file_blob",
        LoadError::Database(_) | LoadError::NegativeInteger { .. } => "load_error",
    }
}

/// Mirrors `ShadowRuntimeError::unsupported_reason` for materializer failures.
fn materialize_reason(error: &MaterializeError) -> &'static str {
    match error {
        MaterializeError::AttachedContentUnsupported { .. } => "attached_content",
        MaterializeError::UnsupportedFlag { .. } => "unsupported_classifier_flag",
        MaterializeError::InvalidInfoHash(_) => "invalid_info_hash",
        _ => "materialize_error",
    }
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

async fn replay_job(ctx: &ReplayContext, job: ArchivedJob) -> Result<Vec<HashRecord>> {
    ctx.throttle.acquire().await;

    let params: ProcessTorrentParams = match serde_json::from_str(&job.payload) {
        Ok(params) => params,
        Err(error) => {
            return Ok(vec![HashRecord::unsupported(
                &job,
                "",
                "undecodable_payload",
                Some(error.to_string()),
            )])
        }
    };

    let requested = unique_hashes(&params.info_hashes);
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    // Job-level gates. The payload may pin a workflow or explicitly enable the
    // attach flags; either makes a flags-off replay a false comparison.
    if !params.classifier_workflow.is_empty() && params.classifier_workflow != "default" {
        return Ok(requested
            .iter()
            .map(|hash| HashRecord::unsupported(&job, hash, "classifier_workflow", None))
            .collect());
    }
    if let Some(flags) = params.classifier_flags.as_ref() {
        let enabled = ["local_search_enabled", "apis_enabled", "tmdb_enabled"]
            .iter()
            .any(|name| flags.get(*name).and_then(Value::as_bool) == Some(true));
        if enabled {
            return Ok(requested
                .iter()
                .map(|hash| HashRecord::unsupported(&job, hash, "attach_flags_enabled", None))
                .collect());
        }
    }

    let (eligible, mut records) = admit_hashes(ctx, &job, &requested).await?;
    if eligible.is_empty() {
        return Ok(records);
    }

    records.extend(compare_batch(ctx, &job, &eligible).await?);

    // A verdict is only trustworthy if it survives a second read: hydration and
    // the live-row read run in separate snapshots, and the live row is a moving
    // target (contract §5.6).
    for record in &mut records {
        let recheck = match record.outcome {
            Outcome::Mismatch => ctx.recheck_mismatches,
            Outcome::Match => ctx.recheck_matches,
            Outcome::Unsupported => false,
        };
        if !recheck {
            continue;
        }
        let hash = record.info_hash.clone();
        let again = compare_group(ctx, &job, std::slice::from_ref(&hash)).await;
        let stable = matches!(again, Ok(ref rerun)
            if rerun.first().map(|rerun| rerun.outcome) == Some(record.outcome));
        record.recheck = Some(if stable {
            "confirmed"
        } else {
            "unconfirmed_live_change"
        });
    }
    Ok(records)
}

/// Per-hash admission, mirroring the mirror's four job-level conditions
/// (`bitmagnet-queue/src/pg.rs`) one hash at a time.
async fn admit_hashes(
    ctx: &ReplayContext,
    job: &ArchivedJob,
    requested: &[String],
) -> Result<(Vec<String>, Vec<HashRecord>)> {
    let decoded = requested
        .iter()
        .filter_map(|hash| hex::decode(hash).ok())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "SELECT encode(h.ih, 'hex') AS info_hash, \
                (t.info_hash IS NULL) AS missing_torrent, \
                COALESCE(t.updated_at > $2::timestamptz, false) AS recrawled, \
                EXISTS (SELECT 1 FROM torrent_hints th WHERE th.info_hash = h.ih) AS explicit_hint, \
                EXISTS (SELECT 1 FROM torrent_contents tc \
                        WHERE tc.info_hash = h.ih AND tc.content_source IS NOT NULL) \
                  AS source_backed \
         FROM unnest($1::bytea[]) AS h(ih) \
         LEFT JOIN torrents t ON t.info_hash = h.ih",
    )
    .bind(&decoded)
    .bind(&job.ran_at)
    .fetch_all(&ctx.pool)
    .await
    .context("evaluating per-hash shadow admission")?;

    let mut eligible = Vec::new();
    let mut excluded = Vec::new();
    for row in rows {
        let info_hash: String = row.try_get("info_hash")?;
        let reason = if row.try_get::<bool, _>("missing_torrent")? {
            Some("source_row_missing")
        } else if row.try_get::<bool, _>("recrawled")? {
            Some("recrawled_after_job")
        } else if row.try_get::<bool, _>("explicit_hint")? {
            Some("explicit_hint")
        } else if row.try_get::<bool, _>("source_backed")? {
            Some("source_backed_association")
        } else {
            None
        };
        match reason {
            Some(reason) => excluded.push(HashRecord::unsupported(job, &info_hash, reason, None)),
            None => eligible.push(info_hash),
        }
    }
    Ok((eligible, excluded))
}

/// Compare a group of admitted hashes, degrading to single-hash groups when the
/// batch fails as a whole so one poisonous hash cannot hide its siblings.
async fn compare_batch(
    ctx: &ReplayContext,
    job: &ArchivedJob,
    hashes: &[String],
) -> Result<Vec<HashRecord>> {
    match compare_group(ctx, job, hashes).await {
        Ok(records) => Ok(records),
        Err(failure) if hashes.len() == 1 => Ok(vec![HashRecord::unsupported(
            job,
            &hashes[0],
            failure.reason,
            Some(failure.detail),
        )]),
        Err(failure) => {
            tracing::debug!(
                job_id = %job.id,
                reason = failure.reason,
                detail = %failure.detail,
                "batch replay failed; degrading to single-hash replays"
            );
            let mut records = Vec::with_capacity(hashes.len());
            for hash in hashes {
                match compare_group(ctx, job, std::slice::from_ref(hash)).await {
                    Ok(single) => records.extend(single),
                    Err(failure) => records.push(HashRecord::unsupported(
                        job,
                        hash,
                        failure.reason,
                        Some(failure.detail),
                    )),
                }
            }
            Ok(records)
        }
    }
}

/// Hydrate, materialize, read the settled live rows, and compare **per hash**.
async fn compare_group(
    ctx: &ReplayContext,
    job: &ArchivedJob,
    hashes: &[String],
) -> Result<Vec<HashRecord>, GroupFailure> {
    let mut params =
        replay_params(hashes).map_err(|error| GroupFailure::new("invalid_info_hash", error))?;

    let loaded = load_torrents(&ctx.pool, &params)
        .await
        .map_err(|error| GroupFailure::new(load_reason(&error), error))?;

    // The Go hint/enrichment surface Rust does not yet reproduce. Admission
    // already excludes these, so a hit here means the live row moved.
    let mut records = Vec::new();
    let (loaded, blocked): (Vec<LoadedTorrent>, Vec<LoadedTorrent>) = loaded
        .into_iter()
        .partition(|torrent| !torrent.attach_hint_unsupported);
    let blocked = blocked
        .into_iter()
        .map(|torrent| torrent.info_hash)
        .collect::<BTreeSet<_>>();
    for info_hash in &blocked {
        records.push(HashRecord::unsupported(job, info_hash, "attach_hint", None));
    }
    if !blocked.is_empty() {
        params
            .info_hashes
            .retain(|id| !blocked.contains(&id.to_hex()));
        if params.info_hashes.is_empty() {
            return Ok(records);
        }
    }

    let write_set = ctx
        .materializer
        .materialize(&params, loaded)
        .map_err(|error| GroupFailure::new(materialize_reason(&error), error))?;

    let comparable = unique_hashes(&params.info_hashes);
    let live = read_live_snapshot(&ctx.pool, &comparable)
        .await
        .map_err(|error| GroupFailure::new("live_read_error", error))?;

    let failed = write_set
        .failed_info_hashes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for info_hash in comparable {
        if failed.contains(&info_hash) {
            // Go republishes these; there is no would-be persisted image.
            records.push(HashRecord::unsupported(
                job,
                &info_hash,
                "no_comparable_write_outcome",
                None,
            ));
            continue;
        }
        let Some(state) = live.get(&info_hash) else {
            records.push(HashRecord::unsupported(
                job,
                &info_hash,
                "live_read_error",
                None,
            ));
            continue;
        };
        let single_live = LiveSnapshot::from([(info_hash.clone(), state.clone())]);
        match compare_write_set(&project_hash(&write_set, &info_hash), &single_live) {
            Ok(comparison) => {
                let Some(torrent) = comparison.torrents.into_iter().next() else {
                    records.push(HashRecord::unsupported(
                        job,
                        &info_hash,
                        "compare_error",
                        None,
                    ));
                    continue;
                };
                records.push(HashRecord {
                    job_id: job.id.clone(),
                    ran_at: job.ran_at.clone(),
                    info_hash,
                    outcome: match torrent.verdict {
                        ComparisonVerdict::Match => Outcome::Match,
                        ComparisonVerdict::Mismatch => Outcome::Mismatch,
                    },
                    content_type: torrent.content_type,
                    drift_fields: torrent
                        .drift_fields
                        .into_iter()
                        .map(bitmagnet_processor::DriftField::as_str)
                        .collect(),
                    reason: None,
                    detail: None,
                    recheck: None,
                });
            }
            Err(error) => records.push(HashRecord::unsupported(
                job,
                &info_hash,
                "compare_error",
                Some(error.to_string()),
            )),
        }
    }
    Ok(records)
}

/// The replay configuration: `default` workflow with the attach flags pinned
/// off, matching `Classifier::flags_off` and the live shadow's supported subset.
fn replay_params(hashes: &[String]) -> Result<ProcessTorrentParams, hex::FromHexError> {
    let info_hashes = hashes
        .iter()
        .map(|hash| ProtocolId::from_hex(hash))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ProcessTorrentParams {
        classifier_workflow: "default".to_owned(),
        classifier_flags: Some(BTreeMap::from([
            ("local_search_enabled".to_owned(), json!(false)),
            ("apis_enabled".to_owned(), json!(false)),
            ("tmdb_enabled".to_owned(), json!(false)),
        ])),
        info_hashes,
        ..ProcessTorrentParams::default()
    })
}

/// Project a canonical job write-set onto exactly one info hash.
///
/// `delete_ids` is intentionally dropped: `compare_write_set` never reads it
/// (stale `torrent_contents` deletes are already reflected in the settled live
/// rows it compares against), and the field is not keyed by info hash.
fn project_hash(write_set: &WriteSet, info_hash: &str) -> WriteSet {
    let torrent_contents = write_set
        .torrent_contents
        .iter()
        .filter(|row| row.info_hash == info_hash)
        .cloned()
        .collect::<Vec<_>>();
    let referenced = torrent_contents
        .iter()
        .filter_map(|row| {
            Some((
                row.content_type.clone()?,
                row.content_source.clone()?,
                row.content_id.clone()?,
            ))
        })
        .collect::<BTreeSet<_>>();
    WriteSet {
        contents: write_set
            .contents
            .iter()
            .filter(|content| {
                referenced.contains(&(
                    content.content_type.clone(),
                    content.source.clone(),
                    content.id.clone(),
                ))
            })
            .cloned()
            .collect(),
        torrent_contents,
        delete_ids: Vec::new(),
        delete_info_hashes: write_set
            .delete_info_hashes
            .iter()
            .filter(|hash| hash.as_str() == info_hash)
            .cloned()
            .collect(),
        add_tags: write_set
            .add_tags
            .get(info_hash)
            .map(|tags| BTreeMap::from([(info_hash.to_owned(), tags.clone())]))
            .unwrap_or_default(),
        failed_info_hashes: Vec::new(),
    }
}

fn unique_hashes(ids: &[ProtocolId]) -> Vec<String> {
    ids.iter()
        .map(|id| id.to_hex())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

enum Sink {
    Stdout(BufWriter<tokio::io::Stdout>),
    File(BufWriter<tokio::fs::File>),
}

struct Writer {
    sink: Sink,
}

impl Writer {
    async fn open(path: Option<&std::path::Path>) -> Result<Self> {
        let sink = match path {
            Some(path) => Sink::File(BufWriter::new(
                tokio::fs::File::create(path)
                    .await
                    .with_context(|| format!("creating {}", path.display()))?,
            )),
            None => Sink::Stdout(BufWriter::new(tokio::io::stdout())),
        };
        Ok(Self { sink })
    }

    async fn write_all(&mut self, records: &[HashRecord]) -> Result<()> {
        let mut buffer = String::new();
        for record in records {
            buffer.push_str(&serde_json::to_string(record)?);
            buffer.push('\n');
        }
        match &mut self.sink {
            Sink::Stdout(writer) => writer.write_all(buffer.as_bytes()).await?,
            Sink::File(writer) => writer.write_all(buffer.as_bytes()).await?,
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        match &mut self.sink {
            Sink::Stdout(writer) => writer.flush().await?,
            Sink::File(writer) => writer.flush().await?,
        }
        Ok(())
    }
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    jobs_scanned: u64,
    hashes_seen: u64,
    hashes_compared: u64,
    matched: u64,
    mismatched: u64,
    /// Mismatches whose verdict survived a single-hash re-run.
    mismatched_confirmed: u64,
    /// Mismatches that flipped on re-run — the live row moved (contract §5.6).
    mismatched_unconfirmed: u64,
    unsupported: u64,
    match_rate: f64,
    unsupported_reasons: BTreeMap<String, u64>,
    drift_by_field: BTreeMap<String, u64>,
    compared_by_content_type: BTreeMap<String, u64>,
    mismatched_by_content_type: BTreeMap<String, u64>,
    drift_by_field_and_content_type: BTreeMap<String, u64>,
    elapsed_seconds: f64,
    mismatch_examples: Vec<HashRecord>,
}

impl Summary {
    fn observe(&mut self, records: &[HashRecord], examples: usize) {
        for record in records {
            self.hashes_seen += 1;
            match record.outcome {
                Outcome::Unsupported => {
                    self.unsupported += 1;
                    *self
                        .unsupported_reasons
                        .entry(record.reason.unwrap_or("unknown").to_owned())
                        .or_default() += 1;
                }
                Outcome::Match | Outcome::Mismatch => {
                    self.hashes_compared += 1;
                    let content_type = record
                        .content_type
                        .clone()
                        .unwrap_or_else(|| "unclassified".to_owned());
                    *self
                        .compared_by_content_type
                        .entry(content_type.clone())
                        .or_default() += 1;
                    if record.outcome == Outcome::Match {
                        self.matched += 1;
                    } else {
                        self.mismatched += 1;
                        match record.recheck {
                            Some("unconfirmed_live_change") => self.mismatched_unconfirmed += 1,
                            Some("confirmed") => self.mismatched_confirmed += 1,
                            _ => {}
                        }
                        *self
                            .mismatched_by_content_type
                            .entry(content_type.clone())
                            .or_default() += 1;
                        for field in &record.drift_fields {
                            *self.drift_by_field.entry((*field).to_owned()).or_default() += 1;
                            *self
                                .drift_by_field_and_content_type
                                .entry(format!("{field}|{content_type}"))
                                .or_default() += 1;
                        }
                        if self.mismatch_examples.len() < examples {
                            self.mismatch_examples.push(record.clone());
                        }
                    }
                }
            }
        }
        self.match_rate = if self.hashes_compared == 0 {
            0.0
        } else {
            self.matched as f64 / self.hashes_compared as f64
        };
    }

    async fn emit(&self, path: Option<&std::path::Path>) -> Result<()> {
        let rendered = serde_json::to_string_pretty(self)?;
        match path {
            Some(path) => tokio::fs::write(path, rendered)
                .await
                .with_context(|| format!("writing {}", path.display()))?,
            None => {
                let mut stderr = tokio::io::stderr();
                stderr.write_all(rendered.as_bytes()).await?;
                stderr.write_all(b"\n").await?;
                stderr.flush().await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bitmagnet_processor::{
        compare_write_set, ComparisonVerdict, LiveSnapshot, LiveTorrentSnapshot, LiveTorrentState,
        TorrentContentWrite, WriteSet,
    };

    use super::{load_reason, materialize_reason, project_hash, replay_params, Throttle};
    use bitmagnet_processor::{LoadError, MaterializeError};

    const HASH_A: &str = "1111111111111111111111111111111111111111";
    const HASH_B: &str = "2222222222222222222222222222222222222222";
    const HASH_C: &str = "3333333333333333333333333333333333333333";

    fn torrent_content(info_hash: &str, size: u64) -> TorrentContentWrite {
        TorrentContentWrite {
            id: format!("{info_hash}:movie:?:?"),
            info_hash: info_hash.to_owned(),
            content_type: Some("movie".into()),
            content_source: None,
            content_id: None,
            languages: vec!["en".into()],
            episodes: String::new(),
            video_resolution: Some("1080p".into()),
            video_source: Some("web".into()),
            video_codec: Some("h264".into()),
            video_3d: None,
            video_modifier: None,
            release_group: Some("group".into()),
            size,
            files_count: Some(1),
        }
    }

    fn live_present(row: TorrentContentWrite, tags: Vec<String>) -> LiveTorrentState {
        LiveTorrentState::Present(LiveTorrentSnapshot {
            contents: Vec::new(),
            torrent_contents: vec![row],
            tags,
        })
    }

    /// The per-hash projection must not change any verdict: for a write-set with
    /// no failed hashes, splitting and comparing hash-by-hash reproduces exactly
    /// what the whole-set comparison reports.
    #[test]
    fn per_hash_split_matches_whole_set_comparison() {
        let write_set = WriteSet {
            torrent_contents: vec![torrent_content(HASH_A, 10), torrent_content(HASH_B, 20)],
            delete_info_hashes: vec![HASH_C.to_owned()],
            delete_ids: vec!["stale".to_owned()],
            add_tags: BTreeMap::from([(HASH_A.to_owned(), vec!["action".to_owned()])]),
            ..WriteSet::default()
        };
        // HASH_B drifts on size; HASH_A and HASH_C match.
        let live = LiveSnapshot::from([
            (
                HASH_A.to_owned(),
                live_present(torrent_content(HASH_A, 10), vec!["action".into()]),
            ),
            (
                HASH_B.to_owned(),
                live_present(torrent_content(HASH_B, 99), Vec::new()),
            ),
            (HASH_C.to_owned(), LiveTorrentState::LiveAbsent),
        ]);

        let whole = compare_write_set(&write_set, &live).expect("whole-set comparison");
        let mut split = Vec::new();
        for (info_hash, state) in &live {
            let single = LiveSnapshot::from([(info_hash.clone(), state.clone())]);
            let comparison = compare_write_set(&project_hash(&write_set, info_hash), &single)
                .expect("per-hash comparison");
            split.extend(comparison.torrents);
        }
        assert_eq!(whole.torrents, split);
        assert_eq!(whole.match_count(), 2);
        assert_eq!(whole.mismatch_count(), 1);
    }

    /// The documented behaviour change: one hash without a comparable outcome
    /// poisons the whole job for `compare_write_set`, but the harness records it
    /// as its own outcome and still compares its siblings.
    #[test]
    fn failed_hash_does_not_poison_its_siblings() {
        let write_set = WriteSet {
            torrent_contents: vec![torrent_content(HASH_A, 10)],
            failed_info_hashes: vec![HASH_B.to_owned()],
            ..WriteSet::default()
        };
        let live = LiveSnapshot::from([(
            HASH_A.to_owned(),
            live_present(torrent_content(HASH_A, 10), Vec::new()),
        )]);

        // Whole-set: rejected outright.
        assert!(compare_write_set(&write_set, &live).is_err());

        // Per-hash: HASH_B is excluded, HASH_A still yields a verdict.
        let comparison = compare_write_set(&project_hash(&write_set, HASH_A), &live)
            .expect("sibling comparison survives");
        assert_eq!(comparison.torrents.len(), 1);
        assert_eq!(comparison.torrents[0].verdict, ComparisonVerdict::Match);
    }

    #[test]
    fn replay_params_pin_the_flags_off_supported_subset() {
        let params = replay_params(&[HASH_A.to_owned()]).expect("valid hash");
        assert_eq!(params.classifier_workflow, "default");
        let flags = params.classifier_flags.expect("flags are always explicit");
        for name in ["local_search_enabled", "apis_enabled", "tmdb_enabled"] {
            assert_eq!(
                flags.get(name).and_then(serde_json::Value::as_bool),
                Some(false)
            );
        }
        assert!(replay_params(&["not-a-hash".to_owned()]).is_err());
    }

    #[test]
    fn unsupported_reasons_match_the_live_shadow_vocabulary() {
        assert_eq!(
            load_reason(&LoadError::CompressedBlobTooLarge { bytes: 1, limit: 0 }),
            "compressed_blob_limit"
        );
        assert_eq!(
            load_reason(&LoadError::JobBudgetExceeded {
                resource: "files",
                actual: 2,
                limit: 1
            }),
            "job_decode_budget"
        );
        assert_eq!(
            materialize_reason(&MaterializeError::AttachedContentUnsupported {
                info_hash: HASH_A.to_owned()
            }),
            "attached_content"
        );
    }

    #[tokio::test]
    async fn throttle_spaces_out_job_starts() {
        let throttle = Throttle::new(50.0);
        let started = std::time::Instant::now();
        for _ in 0..4 {
            throttle.acquire().await;
        }
        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
    }
}
