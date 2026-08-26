//! Fail-closed runtime boundary for the non-persisting ingest shadow.

use std::collections::BTreeSet;

use bitmagnet_classifier::{core_config_digest, SourceError};
use bitmagnet_common::metrics::registry;
use bitmagnet_queue::{
    DequeuedJob, MirrorIneligibleReason, MirrorReport, ProcessTorrentParams, ProtocolId,
    ShadowJobEnvelopeV1, PROCESS_TORRENT, PROCESS_TORRENT_SHADOW, SHADOW_JOB_ENVELOPE_VERSION,
};
use prometheus::{IntCounter, IntCounterVec, Opts};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Row};

use super::{
    compare_write_set, shadow::read_live_snapshot_in, CompareError, ComparisonVerdict,
    MaterializeError, Materializer, ShadowComparison, ShadowReadError, WriterCompareError,
    WriterComparison, WriterDriftField, WriterLoadError, WriterPlanError,
};
use crate::supported_subset::{has_explicit_attach_flags_off, has_explicit_default_workflow};
use crate::writer_compare::compare_writer_persistence_in;
use crate::writer_load::load_writer_torrents_in;
use crate::writer_plan::compose_writer_plan;

/// Causally anchored stable and writer evidence from one database snapshot.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CausalShadowComparison {
    pub source_job_id: String,
    pub source_ran_at: String,
    pub stable: ShadowComparison,
    pub writer: WriterComparison,
}

impl CausalShadowComparison {
    /// True only when both comparison planes match.
    pub fn is_match(&self) -> bool {
        self.stable.is_match() && self.writer.is_match()
    }
}

/// A read-only processor for jobs from `process_torrent_shadow`.
pub struct ShadowRuntime {
    pool: PgPool,
    materializer: Materializer,
}

impl ShadowRuntime {
    /// Compile the embedded core only when it exactly matches the digest
    /// reported by the live Go processor's effective classifier config.
    pub fn from_core(
        pool: PgPool,
        expected_classifier_config_digest: Option<&str>,
    ) -> Result<Self, ShadowRuntimeError> {
        let expected = expected_classifier_config_digest
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or(ShadowRuntimeError::ClassifierConfigDigestMissing)?;
        let actual = core_config_digest()?;
        if expected != actual {
            return Err(ShadowRuntimeError::ClassifierConfigDigestMismatch {
                expected: expected.to_string(),
                actual,
            });
        }

        Ok(Self {
            pool,
            materializer: Materializer::from_core()?,
        })
    }

    /// Decode the exact-source envelope, prove its live causal boundary, compose
    /// the writer plan, and compare stable plus volatile writer evidence from
    /// one read-only repeatable-read snapshot.
    ///
    /// The runtime rejects ordinary default-flag jobs. Rust does not yet
    /// implement the four attach actions, while Go defaults their controlling
    /// flags to true; processing those jobs would produce known false drift.
    pub async fn process_job(
        &self,
        job: &DequeuedJob,
    ) -> Result<CausalShadowComparison, ShadowRuntimeError> {
        if job.queue != PROCESS_TORRENT_SHADOW {
            return Err(ShadowRuntimeError::WrongQueue(job.queue.clone()));
        }
        let envelope: ShadowJobEnvelopeV1 = serde_json::from_str(&job.payload)?;
        if envelope.schema_version != SHADOW_JOB_ENVELOPE_VERSION {
            return Err(ShadowRuntimeError::EnvelopeVersionUnsupported(
                envelope.schema_version,
            ));
        }
        let params: ProcessTorrentParams = serde_json::from_value(envelope.source_payload.clone())?;
        require_default_workflow(&params)?;
        require_flags_off(&params)?;
        if params.info_hashes.is_empty() {
            return Err(ShadowRuntimeError::NoInfoHashes);
        }

        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        validate_causal_source_in(&mut tx, &envelope, &params).await?;
        let loaded = load_writer_torrents_in(&mut tx, &params).await?;
        let writer_plan = compose_writer_plan(&self.materializer, &params, &loaded)?;
        let info_hashes = unique_hashes(&params.info_hashes);
        let live = read_live_snapshot_in(&mut tx, &info_hashes).await?;
        let stable = compare_write_set(writer_plan.write_set(), &live)?;
        let writer = compare_writer_persistence_in(&mut tx, writer_plan.persistence()).await?;
        tx.commit().await?;
        Ok(CausalShadowComparison {
            source_job_id: envelope.source_job_id,
            source_ran_at: envelope.source_ran_at,
            stable,
            writer,
        })
    }
}

async fn validate_causal_source_in(
    connection: &mut PgConnection,
    envelope: &ShadowJobEnvelopeV1,
    params: &ProcessTorrentParams,
) -> Result<(), ShadowRuntimeError> {
    let source_payload = serde_json::to_string(&envelope.source_payload)?;
    let source_is_exact: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
           SELECT 1 FROM queue_jobs AS source_job \
           WHERE source_job.id = $1 \
             AND source_job.queue = $2 \
             AND source_job.status = 'processed' \
             AND source_job.ran_at = $3::timestamptz \
             AND source_job.ran_at <= transaction_timestamp() \
             AND source_job.payload = $4::jsonb \
         )",
    )
    .bind(&envelope.source_job_id)
    .bind(PROCESS_TORRENT)
    .bind(&envelope.source_ran_at)
    .bind(&source_payload)
    .fetch_one(&mut *connection)
    .await?;
    if !source_is_exact {
        return Err(ShadowRuntimeError::SourceJobChangedOrMissing);
    }

    let info_hashes = unique_hashes(&params.info_hashes);
    let later_overlap: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
           SELECT 1 FROM queue_jobs AS later_job \
           WHERE later_job.queue = $1 \
             AND later_job.id <> $2 \
             AND (later_job.status IN ('pending', 'retry') \
                  OR later_job.ran_at >= $3::timestamptz) \
             AND EXISTS ( \
               SELECT 1 \
               FROM jsonb_array_elements( \
                 CASE WHEN jsonb_typeof(later_job.payload->'InfoHashes') = 'array' \
                      THEN later_job.payload->'InfoHashes' ELSE '[]'::jsonb END \
               ) AS requested(value) \
               WHERE jsonb_typeof(requested.value) = 'string' \
                 AND lower(requested.value #>> '{}') = ANY($4::text[]) \
             ) \
         )",
    )
    .bind(PROCESS_TORRENT)
    .bind(&envelope.source_job_id)
    .bind(&envelope.source_ran_at)
    .bind(&info_hashes)
    .fetch_one(&mut *connection)
    .await?;
    if later_overlap {
        return Err(ShadowRuntimeError::LaterOverlappingSourceAttempt);
    }

    let decoded = params
        .info_hashes
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let admission = sqlx::query(
        "SELECT \
           coalesce(bool_or(source_torrent.info_hash IS NULL), false) AS torrent_missing, \
           coalesce(bool_or(source_torrent.updated_at > $2::timestamptz), false) \
             AS torrent_updated_after_ran_at, \
           coalesce(bool_or(EXISTS ( \
             SELECT 1 FROM torrent_hints AS source_hint \
             WHERE source_hint.info_hash = source_torrent.info_hash \
           )), false) AS has_hint, \
           coalesce(bool_or(EXISTS ( \
             SELECT 1 FROM torrent_contents AS source_content \
             WHERE source_content.info_hash = source_torrent.info_hash \
               AND source_content.content_source IS NOT NULL \
           )), false) AS has_content_source, \
           coalesce(bool_or(EXISTS ( \
             SELECT 1 FROM torrents_torrent_sources AS source_row \
             WHERE source_row.info_hash = source_torrent.info_hash \
               AND source_row.updated_at > $2::timestamptz \
           )), false) AS source_updated_after_ran_at, \
           coalesce(bool_or(EXISTS ( \
             SELECT 1 FROM torrent_contents AS source_content \
             WHERE source_content.info_hash = source_torrent.info_hash \
               AND source_content.updated_at > $2::timestamptz \
           )), false) AS content_updated_after_ran_at, \
           coalesce(bool_or(EXISTS ( \
             SELECT 1 FROM torrent_tags AS source_tag \
             WHERE source_tag.info_hash = source_torrent.info_hash \
               AND source_tag.updated_at > $2::timestamptz \
           )), false) AS tag_updated_after_ran_at \
         FROM unnest($1::bytea[]) AS requested(info_hash) \
         LEFT JOIN torrents AS source_torrent \
           ON source_torrent.info_hash = requested.info_hash",
    )
    .bind(&decoded)
    .bind(&envelope.source_ran_at)
    .fetch_one(&mut *connection)
    .await?;
    if admission.try_get("torrent_missing")? {
        return Err(ShadowRuntimeError::SourceTorrentMissing);
    }
    if admission.try_get("torrent_updated_after_ran_at")? {
        return Err(ShadowRuntimeError::SourceTorrentUpdatedAfterRun);
    }
    if admission.try_get("has_hint")? {
        return Err(ShadowRuntimeError::SourceHintPresent);
    }
    if admission.try_get("has_content_source")? {
        return Err(ShadowRuntimeError::SourceBackedContentPresent);
    }
    if admission.try_get("source_updated_after_ran_at")? {
        return Err(ShadowRuntimeError::SourceRowsUpdatedAfterRun);
    }
    if admission.try_get("content_updated_after_ran_at")? {
        return Err(ShadowRuntimeError::TorrentContentUpdatedAfterRun);
    }
    if admission.try_get("tag_updated_after_ran_at")? {
        return Err(ShadowRuntimeError::TorrentTagUpdatedAfterRun);
    }
    Ok(())
}

fn unique_hashes(ids: &[ProtocolId]) -> Vec<String> {
    ids.iter()
        .map(|id| id.to_hex())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn require_flags_off(params: &ProcessTorrentParams) -> Result<(), ShadowRuntimeError> {
    if has_explicit_attach_flags_off(params) {
        Ok(())
    } else {
        Err(ShadowRuntimeError::AttachFlagsNotExplicitlyDisabled)
    }
}

fn require_default_workflow(params: &ProcessTorrentParams) -> Result<(), ShadowRuntimeError> {
    if has_explicit_default_workflow(params) {
        Ok(())
    } else {
        Err(ShadowRuntimeError::ClassifierWorkflowUnsupported)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShadowRuntimeError {
    #[error("shadow runtime refuses queue '{0}'")]
    WrongQueue(String),
    #[error("shadow runtime does not support scratch envelope version {0}")]
    EnvelopeVersionUnsupported(u8),
    #[error("shadow runtime requires at least one source info hash")]
    NoInfoHashes,
    #[error("shadow source job is missing or no longer matches its exact captured identity")]
    SourceJobChangedOrMissing,
    #[error("a nonterminal or later process_torrent attempt overlaps the captured source hashes")]
    LaterOverlappingSourceAttempt,
    #[error("a captured source torrent is no longer present")]
    SourceTorrentMissing,
    #[error("a captured source torrent changed after the source job settled")]
    SourceTorrentUpdatedAfterRun,
    #[error("a captured source torrent now has an explicit hint")]
    SourceHintPresent,
    #[error("a captured source torrent now has a source-backed content association")]
    SourceBackedContentPresent,
    #[error("a captured torrent source row changed after the source job settled")]
    SourceRowsUpdatedAfterRun,
    #[error("a captured torrent_content row changed after the source job settled")]
    TorrentContentUpdatedAfterRun,
    #[error("a captured torrent tag changed after the source job settled")]
    TorrentTagUpdatedAfterRun,
    #[error("shadow runtime requires the live Go effective classifier configuration digest")]
    ClassifierConfigDigestMissing,
    #[error(
        "shadow runtime classifier configuration mismatch: expected live Go digest {expected}, Rust embedded digest {actual}"
    )]
    ClassifierConfigDigestMismatch { expected: String, actual: String },
    #[error("shadow runtime requires ClassifierWorkflow to be explicitly 'default'")]
    ClassifierWorkflowUnsupported,
    #[error(
        "shadow runtime requires local_search_enabled, apis_enabled, and tmdb_enabled to be explicitly false"
    )]
    AttachFlagsNotExplicitlyDisabled,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    WriterLoad(#[from] WriterLoadError),
    #[error(transparent)]
    WriterPlan(#[from] WriterPlanError),
    #[error(transparent)]
    WriterCompare(#[from] WriterCompareError),
    #[error(transparent)]
    Materialize(#[from] MaterializeError),
    #[error(transparent)]
    Read(#[from] ShadowReadError),
    #[error(transparent)]
    Compare(#[from] CompareError),
    #[error(transparent)]
    ClassifierSource(#[from] SourceError),
}

impl ShadowRuntimeError {
    /// Permanent supported-subset exclusions should settle the scratch job
    /// successfully. Retrying them cannot change the captured payload or row
    /// shape and would only consume the bounded scratch backlog.
    #[must_use]
    pub fn unsupported_reason(&self) -> Option<&'static str> {
        match self {
            Self::EnvelopeVersionUnsupported(_) | Self::Json(_) => Some("invalid_envelope"),
            Self::NoInfoHashes => Some("no_infohashes"),
            Self::SourceJobChangedOrMissing => Some("source_job_changed_or_missing"),
            Self::LaterOverlappingSourceAttempt => Some("later_overlapping_attempt"),
            Self::SourceTorrentMissing => Some("torrent_missing"),
            Self::SourceTorrentUpdatedAfterRun => Some("torrent_updated_after_ran_at"),
            Self::SourceHintPresent => Some("has_hint"),
            Self::SourceBackedContentPresent => Some("has_content_source"),
            Self::SourceRowsUpdatedAfterRun => Some("source_updated_after_ran_at"),
            Self::TorrentContentUpdatedAfterRun => Some("content_updated_after_ran_at"),
            Self::TorrentTagUpdatedAfterRun => Some("tag_updated_after_ran_at"),
            Self::ClassifierWorkflowUnsupported => Some("classifier_workflow"),
            Self::AttachFlagsNotExplicitlyDisabled => Some("attach_flags_enabled"),
            Self::WriterLoad(WriterLoadError::Load(crate::LoadError::CompressedBlobTooLarge {
                ..
            })) => Some("compressed_blob_limit"),
            Self::WriterLoad(WriterLoadError::Load(crate::LoadError::JobBudgetExceeded {
                ..
            })) => Some("job_decode_budget"),
            Self::WriterLoad(WriterLoadError::Load(crate::LoadError::Blob(_))) => {
                Some("invalid_or_oversized_file_blob")
            }
            Self::WriterPlan(WriterPlanError::AttachHintUnsupported { .. }) => Some("attach_hint"),
            Self::WriterPlan(WriterPlanError::Materialize(
                crate::MaterializeError::AttachedContentUnsupported { .. },
            )) => Some("attached_content"),
            Self::WriterPlan(WriterPlanError::Materialize(
                crate::MaterializeError::UnsupportedFlag { .. },
            )) => Some("unsupported_classifier_flag"),
            Self::WriterCompare(WriterCompareError::TooManyRows { .. }) => {
                Some("writer_compare_row_limit")
            }
            Self::Materialize(MaterializeError::AttachedContentUnsupported { .. }) => {
                Some("attached_content")
            }
            Self::Materialize(MaterializeError::UnsupportedFlag { .. }) => {
                Some("unsupported_classifier_flag")
            }
            Self::Compare(CompareError::FailedHash(_)) => Some("no_comparable_write_outcome"),
            _ => None,
        }
    }
}

/// Bounded-cardinality Prometheus counters for shadow comparison outcomes.
#[derive(Clone)]
pub struct ShadowMetrics {
    results: IntCounterVec,
    drift: IntCounterVec,
    writer_results: IntCounterVec,
    writer_drift: IntCounterVec,
    unsupported: IntCounterVec,
}

impl ShadowMetrics {
    pub fn register() -> Result<Self, prometheus::Error> {
        let results = IntCounterVec::new(
            Opts::new(
                "bitmagnet_ingest_shadow_results_total",
                "Stable ingest-shadow comparison outcomes.",
            ),
            &["verdict", "content_type"],
        )?;
        let drift = IntCounterVec::new(
            Opts::new(
                "bitmagnet_ingest_shadow_field_drift_total",
                "Stable fields that differed during ingest-shadow comparison.",
            ),
            &["field", "content_type"],
        )?;
        let unsupported = IntCounterVec::new(
            Opts::new(
                "bitmagnet_ingest_shadow_unsupported_total",
                "Scratch jobs excluded from the explicitly supported comparison subset.",
            ),
            &["reason"],
        )?;
        let writer_results = IntCounterVec::new(
            Opts::new(
                "bitmagnet_ingest_shadow_writer_results_total",
                "Volatile writer-row ingest-shadow comparison outcomes.",
            ),
            &["verdict"],
        )?;
        let writer_drift = IntCounterVec::new(
            Opts::new(
                "bitmagnet_ingest_shadow_writer_field_drift_total",
                "Volatile writer fields that differed during ingest-shadow comparison.",
            ),
            &["field"],
        )?;
        registry().register(Box::new(results.clone()))?;
        registry().register(Box::new(drift.clone()))?;
        registry().register(Box::new(writer_results.clone()))?;
        registry().register(Box::new(writer_drift.clone()))?;
        registry().register(Box::new(unsupported.clone()))?;
        for verdict in ["match", "mismatch"] {
            writer_results.with_label_values(&[verdict]);
        }
        for field in WriterDriftField::ALL {
            writer_drift.with_label_values(&[field.as_str()]);
        }
        Ok(Self {
            results,
            drift,
            writer_results,
            writer_drift,
            unsupported,
        })
    }

    pub fn observe(&self, comparison: &CausalShadowComparison) {
        for torrent in &comparison.stable.torrents {
            let content_type = torrent.content_type.as_deref().unwrap_or("unclassified");
            let verdict = match torrent.verdict {
                ComparisonVerdict::Match => "match",
                ComparisonVerdict::Mismatch => "mismatch",
            };
            self.results
                .with_label_values(&[verdict, content_type])
                .inc();
            for field in &torrent.drift_fields {
                self.drift
                    .with_label_values(&[field.as_str(), content_type])
                    .inc();
            }
        }
        for row in &comparison.writer.rows {
            let verdict = match row.verdict {
                ComparisonVerdict::Match => "match",
                ComparisonVerdict::Mismatch => "mismatch",
            };
            self.writer_results.with_label_values(&[verdict]).inc();
            for field in &row.drift_fields {
                self.writer_drift.with_label_values(&[field.as_str()]).inc();
            }
        }
    }

    pub fn observe_unsupported(&self, reason: &str) {
        self.unsupported.with_label_values(&[reason]).inc();
    }
}

/// Bounded-cardinality Prometheus counters for the mirror's admission funnel.
///
/// The mirror is the pilot's first stage: it scans archived source jobs and
/// admits a sampled, eligible subset into the scratch queue. Without these
/// counters a mirror that scans steadily while admitting nothing is invisible
/// to Prometheus, because ineligible candidates are dropped in SQL and never
/// reach the consumer's `bitmagnet_ingest_shadow_unsupported_total`.
///
/// Every series here is created eagerly at startup, including one child per
/// [`MirrorIneligibleReason`]: a labelled child only materializes on its first
/// `with_label_values` call, and a starvation alert must be able to read a
/// present-and-zero series from the first scrape rather than waiting for an
/// absent series to appear.
#[derive(Clone)]
pub struct MirrorMetrics {
    pages: IntCounter,
    scanned: IntCounter,
    sampled: IntCounter,
    inserted: IntCounter,
    capped: IntCounter,
    ineligible: IntCounterVec,
}

impl MirrorMetrics {
    pub fn register() -> Result<Self, prometheus::Error> {
        let pages = IntCounter::new(
            "bitmagnet_ingest_shadow_mirror_pages_total",
            "Mirror page scans completed against the archived source queue.",
        )?;
        let scanned = IntCounter::new(
            "bitmagnet_ingest_shadow_mirror_scanned_total",
            "Archived source jobs examined by the mirror.",
        )?;
        let sampled = IntCounter::new(
            "bitmagnet_ingest_shadow_mirror_sampled_total",
            "Eligible source jobs that passed the deterministic sample gate.",
        )?;
        let inserted = IntCounter::new(
            "bitmagnet_ingest_shadow_mirror_inserted_total",
            "Sampled source jobs admitted into the scratch shadow queue.",
        )?;
        let capped = IntCounter::new(
            "bitmagnet_ingest_shadow_mirror_capped_total",
            "Mirror pages that stopped early on the scratch active-depth cap.",
        )?;
        let ineligible = IntCounterVec::new(
            Opts::new(
                "bitmagnet_ingest_shadow_mirror_ineligible_total",
                "Scanned source jobs refused by the mirror's supported-subset predicate.",
            ),
            &["reason"],
        )?;
        registry().register(Box::new(pages.clone()))?;
        registry().register(Box::new(scanned.clone()))?;
        registry().register(Box::new(sampled.clone()))?;
        registry().register(Box::new(inserted.clone()))?;
        registry().register(Box::new(capped.clone()))?;
        registry().register(Box::new(ineligible.clone()))?;
        for reason in MirrorIneligibleReason::ALL {
            ineligible.with_label_values(&[reason.as_str()]);
        }
        Ok(Self {
            pages,
            scanned,
            sampled,
            inserted,
            capped,
            ineligible,
        })
    }

    pub fn observe(&self, report: &MirrorReport) {
        self.pages.inc();
        self.scanned.inc_by(u64::from(report.scanned));
        self.sampled.inc_by(u64::from(report.sampled));
        self.inserted.inc_by(u64::from(report.inserted));
        if report.capped {
            self.capped.inc();
        }
        for (reason, count) in &report.ineligible {
            self.ineligible
                .with_label_values(&[reason.as_str()])
                .inc_by(u64::from(*count));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bitmagnet_queue::ProcessTorrentParams;
    use serde_json::json;

    use bitmagnet_classifier::core_config_digest;
    use sqlx::postgres::PgPoolOptions;

    use super::{
        require_default_workflow, require_flags_off, CausalShadowComparison, ShadowComparison,
        ShadowMetrics, ShadowRuntime, ShadowRuntimeError, WriterCompareError, WriterComparison,
        WriterDriftField,
    };

    #[tokio::test]
    async fn classifier_config_digest_is_required_and_must_match_before_consumption() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://shadow:shadow@127.0.0.1/shadow")
            .expect("create lazy test pool");

        assert!(matches!(
            ShadowRuntime::from_core(pool.clone(), None),
            Err(ShadowRuntimeError::ClassifierConfigDigestMissing)
        ));
        assert!(matches!(
            ShadowRuntime::from_core(pool.clone(), Some("")),
            Err(ShadowRuntimeError::ClassifierConfigDigestMissing)
        ));
        assert!(matches!(
            ShadowRuntime::from_core(pool.clone(), Some("sha256:not-the-live-digest")),
            Err(ShadowRuntimeError::ClassifierConfigDigestMismatch { .. })
        ));

        let digest = core_config_digest().expect("digest embedded classifier");
        ShadowRuntime::from_core(pool, Some(&digest))
            .expect("matching effective config digest permits startup");
    }

    #[test]
    fn causal_result_serializes_source_and_both_evidence_planes() {
        let result = CausalShadowComparison {
            source_job_id: "source-1".to_owned(),
            source_ran_at: "2026-08-26 12:34:56.123456+00".to_owned(),
            stable: ShadowComparison::default(),
            writer: WriterComparison::default(),
        };
        assert!(result.is_match());
        assert_eq!(
            serde_json::to_value(result).expect("serialize causal result"),
            serde_json::json!({
                "sourceJobId": "source-1",
                "sourceRanAt": "2026-08-26 12:34:56.123456+00",
                "stable": {"torrents": []},
                "writer": {"rows": []}
            })
        );
    }

    #[test]
    fn writer_metrics_are_eager_and_stable_metric_names_are_preserved() {
        let metrics = ShadowMetrics::register().expect("register shadow metrics once");
        metrics
            .results
            .with_label_values(&["match", "unclassified"]);
        metrics
            .drift
            .with_label_values(&["torrent_content.rows", "unclassified"]);

        let gathered = bitmagnet_common::metrics::gather_text();
        assert!(gathered.contains("bitmagnet_ingest_shadow_results_total"));
        assert!(gathered.contains("bitmagnet_ingest_shadow_field_drift_total"));
        for verdict in ["match", "mismatch"] {
            assert!(gathered.contains(&format!(
                "bitmagnet_ingest_shadow_writer_results_total{{verdict=\"{verdict}\"}} 0"
            )));
        }
        for field in WriterDriftField::ALL {
            assert!(gathered.contains(&format!(
                "bitmagnet_ingest_shadow_writer_field_drift_total{{field=\"{}\"}} 0",
                field.as_str()
            )));
        }
    }

    #[test]
    fn attachment_capability_gap_is_fail_closed() {
        let omitted = ProcessTorrentParams::default();
        assert!(matches!(
            require_flags_off(&omitted),
            Err(ShadowRuntimeError::AttachFlagsNotExplicitlyDisabled)
        ));

        let mut flags = BTreeMap::from([
            ("local_search_enabled".into(), json!(false)),
            ("apis_enabled".into(), json!(false)),
            ("tmdb_enabled".into(), json!(false)),
        ]);
        let safe = ProcessTorrentParams {
            classifier_workflow: "default".into(),
            classifier_flags: Some(flags.clone()),
            ..ProcessTorrentParams::default()
        };
        assert!(require_default_workflow(&safe).is_ok());
        assert!(require_flags_off(&safe).is_ok());

        flags.insert("tmdb_enabled".into(), json!(true));
        let unsafe_params = ProcessTorrentParams {
            classifier_flags: Some(flags),
            ..ProcessTorrentParams::default()
        };
        assert!(require_flags_off(&unsafe_params).is_err());
        assert_eq!(
            ShadowRuntimeError::AttachFlagsNotExplicitlyDisabled.unsupported_reason(),
            Some("attach_flags_enabled")
        );

        assert!(matches!(
            require_default_workflow(&ProcessTorrentParams::default()),
            Err(ShadowRuntimeError::ClassifierWorkflowUnsupported)
        ));
        assert_eq!(
            ShadowRuntimeError::ClassifierWorkflowUnsupported.unsupported_reason(),
            Some("classifier_workflow")
        );
    }

    #[test]
    fn causal_guard_failures_have_closed_metric_reasons() {
        let cases = [
            (
                ShadowRuntimeError::LaterOverlappingSourceAttempt,
                "later_overlapping_attempt",
            ),
            (
                ShadowRuntimeError::SourceRowsUpdatedAfterRun,
                "source_updated_after_ran_at",
            ),
            (
                ShadowRuntimeError::TorrentContentUpdatedAfterRun,
                "content_updated_after_ran_at",
            ),
            (
                ShadowRuntimeError::TorrentTagUpdatedAfterRun,
                "tag_updated_after_ran_at",
            ),
        ];
        for (error, reason) in cases {
            assert_eq!(error.unsupported_reason(), Some(reason));
        }
        assert_eq!(
            ShadowRuntimeError::WriterCompare(WriterCompareError::TooManyRows {
                actual: 101,
                limit: 100,
            })
            .unsupported_reason(),
            Some("writer_compare_row_limit")
        );
    }
}
