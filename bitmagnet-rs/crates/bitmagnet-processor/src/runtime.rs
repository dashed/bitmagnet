//! Fail-closed runtime boundary for the non-persisting ingest shadow.

use std::collections::BTreeSet;

use bitmagnet_classifier::{core_config_digest, SourceError};
use bitmagnet_common::metrics::registry;
use bitmagnet_queue::{
    DequeuedJob, MirrorIneligibleReason, MirrorReport, ProcessTorrentParams, ProtocolId,
    PROCESS_TORRENT_SHADOW,
};
use prometheus::{IntCounter, IntCounterVec, Opts};
use sqlx::PgPool;

use super::{
    compare_write_set, load::load_torrents_in, shadow::read_live_snapshot_in, CompareError,
    ComparisonVerdict, LoadError, MaterializeError, Materializer, ShadowComparison,
    ShadowReadError,
};
use crate::supported_subset::{has_explicit_attach_flags_off, has_explicit_default_workflow};

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

    /// Decode, hydrate, materialize, read the settled Go image, and compare.
    ///
    /// The runtime rejects ordinary default-flag jobs. Rust does not yet
    /// implement the four attach actions, while Go defaults their controlling
    /// flags to true; processing those jobs would produce known false drift.
    pub async fn process_job(
        &self,
        job: &DequeuedJob,
    ) -> Result<ShadowComparison, ShadowRuntimeError> {
        if job.queue != PROCESS_TORRENT_SHADOW {
            return Err(ShadowRuntimeError::WrongQueue(job.queue.clone()));
        }
        let params: ProcessTorrentParams = serde_json::from_str(&job.payload)?;
        require_default_workflow(&params)?;
        require_flags_off(&params)?;

        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let loaded = load_torrents_in(&mut tx, &params).await?;
        if let Some(torrent) = loaded
            .iter()
            .find(|torrent| torrent.attach_hint_unsupported)
        {
            return Err(ShadowRuntimeError::AttachHintUnsupported(
                torrent.info_hash.clone(),
            ));
        }
        let write_set = self.materializer.materialize(&params, loaded)?;
        let info_hashes = unique_hashes(&params.info_hashes);
        let live = read_live_snapshot_in(&mut tx, &info_hashes).await?;
        let comparison = compare_write_set(&write_set, &live)?;
        tx.commit().await?;
        Ok(comparison)
    }
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
    #[error("shadow runtime cannot reproduce the complete Go hint/enrichment path for {0}")]
    AttachHintUnsupported(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Load(#[from] LoadError),
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
            Self::ClassifierWorkflowUnsupported => Some("classifier_workflow"),
            Self::AttachFlagsNotExplicitlyDisabled => Some("attach_flags_enabled"),
            Self::AttachHintUnsupported(_) => Some("attach_hint"),
            Self::Load(LoadError::CompressedBlobTooLarge { .. }) => Some("compressed_blob_limit"),
            Self::Load(LoadError::JobBudgetExceeded { .. }) => Some("job_decode_budget"),
            Self::Load(LoadError::Blob(_)) => Some("invalid_or_oversized_file_blob"),
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
        registry().register(Box::new(results.clone()))?;
        registry().register(Box::new(drift.clone()))?;
        registry().register(Box::new(unsupported.clone()))?;
        Ok(Self {
            results,
            drift,
            unsupported,
        })
    }

    pub fn observe(&self, comparison: &ShadowComparison) {
        for torrent in &comparison.torrents {
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

    use super::{require_default_workflow, require_flags_off, ShadowRuntime, ShadowRuntimeError};

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
}
