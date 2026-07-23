//! Fail-closed runtime boundary for the non-persisting ingest shadow.

use std::collections::BTreeSet;

use bitmagnet_common::metrics::registry;
use bitmagnet_queue::{DequeuedJob, ProcessTorrentParams, ProtocolId, PROCESS_TORRENT_SHADOW};
use prometheus::{IntCounterVec, Opts};
use sqlx::PgPool;

use super::{
    compare_write_set, load::load_torrents_in, shadow::read_live_snapshot_in, CompareError,
    ComparisonVerdict, LoadError, MaterializeError, Materializer, ShadowComparison,
    ShadowReadError,
};

const REQUIRED_FALSE_FLAGS: [&str; 3] = ["local_search_enabled", "apis_enabled", "tmdb_enabled"];

/// A read-only processor for jobs from `process_torrent_shadow`.
pub struct ShadowRuntime {
    pool: PgPool,
    materializer: Materializer,
}

impl ShadowRuntime {
    pub fn from_core(pool: PgPool) -> Result<Self, ShadowRuntimeError> {
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
    let flags = params
        .classifier_flags
        .as_ref()
        .ok_or(ShadowRuntimeError::AttachFlagsNotExplicitlyDisabled)?;
    if REQUIRED_FALSE_FLAGS
        .iter()
        .all(|name| flags.get(*name).and_then(serde_json::Value::as_bool) == Some(false))
    {
        Ok(())
    } else {
        Err(ShadowRuntimeError::AttachFlagsNotExplicitlyDisabled)
    }
}

fn require_default_workflow(params: &ProcessTorrentParams) -> Result<(), ShadowRuntimeError> {
    if params.classifier_workflow == "default" {
        Ok(())
    } else {
        Err(ShadowRuntimeError::ClassifierWorkflowUnsupported)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShadowRuntimeError {
    #[error("shadow runtime refuses queue '{0}'")]
    WrongQueue(String),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bitmagnet_queue::ProcessTorrentParams;
    use serde_json::json;

    use super::{require_default_workflow, require_flags_off, ShadowRuntimeError};

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
