//! Composition of the supported processor writer intent.
//!
//! The loader, classifier materializer, volatile-field projection, and
//! transaction kernel deliberately landed as separate parity gates. This
//! module joins the first three into the exact input image expected by
//! [`crate::persist_write_set`] without calling that function. The
//! ingest-shadow runtime consumes the plan for stable comparison only; it does
//! not wire a live database writer or blocking-manager lifecycle.

use std::collections::BTreeMap;

use bitmagnet_queue::ProcessTorrentParams;
use sqlx::PgPool;

use crate::persist::validate_persistence_input;
use crate::supported_subset::{has_explicit_attach_flags_off, has_explicit_default_workflow};
use crate::{
    load_writer_torrents, project_unattached_persistence, MaterializeError, Materializer,
    PersistError, TorrentContentPersistence, WriteSet, WriterLoadError, WriterLoadedTorrent,
    WriterProjectionError,
};

/// A fully materialized supported-subset writer intent.
///
/// `persistence` is keyed by generated `torrent_contents.id` and has exactly
/// one entry for every row in `write_set.torrent_contents`. The fields are
/// private so callers cannot break that keyset after validation. Holding this
/// value performs no database mutation.
///
/// Failed hashes remain in the write-set as retry intent. Matching Go requires
/// any persisting runtime to enqueue that retry successfully before persisting
/// the successful portion of the same plan.
#[derive(Debug, PartialEq, Eq)]
pub struct WriterPlan {
    write_set: WriteSet,
    persistence: BTreeMap<String, TorrentContentPersistence>,
}

impl WriterPlan {
    /// Borrow the canonical classification-derived and retry intent.
    #[must_use]
    pub fn write_set(&self) -> &WriteSet {
        &self.write_set
    }

    /// Borrow the exact volatile metadata keyed by `torrent_contents.id`.
    #[must_use]
    pub fn persistence(&self) -> &BTreeMap<String, TorrentContentPersistence> {
        &self.persistence
    }

    /// Hashes that a persisting writer must republish before persisting successes.
    #[must_use]
    pub fn retry_info_hashes(&self) -> &[String] {
        &self.write_set.failed_info_hashes
    }

    /// Consume the validated plan into the transaction kernel's two inputs.
    #[must_use]
    pub fn into_parts(self) -> (WriteSet, BTreeMap<String, TorrentContentPersistence>) {
        (self.write_set, self.persistence)
    }
}

/// Read one stable source image and compose its non-persisting writer intent.
///
/// The database work is limited to [`load_writer_torrents`]' read-only,
/// repeatable-read transaction. The returned plan is not persisted.
pub async fn load_writer_plan(
    pool: &PgPool,
    materializer: &Materializer,
    params: &ProcessTorrentParams,
) -> Result<WriterPlan, WriterPlanError> {
    validate_supported_params(params)?;
    let loaded = load_writer_torrents(pool, params).await?;
    compose_writer_plan(materializer, params, &loaded)
}

/// Compose the classifier write-set and its volatile persistence projection.
///
/// Hydrated classifier inputs are borrowed rather than cloned. This matters
/// because the loader permits bounded but potentially large decompressed file
/// lists. Attached-content rows fail closed in the projection until the
/// structured content TSV contract is implemented. Direct callers must supply
/// values already admitted by the bounded loader; this pure composer does not
/// reconstruct compressed-size or source-scan evidence from in-memory values.
pub fn compose_writer_plan(
    materializer: &Materializer,
    params: &ProcessTorrentParams,
    loaded: &[WriterLoadedTorrent],
) -> Result<WriterPlan, WriterPlanError> {
    validate_supported_params(params)?;
    let indexed = index_loaded_torrents(loaded)?;
    reject_unrequested_loaded_torrents(params, &indexed)?;
    let write_set =
        materializer.materialize_borrowed(params, loaded.iter().map(|torrent| &torrent.loaded))?;
    let persistence = project_persistence(&write_set, &indexed)?;
    validate_persistence_input(&write_set, &persistence)?;
    Ok(WriterPlan {
        write_set,
        persistence,
    })
}

fn reject_unrequested_loaded_torrents(
    params: &ProcessTorrentParams,
    indexed: &BTreeMap<&str, &WriterLoadedTorrent>,
) -> Result<(), WriterPlanError> {
    let requested = params
        .info_hashes
        .iter()
        .map(|info_hash| info_hash.to_hex())
        .collect::<std::collections::BTreeSet<_>>();
    if let Some(info_hash) = indexed
        .keys()
        .find(|info_hash| !requested.contains(**info_hash))
    {
        Err(WriterPlanError::UnrequestedLoadedTorrent {
            info_hash: (*info_hash).to_owned(),
        })
    } else {
        Ok(())
    }
}

fn validate_supported_params(params: &ProcessTorrentParams) -> Result<(), WriterPlanError> {
    if !has_explicit_default_workflow(params) {
        return Err(WriterPlanError::ClassifierWorkflowUnsupported);
    }
    if has_explicit_attach_flags_off(params) {
        Ok(())
    } else {
        Err(WriterPlanError::AttachFlagsNotExplicitlyDisabled)
    }
}

fn index_loaded_torrents(
    loaded: &[WriterLoadedTorrent],
) -> Result<BTreeMap<&str, &WriterLoadedTorrent>, WriterPlanError> {
    let mut indexed = BTreeMap::new();
    for torrent in loaded {
        if torrent.loaded.attach_hint_unsupported {
            return Err(WriterPlanError::AttachHintUnsupported {
                info_hash: torrent.loaded.info_hash.clone(),
            });
        }
        if torrent.loaded.info_hash != torrent.loaded.classifier_input.id {
            return Err(WriterPlanError::LoadedInfoHashMismatch {
                loaded_info_hash: torrent.loaded.info_hash.clone(),
                classifier_info_hash: torrent.loaded.classifier_input.id.clone(),
            });
        }
        if indexed
            .insert(torrent.loaded.info_hash.as_str(), torrent)
            .is_some()
        {
            return Err(WriterPlanError::DuplicateLoadedTorrent {
                info_hash: torrent.loaded.info_hash.clone(),
            });
        }
    }
    Ok(indexed)
}

fn project_persistence(
    write_set: &WriteSet,
    indexed: &BTreeMap<&str, &WriterLoadedTorrent>,
) -> Result<BTreeMap<String, TorrentContentPersistence>, WriterPlanError> {
    let mut persistence = BTreeMap::new();
    for row in &write_set.torrent_contents {
        let source = indexed.get(row.info_hash.as_str()).ok_or_else(|| {
            WriterPlanError::ProjectionSourceMissing {
                info_hash: row.info_hash.clone(),
            }
        })?;
        let projected = project_unattached_persistence(
            row,
            &source.loaded.classifier_input,
            source.torrent_snapshot,
            &source.source_snapshots,
        )
        .map_err(|source| WriterPlanError::Projection {
            info_hash: row.info_hash.clone(),
            source,
        })?;
        if persistence.insert(row.id.clone(), projected).is_some() {
            return Err(WriterPlanError::DuplicatePersistenceId { id: row.id.clone() });
        }
    }
    Ok(persistence)
}

/// Fail-closed errors from writer-plan composition.
#[derive(Debug, thiserror::Error)]
pub enum WriterPlanError {
    #[error(transparent)]
    Load(#[from] WriterLoadError),
    #[error(transparent)]
    Materialize(#[from] MaterializeError),
    #[error(transparent)]
    Persistence(#[from] PersistError),
    #[error("writer plan requires ClassifierWorkflow to be explicitly 'default'")]
    ClassifierWorkflowUnsupported,
    #[error(
        "writer plan requires local_search_enabled, apis_enabled, and tmdb_enabled to be explicitly false"
    )]
    AttachFlagsNotExplicitlyDisabled,
    #[error("writer plan cannot reproduce the complete Go hint/enrichment path for {info_hash}")]
    AttachHintUnsupported { info_hash: String },
    #[error(
        "loaded torrent key {loaded_info_hash} does not match classifier key {classifier_info_hash}"
    )]
    LoadedInfoHashMismatch {
        loaded_info_hash: String,
        classifier_info_hash: String,
    },
    #[error("writer plan contains duplicate loaded torrent {info_hash}")]
    DuplicateLoadedTorrent { info_hash: String },
    #[error("writer plan contains unrequested loaded torrent {info_hash}")]
    UnrequestedLoadedTorrent { info_hash: String },
    #[error("writer plan has no projection source for materialized torrent {info_hash}")]
    ProjectionSourceMissing { info_hash: String },
    #[error("writer projection failed for {info_hash}")]
    Projection {
        info_hash: String,
        #[source]
        source: WriterProjectionError,
    },
    #[error("writer plan produced duplicate persistence metadata ID {id}")]
    DuplicatePersistenceId { id: String },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bitmagnet_classifier::ClassifierInput;
    use bitmagnet_queue::ProcessTorrentParams;
    use serde_json::Value;

    use super::{
        compose_writer_plan, index_loaded_torrents, project_persistence, validate_supported_params,
        WriterPlanError,
    };
    use crate::{
        LoadedTorrent, TorrentContentWrite, TorrentSnapshot, WriteSet, WriterLoadedTorrent,
        WriterProjectionError,
    };

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn loaded(info_hash: &str) -> WriterLoadedTorrent {
        WriterLoadedTorrent {
            loaded: LoadedTorrent {
                info_hash: info_hash.to_owned(),
                classifier_input: ClassifierInput {
                    id: info_hash.to_owned(),
                    name: "writer plan fixture".to_owned(),
                    size: 1,
                    files_status: "no_info".to_owned(),
                    extension: None,
                    files_count: None,
                    files: Vec::new(),
                    hint: None,
                    contents: Vec::new(),
                },
                existing_content_ids: Vec::new(),
                attach_hint_unsupported: false,
            },
            torrent_snapshot: TorrentSnapshot {
                created_at_micros: 1,
            },
            source_snapshots: Vec::new(),
        }
    }

    fn row(info_hash: &str) -> TorrentContentWrite {
        TorrentContentWrite {
            id: format!("{info_hash}:movie:?:?"),
            info_hash: info_hash.to_owned(),
            content_type: Some("movie".to_owned()),
            content_source: None,
            content_id: None,
            languages: Vec::new(),
            episodes: String::new(),
            video_resolution: None,
            video_source: None,
            video_codec: None,
            video_3d: None,
            video_modifier: None,
            release_group: None,
            size: 1,
            files_count: None,
        }
    }

    fn supported_params() -> ProcessTorrentParams {
        ProcessTorrentParams {
            classifier_workflow: "default".to_owned(),
            classifier_flags: Some(BTreeMap::from([
                ("apis_enabled".to_owned(), Value::Bool(false)),
                ("local_search_enabled".to_owned(), Value::Bool(false)),
                ("tmdb_enabled".to_owned(), Value::Bool(false)),
            ])),
            ..ProcessTorrentParams::default()
        }
    }

    #[test]
    fn supported_params_require_explicit_default_and_attach_flags_off() {
        validate_supported_params(&supported_params()).expect("explicit supported subset");

        let mut implicit_workflow = supported_params();
        implicit_workflow.classifier_workflow.clear();
        assert!(matches!(
            validate_supported_params(&implicit_workflow),
            Err(WriterPlanError::ClassifierWorkflowUnsupported)
        ));

        let mut inherited_flags = supported_params();
        inherited_flags.classifier_flags = None;
        assert!(matches!(
            validate_supported_params(&inherited_flags),
            Err(WriterPlanError::AttachFlagsNotExplicitlyDisabled)
        ));

        let mut enabled_flag = supported_params();
        enabled_flag
            .classifier_flags
            .as_mut()
            .expect("fixture flags")
            .insert("tmdb_enabled".to_owned(), Value::Bool(true));
        assert!(matches!(
            validate_supported_params(&enabled_flag),
            Err(WriterPlanError::AttachFlagsNotExplicitlyDisabled)
        ));
    }

    #[test]
    fn loaded_index_requires_unique_matching_keys() {
        let valid = loaded(HASH_A);
        assert_eq!(index_loaded_torrents(&[valid]).unwrap().len(), 1);

        let duplicate_a = loaded(HASH_A);
        let duplicate_b = loaded(HASH_A);
        assert!(matches!(
            index_loaded_torrents(&[duplicate_a, duplicate_b]),
            Err(WriterPlanError::DuplicateLoadedTorrent { info_hash })
                if info_hash == HASH_A
        ));

        let mut mismatched = loaded(HASH_A);
        mismatched.loaded.classifier_input.id = HASH_B.to_owned();
        assert!(matches!(
            index_loaded_torrents(&[mismatched]),
            Err(WriterPlanError::LoadedInfoHashMismatch {
                loaded_info_hash,
                classifier_info_hash,
            }) if loaded_info_hash == HASH_A && classifier_info_hash == HASH_B
        ));

        let mut unsupported = loaded(HASH_A);
        unsupported.loaded.attach_hint_unsupported = true;
        assert!(matches!(
            index_loaded_torrents(&[unsupported]),
            Err(WriterPlanError::AttachHintUnsupported { info_hash })
                if info_hash == HASH_A
        ));
    }

    #[test]
    fn composition_rejects_loaded_torrents_outside_the_request() {
        let materializer = crate::Materializer::from_core().expect("compile core classifier");
        let mut params = supported_params();
        params.info_hashes =
            vec![bitmagnet_queue::ProtocolId::from_hex(HASH_A).expect("fixture hash A")];
        let error = compose_writer_plan(&materializer, &params, &[loaded(HASH_A), loaded(HASH_B)])
            .expect_err("unrequested loaded torrent fails closed");
        assert!(matches!(
            error,
            WriterPlanError::UnrequestedLoadedTorrent { info_hash }
                if info_hash == HASH_B
        ));
    }

    #[test]
    fn composition_is_independent_of_loaded_torrent_order() {
        let materializer = crate::Materializer::from_core().expect("compile core classifier");
        let mut params = supported_params();
        params.info_hashes = vec![
            bitmagnet_queue::ProtocolId::from_hex(HASH_B).expect("fixture hash B"),
            bitmagnet_queue::ProtocolId::from_hex(HASH_A).expect("fixture hash A"),
            bitmagnet_queue::ProtocolId::from_hex(HASH_B).expect("duplicate fixture hash B"),
        ];

        let forward =
            compose_writer_plan(&materializer, &params, &[loaded(HASH_A), loaded(HASH_B)])
                .expect("compose forward loaded order");
        let reverse =
            compose_writer_plan(&materializer, &params, &[loaded(HASH_B), loaded(HASH_A)])
                .expect("compose reverse loaded order");

        assert_eq!(forward, reverse);
    }

    #[test]
    fn persistence_projection_requires_an_exact_unique_supported_keyset() {
        let loaded = [loaded(HASH_A)];
        let indexed = index_loaded_torrents(&loaded).expect("index valid source");
        let write_set = WriteSet {
            torrent_contents: vec![row(HASH_A)],
            ..WriteSet::default()
        };
        let projected = project_persistence(&write_set, &indexed).expect("project valid row");
        assert_eq!(projected.len(), 1);
        assert!(projected.contains_key(&write_set.torrent_contents[0].id));

        let missing = WriteSet {
            torrent_contents: vec![row(HASH_B)],
            ..WriteSet::default()
        };
        assert!(matches!(
            project_persistence(&missing, &indexed),
            Err(WriterPlanError::ProjectionSourceMissing { info_hash })
                if info_hash == HASH_B
        ));

        let mut attached_row = row(HASH_A);
        attached_row.content_source = Some("tmdb".to_owned());
        attached_row.content_id = Some("42".to_owned());
        let attached = WriteSet {
            torrent_contents: vec![attached_row],
            ..WriteSet::default()
        };
        assert!(matches!(
            project_persistence(&attached, &indexed),
            Err(WriterPlanError::Projection {
                source: WriterProjectionError::AttachedContentUnsupported,
                ..
            })
        ));

        let duplicated_row = row(HASH_A);
        let duplicate = WriteSet {
            torrent_contents: vec![duplicated_row.clone(), duplicated_row],
            ..WriteSet::default()
        };
        assert!(matches!(
            project_persistence(&duplicate, &indexed),
            Err(WriterPlanError::DuplicatePersistenceId { .. })
        ));
    }
}
