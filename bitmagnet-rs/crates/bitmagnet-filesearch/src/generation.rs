//! Generation manager — resolves the current immutable generation layers and
//! swaps them atomically on reload (FB-B1c).
//!
//! The sidecar never mutates a generation; it only ever *reads* the published
//! base, manifest-listed segments, and current delta from `bitmagnet-parquet`.
//! A [`Reload`] re-resolves those layers and swaps the in-memory
//! [`LoadedGeneration`] behind an `RwLock<Arc<…>>`; in-flight queries keep their
//! `Arc` until they finish, so the old Parquet stays readable until the last
//! reader drops it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use bitmagnet_parquet::generation::{artifact, Kind, Layout};
use bitmagnet_parquet::manifest::{read_manifest, Manifest};

use crate::sql::{LayerPaths, LayerSet};

const MAX_RESOLVE_ATTEMPTS: usize = 5;

/// A resolved, immutable generation layer set plus identifying metadata.
#[derive(Debug, Clone)]
pub struct LoadedGeneration {
    pub layers: LayerSet,
    pub base_version: String,
    pub manifest_version: u64,
    pub segment_count: usize,
    pub delta_version: String,
    /// Epoch seconds of the loaded delta's watermark (freshness monitor).
    pub delta_watermark: i64,
}

/// Errors resolving a generation.
#[derive(Debug, thiserror::Error)]
pub enum GenError {
    #[error("no base generation published at {0}")]
    NoBase(String),
    #[error("no delta generation published at {0}")]
    NoDelta(String),
    #[error("manifest resolve failed: {0}")]
    Manifest(#[from] anyhow::Error),
    #[error("manifest segment v{version} is not a directory at {path}")]
    MissingSegment { version: u64, path: String },
    #[error("manifest changed during resolve after {attempts} attempts")]
    TornReadRetryExceeded { attempts: usize },
}

fn version_of(dir: &std::path::Path) -> String {
    dir.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Resolve the current base+delta artifacts into a [`LoadedGeneration`].
pub fn resolve(layout: &Layout) -> Result<LoadedGeneration, GenError> {
    let mut noop = |_: usize, _: &Layout| Ok(());
    resolve_with_delta_hook(layout, &mut noop)
}

fn resolve_with_delta_hook(
    layout: &Layout,
    after_delta_read: &mut impl FnMut(usize, &Layout) -> Result<(), GenError>,
) -> Result<LoadedGeneration, GenError> {
    // Read manifest -> base -> delta, then re-check the manifest mver. If a
    // seal or merge-base swaps the manifest while this resolve is torn across
    // publishes, using the old manifest with a new shrunken delta could
    // under-cover rows. A changed mver means retry the whole ordered read.
    for attempt in 1..=MAX_RESOLVE_ATTEMPTS {
        let manifest = read_manifest(layout)?;
        let manifest_version = manifest.as_ref().map_or(0, |m| m.mver);

        let base_dir = layout
            .resolve_current(Kind::Base)
            .ok_or_else(|| GenError::NoBase(layout.root().display().to_string()))?;
        let delta_dir = layout
            .resolve_current(Kind::Delta)
            .ok_or_else(|| GenError::NoDelta(layout.root().display().to_string()))?;

        after_delta_read(attempt, layout)?;

        let reread_manifest_version = read_manifest(layout)?.as_ref().map_or(0, |m| m.mver);
        if reread_manifest_version != manifest_version {
            continue;
        }

        let layers = build_layer_set(layout, manifest.as_ref(), &base_dir, &delta_dir)?;
        return Ok(LoadedGeneration {
            segment_count: layers.segment_count(),
            layers,
            base_version: version_of(&base_dir),
            manifest_version,
            delta_version: version_of(&delta_dir),
            delta_watermark: layout.read_delta_mark(),
        });
    }
    Err(GenError::TornReadRetryExceeded {
        attempts: MAX_RESOLVE_ATTEMPTS,
    })
}

fn build_layer_set(
    layout: &Layout,
    manifest: Option<&Manifest>,
    base_dir: &Path,
    delta_dir: &Path,
) -> Result<LayerSet, GenError> {
    let s = |d: &Path, f: &str| d.join(f).to_string_lossy().into_owned();
    let mut layers = Vec::new();
    layers.push(LayerPaths {
        fact: s(base_dir, artifact::FACT),
        agg_torrent_ext: s(base_dir, artifact::AGG_TORRENT_EXT),
        tombstones: None,
    });

    if let Some(manifest) = manifest {
        for segment in &manifest.segments {
            let dir = segment_dir(layout, segment.version);
            if !dir.is_dir() {
                return Err(GenError::MissingSegment {
                    version: segment.version,
                    path: dir.display().to_string(),
                });
            }
            layers.push(LayerPaths {
                fact: s(&dir, artifact::FACT),
                agg_torrent_ext: s(&dir, artifact::AGG_TORRENT_EXT),
                tombstones: Some(s(&dir, artifact::TOMBSTONES)),
            });
        }
    }

    layers.push(LayerPaths {
        fact: s(delta_dir, artifact::FACT),
        agg_torrent_ext: s(delta_dir, artifact::AGG_TORRENT_EXT),
        tombstones: Some(s(delta_dir, artifact::TOMBSTONES)),
    });

    Ok(LayerSet::new(layers))
}

fn segment_dir(layout: &Layout, version: u64) -> PathBuf {
    layout.segment_version_dir(version)
}

/// Holds the active generation, swappable on reload.
pub struct GenerationManager {
    layout: Layout,
    current: RwLock<Arc<LoadedGeneration>>,
}

impl GenerationManager {
    /// Open the manager over the generation `layout`, resolving the initial
    /// generation eagerly (fails if nothing has been published yet).
    pub fn open(layout: Layout) -> Result<Self, GenError> {
        let gen = Arc::new(resolve(&layout)?);
        Ok(Self {
            layout,
            current: RwLock::new(gen),
        })
    }

    /// The active generation (cheap `Arc` clone; held by a query for its life).
    pub fn current(&self) -> Arc<LoadedGeneration> {
        self.current
            .read()
            .expect("generation lock poisoned")
            .clone()
    }

    /// Re-resolve `current` symlinks and swap. Returns the new generation (or
    /// the unchanged one if the symlinks didn't move). `expect_version`, when
    /// set, short-circuits if the delta version already matches.
    pub fn reload(
        &self,
        expect_version: Option<&str>,
    ) -> Result<(Arc<LoadedGeneration>, bool), GenError> {
        if let Some(v) = expect_version {
            if self.current().delta_version == v {
                return Ok((self.current(), false));
            }
        }
        let next = Arc::new(resolve(&self.layout)?);
        let changed = {
            let cur = self.current();
            cur.base_version != next.base_version
                || cur.manifest_version != next.manifest_version
                || cur.delta_version != next.delta_version
        };
        if changed {
            *self.current.write().expect("generation lock poisoned") = next.clone();
        }
        Ok((next, changed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitmagnet_parquet::generation::Layout;
    use bitmagnet_parquet::manifest::{write_manifest_cas, BaseEntry, Manifest, SegmentEntry};

    fn tmp(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("bmfs-gen-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Publish a (mostly empty) generation pair under `root` at the given
    /// versions so the manager has something to resolve.
    fn seed(layout: &Layout, base_v: &str, delta_v: &str) {
        layout.ensure_dirs().unwrap();
        for (kind, v, files) in [
            (
                Kind::Base,
                base_v,
                vec![artifact::FACT, artifact::AGG_TORRENT_EXT, artifact::AGG_EXT],
            ),
            (
                Kind::Delta,
                delta_v,
                vec![
                    artifact::FACT,
                    artifact::AGG_TORRENT_EXT,
                    artifact::AGG_EXT,
                    artifact::TOMBSTONES,
                ],
            ),
        ] {
            let dir = layout.new_version_dir(kind, v).unwrap();
            for f in files {
                std::fs::write(dir.join(f), b"").unwrap();
            }
            layout.publish(kind, &dir).unwrap();
        }
    }

    fn manifest(mver: u64, segments: Vec<SegmentEntry>) -> Manifest {
        Manifest::new(
            mver,
            BaseEntry {
                version: 100,
                cut: 100,
            },
            segments,
        )
        .unwrap()
    }

    fn segment(version: u64, from: i64, to: i64) -> SegmentEntry {
        SegmentEntry {
            version,
            from,
            to,
            tier: 0,
        }
    }

    fn publish_segment(layout: &Layout, version: &str) {
        let dir = layout.new_version_dir(Kind::Segment, version).unwrap();
        for f in [
            artifact::FACT,
            artifact::AGG_TORRENT_EXT,
            artifact::TOMBSTONES,
        ] {
            std::fs::write(dir.join(f), b"").unwrap();
        }
        layout.publish_segment_dir(&dir).unwrap();
    }

    #[test]
    fn resolve_builds_all_paths() {
        let layout = Layout::new(tmp("resolve"));
        seed(&layout, "100", "100");
        let g = resolve(&layout).unwrap();
        assert!(g.layers.layers[0].fact.ends_with("base/v100/fact.parquet"));
        assert!(g.layers.layers[1]
            .tombstones
            .as_deref()
            .unwrap()
            .ends_with("delta/v100/tombstones.parquet"));
        assert_eq!(g.base_version, "v100");
        assert_eq!(g.manifest_version, 0);
        assert_eq!(g.segment_count, 0);
    }

    #[test]
    fn reload_swaps_to_new_delta() {
        let layout = Layout::new(tmp("reload"));
        seed(&layout, "100", "100");
        let mgr = GenerationManager::open(layout.clone()).unwrap();
        assert_eq!(mgr.current().delta_version, "v100");

        // publish a newer delta
        let dir = layout.new_version_dir(Kind::Delta, "200").unwrap();
        for f in [
            artifact::FACT,
            artifact::AGG_TORRENT_EXT,
            artifact::TOMBSTONES,
        ] {
            std::fs::write(dir.join(f), b"").unwrap();
        }
        layout.publish(Kind::Delta, &dir).unwrap();

        let (next, changed) = mgr.reload(None).unwrap();
        assert!(changed);
        assert_eq!(next.delta_version, "v200");
        assert_eq!(mgr.current().delta_version, "v200");
    }

    #[test]
    fn torn_read_manifest_mver_change_retries() {
        let layout = Layout::new(tmp("torn"));
        seed(&layout, "100", "200");
        let initial = manifest(1, Vec::new());
        write_manifest_cas(&layout, None, &initial).unwrap();
        publish_segment(&layout, "150");

        let mut calls = 0;
        let g = resolve_with_delta_hook(&layout, &mut |attempt, layout| {
            calls += 1;
            if attempt == 1 {
                write_manifest_cas(layout, Some(1), &manifest(2, vec![segment(150, 100, 150)]))?;
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(calls, 2);
        assert_eq!(g.manifest_version, 2);
        assert_eq!(g.segment_count, 1);
        assert!(g.layers.layers[1].fact.ends_with("seg/v150/fact.parquet"));
        assert_eq!(g.delta_version, "v200");
    }

    #[test]
    fn reload_detects_manifest_only_change() {
        let layout = Layout::new(tmp("manifest-reload"));
        seed(&layout, "100", "100");
        write_manifest_cas(&layout, None, &manifest(1, Vec::new())).unwrap();
        let mgr = GenerationManager::open(layout.clone()).unwrap();
        assert_eq!(mgr.current().manifest_version, 1);
        assert_eq!(mgr.current().segment_count, 0);

        publish_segment(&layout, "150");
        write_manifest_cas(&layout, Some(1), &manifest(2, vec![segment(150, 100, 150)])).unwrap();

        let (next, changed) = mgr.reload(None).unwrap();
        assert!(changed);
        assert_eq!(next.delta_version, "v100");
        assert_eq!(next.manifest_version, 2);
        assert_eq!(next.segment_count, 1);
        assert_eq!(mgr.current().manifest_version, 2);
    }

    #[test]
    fn missing_manifest_segment_errors_and_manager_keeps_current() {
        let layout = Layout::new(tmp("missing-segment"));
        seed(&layout, "100", "100");
        write_manifest_cas(&layout, None, &manifest(1, Vec::new())).unwrap();
        let mgr = GenerationManager::open(layout.clone()).unwrap();

        write_manifest_cas(&layout, Some(1), &manifest(2, vec![segment(150, 100, 150)])).unwrap();

        let err = mgr.reload(None).unwrap_err();
        assert!(matches!(err, GenError::MissingSegment { version: 150, .. }));
        assert_eq!(mgr.current().manifest_version, 1);
        assert_eq!(mgr.current().segment_count, 0);
    }

    #[test]
    fn reload_expect_version_short_circuits() {
        let layout = Layout::new(tmp("expect"));
        seed(&layout, "100", "100");
        let mgr = GenerationManager::open(layout).unwrap();
        let (_, changed) = mgr.reload(Some("v100")).unwrap();
        assert!(!changed);
    }

    #[test]
    fn open_fails_without_published_generation() {
        let layout = Layout::new(tmp("empty"));
        layout.ensure_dirs().unwrap();
        assert!(GenerationManager::open(layout).is_err());
    }
}
