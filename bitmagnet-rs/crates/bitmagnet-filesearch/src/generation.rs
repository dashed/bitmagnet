//! Generation manager — resolves the current immutable generation pair and
//! swaps it atomically on reload (FB-B1c).
//!
//! The sidecar never mutates a generation; it only ever *reads* the `current`
//! base + delta dirs published by `bitmagnet-parquet`. A [`Reload`] re-resolves
//! the `current` symlinks and swaps the in-memory [`LoadedGeneration`] behind an
//! `RwLock<Arc<…>>`; in-flight queries keep their `Arc` until they finish, so
//! the old Parquet stays readable until the last reader drops it.

use std::sync::{Arc, RwLock};

use bitmagnet_parquet::generation::{artifact, Kind, Layout};

use crate::sql::GenPaths;

/// A resolved, immutable generation pair plus identifying metadata.
#[derive(Debug, Clone)]
pub struct LoadedGeneration {
    pub paths: GenPaths,
    pub base_version: String,
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
}

fn version_of(dir: &std::path::Path) -> String {
    dir.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Resolve the current base+delta artifacts into a [`LoadedGeneration`].
pub fn resolve(layout: &Layout) -> Result<LoadedGeneration, GenError> {
    let base_dir = layout
        .resolve_current(Kind::Base)
        .ok_or_else(|| GenError::NoBase(layout.root().display().to_string()))?;
    let delta_dir = layout
        .resolve_current(Kind::Delta)
        .ok_or_else(|| GenError::NoDelta(layout.root().display().to_string()))?;

    let s = |d: &std::path::Path, f: &str| d.join(f).to_string_lossy().into_owned();
    let paths = GenPaths {
        base_fact: s(&base_dir, artifact::FACT),
        delta_fact: s(&delta_dir, artifact::FACT),
        delta_tombstones: s(&delta_dir, artifact::TOMBSTONES),
        base_agg_torrent_ext: s(&base_dir, artifact::AGG_TORRENT_EXT),
        delta_agg_torrent_ext: s(&delta_dir, artifact::AGG_TORRENT_EXT),
    };
    Ok(LoadedGeneration {
        paths,
        base_version: version_of(&base_dir),
        delta_version: version_of(&delta_dir),
        delta_watermark: layout.read_delta_mark(),
    })
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
            cur.base_version != next.base_version || cur.delta_version != next.delta_version
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

    #[test]
    fn resolve_builds_all_paths() {
        let layout = Layout::new(tmp("resolve"));
        seed(&layout, "100", "100");
        let g = resolve(&layout).unwrap();
        assert!(g.paths.base_fact.ends_with("base/v100/fact.parquet"));
        assert!(g
            .paths
            .delta_tombstones
            .ends_with("delta/v100/tombstones.parquet"));
        assert_eq!(g.base_version, "v100");
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
