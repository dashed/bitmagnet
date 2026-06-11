//! Generation layout + atomic swap.
//!
//! A generation is an immutable directory of Parquet artifacts. The pipeline
//! writes a brand-new versioned dir, fsyncs it, then atomically repoints a
//! `current` symlink — readers (the sidecar) only ever see a fully-written
//! generation, and an in-flight reader keeps its old generation until it drops
//! it (FB-B1c: serve from an immutable read-only generation).
//!
//! ```text
//! <root>/
//!   base/  v<ver>/{fact,agg_ext,agg_torrent_ext}.parquet   current -> v<ver>
//!   delta/ v<ver>/{fact,agg_ext,agg_torrent_ext,tombstones}.parquet  current -> v<ver>
//!   watermark    # the BASE'S cut point (compaction-only; the cumulative
//!                # delta carve origin)
//!   delta_mark   # the latest delta window end (freshness display)
//! ```
//!
//! The swap is a symlink `rename(2)` over an existing name — atomic on POSIX.
//! On non-unix platforms we fall back to a `current.txt` pointer file (also
//! rename-swapped); the sidecar resolves whichever exists.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Which half of the generation pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Base,
    Delta,
}

impl Kind {
    fn dirname(self) -> &'static str {
        match self {
            Kind::Base => "base",
            Kind::Delta => "delta",
        }
    }
}

/// Standard artifact file names within a generation dir.
pub mod artifact {
    pub const FACT: &str = "fact.parquet";
    pub const AGG_EXT: &str = "agg_ext.parquet";
    pub const AGG_TORRENT_EXT: &str = "agg_torrent_ext.parquet";
    /// Delta-only: the supersession key set (info_hash list, incl. deletes).
    pub const TOMBSTONES: &str = "tombstones.parquet";
}

/// The on-disk root of an L2 generation tree.
#[derive(Debug, Clone)]
pub struct Layout {
    root: PathBuf,
}

impl Layout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn kind_dir(&self, kind: Kind) -> PathBuf {
        self.root.join(kind.dirname())
    }

    fn current_link(&self, kind: Kind) -> PathBuf {
        self.kind_dir(kind).join("current")
    }

    fn pointer_file(&self, kind: Kind) -> PathBuf {
        self.kind_dir(kind).join("current.txt")
    }

    /// Create (if absent) the `<root>/{base,delta}` directories.
    pub fn ensure_dirs(&self) -> Result<()> {
        for k in [Kind::Base, Kind::Delta] {
            fs::create_dir_all(self.kind_dir(k))
                .with_context(|| format!("creating {}", self.kind_dir(k).display()))?;
        }
        Ok(())
    }

    /// Allocate (and create) a fresh versioned dir for `kind`. `version` is a
    /// caller-supplied monotonic token (e.g. an epoch-second timestamp), kept
    /// explicit so the layout is deterministic and unit-testable.
    pub fn new_version_dir(&self, kind: Kind, version: &str) -> Result<PathBuf> {
        let dir = self.kind_dir(kind).join(format!("v{version}"));
        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        Ok(dir)
    }

    /// Atomically publish `version_dir` as the `current` generation for `kind`.
    ///
    /// fsyncs the version dir's files first, then swaps the pointer. The pointer
    /// is a symlink on unix (atomic `rename`), or a `current.txt` file elsewhere.
    pub fn publish(&self, kind: Kind, version_dir: &Path) -> Result<()> {
        fsync_dir(version_dir).ok(); // best-effort durability of the new gen
        let target = version_dir
            .file_name()
            .context("version dir has no file name")?;

        #[cfg(unix)]
        {
            let link = self.current_link(kind);
            let tmp = self.kind_dir(kind).join(".current.tmp");
            let _ = fs::remove_file(&tmp);
            std::os::unix::fs::symlink(target, &tmp)
                .with_context(|| format!("symlink {} -> {:?}", tmp.display(), target))?;
            fs::rename(&tmp, &link)
                .with_context(|| format!("atomic rename {} -> {}", tmp.display(), link.display()))?;
            fsync_dir(&self.kind_dir(kind)).ok();
        }
        #[cfg(not(unix))]
        {
            let ptr = self.pointer_file(kind);
            let tmp = self.kind_dir(kind).join(".current.txt.tmp");
            fs::write(&tmp, target.to_string_lossy().as_bytes())?;
            fs::rename(&tmp, &ptr)?;
        }
        Ok(())
    }

    /// Resolve the absolute path of the `current` generation dir for `kind`,
    /// or `None` if nothing has been published yet.
    pub fn resolve_current(&self, kind: Kind) -> Option<PathBuf> {
        #[cfg(unix)]
        {
            let link = self.current_link(kind);
            if let Ok(target) = fs::read_link(&link) {
                let dir = self.kind_dir(kind).join(target);
                if dir.is_dir() {
                    return Some(dir);
                }
            }
        }
        let ptr = self.pointer_file(kind);
        if let Ok(name) = fs::read_to_string(&ptr) {
            let dir = self.kind_dir(kind).join(name.trim());
            if dir.is_dir() {
                return Some(dir);
            }
        }
        None
    }

    /// Path to artifact `file` within the current `kind` generation.
    pub fn current_artifact(&self, kind: Kind, file: &str) -> Option<PathBuf> {
        let dir = self.resolve_current(kind)?;
        let p = dir.join(file);
        p.exists().then_some(p)
    }

    // ---- marks ----
    //
    // TWO marks with distinct lifetimes (the CUMULATIVE-delta contract):
    // * `watermark`  — the BASE'S cut point. Written ONLY by compaction (and
    //   the first base). Every delta tick re-carves the WHOLE
    //   `(watermark, now − lag]` window and atomically REPLACES the delta —
    //   the published delta is always cumulative-since-base. (A tick that
    //   advanced this mark would shrink the next carve to a sliver and
    //   un-hide stale base rows — the bug the first live cadence exposed.)
    // * `delta_mark` — the latest delta window END (freshness display only).

    fn watermark_path(&self) -> PathBuf {
        self.root.join("watermark")
    }

    fn delta_mark_path(&self) -> PathBuf {
        self.root.join("delta_mark")
    }

    /// Read the BASE watermark — the cumulative delta carve origin (epoch
    /// seconds), or `0` if unset.
    pub fn read_watermark(&self) -> i64 {
        Self::read_epoch(&self.watermark_path())
    }

    /// Persist the BASE watermark (compaction/base only).
    pub fn write_watermark(&self, epoch: i64) -> Result<()> {
        self.write_epoch(&self.watermark_path(), ".watermark.tmp", epoch)
    }

    /// Read the latest delta window end (freshness), falling back to the base
    /// watermark when no delta has run yet.
    pub fn read_delta_mark(&self) -> i64 {
        match Self::read_epoch(&self.delta_mark_path()) {
            0 => self.read_watermark(),
            v => v,
        }
    }

    /// Persist the delta window end (every delta tick).
    pub fn write_delta_mark(&self, epoch: i64) -> Result<()> {
        self.write_epoch(&self.delta_mark_path(), ".delta_mark.tmp", epoch)
    }

    fn read_epoch(path: &Path) -> i64 {
        fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    fn write_epoch(&self, path: &Path, tmp_name: &str, epoch: i64) -> Result<()> {
        let tmp = self.root.join(tmp_name);
        let mut f = fs::File::create(&tmp)?;
        write!(f, "{epoch}")?;
        f.sync_all().ok();
        fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// Best-effort `fsync` of a directory (POSIX durability of renames within it).
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let f = fs::File::open(dir)?;
        f.sync_all()?;
    }
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("bmp-gen-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn publish_then_resolve_roundtrips() {
        let l = Layout::new(tmp("resolve"));
        l.ensure_dirs().unwrap();
        assert!(l.resolve_current(Kind::Base).is_none());

        let v1 = l.new_version_dir(Kind::Base, "100").unwrap();
        fs::write(v1.join(artifact::FACT), b"x").unwrap();
        l.publish(Kind::Base, &v1).unwrap();
        let cur = l.resolve_current(Kind::Base).unwrap();
        assert!(cur.ends_with("v100"));
        assert!(l.current_artifact(Kind::Base, artifact::FACT).is_some());
    }

    #[test]
    fn publish_is_atomic_swap_to_newer_gen() {
        let l = Layout::new(tmp("swap"));
        l.ensure_dirs().unwrap();
        let v1 = l.new_version_dir(Kind::Base, "100").unwrap();
        fs::write(v1.join(artifact::FACT), b"old").unwrap();
        l.publish(Kind::Base, &v1).unwrap();

        let v2 = l.new_version_dir(Kind::Base, "200").unwrap();
        fs::write(v2.join(artifact::FACT), b"new").unwrap();
        l.publish(Kind::Base, &v2).unwrap();

        let cur = l.resolve_current(Kind::Base).unwrap();
        assert!(cur.ends_with("v200"));
        // old generation dir still exists (immutable; GC is a separate concern)
        assert!(l.kind_dir(Kind::Base).join("v100").is_dir());
    }

    #[test]
    fn delta_mark_falls_back_to_watermark_and_roundtrips() {
        let l = Layout::new(tmp("marks"));
        l.ensure_dirs().unwrap();
        assert_eq!(l.read_delta_mark(), 0);
        l.write_watermark(100).unwrap();
        // No delta yet → freshness falls back to the base cut.
        assert_eq!(l.read_delta_mark(), 100);
        l.write_delta_mark(160).unwrap();
        assert_eq!(l.read_delta_mark(), 160);
        // The carve origin is untouched by delta ticks.
        assert_eq!(l.read_watermark(), 100);
    }

    #[test]
    fn watermark_roundtrips() {
        let l = Layout::new(tmp("watermark"));
        l.ensure_dirs().unwrap();
        assert_eq!(l.read_watermark(), 0);
        l.write_watermark(1_700_000_000).unwrap();
        assert_eq!(l.read_watermark(), 1_700_000_000);
    }
}
