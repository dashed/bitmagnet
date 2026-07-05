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
//!   seg/   v<ver>/{fact,agg_torrent_ext,tombstones}.parquet
//!   delta/ v<ver>/{fact,agg_ext,agg_torrent_ext,tombstones}.parquet  current -> v<ver>
//!   manifest     # base cut + live sealed segment list
//!   watermark    # cumulative delta carve origin
//!   delta_mark   # the latest delta window end (freshness display)
//! ```
//!
//! The swap is a symlink `rename(2)` over an existing name — atomic on POSIX.
//! On non-unix platforms we fall back to a `current.txt` pointer file (also
//! rename-swapped); the sidecar resolves whichever exists.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};

/// Which generation family a versioned dir belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Base,
    Segment,
    Delta,
}

impl Kind {
    fn dirname(self) -> &'static str {
        match self {
            Kind::Base => "base",
            Kind::Segment => "seg",
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

/// Outcome of pruning one generation kind (base or delta).
#[derive(Debug, Clone)]
pub struct PruneReport {
    pub kind: Kind,
    /// How many version dirs were retained (current + newest-N + unparseable).
    pub kept: usize,
    /// The version dirs deleted (or, with `dry_run`, that WOULD be deleted).
    pub deleted: Vec<PathBuf>,
    /// Bytes reclaimed (or that would be reclaimed) across `deleted`.
    pub reclaimed_bytes: u64,
    pub dry_run: bool,
}

impl Layout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn kind_dir(&self, kind: Kind) -> PathBuf {
        self.root.join(kind.dirname())
    }

    fn current_link(&self, kind: Kind) -> PathBuf {
        self.kind_dir(kind).join("current")
    }

    fn pointer_file(&self, kind: Kind) -> PathBuf {
        self.kind_dir(kind).join("current.txt")
    }

    /// Create (if absent) the `<root>/{base,seg,delta}` directories.
    pub fn ensure_dirs(&self) -> Result<()> {
        for k in [Kind::Base, Kind::Segment, Kind::Delta] {
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
        if kind == Kind::Segment {
            anyhow::bail!("segments are manifest-published and have no current pointer");
        }
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
            fs::rename(&tmp, &link).with_context(|| {
                format!("atomic rename {} -> {}", tmp.display(), link.display())
            })?;
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

    /// Durably finish a sealed segment dir. Segments become live through the
    /// manifest, not a per-kind `current` pointer.
    pub fn publish_segment_dir(&self, version_dir: &Path) -> Result<()> {
        fsync_dir(version_dir).ok();
        fsync_dir(&self.kind_dir(Kind::Segment)).ok();
        Ok(())
    }

    /// Resolve the absolute path of the `current` generation dir for `kind`,
    /// or `None` if nothing has been published yet.
    pub fn resolve_current(&self, kind: Kind) -> Option<PathBuf> {
        if kind == Kind::Segment {
            return None;
        }
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

    /// Advance the watermark only if `epoch` is newer than the on-disk value.
    /// Returns `true` when a write happened and `false` for an equal/stale
    /// request. The plain [`Self::write_watermark`] semantics stay unchanged
    /// for compaction.
    pub fn write_watermark_monotonic(&self, epoch: i64) -> Result<bool> {
        if epoch <= self.read_watermark() {
            return Ok(false);
        }
        self.write_watermark(epoch)?;
        Ok(true)
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

    // ---- pruning (generation GC) ----
    //
    // Keep `current` + the newest-N versions per kind; delete the rest. Old
    // generations are immutable, so deleting a non-current, non-newest-N dir is
    // safe once no reader still holds it (reader-hold ≤ the sidecar reload
    // interval + the per-query deadline). The selection REFUSES to delete a
    // `current` target even if it falls outside keep-N, and `prune_kind`
    // re-resolves `current` immediately before each unlink (TOCTOU guard
    // against a concurrent delta tick swapping the symlink mid-prune).

    /// List the `v*` version directories under `<kind>/` (immediate child dirs
    /// whose name starts with `v`). Ignores the `current`/`current.txt` pointers
    /// and any scratch (`.current.tmp`, …). Missing kind dir → empty.
    pub fn list_version_dirs(&self, kind: Kind) -> Result<Vec<PathBuf>> {
        let dir = self.kind_dir(kind);
        let mut out = Vec::new();
        let rd = match fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e).with_context(|| format!("reading {}", dir.display())),
        };
        for entry in rd {
            let entry = entry?;
            let is_v = entry
                .file_name()
                .to_str()
                .map(|n| n.starts_with('v'))
                .unwrap_or(false);
            // is_dir() follows the symlink, so the `current` pointer (name does
            // not start with `v`) is excluded by the name check above anyway.
            if is_v && entry.path().is_dir() {
                out.push(entry.path());
            }
        }
        Ok(out)
    }

    /// PURE partition of `dirs` into `(keep, delete)`: keep `current` (ALWAYS,
    /// even outside keep-N) plus the newest-`keep` by parsed version. Names that
    /// do not parse as `v<epoch>[-empty]` are kept (never delete something we do
    /// not understand). Newest = highest epoch; for an equal epoch the `-empty`
    /// reset delta sorts AFTER the plain dir (compaction publishes it last).
    pub fn select_prunable(
        dirs: &[PathBuf],
        current: Option<&Path>,
        keep: usize,
    ) -> (Vec<PathBuf>, Vec<PathBuf>) {
        let current_name = current.and_then(Path::file_name);
        let mut keep_set: Vec<PathBuf> = Vec::new();
        let mut parseable: Vec<(&PathBuf, (i64, u8))> = Vec::new();
        for d in dirs {
            match d
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(parse_version_key)
            {
                Some(k) => parseable.push((d, k)),
                None => keep_set.push(d.clone()), // unparseable → always keep
            }
        }
        // Newest first.
        parseable.sort_by_key(|(_, key)| std::cmp::Reverse(*key));
        let mut delete_set: Vec<PathBuf> = Vec::new();
        for (i, (d, _)) in parseable.iter().enumerate() {
            let is_current = current_name.is_some() && d.file_name() == current_name;
            if i < keep || is_current {
                keep_set.push((*d).clone());
            } else {
                delete_set.push((*d).clone());
            }
        }
        (keep_set, delete_set)
    }

    /// Prune one kind: keep `current` + newest-`keep`, delete the rest. With
    /// `dry_run`, computes + measures the delete set but unlinks nothing.
    pub fn prune_kind(&self, kind: Kind, keep: usize, dry_run: bool) -> Result<PruneReport> {
        if kind == Kind::Segment {
            anyhow::bail!(
                "prune_kind(Kind::Segment) is unsafe; call prune_segments with an explicit GC grace"
            );
        }
        let current = self.resolve_current(kind);
        let dirs = self.list_version_dirs(kind)?;
        let (keep_set, delete_set) = Self::select_prunable(&dirs, current.as_deref(), keep);
        let mut report = PruneReport {
            kind,
            kept: keep_set.len(),
            deleted: Vec::new(),
            reclaimed_bytes: 0,
            dry_run,
        };
        for d in delete_set {
            let bytes = dir_size(&d).unwrap_or(0);
            if dry_run {
                report.reclaimed_bytes += bytes;
                report.deleted.push(d);
                continue;
            }
            // TOCTOU guard: never unlink a dir that became `current` since the
            // listing (a delta tick may have swapped the symlink in between).
            let live = self.resolve_current(kind);
            if live.as_deref().and_then(Path::file_name) == d.file_name() {
                continue;
            }
            match fs::remove_dir_all(&d) {
                Ok(()) => {}
                // Already gone — a concurrent prune run (the standalone */15 cron
                // racing the compaction job's inline prune) won the race. That is
                // success, not an error: keep going.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e).with_context(|| format!("removing {}", d.display())),
            }
            report.reclaimed_bytes += bytes;
            report.deleted.push(d);
        }
        Ok(report)
    }

    /// Prune both kinds (base keeps `keep_base`, delta keeps `keep_delta`).
    pub fn prune(
        &self,
        keep_base: usize,
        keep_delta: usize,
        segment_gc_grace: Duration,
        dry_run: bool,
    ) -> Result<Vec<PruneReport>> {
        Ok(vec![
            self.prune_kind(Kind::Base, keep_base, dry_run)?,
            self.prune_kind(Kind::Delta, keep_delta, dry_run)?,
            self.prune_segments(segment_gc_grace, dry_run)?,
        ])
    }

    /// Segment GC: delete only dirs absent from the current manifest and older
    /// than the grace window. Base/delta current-pointer rules do not apply.
    pub fn prune_segments(&self, gc_grace: Duration, dry_run: bool) -> Result<PruneReport> {
        let dirs = self.list_version_dirs(Kind::Segment)?;
        let manifest = crate::manifest::read_manifest(self)?;
        let referenced = segment_reference_names(manifest.as_ref());
        let now = SystemTime::now();
        let mut report = PruneReport {
            kind: Kind::Segment,
            kept: 0,
            deleted: Vec::new(),
            reclaimed_bytes: 0,
            dry_run,
        };

        for d in dirs {
            let keep = segment_dir_should_keep(&d, &referenced, now, gc_grace);
            if keep {
                report.kept += 1;
                continue;
            }
            let bytes = dir_size(&d).unwrap_or(0);
            if dry_run {
                report.reclaimed_bytes += bytes;
                report.deleted.push(d);
                continue;
            }
            // TOCTOU guard: the manifest is the liveness source for segments,
            // so re-read it immediately before each unlink.
            let live_manifest = crate::manifest::read_manifest(self)?;
            let live_referenced = segment_reference_names(live_manifest.as_ref());
            if segment_dir_is_referenced(&d, &live_referenced) {
                report.kept += 1;
                continue;
            }
            if !remove_dir_all_tolerating_not_found(&d)
                .with_context(|| format!("removing {}", d.display()))?
            {
                continue;
            }
            report.reclaimed_bytes += bytes;
            report.deleted.push(d);
        }
        Ok(report)
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

/// Parse a `v<epoch>` (optionally `-empty`) version dir name into a sort key
/// `(epoch, empty_rank)`. The `-empty` reset delta (published last by
/// compaction) ranks AFTER the plain dir of the same epoch. Returns `None` for
/// anything that is not `v<integer>[-empty]` — callers keep such dirs.
fn parse_version_key(name: &str) -> Option<(i64, u8)> {
    let rest = name.strip_prefix('v')?;
    let (digits, empty_rank) = match rest.strip_suffix("-empty") {
        Some(d) => (d, 1u8),
        None => (rest, 0u8),
    };
    let epoch: i64 = digits.parse().ok()?;
    Some((epoch, empty_rank))
}

/// Parse a manifest/generation numeric `v<epoch>` name.
pub(crate) fn parse_numeric_version_name(name: &str) -> Option<u64> {
    name.strip_prefix('v')?.parse().ok()
}

fn segment_reference_names(
    manifest: Option<&crate::manifest::Manifest>,
) -> std::collections::HashSet<String> {
    manifest
        .into_iter()
        .flat_map(|m| m.segments.iter().map(|s| format!("v{}", s.version)))
        .collect()
}

fn segment_dir_is_referenced(dir: &Path, referenced: &std::collections::HashSet<String>) -> bool {
    dir.file_name()
        .and_then(|n| n.to_str())
        .map(|n| referenced.contains(n))
        .unwrap_or(false)
}

fn segment_dir_should_keep(
    dir: &Path,
    referenced: &std::collections::HashSet<String>,
    now: SystemTime,
    gc_grace: Duration,
) -> bool {
    let Some(name) = dir.file_name().and_then(|n| n.to_str()) else {
        return true;
    };
    if parse_numeric_version_name(name).is_none() {
        return true;
    }
    if referenced.contains(name) {
        return true;
    }
    let Ok(modified) = fs::metadata(dir).and_then(|m| m.modified()) else {
        return true;
    };
    now.duration_since(modified).unwrap_or_default() < gc_grace
}

/// Recursively sum file sizes under `path` (best-effort; a vanished entry → 0).
fn dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let md = entry.metadata()?;
        if md.is_dir() {
            total += dir_size(&entry.path()).unwrap_or(0);
        } else {
            total += md.len();
        }
    }
    Ok(total)
}

fn remove_dir_all_tolerating_not_found(path: &Path) -> std::io::Result<bool> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
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

    // ---- pruning ----

    fn names(v: &[PathBuf]) -> Vec<String> {
        let mut s: Vec<String> = v
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        s.sort();
        s
    }

    #[test]
    fn parse_version_key_handles_plain_and_empty() {
        assert_eq!(parse_version_key("v100"), Some((100, 0)));
        assert_eq!(parse_version_key("v100-empty"), Some((100, 1)));
        // The `-empty` reset delta ranks AFTER the plain dir of the same epoch,
        // and a newer epoch outranks an older `-empty`.
        assert!(parse_version_key("v100-empty") > parse_version_key("v100"));
        assert!(parse_version_key("v200") > parse_version_key("v100-empty"));
        // Non-numeric / pointer names do not parse (callers keep them).
        assert_eq!(parse_version_key("vmanual"), None);
        assert_eq!(parse_version_key("current"), None);
        assert_eq!(parse_version_key("v"), None);
    }

    #[test]
    fn select_prunable_keeps_current_and_newest_n() {
        let dirs: Vec<PathBuf> = ["v100", "v200", "v300", "v400"]
            .iter()
            .map(PathBuf::from)
            .collect();
        let current = PathBuf::from("v100");
        let (keep, del) = Layout::select_prunable(&dirs, Some(&current), 2);
        // newest-2 = {v400, v300}; current = v100 → kept; delete = v200.
        assert_eq!(names(&keep), vec!["v100", "v300", "v400"]);
        assert_eq!(names(&del), vec!["v200"]);
    }

    #[test]
    fn select_prunable_keeps_current_outside_keep_n() {
        // current is the OLDEST; keep-N would exclude it, but it MUST survive.
        let dirs: Vec<PathBuf> = ["v100", "v200", "v300"].iter().map(PathBuf::from).collect();
        let current = PathBuf::from("v100");
        let (keep, del) = Layout::select_prunable(&dirs, Some(&current), 1);
        assert_eq!(names(&keep), vec!["v100", "v300"]); // newest-1 = v300 + current
        assert_eq!(names(&del), vec!["v200"]);
    }

    #[test]
    fn select_prunable_keeps_unparseable() {
        let dirs: Vec<PathBuf> = ["v100", "v200", "vmanual"]
            .iter()
            .map(PathBuf::from)
            .collect();
        let (keep, del) = Layout::select_prunable(&dirs, None, 1);
        // vmanual unparseable → kept; newest-1 = v200; delete v100.
        assert_eq!(names(&keep), vec!["v200", "vmanual"]);
        assert_eq!(names(&del), vec!["v100"]);
    }

    #[test]
    fn prune_kind_refuses_current_and_keeps_newest_n() {
        let l = Layout::new(tmp("prune"));
        l.ensure_dirs().unwrap();
        for v in ["100", "200", "300"] {
            let d = l.new_version_dir(Kind::Base, v).unwrap();
            fs::write(d.join(artifact::FACT), b"x").unwrap();
        }
        // current is the OLDEST (v100) — keep-1 would exclude it.
        l.publish(Kind::Base, &l.kind_dir(Kind::Base).join("v100"))
            .unwrap();
        let report = l.prune_kind(Kind::Base, 1, false).unwrap();
        assert!(l.kind_dir(Kind::Base).join("v100").is_dir()); // current — kept
        assert!(l.kind_dir(Kind::Base).join("v300").is_dir()); // newest-1 — kept
        assert!(!l.kind_dir(Kind::Base).join("v200").exists()); // pruned
        assert_eq!(report.deleted.len(), 1);
        assert!(report.reclaimed_bytes >= 1);
        // current pointer still resolves after the prune.
        assert!(l.resolve_current(Kind::Base).unwrap().ends_with("v100"));
    }

    #[test]
    fn prune_dry_run_deletes_nothing() {
        let l = Layout::new(tmp("prunedry"));
        l.ensure_dirs().unwrap();
        for v in ["100", "200", "300"] {
            let d = l.new_version_dir(Kind::Base, v).unwrap();
            fs::write(d.join(artifact::FACT), b"xyz").unwrap();
        }
        l.publish(Kind::Base, &l.kind_dir(Kind::Base).join("v300"))
            .unwrap();
        let report = l.prune_kind(Kind::Base, 1, true).unwrap();
        // Dry-run: v100 + v200 reported, but NOTHING removed.
        assert!(l.kind_dir(Kind::Base).join("v100").is_dir());
        assert!(l.kind_dir(Kind::Base).join("v200").is_dir());
        assert!(report.dry_run);
        assert_eq!(report.deleted.len(), 2);
        assert!(report.reclaimed_bytes >= 3);
    }

    #[test]
    fn prune_kind_second_run_is_noop() {
        // Repeated prunes (the */15 standalone cron + the daily compact's inline
        // prune) must be idempotent: once the old dirs are gone, a re-run finds
        // nothing to delete and does not error.
        let l = Layout::new(tmp("pruneidem"));
        l.ensure_dirs().unwrap();
        for v in ["100", "200", "300"] {
            let d = l.new_version_dir(Kind::Base, v).unwrap();
            fs::write(d.join(artifact::FACT), b"x").unwrap();
        }
        l.publish(Kind::Base, &l.kind_dir(Kind::Base).join("v300"))
            .unwrap();
        let first = l.prune_kind(Kind::Base, 1, false).unwrap();
        assert_eq!(first.deleted.len(), 2); // v100 + v200
        let second = l.prune_kind(Kind::Base, 1, false).unwrap();
        assert_eq!(second.deleted.len(), 0); // nothing left to prune
        assert!(l.kind_dir(Kind::Base).join("v300").is_dir());
    }

    #[test]
    fn prune_kind_handles_empty_delta_name() {
        let l = Layout::new(tmp("pruneempty"));
        l.ensure_dirs().unwrap();
        // A plain tick, a compaction `-empty` reset, then another tick.
        for v in ["100", "200-empty", "300"] {
            let d = l.new_version_dir(Kind::Delta, v).unwrap();
            fs::write(d.join(artifact::FACT), b"x").unwrap();
        }
        // current = the empty reset delta.
        l.publish(Kind::Delta, &l.kind_dir(Kind::Delta).join("v200-empty"))
            .unwrap();
        let report = l.prune_kind(Kind::Delta, 1, false).unwrap();
        assert!(l.kind_dir(Kind::Delta).join("v300").is_dir()); // newest-1 (epoch 300)
        assert!(l.kind_dir(Kind::Delta).join("v200-empty").is_dir()); // current — kept
        assert!(!l.kind_dir(Kind::Delta).join("v100").exists()); // pruned
        assert_eq!(report.deleted.len(), 1);
    }

    #[test]
    fn prune_kind_rejects_segment_callers() {
        let l = Layout::new(tmp("prune-seg-direct"));
        l.ensure_dirs().unwrap();

        let err = l.prune_kind(Kind::Segment, 0, true).unwrap_err();

        assert!(format!("{err:#}").contains("prune_segments"));
    }

    #[test]
    fn segment_gc_keeps_manifest_references_and_deletes_old_unreferenced() {
        let l = Layout::new(tmp("seg-gc-old"));
        l.ensure_dirs().unwrap();
        for v in ["110", "120"] {
            let d = l.new_version_dir(Kind::Segment, v).unwrap();
            fs::write(d.join(artifact::FACT), b"x").unwrap();
        }
        let m = crate::manifest::Manifest::new(
            1,
            crate::manifest::BaseEntry {
                version: 100,
                cut: 100,
            },
            vec![crate::manifest::SegmentEntry {
                version: 110,
                from: 100,
                to: 110,
                tier: 0,
            }],
        )
        .unwrap();
        crate::manifest::write_manifest_cas(&l, None, &m).unwrap();

        let report = l.prune_segments(Duration::from_secs(0), false).unwrap();
        assert!(l.kind_dir(Kind::Segment).join("v110").is_dir());
        assert!(!l.kind_dir(Kind::Segment).join("v120").exists());
        assert_eq!(report.deleted.len(), 1);
        assert_eq!(report.kept, 1);
    }

    #[test]
    fn segment_gc_keeps_unreferenced_young_dirs() {
        let l = Layout::new(tmp("seg-gc-young"));
        l.ensure_dirs().unwrap();
        let d = l.new_version_dir(Kind::Segment, "120").unwrap();
        fs::write(d.join(artifact::FACT), b"x").unwrap();
        let m = crate::manifest::Manifest::new(
            1,
            crate::manifest::BaseEntry {
                version: 100,
                cut: 100,
            },
            Vec::new(),
        )
        .unwrap();
        crate::manifest::write_manifest_cas(&l, None, &m).unwrap();

        let report = l.prune_segments(Duration::from_secs(900), false).unwrap();
        assert!(l.kind_dir(Kind::Segment).join("v120").is_dir());
        assert_eq!(report.deleted.len(), 0);
        assert_eq!(report.kept, 1);
    }

    #[test]
    fn segment_gc_not_found_is_success() {
        let l = Layout::new(tmp("seg-gc-notfound"));
        let missing = l.kind_dir(Kind::Segment).join("v404");
        assert!(!remove_dir_all_tolerating_not_found(&missing).unwrap());
    }
}
