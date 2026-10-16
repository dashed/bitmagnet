//! Segment manifest parsing and atomic swap.
//!
//! The manifest is the liveness list for sealed segments. It is intentionally
//! tiny text with a hard format guard: an unknown line is an error, not a thing
//! to skip, so future layout changes do not silently downgrade correctness.

use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use thiserror::Error;

use crate::generation::{parse_numeric_version_name, Kind, Layout};

const HEADER: &str = "bmfs-manifest v1";

/// Base generation recorded by the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseEntry {
    pub version: u64,
    pub cut: i64,
}

/// One sealed segment. `version` is an immutable identity token; the true
/// carved window is recorded by `from`/`to`, ordered oldest to newest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentEntry {
    pub version: u64,
    pub from: i64,
    pub to: i64,
    pub tier: u8,
}

/// Parsed manifest file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub mver: u64,
    pub base: BaseEntry,
    pub segments: Vec<SegmentEntry>,
}

/// Retryable manifest compare-and-swap failure.
#[derive(Debug, Error)]
#[error("manifest mver changed: expected {expected:?}, actual {actual:?}")]
pub struct ManifestCasError {
    pub expected: Option<u64>,
    pub actual: Option<u64>,
}

impl Manifest {
    /// Build and validate a manifest value.
    pub fn new(mver: u64, base: BaseEntry, segments: Vec<SegmentEntry>) -> Result<Self> {
        let m = Self {
            mver,
            base,
            segments,
        };
        m.validate()?;
        Ok(m)
    }

    /// Parse the exact v1 text format.
    pub fn parse(text: &str) -> Result<Self> {
        let mut lines = text.lines();
        match lines.next() {
            Some(HEADER) => {}
            Some(other) => anyhow::bail!("bad manifest header: {other:?}"),
            None => anyhow::bail!("empty manifest"),
        }

        let mver_line = lines.next().context("manifest missing mver line")?;
        let mver = parse_mver_line(mver_line)?;

        let base_line = lines.next().context("manifest missing base line")?;
        let base = parse_base_line(base_line)?;

        let mut segments = Vec::new();
        for line in lines {
            segments.push(parse_segment_line(line)?);
        }

        Self::new(mver, base, segments)
    }

    /// Serialize in the spec's line-oriented format.
    pub fn serialize(&self) -> String {
        let mut out = String::new();
        out.push_str(HEADER);
        out.push('\n');
        out.push_str(&format!("mver {}\n", self.mver));
        out.push_str(&format!(
            "base v{} cut {}\n",
            self.base.version, self.base.cut
        ));
        for s in &self.segments {
            out.push_str(&format!(
                "seg  v{} from {} to {} tier {}\n",
                s.version, s.from, s.to, s.tier
            ));
        }
        out
    }

    /// Check the tiling and version invariants that make the manifest safe to
    /// use as the segment liveness source.
    pub fn validate(&self) -> Result<()> {
        let mut expected_from = self.base.cut;
        let mut prev_version = self.base.version;
        for (idx, s) in self.segments.iter().enumerate() {
            if s.from != expected_from {
                anyhow::bail!(
                    "manifest segment {idx} starts at {}, expected contiguous from {}",
                    s.from,
                    expected_from
                );
            }
            if s.to <= s.from {
                anyhow::bail!(
                    "manifest segment {idx} has non-positive window: from {} to {}",
                    s.from,
                    s.to
                );
            }
            if s.version <= prev_version {
                anyhow::bail!(
                    "manifest segment {idx} version v{} is not greater than previous v{}",
                    s.version,
                    prev_version
                );
            }
            expected_from = s.to;
            prev_version = s.version;
        }
        Ok(())
    }

    /// Last sealed cut point: base cut when there are no segments.
    pub fn cut(&self) -> i64 {
        self.segments.last().map(|s| s.to).unwrap_or(self.base.cut)
    }

    /// Return a copy with the mver bumped by one.
    pub fn bumped(mut self) -> Self {
        self.mver += 1;
        self
    }
}

/// Read `<root>/manifest`; absence is the degenerate two-layer mode.
pub fn read_manifest(layout: &Layout) -> Result<Option<Manifest>> {
    let path = manifest_path(layout);
    match fs::read_to_string(&path) {
        Ok(text) => Manifest::parse(&text)
            .with_context(|| format!("parsing {}", path.display()))
            .map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Atomically write `next`, but only if the manifest still has `expected_mver`.
/// `expected_mver = None` means the file must still be absent.
pub fn write_manifest_cas(
    layout: &Layout,
    expected_mver: Option<u64>,
    next: &Manifest,
) -> Result<()> {
    fs::create_dir_all(layout.root())
        .with_context(|| format!("creating {}", layout.root().display()))?;
    let _lock = acquire_manifest_lock(layout)?;
    next.validate()?;
    let current = read_manifest(layout)?;
    let actual_mver = current.as_ref().map(|m| m.mver);
    if actual_mver != expected_mver {
        return Err(ManifestCasError {
            expected: expected_mver,
            actual: actual_mver,
        }
        .into());
    }
    let required_next = expected_mver.unwrap_or(0) + 1;
    if next.mver != required_next {
        anyhow::bail!(
            "manifest write must bump mver from {:?} to {}; got {}",
            expected_mver,
            required_next,
            next.mver
        );
    }

    let path = manifest_path(layout);
    let tmp = layout.root().join(".manifest.tmp");
    let mut f = fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    f.write_all(next.serialize().as_bytes())?;
    f.sync_all().ok();
    fs::rename(&tmp, &path)
        .with_context(|| format!("atomic rename {} -> {}", tmp.display(), path.display()))?;
    fsync_dir(layout.root()).ok();
    Ok(())
}

/// Build the initial manifest from the currently published base.
pub fn bootstrap_from_layout(layout: &Layout, mver: u64, base_cut: i64) -> Result<Manifest> {
    let current = layout
        .resolve_current(Kind::Base)
        .context("cannot bootstrap manifest without a current base")?;
    let name = current
        .file_name()
        .and_then(|n| n.to_str())
        .context("current base dir has no UTF-8 file name")?;
    let version = parse_numeric_version_name(name)
        .with_context(|| format!("current base dir is not a numeric version: {name}"))?;
    Manifest::new(
        mver,
        BaseEntry {
            version,
            cut: base_cut,
        },
        Vec::new(),
    )
}

fn manifest_path(layout: &Layout) -> std::path::PathBuf {
    layout.root().join("manifest")
}

fn manifest_lock_path(layout: &Layout) -> std::path::PathBuf {
    layout.root().join(".manifest.lock")
}

#[cfg(unix)]
struct ManifestLock {
    _file: fs::File,
}

#[cfg(unix)]
fn acquire_manifest_lock(layout: &Layout) -> Result<ManifestLock> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    let path = manifest_lock_path(layout);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_CLOEXEC)
        .open(&path)
        .with_context(|| format!("opening manifest lock {}", path.display()))?;
    // SAFETY: `file` owns a valid fd for the lifetime of the lock guard. flock
    // is advisory and the kernel releases it when the fd is closed or the
    // process exits.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("locking manifest lock {}", path.display()));
    }
    Ok(ManifestLock { _file: file })
}

#[cfg(not(unix))]
struct ManifestLock;

#[cfg(not(unix))]
fn acquire_manifest_lock(_layout: &Layout) -> Result<ManifestLock> {
    // Non-Unix keeps the pointer-file fallback philosophy: atomic rename still
    // protects torn writes, but there is no process-wide advisory lock.
    Ok(ManifestLock)
}

fn parse_mver_line(line: &str) -> Result<u64> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        ["mver", value] => value
            .parse()
            .with_context(|| format!("invalid mver value: {value}")),
        _ => anyhow::bail!("bad manifest mver line: {line:?}"),
    }
}

fn parse_base_line(line: &str) -> Result<BaseEntry> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        ["base", version, "cut", cut] => Ok(BaseEntry {
            version: parse_v(version)?,
            cut: cut
                .parse()
                .with_context(|| format!("invalid base cut value: {cut}"))?,
        }),
        _ => anyhow::bail!("bad manifest base line: {line:?}"),
    }
}

fn parse_segment_line(line: &str) -> Result<SegmentEntry> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        ["seg", version, "from", from, "to", to, "tier", tier] => Ok(SegmentEntry {
            version: parse_v(version)?,
            from: from
                .parse()
                .with_context(|| format!("invalid segment from value: {from}"))?,
            to: to
                .parse()
                .with_context(|| format!("invalid segment to value: {to}"))?,
            tier: tier
                .parse()
                .with_context(|| format!("invalid segment tier value: {tier}"))?,
        }),
        _ => anyhow::bail!("unknown or malformed manifest line: {line:?}"),
    }
}

fn parse_v(token: &str) -> Result<u64> {
    token
        .strip_prefix('v')
        .context("version token must start with v")?
        .parse()
        .with_context(|| format!("invalid version token: {token}"))
}

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

    fn tmp(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("bmp-manifest-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample() -> Manifest {
        Manifest::new(
            7,
            BaseEntry {
                version: 100,
                cut: 90,
            },
            vec![
                SegmentEntry {
                    version: 120,
                    from: 90,
                    to: 110,
                    tier: 0,
                },
                SegmentEntry {
                    version: 130,
                    from: 110,
                    to: 125,
                    tier: 1,
                },
            ],
        )
        .unwrap()
    }

    #[test]
    fn parse_serialize_roundtrip() {
        let m = sample();
        assert_eq!(Manifest::parse(&m.serialize()).unwrap(), m);
    }

    #[test]
    fn absent_file_is_none() {
        let layout = Layout::new(tmp("absent"));
        assert!(read_manifest(&layout).unwrap().is_none());
    }

    #[test]
    fn unknown_line_is_rejected() {
        let text = "bmfs-manifest v1\nmver 1\nbase v100 cut 90\nfuture x\n";
        assert!(Manifest::parse(text).is_err());
    }

    #[test]
    fn tiling_invariants_reject_gap_overlap_and_non_contiguous_first() {
        let overlap = "bmfs-manifest v1\nmver 1\nbase v100 cut 90\nseg  v101 from 90 to 100 tier 0\nseg  v102 from 99 to 110 tier 0\n";
        let gap = "bmfs-manifest v1\nmver 1\nbase v100 cut 90\nseg  v101 from 90 to 100 tier 0\nseg  v102 from 101 to 110 tier 0\n";
        let bad_first =
            "bmfs-manifest v1\nmver 1\nbase v100 cut 90\nseg  v101 from 91 to 100 tier 0\n";
        assert!(Manifest::parse(overlap).is_err());
        assert!(Manifest::parse(gap).is_err());
        assert!(Manifest::parse(bad_first).is_err());
    }

    #[test]
    fn strictly_increasing_versions_are_required() {
        let text = "bmfs-manifest v1\nmver 1\nbase v100 cut 90\nseg  v101 from 90 to 100 tier 0\nseg  v101 from 100 to 110 tier 0\n";
        assert!(Manifest::parse(text).is_err());
    }

    #[test]
    fn atomic_swap_and_mver_cas() {
        let layout = Layout::new(tmp("cas"));
        let m1 = Manifest::new(
            1,
            BaseEntry {
                version: 100,
                cut: 90,
            },
            Vec::new(),
        )
        .unwrap();
        write_manifest_cas(&layout, None, &m1).unwrap();
        #[cfg(unix)]
        assert!(manifest_lock_path(&layout).is_file());
        assert_eq!(read_manifest(&layout).unwrap(), Some(m1.clone()));

        let mut m2 = m1.clone().bumped();
        m2.segments.push(SegmentEntry {
            version: 120,
            from: 90,
            to: 110,
            tier: 0,
        });
        write_manifest_cas(&layout, Some(1), &m2).unwrap();
        assert_eq!(read_manifest(&layout).unwrap(), Some(m2.clone()));

        let mut stale = m2.clone().bumped();
        stale.segments.push(SegmentEntry {
            version: 140,
            from: 110,
            to: 130,
            tier: 0,
        });
        let err = write_manifest_cas(&layout, Some(1), &stale).unwrap_err();
        assert!(err.downcast_ref::<ManifestCasError>().is_some());
        assert_eq!(read_manifest(&layout).unwrap(), Some(m2));
    }
}
