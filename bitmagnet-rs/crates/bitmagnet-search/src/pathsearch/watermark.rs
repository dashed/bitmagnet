//! The follow-loop watermark file: a single Unix-epoch-seconds value shared
//! between the backfill (which seeds it on full completion) and the serving
//! pod's follow loop (which reads it on startup, then advances it each tick).
//!
//! Keeping the read/write/parse in one module guarantees the backfill writer
//! and the follow reader agree on the on-disk format — a mismatch would silently
//! break the post-backfill freshness hand-off.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;

/// Current wall-clock time as Unix epoch seconds (saturating, never negative).
#[must_use]
pub fn current_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Read the watermark file, returning `None` if it is absent or unparseable so
/// the caller can fall back to a lagged start instead of replaying from epoch 0.
#[must_use]
pub fn read_watermark(path: &Path) -> Option<i64> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// Atomically publish `watermark` to `path` (temp file + rename), creating the
/// parent directory if needed.
///
/// # Errors
/// Returns an error if the parent dir, temp write, or rename fails.
pub fn write_watermark(path: &Path, watermark: i64) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating watermark dir {}", parent.display()))?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, format!("{watermark}\n"))
        .with_context(|| format!("writing temp watermark {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("publishing watermark {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{read_watermark, write_watermark};

    /// The watermark persists across reads and advances monotonically; an absent
    /// file reads as `None` so callers fall back to a lagged start rather than
    /// replaying from epoch zero.
    #[test]
    fn round_trips_and_advances() {
        let path = std::env::temp_dir().join(format!(
            "bitmagnet-pathsearch-wm-{}-{}",
            std::process::id(),
            "mod"
        ));
        let _ = std::fs::remove_file(&path);
        assert_eq!(read_watermark(&path), None);

        write_watermark(&path, 1_600_000_000).unwrap();
        assert_eq!(read_watermark(&path), Some(1_600_000_000));

        write_watermark(&path, 1_600_000_300).unwrap();
        assert_eq!(read_watermark(&path), Some(1_600_000_300));

        let _ = std::fs::remove_file(&path);
    }
}
