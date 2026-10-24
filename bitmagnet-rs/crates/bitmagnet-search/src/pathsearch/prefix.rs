//! Bounded path-segment prefix index for L3 typeahead suggestions.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use anyhow::Context;
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use memmap2::Mmap;

/// Filename of the prefix FST inside the Tantivy index directory.
pub const PREFIX_INDEX_FILENAME: &str = "prefix.fst";

/// Corpus-independent bounds for building and querying the prefix index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrefixIndexConfig {
    /// Hard cap on distinct segments retained by the build accumulator.
    pub max_tracked: usize,
    /// Minimum torrent-document frequency emitted into the final index.
    pub min_freq: u64,
    /// Hard cap on entries emitted into the final index.
    pub max_entries: usize,
    /// Hard cap on matching FST keys examined by one suggestion request.
    pub max_scan: usize,
    /// Minimum Unicode character count for indexed segments and prefixes.
    pub min_seg_chars: usize,
    /// Maximum Unicode character count for indexed segments and prefixes.
    pub max_seg_chars: usize,
}

impl Default for PrefixIndexConfig {
    fn default() -> Self {
        Self {
            max_tracked: 3_000_000,
            min_freq: 2,
            max_entries: 1_000_000,
            max_scan: 50_000,
            min_seg_chars: 2,
            max_seg_chars: 128,
        }
    }
}

/// Normalize the path components that form suggestion entries.
pub fn normalize_segments<'a>(
    path: &'a str,
    cfg: &'a PrefixIndexConfig,
) -> impl Iterator<Item = String> + 'a {
    path.split('/').filter_map(move |component| {
        let trimmed = component.trim();
        if !valid_char_count(trimmed, cfg) {
            return None;
        }
        let normalized = trimmed.to_lowercase();
        valid_char_count(&normalized, cfg).then_some(normalized)
    })
}

fn normalize_prefix(prefix: &str, cfg: &PrefixIndexConfig) -> Option<String> {
    let trimmed = prefix.trim();
    if !valid_char_count(trimmed, cfg) {
        return None;
    }
    let normalized = trimmed.to_lowercase();
    valid_char_count(&normalized, cfg).then_some(normalized)
}

fn valid_char_count(value: &str, cfg: &PrefixIndexConfig) -> bool {
    let chars = value
        .chars()
        .take(cfg.max_seg_chars.saturating_add(1))
        .count();
    chars >= cfg.min_seg_chars && chars <= cfg.max_seg_chars
}

/// Corpus-wide document-frequency accumulator with a strict distinct-key cap.
pub struct PrefixIndexBuilder {
    cfg: PrefixIndexConfig,
    counts: HashMap<String, u64>,
}

impl PrefixIndexBuilder {
    /// Start an empty bounded prefix-index build.
    #[must_use]
    pub fn new(cfg: PrefixIndexConfig) -> Self {
        Self {
            cfg,
            counts: HashMap::new(),
        }
    }

    /// Add the paths from one torrent, counting each normalized segment once.
    ///
    /// The retained key set never exceeds `max_tracked`. Once full, existing
    /// keys continue to accrue document frequency while new keys are dropped.
    /// A keyset-ordered torrent stream is approximately content-random, so
    /// genuinely frequent segments tend to appear early and remain retained;
    /// late-first-seen segments are approximated away at capacity.
    pub fn add_paths(&mut self, paths: &[String]) {
        // Only retained keys enter this per-document set, so this allocation is
        // also bounded by max_tracked even for an adversarially large path bag.
        let mut seen = HashSet::new();

        for segment in paths
            .iter()
            .flat_map(|path| normalize_segments(path, &self.cfg))
        {
            if let Some(count) = self.counts.get_mut(&segment) {
                if seen.insert(segment) {
                    *count = count.saturating_add(1);
                }
            } else if self.counts.len() < self.cfg.max_tracked && seen.insert(segment.clone()) {
                self.counts.insert(segment, 1);
            }
        }
    }

    /// Emit the bounded top-frequency key set as an atomic, lexicographic FST.
    ///
    /// # Errors
    /// Returns filesystem or FST construction errors. An existing output file
    /// remains intact when writing the temporary replacement fails.
    pub fn finalize(self, out_path: &Path) -> anyhow::Result<PrefixIndexStats> {
        if let Some(parent) = out_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating prefix index dir {}", parent.display()))?;
        }

        // BinaryHeap's maximum is deliberately the worst retained item: lowest
        // count, then lexicographically largest key. Replacing only that item
        // keeps the desired (count desc, key asc) top-K in O(n log K).
        let mut top: BinaryHeap<(Reverse<u64>, String)> = BinaryHeap::new();
        for (key, count) in self
            .counts
            .into_iter()
            .filter(|(_, count)| *count >= self.cfg.min_freq)
        {
            if top.len() < self.cfg.max_entries {
                top.push((Reverse(count), key));
                continue;
            }
            let replace = top.peek().is_some_and(|(Reverse(worst_count), worst_key)| {
                count > *worst_count || (count == *worst_count && key.as_str() < worst_key.as_str())
            });
            if replace {
                top.pop();
                top.push((Reverse(count), key));
            }
        }

        let mut kept: Vec<(String, u64)> = top
            .into_iter()
            .map(|(Reverse(count), key)| (key, count))
            .collect();
        kept.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        let entries = kept.len();
        let tmp_path = temporary_path(out_path);
        let write_result = (|| -> anyhow::Result<()> {
            let file = File::create(&tmp_path)
                .with_context(|| format!("creating prefix index {}", tmp_path.display()))?;
            let mut builder = MapBuilder::new(BufWriter::new(file))
                .with_context(|| format!("starting prefix FST {}", tmp_path.display()))?;
            for (key, count) in &kept {
                builder
                    .insert(key.as_bytes(), *count)
                    .with_context(|| format!("inserting prefix key {key:?}"))?;
            }
            builder
                .finish()
                .with_context(|| format!("finishing prefix FST {}", tmp_path.display()))?;
            std::fs::rename(&tmp_path, out_path).with_context(|| {
                format!(
                    "replacing prefix index {} with {}",
                    out_path.display(),
                    tmp_path.display()
                )
            })?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = std::fs::remove_file(&tmp_path);
        }
        write_result?;

        Ok(PrefixIndexStats {
            entries,
            out_path: out_path.to_owned(),
        })
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut path = path.as_os_str().to_os_string();
    path.push(".tmp");
    PathBuf::from(path)
}

/// Summary of one finalized prefix-index build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixIndexStats {
    /// Number of entries written to the FST.
    pub entries: usize,
    /// Final atomically replaced output path.
    pub out_path: PathBuf,
}

/// Read-only mmap-backed prefix index.
pub struct PrefixIndex {
    map: Map<Mmap>,
    cfg: PrefixIndexConfig,
}

impl PrefixIndex {
    /// Mmap an existing prefix index, or return `None` when it has not been built.
    ///
    /// # Errors
    /// Returns file-open, mmap, or FST validation errors.
    pub fn open(path: &Path, cfg: PrefixIndexConfig) -> anyhow::Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let file =
            File::open(path).with_context(|| format!("opening prefix index {}", path.display()))?;
        // SAFETY: the mapping is read-only and Mmap owns the mapping after the
        // File handle is dropped. Backfills replace, rather than mutate, files.
        let mmap = unsafe { Mmap::map(&file) }
            .with_context(|| format!("mapping prefix index {}", path.display()))?;
        let map = Map::new(mmap)
            .with_context(|| format!("validating prefix index {}", path.display()))?;
        Ok(Some(Self { map, cfg }))
    }

    /// Number of path segments stored in the FST.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the FST contains no path segments.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Return top document-frequency matches for one normalized prefix.
    ///
    /// The interactive <50ms latency target rests on `max_scan`: at most that
    /// many matching keys are examined, independent of corpus and prefix width.
    #[must_use]
    pub fn suggest(&self, prefix: &str, limit: usize) -> Vec<Suggestion> {
        let Some(normalized) = normalize_prefix(prefix, &self.cfg) else {
            return Vec::new();
        };
        if limit == 0 || self.cfg.max_scan == 0 {
            return Vec::new();
        }

        // Seeking to the prefix avoids touching earlier keys. Once a key no
        // longer shares the byte prefix, lexicographic ordering proves the
        // matching range is exhausted.
        let mut stream = self.map.range().ge(normalized.as_bytes()).into_stream();
        let mut top: BinaryHeap<Reverse<(u64, Reverse<String>)>> = BinaryHeap::new();
        let mut examined = 0_usize;
        while examined < self.cfg.max_scan {
            let Some((key, count)) = stream.next() else {
                break;
            };
            if !key.starts_with(normalized.as_bytes()) {
                break;
            }
            examined += 1;

            let Ok(value) = std::str::from_utf8(key) else {
                continue;
            };
            top.push(Reverse((count, Reverse(value.to_owned()))));
            if top.len() > limit {
                top.pop();
            }
        }

        let mut suggestions: Vec<Suggestion> = top
            .into_iter()
            .map(|Reverse((score, Reverse(value)))| Suggestion { value, score })
            .collect();
        suggestions.sort_unstable_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.value.cmp(&right.value))
        });
        suggestions
    }
}

/// Domain suggestion returned by [`PrefixIndex::suggest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Suggestion {
    /// Lowercased normalized path segment.
    pub value: String,
    /// Number of torrent documents containing this segment.
    pub score: u64,
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_segments, PrefixIndex, PrefixIndexBuilder, PrefixIndexConfig, Suggestion,
    };
    use tempfile::TempDir;

    fn config() -> PrefixIndexConfig {
        PrefixIndexConfig {
            max_tracked: 100,
            min_freq: 1,
            max_entries: 100,
            max_scan: 100,
            min_seg_chars: 2,
            max_seg_chars: 32,
        }
    }

    fn build(cfg: PrefixIndexConfig, documents: &[&[&str]]) -> (TempDir, PrefixIndex, usize) {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("prefix.fst");
        let mut builder = PrefixIndexBuilder::new(cfg);
        for paths in documents {
            let paths: Vec<String> = paths.iter().map(|path| (*path).to_owned()).collect();
            builder.add_paths(&paths);
        }
        let stats = builder.finalize(&path).expect("finalize prefix index");
        let index = PrefixIndex::open(&path, cfg)
            .expect("open prefix index")
            .expect("prefix index exists");
        (dir, index, stats.entries)
    }

    #[test]
    fn normalizes_path_segments_and_applies_character_limits() {
        let cfg = PrefixIndexConfig {
            min_seg_chars: 2,
            max_seg_chars: 5,
            ..config()
        };
        let segments: Vec<String> =
            normalize_segments(" / Foo // b / ABCDEF / ÉÉ / ", &cfg).collect();
        assert_eq!(segments, vec!["foo", "éé"]);
    }

    #[test]
    fn builder_counts_document_frequency_once_per_torrent() {
        let cfg = config();
        let mut builder = PrefixIndexBuilder::new(cfg);
        builder.add_paths(&[
            "Show/Season 01/One.mkv".to_owned(),
            "Show/Season 01/Two.mkv".to_owned(),
        ]);
        assert_eq!(builder.counts.get("show"), Some(&1));
        assert_eq!(builder.counts.get("season 01"), Some(&1));

        builder.add_paths(&["Show/Season 01/Three.mkv".to_owned()]);
        assert_eq!(builder.counts.get("show"), Some(&2));
        assert_eq!(builder.counts.get("season 01"), Some(&2));
    }

    #[test]
    fn finalize_open_and_suggest_rank_bound_and_filter() {
        let cfg = PrefixIndexConfig {
            min_freq: 2,
            ..config()
        };
        let (_dir, index, entries) = build(
            cfg,
            &[
                &["Alpha"],
                &["Alpha"],
                &["Alpha"],
                &["Alpine"],
                &["Alpine"],
                &["Albatross"],
                &["Albatross"],
                &["Alone"],
            ],
        );
        assert_eq!(entries, 3, "min_freq drops the count-one tail");
        assert_eq!(
            index.suggest(" AL ", 3),
            vec![
                Suggestion {
                    value: "alpha".to_owned(),
                    score: 3,
                },
                Suggestion {
                    value: "albatross".to_owned(),
                    score: 2,
                },
                Suggestion {
                    value: "alpine".to_owned(),
                    score: 2,
                },
            ]
        );
        assert_eq!(index.suggest("al", 1).len(), 1, "limit bounds output");
        assert!(index.suggest("a", 10).is_empty(), "short prefix is empty");
        assert!(
            index.suggest("alone", 10).is_empty(),
            "min_freq excluded the count-one key"
        );
    }

    #[test]
    fn finalize_max_entries_keeps_frequency_top_k() {
        let cfg = PrefixIndexConfig {
            max_entries: 2,
            ..config()
        };
        let (_dir, index, entries) = build(
            cfg,
            &[
                &["prefix-high"],
                &["prefix-high"],
                &["prefix-high"],
                &["prefix-mid"],
                &["prefix-mid"],
                &["prefix-tail"],
            ],
        );
        assert_eq!(entries, 2);
        assert_eq!(index.len(), 2);
        let suggestions = index.suggest("prefix-", 10);
        let values: Vec<&str> = suggestions
            .iter()
            .map(|suggestion| suggestion.value.as_str())
            .collect();
        assert_eq!(values, vec!["prefix-high", "prefix-mid"]);
    }

    #[test]
    fn accumulator_never_exceeds_cap_and_retained_keys_keep_counting() {
        let cfg = PrefixIndexConfig {
            max_tracked: 2,
            min_seg_chars: 1,
            ..config()
        };
        let mut builder = PrefixIndexBuilder::new(cfg);
        builder.add_paths(&["a/b/c/d".to_owned()]);
        assert_eq!(builder.counts.len(), 2);
        assert_eq!(builder.counts.get("a"), Some(&1));

        builder.add_paths(&["a/new-key/another-new-key".to_owned()]);
        assert_eq!(builder.counts.len(), 2);
        assert_eq!(builder.counts.get("a"), Some(&2));
        assert!(!builder.counts.contains_key("new-key"));
    }

    #[test]
    fn max_scan_bounds_wide_prefix_queries() {
        let cfg = PrefixIndexConfig {
            max_scan: 3,
            ..config()
        };
        let documents: Vec<Vec<String>> = (0..20)
            .map(|index| vec![format!("shared-{index:02}")])
            .collect();
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("prefix.fst");
        let mut builder = PrefixIndexBuilder::new(cfg);
        for paths in &documents {
            builder.add_paths(paths);
        }
        builder.finalize(&path).expect("finalize prefix index");
        let index = PrefixIndex::open(&path, cfg)
            .expect("open prefix index")
            .expect("prefix index exists");

        let suggestions = index.suggest("shared-", 10);
        assert_eq!(suggestions.len(), 3);
        assert!(suggestions.len() <= 10);
    }
}
