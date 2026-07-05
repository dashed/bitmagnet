//! Sealed segment publishing plus fold/merge-base planning.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bitmagnet_db::{stream_changed_torrents, PgPool};

use crate::export::{BuildStats, Sinks, CARVE_LAG_SECS};
use crate::fact::SortMode;
use crate::generation::{artifact, Kind, Layout};
use crate::manifest::{self, write_manifest_cas, BaseEntry, Manifest, SegmentEntry};

const MANIFEST_CAS_ATTEMPTS: usize = 3;

/// Result of a seal attempt.
#[derive(Debug, Clone)]
pub enum SealOutcome {
    /// Nothing was published because the low-volume skip rule matched.
    Skipped {
        changed_torrents: u64,
        lag_secs: i64,
    },
    /// A new segment became live in the manifest.
    Sealed {
        segment: SegmentEntry,
        stats: BuildStats,
        manifest_mver: u64,
        watermark_advanced: bool,
    },
}

/// Observable milestones in the seal publish protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealPublishStep {
    SegmentDirSynced,
    ManifestWritten,
    WatermarkWritten,
}

/// Retryable guard for a seal started from an older cut snapshot.
#[derive(Debug, thiserror::Error)]
#[error(
    "seal since snapshot is stale: manifest cut is {manifest_cut}, seal since is {since}; retry with a fresh snapshot"
)]
pub struct StaleSealSinceError {
    pub since: i64,
    pub manifest_cut: i64,
}

/// Result of a fold attempt.
#[derive(Debug, Clone)]
pub struct FoldOutcome {
    pub acted: bool,
    pub input_count: usize,
    pub output: Option<SegmentEntry>,
    pub manifest_mver: Option<u64>,
}

/// Result of a merge-base attempt.
#[derive(Debug, Clone)]
pub struct MergeBaseOutcome {
    pub acted: bool,
    pub input_count: usize,
    pub base: Option<BaseEntry>,
    pub manifest_mver: Option<u64>,
}

/// One layer used to build a folded artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldLayer {
    pub fact: PathBuf,
    pub agg_torrent_ext: PathBuf,
    pub tombstones: Option<PathBuf>,
}

/// Which layered SELECT to build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayeredArtifact {
    Fact,
    AggTorrentExt,
}

/// Run an hourly seal over `(watermark, window_end]`.
#[allow(clippy::too_many_arguments)]
pub async fn run_seal(
    pool: &PgPool,
    layout: &Layout,
    since: i64,
    window_end: i64,
    deleted: &[String],
    page_size: i64,
    min_torrents: u64,
    max_lag_secs: i64,
) -> Result<SealOutcome> {
    layout.ensure_dirs()?;
    ensure_manifest_cut_matches_since(layout, since)?;
    if window_end <= since {
        return Ok(SealOutcome::Skipped {
            changed_torrents: 0,
            lag_secs: 0,
        });
    }
    let lag_secs = window_end - since;
    if lag_secs < max_lag_secs {
        let changed =
            count_changed_torrents_up_to(pool, since, window_end, page_size, min_torrents).await?;
        if should_skip_seal(changed, lag_secs, min_torrents, max_lag_secs) {
            tracing::info!(
                changed_torrents = changed,
                lag_secs,
                min_torrents,
                max_lag_secs,
                "seal skipped by low-volume guard"
            );
            return Ok(SealOutcome::Skipped {
                changed_torrents: changed,
                lag_secs,
            });
        }
    }

    let current_manifest = ensure_manifest_cut_matches_since(layout, since)?;
    let version = choose_seal_segment_version(layout, current_manifest.as_ref(), window_end)?;
    let dir = create_version_dir_exclusive(layout, Kind::Segment, version)?;
    let stats = carve_segment(pool, &dir, since, window_end, deleted, page_size).await?;
    let segment = SegmentEntry {
        version,
        from: since,
        to: window_end,
        tier: 0,
    };
    let publish = publish_sealed_segment(layout, &dir, segment.clone())?;
    Ok(SealOutcome::Sealed {
        segment,
        stats,
        manifest_mver: publish.manifest_mver,
        watermark_advanced: publish.watermark_advanced,
    })
}

fn ensure_manifest_cut_matches_since(layout: &Layout, since: i64) -> Result<Option<Manifest>> {
    let manifest = manifest::read_manifest(layout)?;
    if let Some(manifest) = manifest.as_ref() {
        let cut = manifest.cut();
        if cut != since {
            return Err(StaleSealSinceError {
                since,
                manifest_cut: cut,
            }
            .into());
        }
    }
    Ok(manifest)
}

/// If a seal crashed after manifest publish but before watermark advance, the
/// manifest already covers more than the watermark. Advance to that covered cut
/// before the next carve so the next segment stays contiguous and immutable.
pub fn reconcile_watermark_with_manifest(layout: &Layout) -> Result<i64> {
    let watermark = layout.read_watermark();
    let Some(manifest) = manifest::read_manifest(layout)? else {
        return Ok(watermark);
    };
    let cut = manifest.cut();
    if cut > watermark {
        layout.write_watermark_monotonic(cut)?;
        Ok(cut)
    } else {
        Ok(watermark)
    }
}

/// Compute the lagged seal end used by the CLI.
pub fn default_seal_window_end(now_epoch: i64) -> i64 {
    now_epoch - CARVE_LAG_SECS
}

/// Low-volume guard from spec §5.1.
pub fn should_skip_seal(
    changed_torrents: u64,
    lag_secs: i64,
    min_torrents: u64,
    max_lag_secs: i64,
) -> bool {
    changed_torrents < min_torrents && lag_secs < max_lag_secs
}

struct SealPublishResult {
    manifest_mver: u64,
    watermark_advanced: bool,
}

/// Publish a complete segment dir: fsync dir → manifest CAS → monotonic
/// watermark advance.
fn publish_sealed_segment(
    layout: &Layout,
    version_dir: &Path,
    segment: SegmentEntry,
) -> Result<SealPublishResult> {
    publish_sealed_segment_inner(
        layout,
        version_dir,
        segment,
        |_, _| Ok(()),
        |_, _, _, _| Ok(()),
    )
}

#[cfg(test)]
fn publish_sealed_segment_with_observer(
    layout: &Layout,
    version_dir: &Path,
    segment: SegmentEntry,
    mut observe: impl FnMut(SealPublishStep, &Layout) -> Result<()>,
) -> Result<SealPublishResult> {
    publish_sealed_segment_inner(layout, version_dir, segment, &mut observe, |_, _, _, _| {
        Ok(())
    })
}

fn publish_sealed_segment_inner(
    layout: &Layout,
    version_dir: &Path,
    segment: SegmentEntry,
    mut observe: impl FnMut(SealPublishStep, &Layout) -> Result<()>,
    mut before_manifest_cas: impl FnMut(usize, &Layout, Option<u64>, &Manifest) -> Result<()>,
) -> Result<SealPublishResult> {
    layout.publish_segment_dir(version_dir)?;
    observe(SealPublishStep::SegmentDirSynced, layout)?;

    let mut attempt = 1;
    let next = loop {
        let current = manifest::read_manifest(layout)?;
        let expected_mver = current.as_ref().map(|m| m.mver);
        let base = match current {
            Some(m) => m,
            None => manifest::bootstrap_from_layout(layout, 0, segment.from)?,
        };
        let next = append_segment(base, segment.clone())?;
        before_manifest_cas(attempt, layout, expected_mver, &next)?;
        match write_manifest_cas(layout, expected_mver, &next) {
            Ok(()) => break next,
            Err(err)
                if attempt < MANIFEST_CAS_ATTEMPTS
                    && err.downcast_ref::<manifest::ManifestCasError>().is_some() =>
            {
                attempt += 1;
                continue;
            }
            Err(err) => return Err(err),
        }
    };
    observe(SealPublishStep::ManifestWritten, layout)?;

    let watermark_advanced = layout.write_watermark_monotonic(next.cut())?;
    observe(SealPublishStep::WatermarkWritten, layout)?;

    Ok(SealPublishResult {
        manifest_mver: next.mver,
        watermark_advanced,
    })
}

fn append_segment(mut manifest: Manifest, segment: SegmentEntry) -> Result<Manifest> {
    manifest.segments.push(segment);
    Manifest::new(manifest.mver + 1, manifest.base, manifest.segments)
}

fn choose_seal_segment_version(
    layout: &Layout,
    manifest: Option<&Manifest>,
    window_end: i64,
) -> Result<u64> {
    let mut candidate =
        u64::try_from(window_end).context("seal window end must be non-negative")?;
    while segment_version_exists(layout, candidate)
        || manifest_references_segment(manifest, candidate)
    {
        candidate = candidate
            .checked_add(1)
            .context("exhausted seal segment version space")?;
    }
    Ok(candidate)
}

fn create_version_dir_exclusive(layout: &Layout, kind: Kind, version: u64) -> Result<PathBuf> {
    let dir = match kind {
        Kind::Segment => layout.segment_version_dir(version),
        Kind::Base | Kind::Delta => layout.kind_dir(kind).join(format!("v{version}")),
    };
    fs::create_dir(&dir).with_context(|| format!("creating {}", dir.display()))?;
    ensure_empty_dir(&dir)?;
    Ok(dir)
}

async fn count_changed_torrents_up_to(
    pool: &PgPool,
    since: i64,
    window_end: i64,
    page_size: i64,
    limit: u64,
) -> Result<u64> {
    if limit == 0 {
        return Ok(0);
    }
    let mut count = 0u64;
    let mut cursor = None;
    let page_size = page_size.max(1);
    loop {
        let remaining = limit.saturating_sub(count).max(1);
        let page_limit = page_size.min(remaining as i64);
        let page = stream_changed_torrents(pool, since, window_end, cursor.as_ref(), page_limit)
            .await
            .context("counting changed torrents for seal skip guard")?;
        if page.is_empty() {
            break;
        }
        count += page.len() as u64;
        if count >= limit {
            break;
        }
        cursor = page.last().map(|r| r.info_hash);
    }
    Ok(count)
}

async fn carve_segment(
    pool: &PgPool,
    dir: &Path,
    since: i64,
    window_end: i64,
    deleted: &[String],
    page_size: i64,
) -> Result<BuildStats> {
    ensure_empty_dir(dir)?;
    let mut sinks = Sinks::create(dir, SortMode::InMemory, true)?;
    let mut cursor = None;
    loop {
        let page = stream_changed_torrents(pool, since, window_end, cursor.as_ref(), page_size)
            .await
            .context("streaming seal page")?;
        if page.is_empty() {
            break;
        }
        for row in &page {
            sinks.push_torrent(&row.info_hash.to_string(), row.files())?;
        }
        cursor = page.last().map(|r| r.info_hash);
    }
    for ih in deleted {
        sinks.push_deleted(ih)?;
    }
    sinks.finish_segment(dir)
}

/// Fold all current manifest segments of one tier. Manifest CAS failures are
/// intentionally propagated: fold jobs are content-preserving and idempotent
/// to rerun.
pub fn run_fold(layout: &Layout, tier: u8, preferred_version: u64) -> Result<FoldOutcome> {
    layout.ensure_dirs()?;
    let Some(manifest) = manifest::read_manifest(layout)? else {
        return Ok(FoldOutcome {
            acted: false,
            input_count: 0,
            output: None,
            manifest_mver: None,
        });
    };
    let Some((start, count)) = select_tier_run(&manifest, tier)? else {
        return Ok(FoldOutcome {
            acted: false,
            input_count: 0,
            output: None,
            manifest_mver: Some(manifest.mver),
        });
    };

    let inputs = manifest.segments[start..start + count].to_vec();
    let output_version =
        choose_replacement_version(layout, &manifest, start, count, preferred_version)?;
    let output = SegmentEntry {
        version: output_version,
        from: inputs.first().context("fold input missing first")?.from,
        to: inputs.last().context("fold input missing last")?.to,
        tier: tier.checked_add(1).context("fold tier overflowed u8")?,
    };
    let output_dir = create_version_dir_exclusive(layout, Kind::Segment, output.version)?;
    let layers: Vec<FoldLayer> = inputs.iter().map(|s| segment_layer(layout, s)).collect();
    materialize_layers(&layers, &output_dir, true)?;
    layout.publish_segment_dir(&output_dir)?;

    let next = replace_segment_run(&manifest, start, count, output.clone())?;
    write_manifest_cas(layout, Some(manifest.mver), &next)?;
    Ok(FoldOutcome {
        acted: true,
        input_count: count,
        output: Some(output),
        manifest_mver: Some(next.mver),
    })
}

/// Fold base + all sealed segments into a new base generation.
pub fn run_merge_base(layout: &Layout, preferred_version: u64) -> Result<MergeBaseOutcome> {
    layout.ensure_dirs()?;
    let Some(manifest) = manifest::read_manifest(layout)? else {
        return Ok(MergeBaseOutcome {
            acted: false,
            input_count: 0,
            base: None,
            manifest_mver: None,
        });
    };
    if manifest.segments.is_empty() {
        return Ok(MergeBaseOutcome {
            acted: false,
            input_count: 0,
            base: Some(manifest.base),
            manifest_mver: Some(manifest.mver),
        });
    }

    let base_dir = layout
        .resolve_current(Kind::Base)
        .context("merge-base requires a current base")?;
    let mut layers = vec![FoldLayer {
        fact: base_dir.join(artifact::FACT),
        agg_torrent_ext: base_dir.join(artifact::AGG_TORRENT_EXT),
        tombstones: None,
    }];
    layers.extend(manifest.segments.iter().map(|s| segment_layer(layout, s)));

    let version = choose_base_version(layout, preferred_version)?;
    let output_dir = create_version_dir_exclusive(layout, Kind::Base, version)?;
    materialize_layers(&layers, &output_dir, false)?;
    // Until the manifest CAS below succeeds, readers can briefly combine this
    // new base with the old manifest. That over-covers the merged rows through
    // both base and old segments, but does not under-cover; CAS retry removes
    // only the merged input segment versions and preserves later seals.
    layout.publish(Kind::Base, &output_dir)?;

    let next_base = merged_base_entry(&manifest, version);
    let merged_input_versions: Vec<u64> = manifest.segments.iter().map(|s| s.version).collect();
    let next =
        publish_merged_base_manifest(layout, &manifest, next_base.clone(), &merged_input_versions)?;
    Ok(MergeBaseOutcome {
        acted: true,
        input_count: layers.len(),
        base: Some(next_base),
        manifest_mver: Some(next.mver),
    })
}

fn publish_merged_base_manifest(
    layout: &Layout,
    started_manifest: &Manifest,
    next_base: BaseEntry,
    merged_input_versions: &[u64],
) -> Result<Manifest> {
    publish_merged_base_manifest_inner(
        layout,
        started_manifest,
        next_base,
        merged_input_versions,
        |_, _, _, _| Ok(()),
    )
}

fn publish_merged_base_manifest_inner(
    layout: &Layout,
    started_manifest: &Manifest,
    next_base: BaseEntry,
    merged_input_versions: &[u64],
    mut before_manifest_cas: impl FnMut(usize, &Layout, Option<u64>, &Manifest) -> Result<()>,
) -> Result<Manifest> {
    let merged_input_versions: HashSet<u64> = merged_input_versions.iter().copied().collect();
    let mut attempt = 1;
    let mut expected_mver = Some(started_manifest.mver);
    let mut next = Manifest::new(started_manifest.mver + 1, next_base.clone(), Vec::new())?;

    loop {
        before_manifest_cas(attempt, layout, expected_mver, &next)?;
        match write_manifest_cas(layout, expected_mver, &next) {
            Ok(()) => return Ok(next),
            Err(err)
                if attempt < MANIFEST_CAS_ATTEMPTS
                    && err.downcast_ref::<manifest::ManifestCasError>().is_some() =>
            {
                let current = manifest::read_manifest(layout)?
                    .context("manifest disappeared during merge-base CAS retry")?;
                next =
                    merge_base_retry_manifest(&current, next_base.clone(), &merged_input_versions)?;
                expected_mver = Some(current.mver);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

fn merge_base_retry_manifest(
    current: &Manifest,
    next_base: BaseEntry,
    merged_input_versions: &HashSet<u64>,
) -> Result<Manifest> {
    let preserved: Vec<SegmentEntry> = current
        .segments
        .iter()
        .filter(|s| !merged_input_versions.contains(&s.version))
        .cloned()
        .collect();
    if let Some(first) = preserved.first() {
        if first.from != next_base.cut {
            anyhow::bail!(
                "merge-base CAS retry cannot preserve segments: first preserved segment starts at {}, expected new base cut {}",
                first.from,
                next_base.cut
            );
        }
    }
    Manifest::new(current.mver + 1, next_base, preserved)
}

fn select_tier_run(manifest: &Manifest, tier: u8) -> Result<Option<(usize, usize)>> {
    let indexes: Vec<usize> = manifest
        .segments
        .iter()
        .enumerate()
        .filter_map(|(i, s)| (s.tier == tier).then_some(i))
        .collect();
    if indexes.len() < 2 {
        return Ok(None);
    }
    let start = indexes[0];
    let end = indexes[indexes.len() - 1] + 1;
    if end - start != indexes.len() {
        anyhow::bail!("tier {tier} segments are not one contiguous manifest run");
    }
    Ok(Some((start, indexes.len())))
}

/// Return a manifest copy with one contiguous segment run replaced by `output`.
pub fn replace_segment_run(
    manifest: &Manifest,
    start: usize,
    count: usize,
    output: SegmentEntry,
) -> Result<Manifest> {
    if count == 0 || start + count > manifest.segments.len() {
        anyhow::bail!("invalid segment replacement range");
    }
    let mut segments = manifest.segments.clone();
    segments.splice(start..start + count, [output]);
    Manifest::new(manifest.mver + 1, manifest.base.clone(), segments)
}

fn choose_replacement_version(
    layout: &Layout,
    manifest: &Manifest,
    start: usize,
    count: usize,
    preferred: u64,
) -> Result<u64> {
    let lower = if start == 0 {
        manifest.base.version
    } else {
        manifest.segments[start - 1].version
    };
    let upper = manifest.segments.get(start + count).map(|s| s.version);
    let last_input = manifest.segments[start + count - 1].version;
    if let Some(upper) = upper {
        let mut candidate = last_input.saturating_add(1).max(lower.saturating_add(1));
        if candidate >= upper {
            candidate = lower.saturating_add(1);
        }
        while candidate < upper {
            if !segment_version_exists(layout, candidate) {
                return Ok(candidate);
            }
            candidate = candidate.saturating_add(1);
        }
        anyhow::bail!(
            "no free segment version between v{} and v{} for fold replacement",
            lower,
            upper
        );
    }
    let mut candidate = preferred
        .max(last_input.saturating_add(1))
        .max(lower.saturating_add(1));
    while segment_version_exists(layout, candidate) {
        candidate = candidate.saturating_add(1);
    }
    Ok(candidate)
}

fn choose_base_version(layout: &Layout, preferred: u64) -> Result<u64> {
    let mut candidate = preferred;
    if let Some(current) = layout.resolve_current(Kind::Base) {
        if let Some(v) = current
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(crate::generation::parse_numeric_version_name)
        {
            candidate = candidate.max(v.saturating_add(1));
        }
    }
    while layout
        .kind_dir(Kind::Base)
        .join(format!("v{candidate}"))
        .exists()
    {
        candidate = candidate.saturating_add(1);
    }
    Ok(candidate)
}

fn segment_version_exists(layout: &Layout, version: u64) -> bool {
    layout.segment_version_dir(version).exists()
}

fn manifest_references_segment(manifest: Option<&Manifest>, version: u64) -> bool {
    manifest
        .map(|m| m.segments.iter().any(|s| s.version == version))
        .unwrap_or(false)
}

fn ensure_empty_dir(dir: &Path) -> Result<()> {
    let mut entries = fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    if entries.next().transpose()?.is_some() {
        anyhow::bail!("segment version dir is not empty: {}", dir.display());
    }
    Ok(())
}

fn merged_base_entry(manifest: &Manifest, version: u64) -> BaseEntry {
    BaseEntry {
        version,
        cut: manifest.cut(),
    }
}

fn segment_layer(layout: &Layout, s: &SegmentEntry) -> FoldLayer {
    let dir = layout.segment_version_dir(s.version);
    FoldLayer {
        fact: dir.join(artifact::FACT),
        agg_torrent_ext: dir.join(artifact::AGG_TORRENT_EXT),
        tombstones: Some(dir.join(artifact::TOMBSTONES)),
    }
}

/// Build the rank-aware SELECT for one folded artifact.
pub fn layered_select_sql(layers: &[FoldLayer], artifact: LayeredArtifact) -> Result<String> {
    if layers.is_empty() {
        anyhow::bail!("layered SELECT requires at least one layer");
    }
    let columns = match artifact {
        LayeredArtifact::Fact => "info_hash, file_index, path, extension, size, is_padding",
        LayeredArtifact::AggTorrentExt => "info_hash, extension, file_count, total_size, max_size",
    };
    let mut selects = Vec::with_capacity(layers.len());
    for (i, layer) in layers.iter().enumerate() {
        let path = match artifact {
            LayeredArtifact::Fact => &layer.fact,
            LayeredArtifact::AggTorrentExt => &layer.agg_torrent_ext,
        };
        let alias = format!("l{i}");
        let mut select = format!(
            "SELECT {columns} FROM read_parquet('{}') AS {alias}",
            sql_path(path)
        );
        if let Some(pred) = newer_tombstone_predicate(layers, i, &alias) {
            select.push_str(" WHERE ");
            select.push_str(&pred);
        }
        selects.push(select);
    }
    Ok(selects.join("\nUNION ALL\n"))
}

/// Build the `UNION DISTINCT` tombstone SELECT for a folded segment.
pub fn tombstone_union_select_sql(layers: &[FoldLayer]) -> Result<String> {
    let tombs: Vec<&PathBuf> = layers
        .iter()
        .filter_map(|l| l.tombstones.as_ref())
        .collect();
    if tombs.is_empty() {
        anyhow::bail!("tombstone union requires at least one tombstone layer");
    }
    let selects: Vec<String> = tombs
        .iter()
        .map(|p| format!("SELECT info_hash FROM read_parquet('{}')", sql_path(p)))
        .collect();
    Ok(format!(
        "SELECT DISTINCT info_hash FROM (\n{}\n) AS all_tombstones",
        selects.join("\nUNION ALL\n")
    ))
}

fn newer_tombstone_predicate(
    layers: &[FoldLayer],
    layer_index: usize,
    alias: &str,
) -> Option<String> {
    let tombs: Vec<&PathBuf> = layers[layer_index + 1..]
        .iter()
        .filter_map(|l| l.tombstones.as_ref())
        .collect();
    if tombs.is_empty() {
        return None;
    }
    let selects: Vec<String> = tombs
        .iter()
        .map(|p| format!("SELECT info_hash FROM read_parquet('{}')", sql_path(p)))
        .collect();
    Some(format!(
        "NOT EXISTS (SELECT 1 FROM (\n{}\n) AS newer_tombstones WHERE newer_tombstones.info_hash = {alias}.info_hash)",
        selects.join("\nUNION ALL\n")
    ))
}

#[cfg(feature = "duckdb-sort")]
fn copy_fact_sql(layers: &[FoldLayer], out: &Path) -> Result<String> {
    Ok(format!(
        "COPY (
SELECT * FROM (
{}
) AS folded_fact
ORDER BY extension ASC NULLS LAST, size ASC
) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);",
        layered_select_sql(layers, LayeredArtifact::Fact)?,
        sql_path(out)
    ))
}

#[cfg(feature = "duckdb-sort")]
fn copy_agg_torrent_ext_sql(layers: &[FoldLayer], out: &Path) -> Result<String> {
    Ok(format!(
        "COPY (
SELECT * FROM (
{}
) AS folded_agg_torrent_ext
) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);",
        layered_select_sql(layers, LayeredArtifact::AggTorrentExt)?,
        sql_path(out)
    ))
}

#[cfg(feature = "duckdb-sort")]
fn copy_tombstones_sql(layers: &[FoldLayer], out: &Path) -> Result<String> {
    Ok(format!(
        "COPY (
{}
) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);",
        tombstone_union_select_sql(layers)?,
        sql_path(out)
    ))
}

#[cfg(feature = "duckdb-sort")]
fn materialize_layers(layers: &[FoldLayer], out_dir: &Path, write_tombstones: bool) -> Result<()> {
    let conn = duckdb::Connection::open_in_memory().context("opening duckdb for segment fold")?;
    conn.execute_batch("SET preserve_insertion_order=false;")
        .context("configuring duckdb for segment fold")?;
    conn.execute_batch(&copy_fact_sql(layers, &out_dir.join(artifact::FACT))?)
        .context("folding fact artifact")?;
    conn.execute_batch(&copy_agg_torrent_ext_sql(
        layers,
        &out_dir.join(artifact::AGG_TORRENT_EXT),
    )?)
    .context("folding agg_torrent_ext artifact")?;
    if write_tombstones {
        conn.execute_batch(&copy_tombstones_sql(
            layers,
            &out_dir.join(artifact::TOMBSTONES),
        )?)
        .context("folding tombstone artifact")?;
    }
    Ok(())
}

#[cfg(not(feature = "duckdb-sort"))]
fn materialize_layers(
    _layers: &[FoldLayer],
    _out_dir: &Path,
    _write_tombstones: bool,
) -> Result<()> {
    anyhow::bail!(
        "fold and merge-base require the `duckdb-sort` feature (the default workspace build stays DuckDB-free)"
    )
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{BaseEntry, Manifest, ManifestCasError};
    use std::fs;

    fn tmp(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("bmp-seal-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn manifest_with_segments(segments: Vec<SegmentEntry>) -> Manifest {
        Manifest::new(
            1,
            BaseEntry {
                version: 100,
                cut: 100,
            },
            segments,
        )
        .unwrap()
    }

    #[test]
    fn min_torrents_skip_rule_matches_spec() {
        assert!(should_skip_seal(49, 3_600, 50, 86_400));
        assert!(!should_skip_seal(50, 3_600, 50, 86_400));
        assert!(!should_skip_seal(0, 86_400, 50, 86_400));
    }

    #[test]
    fn seal_publish_protocol_order_is_observable() {
        let layout = Layout::new(tmp("publish-order"));
        layout.ensure_dirs().unwrap();
        let base = layout.new_version_dir(Kind::Base, "100").unwrap();
        fs::write(base.join(artifact::FACT), b"x").unwrap();
        layout.publish(Kind::Base, &base).unwrap();
        layout.write_watermark(100).unwrap();
        let initial = manifest::bootstrap_from_layout(&layout, 1, 100).unwrap();
        manifest::write_manifest_cas(&layout, None, &initial).unwrap();

        let seg_dir = layout.new_version_dir(Kind::Segment, "130").unwrap();
        fs::write(seg_dir.join(artifact::FACT), b"x").unwrap();
        let segment = SegmentEntry {
            version: 130,
            from: 100,
            to: 130,
            tier: 0,
        };
        let mut steps = Vec::new();
        publish_sealed_segment_with_observer(&layout, &seg_dir, segment, |step, layout| {
            match step {
                SealPublishStep::SegmentDirSynced => {
                    assert!(seg_dir.is_dir());
                    assert_eq!(
                        manifest::read_manifest(layout)
                            .unwrap()
                            .unwrap()
                            .segments
                            .len(),
                        0
                    );
                    assert_eq!(layout.read_watermark(), 100);
                }
                SealPublishStep::ManifestWritten => {
                    assert_eq!(
                        manifest::read_manifest(layout)
                            .unwrap()
                            .unwrap()
                            .segments
                            .len(),
                        1
                    );
                    assert_eq!(layout.read_watermark(), 100);
                }
                SealPublishStep::WatermarkWritten => {
                    assert_eq!(
                        manifest::read_manifest(layout)
                            .unwrap()
                            .unwrap()
                            .segments
                            .len(),
                        1
                    );
                    assert_eq!(layout.read_watermark(), 130);
                }
            }
            steps.push(step);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            steps,
            vec![
                SealPublishStep::SegmentDirSynced,
                SealPublishStep::ManifestWritten,
                SealPublishStep::WatermarkWritten
            ]
        );
    }

    #[tokio::test]
    async fn run_seal_stale_since_errors_before_segment_dir_created() {
        let layout = Layout::new(tmp("stale-since"));
        layout.ensure_dirs().unwrap();
        let manifest = manifest_with_segments(vec![SegmentEntry {
            version: 130,
            from: 100,
            to: 130,
            tier: 0,
        }]);
        manifest::write_manifest_cas(&layout, None, &manifest).unwrap();
        let pool =
            bitmagnet_db::PgPool::connect_lazy("postgres://postgres@localhost/bitmagnet").unwrap();

        let err = run_seal(&pool, &layout, 120, 140, &[], 100, 1, 0)
            .await
            .unwrap_err();

        assert!(err.downcast_ref::<StaleSealSinceError>().is_some());
        assert!(layout.list_version_dirs(Kind::Segment).unwrap().is_empty());
    }

    #[test]
    fn seal_publish_retries_manifest_cas_after_concurrent_fold() {
        let layout = Layout::new(tmp("publish-cas-retry"));
        layout.ensure_dirs().unwrap();
        layout.write_watermark(120).unwrap();
        let initial = manifest_with_segments(vec![
            SegmentEntry {
                version: 110,
                from: 100,
                to: 110,
                tier: 0,
            },
            SegmentEntry {
                version: 120,
                from: 110,
                to: 120,
                tier: 0,
            },
        ]);
        manifest::write_manifest_cas(&layout, None, &initial).unwrap();

        let seg_dir = create_version_dir_exclusive(&layout, Kind::Segment, 130).unwrap();
        let segment = SegmentEntry {
            version: 130,
            from: 120,
            to: 130,
            tier: 0,
        };
        let mut injected_fold = false;
        let publish = publish_sealed_segment_inner(
            &layout,
            &seg_dir,
            segment,
            |_, _| Ok(()),
            |attempt, layout, expected_mver, stale_next| {
                if attempt == 1 && !injected_fold {
                    assert_eq!(expected_mver, Some(1));
                    let current = manifest::read_manifest(layout).unwrap().unwrap();
                    let folded = replace_segment_run(
                        &current,
                        0,
                        2,
                        SegmentEntry {
                            version: 125,
                            from: 100,
                            to: 120,
                            tier: 1,
                        },
                    )
                    .unwrap();
                    write_manifest_cas(layout, Some(current.mver), &folded).unwrap();

                    let err = write_manifest_cas(layout, expected_mver, stale_next).unwrap_err();
                    assert!(err.downcast_ref::<ManifestCasError>().is_some());
                    injected_fold = true;
                }
                Ok(())
            },
        )
        .unwrap();

        let final_manifest = manifest::read_manifest(&layout).unwrap().unwrap();
        assert_eq!(publish.manifest_mver, 3);
        assert_eq!(layout.read_watermark(), 130);
        assert_eq!(
            final_manifest.segments,
            vec![
                SegmentEntry {
                    version: 125,
                    from: 100,
                    to: 120,
                    tier: 1,
                },
                SegmentEntry {
                    version: 130,
                    from: 120,
                    to: 130,
                    tier: 0,
                },
            ]
        );
        assert!(injected_fold);
    }

    #[test]
    fn seal_version_skips_existing_manifest_referenced_dir() {
        let layout = Layout::new(tmp("seal-version-skip"));
        layout.ensure_dirs().unwrap();
        let occupied = layout.new_version_dir(Kind::Segment, "130").unwrap();
        fs::write(occupied.join("sentinel"), b"keep").unwrap();
        let manifest = manifest_with_segments(vec![SegmentEntry {
            version: 130,
            from: 100,
            to: 120,
            tier: 1,
        }]);
        manifest::write_manifest_cas(&layout, None, &manifest).unwrap();

        let version = choose_seal_segment_version(&layout, Some(&manifest), 130).unwrap();
        let dir = create_version_dir_exclusive(&layout, Kind::Segment, version).unwrap();

        assert_eq!(version, 131);
        assert!(dir.ends_with("v131"));
        assert_eq!(fs::read(occupied.join("sentinel")).unwrap(), b"keep");
    }

    #[test]
    fn fold_output_version_skips_preexisting_non_empty_candidate_dir() {
        let layout = Layout::new(tmp("fold-output-collision"));
        layout.ensure_dirs().unwrap();
        let occupied = layout.new_version_dir(Kind::Segment, "121").unwrap();
        fs::write(occupied.join("sentinel"), b"occupied").unwrap();
        let manifest = manifest_with_segments(vec![
            SegmentEntry {
                version: 110,
                from: 100,
                to: 110,
                tier: 0,
            },
            SegmentEntry {
                version: 120,
                from: 110,
                to: 120,
                tier: 0,
            },
        ]);

        let version = choose_replacement_version(&layout, &manifest, 0, 2, 121).unwrap();
        let output_dir = create_version_dir_exclusive(&layout, Kind::Segment, version).unwrap();

        assert_eq!(version, 122);
        assert!(output_dir.ends_with("v122"));
        assert_eq!(fs::read(occupied.join("sentinel")).unwrap(), b"occupied");
        assert!(fs::read_dir(output_dir).unwrap().next().is_none());
    }

    #[test]
    fn seal_refuses_non_empty_segment_dir_before_writers() {
        let layout = Layout::new(tmp("seal-non-empty"));
        layout.ensure_dirs().unwrap();
        let dir = layout.new_version_dir(Kind::Segment, "130").unwrap();
        fs::write(dir.join("sentinel"), b"occupied").unwrap();

        let err = ensure_empty_dir(&dir).unwrap_err();
        assert!(format!("{err:#}").contains("not empty"));
    }

    #[test]
    fn watermark_monotonic_guard_does_not_regress() {
        let layout = Layout::new(tmp("wm-guard"));
        layout.ensure_dirs().unwrap();
        assert!(layout.write_watermark_monotonic(200).unwrap());
        assert!(!layout.write_watermark_monotonic(199).unwrap());
        assert_eq!(layout.read_watermark(), 200);
        assert!(!layout.write_watermark_monotonic(200).unwrap());
    }

    #[test]
    fn reconcile_advances_watermark_to_manifest_cut_after_crash() {
        let layout = Layout::new(tmp("reconcile"));
        layout.ensure_dirs().unwrap();
        layout.write_watermark(100).unwrap();
        let m = manifest_with_segments(vec![SegmentEntry {
            version: 130,
            from: 100,
            to: 130,
            tier: 0,
        }]);
        manifest::write_manifest_cas(&layout, None, &m).unwrap();

        assert_eq!(reconcile_watermark_with_manifest(&layout).unwrap(), 130);
        assert_eq!(layout.read_watermark(), 130);
    }

    #[test]
    fn layered_rank_rule_mentions_only_newer_tombstones() {
        let layers = vec![
            FoldLayer {
                fact: "seg/v1/fact.parquet".into(),
                agg_torrent_ext: "seg/v1/agg_torrent_ext.parquet".into(),
                tombstones: Some("seg/v1/tombstones.parquet".into()),
            },
            FoldLayer {
                fact: "seg/v2/fact.parquet".into(),
                agg_torrent_ext: "seg/v2/agg_torrent_ext.parquet".into(),
                tombstones: Some("seg/v2/tombstones.parquet".into()),
            },
            FoldLayer {
                fact: "seg/v3/fact.parquet".into(),
                agg_torrent_ext: "seg/v3/agg_torrent_ext.parquet".into(),
                tombstones: Some("seg/v3/tombstones.parquet".into()),
            },
        ];
        let sql = layered_select_sql(&layers, LayeredArtifact::Fact).unwrap();
        let l1_pos = sql.find("seg/v1/fact.parquet").unwrap();
        let t2_pos = sql.find("seg/v2/tombstones.parquet").unwrap();
        let t3_pos = sql.find("seg/v3/tombstones.parquet").unwrap();
        let l3_pos = sql.find("seg/v3/fact.parquet").unwrap();
        assert!(l1_pos < t2_pos);
        assert!(l1_pos < t3_pos);
        assert!(l3_pos > t3_pos);
        assert_eq!(sql.matches("NOT EXISTS").count(), 2);
    }

    #[test]
    fn rank_rule_fixture_keeps_only_newest_changed_hash() {
        // A changed torrent is present in each layer's fact and tombstone set.
        // The fold rule keeps a layer only when no newer tombstone mentions the
        // hash, so only the newest rank survives.
        let fact_has_hash = [true, true, true];
        let tombstone_has_hash = [true, true, true];
        let visible: Vec<usize> = fact_has_hash
            .iter()
            .enumerate()
            .filter_map(|(i, has_row)| {
                let hidden_by_newer = tombstone_has_hash[i + 1..].iter().any(|v| *v);
                (*has_row && !hidden_by_newer).then_some(i)
            })
            .collect();
        assert_eq!(visible, vec![2]);
    }

    #[test]
    fn tombstone_union_uses_distinct_all_inputs() {
        let layers = vec![
            FoldLayer {
                fact: "a/fact.parquet".into(),
                agg_torrent_ext: "a/att.parquet".into(),
                tombstones: Some("a/tombstones.parquet".into()),
            },
            FoldLayer {
                fact: "b/fact.parquet".into(),
                agg_torrent_ext: "b/att.parquet".into(),
                tombstones: Some("b/tombstones.parquet".into()),
            },
        ];
        let sql = tombstone_union_select_sql(&layers).unwrap();
        assert!(sql.contains("SELECT DISTINCT info_hash"));
        assert!(sql.contains("a/tombstones.parquet"));
        assert!(sql.contains("b/tombstones.parquet"));
    }

    #[test]
    fn manifest_line_replacement_for_fold() {
        let m = manifest_with_segments(vec![
            SegmentEntry {
                version: 110,
                from: 100,
                to: 110,
                tier: 0,
            },
            SegmentEntry {
                version: 120,
                from: 110,
                to: 120,
                tier: 0,
            },
            SegmentEntry {
                version: 130,
                from: 120,
                to: 130,
                tier: 1,
            },
        ]);
        let next = replace_segment_run(
            &m,
            0,
            2,
            SegmentEntry {
                version: 125,
                from: 100,
                to: 120,
                tier: 1,
            },
        )
        .unwrap();
        assert_eq!(next.mver, 2);
        assert_eq!(next.segments.len(), 2);
        assert_eq!(next.segments[0].from, 100);
        assert_eq!(next.segments[0].to, 120);
        assert_eq!(next.segments[0].tier, 1);
        assert_eq!(next.segments[1].version, 130);
    }

    #[test]
    fn cas_error_type_is_retryable_for_callers() {
        let err: anyhow::Error = ManifestCasError {
            expected: Some(1),
            actual: Some(2),
        }
        .into();
        assert!(err.downcast_ref::<ManifestCasError>().is_some());
    }

    #[test]
    fn merge_base_manifest_cas_retry_preserves_post_merge_seal() {
        let layout = Layout::new(tmp("merge-base-cas-retry"));
        layout.ensure_dirs().unwrap();
        let started = manifest_with_segments(vec![
            SegmentEntry {
                version: 110,
                from: 100,
                to: 110,
                tier: 0,
            },
            SegmentEntry {
                version: 120,
                from: 110,
                to: 120,
                tier: 0,
            },
        ]);
        manifest::write_manifest_cas(&layout, None, &started).unwrap();
        let next_base = BaseEntry {
            version: 125,
            cut: 120,
        };
        let input_versions = vec![110, 120];
        let mut injected_seal = false;

        let next = publish_merged_base_manifest_inner(
            &layout,
            &started,
            next_base.clone(),
            &input_versions,
            |attempt, layout, expected_mver, _stale_next| {
                if attempt == 1 && !injected_seal {
                    assert_eq!(expected_mver, Some(1));
                    let current = manifest::read_manifest(layout).unwrap().unwrap();
                    let mut segments = current.segments.clone();
                    segments.push(SegmentEntry {
                        version: 130,
                        from: 120,
                        to: 140,
                        tier: 0,
                    });
                    let concurrent = Manifest::new(current.mver + 1, current.base, segments)
                        .expect("concurrent seal manifest validates");
                    write_manifest_cas(layout, Some(current.mver), &concurrent).unwrap();
                    injected_seal = true;
                }
                Ok(())
            },
        )
        .unwrap();

        assert!(injected_seal);
        assert_eq!(next.mver, 3);
        assert_eq!(next.base, next_base);
        assert_eq!(
            next.segments,
            vec![SegmentEntry {
                version: 130,
                from: 120,
                to: 140,
                tier: 0,
            }]
        );
        next.validate().unwrap();
        assert_eq!(manifest::read_manifest(&layout).unwrap(), Some(next));
    }

    #[test]
    fn merge_base_uses_manifest_cut_when_watermark_lags() {
        let layout = Layout::new(tmp("merge-cut"));
        layout.ensure_dirs().unwrap();
        layout.write_watermark(110).unwrap();
        let manifest = manifest_with_segments(vec![
            SegmentEntry {
                version: 120,
                from: 100,
                to: 120,
                tier: 0,
            },
            SegmentEntry {
                version: 130,
                from: 120,
                to: 130,
                tier: 0,
            },
        ]);

        let next_base = merged_base_entry(&manifest, 200);

        assert_eq!(layout.read_watermark(), 110);
        assert_eq!(next_base.cut, 130);
        assert_eq!(next_base.version, 200);
    }
}
