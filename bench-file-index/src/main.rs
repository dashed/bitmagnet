//! `bench-file-index` — standalone smoke harness for the file-grained Tantivy
//! index (RUN-0b / RUN-4). Builds the §4.1 file schema from decoded blobs WITHOUT
//! the Phase-B `file_schema`/`backfill_files` code, measures on-disk bytes per
//! segment component, and times SearchFiles `ext ∧ size` queries.
//!
//! Sources:
//!   * `--source synthetic` (default) — deterministic fake torrents, no DB. The
//!     smoke/self-test source (numbers are NOT representative; structure is).
//!   * `--source postgres`  — real data via `stream_torrents_with_files` against
//!     the HEL1 restore (gated; the real RUN-4 source).
//!
//! Design only authorizes the synthetic smoke now; postgres runs are gated.
//! See docs/dev/file-index-size-latency-bench-spec.md.

mod schema;

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::{
    BooleanQuery, EmptyQuery, Occur, PhraseQuery, Query, RangeQuery, TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption, Value};
use tantivy::{Index, IndexWriter, Order, TantivyDocument, Term};

use bitmagnet_model::file_extension_from_path;

use schema::{
    build_file_schema, build_recall_schema, register_path_tokenizer, FileFields, PathTokenizer,
    RecallFields, Variant,
};

/// Writer heap — matches the shipped sidecar (`index.rs:15`) so merge/flush
/// behaviour transfers.
const WRITER_HEAP_BYTES: usize = 256 * 1024 * 1024;

/// Synthesized-denorm anchor: a fixed "now" (Date::now is unavailable / would
/// break determinism). `published_at` is drawn into [NOW-3y, NOW].
const SYNTH_NOW: i64 = 1_750_000_000;
const SYNTH_WINDOW: i64 = 3 * 365 * 24 * 3600;

/// Canonical content types for synthesized denorm (cardinality is what matters).
const CONTENT_TYPES: [&str; 10] = [
    "movie",
    "tv_show",
    "music",
    "software",
    "game",
    "book",
    "audiobook",
    "comic",
    "xxx",
    "unknown",
];

/// Representative extensions / size thresholds for the latency sweep.
const QUERY_EXTS: [&str; 10] = [
    "mkv", "mp4", "srt", "mp3", "jpg", "nfo", "flac", "avi", "iso", "pdf",
];
const QUERY_SIZES: [u64; 3] = [0, 100_000_000, 1_000_000_000];

#[derive(Parser, Debug)]
#[command(name = "bench-file-index", about = "File-grained Tantivy index smoke bench")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Build the index for one variant and report per-component bytes + timing.
    Build(BuildArgs),
    /// Open a built index and report SearchFiles p50/p95/p99 latency.
    Query(QueryArgs),
    /// EXP-D: build a PATH-ONLY index with a chosen tokenizer and measure CJK vs
    /// ASCII substring recall/precision/latency against in-process exact truth.
    Recall(RecallArgs),
    /// EXP-E: build a base index (default LogMergePolicy), append deltas, and
    /// measure commit→reload freshness lag, segment growth, query latency, and
    /// supersession (delete_term + re-add) cost.
    Freshness(FreshnessArgs),
    /// Open an ALREADY-BUILT path index and measure per-group cold-first +
    /// warm-rep query latency (no rebuild). Decouples the one-time full-corpus
    /// build from cheap repeatable latency measurement (the RUN-2 pattern).
    Pathquery(PathqueryArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Source {
    /// Deterministic fake torrents — smoke/self-test, no DB.
    Synthetic,
    /// Real torrents via the `files_data` blob (`stream_torrents_with_files`).
    /// Only valid post-blob-backfill (blobs populated).
    Postgres,
    /// Real files directly from the `torrent_files` table (one row = one file
    /// doc). The source for a PRE-blob-backfill dump where blobs are empty but
    /// `torrent_files` is fully present. Extension is re-derived from the path
    /// (G1) — byte-identical content to the blob path.
    TorrentFiles,
}

/// PS-T3 micro-bench: document granularity for the `recall` path index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Granularity {
    /// One Tantivy doc per file (~873 M @ full corpus) — the EXP-D/D2 baseline.
    PerFile,
    /// One "path-bag" doc per torrent (~17 M @ full corpus): all of a torrent's
    /// file paths added as separate values of the one `path` field (each value
    /// tokenized independently → no cross-file boundary grams). Identity + delete
    /// key = info_hash only. Postings shrink with torrent-document-frequency; the
    /// bench measures by how much (G5 size) and whether broad-prefix latency
    /// clears `<50 ms` (G3). Consecutive `torrent_files` rows are grouped by
    /// info_hash, relying on the keyset scan's `ORDER BY info_hash, index`.
    PerTorrent,
}

#[derive(Parser, Debug)]
struct BuildArgs {
    /// Schema variant (V1..V11), or "all" to build every variant in turn.
    #[arg(long, default_value = "V10")]
    variant: String,
    /// Directory for the index (wiped + recreated per variant).
    #[arg(long, default_value = "/tmp/bench-file-index")]
    index_path: PathBuf,
    /// Stop after emitting this many file docs.
    #[arg(long, default_value_t = 100_000)]
    limit_docs: u64,
    /// Commit cadence (file docs).
    #[arg(long, default_value_t = 50_000)]
    commit_interval: u64,
    /// Source of torrents.
    #[arg(long, value_enum, default_value_t = Source::Synthetic)]
    source: Source,
    /// Postgres DSN (else BITMAGNET_POSTGRES_* env). Only for --source postgres.
    #[arg(long, default_value = "")]
    postgres_dsn: String,
    /// Page size for the postgres keyset scan.
    #[arg(long, default_value_t = 2000)]
    batch_size: i64,
    /// Force-merge to a single segment before measuring (production-like).
    #[arg(long, default_value_t = true)]
    merge: bool,
}

#[derive(Parser, Debug)]
struct QueryArgs {
    #[arg(long, default_value = "V10")]
    variant: String,
    #[arg(long, default_value = "/tmp/bench-file-index")]
    index_path: PathBuf,
    /// Number of randomized queries for scenario A (file-level, the <50ms claim).
    #[arg(long, default_value_t = 500)]
    iters: usize,
    /// Scenario B (collapse) iterations — bounded separately because B does a
    /// full match-set scan + per-doc stored read (worst-case; seconds-scale).
    #[arg(long, default_value_t = 30)]
    iters_b: usize,
}

#[derive(Parser, Debug)]
struct RecallArgs {
    /// Path tokenizer to build the path-only index with.
    #[arg(long, value_enum, default_value_t = PathTokenizer::Ngram)]
    tokenizer: PathTokenizer,
    /// PS-T3 micro-bench: per-file (one doc/file) or per-torrent (one path-bag
    /// doc/torrent). per-torrent counts `--limit-docs` in TORRENTS, not files.
    #[arg(long, value_enum, default_value_t = Granularity::PerFile)]
    granularity: Granularity,
    /// TSV file of `group<TAB>query` lines (a `#` header / `#`-prefixed lines are
    /// skipped). `group` is `cjk` or `ascii`; results are reported per group.
    #[arg(long)]
    queries_file: PathBuf,
    /// Number of real file docs to stream + index (the truth covers the same N).
    #[arg(long, default_value_t = 50_000_000)]
    limit_docs: u64,
    /// Directory for the path-only index (wiped + recreated).
    #[arg(long, default_value = "/tmp/bench-recall-index")]
    index_path: PathBuf,
    /// Source of docs (synthetic | torrent-files; postgres-blob not supported).
    #[arg(long, value_enum, default_value_t = Source::TorrentFiles)]
    source: Source,
    /// Postgres DSN (else BITMAGNET_POSTGRES_* env). Only for --source torrent-files.
    #[arg(long, default_value = "")]
    postgres_dsn: String,
    /// Page size for the postgres keyset scan.
    #[arg(long, default_value_t = 2000)]
    batch_size: i64,
    /// Commit cadence (file docs).
    #[arg(long, default_value_t = 1_000_000)]
    commit_interval: u64,
    /// Per-query truth-set cap (identity hashes held in RAM). A query whose exact
    /// match count exceeds this is flagged SATURATED: recall is computed over the
    /// capped sample (a valid estimate) and precision is suppressed (would be a
    /// lower bound). 0 = uncapped. Default bounds RAM for broad ASCII controls.
    #[arg(long, default_value_t = 5_000_000)]
    truth_cap: usize,
    /// Writer worker threads. DEFAULT 1: a single thread = one big arena, so the
    /// ngram token explosion (which under the default ~8-way split starves each
    /// thread's 32 MB arena → flush every ~37k docs → worker death at scale) is
    /// avoided. Raise only for the cheap `default` tokenizer.
    #[arg(long, default_value_t = 1)]
    writer_threads: usize,
    /// Total writer memory arena (MiB), split across `--writer-threads`. tantivy
    /// caps per-thread budget at ~4 GiB; with 1 thread keep this < 4000. 2 GiB
    /// keeps ngram@50M to a handful of segments + a single clean force-merge.
    #[arg(long, default_value_t = 2000)]
    writer_heap_mb: usize,
    /// Ngram min length (only for --tokenizer ngram). 2 = spec.
    #[arg(long, default_value_t = 2)]
    ngram_min: usize,
    /// Ngram max length (only for --tokenizer ngram). 3 = spec bi/tri-gram; set 2
    /// for bigram-only (smaller index, lower precision on ≥3-char queries).
    #[arg(long, default_value_t = 3)]
    ngram_max: usize,
    /// Skip the O(160×N) in-process `path.contains` truth accumulation — needed
    /// at full 879.5 M scale where truth is intractable AND unnecessary (recall
    /// ratios are locked at 50 M). The run still builds the index, force-merges,
    /// reports path-field bytes, and runs each query ONCE for per-group avg hits
    /// + warm latency p50/p95/p99 — just no recall/precision. (Pair with the
    /// `pathquery` subcommand for repeatable cold/warm latency on the built index.)
    #[arg(long, default_value_t = false)]
    skip_truth: bool,
}

#[derive(Parser, Debug)]
struct FreshnessArgs {
    /// Base index size before the delta sweep.
    #[arg(long, default_value_t = 20_000_000)]
    base_docs: u64,
    /// Comma-separated delta sizes appended in turn (cumulative on the base).
    #[arg(long, default_value = "1000,10000,100000")]
    delta_sizes: String,
    /// Commit cadence within a delta (docs per commit — the processor's ~1/s).
    #[arg(long, default_value_t = 1000)]
    commit_batch: u64,
    /// Directory for the index (wiped + recreated). DEFAULT LogMergePolicy — the
    /// build does NOT set NoMergePolicy and does NOT force-merge.
    #[arg(long, default_value = "/tmp/bench-freshness-index")]
    index_path: PathBuf,
    /// Source of docs (synthetic | torrent-files).
    #[arg(long, value_enum, default_value_t = Source::TorrentFiles)]
    source: Source,
    /// Postgres DSN (else BITMAGNET_POSTGRES_* env). Only for --source torrent-files.
    #[arg(long, default_value = "")]
    postgres_dsn: String,
    /// Page size for the postgres keyset scan.
    #[arg(long, default_value_t = 2000)]
    batch_size: i64,
}

#[derive(Parser, Debug)]
struct PathqueryArgs {
    /// Directory of an already-built path index (from `recall`). NOT rebuilt.
    #[arg(long)]
    index_path: PathBuf,
    /// Tokenizer the index was built with — drives query construction (must
    /// match the build, since tokenizers are runtime state, not persisted).
    #[arg(long, value_enum, default_value_t = PathTokenizer::Ngram)]
    tokenizer: PathTokenizer,
    /// TSV `group<TAB>query` file (same format as `recall`).
    #[arg(long)]
    queries_file: PathBuf,
    /// Warm repetitions per query after the timed cold-first execution.
    #[arg(long, default_value_t = 15)]
    warm_reps: usize,
    /// Ngram min length (must match the build; only for --tokenizer ngram).
    #[arg(long, default_value_t = 2)]
    ngram_min: usize,
    /// Ngram max length (must match the build; only for --tokenizer ngram).
    #[arg(long, default_value_t = 3)]
    ngram_max: usize,
}

/// One torrent normalized across sources.
struct SrcTorrent {
    info_hash: [u8; 20],
    name: String,
    size: u64,
    single: bool,
    /// (file_index, path, size) — empty for single-file torrents.
    files: Vec<(u32, String, u64)>,
}

#[tokio::main]
async fn main() -> Result<()> {
    bitmagnet_common::init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build(args) => run_build(args).await,
        Cmd::Query(args) => run_query(args),
        Cmd::Recall(args) => run_recall(args).await,
        Cmd::Freshness(args) => run_freshness(args).await,
        Cmd::Pathquery(args) => run_pathquery(args),
    }
}

// ===========================================================================
// Build
// ===========================================================================

async fn run_build(args: BuildArgs) -> Result<()> {
    let variants: Vec<String> = if args.variant.eq_ignore_ascii_case("all") {
        Variant::all().into_iter().map(String::from).collect()
    } else {
        vec![args.variant.clone()]
    };

    for vname in variants {
        let variant = Variant::from_name(&vname)
            .with_context(|| format!("unknown variant {vname:?} (V1..V11 or all)"))?;
        let dir = args.index_path.join(variant.name);
        build_one(&variant, &dir, &args).await?;
    }
    Ok(())
}

async fn build_one(variant: &Variant, dir: &Path, args: &BuildArgs) -> Result<()> {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;

    let (schema, fields) = build_file_schema(variant);
    let index = Index::create_in_dir(dir, schema).context("create index")?;
    let mut writer: IndexWriter = index.writer(WRITER_HEAP_BYTES).context("writer")?;
    // Deterministic segment control: no background auto-merge racing the final
    // explicit merge. Segments accumulate (heap-flushed) then we merge to one.
    writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

    let mut docs: u64 = 0;
    let mut torrents: u64 = 0;
    let mut since_commit: u64 = 0;
    let start = Instant::now();

    let emit = |t: &SrcTorrent,
                    w: &IndexWriter,
                    docs: &mut u64,
                    since_commit: &mut u64|
     -> Result<bool> {
        for td in torrent_docs(variant, &fields, t) {
            w.add_document(td).context("add_document")?;
            *docs += 1;
            *since_commit += 1;
            if *docs >= args.limit_docs {
                return Ok(true);
            }
        }
        Ok(false)
    };

    match args.source {
        Source::Synthetic => {
            let mut i: u64 = 0;
            'outer: loop {
                let t = synth_torrent(i);
                torrents += 1;
                i += 1;
                if emit(&t, &writer, &mut docs, &mut since_commit)? {
                    break 'outer;
                }
                if since_commit >= args.commit_interval {
                    writer.commit().context("commit")?;
                    since_commit = 0;
                }
            }
        }
        Source::Postgres => {
            use bitmagnet_db::{connect, stream_torrents_with_files, DbConfig};
            let mut cfg = DbConfig::from_env().context("postgres config from env")?;
            if !args.postgres_dsn.is_empty() {
                cfg.dsn = args.postgres_dsn.clone();
            }
            let pool = connect(&cfg).await.context("connect postgres")?;
            let mut cursor: Option<bitmagnet_model::InfoHash> = None;
            'pages: loop {
                let page = stream_torrents_with_files(&pool, cursor.as_ref(), args.batch_size)
                    .await
                    .context("stream page")?;
                if page.is_empty() {
                    break;
                }
                for twb in &page {
                    let t = from_blob(twb)?;
                    torrents += 1;
                    cursor = Some(twb.info_hash.clone());
                    if emit(&t, &writer, &mut docs, &mut since_commit)? {
                        break 'pages;
                    }
                    if since_commit >= args.commit_interval {
                        writer.commit().context("commit")?;
                        since_commit = 0;
                    }
                }
            }
        }
        Source::TorrentFiles => {
            use bitmagnet_db::{connect, DbConfig};
            use sqlx::Row;
            let mut cfg = DbConfig::from_env().context("postgres config from env")?;
            if !args.postgres_dsn.is_empty() {
                cfg.dsn = args.postgres_dsn.clone();
            }
            let pool = connect(&cfg).await.context("connect postgres")?;
            // Keyset over the (info_hash, index) primary key. One row = one file
            // doc; extension re-derived from `path` (G1). `index` quoted (PG kw).
            const SQL: &str = "SELECT info_hash, \"index\", path, size \
                FROM torrent_files \
                WHERE ($1::bytea IS NULL OR (info_hash, \"index\") > ($1, $2)) \
                ORDER BY info_hash, \"index\" LIMIT $3";
            let mut cur: Option<(Vec<u8>, i32)> = None;
            'pages: loop {
                let rows = sqlx::query(SQL)
                    .bind(cur.as_ref().map(|(h, _)| h.clone()))
                    .bind(cur.as_ref().map_or(0, |(_, i)| *i))
                    .bind(args.batch_size)
                    .fetch_all(&pool)
                    .await
                    .context("torrent_files page")?;
                if rows.is_empty() {
                    break;
                }
                for row in &rows {
                    let ih_raw: Vec<u8> = row.try_get("info_hash")?;
                    let idx: i32 = row.try_get("index")?;
                    let path: String = row.try_get("path")?;
                    let size: i64 = row.try_get("size")?;
                    cur = Some((ih_raw.clone(), idx));
                    torrents += 1; // here = rows scanned, not torrents
                    if ih_raw.len() != 20 {
                        continue; // skip v2-only / malformed hashes
                    }
                    let mut ih = [0u8; 20];
                    ih.copy_from_slice(&ih_raw);
                    let (published, ct) = synth_denorm(&ih);
                    let td = build_doc(
                        variant,
                        &fields,
                        &ih,
                        idx as u32,
                        &path,
                        u64::try_from(size).unwrap_or(0),
                        published,
                        ct,
                    );
                    writer.add_document(td).context("add_document")?;
                    docs += 1;
                    since_commit += 1;
                    if docs >= args.limit_docs {
                        break 'pages;
                    }
                }
                if since_commit >= args.commit_interval {
                    writer.commit().context("commit")?;
                    since_commit = 0;
                }
            }
        }
    }

    writer.commit().context("final commit")?;
    let ingest = start.elapsed();

    let pre_merge_segs = index.searchable_segment_ids().map(|s| s.len()).unwrap_or(0);
    let merge_start = Instant::now();
    if args.merge {
        let ids = index.searchable_segment_ids().context("segment ids")?;
        if ids.len() > 1 {
            writer.merge(&ids).await.context("merge")?;
        }
    }
    writer
        .garbage_collect_files()
        .await
        .context("gc files")?;
    let merge_time = merge_start.elapsed();

    let segs = index.searchable_segment_ids().map(|s| s.len()).unwrap_or(0);
    let dps = docs as f64 / ingest.as_secs_f64().max(1e-9);
    println!(
        "\n=== {} | {docs} docs from {torrents} src-rows | ingest {:.1}s ({:.0} docs/s) | merge {:.1}s ({pre_merge_segs}→{segs} segs) ===",
        variant.name,
        ingest.as_secs_f64(),
        dps,
        merge_time.as_secs_f64(),
    );
    report_segment_bytes(dir, docs)?;
    Ok(())
}

/// One Tantivy doc per file (multi), or one synthetic doc (single). Mirrors §4.3
/// + the G1 fix (extension derived from PATH, never the blob's stored `e`).
fn torrent_docs(
    variant: &Variant,
    fields: &FileFields,
    t: &SrcTorrent,
) -> Vec<TantivyDocument> {
    let (published, ct) = synth_denorm(&t.info_hash);
    let mut out = Vec::new();
    if !t.files.is_empty() {
        for (idx, path, size) in &t.files {
            out.push(build_doc(variant, fields, &t.info_hash, *idx, path, *size, published, ct));
        }
    } else if t.single {
        // D5 single-file synthesis: ext from the torrent name, size = total.
        out.push(build_doc(variant, fields, &t.info_hash, 0, &t.name, t.size, published, ct));
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn build_doc(
    variant: &Variant,
    fields: &FileFields,
    info_hash: &[u8; 20],
    file_index: u32,
    path: &str,
    size: u64,
    published: i64,
    content_type: &str,
) -> TantivyDocument {
    let mut td = TantivyDocument::new();
    // Mandatory delete key.
    td.add_bytes(fields.info_hash, info_hash);

    if let Some(f) = fields.doc_id {
        td.add_text(f, format!("{}:{}", to_hex(info_hash), file_index));
    }
    if variant.identity_fast {
        if let Some(f) = fields.info_hash_fast {
            td.add_text(f, to_hex(info_hash));
        }
        if let Some(f) = fields.file_index_fast {
            td.add_u64(f, u64::from(file_index));
        }
    }
    // G1: extension from PATH, skip empty (mirrors indexer.rs:129-133).
    if let Some(f) = fields.extension {
        if let Some(ext) = file_extension_from_path(path) {
            if !ext.is_empty() {
                td.add_text(f, ext);
            }
        }
    }
    if let Some(f) = fields.size {
        td.add_u64(f, size);
    }
    if let Some(f) = fields.content_type {
        td.add_text(f, content_type);
    }
    if let Some(f) = fields.published_at {
        td.add_i64(f, published);
    }
    if let Some(f) = fields.path {
        td.add_text(f, path);
    }
    td
}

// ===========================================================================
// Sources
// ===========================================================================

/// Convert a real `TorrentWithBlob` into the normalized form (decode the blob).
fn from_blob(twb: &bitmagnet_db::TorrentWithBlob) -> Result<SrcTorrent> {
    let mut ih = [0u8; 20];
    let slice = twb.info_hash.as_slice();
    if slice.len() != 20 {
        bail!("non-v1 info_hash len {}", slice.len());
    }
    ih.copy_from_slice(slice);
    let single = twb.files_status.eq_ignore_ascii_case("single");
    // Undecodable blob → treated as no files (documented over_threshold gap).
    let files = twb
        .files()
        .map(|fs| {
            fs.into_iter()
                .filter(|f| !f.path.is_empty())
                .map(|f| (f.index, f.path, f.size))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(SrcTorrent {
        info_hash: ih,
        name: twb.name.clone(),
        size: u64::try_from(twb.size).unwrap_or(0),
        single,
        files,
    })
}

/// Deterministic fake torrent for the smoke source (structure, not realism).
fn synth_torrent(i: u64) -> SrcTorrent {
    let h = fnv1a(&i.to_le_bytes());
    let mut info_hash = [0u8; 20];
    for (k, b) in info_hash.iter_mut().enumerate() {
        *b = (fnv1a(&[(i as u8), k as u8, (h >> 8) as u8]) & 0xff) as u8;
    }
    // ~8% single-file (matches the measured 8.06%).
    let single = h % 100 < 8;
    if single {
        let ext = QUERY_EXTS[(h % QUERY_EXTS.len() as u64) as usize];
        return SrcTorrent {
            info_hash,
            name: format!("Release.{i}.{ext}"),
            size: 50_000_000 + (h % 8_000_000_000),
            single: true,
            files: Vec::new(),
        };
    }
    // Skewed file count: mostly small, occasional large (median ~6, long tail).
    let n = match h % 100 {
        0..=49 => 1 + (h % 8),
        50..=89 => 8 + (h % 50),
        90..=98 => 60 + (h % 700),
        _ => 700 + (h % 4000),
    };
    let files = (0..n)
        .map(|j| {
            let fh = fnv1a(&[(i & 0xff) as u8, (j & 0xff) as u8, (j >> 8) as u8]);
            let ext = QUERY_EXTS[(fh % QUERY_EXTS.len() as u64) as usize];
            let size = 1_000 + (fh % 12_000_000_000);
            (j as u32, format!("Release.{i}/file_{j}.{ext}"), size)
        })
        .collect();
    SrcTorrent {
        info_hash,
        name: format!("Release.{i}"),
        size: 0,
        single: false,
        files,
    }
}

/// Synthesize the immutable denorm pair for a torrent (cardinality + range are
/// what drive FAST/INDEXED column cost; values themselves are irrelevant).
fn synth_denorm(info_hash: &[u8; 20]) -> (i64, &'static str) {
    let h = fnv1a(info_hash);
    let published = SYNTH_NOW - (h % SYNTH_WINDOW as u64) as i64;
    let ct = CONTENT_TYPES[(h % CONTENT_TYPES.len() as u64) as usize];
    (published, ct)
}

// ===========================================================================
// Measurement
// ===========================================================================

/// Map a Tantivy segment-file extension to its logical component.
fn component(ext: &str) -> &'static str {
    match ext {
        "store" => "doc store (doc_id)",
        "fast" => "FAST columns",
        "term" => "term dicts",
        "idx" => "postings",
        "pos" => "positions (path)",
        "fieldnorm" => "field norms",
        "del" => "deletes",
        "json" | "managed" | "lock" => "meta",
        _ => "other",
    }
}

/// Sum on-disk bytes by file extension → logical component, print the table.
fn report_segment_bytes(dir: &Path, docs: u64) -> Result<()> {
    let mut by_component: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut total: u64 = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if !meta.is_file() {
            continue;
        }
        let len = meta.len();
        total += len;
        let ext = entry
            .path()
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_string();
        *by_component.entry(component(&ext)).or_default() += len;
    }
    println!(
        "  {:<22} {:>14} {:>12}",
        "component", "bytes", "bytes/doc"
    );
    for (comp, bytes) in &by_component {
        println!(
            "  {:<22} {:>14} {:>12.3}",
            comp,
            bytes,
            *bytes as f64 / docs.max(1) as f64
        );
    }
    println!(
        "  {:<22} {:>14} {:>12.3}   (= {:.2} MB total)",
        "TOTAL",
        total,
        total as f64 / docs.max(1) as f64,
        total as f64 / 1_048_576.0
    );
    Ok(())
}

// ===========================================================================
// Query latency
// ===========================================================================

fn run_query(args: QueryArgs) -> Result<()> {
    let variant = Variant::from_name(&args.variant)
        .with_context(|| format!("unknown variant {:?}", args.variant))?;
    let dir = if args.index_path.join(variant.name).exists() {
        args.index_path.join(variant.name)
    } else {
        args.index_path.clone()
    };
    let (_schema, fields) = build_file_schema(&variant);
    let ext_field = fields
        .extension
        .context("variant has no extension field — pick V5/V9/V10/V11")?;
    let size_field = fields
        .size
        .context("variant has no size field — pick V3/V4/V9/V10/V11")?;

    let index = Index::open_in_dir(&dir).with_context(|| format!("open {}", dir.display()))?;
    let reader = index.reader().context("reader")?;
    let searcher = reader.searcher();
    println!(
        "Querying {} | {} docs over {} segment(s)",
        variant.name,
        searcher.num_docs(),
        searcher.segment_readers().len()
    );

    // Scenario A: file-level top-20 by size desc + total count (THE <50ms claim).
    let mut a_ms: Vec<f64> = Vec::with_capacity(args.iters);
    let mut total_hits_sum: u64 = 0;
    for i in 0..args.iters {
        let ext = QUERY_EXTS[i % QUERY_EXTS.len()];
        let smin = QUERY_SIZES[(i / QUERY_EXTS.len()) % QUERY_SIZES.len()];
        let query = ext_size_query(ext_field, size_field, ext, smin);
        let t = Instant::now();
        let total = searcher.search(&query, &Count)? as u64;
        let _top: Vec<(Option<u64>, _)> = searcher.search(
            &query,
            &TopDocs::with_limit(20).order_by_fast_field::<u64>("size", Order::Desc),
        )?;
        a_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        total_hits_sum += total;
    }
    a_ms.sort_by(|x, y| x.partial_cmp(y).unwrap());
    println!("avg total_hits/query (A): {}", total_hits_sum / args.iters.max(1) as u64);
    print_pct("A file-level (ext∧size, top20+count)", &a_ms);

    // Scenario B: collapse-to-torrent (full match-set scan + per-doc stored read
    // → worst-case). Bounded iters; realistic size thresholds only (>100MB/>1GB,
    // i.e. "find mkv > 1GB", never "all files").
    if fields.doc_id.is_some() {
        let mut b_ms: Vec<f64> = Vec::with_capacity(args.iters_b);
        let mut b_torrents_sum: u64 = 0;
        for i in 0..args.iters_b {
            let ext = QUERY_EXTS[i % QUERY_EXTS.len()];
            let smin = QUERY_SIZES[1 + (i % (QUERY_SIZES.len() - 1))]; // skip 0
            let query = ext_size_query(ext_field, size_field, ext, smin);
            let t = Instant::now();
            let addrs = searcher.search(&query, &DocSetCollector)?;
            let mut torrents: HashSet<String> = HashSet::with_capacity(addrs.len());
            if let Some(doc_id) = fields.doc_id {
                for addr in &addrs {
                    let d: TantivyDocument = searcher.doc(*addr)?;
                    if let Some(s) = d.get_first(doc_id).and_then(|v| v.as_str()) {
                        torrents.insert(s.split(':').next().unwrap_or(s).to_string());
                    }
                }
            }
            b_torrents_sum += torrents.len() as u64;
            b_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        b_ms.sort_by(|x, y| x.partial_cmp(y).unwrap());
        println!("avg distinct torrents/query (B): {}", b_torrents_sum / args.iters_b.max(1) as u64);
        print_pct("B collapse-to-torrent (scan+stored dedup, worst-case)", &b_ms);
    } else {
        println!("B collapse: skipped (variant stores no doc_id; FAST-identity read not wired)");
    }
    Ok(())
}

fn ext_size_query(ext_field: tantivy::schema::Field, size_field: tantivy::schema::Field, ext: &str, size_min: u64) -> Box<dyn Query> {
    use std::ops::Bound;
    let term_q: Box<dyn Query> = Box::new(TermQuery::new(
        Term::from_field_text(ext_field, ext),
        IndexRecordOption::Basic,
    ));
    let range_q: Box<dyn Query> = Box::new(RangeQuery::new(
        Bound::Included(Term::from_field_u64(size_field, size_min)),
        Bound::Unbounded,
    ));
    Box::new(BooleanQuery::new(vec![
        (Occur::Must, term_q),
        (Occur::Must, range_q),
    ]))
}

fn print_pct(label: &str, sorted: &[f64]) {
    println!(
        "  {:<40} p50 {:>7.3}ms  p95 {:>7.3}ms  p99 {:>7.3}ms  max {:>7.3}ms",
        label,
        pct(sorted, 50.0),
        pct(sorted, 95.0),
        pct(sorted, 99.0),
        sorted.last().copied().unwrap_or(0.0),
    );
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ===========================================================================
// Small helpers
// ===========================================================================

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Compact, low-collision 64-bit identity of `(info_hash, file_index)`. Used so
/// the truth set + a Tantivy hit can be intersected by value (a 20-byte hash +
/// u32 won't fit a `u64` directly; collisions at 50 M docs are ~1e-4, negligible
/// for recall/precision). FNV seed mixed through SplitMix64's finalizer.
fn ident_hash(info_hash: &[u8; 20], file_index: u32) -> u64 {
    let mut h = fnv1a(info_hash) ^ u64::from(file_index).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// Resident set size in KiB (Linux `/proc/self/statm`, 4 KiB pages). 0 if
/// unavailable — peak RSS is a "cheaply available" extra, never load-bearing.
fn read_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|p| p.parse::<u64>().ok()))
        .map(|pages| pages * 4)
        .unwrap_or(0)
}

// ===========================================================================
// Shared doc stream (synthetic | torrent_files) — one file doc at a time
// ===========================================================================

/// Streams `(info_hash, file_index, path, size)` one file doc at a time, from
/// either the deterministic synthetic source or the real `torrent_files` keyset
/// scan. Both `recall` and `freshness` drive it; `freshness` relies on the
/// continuous cursor so its base + deltas are distinct real rows in PK order.
struct DocStream {
    source: Source,
    pool: Option<sqlx::PgPool>,
    /// Keyset cursor `(info_hash, index)` for `torrent_files`.
    cur: Option<(Vec<u8>, i32)>,
    batch_size: i64,
    /// Next synthetic torrent ordinal.
    synth_i: u64,
    /// Refill buffer (a page of rows / one synthetic torrent's files).
    buf: VecDeque<([u8; 20], u32, String, u64)>,
}

impl DocStream {
    async fn connect(source: Source, dsn: &str, batch_size: i64) -> Result<Self> {
        let pool = match source {
            Source::TorrentFiles => {
                let mut cfg = bitmagnet_db::DbConfig::from_env().context("postgres config")?;
                if !dsn.is_empty() {
                    cfg.dsn = dsn.to_string();
                }
                Some(bitmagnet_db::connect(&cfg).await.context("connect postgres")?)
            }
            Source::Synthetic => None,
            Source::Postgres => bail!("DocStream does not support --source postgres (blob); use synthetic|torrent-files"),
        };
        Ok(Self {
            source,
            pool,
            cur: None,
            batch_size,
            synth_i: 0,
            buf: VecDeque::new(),
        })
    }

    /// Next file doc, or `None` when the real source is exhausted (synthetic is
    /// infinite). Refills `buf` a page / one torrent at a time.
    async fn next_doc(&mut self) -> Result<Option<([u8; 20], u32, String, u64)>> {
        if self.buf.is_empty() {
            self.refill().await?;
        }
        Ok(self.buf.pop_front())
    }

    async fn refill(&mut self) -> Result<()> {
        match self.source {
            Source::Synthetic => {
                let t = synth_torrent(self.synth_i);
                self.synth_i += 1;
                if t.files.is_empty() {
                    self.buf.push_back((t.info_hash, 0, t.name, t.size));
                } else {
                    for (idx, path, size) in t.files {
                        self.buf.push_back((t.info_hash, idx, path, size));
                    }
                }
            }
            Source::TorrentFiles => {
                use sqlx::Row;
                // Keyset over the (info_hash, index) PK. `index` quoted (PG kw).
                const SQL: &str = "SELECT info_hash, \"index\", path, size \
                    FROM torrent_files \
                    WHERE ($1::bytea IS NULL OR (info_hash, \"index\") > ($1, $2)) \
                    ORDER BY info_hash, \"index\" LIMIT $3";
                let pool = self.pool.as_ref().context("torrent_files: no pool")?;
                let rows = sqlx::query(SQL)
                    .bind(self.cur.as_ref().map(|(h, _)| h.clone()))
                    .bind(self.cur.as_ref().map_or(0, |(_, i)| *i))
                    .bind(self.batch_size)
                    .fetch_all(pool)
                    .await
                    .context("torrent_files page")?;
                for row in &rows {
                    let ih_raw: Vec<u8> = row.try_get("info_hash")?;
                    let idx: i32 = row.try_get("index")?;
                    let path: String = row.try_get("path")?;
                    let size: i64 = row.try_get("size")?;
                    self.cur = Some((ih_raw.clone(), idx)); // advance past skips too
                    if ih_raw.len() != 20 {
                        continue; // skip v2-only / malformed hashes
                    }
                    let mut ih = [0u8; 20];
                    ih.copy_from_slice(&ih_raw);
                    self.buf
                        .push_back((ih, idx as u32, path, u64::try_from(size).unwrap_or(0)));
                }
            }
            Source::Postgres => bail!("unreachable: postgres rejected at connect"),
        }
        Ok(())
    }
}

// ===========================================================================
// EXP-D — recall: path tokenizer size + CJK/ASCII recall + precision + latency
// ===========================================================================

/// One TSV query: original text + its lowercase form (case-insensitive truth).
struct QuerySpec {
    group: String,
    query: String,
    query_lc: String,
}

/// In-process exact-substring ground truth for one query, over the streamed N.
#[derive(Default)]
struct TruthEntry {
    /// Exact # of file docs whose lowercased path contains the query (uncapped).
    count: u64,
    /// Identity hashes of matching docs, capped at `--truth-cap` (0 = uncapped).
    set: HashSet<u64>,
    /// `count` exceeded the cap → `set` is a sample; precision is suppressed.
    saturated: bool,
}

/// Parse the `group<TAB>query` TSV (skips blank / `#`-prefixed lines).
fn load_queries(path: &Path) -> Result<Vec<QuerySpec>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read queries file {}", path.display()))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut it = line.splitn(2, '\t');
        let group = it.next().unwrap_or("").trim().to_string();
        let query = match it.next() {
            Some(q) if !q.is_empty() => q.to_string(),
            _ => continue,
        };
        out.push(QuerySpec {
            group,
            query_lc: query.to_lowercase(),
            query,
        });
    }
    Ok(out)
}

/// Build the Tantivy query that approximates an exact substring match on the
/// `path` field, given the field's tokenizer. Ngram tokens carry position 0
/// (`ngram_tokenizer.rs:168`) so a substring is the CONJUNCTION of the query's
/// ngram terms (occasional non-contiguous false positive → measured as
/// precision < 1 on ≥4-char queries). Default/Lindera tokens carry sequential
/// positions → a `PhraseQuery` (multi-token) or a `TermQuery` (single token,
/// the usual CJK-run case for `default`). Mirrors the sidecar's word-run →
/// `PhraseQuery` idiom (`query.rs:808`).
fn build_path_query(
    index: &Index,
    tok: PathTokenizer,
    path_field: Field,
    query: &str,
) -> Box<dyn Query> {
    let mut analyzer = match index.tokenizers().get(tok.tantivy_name()) {
        Some(a) => a,
        None => return Box::new(EmptyQuery),
    };
    let mut tokens: Vec<String> = Vec::new();
    let mut ts = analyzer.token_stream(query);
    while ts.advance() {
        tokens.push(ts.token().text.clone());
    }
    match tok {
        // EdgeNgram shares the conjunction-of-grams query shape with Ngram: the
        // query string is tokenized by the same analyzer, and all resulting grams
        // must be present (positions ignored).
        PathTokenizer::Ngram | PathTokenizer::EdgeNgram => {
            let mut seen: HashSet<String> = HashSet::new();
            let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
            for t in tokens {
                if seen.insert(t.clone()) {
                    clauses.push((
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_text(path_field, &t),
                            IndexRecordOption::WithFreqs,
                        )),
                    ));
                }
            }
            if clauses.is_empty() {
                Box::new(EmptyQuery)
            } else {
                Box::new(BooleanQuery::new(clauses))
            }
        }
        PathTokenizer::Default | PathTokenizer::Lindera => match tokens.len() {
            0 => Box::new(EmptyQuery),
            1 => Box::new(TermQuery::new(
                Term::from_field_text(path_field, &tokens[0]),
                IndexRecordOption::WithFreqs,
            )),
            _ => {
                let terms: Vec<Term> = tokens
                    .iter()
                    .map(|t| Term::from_field_text(path_field, t))
                    .collect();
                Box::new(PhraseQuery::new(terms))
            }
        },
    }
}

/// Index one "unit" — one file (per-file) or one torrent's path-bag
/// (per-torrent) — into the recall index. A unit matches a query if ANY of its
/// paths contains it (so per-torrent truth is the OR over the fileset). Every
/// path is added as a SEPARATE value of the `path` field, so each is tokenized
/// independently and no boundary grams span two files.
#[allow(clippy::too_many_arguments)]
fn index_unit(
    writer: &IndexWriter,
    truth: &mut [TruthEntry],
    specs: &[QuerySpec],
    fields: &RecallFields,
    skip_truth: bool,
    truth_cap: usize,
    ident: u64,
    paths: &[String],
) -> Result<()> {
    if !skip_truth {
        // Lowercase each path once (not once per query), then OR over the bag.
        let paths_lc: Vec<String> = paths.iter().map(|p| p.to_lowercase()).collect();
        for (k, spec) in specs.iter().enumerate() {
            if paths_lc.iter().any(|p| p.contains(&spec.query_lc)) {
                let e = &mut truth[k];
                e.count += 1;
                if !e.saturated {
                    if truth_cap == 0 || e.set.len() < truth_cap {
                        e.set.insert(ident);
                    } else {
                        e.saturated = true;
                    }
                }
            }
        }
    }
    let mut td = TantivyDocument::new();
    for p in paths {
        td.add_text(fields.path, p);
    }
    td.add_u64(fields.ident, ident);
    writer.add_document(td).context("add_document")?;
    Ok(())
}

async fn run_recall(args: RecallArgs) -> Result<()> {
    let specs = load_queries(&args.queries_file)?;
    if specs.is_empty() {
        bail!("no queries loaded from {}", args.queries_file.display());
    }
    let n_cjk = specs.iter().filter(|s| s.group == "cjk").count();
    let n_ascii = specs.iter().filter(|s| s.group == "ascii").count();
    println!(
        "recall: tokenizer={:?} granularity={:?} source={:?} queries={} (cjk={} ascii={}) limit_docs={} truth_cap={}",
        args.tokenizer, args.granularity, args.source, specs.len(), n_cjk, n_ascii, args.limit_docs, args.truth_cap
    );

    // --- Build the path-only index --------------------------------------
    let dir = &args.index_path;
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
    let (schema, fields) = build_recall_schema(args.tokenizer);
    let index = Index::create_in_dir(dir, schema).context("create recall index")?;
    register_path_tokenizer(&index, args.tokenizer, (args.ngram_min, args.ngram_max))?;
    // Single-thread (default) writer with a big arena: ngram (min=2,max=3 per
    // char) explodes each path into many tokens, and the default writer splits
    // its 256 MB heap across ~8 worker threads → each ~32 MB arena flushes every
    // ~37k docs → at scale a worker thread dies ("An index writer was killed").
    // One thread = one big arena → far fewer, larger segments, no starvation.
    let heap = (args.writer_heap_mb * 1024 * 1024).max(WRITER_HEAP_BYTES);
    let mut writer: IndexWriter = index
        .writer_with_num_threads(args.writer_threads.max(1), heap)
        .with_context(|| {
            format!(
                "writer ({} thread(s), {} MiB)",
                args.writer_threads, args.writer_heap_mb
            )
        })?;
    // Deterministic: no auto-merge; force-merge to 1 after for clean size + a
    // single fast-field segment to read hit identities from.
    writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

    let mut truth: Vec<TruthEntry> = specs.iter().map(|_| TruthEntry::default()).collect();
    let mut stream = DocStream::connect(args.source, &args.postgres_dsn, args.batch_size).await?;
    let mut docs: u64 = 0;
    let mut since_commit: u64 = 0;
    let start = Instant::now();
    match args.granularity {
        Granularity::PerFile => {
            while docs < args.limit_docs {
                let Some((ih, idx, path, _size)) = stream.next_doc().await? else {
                    break; // real source exhausted before limit_docs
                };
                index_unit(
                    &writer, &mut truth, &specs, &fields, args.skip_truth, args.truth_cap,
                    ident_hash(&ih, idx), std::slice::from_ref(&path),
                )?;
                docs += 1;
                since_commit += 1;
                if since_commit >= args.commit_interval {
                    writer.commit().context("commit")?;
                    since_commit = 0;
                }
            }
        }
        Granularity::PerTorrent => {
            // Group consecutive rows by info_hash (the keyset scan is ordered by
            // info_hash) → one path-bag doc per torrent. The trailing torrent is
            // flushed on stream-end / limit; if --limit-docs cuts mid-torrent the
            // last torrent's fileset is truncated (negligible at bench scale).
            let mut pend: Option<([u8; 20], Vec<String>)> = None;
            loop {
                if docs >= args.limit_docs {
                    break;
                }
                match stream.next_doc().await? {
                    Some((ih, _idx, path, _size)) => match &mut pend {
                        Some((cur, paths)) if *cur == ih => paths.push(path),
                        _ => {
                            if let Some((cur, paths)) = pend.take() {
                                index_unit(
                                    &writer, &mut truth, &specs, &fields, args.skip_truth,
                                    args.truth_cap, ident_hash(&cur, 0), &paths,
                                )?;
                                docs += 1;
                                since_commit += 1;
                                if since_commit >= args.commit_interval {
                                    writer.commit().context("commit")?;
                                    since_commit = 0;
                                }
                            }
                            pend = Some((ih, vec![path]));
                        }
                    },
                    None => {
                        if let Some((cur, paths)) = pend.take() {
                            index_unit(
                                &writer, &mut truth, &specs, &fields, args.skip_truth,
                                args.truth_cap, ident_hash(&cur, 0), &paths,
                            )?;
                            docs += 1;
                        }
                        break;
                    }
                }
            }
        }
    }
    writer.commit().context("final commit")?;
    let ingest = start.elapsed();

    // Force-merge to one segment (clean size attribution + single fast field).
    let ids = index.searchable_segment_ids().context("segment ids")?;
    if ids.len() > 1 {
        writer.merge(&ids).await.context("merge")?;
    }
    writer.garbage_collect_files().await.context("gc")?;
    let segs = index.searchable_segment_ids().map(|s| s.len()).unwrap_or(0);
    println!(
        "\n=== recall {:?} | {docs} docs | ingest {:.1}s ({:.0} docs/s) | {segs} segment(s) ===",
        args.tokenizer,
        ingest.as_secs_f64(),
        docs as f64 / ingest.as_secs_f64().max(1e-9),
    );
    report_segment_bytes(dir, docs)?;

    // --- Evaluate per query ---------------------------------------------
    let reader = index.reader().context("reader")?;
    let searcher = reader.searcher();
    // Identity fast-field columns are only needed for recall/precision; under
    // --skip-truth we only count hits + time the query, so don't open them.
    let ident_cols = if args.skip_truth {
        Vec::new()
    } else {
        searcher
            .segment_readers()
            .iter()
            .map(|sr| sr.fast_fields().u64("ident"))
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("ident fast field")?
    };

    // Per-group accumulators.
    struct GroupAcc {
        recalls: Vec<f64>,
        precisions: Vec<f64>,
        lat_ms: Vec<f64>,
        truth_sum: u64,
        hits_sum: u64,
        n: usize,
        n_truth: usize,
        n_saturated: usize,
    }
    let mut groups: BTreeMap<String, GroupAcc> = BTreeMap::new();
    // Diagnostic: lowest-recall queries (any group).
    let mut worst: Vec<(f64, String, String, u64, u64)> = Vec::new();

    for (k, spec) in specs.iter().enumerate() {
        let query = build_path_query(&index, args.tokenizer, fields.path, &spec.query);
        let (lat, hits_count, recall, precision) = if args.skip_truth {
            // Latency + match-count only (Count collector; no identity reads).
            let t = Instant::now();
            let n = searcher.search(&query, &Count).context("recall search")? as u64;
            (t.elapsed().as_secs_f64() * 1000.0, n, f64::NAN, f64::NAN)
        } else {
            let t = Instant::now();
            let addrs = searcher.search(&query, &DocSetCollector).context("recall search")?;
            let lat = t.elapsed().as_secs_f64() * 1000.0;
            let mut hits: HashSet<u64> = HashSet::with_capacity(addrs.len());
            for addr in &addrs {
                if let Some(v) = ident_cols[addr.segment_ord as usize].first(addr.doc_id) {
                    hits.insert(v);
                }
            }
            let e = &truth[k];
            let inter = e.set.iter().filter(|id| hits.contains(id)).count();
            let recall = if e.set.is_empty() {
                f64::NAN
            } else {
                inter as f64 / e.set.len() as f64
            };
            // Precision is exact only when truth is uncapped (not saturated).
            let precision = if e.saturated || hits.is_empty() {
                f64::NAN
            } else {
                inter as f64 / hits.len() as f64
            };
            (lat, hits.len() as u64, recall, precision)
        };

        let truth_count = if args.skip_truth { 0 } else { truth[k].count };
        let saturated = !args.skip_truth && truth[k].saturated;

        let acc = groups.entry(spec.group.clone()).or_insert_with(|| GroupAcc {
            recalls: Vec::new(),
            precisions: Vec::new(),
            lat_ms: Vec::new(),
            truth_sum: 0,
            hits_sum: 0,
            n: 0,
            n_truth: 0,
            n_saturated: 0,
        });
        acc.n += 1;
        acc.lat_ms.push(lat);
        acc.truth_sum += truth_count;
        acc.hits_sum += hits_count;
        if saturated {
            acc.n_saturated += 1;
        }
        if !recall.is_nan() {
            acc.recalls.push(recall);
            acc.n_truth += 1;
            worst.push((recall, spec.group.clone(), spec.query.clone(), truth_count, hits_count));
        }
        if !precision.is_nan() {
            acc.precisions.push(precision);
        }
    }

    if args.skip_truth {
        println!("\n  per-group latency (--skip-truth: no recall/precision; one warm pass):");
        println!(
            "  {:<6} {:>4} {:>12} {:>9} {:>9} {:>9}",
            "group", "n", "avgHits", "p50", "p95", "p99"
        );
        for (g, a) in &groups {
            let mut lat = a.lat_ms.clone();
            lat.sort_by(|x, y| x.partial_cmp(y).unwrap());
            println!(
                "  {:<6} {:>4} {:>12} {:>7.2}ms {:>7.2}ms {:>7.2}ms",
                g,
                a.n,
                a.hits_sum / a.n.max(1) as u64,
                pct(&lat, 50.0),
                pct(&lat, 95.0),
                pct(&lat, 99.0),
            );
        }
        return Ok(());
    }

    println!("\n  per-group results (recall over docs with truth; precision exact unless SATURATED):");
    println!(
        "  {:<6} {:>4} {:>7} {:>9} {:>9} {:>10} {:>10} {:>9} {:>9} {:>9}",
        "group", "n", "wTruth", "meanRec", "meanPrec", "avgTruth", "avgHits", "lat-p50", "lat-p95",
        "lat-p99"
    );
    for (g, a) in &groups {
        let mut lat = a.lat_ms.clone();
        lat.sort_by(|x, y| x.partial_cmp(y).unwrap());
        println!(
            "  {:<6} {:>4} {:>7} {:>9.4} {:>9.4} {:>10} {:>10} {:>7.2}ms {:>7.2}ms {:>7.2}ms{}",
            g,
            a.n,
            a.n_truth,
            mean(&a.recalls),
            mean(&a.precisions),
            a.truth_sum / a.n.max(1) as u64,
            a.hits_sum / a.n.max(1) as u64,
            pct(&lat, 50.0),
            pct(&lat, 95.0),
            pct(&lat, 99.0),
            if a.n_saturated > 0 {
                format!("  ({} saturated)", a.n_saturated)
            } else {
                String::new()
            },
        );
    }

    // Five lowest-recall queries (the mid-run CJK failure signature).
    worst.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    println!("\n  lowest-recall queries (diagnostic):");
    for (recall, group, query, truth_n, hits_n) in worst.iter().take(5) {
        println!(
            "    [{group}] {:<10} recall={:.4} truth={} hits={}",
            query, recall, truth_n, hits_n
        );
    }
    Ok(())
}

/// Mean of a slice, NaN-safe (empty → NaN).
fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        f64::NAN
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

// ===========================================================================
// pathquery — cold-first + warm-rep latency on an already-built path index
// ===========================================================================

/// Open an already-built path index (no rebuild) and measure, per group, the
/// timed cold-first execution + `--warm-reps` warm executions of each query.
/// The caller drops page cache before this so the cold-first read hits disk
/// (the RUN-2 cold/warm pattern). Query construction + tokenizer registration
/// are the EXACT recall-path code, so default=Term/Phrase and ngram=conjunction
/// match the build.
fn run_pathquery(args: PathqueryArgs) -> Result<()> {
    let specs = load_queries(&args.queries_file)?;
    if specs.is_empty() {
        bail!("no queries loaded from {}", args.queries_file.display());
    }
    println!(
        "pathquery: tokenizer={:?} index={} queries={} warm_reps={}",
        args.tokenizer,
        args.index_path.display(),
        specs.len(),
        args.warm_reps
    );

    let index = Index::open_in_dir(&args.index_path)
        .with_context(|| format!("open index {}", args.index_path.display()))?;
    // Tokenizers are runtime state, not persisted — re-register so query
    // construction tokenizes identically to the build (sidecar `index.rs:41`).
    register_path_tokenizer(&index, args.tokenizer, (args.ngram_min, args.ngram_max))?;
    let path_field = index
        .schema()
        .get_field("path")
        .context("index has no `path` field")?;

    let reader = index.reader().context("reader")?;
    let searcher = reader.searcher();
    println!(
        "  {} docs over {} segment(s)",
        searcher.num_docs(),
        searcher.segment_readers().len()
    );

    struct GroupAcc {
        cold_ms: Vec<f64>,
        warm_ms: Vec<f64>,
        hits_sum: u64,
        n: usize,
    }
    let mut groups: BTreeMap<String, GroupAcc> = BTreeMap::new();

    for spec in &specs {
        let query = build_path_query(&index, args.tokenizer, path_field, &spec.query);
        // Cold-first execution: with page cache just dropped, this reads the
        // query's postings from disk. Timed on its own (Count = match count).
        let t = Instant::now();
        let hits = searcher.search(&query, &Count).context("pathquery cold")? as u64;
        let cold = t.elapsed().as_secs_f64() * 1000.0;
        // Warm repetitions (postings now resident in the OS page cache).
        let mut warm: Vec<f64> = Vec::with_capacity(args.warm_reps);
        for _ in 0..args.warm_reps {
            let t = Instant::now();
            let _ = searcher.search(&query, &Count).context("pathquery warm")?;
            warm.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let acc = groups.entry(spec.group.clone()).or_insert_with(|| GroupAcc {
            cold_ms: Vec::new(),
            warm_ms: Vec::new(),
            hits_sum: 0,
            n: 0,
        });
        acc.n += 1;
        acc.cold_ms.push(cold);
        acc.warm_ms.extend(warm);
        acc.hits_sum += hits;
    }

    println!(
        "\n  per-group latency (cold = first exec/query; warm = {} reps/query):",
        args.warm_reps
    );
    println!(
        "  {:<6} {:>4} {:>12} {:>11} {:>11} {:>10} {:>10} {:>10}",
        "group", "n", "avgHits", "cold-p50", "cold-p95", "warm-p50", "warm-p95", "warm-p99"
    );
    for (g, a) in &groups {
        let mut cold = a.cold_ms.clone();
        cold.sort_by(|x, y| x.partial_cmp(y).unwrap());
        let mut warm = a.warm_ms.clone();
        warm.sort_by(|x, y| x.partial_cmp(y).unwrap());
        println!(
            "  {:<6} {:>4} {:>12} {:>9.2}ms {:>9.2}ms {:>8.2}ms {:>8.2}ms {:>8.2}ms",
            g,
            a.n,
            a.hits_sum / a.n.max(1) as u64,
            pct(&cold, 50.0),
            pct(&cold, 95.0),
            pct(&warm, 50.0),
            pct(&warm, 95.0),
            pct(&warm, 99.0),
        );
    }
    Ok(())
}

// ===========================================================================
// EXP-E — freshness: default LogMergePolicy, live appends, lag + supersession
// ===========================================================================

/// Add one V11 file doc (size+ext+path+content_type) to the writer.
fn add_file_doc(
    writer: &IndexWriter,
    variant: &Variant,
    fields: &FileFields,
    info_hash: &[u8; 20],
    file_index: u32,
    path: &str,
    size: u64,
) -> Result<()> {
    let (published, ct) = synth_denorm(info_hash);
    let td = build_doc(variant, fields, info_hash, file_index, path, size, published, ct);
    writer.add_document(td).context("add_document")?;
    Ok(())
}

/// A guaranteed-unique sentinel info_hash for delta `n` (0xEE marker + counter).
fn sentinel_ih(n: usize) -> [u8; 20] {
    let mut ih = [0xEEu8; 20];
    ih[19] = n as u8;
    ih[18] = (n >> 8) as u8;
    ih
}

async fn run_freshness(args: FreshnessArgs) -> Result<()> {
    let deltas: Vec<u64> = args
        .delta_sizes
        .split(',')
        .map(|s| s.trim().parse::<u64>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parse --delta-sizes {:?}", args.delta_sizes))?;
    println!(
        "freshness: source={:?} base_docs={} deltas={:?} commit_batch={} (DEFAULT LogMergePolicy, no force-merge)",
        args.source, args.base_docs, deltas, args.commit_batch
    );

    // V11 = size + extension + path(default tokenizer) + content_type + delete key.
    let variant = Variant::from_name("V11").context("V11 variant")?;
    let dir = &args.index_path;
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
    let (schema, fields) = build_file_schema(&variant);
    let index = Index::create_in_dir(dir, schema).context("create freshness index")?;
    let mut writer: IndexWriter = index.writer(WRITER_HEAP_BYTES).context("writer")?;
    // NOTE: intentionally NO set_merge_policy → Tantivy's default LogMergePolicy
    // (incremental background merges), and NO force-merge. This is the whole
    // point of EXP-E vs the force-merge-to-1 Build path.
    let ext_field = fields.extension.context("V11 has extension")?;
    let size_field = fields.size.context("V11 has size")?;
    let path_field = fields.path.context("V11 has path")?;

    // Manual reload so we measure commit→reload lag deterministically (the
    // default OnCommitWithDelay reloads in the background — unmeasurable here).
    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()
        .context("reader")?;

    let mut stream = DocStream::connect(args.source, &args.postgres_dsn, args.batch_size).await?;
    let mut peak_rss = read_rss_kb();
    let mut first_ih: Option<[u8; 20]> = None;
    let mut total_docs: u64 = 0;

    // --- Base build (committed in commit_batch chunks) ------------------
    let base_start = Instant::now();
    while total_docs < args.base_docs {
        let Some((ih, idx, path, size)) = stream.next_doc().await? else {
            break;
        };
        if first_ih.is_none() {
            first_ih = Some(ih);
        }
        add_file_doc(&writer, &variant, &fields, &ih, idx, &path, size)?;
        total_docs += 1;
        if total_docs % args.commit_batch == 0 {
            writer.commit().context("base commit")?;
            peak_rss = peak_rss.max(read_rss_kb());
        }
    }
    writer.commit().context("base final commit")?;
    reader.reload().context("reload")?;
    let base_segs = index.searchable_segment_ids().map(|s| s.len()).unwrap_or(0);
    println!(
        "\n  base: {total_docs} docs in {:.1}s → {base_segs} segments",
        base_start.elapsed().as_secs_f64()
    );
    println!(
        "  {:<10} {:>6} {:>12} {:>10} {:>12} {:>10} {:>11} {:>9}",
        "delta", "segs", "fresh-lag", "commit", "cumDocs", "ext∧size", "pathTerm", "peakRSS"
    );

    // --- Delta sweep -----------------------------------------------------
    for (di, d) in deltas.iter().enumerate() {
        let mut added = 0u64;
        while added < *d {
            let want = (*d - added).min(args.commit_batch);
            for _ in 0..want {
                match stream.next_doc().await? {
                    Some((ih, idx, path, size)) => {
                        add_file_doc(&writer, &variant, &fields, &ih, idx, &path, size)?;
                    }
                    None => break, // real source exhausted; commit what we have
                }
                added += 1;
                total_docs += 1;
            }
            writer.commit().context("delta commit")?;
            peak_rss = peak_rss.max(read_rss_kb());
        }
        // Final sentinel for THIS delta + freshness-lag measurement. Its commit
        // is the one whose return→reload→visible lag we time.
        let sih = sentinel_ih(di);
        add_file_doc(&writer, &variant, &fields, &sih, 0, &format!("sentinel/d{di}.mkv"), 4242)?;
        let tc = Instant::now();
        writer.commit().context("sentinel commit")?;
        let last_commit_ms = tc.elapsed().as_secs_f64() * 1000.0;
        total_docs += 1;

        let sentinel_term = Term::from_field_bytes(fields.info_hash, &sih);
        let t0 = Instant::now();
        let fresh_lag_ms = loop {
            reader.reload().context("reload")?;
            let s = reader.searcher();
            let seen = s
                .search(
                    &TermQuery::new(sentinel_term.clone(), IndexRecordOption::Basic),
                    &Count,
                )
                .context("sentinel search")?;
            if seen >= 1 {
                break t0.elapsed().as_secs_f64() * 1000.0;
            }
        };

        // Query latency at this delta volume.
        let searcher = reader.searcher();
        let q = ext_size_query(ext_field, size_field, "mkv", 1_000_000_000);
        let te = Instant::now();
        let _ = searcher.search(&q, &Count)?;
        let _ = searcher.search(
            &q,
            &TopDocs::with_limit(20).order_by_fast_field::<u64>("size", Order::Desc),
        )?;
        let ext_ms = te.elapsed().as_secs_f64() * 1000.0;

        let pq = build_path_query(&index, PathTokenizer::Default, path_field, "1080p");
        let tp = Instant::now();
        let _ = searcher.search(&pq, &Count)?;
        let path_ms = tp.elapsed().as_secs_f64() * 1000.0;

        let segs = index.searchable_segment_ids().map(|s| s.len()).unwrap_or(0);
        println!(
            "  +{:<9} {:>6} {:>10.2}ms {:>8.1}ms {:>12} {:>8.2}ms {:>9.2}ms {:>7}MB",
            d,
            segs,
            fresh_lag_ms,
            last_commit_ms,
            total_docs,
            ext_ms,
            path_ms,
            peak_rss / 1024,
        );
    }

    // --- Supersession (delete_term(info_hash) + re-add a new fileset) ----
    if let Some(ih) = first_ih {
        let term = Term::from_field_bytes(fields.info_hash, &ih);
        reader.reload().ok();
        let before = reader
            .searcher()
            .search(&TermQuery::new(term.clone(), IndexRecordOption::Basic), &Count)?;
        let t = Instant::now();
        writer.delete_term(term.clone());
        for j in 0..3u32 {
            add_file_doc(
                &writer,
                &variant,
                &fields,
                &ih,
                j,
                &format!("superseded/new_{j}.mkv"),
                1_000 + u64::from(j),
            )?;
        }
        writer.commit().context("supersession commit")?;
        reader.reload().context("reload")?;
        let supersede_ms = t.elapsed().as_secs_f64() * 1000.0;
        let after = reader
            .searcher()
            .search(&TermQuery::new(term, IndexRecordOption::Basic), &Count)?;
        println!(
            "\n  supersession: info_hash had {before} docs → delete_term + re-add 3 + commit + reload = {:.1}ms → now {after} docs (expect 3; old fileset gone = {})",
            supersede_ms,
            if after == 3 { "OK" } else { "MISMATCH" }
        );
    } else {
        println!("\n  supersession: skipped (no base doc captured)");
    }
    Ok(())
}
