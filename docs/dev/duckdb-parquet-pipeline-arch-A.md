# ARCH-A — DuckDB-on-Parquet: Generation Pipeline + Refresh / Freshness

**Owner:** `pg-data-bench` (team bitmagnet-bench) · **Task #14** · Design/docs only.
**Settles:** how the per-file analytics Parquet is **produced from the blobs**, **refreshed**, and what **freshness contract** the file-search surface honours — post-cutover, when `torrent_files` is dropped and the blobs (`torrents.files_data`) are the only per-file source.
**Companion tasks:** ARCH-B (DuckDB↔bitmagnet integration), ARCH-C (empirical layout/latency — partitioning config is *coordinated* with C, see §3), ARCH-D (deploy topology / replaces-Tantivy-sidecar).
**Grounded in:** `bitmagnet-model/src/blob.rs`, `bitmagnet-db/src/stream.rs`, `bench/blob_export/src/main.rs`, `internal/blobmigration/serializer.go`, DuckDB `extension/parquet/parquet_extension.cpp` + `src/planner/binder/statement/bind_copy.cpp`, and the measured results in `file-grained-search-benchmark-results.md`.

---

## 0. TL;DR

- **Source post-cutover = the blobs**, not `torrent_files` (dropped at cutover). `torrents.files_data` = `zstd(msgpack[{i,p,e,s}])`, ~14.5 GB total over **16.99 M** with-files torrents (`blob.rs`).
- **Decode is cheap and already built**: `bench/blob_export` streams the corpus (`stream_torrents_with_files`, keyset on `info_hash`), decodes via `deserialize_files`, **derives `extension` from the PATH (G1)**, and writes a ZSTD Parquet of `(info_hash, file_index, [path], extension, size)`. Measured decode 0.6–0.94 µs/file → **full corpus ~1–2 min @ 8–16 threads**. Slim Parquet = **3.86 GB unsorted / 10.3 GB sorted-by-(ext,size)** (the production layout — sorting decorrelates info_hash and triples size, but buys 7–60× query speedups; §3).
- **Refresh model = scheduled FULL REBUILD with atomic swap** (primary). The corpus is tiny and a rebuild is ~3–8 min end-to-end, so a periodic full rebuild beats incremental on simplicity and correctness (no dedup/tombstone/late-update problems). **Base+delta incremental is an optional enhancement** for sub-cadence freshness of *new* torrents, documented in §4.4.
- **Freshness contract (read-your-write split):**
  - **Per-torrent BROWSE / file-list hydrate = LIVE** — served from the blob via `AfterFind` (**G2**), straight from PG. A torrent's own files are always real-time.
  - **Cross-file SEARCH / analytics = eventually consistent** — served from the Parquet, lag ≤ refresh cadence (**E4** tolerates this). Default SLA: **≤ 6 h** (tunable; ≤ 1 h if base+delta is enabled).
- **Layout = a single ZSTD Parquet sorted by `(extension, size)`** with tuned row groups (the exact config is **gated on ARCH-C**'s OPTIMIZE pass; strong prior in §3 says skip hive-partitioning because the 3.86 GB file is RAM-resident, cold≈warm).
- **Lives on** the search service's local-path PVC (the one freed by rejecting the 873 M-doc Tantivy index), written by a CronJob, read by the query service via a versioned-dir + `current` pointer atomic swap (§5).

---

## 1. Why the source is the blobs (and what that costs)

At the D1 cutover `torrent_files` (the 277 GB / 879 M-row per-file table) is dropped; per-file data survives **only** inside `torrents.files_data`. So the production pipeline cannot use the RUN-2 shortcut (DuckDB `postgres_scanner` over `torrent_files`); it must **decode the blobs**.

Blob wire format (`blob.rs:1-18`, byte-identical to Go `serializer.go`):
```
files_data = zstd_level3( msgpack_array[ {"i":u32 index, "p":str path, "e":str ext, "s":u64 size}, … ] )
```
- **G1 is mandatory here**: the blob's `e` field is **empty for crawl-path torrents** (~4–7% of files today, growing with every new crawl — `benchmark-results.md` §1). The pipeline must **ignore `e` and derive the extension from the path** via `file_extension_from_path` (regex `[^/.]\.([a-z0-9]+)$` over `lower(path)` — identical to the dropped generated column and to `torrent_files.go:33`). `blob_export/src/main.rs:169-173` already does exactly this; **the production job must keep it.**
- Decode cost (measured, real `DeserializeFiles`): **0.6–0.94 µs/file**, ~1.6 M files/s single-thread → **~8–9 min single / ~1–2 min @ 8–16 threads** for the full ~860 M files. zstd-decompress + msgpack-unmarshal; the unmarshal dominates and scales with file count, not bytes.
- PG read cost: streaming 14.5 GB of blobs over the `info_hash` keyset is a light sequential scan on a near-idle PG (RUN-2 did the equivalent). ⚠️ The current `STREAM_SQL` (`stream.rs:45-50`) selects **all** torrents (48 M, 64% with `files_data IS NULL`). Production must add **`WHERE files_data IS NOT NULL`** to skip the 31 M empty rows — and since the only relevant index is the *inverse* partial (`torrents_files_data_null_idx … WHERE files_data IS NULL`), either accept a ~30–60 s seq scan of the 13 GB torrents heap or add a complementary partial index `… WHERE files_data IS NOT NULL` (recommended; ~17 M-entry, small).

---

## 2. Pipeline overview

```
                    ┌─────────────── refresh CronJob (k8s) ───────────────┐
  PG torrents       │  keyset stream            decode + G1        write   │     PVC
 (files_data) ──────┼─▶ files_data IS NOT NULL ─▶ deserialize_files ─▶ Parquet ──┼──▶ parquet/v<ts>/  ──▶ atomic swap ──▶ current
  (blobs, live)     │   ORDER BY info_hash       file_extension_     ZSTD(3)  │                                            │
                    │   page 20k                 from_path           sorted   │                                            ▼
                    └──────────────────────────────────────────────────────┘                              DuckDB query service (ARCH-B)
                                                                                                            reads current/, in-RAM, cold≈warm
  per-torrent browse / file hydrate ─────────────────────────────────────────────────────────▶ LIVE from blob via AfterFind (G2), bypasses Parquet
```

**Schema emitted** (`blob_export` schema, `main.rs:109-120`):

| col | type | notes |
|---|---|---|
| `info_hash` | Utf8 (40-hex) | join/collapse key; for distinct-torrent counts |
| `file_index` | UInt32 | blob `i` |
| `path` | Utf8 | **only in the full Parquet** (path-FTS / `ILIKE`); dropped in `--slim` |
| `extension` | Utf8, nullable | **G1 path-derived**; null = no ext (6.8% measured) |
| `size` | UInt64 | blob `s` |

Artifacts the Job emits each refresh:
- **slim fact** (`--slim`, drops `path`), sorted `(ext,size)`: **10.3 GB** (3.86 GB if shipped unsorted — §3 decision) — powers `ext∧size`, GROUP BY, distinct-torrent collapse, histograms, two-sided ranges (RUN-2 Q1–Q5, Q7).
- **pre-aggs**: `per_ext` (~1 MB) + `per_torrent_ext` (1.39 GB) — the `<50 ms` lever for facets + one-sided collapse (§3).
- **full** (adds `path`): **11.71 GB** measured — adds path-`ILIKE` FTS. ⚠️ **path-FTS latency is match-frequency-dependent, NOT uniformly fast**: a common substring with `LIMIT` early-outs (RUN-2 Q6 `'%S01E%' LIMIT 100` = 142 ms), but a **rare/absent pattern or an exhaustive count is a full 11.7 GB column scan ≈ 23 s** (ARCH-C, full corpus) — a leading-wildcard substring match has **no row-group pruning** (DuckDB: min/max `CheckStatistics` needs a prefix/range — `parquet_reader.cpp:1308`; bloom `BloomFilterExcludes` is equality-only — `parquet_statistics.cpp:802`), so my writer config (sort/row-group/bloom) **provably cannot** accelerate it — it streams the whole `path` column. So the full Parquet gives *best-effort* path search (snappy for common+paginated, seconds for rare/exhaustive), suitable as a non-interactive "search file paths" feature — **not** per-keystroke. Ship slim first; add full only if best-effort path search is wanted.

---

## 3. Layout / partitioning  (LOCKED — ARCH-C measured, full 879 M corpus)

**Winning writer config** (ARCH-C OPTIMIZE matrix; warm p50, all cores):
```sql
COPY (SELECT * FROM staging ORDER BY extension, size)
  TO 'files.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000, WRITE_BLOOM_FILTER false);
```
1. **Sort `(extension, size)`: YES.** Row-group min/max pruning (`parquet_reader.cpp:1308 CheckStatistics`) gives **7–60×** on the structured queries — measured v0-unsorted → v1-sorted: distinct-torrent collapse **1311 → 132 ms**, two-sided range **1255 → 109 ms**, exact `ext∧size` file count **1024 → 17 ms**, GROUP BY ext **1425 → 751 ms**, rare-ext find **48 → 19 ms** (common paginated find 30 → 56 ms, a wash).
2. **`row_group_size = 1,000,000`** — beat 100 k and the 122,880 default on both size (10.3 vs ~12 GB) and latency.
3. **Bloom filter OFF** — when sorted by `extension`, contiguous min/max already excludes non-matching groups; the equality-only bloom (`parquet_statistics.cpp:802`) is redundant (rare-ext: bloom-on 19 ms ≈ bloom-off 18 ms). Saves ~0.16 GB + write cost.
4. **Single file, NOT hive-partitioned** — my prior confirmed: a coarse `file_category=video` find was 14 ms but adds directory complexity for **no win** over the native sorted file; `partition_by=extension` (47,628 dirs) is a non-starter.
5. **ZSTD** compression throughout.

🚨 **The sort's cost — size nearly triples:** sorting by `(extension, size)` **decorrelates `info_hash`**, collapsing its RLE/dictionary compression → the slim Parquet goes **3.86 GB (unsorted) → 10.30 GB (sorted)** (+6.4 GB). Still trivially RAM-resident on HEL1 (125 GB) and tiny vs the rejected index (+14–25 GB) or the DB (~121 GB).

**Ship the two pre-aggregate tables alongside the fact** (ARCH-C; the refresh Job emits them in the same pass via a `GROUP BY` over the decoded rows — these are the RUN-3 aggregates in columnar form, and they deliver `<50 ms` **even on the unsorted file**):
- **`per_ext`** — per-extension `count/total/max` (47,628 rows, **~1 MB**) → GROUP-BY-extension facet **12.6 ms**.
- **`per_torrent_ext`** — per-(info_hash, extension) `{max,min,count}` (56 M rows, **1.39 GB** as ZSTD Parquet — vs 5.27 GB in PG; columnar wins) → one-sided distinct-torrent collapse **5.2 ms**.

**DECISION (ARCH-A) — ship sorted fact + pre-aggs (~11.7 GB total):** sorted `files.parquet` (10.3 GB) + `per_ext` (~1 MB) + `per_torrent_ext` (1.39 GB) makes **every structured query <150 ms** (path-FTS excepted, §2). Rationale: the user's explicit latency ask, HEL1's 1.2 TB free, and full RAM-residency. **Documented lean alternative — unsorted fact + pre-aggs (~5.3 GB):** GROUP-BY / one-sided-collapse / per-ext counts stay `<50 ms` via the pre-aggs, but **two-sided range, rare-ext find, and exact per-file counts regress to ~1.0–1.3 s** (they need the sorted base). Pick the lean form only if rebuild-time/disk later tightens; both are RAM-resident. *(The pre-aggs answer one-sided thresholds; two-sided ranges and exact per-file counts still need the sorted fact — the §2 capability split.)*

**Two-step write** (decode stream is `info_hash`-ordered): Rust decode → unsorted *staging* Parquet → the `COPY … ORDER BY extension, size …` above → `files.parquet` (+ the two `GROUP BY` pre-agg COPYs). The sort is a DuckDB external/spilling sort; adds ~1–2 min.

> **Live-freshness note:** the `per_torrent_ext` Parquet is ≤cadence-stale like the fact. If **live** one-sided collapse is wanted, the **PG** per-(torrent,ext) aggregate (RUN-3, built-from-blob on write, 5.27 GB, <50 ms) is the live-freshness variant — ARCH-B/ARCH-D decide whether collapse needs live or ≤6 h-stale is fine. ARCH-A's Job emits the Parquet pre-aggs; the PG aggregate (if used) is maintained on the write path, not here.

---

## 4. Refresh strategy

### 4.1 Primary: scheduled FULL REBUILD + atomic swap
- Each run decodes **all** with-files blobs → a fresh `parquet/v<ts>/`, then atomically swaps `current` (see §5). **No watermark, no dedup, no tombstones** — the rebuild is the source of truth, so re-crawls (a torrent gains files), deletions, and re-classifications are all naturally reflected.
- **Cost per run:** PG scan + decode ~1–2 min (parallel) + sort/write ~1–2 min ⇒ **~3–8 min wall**; PG load light (sequential, near-idle). At a 6 h cadence that's 4 rebuilds/day reading 14.5 GB each — trivial.
- **Why full beats incremental here:** the corpus is tiny and Parquet is **immutable** — incremental needs either rewrite-on-update or read-time dedup + tombstones for deletes, which is real complexity for a surface that explicitly *tolerates lag* (E4). Full rebuild is correct-by-construction.

### 4.2 Cadence / SLA
- **Default search-freshness SLA: ≤ 6 h** (one knob: the CronJob schedule). Tighten to 1 h if desired — still cheap. The lag applies **only** to cross-file SEARCH/analytics, never to per-torrent browse.
- The job writes a `homelab_parquet_refresh_last_success_timestamp_seconds` textfile metric (same pattern as the backup-staleness monitor in MEMORY) → a `ParquetRefreshStale` alert if it exceeds the SLA. Reuses the existing node-exporter textfile collector.

### 4.3 Read-your-write split (the freshness contract)
| Surface | Source | Freshness |
|---|---|---|
| Per-torrent file list / browse / hydrate | blob via `AfterFind` (**G2**) → live PG | **real-time** |
| "find all `.mkv` > 1 GB" cross-file SEARCH | DuckDB-on-Parquet | ≤ cadence (≤6 h) |
| distinct-torrent collapse / counts | per-(torrent,ext) **PG aggregate** (RUN-3) | **live** (built from blob on write) |
| size histograms / analytics | DuckDB-on-Parquet | ≤ cadence |
| path-FTS (`ILIKE`) | DuckDB full Parquet (optional) | ≤ cadence — *and best-effort latency*: ~0.1 s common+`LIMIT`, **~23 s rare/exhaustive** (full scan, unprunable) |

So a **newly crawled torrent is immediately browsable** (live blob) and its distinct-torrent collapse counts are live (PG aggregate), while it becomes **discoverable in cross-file search** at the next refresh. This matches user expectation: you can always open a torrent you just found; a brand-new torrent showing up in a global file-search a few hours later is acceptable.

### 4.4 Optional enhancement: base + delta (only if sub-hour new-torrent freshness is required)
- A weekly/daily **base** (full rebuild) + hourly **delta** partition files holding only `created_at > watermark` torrents (the `torrents_created_at_idx` exists → cheap). DuckDB reads `base/*.parquet + delta/*.parquet` as one table via directory glob.
- **Caveats that make this secondary:** (a) re-crawls that *add files* to an existing torrent bump `updated_at`, not `created_at` → missed by a created_at delta, caught only at the next base rebuild; (b) deletes need the base rebuild too; (c) a torrent present in both base and delta needs read-time `QUALIFY ROW_NUMBER() OVER (PARTITION BY info_hash,file_index ORDER BY _src_ts DESC)=1`. Given E4's lag tolerance, the added complexity is rarely justified — **recommend deferring** unless a product requirement demands <1 h new-torrent search visibility.

---

## 5. Where the Parquet lives + atomic swap

- **Storage:** the search service's **local-path PVC on HEL1** — the 200 Gi PVC originally sized for the (now-rejected) Tantivy index. Production stack: sorted slim fact (10.3 GB) + pre-aggs (1.4 GB) + optional full+path Parquet (11.71 GB) ≈ 23 GB live, ×2 for keeping the previous version during swap + one in-flight rebuild ⇒ peak ~50–70 GB; a **64–100 Gi PVC** is comfortable (the 200 Gi is more than enough). Local disk (not network) → DuckDB mmap is fast and the files stay in page cache (cold≈warm).
- **Atomic swap (no torn reads):** the Job writes to `parquet/v<unix_ts>/{files_slim.parquet,files_full.parquet}`, then flips a `current` symlink (`ln -sfn v<ts> current_tmp && mv -T current_tmp current` — atomic rename on the same fs). **Reopen mechanism (per ARCH-B §5):** the sidecar treats the Parquet as a **disposable cache** and **re-creates its `files`/`torrents` DuckDB views (`read_parquet` over `current/`) on reload** (a schema-version marker guards mismatch → re-export, never crash-loop). So the swap contract is: Job publishes `current/` + bumps a version marker; sidecar recreates views (on signal/poll). Keep the **previous 1–2 versions** for instant rollback + to let in-flight queries finish on the old files; a retention sweep deletes older `v*`.
- **Companion torrent-dimension (seam to ARCH-B §4):** ARCH-B's SQL-gen joins the per-file fact (this pipeline's output) to torrent-level dims (`content_type`, `published_at`, seeders, …) for analytics/filtering. Those dims are **not** in the blob, so they are NOT this Job's output — they come from a small **torrent-dim Parquet** (a second cheap export: `SELECT info_hash, content_type, published_at, … FROM torrent_contents/torrents`) or a live PG view. ARCH-B owns the dim source + join; ARCH-A owns only the file-fact Parquet. If a dim Parquet is chosen, the same refresh Job can emit it in the same pass (one extra `COPY`).

---

## 6. The refresh JOB — implementation

**Shape:** a Kubernetes **CronJob** (not a long-running deployment) — bounded, idempotent, restart-safe; a missed run self-heals next tick.

**Language — LOCKED to Rust by ARCH-B** (`duckdb-integration-arch.md` §1): the query runtime is **DuckDB embedded in the existing Rust `bitmagnet-search` sidecar** (`duckdb-rs`, swapping the Tantivy engine), keeping bitmagnet pure-Go. So the refresh Job is a **Rust bin = productionized `bench/blob_export`** — same crate ecosystem as the sidecar, can use `duckdb-rs` for the in-process sort-pass (no `duckdb` CLI dependency). Delta over the existing 310-line bin: add `WHERE files_data IS NOT NULL`, the DuckDB sort-pass, the versioned-dir swap, the row-count sanity gate, and the staleness metric. **Do not re-implement decode** — call `deserialize_files` + `file_extension_from_path` (already done in `main.rs`; format locked byte-for-byte by `blob_fixture.rs`).
- *(Go fallback retained only if ARCH-B's escape-hatch (a) `go-duckdb` embedded-in-the-Go-app is later chosen: a CronJob over `blobmigration.DeserializeFiles` (`serializer.go:47`) + `FileExtensionFromPath` + arrow-go. Same decode logic, different language. Not the recommended path.)*

**Job algorithm (full rebuild):**
1. `mkdir parquet/v<ts>/`.
2. Keyset-stream `torrents WHERE files_data IS NOT NULL ORDER BY info_hash`, page 20 k.
3. Per torrent: `deserialize_files(blob)` → per file emit `(info_hash, index, path?, file_extension_from_path(path), size)`. Count `blob_errors` (don't abort; log + metric).
4. Write staging Parquet (ZSTD, 1 M-row batches — `blob_export` defaults).
5. DuckDB sort-pass → `files_slim.parquet` (+ `files_full.parquet` if path wanted), with the ARCH-C-blessed `ORDER BY` / `row_group_size`.
6. Sanity gate: row count within ±X% of `SELECT sum(files_count) FROM torrents WHERE files_data IS NOT NULL` (≈ the live ground truth); abort the swap on a gross mismatch (don't publish a half-built Parquet).
7. Atomic swap `current` → `v<ts>`; write staleness metric; prune old `v*`.

**Resource ask:** request ~2 CPU / 4–8 Gi (decode is CPU-bound; the DuckDB sort wants a few GB + spill). Schedule off-peak. Runs on HEL1 (idle, 24 cores / 125 GB).

---

## 7. Open questions / coordination

- **ARCH-C:** final sort order, row-group size, partition-or-not, bloom-on-extension (the 4 hypotheses I sent). Locks §3.
- **ARCH-B:** query-service language (⇒ Rust vs Go job) + the Parquet **reopen-on-swap** mechanism (per-connection `current/` resolution vs reload signal). Locks §5/§6.
- **ARCH-D:** does this CronJob + DuckDB query service **replace** the Phase-3 Tantivy sidecar entirely (the index is rejected), and does it share/inherit the sidecar's PVC + namespace? Failure isolation (a bad rebuild must never take down browse, which is PG-live anyway).
- **Product — path-FTS is THE genuine carve-out:** ship slim-only first, or slim+full together? The +7.85 GB full Parquet gives `ILIKE` path search that is **CJK-safe** (substring match works on any bytes — unlike Tantivy's CJK-broken tokenizer) but only **best-effort latency** (~0.1 s common+`LIMIT`, ~23 s rare/exhaustive — ARCH-C). My sort/row-group/bloom config cannot fix a leading-wildcard scan. So:
  - If "search file paths" as a **non-interactive, paginated** feature is enough → ship the full Parquet (+7.85 GB), done — no index.
  - If **per-keystroke (<50 ms) free-text path search** is a hard product requirement → that is the **one** thing neither the slim Parquet nor the full-Parquet `ILIKE` delivers, and the only justification for a **path-only Tantivy index** (re-scoped: path field only, a CJK-aware tokenizer, ~+? GB) — gate it on that explicit requirement (GATE #12 question 1). Everything else (ext∧size, collapse, ranges, analytics, exact counts/joins) DuckDB does in ≤1.3 s.

---

## 8. Why this is the right shape (summary)

The benchmark suite proved the cheap composition wins: DuckDB-on-Parquet gives exact per-file search/analytics — with the sorted fact + pre-aggs (~11.7 GB, §3) **every structured query is <150 ms** (the unsorted +3.86 GB form is 35 ms–1.3 s), still beating the 873 M-doc index (+14–25 GB) on cost, capability (exact counts/joins/free-text), and latency on broad filters. ARCH-A makes that **producible and fresh in production**: decode the blobs (the only post-cutover source) with the already-built, G1-correct, format-locked decoder; rebuild a tiny sorted Parquet on a cheap schedule; keep per-torrent browse live via the blob (G2) so only lag-tolerant cross-file search rides the periodic Parquet. Simplicity (full rebuild, atomic swap) is affordable precisely *because* the corpus is small — the same fact that made the index unnecessary.
