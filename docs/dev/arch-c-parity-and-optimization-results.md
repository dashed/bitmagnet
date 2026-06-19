# ARCH-C — Complete-Parity Catalog + Latency Optimization (empirical)

**Owner:** `duckdb-bench` (team `bitmagnet-bench`, ARCH-C / tasks #16 + ARCH-F #19)
**Date:** 2026-06-07
**Env:** HEL1 throwaway restore (pre-cutover dump), DuckDB over the real **879,474,852-row** `torrent_files` Parquet (`files_slim` 3.86 GB / `files_full`+path 11.71 GB), 24 cores / 32 GB mem limit, warm p50 unless noted. Read-only; all artifacts in `/home/ansible/bench-scratch`.
**Source grounding:** local DuckDB C++ checkout `/Users/me/aaa/github/duckdb` (citations inline).

---

## 1. Complete-parity query catalog (5 workloads — all proven correct)

| # | Workload | Proven query (shape) | Latency | Notes |
| - | -------- | -------------------- | ------- | ----- |
| 1a | per-file **ext ∧ size** | `WHERE extension='mkv' AND size>1e9` ORDER BY size DESC LIMIT 50 | **1047 ms** (top-N sort of 5.7M) / **30 ms** unordered LIMIT 1000 | exact |
| 1b | per-file **path-FTS** (ILIKE) | `WHERE path ILIKE '%1080p%'` | **23–27 s** (COUNT/no-LIMIT) | CJK 电影 + cyrillic Фильм **sample_ok=True → ILIKE is CJK-safe** |
| 2 | per-torrent **file listing** | `WHERE info_hash=? ORDER BY file_index LIMIT 25` + `count(*)` totalCount | **24 ms** | prod browse uses blob/G2; this is the SQL parity |
| 3 | **distinct-torrent collapse** + deep **keyset** paging | `… GROUP BY info_hash … WHERE info_hash > :last ORDER BY info_hash LIMIT 50` | page1 **1331 ms**, keyset-next **1324 ms** (no degradation), OFFSET-100k 1502 ms | keyset deep paging is flat |
| 4 | **analytics + JOIN→PG** | percentiles; `slim JOIN pg.torrent_contents … content_type='movie'` | percentiles **1102 ms**; JOIN **1491 ms** (movie ∧ mkv>1GB = **728,574** torrents) | PG cols materialized once (48.1M rows, 10.9 s) |
| 5 | **exact counts** | `count(*)` / `count(DISTINCT info_hash)` | files mkv>1GB=**5,699,629** (1014 ms); distinct torrents=**1,723,793** (1297 ms); total=**879,474,852** (**3 ms**, metadata) | exact |

**Conclusion:** every torrent_files workload is a concrete DuckDB query; all correct. Only **path-FTS (substring)** is slow (full scan). Everything structured is ≤1.3 s pre-optimization.

---

## 2. Optimization matrix (the heavy ~1.2–1.3 s queries → before/after)

**Artifact sizes:** slim(info_hash-order) **3.86 GB** · v1 sorted(ext,size) **10.30** · v2 sorted no-bloom 10.14 · v3 sorted rg=100k 12.21 · v8 sorted rg=default(122880) 12.09 · v4 hive(coarse file_category) 12.16 · v5 native+ART(ext) **27.47** · v6 agg_ext **0.001** (47,628 rows) · v7 agg_torrent_ext **1.39** (56,046,830 rows).
🚨 **Sorting by (ext,size) decorrelates info_hash → its RLE/dictionary runs collapse → 3.86 → 10.30 GB (+6.4 GB).** Smaller row groups compress even worse (~12 GB).

| Query | v0 unsorted | v1 sorted(ext,size) | v3 rg=100k | v5 native | pre-agg | Winner |
| ----- | ----------- | ------------------- | ---------- | --------- | ------- | ------ |
| A. paginated FIND, common mkv (LIMIT 1k) | **30 ms** | 56 ms | 59 ms | **1.9 ms** (ART) | — | native / v0 (early-out already fast) |
| A2. FIND, **rare** ext (epub) | 48 ms | **19 ms** | 64 ms | — | — | sorted (zonemap prune) |
| B. distinct-torrent **collapse** | 1311 ms | 132 ms | 147 ms | 71 ms | **5.2 ms** (v7) | **pre-agg** |
| C. **GROUP BY** extension | 1425 ms | 751 ms | — | — | **12.6 ms** (v6) | **pre-agg** |
| D. **two-sided range** distinct torrents | 1255 ms | 109 ms | 103 ms | **40 ms** | (can't) | sorted/native |
| E. exact **COUNT** files (ext∧size) | 1024 ms | **17 ms** | 51 ms | — | 6.3 ms (approx) | sorted |

**Bloom isolation (A2):** v1 bloom-on 19 ms ≈ v2 bloom-off 18 ms → **bloom adds nothing once sorted by ext** (min/max already prunes; bloom is equality-only).

### Lever → mechanism (DuckDB source)
- **Sort (ext,size) → row-group min/max pruning:** `extension/parquet/parquet_reader.cpp:1308` (`expr_filter.CheckStatistics(*min_max_stats)` → `FilterPropagateResult`), min/max from `parquet_statistics.cpp:62-74`; filter pushed by `src/optimizer/pushdown/pushdown_get.cpp:16,50`; parquet scan declares `filter_prune=true` (`parquet_multi_file_info.cpp:434`). Sorting makes per-row-group [min,max] tight/non-overlapping ⇒ the scan skips non-matching groups.
- **Bloom filter:** `parquet_statistics.cpp:802` `BloomFilterExcludes` / `:741,754` `ApplyBloomFilter`+`FilterCheck` (equality only); writer default-on `parquet_extension.cpp:99,143`. Redundant with contiguous min/max ⇒ no measurable gain when sorted by ext.
- **row_group_size:** finer groups = finer pruning vs more metadata/worse compression; **1M won** (smallest file 10.3 GB + best/tied latency).
- **Hive (coarse file_category):** file-level partition pruning before row-group stats; video find 14 ms, but adds dir/metadata complexity for no win over sorted/native (and PARTITION_BY(extension) is a trap: **47,628 distinct exts** → 47k files).
- **Native .duckdb zonemaps + ART:** per-row-group zonemap `src/storage/table/row_group.cpp:668` `CheckZonemap` (gates scan `:386,415`); ART scan `src/execution/index/art/art.cpp:154,236,240`.
- **Pre-agg:** no scan — a lookup over a tiny/medium table.

---

## 3. Index question — "can we ADD indexes (disk cost) to improve things?"

| Lever | Mechanism (source) | Before → After | Added disk | Verdict |
| ----- | ------------------ | -------------- | ---------- | ------- |
| **ART CREATE INDEX** (ext,size + info_hash) on a native table | optimizer has **no analytical IndexScan** (`src/optimizer/*` — grepped, none); ART equality/range exists `art.cpp:154` but planner won't pick it for range | ext∧size `uses_index_scan=**FALSE** (seq_scan)`; count 1270→**314 ms** (from **zonemaps**, not ART); info_hash point **0.2 ms** | **+50 GB** (native table + 2 ART; build 668 s + 156 s) | ❌ ART does **not** accelerate analytical ranges; native zonemaps do. Huge disk. Not worth it. |
| **Native rollup TABLES** (per-ext, size-hist, per-(torrent,ext)) | scan → lookup | GROUP BY 1.28 s→**2.3 ms**; histogram 1.15 s→**2.85 ms**; one-sided collapse 1.27 s→**31.8 ms** | **+1.99 GB** | ✅✅ **THE <50 ms lever** (also works on the unsorted file) |
| **FTS / BM25** (`PRAGMA create_fts_index` on path) | inverted index (out-of-tree ext; `extension_entries.hpp` registers `fts`) | path search **23 s → 147–186 ms** (1080p/bluray/电影) | **+34.9 GB** (extrap. from 20M→0.79 GB; **build ≈ 27 min**) | ⚠️ the only "fast path-FTS" option, but big + slow build; **tokenizer = no CJK segmentation** (matched the exact 电影 token here, but a sub-token CJK query would miss). ILIKE stays the CJK-correct-but-slow fallback. |

**Bottom line on the user's question:** *Adding ART indexes does NOT help analytical ext∧size (confirmed by EXPLAIN — DuckDB has no analytical index-scan).* The real "indexes" that help are **(a) pre-aggregated rollup tables** (+2 GB → GROUP BY/collapse/histogram <35 ms) and **(b) a sorted layout** (zone-map pruning → ranges/counts <150 ms). Free-text path search is the only workload that needs a true inverted index (FTS +35 GB, or Tantivy) — everything else is solved at +2 GB.

---

## 4. ARCH-F — future queries are just new SQL (no re-index)

| Future query | p50 | Tier |
| ------------ | --- | ---- |
| Season-pack: ≥8 mkv >300 MB per torrent (`GROUP BY info_hash HAVING count(*)>=8`) | **1.3 s** | scan-bound (≈Q4); <35 ms with a pre-agg variant |
| Faceting: ext → count + `count(DISTINCT info_hash)` | **2.7 s** | heavier (per-ext distinct); pre-agg → ms |
| Cross-torrent dup `(path,size)` across torrents (`GROUP BY path,size HAVING n>1`) | **134 s** | ⚠️ pathological ~800M-group aggregate — a **batch** job, not interactive |

→ All answerable as plain SQL on the existing Parquet, **no schema/index change**. Only the all-pairs dup-detection is a heavy batch.

---

## 5. Recommended production layout

**Writer (refresh Job):**
```sql
COPY (SELECT info_hash, file_index, extension, size  -- slim; + path for the full file
      FROM … ORDER BY extension, size)
TO 'files_slim.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000, WRITE_BLOOM_FILTER false);
```
**Plus ship two pre-agg rollup tables** (DuckDB `.duckdb` or Parquet): `agg_ext(extension,n,bytes,min,max)` and `agg_torrent_ext(info_hash,extension,max_size,min_size,fc)`.

**Resulting latency (all warm, RAM-resident):** GROUP BY **2.3 ms** · size-histogram **2.85 ms** · one-sided collapse **5–32 ms** · two-sided range **40–109 ms** · exact count **17 ms** · paginated find **30–56 ms** · point hydrate **0.2 ms**. **Disk ≈ 12.3 GB** (sorted slim 10.3 + rollups 2.0).
**Cheaper alt (5.9 GB):** keep slim **unsorted** (3.86) + rollups (2.0) — GROUP BY/collapse/counts still <35 ms via rollups, but two-sided-range / rare-ext-find / exact-file-count stay ~1.0–1.3 s (no pruning).

**The one carve-out:** free-text **path-FTS** — ILIKE is correct (incl. CJK) but ~23 s; DuckDB FTS/BM25 makes it ~150 ms at +35 GB / 27-min build but loses CJK segmentation. This is the sole workload a Tantivy (path-only, CJK-aware) index uniquely serves <50 ms per keystroke.
