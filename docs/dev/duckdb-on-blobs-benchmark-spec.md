# DuckDB-on-Blobs Latency Benchmark — Runnable Spec + Prediction

**Owner:** `duckdb-bench` (team `bitmagnet-bench`, TASK #1)
**Date:** 2026-06-07
**Status:** Design complete + grounded on a real 600-blob decode sample. **Not yet run at scale** (gated on user go-ahead per CLAUDE.md server safety).
**Question being settled:** is "DuckDB-on-blobs latency = 1–10 s" (the cheap-composition keystone, `file-grained-search-team-review.md:75`, spec `file-grained-search-spec.md:323`) TRUE on the real corpus? This benchmark validates/refutes it and settles the index-vs-cheap GATE.

---

## 0. TL;DR — prediction (before running)

The "1–10 s" claim is **architecture-dependent**, and the design matrix conflates two architectures under one row:

| Architecture                                                                     | Full-corpus `ext+size` / GROUP BY / DISTINCT / percentile | Verdict vs "1–10 s"              |
| -------------------------------------------------------------------------------- | --------------------------------------------------------- | -------------------------------- |
| **A. One-time Parquet export (decoded), DuckDB scans Parquet**                   | **0.2–2 s** (slim, no-path) / 2–10 s (with-path FTS)      | ✅ **VALIDATED** (slim beats it) |
| **B. On-demand decode-per-query, full corpus, no Parquet**                       | **~1–4 min** (decode-bound on 856.8 M files)              | ❌ **REFUTED**                   |
| **C. On-demand decode + PG `torrent_file_summary` prefilter, _selective_ query** | <1–10 s (only when candidate set is small)                | ✅ for selective queries only    |

**Net prediction:** the keystone is **TRUE — but only if "DuckDB-on-blobs" means a one-time/periodic Parquet export (option 4b), not the "+0 GB on-demand decode" of matrix row 4a.** Row 4a's "+0 GB **and** 1–10 s" is internally inconsistent for fleet-wide scans: +0 GB (decode every query) ⟹ minutes; 1–10 s ⟹ you must persist a +3–5 GB slim Parquet (or +18–25 GB with paths). The benchmark below is built to expose exactly this cliff and put real p50/p95 numbers on each cell.

This **strengthens** the cheap-composition recommendation (analytics in 1–10 s at +3–5 GB is a great trade vs the 873 M-doc index), while correcting the cost line: it is **+3–5 GB, not +0 GB**, to actually get 1–10 s.

---

## 1. How blobs enter DuckDB — recommendation

### Decode path (grounded in source)

The blob is `torrents.files_data` = `zstd(msgpack_array[ {i:u32, p:str, e:str, s:u64}, … ])` (`bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:6-9,31-46`). Decoder = `deserialize_files` (`blob.rs:60-63`): `zstd::decode_all` → `rmp_serde::from_slice`. The read path already exists: keyset pagination over `torrents` by `info_hash`, selecting the blob, in `bitmagnet-rs/crates/bitmagnet-db/src/stream.rs:46-49` (`stream_torrents_with_files`) and consumed by `bin/backfill.rs:123-135`.

> ⚠️ **klauspost zstd frames omit the embedded content size** — a one-shot `decompress(blob)` fails with `could not determine content size in frame header`. Use a streaming reader (`zstd::stream::read::Decoder` in Rust; `ZstdDecompressor().stream_reader(...)` in Python). The Rust `zstd::decode_all` in `blob.rs:61` already handles this; a naive Python port must not call `decompress()` directly. (Hit and fixed during grounding.)

### 🚨 G1 caveat — extension MUST be path-derived

The exporter must derive `extension` via `file_extension_from_path(path)` (`bitmagnet-rs/crates/bitmagnet-model/src/enums.rs:293-306`), **NOT** the blob's `e` field. The blob `e` is empty for crawl-path torrents (measured: **4.1 %** of files have empty `e`; `transform.rs:64-66` currently reads `f.extension` directly — that's the G1 deploy-time bug for the Tantivy facet). DuckDB analytics must be uniformly path-derived so `ext='mkv'` is correct for crawl-path torrents too. The export query string is identical for slim/full; only `path` is dropped in slim.

`file_extension_from_path` rules (port exactly): lowercase; take substring after the last `.`; reject if the char before the dot is `/` or `.`; reject if the extension is empty or contains anything outside `[a-z0-9]`.

### Recommended architecture: **one-time / periodic Parquet export (option 4b)** — NOT on-demand (4a)

Decode `files_data` **once** into two Parquet files, then point DuckDB at them:

- **`files_slim.parquet`** = `(info_hash BLOB, file_index UINT32, extension VARCHAR, size UBIGINT)` — **drops `path`**. Covers every query in §2 except path-FTS. Predicted **3–5 GB** (matches design 4b). This is the primary analytics surface.
- **`files_full.parquet`** = adds `path VARCHAR` — only needed for path-FTS / "find a file named X". Predicted **18–25 GB**.

**Why export, not on-demand:** the predicate columns (`extension`, `size`) are inside the opaque compressed blob; there is no PG index on them, so any fleet-wide predicate forces a decode of **all** 856.8 M files. At the measured **0.94 µs/file** that is ~13 min single-thread (§3) — far outside 1–10 s. Exporting pays that decode **once**; every subsequent DuckDB query then scans columnar Parquet at GB/s with predicate/zone-map pushdown. On-demand decode (4a) is only competitive for **point queries** (one torrent's blob) or **highly selective** PG-prefiltered queries (option C).

**Exporter implementation (recommended): a small Rust `bin/blob_export.rs`** in `bitmagnet-search`, reusing `stream_torrents_with_files` (`stream.rs:58`) + `deserialize_files` (`blob.rs:60`) + `file_extension_from_path` (`enums.rs:293`) → Arrow `RecordBatch` → `parquet` writer (ZSTD, row-group 128 MB, dictionary on `extension`/`info_hash`). Add `arrow`/`parquet` crates. Parallelize decode across info_hash key-ranges (the K-way scheme already proven in the Phase-1 backfill). A Python/`uv` exporter (`zstandard`+`msgpack`+`pyarrow`) is acceptable for the benchmark itself and ~2–3× slower (the grounding harness already decodes correctly).

DuckDB then needs **no extension** — `read_parquet('files_slim.parquet')` is native.

---

## 2. Representative query set (runnable SQL)

Against `files_slim.parquet` (aliased `f`) unless noted. These mirror the parity questions the dropped `torrent_files` answered.

```sql
-- Q1  per-file conjunction: "find all .mkv files > 1 GB" (the discriminator query)
SELECT info_hash, file_index, size
FROM read_parquet('files_slim.parquet')
WHERE extension = 'mkv' AND size > 1000000000;

-- Q2  GROUP BY extension — file-type histogram over 873M files
SELECT extension, count(*) AS n, sum(size) AS bytes
FROM read_parquet('files_slim.parquet')
GROUP BY extension
ORDER BY n DESC
LIMIT 50;

-- Q3  distinct-torrent collapse — "how many TORRENTS have a >1GB mkv"
--     (the exact distinct-torrent count Tantivy 0.26 cannot collapse)
SELECT count(DISTINCT info_hash) AS torrents
FROM read_parquet('files_slim.parquet')
WHERE extension = 'mkv' AND size > 1000000000;

-- Q4  percentile / analytics — size distribution for a hot extension
SELECT
  count(*)                                   AS n,
  approx_quantile(size, 0.50)                AS p50,
  approx_quantile(size, 0.95)                AS p95,
  approx_quantile(size, 0.99)                AS p99,
  max(size)                                  AS max
FROM read_parquet('files_slim.parquet')
WHERE extension = 'mkv';

-- Q5  two-sided range (the case the §13.2 aggregate canNOT do, only file-grained can)
SELECT count(DISTINCT info_hash)
FROM read_parquet('files_slim.parquet')
WHERE extension = 'mkv' AND size BETWEEN 1000000000 AND 2000000000;

-- Q6  path FTS (requires files_full.parquet) — "find a file named …"
SELECT info_hash, file_index, path, size
FROM read_parquet('files_full.parquet')
WHERE path ILIKE '%1080p%' AND extension = 'mkv'
LIMIT 100;
```

Q1–Q5 scan only `extension`+`size`(+`info_hash`) — DuckDB prunes the (absent) `path` column entirely. Q6 is the only query that pays the big `path` column.

---

## 3. Methodology

### Data source — settled: full-corpus on a throwaway restored PG (no sampling needed)

Per bench-harness (#4): a **restorable ~35 GB pre-cutover `pg_dump` already sits on HEL1**. The bench restores it to a **throwaway PostgreSQL on idle HEL1** (gated) and the export reads the blobs from **that** DB — so the live FSN1 PG is never touched by the bench, and the DuckDB latency numbers are **full-corpus (all 16.97 M with-files torrents / 856.8 M files), validity total — no sampling, no extrapolation required.**

The 100 k / 1 M tiers below are kept **only as smoke tests / warm-up** (fast iteration on the harness and a sanity check that latency scales ~linearly in rows); they are **not** the basis for the reported numbers — the Full row is.

| Tier                  | Torrents    | Files (≈51/torrent) | Slim Parquet (pred.) | Purpose                             |
| --------------------- | ----------- | ------------------- | -------------------- | ----------------------------------- |
| S1 (smoke)            | 100 k       | ~5.1 M              | ~30 MB               | harness shakeout / warm-cache floor |
| S2 (smoke)            | 1 M         | ~51 M               | ~0.3 GB              | linearity sanity check              |
| **Full (the result)** | **16.97 M** | **856.8 M**         | **3–5 GB**           | reported p50/p95 — total validity   |

Extract S1/S2 with `TABLESAMPLE SYSTEM` against the restored DB; Full = a plain full scan of the restored DB via the keyset stream. Report **measured** slim/full Parquet bytes vs predicted (3–5 GB / 18–25 GB).

### Per-query protocol

- **Cold cache:** drop OS page cache before the first run (`echo 3 > /proc/sys/vm/drop_caches` on the bench host, or a fresh DuckDB process + `PRAGMA disable_object_cache`). Measures Parquet read from disk.
- **Warm cache:** repeat R = 20 times in-process; discard run 1; report **p50 / p95** of runs 2..R.
- **Throughput:** record `rows scanned`, wall ms ⟹ **rows/s** and **MB/s** (Parquet bytes touched). DuckDB `EXPLAIN ANALYZE` gives scanned-rows + pushdown confirmation.
- **Threads:** sweep `SET threads=1` and `threads=<ncores>` to get the parallel speedup curve (DuckDB scans Parquet at ~1–5 GB/s/core).
- **Decode/export cost (one-time):** time the full export separately; report files/s and wall-clock — this is the amortized cost that 4a pays _per query_ and 4b pays _once_.

### Runnable bench driver (Python + DuckDB via `uv`)

```python
# /// script
# dependencies = ["duckdb"]
# ///
import duckdb, time, statistics, sys
PARQUET = sys.argv[1] if len(sys.argv) > 1 else "files_slim.parquet"
QUERIES = {  # name -> sql (see §2)
  "Q1_mkv_gt1g": f"SELECT info_hash,file_index,size FROM read_parquet('{PARQUET}') WHERE extension='mkv' AND size>1e9",
  "Q2_groupby":  f"SELECT extension,count(*),sum(size) FROM read_parquet('{PARQUET}') GROUP BY extension ORDER BY 2 DESC LIMIT 50",
  "Q3_distinct": f"SELECT count(DISTINCT info_hash) FROM read_parquet('{PARQUET}') WHERE extension='mkv' AND size>1e9",
  "Q4_pctile":   f"SELECT count(*),approx_quantile(size,0.5),approx_quantile(size,0.95),approx_quantile(size,0.99) FROM read_parquet('{PARQUET}') WHERE extension='mkv'",
  "Q5_range":    f"SELECT count(DISTINCT info_hash) FROM read_parquet('{PARQUET}') WHERE extension='mkv' AND size BETWEEN 1e9 AND 2e9",
}
R = 20
for ncores in (1, 0):  # 0 = all cores
    con = duckdb.connect()
    if ncores: con.execute(f"SET threads={ncores}")
    for name, sql in QUERIES.items():
        ts = []
        for i in range(R):
            t = time.perf_counter(); con.execute(sql).fetchall(); ts.append(time.perf_counter()-t)
        warm = sorted(ts[1:])
        p50 = warm[len(warm)//2]; p95 = warm[int(len(warm)*0.95)]
        print(f"threads={ncores or 'all'} {name:14s} cold={ts[0]*1e3:8.1f}ms p50={p50*1e3:8.1f}ms p95={p95*1e3:8.1f}ms")
```

Run: `uv run bench.py files_slim.parquet`. (DuckDB ships embedded; no server.)

---

## 4. Where to run + resource envelope

**Run on HEL1** (`alberto-hetzner` agent node, Helsinki) — i9-12900K (8P+8E = 24 threads), **125 GB RAM**, 1.8 TB, and **idle** (4 DaemonSet pods only; FSN1 is at ~83 % mem). Confirmed spare capacity in MEMORY (K3s cluster notes). DuckDB is a single embedded process; cap it with `SET memory_limit='32GB'` and `SET threads=16` to stay polite. No K8s deploy needed — copy the two Parquet files to a local-path PVC or host dir and run `uv run bench.py`.

**FSN1 (live PG) is never touched by the bench** — the export reads blobs from the **throwaway restored PG on HEL1** (the 35 GB pre-cutover dump, §3), so there is no live-host load or K-way throttling concern at all. The only live-PG touch in this whole effort was the small read-only grounding probe already done (§5). Restore + bench are entirely HEL1-local.

**Envelope (all on HEL1):**

- Restore the 35 GB dump to throwaway PG: ~10–30 min (one-time, gated).
- Export (one-time): read 15.6 GB blob from local restored PG + parallel decode 856.8 M files → **~1–4 min** (Rust, 16 threads) / ~10–15 min (single-thread Python). Peak RAM modest (streamed). Disk: +3–5 GB slim (+18–25 GB if also writing full).
- Per-query bench: seconds; RAM < 32 GB; trivially fits HEL1.
- Total wall-clock for the whole benchmark (export + all tiers + all queries, cold+warm): **~30–60 min**.

---

## 5. Grounding numbers (measured safely, 2026-06-07)

Read-only probes against live PG (`bitmagnet-postgres-0`, idle), + local decode of a 600-blob `TABLESAMPLE` pulled once (~1.8 MB hex).

**Catalog (instant, `pg_class`):**

- `torrents`: heap 13 GB, **total 36 GB** (TOAST+idx 22 GB — the `files_data` blob lives in TOAST), reltuples ~47.9 M (all torrents).
- `torrent_files`: **856,788,288 rows, 120 GB** ← the table the migration drops; the corpus the export must reproduce.
- `torrent_file_summary`: 15.5 M rows, 2 GB.

**Distribution (`TABLESAMPLE SYSTEM 0.03 %`, 15,264 torrents / 5,426 with-blob):**

- **35.5 %** of torrents carry a blob → 0.355 × 47.9 M ≈ **17 M with-files** ✓ (matches the 16.97 M figure).
- avg **compressed blob ≈ 919 B**; avg **51 files/torrent** → 16.97 M × 51 ≈ 866 M ≈ the 856.8 M `torrent_files` rows ✓; 16.97 M × 919 B ≈ **15.6 GB blob corpus** ✓ (matches "~16 GB").

**Decode (600 blobs, local Python, C-backed zstd+msgpack):**

- msgpack keyset == `{i,p,e,s}` ✓ — decode correctness confirmed against `blob.rs`.
- zstd ratio **4.96×** (1543 B → 7645 B avg in this sample).
- **0.94 µs/file, 1.06 M files/s single-thread.** (Rust reusing `blob.rs` should be ~1.5–3× faster.)
- blob `e` empty for **4.1 %** of files; of those, only 3.8 % gain a path-derived ext → **G1 blast radius is small but real**; the exporter path-deriving ext is correct and cheap.

**Extrapolation to full corpus:**

- Single-thread decode: 856.8 M × 0.94 µs = **~805 s ≈ 13.4 min** (Python). Rust 16-thread: **~20–50 s**.
- This is the number that **decides 4a vs 4b**: a per-query full decode is minutes ⟹ on-demand (4a) can't be 1–10 s for fleet-wide scans; a one-time export (4b) makes every subsequent query a Parquet scan.

---

## 6. Prediction (what the bench will show)

| Query (full corpus, slim Parquet, warm) | Predicted p50 | Predicted p95 | vs 1–10 s     |
| --------------------------------------- | ------------- | ------------- | ------------- |
| Q1 `ext='mkv' AND size>1e9`             | 0.2–0.8 s     | <1.5 s        | ✅ beats it   |
| Q2 GROUP BY extension                   | 0.5–2 s       | 2–3 s         | ✅ within     |
| Q3 COUNT DISTINCT info_hash             | 0.5–2 s       | 2–4 s         | ✅ within     |
| Q4 percentiles (`approx_quantile`)      | 0.3–1.5 s     | <2 s          | ✅ within     |
| Q5 two-sided range                      | 0.5–2 s       | 2–4 s         | ✅ within     |
| Q6 path FTS (full Parquet, +path)       | 2–8 s         | 5–12 s        | ✅/borderline |
| **Cold cache (first hit, disk read)**   | +1–4 s        | —             | ✅ within     |
| On-demand full decode-per-query (4a)    | **60–250 s**  | —             | ❌ refutes    |

**Verdict (to be confirmed by the run):** **DuckDB-on-blobs validates the 1–10 s claim — and for the common slim analytical queries is actually sub-second to ~2 s — _provided_ it is implemented as a one-time/periodic decoded Parquet export (4b, +3–5 GB), not naive on-demand blob decode (4a), which is minutes at fleet scale.** Path-FTS (Q6) is the only query that flirts with the 10 s ceiling and needs the +18–25 GB path Parquet.

**Implication for the GATE:** the cheap-composition path is real and the analytics tier is cheap — but the honest cost of the 1–10 s DuckDB tier is **+3–5 GB and a one-time ~minutes export**, not "+0 GB." The product decision (`team-review.md:75`) should be framed as: _interactive <50 ms per-file search + path FTS (the 873 M-doc index, +8–15 GB) vs 0.2–2 s analytics on a +3–5 GB Parquet (DuckDB) — is sub-second-but-not-per-keystroke acceptable, and do you need realtime freshness (the index updates live; the Parquet is a periodic export)?_

---

## 7. Source references

- Blob format + decoder: `bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:6-9,31-46,60-63`.
- Read/stream path: `bitmagnet-rs/crates/bitmagnet-db/src/stream.rs:46-49,58`; `crates/bitmagnet-search/src/bin/backfill.rs:123-135,165-174`.
- Ext-from-path (G1): `bitmagnet-rs/crates/bitmagnet-model/src/enums.rs:293-306`; current direct-`e` bug `crates/bitmagnet-search/src/transform.rs:64-66`.
- Design context: `docs/dev/perfile-search-with-blob-design.md` (P1, options 4a/4b); `docs/dev/file-grained-search-spec.md:309-331` (§13.3/13.4); `docs/dev/file-grained-search-team-review.md:41,75,87`.
- Corpus reality: live `pg_class` + `TABLESAMPLE` probes (this doc §5); homelab `docs/bitmagnet-fork-deploy-plan.md`.
  </content>
  </invoke>
