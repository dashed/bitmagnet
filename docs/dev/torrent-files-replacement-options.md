# Replacing `torrent_files` — the options (one-page overview)

**Date:** 2026-06-09 · **Status:** living summary. Every number below is **measured** (HEL1 throwaway restore / deployed FSN1), not estimated.
**This is the map.** Each option links to its authoritative deep doc; the full narrative is the [master design+results doc](./per-file-search-master-design-and-results.md); the footprint math is [`space-savings-vs-torrent-files.md`](./space-savings-vs-torrent-files.md).

---

## The problem

`torrent_files` = **~276 GB / ~880 M rows / 74 % of the bitmagnet DB** (heap 119 + indexes 157; dropping it takes the DB 397 → ~121 GB). It isn't one feature — it backs four, and "replacing it" means covering each:

| | capability | who serves it after the drop |
|---|---|---|
| **(a)** | ext/file-type **filter + facet** in content search — *the "DROP gate"* | L1 `file_extensions` JSONB |
| **(b)** | per-torrent **file browser** (list a torrent's files) | L1 blob |
| **(c)** | **hydration** (render file rows in results) | L1 blob |
| **(d)** | net-new **per-file / cross-file search** ("find .mkv > 1 GB", path substring) | L2 (structured) + L3 (free-text) |

The answer is a **layered stack** — stack L1 → L2 → L3, drop `torrent_files` only once each needed layer is proven in prod.

---

## The recommended stack (each measured)

| Layer | What | Size | Covers | Status |
|---|---|---|---|---|
| **L1 — Hybrid Blob** | `files_data` = zstd(msgpack `{i,p,e,s}`) per torrent (~16 GB, 4.96×) + `torrent_file_summary` (~3.3 GB) + `file_extensions` JSONB (+119 MB) | **~19 GB** | (a)(b)(c) | ✅ **DEPLOYED + verified** (real-time dual-write) |
| **L2 — DuckDB-on-Parquet** | blobs → sorted Parquet (per-file) + native rollup tables; gRPC sidecar (HEL1) | **~3.9–12.3 GB** | (d) structured | 📐 designed + benchmarked, **not deployed** |
| **L3 — Tantivy ngram** | per-torrent path-bag char-ngram(2,3) `WithFreqs` | **13.32 GiB (BUILT)** | (d) realtime free-text path | 🟢 **GO (user decision 2026-06-09)** — index built on bench ([PSX](./psx-campaign-RESULTS.md)); deploy pending |

Deep docs: L2 = [`duckdb-parquet-parity-architecture.md`](./duckdb-parquet-parity-architecture.md); L3 = [`pathsearch-master-investigation.md`](./pathsearch-master-investigation.md) + [`pathsearch-microbench-RESULTS.md`](./pathsearch-microbench-RESULTS.md).

---

## Options weighed per capability — winners ✅ and rejected ❌

**(a) DROP gate — ext filter + facet** *(FB-A1, [`fba1-jsonb-dropgate-results.md`](./fba1-jsonb-dropgate-results.md))*
- ✅ **`file_extensions` JSONB `@>`** — +119 MB, real-time, exact parity, ~1 ms facet (budgeted `EXPLAIN`). One flag-gated Go swap (`EXISTS torrent_files` → `@>`) + parity check.
- ❌ **`agg_torrent_ext` PG rollup** — +9.5 GB + delta-upsert pipeline + checker; 80× costlier. Kept only as a *future* option if an `ext ∧ max_size` torrent-grain query is ever needed (JSONB carries no size).

**(d) Per-file structured search** *(RUN-2/3/4, ARCH-C)*
- ✅ **DuckDB-on-Parquet** — +3.9–12.3 GB; every realistic query 0.015–1.3 s (paginated mkv>1GB 35 ms; collapse 32 ms w/ rollups; ranges via row-group pruning). Freshness ~minute (base+delta) or seconds (PG-tail).
- ❌ **slim per-file PG table** — +78–113 GB (RUN-3). Defeats the purpose.
- ❌ **per-file structured Tantivy index** — +14–25 GB, no latency win (scan-bound ~1.3 s) (RUN-4).

**(d) Free-text path search — the L3 carve-out** *(PS-T1–T5 + PS-MB1)*
- ✅ **per-torrent path-bag char-ngram(2,3) `WithFreqs`** — **BUILT at 13.32 GiB** (PSX; confirms the 13.54 computed), p50 24.7 ms, CJK sub-ms, recall 1.0000, latency-neutral vs positions. **Production-shape p95** (TopDocs-by-seeders, the real page collector) on the broadest single grams ≈ **77–94 ms** (the 55–65 ms `Count` figure was a lower bound); realistic multi-word queries **< 50 ms**; the broad tail is engine-irreducible → UX (debounce/min-chars).
- ❌ **per-file ngram** (~90 GB, 873 M docs) — footprint-tripler; latency breaks at scale.
- ❌ **edge-ngram** — bigger in prod (21.3 GiB) *and* misses substrings (`264`→0.19 recall).
- ❌ **external engines** — Meilisearch/Typesense are *prefix not infix*; Quickwit misses local <50 ms; pg_trgm loses 3 ways; **Manticore** the lone gated-spike candidate.
- **Whole layer NO-GO by default** — no demonstrated demand; purely additive; never gates the DROP.

---

## Space-savings scenarios (vs 276 GB)

| scenario | footprint | savings |
|---|---|---|
| Drop + **L1 only** (migration) | ~19 GB | **−93 %** |
| + cheap **L2** | ~27 GB | −90 % |
| + optimized **L2** | ~35 GB | **−87 %** |
| + **L3** free-text index (per-torrent, measured) | ~48 GB | **−83 %** |

The L3 line *used* to read −55 % on the per-FILE index; **PS-MB1 measured (and PSX then BUILT) the per-torrent form at 13.32 GiB**, so even with interactive free-text the saving stays ~83 %. **L3 is now a GO (user decision 2026-06-09)** — the bench artifact exists; deployment is the remaining step.

---

## The hard rule

**Don't drop `torrent_files` until each needed replacement layer is DEPLOYED *and* PROVEN in production.** Order: **L1 ✅ → L2 (deploy + prove parity/latency) → L3 (now a GO; bench-built, deploy after/with L2) → DROP last, gated.** `torrent_files` stays the live fallback/source-of-truth throughout; the **DROP is deferred indefinitely**.

**Next concrete step toward the drop:** deploy and prove **L2** (DuckDB-on-Parquet) in prod; L3 deploy follows per [`pathsearch-T4-deploy-ops.md`](./pathsearch-T4-deploy-ops.md).
