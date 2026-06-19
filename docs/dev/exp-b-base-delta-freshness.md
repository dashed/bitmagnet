# EXP-B — Base+Delta Freshness Prototype (DuckDB, empirical)

**Owner:** `duckdb-bench` (team `bitmagnet-bench`, EXP-B / task #32)
**Date:** 2026-06-07
**Env:** HEL1 throwaway restore, DuckDB. Base = sorted fact Parquet `v3_sorted_rg100k.parquet` (12.2 GB, 879.5M rows). Deltas carved from bench `torrent_files`. Single process, 16 threads, warm p50/p95. Timed runs serialized with EXP-A.
**Question:** how fresh can incremental **base+delta** get, and what does it cost?

---

## TL;DR

**Base+delta with latest-wins dedup gives ~minute freshness at <250 ms query cost, no full rebuild — provided you use the predicate-then-anti-join dedup (not a window).** A delta of 100k torrents (≈ hours of crawl) adds only ~90 ms to the hot collapse query (141→230 ms). Supersession (re-crawl replacing a fileset) is handled correctly. Compaction (full rebuild, ~83 s) only needs to run when the delta grows large (≈1M+ torrents).

---

## 1. Delta-append latency (freshness floor)

Carve N most-recent torrents' files → small unsorted Parquet (server-side via DuckDB `postgres_query()`, PG index-driven):

| Delta      | rows      | Parquet | build time |
| ---------- | --------- | ------- | ---------- |
| delta_1k   | 36,444    | 0.2 MB  | 60.7 s     |
| delta_10k  | 365,933   | 1.7 MB  | 70.9 s     |
| delta_100k | 3,849,061 | 18.1 MB | 73.5 s     |

🚨 **The ~60–73 s is a BENCH ARTIFACT, not the real floor.** It's near-constant in N because it's dominated by `SELECT … FROM torrents ORDER BY created_at DESC LIMIT N` — a 47M-row sort to _select_ "recent" torrents. The **marginal** carve+write cost is `(73.5−60.7)s / (100k−1k torrents)` ≈ **0.3 M file-rows/s**. In production the processor **already knows** which torrents are new (they arrive on the queue) and **already has their blobs** — so real delta production = decode N blobs (grounded 0.6–0.94 µs/file) + write, i.e. **sub-second for thousands of torrents**. The freshness floor is effectively the delta-write _interval_, not a 60 s cost.

## 2. Delta-size → query-latency curve (latest-wins, anti-join pattern)

| Layout       | collapse (count distinct torrents, mkv>1GB) | paginated find (LIMIT 1000) |
| ------------ | ------------------------------------------- | --------------------------- |
| base only    | **141 ms**                                  | **56 ms**                   |
| + delta_1k   | 179 ms                                      | 58 ms                       |
| + delta_10k  | 193 ms                                      | 67 ms                       |
| + delta_100k | **230 ms**                                  | **91 ms**                   |

Gentle, near-linear growth: **+~90 ms collapse / +35 ms find** per 100k-torrent delta. Even a 100k-torrent delta (≈ hours of crawl) keeps the hot queries **<250 ms**. Extrapolated, a ~1M-torrent delta → ~1 s collapse — **that's the compaction trigger** (rebuild the sorted base, ~83 s, swap atomically). So **compaction cadence is generous (hourly/daily); deltas carry intra-period freshness.**

## 3. Dedup pattern cost — use the anti-join, never the window

Latest-wins is **torrent-granular** (a re-crawl replaces the _whole_ fileset — `persist.go` `files_data` `DoUpdates`). Measured on delta_100k:

| Dedup SQL                                                                                                              | p50           | Correct?                                                                     |
| ---------------------------------------------------------------------------------------------------------------------- | ------------- | ---------------------------------------------------------------------------- |
| **predicate-then-anti-join** (`WHERE pred AND info_hash NOT IN (SELECT info_hash FROM delta) UNION …delta WHERE pred`) | **230 ms**    | ✅                                                                           |
| window `QUALIFY v = max(v) OVER (PARTITION BY info_hash)`                                                              | **19,040 ms** | ✅ but 80× slower                                                            |
| ⚠️ `QUALIFY row_number() OVER (PARTITION BY info_hash …)=1` (the obvious one)                                          | —             | ❌ **WRONG** — keeps 1 row/torrent, drops a multi-file torrent's other files |

- **Anti-join wins** because the predicate prunes base to the matching row-groups first (zone-maps), then a **hash ANTI join** (`duckdb/src/execution/operator/join/physical_hash_join.cpp:188`) probes that small set against the _tiny_ delta build side. The window approach forfeits all pushdown and partitions **all 879M+ rows** by info_hash → 19 s.
- **Correctness caveat (important):** `row_number()…=1` partitioned by info_hash alone returns one row per torrent — silently wrong for multi-file torrents. The correct window keeps **all** rows of the latest source (`v = max(v) OVER …`), but it's slow; the anti-join is both correct and fast.
- DuckDB mechanics grounded: UNION BY NAME `src/planner/binder/query_node/bind_setop_node.cpp:76`; QUALIFY `src/planner/binder/query_node/plan_select_node.cpp:176`; multi-file `read_parquet` glob/list; hash ANTI/SEMI/MARK join `physical_hash_join.cpp:188`.

## 4. Supersession correctness ✅

Fixture: a base torrent with an mkv>1GB (995 files) **re-crawled** in the delta with its mkv>1GB files removed (991 files, none qualifying).

```
victim in base mkv>1GB set:        True   (expect True)
victim in base+delta result:       False  (✅ re-crawl removed its mkv → excluded)
collapse count: base 1,723,793 → base+delta_super 1,723,792  (Δ=1, ✅)
victim fileset: base 995 rows → latest-wins serves DELTA's 991  (✅)
```

**SUPERSESSION CORRECT = True.** Base+delta is **not** pure-append — `files_data` is upsert-with-update on re-crawl — and the anti-join dedup handles it: the delta version wins, the stale base fileset is fully replaced.

## 5. End-to-end freshness SLA

`new torrent → processor writes it (already has blob) → append to current delta Parquet → queryable (it's in the read_parquet glob)`. No reindex, no migration.

- **Freshness = the delta-write interval** (e.g. a 1-min delta-flush → ~1-min freshness), since delta production is sub-second and query carries it transparently.
- **Query cost of carrying the delta:** <250 ms up to 100k torrents.
- **Compaction:** full sorted rebuild (~83 s, RUN-2) hourly/daily, atomic swap; reset the delta. Trigger when delta latency or size crosses a threshold (~1M torrents / ~1 s).

**Conclusion:** DuckDB-on-Parquet supports **minute-scale freshness** via base+delta at negligible query cost, with correct re-crawl supersession — without giving up the sorted-base pruning that keeps the hot queries fast. The only discipline required: dedup with the **predicate-then-anti-join** pattern, and **compact** before the delta grows to ~1M torrents.

### Reproduce

`bench/exp_b_build.py` (carve deltas + supersession fixture) → `bench/exp_b_measure.py` (curve + dedup cost + correctness). Artifacts in HEL1 `/home/ansible/bench-scratch/delta_*.parquet`.
