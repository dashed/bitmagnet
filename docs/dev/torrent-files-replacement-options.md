# Replacing `torrent_files` — the options (one-page overview)

**Date:** 2026-06-12 · **Status:** living summary. Every number below is **measured** (HEL1 restore/sidecar, FSN1 builder, or live K3s prod), not estimated.
**This is the map.** Each option links to its authoritative deep doc; the full narrative is the [master design+results doc](./per-file-search-master-design-and-results.md); the footprint math is [`space-savings-vs-torrent-files.md`](./space-savings-vs-torrent-files.md).

> ### 🟢 2026-06-15 UPDATE — the layer stack is COMPLETE; the DROP is now unblocked
>
> **L1 ✅ + L2 ✅ + L3 ✅** are all deployed and proven: the gate-7 L3 path-search route is LIVE on the
> user-facing URL (serve-split read pod, image `gate7-9`, recall 1.0 / precision 100%), exact-refining
> against L1 blobs. So the hard rule "don't drop `torrent_files` until each replacement layer is deployed
> **and** proven" is now SATISFIED. The DROP stays deferred only on its remaining preconditions (all OPEN):
> the **G1 fix** (`FileExtensionFromPath` for empty crawl-path-blob extensions), the crawl-path
> `(info_hash, file_index)` set-equality parity check, a fresh off-host backup, a proving/soak window, then
> explicit user go. (Note: the L2 DuckDB sidecar is _not_ in the live query path — L3 refines against L1
> blobs — so the L2 `fileSearch` Go consumer being unwired does NOT block the DROP.)
> Consolidated status + roadmap: homelab `docs/bitmagnet/gate7-l3-LIVE-status-and-roadmap.md`.

---

## The problem

`torrent_files` = **~276 GB / ~880 M rows / 74 % of the bitmagnet DB** (heap 119 + indexes 157; dropping it takes the DB 397 → ~121 GB). It isn't one feature — it backs four, and "replacing it" means covering each:

|         | capability                                                                    | who serves it after the drop     |
| ------- | ----------------------------------------------------------------------------- | -------------------------------- |
| **(a)** | ext/file-type **filter + facet** in content search — _the "DROP gate"_        | L1 `file_extensions` JSONB       |
| **(b)** | per-torrent **file browser** (list a torrent's files)                         | L1 blob                          |
| **(c)** | **hydration** (render file rows in results)                                   | L1 blob                          |
| **(d)** | net-new **per-file / cross-file search** ("find .mkv > 1 GB", path substring) | L2 (structured) + L3 (free-text) |

The answer is a **layered stack** — stack L1 → L2 → L3, drop `torrent_files` only once each needed layer is proven in prod.

---

## The recommended stack (each measured)

| Layer                      | What                                                                                                                                        | Size                                                        | Covers                                                        | Status                                                                                                                                                                                                              |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **L1 — Hybrid Blob**       | `files_data` = zstd(msgpack `{i,p,e,s}`) per torrent (~16 GB, 4.96×) + `torrent_file_summary` (~3.3 GB) + `file_extensions` JSONB (+119 MB) | **~19 GB**                                                  | (a)(b)(c)                                                     | ✅ **DEPLOYED + verified** (real-time dual-write)                                                                                                                                                                   |
| **L2 — DuckDB-on-Parquet** | blobs → sorted Parquet (per-file) + native rollup tables; gRPC sidecar (HEL1)                                                               | **~3.9–12.3 GB** steady-state, sorted fact currently ~21 GB | (d) structured                                                | ✅ **DEPLOYED + PROVEN** — image `l2-11` on HEL1, GATE A passed, frozen GATE C accepted (known dup-path superset), minute freshness live, hard 10s deadline behavior proven; path-query acceleration deferred to L3 |
| **L3 — Tantivy ngram**     | per-torrent path-bag char-ngram(2,3) `WithFreqs` + delete-key                                                                               | **14.0 GiB (BUILT, keyed)**                                 | (d) realtime free-text path + fast `collapse:path` candidates | 🟢 **GO (user decision 2026-06-09)** — built ([PSX](./psx-campaign-RESULTS.md)) + concurrency-validated ([CB](./cb-campaign-RESULTS.md)); deploy pending                                                            |

Deep docs: L2 = [`duckdb-parquet-parity-architecture.md`](./duckdb-parquet-parity-architecture.md); L3 = [`pathsearch-master-investigation.md`](./pathsearch-master-investigation.md) + [`pathsearch-microbench-RESULTS.md`](./pathsearch-microbench-RESULTS.md).

---

## Options weighed per capability — winners ✅ and rejected ❌

**(a) DROP gate — ext filter + facet** _(FB-A1, [`fba1-jsonb-dropgate-results.md`](./fba1-jsonb-dropgate-results.md))_

- ✅ **`file_extensions` JSONB `@>`** — +119 MB, real-time, exact parity, ~1 ms facet (budgeted `EXPLAIN`). One flag-gated Go swap (`EXISTS torrent_files` → `@>`) + parity check.
- ❌ **`agg_torrent_ext` PG rollup** — +9.5 GB + delta-upsert pipeline + checker; 80× costlier. Kept only as a _future_ option if an `ext ∧ max_size` torrent-grain query is ever needed (JSONB carries no size).

**(d) Per-file structured search** _(RUN-2/3/4, ARCH-C)_

- ✅ **DuckDB-on-Parquet** — +3.9–12.3 GB steady-state, sorted fact currently ~21 GB; deployed sidecar shapes are in the **0.37–3.4 s** class for structured find/collapse/count/facet, with pathological path scans guarded by the 10 s deadline. Freshness is minute-delta + self-reload, proven at `delta_age_seconds=50-54`.
- ❌ **slim per-file PG table** — +78–113 GB (RUN-3). Defeats the purpose.
- ❌ **per-file structured Tantivy index** — +14–25 GB, no latency win (scan-bound ~1.3 s) (RUN-4).

**(d) Free-text path search — the L3 carve-out** _(PS-T1–T5 + PS-MB1)_

- ✅ **per-torrent path-bag char-ngram(2,3) `WithFreqs`** — **BUILT at 14.0 GiB with the production delete key** (13.32 GiB keyless in PSX; 14.0 GiB keyed in CB), p50 24.7 ms, CJK sub-ms, recall 1.0000, latency-neutral vs positions. **Production-shape p95** (TopDocs-by-seeders, the real page collector) on the broadest single grams ≈ **77–94 ms** (the 55–65 ms `Count` figure was a lower bound); realistic multi-word queries **< 50 ms**; the broad tail is engine-irreducible → UX (debounce/min-chars). This is also the first-pass candidate engine for fast `collapse:path`: L3 returns candidate `info_hash` values, then blob/L2 exact-refines the substring and any structured filters.
- ✅ **Batch/cache only:** cross-torrent duplicate discovery by `(path,size)` is a scheduled materialized rollup (`path_hash,size → torrent_count + samples`) with exact path verification, not a live DuckDB GROUP BY.
- ❌ **per-file ngram** (~90 GB, 873 M docs) — footprint-tripler; latency breaks at scale.
- ❌ **edge-ngram** — bigger in prod (21.3 GiB) _and_ misses substrings (`264`→0.19 recall).
- ❌ **external engines** — Meilisearch/Typesense are _prefix not infix_; Quickwit misses local <50 ms; pg_trgm loses 3 ways; **Manticore** the lone gated-spike candidate.
- **Layer status:** GO for this homelab track. It is still additive for ordinary structured L2 search, but it is the chosen candidate engine for fast path free-text and `collapse:path` before any DROP plan is revisited.

---

## Space-savings scenarios (vs 276 GB)

| scenario                                         | footprint | savings   |
| ------------------------------------------------ | --------- | --------- |
| Drop + **L1 only** (migration)                   | ~19 GB    | **−93 %** |
| + cheap **L2**                                   | ~27 GB    | −90 %     |
| + optimized **L2**                               | ~35 GB    | **−87 %** |
| + **L3** free-text index (per-torrent, measured) | ~48 GB    | **−83 %** |

The L3 line _used_ to read −55 % on the per-FILE index; **PS-MB1 measured (and PSX then BUILT) the per-torrent form at 13.32 GiB**, so even with interactive free-text the saving stays ~83 %. **L3 is now a GO (user decision 2026-06-09)** — the bench artifact exists; deployment is the remaining step.

---

## The hard rule

**Don't drop `torrent_files` until each needed replacement layer is DEPLOYED _and_ PROVEN in production.** Order: **L1 ✅ → L2 ✅ → L3 (now a GO; bench-built, deploy/prove next) → DROP last, gated.** `torrent_files` stays the live fallback/source-of-truth throughout; the **DROP is deferred indefinitely**.

**Next concrete step toward the drop:** deploy and prove **L3 pathsearch** per [`pathsearch-T4-deploy-ops.md`](./pathsearch-T4-deploy-ops.md), then wire path-query/collapse candidate routing through L3 -> blob/L2 exact refine. Keep L2 compaction/pruning as operational housekeeping.

---

## Measurement-completeness audit (2026-06-09) — the benchmark phase is DONE

Every architectural decision above now rests on a built artifact or a measured number (L1 verified in prod; the DROP gate, L2 latency/size/freshness/fidelity, the L3 index, agg's retirement, and the FIND-2 wall+fix all measured — see [`psx-campaign-RESULTS.md`](./psx-campaign-RESULTS.md) for the final campaign). **No further research benchmarks are needed to proceed.** What remains, by bucket:

1. ~~Optional — concurrency/load~~ ✅ **MEASURED (CB campaign, 2026-06-10 — [`cb-campaign-RESULTS.md`](./cb-campaign-RESULTS.md)): single-client latency survives production concurrency.** L3: graceful to 24 readers (p95 ~1.9× at 24× load); the live writer is invisible to readers (≤1.05×), fresh-lag sub-ms, supersession 5.2 ms under load; **deployable keyed index = 14.0 GiB**. L2 DuckDB: cursors parallelize; the rollup hot path holds `<250 ms` to N=16; heavy `COUNT(DISTINCT)` shapes route through rollups; sidecar config = 1 instance + cursor pool, per-query `threads≈4`, run warm. The gRPC wrapper is now validated in prod by the l2-11 window.
2. **Deploy-phase validations** (arrive _with_ the deployment, not separate benches): ~~prod ext-parity confirm before flipping the JSONB gate~~ ✅ DONE (Tier-1+2, 0 mismatches; gate FLIPPED + verified, 2026-06-10) · ~~the L2 dual-read shadow vs `torrent_files`~~ ✅ DONE/ACCEPTED (GATE A passed; frozen GATE C 12/13 + documented dup-path superset; l2-11 live shadow residue limited to dup-path/freshness drift) · ~~full-corpus blob-export "0 errors across all 16.97 M"~~ ✅ DONE by real production compact/export (`decode_errors=0`) · per-torrent live-writer freshness ✅ confirmed by minute delta CronJob and l2-11 prod-window `delta_age_seconds=50-54`.
3. **Implementation, not measurement:** the FIND-2 popularity-sort default (product call + small Go change) · FB-B1a/c/d hardening with correctness tests · the L3 sidecar + GraphQL/UX per [`pathsearch-T4-deploy-ops.md`](./pathsearch-T4-deploy-ops.md) (incl. the node-hostname fix and the unimplemented `--follow` mode).
