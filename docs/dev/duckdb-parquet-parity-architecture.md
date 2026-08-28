# DuckDB-on-Parquet — Complete Per-File Parity Architecture

**Date:** 2026-06-07
**Status:** Design — synthesized from a 4-agent opus team (ARCH-A pipeline / ARCH-B integration / ARCH-C empirical+optimization / ARCH-D topology / ARCH-F future-queries) + benchmark evidence. No code changed.
**Decides:** how to extend the benchmark-proven DuckDB-on-Parquet into a production architecture with **complete functional parity** with the dropped `torrent_files` table — informed by **real measurements on the full 879.5M-row corpus**.
**Supersedes:** the Phase-3 Tantivy file-index plan (rejected by benchmark — see [`file-grained-search-benchmark-results.md`](./file-grained-search-benchmark-results.md)).
**Thread detail:** [`duckdb-parquet-pipeline-arch-A.md`](./duckdb-parquet-pipeline-arch-A.md) · [`duckdb-integration-arch.md`](./duckdb-integration-arch.md) · [`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md) · [`bitmagnet-duckdb-parquet-arch.md`](./bitmagnet-duckdb-parquet-arch.md) · [`duckdb-future-query-catalog-arch-F.md`](./duckdb-future-query-catalog-arch-F.md)

---

## 0. TL;DR

Replace the dropped 273 GB `torrent_files` table with a **3-tier composition**, each tier as fresh as its workload needs and **none requiring a per-file search index**:

| Tier                                | Covers                                                                                                | Store                                                                       | Freshness                                                                     | Latency                               |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ------------------------------------- |
| **0 — served, in-app**              | per-torrent **browse**, ext filter/sort, distinct-torrent **collapse** + **facets**, one-sided counts | the **blob** (`files_data`) + a **PG `agg_torrent_ext`** rollup             | **real-time** (synchronous dual-write)                                        | <50 ms                                |
| **1 — interactive per-file search** | per-file `ext∧size`, two-sided ranges, path search, exact per-file counts                             | **DuckDB** over a **sorted slim Parquet + native rollup tables** (~12.3 GB) | eventually-consistent, ≤ refresh cadence (~15 min–hours, rebuild is ~3–5 min) | **<150 ms** (most <35 ms)             |
| **2 — analytics / arbitrary SQL**   | histograms, percentiles, GROUP BY, cross-store JOINs, dedup, quality heuristics, **future queries**   | same DuckDB, on-demand                                                      | ≤ refresh cadence                                                             | 0.03–few s (heavy batch up to ~2 min) |

**Tier 0 alone clears the `torrent_files` DROP** (no DuckDB needed). Tier 1/2 are a **separable, deferrable** DuckDB sidecar. Total marginal disk ≈ **+5.9–12.3 GB**; the deploy footprint is **~10× smaller** than the rejected Tantivy sidecar.

---

## 1. Why this, not the index (benchmark verdict)

Measured on the real corpus ([results doc](./file-grained-search-benchmark-results.md)):

- The 873M-doc Tantivy file index is **scan-bound ~1.3 s p50** on the common `ext∧size` filter — **no `<50 ms` win** — and costs **+14–25 GB**.
- **Optimized DuckDB-on-Parquet beats it on its own turf:** sort-by-(ext,size) → 7–60× (collapse 1311→132 ms, count 1024→**17 ms**); native **rollup tables → <50 ms** (GROUP BY **2.3 ms**); point lookup 0.2 ms.
- A true filter-accelerating secondary index is **not a DuckDB capability** for this scan workload (ART `CREATE INDEX` → `EXPLAIN seq_scan`, confirmed in `src/optimizer`); the gains come from **zone-map pruning + pre-aggregation**, not an index.
- **Future-proofing:** 7 of 8 future-query classes are _just new SQL_ (zero re-index, zero migration) vs a new field + 32-min re-backfill _per query type_ for the index.

The **only** thing the index uniquely offered — per-keystroke CJK-aware free-text **path** search — is the lone carve-out (DuckDB's FTS/BM25 reaches 150 ms but at +34.9 GB + CJK-token-only, i.e. the same index+tokenizer cost). Gate that on an explicit product requirement.

---

## 2. Data model (measured layout)

Produced by one refresh pass from the **blobs** (`files_data`; G1 = extension path-derived):

1. **`files` fact Parquet** — `(info_hash, file_index, path, extension, size)`, **`ORDER BY extension, size`**, `ROW_GROUP_SIZE 1_000_000`, ZSTD, **bloom OFF** (redundant once sorted). Sorting enables row-group min/max pruning (`parquet_reader.cpp:1308`). **≈10.3 GB sorted** (vs 3.86 GB unsorted — the +6.4 GB buys the 7–60× on ranges).
2. **`torrents` dim Parquet** — the **immutable** joinable torrent columns (`info_hash, info_hash_v2, meta_version, created_at, published_at, content_type, content_id/source, video_*, languages[], genres[]`). Enables time-trends + content/video JOINs as pure SQL. **Mutable attrs (seeders/leechers) are NOT frozen here — joined live from PG.**
3. **Native rollup tables** (in a small `.duckdb`) — `per_ext` (~1 MB) + `per_torrent_ext {max,min,count}` (~1.3 GB): collapse/facets/one-sided-counts at **<50 ms**. ≈ **+2 GB**.
4. **PG `agg_torrent_ext`** — the _same_ per-(torrent,ext) rollup kept **live in Postgres** (Tier 0) for the served collapse/facet surface.

**Total ≈ 12.3 GB** (recommended) or **5.9 GB** lean (unsorted fact + rollups: keeps GROUP BY/collapse/counts <35 ms, accepts ~1.2 s for two-sided/rare-find). All ≪ the 200 Gi PVC that the rejected index would have used.

---

## 3. Pipeline & freshness (3 tiers — see §"freshness" detail)

- **Source = the blobs** (future-proof past the `torrent_files` DROP), decoded via the existing `blob_export` logic (~0.6–0.94 µs/file → full corpus ~1–2 min @ 16 threads).
- **Refresh = scheduled FULL REBUILD + atomic swap** (Parquet is immutable; the (ext,size) sort needs the whole set). **~3–5 min** end-to-end (decode + write/sort + emit rollups). Versioned `v<ts>/` dir + `current` symlink swap; the sidecar re-opens on swap. Cost scales with _corpus_, not crawler rate.
- **Incremental base+delta (EXP-B — VALIDATED on real data):** a large sorted **base** (rebuilt on compaction) + a tiny frequent **delta** of new/changed torrents → **~minute freshness at <250 ms query cost, no full rebuild.** Measured base+delta collapse latency: base-only 141 ms → +1k 179 ms → +10k 193 ms → **+100k torrents (~hours of crawl) 230 ms** (gentle, ~linear). Delta-append is **sub-second in production** (the processor already holds the new torrents + decoded blobs). Compaction trigger ≈ 1M delta torrents → an 83 s full rebuild + atomic swap. The write seam is the post-commit point in `dhtcrawler/persist.go runPersistTorrents`.
  - 🚨 **Supersession is TORRENT-granular, via an ANTI-JOIN — not a per-row `row_number()`.** `files_data` is upsert-with-`DoUpdates` (`persist.go:113-123`) so a re-crawl can supersede a torrent's _whole fileset_. **Correct pattern (EXP-B-proven):** exclude from the base every `info_hash` present in the delta, then `UNION ALL` the delta — i.e. `base ANTI JOIN delta ON info_hash` ∪ `delta`. A naïve `row_number() OVER (PARTITION BY info_hash)=1` is **WRONG** (it keeps one _file_ per torrent → drops a multi-file torrent's other files), and a window-max over the whole set is **80× slower (19 s vs 230 ms)**. The anti-join lets the base predicate prune via zonemaps and hash-anti-joins the tiny delta. Supersession correctness confirmed (a re-crawled torrent's old fileset is replaced, not double-counted).
- **Freshness contract:**
  - **Browse / file-hydrate (blob):** **real-time** — synchronous in the crawl persist tx (already exists).
  - **Collapse / facets / one-sided counts (PG `agg_torrent_ext`):** **real-time** — importer dual-writes the rollup + periodic reconcile.
  - **Cross-file search + analytics (Parquet):** **eventually consistent**, lag ≤ the refresh cadence. Rebuild is cheap (~3–5 min) → cadence is a knob; **default ≤6 h, recommend hourly** (or tighter) on idle HEL1.
- **Refresh Job:** a k8s **CronJob** (language matches the sidecar — Rust), `WHERE files_data IS NOT NULL` (+ a complementary partial index), row-count sanity gate before swap, `ParquetRefreshStale` textfile metric.

---

## 4. Integration (DuckDB ↔ bitmagnet)

- **Runtime = DuckDB SIDECAR (Rust, `duckdb-rs`)**, _not_ embedded go-duckdb. Rationale (source-grounded): `ci.Dockerfile` is **pure-Go / CGO-disabled / musl / cross-compiled** — go-duckdb needs CGO + a 50–100 MB libduckdb → breaks the build for a default-off feature. `memory_limit`/`threads` are **global per DB instance** (`config.cpp`, `settings.hpp`) → heavy scans must be **isolated in a dedicated process** (the sidecar). **Reuse the already-scaffolded `bitmagnet-search` sidecar — swap the engine Tantivy→DuckDB**, keep the proto/client/role/PVC plumbing.
- **GraphQL wiring:** `torrentContent.fileSearch` → a direct-serve `filesearch.Service` → DuckDB (Tier 1); `TorrentQuery.files` (per-torrent browse) → the **blob** (G2), not DuckDB. Safe **prepared-statement** SQL only (`duckdb_prepare`/`bind_*`); ORDER BY allowlist; always `LIMIT`; opt-in `totalCount`. Hydrate hits (info_hash + file_index) from PG/blob — match by `Index`, not position; backfill empty per-file timestamps from the parent.
- **Resource safety:** `SET memory_limit/threads`, READ_ONLY, sized temp dir, a concurrency semaphore for heavy queries.

---

## 5. Deploy topology (replaces the Tantivy sidecar)

- **Tier 0** runs entirely in the **existing bitmagnet app + PG on FSN1** — the only thing required to drop `torrent_files`. **No new service.**
- **Tier 1/2 DuckDB** runs **on idle HEL1, never FSN1** (FSN1 is 83% mem-committed; embedding risks OOM-killing the crawler). Ship **on-demand first (+0 GB always-on)**; promote to the persistent sidecar when productized.
- **Phase-3 replacement delta:** **delete** ~1009 LOC Ansible (`roles/bitmagnet-search` + 3 playbooks + group_vars) + 8 Make targets + the Rust Tantivy crate + the **200 Gi PVC** + the Deployment/Service/2×CNP + the single-writer backfill Job + the GHCR search image. **Add** ~2–3 Go fixes + 1 PG migration + a ~100-LOC refresh CronJob. **~10× smaller surface**, and a Parquet re-export is ~83 s vs a 32-min Tantivy rebuild.

---

## 6. Prerequisites (the 0-GB code fixes — Tier 0, ship first)

These clear the `torrent_files` DROP and are needed regardless:

- **G1** — derive file extension from path (`FileExtensionFromPath`) everywhere, never the empty crawl-path blob `e`; add `extension` to the consistency checker. (Also fixes the deployed torrent-index `file_extensions` facet.)
- **G2** — re-point `TorrentQuery.files` at the blob (`AfterFind`-decoded `t.Files`, in-memory orderBy/paginate/totalCount).
- **Hydration** — per-file `created_at/updated_at` from the parent `torrent.created_at`; `ORDER BY index` re-sort.
- **`agg_torrent_ext`** — the PG per-(torrent,ext) rollup + importer dual-write (Tier 0 collapse/facets).

---

## 7. Future queries (forward-compatible by construction)

Bake the **dim Parquet** + the `path` column in now → **7 of 8 future-query classes are just new SQL** (season-packs, time-trends, content/video JOINs, dedup/find-by-filename, fuzzy/regex path, quality heuristics, faceting). Only **BEP-52 per-file merkle** needs new per-file _data_ (a blob-format bump + one re-export). Heavy cross-torrent dedup (~800M groups, ~134 s) is a documented **batch** job, not interactive.

---

## 8. Risks (from ARCH-D, ranked)

R1 (HIGH) embed go-duckdb breaks the pure-Go/musl build → **sidecar, not embedded**. R2 (HIGH) DuckDB on FSN1 risks OOM → **HEL1 only**. R3 (MED) configure `memory_limit/threads`/READ_ONLY explicitly (DuckDB defaults to 80% RAM/all cores). R4 (HIGH) the **DROP is gated by Tier 0, not DuckDB** — never block cutover on the analytics tier. R5 (MED) over_threshold files (6.04%) = pre-existing importer cap, document in UX. R6 (MED) Parquet schema versioned `v<schema>/`; re-export ~83 s. R7/R8 (MED) refresh atomicity + freshness lag (search only). R9 (LOW) concurrent read-only Parquet safe; cap heavy queries with a semaphore.

---

## 9. Implementation task breakdown

**Phase A — clear the DROP (Tier 0, 0–6 GB, no DuckDB):**
A1 G1 (ext-from-path + checker) · A2 G2 (blob file browser) · A3 timestamp/index-sort hydration · A4 `agg_torrent_ext` PG rollup + importer dual-write + reconcile · A5 guard the retired PG file-search path (C6).

**Phase B — DuckDB Tier 1/2 (the per-file search + analytics):**
B1 productionize `blob_export` → the refresh tool (Rust; sorted fact Parquet + dim Parquet + native rollup tables; G1) · B2 the refresh **CronJob** (full rebuild + atomic swap + sanity gate + stale metric; default hourly) · B3 the **DuckDB sidecar** (swap `bitmagnet-search` engine Tantivy→DuckDB via `duckdb-rs`; memory_limit/threads/READ_ONLY; reopen-on-swap) · B4 GraphQL `fileSearch` resolver + safe prepared-SQL builder + hydration · B5 homelab deploy (delete the Tantivy role/PVC/Job; small Parquet volume on HEL1; sidecar deploy) · B6 monitoring (refresh-stale, query latency).

**Phase C — forward / optional:**
C1 path-FTS via DuckDB FTS/BM25 — only on an explicit product need (+34.9 GB, CJK-token-only) · C2 BEP-52 per-file merkle (blob-format bump + re-export) · C3 incremental base+delta refresh (if full-rebuild outgrows the window) · C4 the analytics surface (histograms/dedup/quality) as a read-only endpoint.

**Gate:** Phase A is the cutover prerequisite; Phase B is the per-file search/analytics (deferrable); Phase C is forward.
