# File-Grained Search — Benchmark Results, Findings & Conclusion

> **This is the canonical benchmark record.** Real-data results in §0; grounding-probe findings in §1; the **conclusion/verdict** in the status line + the "GATE #12 — DECIDED BY REAL DATA" section. §2–§5 are the pre-run plan/framing (historical). Companion: [`file-index-bench-RESULTS.md`](./file-index-bench-RESULTS.md) (the detailed RUN-4 Tantivy size/latency record).

**Date:** 2026-06-07
**Status:** ✅ FULL SUITE COMPLETE (2026-06-07, throwaway HEL1 restore, production untouched). RUN-1…RUN-4 all done. **🎯 VERDICT (real data): SHIP THE CHEAP COMPOSITION; REJECT/defer the 873M-doc Tantivy file index** — it gives NO latency win over DuckDB (the `<50 ms` premise was REFUTED: scan-bound ~1.3 s p50 @879.5M on the common broad filter), costs +14–25 GB vs +3.9 GB, and lacks free-text/exact-count. See §0 + the GATE #12 section. RUN-5 (synthesis) ✅ · RUN-6 (teardown) pending.
**Method:** 4-agent opus team (`duckdb-bench` / `index-bench` / `pg-data-bench` / `bench-harness`) + lead synthesis. Probes ran read-only against the near-idle production PG + local blob decode; no mutations, no production hammering.
**Detailed specs:** [`duckdb-on-blobs-benchmark-spec.md`](./duckdb-on-blobs-benchmark-spec.md) · [`file-index-size-latency-bench-spec.md`](./file-index-size-latency-bench-spec.md)
**Settles:** GATE #12 in [`file-grained-search-team-review.md`](./file-grained-search-team-review.md) — the 873M-doc index vs the cheap composition.

---

## 0. LIVE FULL-SUITE EXECUTION (2026-06-07) — measured on the HEL1 restore

All runs on a **throwaway PG restored to idle HEL1** (`bitmagnet-bench` ns, NodePort 30654); **production FSN1 never touched**. Bench infra codified in homelab-infra `playbooks/bitmagnet_bench_pg.yml` + `make bitmagnet-bench-*` targets.

🚨 **Premise correction:** the HEL1 dump (2026-06-05 23:57) is the **PRE-BACKFILL safety backup** → it has the full `torrent_files` (879.47M rows / 261 GB ground truth) but **EMPTY** `files_data` blobs + `torrent_file_summary` (generated the next day). So the benchmarks source per-file data **directly from `torrent_files`** (option **C+**: `torrent_files.extension` is the path-derived GENERATED column = already G1-correct; the blobs are just a re-encoding of it; blob-decode cost already grounded). **No blob regeneration** — C+ gives the same real numbers, faster.

### RUN-1 — restore ✅
- 35 GB pre-backfill `pg_dump -Fc` → **353 GB** restored to throwaway `bench-pg` (HEL1), ~3h18m, `fsync=off` (disposable). **`ANALYZE` run** afterward (pg_restore computes no column stats + `autovacuum=off` → without ANALYZE the planner would give garbage plans).
- Verified: **torrent_files 879,474,880 rows / 261 GB** (119 heap + 142 idx; fresh-packed — cleaner than prod's 277 GB which had index bloat); torrents 48.13M; torrent_contents 61 GB; **16,973,470** with-files torrents.

### RUN-3 — aggregate + slim-table sizing ✅ (real measured)
| object | rows | total size | |
|---|---|---|---|
| `agg_nat` (natural-key info_hash,ext + {max,min,count} + PK + (ext,mx)) | **56,046,830** | **8.4 GB** (149.8 B/row) | |
| `agg_surr` (int4 surrogate + covering(ext_id,mx)) | **54,776,108** | **5.5 GB** (+ 2.0 GB `dim_torrent` to map id↔info_hash = ~7.3 GB) | |
| `slim_norm` (int4 id, ext, size + 2 btrees — the rejected model) | 879M | **~78–89 GB** (anchored) | REJECT |
| `slim_nat` (natural-key 20B info_hash per-file) | 879M | **~99–113 GB** (anchored) | REJECT |
- **Aggregate row count ~55M ✅ validated dead-on** (3.30 pairs/torrent).
- 🚨 **SIZE CORRECTION vs the spec's "+3–5 GB":** with the full `{max,min,count}` payload the aggregate is **5.5 GB (surrogate) / 8.4 GB (natural)** — at/above the high end. **+3–5 GB only holds for a `max_size`-ONLY natural-key aggregate (~5–6 GB, no dim)** — the simplest one-sided-`>T` form (carry min+count only if `<T`/analytics needed).
- **Distinct extensions = 47,628** → exceeds int2's 32,767 → **int4 ext_id required** (validated).
- **Slim per-file PG table ~78–113 GB → +68–92 GB band, REJECT CONFIRMED** (~10–15× the aggregate, ~5× the file index; strains the 200Gi budget). Exact totals not re-measured — rejected option, anchored confirmation suffices.
- **Methodology:** `fsync=off`/`synchronous_commit=off` skew only WRITE wall-times (sizing + read-latency unaffected); `autovacuum=off` (append-only builds, fine); tables `ANALYZE`d. `dim_ext`/`dim_torrent`/`agg_nat`/`agg_surr` left in place for RUN-5.

### RUN-2 — DuckDB query latency ✅ (real, full 879,474,852-row corpus, HEL1 24 cores, warm p50 / cold)
- **Export:** Parquet directly from `torrent_files` via DuckDB postgres-scanner (one PG scan): **slim 3.86 GB (18 s) / full+path 11.71 GB (83 s)**, rows match PG exactly, null-ext 6.8%. Slim **3.86 GB** confirms the cheap-tier disk cost (even *below* the +3–5 GB / smoke ~5.8 GB estimate). Decode not re-measured (grounded 0.6–0.94 µs/file).

| Q | query | warm p50 | cold | |
|---|---|---|---|---|
| Q1b | `mkv > 1GB` **paginated** LIMIT 1000 | **35 ms** | 199 ms | ✅✅ |
| Q2 | `GROUP BY extension` (all 879M) | 1.29 s | 1.20 s | ✅ |
| Q3 | size histogram + percentiles | 1.15 s | 1.18 s | ✅ |
| Q4 | `COUNT DISTINCT info_hash` collapse | 1.27 s | 1.28 s | ✅ |
| Q5 | two-sided range distinct-torrent | 1.23 s | 1.22 s | ✅ |
| Q6 | path-FTS `ILIKE '%S01E%'` LIMIT 100 | **142 ms** | 178 ms | ✅✅ |
| Q7 | single-torrent hydrate (point) | **17 ms** | 27 ms | ✅✅ |

- 🎯 **PREDICTION VALIDATED: every realistic query is 0.015–1.3 s** — squarely inside (mostly below) the predicted 0.2–2 s, on the FULL corpus. Cold ≈ warm (the 15.5 GB Parquet stays resident in HEL1's 125 GB RAM after a ~1.5 s first-read).
- ⚠️ ONE caveat: the *unpaginated* "return all 5,699,629 matching rows" form of Q1 is **14.2 s** — but that's Python `fetchall()` **materialization**, NOT scan/filter (Q4 is the identical filter + count at 1.27 s). No interactive path ships 5.7M rows; it paginates (35 ms) or counts (1.27 s). So it's a batch-extract cost, not a latency wall.
- **Core-scaling (warm p50, 24 cores → 1 core):** heavy full-corpus GROUP-BY/DISTINCT/histogram scans **1.2 s → 10–13 s** (8–10× parallel speedup, scan-bound); light early-out queries (paginated find 34→15 ms, path-FTS 145→41 ms, point hydrate 17→33 ms) stay **sub-50 ms regardless of core count**. ⟹ the cheap tier's *interactive* per-file queries are core-independent; only its *analytical* aggregates need many cores. (CSV: HEL1 `bench-scratch/run2.csv`, 7 queries × {all,1} threads × cold/p50/p95/p99.)
- **GATE read:** the cheap analytics tier is **fast enough** — DuckDB-on-`torrent_files`/Parquet at **+3.86 GB** delivers exact per-file analytics, distinct-torrent collapse, two-sided ranges, histograms, AND paginated per-file find, all **sub-1.3 s**. The 873M-doc index buys ONLY **<50 ms per-keystroke + free-text path FTS + realtime freshness** — not correctness, not "seconds" interactivity.

### RUN-4 — Tantivy file-index size + latency ✅ THE DECISIVE RESULT (real 50M index → 879.5M)
Full detail: [`file-index-bench-RESULTS.md`](./file-index-bench-RESULTS.md). Numbers flat across 1M/10M/50M → extrapolation trustworthy.
- **SIZE:** V10 FAST-only ≈ **25.4 GB**; **optimized (FAST `info_hash`+`file_index` identity + no fieldnorms) ≈ 13.7 GB**; V9 INDEXED ≈ 31 GB; v1.1 +path ≈ **45 GB**. 🚨 `doc_id` STORED is the dominant cost (**12 GB**; §6 guessed 1–2) → **use a FAST identity, not a stored doc_id** (saves ~10 GB). INDEXED tax 5.8 GB → drop it (zero latency benefit). All variants ≪ 74 GB ceiling → **size never says NO-GO.**
- **BACKFILL (H1):** ~450–570k docs/s → 879.5M ≈ **~32 min**. 🚨 use DEFAULT incremental merge (bounded RAM); do NOT force-compact the full corpus to 1 segment (the bench's 4.8 GB peak RSS was a force-merge artifact).
- **🎯 LATENCY — the claim is REFUTED:** Scenario A (`ext∧size` top-20 + exact count) = **p50 72.9 ms / p95 208 ms at 50M** — already >50 ms, and **scan-bound** (~950k matches, no early-termination). Extrapolated to 879.5M (~17.6×) → **~1.3 s p50 / ~3.7 s p95**. **The "<50 ms" premise does NOT hold for the common broad filter.** V9 ≈ V10 (INDEXED = zero latency benefit). Collapse (B) p95 3.4 s. v1 has **no free-text field** (path FTS = v1.1, +45 GB, CJK-broken).
- **➡️ The Tantivy file index gives NO latency win over DuckDB** — DuckDB does the same per-file queries in 35 ms–1.3 s at +3.86 GB *with* exact counts/joins — yet costs **+14–25 GB** and is *slower* on broad filters at scale.

---

## GATE #12 — DECIDED BY REAL DATA: ship the cheap composition; REJECT the file index for v1

The whole point of this suite was to replace estimates with measurements. The measurements **overturned the spec's central assumption** (that a per-file Tantivy index delivers <50 ms interactive search):

| | DuckDB-on-(torrent_files→Parquet) | 873M-doc Tantivy file index (v1) |
|---|---|---|
| Per-file `ext∧size` (paginated) | **35 ms** | 73 ms @50M → **~1.3 s @879.5M** (scan-bound) |
| Exact distinct-torrent count/collapse | 1.27 s | ~1.3–3.7 s |
| path-FTS | 142 ms (`ILIKE`) | ❌ not in v1 (v1.1 = +45 GB, **CJK-broken**) |
| point hydrate | 17 ms | n/a |
| extra disk | **+3.86 GB** (or +0 GB on-demand) | **+14–25 GB** |
| freshness | periodic export | realtime |
| exact counts / arbitrary SQL joins | ✅ | ❌ |

**The index buys no latency win, costs ~4–6× the disk, lacks free-text + exact-count/join, and is *slower* on the common broad filter at full scale.** Its only unique properties are realtime freshness and (in v1.1, at +45 GB, CJK-broken) per-keystroke path-FTS — neither required by the planned surface.

**RECOMMENDATION (data-driven):**
1. **Ship the cheap composition** — the 0-GB code fixes (**G1** + **G2** + timestamp/index-sort hydration) + the **per-(torrent,ext) aggregate** (~5–6 GB max-only, exact one-sided distinct-torrent counts/paging) + **DuckDB-on-blobs/torrent_files** (+3.86 GB persisted, or +0 GB on-demand) for analytics + exact joins. This restores near-complete functional parity at **+3.9–10 GB**, sub-1.3 s, exact.
2. **REJECT / defer the 873M-doc Tantivy file index.** Gate it strictly on a *later, explicit* product requirement for **per-keystroke free-text path search at <50 ms with realtime freshness** — and even then, re-scope (FAST identity not stored doc_id; CJK tokenizer for path-FTS; incremental merge; accept that broad structured filters are scan-bound regardless).
3. The cheap-composition prerequisites (**G1** especially — it's a deploy-time bug for the Phase-3 torrent index too) proceed as the **Phase A** tasks already tracked in the review.

---

## 1. What we already MEASURED (grounding probes — real numbers, no heavy runs)

| Hypothesis | Spec/estimate | **Measured (real data)** | Verdict |
|---|---|---|---|
| Total file count | "873M" | **856.79M** rows (torrent_files, catalog); 16,992,238 torrents-with-blob (exact) | ✅ close (was a safe over-estimate) |
| Files/torrent | 51.8 (or "18") | **avg 51.79** over with-files torrents (p50 6, p90 54, p99 743, **max 88,561**, heavy skew); **17.9** over ALL torrents (64.4% have no files) | ✅ both right — different denominators; index = ~857–871M docs (~860M) |
| Blob corpus size | ~16 GB | **14.5 GB** (avg 856 B/blob, zstd 4.96×) | ✅ |
| Blob decode throughput | unquantified (H1) | **0.6–0.94 µs/file, ~1.6M files/s single-thread** (real `DeserializeFiles` bench) → full corpus **~8–9 min single / ~1–2 min @ 8–16 threads** | ✅ now known |
| Per-(torrent,ext) aggregate | +3–5 GB, ~52M rows | **~55M rows** (real GROUP BY: 3.245 pairs/torrent); **+3–3.5 GB surrogate-keyed / ~6.5 GB natural+min+count** | ✅ holds (surrogate; trends high) |
| Slim PG per-file table | +68–92 GB (reject) | **~68 GB floor / ~90 GB ceiling** (anchored on live per-row btree tax: ~70 GB pure structural overhead) | ✅ REJECT confirmed |
| G1 blast radius | "every crawl-path torrent" | blob `e` empty for **~4–7% of files today** (backfilled ones are correct) — but **grows with every new crawl** | path-derivation still mandatory |
| Single-file / over-threshold | — | **8.06% single-file**; **6.04% over-threshold** (importer path saved them anyway) | informs D5 + the over_threshold gap |

**The keystone (DuckDB-on-blobs latency) — measured/modeled, not guessed:**
- DuckDB cannot decode the blob (zstd+msgpack) → it's **decode → Parquet → DuckDB**.
- **Parquet path: predicted 0.2–2 s** for slim queries (ext+size, GROUP BY, COUNT DISTINCT, percentiles) — **beats** "1–10 s" — at a **+3–5 GB** Parquet cost.
- **On-demand decode per query: ~1–4 min** (decode-bound, from the measured 0.6–0.94 µs/file × 856.8M) — **refutes** "1–10 s".
- ⟹ **The design matrix's "+0 GB AND 1–10 s" (row 4a) does not exist.** The honest cheap analytics path is **+3–5 GB Parquet at 0.2–2 s** (periodic export), or +0 GB at *minutes*.

**File-index size — recomputed with the omitted costs:**
- **v1 FAST-only ≈ 14–18 GB** (recommended); **v1 spec-as-written (INDEXED) ≈ 19–24 GB**. Both **above** the spec's 8–12 GB.
- Biggest under-counted item: `doc_id` STORED-only (3–8 GB). Plus the `size` INDEXED term-dict (+3–5 GB) and `published_at` FAST (~3.3 GB).
- **Size never says NO-GO** (fits the 200 Gi PVC every way). **Latency is the real gate** — still to be measured.

---

## 2. The GATE, now on real numbers

| Option | Latency | Freshness | Extra disk | Capability |
|---|---|---|---|---|
| **DuckDB-on-Parquet** | 0.2–2 s (predicted) | periodic export | **+3–5 GB** | exact per-file + arbitrary SQL/analytics |
| **Per-(torrent,ext) aggregate** (PG) | <50 ms | live (built from blob) | **+3–5 GB** | exact distinct-torrent collapse (one-sided) |
| **G1+G2+hydration** (code) | <50 ms | live | **0 GB** | correct ext filter/sort; browser survives the drop |
| **873M-doc Tantivy file index** | **<50 ms?** (unmeasured) | realtime | **+14–18 GB** | the above at keystroke speed + per-file **path FTS** |

**Cheap composition (G1+G2 + aggregate + Parquet) ≈ +6–10 GB**, all live/periodic, exact, 0.2–2 s analytics + <50 ms collapse. **The file index adds ~+8 GB more for: <50 ms free-text per-file + path FTS + realtime freshness.** Disk is no longer the differentiator (both fit the PVC) — **latency, freshness, and path FTS are.**

**The one unmeasured number that decides it: does the file index actually deliver <50 ms** free-text `ext∧size` (and is collapse fast or does it need the aggregate anyway)? → RUN-4.

---

## 3. Data source (settled) + safety

A **restorable 35 GB pre-cutover `pg_dump`** already sits on HEL1 (`/var/lib/bitmagnet-backups/bitmagnet-pg-20260605-235749.dump`, `-Fc`). It contains **both** `torrent_files` (ground truth) and the blobs. Restore it to a **throwaway k3s PG on idle HEL1** (1.6 TB free, 119 GB RAM) → every heavy benchmark runs at **full fidelity with zero production impact** (FSN1 is never touched; the dump is already local to HEL1). All grounding probes so far were read-only catalog/small-sample queries on the near-idle PG.

---

## 4. Gated run plan (tracked tasks RUN-1 … RUN-5)

> **GATE 0 — needs your explicit go-ahead before any restore/build/run.**

1. **RUN-1** restore the dump → throwaway HEL1 PG (~1–3 h, one-time).
2. **RUN-2** DuckDB: Rust `blob_export` → Parquet (measure decode+export *and* query latency, cold/warm p50/p95/p99) — validate 0.2–2 s.
3. **RUN-3** aggregate + slim-table real `CREATE TABLE AS` sizing (confirm +3–5 GB / +68–92 GB).
4. **GATE 1** — is the cheap composition sufficient? (If DuckDB ≤2 s + aggregate confirmed and no hard <50 ms/path-FTS requirement → likely stop here.)
5. **RUN-4** (only if the index is still in question) standalone `bitmagnet-search-bench` crate → real index bytes/doc (variant matrix, INDEXED tax) + **SearchFiles latency** + backfill time.
6. **RUN-5** feed measured numbers into GATE #12 + update the review doc; recommend.

**Effort:** restore ~1–3 h · Phase A ~½ day compute on idle HEL1 · the index smoke ~hours. All HEL1-local.

---

## 5. Early read (before the runs)

The grounding already shifts the recommendation: the cheap composition is **+6–10 GB** (not the spec's implied +3–5 GB, because the analytics path needs a +3–5 GB Parquet — "free DuckDB" is a mirage), and the index is **+14–18 GB** (bigger than the spec's 8–12). The decision narrows to a **~+8 GB, 873M-doc, write-amplifying index bought purely for <50 ms free-text + path FTS + realtime** — justified only if those are real product requirements (GATE #12 question 1). RUN-2 + RUN-4 turn the two remaining "predicted" latencies into measured ones and close the decision.
