# Disk Footprint: the new file-search stack vs the `torrent_files` table

**Date:** 2026-06-08
**Status:** Synthesis of measured footprints across the whole effort (migration + DuckDB-on-Parquet tier + the EXP-D/E/D2 inverted-index experiments). Every number below is **measured** on the real corpus (HEL1 throwaway env / deployed FSN1), not estimated.
**Question answered:** "What are the space savings vs the `torrent_files` table?"

---

## Baseline — what we're replacing

| | size | notes |
|---|---|---|
| **`torrent_files` table** | **~276 GB** | heap **119 GB** + indexes **157 GB**, ~857M rows. Dropping it frees the full 276 GB to the OS immediately and takes the whole PG DB **397 GB → ~121 GB**. |

This is the table the Hybrid Blob migration retires. Everything below is the footprint of what stands in for it.

---

## Layer 1 — the DROP gate (already deployed): per-file data as a blob

The per-file data that `torrent_files` held is re-encoded into a per-torrent **`files_data` blob** (zstd + msgpack) plus a small **`torrent_file_summary`**:

| | size | source |
|---|---|---|
| `files_data` blob column (whole corpus) | **~16 GB** | blob corpus measured **14.5 GB** (zstd **4.96×**), RUN-2 |
| `torrent_file_summary` | **~3.3 GB** | backfill (16,976,700 summaries) |
| **Layer-1 total (in PG)** | **~19 GB** | replaces 276 GB of table |

➡️ **The migration alone: 276 GB → ~19 GB = −257 GB (−93%).** The per-file *data* shrinks ~14×, almost entirely from zstd compression. This is the irreducible cost — it's what makes per-file information survive the table drop.

---

## Layer 2 — per-file SEARCH parity (DuckDB-on-Parquet + PG aggregate)

Dropping the table also drops its search capability (ext∧size filters, distinct-torrent collapse, ranges, path substring, analytics). That parity is restored **outside PostgreSQL** on the sidecar:

| artifact | size | source | buys |
|---|---|---|---|
| DuckDB **slim Parquet** (info_hash, file_index, path, ext, size) | **+3.86 GB** | RUN-2 | every realistic per-file query 0.015–1.3 s |
| DuckDB **optimized prod layout** — sorted(ext,size) fact + 2 native rollup tables | **~12.3 GB** | ARCH-C | structured queries <150 ms (most <35 ms) |
| (cheaper alt: unsorted fact + rollups) | ~5.9 GB | ARCH-C | rollup-backed queries <50 ms |
| PG **`agg_torrent_ext`** rollup (~55M rows) | **+3–5 GB** | RUN-3 | exact one-sided distinct-torrent counts + keyset deep paging, in-PG |

➡️ **Per-file search parity adds only ~4–16 GB** depending on how much latency optimization you bake in. The Parquet lives on sidecar disk, not in PG.

---

## Layer 3 — broad free-text search (OPTIONAL inverted index)

The one capability the cheap tiers can't make *interactive* is **broad free-text / substring path search** (it's a ~23 s `ILIKE` full scan on DuckDB, an O(match-set) wall in PG). Making it interactive — and CJK-correct — requires an inverted index (EXP-D/D2):

| index option | size | source | notes |
|---|---|---|---|
| **per-torrent path-bag ngram, CJK-correct** ✅ recommended | **14.0 GiB DEPLOYABLE** (keyed, CB E2b; 13.32 GiB keyless — the delete-key is mandatory in prod and adds no read cost) | **PS-MB1 + PSX + CB** | `WithFreqs`; recall 1.0; `ascii3` p50 24.7 ms; broad-gram **production** p95 ~77–94 ms (`TopDocs`); concurrency-validated (24 readers + live writer, fresh-lag sub-ms) |
| ~~per-file ngram CJK-correct path index~~ (superseded) | ~~~90 GB~~ (94 GB @879.5M) | EXP-D2 | footprint-tripler; per-file docs → latency breaks at scale; PS-MB1 replaced it |
| (default-tokenizer path index, ASCII-only) | ~19–30 GB | EXP-D | **CJK recall 0.0037 — broken**, not viable for this 15%-CJK corpus |
| (DuckDB-FTS / BM25, ASCII-only) | +35 GB | ARCH-C | also CJK-token-only |

➡️ **PS-MB1 (2026-06-09) reframed this line item.** The per-FILE ngram was ~90 GB (the historical "swing factor"). The **per-torrent path-bag** form — one doc per torrent (~17 M docs vs 873 M), each file path a separate field value, `WithFreqs` (positions are 83.5 % dead weight for ngram) — measures **13.54 GiB**, ~6.7× smaller, still CJK-correct and median-interactive. So the free-text index is **no longer a footprint-tripler**; it's a modest add-on, gated on demand not cost.

---

## Net footprint — three scenarios

| scenario | in PG | on sidecar | **total replacement** | **vs 276 GB** |
|---|---|---|---|---|
| **A. Migration only** (blob, no search restored) | ~19 GB | — | **~19 GB** | **−93%** |
| **B. + cheap search parity** (slim Parquet + agg) | ~19 + 4 GB | ~3.9 GB | **~27 GB** | **−90%** |
| **C. + optimized search tier** (sorted+rollups + agg) | ~19 + 4 GB | ~12 GB | **~35 GB** | **−87%** |
| **D. + free-text index** (**per-torrent**, PS-MB1) | ~19 + 4 GB | ~12 + **13.5 GiB** | **~48 GB** | **−83%** |
| ~~D′. + free-text index (per-FILE, superseded)~~ | ~~~19 + 4~~ | ~~~12 + 90~~ | ~~~125 GB~~ | ~~−55%~~ |

*(PG-only view is even cleaner: the search tiers live on the sidecar, so PostgreSQL itself goes 397 GB → ~121 GB regardless, and per-file search leaves PG entirely.)*

---

## The headline

- **The migration is a ~93% space win** (276 GB → ~19 GB).
- **Keeping complete per-file search parity barely dents it — still ~87%** (scenario C, ~35 GB total).
- **The free-text index is no longer the swing factor** (PS-MB1): the per-FILE form *was* (−55%, ~3× the rest of the stack), but the **per-torrent path-bag** form measures **13.54 GiB**, so even *with* interactive CJK-correct free-text the saving is **~83%** (scenario D, ~48 GB). The index now costs about the same as the optimized DuckDB tier, not 3× the whole stack.

⟹ **Drop `torrent_files`, keep the cheap composition → ~245 GB saved (−87 to −90%). Add the per-torrent free-text index → still ~228 GB saved (−83%).** The inverted index is still **gated on a hard, measured product demand** — but now because no demand has been demonstrated (it's purely additive and never gates the DROP), *not* because it's expensive. The cheap tiers already give near-complete parity; the index buys only realtime per-keystroke substring/CJK free-text.

### Also measured & REJECTED (would have erased the savings)
- **Slim per-file PG table** (a thinned `torrent_files`): **+78–113 GB** — RUN-3, rejected.
- **Per-file structured Tantivy index** (the original Phase-5.5 plan): +14–25 GB and **no latency win** (scan-bound ~1.3 s p50) — RUN-4, rejected in favor of DuckDB-on-Parquet.

---

## Sources (all measured)
- `torrent_files` 276 GB / DB 397→121 GB, blob 14.5 GB / summary 3.3 GB: backfill + RUN-2 ([`file-grained-search-benchmark-results.md`](./file-grained-search-benchmark-results.md), MEMORY).
- DuckDB-Parquet slim 3.86 GB / optimized 12.3 GB / rollup costs: RUN-2 + ARCH-C ([`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md), [`duckdb-parquet-parity-architecture.md`](./duckdb-parquet-parity-architecture.md)).
- PG aggregate +3–5 GB / slim-table +78–113 GB: RUN-3.
- ngram CJK free-text index: per-FILE 94 GB / default broken / DuckDB-FTS +35 GB: EXP-D/D2 ([`cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`](./cjk-tokenizer-and-incremental-merge-bench-RESULTS.md)); **per-torrent path-bag 13.54 GiB measured: PS-MB1** ([`pathsearch-microbench-RESULTS.md`](./pathsearch-microbench-RESULTS.md), investigation [`pathsearch-master-investigation.md`](./pathsearch-master-investigation.md)).
- Per-file Tantivy structured index +14–25 GB rejected: RUN-4 ([`file-index-bench-RESULTS.md`](./file-index-bench-RESULTS.md)).
