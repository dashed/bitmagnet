# File-Grained Search — Team Review & Task Breakdown

**Date:** 2026-06-07
**Status:** Review of the committed `feat/file-grained-search` design docs. **No code changed.** Analysis + task plan only.
**Method:** 4-agent opus team (`doc-reader` / `code-grounder` / `plan-integrator` / `design-critic`) + lead synthesis. All claims source-verified against the fork (branch `feat/file-grained-search`), Tantivy 0.26.1, and the homelab deploy drafts.
**Reviews:** [`file-grained-search-spec.md`](./file-grained-search-spec.md) + siblings [`perfile-search-with-blob-design.md`](./perfile-search-with-blob-design.md), [`perfile-search-innovative-design.md`](./perfile-search-innovative-design.md), [`perfile-search-complete-parity.md`](./perfile-search-complete-parity.md).

---

## 1. Verdict

The design is **sound, unusually well-adjudicated, and code-accurate** — every load-bearing claim verified (`FileExtensionFromPath` byte-identical to the dropped generated column; empty-blob-`e` bug; `TorrentQuery.files` raw SQL; the Tantivy no-nested-docs / single-writer / delete-term / fast-range citations). G1 and G2 are correctly diagnosed and genuinely **0-GB code fixes**.

**But the 873M-doc file index is the expensive, unvalidated, product-unconfirmed part of the plan**, and the design docs' own §13.4 curve shows most of the parity is restorable far more cheaply. The recommended path is **cheap-composition-first, index-gated-on-a-product-decision** (§4).

---

## 2. Reconciliations (cross-doc / cross-thread)

- **Doc-revision lag, not contradictions.** The sibling docs critique the spec as if G1/G2/aggregate-promotion/checker-extension weren't yet incorporated; the **current core spec already folded them in** (§5.1 derives extension from path; §11.2-3 list G1/G2 as prerequisite steps; §11.7 promotes the aggregate; §13.2 softened the "hard floor"). The siblings are the *rationale* that drove the spec's current state.
- **G1 severity (code-grounder vs design-critic).** Reconciled: **zero current live-query impact** — the running PG system derives extensions via `ExtractUniqueExtensions → FileExtensionFromPath` (`serializer.go:74-96`, `persist.go:228`), and the Rust Tantivy index isn't deployed. **But** the Rust `transform.rs:62-68` builds the torrent-level `file_extensions` facet from the blob's empty `e`, so the bug **manifests the moment the Phase-3 torrent index is backfilled** (crawl-path torrents → empty `file_extensions`), as well as in the file index. → fix G1 **with** the Phase-3 work, not merely before the file index.

---

## 3. Must-fixes before any file-index code

- **B1 — spec-internal contradiction (display hydration).** D8 + §4.1 say `extension` is "hydrated from the blob" while the blob's per-file `e` is empty for crawl-path torrents. The *filter* field is already G1-correct (§5.1/§10), but the *display-hydration* wording must also **derive from the blob's `path`** (present), never `e`. Reconcile the spec, or an implementer following D8 literally reintroduces G1.
- **G1 checker gap.** `consistency/checker.go:63-93` compares only index/path/size — **never extension** — so the empty-`e` bug is invisible to parity tests and backfill-only tests silently pass. Add an `extension` field to the checker (0 GB).
- **C6 — retired PG path guard.** `criteria_torrent_file_extension.go`'s `EXISTS torrent_files` must be guarded so the retired PG file-search mode cannot run post-`DROP TABLE torrent_files`.

---

## 4. Strategic recommendation — SHIP THE CHEAP COMPOSITION (DECIDED BY BENCHMARK, 2026-06-07)

> **UPDATE 2026-06-07 — the empirical benchmark SETTLED this; see [`file-grained-search-benchmark-results.md`](./file-grained-search-benchmark-results.md).** The original framing below ("ship cheap first, then *gate the index on a product decision*") is now superseded by **measurement**: the index's whole premise — sub-50 ms interactive per-file search — was **REFUTED** on the real 879.5M-row corpus.

Measured on the full corpus (real data, HEL1):

| Restorer | Parity it gives | Measured |
|---|---|---|
| G1 + G2 + timestamp/index-sort hydration (code) | correct ext filter/sort; per-torrent browser survives the drop; timestamps | **0 GB** |
| Per-(torrent,ext) aggregate (PG) | exact one-sided distinct-torrent counts + keyset deep paging | **+5–8 GB** (or ~5–6 GB max-only) |
| **DuckDB-on-blobs/Parquet** | exact per-file `ext∧size` + **path-FTS via `ILIKE` (CJK-safe)** + collapse + analytics/joins | **0.015–1.3 s, +3.86 GB** (or +0 GB on-demand) |
| ~~File-grained Tantivy index~~ | ~~the above at <50 ms~~ → **REFUTED:** scan-bound **~1.3 s p50 @879.5M** on the common broad filter, **NO latency win**, **+14–25 GB** | ❌ **rejected** |

→ **Ship the cheap composition (G1 + G2 + hydration + the aggregate + DuckDB-on-Parquet) — full stop.** The benchmark proved the 873M-doc index gives *no* latency advantage (it's scan-bound on the broad filter, just like DuckDB), costs 4–6× the disk, and lacks free-text + exact-count/joins. **Reject/defer the index**; gate it ONLY on a *later, explicit* requirement for per-keystroke free-text path search at <50 ms with realtime freshness — and re-scope it then (FAST identity not stored `doc_id`; CJK tokenizer; incremental merge).

The production **complete-parity architecture** built on DuckDB-on-Parquet (+ the latency-optimization work the user requested) is now being designed → **`duckdb-parquet-parity-architecture.md`** (in progress).

Honest parity level = **complete functional parity via DuckDB-on-Parquet + blob-browse + the PG aggregate, with documented exceptions E1–E6** — and (per the benchmark) at *better* latency than the rejected index on every realistic query.

---

## 5. Sizing & scope caveats (if Phase B proceeds)

- **B3 — sizing is unvalidated + under-counts.** No Tantivy index has ever been built here (Phase 3 is planned, nothing deployed). The §6 table omits the **INDEXED numeric term-dict** for `size` (hundreds of millions of near-unique values at 873M docs) and `published_at` FAST alone ≈ 3.3 GB. Realistic v1 ≈ **10–16 GB, not 8–12**. **Drop INDEXED on `size`/`published_at`** (range comes from FAST — the spec's own cite) and make the `backfill_limit=100000` **smoke pass a hard GO/NO-GO gate** that extrapolates true size.
- **H1 — 873M-doc backfill/merge time/CPU is unquantified** (disk is fine on the 200Gi PVC; time/merge-churn is the real cost).
- **H2 — path FTS (v1.1) is a separate project:** three conflicting size estimates (+2-4 / +8-18 / 16-30 GB total), and the default tokenizer **cannot segment CJK** (common in this corpus) → silently useless for CJK paths. Needs its own tokenizer decision + smoke-sizing, not a flag flip.
- **H3 — content_type denorm ↔ reclassification write-amp:** filesets are immutable per info_hash, so only content_type changes force re-index; a classifier-wide reprocess would push up to 873M doc rewrites through the live hook. Route mass reclassification to a **batch rebuild**, and consider post-filtering content_type at hydration (→ a write-once index) vs denorming.
- **M1 — G2 in-memory browser** is bounded only if the deployed `save_files_threshold` is ≤ ~100 (avg 51.8 files/torrent). Confirm the deployed value.
- **M2 — BEP-52 v2** per-file merkle needs a **blob-format bump first** (merkle isn't in the blob); the "disposable rebuildable cache" framing doesn't make v2 free.

---

## 6. Homelab deploy delta (the file index is a 2nd index on the SAME Phase-3 sidecar)

- **D0 (structural, affects the Phase-3 drafts now):** the role mounts the PVC **at** `/var/lib/bitmagnet/search` (the index dir). A sibling `…/search-files` would land on ephemeral fs. **Fix: mount the PVC at the parent `/var/lib/bitmagnet`** with `search/` + `search-files/` subdirs. Cheap now (nothing deployed); forward-compatible even for Phase-3-alone.
- Second index dir + `bitmagnet_search_file_index_enabled` gate + `BITMAGNET_SEARCH_FILE_INDEX` env; **memory limit 6→8 Gi** (two 256 MiB writer heaps + mmap); **third image COPY** (`backfill_files` bin); a **new file-backfill Job** (commit cadence counts *file* docs not torrents → ~873M docs, source = `torrents`/blob, cursor over `torrents` not `tc.id`, **G1-derived extension**); sequential single-writer backfills; a CNP + Make targets; a `bitmagnet_search_tantivy_file_doc_count` metric; a **second GO/NO-GO ceiling line** (torrent ≤74 + file ≤30 ≈ 104 GB — 200Gi PVC still suffices).

---

## 7. Task breakdown

See the tracked task list (team `bitmagnet-filesearch`). Sequenced **cheap-first, index-gated**:

**Phase A — cheap functional parity (+3–5 GB, no new index, do-first):**
A1 G1 (derive extension from path everywhere + checker `extension` field — also fixes the Phase-3 torrent facet) · A2 G2 (re-point `TorrentQuery.files` at the blob) · A3 per-file timestamp + index-sort hydration · A4 per-(torrent,ext) aggregate in PG · A5 DuckDB-on-blobs companion · A6 reconcile the B1 spec contradiction · A7 C6 retired-PG-path guard.

**DECISION GATE (product):** is interactive (<50 ms) per-file search + path FTS a real requirement, or is DuckDB (1–10 s) + the aggregate acceptable? Does the UI default to file rows or torrent rows?

**Phase B — file-grained Tantivy index (gated on the decision):**
B1 proto `FileSearchService` + regen · B2 Rust v1 (file_schema **without INDEXED numerics**, file_indexer, backfill_files, 2nd-index lifecycle, failure isolation) · B3 Go v1 (BuildFileDocuments, FileClient, guarded dual-write, filesearch.Service, GraphQL `fileSearch`, gate) · B4 **hard smoke-gate** (extrapolate true size) · B5 full backfill + set-equality parity gate on **crawl-path** blobs · B6 homelab deploy delta (§6, incl. D0).

**Phase C — advanced (further gated):**
C1 v1.5 collapse (aggregate bucket-vector + gated DistinctTorrentCollector) · C2 v1.1 path FTS (separate CJK-aware tokenizer spike + smoke-sizing) · C3 BEP-52 v2 blob-format bump for per-file merkle.

---

## 8. Open questions for the user

1. **(strategic)** Ship the cheap composition (Phase A) first and gate the 873M-doc index on measured demand? Is <50 ms per-file search an actual requirement, or is DuckDB at 1–10 s acceptable? **UI default: file rows or torrent rows?**
2. Is per-file **path FTS** wanted enough to fund a dedicated tokenizer/sizing spike (and accept its CJK limitation)?
3. Approve the hard smoke-test GO/NO-GO gate + dropping INDEXED on `size`/`published_at`?
4. Mass reclassification → batch rebuild (not the live hook)? Denorm `content_type` or post-filter?
5. What is the deployed `save_files_threshold`? (bounds G2's in-memory browser)
6. Apply the **D0 parent-mount fix to the Phase-3 drafts now** (cheap, forward-compatible), or defer to the file-index phase?
