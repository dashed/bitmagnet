# File-Grained Index — RUN-4 Empirical Results (REAL data, HEL1)

**Date:** 2026-06-07 · **Owner:** `index-bench` · **Source:** real `torrent_files` (pre-blob-backfill restore on HEL1, 879.5M rows) via the standalone `bitmagnet-search-bench` crate (tantivy 0.26.1, release). G1-correct path-derived extension; denorm synthesized (cardinality+range faithful).

Scales built: **N = 1M / 10M / 50M docs**, all 11 schema variants (V1–V11), each force-merged to 1 segment. Per-doc figures are **flat across all three scales** → extrapolation to 879.5M (×17.59 from 50M) is reliable for the linear (store/FAST) components; term-dict components are sublinear (noted).

---

## 1. SIZE — per-component bytes/doc @ 50M → extrapolated to 879.5M

| variant                                  | B/doc @50M    | **→ 879.5M**     | meaning                                                             |
| ---------------------------------------- | ------------- | ---------------- | ------------------------------------------------------------------- |
| **V10 — FAST-only (spec "recommended")** | 28.90         | **~25.4 GB**     | drop INDEXED, but still STORED `doc_id`                             |
| V9 — INDEXED (spec-as-written)           | 35.52         | ~31.2 GB         | size+published INDEXED\|FAST                                        |
| **V9 − V10 = INDEXED numeric tax**       | 6.62          | **~5.8 GB**      | dropping with N (9.55→7.70→6.62) → my 3.6–6 GB prediction ✅        |
| `doc_id` STORED component                | 13.70         | **~12.0 GB**     | 🚨 the giant; §6 guessed 1–2 GB                                     |
| FAST columns (V10)                       | 9.03          | ~7.9 GB          | size ~4.0 + published ~2.9 + ext/ct fast ~2.1 ✅ matches prediction |
| `.fieldnorm` (V10, 2 text fields)        | 2.00          | ~1.8 GB          | avoidable via `set_fieldnorms(false)`                               |
| **V1 stored-doc_id vs V2 FAST-identity** | 16.15 vs 8.76 | **save ~6.5 GB** | identity choice = biggest single lever                              |
| V11 — + path (v1.1 FTS)                  | 51.00         | ~44.9 GB         | positions 4.7 + postings 17.4 + dicts 3.3 — the expensive axis      |

**On-disk confirmation:** the V10 directory = 1.38 GB at 50M → 24.3 GB at 879.5M ✓.

**Optimized recommended build** (FAST-only numerics + FAST identity instead of stored `doc_id` + `set_fieldnorms(false)`):
≈ 28.9 − 13.7(doc_id) + 2.4(fast id) − 2.0(norms) = **~15.6 B/doc → ~13.7 GB**. Matches my 14–18 GB prediction.

**Verdict on size:** even the un-optimized literal spec (V10 ≈ 25 GB) is ≪ the 74 GB GO ceiling and trivially fits the 200Gi PVC. **Size never says NO-GO.**

---

## 2. BACKFILL — throughput / merge / RAM (H1)

- **Ingest throughput** (single writer, 256 MiB heap): **~450–570k docs/s warm** at every scale (V1 cold-cache ~275k). 50M V10 ingest = 110s → **879.5M ≈ ~32 min ingest**.
- **Merge (force to 1 segment)** @50M: V10 **11.3s** (200→1), V9 35.8s (324→1, INDEXED dicts), V11 **60.5s** (400→1, path). Scales with index size.
- **Peak RSS** (whole `--variant all` run = the heaviest single build's merge peak): 1M **0.42 GB** → 10M **1.54 GB** → 50M **4.83 GB**.
  - 🚨 **Caveat / deploy flag:** the 4.83 GB peak is an artifact of my deliberate **force-merge-to-1-segment** (for clean size attribution). Production must use Tantivy's **default merge policy** (incremental, bounded per-merge RAM) — then ingest RAM ≈ writer heap + buffers and the planned pod limit is fine. **Do NOT force-compact a full-corpus index to one segment** (that merge would need tens of GB). Many-segment steady state is correct.

---

## 3. LATENCY — the decisive number (real 50M index)

`SearchFiles` = `ext ∈ {…} ∧ size ≥ T`, sweeping 10 common extensions × {0, 100 MB, 1 GB}. Avg **~950k matching files/query** at 50M.

| scenario                                           | V10 (FAST-only) @50M                   | V9 (INDEXED) @50M      |
| -------------------------------------------------- | -------------------------------------- | ---------------------- |
| **A — file-level top-20 + exact count**            | **p50 72.9ms / p95 208ms / p99 225ms** | p50 95.6ms / p95 209ms |
| B — collapse-to-torrent (scan + dedup, worst-case) | p50 111ms / p95 **3379ms**             | p50 110ms / p95 3382ms |

**Findings:**

1. **Even at 50M, scenario A is already >50ms** (p50 73ms). It is **scan-bound**: a filter-only query has no selective text term, so Tantivy enumerates the entire match set (no early-termination ordering by a non-sort fast field). Latency ∝ match-set size.
2. **At 879.5M (~17.6× the matches), broad `ext∧size` ≈ p50 ~1.3s, p95 ~3.7s.** The headline "<50ms" does **NOT** hold for the common broad query.
3. **V9 ≈ V10** → INDEXED gives **zero** range-latency benefit (range served by `FastFieldRangeWeight` in both). Confirms dropping INDEXED is free.
4. **The v1 file index has NO free-text field** (per-file path FTS is v1.1: +45 GB, CJK-broken). So v1 answers _only_ structured filters — exactly the scan-bound case.

---

## 4. GATE #12 conclusion (index vs cheap composition)

RUN-2 showed **DuckDB-on-Parquet** (+3.86 GB slim) does the same per-file work in **35ms–1.3s** (paginated mkv>1GB 35ms, collapse/histogram ~1.2s, point-hydrate 17ms), with exact counts/joins/analytics.

This RUN-4 shows the **Tantivy file index v1** costs **+14–25 GB** and, for its only surface (structured filters), is **scan-bound → ~1–4s at full scale** — i.e. **no latency advantage** over the +3.86 GB DuckDB composition (and slower on broad filters).

**The index's only unique capabilities are (a) realtime freshness (live dual-write) and (b) per-keystroke PATH free-text (v1.1, +45 GB, CJK-broken — separate project).** Neither is exercised by v1's structured-filter surface.

➡️ **Recommendation: REJECT the file index for v1.** Ship the cheap composition — per-(torrent,ext) aggregate (+3.86 GB) + DuckDB-on-Parquet (+0 persistent) — which already delivers near-complete functional parity at <2 GB extra and comparable-or-better latency. **Revisit the file index only if per-keystroke path free-text becomes a hard product requirement**, and then budget the +45 GB index + a CJK-capable tokenizer (and even then, only the path-FTS field is the unique win — keep the structured filters in DuckDB/aggregate).

If the index is ever built anyway: drop INDEXED on size/published (free, −6 GB), use FAST identity not stored `doc_id` (−12 GB), `set_fieldnorms(false)` (−1.8 GB) → ~14 GB instead of 25 GB; never force-merge to 1 segment at full scale.
