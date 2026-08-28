# PS-MB1 — per-torrent path-bag micro-bench — RESULTS

**Date:** 2026-06-09 · **Status:** 🟡 SKELETON — awaiting runner logs (`ps-mb-runner` builds/ingests on HEL1; analyst LOCAL-only, fills tables as `psmb_*.log` land in `docs/dev/psmb-logs/`).
**Env:** HEL1 throwaway bench (879.5 M-row pre-blob-backfill restore, `torrent_files` source; userspace rust). Production FSN1 untouched. ONE serial run, ONE ssh connection (runner owns it).
**Spec:** [`pathsearch-microbench-spec.md`](./pathsearch-microbench-spec.md). **Crate:** `bench-file-index` (tantivy 0.26.1) — extended with `--granularity per-file|per-torrent`, `--tokenizer edge-ngram` (`PerWordEdgeNgram`), per-torrent path-bag grouping + OR-truth.
**Baseline it extends:** [`cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`](./cjk-tokenizer-and-incremental-merge-bench-RESULTS.md) (EXP-D/D2/E).

**The two unknowns this resolves (the whole L3 gate):**

- **G3 (latency):** does the per-torrent ngram index clear **< 50 ms warm p50 on the broadest 3-char prefix** (`ascii3` AND `cjk3`)?
- **G5 (size):** is the per-torrent index **materially under the 94 GB per-file ceiling** (target **< ~30 GB**)?

---

## 0. Headline verdict

> **🟢 GO (MEASURED) — build L3 as per-torrent char-ngram(2,3), `WithFreqs`.** Arm A2 (full corpus, 16,973,470 torrents) retires every projection with a direct measurement:
>
> - **G5 size PASS:** production **13.54 GiB** (`WithFreqs`; as-built 81.86 GiB − 83.5 % dead-weight positions) — ≪ 94 GiB ceiling, under the < 30 GiB target. _(Validated the projection chain: as-built 81.86 vs extrapolated 81.72 GiB, 0.18 % off.)_
> - **G3 latency PASS (on p50, the gate):** `ascii3` warm p50 **24.71 ms**, `cjk3` **0.21 ms** — both < 50 ms. **⚠️ Documented caveat:** the _broadest worst-case_ substrings breach 50 ms at the **p95/p99 tail** (`ascii3` p95 58.6 ms; `ascii5` p95 64.4 ms) — median typeahead is interactive (~25 ms), the broad-query tail is ~55–65 ms. **min-chars = 3 does NOT fix it** (`ascii3` _is_ the 3-char floor and it's the breaching row). Real mitigations: real-query selectivity (these are synthetic worst-case grams), debounce + result caps + index-sort(seeders) top-k. Context: even the tail is only ~10–15 ms over 50 ms — **not** the 23 s ILIKE wall, **not** per-file's 100–145 ms.
> - **Recall PASS:** Arm A ngram **1.0000** every group (per-torrent grouping is tokenization-sound).
> - **Arm C / GO-CHEAPER REJECTED:** bigger in production (21.3 GiB) **and** misses the most common substring queries (`264`→0.19 recall).
>
> L3 remains a **purely additive, NO-GO-by-default add-on** — this bench only proves it's _buildable cheaply and fast enough_ if a real product demand (G1) + in-prod ILIKE-wall (G2) ever fire. It never gates the `torrent_files` DROP (G4).
>
> **Size (G5 — the PRODUCTION number): ✅ PASS ≈ 14.2 GiB.** The T3 production design indexes the path field `WithFreqs` only (no positions — ngram conjunction queries never read them). The bench measured `WithFreqsAndPositions`, where **positions = 82.6 % of the 4752 MB** (a dead-weight artifact of the per-torrent multi-valued path-bag). Subtracting the measured `positions (path)` component gives the production size **exactly** (a `WithFreqs` build simply never writes the `.pos` files; postings/term/fast/store are byte-identical): **(4752 − 4117) = 635 MB → ×17.608 ≈ 14.2 GiB** full-corpus — materially under the 94 GiB per-file ceiling **and** under the < 30 GiB target. _(As-measured 4752 MB → ~81.7 GiB is **with positions, which are dropped in production** — not the prod number.)_ The subtraction is **computed-exact**, not an estimate: in Tantivy, positions live in their own `.pos` segment files; the postings (`.idx` = doc-ids + freqs) and term dict (`.term`) are byte-identical between `WithFreqsAndPositions` and `WithFreqs`, so `production_total = measured_total − positions_component` exactly → **G5 is settled PASS without any re-measure.** The win is the **postings collapse**: per-file ~88 GiB postings → per-torrent ~13 GiB (~6.7×), because each gram is one posting per _torrent_, not per _file_.
>
> **Latency (G3 — PROJECTED to full corpus, now anchored on same-env Arm B): likely PASS, ~26–30 ms central, MARGINAL.** The 1.36 ms `ascii3` is at the **965 k-torrent / ~50 M-file slice**; full per-torrent corpus is ~16.99 M docs (×17.6). Arm B (per-file, same harness/method) gives the in-env scaling: per-file `ascii3` 5.25 ms @50 M → EXP-D2 101 ms @879.5 M = **×19.2** for ×17.6 docs (mildly super-linear, ×1.09). Three methods bracket the full-corpus `ascii3`: doc-scaling **~26 ms**, slice-per-hit **~24 ms**, conservative at-scale-per-hit **~39 ms** ⟹ **~24–39 ms (central ~26–30)** — clears 50 ms in every method, `cjk3` ≪ 1 ms, **but the conservative tail (~39 ms) is not "comfortably < 25 ms."** Per-file by contrast **BREAKS** the gate at full scale (EXP-D2 101–145 ms) — its docs grow with the corpus; per-torrent caps at ~17 M docs, which is exactly why Arm A is the gate-holding arm.
>
> **What's pending before FINAL GO** — the verdict is structured to land as **"GO (projected)" → "GO (measured)"**:
>
> 1. **Arm B (per-file control @50 M)** — anchors the actual same-env doc-count scaling factor (does the harness reproduce EXP-D's ~16.4 ms ngram-ASCII @50 M _in this env_?), refining the ×6.16. _Blocker on even the projected verdict._
> 2. **Arm A2 (full-corpus per-torrent build, all ~16.99 M torrents / 879.9 M rows) — the likely capstone.** Because a GO flips a major architecture decision (NO-GO → add the L3 index) and the conservative projection tail (~39 ms) sits above the lead's "comfortably < 25 ms" confidence bar, the lead is leaning toward **MEASURING** full-scale latency + size directly rather than resting on a projection. A2 also yields the **exact** full-corpus size (no ×17.6 extrapolation; positions-on fine — subtract `.pos` as before). The lead makes the A2 call right after Arm B + the refined projection.
> 3. _(Settled — not pending)_ Size: G5 PASS at 14.2 GiB by exact `.pos` subtraction; no positions-off re-measure needed.
>
> **Arm C (edge-ngram) — DONE: NOT GO-CHEAPER.** In the shipping config (`WithFreqs`) Arm C is **bigger** than Arm A (21.3 vs 14.2 GiB — edge-grams to 12 chars inflate the term-dict 8×) **and narrower** (per-word prefix). Recall sanity (§4) seals it: Arm C **misses the most common substring queries** — `264` recall **0.19**, `265` recall **0.13** (can't find `x264`/`x265` by codec substring). Arm A ngram = recall **1.0000 on every group**. **Recommendation is Arm A (char-ngram); GO-CHEAPER does not fire.**
>
> **Recall sanity — DONE ✅:** Arm A per-torrent ngram recall **1.0000** (grouping is tokenization-sound). **Only Arm A2 (full-corpus capstone) remains** to convert GO (projected) → GO (measured) — see §5/§6. _Note: the runner closed the 3-arm run (#78) **without** running A2 and handed the full-corpus sizing back as an extrapolation; A2 needs an explicit go from the lead while the bench is still up._
>
> **Correction noted:** the often-quoted "9.5 ms @50 M" ASCII figure is the **default** tokenizer; the ngram tokenizer (what we ship) is the right anchor — superseded below by the same-method Arm B datapoint (5.25 ms @50 M).

---

## 1. The arms

All on the existing 879.5 M-row HEL1 restore, ~50 M-row slice. `965000 ≈ 50 000 000 / 51.79` (measured avg files/torrent) so A/C and B cover ~the same first ~50 M `torrent_files` rows.

| arm               | granularity | tokenizer        | `--limit-docs`    | docs indexed | answers                                                           |
| ----------------- | ----------- | ---------------- | ----------------- | ------------ | ----------------------------------------------------------------- |
| **A (PRIMARY)**   | per-torrent | ngram(2,3)       | `965000` torrents | ⏳           | **G3 + G5** for the recommended design                            |
| **B (control)**   | per-file    | ngram(2,3)       | `50000000` files  | ⏳           | reproduces EXP-D2 @50 M — anchors comparison / harness-soundness  |
| **C (secondary)** | per-torrent | edge-ngram(2,12) | `965000` torrents | ⏳           | cheaper ASCII-prefix arm — beats A on size while staying < 50 ms? |

### Extrapolation factors (pre-computed)

Full-corpus index size = `measured_total_bytes × factor`:

| arm class          | formula                         | target docs         | indexed docs | **factor**   |
| ------------------ | ------------------------------- | ------------------- | ------------ | ------------ |
| per-torrent (A, C) | `16,992,238 / torrents_indexed` | 16,992,238 torrents | 965,000      | **× 17.609** |
| per-file (B)       | `879,474,852 / docs_indexed`    | 879,474,852 files   | 50,000,000   | **× 17.590** |

Both ≈ **17.6×** — the same factor EXP-D2 used (50 M → 879.5 M). Use the **actual docs emitted** from each build log's header (`=== recall … | <docs> docs | …`) as `docs_indexed`, not the assumed 965 000 — torrent grouping over `torrent_files` yields ≠ 965 000.

> **⚠️ Per-torrent denominator caveat (keep 16,992,238 — do NOT use 48 M).** The bench `torrents` table is 48.03 M rows, but the per-torrent path-bag index is built from **`torrent_files`** → one doc per DISTINCT info*hash that \_has* files (= multi-file torrents, **≈ 16,992,238**). The other ~31 M are single-file/no-file torrents absent from `torrent_files` in this pre-backfill dump, so they are **not** in the measured index. In production a real path-FTS index would _also_ index those single-file torrents (name = the one short path, ~8 % of torrents, 1 path each → negligible size add). ⟹ the extrapolated size is a slight **UNDER-count** of a true production index, but the per-torrent-vs-per-file **shape** comparison and the **latency gate are unaffected**.

---

## 2. Size — the G5 half of the gate

`--skip-truth` build reports path-field **B/doc** (term-dict + postings + positions) and **total index MB** (`report_segment_bytes`), force-merged to 1 segment.

| arm                                         | docs indexed         | path **B/doc** (term+post+pos)                | full **B/doc** (incl. identity) | **MEASURED total** (MB)   | ingest s (docs/s) | segments | **EXTRAPOLATED full-corpus index**                                                      |
| ------------------------------------------- | -------------------- | --------------------------------------------- | ------------------------------- | ------------------------- | ----------------- | -------- | --------------------------------------------------------------------------------------- |
| **A** per-torrent ngram(2,3)                | 965,006              | 5150.59 (55.3 + 828.9 + **4266.4 pos**)       | 5163.62                         | **4752.05**               | 249.0 (3875/s)    | 1        | **as-built ~81.7 GiB** (87.7 GB) ⚠️ pos-dominated · **pos-OFF ~14.2 GiB** ✅ (× 17.608) |
| **B** per-file ngram(2,3)                   | 50,000,000           | 103.31 (1.10 + 100.29 + **1.92 pos**)         | 116.34                          | **5547.27**               | 328.4 (152,272/s) | 1        | **~95.3 GiB** (102.3 GB) · WithFreqs ~93.7 GiB (× 17.590)                               |
| **C** per-torrent edge-ngram(2,12)          | 965,000              | 3443.57 (453.2 term + 877.5 + **2112.9 pos**) | 3456.60                         | **3181.09**               | 260.7 (3702/s)    | 1        | as-built ~54.7 GiB · **WithFreqs ~21.3 GiB** ⚠️ _> Arm A_ (× 17.608)                    |
| **A2** per-torrent ngram(2,3) _full corpus_ | **16,973,470** (all) | 5163.06 (15.7 + 827.8 + **4322.2 pos**)       | 5178.77                         | **83,829.58** (81.86 GiB) | 4444.2 (3819/s)   | 1        | **MEASURED: as-built 81.86 GiB · production WithFreqs 13.54 GiB** ✅                    |

> **✅ A2 (capstone, full corpus) — G5 PASS, MEASURED, no extrapolation.** 16,973,470 torrent path-bag docs (corpus exhausted). Components: positions(path) **4322.2 B/doc (83.5 %)**, postings **827.8 B/doc**, term dicts **15.7 B/doc**, identity ~13 B/doc. **TOTAL 81.86 GiB** with positions; **production (`WithFreqs`, drop `.pos`) = TOTAL − positions = 14,538,422,994 B = 13.54 GiB** — ≪ 94 GiB ceiling, under the < 30 GiB target.
> **Projection chain validated:** as-built measured **81.86 GiB** vs Arm A extrapolation **81.72 GiB** (0.18 % off); production measured **13.54 GiB** vs projected **14.2 GiB** (measured _better_ — term-dict B/doc fell 55.3 → 15.7 from slice to full corpus, i.e. the gram vocabulary is bounded so term-dict scales _sublinearly_). Postings B/doc 827.8 ≈ slice 828.9 (constant, as predicted). The ×17.6 extrapolation is now retired by a direct measurement.

> **🚨 Arm C size — the "33 % smaller" REVERSES in production.** As-built (with positions) Arm C 3181 MB _is_ 33 % under Arm A's 4752 MB — but production drops positions (`WithFreqs`), and there the comparison **flips**: edge-grams up to 12 chars **inflate the term dict 8×** (Arm C term-dict **453.2 B/doc / 7.2 GiB** vs Arm A **55.3 B/doc / 0.9 GiB**), which positions-removal does _not_ touch. ⟹ **production Arm C ≈ 21.3 GiB > Arm A's 14.2 GiB.** Postings are comparable (13.9 vs 13.1 GiB). So the cheaper-on-disk premise of the GO-CHEAPER arm **does not hold in the config we'd ship.**

> **Arm A component breakdown** (965,006 torrent-docs, 1 segment, force-merged): positions(path) **4117.1 MB / 4266.4 B/doc (82.6 %)** · postings 799.9 MB / 828.9 B/doc · term dicts 53.4 MB / 55.3 B/doc · FAST 7.7 MB · doc store 3.9 MB · field norms 1.0 MB. **TOTAL 4752.05 MB.** > **Extrapolation (× 16,992,238/965,006 = 17.608):** as-built **81.7 GiB**; positions **67.5 GiB**; postings **13.1 GiB**; **positions-OFF total ≈ 14.2 GiB**. ⟹ the per-torrent postings collapse (per-file ~88 GiB → 13 GiB, ~6.7×) is real; positions are the only thing keeping Arm A near the per-file ceiling, and they are **provably unused** by the conjunction-of-grams query (`main.rs:1042-1060`, `IndexRecordOption::WithFreqs`).

**Anchors / expectations:**

- **B must reproduce EXP-D2's per-file ngram numbers** (harness-soundness sanity): path **101.6 B/doc** (postings 100.17 + pos 1.13 + term-dict 0.31), **~89.3 GB** path index / **94 GB** total @ 879.5 M, ingest ~150 k docs/s. If B's extrapolation lands far from 94 GB, the harness or slice is suspect → flag before trusting A/C.
- **A (per-torrent) is the size-win hypothesis:** collapsing 51.79 files → 1 path-bag doc deduplicates each gram's posting to **one entry per torrent** (a gram recurring across a torrent's files counts once), so postings — which dominate ngram size — should shrink sharply vs per-file. Positions/term-freq per doc may rise (many files → one doc), the countervailing force. Net G5 question: does the postings collapse beat the per-doc TF growth? Target extrapolation **< ~30 GB**.
- **C (edge-ngram) cost driver:** per-word edge-grams up to width 12 → fewer grams per ASCII word than full ngram(2,3) BUT wider max → more distinct prefix terms; CJK still full sliding ngram. Net size vs A is the open question.

---

## 3. Latency — the G3 half of the gate

Cold-first + 15 warm reps after `drop_caches` (if root unavailable, warm-only — runner to note). p50/p95/p99 per `charset × prefix-length` group. **Gate rows = `ascii3` and `cjk3`** (min-chars=3 design floor). 2-char rows = diagnostic (what min-chars buys); 4/5-char = selectivity gradient.

### Arm A — per-torrent ngram(2,3) _(PRIMARY — this is the GO/NO-GO build)_

965,000 docs, 1 segment, drop_caches cold + 15 warm reps.

| group         | avg hits | cold p50 | cold p95 | **warm p50**   | warm p95 | warm p99 |
| ------------- | -------- | -------- | -------- | -------------- | -------- | -------- |
| ascii2        | 251,510  | 0.66 ms  | 2.89 ms  | **0.16 ms**    | 0.67 ms  | 0.77 ms  |
| **ascii3** 🚪 | 121,039  | 2.29 ms  | 3.36 ms  | **1.36 ms ✅** | 3.33 ms  | 3.48 ms  |
| ascii4        | 49,223   | 1.57 ms  | 3.71 ms  | **1.04 ms**    | 3.11 ms  | 3.20 ms  |
| ascii5        | 39,182   | 2.08 ms  | 4.35 ms  | **1.48 ms**    | 3.60 ms  | 3.72 ms  |
| cjk2          | 12,584   | 0.69 ms  | 0.84 ms  | **0.01 ms**    | 0.03 ms  | 0.03 ms  |
| **cjk3** 🚪   | 1,673    | 0.59 ms  | 1.35 ms  | **0.01 ms ✅** | 0.06 ms  | 0.06 ms  |
| cjk4          | 2,309    | 0.54 ms  | 0.92 ms  | **0.04 ms**    | 0.10 ms  | 0.10 ms  |

**⚠️ These are SLICE (965 k-torrent / ~50 M-file) latencies — the gate is on the PROJECTED full-corpus number.** Full per-torrent corpus = ~16.99 M docs (×17.6 more), so postings lists are ~17.6× longer.

**Projected full-corpus latency** — REFINED with the same-env Arm B anchor (replaces the earlier cross-experiment ×6.16). Three methods bracket the gate row:

| method                              | basis                                                                                                                | **`ascii3` full-corpus** |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| (a) doc-scaling ×19.2               | per-file in-env Arm B 5.25 ms @50 M → EXP-D2 101 ms @879.5 M (= ×19.2 for ×17.6 docs) applied to Arm A slice 1.36 ms | **~26 ms**               |
| (b) slice per-hit                   | Arm A slice 11.2 ns/hit × full-corpus 2.13 M torrent-hits (121 k × 17.6)                                             | **~24 ms**               |
| (c) at-scale per-hit (conservative) | EXP-D2's at-scale 18.1 ns/hit × 2.13 M hits                                                                          | **~39 ms**               |

⟹ **projected full-corpus `ascii3` ≈ 24–39 ms (central ~26–30 ms)** — clears the 50 ms gate in every method, but **NOT comfortably < 25 ms**. `cjk3` ≈ **≪ 1 ms** (full-corpus CJK hits ~29 k → sub-ms, no concern). Other groups: `ascii2` ~3–4 ms (single-term, ~flat in size), `ascii4`/`ascii5` ~20–28 ms.

> **⚠️ Projection caveat (why A2 is the real number):** the ×19.2 doc-scaling uses EXP-D2's full per-file `101 ms`, which was measured on the **OLD ASCII query set**, not this prefix sweep — so it is **cross-query-set and only approximate**. The clean in-env, same-query datapoint we _do_ have is Arm B `ascii3` **5.25 ms @50 M per-file**; everything beyond 50 M for _this_ sweep is extrapolation. **Methods (a)/(c) are approximate; treat 24–39 ms as a bracket, not a measurement.** The honest full-corpus number comes from **Arm A2** (authorized).

> **✅ PROJECTION CONFIRMED BY A2:** the full-corpus measurement (below) landed `ascii3` warm p50 at **24.71 ms** — squarely in this 24–39 ms bracket, at the optimistic end. The projection method was sound; the A2 capstone retired its residual uncertainty (and surfaced the p95/p99 broad-substring tail the projection couldn't see).

**This lands in the "marginal" band → too close to declare GO on projection alone. Arm A2 (full-corpus per-torrent) is AUTHORIZED to MEASURE directly** (a GO flips NO-GO → add the L3 index, so the conservative ~39 ms tail must be retired with a number, not a model). Encouraging signs: per-file scaling is only _mildly_ super-linear (×1.09 over linear), and **per-torrent is 3.9× faster than per-file at the identical 50 M slice** (1.36 vs 5.25 ms `ascii3`). The open question A2 settles: per-torrent postings are _denser_ (12.5 % of docs match `ascii3` vs ~0.6 % per-file), so at-scale behavior must be confirmed, not modeled.

**Note (not an anomaly):** `ascii2` (251 k hits) is _faster_ (0.16 ms) than `ascii3` (121 k hits, 1.36 ms) despite more hits — a 2-char query is a **single-bigram TermQuery** (one postings scan / `doc_freq` count), while 3-char queries are a **conjunction of ≥2 grams** (postings intersection), so the longer query does more work. Expected, monotone within charset for ≥3 chars. Positions (82.6 % of disk) are never read by these queries → the size artifact does not touch latency.

### Arm B — per-file ngram(2,3) _(control — should mirror EXP-D2 @50 M)_

| group  | avg hits  | cold p50 | cold p95 | **warm p50** | warm p95 | warm p99 |
| ------ | --------- | -------- | -------- | ------------ | -------- | -------- |
| ascii2 | 4,246,096 | 4.46 ms  | 17.10 ms | 3.09 ms      | 7.76 ms  | 7.85 ms  |
| ascii3 | 1,207,555 | 7.85 ms  | 22.16 ms | **5.25 ms**  | 25.93 ms | 26.60 ms |
| ascii4 | 344,193   | 6.01 ms  | 14.37 ms | 4.76 ms      | 12.59 ms | 12.73 ms |
| ascii5 | 256,503   | 7.47 ms  | 15.58 ms | 5.42 ms      | 13.80 ms | 14.00 ms |
| cjk2   | 111,947   | 0.84 ms  | 1.31 ms  | 0.05 ms      | 0.34 ms  | 0.34 ms  |
| cjk3   | 17,562    | 0.72 ms  | 1.24 ms  | 0.04 ms      | 0.53 ms  | 0.55 ms  |
| cjk4   | 20,481    | 1.01 ms  | 1.29 ms  | 0.06 ms      | 1.14 ms  | 1.25 ms  |

> **✅ Harness-soundness: PASS.** Arm B reproduces EXP-D2's per-file ngram fingerprint in this env: postings **100.29 B/doc** ≈ EXP-D2 **100.17**; size extrapolates to **95.3 GiB** ≈ EXP-D2's measured **94 GiB**; positions negligible (1.92 B/doc ≈ EXP-D2's 1.13). Shape correct (CJK fast, broad-ASCII the ceiling). Harness is trustworthy.
> **✅ Path-bag positions artifact independently CONFIRMED:** per-file positions **1.92 B/doc** vs per-torrent (Arm A) **4266 B/doc** — same tokenizer, same text → the blow-up is unambiguously the per-torrent _multi-valued path-bag_ layout (position gaps between ~52 file-values), not the tokenizer. Confirms `WithFreqs` is the lossless production choice.
> **Per-file BREAKS the gate at full scale (why per-torrent is the right arm):** at 50 M (5.7 % of corpus) per-file `ascii3` is 5.25 ms warm p50 / 25.9 ms p95; per-file docs grow _with the corpus_ → EXP-D2 measured full per-file broad-ASCII at **101–145 ms p50 / 244–320 ms p95** = over the 50 ms gate. Per-torrent caps docs at ~17 M _regardless of corpus growth_ → Arm A holds the gate where Arm B cannot.

### Arm C — per-torrent edge-ngram(2,12)

965,000 docs, 1 segment, drop_caches cold + 15 warm reps. **Semantic caveat below — not substring-equivalent to Arm A.**

| group         | avg hits | cold p50 | cold p95 | **warm p50** | warm p95 | warm p99 |
| ------------- | -------- | -------- | -------- | ------------ | -------- | -------- |
| ascii2        | 206,867  | 0.95 ms  | 2.63 ms  | **0.12 ms**  | 0.39 ms  | 0.57 ms  |
| **ascii3** 🚪 | 104,877  | 1.44 ms  | 2.61 ms  | **0.52 ms**  | 2.18 ms  | 2.19 ms  |
| ascii4        | 48,669   | 0.82 ms  | 1.74 ms  | **0.46 ms**  | 1.41 ms  | 1.45 ms  |
| ascii5        | 38,826   | 0.76 ms  | 1.68 ms  | **0.46 ms**  | 1.61 ms  | 1.61 ms  |
| cjk2          | 12,584   | 0.92 ms  | 1.36 ms  | **0.01 ms**  | 0.03 ms  | 0.03 ms  |
| **cjk3** 🚪   | 1,673    | 0.85 ms  | 1.38 ms  | **0.01 ms**  | 0.06 ms  | 0.06 ms  |
| cjk4          | 2,309    | 0.79 ms  | 1.14 ms  | **0.05 ms**  | 0.11 ms  | 0.11 ms  |

Slice latency edges out Arm A (`ascii3` 0.52 vs 1.36 ms — edge-ngram emits fewer grams per query → smaller conjunction); projects to ~10 ms full corpus. But latency was never the deciding axis (Arm A already passes), and **Arm C loses on the two axes that matter:**

> **🚨 Arm C verdict: NOT GO-CHEAPER — Arm A dominates it in production.**
>
> 1. **Size (production):** Arm C ≈ **21.3 GiB > Arm A 14.2 GiB** (term-dict inflation; see §2). The cheaper-on-disk premise is false once positions are dropped.
> 2. **Capability:** edge-ngram = **per-word PREFIX** matching for ASCII (+ full ngram for CJK runs), **NOT arbitrary substring** like Arm A's char-ngram. It answers word-prefix typeahead but **misses mid-word substring matches** (e.g. typing `264` would not find `x264` as a mid-token hit). Different — and narrower — recall semantics. _(P5 recall sanity, running now, quantifies the exact loss.)_
>    ⟹ Arm C is **bigger AND less capable** in the shipping config; its only edge (latency) is moot since Arm A clears the gate. **GO-CHEAPER does not fire; the recommendation is Arm A (char-ngram).** Arm C stays a fallback _only_ if a hard size crunch ever made 21.3 GiB unacceptable AND word-prefix-only UX were accepted — neither holds today.

### Arm A2 — per-torrent ngram(2,3), FULL CORPUS (~16.99 M torrents / 879.9 M rows) _(capstone — MEASURED full-scale, no projection)_

Only run if the lead authorizes after Arm B. These are the **direct** full-corpus gate numbers (supersede the Arm A projection).

| group         | avg hits  | cold p50 | cold p95 | **warm p50**         | warm p95    | warm p99    |
| ------------- | --------- | -------- | -------- | -------------------- | ----------- | ----------- |
| ascii2        | 4,424,942 | 4.91 ms  | 17.57 ms | **2.83 ms**          | 7.36 ms     | 7.54 ms     |
| **ascii3** 🚪 | 2,129,305 | 30.99 ms | 49.24 ms | **24.71 ms ✅ GATE** | 58.58 ms ⚠️ | 59.71 ms ⚠️ |
| ascii4        | 865,803   | 23.65 ms | 60.03 ms | **18.57 ms**         | 55.84 ms ⚠️ | 56.38 ms    |
| ascii5        | 689,802   | 32.93 ms | 67.53 ms | **27.30 ms**         | 64.39 ms ⚠️ | 64.91 ms    |
| cjk2          | 223,372   | 1.28 ms  | 1.67 ms  | **0.14 ms**          | 0.53 ms     | 0.57 ms     |
| **cjk3** 🚪   | 29,793    | 1.20 ms  | 2.16 ms  | **0.21 ms ✅ GATE**  | 1.15 ms     | 1.16 ms     |
| cjk4          | 41,171    | 1.85 ms  | 1.97 ms  | **0.62 ms**          | 1.76 ms     | 1.77 ms     |

> **✅ G3 GATE PASS (measured): `ascii3` warm p50 24.71 ms, `cjk3` warm p50 0.21 ms** — both < 50 ms. avgHits 2.13 M matches the projection exactly; measured p50 24.71 ms lands at the optimistic end of the projected 24–39 ms bracket. CJK is trivially fast (sub-ms).
>
> **⚠️ Documented broad-substring TAIL caveat (the full-scale nuance the 965 k slice hid):** on the _deliberately broadest worst-case_ substrings, the **p95/p99 tail breaches 50 ms** — `ascii3` warm p95 **58.6** / p99 59.7 ms; `ascii4` p95 55.8; `ascii5` p95 **64.4** / cold p95 67.5 ms. The gate is on **p50** (the per-keystroke median), which passes comfortably; the tail is the worst ~5 % of the broadest-possible 3–5-char substrings (2.1 M-hit match-sets). Real/more-selective queries land far lower (`ascii2` 2.8 ms, all CJK < 2 ms). **🚨 min-chars = 3 does NOT rescue this** — `ascii3` _is_ the 3-char design floor, and it's exactly the breaching row (p95 58.6 ms). The real mitigations are: **natural selectivity of real queries** (these grams are synthetic worst-case, avgHits 2.1 M), **debounce**, **result caps**, and **index-sort by seeders → top-k** (return the top results without fully scanning the match-set). Honest read: **median typeahead is interactive (~25 ms); the broadest-query p95 tail is ~55–65 ms** — the one place the inverted index does not stay strictly < 50 ms, but still ~10–15 ms over (not the 23 s ILIKE wall, not per-file's 100–145 ms).

---

## 4. Correctness sanity (separate WITH-truth run)

Per-torrent run @150 k torrents WITH in-process exact truth (OR over fileset), `--truth-cap 5 M`. Recall over the broad sweep:

| group              | **A ngram recall** | A precision | **C edge-ngram recall** | C precision |
| ------------------ | ------------------ | ----------- | ----------------------- | ----------- |
| ascii2             | **1.0000**         | 1.0000      | 0.7906                  | 1.0000      |
| ascii3 🚪          | **1.0000**         | 1.0000      | 0.8295                  | 1.0000      |
| ascii4             | **1.0000**         | 0.9103      | 0.9535                  | 0.9421      |
| ascii5             | **1.0000**         | 0.9795      | 0.9497                  | 0.9312      |
| cjk2 / cjk3 / cjk4 | **1.0000**         | 1.0000      | 1.0000                  | 1.0000      |

**Arm A (ngram) ✅ — per-torrent grouping is tokenization-sound:** recall **1.0000 on every group** (zero misses) → the path-bag grouping does not change ngram behaviour. Precision dips to 0.91 / 0.98 at `ascii4` / `ascii5` — a **known, benign** char-ngram(max=3) property: a ≥4-char query is a conjunction of trigrams that can co-occur _non-contiguously_ in a long path → a few false-positive candidates. The index is a **candidate generator**; the exact substring is verified on hydration (cheap post-filter), so precision < 1.0 costs a little extra fan-out, never a wrong result. recall = 1.0 is the gate, and it's met.

**Arm C (edge-ngram) ❌ for substring typeahead — quantifies the capability gap:** CJK recall 1.0, but **ASCII recall 0.79–0.95**, and **catastrophic on mid-word substrings** — `265` recall **0.1294**, `264` recall **0.1895**, `dv` 0.566. Edge-ngram is per-word _prefix_ only, so the extremely common codec-substring queries ("find `x264`/`x265` by typing `264`/`265`") essentially fail. This is the empirical seal on **NOT GO-CHEAPER**: edge-ngram's size/latency win buys a search that can't do the most common real query. **ngram (Arm A) is the required tokenizer for substring typeahead.**

**🚩 Anomaly watchlist — all CLEAR:** Arm A recall = 1.0 (✅ not < 1.0) · no writer crash (single-thread + 2 GB arena held across all 4 builds) · the `ascii2`-faster-than-`ascii3` ordering is explained (single-term vs conjunction), not a true inversion · docs_indexed = 965,006 (factor = ×17.608) · Arm B extrapolation 95.3 GiB ≈ EXP-D2 94 GiB (harness sound).

---

## 5. GO / NO-GO rubric (verbatim from spec §4)

- **GO (build L3 as per-torrent ngram, arm A)** iff **`ascii3` AND `cjk3` warm p50 < 50 ms** on arm A **AND** arm A's extrapolated full-corpus index is **materially under 94 GB** (target **< ~30 GB**, i.e. the per-torrent shrink is real). This flips G3+G5 to PASS and the L3 add-on from "triples the footprint" to "acceptable."
- **GO-CHEAPER (arm C)** if arm C clears the same latency bar at a **smaller** index than A — then ship ASCII edge-ngram typeahead + degrade CJK to submit-time substring (PS-T5 option (b)), the most defensible cost-down.
- **NO-GO / hold** if neither per-torrent arm clears `ascii3`/`cjk3` < 50 ms warm p50 — then min-chars=3 + debounce cannot rescue per-keystroke and the honest product answer is **search-on-submit** (DuckDB-FTS ~150 ms), not a +90 GB index. PS-T5's NO-GO-by-default stands.

Whatever the result: this index is **purely additive and never gates the `torrent_files` DROP** (PS-T5 G4). The micro-bench only decides whether the _optional_ L3 layer is worth building if/when a real product demand (G1) and an in-prod ILIKE-wall (G2) ever materialize.

### Evaluation (filled at verdict time)

**The gate is on the PRODUCTION size (`WithFreqs`) and the PROJECTED full-corpus latency** — not the as-measured-with-positions size nor the 50 M slice latency.

| gate                                            | arm A2 (full corpus, MEASURED)                                                       | arm C                                                                | pass?                   |
| ----------------------------------------------- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------- | ----------------------- |
| `ascii3` warm p50 < 50 ms                       | **24.71 ms** (p95 58.6 ⚠️ tail)                                                      | ~10 ms (slice 0.52, narrower recall)                                 | ✅ A2 (p50)             |
| `cjk3` warm p50 < 50 ms                         | **0.21 ms**                                                                          | ≪ 1 ms                                                               | ✅ A2                   |
| **production** index < 94 GiB (target < 30 GiB) | **13.54 GiB** (WithFreqs, MEASURED) ✅ · _(as-built-with-pos 81.86 GiB)_             | 21.3 GiB (term-dict inflated) > A                                    | ✅ A2, smaller          |
| recall (substring)                              | **1.0000** all groups                                                                | 0.79–0.95 ASCII; `264`→0.19 ❌                                       | A2 sound                |
| capability                                      | arbitrary substring (char-ngram)                                                     | word-PREFIX only                                                     | A strictly more capable |
| **→ verdict**                                   | **✅ GO (measured)** — G3 (p50) + G5 PASS; broad-query p95 tail ~55–65 ms documented | **NOT GO-CHEAPER** — bigger in prod AND narrower recall; A dominates |                         |

### What finalizes the verdict (lands as **GO (projected) → GO (measured)**)

1. **Arm B (per-file @50 M) — ✅ DONE.** Harness-soundness PASS (reproduces EXP-D2 fingerprint); anchored the in-env scaling and independently confirmed the positions artifact. Refined `ascii3` projection = ~24–39 ms = **marginal**.
2. **Arm A2 (full-corpus per-torrent, all ~16.99 M docs / 879.9 M rows) — ✅ AUTHORIZED, the last input.** Converts GO (projected) → **GO (measured)**: measures full-scale latency + **exact** size directly, no extrapolation, no cross-query-set approximation. Runs after Arm C + the recall sanity.
   - **A2 GO criteria (verbatim):** GO(measured) iff **A2 `ascii3` AND `cjk3` warm p50 < 50 ms** AND **A2 exact production size (`TOTAL − .pos`) < 30 GiB**.
3. **Size — already decided, not pending.** Production `WithFreqs` ≈ **14.2 GiB** by exact `.pos` subtraction → G5 PASS (A2 reconfirms exactly). Correctness-neutral (ngram conjunction `main.rs:1042-1060` passes `WithFreqs`, never reads positions; EXP-D recall **and** precision = 1.0).

---

## 6. What it means for PS-T5

- **G3 (latency): ✅ PASS on the defined gate (warm p50), MEASURED full corpus — NOT a clean uniform < 50 ms.** `ascii3` warm p50 **24.71 ms**, `cjk3` **0.21 ms** — both < 50 ms on the 16.97 M-torrent corpus. **Caveat:** broadest-ASCII p95/p99 tail ~55–65 ms (`ascii3` p95 58.6/p99 59.7; `ascii4`/`ascii5` p95 56–64; cold p95 ≤ 67.5). **min-chars = 3 does NOT fix it (`ascii3` is the 3-char floor).** Mitigations: real-query selectivity, debounce, result caps, seeder-sort top-k. Per-file _cannot_ hold the gate at full scale (EXP-D2 101–145 ms; docs grow with the corpus) — per-torrent's ~17 M doc cap is what makes G3 pass.
- **G5 (size): ✅ PASS (MEASURED full corpus).** Production `WithFreqs` = **13.54 GiB** (as-built 81.86 GiB − 83.5 % positions, which the conjunction query never reads). Materially under the 94 GiB ceiling and under the 30 GiB target. No extrapolation — measured directly at full corpus.
- **L3 disposition: acceptable add-on, GO (measured)** (per-torrent ngram, `WithFreqs`) — buildable at 13.5 GiB / ~74 min ingest, interactive median latency, recall 1.0. Still purely additive; **never gates the `torrent_files` DROP (G4); NO-GO-by-default until a real product demand (G1) + in-prod ILIKE-wall (G2) fire.**

### 🔑 PS-T5 cost-case reframing (the headline update)

The per-torrent measurement **transforms the L3 cost case** that PS-T5 evaluated:

|                                          | PS-T5 assumption (per-file)          | **PS-MB1 measured (per-torrent `WithFreqs`)** |
| ---------------------------------------- | ------------------------------------ | --------------------------------------------- |
| L3 index footprint                       | +90 GB                               | **+13.54 GiB** (~6.7× smaller)                |
| space savings vs dropped `torrent_files` | 87 % → **55 %** (footprint-tripling) | 87 % → **~84 %** (cheap add-on)               |
| per-keystroke latency                    | 100–145 ms (breaks at full scale)    | **~25 ms median** (p95 tail ~55–65 ms)        |

⟹ **IF the product-demand gate ever fires (G1 real demand + G2 in-prod ILIKE wall), L3 is now a cheap, viable, interactive add-on — NOT the footprint-tripling liability PS-T5 modeled.** The **BUILD-gate itself is UNCHANGED**: still no demonstrated demand, still purely additive, still never gates the `torrent_files` DROP (G4). What changed is only the _cost if/when triggered_ — from "triples the footprint" to "+13.5 GiB / interactive." This is the headline update to fold into PS-T5.

### 🔧 Standing recommendation (applies beyond L3)

**Index path-ngram fields with `IndexRecordOption::WithFreqs`, not `WithFreqsAndPositions`.** Positions are **83.5 % of a per-torrent path-bag index** and are **never read** by the conjunction-of-grams substring query (`main.rs:1042-1060`). This is a pure, lossless 83.5 % size cut for _any_ path-ngram field — **flag it for the existing Tantivy search-sidecar schema too**, not just the hypothetical L3.

### Bottom line

**🟢 GO (MEASURED) — build L3, if/when triggered, as per-torrent char-ngram(2,3) `WithFreqs`.** Every gate is now a direct full-corpus measurement (Arm A2, 16,973,470 torrents):

- **G5 (size): PASS** — production **13.54 GiB** (measured; as-built 81.86 GiB − 83.5 % dead-weight positions). ≪ 94 GiB ceiling, under < 30 GiB target. Projection chain validated (as-built 81.86 vs extrapolated 81.72 GiB).
- **G3 (latency): PASS on p50** — `ascii3` warm p50 **24.71 ms**, `cjk3` **0.21 ms**. **Caveat:** broadest-substring p95/p99 tail ~55–65 ms (`ascii3` p95 58.6, `ascii5` p95 64.4) — worst-case probe; median interactive. **min-chars = 3 does NOT fix it (`ascii3` is the floor);** mitigations = selectivity + debounce + result caps + seeder-sort top-k.
- **Recall: PASS** — 1.0000 every group; per-torrent grouping tokenization-sound.
- **Arm C / GO-CHEAPER: rejected** — bigger in production (21.3 GiB) and misses the most common substring queries (`264`/`265` recall ~0.13–0.19).

**The per-torrent path-bag is the key design win:** it collapses per-file's ~94 GiB → 13.54 GiB _and_ keeps doc-count capped at ~17 M (so latency holds the gate where per-file breaks at full scale, EXP-D2 101–145 ms). The only unlock needed for size is `WithFreqs` (drop the 83.5 % dead-weight positions). **L3 stays NO-GO-by-default** — this micro-bench only proves it's _cheap and fast enough to build_ when a real product demand (G1) + in-prod ILIKE-wall (G2) ever justify it; it never gates the `torrent_files` DROP (G4).

Raw logs (11): `docs/dev/psmb-logs/psmb_{A,A2,B,C}_{build,pq}.log`, `psmb_recall{,_ngram,_edge}.log`. Bench indexes retained on HEL1 `bench-scratch` (`idx_pt_ngram` 4.7G, `idx_pt_ngram_full` ~82G, `idx_pf_ngram` 5.5G, `idx_pt_edge` 3.2G, `idx_recall_*`) pending RUN-6 teardown.
