# EXP-D (CJK tokenizer) + EXP-E (incremental merge / freshness) — RESULTS

**Date:** 2026-06-07 → 08
**Status:** EXP-D ✅ DONE · EXP-E ✅ DONE · EXP-D2 (full-scale latency) ✅ DONE. All four free-text numbers now MEASURED.
**Env:** HEL1 throwaway bench (879.5M-row pre-blob-backfill restore + `torrent_files` source; userspace rust). Production FSN1 untouched.
**Spec:** [`cjk-tokenizer-and-incremental-merge-bench-spec.md`](./cjk-tokenizer-and-incremental-merge-bench-spec.md). **Crate:** `bench-file-index` (tantivy 0.26.1) — added `PathTokenizer{Default,Ngram,Lindera}`, `recall`/`pathquery`/`freshness` subcommands.
**Why:** the free-text answer rests on 4 numbers; 2 were measured (index **+35 GB**, **23 s→150 ms** — `arch-c-…:57`); these experiments put the other two — **"needs a CJK tokenizer"** and **"incremental merge for freshness"** — on the same measured footing.

> **Grounding probe:** **15.217 %** of files (133,825,554 / 879,474,852) carry a CJK codepoint in the path. The CJK question is not academic.

---

## EXP-D — CJK path tokenizer (real 50M-doc run)

Method: build a path-only index per tokenizer over the same 50M real `torrent_files` rows; ground truth = **in-process exact `path.contains(q)`** over the identical docs (so recall is meaningful). Queries = **150 mid-run CJK substrings** (2–3 chars drawn from the *middle* of CJK runs, straddling token boundaries) + **10 ASCII controls**. `ngram` = char bi/tri-gram (`min=2,max=3`), queried as a conjunction of grams. Single-thread writer / 2 GB arena (the multi-thread 256 MB writer **crashes** on ngram's token explosion — "index writer killed" at ~330k docs; single-thread fixes it). `lindera` = **skipped** (no `lindera-tantivy` builds against tantivy 0.26's `tantivy-tokenizer-api 0.7`; `default` vs `ngram`, both dict-free, proves the claim).

| metric @ 50M | `default` (word) | `ngram` (CJK-correct) |
|---|---|---|
| **mid-run CJK recall** | **0.0037** | **1.0000** |
| mid-run CJK precision | — (≈0 hits) | **1.0000** |
| ASCII control recall | 0.8112 | 1.0000 |
| ASCII control precision | 0.9992 | 0.9998 |
| path-field **B/doc** (term+postings+pos) | **21.48** (2.52+14.25+4.71) | **103.3** (1.10+100.29+1.92) |
| → path index **@ 879.5M** | **~18.9 GB** | **~90.9 GB** |
| full index @ 879.5M (w/ identity overhead) | ~30.3 GB | ~102 GB |
| ingest (50M, 1 thread) | 200.6 s | 331.0 s |
| CJK query p50 / p95 | 0.00 / 0.01 ms *(meaningless — 0 hits)* | 0.08 / 5.54 ms |
| ASCII query p50 / p95 | 9.5 / 27.8 ms | 16.4 / 61.2 ms |

**Findings:**
1. **The default tokenizer is empirically CJK-broken for substring search** — recall **0.0037** on mid-run CJK substrings (avg 42,135 true matches/query, ~12 found). Confirms the previously-*reasoned* claim with a number. (It also misses ~19 % of ASCII substrings that fall *inside* a token — same whole-token failure mode, milder for Latin.)
2. **A char-ngram tokenizer fully fixes it** — CJK recall **and precision = 1.0000** (the `max=3` gram enforces contiguity for our 2–3-char queries, so the conjunction has no false positives at these lengths).
3. **The cost is steep and postings-dominated.** ngram blows the path index from **~18.9 GB → ~90.9 GB** at full corpus (**~4.8×**), almost entirely in postings (14.25 → **100.3 B/doc**, ~7×: each gram appears in many docs). The ngram *term-dict* is actually **smaller** than default's (bounded 2–3-char vocabulary), and positions shrink (all grams at position 0). So "CJK-correct free-text" ≈ **+90 GB for the path index alone** — larger than the entire DuckDB-on-Parquet architecture (~12 GB) and ~2.6× the ASCII-only DuckDB-FTS estimate (+35 GB).
4. **Latency at 50M is fast** (CJK p50 0.08 ms on a force-merged 1-segment index) — **but 50M latency does not answer the production question**; postings lists grow ~17.6× at full corpus → **EXP-D2** measures the real, full-scale, cold/warm path-query latency (the "is ngram free-text actually interactive?" number).

Raw: HEL1 `bench-scratch/expd_default.log`, `expd_ngram_50M_RESULT.txt`.

---

## EXP-E — inverted-index freshness under live dual-write (real, base 20M)

Method: build a base index with the **default `LogMergePolicy`** (incremental — *not* force-merge), then append +1k/+10k/+100k real docs in 1k commit-batches; measure **freshness lag** (`commit()` → `reader.reload()` sees the new doc), segment count, query latency, peak RSS, and **supersession** (`delete_term(info_hash)` + re-add). The Tantivy analog of EXP-B (DuckDB base+delta).

| delta | segments | **fresh-lag** | commit | ext∧size q | pathTerm q | peak RSS |
|---|---|---|---|---|---|---|
| base 20M | 29 | — | — | — | — | 1006 MB |
| +1k | 17 | **2.26 ms** | 5.8 ms | 46.2 ms | 0.42 ms | 1006 MB |
| +10k | 21 | **1.81 ms** | 8.2 ms | 40.1 ms | 0.26 ms | 1006 MB |
| +100k | 17 | **1.89 ms** | 5.6 ms | 39.8 ms | 0.27 ms | 1006 MB |

- **Supersession:** an info_hash with 28 docs → `delete_term` + re-add 3 + commit + reload = **11.0 ms** → 3 docs, old fileset gone (the inverted-index analog of EXP-B's torrent-granular anti-join). ✓

**Findings:**
1. **Incremental merge for freshness is validated.** `LogMergePolicy` keeps segment count **bounded** (29 at base → 17–21 after deltas+background merges; no unbounded fan-out), at bounded RAM (1006 MB peak).
2. **Freshness lag ≈ 2 ms, flat across delta size** — searchability is near-instant after commit. This is the inverted index's **genuine, measured advantage over batch DuckDB-on-Parquet**: real-time (ms) freshness vs DuckDB base+delta's flush-interval/minute-scale (EXP-B: 141→230 ms collapse, freshness = flush cadence). An always-on inverted writer trades disk + a maintenance process for millisecond freshness.
3. **Query latency on a live (un-force-merged) index** is ~40–46 ms for `ext∧size` over 17–29 segments — the realistic, freshness-coupled latency (vs EXP-D's best-case single-segment). path-term lookups stay sub-ms.

Raw: HEL1 `bench-scratch/expe.log`, `expe_RESULT.txt`.

---

## EXP-D2 — full 879.5M ngram build + cold/warm path-query latency ✅ DONE

Built the full-corpus ngram path index, force-merged to **1 segment**, then `drop_caches` + `pathquery` (cold-first + 15 warm reps) ×3. The three reps were near-identical (numbers below are representative; full set in the raw logs).

**Build:** ingest **5841.6 s (~97 min, 150,554 docs/s)** single-thread; **94 GB** total on disk (100.8 GB raw = 96,143 MB). Path-field **101.6 B/doc** (postings **100.17** + positions 1.13 + term-dict 0.31) → **~89.3 GB path index** (confirms the 50M extrapolation of ~90.9 GB); full 114.6 B/doc incl. identity overhead.

| group | avg hits | **cold p50** | cold p95 | **warm p50** | warm p95 | warm p99 |
|---|---|---|---|---|---|---|
| **CJK** (selective) | 736,858 | **0.86 ms** | 12.4 ms | **0.07 ms** | 7.9 ms | 80.0 ms |
| **ASCII** (broad) | 5,569,267 | **145 ms** | 320 ms | **101 ms** | 244 ms | 247 ms |

**Findings — the headline latency answer:**
1. **ngram CJK free-text is genuinely interactive at production scale.** Even with postings ~17.6× longer than at 50M (CJK match-sets grew 42k → 737k), CJK queries stay **sub-ms warm (0.07 ms p50) / sub-ms cold (0.86 ms p50)**, p95 ≈ 8–12 ms; only the broadest-CJK tail reaches **p99 ≈ 80 ms**. The `<50 ms` premise **holds** for selective CJK free-text. Latency did *not* erode toward DuckDB-ILIKE territory.
2. **The broadest free-text (5.6M-hit ASCII grams) is the real cost ceiling: ~100–145 ms p50, ~245–320 ms p95** — still **sub-second, and ~100–160× faster than DuckDB-ILIKE's ~23 s**, comparable to DuckDB-FTS's ~150 ms. So "broad free-text" latency scales with match-set size, but even the broadest queries stay interactive on the inverted index.
3. **Cold ≈ warm for CJK** (0.86 vs 0.07 ms — both fast); ASCII shows a real cold penalty (145 vs 101 ms p50) since a 94 GB index can't sit fully in page cache — the cold read touches disk for the longer postings.

⟹ **An inverted index delivers fast broad free-text at full scale — confirmed end-to-end** (sub-ms to sub-second, CJK-correct), at **+~90 GB** for the path index. Raw: HEL1 `bench-scratch/expd2_build.log`, `expd2_pq_{1,2,3}.log`.

---

## Synthesis — the four free-text numbers, now all MEASURED

| claim | before | now (measured) |
|---|---|---|
| inverted index disk | +35 GB (DuckDB-FTS, ASCII) | ✅ +35 GB FTS **and** **~+90 GB for CJK-correct ngram path index** (4.8× default; 94 GB measured @879.5M) |
| fast vs slow | 23 s → 150 ms | ✅ **MEASURED @879.5M**: CJK free-text **0.07–0.86 ms** (p50), broadest ASCII **~100–145 ms** — all sub-second vs ILIKE 23 s |
| **CJK tokenizer needed** | *reasoned* | ✅ **MEASURED**: default CJK recall **0.0037**; ngram **1.0** — the default tokenizer silently returns ~nothing on mid-run CJK |
| **incremental merge for freshness** | *reasoned* (DuckDB only) | ✅ **MEASURED**: Tantivy live dual-write = **~2 ms** freshness lag, **bounded** segments, 11 ms supersession |

**Bottom line for "what does it take to get fast broad free-text":** an inverted index is the only thing that delivers it — and it **does** deliver, end-to-end, at full production scale: CJK free-text **sub-ms (p50)** / sub-second worst case, the broadest ASCII grams **~100–320 ms** — vs DuckDB-ILIKE's **~23 s**. For *this* corpus (15 % CJK) "CJK-correct" is the dominant cost: a char-ngram path index is **~+90 GB** (94 GB measured), almost entirely postings — larger than the entire DuckDB-on-Parquet architecture (~12 GB) and ~2.6× the ASCII-only DuckDB-FTS (+35 GB). The index *also* uniquely buys **millisecond freshness** (EXP-E: ~2 ms lag, bounded segments) that batch DuckDB-on-Parquet cannot match. So the trade is now fully quantified:

> **Fast broad free-text = an inverted index + a CJK-aware (ngram) tokenizer + incremental merge → ~+90 GB on disk + an always-on single-writer maintenance process, in exchange for <1 ms–sub-second CJK-correct free-text and ~2 ms freshness.**

Gate it strictly on a measured product need for interactive broad free-text — structured per-file search is already cheap on DuckDB-on-Parquet (+3.9 GB, <250 ms) at minute freshness, so the +90 GB index earns its keep *only* when per-keystroke free-text path search (especially CJK) is a hard requirement.

**Space-savings impact:** the +90 GB index nearly triples the replacement footprint vs the dropped 276 GB `torrent_files` table — drop+cheap-composition saves ~87–90 %, but adding this index drops that to ~55 %. Full layered accounting in [`space-savings-vs-torrent-files.md`](./space-savings-vs-torrent-files.md).
