# EXP-D (CJK tokenizer) + EXP-E (incremental merge / freshness) — Benchmark Spec

**Date:** 2026-06-07
**Status:** DESIGN — gated runs on the HEL1 throwaway bench env (production FSN1 untouched).
**Why:** The free-text answer rests on three numbers. Two were directly measured (inverted-index **+35 GB**, path-FTS **23 s → 150 ms** — `arch-c-parity-and-optimization-results.md:57`). The other two were _reasoned, not benchmarked_:

- **"needs a CJK-aware tokenizer"** — we measured that `ILIKE` is CJK-correct and that DuckDB FTS matched a _whole_ CJK token, then **inferred** sub-token CJK misses. Never measured a segmentation/ngram tokenizer's size + recall.
- **"incremental merge for freshness"** — we measured Tantivy _structured_ build/merge RAM (`file-index-bench-RESULTS.md:33-36`) but never measured **inverted-index freshness under live dual-write** (EXP-B did this for DuckDB-on-Parquet only).

These two experiments close that gap. Both **extend the existing `bench-file-index` crate** (tantivy 0.26.1); no app crate changes.

**Grounding probe (read-only, done):** the corpus is **15.217 % CJK files — 133,825,554 of 879,474,852** have a CJK codepoint in the path. The tokenizer question is not academic.

---

## EXP-D — CJK path tokenizer: size + recall + latency

### Question

Does the default tokenizer actually miss sub-token CJK substring queries, and does a CJK-aware tokenizer fix recall — at what index-size and latency cost?

### Tokenizers compared (on the `path` field, identical real doc population)

| id        | tokenizer                                                                 | dict?                  | rationale                                                                |
| --------- | ------------------------------------------------------------------------- | ---------------------- | ------------------------------------------------------------------------ |
| `default` | Tantivy `SimpleTokenizer` (`TEXT`, the V11 path field)                    | none                   | the known CJK-broken baseline                                            |
| `ngram`   | Tantivy built-in `NgramTokenizer` (char bi/tri-gram, `prefix_only=false`) | none                   | language-agnostic CJK substring; the pragmatic multilingual choice       |
| `lindera` | `lindera-tantivy` CJK morphological segmentation                          | yes (ipadic/cc-cedict) | proper segmentation; **stretch** (heavier build, behind a cargo feature) |

**Priority:** `default` vs `ngram` (both dict-free) _proves the claim_. `lindera` is additive — if its dict build is heavy/fails it must not block the core result.

### Recall methodology (the load-bearing correctness point)

Index and ground truth **must cover the identical doc population** or recall is meaningless. So ground truth is computed **in-process, in Rust**:

1. Pre-sample ~150 real **CJK substrings of length 2–4 chars drawn from the _middle_ of CJK runs** in actual paths (via duckdb on `files_full.parquet`) → `cjk_queries.txt`. Mid-run substrings deliberately straddle token boundaries — exactly where `default` fails. Include a control group of ASCII substrings (e.g. `1080p`, `bluray`) that `default` _should_ get right.
2. Stream the same `--limit-docs N` real docs (`--source torrent-files`, keyset order). For each query string, accumulate the **exact** `path.contains(query)` doc-identity set = truth.
3. For each tokenizer index over that same N: `recall = |hits ∩ truth| / |truth|`, `precision = |hits ∩ truth| / |hits|`, plus query latency p50/p95.

### Outputs

- Per tokenizer: **path-field component bytes** (term dict + postings + positions) at N=50 M → extrapolate to 879.5 M; **build time**; **CJK recall/precision** (mid-run substrings) vs **ASCII recall** (control); query **latency**.
- Expected shape: `default` ≈ high ASCII recall / **near-zero mid-run CJK recall**; `ngram` ≈ high CJK recall but **larger term-dict/postings** (n-gram explosion) → the real disk cost of CJK-correct free-text. `lindera` ≈ high recall at a different (token-dict) size point.

---

## EXP-E — inverted-index freshness under live dual-write

### Question

Can a Tantivy inverted index actually be kept live-fresh (its claimed unique edge over batch DuckDB-Parquet), under continuous dual-write, with the **default incremental merge policy** — and at what latency / segment-count / RAM / supersession cost? (The Tantivy analog of EXP-B's DuckDB base+delta.)

### Protocol

1. **Base**: build an index over `--base-docs N` with the **default `LogMergePolicy`** (incremental) — _not_ the current `NoMergePolicy` + force-merge.
2. **Live append**: append deltas as small frequent commits (the processor commits ~per-second batches) and at the EXP-B delta sizes (`+1k / +10k / +100k`), reloading the reader after each.
3. **Measure**:
   - **Freshness lag** = wall time from `writer.commit()` → `reader.reload()` seeing the new doc (Tantivy NRT searchability) — the headline number, vs EXP-B's DuckDB minute-scale / <250 ms.
   - **Segment count** growth + whether `LogMergePolicy` keeps it bounded (no unbounded fan-out).
   - **Query latency** (`ext∧size` + a path/CJK query) as a function of delta volume + segment count.
   - Background **merge RAM/CPU** during steady-state appends (contrast the 4.83 GB force-merge-to-1 artifact — production must _not_ force-merge).
   - **Supersession** = `delete_term(info_hash)` then re-add (a re-crawl replaces the whole fileset) → cost + confirm stale docs vanish after commit+reload. The inverted-index analog of EXP-B's torrent-granular anti-join.

### Outputs

- Freshness-lag curve vs delta size; segment-count + merge-cost curve; query latency vs delta volume; supersession delete+re-add cost. Head-to-head vs EXP-B (DuckDB base+delta).

---

## Crate changes (`bench-file-index`, throwaway)

- `schema.rs`: add `PathTokenizer {Default, Ngram, Lindera}`; register the tokenizer on the index; path `TextOptions.set_tokenizer(...)` with positions (phrase/substring).
- `main.rs`: `recall` subcommand (`--tokenizer --queries-file --limit-docs --source torrent-files`) → per-tokenizer build + in-process exact-substring truth + recall/precision/latency/size; `freshness` subcommand (`--base-docs --delta-sizes --commit-batch`) → default merge policy, append loop, commit→reload lag, segment count, supersession.
- `Cargo.toml`: ngram is in tantivy (no dep); `lindera-tantivy` behind an **optional feature** so a heavy/failed lindera build can't block default-vs-ngram.

## Execution & safety

- Build + run **on HEL1** (path-deps + sqlx link there, as RUN-4 did). **Serial**: EXP-D fully, then EXP-E — each confirms the box is idle first (the RUN-4 CPU-contamination lesson).
- Everything is the **HEL1 throwaway bench env** (PG restore + Parquet + userspace rust/uv). Production FSN1 untouched; no Ansible/prod path; no new infra. Cleaned by RUN-6.
- Data: CJK query file pre-sampled from `files_full.parquet`; EXP-E streams real rows from the bench PG (NodePort DSN) for live-append realism; `delta_*.parquet` (EXP-B) reused for delta shapes where useful.

## Deliverable

Results → `cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`; MEMORY update; and the free-text answer upgraded from _reasoned_ to _measured_ on all four numbers.
