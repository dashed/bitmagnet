# PS-T3 — Per-keystroke `<50 ms` path-search index design

**Thread:** `ps-t3-index` (team `bitmagnet-bench`) · **Task #74** · blocks #76 (cost/benefit) + #77 (lead synthesis)
**Status:** design only — read-only. One gated micro-bench proposed at the end; nothing run.
**Source of truth:** `feat/tantivy-search-sidecar` (`bitmagnet-rs/crates/bitmagnet-search`) + the `bench-file-index` crate, both on tantivy **0.26.1** (verified against the vendored crate source under `~/.cargo/registry/.../tantivy-0.26.1`).

---

## 0. The problem, restated precisely

We want a free-text **path** search that responds **per keystroke in `<50 ms`**, CJK-correct, at full corpus scale.

Taken as GIVEN (measured — `cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`, EXP-D/D2/E):

| fact | number |
|---|---|
| per-file char-ngram(2,3) path index, full corpus | **94 GB** (879.5 M docs; ~4.8× the default tokenizer; postings-dominated) |
| CJK substring recall: default tokenizer / ngram | **0.0037 / 1.0000** |
| CJK free-text latency @ full scale (selective) | warm p50 **0.07 ms**, cold p50 0.86 ms, p99 ≈ 80 ms |
| **broadest ASCII grams (5.6 M hits)** | **cold p50 145 ms / p95 320 ms · warm p50 101 ms / p95 244 ms** |
| freshness under live dual-write (LogMergePolicy) | **~2 ms** lag, bounded segments, supersession `delete_term` **11 ms** |
| writer | **single thread + ≥2 GB arena** (multi-thread 256 MB writer **crashes** on ngram token explosion) |

**The tension:** per-keystroke means the user types `a`, `ab`, `abc`, …. Short queries = broadest match-sets = the slow case. The measured broad-ASCII tail (**100–320 ms**) blows the `<50 ms` budget. Everything below is about closing that gap *without* throwing away CJK correctness.

### The decisive structural fact (verified, not assumed)

I traced the question "can Tantivy early-terminate a broad query and return top-k without scanning the whole match-set?" through the 0.26.1 source. **For our query shape, the answer is no.** Three independent confirmations:

1. **Block-max WAND is wired only for score-sorted top-k.** `Weight::for_each_pruning` (the WAND entry point) is invoked from exactly two call sites, both in `src/collector/sort_key/sort_by_score.rs:41` and `:50`. Sorting by a fast field goes through `TopDocs::order_by_fast_field → order_by → TopBySortKeyCollector` (`top_score_collector.rs:299-331`), which **never calls `for_each_pruning`** — it visits every matching doc.
2. **WAND only optimizes disjunctions.** `BooleanWeight::for_each_pruning` (`boolean_weight.rs:516-530`) calls `block_wand(...)` **only** for `SpecializedScorer::TermUnion`; anything else (incl. an intersection) falls to `for_each_pruning_scorer`, which threshold-prunes per-doc but still **iterates the full docset** (no block skipping).
3. **Our path query is a conjunction.** `build_path_query` (bench `main.rs:1019-1039`) and the sidecar's phrase idiom build the ngram query as a `BooleanQuery` of `(Occur::Must, …)` clauses → an **intersection** scorer = `SpecializedScorer::Other`. So even if we sorted by score, WAND would not engage.

And there is no escape hatch at the collector layer: `SegmentCollector::collect(doc, score)` (`collector/mod.rs:302`) returns `()` — **there is no early-abort signal**. A "cap the scan at N docs" collector can stop *buffering* after N, but the scorer still leaps through the entire intersection. A cap saves heap/sort work, **not scan work**.

> **Conclusion that drives the whole design:** the only lever that reduces broad-prefix latency is **reducing the size of the match-set the scorer must enumerate**. Top-k tricks (WAND, capped collectors, `TopDocs`) do **not** help the broad conjunction. Plan accordingly.

---

## 1. The levers, each reasoned against the code

### Lever 1 — min-chars threshold + debounce (client + server) — **PRIMARY, free**

The match-set is a monotone-decreasing function of query length, and the drop is steep because the query is a **conjunction of grams**:

| typed | ngram(2,3) tokens produced (`build_path_query`) | query | selectivity |
|---|---|---|---|
| `a` | *(none — min_gram=2)* → `EmptyQuery` | — | returns nothing (already!) |
| `ab` | `{ab}` | **single** broadest bigram TermQuery | **worst case** (every doc with `ab` anywhere) |
| `abc` | `{ab, bc, abc}` | conjunction of 3 grams | much narrower |
| `abcd` | `{ab, bc, cd, abc, bcd}` | conjunction of 5 grams | selective |

Two facts fall straight out of the tokenizer (`NgramTokenizer`, min=2):
- **1-char queries already return nothing** — the tokenizer emits no gram, `build_path_query` produces zero clauses → `EmptyQuery`. So the broadest possible firing query is **2 chars = one bigram** = the exact 100–320 ms case.
- By **3 chars** the query is a 3-gram conjunction; the `max_gram=3` term enforces contiguity, and the intersection of three common grams is dramatically smaller than any single bigram's postings.

**Design:**
- **Client:** `min-chars = 3`, debounce **120 ms** (a typist at 150 ms/keystroke fires ~once per pause, not once per key). Below 3 chars: show "keep typing…", fire nothing.
- **Server guard:** reject (`INVALID_ARGUMENT` or empty result) any free-text term whose tokenized gram count `< 2`, so a hand-crafted 2-char request can't hammer the broadest bigram. This is the cheapest, highest-leverage lever and it is **purely additive** — no index change.

This alone removes the measured worst case (the single broadest bigram) from the per-keystroke path. It does **not** by itself guarantee `<50 ms` for the broadest *3-char* grams — that is what Levers 2/5 and the micro-bench address.

### Lever 2 — edge-ngram / prefix-anchored tokenizer — **UNMEASURED, conditional**

`NgramTokenizer::new(min, max, prefix_only=true)` exists and is cheap (verified `ngram_tokenizer.rs:163` — `if self.prefix_only && offset_from > 0 { return false }`). The doc table (`ngram_tokenizer.rs:17-22`) shows `hello` → `{he, hel, hell, hello}` — only grams **anchored at offset 0**. Postings shrink hard: a length-L token yields `L-min+1` edge-grams instead of the full window's `~L·(max-min+1)`, and crucially each edge-gram's **document frequency** is far lower than an interior gram's (only docs whose field *starts* with it).

**Two hard limitations, both code-grounded:**
1. **Anchors at offset 0 of the *whole text*, not per word.** Tantivy's `NgramTokenizer` is a single-token-stream tokenizer; `prefix_only` anchors at the start of the *entire* string handed to it. For a path like `Movies/Foo (2021)/foo.s01e01.mkv`, a bare prefix tokenizer would only match queries that are a prefix of `Movies/...`. To get useful typeahead you must **first split into words** (a `SimpleTokenizer`/the bitmagnet tokenizer) and **then** edge-ngram each word — i.e. a *composed* `TextAnalyzer`, or a small custom tokenizer. Tantivy's stock `prefix_only` does **not** do per-word anchoring on its own.
2. **Loses infix + CJK-mid-run.** Edge-ngrams only match word-prefixes. The substring `e01` inside `s01e01` won't match; and a CJK run (no spaces → one "word") edge-ngrams only from its start, re-breaking the exact mid-run CJK case that full ngram fixed (recall 0.0037 → 1.0). So **edge-ngram trades CJK-correctness back away** unless paired with full ngram for the CJK code-point range.

⟹ Edge-ngram is attractive for *ASCII word-prefix typeahead* (far fewer postings → likely `<50 ms` even broad) but is **not** a drop-in for CJK. A viable hybrid: **per-word edge-ngram for ASCII words + full char-ngram only for CJK runs** (route by code-point in a custom tokenizer). Its **size and broad-prefix latency are unmeasured** → this is the core of the proposed micro-bench (§3).

### Lever 3 — top-k early termination (TopDocs / block-max WAND) — **VERIFIED INEFFECTIVE here**

Per §0: WAND fires only for score-sorted **disjunctions**; our query is a fast-field-sorted **conjunction**. `TopDocs::with_limit(k)` does **not** bound the scan for it. Do **not** design around early termination. (If we ever expose a *ranked* free-text mode as a pure OR of grams with `order_by_score`, WAND *would* engage — but OR-of-grams destroys substring precision, so that is not our shape.) This lever is a dead end for the broad-conjunction problem and is documented here so #76/#77 don't reach for it.

### Lever 4 — result caps + ranking + index sort — **bounds heap, not scan; useful with Lever 5**

Because `collect` can't abort (§0), a cap on returned hits does **not** cap the scan. What a cap *does* buy: with **index sort** (`IndexSettings.sort_by_field`, `index/index_meta.rs:214`) by a desirability key (e.g. `seeders DESC`, or `size DESC`, or shortest-path proxy `files_count ASC`), docids are laid out best-first per segment, so a `TopDocs::with_limit(k)` returns the *desirable* k even though it scanned everything. This keeps result quality high once Lever 5 has made the scan itself cheap. Ranking/tie-break for typeahead, in order: **(a) shorter path** (proxy: fewer path tokens / smaller `files_count` at torrent granularity), **(b) more seeders**, **(c) larger size**. Implement as the index sort key + optional `order_by_fast_field(seeders, Desc)` at query time. Index sort also tightens postings locality (better block compression) — a modest size win.

Caveat: index sort makes the writer's merge more expensive and interacts with the single-writer ngram arena pressure (Lever 7); validate merge time in the micro-bench if we adopt it.

### Lever 5 — doc granularity: per-file vs per-torrent path-bag — **the major structural lever**

| | per-file (current bench) | **per-torrent path-bag** |
|---|---|---|
| docs | ~873 M | **~17 M** (one doc/torrent, `path` field = all that torrent's file paths concatenated, multi-valued) |
| a gram's postings length | # **files** containing it | # **torrents** containing it |
| broad-prefix scan cost | the 100–320 ms case | **bounded by torrent-doc-frequency** — plausibly up to ~50× shorter (avg 51.79 files/torrent), since grams cluster within a torrent's filelist |
| index size | 94 GB | **unmeasured — expected materially smaller** (postings dominated by doc-freq) |
| freshness supersession | `delete_term(info_hash)` re-adds N file docs | `delete_term(info_hash)` re-adds **1** doc → cheaper |
| what you lose | — | per-**file** identity in the hit (returns the *torrent*, not which file matched); file-level `ext ∧ size ∧ path` in one query |

For the product question "**find torrents whose files match this text**" (the typeahead use-case from PS-T1), per-torrent is both the **better fit** and **far cheaper**. The thing it gives up — file-level structured filtering (`mkv ∧ >1 GB ∧ path~X`) and "which file matched" — is **already served cheaply by the DuckDB-on-Parquet tier** (`+3.9 GB`, `<250 ms`, ARCH-C). So the clean split is:

- **Inverted index (per-torrent path-bag):** fast *free-text* typeahead → returns ranked **info_hashes**.
- **DuckDB-Parquet:** exact file-level structured queries + hydration of which files matched.

This is the lever most likely to drag the broadest 3-char case under 50 ms while *keeping* CJK-correct ngram. Its size and broad-prefix latency are **the unmeasured numbers** → micro-bench (§3).

### Lever 6 — schema & fields (concrete) — see §2.

### Lever 7 — the incremental writer process — see §2.4.

---

## 2. Concrete design

### 2.1 Doc granularity & index identity

**Per-torrent path-bag.** One Tantivy doc per torrent (`~17 M`). The delete/upsert key is `info_hash` (mirrors the shipped sidecar: `schema.rs:164` indexed+stored bytes; `indexer.rs:165-167` `delete_term(info_hash)`). This makes supersession a single `delete_term` + single re-add (EXP-E measured 11 ms for the per-file variant; per-torrent re-adds 1 doc, strictly cheaper).

### 2.2 Schema (file-grained typeahead index — separate from the torrent-content index)

```text
field          type     flags                         why
------------   ------   ---------------------------   --------------------------------------------
info_hash      bytes    INDEXED | STORED              delete key + the hit identity returned to caller
path_grams     text     ngram(2,3) tokenizer,         the ONLY searchable field. INDEXED-ONLY.
                        IndexRecordOption::WithFreqs   NO positions (all ngrams are position 0 —
                        (NOT WithFreqsAndPositions)    ngram_tokenizer.rs:168 — positions are dead weight)
seeders        u64      FAST                          index-sort key + query-time tie-break ranking
size           u64      FAST                          (optional) file-level range delegated to DuckDB;
                                                       kept FAST only if we want a coarse torrent-max filter
files_count    u64      FAST                          shortest-path proxy for ranking
```

Decisions, each grounded:
- **`path_grams` is `WithFreqs`, not `WithFreqsAndPositions`.** Every ngram carries `position = 0` (`ngram_tokenizer.rs:168`, verified). Positions are therefore constant and useless, and a substring is matched as a **conjunction of grams** (`build_path_query` `main.rs:1020-1039`), never a `PhraseQuery`. Dropping positions removes the `.pos` segment component entirely (EXP-D already noted positions "shrink"; here we eliminate them). **Index-only** — never STORED (storing paths is the 273 GB cost the blob migration removed; `schema.rs:36-38`).
- **No `extension`/`content_type` in *this* index.** They live in the torrent-content index and in DuckDB. Keeping this index single-field-searchable keeps its term dict and postings minimal and keeps the writer arena (Lever 7) for ngrams alone.
- **`info_hash` STORED** so a hit yields the identity directly without a doc-store of paths; the UI hydrates display text from the blob/DuckDB by `info_hash`.
- Combined **`ext ∧ size ∧ free-text`** is **not** answered in one Tantivy query. The typeahead index answers free-text → info_hashes; structured constraints are applied by the **DuckDB-Parquet** tier (or a cheap PG `agg_torrent_ext @>` for the ext facet). This avoids forcing FAST file-level columns into an 873 M→17 M index and keeps each engine on what it's best at.

### 2.3 Read path

```text
1. client: debounce 120 ms; require ≥3 chars; send term.
2. server: tokenize term with the SAME ngram analyzer (runtime-registered, index.rs:44-46);
   reject if <2 grams.
3. query = BooleanQuery[ (Must, TermQuery(gram_i)) for each distinct gram ]   // intersection
4. collect = TopDocs::with_limit(k).order_by_fast_field::<u64>("seeders", Desc)
            (index is sorted by seeders DESC, so top-k is desirable; see Lever 4)
5. return info_hashes (+ seeders/files_count) → UI; structured filters + matched-file
   hydration handled by DuckDB-Parquet keyed on the returned info_hashes.
```

No `Count` on the hot path (the sidecar computes `total_hits` via `Count` — `query.rs:87` — which is a *full* match-set scan; for typeahead we **skip total_hits** or compute it lazily/asynchronously, since it is the same O(match-set) cost the broad case can't afford).

### 2.4 The incremental writer process

- **Single writer, single process** (Tantivy permits exactly one — `index.rs:60-70`). It consumes the **same crawler dual-write stream** the torrent-content index already consumes (`server.rs` upsert path), filtered to file-bearing torrents. One torrent crawl → one `delete_term(info_hash)` + one `add_document` (the path-bag).
- **Writer config:** `writer_with_num_threads(1, ≥2 GiB)`. This is **load-bearing** and measured: the default multi-thread 256 MB writer **crashes** ("index writer killed") on ngram token explosion because each worker's ~32 MB arena starves (`bench-file-index` `RecallArgs` doc-comment `main.rs:181-192`; EXP-D RESULTS §). One thread = one big arena.
- **Merge policy:** default `LogMergePolicy` (no `NoMergePolicy`, no force-merge). EXP-E showed this keeps **bounded segments** (29 → 17–21) and **~2 ms** commit→reload freshness with `ReloadPolicy::OnCommitWithDelay` (sidecar default, `index.rs:53-58`). Per-torrent granularity makes each commit smaller → freshness if anything improves.
- **Supersession:** re-crawl replaces the whole fileset → `delete_term(info_hash)` + re-add the new path-bag (TORRENT-granular, matching the EXP-B/EXP-E supersession semantics). 11 ms upper bound from the per-file EXP-E; cheaper here.
- **Index sort (if adopted, Lever 4):** `IndexSettings { sort_by_field: Some(IndexSortByField { field: "seeders", order: Desc }), .. }`. Validate merge-time impact in the micro-bench.

### 2.5 What hits `<50 ms` and how (the synthesis)

| query class | mechanism | expected |
|---|---|---|
| 1-char | tokenizer emits no gram → `EmptyQuery` | n/a (UI shows "keep typing") |
| 2-char | **blocked** by min-chars=3 (client) + server guard | never fires |
| ≥3-char, **selective** (incl. CJK) | conjunction is small; per-torrent shrinks postings | already `<50 ms` (EXP-D2: CJK sub-ms) |
| ≥3-char, **broad ASCII** (the risk) | **per-torrent path-bag** (Lever 5) shrinks doc-freq up to ~50×; index-sort+top-k returns desirable k | **the number the micro-bench must confirm** |

The chosen path to `<50 ms` is **Lever 1 (min-chars≥3 + debounce) + Lever 5 (per-torrent path-bag) + Lever 4 (index-sort by seeders for quality)**, with structured filters delegated to DuckDB. We do **not** rely on early termination (Lever 3, proven ineffective). Edge-ngram (Lever 2) is held in reserve as the fallback if per-torrent alone doesn't clear the broadest 3-char grams — and only for the ASCII path, never CJK.

---

## 3. The one gated micro-bench to run

**Question it answers (the two unmeasured numbers that decide the design):** for a **per-torrent path-bag ngram(2,3)** index, (a) what is the **index size** vs the 94 GB per-file index, and (b) does the **broadest 3-char prefix** clear **`<50 ms`**?

This is the single highest-leverage measurement: per-torrent granularity is the structural lever, and its size + broad-3-char latency are the only things standing between this design and a go/no-go. Edge-ngram is added as a *cheap second arm* only because the same harness run produces it for near-zero extra cost.

### Method — reuse `bench-file-index`, minimal additions

1. **Add a `--granularity {per-file,per-torrent}` flag** to the `recall`/`pathquery` build path. `per-torrent` groups the `torrent_files` keyset stream by `info_hash` (already ordered by `(info_hash, index)` — `main.rs:389-392`), concatenating each torrent's paths into **one** doc's `path` field (multi-value add, one `add_document` per info_hash). Everything else (single-thread writer, 2 GB arena, `NoMergePolicy` + force-merge-to-1 for clean size attribution) is unchanged.
2. **Add a `--tokenizer edge-ngram` arm** = `NgramTokenizer::new(2, 4, /*prefix_only=*/true)` composed **after** a word-splitter (`SimpleTokenizer` + `LowerCaser`) so it anchors per word, plus a CJK fallback to full ngram for code points `> U+1FFF` (mirror the tokenizer's existing CJK cutoff, `tokenizer.rs:61`). (If the composed tokenizer is non-trivial, run edge-ngram as bare `prefix_only` first and document the per-word caveat.)
3. **Corpus slice:** the existing **50 M `torrent_files` rows** on the HEL1 restore (≈ the same slice EXP-D used → directly comparable). Per-torrent that is ~1 M torrents; report extrapolation to 17 M.
4. **Query set (the crux):** a TSV of **2-, 3-, 4-, and 5-char prefixes** of the most common ASCII path fragments (`108`, `720`, `s01`, `x26`, `mkv`, `the`, `mp4`, …) + the existing 150 CJK mid-run substrings. Tag each row with its char-length group so the report bins latency by prefix length.
5. **Measure** with the existing `pathquery` subcommand (cold-first after `drop_caches` + 15 warm reps): per-(granularity × tokenizer × prefix-length) **avg hits, cold/warm p50/p95/p99**, plus `report_segment_bytes` for **path-field bytes/doc and total index size**, and the **force-merge time** (proxy for merge cost under index-sort).

### Metrics & go/no-go

| metric | go threshold |
|---|---|
| per-torrent index size (extrapolated to 17 M) | materially `< 94 GB`; ideally `≤ ~20 GB` |
| broadest **3-char** prefix, warm p50 | **`< 50 ms`** |
| broadest **3-char** prefix, warm p95 | `< 150 ms` (interactive tail) |
| CJK recall (per-torrent must not regress) | `= 1.0` (sanity: grouping doesn't change tokenization) |
| edge-ngram arm: size & 2-char p50 | informational — decides whether to keep Lever 2 in reserve |

**Gating:** read-only on the existing HEL1 throwaway restore (no prod touch), one serial run, single writer thread + 2 GB arena (the crash-avoidance invariant), guarded by the lock+pgrep orchestration and the tailscale-IP SSH ops notes from the bench-env memory. No new infra. If per-torrent clears the 3-char `<50 ms` bar, the design is GO and the edge-ngram complexity is **not needed**; if it doesn't, the edge-ngram arm's numbers tell us whether ASCII-prefix routing rescues it before we consider the full +90 GB per-file index.

---

## 4. Verified-against-source claim ledger

| claim | verdict | evidence |
|---|---|---|
| Block-max WAND gives early-out for our query | **FALSE** | `for_each_pruning` called only from `sort_by_score.rs:41,50`; WAND only for `TermUnion` (`boolean_weight.rs:516-530`); our query is an intersection (`main.rs:1019-1039`) |
| `order_by_fast_field` prunes the scan | **FALSE** | routes to `TopBySortKeyCollector` (`top_score_collector.rs:299-331`), no `for_each_pruning` |
| A collector can cap/abort the scan | **FALSE** | `SegmentCollector::collect` returns `()` (`collector/mod.rs:302`); cap saves heap, not scan |
| ngram tokens are all position 0 → drop positions | **TRUE** | `ngram_tokenizer.rs:168` `self.token.position = 0` |
| `prefix_only` anchors at whole-text offset 0 (not per word) | **TRUE** | `ngram_tokenizer.rs:17-22` table + `:163` `offset_from > 0 → return false` |
| Multi-thread 256 MB writer crashes on ngram; need 1 thread + ≥2 GB | **TRUE (measured)** | EXP-D RESULTS; `main.rs:181-192` doc-comment |
| Index sort is available in 0.26 | **TRUE** | `IndexSettings`/`IndexSortByField`, `index/index_meta.rs:214` |
| Default merge = bounded segments + ~2 ms freshness | **TRUE (measured)** | EXP-E; sidecar `index.rs:53-58` `OnCommitWithDelay` |
| `total_hits`/`Count` is a full match-set scan | **TRUE** | `query.rs:87`; skip on hot path |

---

## 5. One-paragraph summary (for #76/#77)

Per-keystroke `<50 ms` cannot be bought with Tantivy top-k tricks: I verified in the 0.26.1 source that block-max WAND fires only for score-sorted **disjunctions**, our ngram path query is a fast-field-sorted **conjunction**, and `SegmentCollector` has no abort — so the *only* lever that lowers broad-prefix latency is **shrinking the match-set**. The recommended design therefore stacks three match-set-shrinking moves: **(1) client `min-chars=3` + 120 ms debounce + server gram-count guard** (1-char already returns nothing; 2-char is the measured 100–320 ms worst case and is simply never fired), **(2) per-torrent path-bag granularity** (~17 M docs instead of 873 M → postings shrink with torrent-doc-frequency, plausibly up to ~50×, and the index shrinks well below 94 GB) with structured `ext∧size` and matched-file hydration delegated to the cheap DuckDB-Parquet tier, and **(3) index-sort by `seeders DESC`** so a capped top-k returns *desirable* hits. CJK correctness is preserved (still char-ngram). Edge-ngram is kept only as an ASCII-prefix fallback. **The one micro-bench to run:** build a **per-torrent path-bag ngram(2,3)** index on the existing 50 M-row HEL1 restore and measure **index size + cold/warm latency of the broadest 2/3/4/5-char prefixes** (with an edge-ngram second arm for free) — go iff the broadest 3-char prefix clears `<50 ms` warm p50 and the extrapolated 17 M index is materially under 94 GB.
