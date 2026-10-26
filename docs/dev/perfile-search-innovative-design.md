# Per-File Search — Innovative Design Research

**Date:** 2026-06-07
**Status:** Research / design analysis (read-only). No code changed.
**Question:** Beyond the already-documented options, is there an _innovative_ system/engineering design that restores `torrent_files` parity (true per-file search) at materially lower disk / better completeness than the spec baseline?
**Method:** 6-agent opus team — 4 builders exploring distinct innovations (`collector`, `encoding`, `columnar`, `pgnative`), 1 adversarial `evaluator`, + lead adjudication. Every mechanism source-verified against **Tantivy v0.26** (`/Users/me/aaa/github/tantivy`) and **PostgreSQL 16** (`/Users/me/aaa/github/postgres`).
**Inputs:** [`perfile-search-with-blob-design.md`](./perfile-search-with-blob-design.md) (option matrix), [`file-grained-search-spec.md`](./file-grained-search-spec.md) (the baseline: file-grained Tantivy index + per-(torrent,ext) aggregate + DuckDB-on-blobs).

---

## 0. Executive summary

The deep dive's most important result is a **confirmation, not a new winner**: the per-file `(extension, size)` **conjunction is irreducible** — answering "the `.mkv` _itself_ is > 1 GB" requires either a **per-file record** (the Tantivy file index — cheap, ~8–15 GB) or a **blob-decode recheck** (DuckDB-on-blobs — exact, 0 GB, seconds). Every design that tries to dodge _both_ provably degrades on one of three axes: it goes **approximate** (size bucketing), **loses incremental writes** (static columnar), or **pays a full scan** (no value index / collapse without early-termination). Four independent innovations were pushed hard against the source; none beats the baseline outright.

But the research is not null — it produced **three concrete, adoptable refinements** and **downgraded one documented "impossibility":**

1. **The §13.2 "hard floor" (deep stable pagination of _distinct torrents_) is not an engine limit** — a ~150-LOC custom Tantivy `Collector` achieves it in-engine, exact, for **all** predicates (incl. two-sided ranges + path-FTS). Cost: a query-time match-set scan → gate behind a selectivity cap. _Adopt as an optional collapse-paging path, not v1._
2. **A bucketed-size vector on the per-(torrent,ext) aggregate** closes most of the _two-sided_ distinct-torrent gap at bucket precision, for ~free, on a structure the baseline already recommends. _Adopt as an optional enhancement._
3. **A ~0.3 GB composite-term field on the existing torrent index** gives torrent-grained bucketed `ext+size` _before_ the file index ships. _Optional cheap interim._

Two ideas are **confirmed traps**: the standalone `columnar` store (no delete, unstable internal format, no value index) and any **per-file PG table / PG-recheck structure** (10.5 GB of btree overhead alone; JSONB-GIN drops `>` and always rechecks → "a worse DuckDB").

**Net:** keep the baseline; fold in refinements #1–#3 as optional, clearly-scoped additions. Confidence in the baseline is now high and source-grounded.

---

## 1. The irreducibility result (why this is the framing)

A per-file predicate `extension = X AND size ⋈ T` is a **conjunction over the same file**. Three structural facts, each source-verified, bound every solution:

- **Tantivy has no nested documents.** A torrent doc holding `file_extensions[]` + `size[]` matches `ext:mkv AND size:<1MB` even when the only mkv is 9 GB (`tantivy/src/query/boolean_query/boolean_query.rs:421-447`). So the pairing must be carried by a **single indexed unit** — either a per-file doc, or a single token that fuses both dimensions.
- **PostgreSQL has no index-organized table.** Any per-file row pays a 28 B heap tuple (`htup_details.h:185,602,617`) + 12 B per btree entry (`itup.h:35-51` + line pointer). At 873 M files that is **~10.5 GB of btree overhead before any payload**, atop a ~58 GB heap. Per-file granularity in PG cannot be small.
- **The data already exists, opaque, in the blob.** `files_data` keeps `{index, path, extension, size}` per file (`bitmagnet-model/src/blob.rs:31-46`); decoding is `zstd`+`msgpack` (`blob.rs:60`). So an exact answer is _always_ recoverable by decode — the only question is latency.

Therefore the exact-answer corners are: **(A) a per-file record** (carry the pairing physically) or **(B) a blob recheck** (recompute it on demand). The baseline takes both corners (Tantivy file index = A at 8–15 GB; DuckDB-on-blobs = B at 0 GB). Everything below is an attempt to find a _third_ corner — and each forfeits exactness, writes, or latency to do so.

---

## 2. Innovation analyses (source-verified)

### 2.1 T1 — Custom Tantivy `Collector` for distinct-torrent collapse

**Idea:** the documented floor (no deep stable pagination of distinct torrents over a file-grained index) is a property of tantivy's _built-in_ collectors, not the engine. A custom `Collector`/`SegmentCollector` (`src/collector/mod.rs:141,296`) can collapse files → torrents in-engine.

**Mechanism (verified):** collapse key = `max(matching file size)` per torrent, which is non-monotonic in doc-iteration order → a streaming top-N-of-groups is invalid → **two-phase is forced**. Per segment: `HashMap<info_hash_term_ord → maxSize>` (term ords are lexicographic, `dictionary_encoded.rs:14-17`, so within-segment they sort as bytes); `harvest()` emits top `(offset+limit)` resolving ord→bytes for survivors only; `merge_fruits()` regroups on info_hash **bytes** (ords are segment-local), selects by `(size desc, info_hash asc)` → total order → deterministic stable deep paging (same proven mechanic as `merge_top_k`, `sort_key_top_collector.rs:76-95`). Works for **all predicates**, incl. two-sided ranges and path-FTS, which the §13.2 aggregate provably cannot.

**The decisive cost (both T1 and the evaluator agree):** collapse **forfeits TopDocs' score-threshold early termination** (`top_score_collector.rs:625-633`) — a low-score/late doc may be the first member of a new distinct torrent — so it must `collect()` **every matching doc**: O(match-set), page-depth-independent. Selective `.mkv>1GB` → tiny, fast, exact ✅. Broad `mp4`, no size bound → hundreds of millions of docs + a multi-hundred-MB group map ⚠️.

**Adjudication:** the optimistic ("closes the floor") and adversarial ("trap: full scan + couples to 0.26 internals") views are both _mechanically correct_; they differ only on framing. Verdict: **feasible and genuinely in-engine, but only valuable for an _optional_ collapse-paging view.** The v1 default is file-level results (`collapse=false`), which never needs it. So: **adopt as a gated option (selectivity cap → exact collapse; over cap → §13.2 aggregate/HLL), not v1 work.** Documentation impact: §13.2's "impossible in Tantivy 0.26" → "achievable via a custom collector, gated by a selectivity cap."

### 2.2 T2 — Composite-term encoding `(ext | size-bucket)` on one doc/row

**Idea:** per file, emit a single token fusing extension and a log-scale size bucket (`mkv|b30`, `k = floor(log₂ size)`). Because the token is **atomic to one file**, it pairs ext∧size correctly — dodging the nested-doc trap — on a **one-doc-per-torrent** structure (the _existing_ torrent index, or a PG `text[]`). Single-bucket (OR over buckets ≥ k(T) at query) beats threshold-token (emit-all-≤, ~3–5× larger and breaks two-sided).

**Sizing (verified):** multivalued STRING field on the existing torrent doc ≈ **+0.2–0.5 GB** (mechanism already proven by `file_extensions`, `bitmagnet-search/src/schema.rs`); per-torrent distinct tokens ≈ ext-count × ~1–3 buckets, **bounded by extensions, not files**. PG `text[]`+GIN ≈ 2–4 GB (`ginpostinglist.c:22-84`).

**The boundary (where optimism and adversary meet):** the torrent doc still holds a **bag** of tokens. So it delivers torrent-grained, **bucket-precision** answers — exact for one-sided _grid-aligned_ thresholds and two-sided _interior_ buckets; off-grid / boundary buckets need a blob-refine; and it **cannot** give file-level `total_hits`, exact size-sort, or per-file hits. The evaluator's "approximate re-skin of P0" is too harsh (P0's `largest_file_size` can't pair ext+size _at all_; this can, at bucket precision) but its substance holds: it sits _between_ P0 and the file index, and **DuckDB already gives exact at 0 GB**.

**Adjudication:** the bucket _kernel_ is real and cheap; its best home is **not** a standalone scheme but **relocated onto the per-(torrent,ext) aggregate** (§2.4 / refinement #2), where the structure is already torrent-grained-by-ext (no bag problem). As a **standalone ~0.3 GB field on the existing torrent index**, it is a legitimate **cheap interim** to get torrent-grained bucketed ext+size search _before_ the file index ships — but it is redundant once the file index exists. **Adopt the kernel into the aggregate; offer the 0.3 GB field only as an interim.**

### 2.3 T3 — `tantivy-columnar` as a standalone per-file column store

**Idea:** skip the inverted index; use tantivy's standalone `columnar` crate to store `{torrent_id, ext, size}` for 873 M files (~5.5–8 GB; single-valued ⇒ `ColumnIndex::Full` = zero index bytes), and scan.

**Why it fails (T3 author and evaluator agree):**

- **No value index** → `size>T` is an O(num_rows) full column scan (`column_values/mod.rs`), ~1–5 s. The only escape is to **sort rows by `(ext,size)` at build** and binary-search `get_val` (<1 ms) — but that is single-axis (only that one query is fast) and makes torrent_id random.
- **Immutable / write-once, no delete or update** — per-torrent re-crawl (the live D4 upsert) is impossible without rewriting the segment; `merge_columnar` + alive-bitsets is internal machinery.
- **Dense positional RowId renumbered on every merge** → no stable external key; **on-disk format already broke V1→V2**; the crate documents itself as a tantivy-internal building block.

**Adjudication:** strictly dominated — DuckDB/Parquet beats it for scan-analytics (real SQL, 0 GB, stable format) and the file index beats it for interactive search _and_ live writes. **Reject (hard trap).** The only salvage (sorted single-axis <1 ms fast-path) is a worse, less-maintainable version of the per-(torrent,ext) aggregate.

### 2.4 T4 — PG-native (JSONB/GIN, BRIN, covering) + bloom/blob-refine

**Negative results (verified):** **BRIN** on `size` prunes nothing — crawl-order inserts put a 4 KB `.nfo` and a 2 GB `.mkv` in every page range → minmax `[~0, huge]` matches everything (`brin` requires heap/value correlation). **contrib/bloom** is equality-only (`bloom.sgml:37-38`) → `size>T` impossible. **JSONB GIN** drops the `>` operator (`jsonb_gin.c:713-714` `jpiGreater → NULL`) and **always sets `*recheck=true`** (`:944-999`) → cannot tell whether the matched ext and matched size are the _same_ element → every candidate must **decompress+decode the BYTEA blob in the backend = a worse DuckDB.**

**Positive results:**

- **Smallest PG store = the per-(torrent,ext) min/max aggregate** (the baseline's §13.2 structure), surrogate-keyed (int4 torrent_id + int2 ext_id) + covering btree ≈ **2–3 GB** — exact one-sided distinct-torrent answers <50 ms with trivial deep paging; ~30× smaller than the slim table. Confirms and sizes the baseline.
- **Near-zero exact _tail_ pipeline:** GIN-on-ext (~0.3 GB) + per-torrent **bloom of `(ext|log₂-size)` tokens** (~0.2 GB) prunes ~85–95%, then blob-refine survivors → two-sided/per-file exact in ~0.5–1 s (beats naive blob-scan because pre-pruned). A legitimate **optional** 0.5 GB tail, but more moving parts than DuckDB-on-blobs for the same job.

**Adjudication:** **reject per-file PG tables and PG-recheck structures as primary** (evaluator's "worse DuckDB" holds for the recheck path). **Keep the aggregate** (it _is_ the baseline). The bloom+GIN tail is optional and only worthwhile if a PG-only deployment wants sub-second two-sided without DuckDB.

---

## 3. Scoring (lead-adjudicated)

Builder optimism reconciled with adversarial review; 1–5, Complexity inverted (5 = low burden).

| Proposal                                       | Feasible | Impact | Complexity | Parity                          | Verdict                             |
| ---------------------------------------------- | -------- | ------ | ---------- | ------------------------------- | ----------------------------------- |
| **Baseline** (file index + aggregate + DuckDB) | 5        | 5      | 3          | 4.5                             | **Ship**                            |
| T1 custom collapse Collector                   | 4        | 3      | 2          | 4 (all-predicate collapse)      | **Adopt as gated option, not v1**   |
| T2 bucket kernel → on the aggregate            | 4        | 3      | 4          | 3 (two-sided, bucket-precision) | **Adopt as optional enhancement**   |
| T2 composite field on existing torrent index   | 4        | 2      | 4          | 2 (torrent-grained interim)     | **Optional interim only**           |
| T4 per-(torrent,ext) aggregate                 | 5        | 4      | 4          | 3.5 (one-sided exact)           | **Already in baseline**             |
| T4 GIN+bloom+blob-refine tail                  | 3        | 2      | 2          | 4 (exact, slow)                 | **Optional, PG-only deployments**   |
| T2 composite as a _standalone_ scheme          | 4        | 1      | 3          | 2                               | **Skip — DuckDB dominates at 0 GB** |
| T3 columnar standalone store                   | 2        | 1      | 1          | 1                               | **Reject — hard trap**              |
| Slim per-file PG table                         | 5        | 5      | 2          | 5                               | **Reject — +68–92 GB re-bloat**     |

---

## 4. Reconciling optimism vs. the adversarial review

The four builders each reported their idea as a win; the evaluator called all four traps. Both were _mechanically right_ — the disagreement is **scope of claim**, and the lead adjudication resolves it:

- **T1:** "closes the floor" (true, in-engine) vs "trap: full scan" (true, broad queries). → _Real, but only for an optional collapse view; gate it._
- **T2:** "killer 0.3 GB, exact" (true: torrent-grained, bucketed) vs "approximate re-skin" (true: not file-level, not continuous). → _Kernel is real; relocate it onto the aggregate; standalone niche is occupied by DuckDB._
- **T3:** "viable, <1 ms sorted" (true for one frozen query) vs "hard trap" (true: no delete, unstable format). → _Reject; the live write-path and format stability matter more than 1 ms on one query._
- **T4:** "smallest PG + near-zero tail" (true: the aggregate; the bloom pipeline) vs "worse DuckDB" (true: the _recheck_ path). → _Keep the aggregate (=baseline); tail is optional._

The unifying lesson: **a single innovation that claims to replace the baseline is almost always over-claiming one corner of the (exact, cheap, writable, fast, all-predicate) pentagon.** The baseline already occupies two corners deliberately; the worthwhile research output is _targeted refinements_, not a silver bullet.

---

## 5. Recommendations

1. **Keep the spec baseline** unchanged as the core: file-grained Tantivy index (exact, file-level, interactive) + per-(torrent,ext) aggregate (exact one-sided distinct-torrent) + DuckDB-on-blobs (exact analytics, 0 GB). The research **validates** it.
2. **Refinement #1 — custom collapse Collector (optional, post-v1).** Implement `DistinctTorrentCollector` as the "exact collapse" path gated by a match-set cap; over cap → aggregate/HLL. Downgrade §13.2's "hard floor" wording. Closes deep distinct-torrent pagination for two-sided + path-FTS.
3. **Refinement #2 — bucketed-size vector on the aggregate (optional).** Add a small per-(torrent,ext) log-bucket count vector (~16 buckets) so two-sided distinct-torrent queries are exact at bucket precision with btree deep-paging, on a structure already planned (~+0.5–1 GB).
4. **Refinement #3 — composite-term field on the existing torrent index (optional interim).** ~0.3 GB to get torrent-grained bucketed ext+size _before_ the file index ships; retire once the file index is live.
5. **Reject:** standalone `columnar` store (T3), per-file PG table, and PG JSONB/BRIN/bloom as a _primary_ exact path (the bloom+GIN+blob-refine tail is optional for PG-only deployments).

**Spec edits to make** (follow-up): in `file-grained-search-spec.md`, soften §13.2's "hard floor" to "achievable via a gated custom collector"; add the bucket-vector and collector as optional items under §11 phasing; cross-link this doc.

---

## 6. Source references

- **Tantivy collectors / collapse:** `src/collector/mod.rs:141,296`, `top_score_collector.rs:512,625-633`, `sort_key_top_collector.rs:76-95`, `columnar/src/column/dictionary_encoded.rs:14-17,44,53`.
- **Tantivy no-nested-docs / fields:** `src/query/boolean_query/boolean_query.rs:421-447`, `src/schema/json_object_options.rs`, `bitmagnet-search/src/schema.rs`.
- **Tantivy columnar (standalone):** `columnar/src/{lib.rs,columnar/writer/mod.rs:50,151,208,249,reader/mod.rs:95,178,column_index/mod.rs:55,77,column_values/mod.rs}`, `merge/mod.rs:76`.
- **PostgreSQL storage / index internals:** `src/include/access/htup_details.h:185,602,617`, `itup.h:35-51`, `nbtree.h`; `src/backend/access/gin/ginpostinglist.c:22-84`; `src/backend/utils/adt/jsonb_gin.c:713-714,944-999`; `src/backend/access/brin/*`; `doc/src/sgml/bloom.sgml:37-38`.
- **Blob = exact source:** `bitmagnet-model/src/blob.rs:31-46,60`.
- **Baseline & numbers:** `docs/dev/file-grained-search-spec.md` (§11, §13), `docs/dev/perfile-search-with-blob-design.md`, `docs/space-savings-verification.md`.
