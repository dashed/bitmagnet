# PSX campaign — RESULTS (D1–D4 synthesis)

**Date:** 2026-06-09 · **Status:** 🟡 SKELETON — design verdicts settled from the four specs; measured tables AWAITING runner logs in [`psx-logs/`](./psx-logs/). Analyst (`psx-analyst`) is **LOCAL-only** — never SSHes to HEL1; fills tables as `psx_*.log` land.
**Env:** HEL1 throwaway bench (879.5 M-row **pre-blob-backfill** restore, `torrent_files` source; bench-pg NodePort DSN `…@127.0.0.1:30654/bitmagnet`; userspace rust/uv). **Production FSN1 untouched.** ONE serial run, ONE ssh connection (the runner owns it).
**Specs:** [`psx-D1`](./psx-D1-blob-parquet-gap-spec.md) · [`psx-D2`](./psx-D2-l3-prod-confirmation-spec.md) · [`psx-D3`](./psx-D3-agg-extmaxsize-spec.md) · [`psx-D4`](./psx-D4-find2-ftswall-spec.md)
**Baselines extended:** [`pathsearch-microbench-RESULTS.md`](./pathsearch-microbench-RESULTS.md) (PS-MB1, the L3 GO) · [`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md) (ARCH-C DuckDB) · [`fba1-jsonb-dropgate-results.md`](./fba1-jsonb-dropgate-results.md) (FB-A1 JSONB) · [`cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`](./cjk-tokenizer-and-incremental-merge-bench-RESULTS.md) (EXP-D/D2/E).

---

## 0. Headline — one-line verdict per thread

| Thread | Question | Verdict | Status |
|---|---|---|---|
| **D1** blob→Parquet | Does the real **decode→ext→Parquet** pipeline over actual blob bytes match the `torrent_files`-sourced Parquet, and at what throughput? | _(awaiting run)_ — **expected: 0 blob errors, exact parity, 0.6–0.94 µs/file confirmed** | ⏳ MEASURE |
| **D2** L3 prod-shape | Confirm `WithFreqs` **13.54 GiB** + recall **1.0** + the **REAL** prod-shape latency (TopDocs-by-seeders), and settle the broad p95 tail | _(awaiting A build/B5/B6)_ — **(B) source-proven ENGINE-IRREDUCIBLE → UX, settled now;** size/recall/latency to confirm | ⏳ MEASURE (B settled) |
| **D3** `agg ext∧max_size` | Is a PG `agg_torrent_ext` rollup worth its disk vs DuckDB? | 🟥 **RETIRE** — route ext∧max_size to DuckDB (5–132 ms, **+0 PG disk**); corrected agg sizing ≈ **10 GB**; **no run** | ✅ SETTLED (design-only) |
| **D4** FIND-2 FTS wall | The broad-common-term `ORDER BY ts_rank_cd` wall — fix? | **DEFER RUM** (write-amp dealbreaker + 30–50 GB + semantics change); **code-only bounded-candidate ranking** is the lever; characterise-then-decide | ✅ REC SETTLED · ⏳ EXPLAIN optional |

> **Cross-cutting standing constraint (unchanged):** the `torrent_files` **DROP stays deferred** until every replacement layer is proven in prod. None of D1–D4 touches that sequencing. D1/D2 are bench-only; D3 is design-only; D4 is pre-existing + DROP-independent.

---

## D1 — End-to-end blob → Parquet on REAL blobs (closes the L2 measurement gap)

**The gap:** every prior L2/DuckDB/file-index number was sourced from `torrent_files`, never from the production blob (the bench restore is the pre-backfill dump → `files_data`/`torrent_file_summary` EMPTY). D1 re-encodes `files_data` **on the bench** with the exact production encoder (`blobmigration.SerializeFiles`), then runs the real `decode→ext→Parquet` path and proves parity against the `torrent_files`-sourced Parquet — **no prod blob reads, ever**.

**Why bench re-encode is faithful (code-verified):** prod format = `zstd_L3(msgpack_named_array[{i,p,e,s}])`; Go⇄Go, Rust⇄Rust, and cross-language byte-identical inner-msgpack round-trips are all proven (`serializer_test.go`, `blob.rs` tests, `blob_fixture.rs`). Bench-encoded blob is indistinguishable from prod for every downstream consumer. Backfill encodes **all** `torrent_files` rows (no cap) → decoded fileset === `torrent_files` for that hash → exact Stage-3 parity.

### D1 gates flagged to lead

| Gate | Question | Result |
|---|---|---|
| **G0** | post-backfill dump exists? (zero-encode fast path) | ⏳ _(Stage-0 probe — known only the **pre-backfill** dump today → default = Stage-1 re-encode)_ |
| **G1** | Go toolchain on HEL1? (else Rust-encode fallback, zstd-frame caveat) | ⏳ |
| **G2** | ≥ ~50 GB free disk before Stage 1 | ⏳ |
| **G3** | encode smoke (`--limit 100000`) throughput acceptable | ⏳ |
| **G4** | lead GO + bench-up (pre RUN-6 teardown) | ⏳ |

### D1.1 — Encode path (Stage 1): importer encode µs/file

| Metric | Smoke (100k) | Full (16.97M) | Notes |
|---|---|---|---|
| torrents encoded | ⏳ | ⏳ | |
| files encoded | ⏳ | ⏳ | full ≈ 856.79 M |
| **encode µs/file** (pure `SerializeFiles`) | ⏳ | ⏳ | the importer encode-path number |
| write throughput (t/s) | ⏳ | ⏳ | PG-write-bound |
| `files_data` bytes written | ⏳ | ⏳ | est. ~16 GB |
| encoder used | ⏳ | ⏳ | Go primary / Rust fallback (flag zstd-frame caveat) |

**Context check:** compare encode µs/file vs the live `persist.go` hot path (~1–1.5 ms/torrent @ ≤100 files). _(fill from log)_

### D1.2 — REAL blob → Parquet (Stage 2): end-to-end throughput

| Metric | slim | full (with path) | Notes |
|---|---|---|---|
| torrents | ⏳ | ⏳ | ≈ 16.97 M |
| file-rows | ⏳ | ⏳ | ≈ 856.79 M |
| **blob errors** | ⏳ | ⏳ | 🚨 **MUST be 0** (non-zero = encoder/format bug) |
| wall (s) | ⏳ | ⏳ | |
| torrents/s | ⏳ | ⏳ | |
| **M files/s** → **ns/file end-to-end** | ⏳ | ⏳ | confirm/refute 0.6–0.94 µs/file |
| Parquet size | ⏳ (~3.9 GB) | ⏳ (~11.7 GB) | |

**Decode-only isolation (Stage 4):** `--from-hex` smoke over a 1M-torrent PSV separates decode-only cost from PG-read+Parquet-write overhead. decode-only µs/file = ⏳. _(Determines whether 0.6–0.94 µs/file was decode-only or end-to-end.)_

### D1.3 — PARITY: Parquet-from-blobs == Parquet-from-torrent_files (Stage 3)

| Check | blob-sourced | tf-sourced | Match? |
|---|---|---|---|
| slim row count | ⏳ | ⏳ (≈856.79 M) | ⏳ |
| slim content md5 `(info_hash,file_index,extension,size)` ordered | ⏳ | ⏳ | ⏳ |
| full content md5 (+ `path`) | ⏳ | ⏳ | ⏳ |
| ANTI-JOIN residual (if hashes differ) | ⏳ | — | expect 0 rows |

### D1 success criteria

1. **Decode integrity** — 0 blob errors across ~16.97 M torrents → ⏳
2. **Parity** — `blob_rows == tf_rows` AND content-hash identical (slim + full incl. `path`); any delta attributable only to documented cap semantics → ⏳
3. **Throughput** — end-to-end µs/file reported; PASS if at/near 0.6–0.94 µs/file (decode-only) → ⏳
4. **Format fidelity** — bench blob is valid zstd (`28 b5 2f fd`) decoding to exact `{i,p,e,s}` → ⏳ (implied by #1+#2)
5. **Encode path timed** — importer encode µs/file reported + contextualised → ⏳

> **D1 verdict (provisional, pending logs):** _Expected_ to close the measurement gap — confirm the prod blob decodes cleanly at scale, the Parquet is byte-for-byte the `torrent_files`-sourced artifact all prior conclusions rest on, and the 0.6–0.94 µs/file figure holds end-to-end. **Anomaly flags to watch:** any blob errors > 0; any parity delta beyond cap semantics; throughput materially off 0.6–0.94 µs/file.

---

## D2 — L3 production-shape confirmation + p95-tail attack

L3 (per-torrent ngram free-text path index) is a **GO** (user decision). PS-MB1 measured the GO on the *as-built* shape and *projected* the prod shape. D2 closes three residual gaps: **(A)** build the real `WithFreqs` artifact (size was computed by `.pos` subtraction, never built); **(B)** attack the broad-query p95 tail and decide reducible-vs-UX; **(C)** confirm per-torrent freshness/supersession.

### D2(A) — `WithFreqs` production-shape build

> **Source-settled before any run:** `WithFreqs` drops only the `.pos` segment files; `.idx` (postings) + `.term` (term dict) are **byte-identical** to the positions-on build (`IndexRecordOption::has_freq` true for both; only `has_positions` differs). So the build lands at PS-MB1's **13.54 GiB ± a sliver**, recall **1.0000** (conjunction never reads positions), same p50/p95/p99. **(A) is a confirmation** that removes the "computed-not-built" caveat and yields a deployable artifact.

| Gate | Expectation | PASS rule | Result |
|---|---|---|---|
| **G5-A size** | TOTAL ≈ **13.54 GiB**, positions component == 0 | within ±3 % AND `.pos` == 0 | ⏳ |
| build sanity | docs == **16,973,470**, 1 segment, ingest ≈ 60–100 min | docs within 0.1 %, segments == 1 | ⏳ |
| postings invariant | `.idx` ≈ **827.8 B/doc**, term dict ≈ **15.7 B/doc** (A2 values, unchanged by dropping positions) | both within ±2 % of A2 | ⏳ |
| recall (separate WITH-truth run) | ngram recall == **1.0000** every group | byte-identical to positions-on | ⏳ |
| latency (prod shape, informational) | `ascii3` warm p50 ≈ 24.7 ms, `cjk3` ≈ 0.2 ms, `ascii3` p95 ≈ 58–60 ms | divergence > 15 % flags a build/measure problem | ⏳ |

**Crate change (drafted, `cargo check` green):** a `--no-positions` flag on `recall` flipping the path field `WithFreqsAndPositions`→`WithFreqs` (3 edits: `schema.rs`, `RecallArgs`, `run_recall`; guard rejects `--no-positions` with `default`/`lindera` tokenizers — those use `PhraseQuery` which needs positions). `pathquery` needs no change.

### D2(B) — broad-query p95 tail: **ENGINE-IRREDUCIBLE → UX** (source-proven, settled now)

> 🔑 **Reframing the measured number:** PS-MB1's p95 was measured with the **`Count`** collector — the *cheapest possible* read. Production needs `Count` (totalCount) **plus** `TopDocs::order_by_fast_field(seeders)` (the page), and the TopDocs path *also* full-scans (proven below) + adds heap work. **So PS-MB1's 24.71 ms p50 / 58.6 ms p95 is a LOWER BOUND on production latency, not an estimate of it.** You cannot even match the floor by adding a sort, let alone beat it. **The REAL prod-shape number (TopDocs-by-seeders) ≥ the Count lower bound** — D2(A)'s latency pass measures it directly.

Candidate p95 mitigations — verdict from tantivy 0.26.1 source (`(F)` citations in spec):

| # | candidate | source verdict | runnable? |
|---|---|---|---|
| B1 | min-chars 4/5 | **already worse** (A2 ascii4/5 p95 55.8/64.4 — longer substrings keep huge match-sets; `ascii3` *is* the floor) | yes (TSV only) → ⏳ |
| B2 | rarest-gram-first conjunction | **no-op** — `intersect_scorers` already `sort_by_key(cost())` (`intersection.rs:31`); intersection already rarest-driven | n/a |
| B3 | stop-gram (drop commonest bigram) | likely no-op + lossy (commonest gram is the cheap non-driver; dropping a `Must` → false positives) | optional (not built) |
| B4 | index-sort seeders + capped TopDocs | **double dead-end** — index-sort *removed* from tantivy (#2434, `CHANGELOG.md:98`); `order_by_fast_field` requires_scoring==false → full-scan, no early-term; Block-WAND only fires for BM25-score on unions | **not runnable** (engine lacks feature) |
| B5 | **realistic multi-word selectivity** | the real reducer — multi-word → larger conjunction → tiny intersection | yes (TSV) → ⏳ |
| B6 | `WithFreqs` vs `WithFreqsAndPositions` query delta | query reads only `.idx`; delta = page-cache pressure (82 GB vs 14 GB), not algorithmic | yes → ⏳ |

**B5 — realistic-query selectivity (load-bearing): what fraction of realistic queries breach 50 ms?**

| query | match-set | warm p50 | warm p95 | < 50 ms? |
|---|---|---|---|---|
| `1080p` | ⏳ | ⏳ | ⏳ | ⏳ |
| `x264` | ⏳ | ⏳ | ⏳ | ⏳ |
| `1080p bluray` | ⏳ | ⏳ | ⏳ | ⏳ |
| `x264 1080p` | ⏳ | ⏳ | ⏳ | ⏳ |
| `2160p x265 hdr` | ⏳ | ⏳ | ⏳ | ⏳ |
| `s01e01` | ⏳ | ⏳ | ⏳ | ⏳ |
| `<CJK title fragment>` | ⏳ | ⏳ | ⏳ | ⏳ |

**B6 — positions on/off query delta:**

| | warm p50 | warm p95 | cold p95 | resident |
|---|---|---|---|---|
| `idx_pt_ngram_full` (WithFreqsAndPositions) | ⏳ | ⏳ | ⏳ | ~82 GB |
| `idx_pt_ngram_full_nopos` (WithFreqs) | ⏳ | ⏳ | ⏳ | ~14 GB |

Hypothesis: warm ≈ equal (postings byte-identical); **cold p95 better for nopos** (14 GB resident vs 82 GB to fault in) → `WithFreqs` is a latency-neutral-or-better win on top of the 83 % size cut.

### D2(C) — per-torrent freshness sanity

Per-torrent path-bag is **strictly cheaper** than the already-validated EXP-E per-file numbers: supersession = `delete_term(info_hash)` + re-add **one** path-bag doc (vs ~52 file docs), torrent-granular = EXP-B's anti-join analog; LogMergePolicy caps at ~17 M docs (vs 879 M). EXP-E measured per-file supersession **11 ms**, fresh-lag ~2 ms flat → per-torrent ≤ that. **Reasoning-confirmed sanity; optional `freshness --granularity per-torrent` extension is LOW priority** (measure only on explicit go). Measured: ⏳ _(if run)_.

### D2 deliverable verdict

> **(A)** Build the full-corpus `WithFreqs` per-torrent index → confirm **13.54 GiB / recall 1.0000 / prod-shape latency**. Yields a deployable artifact. _(pending logs)_
> **(B) SETTLED:** broad-query p95 is **source-proven engine-irreducible** in tantivy 0.26.1 — every engine lever is already-done (B2), unsupported (B4), or can't shrink a full-match-set conjunction count (B1/B3). The only reducers are **query selectivity** (B5, measured) + **client UX** (debounce, client min-chars, loading state, result caps). The measured p95 is a **lower bound** on prod (TopDocs-by-seeders ≥ Count). **Verdict: irreducible at engine, solved at UX — ship with UX guards.** If even realistic multi-word queries tail > 50 ms → fall back to **search-on-submit** (~25 ms median, not promised per-keystroke).
> **(C)** Per-torrent freshness inherits + beats the validated EXP-E numbers (1 doc/torrent supersession). **Anomaly flags:** recall < 1.0000; size > 13.54 GiB +3 %; prod-shape latency *far* above the Count lower bound (a tail materially worse than ~58–60 ms p95 on `ascii3` would flag a build/measure problem).

---

## D3 — `agg_torrent_ext` for ext∧max_size: **RETIRE** (design-only, no run)

**The one question:** is a PG `agg_torrent_ext` rollup worth its disk to serve the torrent-grain **ext∧max_size** query ("torrents with an `.mkv` > 1 GB"), or should that route to the DuckDB-Parquet tier (per-file size already there, +0 PG disk)?

**Context:** `torrents.file_extensions` JSONB already won the plain-ext DROP gate (FB-A1: +119 MB vs agg's +9.5 GB). agg's *only* remaining justification is ext∧max_size (JSONB carries no size). This is the last decision keeping agg alive.

### D3 — backend contest (no run; sizing + latency cited from FB-A1/ARCH-C/RUN-3)

| backend | size source | new PG disk | composes into PG text search? | ext∧max_size latency |
|---|---|---|---|---|
| **A** `EXISTS torrent_files … size > N` | the table **being dropped** | 0 (but it's the 261 GB we remove) | ✅ native (parity truth) | (baseline) |
| **B** `EXISTS agg_torrent_ext … max_size > N` | rollup `max(size)` per (torrent,ext) | **+~10 GB** (corrected, see below) | ✅ native correlated EXISTS | (PG — design-only, not run) |
| **D** DuckDB-Parquet | per-file `size` (already there) | **0** | ❌ cross-engine (hand info_hash set to PG) | **5–132 ms** (ARCH-C) |

**Corrected agg sizing (the spec's tightening):** the *usable* shape (both query directions) needs PK `(info_hash, extension)` **+** `(extension, max_size) INCLUDE (info_hash)` covering index → **~10 GB natural** (heap 3.5 + PK 3.0 + covering 3.5) / ~5 GB surrogate+dims. This **supersedes** FB-A1's +9.5 GB (used the *wrong* `(extension, info_hash)` index) and RUN-3's +3–5 GB (no/wrong index). `agg ext∧max_size` Parquet rollup is **1.39 GB out-of-PG**, collapses **5.2 ms** (ARCH-C).

**DuckDB already serves it (ARCH-C, same 879.5 M-row restore, 24 cores):** `files_slim` sorted(ext,size) collapse **132 ms** / exact count **17 ms**; out-of-PG agg Parquet rollup **5.2 ms**. Ground truth: mkv>1 GB = 5,699,629 files / **1,723,793 distinct torrents**; movie∧mkv>1 GB = 728,574.

### D3 verdict — **RETIRE `agg_torrent_ext`**

> **🟥 RETIRE (default).** ext∧max_size has **zero Go surface today** (`grep max_size` → only the log rotator) — it is uncommitted. DuckDB answers it at **5–132 ms with +0 PG disk** on the side we're *adding* capacity. The DROP project's whole point is shedding PG disk (−245 GB); re-adding ~10 GB into the DB we're shrinking, **plus** a dual-write delta pipeline **plus** a parity checker, for one hypothetical query is a direct regression. agg wins in **exactly one uncommitted scenario**: ext∧max_size must be a **correlated filter composed inside the main PG text search**, with qualifying sets too large for a DuckDB→PG info_hash handoff. **Recommend:** keep agg **DEFERRED/unbuilt, retired from the active plan**; route ext∧max_size to DuckDB (standalone discovery: sorted slim 132 ms / +1.4 GB out-of-PG rollup 5.2 ms). **Re-open ONLY** on a hard committed product requirement for the correlated-PG-filter scenario — and then build the §5.1 minimal **natural-key, max-only, PK + `(extension, max_size) INCLUDE (info_hash)`** shape (~10 GB), *not* the surrogate, *not* count/min, *not* the `(extension, info_hash)` index. **No run was performed; the verdict is design-grounded on cited measurements.**

---

## D4 — FIND-2: main-search broad-common-term ranked FTS wall

**The wall (source-confirmed, not assumed):** the WebUI's **default** sort for any keyword search is `relevance` DESC → Go compiles to `ORDER BY ts_rank_cd(torrent_contents.tsv, $q::tsquery) DESC`. So `ts_rank_cd` **is on the default hot path** — every keyword search is ranked unless the user picks another sort. There is **no ordering index for `ts_rank_cd`**, so PG must compute the rank for *every* matching row before top-N. For a broad common term (`x264` ≈ 4.28 M matches) the GIN match is cheap (~482 ms) but ranking the whole match-set is the **~49 s single-core wall**.

**Why the existing CTE optimisation doesn't help:** `shouldTryCteStrategy()` (`query.go:812-826`) returns **false** for single `query_string_rank DESC` ordering — the CTE 50k stopping-point plan is *never even started* for the relevance default; only the full-match-set ranking plan runs. The CTE bound only covers query + seeders/published_at/size sorts.

**DROP-independent:** touches only `torrent_contents` (+ joins to `torrents`/`content`), never `torrent_files`. Pre-existing, orthogonal to the migration.

### D4 — EXPLAIN characterisation (read-only; optional bench run)

The plan shape (planner reasoning, postgres-performance lens): a **Bitmap Index Scan** on the composite `gin(content_type, tsv)` (the ~14 GB index) → **Bitmap Heap Scan** (recheck) → feeds a **Sort / top-N heapsort** that must consume the *whole* ~4.28 M-row match-set computing `ts_rank_cd` per row before emitting `LIMIT 30`. The GIN is **not** the cost; the per-row rank + full sort is. GIN cannot return rows in rank order → no index-ordered early-out exists with the current index.

| Probe | What it isolates | Result |
|---|---|---|
| **P0** corpus + term selectivity | `count(*) WHERE tsv @@ 'x264'` ≈ 4.28 M; index sizes | ⏳ |
| **P1** served wall (relevance, paginated `LIMIT 30`) | Execution Time + Sort node actual time | ⏳ (~49 s expected) |
| **P2** + the app joins (torrents + content) | confirm joins are NOT dominant | ⏳ |
| **P3a** GIN match only (`count(*)`, no rank/order) | GIN cost | ⏳ (~482 ms expected) |
| **P3b** match + rank, NO order | rank-compute cost = Δ(b−a) | ⏳ |
| **P3c** match + rank + order + LIMIT | sort cost = Δ(c−b) | ⏳ (= P1) |
| **P4** same term, ORDER BY `published_at`/`seeders` (btree) | early-term vs bitmap+sort regime | ⏳ |
| term matrix | broad (`x264`) / medium (`1080p`) / rare group / 2-term AND / phrase | ⏳ — locates the cliff |

### D4 — fix candidate matrix

| Option | Keeps `ts_rank_cd` relevance? | New disk | Build/migration | Write-path cost | Broad-term latency | Risk |
|---|---|---|---|---|---|---|
| **Baseline (today)** | yes | 0 | — | current | ~49 s (the wall) | — |
| **2.1 RUM `<=>`** | **no** (`ts_rank`, no cover-density) | **+30–50 GB** (~2–3× GIN) | slow single-thread build + `CREATE EXTENSION` | **high** (positional posting-list updates; FIND-1 risk) | tens of ms (the win) | **high** — write-amp + semantics + ext |
| 2.2 `ts_rank` swap | ~similar | 0 | code 1-liner | unchanged | ~tens of s (constant-factor only) | low — **not a fix** |
| 2.3a published_at/seeders default | no (popularity) | 0 | code + UX | unchanged | fast for common terms (early-term) | medium (product default change) |
| **2.4 bounded-candidate CTE (approx)** | yes (over a window) | 0 | code | unchanged | bounded (rank ~50k) | low-medium (approx recall) |

_(latency cells filled from §P-probes if bench runs; otherwise hypotheses from code analysis + MEMORY)_

### D4 verdict — **DEFER RUM; code-only bounded-candidate ranking is the lever**

> **DEFER RUM.** It is the textbook fix (index-ordered ranking, early-termination, tens-of-ms) and the only option that keeps interactive latency *and* relevance order — but its costs are real and stacked **against this exact project**: **+30–50 GB** on a footprint-minimising migration; a slow single-threaded build; a new shared-lib extension to bake into the PG image; a **ranking-semantics change** (`<=>` ≈ `ts_rank`, no cover-density vs `ts_rank_cd`); and — **the dealbreaker — write amplification on an upsert-heavy table that already shows super-linear tsv-update cost (FIND-1)**. RUM's positional posting lists are far heavier to update than GIN's. Pursue RUM **only** on a confirmed product requirement for sub-second relevance-ranked broad-term results, and even then gate on the write-amp probe.
>
> **If broad-term latency is a real user complaint, ship the code-only lever (no extension, no disk, no write-path impact):** **§2.4 bounded-candidate ranking** — cap the candidate window via the existing `published_at`/`seeders` btree (early-terminates), `ts_rank_cd`-rank that bounded window, return top-N. Keeps relevance semantics *approximately* (cost = a low-rank-but-old true match could fall outside the window — arguably fine, since `ts_rank_cd` over millions of equally-weighted `simple` single-term matches is already near-arbitrary). Implemented by extending `shouldTryCteStrategy()`/the CTE branch to the relevance path. Companion fallback: **§2.3a** degrade broad single common terms to a popularity sort (the existing CTE already accelerates that shape). **Reject §2.2** (`ts_rank` swap) — constant-factor only, doesn't remove the cliff.
>
> **Now vs defer:** characterise on bench now (cheap, read-only EXPLAIN ANALYZE); **defer any code/extension change** pending the numbers + product decisions: (a) is broad-term relevance latency actually user-visible? (most real queries are multi-term/selective, already < 25 ms per EXP-A) (b) is approximate relevance acceptable? **Nothing here blocks the migration; no production change is proposed.**

---

## Appendix — run log index

Logs land in [`psx-logs/`](./psx-logs/) as the runner deposits them; this section maps each to the table it fills.

| log file | thread | fills |
|---|---|---|
| `psx_d1_*` (encode smoke/full, blob→parquet, parity) | D1 | §D1.1–D1.3 |
| `psx_l3_A_build.log` / `psx_l3_A_recall.log` / `psx_l3_A_pq.log` | D2(A) | §D2(A) |
| `psx_l3_B5_*` / `psx_l3_B6_*` (minchars, realistic, pos on/off) | D2(B) | §D2(B) |
| `psx_l3_C_freshness.log` (optional) | D2(C) | §D2(C) |
| _(D3: no run — design-only)_ | D3 | §D3 (settled) |
| `psx_d4_explain_*.log` (optional EXPLAIN) | D4 | §D4 P-probes |

_Last updated: 2026-06-09 (skeleton; awaiting logs)._
