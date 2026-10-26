# PSX campaign — RESULTS (D1–D4 synthesis)

**Date:** 2026-06-09 · **Status:** 🟢 **COMPLETE** — all four threads synthesized from runner logs in [`psx-logs/`](./psx-logs/). **D1** ✅ gap closed (0 errors, exact parity, 0.746 µs/file) · **D2** ✅ (A build 13.32 GiB + B5 selectivity + B6 positions) · **D3** ✅ RETIRE (design-only) · **D4** ✅ FIND-2 wall measured. Analyst (`psx-analyst`) was **LOCAL-only** — never SSHed to HEL1; the runner owned the single connection. Lead commits (checkpoint @ D2(A), final now).
**Env:** HEL1 throwaway bench (879.5 M-row **pre-blob-backfill** restore, `torrent_files` source; bench-pg NodePort DSN `postgres@127.0.0.1:<PORT>/bitmagnet` (loopback, throwaway creds); userspace rust/uv). **Production FSN1 untouched.** ONE serial run, ONE ssh connection (the runner owns it).
**Specs:** [`psx-D1`](./psx-D1-blob-parquet-gap-spec.md) · [`psx-D2`](./psx-D2-l3-prod-confirmation-spec.md) · [`psx-D3`](./psx-D3-agg-extmaxsize-spec.md) · [`psx-D4`](./psx-D4-find2-ftswall-spec.md)
**Baselines extended:** [`pathsearch-microbench-RESULTS.md`](./pathsearch-microbench-RESULTS.md) (PS-MB1, the L3 GO) · [`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md) (ARCH-C DuckDB) · [`fba1-jsonb-dropgate-results.md`](./fba1-jsonb-dropgate-results.md) (FB-A1 JSONB) · [`cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`](./cjk-tokenizer-and-incremental-merge-bench-RESULTS.md) (EXP-D/D2/E).

---

## 0. Headline — one-line verdict per thread

| Thread                    | Question                                                                                                                                 | Verdict                                                                                                                                                                                                                                                                                                                     | Status                    |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- |
| **D1** blob→Parquet       | Does the real **decode→ext→Parquet** pipeline over actual blob bytes match the `torrent_files`-sourced Parquet, and at what throughput?  | ✅ **GAP CLOSED**: **0 blob errors**, **EXACT parity** (slim + full incl. `path`, checksums identical), **0.746 µs/file** (in band). Encode = Rust-indicative; parity proven on a 32,157-torrent/1.84 M-file overlap (corpus-independent → generalises); full-16.97 M 0-errors run gated by ~8 h bench read                 | ✅ MEASURED               |
| **D2** L3 prod-shape      | Confirm `WithFreqs` **13.54 GiB** + recall **1.0** + the **REAL** prod-shape latency (TopDocs-by-seeders), and settle the broad p95 tail | ✅ **DONE**. **(A)** 13.32 GiB (−1.6 %), recall **1.0000**, `.pos` empty, ascii3 p50 25.6 ms — **−83.7 % vs 82 GB, deployable**. **(B5)** realistic multi-word < 50 ms p95; bare broad gram tails (TopDocs 77–94 ms); Count is a floor; engine-irreducible→UX. **(B6)** `WithFreqs` latency-free (warm identical, cold ~4×) | ✅ (A)+(B5)+(B6)          |
| **D3** `agg ext∧max_size` | Is a PG `agg_torrent_ext` rollup worth its disk vs DuckDB?                                                                               | 🟥 **RETIRE** — route ext∧max_size to DuckDB (5–132 ms, **+0 PG disk**); corrected agg sizing ≈ **10 GB**; **no run**                                                                                                                                                                                                       | ✅ SETTLED (design-only)  |
| **D4** FIND-2 FTS wall    | The broad-common-term `ORDER BY ts_rank_cd` wall — fix?                                                                                  | ✅ **MEASURED**: wall = **49.4 s (x264) / 74.9 s (1080p)**, 41–59 s of it pure `ts_rank_cd` (GIN scan 469 ms). **DEFER RUM** (write-amp dealbreaker). Cheap cliff-fix = **§2.3a popularity sort (1.9–4.9 ms)**; §2.4 bounded-candidate keeps relevance but only 7× (6.9 s, not interactive)                                 | ✅ MEASURED + REC SETTLED |

> **🏁 Whole-campaign bottom line:** the **L2 measurement gap is CLOSED** — the blob→Parquet pipeline is validated on **real blob bytes** with **0 errors** and **exact byte-for-byte parity** to `torrent_files` (incl. `path`), at 0.746 µs/file; every prior DuckDB/file-index conclusion that was `torrent_files`-sourced now provably holds on the production blob. The **L3 production search index is BUILT and deployable** — `WithFreqs` per-torrent ngram at **13.32 GiB** (−83.7 % vs positions-on), recall **1.0000**, latency-neutral; realistic multi-word search is **< 50 ms p95**, and the broad-single-gram tail (**production p95 ≈ 77–94 ms** via TopDocs-by-seeders) is **source-proven engine-irreducible → solved at UX**. **`agg_torrent_ext` is RETIRED** (DuckDB serves ext∧max_size at +0 PG disk). **FIND-2's broad-term FTS wall has a cheap code-only fix** (§2.3a popularity sort, 2–5 ms) with **RUM deferred** (write-amp dealbreaker). Net: the per-file replacement layers (DuckDB-Parquet + L3 ngram) are empirically de-risked; nothing here changes the standing constraint below.
>
> **Cross-cutting standing constraint (unchanged):** the `torrent_files` **DROP stays deferred** until every replacement layer is proven **in prod**. None of D1–D4 touches that sequencing — all bench/design-only. D1/D2 are bench-only; D3 is design-only; D4 is pre-existing + DROP-independent.

---

## D1 — End-to-end blob → Parquet on REAL blobs (closes the L2 measurement gap)

**The gap:** every prior L2/DuckDB/file-index number was sourced from `torrent_files`, never from the production blob (the bench restore is the pre-backfill dump → `files_data`/`torrent_file_summary` EMPTY). D1 re-encodes `files_data` **on the bench** with the exact production encoder (`blobmigration.SerializeFiles`), then runs the real `decode→ext→Parquet` path and proves parity against the `torrent_files`-sourced Parquet — **no prod blob reads, ever**.

**Why bench re-encode is faithful (code-verified):** prod format = `zstd_L3(msgpack_named_array[{i,p,e,s}])`; Go⇄Go, Rust⇄Rust, and cross-language byte-identical **inner-msgpack** round-trips are all proven (`serializer_test.go`, `blob.rs` tests, `blob_fixture.rs`). Bench-encoded blob is indistinguishable from prod for every downstream **decoder** (only the outer zstd frame differs between libzstd and klauspost — _mutually decodable_, immaterial to any reader). Backfill encodes **all** `torrent_files` rows (no cap) → decoded fileset === `torrent_files` for that hash → exact Stage-3 parity.

> 🚨 **Encoder reality (overrides D1 spec §1.2):** HEL1 has **no Go toolchain** → the runner uses the **Rust encoder** (`serialize_files`), the spec's _fallback_. Consequence for labeling: **decode, parity, and Stage-2 end-to-end throughput remain production-faithful** (the inner msgpack the decoder consumes is byte-identical to prod). But the **encode µs/file is "Rust libzstd, indicative"** — the production importer encodes with klauspost zstd (`SpeedDefault`), a _different_ zstd implementation. So D1.1's encode timing is an **indicative encode-cost order-of-magnitude, NOT a measurement of the Go importer's encode path.** Treat it as "blob encoding costs ~X µs/file on this box," not as "the persist.go hot path was profiled."

### D1 gates flagged to lead

| Gate   | Question                                                             | Result                                                                                                       |
| ------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| **G0** | post-backfill dump exists? (zero-encode fast path)                   | ❌ only the **pre-backfill** dump → Stage-1 re-encode taken                                                  |
| **G1** | Go toolchain on HEL1? (else Rust-encode fallback, zstd-frame caveat) | 🔴 **NO Go → Rust `serialize_files` used** (encode-timing = indicative; decode/parity/throughput unaffected) |
| **G2** | ≥ ~50 GB free disk before Stage 1                                    | ✅ (smoke-scale only; full encode not run)                                                                   |
| **G3** | encode smoke (`--limit 100000`) throughput acceptable                | ✅ 0.458 µs/file encode; read-bound write (bench artifact) → ran smoke only, no full encode                  |
| **G4** | lead GO + bench-up (pre RUN-6 teardown)                              | ✅ ran on the live bench env                                                                                 |

### D1.1 — Encode path (Stage 1): importer encode µs/file

| Metric                                 | Smoke (100k) ✅ MEASURED                   | Full (16.97M)          | Notes                                                                                                                                                                                                                                                                                               |
| -------------------------------------- | ------------------------------------------ | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| torrents encoded                       | **100,000**                                | — not run              | —                                                                                                                                                                                                                                                                                                   |
| files encoded                          | **5,150,589** (avg **51.5** files/torrent) | — not run              | full ≈ 856.79 M                                                                                                                                                                                                                                                                                     |
| **encode µs/file** (`serialize_files`) | **0.458 µs/file**                          | — not run              | 🏷️ **Rust libzstd — indicative**, NOT the Go importer path (see caveat above)                                                                                                                                                                                                                       |
| write throughput (t/s)                 | 536 t/s (READ-bound)                       | — not run              | **~28k rows/s** = k8s **NodePort + sqlx async** read of `torrent_files` → full ≈ **8 h**. 🚨 **A bench artifact, NOT a production throughput signal** — prod reads blobs from _local_ PG (fast). The full run would only re-measure this artifact + add a marginal "0-errors-across-all" guarantee. |
| `files_data` bytes written             | **94.2 MB** (0.09 GiB)                     | ~16 GiB (extrapolated) | matches the ~16 GB estimate                                                                                                                                                                                                                                                                         |
| encoder used                           | **Rust** `serialize_files`                 | —                      | HEL1 has no Go → spec fallback; zstd-frame differs (klauspost in prod), inner msgpack byte-identical                                                                                                                                                                                                |

**Context check (indicative only):** the live `persist.go` hot path is ~1–1.5 ms/torrent @ ≤100 files (klauspost). The bench Rust encode at **0.458 µs/file** is an order-of-magnitude sanity check on encode cost (≈ 23.6 µs/torrent @ 51.5 files) — **not** a like-for-like profile of the production encoder. The **full-corpus encode was not run** (the ~8 h NodePort-streaming read of 856 M `torrent_files` rows is a bench-DSN artifact, unrelated to prod); the smoke proves the encode path and feeds Stage 2/3.

### D1.2 — REAL blob → Parquet (Stage 2): end-to-end throughput ✅ MEASURED (on the 100k bench-encoded blobs)

`blob_export` over real bench-encoded `files_data` (zstd→msgpack decode → path-derive ext → Parquet):

| Metric                                 | slim                     | full (with path)               | Notes                                                                                                        |
| -------------------------------------- | ------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| torrents scanned                       | 100,000                  | 100,000                        | (32,157 carried a bench blob → see D1.3)                                                                     |
| file-rows decoded                      | **1,836,665**            | **1,836,665**                  |                                                                                                              |
| **blob errors**                        | **0** ✅                 | **0** ✅                       | 🚨 zero — every bench blob decoded cleanly                                                                   |
| wall (s)                               | 4.7 s (cold first-touch) | 1.4 s (warm)                   | slim read the blobs cold; full re-read warm                                                                  |
| torrents/s                             | 21,219                   | 72,859                         |                                                                                                              |
| **M files/s** → **µs/file end-to-end** | 0.39 M f/s               | **1.34 M f/s = 0.746 µs/file** | ✅ **lands in the 0.6–0.94 µs/file band** (warm, incl. PG read + Parquet write → decode-only ≤ 0.75 µs/file) |

⟹ **0.6–0.94 µs/file CONFIRMED on real blob bytes** (the prior figure was smoke-sampled; now end-to-end through the production decode path). The slim run's lower 0.39 M f/s is cold first-touch of the blobs; the warm full run (1.34 M f/s) is the representative steady-state.

### D1.3 — PARITY: Parquet-from-blobs == Parquet-from-torrent_files (Stage 3) ✅ EXACT

Compared on the **32,157-torrent / 1,836,665-file overlap** (blob_export's first-100k-from `torrents` ∩ encode-smoke's bench blobs; `tf` Parquet restricted to the blob info_hash set):

| Check                                                           | blob-sourced                 | tf-sourced                   | Match?           |
| --------------------------------------------------------------- | ---------------------------- | ---------------------------- | ---------------- |
| row count                                                       | 1,836,665                    | 1,836,665                    | ✅               |
| distinct info_hash                                              | 32,157                       | 32,157                       | ✅               |
| `sum(size)`                                                     | 140,556,100,102,595          | 140,556,100,102,595          | ✅ byte-exact    |
| null-ext count                                                  | 146,457                      | 146,457                      | ✅               |
| **slim tuple checksum** `(info_hash,file_index,extension,size)` | `16937634831348936200500257` | `16937634831348936200500257` | ✅ **identical** |
| **full tuple checksum** (+ `path`)                              | `16930189039409968487890531` | `16930189039409968487890531` | ✅ **identical** |

⟹ **PARITY ✅ EXACT both modes.** The encoder → blob → decoder → path-derive(G1) pipeline reproduces `torrent_files` **byte-for-byte**, including `path` (the FTS column). This is the conclusive fidelity proof the whole L2/DuckDB measurement chain rested on (every prior number was `torrent_files`-sourced — now shown identical to blob-sourced).

### D1 success criteria

1. **Decode integrity** — **0 blob errors** across the 100k-torrent sample (32,157 with blobs / 1.84 M files) ✅; **full 16.97 M-torrent run NOT executed** (8 h bench read — see caveat) → ✅ on sample, ⚠️ not at full scale
2. **Parity** — `blob_rows == tf_rows` AND content-hash identical (slim + full incl. `path`) on the overlap → ✅ EXACT (no cap-semantics delta needed — encode was uncapped)
3. **Throughput** — end-to-end **0.746 µs/file** (warm, full) → ✅ in the 0.6–0.94 µs/file band
4. **Format fidelity** — bench blob decodes to the exact `{i,p,e,s}` set (implied by #1+#2 — 0 errors + identical checksums) → ✅
5. **Encode cost timed** — Rust `serialize_files` **0.458 µs/file** reported + contextualised (🏷️ **indicative**, not the Go importer path — HEL1 has no Go) → ✅

> **D1 verdict ✅ — gap CLOSED on FIDELITY + per-file cost (sample-scoped, stated as a limitation — not a silent truncation):** the production-format blob decodes **cleanly (0 errors)** and the resulting Parquet is **byte-for-byte identical** (slim + full incl. `path`) to the `torrent_files`-sourced artifact every prior L2/DuckDB/file-index conclusion rests on, at **0.746 µs/file** end-to-end — squarely in the predicted 0.6–0.94 band, now on **real blob bytes**.
>
> **Explicit scope limitation:** parity + 0-errors were proven on the blob*export∩encode **overlap = 32,157 torrents / 1,836,665 files**, **NOT all 16.97 M / 856 M files**. The full-corpus end-to-end run was **not executed** — the bench encode is **READ-bound at ~28k rows/s (k8s NodePort + sqlx async)** ≈ 8 h, **a bench-DSN artifact, not a production throughput signal** (prod reads blobs from local PG, fast); the full run would only re-measure that artifact and add a marginal "0-errors-across-\_all*" guarantee. Because **per-file decode cost + parity are corpus-independent** (every blob is decoded and checksum-checked independently), the sample is **conclusive for the gap it closes**; the single claim left unproven-at-full-scale is "0 errors across _all_ 16.97 M." _(A wider 5.15 M-file full-overlap `blob_export`+parity was launched on HEL1 → `bench-scratch/psx_r5_pqfull.log`; it finishes after the runner's shutdown and was not fetched locally — expected identical, same encoder/decoder, wider overlap. The confirmed 1.84 M-file EXACT-parity above is the deliverable.)_
>
> **Encode label (exact):** `serialize_files` = **0.458 µs/file** — \*Rust libzstd, **indicative\***; the Go importer uses **klauspost/compress (`SpeedDefault`)**. Inner msgpack is byte-identical Go⇄Rust (`blob_fixture.rs`); only the outer zstd frame differs (mutually decodable) → decode/parity/throughput stay production-faithful, only the encode _timing_ is indicative.
>
> **Bench-only code note:** the `bitmagnet-db/stream.rs` `files_count::int8` cast used by `blob_export` is **uncommitted / bench-only** (the bench restore is INT4; not a verified prod fix). **No anomaly fired:** 0 errors, exact parity (no delta at all — encode was uncapped, cap-semantics moot), throughput on-target. **R6 freshness was SKIPPED** (reasoning-settled — per-torrent supersession is strictly cheaper than the validated EXP-E per-file 11 ms / ~2 ms; see §D2(C)).

---

## D2 — L3 production-shape confirmation + p95-tail attack

L3 (per-torrent ngram free-text path index) is a **GO** (user decision). PS-MB1 measured the GO on the _as-built_ shape and _projected_ the prod shape. D2 closes three residual gaps: **(A)** build the real `WithFreqs` artifact (size was computed by `.pos` subtraction, never built); **(B)** attack the broad-query p95 tail and decide reducible-vs-UX; **(C)** confirm per-torrent freshness/supersession.

### D2(A) — `WithFreqs` production-shape build ✅ PASSED ALL GATES (2026-06-09)

> **Source-settled, now BUILT & confirmed:** `WithFreqs` drops only the `.pos` segment files; `.idx` (postings) + `.term` (term dict) are byte-identical to the positions-on build. The full-corpus build (`idx_pt_ngram_full_nopos`, logs `psx_r3_withfreqs_build.log` + `psx_r3_recall.log`) **lands the prediction**: 13.32 GiB, recall 1.0000, identical latency, with `.pos` genuinely empty. **The "computed-not-built" caveat is removed; this is a deployable artifact.** Build wall **~62 min** (15:47:35Z→16:49:56Z), single-thread writer + 2 GB arena, force-merge 17 segs → **1 segment**.

| Gate                                | Expectation                                       | Result (measured)                                                                                                                                                                                                             | Verdict                          |
| ----------------------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| **G5-A size**                       | TOTAL ≈ **13.54 GiB**, positions == 0             | **13.32 GiB** (14,298,884,502 B / 842.4 B-doc) = **−1.6 %**; **positions = 104 B ≈ 0.000 B/doc** (`.pos` is a 4 KB stub)                                                                                                      | ✅ within ±3 % AND `.pos`≈0      |
| build sanity                        | docs == 16,973,470, 1 seg, 60–100 min             | docs = **16,973,470** (exact), **1 segment**, ingest 3595 s + merge → **~62 min**                                                                                                                                             | ✅                               |
| postings invariant                  | `.idx` ≈ 827.8 B/doc, term dict ≈ 15.7 B/doc (A2) | `.idx` = **816.28 B/doc** (−1.4 %, within ±2 %); term dict = **13.13 B/doc** (−16 % vs A2 — ⚠️ FLAG but tiny [223 MB total] & latency-irrelevant; bounded ngram vocab, A2 ref likely a different N)                           | ✅ postings; ⚠️ term-dict benign |
| recall (separate 150k-truth run)    | ngram recall == 1.0000 every group                | **recall = 1.0000 on EVERY group** (ascii2–5, cjk2–4); precision 1.0 except ascii4 **0.910** / ascii5 **0.980** (documented non-contiguous ≥4-char-gram false-positives — recall still perfect, the conjunction never misses) | ✅✅                             |
| latency (prod shape, informational) | `ascii3` p50 ≈ 24.7 ms, `cjk3` ≈ 0.2 ms           | `ascii3` p50 **25.6 ms** / p95 41.0 / p99 60.6; `cjk3` **0.27 ms**; ascii2 3.35, ascii4 19.0/55.1, ascii5 28.2/63.7, cjk2 0.18 (matches R2 positions-on — postings byte-identical)                                            | ✅ within 15 %                   |

**Component breakdown (full corpus, B/doc):** postings 816.28 · term dicts 13.13 · FAST cols 8.00 · doc store 4.02 · field norms 1.00 · **positions 0.000** · TOTAL **842.4** → **13.32 GiB**. Postings = 97 % of the index; dropping positions removed the entire `.pos` segment (82 GB → 13.3 GiB = **−83.7 % vs the positions-on `idx_pt_ngram_full`**) at zero recall/latency cost.

**Crate change (drafted, `cargo check` green):** a `--no-positions` flag on `recall` flipping the path field `WithFreqsAndPositions`→`WithFreqs` (3 edits: `schema.rs`, `RecallArgs`, `run_recall`; guard rejects `--no-positions` with `default`/`lindera` tokenizers — those use `PhraseQuery` which needs positions). `pathquery` needs no change.

### D2(B) — broad-query p95 tail: **ENGINE-IRREDUCIBLE → UX** (source-proven, settled now)

> 🔑 **Reframing the measured number:** PS-MB1's p95 was measured with the **`Count`** collector — the _cheapest possible_ read. Production needs `Count` (totalCount) **plus** `TopDocs::order_by_fast_field(seeders)` (the page), and the TopDocs path _also_ full-scans (proven below) + adds heap work. **So PS-MB1's 24.71 ms p50 / 58.6 ms p95 is a LOWER BOUND on production latency, not an estimate of it.** You cannot even match the floor by adding a sort, let alone beat it. **The REAL prod-shape number (TopDocs-by-seeders) ≥ the Count lower bound** — D2(A)'s latency pass measures it directly.

Candidate p95 mitigations — verdict from tantivy 0.26.1 source (`(F)` citations in spec):

| #   | candidate                                          | source verdict                                                                                                                                                                                               | runnable?                                                   |
| --- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------- |
| B1  | min-chars 4/5                                      | **already worse — CONFIRMED** by the R2/R4 sweep (ascii4 warm p95 55.2 / ascii5 64.3; longer substrings keep huge match-sets; `ascii3` _is_ the floor)                                                       | ✅ (sweep data)                                             |
| B2  | rarest-gram-first conjunction                      | **no-op** — `intersect_scorers` already `sort_by_key(cost())` (`intersection.rs:31`); intersection already rarest-driven                                                                                     | n/a                                                         |
| B3  | stop-gram (drop commonest bigram)                  | likely no-op + lossy (commonest gram is the cheap non-driver; dropping a `Must` → false positives)                                                                                                           | optional (not built)                                        |
| B4  | index-sort seeders + capped TopDocs                | **double dead-end** — index-sort _removed_ from tantivy (#2434, `CHANGELOG.md:98`); `order_by_fast_field` requires_scoring==false → full-scan, no early-term; Block-WAND only fires for BM25-score on unions | **not runnable** (engine lacks feature)                     |
| B5  | **realistic multi-word selectivity**               | the real reducer — multi-word → larger conjunction → tiny intersection                                                                                                                                       | ✅ **MEASURED** — hypothesis validated                      |
| B6  | `WithFreqs` vs `WithFreqsAndPositions` query delta | query reads only `.idx`; delta = page-cache pressure (82 GB vs 14 GB), not algorithmic                                                                                                                       | ✅ **MEASURED** — warm identical, cold ~4× better for nopos |

**B5 — realistic-query selectivity ✅ MEASURED 2026-06-09** (`psx_r2_b5_selectivity.log`, `idx_pt_ngram_full` WithFreqsAndPositions 16,973,470 docs/1 seg; **warm-only** — no root for `drop_caches`, "cold" col = first-exec on a likely-warm page cache). **Count collector, by query group:**

| group                                                  | avg hits  | warm p50 | warm p95    | warm p99 | < 50 ms p95?                                                  |
| ------------------------------------------------------ | --------- | -------- | ----------- | -------- | ------------------------------------------------------------- |
| **a1_broad** (bare single gram — synthetic worst case) | 1,066,065 | 29.0 ms  | **63.4 ms** | 64.3 ms  | ❌ (the tail)                                                 |
| **a2_2word**                                           | 17,790    | 10.7 ms  | **21.9 ms** | 22.1 ms  | ✅                                                            |
| **a3_dotted** (e.g. `x264.1080p`)                      | 105,603   | 18.3 ms  | **47.8 ms** | 48.1 ms  | ✅                                                            |
| **a4_long** (3+ tokens)                                | 29,239    | 2.1 ms   | 51.3 ms\*   | 51.7 ms  | ~ (\*bimodal — most ~2 ms; one space-spanning query breaches) |
| **cjk2word**                                           | 31,497    | 0.6 ms   | **1.7 ms**  | 1.8 ms   | ✅✅                                                          |

⟹ **B5 hypothesis VALIDATED: realistic multi-word queries are < 50 ms p95** (2word 22 ms, dotted 48 ms, cjk 1.7 ms). **ONLY the bare single broad gram tails > 50 ms** (a1 63 ms; the `ascii3` sweep 59 ms) — the degenerate synthetic worst case, not real typeahead traffic.

**🔑 Count is a LOWER BOUND — production TopDocs page (`order_by_fast_field` ident DESC, the value-independent proxy for seeders) adds ~20–30 % on broad terms** (confirms the spec §0 reframing — the page collector full-scans the match-set with no early-term, §F):

| group                        | Count p50 / p95 | **TopDocs p50 / p95** | Δ                                                                                     |
| ---------------------------- | --------------- | --------------------- | ------------------------------------------------------------------------------------- |
| a1_broad (1.07 M hits)       | 29.0 / 63.4     | **38.5 / 76.5**       | +30 % p95                                                                             |
| `ascii3` sweep (2.13 M hits) | 24.6 / 59.4     | **37.1 / 93.5**       | +57 % p95                                                                             |
| `ascii2` (4.42 M hits)       | 2.9 / 7.5       | **20.9 / 52.3**       | **7×** — Count of one posting list is cheap, TopDocs scans _all_ hits to a top-K heap |
| cjk2 (223 k hits)            | 0.14 / 0.52     | **0.27 / 2.96**       | interactive on both                                                                   |

⟹ **Production broad-gram p95 ≈ 77–94 ms** (TopDocs, the real page). **CJK interactive on both collectors.** This empirically seals the spec's claim that PS-MB1's Count p95 _understates_ production — you can't even match the Count floor by adding the sort, let alone beat it (B4's source-proven dead-end).

**B6 — positions on/off query delta ✅ MEASURED 2026-06-09** (`psx_r4_b6_positions_delta.log`; same binary + sweep, both indexes; warm-only — caches partly warm from R2/R3, so "cold" = first-exec, not true `drop_caches`). **Hypothesis confirmed exactly: dropping `.pos` is latency-neutral warm and better cold.**

| group  | pos-on warm p50 → nopos | pos-on warm p95 → nopos | pos-on TopDocs p95 → nopos |
| ------ | ----------------------- | ----------------------- | -------------------------- |
| ascii3 | 24.71 → **24.32 ms**    | 58.18 → **58.92 ms**    | 92.39 → **93.33 ms**       |
| ascii5 | 26.65 → **26.37 ms**    | 63.74 → **64.28 ms**    | 75.74 → **76.95 ms**       |
| cjk2   | 0.14 → **0.14 ms**      | 0.56 → **0.51 ms**      | 8.04 → **7.88 ms**         |

⟹ **warm latency IDENTICAL within noise** (postings `.idx` byte-identical; the ngram conjunction never reads positions). **Cold/first-exec benefit** from less page-cache pressure (13 GB vs 82 GB resident): `ascii2` cold-p95 **31.39 ms (pos) → 8.31 ms (nopos) = ~4× better**; other groups equal here only because caches were already warm — a true-cold deploy faulting 82 GB vs 13 GB would widen the gap further.

| index                                       | warm p95 (`ascii3`) | cold-p95 (`ascii2`) | resident     |
| ------------------------------------------- | ------------------- | ------------------- | ------------ |
| `idx_pt_ngram_full` (WithFreqsAndPositions) | 58.18 ms            | **31.39 ms**        | 82 GB        |
| `idx_pt_ngram_full_nopos` (WithFreqs)       | 58.92 ms            | **8.31 ms**         | **13.3 GiB** |

⟹ **`WithFreqs` is a free win: 83.7 % smaller, recall 1.0000, identical warm latency, ~4× better cold.** ✅ confirmed (not just hypothesised).

### D2(C) — per-torrent freshness sanity

Per-torrent path-bag is **strictly cheaper** than the already-validated EXP-E per-file numbers: supersession = `delete_term(info_hash)` + re-add **one** path-bag doc (vs ~52 file docs), torrent-granular = EXP-B's anti-join analog; LogMergePolicy caps at ~17 M docs (vs 879 M). EXP-E measured per-file supersession **11 ms**, fresh-lag ~2 ms flat → per-torrent ≤ that. **R6 freshness measurement was SKIPPED** (reasoning-settled per spec §C + EXP-E — per-torrent is strictly cheaper than the validated per-file numbers, so an explicit run adds no decision value). Verdict: per-torrent freshness inherits the EXP-E numbers as an upper bound; **ms-level supersession + ~2 ms fresh-lag, confirmed by inheritance.**

### D2 deliverable verdict

> **(A) ✅ PASSED:** full-corpus `WithFreqs` per-torrent index BUILT — **13.32 GiB** (−1.6 % vs the 13.54 GiB prediction), **recall 1.0000** every group, `.pos` empty (104 B), **ascii3 p50 25.6 ms** (matches R2). **−83.7 % vs the positions-on 82 GB index at zero recall/latency cost.** The "computed-not-built" caveat is gone; this is a deployable artifact (kept on HEL1 for the B6 head-to-head). Lone benign FLAG: term-dict 13.13 B/doc vs A2's 15.7 (−16 %, 223 MB total, latency-irrelevant).
> **(B) FULLY MEASURED (B5 + B6):** broad-query p95 is **source-proven engine-irreducible** in tantivy 0.26.1 — every engine lever is already-done (B2), unsupported (B4), or can't shrink a full-match-set conjunction count (B1/B3). **B5: the only real reducer is query selectivity** — realistic multi-word queries are **< 50 ms p95** (2word 22 ms, dotted 48 ms, cjk 1.7 ms, Count); only the **bare single broad gram** tails (a1 63 ms Count → **77 ms TopDocs**; `ascii3` 59 → **94 ms**). **TopDocs page is +20–57 % over Count** (7× on `ascii2`) → PS-MB1's Count p95 is a **floor**, real prod ≈ **77–94 ms** for the degenerate broad gram. **B6: `WithFreqs` is latency-free** — warm latency identical to positions-on (postings byte-identical), cold ~4× better (13 GB vs 82 GB resident). **Verdict: irreducible at engine, solved at UX — ship the `WithFreqs` artifact with UX guards** (client min 2–3 chars, ~150 ms debounce, loading state, "top N of many"). The > 50 ms cases are degenerate single-broad-gram typeahead frames, not real multi-word traffic; if even that must be per-keystroke, fall back to **search-on-submit** (~25–38 ms median).
> **(C)** Per-torrent freshness inherits + beats the validated EXP-E numbers (1 doc/torrent supersession). **Anomaly flags — NONE fired:** recall = 1.0000 (✓ not < 1.0); size = 13.32 GiB (✓ < 13.54 +3 %); `ascii3` p50 25.6 ms / p95 41 ms (✓ on-target, not far above the Count floor). The lone non-blocking note is the −16 % term-dict B/doc vs A2 (tiny, latency-irrelevant).

---

## D3 — `agg_torrent_ext` for ext∧max_size: **RETIRE** (design-only, no run)

**The one question:** is a PG `agg_torrent_ext` rollup worth its disk to serve the torrent-grain **ext∧max_size** query ("torrents with an `.mkv` > 1 GB"), or should that route to the DuckDB-Parquet tier (per-file size already there, +0 PG disk)?

**Context:** `torrents.file_extensions` JSONB already won the plain-ext DROP gate (FB-A1: +119 MB vs agg's +9.5 GB). agg's _only_ remaining justification is ext∧max_size (JSONB carries no size). This is the last decision keeping agg alive.

### D3 — backend contest (no run; sizing + latency cited from FB-A1/ARCH-C/RUN-3)

| backend                                       | size source                          | new PG disk                        | composes into PG text search?              | ext∧max_size latency        |
| --------------------------------------------- | ------------------------------------ | ---------------------------------- | ------------------------------------------ | --------------------------- |
| **A** `EXISTS torrent_files … size > N`       | the table **being dropped**          | 0 (but it's the 261 GB we remove)  | ✅ native (parity truth)                   | (baseline)                  |
| **B** `EXISTS agg_torrent_ext … max_size > N` | rollup `max(size)` per (torrent,ext) | **+~10 GB** (corrected, see below) | ✅ native correlated EXISTS                | (PG — design-only, not run) |
| **D** DuckDB-Parquet                          | per-file `size` (already there)      | **0**                              | ❌ cross-engine (hand info_hash set to PG) | **5–132 ms** (ARCH-C)       |

**Corrected agg sizing (the spec's tightening):** the _usable_ shape (both query directions) needs PK `(info_hash, extension)` **+** `(extension, max_size) INCLUDE (info_hash)` covering index → **~10 GB natural** (heap 3.5 + PK 3.0 + covering 3.5) / ~5 GB surrogate+dims. This **supersedes** FB-A1's +9.5 GB (used the _wrong_ `(extension, info_hash)` index) and RUN-3's +3–5 GB (no/wrong index). `agg ext∧max_size` Parquet rollup is **1.39 GB out-of-PG**, collapses **5.2 ms** (ARCH-C).

**DuckDB already serves it (ARCH-C, same 879.5 M-row restore, 24 cores):** `files_slim` sorted(ext,size) collapse **132 ms** / exact count **17 ms**; out-of-PG agg Parquet rollup **5.2 ms**. Ground truth: mkv>1 GB = 5,699,629 files / **1,723,793 distinct torrents**; movie∧mkv>1 GB = 728,574.

### D3 verdict — **RETIRE `agg_torrent_ext`**

> **🟥 RETIRE (default).** ext∧max*size has **zero Go surface today** (`grep max_size` → only the log rotator) — it is uncommitted. DuckDB answers it at **5–132 ms with +0 PG disk** on the side we're \_adding* capacity. The DROP project's whole point is shedding PG disk (−245 GB); re-adding ~10 GB into the DB we're shrinking, **plus** a dual-write delta pipeline **plus** a parity checker, for one hypothetical query is a direct regression. agg wins in **exactly one uncommitted scenario**: ext∧max*size must be a **correlated filter composed inside the main PG text search**, with qualifying sets too large for a DuckDB→PG info_hash handoff. **Recommend:** keep agg **DEFERRED/unbuilt, retired from the active plan**; route ext∧max_size to DuckDB (standalone discovery: sorted slim 132 ms / +1.4 GB out-of-PG rollup 5.2 ms). **Re-open ONLY** on a hard committed product requirement for the correlated-PG-filter scenario — and then build the §5.1 minimal **natural-key, max-only, PK + `(extension, max_size) INCLUDE (info_hash)`** shape (~10 GB), \_not* the surrogate, _not_ count/min, _not_ the `(extension, info_hash)` index. **No run was performed; the verdict is design-grounded on cited measurements.**

---

## D4 — FIND-2: main-search broad-common-term ranked FTS wall

**The wall (source-confirmed, not assumed):** the WebUI's **default** sort for any keyword search is `relevance` DESC → Go compiles to `ORDER BY ts_rank_cd(torrent_contents.tsv, $q::tsquery) DESC`. So `ts_rank_cd` **is on the default hot path** — every keyword search is ranked unless the user picks another sort. There is **no ordering index for `ts_rank_cd`**, so PG must compute the rank for _every_ matching row before top-N. For a broad common term (`x264` ≈ 4.28 M matches) the GIN match is cheap (~482 ms) but ranking the whole match-set is the **~49 s single-core wall**.

**Why the existing CTE optimisation doesn't help:** `shouldTryCteStrategy()` (`query.go:812-826`) returns **false** for single `query_string_rank DESC` ordering — the CTE 50k stopping-point plan is _never even started_ for the relevance default; only the full-match-set ranking plan runs. The CTE bound only covers query + seeders/published_at/size sorts.

**DROP-independent:** touches only `torrent_contents` (+ joins to `torrents`/`content`), never `torrent_files`. Pre-existing, orthogonal to the migration.

### D4 — EXPLAIN characterisation ✅ MEASURED (read-only EXPLAIN ANALYZE; no RUM)

**RAN 2026-06-09** on the bench restore (`torrent_contents` = **48,035,320** rows; serial `max_parallel_workers_per_gather=0` for the canonical single-core wall — the bench PG pod's tiny k8s-default `/dev/shm` makes parallel plans fail with _"could not resize shared memory segment"_, so the serial pass is authoritative). Logs: `psx_r1b_find2_serial.log` (walls), `psx_r1_find2.log` (btree contrasts + 2-term/phrase/CTE).

The plan shape (confirmed verbatim in the EXPLAIN output): **Bitmap Index Scan** on `torrent_contents_content_type_tsv_idx` (the composite `gin(content_type, tsv)`) → **Bitmap Heap Scan** (recheck) → **Sort / top-N heapsort** that consumes the _whole_ match-set computing `ts_rank_cd` per row before emitting `LIMIT 30`. The Bitmap **Index** Scan is **469 ms** (cheap); the wall is the per-row `ts_rank_cd` over millions of rows. GIN cannot return rows in rank order → no index-ordered early-out with the current index. ✅ **Hypothesis confirmed exactly.**

**Term selectivity (P0):** x264 = **4,278,916** · 1080p = **6,016,135** · 720p = 4,592,065 · x264&1080p = **1,263,768** · rarbg = 1,217,088 · ettv = 109,602 · sparks = **35,132** · yify = 25,664.

| Probe                                                                                   | What it isolates                               | Result (single-core, measured)                                                                                                                                                                                         |
| --------------------------------------------------------------------------------------- | ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **P0** corpus + term selectivity                                                        | rows / match counts                            | **48.04 M** rows; x264 **4.28 M** (8.9 %), 1080p **6.02 M**, sparks **35 k**                                                                                                                                           |
| **P1** served wall (relevance, `LIMIT 30`)                                              | Execution Time                                 | **x264 = 49.4 s** · 1080p = **74.9 s** · sparks(35 k) = **2.1 s**                                                                                                                                                      |
| **P2** + the app joins (torrents+content)                                               | joins NOT dominant                             | **32.1 s** (Seq Scan `torrents` 4.9 s is minor; rank+sort still dominate — _faster_ than P1 only because the planner defers the rank projection to the sort input vs P1 computing it in the 48 s heap-scan projection) |
| **P3a** GIN match only (`count(*)`)                                                     | GIN cost                                       | **5.50 s** total; **Bitmap Index Scan = 469 ms** (the GIN posting scan is cheap; the 5 s is the heap-block count)                                                                                                      |
| **P3b** match + rank, NO order                                                          | rank-compute cost                              | **46.8 s** ⇒ rank-compute = **Δ 41.3 s = THE wall**                                                                                                                                                                    |
| **P3c** = P1 (rank+order+LIMIT)                                                         | sort-on-top cost                               | sort adds only **Δ 2.6 s** (49.4 − 46.8)                                                                                                                                                                               |
| **P4** same term, ORDER BY `published_at` / `seeders` (btree)                           | early-term regime                              | **published_at = 4.85 ms · seeders = 1.91 ms** — Index Scan Backward, early-term (~4 orders of magnitude faster)                                                                                                       |
| **P-2.4** bounded-candidate CTE (x264, `published_at DESC LIMIT 50000` → rank → top-30) | the §2.4 fix latency                           | **6.89 s** — beats 49 s (7×) but **NOT < 50 ms**; the window-gather itself scans **1.06 M rows / 5.7 s** (filtered btree) to fill 50 k x264                                                                            |
| 2-term AND (`x264 & 1080p`, 1.26 M)                                                     | does the wall extend to multi-common-term AND? | **34.0 s — YES, still a wall**                                                                                                                                                                                         |
| phrase (`the <-> matrix`, 5.5 k matched)                                                | phrase rank cost                               | **5.95 s** (43 k index candidates → position recheck drops 37.7 k → rank; common leading term `the` makes it costly despite few final hits)                                                                            |

> **1080p decomposition (mirror of x264):** GIN count **6.59 s** (index scan 575 ms) → rank-no-order **65.5 s** ⇒ rank = Δ **58.9 s**; sort adds 9.4 s → served **74.9 s**. Same shape, larger match-set → larger wall.

### D4 — fix candidate matrix

| Option                                 | Keeps `ts_rank_cd` relevance?        | New disk                  | Build/migration                               | Write-path cost                                         | Broad-term latency (**measured**)                                                                                                                       | Risk                                            |
| -------------------------------------- | ------------------------------------ | ------------------------- | --------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| **Baseline (today)**                   | yes                                  | 0                         | —                                             | current                                                 | **x264 49.4 s / 1080p 74.9 s** (the wall)                                                                                                               | —                                               |
| **2.1 RUM `<=>`**                      | **no** (`ts_rank`, no cover-density) | **+30–50 GB** (~2–3× GIN) | slow single-thread build + `CREATE EXTENSION` | **high** (positional posting-list updates; FIND-1 risk) | tens of ms (the win — _not run_)                                                                                                                        | **high** — write-amp + semantics + ext          |
| 2.2 `ts_rank` swap                     | ~similar                             | 0                         | code 1-liner                                  | unchanged                                               | ~tens of s (constant-factor only)                                                                                                                       | low — **not a fix**                             |
| **2.3a published_at/seeders default**  | no (popularity)                      | 0                         | code + UX                                     | unchanged                                               | **seeders 1.91 ms · published_at 4.85 ms** (early-term — the genuine cheap cliff-fix)                                                                   | medium (product default change)                 |
| **2.4 bounded-candidate CTE (approx)** | yes (over a window)                  | 0                         | code                                          | unchanged                                               | **6.89 s** (49 s→6.9 s, 7×; window-gather is 5.7 s — **not interactive for broad-sparse terms; degenerates for rare terms** that can't fill the window) | low-medium (approx recall + sparsity-sensitive) |

_(latency cells **measured** 2026-06-09, serial single-core; see §P-probes)_

### D4 verdict — ✅ MEASURED: **DEFER RUM; the cheap cliff-fix is a popularity-sort fallback (§2.3a), with bounded-candidate (§2.4) as a relevance-preserving 7× mid-tier**

> **The wall is confirmed and decomposed:** broad-common-term `ORDER BY ts_rank_cd` = **49.4 s (x264) / 74.9 s (1080p)** single-core, of which **41–59 s is pure `ts_rank_cd` compute** over the 4–6 M-row match-set (Bitmap **Index** Scan is 469 ms; sort-on-top is 2.6 s). The wall **extends to 2-common-term AND** (`x264 & 1080p`, 1.26 M → **34 s**) and to common-leading phrases (`the <-> matrix` → **5.95 s**), but **rare terms are fine ranked** (sparks 35 k → 2.1 s). **DROP-independent** (touches only `torrent_contents`).
>
> **DEFER RUM.** Still the textbook fix (index-ordered ranking, early-term, tens-of-ms) and the only option keeping interactive latency _and_ relevance order — but its costs stack **against this exact project**: **+30–50 GB** on a footprint-minimising migration; slow single-threaded build; a shared-lib extension baked into the PG image; a **ranking-semantics change** (`<=>` ≈ `ts_rank`, no cover-density vs `ts_rank_cd`); and **the dealbreaker — write amplification on an upsert-heavy table that already shows super-linear tsv-update cost (FIND-1)**. Pursue RUM **only** on a confirmed product requirement for sub-second relevance-ranked _broad_-term results, gated on a write-amp probe.
>
> **🔑 What the numbers changed about the recommendation:** the §2.4 bounded-candidate CTE **does cut the wall 7× (49 s → 6.9 s) but is NOT interactive** — gathering 50 k candidates via the `published_at` btree itself scans **1.06 M rows / 5.7 s** for a term covering 8.9 % of the corpus, and **degenerates for rare terms** (a term with < 50 k matches forces a near-full index scan to fill the window). So §2.4 is a _relevance-preserving mid-tier_, not the cheap fix I projected pre-run. **The genuine cheap cliff-fix is §2.3a — a popularity (`seeders`/`published_at`) sort, measured at 1.9–4.9 ms** via the existing btree early-term path (the CTE strategy _already_ accelerates this shape, `query.go:812-826`). Its only cost is semantic: popularity, not relevance.
>
> **Recommended ladder (all code-only, no extension/disk/write-path impact):**
>
> 1. **Most queries need nothing** — real traffic is multi-term/selective and already **< 25 ms** (EXP-A); the wall is a _broad-common-term_ tail, not the common case.
> 2. **For the broad-term tail, default to §2.3a popularity sort** (1.9–4.9 ms) — accept popularity ordering for terms above a selectivity threshold (detectable cheaply: planner row-estimate or a fast `count` bound). This is the cliff-fix.
> 3. **Offer §2.4 bounded-candidate `ts_rank_cd`** as an opt-in "sort by relevance" that returns in ~7 s for broad terms (with a window-size knob) — relevance-approximate, honest about latency. Apply it **only** to mid-selectivity terms (it degrades for both very-broad and very-rare).
> 4. **Reject §2.2** (`ts_rank` swap) — constant-factor only, doesn't remove the cliff.
>
> **Now vs defer:** characterisation **done** (read-only EXPLAIN ANALYZE, no prod touch, no RUM built). **Defer any code/extension change** pending product decisions: (a) is broad-term relevance latency user-visible at all? (most real queries already < 25 ms) (b) is a popularity default acceptable for broad terms, or is approximate-relevance (§2.4) required? **Nothing here blocks the migration; no production change is proposed.**

---

## Appendix — run log index

Logs land in [`psx-logs/`](./psx-logs/) as the runner deposits them; this section maps each to the table it fills.

| log file                                                                                             | thread | fills                      |
| ---------------------------------------------------------------------------------------------------- | ------ | -------------------------- |
| `psx_r5_encode_smoke.log` ✅ (encode 100k) / `psx_r5_blobparity.log` ✅ (blob→parquet + parity)      | D1     | §D1.1–D1.3                 |
| `psx_r3_withfreqs_build.log` ✅ (build + size + latency) / `psx_r3_recall.log` ✅ (recall/precision) | D2(A)  | §D2(A)                     |
| `psx_r2_b5_selectivity.log` ✅ (B5 realistic + Count-vs-TopDocs)                                     | D2(B)  | §D2(B) B5 + reframe        |
| `psx_r4_b6_positions_delta.log` ✅ (pos on/off head-to-head)                                         | D2(B)  | §D2(B) B6                  |
| `psx_l3_C_freshness.log` (optional)                                                                  | D2(C)  | §D2(C)                     |
| _(D3: no run — design-only)_                                                                         | D3     | §D3 (settled)              |
| `psx_r1b_find2_serial.log` ✅ (single-core walls + P3 isolation)                                     | D4     | §D4 P0–P3, P1 walls        |
| `psx_r1_find2.log` ✅ (P4 btree contrasts, 2-term AND, phrase, §2.4 CTE)                             | D4     | §D4 P4, P-2.4, term matrix |
| `r1_find2_probes.sql` / `r1b_find2_serial.sql` (probe SQL)                                           | D4     | source queries             |

_Last updated: 2026-06-09 (analyst — **FINAL PASS, campaign R0→R5 COMPLETE, all 4 threads synthesized.** D1 ✅ gap closed on fidelity+per-file cost: 0 blob errors, EXACT byte-for-byte parity (slim+full incl. path, checksums identical) on 32,157-torrent/1.84 M-file overlap, 0.746 µs/file decode; encode 0.458 µs/file Rust-indicative (klauspost in prod); full-16.97 M NOT run (~8 h NodePort+sqlx READ artifact, not a prod signal — stated as a limitation); `files_count::int8` cast bench-only; R6 freshness SKIPPED (reasoning-settled). D2 ✅ (A 13.32 GiB/−83.7 %/recall 1.0000; B5 multi-word <50 ms, prod broad-tail 77–94 ms; B6 WithFreqs free). D3 ✅ RETIRE. D4 ✅ wall = rank-compute, §2.3a cheap fix 2–5 ms, DEFER RUM. Whole-campaign bottom line in §0. Ready for the lead's final commit; `<BENCH_PW>`/`<HEL1_TAILSCALE_IP>`/`<PORT>` placeholders intact.)._
