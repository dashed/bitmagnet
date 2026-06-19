# PS-D2-L3 — production-shape confirmation + p95-tail attack — SPEC

**Owner:** `psx-d2-l3` (team `bitmagnet-bench`) · **Date:** 2026-06-09 · **Status:** 🟦 SPEC (design-only; crate change drafted + `cargo check` green; nothing run on HEL1)
**Context:** L3 (per-torrent ngram free-text path index) is now a **GO** (user decision). PS-MB1 measured the GO on the *as-built* shape and *projected* the production shape. This spec closes the two residual gaps PS-MB1 left:
1. The production **`WithFreqs` 13.54 GiB** number was *computed by exact `.pos` subtraction, never built*. → **(A)** build it.
2. The broadest realistic-query **p95/p99 tail breaches 50 ms** (`ascii3` p95 58.6 / `ascii5` p95 64.4 ms). PS-MB1 asserted this is irreducible but did not attack it. → **(B)** attack it; decide reducible-vs-UX.
3. PS-MB1's freshness evidence (EXP-E) was **per-file**; L3 ships **per-torrent path-bag**. → **(C)** confirm supersession + incremental merge at path-bag granularity.

**Source of truth:** `/Users/me/aaa/github/bitmagnet`. **Crate:** `bench-file-index` (tantivy **0.26.1**). **Seed asset:** the existing `idx_pt_ngram_full` (~82 GB, `WithFreqsAndPositions`, full 16,973,470-torrent corpus) on HEL1 `bench-scratch` — seeds (B) immediately with no rebuild.

---

## 0. Headline (what this spec concludes before any run)

> **(A) Will confirm, not surprise.** `WithFreqs` drops only the `.pos` segment files; `.idx` (postings) + `.term` (term dict) are **byte-identical** to the positions-on build — verified in tantivy 0.26.1 source (`IndexRecordOption::has_freq` is `true` for both `WithFreqs` and `WithFreqsAndPositions`; only `has_positions` differs). So the build will land at **PS-MB1's 13.54 GiB ± a sliver**, recall **1.0000** (the conjunction query never reads positions), and the same p50/p95/p99 the positions-on index already showed. (A) is a *confirmation* — it removes the last "computed-not-built" caveat and gives a clean production artifact to hand to deploy.
>
> **(B) The broad-query p95 is IRREDUCIBLE at the tantivy-0.26.1 engine level. Verdict = UX, not engine.** Three of the four candidate mitigations are dead on source inspection, before measuring:
> - **rarest-gram-first conjunction** — **no-op.** `intersect_scorers` already `sort_by_key(|s| s.cost())` (`query/intersection.rs:31`): the intersection is *already* driven by the rarest gram. Reordering query clauses changes nothing.
> - **index-sort by seeders DESC + capped TopDocs** — **double dead-end.** (1) Index sorting was **removed from tantivy** (`CHANGELOG.md:98`, PR #2434) — `IndexSettings` in 0.26.1 has *no* `sort_by_field` (`index/index_meta.rs:214`, only docstore knobs). (2) Even query-time `TopDocs::order_by_fast_field` cannot early-terminate: a fast-field sort key has `requires_scoring() == false` (`sort_key/sort_key_computer.rs:113`) → `default_collect_segment_impl(.., with_scoring=false)` → `weight.for_each_no_score` → **full scan of the whole match-set**, every hit pushed to a top-K heap (`collector/mod.rs:185-215`, `query/weight.rs:101`). The only early-exit path, Block-WAND (`for_each_pruning`), fires **only** for BM25-`score` sort (`sort_key/sort_by_score.rs:41`) and the source comment says it benefits **unions**, not our pure conjunction (`query/weight.rs:114`).
> - **`SegmentCollector::collect` cannot abort** — confirmed: it returns `()` (`collector/mod.rs:302`); `for_each_no_score` iterates to `TERMINATED` with no break. There is no supported way to stop a scan after K hits.
>
>   The fourth, **min-chars 4/5**, is *already measured worse* (A2: `ascii4` p95 55.8, `ascii5` p95 64.4 ms) — longer substrings keep huge match-sets (`ascii3` *is* the floor). ⟹ **No engine lever moves the broadest-query p95 under 50 ms.** The only real reducers are **query selectivity** (real multi-word queries have far smaller match-sets than the synthetic worst-case single grams) and **client UX** (debounce, min-chars, a loading state, result caps). (B) MEASURES the two that *can* help — realistic multi-word selectivity, and the `WithFreqs`-vs-`WithFreqsAndPositions` query-latency delta — and records the rest as source-proven dead-ends so nobody re-litigates them.
>
> **🔑 Reframing the measured number:** PS-MB1's p95 was measured with the **`Count`** collector — the *cheapest possible* read (count postings, no doc materialization). Production needs `Count` (totalCount) **plus** `TopDocs::order_by_fast_field(seeders)` (the page), and the TopDocs path *also* full-scans (proven above) and adds heap work. So **the measured 24.71 ms p50 / 58.6 ms p95 is a LOWER BOUND on production latency, not an estimate of it.** This strengthens "irreducible → UX": you cannot even match the floor, let alone beat it, by adding a sort.
>
> **(C) Per-torrent freshness ≥ per-file (already-validated) freshness.** Supersession is `delete_term(info_hash)` + re-add **one** path-bag doc (vs ~52 file docs per-file); the match is torrent-granular, exactly EXP-B's anti-join analog. LogMergePolicy caps at ~17 M docs (vs 879 M per-file) → fewer/smaller segments, cheaper merges. EXP-E measured per-file supersession at **11 ms** and flat ~2 ms fresh-lag; per-torrent is strictly cheaper. (C) is a *sanity confirm*; an optional small extension measures the exact number.

---

## (A) — `WithFreqs` production-shape confirmation build

### A.1 What it proves

The PS-MB1 production size (13.54 GiB), recall (1.0000), and the production-shape latency tail were established by **subtraction + projection**, never by a real `WithFreqs` artifact. (A) builds the exact production index so all three are *measured on the shipping shape*, and yields a deployable index for the eventual sidecar.

### A.2 The crate change it needs (DRAFTED + `cargo check` green)

A `--no-positions` flag on the `recall` subcommand that flips the path field's `IndexRecordOption` from `WithFreqsAndPositions` → `WithFreqs`. Three small edits (full diff in §4):

- `schema.rs::build_recall_schema(tok, with_positions: bool)` — choose `WithFreqs` when `with_positions == false`.
- `main.rs::RecallArgs.no_positions` — the `--no-positions` flag (default `false` = back-compat).
- `main.rs::run_recall` — pass `!args.no_positions`; **guard**: reject `--no-positions` for `--tokenizer default|lindera` (their multi-token query is a `PhraseQuery`, which *requires* positions; ngram/edge-ngram query as a `BooleanQuery` of `Must` `TermQuery`s and never read positions).

`pathquery` needs **no** change — it opens the built index, reads the persisted schema, and constructs ngram queries with `IndexRecordOption::WithFreqs` already (`main.rs:1051`), which reads cleanly from a positions-off field.

### A.3 Build command (full corpus, production shape)

Single connection, `flock`-guarded, single-thread writer + 2 GB arena, force-merge to 1 segment (the PS-MB1 A2 protocol, **+ `--no-positions`**):

```bash
DSN='postgresql://postgres:<BENCH_PW>@127.0.0.1:30654/bitmagnet'
SCRATCH=/home/ansible/bench-scratch
flock -n /tmp/psx_l3.lock setsid bash -c "
  $SCRATCH/bench-file-index recall \
    --granularity per-torrent --tokenizer ngram --ngram-min 2 --ngram-max 3 \
    --no-positions \
    --source torrent-files --limit-docs 20000000 \
    --writer-threads 1 --writer-heap-mb 2000 --commit-interval 1000000 \
    --skip-truth \
    --index-path $SCRATCH/idx_pt_ngram_full_nopos \
    --postgres-dsn '$DSN' \
  > $SCRATCH/psx_l3_A_build.log 2>&1
"
```

(`--limit-docs` counts **torrents** in per-torrent mode; 20 M exhausts the 16,973,470-torrent corpus. `--skip-truth` = build + size + one warm latency pass, no in-RAM truth.)

### A.4 Success criteria (gates)

| gate | expectation | PASS rule |
|---|---|---|
| **G5-A size** | TOTAL ≈ **13.54 GiB** (no `.pos` files at all → `report_segment_bytes` "positions (path)" component = **0**) | TOTAL within **±3 %** of 13.54 GiB **AND** positions component == 0 |
| **build sanity** | docs == **16,973,470**, 1 segment, ingest ≈ 60–100 min | docs within 0.1 % of 16.97 M; segments == 1 |
| **postings invariant** | postings (`.idx`) B/doc ≈ **827.8**, term dict ≈ **15.7** (A2 values — must be unchanged by dropping positions) | both within ±2 % of A2 |

### A.5 Recall confirmation (separate WITH-truth run, fast)

```bash
flock -n /tmp/psx_l3.lock setsid bash -c "
  $SCRATCH/bench-file-index recall \
    --granularity per-torrent --tokenizer ngram --ngram-min 2 --ngram-max 3 \
    --no-positions \
    --source torrent-files --limit-docs 150000 --truth-cap 5000000 \
    --writer-threads 1 --writer-heap-mb 2000 \
    --queries-file $SCRATCH/queries_sweep.tsv \
    --index-path $SCRATCH/idx_pt_ngram_nopos_recall \
    --postgres-dsn '$DSN' \
  > $SCRATCH/psx_l3_A_recall.log 2>&1
"
```

**Gate:** ngram recall == **1.0000** on every group (must be *byte-identical* to the positions-on recall — the conjunction query never touched positions, so this is a tautology the run makes explicit).

### A.6 Latency on the production shape

```bash
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches   # warm-only if no root — note it
flock -n /tmp/psx_l3.lock setsid bash -c "
  $SCRATCH/bench-file-index pathquery \
    --tokenizer ngram --ngram-min 2 --ngram-max 3 --warm-reps 15 \
    --queries-file $SCRATCH/queries_sweep.tsv \
    --index-path $SCRATCH/idx_pt_ngram_full_nopos \
  > $SCRATCH/psx_l3_A_pq.log 2>&1
"
```

**Gate (informational, must match A2):** `ascii3` warm p50 ≈ 24.7 ms, `cjk3` ≈ 0.2 ms; tail `ascii3` p95 ≈ 58–60 ms. Any *material* divergence (>15 %) flags a build/measure problem, since postings are byte-identical.

---

## (B) — Tail-mitigation sweep

> **All four candidates analysed against tantivy 0.26.1 source FIRST (§0, §5). Verdict before measuring: the broadest-query p95 is engine-irreducible.** (B) measures the two axes that *can* move latency — real-query selectivity and the positions-on/off query delta — and empirically seals the dead-ends so the UX decision is defensible.

### B.1 Candidate matrix

| # | candidate | source verdict | runnable? | crate change |
|---|---|---|---|---|
| B1 | **min-chars 4 / 5** | already worse (A2 ascii4/5 p95 55.8/64.4) | yes (existing `idx_pt_ngram_full`) | none — query TSV only |
| B2 | **rarest-gram-first conjunction** | **no-op** — `intersect_scorers` already cost-sorts (`intersection.rs:31`) | n/a (nothing to change) | none |
| B3 | **stop-gram (drop commonest bigram)** | likely no-op + lossy (commonest gram is the cheap non-driver; dropping a `Must` term → false positives, recall-neutral but precision-down; driver unchanged) | optional | small (described, **not** built) |
| B4 | **index-sort seeders + capped TopDocs** | **double dead-end** — index-sort *removed* from tantivy (#2434); `order_by_fast_field` full-scans, no early-term | **not runnable** (engine lacks the feature) | none possible |
| B5 | **realistic multi-word selectivity** | the real reducer — multi-word queries → larger conjunction → smaller match-set | yes | none — query TSV only |
| B6 | **`WithFreqs` vs `WithFreqsAndPositions` query latency** | query reads only `.idx`; delta = page-cache pressure (82 GB vs 14 GB), not algorithmic | yes (A's index vs existing) | none |

### B2 Measurements to run (all on existing `idx_pt_ngram_full`, no rebuild unless noted)

**B1 — min-chars sweep.** Build TSV `queries_minchars.tsv` with groups `ascii3..ascii7`, `cjk3..cjk7` (extend the existing sweep generator to length 7). Run `pathquery --warm-reps 15`. **Expected:** p95 stays 50–65 ms across ascii3–5, *maybe* dips at ascii6–7 if real corpora thin out — but those are not the typeahead floor. **Decision:** min-chars does not rescue the gate.

**B5 — realistic-query selectivity (the load-bearing measurement).** Build TSV `queries_realistic.tsv` from real release-name fragments, e.g.:
```
ascii	1080p
ascii	x264
ascii	bluray
ascii	1080p bluray
ascii	x264 1080p
ascii	s01e01
ascii	flac 24bit
ascii	2160p x265 hdr
cjk	<a real 2-3 char CJK title fragment>
cjk	<a real CJK title + ascii year>
```
Run `pathquery`. **Hypothesis to confirm:** multi-token queries (the dominant real case) produce a conjunction of *many* grams → tiny intersection → **p95 well under 50 ms**; only the *bare single broad gram* (synthetic worst case) tails out. **This is the empirical heart of "irreducible-but-rare":** quantify what fraction of realistic queries actually breach 50 ms.

**B6 — positions on/off query delta.** Run the *same* `queries_sweep.tsv` against both:
- `idx_pt_ngram_full` (WithFreqsAndPositions, 82 GB)
- `idx_pt_ngram_full_nopos` (WithFreqs, ~14 GB, from (A))

**Hypothesis:** warm p50/p95 ~equal (postings byte-identical, positions never read); **cold** p95 *better* for nopos (14 GB resident vs 82 GB → less to fault in). Confirms `WithFreqs` is a free latency-neutral-or-better win on top of the 83 % size cut.

**B3 — stop-gram (OPTIONAL, only if B5 still shows a painful tail on real queries).** Would add a `--drop-commonest-grams N` knob to `build_path_query` that, after tokenizing, removes the `N` grams with the highest segment `doc_freq` from the `Must` conjunction. Source-predicted no-op (driver is the *rarest* gram; the commonest is already a cheap skip-list seek), and it converts exact-substring into approximate (precision loss). **Do not build unless B5 forces it.**

**B4 — index-sort: DO NOT ATTEMPT.** Not implementable in tantivy 0.26.1. Record the source citations (§5) and move on.

### B3 Success criterion (the (B) deliverable)

> **Does ANY mitigation get the broadest *realistic* query p95 < 50 ms?**
> - **If B5 shows real multi-word queries are < 50 ms p95** (expected): the gate is met *for real traffic*; the only > 50 ms cases are degenerate single-broad-gram queries → **mitigate in UX** (min 2–3 chars client-side, debounce ~150 ms, loading spinner, "showing top N of many"). **Verdict: irreducible at engine, solved at UX. Ship with UX guards.**
> - **If even realistic multi-word queries tail > 50 ms**: the honest product answer is **search-on-submit** (not per-keystroke) backed by this index — still ~25 ms median, just not promised per-keystroke. PS-T5's NO-GO-by-default framing already anticipated this.
>
> Either way **no tantivy-0.26.1 engine change reduces the broadest-query p95** — that conclusion is settled by source, not pending a measurement.

---

## (C) — Per-torrent freshness sanity

### C.1 What's already known (EXP-E, per-file) and why per-torrent inherits it

EXP-E (per-file, default LogMergePolicy) measured: live dual-write **fresh-lag ~2 ms flat** (commit→reload→searchable), segments **bounded** (29→17–21, no fan-out), supersession via `delete_term(info_hash)` + re-add + commit + reload **11 ms** (old fileset gone). Per-torrent path-bag is *strictly more favorable*:

- **Supersession** deletes by `info_hash` and re-adds **one** path-bag doc (vs ~52 file docs). The delete-key is torrent-granular — exactly the EXP-B latest-wins anti-join, in inverted-index form. Re-crawl replaces the whole fileset (`files_data` is upsert-with-update, not pure-append), which is precisely what one path-bag doc per info_hash models.
- **Incremental merge** operates over ~17 M docs (vs 879 M per-file) → fewer, smaller segments, cheaper background merges; the per-doc token load is higher (a torrent's whole path-bag) but bounded.

⟹ **Per-torrent freshness is at least as good as the already-validated per-file numbers.** This is a reasoning-confirmed sanity, not an open risk.

### C.2 Optional measurement (small extension — SPEC only, not drafted)

If the lead wants the exact per-torrent number, extend the `freshness` subcommand minimally:
- add `--granularity per-file|per-torrent` (reuse the `recall` path-bag grouping loop);
- give the per-torrent freshness schema an `info_hash` **indexed-bytes** delete key (the `recall` schema currently has none — it carries only `path` + `ident`), so `delete_term(info_hash)` works;
- supersede a real multi-file torrent: assert post-reload its old path-bag is gone and the new one (with a different fileset) is searchable; record fresh-lag + supersession ms.

**Expected:** fresh-lag ~2 ms, supersession ≤ 11 ms (fewer docs than per-file), bounded segments. **Gate:** old fileset's distinctive substring returns 0 hits post-supersession; new fileset's substring returns 1. **Priority: LOW** — reasoning already settles (C); measure only on explicit go.

---

## (D) — Run protocol (HEL1, single-connection, design-safe)

🚨 **DESIGN-ONLY for this agent.** The commands below are the runner's protocol; this agent does not execute them.

- **Host:** HEL1 via **tailscale** `ansible@<HEL1_TAILSCALE_IP>` (public-IP SSH `<HEL1_PUBLIC_IP>` is flaky 255/124; **maple-bastion ProxyJump FAILS** — `AllowTcpForwarding no`). SSH key: `ssh -o IdentityAgent=none -i ~/.ssh/id_ed25519`.
- **ONE connection at a time, gentle pollers.** ControlMaster / tight pollers trip HEL1 sshd. No concurrent runs.
- **`setsid` launches survive client-side SSH timeouts** — a rc=124/255 "fail" can still have *landed* the process. **Guard every launch with `flock -n /tmp/psx_l3.lock` + a `pgrep -f bench-file-index` precheck** to avoid duplicate concurrent writers colliding on the same index dir.
- **Single-thread writer + ≥2 GB arena** (`--writer-threads 1 --writer-heap-mb 2000`) is mandatory for ngram at scale — the multi-thread 256 MB default starves per-thread arenas → "index writer killed" (EXP-D2 crash). Confirmed required by PS-MB1.
- **drop_caches** needs root; if unavailable, run **warm-only** and note it (cold numbers then unavailable, p50 warm still valid).
- **Bench env is throwaway** (879.5 M-row pre-blob-backfill restore; `torrent_files` source; production FSN1 untouched). DSN `postgresql://postgres:<BENCH_PW>@127.0.0.1:30654/bitmagnet`. Indexes live at `/home/ansible/bench-scratch/`. **RUN-6 teardown still pending user OK** — do not tear down.
- **Disk check before (A):** the WithFreqs build is ~14 GB; `idx_pt_ngram_full` (~82 GB) + `idx_pf_ngram` (5.5 G) + others already on scratch. Confirm free space before launching.

---

## (E) — Crate change (drafted; `cargo check` PASSED)

`cargo check` on `bench-file-index` is **green** with these edits. Diff summary:

### `bench-file-index/src/schema.rs`
`build_recall_schema(tok)` → `build_recall_schema(tok, with_positions: bool)`; the path field's index option becomes `WithFreqs` when `with_positions == false`, else `WithFreqsAndPositions` (unchanged default). Postings/term-dict are byte-identical between the two (`IndexRecordOption::has_freq` true for both); only `.pos` is dropped.

### `bench-file-index/src/main.rs`
- `RecallArgs`: new `#[arg(long, default_value_t = false)] no_positions: bool`.
- `run_recall`: guard `bail!` if `--no-positions` with `--tokenizer default|lindera` (PhraseQuery needs positions); then `build_recall_schema(args.tokenizer, !args.no_positions)`; build banner reports the positions mode.

**Back-compat:** default (`no_positions=false`) reproduces the exact prior `WithFreqsAndPositions` behaviour — all existing PS-MB1 / EXP-D commands are unaffected. `pathquery` needs no change (reads persisted schema; ngram query already uses `WithFreqs` term reads).

---

## (F) — Tantivy 0.26.1 source verification (claims → file:line)

All checked against the vendored crate `~/.cargo/registry/src/index.crates.io-*/tantivy-0.26.1`.

| claim | evidence |
|---|---|
| `WithFreqs` drops `.pos`, keeps `.idx`+`.term` identical | `src/schema/index_record_option.rs` — `has_freq()` true for both `WithFreqs` & `WithFreqsAndPositions`; `has_positions()` true *only* for `WithFreqsAndPositions`. So positions live in their own `.pos` files; postings/term-dict are written identically. ⟹ `WithFreqs total == positions-on total − .pos`. |
| ngram conjunction never needs positions | query built as `BooleanQuery` of `Must` `TermQuery(.., WithFreqs)` (`main.rs:1042-1060`) — no `PhraseQuery`, no position reads. |
| **rarest-gram-first is a no-op** | `src/query/intersection.rs:31` — `scorers.sort_by_key(|scorer| scorer.cost())` before building the leapfrog. Intersection already driven by the cheapest (rarest) scorer. |
| **fast-field-sorted TopDocs does a full scan (no early-term)** | `src/collector/sort_key/sort_key_computer.rs:113` default `requires_scoring() == false`; `:117` `collect_segment_top_k` → `default_collect_segment_impl(.., with_scoring=false)`; `src/collector/mod.rs:185-215` → `weight.for_each_no_score`; `src/query/weight.rs:101-110` iterates the whole docset via `for_each_docset_buffered` (no break). |
| Block-WAND only for BM25 score, benefits unions | `src/collector/sort_key/sort_by_score.rs:41,50` — only the score path calls `weight.for_each_pruning`; `src/query/weight.rs:114` doc: pruning "makes it possible for scorers to implement … BlockWAND for **union**." Our query is a conjunction sorted by a fast field → never enters this path. |
| **`SegmentCollector::collect` cannot signal abort** | `src/collector/mod.rs:302` `fn collect(&mut self, doc, score)` returns `()`; no `TerminationReason` exists (grep: 0 hits). |
| **index sorting is REMOVED from tantivy 0.26.1** | `CHANGELOG.md:98` "remove index sorting [#2434]"; `src/index/index_meta.rs:214` `struct IndexSettings` has only `docstore_*` fields — no `sort_by_field` / `IndexSortByField` (grep across `src/`: 0 hits). The 0.20-era "Top-n optimization on sorted index" (#1026) is gone with it. |

**Conclusion:** every proposed engine-level p95 mitigation is either already done by tantivy (B2), unsupported by this tantivy version (B4), or unable to reduce a full-match-set conjunction count (B1/B3). The broad-query p95 is **engine-irreducible**; the remaining levers are **query selectivity** (B5, measure) and **client UX** (debounce / min-chars / loading-state / result caps).

---

## (G) — Deliverable summary for the lead

- **(A)** `--no-positions` flag drafted + `cargo check` green (§E). Build the full-corpus `WithFreqs` per-torrent path-bag index (§A.3) → confirm **13.54 GiB / recall 1.0000 / p50≈25 ms** on the real production shape. Yields a deployable artifact.
- **(B)** Broad-query p95 is **source-proven engine-irreducible** (§0, §F). Run only B5 (realistic multi-word selectivity — the real reducer) + B6 (positions on/off query delta — confirm `WithFreqs` is latency-neutral-or-better). Decision: **UX guards** (debounce + client min-chars + loading state + result caps), not an engine change.
- **(C)** Per-torrent freshness **inherits** the validated EXP-E per-file numbers and is strictly cheaper (1 doc/torrent supersession). Optional small `freshness --granularity per-torrent` extension specced but not drafted — LOW priority.
