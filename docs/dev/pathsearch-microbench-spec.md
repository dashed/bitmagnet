# PS-MB1 — per-torrent path-bag micro-bench (the one gated experiment behind the L3 gate)

**Date:** 2026-06-09 · **Status:** 📋 DRAFT — code ready, **NOT run** (gated on PS-T5 G3/G5).
**Owner question it resolves:** the two unknowns the whole L3 path-search decision turns on —

- **G3 (latency):** does the per-torrent ngram index clear **<50 ms warm p50 on the broadest 3-char prefix** (the per-keystroke worst case EXP-D2 left open at the per-file granularity)?
- **G5 (size):** is the per-torrent index **materially under the 94 GB per-file ceiling** (so it fits the HEL1 PVC and doesn't triple the footprint)?

If both clear, the L3 carve-out becomes an "acceptable add-on" rather than a "triples the footprint" liability. If either fails, prefer the cheaper edge-ngram arm or hold at NO-GO. **This experiment is the FIRST spend if the gate ever opens — it is read-only and touches no production system.**

Cross-refs: [`pathsearch-T3-index-design.md`](./pathsearch-T3-index-design.md) (the design this measures), [`pathsearch-T5-decision.md`](./pathsearch-T5-decision.md) (the gate), [`cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`](./cjk-tokenizer-and-incremental-merge-bench-RESULTS.md) (EXP-D/D2/E baseline this extends).

---

## 1. What's new in the harness

Extends the existing `bench-file-index` crate (the EXP-D/D2/E tool) with two reviewable additions — **no new infra, no new data source**:

1. **`--granularity per-file | per-torrent`** on the `recall` subcommand (`main.rs`). `per-torrent` groups consecutive `torrent_files` rows by `info_hash` (relying on the keyset scan's `ORDER BY info_hash, "index"`) and emits **one path-bag doc per torrent**: every file path added as a _separate value_ of the one `path` field, so each value is tokenized independently and **no boundary grams span two files**. Identity/delete key = `info_hash` only. Truth becomes the OR over the fileset (a torrent matches a query if ANY of its paths contains it).
2. **`--tokenizer edge-ngram`** (`schema.rs`, arm C). A custom `PerWordEdgeNgram` tokenizer: splits on non-alphanumerics, emits **per-word edge-grams (prefixes)** for ASCII words and **full sliding char-ngrams** for CJK runs (routed per code point). This is the cheaper ASCII-prefix-typeahead arm PS-T3 flagged as _unmeasured_; it sidesteps the stock-`prefix_only` "anchors at offset 0 of the whole path" trap. Recommended build width `--ngram-min 2 --ngram-max 12` (wide max = real prefix discrimination).

Both reuse the existing single-thread-writer + `--writer-heap-mb 2000` path (the EXP-D crash fix), the `--skip-truth` broad-sweep mode, the `report_segment_bytes` size attribution, and the `pathquery` cold/warm latency subcommand — so the numbers are directly comparable to EXP-D2.

**Prefix-sweep query set:** [`bench-file-index/queries/ps_prefix_sweep.tsv`](../../bench-file-index/queries/ps_prefix_sweep.tsv) — broadest substrings grouped by `charset×length` (`ascii2/3/4/5`, `cjk2/3/4`). The `ascii3`/`cjk3` rows are the **gate rows** (min-chars=3 is the design floor); 2-char rows quantify what min-chars buys; 4/5-char rows show the selectivity gradient.

---

## 2. The arms (all on the existing 879.5 M-row HEL1 restore, ~50 M-row slice)

| arm               | granularity | tokenizer        | `--limit-docs`                          | answers                                                                     |
| ----------------- | ----------- | ---------------- | --------------------------------------- | --------------------------------------------------------------------------- |
| **A (PRIMARY)**   | per-torrent | ngram(2,3)       | `965000` torrents (≈ first ~50 M files) | **G3 + G5** for the recommended design                                      |
| **B (control)**   | per-file    | ngram(2,3)       | `50000000` files                        | reproduces the EXP-D2 baseline at 50 M — anchors the comparison             |
| **C (secondary)** | per-torrent | edge-ngram(2,12) | `965000` torrents                       | the cheaper ASCII-prefix arm — does it beat A on size while staying <50 ms? |

`965000 ≈ 50 000 000 / 51.79` (the measured avg files/torrent), so A and B cover ~the same first ~50 M rows of the corpus and **extrapolate to the full corpus by the same ~17.6× factor** that EXP-D2 used (50 M → 879.5 M / 17 M-torrent). Report the extrapolated full-corpus index size as `measured_total_bytes × (16 992 238 / torrents_indexed)`.

---

## 3. Exact run plan (gated — do NOT run until G-gate opens)

> **Safety preconditions (identical to EXP-D/D2/E):** read-only on the throwaway HEL1 restore (`torrent_files` only; production FSN1 untouched); ONE serial run, ONE ssh connection at a time; single-thread writer + 2 GB arena (the ngram multi-thread writer **crashes**); orchestrate under a lock + `pgrep` guard because `setsid` launches survive client-side ssh timeouts (a rc=124 "fail" can still land → duplicate concurrent writers). Connect via the HEL1 **tailscale** IP (the public IP ssh is flaky); never via the maple bastion (AllowTcpForwarding off). `drop_caches` for the cold read needs root (`sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'`); if unavailable, report warm-only and note it.

Build (per arm; `BITMAGNET_POSTGRES_*` point at the bench PG NodePort DSN):

```bash
# Arm A — per-torrent ngram(2,3): the GO/NO-GO build
bench-file-index recall --source torrent-files \
  --granularity per-torrent --tokenizer ngram --ngram-min 2 --ngram-max 3 \
  --limit-docs 965000 --writer-threads 1 --writer-heap-mb 2000 \
  --skip-truth --queries-file queries/ps_prefix_sweep.tsv \
  --index-path /home/ansible/bench-scratch/idx_pt_ngram

# Arm B — per-file ngram(2,3): control (≈ EXP-D2 @50M)
bench-file-index recall --source torrent-files \
  --granularity per-file --tokenizer ngram --ngram-min 2 --ngram-max 3 \
  --limit-docs 50000000 --writer-threads 1 --writer-heap-mb 2000 \
  --skip-truth --queries-file queries/ps_prefix_sweep.tsv \
  --index-path /home/ansible/bench-scratch/idx_pf_ngram

# Arm C — per-torrent edge-ngram(2,12): the cheaper ASCII-prefix arm
bench-file-index recall --source torrent-files \
  --granularity per-torrent --tokenizer edge-ngram --ngram-min 2 --ngram-max 12 \
  --limit-docs 965000 --writer-threads 1 --writer-heap-mb 2000 \
  --skip-truth --queries-file queries/ps_prefix_sweep.tsv \
  --index-path /home/ansible/bench-scratch/idx_pt_edge
```

`--skip-truth` builds + force-merges + reports **path-field bytes/doc and total index size** + one warm pass per query (avg hits + p50/p95/p99) — the size half of the gate, cheaply, at scale.

Latency (cold-first + 15 warm reps, after `drop_caches`; match `--ngram-*` to the build):

```bash
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
bench-file-index pathquery --index-path /home/ansible/bench-scratch/idx_pt_ngram \
  --tokenizer ngram --ngram-min 2 --ngram-max 3 \
  --queries-file queries/ps_prefix_sweep.tsv --warm-reps 15
# …repeat per arm (edge-ngram arm uses --tokenizer edge-ngram --ngram-max 12)
```

A **separate recall run WITH truth** (drop `--skip-truth`, cap `--limit-docs ~20000000` files / proportional torrents, keep `--truth-cap 5000000`) confirms **per-torrent ngram recall stays 1.0** (grouping must not change tokenization) and that edge-ngram's ASCII recall/precision are acceptable — a correctness sanity gate before trusting the latency numbers.

---

## 4. GO / NO-GO

Report a table of `(arm × charset × prefix-length) → avg hits, cold/warm p50/p95/p99, path bytes/doc, extrapolated full-corpus index GB`. Then:

- **GO (build L3 as per-torrent ngram, arm A)** iff **`ascii3` AND `cjk3` warm p50 < 50 ms** on arm A **AND** arm A's extrapolated full-corpus index is **materially under 94 GB** (target **< ~30 GB**, i.e. the per-torrent shrink is real). This flips G3+G5 to PASS and the L3 add-on from "triples the footprint" to "acceptable."
- **GO-CHEAPER (arm C)** if arm C clears the same latency bar at a **smaller** index than A — then ship ASCII edge-ngram typeahead + degrade CJK to submit-time substring (PS-T5 option (b)), the most defensible cost-down.
- **NO-GO / hold** if neither per-torrent arm clears `ascii3`/`cjk3` < 50 ms warm p50 — then min-chars=3 + debounce cannot rescue per-keystroke and the honest product answer is **search-on-submit** (DuckDB-FTS ~150 ms), not a +90 GB index. PS-T5's NO-GO-by-default stands.

Whatever the result: this index is **purely additive and never gates the `torrent_files` DROP** (PS-T5 G4). The micro-bench only decides whether the _optional_ L3 layer is worth building if/when a real product demand (G1) and an in-prod ILIKE-wall (G2) ever materialize.

---

## 5. Status of this draft

- ✅ Harness code drafted in `bench-file-index` (`--granularity`, `PerWordEdgeNgram` edge-ngram arm, per-torrent path-bag grouping, OR-truth). Reviewable diff; **compile-checked locally**.
- ✅ Prefix-sweep query set committed.
- ⛔ **Not executed.** Runs are gated on the PS-T5 gate opening; even then, run under the §3 safety protocol on the throwaway HEL1 restore only. The bench env teardown (RUN-6) is still pending — this can run before teardown if the gate opens first, else it needs a fresh gated restore.
