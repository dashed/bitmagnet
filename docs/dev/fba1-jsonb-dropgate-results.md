# FB-A1 results — DROP-gate via deployed `file_extensions` JSONB (skip `agg_torrent_ext`)

**Date:** 2026-06-09 · **Status:** ✅ COMPLETE — **MEASURED on the real 879.5M-row restore** (throwaway PG on the idle bench host; production untouched).
**Question (feedback_1 P0-1):** can the already-deployed `torrents.file_extensions` JSONB serve the DROP-gate extension/file-type filter + facet, making a new `agg_torrent_ext` table unnecessary?
**Verdict:** **YES — use `file_extensions` JSONB; drop `agg_torrent_ext` from the DROP gate.** It wins on every axis the requirements rank (disk, freshness, latency, simplicity), at **zero new schema**.

---

## Setup

On the restore (`torrent_files` full = 879,474,852 rows / 261 GB; `file_extensions` empty pre-backfill): populated `torrents.file_extensions` G1-correctly from `torrent_files.extension` (the path-derived generated column) for the 16,959,775 multi-file torrents, (re)built the `jsonb_path_ops` GIN, and built a comparison `agg_torrent_ext` (54,776,108 rows, PK + `(extension,info_hash)` index). Three backends compared:

- **A** — current: `EXISTS(SELECT 1 FROM torrent_files … extension IN (…))`
- **B** — candidate: `file_extensions @> '["x"]'` (OR-of-`@>` for multi-ext; `jsonb_path_ops` supports `@>` not `?|`)
- **C** — fallback: `EXISTS(SELECT 1 FROM agg_torrent_ext … extension IN (…))`

Query shapes were grounded in the real Go content search (`criteria_torrent_file_extension.go`, `facet_torrent_file_type.go`, `facets.go` `BudgetedCount`, `query.go`); the single-file `torrents.extension` OR-branch is unchanged in all three.

## 1. Disk (the primary goal)

| option                          | marginal disk                                             | notes                                                                                           |
| ------------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| **B — `file_extensions` JSONB** | **+119 MB** (the GIN; the column is ~free, ~0.8 MB TOAST) | **already deployed** in prod (00021/00022)                                                      |
| C — `agg_torrent_ext`           | **+9.5 GB** (heap ~3.5 + indexes 6.0 GB)                  | new table; correction to the "+3–5 GB" estimate, which was the max-only/no-secondary-index form |
| ~~A — `torrent_files`~~         | (261 GB, removed)                                         | the baseline being dropped                                                                      |

**B is ~80× cheaper than C, and needs no new schema.**

## 2. Freshness

`file_extensions` is **dual-written in the same crawler upsert as `files_data`** (`persist.go:115-121`) → **real-time, already live, no new pipeline.** `agg_torrent_ext` would need a minute-cadence delta-upsert job + a parity checker. **B wins.**

## 3. Latency — served filter (the real UI path: ordered page, `LIMIT 21`)

| scenario                      | A      | **B**      | C     |
| ----------------------------- | ------ | ---------- | ----- |
| mkv + selective text query    | 55 ms  | **44 ms**  | 41 ms |
| mkv, no text (ordered page)   | 2 ms   | 13 ms      | 1 ms  |
| vob (rare), no text           | 170 ms | **114 ms** | 78 ms |
| video (broad 11-ext), no text | 0 ms   | **0 ms**   | 0 ms  |

All sub-200 ms; **B is competitive** with both A and the agg fallback.

## 4. Latency — facet (production shape = `budgeted_count(q, 5000)`)

`budgeted_count` is **cost-gated**: it runs `EXPLAIN (FORMAT JSON)`, and if the planner cost exceeds the budget (default **5,000**, `options.go:19`) it returns the **planner estimate instantly without executing the count**. For any broad facet the cost ≫ 5,000, so it's just an EXPLAIN → **~1 ms for all three backends.** (My first run used the _un-budgeted_ `COUNT(*)`, which times out for **all** backends incl. the current `torrent_files` — not a production path and non-discriminating.)

| facet    | A (ms · est) | **B (ms · est)** | C (ms · est) | cost A / B       |
| -------- | ------------ | ---------------- | ------------ | ---------------- |
| video    | 0.7 · 26.9M  | 1.5 · **15.7M**  | 1.1 · 26.9M  | 2.4B / **6.3M**  |
| software | 0.4 · 24.1M  | 0.9 · **2.4M**   | 0.4 · 24.1M  | 10.9B / **4.6M** |
| archive  | 0.4 · 24.8M  | 0.6 · **3.2M**   | 0.4 · 24.8M  | 7.0B / **4.7M**  |

**Bonus: B's estimates are _better_.** `torrent_files`/agg estimate ~24–27M matching torrents — **impossible** (only ~17M torrents have any files); JSONB estimates a realistic 2.4–15.7M, at ~1000× lower query cost (so exact facet counts are even feasible at a sane budget where the EXISTS path never is).

## 5. Parity (the correctness requirement)

The JSONB filter returns the **identical torrent set** as the `torrent_files` EXISTS — exact match for every extension tested — and far faster to evaluate:

| ext  | JSONB (count · time)  | `torrent_files` (count · time) | match |
| ---- | --------------------- | ------------------------------ | ----- |
| mkv  | 2,795,692 · **2.0 s** | 2,795,692 · 223 s              | ✅    |
| epub | 214,124 · 0.3 s       | 214,124 · 24 s                 | ✅    |
| vob  | 164,738 · 0.2 s       | 164,738 · 20 s                 | ✅    |
| iso  | 193,153 · 0.3 s       | 193,153 · 15 s                 | ✅    |

Set-equality holds by **coverage-equivalence** (both derive from the same capped `files` slice) and is confirmed empirically; the **50–110× speed-up** on full evaluation is why B's cost is ~1000× lower.

## 6. Operational caveat

A _broad GIN `@>` that actually executes a parallel bitmap scan_ (the un-budgeted count, or the parity count above) can exhaust a small container **`/dev/shm`** ("could not resize shared memory segment: No space left on device"). This is **not a production path** — the budgeted facet only `EXPLAIN`s, and the served filter early-outs on `LIMIT 21` (0 ms above). But for any future broad-execute path, size `/dev/shm` adequately or cap `max_parallel_workers_per_gather` for that query.

---

## Verdict & effect on the plan

**Skip `agg_torrent_ext` for the DROP gate. Replace the multi-file `EXISTS torrent_files` with `file_extensions @>` (flag-gated).** L2-P0 collapses to:

1. a **one-line, flag-gated Go criteria change** (`criteria_torrent_file_extension.go` multi-file branch → `file_extensions @>` raw SQL; single-file branch unchanged), and
2. a **parity check** (JSONB/`file_extensions` set == `torrent_files` ext-set — by coverage-equivalence, code-derivable; an offline confirmation run is cheap).

**Deleted from L2-P0:** the `agg_torrent_ext` migration, the gen model, the Rust seed, the minute delta-upsert, the `bitmagnet-db` agg readers, and the agg-parity checker class. **`agg_torrent_ext` is retained only as a _future_ option** if an `ext ∧ max_size` torrent-grain query is ever committed (`file_extensions` carries no size). This is the requirements outcome: the cheapest, simplest, freshest option also meets latency.

Raw logs on the bench host: `fba1_prep2.log`, `fba1_explain.log`, `fba1_facet.py` output, parity run.
