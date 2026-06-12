# L2 sorted layout — production results (the external sort, and what it traded away)

**Date:** 2026-06-11 · **Status:** measured in production. External sort shipped as image `l2-9` (fork `5b36aab4`); batch-probe fix shipped as image `l2-10` (fork `48d9c73b`, digest `sha256:b8e68654f0450889ec205137c395ac15e4c554b5d7487b3dd6ef5a36fac4a049`).
**Closes:** dv2 stub #3 (the spilling external sort). **Parent:** [`l2-verify-and-shadow-runbook.md`](./l2-verify-and-shadow-runbook.md) · ARCH-C ([`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md), the design's source).

## What ran

`--sort external` (`bitmagnet-parquet`, feature `duckdb-sort`): the base fact streams
unsorted, then — inside the still-unpublished generation dir — a DuckDB
`COPY … ORDER BY extension, size` rewrites it (spill to `.sort-tmp` on the PVC,
ZSTD, row-group 1 M, `preserve_insertion_order=false`; rename-swap before the
atomic publish). Knobs `BITMAGNET_SORT_MEMORY=8GB`, `BITMAGNET_SORT_THREADS=8`.

**Production compact (883,130,584 rows): 26 min total — 20.5 min scan + 5.7 min sort.**
Verified on disk: 882 row groups, `extension` min/max **strictly monotonic** (the
pruning precondition). Cost: the fact grew **13 G → 21 G** (sorting decorrelates
`info_hash`/`path` → RLE loss; RUN-2 predicted exactly this).

## Measured through the production sidecar (warm, threads=4, live data)

| shape | unsorted | sorted | note |
|---|---|---|---|
| `find ext∧size` (mkv>1 GB top-100) | 11.2 s | **0.75 s** | ✅ the 10 s-deadline item, fixed |
| `find` two-sided range | 13.6 s | **2.3 s** | ✅ |
| `find` NULL-ext bucket | 13.3 s | **3.4 s** | ✅ |
| `count files` (flac) | 1.6 s | **0.99 s** | ✅ (raw DuckDB probe: 14 ms) |
| `count torrents` (mkv>4 GB) | 0.57 s | **0.54 s** | rollup-served, unchanged |
| facets | 1.3–4.3 s | **1.4–2.1 s** | rollup-served |
| **collapse (ext-only / size)** | 3–5.7 s | **10.8–21 s** | ❌ regressed |
| **collapse path~** | 71 s | **582 s** | ❌ badly (L3-carve-out shape) |

Raw zone-map wins at the storage layer (DuckDB probes, threads=4): `count flac`
**14 ms**, `count mkv>4G` **20 ms**, `find mkv>1g top-100` **341 ms** — the
ARCH-C numbers (`collapse 1311→132 ms`, `count 1024→17 ms` class) reproduced.

## The regression mechanism (singular)

Sorting by `(extension, size)` **destroyed `info_hash` locality**: previously the
fact sat in info_hash-keyset order, so a per-torrent probe (`info_hash = ?`)
touched ~1 row group via zone-maps; now every probe scans all 882 (~0.4 s each).
The collapse path runs **up to 50 such probes per request** — the per-group
*preview* queries (even on the pure-rollup `collapse:flac`, which never needs the
fact otherwise) and the exact-*hydration* queries on the `size_min` path:
50 × ~0.4 s ≈ the observed ~20 s. `collapse:path` adds a GROUP BY over
maximally-scattered hashes on the bigger file (582 s) — path shapes are the L3
carve-out regardless.

Confirmed structural, not cold-cache: an immediate warm second run reproduced
the numbers within noise.

## Fix (`l2-10`, contained sql.rs/duck.rs/service.rs change)

**Batch the probes — one scan instead of fifty:**
1. Hydration: one `… WHERE info_hash IN (?,…50) AND <pred> GROUP BY info_hash`.
2. Previews: one windowed query — `row_number() OVER (PARTITION BY info_hash
   ORDER BY size DESC, file_index) <= preview_limit` *after* the `IN`-filter
   cuts the scan (the window then runs over ≤50 torrents' files, so the EXP-B
   window-function caveat doesn't apply).

Expected: collapse returns to the ~1–3 s class while keeping every find/count
win. Alternatives considered and deferred: a second info_hash-sorted slim fact
(+4–10 G, only if point-grain lookups become a served need — G2 already serves
per-torrent hydration from the blob in Go); dropping sidecar previews outright
(`clamp_preview` floors at 1, and the proto promises them).

### l2-10 live result (2026-06-11)

Built on FSN1 and deployed to HEL1 as
`ghcr.io/dashed/bitmagnet-filesearch:l2-10@sha256:b8e68654f0450889ec205137c395ac15e4c554b5d7487b3dd6ef5a36fac4a049`.
The first l2-10 delta tick completed cleanly:
`torrents_ok=7673 decode_errors=0 file_rows=316633 padding_rows=11497 agg_ext=701 agg_torrent_ext=16437 tombstones=8073 clean=true`.

Live `v2-shadow` subset (12 pairs, `collapse:path` excluded; temporary 240s
deadline restored to 10s afterward) confirms the batch-probe fix:

| shape | equality | sidecar |
|---|---:|---:|
| `collapse:mkv>1g` | ✅ | **1.498 s** |
| `collapse:flac` | ✅ | **1.744 s** |
| `collapse:smallmkv` | ✅ | **0.401 s** |
| `find:mkv>1g` | ✅ | 0.747 s |
| `find:range` | ✅ | 2.204 s |
| `find:nullext` | ✅ | 3.222 s |
| `count:flac-files` | ✅ | 0.947 s |
| `count:flac-torrents` | ✅ | 0.440 s |

Strict live gate result: `pairs=12 mismatches=3 gate=FAIL`, for expected live
reasons only: the known `facet:video` `avi +10` dup-path residue, plus two
±1 freshness drifts on moving prod data (`count:mkv>4g-torrents`,
`facet:>1g`). A frozen-snapshot run is still the strict DROP gate; this live
run is the post-rollout parity/perf smoke.

## Watchdog bug (separate, production-relevant)

The 582 s collapse ran under a **240 s** test deadline *uninterrupted* — the
`InterruptHandle` watchdog (`duck.rs::with_conn`) did not kill it. At the 10 s
production deadline the same mechanism DID fire earlier in the day (the
pre-sort `find:path` abort), so the failure is shape- or duration-dependent —
possibly the interrupt handle's interaction with `try_clone`'d connections, or
DuckDB only polling interrupts at certain pipeline boundaries (the hash
aggregate + spill phase). **The deadline is the sidecar's production
protection; needs a repro + fix.** Tracked as a follow-up.

## Operational notes

* The two live-run "mismatches" are fully understood: the known +10 dup-path
  avi residue ([runbook §5](./l2-verify-and-shadow-runbook.md)) and ±1–6-file
  crawl drift inside the ≤2 min freshness window — expected off-snapshot.
* Generation housekeeping is now a real item: each compact leaves the previous
  ~13–21 G base behind, and the minute delta adds ~1,440 tiny dirs/day — prune
  non-current dirs after compacts (manual today; automate in `publish()`).
* The production deadline (10 s) was restored after the measurement runs.
