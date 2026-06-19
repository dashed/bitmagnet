# psx-D3 — Finalize `agg_torrent_ext` for the ONE query it survives for: **ext ∧ max_size**

**Owner:** `psx-d3-agg` (team `bitmagnet-bench`)
**Date:** 2026-06-09
**Status:** SPEC (DESIGN-ONLY — no execution; read-only against the HEL1 bench restore is what this *prescribes*, but nothing here was run. "Do not drop `torrent_files` until each replacement layer is proven in prod" remains in force.)
**Parents / supersedes-context:**
- [`L2-P0-agg-torrent-ext-and-checker-spec.md`](./L2-P0-agg-torrent-ext-and-checker-spec.md) — the original agg spec (now 🛑 SUPERSEDED for the DROP gate).
- [`fba1-jsonb-dropgate-results.md`](./fba1-jsonb-dropgate-results.md) — **FB-A1 MEASURED**: `torrents.file_extensions` JSONB wins the plain-ext gate (+119 MB vs agg's +9.5 GB), so agg is **dropped from the DROP gate** and **retained ONLY** as a future option for `ext ∧ max_size` (JSONB carries no size). *This doc finalizes that residual option.*
- [`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md) — **ARCH-C MEASURED** the DuckDB-Parquet equivalent of this exact query (see §4).
- [`run3-pg-sizing-bench.sql`](./run3-pg-sizing-bench.sql) — RUN-3 partial sizing (this doc tightens it to the *correct* index for the size predicate).

---

## 0. The one question this spec closes

> Is a PG `agg_torrent_ext` rollup **worth its disk** to serve the torrent-grain
> **ext ∧ max_size** query — *"torrents that have at least one file of extension X
> whose largest file of that ext exceeds N bytes"* (e.g. **"torrents with an `.mkv` > 1 GB"**) —
> or should that query be served from the **DuckDB-Parquet tier**, which already
> carries per-file size at **zero new PG disk**?

This is the **last** decision keeping `agg_torrent_ext` alive. Everything else it
was proposed for (the file-type filter + facet on the DROP gate) was won by the
already-deployed `file_extensions` JSONB in FB-A1. The experiment below produces
the disk + latency numbers to either (a) commit a **minimal** agg shape, or (b)
**retire `agg_torrent_ext` entirely** and serve ext∧max_size from DuckDB.

**Adversarial prior (to be confirmed/refuted, not assumed):** agg is PG disk the
entire DROP project exists to shed (drop `torrent_files` ≈ −245 GB). Re-adding
3–10 GB to PG **plus** a dual-write delta pipeline **plus** a parity checker, to
serve **one not-yet-committed query** that DuckDB already answers in 5–132 ms,
needs a strong, query-shape-specific justification to survive. The bar is high.

---

## 1. Why JSONB cannot serve it (and what *can*)

`torrents.file_extensions` is a presence set — `["mkv","srt","nfo",…]` — dual-written
in the crawler upsert (`persist.go:115-121`), GIN-indexed (`jsonb_path_ops`), +119 MB.
`file_extensions @> '["mkv"]'` proves the torrent **has** an mkv, but carries **no
size**, so it cannot answer "…with an mkv **> 1 GB**". That single gap is the entire
reason agg might still earn its place.

Three backends *can* answer ext∧max_size. The experiment pits them head-to-head:

| id | backend | source of per-file size | new PG disk | composes into main PG search? |
|----|---------|--------------------------|-------------|-------------------------------|
| **A** | `EXISTS torrent_files … size > N` | the table **being dropped** | 0 (but it's the 261 GB we're removing) | ✅ native (baseline / source-of-truth for parity) |
| **B** | `EXISTS agg_torrent_ext … max_size > N` | rollup `max(size)` per (torrent,ext) | **+3–10 GB** (the variable under test) | ✅ native correlated EXISTS, bytea-keyed |
| **D** | DuckDB-Parquet (`files_slim`/agg Parquet) | per-file `size` column (already there) | **0** | ❌ cross-engine — must hand an `info_hash` set back to PG |

A is the **baseline** (parity truth) but is structurally disqualified — it reads the
table the DROP removes. The real contest is **B (agg, +disk) vs D (DuckDB, +0)**.

---

## 2. Query shape — grounded in the live Go criteria

The existing file-type filter is a correlated `EXISTS` OR-chain over `torrent_files`,
composed into the main `torrent_contents` search (`criteria_torrent_file_extension.go:24-34`):

```go
gen.Exists(q.TorrentFile.Where(
    q.TorrentFile.InfoHash.EqCol(q.Torrent.InfoHash),
    q.TorrentFile.Extension.In(extensions...)))
```

The **single-file** OR-branch (`torrents.extension`) is unchanged in every option.
ext∧max_size is the **multi-file branch**, extended with a size predicate. There is
**no existing Go surface** for it (confirmed: `grep max_size internal/**/*.go` → only
the log rotator) — it is a **future** query, not a committed requirement. The
candidate B predicate (multi-file branch) is:

```sql
EXISTS (SELECT 1 FROM agg_torrent_ext a
        WHERE a.info_hash = torrents.info_hash
          AND a.extension = $ext
          AND a.max_size  > $threshold)
```

Two **directions** the planner can take this, and **both must be measured** because
they exercise different indexes and decide the minimal shape:

- **Direction-1 (probe / text-first):** the outer query is already selective (a text
  query + content_type narrowed `torrents` to a few-k rows); the EXISTS is a
  per-torrent **PK probe** `(info_hash, extension)` → fetch `max_size` → compare.
  *No secondary index needed.* This is the dominant **UI** path (filter composed
  with a search box).
- **Direction-2 (semi-join / filter-first):** no text query — ext∧size *is* the
  selective predicate and must **drive** the scan: range `extension=$ext AND
  max_size>$N` → emit `info_hash`s → semi-join `torrents`. *Needs a
  `(extension, max_size)` covering index.* This is the **discovery/analytics**
  path ("show me all torrents with a 4 GB+ iso").

The original L2-P0 spec's secondary index `(extension, info_hash)` is **wrong for this
query** — it omits `max_size`, so Direction-2 still heap-fetches every (ext) row to
test size. FB-A1's measured +9.5 GB used that wrong index. **This experiment measures
the *correct* `(extension, max_size) INCLUDE (info_hash)` covering index instead.**

---

## 3. EXPERIMENT PART 1 — exact sizing (`pg_total_relation_size`)

### 3.1 Variants to build & measure

All built from `torrent_files` on the bench restore (full 879.5 M rows / 261 GB;
multi-file torrents only — single-file ext stays on `torrents.extension`). Expected
~**54.8 M rows** (FB-A1), ~47,628 distinct extensions.

| variant | key | payload | secondary index | purpose |
|---------|-----|---------|------------------|---------|
| **V1** natural, max-only | `(info_hash bytea, extension text)` PK | `max_size int8` | — | minimal Direction-1-only |
| **V1+idx** natural, max-only, +covering | same | `max_size` | `(extension, max_size) INCLUDE (info_hash)` | minimal Direction-1 **and** Direction-2 → **the recommended-if-built shape** |
| **V2** natural, +count+min | same | `max_size, min_size int8, file_count int4` | `(extension, max_size) INCLUDE (info_hash)` | cost of carrying count/min (do they earn it?) |
| **V3** surrogate, max-only, +covering | `(torrent_id int4, ext_id int4)` PK | `max_size int8` | `(ext_id, max_size) INCLUDE (torrent_id)` + `dim_torrent`, `dim_ext` | does an int4 surrogate beat natural enough to justify a join-at-query-time? |

> The build SQL is RUN-3's (`run3-pg-sizing-bench.sql` §2–3) **with two corrections**:
> (1) the secondary index is `(extension, max_size) INCLUDE (info_hash)` — *not*
> `(extension, info_hash)` (wrong for the size range) and *not* `(extension, mx)`
> without INCLUDE (forces heap fetch for info_hash in Direction-2);
> (2) measure each index **separately** so the ±secondary delta is explicit.

### 3.2 Exact build + sizing SQL (bench session tuning identical to RUN-3)

```sql
\timing on
\set ON_ERROR_STOP on
SET work_mem='2GB'; SET maintenance_work_mem='4GB';
SET max_parallel_workers_per_gather=4; SET max_parallel_maintenance_workers=4;
SET synchronous_commit=off;

-- ---- V1 / V1+idx (natural, max-only) ----
DROP TABLE IF EXISTS agg_v1;
CREATE TABLE agg_v1 AS
  SELECT info_hash, extension, max(size)::bigint AS max_size
  FROM torrent_files
  WHERE extension IS NOT NULL            -- valid exts only (matches ExtractUniqueExtensions; PK forbids NULL)
  GROUP BY info_hash, extension;
CREATE UNIQUE INDEX agg_v1_pk ON agg_v1(info_hash, extension);   -- Direction-1 probe
ANALYZE agg_v1;
-- size WITHOUT secondary (V1):
SELECT 'V1 heap'  AS k, pg_size_pretty(pg_relation_size('agg_v1'))            AS v
UNION ALL SELECT 'V1 pk',  pg_size_pretty(pg_relation_size('agg_v1_pk'))
UNION ALL SELECT 'V1 total(no-sec)', pg_size_pretty(pg_total_relation_size('agg_v1'))
UNION ALL SELECT 'V1 rows', (SELECT count(*)::text FROM agg_v1)
UNION ALL SELECT 'V1 bytes/row',
  (SELECT round(pg_total_relation_size('agg_v1')::numeric/NULLIF(count(*),0),1)::text FROM agg_v1);
-- add the CORRECT secondary (V1+idx) and re-measure the delta:
CREATE INDEX agg_v1_ext_sz ON agg_v1(extension, max_size) INCLUDE (info_hash);  -- Direction-2
ANALYZE agg_v1;
SELECT 'V1+idx sec',  pg_size_pretty(pg_relation_size('agg_v1_ext_sz'))
UNION ALL SELECT 'V1+idx total', pg_size_pretty(pg_total_relation_size('agg_v1'))
UNION ALL SELECT 'V1+idx total_GB',
  round(pg_total_relation_size('agg_v1')/1e9::numeric,2)::text;

-- ---- V2 (natural, +count+min) ----
DROP TABLE IF EXISTS agg_v2;
CREATE TABLE agg_v2 AS
  SELECT info_hash, extension,
         max(size)::bigint AS max_size, min(size)::bigint AS min_size, count(*)::int4 AS file_count
  FROM torrent_files WHERE extension IS NOT NULL
  GROUP BY info_hash, extension;
CREATE UNIQUE INDEX agg_v2_pk ON agg_v2(info_hash, extension);
CREATE INDEX agg_v2_ext_sz ON agg_v2(extension, max_size) INCLUDE (info_hash);
ANALYZE agg_v2;
SELECT 'V2 heap', pg_size_pretty(pg_relation_size('agg_v2'))
UNION ALL SELECT 'V2 total', pg_size_pretty(pg_total_relation_size('agg_v2'))
UNION ALL SELECT 'V2 total_GB', round(pg_total_relation_size('agg_v2')/1e9::numeric,2)::text
UNION ALL SELECT 'V2 bytes/row',
  (SELECT round(pg_total_relation_size('agg_v2')::numeric/NULLIF(count(*),0),1)::text FROM agg_v2);

-- ---- V3 (surrogate int4/int4) — reuse RUN-3 dim_torrent/dim_ext ----
-- (build dim_torrent, dim_ext per run3-pg-sizing-bench.sql §1 first)
DROP TABLE IF EXISTS agg_v3;
CREATE TABLE agg_v3 AS
  SELECT dt.torrent_id, de.ext_id, max(f.size)::bigint AS max_size
  FROM torrent_files f
  JOIN dim_torrent dt USING (info_hash)
  JOIN dim_ext      de USING (extension)     -- inner join drops NULL ext
  GROUP BY dt.torrent_id, de.ext_id;
CREATE UNIQUE INDEX agg_v3_pk  ON agg_v3(torrent_id, ext_id);
CREATE INDEX agg_v3_ext_sz ON agg_v3(ext_id, max_size) INCLUDE (torrent_id);
ANALYZE agg_v3;
SELECT 'V3 heap', pg_size_pretty(pg_relation_size('agg_v3'))
UNION ALL SELECT 'V3 total(+dims)',
  pg_size_pretty(pg_total_relation_size('agg_v3')
               + pg_total_relation_size('dim_torrent')
               + pg_total_relation_size('dim_ext'))
UNION ALL SELECT 'V3 total_GB(+dims)',
  round((pg_total_relation_size('agg_v3')+pg_total_relation_size('dim_torrent')
       +pg_total_relation_size('dim_ext'))/1e9::numeric,2)::text;
```

### 3.3 Predicted sizes (from FB-A1 + RUN-3 + ARCH-C; the run confirms/corrects)

| variant | heap | PK | secondary `(ext,max_size)+incl` | **total** | note |
|---------|------|----|----------------------------------|-----------|------|
| V1 (no sec) | ~3.5 GB | ~3.0 GB | — | **~6.5 GB** | Direction-1 only |
| **V1+idx** | ~3.5 GB | ~3.0 GB | ~3.5 GB | **~10 GB** | the if-built shape; ≈ FB-A1's 9.5 GB but with the *right* index |
| V2 (+count+min) | ~4.4 GB | ~3.0 GB | ~3.5 GB | **~11 GB** | +count+min ≈ +0.9–1.0 GB heap for **no ext∧max_size benefit** |
| V3 surrogate (+dims) | ~1.3 GB | ~1.3 GB | ~1.5 GB + dims ~1.0 GB | **~5 GB** | smallest, **but** needs a `dim_torrent` join at query time (§5) |

> **Reconciliation note for the run:** FB-A1 measured **+9.5 GB** for the deployed-candidate
> shape (PK + `(extension, info_hash)` secondary, with `max_size`). RUN-3 predicted
> "natural ~5.5–6.5 GB / surrogate ~3–3.5 GB" but that was the **no-secondary-or-wrong-secondary**
> form. The honest number for a *usable* ext∧max_size agg (both directions) is **~10 GB
> natural / ~5 GB surrogate-incl-dims** — record the measured values and supersede both
> earlier estimates here.

---

## 4. EXPERIMENT PART 2 — the KEY measurement: ext∧max_size **latency** (EXPLAIN ANALYZE)

### 4.1 What ARCH-C already measured on DuckDB (backend D — do **not** re-run, cite)

The torrent-grain ext∧max_size = the **distinct-torrent collapse** workload, already
measured on the **same 879.5 M-row restore**, 24 cores (ARCH-C §1 row 1a, §2 rows B/D/E):

| DuckDB form | "mkv > 1 GB" → distinct torrents | exact COUNT | rare-ext range |
|-------------|----------------------------------|-------------|-----------------|
| `files_slim` **unsorted** (3.86 GB) | collapse **1331 ms** | 1024 ms | 48 ms |
| `files_slim` **sorted (ext,size)** (10.3 GB) | collapse **132 ms** (zonemap prune) | exact count **17 ms** | **19 ms** |
| **DuckDB `agg_torrent_ext` Parquet** rollup (v7, **1.39 GB**, 56 M rows) | collapse **5.2 ms** | — | — |

Ground truth from ARCH-C: mkv>1 GB = **5,699,629 files / 1,723,793 distinct torrents**;
movie ∧ mkv>1 GB (JOIN→PG) = 728,574 torrents. **DuckDB serves ext∧max_size at
5–132 ms with ZERO new PG disk** (per-file size is already in the slim Parquet that
the per-file search tier ships anyway). This is the bar backend B must clear to justify
its disk.

### 4.2 Backends A & B to measure on PG (new — this is the spec's core run)

Run **both directions** × a **threshold/selectivity matrix**, `EXPLAIN (ANALYZE, BUFFERS)`,
cold (`drop_caches`) then 3 warm reps, single connection. Use the **same exts/thresholds**
as FB-A1/ARCH-C for comparability:

| ext | threshold | selectivity | exercises |
|-----|-----------|-------------|-----------|
| mkv | > 1 GB | broad (~1.7 M torrents) | Direction-2 worst case (huge result) |
| iso | > 4 GB | medium | DVD/BD discovery |
| vob | > 0 | rare ext | zonemap/PK prune |
| epub | > 0 | rare-ish | small-file ext |

**Direction-1 (probe, text-first)** — simulate a selective outer set (the realistic UI
path: search box already cut torrents to a few thousand). Materialize a 5 k-info_hash
sample table `sel(info_hash)` once, then:

```sql
-- B (agg):
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM sel t
WHERE EXISTS (SELECT 1 FROM agg_v1 a
              WHERE a.info_hash=t.info_hash AND a.extension='mkv' AND a.max_size>1000000000);
-- A (torrent_files baseline / parity truth):
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM sel t
WHERE EXISTS (SELECT 1 FROM torrent_files f
              WHERE f.info_hash=t.info_hash AND f.extension='mkv' AND f.size>1000000000);
```

**Direction-2 (semi-join, filter-first)** — ext∧size is the selective predicate; this
is where the `(extension, max_size) INCLUDE (info_hash)` covering index earns its keep:

```sql
-- B (agg): distinct-torrent count and a LIMIT-21 page (the real served shape)
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM agg_v1 WHERE extension='mkv' AND max_size>1000000000;       -- index-only scan target
EXPLAIN (ANALYZE, BUFFERS)
SELECT info_hash FROM agg_v1 WHERE extension='mkv' AND max_size>1000000000 LIMIT 21;
-- A (torrent_files): note this needs DISTINCT info_hash (per-file table has dups)
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(DISTINCT info_hash) FROM torrent_files WHERE extension='mkv' AND size>1000000000;
```

**Backend B with the WRONG index (control):** repeat Direction-2 against a copy carrying
only `(extension, info_hash)` (FB-A1's index) to **quantify the heap-fetch penalty** and
prove the `INCLUDE (info_hash)` covering index is necessary.

### 4.3 Cross-engine reality for backend D (the load-bearing adversarial measurement)

DuckDB serving ext∧max_size is **not** free to *compose into the main PG search* —
it produces an `info_hash` set in a different engine. Two integration modes, both to be
characterized (cardinality + handoff cost), because they decide whether D can actually
*replace* B:

1. **Filter-first standalone (discovery):** DuckDB returns the page of info_hashes
   directly (collapse 5–132 ms) → PG hydrates 21 rows by PK (`torrent_contents` point
   lookups ≈ sub-ms each). **Works great** — total < 200 ms. ✅
2. **Composed with a PG text query (UI filter):** you cannot push a PG `tsquery` into
   DuckDB nor a 1.7 M-row info_hash IN-list into PG. The viable shape is **PG-leads**:
   PG runs the text query (already selective) → for the surviving page candidates,
   test ext∧max_size. If PG leads, the per-torrent size test must come from **somewhere
   PG can probe** = **either** `torrent_files` (dropped) **or** `agg` (backend B) **or**
   a per-request DuckDB point-probe per candidate (network round-trips). Measure the
   per-probe DuckDB latency to show it's the wrong tool for a correlated filter.

**This is the crux:** if ext∧max_size only ever needs to be a **standalone discovery
query**, DuckDB wins outright (mode 1, +0 PG disk). agg is justified **only** if
ext∧max_size must be a **composable correlated filter inside the text-search UI**
(mode 2) — a product requirement that **does not exist today**.

---

## 5. Recommendation (minimal shape) + adversarial verdict

### 5.1 If `agg_torrent_ext` is built at all — the minimal shape

- **Natural key `(info_hash bytea, extension text)`**, payload **`max_size int8` only**.
  - **Drop `count`/`min`** — V2 measures their cost (~+1 GB) and they do **nothing** for
    ext∧max_size. Add later iff a count/sum surface is committed.
  - **Reject the surrogate (V3)** despite its smaller heap: the main search keys on
    `bytea info_hash`; an int4 surrogate forces a `dim_torrent` join into the hot
    correlated EXISTS (Direction-1), trading 5 GB of disk for a join + a dimension table
    to maintain. Natural key composes as a clean one-line EXISTS (the §2 shape). The
    disk saved (~5 GB) is not worth the query/maintenance complexity for a single filter.
- **Indexes:** PK `(info_hash, extension)` (Direction-1 probe) **+** `(extension,
  max_size) INCLUDE (info_hash)` (Direction-2 covering). **Not** `(extension, info_hash)`
  (FB-A1's index — wrong for the size range; §4.2 control quantifies the penalty).
- **Hardened rollout** (per L2-P0 §3 note): create heap → COPY-seed → build indexes →
  ANALYZE; FK `REFERENCES torrents(info_hash) ON DELETE CASCADE` added `NOT VALID` then
  `VALIDATE` off-peak. Dual-write delta-upsert in the crawler path; parity via the
  Rust `bitmagnet-parquet verify` (L2-P0 §7, Jobs A+B).
- **Expected cost (to be confirmed by Part 1):** **~10 GB** PG (heap 3.5 + PK 3.0 +
  covering 3.5) + a dual-write pipeline + a checker.

### 5.2 Adversarial verdict — **agg is very likely NOT worth it; default = retire it, serve ext∧max_size from DuckDB**

Stack the measured facts against the agg's ~10 GB + pipeline + checker:

1. **No committed query needs it.** ext∧max_size has **zero** Go surface today. The
   deployed file-type filter/facet is served by `file_extensions` JSONB (FB-A1). agg is
   speculative.
2. **DuckDB already answers it at 5–132 ms, +0 PG disk.** The per-file slim Parquet (the
   per-file search tier ships it regardless) carries `size`; the optional DuckDB
   `agg_torrent_ext` **Parquet** rollup is **1.39 GB out-of-PG** and collapses in **5.2 ms**
   (ARCH-C). Same data, same ~55 M rows — but on the side of the fence we're *adding*
   capacity, not the side we're *shedding*.
3. **The DROP project's whole point is shedding PG disk** (−245 GB). Re-adding ~10 GB to
   PG to serve one hypothetical query is a direct regression against the goal. A DuckDB
   Parquet rollup achieves the identical query at ~1.4 GB **outside** PG.
4. **agg only wins in one narrow, uncommitted scenario:** ext∧max_size must be a
   **correlated filter composed inside the main PG text search** (Direction-1, mode-2 of
   §4.3) where the qualifying set is too large to hand back from DuckDB as an IN-list.
   That product requirement does not exist. Until it does, agg is disk + a pipeline +
   a checker for nothing.

**Recommendation to the lead:**
- **Keep `agg_torrent_ext` DEFERRED / unbuilt.** Mark it **retired from the active plan**;
  ext∧max_size, if/when surfaced, is served by the **DuckDB tier** (standalone discovery:
  slim Parquet sorted(ext,size) 132 ms, or the +1.4 GB out-of-PG Parquet rollup 5.2 ms).
- **Re-open agg ONLY on a hard, committed product requirement** that ext∧max_size be a
  **correlated filter inside the PG content-search UI** (composed with text + ordering +
  pagination) with result sets too large for a DuckDB→PG info_hash handoff. If that ever
  lands, build the **§5.1 minimal natural-key max-only shape (~10 GB)** with the corrected
  covering index — not the surrogate, not count/min, not the `(extension, info_hash)` index.

This experiment's value is to **nail the numbers** (exact agg size with the *correct*
index; PG ext∧max_size latency vs the DuckDB numbers already in hand) so the retire
decision is data-grounded rather than estimated — and so a future re-open starts from
the right shape.

---

## 6. Success criteria

The run is complete when it has produced, on the bench restore:
1. **Exact `pg_total_relation_size`** for V1, V1+idx, V2, V3 (+dims), with the heap / PK /
   secondary broken out and bytes/row — superseding the FB-A1 (+9.5 GB, wrong index) and
   RUN-3 (+3–5 GB, no/wrong index) estimates with the **correct-index** number.
2. **`EXPLAIN (ANALYZE, BUFFERS)`** for ext∧max_size on backends **A** (torrent_files,
   parity truth) and **B** (agg V1+idx) across the 4-ext × 2-direction matrix, cold+warm,
   incl. the WRONG-index control (§4.2) — demonstrating index-only scans for Direction-2
   and PK probes for Direction-1, with row counts matching ARCH-C ground truth
   (mkv>1 GB = 1,723,793 distinct torrents).
3. A **side-by-side table**: backend A vs B (this run) vs **D** (DuckDB, cited from ARCH-C)
   — disk delta, Direction-1 latency, Direction-2 latency, freshness, composability.
4. The §5 recommendation either **confirmed** (retire agg; DuckDB serves it) or
   **overturned** with a measured reason (e.g. PG agg Direction-1 ≪ any DuckDB handoff for
   a committed UI-filter requirement).

**Gate flags:**
- **RETIRE-agg** (expected): Part-2 confirms DuckDB serves standalone ext∧max_size
  ≤200 ms AND no committed UI-filter requirement exists ⇒ `agg_torrent_ext` is removed
  from the plan; ext∧max_size routed to DuckDB.
- **BUILD-agg** (only if): a committed product requirement for ext∧max_size as a
  composable PG text-search filter is on record ⇒ build §5.1 minimal shape, gate the
  migration behind a shadow flag, parity via `bitmagnet-parquet verify`.
- Either way: **`torrent_files` DROP stays deferred** until every replacement layer is
  proven in prod (standing sequencing constraint).

---

## 7. Single-connection safety protocol (HEL1 bench)

🚨 **DESIGN-ONLY — nothing here was executed.** When the lead authorizes the run:

- **Host:** HEL1 **tailscale** `ansible@<HEL1_TAILSCALE_IP>` (the public IP `<HEL1_PUBLIC_IP>`
  is SSH-flaky; maple-bastion ProxyJump fails — `AllowTcpForwarding no`).
- **DSN:** `postgresql://postgres:<BENCH_PW>@127.0.0.1:30654/bitmagnet` (NodePort,
  ns `bitmagnet-bench`, `deploy/bench-pg`). **Production FSN1 is never touched.**
- **One connection at a time; gentle pollers.** No ControlMaster/tight loops — they trip
  HEL1 sshd. Pipe SQL over stdin (`psql -X -P pager=off -f -`).
- 🚨 **`setsid`/`nohup` launches SURVIVE client-side SSH timeouts** — a rc=124 "fail" can
  still be running. **Guard the orchestrator with a lockfile + `pgrep`** before launching
  to avoid colliding writers (this previously caused duplicate concurrent runs).
- **Scheduling:** the heavy `GROUP BY` builds (Part 1) and `EXPLAIN ANALYZE` runs (Part 2)
  are minutes-to-~1 h; run under `nohup` in the **background**, **never overlap** the build
  with the latency reps (build first, ANALYZE after), and don't co-schedule with any other
  bench latency run on the box.
- **Cold/warm:** `echo 3 > /proc/sys/vm/drop_caches` (or restart the pod) before the cold
  rep; 3 warm reps after. `\timing on`, `ON_ERROR_STOP on`.
- **Teardown:** these tables live in the throwaway bench DB; RUN-6 (`make
  bitmagnet-bench-pg-teardown`) drops the whole namespace + PVC. Drop `slim_*`/big interim
  tables immediately after measuring to bound peak disk (RUN-3 pattern).
- **Be patient during long runs** (per working-style memory): no status-polling; report
  on completion.

---

## 8. One-line summary for the lead

Finalized: the *correct* usable agg shape for ext∧max_size is natural-key, max-only,
PK + `(extension, max_size) INCLUDE (info_hash)` ≈ **~10 GB** (not the +3–5 GB estimate —
that omitted the size index; FB-A1's +9.5 GB used the *wrong* index). But the adversarial
read says **don't build it**: ext∧max_size has no committed query, DuckDB already serves
it at **5–132 ms with +0 PG disk** (out-of-PG 1.4 GB Parquet rollup vs +10 GB *into* the
DB we're shrinking), and agg only wins in an uncommitted "correlated filter inside the PG
text-search UI" scenario. **Recommend: retire `agg_torrent_ext` from the plan; route
ext∧max_size to the DuckDB tier; re-open only on a hard UI-filter product requirement.**
The experiment (Part 1 exact sizing with the corrected index + Part 2 PG A/B latency vs
ARCH-C's DuckDB numbers) exists to make that retire decision data-grounded.
