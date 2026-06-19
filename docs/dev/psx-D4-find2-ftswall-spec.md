# FIND-2 — Main-search broad-common-term ranked FTS wall: investigation & fix-evaluation spec

**Status:** DESIGN-ONLY (read-only). Nothing here was executed on HEL1. This document specifies *how* to
reproduce and *how* to evaluate fixes via `EXPLAIN ANALYZE` on the throwaway bench restore, plus a
recommendation. Author: `psx-d4-find2` (team `bitmagnet-bench`, task #62).

---

## 0. TL;DR

- **FIND-2 confirmed at the source level, not assumed.** The WebUI's *default* sort for any keyword search is
  `relevance` descending, which the Go layer compiles to `ORDER BY ts_rank_cd(torrent_contents.tsv, $q::tsquery) DESC`.
  So `ts_rank_cd` **is on the default hot path** — every keyword search a user types is ranked unless they
  explicitly pick another sort.
- The wall is structural: `ORDER BY ts_rank_cd(...)` has **no ordering index**, so PostgreSQL must compute the
  rank for *every* matching row before it can take the top-N. For a broad common term (`x264` ≈ 4.28M matches)
  the GIN match is cheap (~482 ms) but ranking the whole match-set is the ~49 s single-core wall.
- The existing CTE "stopping-point" optimisation (`query.go:812-826`) **does not cover the relevance default** —
  it is explicitly disabled for single `query_string_rank DESC` ordering. It only bounds *non-relevance* sorts
  (seeders / published_at / size) that are combined with a query string. So broad relevance searches have no
  early-out path today.
- **DROP-independent:** the query touches only `torrent_contents` (+ joins to `torrents`/`content`), never
  `torrent_files`. It is a pre-existing perf issue, orthogonal to the migration/DROP work.
- **Recommendation (preview):** *Defer* the heavyweight RUM-index fix; it costs +30-50 GB, a slow single-threaded
  build, an extension install, a **ranking-semantics change (`ts_rank` not `ts_rank_cd`)**, and — most importantly —
  **write amplification on a write-heavy table** that already shows a super-linear tsv-update cost (FIND-1). The
  low-risk, near-term lever is product/UX: cap the ranked candidate window (extend the existing CTE strategy to the
  relevance path) and/or fall back broad terms to a btree-ordered popularity sort. Characterise first on bench,
  ship only if broad-term latency is a real user complaint. See §4.

---

## 1. Reproduce — the served query and where ranking lives

### 1.1 Code trace (verified against `/Users/me/aaa/github/bitmagnet`)

| Step | File:line | Fact |
|---|---|---|
| WebUI default sort with a query | `webui/src/app/torrents/torrents-search.component.ts:292` | `field: hasQuery ? "relevance" : "published_at"` |
| Default order object | `webui/.../torrents-search.controller.ts:526` | `defaultQueryOrderBy = { field: "relevance", descending: true }` |
| Always sent to API | `webui/.../torrents-search.controller.ts:66` | `orderBy: [ctrl.orderBy]` — the GraphQL `orderBy` arg is **always** populated; for a typed query it is `[{relevance, desc}]` |
| Server keeps relevance when query present | `internal/gql/gqlmodel/torrent_content.go:149-151` | relevance order is skipped only `&& !hasQueryString`; with a query it is retained |
| Relevance → rank field | `internal/database/search/order_torrent_content.go:18-26` | `TorrentContentOrderByRelevance` → `OrderByColumn{Name: QueryStringRankField}` |
| Rank field → SQL | `internal/database/query/query.go:613-625` | `ts_rank_cd(<table>.tsv, ?::tsquery) AS _order_0` |
| WHERE clause | `internal/database/query/query.go:646-648` | `<table>.tsv @@ ?::tsquery` |
| `SearchParams` also force-adds rank order | `internal/database/query/params.go:20-22` | when `QueryString.Valid` → `SearchString(...) , OrderByQueryStringRank()` |
| Table | `internal/model/torrent_contents.gen.go:14` | `torrent_contents` |
| tsquery builder | `internal/database/fts/tsquery.go:9` | `AppQueryToTsquery` — `x264` → tsquery `'x264'`; quotes/`.`→`<->`, `&`/`|`, `!`, trailing `*`→`:*` prefix |

**Conclusion:** `ts_rank_cd` is unambiguously the default ranking on the keyword hot path. The lead's caution
("don't assume it's on the default path; the UI default may be seeders/published_at") is resolved: the UI default
is `published_at` **only when the search box is empty**. The instant a query string is present, the default flips
to `relevance` = `ts_rank_cd`.

### 1.2 The tsvector column & indexes (verified in `migrations/`)

- `torrent_contents.tsv` is a **plain `tsvector` column** (no longer generated) — `00006_tsv.sql` dropped the
  generated column and made it app-maintained (`UpdateTsv`, the FIND-1 hot spot). Config is `to_tsvector('simple', …)`
  (no stemming; exact lexemes).
- The live FTS index is the **composite GIN** `gin(content_type, tsv)` via `btree_gin`
  (`00011_indexes.sql:8-10`) — this is the ~14 GB "content_type_tsv GIN" in MEMORY. The plain
  `torrent_contents_tsv_idx` was dropped in the same migration.
- Btree ordering indexes that exist (relevant to fix options): `published_at` (`00017_ordering_fields.sql:14`),
  `size` & `coalesce(files_count,0)` (`00018:6-7`), `coalesce(seeders,-1)` / `coalesce(leechers,-1)`
  (`00018:9-10`), `updated_at` & `(content_type, updated_at)` (`00004`). **There is no ordering index that can
  serve `ts_rank_cd` order** — that is the whole problem.

### 1.3 Why the existing CTE strategy doesn't help relevance

`GenericQuery` races two goroutines (`query.go:186-252`):

1. **Default plan** — the query as-is: `… WHERE tsv @@ q ORDER BY ts_rank_cd(tsv,q) DESC LIMIT n OFFSET m`.
2. **CTE "stopping-point" plan** — only started when `shouldTryCteStrategy()` is true. It materialises up to
   `stoppingPoint = 50_000` matches into a CTE, counts them, and returns the sorted page **only if the total
   match count < 50 000** (`query.go:210-248`). For a broad term it bails (count ≥ 50k → `WHERE … < 50000`
   false), leaving the slow default plan to win the race.

`shouldTryCteStrategy()` (`query.go:812-826`):

```go
return b.tsquery != "" && (len(b.orderBy) != 1 ||
    b.orderBy[0].Column.Name != QueryStringRankField ||
    !b.orderBy[0].Desc)
```

For the relevance default the single order column **is** `query_string_rank DESC`, so the whole expression is
`false` → **the CTE plan is never even started**. Only the full-match-set ranking plan runs. (The CTE bound is
reserved for query + seeders/published_at/size, where it caps work at 50k.)

### 1.4 Reproduction probes (run on bench, read-only)

> All probes are `EXPLAIN (ANALYZE, BUFFERS, …)` or `SELECT` — read-only. Pick the term empirically; `x264` per
> MEMORY ≈ 4.28M matches. Use a *paginated* shape (`LIMIT 30 OFFSET 0`) to mirror the UI. `simple` config means
> the literal lexeme; verify with `to_tsquery('simple','x264')`.

**P0 — corpus & term selectivity (sanity):**
```sql
SELECT reltuples::bigint AS est_rows FROM pg_class WHERE relname = 'torrent_contents';
SELECT count(*) FROM torrent_contents WHERE tsv @@ to_tsquery('simple','x264');   -- expect ~4.28M
-- index sizes for cost baselining:
SELECT indexrelid::regclass AS idx, pg_size_pretty(pg_relation_size(indexrelid)) AS sz
FROM pg_index WHERE indrelid = 'torrent_contents'::regclass ORDER BY pg_relation_size(indexrelid) DESC;
```

**P1 — the exact served wall (relevance, paginated):**
```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING)
SELECT torrent_contents.*, ts_rank_cd(torrent_contents.tsv, $q::tsquery) AS _order_0
FROM torrent_contents
WHERE torrent_contents.tsv @@ $q::tsquery
ORDER BY _order_0 DESC
LIMIT 30 OFFSET 0;
-- with $q = to_tsquery('simple','x264')
```
Expected shape: `Bitmap Heap Scan` on the GIN (recheck) feeding a `Sort` (or top-N heapsort) on `_order_0` over
~4.28M rows → the dominant time is the per-row `ts_rank_cd` + sort, not the GIN. Capture `Execution Time` and the
`Sort`/`WindowAgg` node actual time.

**P2 — served query *with the joins* the app actually issues** (to confirm the join isn't the dominant cost):
```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT tc.*, ts_rank_cd(tc.tsv, $q::tsquery) AS _order_0
FROM torrent_contents tc
JOIN torrents t ON tc.info_hash = t.info_hash
LEFT JOIN content c ON tc.content_type = c.type AND tc.content_source = c.source AND tc.content_id = c.id
WHERE tc.tsv @@ $q::tsquery
ORDER BY _order_0 DESC
LIMIT 30 OFFSET 0;
```

**P3 — isolate the three cost components** (prove "GIN cheap, rank is the wall"):
```sql
-- (a) GIN match only, no rank, no order:
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM torrent_contents WHERE tsv @@ to_tsquery('simple','x264');
-- (b) match + rank but NO order (forces full rank compute, no sort):
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM (
  SELECT ts_rank_cd(tsv, to_tsquery('simple','x264')) r
  FROM torrent_contents WHERE tsv @@ to_tsquery('simple','x264')) s WHERE s.r > -1;
-- (c) match + rank + order + LIMIT (the wall):  -> P1 above
```
Δ(b−a) = rank-compute cost; Δ(c−b) = sort cost. Confirms MEMORY's "GIN 482 ms, rank = the wall".

**P4 — contrast: the SAME term sorted by a btree-backed column (what non-relevance sorts get):**
```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT torrent_contents.* FROM torrent_contents
WHERE tsv @@ to_tsquery('simple','x264')
ORDER BY published_at DESC LIMIT 30;            -- can the planner index-order + filter early-out?
EXPLAIN (ANALYZE, BUFFERS) SELECT torrent_contents.* FROM torrent_contents
WHERE tsv @@ to_tsquery('simple','x264')
ORDER BY coalesce(seeders,-1) DESC LIMIT 30;
```
This measures the realistic cost of the "make broad-term default a popularity sort" fix (§3.3). Watch whether the
planner picks `Index Scan … Filter (tsv @@ …)` with early termination vs `Bitmap … + Sort` (a very common term
favours index-order-scan + filter; a rare term favours bitmap+sort — note both regimes).

**Term matrix:** run P1/P3 for a *broad* term (`x264`), a *medium* term (e.g. `1080p`), and a *rare* term
(e.g. a specific release group), plus a 2-term AND (`x264 & 1080p`) and a phrase (`"the matrix"` → `<->`). FIND-2
is specifically the broad single-common-term regime; the matrix proves where the cliff is.

---

## 2. Fix candidates to evaluate (each with its bench probe)

Run every variant on the **same** restore, same term matrix, `EXPLAIN (ANALYZE, BUFFERS)`, warm + `drop_caches`
cold, ≥5 reps, record p50/p95. Compare against the P1 baseline.

### 2.1 RUM index (`postgrespro/rum`) — index-ordered ranking, early-termination

The canonical fix for "ORDER BY rank over a huge match-set": a `rum` index stores lexeme positions/addon columns
so `ORDER BY tsv <=> q LIMIT n` is answered **by the index in rank order**, stopping after N — no full match-set
scan.

**Prerequisite (gated, heavier):** the bench PG image does **not** ship `rum`. To evaluate you must install it
into the *throwaway* bench Postgres only (build `make`/`pg_config` against the running PG major, then
`CREATE EXTENSION rum;`). This is a non-trivial, explicitly-gated step — see §5 GATE-RUM. Do **not** touch prod.

```sql
-- build (SLOW, single-threaded; time it):
CREATE EXTENSION IF NOT EXISTS rum;
CREATE INDEX CONCURRENTLY tc_tsv_rum ON torrent_contents USING rum (tsv rum_tsvector_ops);
SELECT pg_size_pretty(pg_relation_size('tc_tsv_rum'));        -- expect ~2-3x the 14GB GIN
-- query (NOTE: <=> uses ts_rank, NOT ts_rank_cd):
EXPLAIN (ANALYZE, BUFFERS)
SELECT *, tsv <=> to_tsquery('simple','x264') AS dist
FROM torrent_contents
WHERE tsv @@ to_tsquery('simple','x264')
ORDER BY tsv <=> to_tsquery('simple','x264')
LIMIT 30;
```
**What to capture:**
- Latency at the broad term (expected: tens-of-ms — the win).
- **Index size** (`tc_tsv_rum`) and **build time** — the headline cost. Also test the addon-ops variant
  `rum (tsv rum_tsvector_addon_ops, published_at)` if combined rank+recency ordering is wanted.
- **Write cost** — the deciding risk. Measure insert/update amplification with rum present:
  ```sql
  EXPLAIN (ANALYZE) UPDATE torrent_contents SET tsv = tsv WHERE info_hash = ANY($sample);  -- before/after rum exists
  ```
  Compare wall-time of a representative tsv-rewrite batch with and without the rum index. Cross-reference FIND-1
  (importer tsv update already super-linear). RUM's positional posting lists are far heavier to update than GIN's.
- **Ranking-semantics diff:** `<=>` ≈ `1/ts_rank`, **not** `ts_rank_cd` (no cover-density). Spot-check that the
  top-30 ordering for representative queries is acceptable vs current `ts_rank_cd`. This is a product call, not
  just a perf one.

### 2.2 `ts_rank` vs `ts_rank_cd` (cheap, but not a fix)

```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT torrent_contents.*, ts_rank(tsv, $q) AS _o
FROM torrent_contents WHERE tsv @@ $q ORDER BY _o DESC LIMIT 30;
```
Hypothesis: `ts_rank` is marginally cheaper per row than `ts_rank_cd` (no cover-density positions), **but it still
requires computing the rank for every match and a full sort** — same O(match-set) wall. Expect a small constant-factor
improvement, *not* a cliff fix. Document the delta; reject as a standalone fix. (Worth noting because `simple`
config + no positions means `_cd` adds cost for little ranking benefit on single terms.)

### 2.3 Precomputed popularity / published_at default (no extension)

Two sub-variants:

**(a) Flip the broad-term default to a btree-ordered column** (published_at or coalesce(seeders,-1)). Measured by
P4 above. Pros: zero new objects, indexes already exist, early-termination for common terms. Cons: changes the
product default ("relevance" is what users expect from a search box); rare terms still scan a lot; a query +
ORDER BY published_at over a common term *may* still bitmap+sort (term matrix tells us). The existing CTE strategy
already accelerates this exact shape (≤50k), so the realistic served latency for medium terms is already bounded.

**(b) Static popularity score column** (query-independent), e.g. a stored `rank_bucket` from seeders/recency, btree
indexed, used as the order. This is just (a) generalised; no per-query rank can be precomputed (rank depends on the
query), so the only precomputable order is query-independent popularity.

### 2.4 Bounded-candidate ranking — extend the CTE strategy to relevance (no extension, keeps ts_rank_cd)

The most surgical code-only fix: **stop excluding the relevance path from the stopping-point CTE.** Today
`shouldTryCteStrategy()` returns false for `query_string_rank DESC`. If we allow a bounded candidate plan for
relevance too, we can rank only a capped window instead of all 4.28M rows. Two designs:

- **Exact-when-small (current semantics):** reuse the existing ≤50k materialise-then-count gate, but for the rank
  order. For terms with <50k matches it returns the exact ranked page fast; broad terms still fall through to the
  wall (no worse than today). Net: medium terms get faster, broad terms unchanged. Low value for FIND-2 itself.
- **Approximate-for-broad (the real lever):** take a *bounded candidate set* — e.g. the 50k most-recent (or most-seeded)
  matches via the `published_at`/`seeders` btree (`WHERE tsv @@ q ORDER BY published_at DESC LIMIT 50000`), then
  `ts_rank_cd`-rank *those* and return the top-N. This early-terminates on the btree and ranks a bounded set →
  bounded latency, at the cost of *approximate* relevance (a low-rank-but-old true match could fall outside the
  window). Probe:
  ```sql
  EXPLAIN (ANALYZE, BUFFERS)
  WITH cand AS (
    SELECT * FROM torrent_contents
    WHERE tsv @@ to_tsquery('simple','x264')
    ORDER BY published_at DESC LIMIT 50000)
  SELECT *, ts_rank_cd(tsv, to_tsquery('simple','x264')) AS _o
  FROM cand ORDER BY _o DESC LIMIT 30;
  ```
  Capture latency + a *recall* check (how often the exact top-30 ⊂ candidate window) on the term matrix. This is
  the best "keep relevance, no new disk, no extension, no write-amplification" option — its only cost is
  approximate ranking on broad terms (which is arguably fine, since `ts_rank_cd` over millions of equally-weighted
  `simple` single-term matches is already near-arbitrary).

### 2.5 LIMIT + early-out via index ordering (covered by 2.1/2.3)

There is no way to early-terminate a `ts_rank_cd` ORDER BY with the current GIN — GIN cannot return rows in rank
order. Index-ordered early-out requires either RUM (2.1, rank order) or a btree-ordered column (2.3, popularity
order). No separate probe; this row exists to record that "just add LIMIT" does nothing (LIMIT is already present;
the Sort still consumes the whole match-set).

---

## 3. Comparison matrix (fill from bench)

| Option | Keeps `ts_rank_cd` relevance? | New disk | Build/migration | Write-path cost | Broad-term latency | Risk |
|---|---|---|---|---|---|---|
| **Baseline (today)** | yes | 0 | — | current | ~49 s (the wall) | — |
| 2.1 RUM `<=>` | **no** (`ts_rank`) | **+30-50 GB** (~2-3× GIN) | slow single-thread build + `CREATE EXTENSION` | **high** (positional posting-list updates; FIND-1 risk) | tens of ms (the win) | high (write amp + semantics + ext) |
| 2.2 `ts_rank` | similar | 0 | code 1-liner | unchanged | ~tens of s (constant-factor only) | low — but **not a fix** |
| 2.3a published_at default | no (popularity) | 0 | code + UX | unchanged | fast for common terms (early-term) | medium (product default change) |
| 2.4 bounded-candidate CTE (approx) | yes (over a window) | 0 | code | unchanged | bounded (rank 50k) | low-medium (approx recall) |

(Numbers to be filled by the bench run; latency cells are hypotheses from the code analysis + MEMORY.)

---

## 4. Recommendation

1. **Do the characterisation now (cheap, read-only):** run §1.4 P0-P4 + the term matrix on the existing bench
   restore. It is a handful of `EXPLAIN ANALYZE` statements, no new objects, single connection. This produces the
   real cliff curve and confirms the join is not the dominant cost. **This is the only step worth doing
   immediately**, because FIND-2 is pre-existing and DROP-independent — it is *not* on the critical path for the
   `torrent_files` migration/DROP and need not gate it.

2. **Defer RUM.** It is the textbook fix and the only option that keeps interactive latency *and* relevance order,
   but for this workload its costs are real and stacked: +30-50 GB on a project that is actively minimising
   footprint; a slow single-threaded build; a new shared-lib extension to bake into the PG image; a
   **ranking-semantics change** (`ts_rank`, no cover density); and — the dealbreaker — **write amplification on a
   table that is upsert-heavy and already exhibits super-linear tsv-update cost (FIND-1)**. Only pursue RUM if a
   *confirmed* product requirement emerges for sub-second relevance-ranked results on broad common terms, and even
   then gate it on the write-amplification probe (§2.1) showing the importer/dual-write path stays healthy.

3. **If broad-term latency is a real user complaint, ship the code-only lever (no extension, no disk):** prefer
   **§2.4 bounded-candidate ranking** (cap the candidate window via the existing `published_at`/`seeders` btree,
   rank the window with `ts_rank_cd`, keep relevance semantics approximately) — implemented by extending
   `shouldTryCteStrategy()`/the CTE branch to the relevance path. Fallback/companion: **§2.3a** make broad single
   common terms degrade to a popularity (`seeders`/`published_at`) sort, which the existing CTE strategy already
   accelerates. Both are reversible Go-only changes with zero schema/disk cost and no write-path impact.

4. **Reject §2.2** (`ts_rank` swap) as a standalone fix — constant-factor only, doesn't remove the cliff.

**Now vs defer:** characterise now; **defer any code/extension change** until the bench numbers + a product decision
on (a) is broad-term relevance latency actually user-visible and (b) is approximate-relevance acceptable. Nothing
here blocks the migration. No production change is proposed.

---

## 5. Safety protocol & gate flags (for whoever runs the bench probes)

**This document is read-only design; do NOT execute on HEL1.** When the lead authorises a run:

- **Single connection, one at a time.** HEL1 public IP SSH is flaky (255/124); use the **tailscale IP**:
  `ssh -o IdentityAgent=none -i ~/.ssh/id_ed25519 ansible@<HEL1_TAILSCALE_IP>`. maple-bastion ProxyJump FAILS
  (`AllowTcpForwarding no`).
- **DSN (bench throwaway PG, NodePort):** `postgresql://postgres:<BENCH_PW>@127.0.0.1:30654/bitmagnet`
  (reach via the tailscale host). This is the **pre-backfill restore**, isolated from prod.
- **`setsid` launches survive client-side SSH timeouts** → a rc=124/255 "failure" can still have *landed* the
  command. Guard every orchestrator with a lockfile + `pgrep` before relaunch to avoid duplicate concurrent runs
  (colliding sessions tripped HEL1 sshd before). Gentle pollers only; no ControlMaster/tight loops.
- **Read-only by default.** P0-P4 + §2.2/§2.3/§2.4 probes are pure `SELECT`/`EXPLAIN` — safe. They create no
  objects. Run inside a `SET statement_timeout = '120s';` guard so a wall-probe can't wedge the box, and prefer
  `EXPLAIN (ANALYZE …)` over bare `SELECT *` to avoid materialising millions of rows over the wire.
- **GATE-RUM (heavier, opt-in):** §2.1 requires installing the `rum` extension and building a ~30-50 GB index on
  the bench PG. This is the only destructive-ish step (writes to the throwaway DB, long build, disk pressure on
  HEL1's local-path PV). Do **not** run it without explicit lead approval, and only ever on the bench restore —
  never prod. Build with `CREATE INDEX CONCURRENTLY`, watch HEL1 disk headroom, and drop the index after the
  measurement. Confirm the bench PV has ≥60 GB free first (`df`).
- **Teardown:** these probes leave no artifacts except (if GATE-RUM is taken) the rum extension/index — drop them
  and `CREATE EXTENSION`-free the DB at the end. Fold into the pending RUN-6 `make bitmagnet-bench-pg-teardown`.

---

## 6. Open questions for the lead / product

- Is broad-common-term relevance latency an actual user-visible complaint, or a theoretical worst case? (Most real
  queries are multi-term/selective and already <25 ms per MEMORY/EXP-A.) This decides whether *any* fix ships.
- Is **approximate** relevance on broad terms acceptable (§2.4)? If yes, the cheap code-only lever is enough and RUM
  is unnecessary.
- Is `ts_rank_cd`'s cover-density even meaningful here? Config is `simple` (no positions from stemming) and most
  walls are *single*-lexeme queries where `_cd` ≈ `ts_rank` ≈ term-frequency. If cover-density buys little, §2.2/§2.4
  semantics shifts are cheap to accept.
