# Phase 6 design — Tantivy-*served* main-search results

**Status:** design only (not built). Scopes audit revival item #9
(`docs/dev/tantivy-p3p4-audit-2026-07-03.md:166`) — turning the dormant Phase-4
shadow machinery into a real BM25 read path. Phase 4 *observes* Tantivy; Phase 6
lets Tantivy *serve*.

**Audience:** whoever picks up `router.go:111-115` (the `TODO(phase6)`). Read the
audit first — this doc assumes its verdict (engine core is production-quality,
blockers are DB-boundary schema drift + one goroutine-leak hazard).

---

## 0. The one-paragraph shape

Serving is a *strict superset* of the shadow path that already runs the Tantivy
query. Where `runShadow` (`router.go:136-176`) throws the `pb.SearchResponse`
away after comparing it, Phase 6 keeps it: hydrate the hit info-hashes back into
`search.TorrentContentResultItem`s from PostgreSQL (mirroring the L3 composer's
`orderItemsByIDs` requery, `pathsearch/composer.go:812-842`), preserve Tantivy's
score order, and return `resp.TotalHits` as the count. The hard parts are **not**
the hydration — that is a solved pattern — they are the **eligibility gate**
(which queries Tantivy is *allowed* to serve) and the **freshness gate** (when
the index is current enough to trust). Both are default-deny whitelists, and both
already have in-repo templates (`pathsearchOrderEligible`,
`registerPathsearchHealthReporter`).

---

## 1. Serving shape — hits → `search.TorrentContentResult`

**Decision: PG-requery-by-info_hash + score-order restore + `TotalHits` count.**
Do *not* build result items from the Tantivy document — the stored doc is a
denormalised search projection, not the GraphQL/Torznab shape, and re-deriving
`Content`, `Episodes`, source rows, etc. from it would fork the hydration logic.
Instead reuse the exact PG hydrators the fallback path uses.

Concretely, in the serving branch of `Router.TorrentContent`:

1. Build the request with the existing `requestBuilder` but **page it in
   Tantivy**: set `pb.Pagination{Limit, Offset}` from the query's page window
   (the recorder already captures limit/offset, `request_builder.go:104-117`).
   Tantivy owns ranking, so unlike L3 there is **no oversample and no candidate
   budget** — we ask for exactly the page.
2. `resp, err := r.tantivy.Search(ctx, req)` on the hot path under a serving
   deadline (§6). On any error/timeout → fall through to PG (never error).
3. Extract ordered ids: `extractTantivyIDs(resp)` already exists
   (`router.go:193-202`) and returns `DocID`s in rank order. Derive the
   `protocol.ID` set (info_hash) for the PG `IN(...)`.
4. Requery PG restricted to those info-hashes with the *same hydrators* the
   fallback uses (`HydrateTorrentContentContent` + `HydrateTorrentContentTorrent`
   + `TorrentContentCoreJoins`), **no page window, no tsquery** — exactly
   `composer.candidateRows` (`composer.go:506-519`).
5. Restore Tantivy order: `orderItemsByIDs(items, ids)`
   (`composer.go:822-842`) — a stable sort by rank position. This is the proven
   template; the only wrinkle is that Tantivy keys per *torrent_content* (DocID
   includes content_type/source/id) while `orderItemsByIDs` keys by
   `Torrent.InfoHash`. A torrent with N classifications produces N DocIDs but one
   info_hash, so ordering by info_hash alone is ambiguous for multi-classified
   torrents. **Fix:** order by the full DocID (`InferID()`), not info_hash —
   `extractPGIDs`/`extractTantivyIDs` already agree on `InferID()`
   (`router.go:178-202`), so add an `orderItemsByInferID` variant rather than
   reuse the info_hash one. This is the single non-trivial serving-code delta.

**Counts / paging — simpler than L3, and *exact*:**

| Field | Value | Why |
|---|---|---|
| `TotalCount` | `resp.TotalHits` (`search.pb.go:915`) | Tantivy returns the exact match total, not a gram-conjunction upper bound. No refine, no false positives to drop. |
| `TotalCountIsEstimate` | **false** | Unlike L3 (`composer.go:1145`), this is an exact count. |
| `HasNextPage` | `offset+len(page) < TotalHits` | Direct — Tantivy paged natively. |
| `Aggregations` | **empty** — facets are ineligible in v1 (§2). | Facet parity is a known caveat (audit defect #3); not in Phase-6 scope. |

**Why no exact-refine step (the big divergence from L3):** the L3 composer
blob-decodes candidates and drops false positives because ngram-recall is a
*superset*. Tantivy main-search returns the authoritative ranked matches
directly — there is nothing to refine, no blob decode, no file-budget chunking,
no `RetainedFileBudget`. Serving is therefore *cheaper and simpler* than the L3
route: one Tantivy RPC + one PG `IN(...)` hydrate of ≤`limit` rows.

---

## 2. Eligibility gate — which queries Tantivy may serve

**Decision: default-deny whitelist, mirroring `pathsearchOrderEligible`
(`torrent_content.go:441-449`).** A query is Tantivy-eligible in v1 **only if
all** hold:

1. **Has a free-text query string.** Empty query = browse/filter listing → PG.
2. **No structured filters.** Reuse the *existing* `canCompare` signal: the
   `requestBuilder` maps **zero** `query.Where` criteria to `pb.SearchFilters`
   (`request_builder.go:31-59` — `skippedFilter` on any filter option). Phase 4
   already treats `canCompare=false` as "skip the shadow" (`router.go:145-157`);
   Phase 6 treats it as "not eligible to serve." This is the *same gate*, reused,
   and it is what makes serving safe under the L3 route (§3).
3. **Relevance ordering only.** Empty `OrderBy` or explicit relevance — identical
   to `pathsearchOrderEligible`. Structured sorts are excluded because the
   Phase-3 proto honours only the first `SortBy` and field-sorted hits carry
   score 0.0 (audit defect / plan caveat 4, `rust-rewrite-plan.md:274`). The
   `sortableFields` map (`request_builder.go:178-185`) exists for *shadow*
   comparison; do **not** promote it to a serving contract in v1.
4. **No facets/aggregations requested.** Facet counts diverge (overcount on 2+
   same-type extensions, no null bucket — audit defect #3). The web UI always
   requests the facet sidebar, so in practice **v1 serves the facet-free clients:
   Torznab and the JSON/GraphQL API queries that omit `facets`.** The web UI
   stays on PG until facet parity (a later phase).
5. **Sidecar healthy + fresh** (§4).

**Alternatives weighed:**

- *Serve facets from Tantivy `GetFacets`* — rejected for v1: the overcount +
  null-bucket gaps are a visible UX regression in the sidebar, and reconciling
  them is a whole workstream (audit revival #4). Explicitly out of scope (§7).
- *Whitelist the mapped structured sorts* — rejected for v1: the single-key-sort
  proto limit means a multi-key sort silently drops keys. Revisit only after the
  proto grows a real multi-sort + non-zero field-sorted scores.
- *Map a safe subset of filters to `pb.SearchFilters`* (content_type, size,
  release_year all exist, `search.pb.go:335-349`) — deferred: it needs the
  call-site cooperation the audit flags as "Phase-5 work" and every mapped filter
  is a new parity surface. v1 ships the unfiltered class and grows the whitelist
  later.

**Net v1 eligible class:** plain free-text relevance queries, no filters, no
facets, no structured sort — i.e. the Torznab search box and API free-text
search. Small, but it is exactly the class where BM25 name-ranking beats
`tsvector` and where shadow parity is already measured cleanly.

---

## 3. Interaction with the L3 route — precedence

The read pod (`bitmagnet-l3`) already intercepts query-string searches **before**
the router: the resolver calls `Pathsearch.TorrentContent` first and only falls
to the router-decorated PG search on `served=false`
(`torrent_content.go:210-229`). So the two search engines answer *different
questions*:

- **L3** = path/file-substring relevance (ngram recall over file paths).
- **Tantivy main-search** = name/title BM25 relevance (the `tsvector` replacement).

**Decision: L3 → Tantivy → PG precedence, and they compose safely by
construction.** Order of interception for a query-string search:

1. **L3 composer** takes its eligible class (typeahead on, eligible length,
   relevance order, healthy) and serves or declines.
2. On L3 `served=false` the query hits the router-decorated
   `TorrentContentSearch.TorrentContent`. *Here* Phase-6 serving applies to the
   eligible residual (§2).
3. Otherwise PG.

**The safety property that makes this work:** the L3 composer's own PG hydrate
calls the router-decorated search (`composer.go:518`), and those calls carry
`query.Where(TorrentContentInfoHashCriteria(...))` — a **filter** — so they hit
`canCompare=false` → **ineligible** → straight to PG. The router can *never*
recursively re-route an L3 hydrate through Tantivy, for exactly the reason the
shadow path already skips those chunks (audit "Interaction with the L3 route").
The eligibility gate is load-bearing for this, not just for parity.

**Where Phase-6 serving actually lights up:** on **`bitmagnet-0` and Torznab**
(no L3 interception, no facet sidebar for Torznab), and on the L3 pod only for
the free-text residual L3 declined. Recommend enabling `ModeTantivy`/`ModeCanary`
serving **first on a non-L3 serving pod / Torznab**, where the eligible class is
the majority of traffic, rather than on the L3 read pod where it is the scraps.

---

## 4. Freshness — the staleness gate (has a hard prerequisite)

**Prerequisite (blocking): the 00024 follow-contract incremental indexer** (audit
revival #3, task #7, being built in parallel). Until deletes/updates propagate,
the index drifts monotonically and serving stale results is a correctness bug,
not a latency one. **Phase 6 must not serve until #3 lands.**

**Second prerequisite (small proto change): the main-search health message
carries no freshness signal.** `HealthCheckResponse` has only
`{Status, DocCount}` (`search.pb.go:1212`, verified). The L3 path already solved
this — `PathSearchHealth` carries `WatermarkEpoch` + `Writable`
(`path_search.pb.go:278`). Phase 6 must **add `watermark_epoch` to
`HealthCheckResponse`**, published by the incremental indexer as the max
`torrents.updated_at` it has indexed (the same follow-contract cursor as #3).

**The gate (mirrors `registerPathsearchHealthReporter`,
`searchfx/module.go:193-269`):** a background poller calls `HealthCheck`, and
publishes a cached `healthy && fresh` boolean the router reads on the hot path —
**never a blocking RPC per query** (the L3 finding-#4 lesson). Definition of
serve-eligible sidecar:

```
reachable  &&  Status == SERVING  &&  DocCount > 0  &&  now - watermark_epoch <= maxStaleness
```

`maxStaleness` default **2 min**, matching the L2 file-search freshness SLA
(memory: `bitmagnet-l2-filesearch-deploy`). On stale/unhealthy → the query is
ineligible → PG serves. Staleness is fail-*safe*: a lagging index silently yields
to PG, never serves wrong results.

---

## 5. Cutover gates & rollout

Shadow mode already computes every signal needed to green-light serving
(`shadow/comparator.go`): `JaccardAt20`, `JaccardAt50`, `RBO` (p=0.9),
`Top1Match`, and `PGCount==TantivyCount`. Serving is gated on these *over the
eligible class only* (filtered/faceted queries are skipped in shadow too, so the
metric already reflects the servable population).

**Gate thresholds (over ≥7 days of real traffic, sampled):**

| Metric | Serve-GO threshold | Rationale |
|---|---|---|
| `Top1Match` rate | ≥ 0.98 | The #1 result is what most users act on; near-parity required. |
| `JaccardAt20` (mean) | ≥ 0.90 | First page set-overlap. |
| `RBO` (mean) | ≥ 0.92 | Rank-weighted agreement; catches reordering the Jaccard misses. |
| `PGCount==TantivyCount` rate | ≥ 0.95 | `TotalHits` drives the UI count; large disagreement means a match-set defect (audit defects #1/#2 — phrase-over-group, prefix cap). |
| Tantivy p99 serve latency | ≤ PG p99 | No latency regression on the served path. |

Sub-threshold on any → **do not cut over**; the failing metric points at a
specific audit defect to fix first.

**Canary steps (sticky per query via `canaryBucket`, `router.go:204-213`):**

1. `ModeShadow`, `SampleRate` small — collect the gate metrics. (Where we are.)
2. `ModeCanary`, `CanaryPercent = 1` — serve 1 % of the eligible class from
   Tantivy; watch error rate + user-visible count deltas.
3. `CanaryPercent` 1 → 5 → 25 → 100 with a soak at each step.
4. `ModeTantivy` — serve the whole eligible class. (PG still serves everything
   ineligible.)

**Rollback = a single env flip** to `SEARCH_ENGINE=postgres` (or
`SEARCH_ENABLED=false`). `routerConfig()` forces `ModePostgres` and the path is
pure passthrough (audit §"Master switch"). No redeploy, no data migration,
instant. This is the cheapest rollback in the whole stack and is the reason to
keep the mode env-driven, not config-file-driven.

---

## 6. Failure modes & bounds (one is a hard prerequisite)

- **Serving is now on the hot path** (shadow was detached via
  `context.WithoutCancel`, `router.go:106`). The serving Tantivy RPC therefore
  needs its **own bounded deadline** (a new `ServeTimeout`, default ~800 ms —
  well inside a web request budget, distinct from the 5 s `ShadowTimeout`). On
  deadline or **any** RPC error → **fall through to PG and serve** (log + a
  `serve_fallback` counter). Serving must **never** return an error the PG path
  could have answered — the fail-closed-to-PG contract is identical to the L3
  route's (`composer.go:1163-1172`).
- **Sidecar down / not SERVING / stale** → caught by the §4 health gate *before*
  the RPC (ineligible → PG), so a dead sidecar costs zero per-query RPCs, exactly
  as `Composer.Healthy()` gates the L3 route (`torrent_content.go:213`).
- **Hard prerequisite — the unbounded-goroutine fix (audit #8, task #6, in
  parallel).** Today `r.run = func(f){ go f() }` (`router.go:81`) spawns one
  unbounded goroutine per sampled shadow. Serving does *not* add goroutines (it
  runs inline on the request), but a canary runs shadow **and** serve, so the
  flip still amplifies the existing leak. **Cap shadow concurrency and default
  `SEARCH_SAMPLE_RATE ≪ 1` before any `ModeCanary`/`ModeTantivy` flip** — this is
  already a standalone blocker (audit revival #8); Phase 6 inherits it as a
  gate, it does not re-solve it.
- **Match-set correctness defects gate the *count* not the crash.** Audit defects
  #1 (phrase-over-group) and #2 (prefix-expansion cap) mean Tantivy can miss docs
  PG returns. These surface as `PGCount != TantivyCount` and a depressed
  `JaccardAt20` — caught by the §5 gates, not a runtime failure. Serving a
  slightly-different set is acceptable *within* the gate thresholds; below them,
  fix the defect first (revival #6).

---

## 7. Explicitly **out** of Phase 6 scope

- **Ranking-quality / BM25 tuning.** `WithFreqs` vs `WithFreqsAndPositions`
  (audit #5), field-boost tuning, the phrase/prefix defects (#1/#2) — Phase 6
  *serves* the ranking the engine produces and *measures* it via the gates; it
  does not tune it. Sub-threshold parity is a signal to fix those items, not a
  Phase-6 deliverable.
- **Facets / aggregations from Tantivy.** Faceted queries stay on PG (§2). The
  overcount + null-bucket + per-facet-OR gaps (audit #3, plan caveats 1-3) are a
  separate phase. This is why the web UI stays on PG in v1.
- **Structured filters beyond the empty-filter class.** Mapping `content_type` /
  `size` / `release_year` (which the proto *has*) to `pb.SearchFilters` is a
  later whitelist expansion, each entry a new parity surface.
- **Structured sorts.** Blocked on the proto's single-key-sort + zero-score
  field-sort limits.
- **The file-grained (L2/L3) indexes.** Different indexes, different sidecar
  paths, already live — Phase 6 is only the torrent-grained main search.
- **Torznab result-count semantics beyond `TotalHits`.** If a Torznab client
  relies on exact offset-paging past what Tantivy pages cheaply, keep it on PG;
  do not build deep-paging machinery here.

---

## Rollout checklist

- [ ] **(Blocker, external)** 00024 incremental indexer landed & propagating
      deletes/updates (audit #3 / task #7).
- [ ] **(Blocker, external)** Shadow-goroutine concurrency cap + `SAMPLE_RATE ≪ 1`
      default (audit #8 / task #6).
- [ ] Add `watermark_epoch` to `HealthCheckResponse` proto; indexer publishes it.
- [ ] Router serving branch: page in Tantivy, hydrate via PG `IN(...)`, order by
      `InferID` (new `orderItemsByInferID`), `TotalHits` → exact count.
- [ ] Eligibility gate: reuse `canCompare`, add relevance-only + no-facets +
      has-query checks (default-deny).
- [ ] Freshness/health poller for main search (mirror
      `registerPathsearchHealthReporter`); cached `healthy && fresh`.
- [ ] `ServeTimeout` (~800 ms) + fail-closed-to-PG on any error/timeout +
      `serve_fallback` metric.
- [ ] Wire `CanaryPercent` to the real serving decision (retire the
      `router.go:111-115` TODO).
- [ ] Prove shadow gates (§5) over ≥7 days on the eligible class.
- [ ] Canary 1 → 5 → 25 → 100 on a **non-L3 / Torznab** pod first; soak each.
- [ ] `ModeTantivy`; document the one-env-flip rollback in the runbook.
