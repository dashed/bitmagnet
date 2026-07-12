# Phase 2 — GraphQL read API + tier routing + L1 composer + facets: task ledger (started 2026-07-11)

Execution ledger for `05-roadmap-and-gates.md §Phase 2` + `04 §3` (pod layout,
goose rule) + `04 §4.2` (Tantivy-serving fold-in) + `phase6-tantivy-served-design.md`
(the serving spec Phase 2 inherits). Five lanes, disjoint crate/dir ownership,
branched off `rust-rewrite-phase1-20260710` (@ `66ff3b0a`).

**Phase 1 is COMPLETE.** All 10 rows across Lanes Q/T/G/I are ✅ (66/66 Torznab
goldens green): the `bitmagnet-search-query` crate ships the Torznab predicate
subset + FTS tokenizer, `bitmagnet-torznab` ships the axum adapter, the parity
goldens + shadow-replay harness are in, and the dark deploy role landed on homelab
master. Q2b (content-JOIN hydration for `release_year`/`imdb_id`/`tmdb_id`, branch
`p1-q2b-hydration`) is the last Phase-1 consolidation and is assumed merged into
the Phase-2 base before build lanes fork. Phase 2 **builds on that crate**, not on
the docs' guesses.

**The Rules of engagement in `phase0-tasks.md` apply verbatim** (WIP-commit +
push before/after every codex stage and any long wait; all builds/tests in a
Coder workspace via a self-healing `remote-test.sh` with `$HOME` toolchains +
`CARGO_BUILD_JOBS=4`; stay in your lane — if you need a file another lane owns,
STOP and message the team lead; honest failure reports with log evidence, never
force-commit red; team-lead review before any merge into the Phase-2 integration
branch) plus Phase 1's rule: **builds take explicit SHAs, never branch names**
(the worktree-push stale-ref trap).

---

## 0. Base-branch prep (team lead, before any lane forks)

Three consolidation steps the lead lands on the Phase-2 base branch first, so no
lane races on shared files (the same discipline as Phase 1, where crate skeletons +
`Cargo.toml` member lines were pre-created on the base branch):

- **P0-0 — cut the base branch AFTER Q2b merges.** Q2b (content-JOIN hydration for
  `release_year`/`imdb_id`/`tmdb_id`, branch `p1-q2b-hydration`) is the last Phase-1
  consolidation and is **in flight now**. The Phase-2 base branch is cut *after* it
  merges, so **Lane S builds on the hydrated `fetch()`** (the full-builder expansion
  extends the hydrated result path, not the pre-Q2b one). Build lanes fork off this
  post-Q2b base.
- **P0-1 — crate skeletons + workspace members.** Create empty skeletons for the
  three new crates (`bitmagnet-fts`, `bitmagnet-search-serve`, `bitmagnet-graphql`)
  and add their `members` lines to `bitmagnet-rs/Cargo.toml`. **No lane touches
  `bitmagnet-rs/Cargo.toml` after this** (Phase-0/1 rule).
- **P0-2 — the `bitmagnet-fts` extraction (the "team-lead FTS-consolidation").**
  Move the FTS tokenizer that currently lives inside `bitmagnet-search-query`
  (`src/fts.rs` + `src/fts/{tables,tokenizer}.rs`, ~330 LOC, the Go
  `internal/database/fts` port proven at 4223 tokenizer fixtures) into the new
  `bitmagnet-fts` crate and re-point `bitmagnet-search-query` at it. This is a pure
  move + re-export (the tokenizer parity fixtures move with it); it exists so both
  Lane S (full builder) and any future consumer share ONE tokenizer, not two
  copies. Gate: `cargo test -p bitmagnet-fts` green on the moved fixtures + Lane
  S's crate still builds. Doing it as a lead consolidation avoids Lane S and a
  future ingest-side FTS consumer both forking the tokenizer.

Both are small and mechanical; they exist to remove cross-lane file contention,
not to add scope.

---

## Traffic reality — how the ≥7-day GraphQL shadow actually runs in THIS fleet

Unlike Phase 1 (the fleet has **no** Torznab client, so its shadow ran on a
synthetic corpus), the GraphQL surface has **real, continuous traffic**: the
Angular webui (`/webui`), the React webui (`/app`), and Hermes all query the live
Go `/graphql`. That is the shadow population, and it must be sampled from
production, not synthesised.

**Mechanism (lead with this):** Traefik **request-mirroring**. The Rust GraphQL
service ships **dark** (Lane I) as a second Kubernetes Service; a Traefik
`mirroring` service copies a sampled fraction of live `/graphql` POSTs to it. The
dark service runs a **shadow mode** in which, per mirrored request, it (a) executes
its own resolvers, (b) issues the identical query to the live Go `/graphql` as the
reference, and (c) runs a comparator over the two responses — diffing the
**result-set** (ordered `InferID` list), the **9 facet counts**, and the
**total-count** — emitting `graphql_shadow_*` metrics (the numeric gate feed). The
Rust result is discarded; nothing is served. This keeps the **Go side entirely
untouched** during the dark soak (the whole point of a dark deploy) and reuses the
existing `internal/search/shadow` comparator math (Jaccard/RBO/Top1/count-delta,
`comparator.go`) extended with facet-count + total-count diffs.

**🚨 HARD SAFETY GATE — mutations must NEVER be double-executed (blocking, not a
note).** Live `/graphql` traffic includes **mutations** (`QueueMutation.*`,
`TorrentMutation.{delete,putTags,setTags,deleteTags,reprocess}`). The mirrored copy
reaches the dark Rust service, and its self-shadow *issues a reference call to the
live Go `/graphql`* — so a mutation reaching that reference call would **double-apply
a prod side effect** (a second delete, a second reprocess enqueue). The self-shadow
therefore MUST, in this order:

1. **Parse the GraphQL operation type FIRST**, before any execution or reference
   call. Anything that is not a read-only `query` operation (i.e. any `mutation`,
   and any `subscription`) is **hard-dropped** — no Rust resolver execution, no Go
   reference call, no comparison. Only `query` operations proceed.
2. **Only re-issue READ (`query`) operations** to the Go reference endpoint.

This is a correctness gate on the comparator (Lane P, P2) AND the mirror wiring
(Lane I, I2), verified by a test that a mutation document produces zero Go
reference calls. It is the load-bearing safety property of the whole shadow
mechanism.

Additional mirror controls (Lane I, I2): a **kill-switch flag on the Traefik mirror**
(Traefik-side, default off, single-revert), a **sampling-percentage knob**, and a
**`maxBodySize` bound** (Traefik request-mirroring buffers request bodies to
replay them — set `maxBodySize` sanely so large GraphQL POST bodies don't balloon
proxy memory).

**Why not the P4-router pattern verbatim?** The existing search shadow
(`internal/search/router`) is *Go-embedded* — the Go resolver samples and
background-compares against the Tantivy sidecar. Reusing that shape for GraphQL
would require editing the Go resolver to fan out to a Rust service, i.e. a Go
change during the dark phase. Traefik-mirror + Rust-self-compare gets the same
signal with zero Go delta. **Documented fallback** (if Traefik mirroring proves
lossy on POST bodies or the self-reference fan-out doubles PG load unacceptably):
a Go-embedded sampling comparator modelled on `router.go`'s `runShadow`
(`context.WithoutCancel` background compare, `SampleRate ≪ 1`, semaphore-capped) —
same numeric gate, at the cost of a temporary Go-side shadow hook. That fallback
compares against the Go result the resolver **already computed**, so it needs no
reference fan-out and the mutation-double-execute hazard does not arise for it.
Lane P owns the comparator either way; Lane I owns the Traefik mirror wiring.

**Sampling + node-contention guard (06 R5):** the mirror runs at
`SEARCH_SAMPLE_RATE`-style low fraction, semaphore-capped, off the ARC-CI peak, so
the dark self-shadow (which doubles PG read load on the sampled slice) can't starve
the live path on the single HEL1 node. Wire `NodeDiskIOSaturation` + live-path p99
as soak abort signals.

---

## Lane S — full PG search-query builder + FTS crate (branch `p2s-searchquery`)

Owns: `bitmagnet-rs/crates/bitmagnet-search-query/**` (EXPAND the existing crate),
`bitmagnet-rs/crates/bitmagnet-fts/**` (the P0-2 extraction it inherits),
`testdata/parity/searchquery/**`, new Go generator test files in `internal/parity/`
(new files only). Read-only reference (NO changes): `internal/database/search/**`,
`internal/database/query/**`, `internal/gql/gqlmodel/{facet,torrent_content}.go`
(the facet-key → agg mapping + FIND-2), `internal/gql/gqlmodel/collapse_paths.go`.

Phase 1 shipped only the Torznab predicate subset. Lane S expands the crate to the
**full** builder the GraphQL API needs: every criteria builder, all 9 facet
aggregations, full ordering (incl. the GraphQL-only FIND-2 popularity-sort), and
offset/limit paging with total-count + has-next-page. This is the cross-lane
contract Lanes C and G both consume, so its **public API lands EARLY** (S1).

| # | Task | Status |
|---|------|--------|
| S1 | **Contract-first API expansion (commit EARLY).** Extend the crate's public types to the full GraphQL search surface: the complete `Criteria` enum (all ~20 leaves — content_type, torrent_source, torrent_tag, file_type, language, content_genre, release_year, video_resolution/source/codec/3d/modifier, size, published_at, info_hash, content identifiers, episodes, file_extension JSONB `@>`, attribute, collection), the full order field enum (`order_torrent_content_enum.go`), a `SearchOptions`-shaped input carrying filters + order + facets-requested + page window + `total_count`/`has_next_page` flags, and a `GenericResult`-shaped output (`items`, `total_count`, `total_count_is_estimate`, `has_next_page`, `aggregations`). Document as `CONTRACT.md` §Phase-2. Lanes C/G code against it. | ⬜ pending |
| S2 | **Criteria builders (SQL port).** Port every criteria builder from `internal/database/search/criteria_*.go` to the crate's hand-written sqlx SQL (house style, no ORM), incl. the dynamic-join derivation (`torrents`/`content`/`content_collections` pulled in only when a criterion/order/facet requires them — `extractRequiredJoins`). The heavy ones: `criteria_torrent_content_published_at.go` (281 LOC), `criteria_content_identifier.go`, `criteria_torrent_file_extension.go` (JSONB `@>`, gated by `GateFileExtensionsJSONB`). Unit tests assert SQL shape (no DB). | ⬜ pending |
| S3 | **9-facet aggregation execution.** Port `query/facets.go` + the 9 `facet_*.go` builders + `gqlmodel/facet.go`'s key→agg mapping: per-facet aggregation subqueries with the aggregation **budget** (default 5000, `budgeted_count()` estimate path, `TotalCountIsEstimate`), the natsort ordering of agg items, and the `content_type/torrent_source/torrent_tag/file_type/language/content_genre/release_year/video_resolution/video_source` facet keys exactly as the resolver expects them. Unit tests assert per-facet SQL + item ordering. | ⬜ pending |
| S4 | **Ordering incl. FIND-2 + paging + counts.** Port `order_torrent_content.go` (all order fields + the `_order_<i>` alias projection) AND the **GraphQL-only FIND-2** popularity-sort rewrite (`gqlmodel/torrent_content.go`: lone-relevance+query → `seeders DESC`, flag `POPULARITY_SORT_DEFAULT`, default OFF) — this does NOT apply on the Torznab path but DOES here. Offset/limit paging with `WithTotalCount(true)` → `doCount` + the has-next-page over-fetch (+1). **NB: TorrentContent pagination is offset/limit, not keyset/cursor** (`query/resolve.go` — `ResolvedOptions{Limit, Offset}`, HasNextPage via over-fetch). The "cursor pagination" wording in **05 §Phase 2** and **01 §1.8** is a misnomer for *this* builder; the S1 contract carries the corrected offset/limit framing. (The only genuine keyset cursor in the tree is L2 `FileSearchService`'s `FilePagination{limit,cursor}`, 01 §2.4 — unrelated to this builder.) CONTRACT.md does not repeat the error, so no crate edit here. | ⬜ pending |
| S5 | **Full-builder differential parity.** Extend the Phase-1 Go generator (`internal/parity/`, new file) to emit fixture pairs `{SearchOptions JSON → (ordered InferID list, per-facet counts, total_count) }` from the REAL Go builder against the live-PG CI lane's seeded fixtures; Rust `#[ignore]` integration test consumes them via `bitmagnet-diff` → 0 diffs. Corpus MUST cover: every facet, multi-criteria AND/OR, JSONB extension filter, FIND-2 on/off, estimate vs exact count, and deterministic tie-broken orders (per the Phase-1 CONTRACT tie-break warning). | ⬜ pending |

Estimate: **L, ~3–4 ew.** Mechanical against the Go builder (the Go tree is the
spec), review-bound on the facet + FIND-2 semantics and the differential corpus.

---

## Lane C — L1 composer + tier routing + Tantivy-serve (branch `p2c-searchserve`)

Owns: `bitmagnet-rs/crates/bitmagnet-search-serve/**` (NEW). Read-only reference
(NO changes): `internal/search/pathsearch/**` (composer + refine + client + health
+ metrics — the port target), `internal/search/router/**` +
`internal/search/shadow/**` (the Tantivy router/serve/shadow templates),
`internal/search/searchfx/module.go` (wiring), `bitmagnet-rs/crates/bitmagnet-search/**`
(the Tantivy main-search sidecar it RPCs to). Consumes Lane S for PG hydrate +
PG-fallback. **The XL lane of Phase 2** (hairy-part #6).

This crate is the search *backend* the GraphQL resolvers call: it houses the L1
blob-refine composer, the L3 gRPC client, and the engine-level Tantivy-serve
router-decorator. It exposes ONE resolver-callable entry (`TorrentContent(filters,
opts, limit, offset, sorts) -> (result, served)`) whose `served=false` return is
the resolver's signal to fall to plain Lane-S PG search. Its public trait +
config-struct **land EARLY** (C1) as Lane G's contract.

| # | Task | Status |
|---|------|--------|
| C1 | **Contract-first trait + config (commit EARLY).** Define the resolver-callable trait (`TorrentContent`, `SearchFileRows`, `Suggest`/typeahead, `CollapsePaths`), the `(result, served: bool)` return shape, and the full `ComposerConfig`/`ServeConfig` knob struct (every `SEARCH_PATHSEARCH_*` env + the Phase-6 `SEARCH_*` serve knobs) with defaults. Lane G codes against this; until it lands, Lane G builds against an in-crate stub behind a feature flag (Phase-1 Lane-T pattern). | ⬜ pending |
| C2 | **L3 client + health snapshot.** Port `pathsearch/client.go` (the `PathCandidates`/`Suggest`/`HealthCheck` gRPC client on the Phase-0 tonic stack) + `pathsearch/health.go` (the lock-free cached health snapshot, fail-closed default). Reuse `bitmagnet-proto` `PathSearchService`. | ⬜ pending |
| C3 | **The L1 blob-refine composer (the XL core).** Port `pathsearch/composer.go` (1426 LOC) + `refine.go` (218): the pipeline `candidates()` (L3 oversample+truncate) → `FileCounts()` cheap probe (PK point-lookup on `torrent_file_summary.file_count`, NO blob decode) → `declineOversized()` → `chunkByFileBudget()` → per-chunk `candidateRows()` (PG `IN()` + hydrate via Lane S) → `refineMatches()` (blob decode → exact substr/ext/size predicate) → Go-side `paginate()` → decode-free refined-set facet re-aggregation → estimated `TotalCount` from sidecar `candidate_total`. **All gate-7 bounds ported with their Go defaults** and re-derived for Rust's allocator (see §Gate thresholds): `MaxRefineFiles=300_000`, `RefineFileBudget=300_000`, `MaxChunkTorrents=1024`, `RetainedFileBudget=1_000_000`, `RouteTimeout=8s`, `MaxCandidates=2000`, `MaxConcurrentRefines=NumCPU`, the `SlotWait` load-shed. **Fail-safe-to-PG decorator semantics:** `served=false` / `refineFailLoud()` / zero-candidate-while-unhealthy → PG fallback; cap reasons (`capNone/capRetained/capDeadline`) serve the accumulated top-relevance prefix with `TotalCountIsEstimate=true`, never a PG broad-FTS wall. | ⬜ pending |
| C4 | **File-grained variant + typeahead.** Port `pathsearch/file_rows.go` (585): `SearchFileRows` + `PathTypeahead`/`Suggest` (FileRow sort fields, `visitMatchingFiles`, `pageFileRows`) — backs the GraphQL `fileSearch` text route + `pathTypeahead`. | ⬜ pending |
| C5 | **Tantivy-serve router-decorator (Phase-6 fold-in — GATED, see risks).** Port `router.go`'s serving branch per `phase6-tantivy-served-design.md §1–§4`: the eligibility gate (free-text + no structured filters via `canCompare` + relevance-only order + no facets), the freshness gate (cached `healthy && fresh` poller, `maxStaleness=2min`, reads `watermark_epoch` off `HealthCheckResponse`), and the serve path (Tantivy RPC under `ServeTimeout≈800ms` → hydrate hit info-hashes from PG via Lane S → `orderItemsByInferID` → `TotalHits` exact count, `TotalCountIsEstimate=false`). Fail-closed-to-PG on any error/timeout. **Precedence L3 → Tantivy → PG** (composer intercepts first; on `served=false` the residual hits the router). **GATED — do not serve until both P2-2 gates clear** (Go-side 00024 indexer + `watermark_epoch` proto; P4 shadow soak favorable ≥2026-07-16). If unmet at build time, Phase 2 ships without C5 and it follows on. | ⬜ blocked (Risk P2-2) |
| C6 | **Composer + serve metrics.** Port `pathsearch/metrics.go` (292) + `router/metrics.go`: the `search_pathsearch_*` series (`route_total{result}`, `refine_declined_oversized_total`, `refine_retained_capped_total`, `refine_deadline_capped_total`, `refine_shed_total`, `refine_agg_error_total`, health/watermark gauges) and `search_serve_*` (`total{outcome}`, sidecar_healthy, watermark) on the Phase-0 `bitmagnet-common` metrics layer — metric-name parity gated by the Phase-0 metric-name golden. | ⬜ pending |
| C7 | **Composer bound tests (the gate-7 backstop).** Port the Go composer bound tests (`composer_bound_test.go` 574, `composer_chunk_test.go` 983, `composer_route_test.go` 166) as Rust tests proving the bounds hold: oversized decline fail-loud, per-chunk file budget cap, retained-file-budget cap, route-deadline cap, load-shed, and **zero unbounded refines** under a synthetic large-torrent load. This is R8's review backstop for the XL piece. | ⬜ pending |

Estimate: **XL, ~4–6 ew** (the tentpole of Phase 2). C3 is the irreducible cost —
the chunked exact-refine pipeline with intricate memory/latency bounds is
review-heavy (06 R8); C5 is gated (see risks) and may split to a follow-on.

---

## Lane G — async-graphql read API (branch `p2g-graphql`)

Owns: `bitmagnet-rs/crates/bitmagnet-graphql/**` (NEW, crate + bin). Read-only
reference (NO changes): `internal/gql/**` (schema SDL, resolvers, gqlmodel,
`gqlgen.yml`, enums), `graphql/schema/*.graphqls` (the 869-line SDL — the 0-diff
target), `internal/gql/gqlmodel/{torrent_content,facet,file_search}.go` (the
resolver→search glue). Consumes Lane S (full builder) + Lane C (composer/serve
trait) — until those land, builds against their EARLY-committed API skeletons /
in-crate stubs behind a feature flag (Phase-1 Lane-T pattern).

| # | Task | Status |
|---|------|--------|
| G1 | **Code-first schema = the SDL (0-diff target).** Define the async-graphql code-first schema reproducing all 869 lines / 10 `.graphqls` files: the 7 custom scalars (`Hash20`→infohash20, `Hash32`→v2, `Date`, `DateTime`, `Duration`, `Year`, `Void`), all 13 enums with **value strings identical to the Go domain enums** (generated by `enums/gen/genenums.go` — the strings are the contract), all object/input types with the exact nullability the gqlgen config produces (`nullable_input_omittable: true` + `omit_slice_element_pointers: true` → async-graphql `Option`/`MaybeUndefined`), AND the **full Mutation type declarations** (even though Phase 2 is read-only — the SDL golden includes `mutation.graphqls`; see Risk P2-3 for how mutations are routed). Gate: the SDL 0-diff test (P1). | ⬜ pending |
| G2 | **Read resolvers — Query root + search.** Port the read resolvers (`query.resolvers.go` + `gqlmodel/torrent_content.go` + `facet.go`): `Query.{version, workers, health, queue, torrent, torrentContent}`; `TorrentContentQuery.{search, fileSearch, fileSearchFacets, pathTypeahead, collapsePaths}`; the thin **resolver-level tier selection** (mirror gqlmodel: try Lane-C composer if `TypeaheadEnabled && hasQueryString && Eligible && pathsearchOrderEligible && Healthy` → else Lane-S decorated PG; fileSearch text via `shouldRouteFileSearchText` → `SearchFileRows` else L2); the 3 `QueryOptions` sets (Combined/Refine/Agg), page clamps (`maxPathSearchLimit=200`), and `transformTorrentContentSearchResult` / `transformTorrentContentAggregations`. **No dataloaders** — mirror Go's eager one-round-trip hydration (Risk P2-4). | ⬜ pending |
| G3 | **Read resolvers — Torrent / Queue / Health.** Port `TorrentQuery.{files, listSources, suggestTags, metrics}` (incl. the G2-blob `torrent_files` path via `gqlmodel/torrent_files.go` + `collapse_paths.go`), `QueueQuery.{jobs, metrics}` (`queue.resolvers.go` — read only), and `HealthQuery.{status, checks}` incl. the **federated peer health merge** (`resolvers/health_peer.go`, 296 LOC — multi-instance aggregation across peers; fork-complex, budget review). | ⬜ pending |
| G4 | **axum handler + composition root + bin.** The `POST/GET /graphql` handler + playground on the Phase-0 `bitmagnet-common` bootstrap (`serve_with_shutdown`, metrics, config, `grpc.health.v1` N/A here — HTTP `/livez`+`/status`), an explicit composition root wiring Dao(Lane-S PG pool) + Lane-C search-serve + config (replacing Go's fx graph — hairy-part #8), and the `goose_db_version` boot assert (04 §3.2: Rust asserts, never migrates). | ⬜ pending |

Estimate: **L, ~2–3 ew.** Resolver volume is mechanical; the cost concentrates in
SDL fidelity (G1, the 0-diff gate is unforgiving — see Risk P2-1) and the federated
health merge (G3).

---

## Lane P — SDL golden + GraphQL shadow + numeric gate (branch `p2p-parity`)

Owns: new Go-side parity test files in `internal/parity/**` (new files only),
`internal/gql/*parity_test.go` (new files only), `testdata/parity/graphql/**`
(creates it), and the shadow comparator/harness (new files). Read-only reference:
`internal/search/shadow/comparator.go` (the Jaccard/RBO/Top1 math to extend),
`internal/gql/schema_sdl_parity_test.go` (the existing Go-side SDL↔resolver guard),
`testdata/parity/schema.graphql` (the Phase-0 B1 golden). Mirrors Phase-1 Lane G
(goldens + gates), and owns the shadow **comparator** regardless of which shadow
mechanism ships (Lane I owns the Traefik wiring).

| # | Task | Status |
|---|------|--------|
| P1 | **SDL 0-diff golden gate.** Wire the Rust `bitmagnet-graphql::schema().sdl()` output through a normalizer that canonicalizes it to the SAME shape as the Phase-0 `testdata/parity/schema.graphql` golden (the P0 golden is a *normalized concatenation of the source `.graphqls` files*; async-graphql's printed SDL differs in field/type ordering, scalar/directive syntax, and description formatting — the normalizer MUST reconcile both to one canonical form). CI assert → **0 diffs**. This is Risk P2-1's control; **define the normalization rules explicitly** (they are the crux of the gate). | ⬜ pending |
| P2 | **GraphQL shadow comparator.** Extend the `shadow.Compare` math (Jaccard@20/@50, RBO p=0.9, Top1, count-delta) with **per-facet-count diff** and **total-count diff** over the full GraphQL response; a driver that, given a query, diffs the Rust response vs the live Go `/graphql` reference and emits `graphql_shadow_*` metrics. Reused by whichever mechanism ships (Traefik-mirror self-shadow in the Rust service, or the Go-embedded fallback). | ⬜ pending |
| P3 | **Numeric gate wiring + soak dashboard.** Wire the gate thresholds (§Gate thresholds) as a promql/alert bundle over the `graphql_shadow_*` series so the ≥7-day soak is machine-evaluable (Top1≥0.98, JaccardAt20≥0.90, RBO≥0.92, count-match≥0.95, Rust p99 ≤ Go p99), plus the composer-bound counters (zero `refine_*_capped` unexpected spikes / zero unbounded refines). Same evidential discipline as the Phase-1 shadow (a passing gate is the R8 review backstop). | ⬜ pending |

Estimate: **M, ~1–2 ew.** The SDL normalizer (P1) is the fiddly bit; the comparator
extends existing math.

---

## Lane I — deploy IaC, ships DARK (branch: homelab master, own files)

Owns (homelab repo): new `ansible/roles/bitmagnet-graphql/**`, a `graphql` image
kind in `playbooks/bitmagnet_image_import.yml` + Makefile, the dark Kubernetes
Service + the Traefik **mirroring** wiring for the shadow, all behind a default-off
flag. Mirrors Phase-1 Lane I (the `bitmagnet-torznab` role pattern). **NO deploys,
NO route flip, NO cutover** — cutover is USER-GATED and outside this ledger.

| # | Task | Status |
|---|------|--------|
| I1 | **Role per the sidecar pattern.** `bitmagnet-graphql` role following Phase-0/1 conventions: tag-only image pin, `IfNotPresent`, Cilium CNP default-deny + Prometheus allow + PG allow + L3/Tantivy sidecar allow, ServiceMonitor, `BITMAGNET_METRICS_ADDR`, the `goose_db_version` boot-assert env, resource limits sized so the dark self-shadow can't starve the live path (06 R5). Registry-less image pipeline gains the `graphql` image kind (excluded from `IMAGE=all` until the Dockerfile lands, per Phase-1 I2). | ⬜ pending |
| I2 | **Dark Service + Traefik shadow mirror.** Stand up the dark GraphQL Service and the Traefik `mirroring` service that copies a sampled fraction of live `/graphql` POSTs to it, behind `bitmagnet_graphql_shadow_enabled: false` (default off, **single-revert kill-switch**), with a sampling-percentage knob and a sane `maxBodySize` (Traefik mirroring buffers request bodies). The **mutation-double-execute safety gate** (§Traffic reality) is enforced Rust-side in the self-shadow, but the mirror wiring must document it as a precondition. The serve-cutover IngressRoute (route `/graphql` to Rust, Go kept warm; **mutations routed to Go** per Risk P2-3) is PREPARED behind a second default-off flag but NOT wired. | ⬜ pending |

Estimate: **S, ~0.5–1 ew.** Reuses the Phase-1 torznab role wholesale; the Traefik
mirror is the only new mechanism.

---

## Cross-lane contracts (fixed here so no lane waits)

- **`bitmagnet-search-query` full public API (Lane S, S1 — commit EARLY).** The
  `SearchOptions` input + `GenericResult` output + full `Criteria`/order enums.
  Lanes C (hydrate/fallback) and G (resolvers) both code against it. Frozen at S1.
- **`bitmagnet-search-serve` resolver trait + config (Lane C, C1 — commit EARLY).**
  The `TorrentContent(...) -> (result, served)` entry + `ComposerConfig`/`ServeConfig`.
  Lane G codes against it; stub behind a feature flag until C1 lands (Phase-1 Lane-T
  pattern).
- **`bitmagnet-fts` crate API (pre-lane P0-2).** The tokenizer `tokenize`/
  `tokenize_flat` + `app_query_to_tsquery` surface, re-exported by
  `bitmagnet-search-query`. Frozen before lanes fork.
- **The SDL golden (`testdata/parity/schema.graphql`)** is Phase-0 B1's artifact,
  read-only for Lane P; Lane G's schema must normalize to it (P1 owns the
  normalizer, G1 owns matching it).
- **Fixture roots — no overlap:** `testdata/parity/searchquery/` (Lane S),
  `testdata/parity/graphql/` (Lane P). Composer bound tests live in-crate (Lane C).
- **`bitmagnet-proto` `watermark_epoch` addition** (needed by C5 Tantivy-serve
  freshness) is a Go-side/proto prerequisite tracked under Risk P2-2 — NOT owned by
  a build lane; the lead sequences it with the Go incremental indexer.

---

## Gate thresholds (from the roadmap + phase6 §5)

- **SDL golden:** `schema().sdl()` normalized diff vs `testdata/parity/schema.graphql`
  → **0 diffs** (custom scalars, enum value strings, nullability). CI merge gate
  (06 R3).
- **Shadow (≥7 days, sampled real traffic):** per-query **result-set** (ordered
  `InferID`), **9 facet-count**, and **total-count** diff, Rust vs Go.
- **Numeric (search-serving, phase6 §5):** `Top1Match ≥ 0.98`, `JaccardAt20 ≥ 0.90`,
  `RBO ≥ 0.92`, `count-match ≥ 0.95`, **Rust p99 ≤ Go p99** on the served path.
  Sub-threshold on any → do not cut over; the failing metric points at the defect.
- **Composer gate-7 bounds (must hold under load, C7 proves + C6 meters):**
  `MaxRefineFiles=300_000` (per-torrent, oversized declined fail-loud),
  `RefineFileBudget=300_000` (per-chunk transient decode ≤ ~300MB),
  `RetainedFileBudget=1_000_000` (cumulative retained, ~200MB — the match-rate-
  independent bound), `RouteTimeout=8s` (whole route), `ServeTimeout≈800ms` (Tantivy
  serve RPC, phase6 §6). **Fail-loud accounting parity; zero unbounded refines.**
  The Rust byte-per-file constants must be **re-derived for Rust's allocator** (Go's
  ~1KB/file transient and ~200B/file retained assume Go's slice layout; a Rust
  `Vec<u8>` blob decode differs — C3 must re-measure, not copy the byte figures
  blind — see Risk P2-5).
- **Rollback (all served paths):** env/route flip to the Go endpoint (kept warm);
  the composer + Tantivy-serve are decorators → PG-only fallback is intrinsic
  (fail-safe-to-PG, phase6 §6).

---

## Risks & open questions (Phase-2-specific — lead reviews before build lanes spawn)

- **P2-1 — async-graphql SDL fidelity vs gqlgen (06 R3, Low×High).** async-graphql
  is code-first; its printed `schema.sdl()` differs from the Go source `.graphqls`
  in **type/field ordering, custom-scalar declaration syntax, enum value emission,
  directive placement, and description/comment formatting**. The Phase-0 golden is
  a *normalized concatenation of the source files*, NOT an introspection dump — so
  the 0-diff gate hinges on a normalizer (P1) that reconciles BOTH sides to one
  canonical form. **Open:** is the P0 golden the right canonical target, or does the
  gate need a fresh introspection-based golden generated from the Go server? The
  gqlgen quirks to pin: `Omittable[T]`/`nullable_input_omittable` → which
  async-graphql wrapper (`Option<Option<T>>` vs `MaybeUndefined`) reproduces the
  exact nullability; `omit_slice_element_pointers`; and whether async-graphql can
  emit the `Void` scalar + the exact enum strings without per-type overrides.
  **Recommend:** spike G1's scalar+enum+nullability subset against P1's normalizer
  FIRST, before the resolver volume — the gate mechanism must be proven before the
  bulk port commits to it.
- **P2-2 — Tantivy-serve (C5) is GATED on non-rewrite prerequisites (04 §4.2).**
  Serving is conditional on **two independent gates**, both outside the build lanes:
  1. **The Go-side serving prerequisites (phase6 §4).** The 00024 follow-contract
     incremental indexer (so deletes/updates propagate — serving a stale index is a
     correctness bug) **and** adding `watermark_epoch` to `SearchService`
     `HealthCheckResponse` (proto change; today it carries only `{status, doc_count}`).
     Both are Go/sidecar-track hardening, explicitly **not** rewrite work (04 §4.2
     says keep prerequisites on the Go track).
  2. **The P4 Tantivy shadow soak coming back favorable (≥2026-07-16).** The
     re-instrumented Phase-4 shadow (comparator on `bitmagnet-0` + ServiceMonitor +
     synthetic-traffic CronJob, shipped 2026-07-09) is running a **7-day soak whose
     evaluation date is ≥2026-07-16** (homelab memory
     `bitmagnet-remaining-work-audit-2026-07-09`). C5 must not cut over until that
     soak's numeric gates (the same phase6 §5 thresholds this ledger uses) come back
     favorable — an unfavorable soak points at a match-set defect (audit #1/#2) that
     must be fixed in the Go/Tantivy engine *before* the Rust serve path inherits it.

  **Honest framing:** if either gate is unmet when Lane C reaches C5, **Phase 2
  ships WITHOUT Tantivy-serve** — the full builder (S) + composer + L3 route
  (C1-C4,C6,C7) + GraphQL API (G) is a valid, shippable partial (the read path is
  Rust, L3 serves, PG is authoritative), and C5 becomes a gated follow-on the
  moment both gates clear. **Open:** live status of the 00024 incremental indexer
  (homelab memory notes follow ON as of 2026-07-09, but the phase6 doc still lists
  the incremental indexer as a prerequisite — lead confirms before C5 unblocks) and
  the P4 soak verdict. Also **open:** does the serving service **embed** the Tantivy search
  crate in-process (00 §2 / 06 Q6 recommendation — removes the gRPC hop once both
  are Rust) or keep RPCing the sidecar (phase6's assumption)? For Phase 2, keep the
  gRPC boundary (matches the sidecar it inherits); flag in-process embed as a
  deferred optimization — this is the "Tantivy index ownership between
  bitmagnet-search and the new service" question.
- **P2-3 — the SDL must declare mutations Phase 2 does not implement.** The 0-diff
  golden includes `mutation.graphqls` (`TorrentMutation.*`, `QueueMutation.*`), so
  the code-first schema MUST declare the full Mutation type — but Phase 2 is
  read-only. **Options:** (a) declare mutation resolvers that proxy to the live Go
  `/graphql` (keeps the endpoint whole), or (b) the dark/served Rust service handles
  only reads and Traefik routes mutation operations to Go (operation-level routing).
  **Recommend (b)** for the dark soak + initial cutover — it avoids porting write
  paths (Phase 3+ scope) while keeping the SDL whole; revisit when the write side
  is ported. Either way the SDL declares them; only the routing differs.
- **P2-4 — dataloader parity.** Go has **no dataloaders** — N+1 is avoided by eager
  hydration in one SQL round-trip (`TorrentContentCoreJoins`, batch-hydrated
  `FileSearchItem.torrentContent`). async-graphql *offers* a `DataLoader`, but using
  it would change the query shape and the shadow's PG-load profile. **Recommend:**
  mirror Go's eager-hydration model exactly (no async-graphql DataLoader) so the
  shadow compares like-for-like and the single-round-trip contract holds.
- **P2-5 — the composer's bounded-memory refine in Rust (06 R8, the XL review
  risk).** The gate-7 byte budgets (`RefineFileBudget` ~300MB transient at ~1KB/file,
  `RetainedFileBudget` ~200MB at ~200B/file) are calibrated to **Go's** slice/GC
  layout. A Rust `Vec<u8>` blob-decode + retained-match set has a different memory
  profile; C3 must **re-measure the per-file cost under Rust and re-derive the
  budgets to preserve the same worst-case RSS**, not copy the Go byte figures blind.
  This is where deep human review (not just the parity corpus) is required — the
  corpus proves match-set parity, not memory-bound parity; C7 must assert the bounds
  under a synthetic large-torrent load.
- **P2-6 — shadow doubles PG load on the sampled slice (06 R5).** The Traefik-mirror
  self-shadow has the Rust service query BOTH its own resolvers AND live Go
  `/graphql` (the reference) per mirrored request — doubling PG reads on the sampled
  fraction, on the single HEL1 node that already flaps `NodeDiskIOSaturation`.
  Control: low sample rate, semaphore cap, off-peak soak, resource limits, and
  `NodeDiskIOSaturation`/live-p99 as abort signals. **Open:** if even the sampled
  double-read is too costly, fall back to the Go-embedded comparator (compares
  against the primary Go result it already computed — no reference fan-out), at the
  cost of a temporary Go-side hook.

---

## Estimate ladder

| Lane | Scope | Difficulty | Engineer-weeks |
|---|---|---|---|
| S | full PG search-query builder + FTS crate | L (mechanical vs Go) | 3–4 |
| C | L1 composer + tier routing + Tantivy-serve | **XL** (hairy-part #6) | 4–6 |
| G | async-graphql read API | L (SDL-fidelity fiddly) | 2–3 |
| P | SDL golden + shadow + numeric gate | M | 1–2 |
| I | deploy IaC (dark) | S (reuses torznab role) | 0.5–1 |
| | **Total** | | **~10.5–16** |

Matches the roadmap's Phase-2 estimate (**10–16 ew**, §05 §3). The XL cost is Lane
C's composer (C3) + its bound re-derivation (P2-5); the schedule risk is human
review of that XL piece (06 R8), not LOC. If C5 (Tantivy-serve) is deferred on the
P2-2 prerequisite, the phase lands at the lower end and C5 follows on.
