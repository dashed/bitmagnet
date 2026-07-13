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

## Traffic reality — how the ≥7-day GraphQL shadow runs in THIS fleet

Unlike Phase 1 (the fleet has **no** Torznab client, so its shadow ran on a
synthetic corpus), the GraphQL surface has **real, continuous traffic**: the
Angular webui (`/webui`), the React webui (`/app`), and Hermes all query the live
Go `/graphql`. That is the shadow population, and it must be sampled from
production, not synthesised.

**Selected mechanism (2026-07-12 reconciliation): Go-embedded, not Traefik
mirroring.** The live Go GraphQL path measures its already-computed response and
latency, then a bounded background hook samples eligible read-only operations,
calls the dark Rust GraphQL Service with the same request, and compares Rust with
the captured Go result. This avoids a second Go execution and the self-shadow's
double-PG-read cost. The Rust result is discarded; nothing is served.

Lane P now contains the selected runtime hook (`7c5c7eea`): gqlgen captures the
already-computed Go response, seals request-entry-through-pre-write response
generation latency, and dispatches an admitted Rust request after the primary
handler returns. The old self-shadow/reference-client shape is gone, so a second
Go execution is structurally impossible. Rust supplies its comparable server-side
duration in `X-Bitmagnet-Graphql-Handler-Duration-Us`; missing/invalid timing and
Rust/GraphQL/projection failures enter the soak-validity denominator rather than
silently disappearing.

**🚨 HARD SAFETY GATE — classify before the Rust call.** Live `/graphql` traffic
includes mutations. The embedded hook MUST parse/select the GraphQL operation
first and hard-drop anything other than a read-only `query` before calling Rust.
This remains load-bearing even though Go executes the primary request only once:
the Rust mutation stubs may become real later. Parse failures and ambiguous
multi-operation documents fail closed. The existing operation-gate tests are the
starting proof; runtime-hook tests must assert zero Rust calls for mutations,
subscriptions, and unclassifiable documents.

**Sampling + node-contention guard (06 R5):** use
`context.WithoutCancel`/bounded timeout, a low sample rate, and a semaphore-capped
runner modelled on `internal/search/router`. Lane I owns only the dark Service,
Go→Rust egress, configuration, and observability plumbing. There is no Traefik
mirror or mirrored-body buffer to configure.

---

## Reconciled execution status (2026-07-13)

This table supersedes the original status cells in the detailed task-definition
tables below. Those rows remain the scope contract, but were not kept current while
the lane work was produced.

| Lane | Verified / current tip | Current state | Remaining critical path |
|---|---|---|---|
| Base | `cb7c970e` | verified; base check/fmt/clippy/test passed | preserve the currently untracked verification log/helper if desired |
| S | `9dbf257b` | expanded canonical result item plus composer-only decoded-file carrier; fmt/strict clippy, 47 tests (live-PG ignored), Torznab 41/41 | S4 ordering/paging/counts; S5 live-PG differential parity; real C adapter tests |
| C | `43a22125` | C3b bounded composer implemented behind `lane-s-stub`; Coder fmt/strict clippy/stub check + 37 tests | re-point to real Lane S; Rust allocator/RSS measurement; C4, C6, full C7 bounds |
| G | `4240eaaf` | federated cached health/workers resolvers plus positive handler-duration header; fmt/strict clippy/tests/SDL/bin green | tier routing, search/file result mapping, remaining read resolvers, Dockerfile; regenerate workspace lockfile at integration |
| P | `7c5c7eea` | Go-embedded hook complete: one Go execution, query/projection gate, bounded Rust client, comparable latency, validity metrics/rules/dashboard; race/lint/typecheck/promtool green | integrate with the Rust G service; low-rate dark soak after Lane I; optional redirect/content-type hardening |
| G0/P1 | `244f66cd` | complete; reference only | none |
| I | homelab `master` | not started | dark GraphQL role/image import/CNP/ServiceMonitor after final binary contract |

**C5 status:** `watermark_epoch` is already complete in source and live. C5 is
blocked only on search-quality evidence; the 2026-07-13 production snapshot is far
below every agreement threshold, so it remains dormant regardless of the formal
2026-07-16 evaluation date.

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
| S1 | **Contract-first API expansion (commit EARLY).** Extend the crate's public types to the full GraphQL search surface: the complete `Criteria` enum (all ~20 leaves — content_type, torrent_source, torrent_tag, file_type, language, content_genre, release_year, video_resolution/source/codec/3d/modifier, size, published_at, info_hash, content identifiers, episodes, file_extension JSONB `@>`, attribute, collection), the full order field enum (`order_torrent_content_enum.go`), a `SearchOptions`-shaped input carrying filters + order + facets-requested + page window + `total_count`/`has_next_page` flags, and a `GenericResult`-shaped output (`items`, `total_count`, `total_count_is_estimate`, `has_next_page`, `aggregations`). Document as `CONTRACT.md` §Phase-2. Lanes C/G code against it. | ✅ through `75d920c6`; canonical item expanded in `2aae661b`, decoded-file seam `9dbf257b` |
| S2 | **Criteria builders (SQL port).** Port every criteria builder from `internal/database/search/criteria_*.go` to the crate's hand-written sqlx SQL (house style, no ORM), incl. the dynamic-join derivation (`torrents`/`content`/`content_collections` pulled in only when a criterion/order/facet requires them — `extractRequiredJoins`). The heavy ones: `criteria_torrent_content_published_at.go` (281 LOC), `criteria_content_identifier.go`, `criteria_torrent_file_extension.go` (JSONB `@>`, gated by `GateFileExtensionsJSONB`). Unit tests assert SQL shape (no DB). | ✅ `75d920c6` |
| S3 | **9-facet aggregation execution.** Port `query/facets.go` + the 9 `facet_*.go` builders + `gqlmodel/facet.go`'s key→agg mapping: per-facet aggregation subqueries with the aggregation **budget** (default 5000, `budgeted_count()` estimate path, `TotalCountIsEstimate`), the natsort ordering of agg items, and the `content_type/torrent_source/torrent_tag/file_type/language/content_genre/release_year/video_resolution/video_source` facet keys exactly as the resolver expects them. Unit tests assert per-facet SQL + item ordering. | ✅ `75d920c6`; natural tag ordering fixed in `2aae661b` |
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
| C1 | **Contract-first trait + config (commit EARLY).** Define the resolver-callable trait (`TorrentContent`, `SearchFileRows`, `Suggest`/typeahead, `CollapsePaths`), the `(result, served: bool)` return shape, and the full `ComposerConfig`/`ServeConfig` knob struct (every `SEARCH_PATHSEARCH_*` env + the Phase-6 `SEARCH_*` serve knobs) with defaults. Lane G codes against this; until it lands, Lane G builds against an in-crate stub behind a feature flag (Phase-1 Lane-T pattern). | ✅ `4e4ee6ee` |
| C2 | **L3 client + health snapshot.** Port `pathsearch/client.go` (the `PathCandidates`/`Suggest`/`HealthCheck` gRPC client on the Phase-0 tonic stack) + `pathsearch/health.go` (the lock-free cached health snapshot, fail-closed default). Reuse `bitmagnet-proto` `PathSearchService`. | ✅ `4e4ee6ee` |
| C3 | **The L1 blob-refine composer (the XL core).** Port `pathsearch/composer.go` (1426 LOC) + `refine.go` (218): the pipeline `candidates()` (L3 oversample+truncate) → `FileCounts()` cheap probe (PK point-lookup on `torrent_file_summary.file_count`, NO blob decode) → `declineOversized()` → `chunkByFileBudget()` → per-chunk `candidateRows()` (PG `IN()` + hydrate via Lane S) → `refineMatches()` (blob decode → exact substr/ext/size predicate) → Go-side `paginate()` → decode-free refined-set facet re-aggregation → estimated `TotalCount` from sidecar `candidate_total`. **All gate-7 bounds ported with their Go defaults** and re-derived for Rust's allocator (see §Gate thresholds): `MaxRefineFiles=300_000`, `RefineFileBudget=300_000`, `MaxChunkTorrents=1024`, `RetainedFileBudget=1_000_000`, `RouteTimeout=8s`, `MaxCandidates=2000`, `MaxConcurrentRefines=NumCPU`, the `SlotWait` load-shed. **Fail-safe-to-PG decorator semantics:** `served=false` / `refineFailLoud()` / zero-candidate-while-unhealthy → PG fallback; cap reasons (`capNone/capRetained/capDeadline`) serve the accumulated top-relevance prefix with `TotalCountIsEstimate=true`, never a PG broad-FTS wall. | 🟡 stub-backed pipeline `43a22125`; real Lane-S adapter and Rust RSS re-derivation remain acceptance blockers |
| C4 | **File-grained variant + typeahead.** Port `pathsearch/file_rows.go` (585): `SearchFileRows` + `PathTypeahead`/`Suggest` (FileRow sort fields, `visitMatchingFiles`, `pageFileRows`) — backs the GraphQL `fileSearch` text route + `pathTypeahead`. | ⬜ pending |
| C5 | **Tantivy-serve router-decorator (Phase-6 fold-in — GATED, see risks).** Port `router.go`'s serving branch per `phase6-tantivy-served-design.md §1–§4`: the eligibility gate (free-text + no structured filters via `canCompare` + relevance-only order + no facets), the freshness gate (cached `healthy && fresh` poller, `maxStaleness=2min`, reads `watermark_epoch` off `HealthCheckResponse`), and the serve path (Tantivy RPC under `ServeTimeout≈800ms` → hydrate hit info-hashes from PG via Lane S → `orderItemsByInferID` → `TotalHits` exact count, `TotalCountIsEstimate=false`). Fail-closed-to-PG on any error/timeout. **Precedence L3 → Tantivy → PG** (composer intercepts first; on `served=false` the residual hits the router). The incremental indexer and health-check watermark prerequisites are complete; do not serve until a fresh ≥7-day P4 shadow soak passes every quality threshold. | ⬜ blocked on search quality (Risk P2-2) |
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
| G0 | **SDL-normalizer spike FIRST (scheduled before the resolver bulk).** Before any resolver volume commits to the gate mechanism (Risk P2-1), prove it: stand up a minimal async-graphql schema covering ONLY the hard fidelity cases (the 7 custom scalars incl. `Void`, a representative enum with Go-string values, and the `Omittable`/nullable-input wrapper choice) and run it through Lane P's P1 normalizer against `testdata/parity/schema.graphql` → 0 diffs on that subset. This de-risks the 0-diff gate (and pins the `Option<Option<T>>` vs `MaybeUndefined` decision) before G1's full schema and G2-G4's resolvers depend on it. **Co-scheduled with P1** (the normalizer) as the joint first move of Lanes G+P. | ✅ `244f66cd` spike, adopted into G/P |
| G1 | **Code-first schema = the SDL (0-diff target).** Define the async-graphql code-first schema reproducing all 869 lines / 10 `.graphqls` files: the 7 custom scalars (`Hash20`→infohash20, `Hash32`→v2, `Date`, `DateTime`, `Duration`, `Year`, `Void`), all 13 enums with **value strings identical to the Go domain enums** (generated by `enums/gen/genenums.go` — the strings are the contract), all object/input types with the exact nullability the gqlgen config produces (`nullable_input_omittable: true` + `omit_slice_element_pointers: true` → async-graphql `Option`/`MaybeUndefined`), AND the **full Mutation type declarations** (even though Phase 2 is read-only — the SDL golden includes `mutation.graphqls`; see Risk P2-3 for how mutations are routed). Gate: the SDL 0-diff test (P1). | ✅ `e159f7bd`; gate remains green at `4240eaaf` |
| G2 | **Read resolvers — Query root + search.** Port the read resolvers (`query.resolvers.go` + `gqlmodel/torrent_content.go` + `facet.go`): `Query.{version, workers, health, queue, torrent, torrentContent}`; `TorrentContentQuery.{search, fileSearch, fileSearchFacets, pathTypeahead, collapsePaths}`; the thin **resolver-level tier selection** (mirror gqlmodel: try Lane-C composer if `TypeaheadEnabled && hasQueryString && Eligible && pathsearchOrderEligible && Healthy` → else Lane-S decorated PG; fileSearch text via `shouldRouteFileSearchText` → `SearchFileRows` else L2); the 3 `QueryOptions` sets (Combined/Refine/Agg), page clamps (`maxPathSearchLimit=200`), and `transformTorrentContentSearchResult` / `transformTorrentContentAggregations`. **No dataloaders** — mirror Go's eager one-round-trip hydration (Risk P2-4). | ⬜ pending |
| G3 | **Read resolvers — Torrent / Queue / Health.** Port `TorrentQuery.{files, listSources, suggestTags, metrics}` (incl. the G2-blob `torrent_files` path via `gqlmodel/torrent_files.go` + `collapse_paths.go`), `QueueQuery.{jobs, metrics}` (`queue.resolvers.go` — read only), and `HealthQuery.{status, checks}` incl. the **federated peer health merge** (`resolvers/health_peer.go`, 296 LOC — multi-instance aggregation across peers; fork-complex, budget review). | 🟡 cached health/workers federation `c8c71255`; Torrent/Queue reads pending |
| G4 | **axum handler + composition root + bin.** The `POST/GET /graphql` handler + playground on the Phase-0 `bitmagnet-common` bootstrap (`serve_with_shutdown`, metrics, config, `grpc.health.v1` N/A here — HTTP `/livez`+`/status`), an explicit composition root wiring Dao(Lane-S PG pool) + Lane-C search-serve + config (replacing Go's fx graph — hairy-part #8), and the `goose_db_version` boot assert (04 §3.2: Rust asserts, never migrates). | 🟡 HTTP/bin scaffold `e159f7bd`, handler-duration contract `4240eaaf`; real S/C resolver wiring pending |

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
mechanism ships (Lane I owns only the dark-service deployment and Go-to-Rust
egress/configuration).

| # | Task | Status |
|---|------|--------|
| P1 | **SDL 0-diff golden gate.** Wire the Rust `bitmagnet-graphql::schema().sdl()` output through a normalizer that canonicalizes it to the SAME shape as the Phase-0 `testdata/parity/schema.graphql` golden (the P0 golden is a *normalized concatenation of the source `.graphqls` files*; async-graphql's printed SDL differs in field/type ordering, scalar/directive syntax, and description formatting — the normalizer MUST reconcile both to one canonical form). CI assert → **0 diffs**. This is Risk P2-1's control; **define the normalization rules explicitly** (they are the crux of the gate). | ✅ `464ba7d6` |
| P2 | **GraphQL shadow comparator + embedded runtime hook.** The response projection, Jaccard/RBO/Top1/count metrics, and operation classifier are complete. Replace the uncalled self-shadow driver with a Go operation/response hook that captures the primary Go result and latency, classifies the selected operation before any Rust call, and asynchronously calls Rust under sampling, timeout, and semaphore limits. Unit tests must prove mutations, subscriptions, ambiguous documents, and parse failures make zero Rust calls. | ✅ `7c5c7eea` |
| P3 | **Numeric gate wiring + soak dashboard.** Wire the gate thresholds (§Gate thresholds) as a promql/alert bundle over the `graphql_shadow_*` series so the ≥7-day soak is machine-evaluable (Top1≥0.98, JaccardAt20≥0.90, RBO≥0.92, count-match≥0.95, Rust p99 ≤ Go p99), plus the composer-bound counters (zero `refine_*_capped` unexpected spikes / zero unbounded refines). Same evidential discipline as the Phase-1 shadow (a passing gate is the R8 review backstop). | ✅ `7c5c7eea`; Rust-validity denominator added; metric-name reconciliation waits on C6 |

Estimate: **M, ~1–2 ew.** The SDL normalizer (P1) is the fiddly bit; the comparator
extends existing math.

---

## Lane I — deploy IaC, ships DARK (branch: homelab master, own files)

Owns (homelab repo): new `ansible/roles/bitmagnet-graphql/**`, a `graphql` image
kind in `playbooks/bitmagnet_image_import.yml` + Makefile, the dark Kubernetes
Service, Go→Rust egress, and sampling/client configuration. Mirrors Phase-1 Lane I
(the `bitmagnet-torznab` role pattern). **NO deploys, NO route flip, NO cutover** —
cutover is USER-GATED and outside this ledger.

| # | Task | Status |
|---|------|--------|
| I1 | **Role per the sidecar pattern.** `bitmagnet-graphql` role following Phase-0/1 conventions: tag-only image pin, `IfNotPresent`, Cilium CNP default-deny + Prometheus allow + PG allow + L3/Tantivy sidecar allow, ServiceMonitor, `BITMAGNET_METRICS_ADDR`, the `goose_db_version` boot-assert env, and resource limits so dark comparisons cannot starve the live path (06 R5). Registry-less image pipeline gains the `graphql` image kind (excluded from `IMAGE=all` until the Dockerfile lands, per Phase-1 I2). | ⬜ pending |
| I2 | **Dark Service + embedded-shadow plumbing.** Stand up the internal-only dark GraphQL Service, allow Go→Rust egress, and provide default-off endpoint/sample-rate/timeout/concurrency settings with a single-revert kill switch. Do not add a Traefik mirror. The Go runtime hook must already enforce the operation gate before this can be enabled. A future serve-cutover route remains separate and user-gated. | ⬜ pending |

Estimate: **S, ~0.5–1 ew.** Reuses the Phase-1 torznab role wholesale; the new
pieces are Go→Rust policy and embedded-shadow configuration.

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
  freshness) is already complete in `463d0d32` and present in the Phase-2 base.

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
  **Scheduled (lead-approved):** the scalar+enum+nullability spike against P1's
  normalizer is **row G0** — the joint first move of Lanes G+P, before the resolver
  volume, so the gate mechanism is proven before the bulk port commits to it.
- **P2-2 — Tantivy-serve (C5) is QUALITY-GATED (04 §4.2).** The Go-side
  prerequisites are complete: the 00024 follow-contract incremental indexer is live,
  and `463d0d32` added `watermark_epoch` to `SearchService.HealthCheckResponse`.
  Production reports a fresh watermark and the 2026-07-13 propagation probe passed.
  The only remaining gate is a new, valid ≥7-day P4 main-search shadow soak meeting
  every threshold. The current live evidence is not borderline: 3376 comparisons
  yielded Jaccard@20 0.120, RBO 0.108, and Top1 1.42%, versus 0.90/0.92/0.98 gates.
  Treat that as a ranking/membership defect, not as a date gate; repair it and restart
  the soak before C5 can serve.

  **Honest framing:** Phase 2 ships without Tantivy-serve if quality remains below
  threshold. The full builder (S), composer/L3 route (C1-C4,C6,C7), and GraphQL API
  (G) remain a valid shippable slice with PG authoritative. C5 stays dormant and
  fail-closed-to-PG until a new soak passes. For Phase 2, retain the gRPC boundary to
  the existing sidecar; embedding Tantivy in-process remains a deferred optimization.
- **P2-3 — the SDL must declare mutations Phase 2 does not implement.** The 0-diff
  golden includes `mutation.graphqls` (`TorrentMutation.*`, `QueueMutation.*`), so
  the code-first schema MUST declare the full Mutation type while Phase 2 remains
  read-only. During dark operation no live traffic is routed to Rust; the embedded
  hook rejects mutations before its Rust call. Before any full endpoint cutover,
  Rust mutation resolvers must proxy the original request to the warm Go endpoint
  (or mutation implementations must land). Do not assume Traefik can inspect a
  GraphQL POST body and route by operation type.
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
  `43a22125` fixes the representation mismatch found in review: retained matches
  now move their decoded `Vec<BlobFile>` into an explicit result carrier and clear
  the raw blob, so the file-count budget describes the data that remains live. The
  absolute constants are still Go-derived and are **not accepted** until
  production-shaped Rust RSS measurement and the remaining C7 stress tests pass.
  This is where deep human review (not just the parity corpus) is required — the
  corpus proves match-set parity, not memory-bound parity; C7 must assert the bounds
  under a synthetic large-torrent load.
- **P2-6 — shadow adds one sampled Rust/PG read (06 R5).** The selected Go-embedded
  hook reuses the already-computed primary Go result and adds only the sampled Rust
  execution; it must never re-issue the Go request. Control the remaining load with
  a low sample rate, non-blocking semaphore admission, a hard timeout, off-peak
  soak, resource limits, and `NodeDiskIOSaturation`/live-p99 abort signals.

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
review of that XL piece (06 R8), not LOC. If C5 (Tantivy-serve) remains deferred by
the P2-2 quality gate, the phase lands at the lower end and C5 follows on.
