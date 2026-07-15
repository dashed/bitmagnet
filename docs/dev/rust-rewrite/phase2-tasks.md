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

## Reconciled execution status (2026-07-15)

This table supersedes the original status cells in the detailed task-definition
tables below. Those rows remain the scope contract, but were not kept current while
the lane work was produced.

| Lane | Verified / current tip | Current state | Remaining critical path |
|---|---|---|---|
| Base | `cb7c970e` | verified; base check/fmt/clippy/test passed | preserve the currently untracked verification log/helper if desired |
| Integration | latency candidate `edb112be`; historical accepted evidence `2b8d2912`; deployed ledger `7e5e1360` | S/C/G/P merged; the old accepted artifact remains immutably pinned and the internal-only dark Service is healthy. The bounded aggregation-concurrency candidate is source-gated but has not passed a fresh exact-image RSS gate, promotion, deploy, or production latency proof | fresh Maple four-artifact gate -> exact promotion/pins -> dark deploy -> reset/pilot/new green-hour cohort; keep G3 point reads outside eligibility; deploy-branch merge |
| S | `e7e712c8` | S1-S5 complete; 17-case live-PostgreSQL generator/Rust differential gate rerun 2026-07-13 with zero diffs | none for the Phase-2 search builder |
| C | byte envelope `deb351d7`; component evidence `d173f2e6`; accepted gate `2b8d2912` | C1-C4/C6/C7 integrated; compressed/decompressed, decoded-allocation, and retained-string byte limits close the owned-path defect; release Linux component and repeated whole-container RSS scenarios pass | C5 remains separately quality-gated |
| G | `3addfd5c` + projection fix in `deb351d7` | SDL/search/file/facet/typeahead/collapse, real S+C runtime, HTTP lifecycle, metrics, container, and lookahead-controlled file retention complete | G3 Queue/Torrent point-read fields remain explicitly unserved; mutations remain declarations only |
| P | `7c5c7eea` + hardening through `41c63910` | Go-embedded hook executes Go once and admits only root `torrentContent.search` queries plus `__typename`; mixed root siblings, redirects, and invalid response media types fail closed. A corrected 44-row rotation passed correctness, but the live p99 gate failed and the evidence driver is suspended | admit/deploy the source-gated latency candidate, then reset and prove a fresh full green hour before any >=7-day clock |
| G0/P1 | `244f66cd` | complete; reference only | none |
| I | role `6f3ee63`; RSS evidence `2b8d2912`; homelab pin `4dc037f` | exact accepted artifact promoted without rebuilding; immutable pin and internal-only Deployment/ClusterIP/EndpointSlice/CNPs/ServiceMonitor are live and healthy; generic enablement remains fail-closed and the live L3 workload has no Rust GraphQL shadow env | separately authorize low-rate shadow and >=7-day soak; routing/cutover and C5 remain off |

**C5 status:** `watermark_epoch` is already complete in source and live. C5 is
blocked only on search-quality evidence; the 2026-07-13 production snapshot is far
below every agreement threshold, so it remains dormant regardless of the formal
2026-07-16 evaluation date.

### Integration verification at `3addfd5c` (2026-07-13)

- Coder Rust 1.97 exact-tree gate: `cargo fmt --all --check`, workspace/all-target/
  all-feature clippy with `-D warnings`, and `cargo test --workspace --all-features`
  all returned zero. The remote harness now transfers the complete `testdata/`
  tree, including the file-extension fixture required by all-target builds.
- Local GraphQL gate: 42 library tests, 9 binary tests, and the full SDL 0-diff
  test passed. The search-serve gate passed 56 library tests, two RSS tests, and
  the PostgreSQL adapter integration target.
- Disposable PostgreSQL 16 gate: the Go S5 fixture generator, ignored Rust
  full-builder parity driver, and real C3 `PgSearch` adapter test all passed. The
  regenerated 17-case search corpus was byte-identical.
- `go test ./...` passed. The Rust config-env fixture exactly matches the
  122-line Go ground truth, including all six `GRAPHQL_SHADOW_*` keys and
  `SEARCH_PATHSEARCH_MAX_DECODE_CANDIDATES`.
- `docker build --locked` succeeded from `Dockerfile.graphql` on Rust 1.97. The
  local verification image was `sha256:414d1376e804...`, ran as `65532:65532`,
  and exposed the expected GraphQL entrypoint/help surface.
- Homelab `f97c1c0` passed the GraphQL playbook and parent image-import syntax
  checks, production-profile targeted ansible-lint, rendered-YAML parsing,
  targeted yamllint, 29/29 Rust-to-Deployment `SEARCH_*` set parity, and
  `git diff --check`. It was committed and pushed but **not deployed at that
  checkpoint**; the 2026-07-15 internal dark closeout below supersedes that
  historical state.
- Intentional compatibility debt: Rust rejects unsafe cross-budget override
  orderings that Go accepts and caps dynamically; production values satisfy the
  stricter ordering and rejection tests pin it. The L2 tonic client does not
  support Go's Unix-socket target syntax; production uses a ClusterIP endpoint.

### P2-5 byte-envelope and deployment-admission closeout (2026-07-15)

- `deb351d7` bounds compressed SQL input and streaming zstd/MessagePack output,
  fallible decode growth, decoded MessagePack plus owned path/extension bytes per
  chunk, and retained owned strings per request. Every route, including the
  single-chunk path, now holds the refine semaphore. GraphQL lookahead retains
  files only when `torrent.files` is selected.
- The selected envelope is 64 MiB per compressed/decompressed blob, 128 MiB
  decoded allocation per chunk, and 64 MiB retained owned strings per request,
  with four concurrent refines. Homelab `6f3ee63` wires and validates those exact
  values. Generic inventory auto-enablement remains false; the accepted evidence
  permitted only the separate fail-closed RSS-admission boolean to advance at
  this checkpoint. A later confirmed dedicated deployment supplied an explicit
  override and revalidated the immutable image pin.
- Release Linux x86_64 component evidence in `d173f2e6` covers chunk pressure,
  retained pressure, the accepted 650-byte-path positive control, mixed lengths,
  and adversarial 1024-byte paths. The accepted control retained 88,561 files at
  about 128,076 KiB peak without a cap; the 1024-byte case retained zero and
  peaked at about 69,656 KiB after a bounded cap, versus the old approximately
  1.24 GiB unbounded result.
- The relevant package gates pass: GraphQL 44 library + 9 binary + SDL parity;
  model 26 plus fixtures; search-query 52; search-serve 63 plus both RSS binaries
  and the live-adapter target. Focused all-target clippy denies warnings, Cargo
  formatting is clean, and the focused Go shadow/HTTP tests pass.
- `bench/graphql-rss` through `3f555ef5` builds the exact GraphQL image and exercises
  real PostgreSQL/sqlx hydration, a fresh L3 mock per case, four simultaneous
  clients, minimal versus `torrent.files` projections, and kernel cgroup-v2 RSS/
  OOM evidence. It requires amd64, three repeats, a fixed 8 GiB cgroup and 6 GiB
  peak ceiling, source-linked images, immutable image IDs, stable workspace
  provenance, verified cleanup, and complete response/barrier/state evidence.
  A forced-RLS hook was dynamically proven with four distinct PostgreSQL
  backends after the composer semaphore; the helper Docker context is 39.46 kB.
  An independent exact-commit re-review found no remaining code blockers after
  all nine original findings closed. The runner records separate GraphQL and
  helper builder backends, uses Docker-managed evidence volumes plus `docker cp`
  so client and daemon need no shared host filesystem, creates JSONL exclusively,
  and verifies volume cleanup. The Kata/VFS compatibility path keeps classic
  Docker available for GraphQL while the helper remains on BuildKit; native Maple
  uses BuildKit for both. All 21 harness unit tests, Python compile, shell syntax,
  dynamic TERM normalization, and fail-closed cleanup probes pass.
- Homelab `222a811` provisions `ansible-bot/bm-p2-rss` with a 4-CPU/16-GiB DinD
  request, 8-CPU/16-GiB limit, VFS, 100-GiB ephemeral Docker store, 4-GiB dev
  container, 20-GiB home PVC, amd64, cgroup v2, and `kata-qemu`. Runtime preflight
  passes with Docker 29.6.1, 11 visible CPUs, and 23,487,074,304 bytes.
- The first historical Coder smoke at `687bb0fc` reached no workload case: at
  2026-07-15 03:43:28Z the Kata agent timed out and QEMU was killed while HEL1's
  63-GiB `/dev/shm` backing store was 95% full from concurrent workspaces. Host OOM
  counters remained zero. The guarded `bm-p2-rss` lane remains anti-affinity-
  serialized and Pending rather than interrupting other owners' workspaces; this
  failed pre-workload attempt is not acceptance evidence.
- The isolated native-amd64 Maple run at exact clean source/harness commit
  `3f555ef5` closes deployment admission. Smoke session
  `9a22a902a86f46fbae0edd8b42df3f74` passed 4/4 cases, then gate session
  `43e73ec5c09e4b7fa63dd3f98f66d18b` passed 12/12 cases under the fixed 8 GiB
  cgroup with a 755,322,880-byte maximum peak. Every per-run evaluation, HTTP/L3
  and SQL four-party barrier, source/image provenance check, and terminal cleanup
  passed; swap peak and cgroup OOM, OOM-kill, and OOM-group-kill counters were zero.
- Maple retained substantial host headroom: minimum `MemAvailable` was 22,968,144
  KiB, minimum Docker-root free space was 881,615,292 KiB, memory PSI `some` and
  `full` maxima were both 0.00, Docker sampling had no terminal failures, and the
  Hermes pod identity/readiness/restart snapshot was byte-identical before and
  after. No Coder workspace or production Bitmagnet node was benchmarked or
  changed.
- Accepted evidence is committed at `2b8d2912` as the exact four-file set below:
  - `graphql-rss-gate-maple-20260715-3f555ef5.jsonl` —
    `fe8f8f9ebf02413e7ae7637e6c7c0086e5a0a4091fcc7465b393ec8fee06b020`
  - `graphql-rss-gate-maple-20260715-3f555ef5.smoke.jsonl` —
    `09510f81be26f8ee312268afcdc498289b1eb5bb5fcc7fc0fbe151b33beec08a`
  - `graphql-rss-gate-maple-20260715-3f555ef5.headroom.jsonl` —
    `1c4a3ab627ea4c76e35a5783486d8ff2430deeb5b85d230d39dbca9e01243a7b`
  - `graphql-rss-gate-maple-20260715-3f555ef5.inventory.json` —
    `5052c3bd8cddb335f52dfcad97c95fe65764122e0cf2d507b9e788856c336a83`
  This evidence accepts only the P2-5 RSS gate. Promotion and the internal dark
  deployment were authorized separately and completed later; GraphQL shadow
  sampling, any cutover, and C5 remain independently gated.

### Internal-only dark deployment closeout (2026-07-15)

- Homelab `d5540b2` added the guarded exact-artifact promotion path, `7309f33`
  hardened its cleanup proof, and `4dc037f` pinned the result. The accepted Maple
  image was promoted byte-for-byte without rebuilding after proving accepted ref
  `3f555ef5`, reconciled ref `7e5e1360`, and their shared `bitmagnet-rs` tree
  `f9d6a83773397e241237c71482188aafe0cf1038`.
- The intended live tag is
  `ghcr.io/dashed/bitmagnet-graphql:graphql-rss-gated-20260715-7e5e1360` at
  immutable OCI index
  `sha256:a503f1cf4c111f7b41aa012c92322fe2ac38eb8b0e71f7f17d59ac1a8a740b54`.
  HEL1 retains the exact tar with SHA-256
  `f2a12e005108b4b5703af91256077880b8c19a7117081316d41ae1d0b0852179`.
- The separately confirmed deploy created only the internal dark
  `bitmagnet-graphql` Deployment and ClusterIP Service plus its EndpointSlice,
  four Cilium policies, and ServiceMonitor. The pod is Ready 1/1 with zero
  restarts; `/livez`, `/status`, goose 25, internal dependency health, logs, and
  Prometheus `up == 1` pass.
- There is no IngressRoute, certificate, DNS record, external IP, Tantivy egress,
  GraphQL shadow traffic, routing change, or cutover. Existing ingress still
  targets Go `bitmagnet-l3:3333`, and the live L3 Deployment has no Rust GraphQL
  shadow environment. Generic homelab inventory runs remain fail-closed with
  GraphQL enablement false; only the confirmed dedicated playbook supplies the
  deployment override and revalidates the pin.
- G3 Queue/Torrent point-read fields remain explicit `unserved(...)` errors and
  were excluded from this infrastructure-only health soak because no user or
  shadow query reaches Rust. They must be implemented or excluded by an explicit
  eligibility contract before shadow traffic can select them.
- The guarded RSS Coder workspace remains Pending under cross-namespace
  anti-affinity. Active Coder identities, container IDs, and restart counts were
  unchanged across promotion and deployment; no active workspace was
  interrupted.

### Post-closeout aggregation-latency candidate (2026-07-15)

- The repaired Go L3 candidate completed a fresh 11-batch/44-row production
  rotation with perfect ranked, exact-count, facet, and error results. The first
  green-hour attempt nevertheless failed closed when stored sample
  `2026-07-15T19:50:29.972Z` moved Rust/reference p99 to approximately
  `0.488448s/0.39424s` as one old slow Go observation aged out of the rolling
  hour. The traffic controller immediately suspended the evidence driver. Go
  remains authoritative and public routing did not change.
- Source diagnosis found three avoidable Rust PostgreSQL costs: items, total
  count, and aggregations were top-level serial work; facet bucket counts were
  serial; and the refined aggregation pass hydrated items that the composer
  discarded. `ee6773ea` runs the independent top-level branches with
  `try_join!`, keeps facet groups deterministic and sequential while bounding
  bucket-query fanout to four, and makes the composer aggregation request
  explicitly itemless with `limit=0`. Against the production pool of eight,
  the resulting approximate per-request peak is six active queries, leaving two
  connections of headroom.
- `edb112be` adds a deterministic, timing-free regression proving exactly four
  facet futures overlap, no fifth starts early, all seven complete, and peak
  concurrency remains four. Its full tree is
  `7800cf2c551e63eb6294ddeb52065da1d91859f4`; the `bitmagnet-rs` subtree is
  `8fe7347e47bd250e47d5725bc99cac5752ebb544`.
- Exact clean-source verification on Rust 1.97 passed formatting; workspace,
  all-target, all-feature Clippy with warnings denied; all workspace
  all-feature tests and doctests; and cache-bypassed `go test -count=1 ./...`.
  A disposable PostgreSQL 16 run regenerated the real Go 17-case corpus and ran
  the ignored Rust differential driver with zero diffs. The fixture remained
  byte-identical at SHA-256
  `52e74d3fc01314172267f77c66601db726066a27aa77c9fff192e2a0bc405513`.
  The Coder test cgroup recorded no OOM or OOM-kill events, and no protected
  workspace was restarted or stopped by this work.
- Homelab `e3c5f06` pins both trusted RSS entry points to the full source commit.
  This is source evidence only. The old `7e5e1360` image remains deployed; the
  replacement still requires a fresh trusted Maple smoke/gate/headroom/inventory
  set, evidence commit, immutable promotion-pin refresh, byte-for-byte promotion,
  dark deploy attestation, counter reset, supervised pilot, and a wholly new
  production green-hour cohort before the seven-day clock can start.

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
| S4 | **Ordering incl. FIND-2 + paging + counts.** Port `order_torrent_content.go` (all order fields + the `_order_<i>` alias projection) AND the **GraphQL-only FIND-2** popularity-sort rewrite (`gqlmodel/torrent_content.go`: lone-relevance+query → `seeders DESC`, flag `POPULARITY_SORT_DEFAULT`, default OFF) — this does NOT apply on the Torznab path but DOES here. Offset/limit paging with `WithTotalCount(true)` → `doCount` + the has-next-page over-fetch (+1). **NB: TorrentContent pagination is offset/limit, not keyset/cursor** (`query/resolve.go` — `ResolvedOptions{Limit, Offset}`, HasNextPage via over-fetch). The "cursor pagination" wording in **05 §Phase 2** and **01 §1.8** is a misnomer for *this* builder; the S1 contract carries the corrected offset/limit framing. (The only genuine keyset cursor in the tree is L2 `FileSearchService`'s `FilePagination{limit,cursor}`, 01 §2.4 — unrelated to this builder.) CONTRACT.md does not repeat the error, so no crate edit here. | ✅ `e7e712c8` |
| S5 | **Full-builder differential parity.** Extend the Phase-1 Go generator (`internal/parity/`, new file) to emit fixture pairs `{SearchOptions JSON → (ordered InferID list, per-facet counts, total_count) }` from the REAL Go builder against the live-PG CI lane's seeded fixtures; Rust `#[ignore]` integration test consumes them via `bitmagnet-diff` → 0 diffs. Corpus MUST cover: every facet, multi-criteria AND/OR, JSONB extension filter, FIND-2 on/off, estimate vs exact count, and deterministic tie-broken orders (per the Phase-1 CONTRACT tie-break warning). | ✅ `e7e712c8`; 17-case Go generator + ignored Rust driver rerun against disposable PostgreSQL 16 on 2026-07-13, zero diffs |

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
| C3 | **The L1 blob-refine composer (the XL core).** Port `pathsearch/composer.go` (1426 LOC) + `refine.go` (218): the pipeline `candidates()` (L3 oversample+truncate) → `FileCounts()` cheap probe (PK point-lookup on `torrent_file_summary.file_count`, NO blob decode) → `declineOversized()` → `chunkByFileBudget()` → per-chunk `candidateRows()` (PG `IN()` + hydrate via Lane S) → `refineMatches()` (blob decode → exact substr/ext/size predicate) → Go-side `paginate()` → decode-free refined-set facet re-aggregation → estimated `TotalCount` from sidecar `candidate_total`. **All gate-7 bounds ported with their Go defaults** and re-derived for Rust's allocator (see §Gate thresholds): `MaxRefineFiles=300_000`, `RefineFileBudget=300_000` (count bound per chunk), `MaxChunkTorrents=1024`, `RetainedFileBudget=1_000_000`, `RouteTimeout=8s`, `MaxCandidates=2000`, `MaxConcurrentRefines=NumCPU`, the `SlotWait` load-shed. **Fail-safe-to-PG decorator semantics:** `served=false` / `refineFailLoud()` / zero-candidate-while-unhealthy → PG fallback; cap reasons (`capNone/capRetained/capDeadline`) serve the accumulated top-relevance prefix with `TotalCountIsEstimate=true`, never a PG broad-FTS wall. | ✅ implementation + real adapter `deb351d7`; component RSS `d173f2e6`; accepted integrated GraphQL/sqlx cgroup evidence `2b8d2912` |
| C4 | **File-grained variant + typeahead.** Port `pathsearch/file_rows.go` (585): `SearchFileRows` + `PathTypeahead`/`Suggest` (FileRow sort fields, `visitMatchingFiles`, `pageFileRows`) — backs the GraphQL `fileSearch` text route + `pathTypeahead`. | ✅ `14698fce`, merged at `89465794` |
| C5 | **Tantivy-serve router-decorator (Phase-6 fold-in — GATED, see risks).** Port `router.go`'s serving branch per `phase6-tantivy-served-design.md §1–§4`: the eligibility gate (free-text + no structured filters via `canCompare` + relevance-only order + no facets), the freshness gate (cached `healthy && fresh` poller, `maxStaleness=2min`, reads `watermark_epoch` off `HealthCheckResponse`), and the serve path (Tantivy RPC under `ServeTimeout≈800ms` → hydrate hit info-hashes from PG via Lane S → `orderItemsByInferID` → `TotalHits` exact count, `TotalCountIsEstimate=false`). Fail-closed-to-PG on any error/timeout. **Precedence L3 → Tantivy → PG** (composer intercepts first; on `served=false` the residual hits the router). The incremental indexer and health-check watermark prerequisites are complete; do not serve until a fresh ≥7-day P4 shadow soak passes every quality threshold. | ⬜ blocked on search quality (Risk P2-2) |
| C6 | **Composer + serve metrics.** Port `pathsearch/metrics.go` (292) + `router/metrics.go`: the `search_pathsearch_*` series (`route_total{result}`, `refine_declined_oversized_total`, `refine_retained_capped_total`, `refine_deadline_capped_total`, `refine_shed_total`, `refine_agg_error_total`, health/watermark gauges) and `search_serve_*` (`total{outcome}`, sidecar_healthy, watermark) on the Phase-0 `bitmagnet-common` metrics layer — metric-name parity gated by the Phase-0 metric-name golden. | ✅ `efcbfabd`; shared composer/poller metrics registered once at `3addfd5c` |
| C7 | **Composer bound tests (the gate-7 backstop).** Port the Go composer bound tests (`composer_bound_test.go` 574, `composer_chunk_test.go` 983, `composer_route_test.go` 166) as Rust tests proving the bounds hold: oversized decline fail-loud, per-chunk file budget cap, retained-file-budget cap, route-deadline cap, load-shed, and **zero unbounded refines** under a synthetic large-torrent load. This is R8's review backstop for the XL piece. | ✅ functional/count/byte bounds and component RSS through `d173f2e6`; exact four-client GraphQL/sqlx cgroup admission accepted at `2b8d2912` |

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
| G2 | **Read resolvers — Query root + search.** Port the read resolvers (`query.resolvers.go` + `gqlmodel/torrent_content.go` + `facet.go`): `Query.{version, workers, health, queue, torrent, torrentContent}`; `TorrentContentQuery.{search, fileSearch, fileSearchFacets, pathTypeahead, collapsePaths}`; the thin **resolver-level tier selection** (mirror gqlmodel: try Lane-C composer if `TypeaheadEnabled && hasQueryString && Eligible && pathsearchOrderEligible && Healthy` → else Lane-S decorated PG; fileSearch text via `shouldRouteFileSearchText` → `SearchFileRows` else L2); the 3 `QueryOptions` sets (Combined/Refine/Agg), page clamps (`maxPathSearchLimit=200`), and `transformTorrentContentSearchResult` / `transformTorrentContentAggregations`. **No dataloaders** — mirror Go's eager one-round-trip hydration (Risk P2-4). | ✅ resolver/search surfaces `9829a1a6`, L2 `2036361b`, real Lane-C composition `3addfd5c` |
| G3 | **Read resolvers — Torrent / Queue / Health.** Port `TorrentQuery.{files, listSources, suggestTags, metrics}` (incl. the G2-blob `torrent_files` path via `gqlmodel/torrent_files.go` + `collapse_paths.go`), `QueueQuery.{jobs, metrics}` (`queue.resolvers.go` — read only), and `HealthQuery.{status, checks}` incl. the **federated peer health merge** (`resolvers/health_peer.go`, 296 LOC — multi-instance aggregation across peers; fork-complex, budget review). | 🟡 cached health/workers federation `c8c71255`; Queue jobs/metrics and Torrent files/sources/tags/metrics remain explicitly `unserved(...)` |
| G4 | **axum handler + composition root + bin.** The `POST/GET /graphql` handler + playground on the Phase-0 `bitmagnet-common` bootstrap (`serve_with_shutdown`, metrics, config, `grpc.health.v1` N/A here — HTTP `/livez`+`/status`), an explicit composition root wiring Dao(Lane-S PG pool) + Lane-C search-serve + config (replacing Go's fx graph — hairy-part #8), and the `goose_db_version` boot assert (04 §3.2: Rust asserts, never migrates). | ✅ `3addfd5c`; real S+C runtime, fail-closed poller lifecycle, 29-key env contract, metrics, and Rust 1.97 container |

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
| P2 | **GraphQL shadow comparator + embedded runtime hook.** The response projection, Jaccard/RBO/Top1/count metrics, and operation classifier are complete. Replace the uncalled self-shadow driver with a Go operation/response hook that captures the primary Go result and latency, classifies the selected operation before any Rust call, and asynchronously calls Rust under sampling, timeout, and semaphore limits. Unit tests must prove mutations, subscriptions, ambiguous documents, and parse failures make zero Rust calls. | ✅ runtime `7c5c7eea`; strict mixed-root rejection `90fc6845`; production-query eligibility, metric-contract, redirect, and response-media-type hardening through `41c63910` |
| P3 | **Numeric gate wiring + soak dashboard.** Wire the gate thresholds (§Gate thresholds) as a promql/alert bundle over the `graphql_shadow_*` series so the ≥7-day soak is machine-evaluable (Top1≥0.98, JaccardAt20≥0.90, RBO≥0.92, count-match≥0.95, Rust p99 ≤ Go p99), plus the composer-bound counters (zero `refine_*_capped` unexpected spikes / zero unbounded refines). Same evidential discipline as the Phase-1 shadow (a passing gate is the R8 review backstop). | ✅ runtime `7c5c7eea`; production-L3 label scoping, canary-isolation regression, Compose loading, and standard-task promtool checks through `41c63910` |

Estimate: **M, ~1–2 ew.** The SDL normalizer (P1) is the fiddly bit; the comparator
extends existing math.

---

## Lane I — deploy IaC, ships DARK (branch: homelab master, own files)

Owns (homelab repo): new `ansible/roles/bitmagnet-graphql/**`, a `graphql` image
kind in `playbooks/bitmagnet_image_import.yml` + Makefile, the dark Kubernetes
Service, Go→Rust egress, and sampling/client configuration. Mirrors Phase-1 Lane I
(the `bitmagnet-torznab` role pattern). **Internal dark deployment is live; NO
shadow, route flip, or cutover** — those remain USER-GATED and outside this
ledger.

| # | Task | Status |
|---|------|--------|
| I1 | **Role per the sidecar pattern.** `bitmagnet-graphql` role following Phase-0/1 conventions: tag-only image pin, `IfNotPresent`, Cilium CNP default-deny + Prometheus allow + PG allow + L3/Tantivy sidecar allow, ServiceMonitor, `BITMAGNET_METRICS_ADDR`, the `goose_db_version` boot-assert env, and resource limits so dark comparisons cannot starve the live path (06 R5). Registry-less image pipeline gains the `graphql` image kind (excluded from `IMAGE=all` until the Dockerfile lands, per Phase-1 I2). | ✅ homelab `d5540b2`/`7309f33`/`4dc037f`; exact artifact immutably pinned, dedicated deployment live and Ready, ClusterIP/EndpointSlice/ServiceMonitor/four CNPs healthy, Prometheus `up == 1`; generic inventory enablement remains false |
| I2 | **Dark Service + embedded-shadow plumbing.** Stand up the internal-only dark GraphQL Service, allow Go→Rust egress, and provide default-off endpoint/sample-rate/timeout/concurrency settings with a single-revert kill switch. Do not add a Traefik mirror. The Go runtime hook must already enforce the operation gate before this can be enabled. A future serve-cutover route remains separate and user-gated. | ✅ internal Service and default-off plumbing are complete. Homelab `65d8d7b`, hardened through `694bc07`, adds an offline-validated, uniquely named serve-only canary profile with an exact zero sample ceiling, manifest/config image admission, post-rollout CRI attestation, fixed resources/placement, and namespace-scoped reciprocal TCP 3337 policy; it is not imported or deployed. Live L3 still has no Rust GraphQL shadow env, and Tantivy/routing/cutover remain off. |

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
  `RefineFileBudget=300_000` (count bound per chunk),
  `RetainedFileBudget=1_000_000` (count bound per request), `RouteTimeout=8s`
  (whole route), `ServeTimeout≈800ms` (Tantivy serve RPC, phase6 §6).
  Rust adds a 64 MiB compressed/decompressed per-blob ceiling, 128 MiB
  MessagePack-plus-owned-string allocation budget per chunk, and 64 MiB retained
  owned-string budget per request. **Fail-loud accounting parity; zero unbounded
  count or owned-path refines.** Component RSS evidence passes. The repeated
  four-client GraphQL/sqlx 8 GiB-cgroup gate passed its 6 GiB peak ceiling on
  native amd64 Maple and is accepted at `2b8d2912`; see Risk P2-5.
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
  risk).** The original Go-derived count budgets did not bound Rust-owned path
  bytes: one fixed-1024-byte retained-cap request reached 1,270,160 KiB (~1.24
  GiB). `deb351d7` closes that implementation defect with bounded streaming
  decompression, fallible growth, compressed/decompressed preflight, decoded
  allocation accounting, retained owned-string accounting, and semaphore coverage
  on every refine route. `d173f2e6` records release Linux x86_64 component runs
  showing the adversarial 1024-byte case capped at about 69,656 KiB while an
  accepted near-budget positive control remains functional.

  The P2-5 deployment-admission risk is closed and accepted at `2b8d2912`.
  `3f555ef5` provides the exact-image, real-sqlx, four-client cgroup-v2 harness and
  fails closed on provenance, concurrency, response semantics, OOM/state evidence,
  cleanup, architecture, repeats, output reuse, and evidence transport. On native
  amd64 Maple, the same-tip 4-case smoke and 12-case gate both passed; the maximum
  peak was 755,322,880 bytes in the fixed 8 GiB cgroup, with zero swap/OOM events,
  complete barriers/provenance/cleanup, healthy host headroom, and unchanged Hermes.
  The earlier Coder `/dev/shm` failure remains a pre-workload capacity incident,
  not contradictory workload evidence. Acceptance alone did not authorize an
  image promotion/pin or dark deployment; those two gates were crossed later
  under separate authorization. GraphQL shadow sampling, cutover, and C5 remain
  unauthorized.
- **P2-6 — shadow adds one sampled Rust/PG read (06 R5).** The selected Go-embedded
  hook reuses the already-computed primary Go result and adds only the sampled Rust
  execution; it must never re-issue the Go request. Control the remaining load with
  a low sample rate, non-blocking semaphore admission, a hard timeout, off-peak
  soak, resource limits, and `NodeDiskIOSaturation`/live-p99 abort signals. The
  2026-07-15 production rotation proved those abort signals are load-bearing: all
  correctness rows passed, but rolling Rust p99 exceeded Go after an old reference
  outlier aged out, so the controller suspended traffic. `edb112be` is the
  bounded-query source candidate for that defect; production p99 remains unproven
  until the exact candidate image is admitted, promoted, dark-deployed, and passes
  a fresh cohort rather than reusing the failed T0.

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
