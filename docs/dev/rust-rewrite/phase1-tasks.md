# Phase 1 — Torznab read adapter: task ledger (started 2026-07-10)

Execution ledger for `05-roadmap-and-gates.md §Phase 1` + `04 §3` (pod layout,
goose rule). Four lanes, disjoint ownership, branched off
`rust-rewrite-phase1-20260710`. **The Rules of engagement in
`phase0-tasks.md` apply verbatim** (WIP-commit+push around every stage,
Coder-workspace builds w/ `CARGO_BUILD_JOBS=4` + $HOME toolchains, stay in
lane, honest reports, team-lead review before merge) — plus one new rule:
**builds take explicit SHAs, never branch names** (worktree-push stale-ref
trap).

Crate skeletons for `bitmagnet-search-query` + `bitmagnet-torznab` and the
workspace member lines are ALREADY on the base branch — no lane touches
`bitmagnet-rs/Cargo.toml`.

**Traffic reality (adapts the roadmap's shadow gate):** this fleet has NO
Torznab client. The shadow gate runs on a curated synthetic query corpus
(Lane G designs it; same evidential caveat as the search soak). Cutover
(Traefik route flip + Go endpoint kept warm) is USER-GATED and outside this
ledger.

## Lane Q — `bitmagnet-search-query` crate (branch `p1q-searchquery`)
Owns: `bitmagnet-rs/crates/bitmagnet-search-query/**`,
`testdata/parity/searchquery/**`, ONE new Go generator test file in
`internal/parity/`. Read-only reference: `internal/database/search/**` (the
Go builder being ported — NO changes there).

| # | Task | Status |
|---|------|--------|
| Q1 | Inventory the exact predicate/order/limit subset the Go Torznab adapter exercises (trace `internal/torznab/adapter*` → search options → `internal/database/search` builder); document it in the crate as the v1 contract | ✅ done (ce1ad303; FIND-2 does NOT apply to Torznab — deviation noted) |
| Q2 | Port that subset to sqlx query construction (hand-written SQL per the house style in bitmagnet-db; no ORM) incl. FIND-2 default ordering semantics where Torznab hits them | ✅ done (merged 2026-07-11) |
| Q3 | Parity: Go generator test emits fixture pairs (search options JSON → result infohash list against the live-PG CI lane's migrated schema + seeded fixture rows); Rust side consumes the same fixtures via the Phase-0 `bitmagnet-diff` harness → 0 diffs | ✅ done (merged 2026-07-11) |

## Lane T — `bitmagnet-torznab` crate + bin (branch `p1t-torznab`)
Owns: `bitmagnet-rs/crates/bitmagnet-torznab/**`. Read-only reference:
`internal/torznab/**` (Go adapter/XML being mirrored — NO changes there).
Consumes Lane Q's live `build_query`/`fetch` crate API through the production
PostgreSQL router seam; fixture-only tests may still inject `SearchClient`.

| # | Task | Status |
|---|------|--------|
| T1 | quick-xml response structs: caps, categories, search/tv/movie/music/book result feeds, `torznab:attr` emission, RSS date format — byte-parity with Go's output as the target (see Lane G goldens) | ✅ done (merged 2026-07-11) |
| T2 | axum handler on the Phase-0 bootstrap (bitmagnet-common serve/metrics/config): param parsing (t=, q=, cat=, imdbid= etc. — mirror Go's accepted params exactly incl. error XML), category mapping | ✅ done (merged 2026-07-11) |
| T3 | Torznab metric series parity (§01 §2.5 names) via bitmagnet-common metrics; goose_db_version boot assert (04 §3.2: Rust processes assert, never migrate) | ✅ done (merged 2026-07-11) |

## Lane G — parity goldens + gates (branch `p1g-goldens`)
Owns: `internal/torznab/*parity_test.go` (new test files only),
`testdata/parity/torznab/**`, the synthetic Torznab query corpus + replay
harness under `internal/parity/` (new files only).

| # | Task | Status |
|---|------|--------|
| G1 | Golden corpus: run the REAL Go Torznab adapter over (a) caps, (b) a fixed ~50-query corpus (all t= modes, category combos, paging, edge params) against deterministic fixture data → `testdata/parity/torznab/*.golden.xml` + regen/assert tests; document the namespace/whitespace normalization | ✅ done (merged 2026-07-11) |
| G2 | Shadow-replay harness: replayer that fires the corpus at BOTH endpoints (Go :3333/torznab and the Rust service) and diffs infohash sets + ordering + counts → the ≥0.99 set-match / ≥0.98 count gates from the roadmap | ✅ done (merged 2026-07-11) |

## Lane I — deploy IaC, ships DARK (branch: homelab master, own files)
Owns (homelab repo): new `ansible/roles/bitmagnet-torznab/**`, a `torznab`
image kind in `playbooks/bitmagnet_image_import.yml` + Makefile, Traefik
route PREPARED behind a default-off flag. NO deploys, NO route flip.

| # | Task | Status |
|---|------|--------|
| I1 | Role per the sidecar pattern (Phase-0 conventions: tag-only pin, IfNotPresent, CNP default-deny + Prometheus allow, ServiceMonitor, BITMAGNET_METRICS_ADDR, goose-assert env) | ✅ done (homelab d0e4df5, dark) |
| I2 | Image-import pipeline gains the torznab image; route flag `bitmagnet_torznab_route_enabled: false` with the Traefik IngressRoute staged | ✅ done (homelab 4fa1439; torznab excluded from IMAGE=all until Dockerfile lands) |

## Cross-lane contracts
- `bitmagnet-search-query` public API: builder functions taking a
  `TorznabSearchParams`-shaped struct (Lane Q defines in Q1, commits EARLY —
  Lane T codes against it).
- Fixture roots: `testdata/parity/searchquery/` (Q) vs
  `testdata/parity/torznab/` (G) — no overlap.
- Gate thresholds are the roadmap's: XML golden 0-diff, set-match ≥0.99,
  count ≥0.98, Rust p99 ≤ Go p99 (measured at gate time, not now).
