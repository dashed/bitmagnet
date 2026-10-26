# 00 — Rust Rewrite: Overview & Recommendation

> ⚠️ **Stale in places.** See
> [`08-current-state-2026-08-09.md`](08-current-state-2026-08-09.md) for the
> authoritative current state — Phases 0–3 are landed and deployed, the live
> ingest-shadow pilot is dead and replaced by offline write-set replay, and B′
> flags-ON classifier parity is now functionally complete and measured (what
> remains is evidence at scale, one structural limit in the write-set oracle, and
> production wiring). Where this document disagrees with that one, that one is
> correct.

**Status:** strategy / decision-support only. No code, no commitment. This is the
top of a five-document set that investigates what a full Rust rewrite of
bitmagnet would take, whether it is worth it, and how it would be sequenced if
ever started.

**Companion docs:**

| Doc | Contents |
|---|---|
| `01-go-inventory.md` | Full Go subsystem map, external contracts, hairy-parts ranking, fork deltas, quant table. |
| `02-rust-assets.md` | The existing `bitmagnet-rs/` estate (8 crates / 31.6k LOC, live prod sidecars), and its gaps. |
| `03-ecosystem-mapping.md` | Per-subsystem Rust crate verdicts (DHT, GraphQL, sqlx, CEL, axum, metrics, …) with maturity grounding. |
| **`04-migration-strategy.md`** | The core doc: strangler vs big-bang vs FFI-hybrid, the recommended decomposition, and how each piece runs in prod during transition. |
| `05-roadmap-and-gates.md` | Phased roadmap, per-phase parity gates, engineer-week estimates, NO-GO checkpoints, timeline scenarios. |
| `06-risks-open-questions.md` | Risk register + open questions for the maintainer, each with a recommendation. |

---

## 1. The recommendation in three sentences

Do **not** rewrite bitmagnet as a project; adopt a **strangler-fig migration**
that only ever runs when a specific subsystem independently earns a rewrite,
cutting it over one process at a time behind the wire contracts that already
exist, against the live PostgreSQL and the already-Rust search sidecars. Sequence
it read-path-first (Torznab, then the GraphQL API) because those are pure
consumers of the search estate that is *already Rust and live in prod*, and put
the DHT crawler and the CEL classifier — the two irreplaceable, from-scratch
subsystems — **last**, gated behind Go↔Rust differential-test corpora. Treat
everything before the first cutover (Phase 0: shared bootstrap/config/metrics
crates plus five golden-file CI harnesses) as work that **pays for itself even if
the rewrite never proceeds**, because it hardens the Go app and closes the
observability gap in the Rust estate today.

---

## 2. Why this is even a question

The honest case *for* a rewrite, and the honest case against, both start from one
fact: **the read/search path is already Rust and already in production.** The
`bitmagnet-rs/` workspace (§02) is not a prototype — it is 8 crates, ~31.6k LOC,
~290 tests, and five live services on HEL1 (main Tantivy search + follow loop, L2
DuckDB file-search, L3 pathsearch, the Parquet export pipeline, and the dual-read
shadow gate). The Go application has *already been strangled once*, on its highest-
value subsystem, using exactly the pattern this document recommends.

### What a rewrite buys

- **A single-language estate that converges on where the code already went.**
  Every new search capability of the last year landed in Rust (§02 §3). The Go
  monolith and the Rust sidecars talk over gRPC and a shared blob format; a
  rewrite collapses that seam — the Rust read API could eventually *embed* the
  search crates instead of gRPC-ing to its own sidecars.
- **Memory and tail-latency headroom.** The Go binary is ~64 MB with a GC; the
  Rust services run 50–80 MB RSS with flat tail latency under load and no GC
  pauses (§03 §9). The crawler's allocation-heavy hot path is exactly where this
  helps most. This is a *nice-to-have* on a box that is usually far under its 32Gi
  pod limit — not a driver on its own.
- **A metrics and bootstrap reset.** The Rust estate has **zero metrics** today
  (§02 §4.1) and no shared server-bootstrap (§02 §4.2). A rewrite forces both to
  be built as foundations rather than bolted on — and that foundation work is
  useful to the *current* sidecars immediately.
- **Retiring the fx dependency graph.** The whole Go app is one implicit
  uber-go/fx DI graph (§01 §1.10, hairy-part #8); explicit Rust composition-root
  wiring is more readable and removes a load-bearing piece of magic.

### What it costs / risks

- **Two subsystems have no buy option and no head start.** The hand-rolled DHT
  protocol engine (§01 §1.1, hairy-part #3) and the CEL-based classifier
  (§01 §1.3, hairy-part #1) are ~10k LOC of the hardest, most edge-case-laden,
  most correctness-critical code in the tree, and `03` finds no mature Rust crate
  that covers BEP-51 sampling + BEP-9 fetch, and only a *conformance-uncertain*
  CEL interpreter. These are the tentpoles, and they carry the 3–6× first-service
  rewrite multiplier (§03 §1, §9).
- **It orphans the upstream relationship.** This is a fork **+218 commits / +161k
  lines ahead of `bitmagnet-io/bitmagnet`** (§01, header). Today the fork can
  still cherry-pick upstream fixes (e.g. the recent go-resty/TMDB fix). A Rust
  rewrite makes that permanently impossible — every upstream fix becomes a manual
  Rust re-implementation. See `06` open question #1.
- **The bulk of the app is untouched.** Despite the Rust head start, ~75%+ of the
  runtime code — ingest, enrichment, the entire API/UI surface, the write side and
  schema — is still Go (§02 §3). A rewrite is *far* from half-done.
- **Solo maintainer, long horizon.** This is a homelab run by one person with
  agent-team assistance. A multi-quarter rewrite competes with everything else and
  is exposed to motivation/bus-factor risk (`06` risk #7).

**Net:** the value is real but incremental (convergence + a metrics reset), and
the cost is concentrated in two irreplaceable subsystems and a permanent upstream
divorce. That asymmetry is *precisely* why the recommendation is a contingency-
gated strangler, not a program — see `04`.

---

## 3. Current-state one-pager

| | Go (`internal/`) | Rust (`bitmagnet-rs/`) |
|---|---|---|
| **Hand-written LOC** | **~50k** (88.4k total − 26.5k `gql.gen.go` − 7.6k generated DAO) | **~31.6k** across 8 crates |
| **Tests** | Go test suite | ~290 (`cargo test --workspace` green offline) |
| **Role** | Ingest, enrichment, API/UI, write-side, **schema owner** | Read/search satellites hanging off Go-owned PG |
| **Prod status** | The whole app | 5 live services on HEL1 (search, filesearch, pathsearch, parquet, shadow) |
| **Biggest strength** | Battle-tested classifier + crawler; the durable schema | tonic/sqlx/tantivy/duckdb stack proven; parity-gate culture |
| **Biggest gap** | fx-implicit wiring; the whole thing is one binary | **zero metrics**; no shared bootstrap; migrations are Go-owned |

The durable contract is **not the Go code** — it is the **PostgreSQL schema +
goose migration history + the three gRPC sidecar protos + the GraphQL/Torznab
wire formats + the Prometheus metric names**. A rewrite shares the live 500Gi DB
and the existing sidecars; it re-expresses the code that sits on top of that
contract, one process at a time.

---

## 4. The five golden-file contract surfaces

These are the surfaces that **silently break a deployed consumer** if the rewrite
drifts. Each becomes an automated golden-file diff in CI *before* the subsystem
that owns it is touched — this is the single biggest de-risker and it is already
this repo's established practice (blob-fixture, tokenizer-fixture, enum-
discriminant parity tests; the live shadow gate). From `03` §9:

| # | Surface | What breaks | Gate mechanism |
|---|---|---|---|
| 1 | **GraphQL SDL** (§01 §2.2) | Angular + React SPAs, Hermes | Export `async-graphql schema.sdl()`, normalise, diff vs committed `schema.graphql`. |
| 2 | **Classifier rules-file syntax** (§01 §1.3) | User-authored `classifier.yml` overrides | Go-vs-Rust classification parity corpus (same torrent in → same `Classification` out). |
| 3 | **Config env-var strcase mapping** (§01 §1.9) | ~25 deployment env vars | Assert every documented env var resolves to the same node as the Go binary. |
| 4 | **Prometheus metric names** (§01 §2.5) | ~40 series behind Grafana dashboards + Loki alert rules | Golden list of `name{labels}`; diff exposition. |
| 5 | **goose migration history** (§01 §2.1) | On-disk 500Gi PG (`goose_db_version`) | Never renumber/re-run; assert version at boot; keep goose as the sole migrator until Phase 5. |

Two more wire contracts are byte-format-critical but already have Rust parity
harnesses: the **Torznab XML** (Prowlarr/*arr) and the **three gRPC sidecar
protos** (`bitmagnet.v1`, discriminants already locked to Go by unit test,
§02 §1 bitmagnet-proto).

---

## 5. Reading order

If you read nothing else, read **`04-migration-strategy.md`** — it holds the
decomposition and the prod-transition mechanics. `05` turns that into a dated,
gated, estimated roadmap with explicit stop points. `06` is the risk register and
the decisions this document cannot make for you. `01`/`02`/`03` are the evidence
base the recommendations are cited back to.
