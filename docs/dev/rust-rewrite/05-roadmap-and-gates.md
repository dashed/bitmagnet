# 05 — Roadmap, Gates & Estimates

**Status:** planning. Turns `04`'s decomposition into a phased roadmap with
measurable gates, effort estimates, rollback stories, and explicit stop points.
Assumes `00`–`04`.

---

## 0. How to read the estimates

Estimates are in **engineer-weeks of focused maintainer effort**, calibrated to
the real operating model of this repo: **one solo maintainer, assisted by
codex/claude agent teams.** That reality changes the shape of the numbers, and it
is worth being explicit about how:

- **Bulk/mechanical port work is cheap and fast.** Translating a well-specified Go
  subsystem (the DAO surface, the Torznab XML structs, the release-name regex
  tables, the queue SQL) to Rust is exactly the "clear-spec implementation /
  migration" work that agent teams do well and cheaply (per this repo's model-
  routing guidance). The Go tree is a *spec, not a black box* (§03 §9) — agents
  port against it.
- **The bottleneck is human review + semantic parity, not typing.** The cost
  concentrates in (a) reviewing agent-generated Rust for the subtle correctness
  properties the parity corpora encode, and (b) the two subsystems where no spec-
  port suffices because the *semantics* must be re-derived and re-verified
  torrent-by-torrent: the CEL classifier and the DHT edge cases. `06` risk #8
  (review burden) is the real schedule risk, not LOC.
- **Therefore estimates are review-bound.** A phase's engineer-weeks is dominated
  by how much of it a human must personally verify, not how many lines it is. This
  is why the 30k-LOC GraphQL layer (mostly generated, mechanical) estimates *lower
  per LOC* than the 4k-LOC classifier (irreducibly semantic).

Engineer-weeks are **focused effort**, not calendar time. On a homelab run around
a day job, calendar time stretches by whatever factor your attention allows;
Section 4's scenarios are in engineer-weeks for that reason.

---

## 1. Phase 0 — Foundations (no cutover)

**Scope.** Close the `02 §4` gaps and build the parity infrastructure every later
phase depends on. This phase touches the **existing Rust sidecars** and the **Go
CI**, not a new port.

**Deliverables.**
- A shared bootstrap crate (`serve(service, opts)`): listener parsing, tracing
  init, SIGINT+SIGTERM shutdown, `grpc.health.v1` registration — extracted from
  the 8 bins' copy-pasted wiring (§02 §4.2, §4.3, §4.6).
- A metrics layer (the `prometheus` crate, §03 §7) wired into the shared bootstrap
  — closing the **zero-metrics gap** (§02 §4.1), the single biggest liability, and
  a win for the *current* sidecars immediately.
- A layered config crate (figment + an explicit strcase port, §03 §7) with the
  env-var parity harness.
- The **five golden-file CI harnesses** (§00 §4): GraphQL SDL diff, classifier
  parity corpus runner, config-env parity, metric-name list, goose-history
  assertion.
- A **Go↔Rust differential-test harness**: a reusable driver that feeds identical
  inputs to a Go subsystem and its Rust port and diffs outputs, generalising
  `bitmagnet-shadow` (§02 §1).
- CI: a live-PG integration lane, filesearch-image build, `cargo deny`/audit
  (§02 §5).

**Parity gate.** N/A (no cutover). Done-definition: all five golden-file harnesses
run green in CI against the *current* Go app and Rust sidecars; the metrics layer
exports from at least one live sidecar; `cargo test --workspace` + the new lanes
green.

**Rollback.** N/A — nothing is cut over. The shared crates are additive to the
sidecars; the harnesses are CI-only.

**Estimate:** **3–5 ew.** Breadth work, low depth. The metrics + bootstrap
extraction is mechanical; the strcase parity port and the classifier-corpus runner
are the fiddly bits.

---

## 2. Per-subsystem phases

Each phase below is a strangler cutover: build → shadow → gate → canary → serve →
disable the Go worker. All share the §04 §3.4 gate pattern; only the specifics
differ.

### Phase 1 — Torznab read adapter

- **Scope.** Rust Torznab/Newznab XML endpoint (`GET /torznab/*`) on axum +
  quick-xml (§03 §6), translating params → search options → results, plus the
  **subset of the PG search query builder Torznab needs** ported to a Rust crate
  (reused by Phase 2). Reads blob/sidecars only, never `torrent_files`.
- **Deliverables.** axum Torznab handler; quick-xml response structs (caps/
  categories/search/tv/movie/music/book); the shared `search-query` Rust crate
  (v1: the predicates Torznab exercises); metric parity for the Torznab series.
- **Parity gate.**
  - *Golden file:* byte-diff Rust vs Go XML for the caps document + a fixed query
    corpus (categories, `torznab:attr` names, RSS date format — §01 §2.3) → **0
    diffs** after namespace/whitespace normalisation.
  - *Shadow:* replay a week of real Prowlarr/*arr queries against both; diff result
    sets (infohash sets + ordering) → **≥0.99 set-match**, count parity ≥0.98.
  - *Numeric:* Rust p99 ≤ Go p99 on the served path.
- **Rollback.** Traefik route flip back to the Go Torznab endpoint (kept warm);
  instant, no data change.
- **Estimate:** **4–6 ew.** Torznab itself is M; the reusable PG-search-builder
  subset is the real cost (L, but mechanical against the Go builder).

### Phase 2 — GraphQL read API + tier routing + L1 composer + facets

- **Scope.** async-graphql code-first API (§03 §2) reproducing the SDL; the full
  PG search query builder (9-facet aggregation, cursor pagination, criteria
  builders — §01 §1.8); the **L1 blob-refine composer** (hairy-part #6) and the
  resolver-embedded L3→Tantivy→PG tier routing; **Tantivy-serving folded in from
  the Phase-6 design** (§04 §4.2). Served to Angular, React, and Hermes.
- **Deliverables.** async-graphql schema + resolvers; the complete `search-query`
  crate incl. composer + tier router + facets; the Phase-6 eligibility/freshness
  gates + hits→hydrate→`order-by-InferID` serving path; SDL golden-file gate wired
  in CI.
- **Parity gate.**
  - *Golden file:* `schema.sdl()` diff vs committed `schema.graphql` → **0 diffs**
    (custom scalars, enum value strings, nullability — §00 §4 surface 1).
  - *Shadow:* per-query result-set + facet-count + total-count diff Rust vs Go over
    ≥7 days on real traffic.
  - *Numeric (search-serving, from `phase6` §5):* Top1≥0.98, JaccardAt20≥0.90,
    RBO≥0.92, count-match≥0.95, Rust p99 ≤ Go p99. **The composer's gate-7 bounds**
    (max-refine-files, retained-file-budget, route deadline — §01 hairy-part #6)
    must hold under load: fail-loud accounting parity, zero unbounded refines.
- **Rollback.** Env/route flip to the Go GraphQL endpoint; the composer is a
  decorator, so PG-only fallback is intrinsic (fail-safe to PG, `phase6` §6).
- **Estimate:** **10–16 ew.** async-graphql resolver volume is mechanical (L); the
  **L1 composer is the XL cost** — the chunked exact-refine pipeline with intricate
  memory/latency bounds is the hardest non-tentpole piece and is review-heavy.

### Phase 3 — Ingest enrichment: queue + processor + classifier + release parsing

- **Scope.** The PG queue ported onto sqlx `FOR UPDATE SKIP LOCKED` with exact
  fingerprint/retry/dedup semantics (§03 §4); the processor pipeline (§01 §1.2);
  the **CEL classifier** on `cel-interpreter` (§03 §5) with `classifier.core.yml`
  kept byte-compatible; the **release-name parsing tables** (keywords DSL, regex,
  video/episode/language parsers — §01 hairy-part #2). **Born blob-only** (D1 drop
  sequenced first, §04 §4.1).
- **Deliverables.** Rust queue crate; processor consumer; cel-interpreter env
  binding prost `Torrent`/`Classification` types; ported rules loader (core embed +
  XDG + CWD + config injection); release-parsing crate with the `languages.csv` +
  enum tables.
- **Parity gate.**
  - *Golden file (surface 2):* classification parity corpus — the Go classifier's
    fixtures + `classifier.core.yml` → **identical `Classification` output** on a
    large real-torrent sample. This gate **must be green before the classifier is
    even a candidate to cut over** (§03 §5).
  - *Shadow:* run the Rust processor in shadow off the live queue (claims into a
    scratch queue, §04 §3.3), diff `TorrentContent`/`Content`/`TorrentTag` writes
    vs the Go processor.
  - *Numeric:* classification-match rate ≥ **0.999** on the corpus (drift on 48.6M
    rows is `06` risk #2 — set the bar high); queue throughput ≥ Go baseline; zero
    double-processed jobs during the shadow soak.
- **Rollback.** Re-enable the Go processor worker, disable the Rust one (single
  toggle, §04 §3.1); the queue table is shared so no in-flight jobs are lost.
- **Estimate:** **10–16 ew.** Queue (M) + processor (M) are quick; the **classifier
  is XL and gated on CEL conformance** — if `cel-interpreter` misses an operator,
  add upstream-contribution/shim time (`06` risk, §03 §5). Release-parsing tables
  are L and pure review-against-corpus.

### Phase 4 — DHT crawler + BEP-9/10/51 metadata (the tentpole)

- **Scope.** Build the DHT engine in-tree on `bendy` + tokio UDP/TCP (§03 §1): KRPC
  codec, k-bucket routing trie, BEP-5 responder, BEP-51 sampler, BEP-9/10 metadata
  leech with infohash verification (v1 SHA-1 + v2 truncated-SHA-256, §01 §1.1,
  hairy-part #4), the crawler pipeline (discovery→triage→get_peers→leech→persist),
  and blob-only persist.
- **Deliverables.** `bitmagnet-dht` crate (codec + routing + responder + sampler);
  metadata-requester crate; crawler pipeline; the concurrency primitives
  (per-IP rate limiting, buffered/batching channels — §01 §1.1); v1/v2 hybrid-
  collapse dedup.
- **Parity gate.**
  - *Golden file:* port the Go DHT wire fixtures (`msg_test.go`, `nodeaddr_test.go`)
    as Rust parity tests → byte-exact codec (§03 §1).
  - *Shadow / A-B:* run the Rust crawler alongside the Go crawler against the live
    DHT (distinct node, distinct queue); compare **crawl throughput**
    (`persisted_total`/hr), **drop reasons** (`torrents_dropped_total{reason}`),
    ban-rate, and metadata-fetch success rate over a multi-day soak.
  - *Numeric (`06` risk #1):* Rust throughput ≥ **0.95×** Go; ban-rate ≤ Go;
    v2-dedup and info-dict-verify reject rates match within 1%.
- **Rollback.** Disable the Rust crawler worker, re-enable the Go crawler; ingest
  continues uninterrupted (the queue and PG are shared).
- **Estimate:** **12–20 ew.** The tentpole. Mechanical *against the Go spec* but
  the edge cases (anti-forgery TID+addr matching, BEP-51 backoff, BEP-9 piece
  quirks, banning) are only documented in code and surface as slow-crawl/poisoning
  regressions — this is where the 3–6× multiplier and the review burden peak.

### Phase 5 — Cutover completion

- **Scope.** Migration ownership handoff (goose → refinery, seeding its history to
  match `goose_db_version`, §03 §3); retire the fx graph, urfave CLI,
  `blobmigration` (once physical drop done), and the dual-frontend Go embed
  (rust-embed, §03 §6); decommission the Go binary and image.
- **Deliverables.** refinery migrator + reconciled history; Rust CLI surface
  (`worker`, `config`, `reprocess`, etc.); rust-embed webui serving; Go image
  removed from the deploy.
- **Parity gate.** goose-history golden file still asserts no renumber; a boot-time
  check that refinery's reconciled history == the 25+ applied goose versions;
  smoke-test every retired CLI subcommand.
- **Rollback.** Keep the Go binary buildable for one release cycle post-cutover;
  refinery handoff is the only irreversible step and is gated on the reconciliation
  assertion.
- **Estimate:** **3–5 ew.** Mostly retirement + the one careful migrator handoff.

---

## 3. Estimate summary

| Phase | Subsystem | Difficulty | Engineer-weeks |
|---|---|---|---|
| 0 | Foundations + harnesses | M (breadth) | 3–5 |
| 1 | Torznab read adapter | M + L | 4–6 |
| 2 | GraphQL + composer + facets + Tantivy-serve | L + **XL** | 10–16 |
| 3 | Queue + processor + classifier + release-parse | **XL** semantic | 10–16 |
| 4 | DHT crawler (tentpole) | **XL** | 12–20 |
| 5 | Cutover completion | M | 3–5 |
| | **Total** | | **42–68 ew** |

---

## 4. Cumulative timeline scenarios

Engineer-weeks of focused maintainer effort. "Aggressive" assumes agent teams port
cleanly, CEL/DHT parity lands first-pass, and review keeps up. "Conservative"
assumes CEL needs upstream contributions, DHT throws throughput regressions
requiring iteration, and review is the bottleneck (`06` risks #2/#1/#8 all bite).

| Cumulative through… | Aggressive | Realistic | Conservative |
|---|---|---|---|
| Phase 0 | 3 | 4 | 5 |
| + Phase 1 (Torznab) | 7 | 10 | 12 |
| + Phase 2 (read API) | 17 | 24 | 30 |
| + Phase 3 (enrichment) | 27 | 37 | 48 |
| + Phase 4 (crawler) | 39 | 54 | 70 |
| + Phase 5 (decommission) | **42** | **59** | **~80** |

**Realistic all-in ≈ 59 engineer-weeks.** For a solo maintainer this is a
multi-quarter-to-multi-year *calendar* commitment depending on attention share —
which is the strongest argument for the NO-GO structure below: you bank value at
each boundary rather than betting the whole ~59 weeks up front.

---

## 5. NO-GO checkpoints (where you can stop, prod strictly better)

The roadmap is built so that **every phase boundary is a legitimate terminal
state.** At each, the estate is stable and better than before; nothing downstream
is required to realise the value already banked.

| After… | Prod is strictly better because… | If you stop here you keep… |
|---|---|---|
| **Phase 0** | Rust sidecars gain **metrics + shared bootstrap + health**; the Go app gains **five golden-file CI gates** + a differential harness | A hardened, observable hybrid — the highest-ROI phase, and it required **zero cutover** |
| **Phase 1** | The *arr wire contract is now golden-file-proven; one external surface is Rust and diffable | A Rust Torznab; the reusable search-query crate for later |
| **Phase 2** | The entire **read/API path is Rust**, SDL-gated, converged with the live sidecars; Tantivy-serving shipped | A Go *ingest* half + a Rust *serve* half — a clean, stable split |
| **Phase 3** | Ingest enrichment is Rust; only the DHT firehose remains Go | Everything but the crawler in Rust |
| **Phase 4** | The whole app logic is Rust; Go remains only as migrator + shell | A functionally complete Rust estate |
| **Phase 5** | Single-language estate; Go decommissioned | The end state |

The **decision to proceed is re-taken at each boundary**, against fresh evidence
(did the last phase's gate pass cleanly? did the estimate hold? is the maintainer
still motivated?). This is a contingency-gated program, not a runway commitment.
The recommended *minimum* commitment is **Phase 0 alone** — it is low-regret,
improves prod immediately, and buys the option (not the obligation) on everything
after it.
