# 06 — Risk Register & Open Questions

**Status:** decision-support. The risks a rewrite carries and the decisions this
document set cannot make for the maintainer. Assumes `00`–`05`.

---

## 1. Risk register

Likelihood and impact are Low/Med/High. "Early-warning signal" is the metric or
observation that tells you the risk is materialising *before* it hits users —
these should be wired into the parity gates (§04 §3.4, §05) so the strangler
surfaces them, not prod.

### R1 — DHT behavioural regression (ban-rate, crawl throughput)

- **Likelihood: High · Impact: High.** The DHT engine is the tentpole (§03 §1,
  §01 hairy-parts #3/#4): hand-rolled anti-forgery (TID+addr matching), BEP-51
  backoff, BEP-9 piece quirks, banning heuristics — all documented only in code.
  A subtle port error doesn't crash; it silently slows the crawl or gets the node
  banned/poisoned. The crawl *is* the product's data supply.
- **Mitigation.** Port the Go wire fixtures as byte-exact parity tests (§05 P4);
  run the Rust crawler as a **distinct DHT node on a distinct queue alongside the
  Go crawler** for a multi-day A-B before any cutover; keep the Go crawler one
  toggle away for instant rollback (§04 §3.1).
- **Early-warning signal.** `dht_crawler_persisted_total`/hr below 0.95× Go;
  `torrents_dropped_total{reason}` distribution shift; ban-rate / metadata-fetch-
  success divergence in the A-B soak.

### R2 — Classifier semantic drift on 48.6M rows

- **Likelihood: Med · Impact: High.** The CEL classifier (§01 hairy-part #1, §03
  §5) drives content-type, tags, and search facets across the whole corpus. A
  `cel-interpreter` conformance gap or a mis-ported release-name regex silently
  mis-classifies at scale; because classification is persisted, drift is *baked
  into 48.6M rows* and only fully visible after a reprocess.
- **Mitigation.** The classification parity corpus is a **hard pre-cutover gate**
  (§05 P3, ≥0.999 match); shadow the Rust processor against the Go writes before
  cutover; keep the CEL rules files byte-compatible so a re-classify with the Go
  engine remains possible as a fallback until confidence is high.
- **Early-warning signal.** Corpus match-rate < 0.999; content-type facet-count
  distribution drift between Go and Rust processors in shadow; a spike in
  `unmatched` classifications.

### R3 — GraphQL schema drift breaks the SPAs

- **Likelihood: Low · Impact: High.** async-graphql is code-first; a derive-macro
  difference (a nullability, an enum value string, a scalar shape) silently
  produces an SDL the Angular/React codegen rejects — breaking both frontends and
  Hermes (§01 §2.2, §03 §2).
- **Mitigation.** The SDL golden-file diff is a **CI merge gate** (§00 §4 surface
  1) — the schema literally cannot drift without a red build; introspection parity
  falls out of SDL parity.
- **Early-warning signal.** Any non-zero `schema.sdl()` diff in CI (fails the
  build); SPA codegen errors in the frontend repos' CI.

### R4 — Queue double-processing during dual-run

- **Likelihood: Med · Impact: Med.** `FOR UPDATE SKIP LOCKED` lets Go and Rust
  co-consume a queue, but if the fingerprint (`sha256(queue+payload)`), the
  partial unique index, or the retry/backoff differ by a byte, jobs get processed
  twice or dropped (§01 §1.2, §04 §3.3).
- **Mitigation.** Operating rule: **one active consumer per queue name** — the Rust
  consumer shadows into a scratch queue until gated, and the Go worker for that
  queue is disabled in the *same* change that enables the Rust one (§04 §3.3).
  Fingerprint parity is a golden-file test.
- **Early-warning signal.** Duplicate `queue_jobs` fingerprints in flight;
  processed-count exceeding enqueued-count in the shadow soak; reprocessing
  side-effects (double TMDB calls) in logs.

### R5 — Single-node resource contention during parallel-run

- **Likelihood: Med · Impact: Med.** The whole strangler runs **both estates on
  the one HEL1 node** (§04 §3.1). Running a Rust crawler *and* the Go crawler, or a
  shadow processor alongside the live one, competes for CPU, RAM, and — acutely —
  disk I/O on the NVMe RAID1, which already flaps `NodeDiskIOSaturation` from ARC
  CI + bitmagnet/DuckDB bursts (homelab memory `hel1-disk-io-saturation-alert`).
- **Mitigation.** Rust's lower RSS/flat-latency profile *helps* steady-state (§03
  §9), but the transient double-run is the risk: shadow with **sampling, not full
  duplication** (mirror `SEARCH_SAMPLE_RATE ≪ 1`); schedule crawler A-B soaks off
  the CI-runner peak; set pod resource limits so a shadow can't starve the live
  path; keep soaks time-boxed.
- **Early-warning signal.** `NodeDiskIOSaturation` firing during a soak; PSI
  pressure > baseline; live-path p99 regressing while a shadow runs.

### R6 — Fork-vs-upstream divergence becomes permanent

- **Likelihood: High (by construction) · Impact: Med.** A rewrite orphans the
  upstream merge channel for every ported subsystem (§04 §1). The fork is already
  +218 commits ahead; today it still merges upstream fixes. Post-port, each
  upstream Go fix is a manual Rust re-implementation — or is silently missed.
- **Mitigation.** Port the **most-diverged, near-net-new subsystems first**
  (`internal/search`, `blobmigration`, `webui-react` — §01 §4), which orphan almost
  nothing; keep the **least-diverged subsystems (DHT, classifier core) on Go
  longest** so they track upstream until the last possible phase (§04 §2). This is
  a design driver of the ordering, not just a mitigation.
- **Early-warning signal.** Upstream security/correctness fixes landing in a
  subsystem you've already ported (watch upstream releases); a growing backlog of
  "upstream fixed X, we must re-port" notes.

### R7 — Maintainer bus-factor / motivation over a long rewrite

- **Likelihood: Med · Impact: High.** ~59 realistic engineer-weeks (§05 §4) for a
  solo maintainer is a multi-quarter-plus commitment competing with everything
  else. Momentum loss mid-rewrite is the classic failure mode — worse if it stalls
  in a *half-ported* state that is harder to operate than either pure estate.
- **Mitigation.** The **NO-GO checkpoint structure** (§05 §5) is the primary
  control: every phase boundary is a stable terminal state, so a stall lands on
  solid ground, never mid-subsystem. Re-decide to proceed at each boundary. Never
  leave two consumers live on one queue or two migrators on one DB (the only
  genuinely unstable intermediate states, and both are explicitly forbidden).
- **Early-warning signal.** A phase's calendar time blowing past its engineer-week
  budget by >2×; the maintainer reaching for the Go path in day-to-day ops on a
  subsystem mid-port.

### R8 — Agent-generated-code review burden

- **Likelihood: High · Impact: Med.** The estimates *depend* on agent teams doing
  the bulk port (§05 §0), but that inverts the workload: the maintainer becomes a
  full-time reviewer of Rust they didn't write, for subsystems whose correctness
  lives in subtle parity properties (DHT edge cases, CEL semantics, the composer's
  gate-7 bounds). Review, not typing, is the schedule bottleneck — and under-review
  is how R1/R2/R4 slip through.
- **Mitigation.** Lean on the **parity corpora as the review backstop** — a green
  differential gate is worth more than line-by-line reading for the mechanical
  ports (§03 §9 culture); reserve deep human review for the two tentpoles and the
  composer, where the corpus can't cover every edge case; route the hard
  adversarial review to the strongest available model per the repo's model-routing
  guidance.
- **Early-warning signal.** Review queue backing up while ports pile ahead of it;
  parity gates passing but production behaviour surprising (a sign the corpus
  under-covers and review didn't catch it).

### Risk heat summary

| Risk | Likelihood | Impact | Primary control |
|---|---|---|---|
| R1 DHT regression | High | High | A-B soak + fixtures + instant toggle-back |
| R2 Classifier drift | Med | High | ≥0.999 parity corpus as hard gate |
| R3 GraphQL SDL drift | Low | High | SDL diff as CI merge gate |
| R4 Queue double-processing | Med | Med | One consumer per queue; fingerprint parity |
| R5 Node contention | Med | Med | Sampled shadow; resource limits; off-peak soaks |
| R6 Upstream divorce | High | Med | Order by divergence; tentpoles last |
| R7 Maintainer stall | Med | High | NO-GO checkpoints; stable phase boundaries |
| R8 Review burden | High | Med | Parity corpora as backstop; review the tentpoles |

The two High×High-ish risks (R1, R7) are controlled structurally, not by effort:
R1 by never cutting over the crawler without a passing A-B and an instant toggle-
back, R7 by making every stop point safe. If either control is compromised, the
rewrite is not de-risked — those controls are load-bearing.

---

## 2. Open questions for the maintainer

Each has a recommendation, but these are genuinely yours to decide.

### Q1 — Do you keep feeding upstream?

The fork still merges upstream fixes; a rewrite ends that permanently for ported
subsystems (R6, §04 §1). **Recommendation:** decide this *before* Phase 3. If
staying mergeable with upstream has ongoing value to you (you pull their crawler/
classifier fixes), that is a direct argument to **keep DHT + classifier on Go
indefinitely** and cap the rewrite at Phase 2 (read/API path only) — a perfectly
good terminal state (§05 §5). If you've effectively already forked away (the +218-
commit reality suggests you have), then upstream tracking is worth little and the
full rewrite is coherent. My read: you are *de facto* a hard fork already, so this
argues *for* eventual full divergence — but consciously, not by accident.

### Q2 — Rewrite the webui serving, or keep Go serving the static SPAs?

The dual-frontend embed (`?frontend` cookie, Angular + React) is small and works
(§01 §1.7). Porting it to rust-embed is Phase 5 tidy-up, not value. **Recommendation:**
keep the Go static-serving shim alive as long as *any* Go remains, and only fold it
into Rust in Phase 5 when the Go binary is decommissioned anyway. It is never worth
prioritising — the SPAs are compiled artefacts served identically by either
language. (If you stop at Phase 2/3, a tiny Go static-file server is a fine thing to
keep forever.)

### Q3 — What actually triggers *starting* (vs this staying a contingency plan)?

This document set is deliberately a **contingency plan**, not a program (§00 §1,
§04 §5). **Recommendation:** commit to **Phase 0 only, now** — it is low-regret,
closes the Rust metrics gap, and hardens the Go app with golden-file CI regardless
of what follows (§05 §5). Treat Phases 1+ as *triggered*, not scheduled: start a
phase only when its subsystem independently earns it — e.g. a concrete pain (a Go
GC/latency problem the crawler hits; a classifier change that's painful in
CEL-on-Go; an upstream you no longer want to track). Absent a trigger, banking
Phase 0 and stopping is the *expected* outcome, and a good one.

### Q4 — Migration ownership: goose-as-tool forever, or hand off to refinery?

goose stays authoritative through Phase 4 (§04 §3.2). **Recommendation:** only hand
off to refinery in Phase 5, and **only if you actually decommission Go** — if you
stop at Phase 2/3, keep goose (a standalone binary the deploy runs), since a
Go-owned migrator against a shared schema is not a problem worth solving. The
refinery handoff is the one irreversible step in the whole plan; don't take it
unless the Go tree is truly gone.

### Q5 — CEL: `cel-interpreter`, re-model the rules, or rhai?

The classifier decision gates Phase 3 (§03 §5, R2). **Recommendation:** build the
parity corpus **first** (it's a Phase-0 deliverable, §05 §1) and run it against
`cel-interpreter` *before* committing a line of classifier port. If it clears the
corpus (custom functions + list ops + string ext + the flags/keywords/extensions
namespace trick), ship it and keep the rules files as a public contract. If it
misses one operator, contribute it upstream or shim it — cheaper than abandoning
CEL. Reserve rhai (which breaks the rules-file contract) only for a proven-
inadequate cel-rust. **Do not decide this abstractly — decide it against the corpus.**

### Q6 — Single binary, or keep the multi-process split?

The Go app is one fx binary; the Rust estate is already 8 bins (§02). **Recommendation:**
keep the **multi-process split** — it is what makes the strangler work (each
subsystem is an independently deployable/rollback-able pod, §04 §3.1) and it matches
where the Rust estate already is. Do *not* collapse to a single binary as an end-
state goal; the operational independence is worth more than one process's
convenience, especially on a single node where you want to scale/restart subsystems
independently. The one exception worth taking: let the Rust read API **embed** the
search crates in-process (§00 §2) rather than gRPC to its own sidecars, once both
are Rust — that removes a network hop without giving up subsystem independence
elsewhere.

### Q7 — What's the smallest experiment that would tell you the most?

Not asked, but the highest-information cheap probe. **Recommendation:** after Phase
0, do **Phase 1 (Torznab) as a deliberate pathfinder** — it is the smallest real
cutover, exercises the full strangler machinery (shadow → gate → canary → toggle),
and tells you empirically how the agent-assisted port + review loop actually
performs on *this* codebase before you commit to the expensive phases. If Torznab
costs 2× its estimate or the review loop is miserable, you've learned that for
~5 engineer-weeks instead of for ~59.
