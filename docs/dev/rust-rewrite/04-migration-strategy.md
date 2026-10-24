# 04 — Migration Strategy

**Status:** strategy. The core document of the set. Assumes `00`–`03`.

This doc picks *how* a Rust rewrite would proceed if it proceeds at all. It
compares four approaches, recommends one, and then spends most of its length on
the part that actually determines success: **how each subsystem runs in
production alongside the Go monolith during the cutover, and the parity gate that
lets each cutover be trusted.**

---

## 1. The four approaches

### (a) Strangler-by-subsystem behind existing contracts — RECOMMENDED

New Rust processes share the live PostgreSQL and the existing gRPC sidecars. Cut
over one subsystem at a time; each new Rust process runs in shadow, is proven
against a parity gate, then takes the traffic while the corresponding Go worker is
disabled. The Go monolith keeps running everything not yet ported.

This is not a hypothetical — **it is the pattern the fork already executed** on
its highest-value subsystem. The entire search read-path was strangled out of Go
into `bitmagnet-rs/` this way: dual-write indexer → shadow comparator (Jaccard/
RBO/Top1) → canary percent → serve, with a one-env-flip rollback (§02 §3; the
Phase-6 design doc is the current instance of this playbook). The substrate that
makes it possible is that **the durable contract is the schema, not the code**
(§00 §3): Go and Rust processes are just two clients of the same PG.

**Why it wins:** every intermediate state is shippable; rollback per subsystem is
an env flip or a worker toggle; the blast radius is one subsystem; and it reuses a
proven-in-this-repo mechanism instead of inventing one. Its cost is living with a
**bilingual estate for the whole duration** — two toolchains, two images, a
coordination seam per shared table (§3.3).

### (b) Library-first port (Rust crates embedded in the Go process via FFI/cgo) — REJECTED

Port subsystems to Rust crates and call them from the surviving Go binary through
cgo, strangling *within* one process. **Rejected, explicitly.** The bitmagnet
subsystems are not leaf pure-functions — they are stateful, async, and
tokio-driven (the DHT server owns a UDP socket and a routing trie; the classifier
holds a compiled CEL program graph; the sidecar clients hold gRPC channels).
Bridging tokio↔Go-runtime across cgo means marshalling across two async schedulers
and two allocators on the hot path, giving up Go's race detector and Rust's
tooling at the boundary, and — the killer — cgo's per-call overhead lands on the
crawler's highest-frequency paths. There is exactly one narrow place FFI would be
defensible (reusing the classifier's *release-name regex tables* as a shared C
ABI to avoid re-verifying them byte-for-byte, §01 hairy-part #2), and even there
the parity-corpus approach (§3.4) is cleaner. FFI trades a language seam for a
*runtime* seam, which is strictly worse.

### (c) Big-bang parallel build + one cutover — REJECTED

Build the whole Rust app in parallel, cut over once. **Rejected** for the standard
reasons, sharpened by this codebase: (i) the two tentpoles (DHT, classifier) would
have to reach parity *simultaneously* before any value ships, so the first
shippable moment is quarters away and the project carries maximum
work-in-progress risk the whole time; (ii) a single cutover of a 500Gi-DB service
has no incremental rollback — you flip everything or nothing; (iii) it throws away
the fork's proven shadow-gate mechanism, which is *inherently* incremental. Big-
bang only makes sense when the old and new systems can't coexist against shared
state — but here they demonstrably can (the sidecars already do).

### (d) Do nothing / keep the hybrid — the honest baseline

Worth stating plainly because it is the default and it is *fine*. The current
Go-monolith-plus-Rust-sidecars hybrid works, is in prod, and can absorb the
Phase-0 improvements (§05) without any cutover. Every other option must beat
*this*, not beat a strawman. The recommendation (a) is explicitly structured so
that its early phases improve this baseline whether or not the later phases ever
run.

### The upstream-divergence consideration (applies to all of a–c)

This is a fork **+218 commits / +161k lines ahead of upstream** (§01 header). The
fork today still merges upstream fixes (the recent go-resty/TMDB fix, §01 §1.11).
**Any** rewrite — strangler or big-bang — *permanently orphans that channel*: once
a subsystem is Rust, an upstream Go patch to it becomes a manual re-port. Strangler
softens this only in that unported subsystems keep tracking upstream, so the
divorce is gradual rather than total. This is not a tie-breaker between approaches
— it is a cost the *whole idea* carries, and it is the first open question in `06`.
It argues for porting the subsystems that have **diverged most from upstream
already** first (the fork's `internal/search`, `blobmigration`, `webui-react` are
essentially net-new, §01 §4 — porting them orphans almost nothing), and porting
the subsystems that **still track upstream closely** (DHT, classifier core) as late
as possible, keeping the upstream merge channel open on them for as long as
possible.

---

## 2. Recommended decomposition & ordering

Read-path-first, tentpoles-last. The ordering rule is: **maximise value shipped
per unit of parity risk, and keep the upstream merge channel open on the least-
diverged subsystems longest.**

| Phase | Subsystem | Why here | Difficulty (§01 §5) |
|---|---|---|---|
| **0** | Foundations (shared bootstrap/config/metrics/health crates; 5 golden-file harnesses; Go↔Rust differential harness) | No cutover; improves both estates; every later phase depends on it | M (breadth, not depth) |
| **1** | **Torznab read adapter** | Smallest external read contract; facet-free; already the eligible class for Tantivy serving; byte-diffable | M + partial PG-search-builder port |
| **2** | **GraphQL read API** + tier-routing + L1 blob-refine composer + facets | Completes the read path; serves the 2 SPAs + Hermes from Rust; converges with the Rust sidecars | L (schema) + **XL** (composer) |
| **3** | **Ingest enrichment**: PG queue + processor + classifier + release-name parsing | The write side begins; highest *semantic* risk (CEL) | **XL** (classifier) + M/L |
| **4** | **DHT crawler** + BEP-9/10/51 metadata leech | The tentpole; no buy option; highest edge-case risk; kept last so upstream tracks it longest | **XL** |
| **5** | **Cutover completion**: migration ownership handoff, fx/CLI/blobmigration retirement, decommission the Go binary | Only meaningful once everything above has shipped | M |

### Why read-path-first, and why *this* read order

The read path is the natural first bite because **its hardest dependency is
already Rust and already live** (§02 §3). A Rust Torznab or GraphQL resolver talks
to the L2/L3/main-search sidecars *natively* — same language, same process
eventually — rather than the current Go→gRPC→Rust hop. Value ships immediately
(a served query is user-visible), and every read query is trivially shadow-
comparable against the running Go path.

- **Torznab before GraphQL** because Torznab is the smaller, simpler surface: no
  facets, no SPA codegen, a fixed XML byte-format that diffs cleanly (§01 §1.6),
  and it exercises the shared PG-search-builder port that Phase 2 will then reuse.
  It de-risks the search-query port on the low-stakes consumer before betting the
  two SPAs on it.
- **GraphQL is Phase 2, not Phase 1**, because it drags in the genuinely hard
  search piece: the **L1 blob-refine composer** (`pathsearch/composer.go`, ~6k LOC,
  hairy-part #6) and the resolver-embedded tier routing (§01 §1.5, §1.8). That is
  XL work — the chunked exact-refine pipeline with the gate-7 memory/latency
  bounds — and it should not be on the critical path to the *first* shipped Rust
  read process. async-graphql is code-first, so the SDL becomes a golden-file diff
  gate rather than codegen (§03 §2), protecting both SPAs automatically.

### Why the tentpoles are last

The DHT crawler (Phase 4) and classifier (Phase 3) are the two subsystems with
**no head start, no mature Rust dependency, and the highest correctness stakes**
(§03 §1, §5; §01 hairy-parts #1–#4). Putting them last means: (i) the shared
foundations, differential-harness discipline, and sqlx/tonic patterns are all
battle-tested on easier subsystems first; (ii) the upstream merge channel stays
open on them — the least-diverged code — for the longest; (iii) the project can
**stop before them** and still have banked most of the value (§05 NO-GO
checkpoints). The classifier precedes the crawler because the processor pipeline
(which the classifier lives inside) is a cleaner, more testable boundary than the
UDP firehose, and the crawler is the single riskiest thing in the tree — you want
every tool sharpened before you touch it.

---

## 3. How each piece runs in prod during the transition

The single HEL1 node (single-server k3s, SQLite datastore — see homelab memory;
do not add a control plane) hosts both estates simultaneously. This section is the
operational contract of the strangler.

### 3.1 Per-subsystem process/pod layout

The Go app is one binary with a **worker registry** — named workers that
enable/disable/start/stop independently (§01 §1.10, `internal/worker`). This is
the cutover lever: **disabling a Go worker and standing up the equivalent Rust
Deployment is the atomic unit of migration.**

- The Go monolith keeps running, with the not-yet-ported workers enabled and the
  ported ones **disabled** (e.g. `WORKER_*` toggles / the CLI `worker run` set).
- Each ported subsystem is a new Rust Deployment/pod on HEL1, following the
  existing sidecar pattern (§02 §2: tonic UDS-or-TCP, `serve_with_shutdown`,
  SIGINT+SIGTERM, one image / multiple entrypoints).
- All processes — Go monolith, existing sidecars, new Rust services — are clients
  of the **same live PG** and the same gRPC sidecars. Nothing forks the datastore.
- Serving subsystems (Torznab, GraphQL) sit behind the existing Traefik
  externalIPs; cutover is a route/Service change plus a Go-worker disable, with
  the Go endpoint kept warm for instant rollback.

### 3.2 Who runs migrations

**goose stays the sole migration authority for the entire transition (Phases
0–4).** New migrations continue at 00026+. Every process — Go or Rust — treats the
schema as read-of-contract: the Go monolith auto-runs goose on startup as today
(§01 §2.1), and Rust processes **assert the expected `goose_db_version` at boot
and never migrate** (§03 §3 — the "goose-as-tool" option). Ownership handoff to a
Rust-native migrator (refinery, seeding its history table to match goose) is
**Phase 5 only**, after the Go tree is retired. Running two migrators against one
DB during the transition is a split-brain hazard with zero upside; the golden-file
gate (§00 §4, surface 5) is exactly the assertion that neither estate renumbers or
re-runs history.

### 3.3 Queue-sharing semantics (the coordination seam)

The PG queue table (`queue_jobs`, `FOR UPDATE SKIP LOCKED`, fingerprint dedup,
§01 §1.2) is the one place Go and Rust processes actively coordinate rather than
just co-read. Two rules make dual-run safe:

1. **One active consumer per queue name.** `SKIP LOCKED` *permits* Go and Rust to
   co-consume the same queue without lost jobs, but running both invites
   double-processing if the two implementations' dedup/retry semantics differ by
   an inch (fingerprint = `sha256(queue+payload)` must be byte-identical; the
   retry backoff and the `(fingerprint) WHERE status IN (pending,retry)` partial
   index must match exactly). The safe operating rule is therefore: **the Rust
   consumer runs in shadow (claims nothing, or claims into a scratch queue) until
   its gate passes; at cutover the Go worker for that queue is disabled in the
   same change that enables the Rust one.** Never leave both live on the same
   queue name in steady state. (`06` risk #4.)
2. **Producers can overlap safely.** A Go crawler enqueuing while a Rust processor
   consumes (or vice versa) is fine *as long as the fingerprint function matches*,
   because dedup is enforced by the DB index, not by either process.

### 3.4 The parity gate per subsystem

Every cutover is gated by the same three-instrument pattern the fork already uses,
specialised per subsystem. The gate is **default-deny**: the Rust process serves
nothing real until its gate is green over a real-traffic soak.

| Instrument | What it is | Where it already exists |
|---|---|---|
| **Golden-file diff** | Byte/structural diff of a wire contract (SDL, Torznab XML, metric list, config-env resolution) in CI | enum-discriminant, tokenizer, blob-fixture tests (§02 §1) |
| **Shadow compare** | Run Rust and Go on the same live input, diff the outputs, emit metrics; Rust result discarded | `bitmagnet-shadow`, the search Router shadow (§01 §1.8, §02 §1) |
| **Numeric gate** | Thresholds on the shadow metrics over ≥7 days that must clear before canary→serve | Phase-6 design §5 (Jaccard≥0.90, RBO≥0.92, Top1≥0.98, count-match≥0.95) |

Per-subsystem gate specifics are in `05`; the shape is: **golden file proves the
*contract*, shadow proves the *behaviour*, the numeric threshold decides the
*cutover*, and any RPC/error falls through to the Go/PG path — the same
"fail-closed-to-PG" posture the Phase-6 design uses (closed to the new engine,
open to the proven one).**

---

## 4. Sequencing around the two in-flight programs

Two Go-side efforts are live right now and the rewrite must interleave with them,
not fight them.

### 4.1 The D1 `torrent_files` drop

State (§01 §2.1 + homelab memory `bitmagnet-fork-deploy-plan`): dual-write is ON
everywhere (crawler + processor write both `torrent_files` **and**
`files_data`/`torrent_file_summary`); **logical D1 — reads off `torrent_files` —
is complete**; physical `DROP TABLE` is gated behind `DropCompatibleReads` and not
yet done. The Rust read-side already reads the **blob** (`files_data`), never
`torrent_files` (§02 §1 bitmagnet-model, the shadow gate is literally the DROP
gate).

**Sequencing rule:** finish the Go-side D1 story — including the physical drop or
at least a frozen "blob is the only read source" invariant — **before Phase 3**
(the crawler/processor write-side port). That way the **Rust processor and crawler
are born blob-only**: they serialise files to `files_data`/`torrent_file_summary`
via the parity-proven `bitmagnet-model::blob` codec and never carry the legacy
dual-write to `torrent_files`. Porting the write side *before* the drop would force
re-implementing dual-write in Rust — pure throwaway risk. If the physical drop
slips, Phase 3 can still proceed against the invariant "no served path reads
`torrent_files`" (the feature-flag static assertion, §01 §4), with the Rust writer
writing blob only and the legacy table left to the Go writer until it is
decommissioned in Phase 5.

### 4.2 Phase-6 Tantivy-served main search

State (`phase6-tantivy-served-design.md`): a *design-only* Go-side effort to turn
the dormant shadow machinery into a real BM25 serve path, blocked on two
prerequisites (the 00024 follow-contract incremental indexer; the shadow-
goroutine concurrency cap + `SAMPLE_RATE ≪ 1`).

**Sequencing rule — do not build Phase-6 serving twice.** Split it:

- **Its prerequisites are subsystem-agnostic hardening** — the incremental indexer
  (freshness) and the goroutine cap (resource safety) make prod better regardless
  of language. **Keep them on the Go/existing-sidecar track and land them now.**
  They are not rewrite work.
- **The Phase-6 *serving switch itself* (router serving branch, eligibility gate,
  freshness gate, canary wiring) should NOT be built in Go if Phase 2 is on the
  horizon** — it would be re-implemented weeks later in the Rust GraphQL read API.
  Instead, **fold Tantivy-serving into Phase 2**: the Rust read API is being
  written anyway, so it ships the eligibility/freshness gates and the
  hits→hydrate→order-by-InferID serving path (`phase6` §1–§4) natively, reusing
  the exact gate thresholds (`phase6` §5) as its own numeric parity gate. The
  Phase-6 doc's rollout checklist becomes Phase 2's search-serving acceptance
  criteria.

This is the cleanest interaction: the rewrite *inherits* the Phase-6 design as the
spec for how its read API serves search, and avoids paying for the serving switch
in both languages.

---

## 5. What "done" looks like — and why you may never reach it

The end state is a single Rust estate: async-graphql + axum serving the API and
Torznab, a Rust crawler feeding a Rust processor/classifier through the ported PG
queue, all on sqlx over the same PG, with a Rust-native migrator, and the Go
binary decommissioned.

But the strangler is explicitly designed so that **stopping is a valid outcome at
every phase boundary** (§05 NO-GO checkpoints). After Phase 0 the Go app has
golden-file CI and the Rust estate has metrics — strictly better, zero cutover.
After Phase 1 Torznab is Rust and the *arr contract is proven diffable. After each
subsequent phase the estate is a stable, shippable Go/Rust hybrid with one more
subsystem converged. There is no phase whose *only* value is unlocking the next
one. That property — every stop point leaves prod better than it found it — is the
whole reason to prefer the strangler, and it is what makes committing to Phase 0
low-regret even if the tentpoles are never touched.
