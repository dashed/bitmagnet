# PS-T5 — Per-keystroke <50 ms free-text PATH search: critical cost/benefit & GATED go/no-go

**Date:** 2026-06-09
**Thread:** PS-T5 (adversarial / decision) — team `bitmagnet-bench`. Task #76. Synthesizes PS-T1/T2/T3/T4.
**Mandate:** make the honest case AGAINST the L3 path-FTS index, weigh it against alternatives, and deliver a hard, falsifiable gate.
**Status:** Synthesis of measured numbers from prior threads (EXP-D/D2/E, RUN-2/3/4, ARCH-C, EXP-A/B) + the four PS-T docs. Every quantity below is **measured/verified elsewhere, not new** — see Sources.

---

## TL;DR — recommendation

> **NO-GO by default. DEFER the L3 path-FTS index indefinitely.** It is **purely additive** and **NEVER gates the `torrent_files` DROP**. Build it only if the gate below fires — and the **first** spend even then is a **cheap, read-only micro-bench (PS-T3)**, not the index.

> 🚦 **SUPERSEDED BY USER DECISION + PSX (2026-06-09, later same day):** the user **committed to building L3** (overriding the NO-GO-by-default below), and the [PSX campaign](./psx-campaign-RESULTS.md) then **BUILT** the production index: **13.32 GiB**, recall 1.0000, latency-neutral `WithFreqs`. Production-shape correction: broad-single-gram **p95 ≈ 77–94 ms** via the real `TopDocs` page collector (the 55–65 ms below is a `Count`-collector lower bound); realistic multi-word queries < 50 ms; the broad tail is engine-irreducible → UX. This doc's _analysis_ stands; its _recommendation_ is superseded.
>
> 🟢 **MEASURED UPDATE (PS-MB1 ran 2026-06-09 — see [RESULTS](./pathsearch-microbench-RESULTS.md)):** the micro-bench this decision gated on has now been **executed on the full 879.5 M-row restore**. It does **not** change the recommendation (still NO-GO-by-default / deferred / never gates the DROP — G1/G2/G4 unchanged) but it **settles G3 and G5 with measured numbers**, retiring the "UNMEASURED" / "+90 GB" caveats below:
>
> - **G5 size = MEASURED PASS:** full-corpus per-torrent `WithFreqs` = **13.54 GiB** (not the +90 GB per-file figure). Savings impact is **87% → ~84%**, not 87%→55%. The +90 GB / 300 Gi / −55% numbers throughout this doc are the **per-FILE upper bound — superseded** for the recommended per-torrent design.
> - **G3 latency = MEASURED PASS on p50, with a tail caveat:** `ascii3` warm **p50 24.71 ms**, `cjk3` 0.21 ms (both < 50 ms; docs cap at ~17 M so latency holds where per-file breaks). **But** the broadest worst-case substrings breach 50 ms at **p95/p99 (~55–65 ms)** — so it is "median-interactive," not "uniformly < 50 ms." min-chars≥3 does _not_ fix the tail (the 3-char broad case _is_ the floor); real-query selectivity + debounce + top-k do.
> - **Cheaper variant (b) edge-ngram = REJECTED by measurement:** in production (`WithFreqs`) it is _bigger_ than the char-ngram (21.3 vs 13.5 GiB, term-dict 8× inflation) **and** misses common substrings (`264`→0.19 recall). So if L3 is ever built, it is the **per-torrent char-ngram(2,3) `WithFreqs`** — the (b) ASCII-edge-ngram default below is superseded.
> - **Net:** L3 is **not a footprint-tripler** — it's a ~13.5 GiB, median-interactive add-on **if** demand (G1) + an in-prod ILIKE wall (G2) ever materialize. The build-gate is **unchanged**; the _cost-against_ (§1, §3) is softened by the real numbers.

The cheap tiers — L1 blob (✅ deployed) + L2 DuckDB-on-Parquet + PG `agg_torrent_ext` (planned) — already give **near-complete per-file search parity** at **~87–90% space savings**, covering every _demonstrated_ "find my file" need at <250 ms. The L3 index buys exactly **one** capability nothing else makes interactive — **per-keystroke broad/CJK free-text _substring_ path search** — and:

- **No one has demonstrated needing it.** PS-T1: zero demand signal in the product; the feature is greenfield on _both_ the UI and backend axes.
- **Its headline `<50 ms` premise is partly false by construction.** PS-T1/T2/T3 all confirm: per-keystroke generates the _broadest_ match-sets first, measured at **100–320 ms** — over budget. PS-T3 _verified in the tantivy 0.26.1 source_ that the obvious escape hatch (top-k / block-max WAND early termination) **does not work for our query shape**. The only lever is shrinking the match-set.
- **Its cost is a wide, partly-unmeasured range.** The headline **+~90 GB (→ savings 87%→55%)** is the **per-FILE worst case** (873 M docs). PS-T3's **per-torrent path-bag** design (~17 M docs) could shrink it _materially_ — possibly to ≤~20 GB — but that number is **UNMEASURED**. PS-T2 confirms the postings cost for CJK-correct substring is **intrinsic and engine-independent** (no external engine undercuts it; they only relocate it + add a service).
- **It is the _only_ tier that needs an always-on stateful single-writer** (PS-T4) — a new failure-mode class (the multi-thread-writer crash footgun; 94 GB; ~97 min rebuild) versus L1/L2's stateless batch. And its one unique upside — **~2 ms freshness** — is **not needed for typeahead** (PS-T4) and would require _breaking_ keep-everything to get.

⟹ Disproportionate cost + a partly-unmet core premise + zero demonstrated demand ⇒ **NO-GO by default, gated.**

---

## What is being proposed

An L3 **Tantivy char-ngram path-only inverted index**, CJK-correct, with an **always-on single-writer** maintenance process, delivering interactive (per-keystroke, target `<50 ms`) free-text **substring** search over file paths. It is a **third, narrow engine** (PS-T4): PG-FTS keeps main torrent search; DuckDB-on-Parquet owns structured per-file search; this owns _only_ broad free-text path typeahead — the one thing the cheap tiers can't make interactive (DuckDB path-ILIKE substring is a ~23 s full scan worst case).

---

## The honest case AGAINST

### 1. Cost disproportion — but it is a RANGE, and the headline figure is the worst case

The migration's thesis is a **~93% space win** (torrent*files **276 GB → ~19 GB blob**). Keeping \_complete cheap* search parity barely dents it (**~87%**, ≈35 GB). The L3 index is the swing factor — **but how big it actually is is not settled:**

| L3 granularity                            | docs   | index size                                                                                                   | savings vs 276 GB             | status            |
| ----------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------ | ----------------------------- | ----------------- |
| **per-file** (the EXP-D2 build)           | ~873 M | **+~90 GB** (94 GB measured)                                                                                 | **−55%** (~125 GB total)      | ✅ measured       |
| **per-torrent path-bag** (PS-T3's design) | ~17 M  | **materially less — possibly ≤~20 GB** (postings shrink with torrent-doc-frequency; avg 51.79 files/torrent) | better, between −87% and −55% | ⚠️ **UNMEASURED** |

So the "+90 GB → halves the savings" line is the **per-file ceiling**, not a settled cost. PS-T3's per-torrent path-bag is the right granularity for the typeahead use-case ("find torrents whose files match this text" — PS-T1) and plausibly shrinks the index up to ~50×, _and_ makes supersession cheaper (one `delete_term` + one re-add per torrent). **This directly weakens the cost-against argument** — which is exactly why the gate (below) is conditioned on **measuring the real per-torrent size**, not on the 94 GB number. Even so, the cheap tiers already deliver parity at +4–16 GB, so L3 at _any_ size is incremental spend on a niche.

### 2. The `<50 ms` per-keystroke premise is partly false — and the escape hatch is a VERIFIED dead end

- RUN-4 already **rejected** the per-file _structured_ Tantivy index (scan-bound ~1.3 s) — the cautionary precedent.
- EXP-D2: free-text ngram is interactive **only for selective queries** (CJK p50 **0.07–0.86 ms**). The **broadest ASCII grams (5.6 M hits) measured p50 100–145 ms / p95 244–320 ms** — and **per-keystroke typing starts with short, common prefixes** = exactly the broad, large-match-set case. PS-T1: a 1–2-char query is broader still, so 5.6 M is an **optimistic floor**.
- **PS-T3 closed my earlier hedge:** I had said "whether a top-k early-terminating collector closes that gap is unverified." PS-T3 _traced the tantivy 0.26.1 source_ and **verified it does NOT**: block-max WAND fires only for _score-sorted disjunctions_; our ngram path query is a _fast-field-sorted conjunction_ (`SpecializedScorer::Other`); and `SegmentCollector::collect` has **no abort signal** — a cap saves heap, not scan. **The only lever that lowers broad-prefix latency is shrinking the match-set** (min-chars≥3 + debounce + server gram-guard removes the 2-char worst case; per-torrent path-bag shrinks postings). Whether those mitigations bring the **broadest 3-char prefix** under 50 ms is **UNMEASURED** — it is the crux question of PS-T3's micro-bench. As it stands, the named target `<50 ms per-keystroke` is **not demonstrated** for the head of every search.

### 3. The always-on single-writer — a new failure-mode class unique to L3 (PS-T4)

L1 (backfill) and L2 (Parquet CronJob, immutable read-only generations) are **batch, stateless, restartable**. L3 alone forces a **stateful, always-on single-writer serving pod** (PS-T4 §3):

- The **multi-thread writer CRASHES** at scale ("index writer killed", arena starvation) — fixed only by single-thread + ≥2 GB arena. Documented footgun, not hypothetical.
- **94 GB on disk → 300 Gi non-expandable PVC** (PS-T4 §5: local-path can't expand; under-sizing = destructive re-create); **~97 min single-thread rebuild** (not parallelizable).
- **The follow loop is unimplemented** (PS-T4 §3.2 🚩) — a `--follow`/watermark-poll mode is a required fork code addition; without it the index goes stale.
- Single-writer locking, Recreate strategy, scale-0 backfill dance, a node-label footgun (PS-T4 §11 MUST-VERIFY) — all new on-call surface for a homelab.

### 4. The one unique upside (ms freshness) is NOT needed for this feature

EXP-E measured the index's genuinely unique advantage: **~2 ms freshness** vs DuckDB's minute-freshness. But **PS-T1 and PS-T4 both conclude typeahead does not need it** — users don't perceive a torrent crawled 10 s ago not yet being searchable; what they perceive is a 300 ms keystroke. And PS-T4 §3.2 shows **ms freshness requires the gRPC-push source, which _breaks_ keep-everything** (a Go image change); the keep-everything PG-tail source gives only seconds. So the index's headline differentiator is a capability this feature doesn't require and can't get without extra cost.

### 5. Risk of reinventing / re-buying a typeahead engine (PS-T2)

PS-T2's decisive finding: **the requirement is infix/substring, not prefix** — which _demotes_ the prefix-first typeahead engines (Meilisearch, Typesense) whose core competency we'd be reaching for. And the **+~90 GB postings cost is intrinsic to CJK-correct substring and engine-independent** — every engine that does CJK substring pays it; they only **relocate** it (Quickwit→object store but sub-second-not-<50 ms; Typesense→RAM, infeasible at 879 M docs ≈ hundreds of GB; OpenSearch→JVM heap, anti-recommended for ngram bloat) and **add a service**. pg*trgm loses three ways (CJK-broken <3 chars; degenerates to full scan on short queries; needs its \_own* path table on the very PG node we're unloading). The one external worth a _gated spike_ is **Manticore** (single binary; `min_infix_len` claims +1–10% disk) — but its wildcard expansion blows up on broad/CJK infixes and its CJK is 1-gram-only vs our measured-1.0 bi/tri-gram; its claim is Latin-only and unmeasured for us.

### 6. Demand is unproven (PS-T1)

PS-T1 read the code: the UI search bar fires on **Enter only** (no `(input)` binding, no autocomplete anywhere in `webui/src/`); search is **torrent-grained** FTS; there is **no per-file path-search backend at all** (`TorrentFiles` is an `infoHashes`-scoped lister with no path filter). The feature is **greenfield on both axes** and **nothing in the product reaches for it.** CJK is a _correctness_ footnote (15.2% of files; the fast case anyway at 0.07–0.86 ms), **not** a demand driver. Verdict: **nice-to-have, not a hard need.**

---

## Alternatives, weighed (folding PS-T2)

| #       | Alternative                                                                                                                                                 | Cost                                                                                           | Verdict                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **(a)** | **Search-on-SUBMIT, not per-keystroke.** DuckDB path-ILIKE common-substring+LIMIT **79 ms**; DuckDB-FTS ASCII **~150 ms** (+34.9 GB, ASCII/CJK-token-only). | +0 GB (ILIKE) / +35 GB (FTS)                                                                   | **Strong.** 150 ms on submit is well under the human "interactive" threshold. Kills the primary justification — `<50 ms` only matters per-keystroke.                                                                                                                                                                                                                                                                                                                                                                                                            |
| **(b)** | **ASCII edge-ngram typeahead + CJK-degraded-to-substring-on-submit.**                                                                                       | Much smaller than full bi/tri-gram (DuckDB-FTS ASCII = +35 GB; ASCII edge-ngram cheaper still) | **The most defensible cost-down — PS-T2's recommended DEFAULT if _any_ index is greenlit.** The +90 GB is CJK char-ngram postings; ASCII-only avoids the explosion, keeping per-keystroke fast for the ~85% ASCII case and degrading CJK (15.2%) to submit-time substring (DuckDB-ILIKE 142 ms paginated). Caveat (PS-T3): stock `prefix_only` anchors at whole-text offset 0 (needs a composed per-word tokenizer) and CJK becomes a **second-class, non-realtime** experience — acceptable _only_ if the product accepts that. Sized by the same micro-bench. |
| **(c)** | **External engine (Meilisearch / Typesense / Quickwit / Manticore).**                                                                                       | Relocates the intrinsic +90 GB-class cost + adds a service                                     | **No net win (PS-T2).** Prefix-first engines don't do infix; object-store misses <50 ms; all-RAM is infeasible; **Manticore is the only one worth a gated spike** — and only if ops-simplicity is judged to outweigh our measured CJK fidelity.                                                                                                                                                                                                                                                                                                                 |
| **(d)** | **No free-text path search.** Structured ext∧size + ranges + collapse + path-ILIKE-on-submit via DuckDB.                                                    | +~4 GB (L2 only)                                                                               | **The honest baseline.** Covers every _demonstrated_ "find my file" query at <250 ms (ARCH-C / RUN-2).                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

**Key finding (all four threads agree):** there is **no demonstrated query that (a)+(d) cannot serve acceptably on submit.** The L3 index's unique residual value collapses to one narrow niche — **per-keystroke free-text where the query may be a _rare_ CJK substring** (the only case DuckDB serves both correctly _and_ slowly). Real, but unproven as a _need_.

---

## THE GATE — ALL must hold before spending on L3; even then, micro-bench first

> Build L3 **only if every one** of these is **demonstrably TRUE** (each is falsifiable). If **any** fails → **keep deferred**. And note the **sequencing within the gate**: G1→G2 establish need, then **the cheap PS-T3 micro-bench runs FIRST**, then G3/G5 are evaluated against _its_ numbers — not against the 94 GB per-file figure.

**G1 — DEMAND (kills "unproven need"; feeds from PS-T1).** Recorded, concrete demand for interactive per-keystroke free-text path _substring_ search in the bitmagnet web UI — via **either** (i) a feature request / issue with real user pull, **or** (ii) production query-logs/telemetry showing users issuing free-text path-substring queries at volume (not merely structured ext∧size filters), **with a measurable fraction being rare / CJK substrings**. _Falsifiable: no such signal ⇒ gate fails._ **(Today: PS-T1 confirms this signal does NOT exist.)**

**G2 — CHEAP TIER PROVEN INSUFFICIENT (kills "alternatives cover it"; feeds from PS-T2/d).** L2 (DuckDB search-on-submit: ILIKE 79 ms / FTS 150 ms + structured ext∧size + path-ILIKE) is **deployed in prod** and the **demanded queries are measured to actually hit the ~22–23 s ILIKE wall in production** — not in theory. _Falsifiable: measure prod p95 of the real demanded query mix; if <~1 s, gate fails._

**G3 — PER-KEYSTROKE REALLY NEEDED _and_ the index actually delivers on REAL prefixes (kills "submit is fine" + the §2 verified latency hole; feeds from PS-T3).** A product decision that per-**keystroke** (not on-submit) is required, **AND** PS-T3's gated micro-bench has run and shows, on the **per-torrent path-bag** design, that the **broadest 3-char prefix clears `<50 ms` warm p50** (recall: broad short prefixes measured 100–320 ms per-file; top-k early-term is a _verified_ dead end, so this rests entirely on per-torrent + min-chars match-set shrinkage). If the micro-bench fails the 3-char bar, the cheaper edge-ngram (b) / external (c) options must be evaluated and shown inferior **before** any per-file +90 GB build. _Falsifiable: micro-bench broadest-3-char warm p50 ≥ 50 ms ⇒ gate fails as specified._

**G4 — SEQUENCING (standing constraint, non-negotiable).** L1 (✅ deployed + verified) and L2 (DuckDB-Parquet + `agg_torrent_ext`) are **both deployed AND proven in production** first. L3 is **purely additive** and **NEVER on the `torrent_files` DROP path.** _(The DROP remains gated only on L1+L2 proven live, layer by layer — never on L3.)_

**G5 — COST MEASURED & ACCEPTED (replaces "+90 GB" with the real number; feeds from PS-T3/T4).** The PS-T3 micro-bench has measured the **real per-torrent index size** (the 94 GB / 300 Gi / 87→55% figures are the **per-file UPPER BOUND** — PS-T4 sized the PVC on it); the measured size fits the chosen node (HEL1) PVC with margin; **and** the operator explicitly accepts, in writing, the measured savings impact (between −87% and −55%) **and** the always-on single-writer ops burden (PS-T4: scaled-1 writer, unimplemented follow loop, single-thread 97-min rebuild, non-expandable PVC, node-label verify).

**If all hold → conditional GO at the cheapest variant the micro-bench supports.** Per PS-T2, the **default flavor should be (b) ASCII edge-ngram typeahead + CJK-on-submit**, escalating to the full per-torrent CJK ngram index **only if** the demand in G1 is specifically for _realtime CJK per-keystroke_ substring; if going external, **spike Manticore gated** (don't adopt on the brochure number). **If any fails → NO-GO / deferred.**

---

## Sequencing constraint (reinforced — non-negotiable)

Per the standing user directive (2026-06-08): **do NOT drop `torrent_files` until each replacement layer is deployed AND proven in production, layer by layer.** L3 sits **outside** that chain entirely:

```
L1 blob (✅ deployed+verified) ─→ L2 DuckDB-Parquet + agg_torrent_ext (deploy → prove parity+latency in prod) ─→ [torrent_files DROP, gated ONLY on L1+L2 proven]
                                                                                                                   ▲
L3 path-FTS index  ──────────── purely additive, off the DROP path; built ONLY if the gate fires, micro-bench FIRST ─┘
```

L3 is **never a prerequisite for the DROP** and is **never deployed before L1+L2 are proven**. Its absence never blocks anything.

---

## Synthesis of PS-T1–T4

- **PS-T1 (requirements) — ✅ corroborates NO-GO.** Nice-to-have, no demand signal; greenfield on both UI and backend; `<50 ms per-keystroke` structurally unmet for its dominant (broad-prefix) query shape; achievable contract is **Contract B: debounced, min-3-char, search-on-pause at sub-second p95**, not true <50 ms typeahead. Freshness req = seconds–minutes (NOT ms). Feeds **G1**.
- **PS-T2 (alternatives) — ✅ corroborates "gate it," refines the engine choice.** Requirement is **infix not prefix** (demotes prefix engines); the +~90 GB postings cost is **intrinsic to CJK substring and engine-independent**; only a **local mmap inverted index measured <50 ms** (selective); external engines relocate cost + add a service; **Manticore** is the lone external worth a _gated spike_; pg*trgm loses. Verdict: hand-rolled Tantivy is best-fit for the bar **but gate the whole thing on measured demand**; if greenlit, **default to (b) ASCII-typeahead + CJK-on-submit before paying for the full +90 GB CJK ngram**, and spike Manticore gated if going external. (PS-T2 also reframes "reinventing typeahead": we don't need \_prefix* completion — we need CJK-correct _infix_ over 879 M paths, a narrower/harder niche off-the-shelf typeahead engines don't serve.) Feeds **(b)/(c)/G2/G3**.
- **PS-T3 (index design) — ⚠️ weakens the cost-against, hardens the latency-against.** (1) **Top-k early termination is a VERIFIED dead end** for our conjunction query (source-traced) — the only lever is shrinking the match-set. (2) **Per-torrent path-bag (~17 M docs)** is the major structural lever — plausibly shrinks the index **materially below 94 GB** (UNMEASURED) and the broad-prefix match-set up to ~50×. (3) Recommends min-chars≥3 + 120 ms debounce + server gram-guard + per-torrent + index-sort by seeders; edge-ngram held in reserve (ASCII-only, trades CJK away). (4) **One gated micro-bench** decides it: per-torrent size + broadest-3-char latency on the existing HEL1 restore. Feeds **G3/G5**.
- **PS-T4 (deploy/ops) — ✅ quantifies the always-on-writer burden.** Resurrects the Tantivy sidecar as a _third_ engine (path-FTS only) on HEL1; reuses the drafted `bitmagnet-search` role with **five load-bearing deltas** (file-grained 94 GB index; **writer scaled-1 always-on**; **300 Gi non-expandable PVC**; single-thread+2 GB backfill ~97 min; new `FileSearchService` RPC). **Follow loop unimplemented** (required fork addition). **Keep-everything holds** for the deploy (PG-tail, seconds freshness); **ms freshness needs gRPC-push = breaks keep-everything** and isn't needed for typeahead. Confirms L3 **never gates the DROP**. Feeds **§3/§4/G5**; the 94 GB/300 Gi figures are the **per-file upper bound**.

---

## Sources (all measured/verified elsewhere — not re-derived here)

- PS-T1 `pathsearch-T1-requirements-ux.md`; PS-T2 `pathsearch-T2-gap-and-alternatives.md`; PS-T3 `pathsearch-T3-index-design.md`; PS-T4 `pathsearch-T4-deploy-ops.md`.
- Space scenarios A–D, 276 GB baseline, −93%/−87%/−55%: `space-savings-vs-torrent-files.md`.
- ngram index 94 GB @879.5 M, CJK recall 0.0037→1.0, latency CJK 0.07–0.86 ms / broad ASCII 100–320 ms, writer crash, 97-min build, ~2 ms freshness: `cjk-tokenizer-and-incremental-merge-bench-RESULTS.md` (EXP-D/D2/E).
- DuckDB per-file parity <250 ms, path-ILIKE 79 ms common / 22–23 s rare, FTS 150 ms / +34.9 GB, sort/rollup levers, ART CREATE INDEX no analytical speedup: `arch-c-parity-and-optimization-results.md`, `file-grained-search-benchmark-results.md` (RUN-2/3), `duckdb-parquet-parity-architecture.md`.
- Structured per-file Tantivy index rejected (scan-bound ~1.3 s, +14–25 GB): `file-index-bench-RESULTS.md` (RUN-4).
- Main content-search realtime <25 ms / base+delta freshness: `exp-a-write-read-path.md` (EXP-A), `exp-b-base-delta-freshness.md` (EXP-B).
- Sequencing constraint (no DROP until each layer proven in prod): user directive 2026-06-08 (MEMORY).
