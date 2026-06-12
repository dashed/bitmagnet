# Realtime, per-keystroke, <50 ms free-text PATH search — investigation & decision

**Date:** 2026-06-09 · **Status:** ✅ Investigation complete — **NO-GO by default; DEFER (gated).**
**Question:** can/should bitmagnet offer *realtime, per-keystroke, <50 ms, free-text **path** search* after the `torrent_files` DROP?
**Method:** 5 parallel opus research threads (team `bitmagnet-bench`, tasks #72–77), grounded in the existing measured benchmark suite (EXP-D/D2/E, ARCH-C) and firsthand code reads. Read-only; nothing built, applied, or measured anew.

> **One-line answer:** This is the single capability the cheap replacement stack (DuckDB-on-Parquet + `file_extensions` JSONB + blob) deliberately cannot serve — but it is a **nice-to-have with no demonstrated demand**, its own headline target (`<50 ms per keystroke`) is **structurally unmet for the early keystrokes**, and it would **roughly triple the replacement footprint**. **Defer it behind a hard 5-part gate; it never gates the DROP.**

Thread docs: [T1 requirements/UX](./pathsearch-T1-requirements-ux.md) · [T2 gap & alternatives](./pathsearch-T2-gap-and-alternatives.md) · [T3 index design](./pathsearch-T3-index-design.md) · [T4 deploy/ops](./pathsearch-T4-deploy-ops.md) · [T5 decision](./pathsearch-T5-decision.md) · **[PS-MB1 micro-bench RESULTS](./pathsearch-microbench-RESULTS.md)** · [spec](./pathsearch-microbench-spec.md). Upstream: [cjk-tokenizer-…-RESULTS](./cjk-tokenizer-and-incremental-merge-bench-RESULTS.md), [arch-c-parity-and-optimization-results](./arch-c-parity-and-optimization-results.md), [space-savings-vs-torrent-files](./space-savings-vs-torrent-files.md), [duckdb-parquet-parity-architecture](./duckdb-parquet-parity-architecture.md).

---

> 🚦 **LATER SAME DAY:** the user **committed to building L3** (the NO-GO-by-default is superseded), and the [PSX campaign](./psx-campaign-RESULTS.md) **BUILT** the production `WithFreqs` index: **13.32 GiB**, recall 1.0000. Production-shape correction: broad-single-gram **p95 ≈ 77–94 ms** (`TopDocs` page collector; the 55–65 ms below is a `Count` lower bound); realistic multi-word < 50 ms; tail engine-irreducible → UX.

## 🟢 MEASURED UPDATE — PS-MB1 ran (2026-06-09): the cost case flips

The investigation below (§1–§7) reached its decision on the **per-file** index (~90 GB, broad-prefix 100–320 ms). The gated micro-bench it recommended was then **executed on the full 879.5 M-row HEL1 restore** (team `bitmagnet-bench`, runner+analyst; arms A/B/C + recall + the full-corpus capstone A2). The headline numbers move materially — **per-torrent path-bag granularity transforms the economics**:

| dimension | per-file (investigation assumption) | **per-torrent, MEASURED (PS-MB1 A2, 16,973,470 torrents)** |
|---|---|---|
| index size | ~90 GB | **13.54 GiB** production (`WithFreqs`; as-built 81.86 GiB is 83.5 % droppable positions) |
| `ascii3` warm **p50** | 100–145 ms (breaks the gate) | **24.71 ms** — PASS (docs cap at ~17 M regardless of corpus growth) |
| `cjk3` | sub-ms | **0.21 ms** |
| broadest-substring **p95/p99** | 244–320 ms | **~55–65 ms** (`ascii3` p95 58.6 / p99 59.7) — over 50 ms only at the worst-case tail |
| recall | 1.0 (ngram) | **1.0000** every group |
| **space-savings impact** | drop+index ⇒ **87 % → 55 %** | drop+index ⇒ **87 % → ~84 %** |

**What changes:** the L3 path-FTS index is **NOT a footprint-tripler** — at per-torrent granularity it is a **~13.5 GiB, median-interactive (~25 ms) add-on**. The `<50 ms` target is met at **p50** but the *broadest* synthetic substrings breach it at the **p95/p99 tail** (~55–65 ms; mitigated by min-chars≥3, real-query selectivity, debounce, top-k) — so it's "median-interactive," not "uniformly <50 ms." The one design unlock = index the path field **`WithFreqs` not `WithFreqsAndPositions`** (ngram positions are dead weight → drops 83.5 % of the index); this is a standing recommendation for any path-ngram field (incl. the existing Tantivy sidecar).

**What does NOT change:** the **build-gate is unchanged** — L3 is still **NO-GO by default, purely additive, and never gates the `torrent_files` DROP** (PS-T5 G4). PS-MB1 only proves L3 is *buildable cheaply and fast enough* **if** a real product demand (G1) + an in-prod ILIKE wall (G2) ever fire. Absent that demand, defer. So §1–§7 below stand on the *decision* (defer); only their *cost framing* ("triples the footprint," "structurally unmet") is superseded by these measured per-torrent numbers. Full detail: [PS-MB1 RESULTS](./pathsearch-microbench-RESULTS.md).

---

## 1. The headline finding — "per-keystroke" and "<50 ms" are in tension

Per-keystroke search is, **by construction, a worst-case generator**: the first keystrokes are very short (1–3 char) substrings → the **broadest** match-sets → the **longest** ngram postings lists → the **slowest** queries. The measured shape (EXP-D2, full 879.5 M-doc ngram index):

| query regime | example | measured latency (full index) | vs 50 ms |
|---|---|---|---|
| **broad short prefix** (early keystrokes) | 2–3 char, 5.6 M+ hits | **101–145 ms p50 / 244–320 ms p95** | ✗ **2–6× over** |
| selective / CJK (later keystrokes) | longer, 737 k hits | **0.07 ms warm / 0.86 ms cold p50** | ✓ 100–700× under |

So "<50 ms per keystroke" is met **only for the selective tail**, never for the broad head — and the broad head is **non-optional** under a per-keystroke contract. **All four research threads reached this independently.** The honest target is **sub-second p95**, not `<50 ms`/keystroke.

T3 then **source-traced the escape hatch shut**: in tantivy 0.26.1, block-max-WAND / top-k early termination (`for_each_pruning`) is wired **only for disjunctions** (`TermUnion`); our query is a fast-field-sorted **conjunction** (`SpecializedScorer::Other`) → **no score pruning**, and `SegmentCollector::collect` returns `()` → no early abort. **The only lever that lowers broad-prefix latency is shrinking the match-set.** (Levers that *don't* work are documented so we don't reach for them.)

---

## 2. Where it would live — greenfield on both axes (T1)

- The web UI search bar is **Enter-submit only** (`webui/.../torrents-search.component.html:148`, `(keyup.enter)`); there is **no autocomplete anywhere**. The 100 ms debounce in the controller is for facet/paging, not keystrokes.
- The query is **torrent-grained FTS** (`queryString → AppQueryToTsquery → torrent_contents.tsv @@`), **not per-file**. The only per-file surface (`TorrentFiles`) is an `infoHashes`-scoped **file lister** with **no path/free-text filter**. Torznab + the Tantivy sidecar share the same torrent-grained FTS.

⟹ per-file path search is **doubly absent** — no UI affordance **and** no backend. Delivering it needs a **new `fileSearch`/`pathTypeahead` GraphQL resolver + a new debounced UI component** in addition to any index. **No demand signal exists in the code.**

**UX contract** (if ever built): typeahead = min-3-chars **mandatory** (1–2 char never fires), 150–250 ms debounce, in-flight cancellation (switchMap), top-k 20–50, file-row results on a new surface. Freshness need = **seconds-to-minutes**, *not* the index's ~2 ms.

---

## 3. The option matrix — is a hand-rolled Tantivy ngram index even the right tool? (T2)

| option | marginal disk | broad-short p50/p95 | selective | CJK | freshness | survives DROP? | verdict |
|---|---|---|---|---|---|---|---|
| PG `ILIKE` on path | — | ~23 s | ~23 s | ✓ (correct, slow) | realtime | ✗ (table dropped) | moot post-cutover |
| **DuckDB-FTS/BM25** | +35 GB | ~150 ms | ~150 ms | ✗ token-only (CJK-broken) | batch (min) | ✓ (Parquet) | **covers non-realtime ASCII** |
| `file_extensions` JSONB | +0.1 GB | N/A (not free-text) | — | — | realtime | ✓ | N/A for path FTS |
| per-torrent blob decode | +0 GB | per-torrent, not per-key | — | ✓ | realtime | ✓ | wrong granularity |
| PG `pg_trgm` GIN on a path table | large | full-scan on <3-char | — | ✗ 1–2 char | realtime | ✗ needs own path table on the node we're unloading | **loses 3 ways** |
| External (Meili/Typesense/Algolia) | RAM-heavy | prefix-first | — | varies | realtime | ✓ | **wrong shape** (infix ≠ prefix) |
| Quickwit (Tantivy, object-store) | +~90 GB (cheap storage) | sub-second | sub-second | ✓ ngram | near-RT | ✓ | misses **local <50 ms** bar |
| **Manticore** | claims +1–10% (Latin-only, **unproven CJK**) | wildcard blow-up on broad/CJK | — | 1-gram only | realtime | ✓ | **lone external worth a gated spike** |
| **hand-rolled Tantivy char-ngram** | **+~90 GB (per-file ceiling)** | 100–320 ms | **0.07–0.86 ms** | **✓ bi/tri-gram recall 1.0** | ~2 ms | ✓ | **best fit for the stated bar** |

Three load-bearing reasons the hand-rolled index wins the *stated* bar — but **not** as a unique trick:
1. The **~+90 GB CJK-substring postings cost is intrinsic and engine-independent** (EXP-D postings math). No external engine undercuts it; they only **relocate** it (Quickwit→object-store, Typesense→RAM) and **add a service**.
2. **Only a local mmap inverted index measured <50 ms** (selective). Quickwit object-store = sub-second; Typesense all-RAM at ~17–873 M docs = hundreds of GB RAM = infeasible.
3. We **already own the hardest part**: `tokenizer.rs` registers **one** tokenizer for writer **and** query — the exact fix for the CJK QueryParser gotcha (tantivy #718) that breaks Quickwit/Tantivy out of the box.

Two honest caveats: the requirement is **infix/substring, not prefix** (this demotes the "typeahead specialist" engines — their core competency is the wrong shape); and **switching engines does not solve §1** — the broad-short-prefix latency is intrinsic to match-set size.

---

## 4. The index design that *does* close the gap (T3)

Three **match-set-shrinking** moves (the only lever that works), CJK preserved:
1. **min-chars = 3 + 120 ms debounce + a server-side gram-count guard.** 1-char already returns nothing (ngram `min=2` → no token → `EmptyQuery`); the 2-char single-broadest-bigram (the measured 100–320 ms case) is simply **never fired**.
2. **Per-torrent path-bag granularity (~17 M docs vs ~873 M per-file).** Postings shrink with torrent-doc-frequency (avg 51.79 files/torrent) — plausibly **well below the 94 GB per-file figure** (UNMEASURED — see §6). File-level `ext∧size` + matched-file hydration delegate to the cheap DuckDB-Parquet tier (clean engine split).
3. **Index-sort by `seeders DESC`** so a capped top-k returns *desirable* hits despite the full scan.

Schema: one searchable `path_grams` ngram(2,3) field, **WithFreqs, no positions** (all ngrams at position 0), index-only never STORED; `info_hash` INDEXED+STORED (delete key + hit identity); `seeders/size/files_count` FAST. Writer = **1 thread + ≥2 GB arena** (multi-thread 256 MB writer **crashes** on ngram token explosion — measured), default `LogMergePolicy` (~2 ms freshness, bounded segments), `delete_term(info_hash)` supersession. Edge-ngram (ASCII-prefix, loses CJK-mid-run) kept only as a fallback arm.

**Serving composition:** L3 is also the candidate engine for fast
`collapse:path`. Do not ask L2/DuckDB to scan and group the full `path` column
for broad substrings. Query L3 for candidate `info_hash` values, then exact-refine
those candidates through the blob/L2 path with the real substring and any
structured filters (`extension`, size bounds), hydrating previews only after
exact filtering. Broad exact counts are estimates or background/cache work.

---

## 5. Deployment shape — if the gate ever fires (T4)

Resurrect Tantivy **solely** as a *third, narrow* engine: per-file/per-torrent **path-FTS typeahead only** (it does **not** serve main torrent-grained search — PG-FTS keeps that — nor structured per-file search — DuckDB-on-Parquet owns that). One process, one index.

- **Topology:** HEL1 (idle; FSN1 ~83 % mem), ClusterIP gRPC `bitmagnet-pathsearch:50051`, **internal-only** (reaches users via the already-authed web UI; no new Traefik/Authentik route). Reuses the drafted `bitmagnet-search` role shape.
- **Sizing (per-file upper bound):** PVC 200→**300 Gi** (94 GB index + ~94 GB force-merge transient; local-path not expandable), mem 6→**10 Gi** (2 GB arena + 94 GB mmap). **These are the per-file ceiling; per-torrent (§4/§6) is likely much smaller.**
- **Writer scaled-1, not the batch plan's scaled-0:** the serving pod is the permanent sole writer; steady-state freshness via an in-pod **PG-tail follow loop → seconds freshness, zero Go change** (plenty, since the hard requirement is *query* latency, not new-torrent freshness). **ms** freshness would need gRPC-push from Go = **breaks keep-everything** and isn't needed. 🚩 the `--follow` watermark mode is **unimplemented** in bitmagnet-rs (fork code to add).
- 🚨 **Must-verify before any deploy:** the drafted role pins `node_hostname: alberto-hetzner`, but the inventory HEL1 host is `alberto-hetzner-hel1` — confirm the real `kubernetes.io/hostname` label or the node-bound PVC strands the pod (likeliest first-deploy failure).

---

## 6. The decision & the gate (T5)

**NO-GO by default. Defer the L3 path-FTS index indefinitely. Purely additive — it NEVER gates the `torrent_files` DROP. If the gate ever fires, the FIRST spend is the cheap read-only micro-bench below, not the index.**

The case against, all measured:
- **Halves the headline win.** Drop + cheap composition = **−87 to −90 %**; adding the per-file index = **−55 %** (~125 GB). The index alone ≈ 3× the rest of the replacement stack. *(Cost is a **range**: per-file +~90 GB is the worst-case ceiling; per-torrent path-bag is plausibly far less but **unmeasured**.)*
- **The `<50 ms` premise is structurally unmet** for its own dominant (broad-short-prefix) query shape (§1), and the early-termination escape hatch is a **verified dead-end** (§1/T3).
- **A new failure-mode class:** an always-on stateful single-writer (multi-thread crash footgun), ~94 GB, ~97-min rebuild — against an otherwise stateless/batch L1/L2.
- **Demand is unproven:** search is Enter-submit + torrent-grained today; per-file path search and typeahead are both greenfield (§2). Alternatives **(a)** search-on-submit DuckDB-FTS ~150 ms and **(d)** structured `ext∧size` + path-`ILIKE`-on-submit (<250 ms) cover **every demonstrated need**.

**The gate — all 5 must hold; the micro-bench runs FIRST, then G3/G5 are judged against its real numbers:**
- **G1** — recorded demand for free-text path-substring (esp. rare/CJK) in UI/telemetry. *(T1: absent today.)*
- **G2** — L2 deployed **and** demanded queries proven to hit the ~23 s ILIKE wall in prod.
- **G3** — per-keystroke genuinely required **and** the **per-torrent micro-bench clears the broadest 3-char prefix at <50 ms warm p50** (else evaluate cheaper edge-ngram / a Manticore CJK-infix spike first).
- **G4** — L1 + L2 proven in prod first; **L3 never on the DROP path**.
- **G5** — the **real measured** index size fits the HEL1 PVC **and** the operator accepts, in writing, both the savings hit (−87 → −55 %) and the always-on-writer ops burden.

**The one gated micro-bench to run first (read-only, HEL1 restore, single serial run, lock+pgrep-guarded, no prod touch):** build a **per-torrent path-bag ngram(2,3)** index on the existing 50 M-row restore (reuse `bench-file-index` + a `--granularity per-torrent` flag; add an edge-ngram arm for ~free) and measure **index size + cold/warm latency of the broadest 2/3/4/5-char prefixes**. **GO** iff the broadest 3-char prefix clears `<50 ms` warm p50 **and** the extrapolated ~17 M index is materially under 94 GB. This single experiment resolves the two unknowns (G3 latency, G5 size) that the whole decision turns on.

---

## 7. Bottom line

The cheap replacement stack already delivers structured per-file search (+3.9–12.3 GB, <250 ms) and non-realtime ASCII path-FTS (ILIKE 142 ms / DuckDB-FTS 150 ms @ +35 GB). Realtime per-keystroke `<50 ms` CJK-correct free-text path search is **real, achievable only by a local inverted ngram index, intrinsically ~+90 GB at per-file granularity, structurally over-budget for the very keystrokes that define it, and unbacked by any demand.** **Defer behind the gate; if it ever fires, measure the per-torrent variant first — it is the only thing that could turn a "triples the footprint" into an "acceptable add-on."**
