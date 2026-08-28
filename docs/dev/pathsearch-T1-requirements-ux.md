# PS-T1 — Requirements & UX surface for "realtime, per-keystroke, <50 ms free-text PATH search"

**Author:** `ps-t1-reqs` (team `bitmagnet-bench`) · **Date:** 2026-06-09 · **Status:** requirements / UX-contract spec
**Scope:** define precisely what _realtime, per-keystroke, <50 ms free-text path search_ means for bitmagnet, where it would live in the real product surfaces, and whether it is a hard need. **Read-only investigation — source of truth = the bitmagnet codebase.**
**Cross-refs:** `docs/dev/cjk-tokenizer-and-incremental-merge-bench-RESULTS.md` (EXP-D/D2/E latency + freshness, _measured_), `docs/dev/duckdb-parquet-parity-architecture.md` (the structured per-file search tier), `docs/dev/arch-c-parity-and-optimization-results.md` (path-FTS via ILIKE / DuckDB-FTS numbers).

> **One-line verdict:** Per-keystroke <50 ms free-text _path_ search is a **nice-to-have, not a hard need**. It does not exist on any surface today (neither the UI affordance nor the per-file backend), and the latency target is **structurally unmet for the very query shape per-keystroke generates most** (short prefixes → broadest match-sets). The defensible product target is **debounced, min-3-char, search-on-pause free-text path search at sub-second p95** — which DuckDB-on-Parquet already serves for structured per-file queries and which an inverted index serves for free-text at sub-second (not <50 ms) for the broad tail.

---

## 1. What exists today (READ THE CODE)

### 1.1 The main search bar is **search-on-submit (Enter)**, not per-keystroke

`webui/src/app/torrents/torrents-search.component.html:146-164` — the query box is a single `<input matInput>` whose **only** firing trigger is:

```html
<input
  matInput
  [placeholder]="t('torrents.search')"
  [formControl]="queryString"
  autocapitalize="none"
  (keyup.enter)="controller.setQueryString(queryString.value)"
/>
```

- Fires on **Enter only**. There is **no `(input)` / `(keyup)` per-character binding**, no `mat-autocomplete`, no typeahead panel. A grep of `webui/` for autocomplete/typeahead finds nothing in `src/`.
- The `debounceTime(100)` in `torrents-search.controller.ts:145` debounces **control-state changes** (facet toggles, paging, order-by) into the GraphQL params — it does **not** debounce keystrokes, because `queryString` is only mutated on Enter (`setQueryString`, controller.ts:274-292).
- ⟹ The current UX contract is: _type a full query, press Enter, get a page of results._ There is **no realtime/per-keystroke behaviour anywhere in the product.**

### 1.2 The search is **torrent-grained full-text**, not per-file path search

The query string flows: UI `queryString` → GraphQL `TorrentContentSearchQueryInput.queryString` (`graphql/schema/torrent_content.graphqls:2`) → `query.SearchString(...)` → `fts.AppQueryToTsquery` (`internal/database/fts/tsquery.go:9`) → Postgres `tsv @@ tsquery` over **`torrent_contents.tsv`** (a `tsvector` GENERATED column, `migrations/00006_tsv.sql:29`; `to_tsvector('simple', search_string)`).

- The match unit is a **torrent** (returns `TorrentContent` rows), not a file.
- File _names_ leak into the FTS only as the **D-weight** of the tsv via `Torrent.fileSearchStrings()` (`internal/model/torrents.go:218-272`, built in `UpdateTsv` `internal/model/torrent_contents.go:66`). But this is: (a) torrent-grained, (b) **capped at 100 files** by the crawler, (c) a lossy prefix/suffix-deduped extract, (d) pre-computed into the vector. It is **not** per-file path matching and cannot return file rows or do path substrings.
- The tsquery grammar _does_ support a trailing prefix wildcard (`expr:*`, `tsquery.go:132-135`) — but only when the user **explicitly types `*`**. It is not automatic per-prefix matching, and it is still torrent-grained word-prefix, not path substring.

### 1.3 There is **no per-file search surface at all**

The only per-file GraphQL surface is `TorrentFilesQueryInput` (`graphql/schema/torrent_files.graphqls:1-10`): its inputs are `infoHashes`, `limit/page/offset`, `orderBy`, `cached` — **no `queryString`, no path filter**. Backed by `search.TorrentFiles` (`internal/database/search/search_torrent_files.go:17`), it exists to **list the files of a given torrent** (the detail-view "files" tab), not to search across files. File-extension filtering on the _content_ search exists (`criteria_torrent_file_extension.go`) but it is a structured `EXISTS`/`IN` filter, not free-text path search.

### 1.4 Torznab and the Tantivy sidecar are the same torrent-grained surface

- **Torznab** (`internal/torznab/adapter/search_options.go:55`): `r.Query` → `query.SearchString(r.Query)` → the identical torrent-content FTS. No path search.
- **Tantivy sidecar (Phase 3)** explicitly mirrors `tsv @@ tsquery` torrent-content search (`internal/search/tantivy/document.go:22`: "mirroring bitmagnet's `tsv @@ tsquery` Postgres search"). It is a torrent-content index, **not** a per-file path index.

**Net:** per-keystroke free-text path search is **doubly absent** — there is neither a per-keystroke UI affordance nor any per-file path-search backend. It would be a brand-new feature on _both_ axes.

---

## 2. Defining the terms (what would "per-keystroke" actually require)

If we were to build it, "per-keystroke" must be pinned to one of two contracts:

| Contract                                       | Trigger                               | Backend pattern                              | Implication                                                                    |
| ---------------------------------------------- | ------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------ |
| **A. True typeahead / autocomplete**           | fire on (almost) every keystroke      | prefix/substring match, top-k, instant panel | implies <50 ms _per keystroke_ — the hard reading; this is what the task names |
| **B. Search-on-pause (debounced live search)** | fire after the user pauses (debounce) | same query as submit, just auto-fired        | sub-second p95 is fine; "realtime" = "I didn't press Enter"                    |

The task's phrase _"keep typing to narrow"_ + _"<50 ms"_ points at **Contract A** (typeahead). That is the demanding one and the one this doc stress-tests. **Contract B is the pragmatic, achievable interpretation** and is what we recommend if any "live" behaviour is wanted at all.

### 2.1 The UX contract for Contract A (if pursued)

- **Min chars before firing:** ≥ 3 (a 1–2 char query is meaningless and, see §3, pathologically broad). **Mandatory**, not optional.
- **Debounce:** 150–250 ms idle before firing (coalesce a burst of keystrokes into one query; otherwise a 10-char word = 10 queries).
- **In-flight cancellation:** every new keystroke must cancel the prior request (client-side `switchMap`); without it, slow broad queries pile up.
- **Top-k cap:** small (e.g. 20–50) — a typeahead panel never paginates; this also enables backend early-termination _only if_ results are sorted by a fast field (not by relevance over the full match-set).
- **"Keep typing to narrow":** the contract promises each added char shrinks the result set and _lowers_ latency — which is true (selective ⇒ fast) but front-loads the cost onto the **early, broad** keystrokes (§3).
- **Freshness:** typeahead users do not perceive "this file was crawled 40 s ago"; **seconds-to-minutes freshness is fine.** Millisecond freshness is _not_ a typeahead requirement.

---

## 3. THE CENTRAL TENSION (the spine)

> **A per-keystroke contract is, by construction, a generator of the worst-case query shape for any path index.**

Per-keystroke means the system must answer at prefix lengths 1, 2, 3, 4, … as the user types. **Short prefixes match the broadest sets.** For the ngram path index that EXP-D measured, a short query maps to the **most common grams**, i.e. the **longest postings lists** — exactly the slow case. So the latency budget is blown _precisely where per-keystroke creates the most load_ (the first few keystrokes of every single search).

The measured numbers (`cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`, EXP-D2, full **879.5 M**-doc ngram index, force-merged 1 segment):

| Query shape (what the keystroke produces)    | avg hits  | cold p50    | cold p95 | warm p50    | warm p95 | warm p99 |
| -------------------------------------------- | --------- | ----------- | -------- | ----------- | -------- | -------- |
| **Broad** (short prefix / common ASCII gram) | 5,569,267 | **145 ms**  | 320 ms   | **101 ms**  | 244 ms   | 247 ms   |
| **Selective** (longer / CJK substring)       | 736,858   | **0.86 ms** | 12.4 ms  | **0.07 ms** | 7.9 ms   | 80 ms    |

(`RESULTS.md:68-71`.)

So against a 50 ms budget:

- **Selective queries PASS by 100–700×** (0.07–0.86 ms p50). These are the _later_ keystrokes once the user has typed enough to be specific.
- **Broad queries FAIL by 2–6×** (101–145 ms p50, 244–320 ms p95). These are the _early_ keystrokes — and a 1–2 char query is **broader still than the 5.6 M-hit grams tested** (a single char like `a`/`e`/`の` matches a large fraction of 879.5 M file docs → effectively unbounded). The measured 5.6 M-hit number is therefore an **optimistic floor** for the broadest case.

**Conclusion: even _with_ the inverted index, "<50 ms per keystroke" is NOT automatically met.** It holds for the selective tail and is violated for the broad head — and the broad head is non-optional under a true per-keystroke contract. The min-3-char rule and debounce _reduce how often_ the broad queries fire and _narrow_ them, but they do not bring a broad-prefix query _under_ 50 ms; they only move the median toward the selective regime. The honest latency story is **"sub-second for the broad tail, sub-ms once selective"**, not "<50 ms per keystroke".

(For contrast, the _structured_ per-file tier — DuckDB-on-Parquet — answers paginated structured queries in 17–35 ms and path-substring ILIKE in ~142 ms for common-substring+LIMIT but ~23 s for an unprunable rare/no-LIMIT scan; see `duckdb-parquet-parity-architecture.md` and `arch-c-parity-and-optimization-results.md`. DuckDB is _not_ a per-keystroke engine for free-text; the inverted index is the only thing that even gets the broad case to sub-second.)

---

## 4. Concrete targets

Stated honestly, separating the _aspirational_ per-keystroke target from the _achievable_ one:

| Target               | Per-keystroke (Contract A, aspirational)                                                                                      | Search-on-pause (Contract B, achievable & recommended)               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| **p50 latency**      | <50 ms — **met only for selective (≥4–5 char) queries**; broad short prefixes ≈ 100–145 ms                                    | <150 ms p50 (met: selective sub-ms, broad ~100 ms)                   |
| **p95 latency**      | <50 ms — **not met** for broad prefixes (244–320 ms)                                                                          | <350 ms p95 (met by the inverted index; broad tail 244–320 ms)       |
| **p99 latency**      | best-effort; broad-prefix p99 ≈ 247 ms, CJK tail 80 ms                                                                        | <500 ms p99                                                          |
| **Min query length** | ≥3 chars **mandatory** (1–2 chars unbounded)                                                                                  | ≥3 chars                                                             |
| **Freshness**        | **seconds–minutes is fine** for typeahead; the inverted index _can_ do ~2 ms (EXP-E) but that is not required by this feature | minutes (DuckDB base+delta) or ms (inverted index) — both acceptable |

Freshness note: EXP-E measured ~2 ms inverted-index freshness lag (`RESULTS.md:53-55`), and EXP-B measured DuckDB base+delta at minute-scale. **Neither this feature's UX nor any product evidence demands millisecond freshness** — so freshness is _not_ a differentiator that justifies the index for _this_ feature.

---

## 5. Where it would live

| Surface                                             | File(s)                                                                                         | Path/free-text search today?                                 | Closest existing hook                                                                                                        |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| **Web UI search bar**                               | `webui/.../torrents-search.component.html:148`, `.controller.ts:274`                            | No (Enter-submit, torrent-grained FTS)                       | Would need a new `(input)`+debounce+`switchMap` typeahead and a new results surface (file rows, not torrent rows)            |
| **GraphQL `torrentContent.search`**                 | `graphql/schema/torrent_content.graphqls`, `internal/database/search/search_torrent_content.go` | Torrent-grained FTS via `queryString`→tsquery                | n/a — wrong grain (torrents, not files)                                                                                      |
| **GraphQL `torrentContent.files` / `TorrentFiles`** | `graphql/schema/torrent_files.graphqls:1`                                                       | **No path filter at all** — `infoHashes`-scoped listing only | The natural place to add a `pathQuery`/free-text input — but it is currently a per-torrent lister, not a cross-corpus search |
| **Torznab**                                         | `internal/torznab/adapter/search_options.go:55`                                                 | Torrent-grained FTS                                          | n/a (API contract is torrent-grained by spec)                                                                                |
| **Tantivy sidecar**                                 | `internal/search/tantivy/document.go`                                                           | Torrent-content mirror of `tsv@@tsquery`                     | Would need a **second, per-file** index/field — this is the `feat/file-grained-search` / EXP-D ngram-path-index work         |

**Realistic home:** a **new** cross-corpus per-file search resolver (a `fileSearch` query, not the existing per-torrent `TorrentFiles`), fed either by the DuckDB-on-Parquet per-file tier (structured + ILIKE path substring) or, for true free-text path search, by the ngram path index (the +90 GB sidecar). The web UI would need a genuinely new component (file-row results, debounced input). **No existing surface can be incrementally upgraded to per-keystroke path search; both UI and backend are greenfield.**

---

## 6. Honest verdict: hard need or nice-to-have?

**Nice-to-have.** Evidence:

1. **No demand signal in the product.** The current search is deliberately Enter-submit and torrent-grained; there is no per-file search and no typeahead anywhere. Nothing in the UI, GraphQL schema, or Torznab adapter reaches for it. bitmagnet's core job is _discover torrents_, and the content-search tier already serves that.
2. **The headline target is structurally unmet for its own dominant query shape.** Per-keystroke ⇒ broad short prefixes ⇒ 100–320 ms (EXP-D2) — over budget by 2–6×. The thing the feature is _named_ for (<50 ms per keystroke) is the thing it cannot reliably deliver. Delivering it as specified would require accepting that "the budget is met only after you've typed a specific-enough query," i.e. quietly redefining the contract to Contract B.
3. **The cheap tier already covers the real use-cases.** Structured per-file search ("find `.mkv` > 1 GB", per-torrent file lists, path ILIKE for common substrings) is served by DuckDB-on-Parquet at **+3.9 GB / <250 ms** (`duckdb-parquet-parity-architecture.md`; RUN-2). That covers the overwhelming majority of "I want to find a file" intents without per-keystroke and without the index.
4. **The only thing the index uniquely buys** is interactive _broad free-text_ path search (sub-second, CJK-correct) + ms freshness — at **+~90 GB** (94 GB measured, `RESULTS.md:66`), which is **larger than the entire DuckDB-on-Parquet architecture (~12 GB)** and ~2.6× the ASCII-only DuckDB-FTS option. Neither broad free-text nor ms freshness is demanded by any product requirement on file.

**Recommendation:** treat per-keystroke <50 ms path FTS as a **gated, evidence-required enhancement**, not a deliverable. If _any_ "live" search is wanted, ship **Contract B** (debounced, min-3-char, search-on-pause) over the cheap DuckDB-on-Parquet tier first, measure whether users actually want free-text-over-paths, and only then gate the +90 GB ngram index on a _measured_ demand for interactive broad/CJK free-text path search. Per the standing sequencing constraint, none of this gates (and the index certainly does not gate) the `torrent_files` DROP.

---

## 7. Summary for downstream threads (PS-T2/T3/T4/T5)

- **UX contract (if built):** typeahead = min-3-chars, 150–250 ms debounce, in-flight cancellation, top-k 20–50, file-row results on a **new** surface. Freshness req = seconds–minutes (NOT ms).
- **The tension (verdict):** per-keystroke generates the broadest match-sets first; EXP-D2 puts those at **101–145 ms p50 / 244–320 ms p95** — **the <50 ms budget is not met for the head of every search**, only for the selective tail (0.07–0.86 ms). Min-chars + debounce narrow/throttle but do not fix the broad-prefix latency. So PS-T3's index design must either (a) accept sub-second-not-50 ms for broad prefixes, or (b) restrict the contract to ≥N chars where the match-set is provably selective.
- **Where it lives:** greenfield on both axes — a new `fileSearch` resolver (not the per-torrent `TorrentFiles` lister) + a new debounced UI component. No existing surface upgrades incrementally.
- **Hard need?** **No — nice-to-have.** Cheap DuckDB-on-Parquet already covers structured per-file search (+3.9 GB / <250 ms); the +90 GB ngram index uniquely buys only broad/CJK free-text + ms freshness, neither of which has a product demand signal. Gate strictly.
