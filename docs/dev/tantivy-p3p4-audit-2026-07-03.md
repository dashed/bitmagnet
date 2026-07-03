# Tantivy main-search implementation audit (Phase 3 + Phase 4) — 2026-07-03

**Scope:** adversarial, read-only audit of the dormant BM25 main-search implementation:

- **Phase 3** — the Rust Tantivy search sidecar on branch `feat/tantivy-search-sidecar`
  (commit `5f5093a9`, frozen 2026-05-29). Never deployed.
- **Phase 4** — the Go shadow-mode integration (`internal/search/{tantivy,shadow,router,searchfx}`
  + the processor dual-write hook) as present on the **deployed lineage** `alberto/my-fork`
  (audited at `c2b505ac`). Compiled into the production binary, disabled by default.

**Method:** two independent audit agents; the Rust side was built + tested in a throwaway jj
workspace on the frozen branch; the Go side was audited on the deployed tip with
`go test ./internal/search/... ./internal/processor/...` (all green).

**Context:** main search still runs PostgreSQL tsvector. The L1/L2/L3 stack replaced every
other capability the Phase-3 sidecar promised (see `torrent-files-replacement-options.md`,
`l3-live-status`), and the FIND-2 popularity-sort default covers the broad-query ranking
wall. This audit answers: (a) is the dormant Phase-4 code safe in production today, and
(b) what would a revival actually cost.

---

## Verdict summary

| Target | Grade | One-liner |
|---|---|---|
| Phase-4 shadow machinery (live binary, disabled) | **A — dormant-safe** | Inert when off; observation-only when on; one load hazard at flip time |
| Phase-3 Rust sidecar (frozen branch) | **A− — production-quality engine** | Would NOT ship today without the v2-infohash fix; all blockers at the DB boundary |

---

## Phase 4 — dormancy safety (matters in prod today)

- **Master switch** = `Enabled` (env `SEARCH_ENABLED`), `searchfx/config.go:17-20,155-157`.
  When false:
  - The Tantivy client is **not constructed** — `newClient` returns a true nil
    (`module.go:440-443`); even when built, `grpc.NewClient` is lazy (no dial until first
    RPC, `client.go:45-53`).
  - The `SearchRouter` **is** decorated into every consumer unconditionally
    (`appfx/module.go:84`, `searchfx` Decorator `module.go:419-433`), but `routerConfig()`
    forces `ModePostgres` when `!Enabled` (`config.go:250-262`) and `shouldShadow()` returns
    false for that mode (`router.go:120-130`) → pure passthrough. Only always-on cost: one
    `time.Now()`/`time.Since` per `TorrentContent` call.
  - Background pollers (`registerDocCountReporter` `module.go:136-144`,
    `registerPathsearchHealthReporter` `module.go:357-367`) early-return on a nil client —
    **no goroutines** when disabled.
  - **Processor dual-write** per persisted torrent when disabled: a single nil-interface
    check (`persist.go:138-141`; `provideSearchIndexer` returns true nil,
    `processorfx/module.go:27-33`). No constructed-but-idle client, no channel send.
  - Residue vs. code-absent: **7 Prometheus series registered at value 0** (`shadow.New`
    provided unconditionally, `metrics.go:130-160`). Harmless.
- **Env-namespace collisions: structurally impossible.** `SEARCH_ENABLED`,
  `SEARCH_FILE_SEARCH_*`, `SEARCH_PATHSEARCH_*`, `SEARCH_PATH_TYPEAHEAD/COLLAPSE_*` are all
  sibling fields of ONE `searchfx.Config` (`config.go:17-152`). Resolution is pull-based
  exact-match (field → `strcase.ToSnake` → joined path → env map lookup,
  `config.go:141`, `envresolver.go:22-27`): each field builds its own fully-qualified key,
  no prefix matching, unknown envs are never read, no strict-unmarshal path exists.
- **No reachable nil/panic surface when disabled.** `Mode != postgres ⟹ client != nil` is
  an invariant (nil client only when `!Enabled`, which forces `ModePostgres`; an enable-time
  client error fails fx startup loudly).

## Phase 4 — behavior if flipped today (`SEARCH_ENGINE=shadow|tantivy`)

- 🔑 **Observation-only.** `router.go:111-115`: `ModeCanary`/`ModeTantivy` still serve the
  PostgreSQL result; Tantivy-*served* results are a **Phase-6 TODO that was never built**
  (`config.go:18-24`). A flip cannot change served results, bypass the L3 pathsearch route,
  fight the FIND-2 rewrite, or touch the drop-compatible read gates.
- **Interaction with the L3 route:** the gql resolver takes the L3 composer route first
  (`gqlmodel/torrent_content.go:210-229`), independent of the router. The composer's PG
  hydrate calls the router-decorated `search.Search` (`composer.go:518`), so each refine
  chunk passes through the router — but chunk queries carry `q.Where(...)` criteria, which
  the shadow request recorder cannot map (`request_builder.go:31-59`) → `canCompare=false`
  → the shadow is skipped without a Tantivy RPC. Only plain free-text queries on the PG
  fallback path actually compare.
- **Bounding:** comparisons are sampled (`SampleRate`), detached
  (`context.WithoutCancel`), and self-bounded by `ShadowTimeout` (default 5s)
  (`router.go:102-108,142-143`; `config.go:63-69`); serving latency is untouched.
  ⚠️ **The one flip hazard:** shadow goroutines are **unbounded** — `r.run = func(f){ go f() }`
  (`router.go:81`), one per sampled query, amplified per refine chunk on the read pod.
  Any revival must set `SEARCH_SAMPLE_RATE ≪ 1` and/or add a concurrency cap.
- **Document builder is drop-safe:** `BuildDocument` (`document.go:37-83`) reads in-memory
  models only — never a `torrent_files` SELECT; single-file extensions derive from the name
  via `FileExtensionFromPath` (G9 arm, `document.go:266-276`). The dual-write uses the
  original payload (Torrent assoc intact, `persist.go:147`).
- **Misconfig fails loud:** `Enabled=true` + empty address errors at fx startup
  (`client.go:69-72`); a missing listener degrades to logged background RPC failures while
  PG keeps serving; a bogus `Engine` value fails validation (`config.go:26`).

---

## Phase 3 — the Rust sidecar (frozen at `5f5093a9`)

`cargo test --workspace` **passes today** on a current toolchain (~93 tests, 3 live-PG
tests correctly `#[ignore]`d; this era has no DuckDB dependency).

### Verified-true claims

- **Tokenizer parity is real.** `tokenize_flat` (`tokenizer.rs:168-207`) is a rune-by-rune
  port; go-unidecode tables transcoded verbatim (`tokenizer/tables.rs`); the fixture file
  contains exactly **4,223** entries and `tests/tokenizer_parity.rs` runs every one. Hard
  cases handled: `İ`→`i` single-rune lower, Nl/roman numerals excluded, CJK one-token-each
  with case preserved.
- **Backfill avoids this fork's cursor-bug family.** Cursor is the generated text
  composite `tc.id`, bound `::text`, keyset `WHERE tc.id > $1 ORDER BY tc.id ASC`
  (`stream.rs:163-207`) — text-typed both sides. Upsert = delete-by-doc_id + add
  (`indexer.rs:155-161`), `doc_id == Go InferID` (`indexer.rs:194-211`), so resume/re-run
  replaces, never duplicates; multi-classification torrents coexist.
- **The L3 `VOLUME` index-wipe landmine is NOT reproduced.** `Dockerfile.search:40`
  declares `VOLUME` at exactly the documented PVC mountpoint, so a k8s mount overrides it
  cleanly (still worth deleting the line for `docker run` hygiene).
- Single-writer discipline correct (`Arc<Mutex<IndexWriter>>`, per-RPC commits, crash loses
  at most one uncommitted batch, never corrupts).

### Defects found

1. **Match-set divergence — phrase-over-group.** `"a" . (b|c)`: PG distributes adjacency
   (`(a<->b)|(a<->c)`); `build_phrase` (`query.rs:694-707`) falls back to
   `a & (b|c)`, dropping adjacency entirely. Docs matching Rust but not PG exist.
2. **Match-set divergence — prefix cap.** `mayo*` uses `PhrasePrefixQuery` with
   `PREFIX_MAX_EXPANSIONS = 256` (`query.rs:60,771,805`); PG prefix-matches the whole
   lexicon. High-cardinality prefixes silently miss the overflow.
3. **Facet gaps (self-documented):** `file_type` overcounts torrents with 2+ same-type
   extensions; `files_count` range buckets are invented (no Go reference); flat
   `SearchFilters` means no per-facet OR-exclusion.
4. **Health-probe mismatch:** the server's custom `HealthCheck` always returns `SERVING`
   (`server.rs:198-207`) while `Dockerfile.search:44-45` tells operators to use
   `grpc_health_probe`, which speaks the unimplemented standard `grpc.health.v1`.
5. **Index bloat:** all four text tiers use `WithFreqsAndPositions` (`schema.rs:141-145`).
   Positions are only needed where phrase/prefix queries actually target (weight-A text);
   the PS-MB1 finding (~83% of an ngram-ish field's bytes are dead positions) applies to
   `text_b/c/d` — the single biggest lever on the 39–78GB index estimate.
6. Stale docstrings claim the read path is a stub (`main.rs:4,65`) — it is fully
   implemented and tested.

### Bitrot vs. today's schema (the branch predates migrations 00023–00025)

- 🚨 **BLOCKING — v2 info hashes (00023):** `InfoHash` is hard-pinned to 20 bytes
  (`info_hash.rs:29-34,108-110`) and a decode error propagates page-level
  (`stream.rs:229-231` → `backfill.rs:123-125`), **aborting the entire backfill** at the
  first non-20-byte `info_hash` row. Today's corpus is v1-only, but any pure-v2/hybrid
  torrent breaks the run.
- **MEDIUM — no incremental path (00024):** `deleted_torrents` + the
  `torrents_updated_at_info_hash_idx` follow contract postdate the branch; `DeleteDocument`
  exists but nothing drives it, and the backfill is full-scan only. Deletes would never
  propagate.
- **MEDIUM — extension authority drift (G9):** the transform derives `file_extensions`
  from the decoded blob (`transform.rs:61-67`) rather than the authoritative
  `torrents.file_extensions` JSONB.
- **LOW:** `dht seen_count` (00025) unused, no impact; `stream_torrents_with_files` is
  dead code.

---

## Revival checklist (blocking → nice-to-have)

1. ✅ DONE (2026-07-03, commit 28d222ef) — **BLOCKING:** handle v2 info hashes — widen `InfoHash` to 20/32 bytes, or scope the
   backfill to `meta_version = 1` / key on `info_hash_v1`.
2. ✅ DONE (28d222ef) — Make info_hash decode a per-row skip (like bad blobs), not a page-level abort.
3. ✅ DONE (f60206cb; fable review SHIP-WITH-FIX, memory-bound fix applied) — Build the incremental delete/update path on the 00024 follow contract.
4. ✅ DONE (6445106e) — Point the `file_type` facet at the authoritative `file_extensions` JSONB.
5. ✅ DONE (8fc90b84 + recall-preserving lower-tier degradation 609a68a5 per opus review) — Drop `WithFreqsAndPositions` → `WithFreqs` on `text_b/c/d`.
6. ✅ DONE (ac18a8c8; opus review SHIP) — Fix phrase-over-group distribution; document or raise the 256 prefix-expansion cap.
7. ✅ DONE (105918b0) — Implement `grpc.health.v1` (or fix probe guidance); delete the `VOLUME` line; refresh
   the stale stub docstrings.
8. ✅ DONE (9c5800ca) — Cap shadow-comparison concurrency and default `SEARCH_SAMPLE_RATE ≪ 1` before any flip.
9. 🟡 DESIGN DONE (docs/dev/phase6-tantivy-served-design.md); build remains — **Design + build Phase 6** (Tantivy-*served* results) — Phase 4 only observes; a real
   BM25 cutover does not exist yet.
10. ✅ DONE (105918b0, documented on both binaries) — Ops note: the backfill binary and the server cannot share an index dir concurrently
    (writer lock; safe-fail).

**Bottom line:** the engine core (tokenizer, query translation, facets, indexer,
idempotent upsert) is production-quality and Go-faithful; the dormant Phase-4 code is safe
to leave in the production binary indefinitely. The blockers are the predictable cost of
~5 weeks of schema drift, and reviving this is a bounded checklist, not an open-ended
project — to be spent only if corpus-aware text ranking (BM25) ever shows real demand
beyond what the L3 route + popularity-sort default already deliver.
