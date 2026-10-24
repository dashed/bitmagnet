# Retaining Per-File Search With the Hybrid Blob

**Date:** 2026-06-06
**Status:** Investigation complete (read-only); no code changes. Design options + recommendation.
**Method:** 4-agent opus team (`inventory` / `designspace` / `quant` / `integrate`) + lead synthesis. All claims source-verified against the fork, Tantivy v0.26.0, and PostgreSQL 16.
**Question:** _With the hybrid blob we lose things like per-file search ("find all .mkv files > 1 GB"). Can we retain that ability while keeping the space savings? Is it possible?_

> **Spec (2026-06-06):** the recommended option **P2 (file-grained Tantivy index, 1 doc/file)** is now specced for implementation in [`file-grained-search-spec.md`](./file-grained-search-spec.md) — proto/Rust/Go/GraphQL surface, identity & denormalization model, sizing (v1 ≈ 8–12 GB), backfill/ops, pre-cutover set-equality validation, and the adversarial punch-list resolution.

---

## TL;DR

**Yes — and cheaply — because no per-file data is ever lost.** The hybrid blob keeps every file's `{index, path, extension, size}` verbatim (`internal/blobmigration/serializer.go:21-25`, `bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:31-46`). What the migration removes is _queryability at scale_, not data. And it removes even that only **after the deferred `DROP TABLE torrent_files`** — which has not run. So this is a **forward feature**, not a live regression.

The genuine difficulty is one structural fact: **"find all `.mkv` files > 1 GB" pairs an extension with a size at single-file granularity**, and neither of the surviving stores can express that conjunction:

- `torrent_file_summary` keeps `largest_file_size` (max over **all** types) and `extensions` (a deduped **set**) as **separate, uncorrelated** columns — it can say "this torrent has an mkv AND has some file > 1 GB," never "the mkv _is_ > 1 GB."
- The Tantivy torrent doc is **one doc per torrent** with multivalued `file_extensions[]` and a single torrent-total `size` — and Tantivy has **no nested documents**, so multivalued bags can't be pair-correlated.

Restoring the true per-file conjunction therefore means putting a thin **file-grained** query layer over the data the blob already retains. The cheapest options cost **0–15 GB** (versus the 273 GB table the migration eliminated); the "obvious" SQL option is the one trap.

---

## The discriminator: two incompatible meanings

| Reading                           | Meaning                                                            | Requires                                                  |
| --------------------------------- | ------------------------------------------------------------------ | --------------------------------------------------------- |
| **Torrent-grained** (approximate) | torrent _contains_ a `.mkv` AND its largest file (any type) > 1 GB | data that exists today                                    |
| **True per-file** (exact)         | the `.mkv` _itself_ is > 1 GB                                      | a row/doc **pairing** (ext, size) at one-file granularity |

The torrent-grained reading has **zero false negatives** for monotone size predicates (a torrent whose largest file < 1 GB cannot contain a > 1 GB mkv) but real **false positives** (a 5 GB `.iso` next to a 100 MB `.mkv` matches). That property is what makes it a perfect _candidate generator_ for an exact refine pass (see Option 1+refine).

---

## Two load-bearing facts (source-verified)

**1. No data is lost; only queryability, and only post-cutover.**

- Blob retains per-file `{i:index, p:path, e:extension, s:size}` — `serializer.go:21-25`, `blob.rs:31-46`. Wire = `zstd(msgpack[...])`, ~16 GB total.
- The blob is an **opaque `BYTEA`** (`torrents.gen.go:26`, `json:"-"`), decoded only in Go app memory by `AfterFind` → `t.Files` (`model/torrents.go:19-24`). Useful for displaying one fetched torrent; **not** a fleet-wide SQL predicate.
- `torrent_files` (the 273 GB / 873 M-row table) is **still fully populated** via dual-write and **not dropped** — the only `DROP TABLE torrent_files` lives in the deferred cutover (homelab-infra `bitmagnet-fork-deploy-plan.md:104,270`). **Per-file SQL still works today.** The capability gap materializes only after that explicit, gated drop.

**2. Why true per-file breaks (after cutover) — two structural flattenings.**

- `torrent_file_summary` builder writes `extensions` (set) and `largest_file_size` (max over all types) as separate columns (`serializer.go:105-118`). No column ties "largest mkv = X."
- Tantivy doc is one-per-torrent; `file_extensions` is a deduped `BTreeSet` keyword facet (`transform.rs:62-68`, `schema.rs:191`), `size` is a single u64 = torrent total (`transform.rs:111`, `schema.rs:197`); the blob's per-file size/index is **explicitly not indexed** (`transform.rs:46-47`).
- **Tantivy 0.26 has no nested/child docs** (proof below), so you cannot pair two multivalued fields within a torrent doc.

### Proof: Tantivy cannot pair (ext, size) in one document

Verified in the local Tantivy v0.26.0 checkout (`/Users/me/aaa/github/tantivy`):

- `src/query/` enumerates **every** query type (all/boolean/boost/const_score/dismax/exist/fuzzy/more_like_this/phrase/phrase_prefix/query_parser/range/regex/set/term/union). **No nested / block-join / parent-child query exists.**
- The executable test `src/query/boolean_query/boolean_query.rs:421-447` indexes one doc with an array `[{sneakers,white},{t-shirt,red},{cd,blues}]` and asserts that `product_type:sneakers AND color:red` **also** matches — with the comment (≈`:440`) _"Unexpected match, due to the fact that array do not act as nested docs."_

That is precisely the `(file_extensions[], file_sizes[])` problem: a torrent `exts=[mkv,srt], sizes=[8.9 GB, 50 KB]` would match `ext:mkv AND size<1MB`. ⇒ **True per-file requires a separate one-doc-per-file index** (multivalued `u64` fast fields exist — `columnar/src/lib.rs:82` `Cardinality::{Full,Optional,Multivalued}` — but they don't solve cross-field positional correlation; the fix is granularity, not multivalue).

---

## Option matrix

N = 873 M files; 16.86 M torrents-with-files. Post-cutover BASE ≈ **121 GB**; budget ceiling **200 GB**; the re-bloat trap = the old 273 GB `torrent_files`. Storage figures from `quant`, grounded in PG16 (`htup_details.h`/`itup.h`) and Tantivy v0.26 (`columnar/`, `ARCHITECTURE.md`).

| #   | Option                                                                                     | True per-file?                      | Marginal disk                   | RAM                     | Latency | Post-cutover total           | Verdict                                    |
| --- | ------------------------------------------------------------------------------------------ | ----------------------------------- | ------------------------------- | ----------------------- | ------- | ---------------------------- | ------------------------------------------ |
| 1   | **PG summary — `torrent_file_summary` (BUILT, 16.97M rows) + `file_extensions` JSONB GIN** | ❌ torrent-grained                  | +0 (paid)                       | shared                  | <50 ms  | 121 GB                       | Exists today; **candidate generator only** |
| 1+  | **Option 1 prefilter → blob-decompress refine**                                            | ✅ exact                            | +0                              | shared                  | low     | 121 GB                       | **Cheapest EXACT path; no new store**      |
| 2   | Slim normalized PG per-file table (`info_hash, ext, size` + 2 btrees)                      | ✅                                  | **68–92 GB**                    | shared (cache pressure) | <50 ms  | 189–213 GB                   | ❌ **REJECT — re-bloats, breaches budget** |
| 3   | **File-grained Tantivy index (1 doc/file)**                                                | ✅✅ (+ true per-file **path** FTS) | **8–15 GB**                     | 2–4 GB                  | <50 ms  | 129–136 GB                   | ✅ **Best interactive true per-file**      |
| 4a  | DuckDB scanning existing blobs                                                             | ✅ exact (analytical)               | **+0 GB**                       | 2–8 GB                  | 1–10 s  | 121 GB                       | ✅ **Cheapest; zero disk**                 |
| 4b  | DuckDB/Parquet side table (`info_hash, idx, ext, size`)                                    | ✅ exact (analytical)               | 3–5 GB                          | 2–8 GB                  | 1–10 s  | 124–126 GB                   | ✅ fast analytical, tiny                   |
| 5   | Manticore columnar, 1 doc/file (+ optional path FTS)                                       | ✅✅                                | 10–18 GB (50–80 GB w/ path FTS) | 2–4 GB                  | <50 ms  | 131–139 GB (171–201 w/ path) | Redundant 2nd engine vs #3                 |

### Why the numbers fall this way

- **Slim PG table is the trap.** Heap row = 24 B header + 8 B size + 4 B id + ~5 B ext → MAXALIGN(41)=48 B + 4 B line pointer = **52 B × 873 M ≈ 45 GB heap**; btree(ext,size) ≈ 27 GB; btree(id) ≈ 19 GB → **~92 GB** (≈68 GB floor with a dictionary-encoded ext + one covering index). PG pays a fixed ~28 B tuple+line-pointer tax on every one of 873 M rows, _three times_. This rebuilds exactly the cost the blob migration removed.
- **File-grained Tantivy is ~⅙ the cost despite 19× the docs.** size u64 fast (~40-bit values) ≈ 4–5 B/doc → 4.4 GB; torrent*id monotonic → Linear codec ~1–2 B/doc; extension postings (~100 terms, delta+bitpacked) ≈ 1–1.3 GB; doc store = 0 (store nothing); full-cardinality column index is **free**. Total ≈ **8–15 GB**. There is no tokenized text — and tokenized text (the 17 GB tsvector) is what drove the 39–78 GB \_content* FTS figure. (Adding per-file **path** FTS re-introduces big text and is the only expensive Tantivy variant.)
- **DuckDB-on-blobs adds literally nothing** — it decodes the 16 GB blob you already store and runs exact SQL; the only cost is 1–10 s analytical latency.

### Three families

- **(A) Approximate, interactive, $0:** Option 1 alone — torrent-grained, ships today.
- **(B) Exact via retained data + compute, ≈0 persistent storage:** Option 1+refine, DuckDB (4a/4b). Best exactness-per-byte; not per-keystroke.
- **(C) Exact persistent per-file index, interactive + realtime:** file-grained Tantivy (#3), Manticore (#5), slim PG (#2, rejected). Only #3/#5 also restore true per-file **path** FTS; #3 dominates #5 because the project already committed to the Rust Tantivy sidecar.

---

## Recommendation — phased, minimal-effort first

The sidecar is **not yet deployed** (Phase 3 planned, Phase 4 shadow after), `torrent_files` is **not yet dropped**, and disk is **not a constraint** (~1.1 TB free). So every phase below is additive and reversible, and can be added before or after cutover.

**P0 — Expose what already exists (do now; near-zero effort; APPROXIMATE).**
Add a GraphQL filter that maps to `file_extensions @> '["mkv"]' AND torrent_file_summary.largest_file_size > 1e9`, reusing the already-shipped GIN indexes. Touches GraphQL filter input + resolver/criteria only (`internal/database/search/*`, `internal/database/query/criteria_*.go`) — no storage, no migration. ⚠️ Approximate by the nested-doc proof; label it as such in the UI. Optional hardening: wire `BuildFileSummary` into the live `persist.go` path so the summary is exact for _new_ crawls too (today it is written only by the migration queue, so it can go stale for fresh torrents).

**P1 — DuckDB-on-blobs for EXACT analytical per-file (~0 added storage).**
A small exporter decodes `files_data` (reuse `blob.rs` `deserialize_files`) to Parquet/NDJSON `(info_hash, idx, path, ext, size)`; DuckDB answers exact `WHERE ext='mkv' AND size>1e9`, GROUP BY, percentiles. Best for CLI/analyst/verification. New code only (a `bin/blob_export.rs` or Go cmd); no schema change.

**P2 — Second file-grained Tantivy index on the SAME sidecar (the real interactive feature).**
One doc **per file** (required by the proof). Concrete surface:

- `proto/bitmagnet/search.proto`: `FileDocument {bytes info_hash; uint32 file_index; string path; string extension; uint64 size; ContentType torrent_content_type; int64 published_at}`, `FileSearchFilters {repeated string extensions; optional uint64 size_min; optional uint64 size_max; …}`, RPCs `SearchFiles` / `IndexFiles` / `BatchIndexFiles`.
- new `file_schema.rs`: `doc_id = hex:idx` (STRING|STORED), `info_hash` bytes (delete key), `path` TEXT(tokenizer)+STORED, `extension` STRING|STORED|FAST, `size` u64 STORED|INDEXED|FAST, plus copied-down torrent facets (content_type, published_at) for joined filtering.
- new `file_indexer.rs`: one doc per `BlobFile`; upsert = delete-by-`info_hash` then re-add all the torrent's file docs.
- new `bin/backfill_files.rs`: page torrents, decode blob, emit one doc/file — **source is the 16 GB blob, not the 273 GB rows**.
- Go: `internal/search/tantivy/BuildFileDocuments` + client `SearchFiles`; `router` file-query path; `searchfx` wiring. Same sidecar process, second index directory.

**P-alt — slim PG `torrent_file(info_hash, idx, path, ext, size)` (COSTLIEST; fallback only).**
Exact + interactive in SQL, but re-creates 68–92 GB and adds hot-path write amplification — the exact cost the blob migration removed. Only if a no-sidecar constraint is mandatory.

### Decision guide

- Want it **today, free, approximate** → **P0**.
- Want **exact** answers for analysis/CLI at **0 storage**, seconds-latency → **P1 (DuckDB-on-blobs)**.
- Want **interactive (<50 ms) true per-file** filtering + path FTS at scale → **P2 (file-grained Tantivy, +8–15 GB)**.
- Avoid **P-alt / slim PG table** unless a sidecar is forbidden.

---

## Framing & cross-references

- This is a **forward feature, not a parity fix**: pre-cutover `torrent_files` still answers per-file SQL, so there is no live regression (cross-ref **G1d** in `docs/bep-compliance-audit.md`, which was likewise reclassified feature-not-fix).
- P2's one-doc-per-file granularity is the natural home for **BEP-52 / v2 per-file identity** (v2 makes files first-class with per-file merkle roots) — a future convergence point with the v2 work.
- Only **P1** and **P2** deliver exact `(ext, size)` pairing; **P0** is deliberately approximate per the nested-doc proof.

---

## Appendix — key source references

- **Blob / data retained:** `internal/blobmigration/serializer.go:21-25,105-118` · `bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:31-46` · `internal/model/torrents.go:19-24` (`AfterFind`) · `internal/model/torrents.gen.go:26`.
- **As-built schema:** `migrations/00021_blob_storage.sql:4-18` (`files_data`, `file_extensions` JSONB, `torrent_file_summary`) · `00022_blob_indexes.sql:4-5` (jsonb_path_ops GIN) · `internal/dhtcrawler/persist.go:121` (live `file_extensions` write).
- **PG query surface:** `internal/database/search/criteria_torrent_file_extension.go:14-35` · `facet_torrent_file_type.go` · `search_torrent_files.go:17-29`.
- **Tantivy torrent doc (torrent-grained):** `bitmagnet-rs/crates/bitmagnet-search/src/schema.rs:191,197` · `transform.rs:46-47,62-68,111`.
- **Tantivy no-nested-docs proof:** `tantivy/src/query/boolean_query/boolean_query.rs:421-447` (+ comment ≈`:440`) · `tantivy/src/query/` (query inventory) · `tantivy/columnar/src/lib.rs:82` (`Cardinality`) · `tantivy/ARCHITECTURE.md` (fast-field/columnar sizing).
- **PG sizing:** `postgres/src/include/access/htup_details.h` (23 B→24 B header) · `itup.h` (8 B IndexTuple) · `itemid.h` (4 B line pointer).
- **Deploy reality:** homelab-infra `docs/bitmagnet-fork-deploy-plan.md` (summary backfilled 16,976,700 rows; cutover deferred; ~1.1 TB free) · `bitmagnet-tantivy-phase3-deploy-plan.md`.
