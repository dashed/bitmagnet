# File-Grained Search Index — Implementation Spec

**Date:** 2026-06-06
**Status:** Spec, ready for implementation. No code changed by this document.
**Feature:** A **second Tantivy index, one document per file**, served by the existing search sidecar, restoring **true per-file search** ("find all `.mkv` files > 1 GB", per-file path FTS) that the hybrid-blob migration removes once `torrent_files` is dropped.
**Branch:** `feat/file-grained-search` (top of the `rust_rewrite` git chain).
**Method:** 5-agent opus team (`rust-spec` / `proto-go` / `ops` / `auditor` + lead synthesis). All mechanics source-verified against the fork, Tantivy v0.26.0, and PostgreSQL 16.
**Precursor:** [`perfile-search-with-blob-design.md`](./perfile-search-with-blob-design.md) (the option analysis; this is the chosen **P2**).

---

## 1. Why this exists (one paragraph)

The blob keeps every file's `{index, path, extension, size}` verbatim, but flattens it: `torrent_file_summary` stores `largest_file_size` (max over **all** types) and `extensions` (a deduped **set**) as **uncorrelated** columns, and the torrent-grained Tantivy doc has one multivalued `file_extensions[]` plus a single torrent-total `size`. Neither can express the per-file **conjunction** "the `.mkv` _itself_ is > 1 GB" — and Tantivy 0.26 has **no nested documents**, proven by `tantivy/src/query/boolean_query/boolean_query.rs:421-447` (a doc with array `[{sneakers,white},{t-shirt,red}]` matches `sneakers AND red`; comment: _"array do not act as nested docs"_). The only structure that pairs `(extension, size)` at file granularity is **one document per file**. Per `quant`'s costing it is also the cheapest interactive option (~8–15 GB, vs a slim PG `torrent_file` table's 68–92 GB which re-bloats exactly what the migration removed).

This is a **forward feature, not a parity fix**: pre-cutover `torrent_files` still answers per-file SQL, so there is no live regression (cf. **G1d** in `docs/bep-compliance-audit.md`).

---

## 2. Locked design decisions

| #   | Decision             | Choice                                                                                                                                                                                                                                                                   |
| --- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| D1  | Service topology     | A **new `FileSearchService`** in the same `search.proto`, served by the **same sidecar process**, backed by a **second index directory**. One process/server struct holds **both** indexes (each with its own writer/reader/mutex).                                      |
| D2  | Granularity          | **One Tantivy doc per file** (forced by the no-nested-docs proof).                                                                                                                                                                                                       |
| D3  | Identity             | `doc_id = hex(info_hash):file_index`. STORED-only (not indexed). The delete key is the separate `info_hash` bytes term.                                                                                                                                                  |
| D4  | Upsert / delete      | **Per-torrent replace**: `delete_term(info_hash)` then add all N file docs in **one commit**. Whole-torrent delete fans out to **both** services (`DeleteDocument` + `DeleteFiles`).                                                                                     |
| D5  | Single-file torrents | Both Go builder and Rust backfill emit **one synthetic doc** `{file_index:0, path:torrent_name, extension:ext-from-name, size:torrent.size}`. **[PARITY]** — mandatory or single-file releases are unsearchable per-file (the G9 analogue).                              |
| D6  | Result shape         | **File-level is primary** (one hit = one file; exact `total_hits`, clean offset/limit, sort). Collapse-to-torrent is an optional, explicitly-approximate mode (Tantivy 0.26 has no native group-by).                                                                     |
| D7  | Denormalization      | Copy down **immutable only**: `content_type` (multivalued) + `published_at`. **Never `seeders`/`leechers`** (scrape-mutable → ~52 doc rewrites per torrent per scrape across 873 M docs). Staleness eliminated by construction.                                          |
| D8  | Storage              | **Store only `doc_id`.** `path` is tokenized+indexed (v1.1) but **never STORED**; `size`/`extension` are FAST+INDEXED, not stored. Display values **hydrated from the blob** by `(info_hash, file_index)`. Keeps the index ≈8–15 GB; storing `path` would add ~17–35 GB. |
| D9  | Phasing              | **v1**: ext + size + denorm facets — answers the literal "find `.mkv` > 1 GB" at **~8–12 GB**. **v1.1**: opt-in tokenized `path` for path FTS (~16–30 GB, smoke-test first). The expensive axis is **path FTS, not the denorm scalars**.                                 |
| D10 | Source of truth      | Indexed **from the 16 GB `files_data` blob**, never the 873 M `torrent_files` rows (future-proof against the deferred `DROP TABLE torrent_files`).                                                                                                                       |
| D11 | Surface              | **GraphQL only** for v1 (new `torrentContent.fileSearch`). Torznab stays torrent-grained (per-file hits don't fit its item model).                                                                                                                                       |
| D12 | Router role          | **Direct serve, not shadow** — there is no exact PG baseline to blend against at serve time (PG only does the approximate torrent-grained query).                                                                                                                        |
| D13 | Feature gate         | New `SEARCH_FILE_INDEX_ENABLED` (default `false`), independent of `SEARCH_ENABLED`.                                                                                                                                                                                      |

---

## 3. Protobuf additions (`bitmagnet-rs/proto/bitmagnet/search.proto`)

A new service in the existing proto; `task gen-search-proto` (see `internal/search/tantivy/gen.go`) regenerates `internal/search/tantivy/pb` for both services — no new tooling.

```proto
// File-grained index (one document PER FILE). Second Tantivy index directory on
// the same sidecar. doc_id = hex(info_hash):file_index.
message FileDocument {
  bytes info_hash = 1;            // 20-byte v1 hash; INDEXED delete key + part of doc_id
  uint32 file_index = 2;          // 0-based; with info_hash forms the only STORED value
  string path = 3;                // tokenized TEXT for path FTS (v1.1); INDEXED, NOT stored
  string extension = 4;           // STRING + FAST + INDEXED; NOT stored (hydrate from blob)
  uint64 size = 5;                // FAST + INDEXED; the (ext,size) pairing; NOT stored
  repeated ContentType torrent_content_type = 6; // immutable denorm, multivalued (per-torrent)
  int64 published_at = 7;         // immutable denorm, FAST (Unix seconds)
  // (no seeders — high-churn; resolved at hydration time. See D7.)
}

message FileSearchFilters {
  repeated string extensions = 1;
  optional uint64 size_min = 2;
  optional uint64 size_max = 3;
  repeated ContentType content_types = 4;   // denorm
  optional int64 published_after = 5;       // denorm
}

message FileSearchRequest { string query = 1; FileSearchFilters filters = 2; Pagination pagination = 3; repeated SortBy sort = 4; }
message FileSearchHit { FileDocument document = 1; float score = 2; }   // document carries only info_hash+file_index populated
message FileSearchResponse { repeated FileSearchHit hits = 1; uint64 total_hits = 2; } // total = matching FILES (D6)

// Per-torrent replace envelope: carries info_hash + the whole fileset so the
// sidecar does delete_term(info_hash) then add(files...) atomically (and an
// empty files list expresses a pure delete).
message IndexFilesRequest { bytes info_hash = 1; repeated FileDocument files = 2; }
message IndexFilesResponse { bool ok = 1; }
message DeleteFilesRequest { bytes info_hash = 1; }
message DeleteFilesResponse { bool ok = 1; }
message BatchIndexFilesResponse { uint64 indexed_torrents = 1; uint64 indexed_files = 2; uint64 error_count = 3; }

// Phase-2: file facets (extension distribution, size buckets). Reuses Facet/FacetBucket.
message GetFileFacetsRequest { string query = 1; FileSearchFilters filters = 2; repeated string facet_fields = 3; }
message GetFileFacetsResponse { repeated Facet facets = 1; }

service FileSearchService {
  rpc IndexFiles(IndexFilesRequest) returns (IndexFilesResponse);
  rpc BatchIndexFiles(stream IndexFilesRequest) returns (BatchIndexFilesResponse);
  rpc DeleteFiles(DeleteFilesRequest) returns (DeleteFilesResponse);
  rpc SearchFiles(FileSearchRequest) returns (FileSearchResponse);
  rpc GetFileFacets(GetFileFacetsRequest) returns (GetFileFacetsResponse);   // phase-2
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);          // own file_doc_count
}
```

---

## 4. Rust sidecar (`bitmagnet-rs/crates/bitmagnet-search`)

Tantivy mechanics verified in the local v0.26.0 checkout (cited inline).

### 4.1 `file_schema.rs` (NEW)

Mirror `schema.rs` conventions (`keyword_facet = (STRING|STORED).set_fast(None)` at `schema.rs:149`; `numeric = (STORED|INDEXED|FAST)` at `schema.rs:152`; bytes via `BytesOptions::default().set_indexed().set_stored()`). Provide `build_file_schema()`, `FileFields::from_schema()`, and `FILE_FIELD_NAMES` (drives the completeness test).

| field          | flags                                                         | notes                                                                             |
| -------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `doc_id`       | `STRING.set_stored()` **stored-only, not indexed**            | hit id; nothing queries it (delete is by info_hash) → no 873 M-entry term dict    |
| `info_hash`    | bytes `INDEXED` (not stored)                                  | delete/upsert key; value recoverable from `doc_id`                                |
| `extension`    | `STRING\|STORED?\|FAST` (not stored)                          | exact filter + facet. (Stored omitted — hydrate from blob.)                       |
| `size`         | `u64 INDEXED\|FAST` (not stored)                              | per-file size; FAST → `FastFieldRangeWeight` (`range_query.rs:102-117`)           |
| `path`         | `TEXT`(TOKENIZER_NAME, WithFreqsAndPositions), **not stored** | **v1.1 only**; per-file path FTS                                                  |
| `content_type` | `STRING\|FAST`, **multivalued**                               | immutable denorm; multivalued so a multi-classification torrent stays one doc-set |
| `published_at` | `i64 INDEXED\|FAST`                                           | immutable denorm                                                                  |

### 4.2 Second-index lifecycle

- Tantivy enforces **one writer per directory** via a lockfile (`index.rs:539-558`; `writer()` `:613`) and **exact schema match** on `open_or_create` (`index.rs:218-231`, recheck `:426`). ⇒ the file index **must** be a separate `Index` in its **own directory** with its **own writer**.
- Reuse `index.rs` (`open_or_create`/`reader`/`writer`/`register_tokenizer`) unchanged for the file index at a second path. `register_tokenizer` **must** run on it too (path uses TOKENIZER_NAME).
- The server struct holds **both** index handle sets; expose **two gRPC services** off the one process (D1). `main.rs`: add `--file-index-path` (env `BITMAGNET_SEARCH_FILE_INDEX`, default sibling `…/search-files`); open both on startup. **Failure isolation (auditor #10):** open the file index independently and **degrade to "file search unavailable"** rather than failing the whole process; size the second writer heap explicitly (two 256 MiB heaps = 512 MiB).

### 4.3 `file_indexer.rs` (NEW)

- `file_documents(fields, info_hash, &[BlobFile], denorm) -> Vec<TantivyDocument>` — one doc per `BlobFile` (`bitmagnet-model::BlobFile`). Skip empty-extension _term_ (like `indexer.rs:129-133`) but always index `size` (+`path` in v1.1).
- `upsert_files(writer, fields, info_hash, files, denorm)`: `delete_term(Term::from_field_bytes(info_hash))` then `add_document` per file. **delete-then-add in one commit is correct** — `delete_term` only affects docs from previous commits / earlier in the same commit (`index_writer.rs:672-680`). Keyed on **info_hash** (the fileset is a torrent property), not per-file doc_id.
- `delete_files(writer, fields, info_hash)` = `delete_term(info_hash)`.
- **Single-file synthesis (D5):** when the blob is empty and `files_status == single`, emit one doc `{file_index:0, path:name, extension:file_extension_from_path(name), size:torrent.size}` — matching `transform.rs:76-88` and the Go builder.

### 4.4 `bin/backfill_files.rs` (NEW)

Mirror `backfill.rs` (keyset loop, commit interval, `--after-*` resume, blob-error skip). **Source = `torrents` per-torrent** (reuse `stream_torrents_with_files`, `db/stream.rs`), decode `files_data` via `deserialize_files` (`blob.rs:60`), emit N docs/torrent. **Commit cadence counts file docs, not torrents** (a 50k-file torrent = 50k docs). Per-classification sourcing is **rejected** (a multi-classification torrent's identical blob would be indexed N times). For denorm: a `content_types[] = DISTINCT(content_type)` + `published_at = epoch(coalesce(min(published_at), created_at))` aggregate per torrent.

### 4.5 `server.rs` handlers

`index_files` (lock file writer → `upsert_files` → commit → reload); `batch_index_files` (**chunk commits + release/re-acquire the writer mutex periodically — do NOT hold it for the whole stream**, auditor #8); `delete_files`; `search_files` (delegates to a new `file_query.rs` read path); `health_check` reports a **separate** `file_doc_count`.

---

## 5. Go surface (`internal/search`, `internal/processor`, GraphQL)

### 5.1 `tantivy/document.go` — `BuildFileDocuments` + `FileDocID`

Go twin of `backfill_files.rs` (byte-parity discipline like `BuildDocument`). `extension` comes from the **stored blob value**, not re-derived from path (parity with `fileExtensions()`). `FileDocID = hex(info_hash) + ":" + file_index`. Emits the **synthetic single-file doc** (D5). **[PARITY]** the `content_type` copy-down rule must match the Rust backfill (use the torrent's distinct content types; `UNKNOWN`/empty if unclassified).

### 5.2 `tantivy/client.go` — `FileClient`

Thin wrapper over the generated `FileSearchServiceClient` (`IndexFiles`/`DeleteFiles`/`SearchFiles`/`BatchIndexFiles`/`HealthCheck`), sharing the torrent client's `*grpc.ClientConn` (`NewFileClientOnConn(conn, cfg)`), built only when `Enabled && FileIndexEnabled`.

### 5.3 Dual-write hook (`internal/processor`)

Extend the `SearchIndexer` interface with `IndexFiles`/`DeleteFiles` (nil-safe → no-op when the file index is disabled). The file write rides the **existing post-commit, fire-and-forget** path in `persist.go::indexBatch`, **deduped by info_hash** (the data is already preloaded by `Process()` + `AfterFind` → zero extra DB reads).

**Write-amplification guard (auditor #5 + proto-go reconciliation):** file docs depend only on (the fileset) and (the content_type set) — both rare. To avoid rewriting a torrent's whole fileset on every re-classification, **gate `IndexFiles` on a change check** (file-set or content-type set changed since last index) rather than firing unconditionally per classification. The periodic **file-index reconciler** (§7) covers any gaps. Alternative considered: fire at the crawl/blob-persist path (`dhtcrawler/persist.go`) where the fileset is written — most correct for fileset freshness but it lacks `content_type` (would need a reconciler to fill the denorm); deferred in favor of the guarded processor-path write.

Whole-torrent delete fans out to **both** services: `DeleteDocument(info_hash)` + `DeleteFiles(info_hash)` (auditor #6).

### 5.4 Router / searchfx

File search is **direct serve** (D12) via a small `filesearch.Service` consumed by the GraphQL resolver (not on the `Router` type — it has no `search.Search` method to override). Maps `FileSearchParams → pb.FileSearchRequest` with a direct builder (no `OptionBuilder` replay). Sort allowlist = `size`, `published_at` (no `seeders`). New knob `SEARCH_FILE_INDEX_ENABLED` (default false). A second Prometheus gauge `bitmagnet_search_tantivy_file_doc_count` from the file index's `HealthCheck`.

### 5.5 GraphQL (`graphql/schema`, `internal/gql`)

New `torrentContent.fileSearch(input: TorrentFileSearchQueryInput!): TorrentFileSearchResult!`:

```graphql
input TorrentFileSearchQueryInput {
  queryString: String # path FTS (v1.1)
  limit: Int
  page: Int
  offset: Int
  totalCount: Boolean
  hasNextPage: Boolean
  extensions: [String!]
  sizeMin: Int
  sizeMax: Int # the exact per-file (ext,size) conjunction
  contentTypes: [ContentType!]
  publishedAfter: DateTime
  collapse: Boolean # default false (file-level primary); true = group-by-torrent (approx)
  orderBy: [TorrentFileSearchOrderByInput!] # relevance | size | publishedAt  (no seeders)
}
type TorrentFileSearchResult {
  totalCount: Int!
  totalCountIsEstimate: Boolean!
  hasNextPage: Boolean
  items: [TorrentFileSearchItem!]!
}
type TorrentFileSearchItem {
  torrent: Torrent!
  matchedFiles: [TorrentFile!]!
  matchCount: Int!
  score: Float!
}
```

**Resolver flow:** map input → `SearchFiles` → hits carry only `(info_hash, file_index, score)`; collect distinct info_hashes; **one** PG round-trip hydrates the `Torrent`s (whose `AfterFind` already decoded the blob); resolve `matchedFiles` from `torrent.Files[file_index]` (the D8 hydrate-from-blob step); **single-file** items re-synthesize the `{name, ext, size}` triple (no blob to read). `collapse=false`: one item/hit, `totalCount = total_hits` exact. `collapse=true`: group by info_hash in-page, `totalCount` = approximate distinct-torrent cardinality, `totalCountIsEstimate=true`, with a documented page-straddle caveat. **No PG fallback** when disabled → typed "file search not enabled" error.

---

## 6. Sizing, layout, RAM

| Component (873 M docs)                          | ≈                         | Notes                               |
| ----------------------------------------------- | ------------------------- | ----------------------------------- |
| `size` u64 FAST (bitpacked)                     | 4.4 GB                    |                                     |
| `info_hash` postings (delete key)               | 1–2 GB                    | shared across a torrent's ~52 files |
| `extension` postings + FAST                     | 1–2 GB                    | ~100 terms                          |
| `content_type[]` + `published_at` FAST (denorm) | 2–4 GB                    | immutable                           |
| `doc_id` stored-only                            | 1–2 GB                    |                                     |
| `path` term dict + postings (**v1.1 only**)     | +2–4 GB est. (smoke-test) | the only "big text" axis            |
| `path`/`size`/`ext` doc store                   | **0**                     | hydrated from blob                  |

**v1 ≈ 8–12 GB; v1.1 ≈ 16–30 GB (uncertain — measure via a `backfill_limit=100000` smoke pass).** Both ≪ the existing **200 Gi HEL1 PVC**. Layout: a second directory `/index/files/` alongside `/index/torrents/`. RAM ≈ 2–4 GB (mmap; hot FAST fields), co-resident on the 125 GB HEL1 box. No new PVC, no RAM blocker.

---

## 7. Backfill & operations

- **Separate Job** (mirror the torrent backfill's scale-Deployment-to-0 + C1/C2/C3 orchestration; `backoffLimit:0`, `restartPolicy:Never`, cursor log, `--after-id` resume). Single-writer: the backfill Job and the live `IndexFiles` writer both hold `/index/files/` → run the bootstrap backfill with the serving Deployment scaled to 0 (or before file dual-write is enabled).
- **Source = blob**, ~1/17th the I/O of reading rows and future-proof past cutover.
- **Pre-cutover coverage gap (auditor #9):** `files_status=multi` rows where `files_data` is still NULL → either fall back to `torrent_files` during backfill, or **gate file-index rollout on verified 100% blob backfill** (deploy plan reports 16,976,700 summary rows — confirm `files_data` parity).
- **`over_threshold` torrents** (`save_files_threshold`, blob absent/truncated) = a documented known gap.
- **Reconciler:** the file index gets its **own periodic backfill reconciler** (like the torrent index) — never assume cross-index consistency at read time (auditor #7).

---

## 8. Validation, write-amp, failure modes, schema versioning

- **Shadow/parity (pre-cutover only):** `torrent_files` is an **exact** per-file ground truth. Assert **set equality** of `(info_hash, file_index)` between a PG query (`WHERE extension=$e AND size>$s`, ∪ single-file synthesis) and `SearchFiles` (precision=recall=1.0) — stronger than the torrent index's Jaccard/RBO. **Gate on D1** (`DROP TABLE torrent_files`); after D1 only index-vs-blob self-consistency remains. Exactness depends on D5 (single-file synthesis) holding, else Tantivy systematically under-returns and every "diff" is false (auditor #12).
- **Write-amp:** +1 sink — one `IndexFiles`/`BatchIndexFiles` per ingest/re-classification (guarded), post-commit, fire-and-forget, no-op when disabled, never fails crawling. Once per ingest, **not per scrape** (the payoff of D7).
- **Failure modes:** sidecar down → pre-cutover fall back to exact PG; post-cutover 503/"unavailable" (optionally a labeled-approximate `torrent_file_summary` degrade). Backfill resume = supervised (`--after-id`), idempotent upsert.
- **Schema versioning (auditor #11):** treat the file index as a **disposable, rebuildable cache** with a schema-version marker; on mismatch **wipe + re-backfill from the blob** (hours, zero PG impact) — never crash-loop. Important because BEP-52 v2 per-file merkle roots will later add a field.

---

## 9. Auditor punch-list — resolution status

| #   | Risk                                   | Resolution                                                                                              |
| --- | -------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 1   | Single-file coverage gap               | **D5** synthetic doc, both sides (mandatory). `over_threshold` = documented gap.                        |
| 2   | File vs torrent total_hits collapse    | **D6** file-level primary (exact); collapse = approximate, caveated.                                    |
| 3   | Volatile denorm (seeders)              | **D7** immutable-only; seeders via hydration.                                                           |
| 4   | Delete/upsert atomicity                | **D4** per-torrent envelope = delete_term + add-all + one commit.                                       |
| 5   | Write-amp on re-classify               | §5.3 change-guard on the processor-path write.                                                          |
| 6   | Whole-torrent delete fan-out           | §5.3 `DeleteDocument` + `DeleteFiles`.                                                                  |
| 7   | Cross-index consistency                | §7 own reconciler; no read-time consistency assumption.                                                 |
| 8   | Single-writer / mutex-for-whole-stream | §4.5 chunked commits, release/re-acquire; bootstrap Job scaled-to-0.                                    |
| 9   | Pre-cutover blob NULL coverage         | §7 fall back to rows or gate on 100% blob parity.                                                       |
| 10  | Two-index health / failure isolation   | §4.2 independent open, degrade not crash; separate `file_doc_count`.                                    |
| 11  | 2nd-index schema versioning            | §8 disposable cache + version marker + wipe/rebuild.                                                    |
| 12  | Shadow parity vs PG                    | §8 set-equality, valid only once D5 holds; needs a file-query builder.                                  |
| 13  | doc_id key across hybrid/v2            | Key on the **20-byte `info_hash`** the blob/torrents table uses (v1/truncated), **not** `info_hash_v2`. |
| 14  | path STORED sizing                     | **D8** never stored; empty-extension files skip the ext term (minor).                                   |

---

## 10. [PARITY] cross-boundary contracts (must match Go ↔ Rust)

1. `doc_id` = `hex(info_hash):file_index` exactly.
2. A `FileDocument` built by Go from a torrent's blob is field-identical to one `backfill_files.rs` builds from the same blob (extension from stored blob value; single-file synthesis D5; the content_type copy-down rule).
3. Round-trip: `IndexFiles` a torrent then `SearchFiles(ext=X, size_min=N)` returns exactly the satisfying files. Add a cross-system parity test (mirror the existing torrent `DocID`/`BuildDocument` parity test).

---

## 11. Phased rollout

1. **Spec** (this doc) — done.
2. **Proto + regen** `pb` (both services).
3. **Rust v1**: `file_schema` (no path) + `file_indexer` + `backfill_files` + `server` handlers + 2nd-index wiring in `main`.
4. **Go v1**: `BuildFileDocuments` + `FileClient` + guarded dual-write + `filesearch.Service` + GraphQL `fileSearch` + searchfx gate.
5. **Backfill Job** + pre-cutover **set-equality parity gate**.
6. **v1.1** (opt-in): add tokenized `path` + path-FTS query, after smoke-sizing.
7. **Cross-ref G1d**: file index carries per-file v2 merkle identity when G1d lands.

Each step is additive, reversible, and disabled by default (`SEARCH_FILE_INDEX_ENABLED=false`).

---

## 12. Open questions for product/ops

- v1.1 path FTS: confirm demand before paying the ~2–4 GB (uncertain) text cost; decide after the smoke pass.
- `content_type` denorm vs post-hydration content filtering: denorm chosen (correct sidecar-side pagination); confirm the `UNKNOWN`-when-unclassified rule is acceptable.
- Collapse-to-torrent default: spec sets `collapse=false` (file-level) as primary; confirm the UI wants file rows, not torrent rows, by default.

---

## 13. Source references

- **No-nested-docs proof / single-writer / schema-match / fast-field range:** `tantivy/src/query/boolean_query/boolean_query.rs:421-447`, `tantivy/src/index/index.rs:218-231,539-558,613`, `tantivy/src/indexer/index_writer.rs:672-680`, `tantivy/src/query/range_query/range_query.rs:102-117`, `tantivy/columnar/src/lib.rs:82`.
- **Existing sidecar surfaces:** `bitmagnet-rs/crates/bitmagnet-search/src/{schema.rs,index.rs,indexer.rs,server.rs,transform.rs,bin/backfill.rs}`, `proto/bitmagnet/search.proto`.
- **Go surfaces:** `internal/search/tantivy/{document.go,client.go,gen.go}`, `internal/search/router/*`, `internal/search/searchfx/*`, `internal/processor/persist.go`.
- **Blob = source + hydration:** `bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:31-46,60`, `internal/model/torrents.go:19-24` (`AfterFind`).
- **Numbers / deploy reality:** `docs/space-savings-verification.md`, `docs/dev/perfile-search-with-blob-design.md`, homelab-infra `docs/bitmagnet-fork-deploy-plan.md`, `docs/bitmagnet-tantivy-phase3-deploy-plan.md`.
