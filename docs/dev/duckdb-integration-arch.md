# ARCH-B — DuckDB-on-Parquet ↔ bitmagnet Integration (production design)

**Date:** 2026-06-07 · **Owner:** `index-bench` · **Status:** DESIGN ONLY (no code changed).
**Decision context:** GATE #12 chose **DuckDB-on-Parquet** over the 873M-doc Tantivy file index (RUN-4: index +14–25 GB, scan-bound ~1.3 s; DuckDB +3.86 GB, 35 ms–1.3 s — `docs/dev/file-index-bench-RESULTS.md`, `file-grained-search-benchmark-results.md`). This doc designs the production runtime, resolver wiring, and SQL generator.

All claims grounded in the Go repo (`feat/file-grained-search`), the bench artifacts, and the DuckDB C++ source (`/Users/me/aaa/github/duckdb`).

---

## 1. Runtime decision — DuckDB **sidecar** (extend the existing search sidecar)

| Option | Verdict | Why |
| --- | --- | --- |
| **(b) Sidecar gRPC, DuckDB embedded in the sidecar (Rust `duckdb-rs`)** | ✅ **RECOMMENDED** | Keeps bitmagnet **pure-Go**; crash/memory isolation; **reuses the already-built scaffolding** |
| (a) Embedded `go-duckdb` (CGO) in the main app | fallback | Simplest topology / no hop, but forces **CGO on every build** for an optional feature |
| (c) chdb / DuckDB CLI subprocess | ❌ reject | process-per-query, stdout parsing, no safe bound params |

**Why sidecar, concretely:**
1. **Preserves pure-Go.** bitmagnet ships `CGO_ENABLED=0` static, multi-arch. `go-duckdb` needs CGO + `libduckdb` (~40–80 MB) per arch. The project already **deliberately chose a Rust gRPC sidecar for Tantivy over CGO FFI** — the architectural grain favors sidecars. Forcing CGO on the whole app for a **default-off** feature (`SEARCH_FILE_INDEX_ENABLED`, spec D13) is a bad trade.
2. **Reuses built scaffolding ≈ free wrapper.** DuckDB has **no first-party server** (no Flight/HTTP — confirmed: only an ADBC header comment at `duckdb/src/include/duckdb/common/adbc/adbc.h:1400`; a sidecar always needs a custom wrapper). But the Phase-3 sidecar (`bitmagnet-rs/crates/bitmagnet-search`, tonic gRPC, the `FileSearchService` proto, the `FileClient` Go wiring from spec §5.2, the `bitmagnet-search` deploy role + 200Gi PVC) is **already designed/built**. We **swap the engine** (Tantivy → `duckdb-rs`) and keep the proto + client + deploy. The "custom wrapper" cost is largely paid.
3. **Isolation.** `memory_limit`/`threads` are **global per DuckDB database instance** (`duckdb/src/include/duckdb/main/settings.hpp:1448,1836`; `memory_limit` is an alias of `max_memory`, `src/main/config.cpp:242`) — not per-connection. A heavy GROUP-BY/DISTINCT (1.2 s, or a pathological 14 s full materialization) is best contained in a dedicated process with its own `memory_limit` + concurrency cap; an OOM crashes the sidecar, not the crawler/API.
4. **Hop is noise.** localhost gRPC ≈ 0.2 ms vs query latencies of 17 ms–1.3 s.

**Embedded fallback (a):** if the team prefers zero extra services and accepts CGO, `go-duckdb` is fully viable — the C API has everything (open-with-config `duckdb_open_ext` `duckdb.h:984`, bound-param prepared statements `duckdb.h:1901/2056-2167/2200`, chunked fetch `duckdb.h:5409`, `duckdb_interrupt` `duckdb.h:1013`, **bundled** Parquet reader — `read_parquet` is statically linked, `extension/parquet/parquet_extension.cpp:924`, `src/include/duckdb/main/extension_entries.hpp:566`, so local `.parquet` reads need no install/network). The SQL-gen, hydration, and data-model sections below apply **identically** to either runtime — only the transport differs (gRPC vs in-process call).

---

## 2. Topology & data model

```
┌────────────── bitmagnet (pure Go) ──────────────┐        ┌──── search sidecar (Rust) ────┐
│ GraphQL torrentContent.fileSearch ──► FileClient │──gRPC─►│ FileSearchService (duckdb-rs) │
│ GraphQL TorrentQuery.files ──► PG blob (G2)      │        │  DuckDB over local Parquet    │
│ hydrate Torrent from PG (AfterFind decode)       │◄─info_hash,file_index─               │
└──────────────────────────────────────────────────┘        └───────────────┬───────────────┘
        ▲                                                                     │ read_parquet
        │ periodic export Job (from blob, future-proof past torrent_files DROP)│
        └──────────────────► /data/files.parquet (+ /data/torrents.parquet) ◄─┘  (shared PVC)
```

**Parquet schema** (from `bench/blob_export/src/main.rs:109-120`, RUN-2 proven):
- **Fact (per file), slim — 3.86 GB / 879.5M rows:** `info_hash` VARCHAR(40-hex), `file_index` UINT32, `extension` VARCHAR **nullable** (path-derived via `file_extension_from_path`, **never the blob's empty `e`** — G1, `main.rs:169-173`), `size` UBIGINT. ZSTD-3, dict-encoded, 1M-row batches (`main.rs:141-145`).
- **Dim (per torrent), tiny — ~17M rows:** `info_hash` VARCHAR, `content_type` VARCHAR, `published_at` BIGINT (epoch). **Immutable columns only — never `seeders`/`leechers`** (mutable → would force constant rebuild). DuckDB hash-joins the dim in ms, **only when** a `contentTypes`/`publishedAfter` filter is present; otherwise the fact alone is queried (stays the proven 3.86 GB slim path).

**Freshness model:** Parquet is rebuilt **periodically from the blob** (`files_data`), not per-crawl. Rationale: you cannot cheaply append to a Parquet file per ingest, and the corpus is **immutable-once-crawled**. Consequence:
- **Cross-torrent per-file SEARCH (DuckDB) lags ≤ refresh interval** for brand-new torrents — acceptable for a DHT archive.
- **Per-torrent file BROWSE is ALWAYS fresh** — `TorrentQuery.files` reads the live PG blob (G2, §4), never DuckDB.
- The dual-write seam (`internal/processor/persist.go:138-156`, the post-commit fire-and-forget `indexToSearchSidecar`) is the **wrong** vehicle for Parquet (no cheap per-row append). Use a **periodic Job** (mirrors the existing search backfill Job / the RUN-0a `blob_export`). A live **delta-Parquet** (`UNION` base + a small "recent" file refreshed often) is a documented future optimization, and *that* is where the dual-write seam would later hook.

---

## 3. SQL generator — safe, parameterized

**Mechanism:** `duckdb_prepare` + typed binds + `duckdb_execute_prepared` (`duckdb.h:1901/2056-2167/2200`). User values are **bound parameters only — never string-concatenated.** Lists (`extensions`, `contentTypes`) bind as a LIST param via `col = ANY($n)`.

**Template (derived from RUN-2 Q1b = 35 ms warm):**
```sql
SELECT f.info_hash, f.file_index
FROM read_parquet('/data/files.parquet') f
[ JOIN read_parquet('/data/torrents.parquet') d USING (info_hash) ]   -- only if content_type/published filter
WHERE TRUE
  [ AND f.extension = ANY($exts) ]
  [ AND f.size >= $size_min ] [ AND f.size <= $size_max ]
  [ AND d.content_type = ANY($cts) ]
  [ AND d.published_at >= $published_after ]
  [ AND f.path ILIKE $q ]            -- path FTS, full Parquet only (v1.1; Q6 = 142 ms)
ORDER BY <allowlisted>               -- {size | published_at} × {asc|desc}; NO seeders/relevance
LIMIT $limit OFFSET $offset;
```

**Rules:**
- **ORDER BY allowlist = `{size, published_at}` only** (matches spec §5.4 + the `request_builder.go:178-185` `sortableFields` seam; the file surface deliberately drops `seeders`). The sort column/direction are chosen from a fixed allowlist enum, **not** interpolated from user text.
- **`collapse=true`** → exact distinct-torrent view via `GROUP BY info_hash` (or `COUNT(DISTINCT info_hash)` for the count, RUN-2 Q4 = 1.27 s). `totalCountIsEstimate=false` (DuckDB is exact, unlike the Tantivy-collapse caveat).
- **`totalCount`** is **opt-in** (the existing `totalCount: Boolean` input convention): `COUNT(*)`/`COUNT(DISTINCT …)` is the ~1.27 s scan; the paginated page itself is 35 ms. Never compute it unless requested.
- 🚨 **Always `LIMIT`.** RUN-2's only slow case (14.2 s) was *materializing* all 5.7M rows via `fetchall()` — a client artifact, not scan cost. The generator must **page or count, never ship the full set**.
- **Per-file `path` FTS** (`ILIKE`) needs the **full** Parquet (+path, 11.71 GB) — gate it behind a separate flag; the slim fact has no `path`. (CJK `ILIKE` works char-wise, unlike the Tantivy default tokenizer — a DuckDB advantage.)

---

## 4. Resolver wiring (file:line seams)

### 4.1 `torrentContent.fileSearch` → DuckDB (NEW)
- **Schema:** add `fileSearch(input: TorrentFileSearchQueryInput!): TorrentFileSearchResult!` to `type TorrentContentQuery` — `graphql/schema/query.graphqls:35-37`.
- **Service injection (NOT on `search.Search`, NOT decorated):** add `FileSearch filesearch.Service` to the `Resolver` struct `internal/gql/resolvers/resolver.go:19-29`; populate in `internal/gql/gqlfx/module.go` (Params + the `&resolvers.Resolver{…}` literal, beside `Search: s`). This is the spec's "direct serve" (D12): there is no `search.Search` per-file method to override and no PG baseline to shadow, so it bypasses the `router.Router` (`internal/search/router/router.go:48-116`) entirely.
- **Resolver impl:** mirror `gqlmodel.TorrentContentQuery.Search` (`internal/gql/gqlmodel/torrent_content.go:129-178`) — add a `FileSearch(ctx,input)` method on the query model, injected from `r.FileSearch` at the top-level `Query.torrentContent` resolver (`internal/gql/resolvers/query.resolvers.go:102-106`). Maps input → bound SQL (§3) via a **direct builder** (not the `OptionBuilder` replay — spec §5.4).
- **Client:** mirror `internal/search/tantivy/client.go:47-64`; share the existing gRPC `*grpc.ClientConn` (`NewFileClientOnConn`, spec §5.2). Provider is nil-on-disabled like `newClient` (`internal/search/searchfx/module.go:136-151`).

### 4.2 `TorrentQuery.files` → blob, **not DuckDB** (G2)
The per-torrent file **browse** must move off the `torrent_files` table before it's dropped — and it must **not** use DuckDB (DuckDB is for cross-torrent search; a single torrent's files live in the always-fresh blob).
- **Current (table-backed):** `gqlmodel.TorrentQuery.Files` (`internal/gql/gqlmodel/torrent_files.go:25-67`) → `search.TorrentFiles` (`internal/database/search/search_torrent_files.go:17-29`) reads `model.TableNameTorrentFile = "torrent_files"`; ordering columns at `order_torrent_files.go:14-78`.
- **G2 re-point:** load the `model.Torrent` by infoHash (selecting `files_data`) so `AfterFind` (`internal/model/torrents.go:19-39`) decodes the blob into `t.Files`; then do **orderBy / pagination / totalCount / hasNextPage in Go** over the decoded slice (the SQL column orderings won't survive the drop). Handle multi-infoHash merge + PG NULL-ordering parity (spec G2). Reuse `query.GenericResult` (`internal/database/query/query.go:27-33`) so the GraphQL result type is unchanged.

### 4.3 Hydration (DuckDB `(info_hash, file_index)` → GraphQL items)
1. Collect **distinct** `info_hash` from the DuckDB page → **one** PG load of `Torrent`s (via `r.Dao` or `search.Torrents`, cf. hydrator `internal/database/search/hydrator_torrent_content_torrent.go:44-70`). `AfterFind` decodes `files_data` → `t.Files`.
2. 🚨 **`t.Files` is sorted by `Path`** (`internal/model/torrents.go:26-30`), so it is **NOT positionally indexed by `file_index`.** Resolve `matchedFiles` by matching `TorrentFile.Index == file_index` (build `map[uint]TorrentFile`), **never `t.Files[file_index]`** — positional indexing returns the wrong file.
3. 🚨 Blob-decoded `TorrentFile`s have **empty `InfoHash` and zero `CreatedAt`/`UpdatedAt`** (`internal/blobmigration/serializer.go:58-69`, keys `{i,p,e,s}` only). Backfill `InfoHash` from the parent `Torrent.InfoHash`; set `createdAt/updatedAt` from `torrent.CreatedAt` (spec §13.1 — crawl-time-uniform, **not** `updated_at` which drifts on scrape).
4. Assemble `TorrentFileSearchItem{ torrent, matchedFiles, matchCount, score }`. Single-file torrents (no blob) re-synthesize the `{name, ext-from-name, size}` triple (D5).

---

## 5. Sidecar internals (pooling, memory, concurrency, lifecycle)

- **One `duckdb_database`** opened via `duckdb_open_ext` + a `duckdb_config` (`duckdb.h:984/1108-1152`) setting `memory_limit` (e.g. `"6GB"`), `threads` (e.g. `8`), `temp_directory` (spill, `settings.hpp:1815`). These are **global per instance** → bound *total* memory by **capping concurrent heavy queries** with a semaphore (the paginated find is cheap; GROUP-BY/DISTINCT are the scans to limit).
- **Connection pool:** many `duckdb_connection` off the one DB (`connection.hpp:38`, each its own `ClientContext`); **one goroutine/request per connection** (connections aren't for concurrent use).
- **Query timeout / cancellation:** a watchdog calls `duckdb_interrupt(conn)` (`duckdb.h:1013`) on gRPC-context deadline; optionally surface `duckdb_query_progress` (`duckdb.h:1021`).
- **Parquet hot-reload:** the export Job writes a new file then atomically renames; the sidecar re-creates the `files`/`torrents` views (`read_parquet`, bundled — no install). Treat the Parquet as a **disposable cache** (schema-version marker; on mismatch, re-export — never crash-loop).
- **Health/metrics:** `HealthCheck` returns row count → a `bitmagnet_search_file_doc_count` gauge via the existing poller pattern (`internal/search/searchfx/module.go:52-107`; gauge alongside `shadow/metrics.go:74-79`).
- **Postgres-scanner note:** DuckDB *can* query PG live (`postgres_scan`/pushdown, `extension_entries.hpp:528-536`) — usable for the export step **pre-`torrent_files`-DROP**, but **not** as the serving path (it would re-load the live PG we're offloading, and can't decode the zstd+msgpack blob post-DROP). Serving = Parquet.

### 5.1 C-API & concurrency grounding (DuckDB source)

Confirms the embedded/sidecar concurrency model is sound either way (`go-duckdb` and `duckdb-rs` both bind this stable C API):
- **One engine, many connections, lock-guarded.** The shared `DatabaseInstance` (`src/include/duckdb/main/database.hpp:42`) tracks its connections in a `mutex`-guarded map — `connection_manager.hpp:40-41` (`mutex connections_lock;` / `reference_map_t<ClientContext, weak_ptr<ClientContext>> connections;`). Each `Connection` carries its own `ClientContext` (`connection.hpp:38,50`). So **registering/closing connections is thread-safe**, but a *single* connection's `ClientContext` is **not** for concurrent use → the pattern is **one DB + a pool of connections, each used by one goroutine at a time** (§5).
- **Config is global per instance.** `threads`/`max_memory`(=`memory_limit` alias, `src/main/config.cpp:242`)/`temp_directory` are `SetGlobal` on the `DBConfig` (`settings.hpp:1448,1815,1836`; `custom_settings.cpp:1576,1583` → `TaskScheduler::SetThreads`) — **not** per-connection. ⇒ per-tenant memory isolation needs a **separate `duckdb_database`**; within one instance, bound total memory by **capping concurrency** (the §5 semaphore). One shared `TaskScheduler` parallelizes across all connections.
- **Safe query path:** `duckdb_open_ext`(+`duckdb_config`) `duckdb.h:984/1108-1152` → `duckdb_prepare` `:1901` → typed `duckdb_bind_*` `:2056-2167` → `duckdb_execute_prepared` `:2200` → `duckdb_fetch_chunk` `:5409`; cancel via `duckdb_interrupt` `:1013`.
- **Appender (future live-delta only):** the chunk-wise **appender** C API exists (`duckdb_appender_create` `duckdb.h:4649`, `duckdb_append_data_chunk`, `duckdb_appender_flush` `:4743`) — relevant *only if* we later feed a live delta table instead of full Parquet rebuilds (§2 future option). v1 uses periodic Parquet export, so the appender is unused.

---

## 6. Feature gate, config, write-path

- **Gate:** add `FileIndexEnabled bool` to `internal/search/searchfx/config.go:15-38` (auto `SEARCH_FILE_INDEX_ENABLED`, default `false` in `NewDefaultConfig` `:41-53`; registration via `configfx/factory.go:10-37`). The `filesearch` provider is nil unless `Enabled && FileIndexEnabled` (spec §5.2). Plus an export-Job knob (Parquet path, refresh schedule) on the sidecar/Job side.
- **Write-path:** the periodic export Job (from blob) is the only writer; no change to the crawl/persist hot path. Whole-torrent delete is handled implicitly by the next rebuild (no live delete fan-out needed for a periodic cache); if/when a live delta is added, it rides `persist.go:162-178` (`SearchIndexer` interface `internal/processor/processor.go:26-29`).

---

## 7. Risks / open questions

1. **Refresh interval vs freshness SLA** — pick the Job cadence (hourly/nightly). New-torrent per-file *search* lag is the only gap; *browse* is always fresh. Product call.
2. **Sidecar language** — Rust `duckdb-rs` (reuses the `bitmagnet-search` crate/deploy, recommended) vs a small Go+CGO `go-duckdb` sidecar (isolates CGO to one binary, but a new build). Both keep the main app pure-Go.
3. **Embedded escape hatch** — if ops strongly prefers no second service, (a) `go-duckdb` embedded is viable at the cost of CGO on the main app; §3–4 apply unchanged.
4. **`content_type`/`published_at` denorm** — dim-join (recommended, slim fact) vs denorm-into-fact (simpler SQL, +~1 GB). Default: dim-join.
5. **path-FTS (v1.1)** — needs the full 11.71 GB Parquet; ship slim-only first, add path on demand (DuckDB `ILIKE` is CJK-correct, a real edge over the rejected Tantivy default tokenizer).
6. **Concurrency cap tuning** — the heavy-scan semaphore limit + `memory_limit` need a load test under the real API mix (the GROUP-BY scans go 1.2 s → 10–13 s at 1 core — `file-grained-search-benchmark-results.md:51`).
