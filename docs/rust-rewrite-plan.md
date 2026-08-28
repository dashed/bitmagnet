# Bitmagnet Rust Rewrite Plan: Hybrid Blob Migration & Tantivy Integration

**Status:** Phase 1 (Hybrid Blob) implemented — [PR #1](https://github.com/dashed/bitmagnet/pull/1); Phases 2-9 planned  
**Date:** 2026-05-28 (updated with Python-verified production data + Phase 1 implementation)  
**Branch:** `feat/rust-rewrite-plan`

---

## Executive Summary

Optimize and rewrite bitmagnet in phases, starting with a **Hybrid Blob migration** that eliminates the 273 GB `torrent_files` table (74% of the database), followed by a **Tantivy search sidecar** and incremental Rust port. Each phase is independently valuable — the project can stop at any checkpoint and still deliver meaningful improvements.

**Key finding from live database analysis (2026-05-28, Python-verified):** Tantivy alone is roughly **disk-neutral** — it replaces ~39 GB of PG FTS data but adds 39-78 GB as its own index. The real space savings come from **Hybrid Blob migration** (368 GB → ~128 GB, a 66% reduction) by replacing 873M individual file rows with ZSTD-compressed blobs per torrent. Only 16.8M of 48M torrents (35%) actually have file data — a critical finding that keeps blob sizes at ~16 GB. Combined with Tantivy and ZFS, total storage drops to 45-56 GB (85% reduction).

**Key architectural decision:** Use a **Rust gRPC sidecar** (not tantivy-go FFI) for search. The tantivy-go bindings lack numeric fields, faceted search, aggregations, and are pinned to Tantivy 0.22 (upstream is 0.26). A gRPC sidecar provides full Tantivy access and establishes the first Rust component of the port.

**Timeline:** ~33 weeks for full port. First value at **week 0** (drop unused indexes, 14-29 GB), biggest impact at **week 3** (Hybrid Blob complete, 368 → ~132 GB), search upgrade at week 7 (Tantivy sidecar).

---

## Implementation Status

| Phase                                   | Status                                                                                                                            | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Phase 0: Quick wins                     | 📋 Planned                                                                                                                        | Index drops are operational (run against the live DB), not code                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| **Phase 1: Hybrid Blob migration**      | ✅ **Implemented** (code merged, [PR #1](https://github.com/dashed/bitmagnet/pull/1))                                             | Built as a **zero-downtime live migration** (see [`live-migration-design.md`](./live-migration-design.md)) — dual-write + `AfterFind` hook + queue-based migration + live consistency verification + operational CLI. 42 unit tests + 4 E2E tests. **Destructive cutover (drop `torrent_files`, switch facet to JSONB containment) is deferred behind the `cleanup` safety gate and has not run.**                                                                                                                                                                                                                                                                                                                                                                                      |
| **Phase 2: Rust infrastructure**        | ✅ **Implemented** (branch `feat/rust-infrastructure`, [PR #2](https://github.com/dashed/bitmagnet/pull/2))                       | `bitmagnet-rs/` Cargo workspace (5 crates: proto, common, model, db, search), `bitmagnet.v1` protobuf (tonic 0.14 / `tonic-prost`), gRPC server skeleton (all RPCs stubbed except HealthCheck), Tantivy 0.26 schema, multi-stage `Dockerfile.search` (built, 103 MB, runs non-root), `.github/workflows/rust.yml` CI. 38 tests pass incl. **Go↔Rust blob wire-compat fixtures**. Domain logic is Phase 3.                                                                                                                                                                                                                                                                                                                                                                              |
| **Phase 3: Tantivy search sidecar MVP** | ✅ **Implemented** (branch `feat/tantivy-search-sidecar`, stacked on PR #2)                                                       | `TokenizeFlat` ported with **go-unidecode tables verbatim** (4223-fixture Go parity); schema/index/indexer + all RPCs live; **run_search** (Go tsquery port: `&` `                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | ` `.` `!` `\*`, A/B/C/D field boosts via DisjunctionMaxQuery) + **run_facets** (all 14 facets, typed aggregations); **doc_id** composite key (= Go `InferID`) so multi-classification torrents coexist; backfill CLI (drives FROM torrent_contents = PG search parity). Read RPCs proven by a server-level integration test. Live-PG backfill + 48M-doc index run are operational (deferred). 4 proto caveats noted for Phase 4 below. |
| **Phase 4: Shadow mode Go integration** | ✅ **Implemented** ([PR #4](https://github.com/dashed/bitmagnet/pull/4), branch `feat/shadow-mode-integration`, stacked on PR #3) | New Go packages `internal/search/{tantivy,shadow,router,searchfx}`: gRPC client + `BuildDocument` (matches Rust backfill, `DocID==InferID`); comparator (Jaccard@20/50, RBO p=0.9, top-1) + Prometheus metrics; `SearchRouter` implements `search.Search`, modes postgres/shadow/canary/tantivy; dual-write in `processor/persist.go` (fire-and-forget); fx `Decorate` swaps the router into gql/torznab; `fx.ValidateApp` test. **Disabled by default** (app unchanged when off). Filtered queries skip shadow comparison (free-text parity signal stays clean). Also fixed a Phase-3 `video_resolution` parity bug ("V1080p"→"1080p"). 27 Go pkgs + Rust workspace build/test green; **full CI (`generated` / `lint` / `test`) is green on PR #4** — see the CI-hardening note below. |
| Phases 5-9: Validation + Rust port      | 📋 Planned                                                                                                                        | Unchanged below                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |

> **CI hardening (PRs #1–3).** The `Checks` workflow's `lint` job runs `prettier --check` _before_ a separate `golangci-lint` (v2.1.6) step, so an early prettier failure had **masked** the linter. Fixing prettier surfaced ~107 pre-existing `golangci-lint` findings (mostly in Phase-1 blob-migration code: `paralleltest`, `wsl`, `contextcheck`, `errchkjson`, `goconst`, `gocritic`, `gosmopolitan`, `protogetter`, `tagalign`, …). Those — plus `gofmt`, a `torrents.gen.go` regen from the gen config, `go mod tidy` (a stale `// indirect`), and doc/prettier formatting (the generated `tokenizer_fixtures.json` is `.prettierignore`d, not reformatted) — were fixed, and **PRs #1–3 are now fully green and mergeable**. The fix was stacked linearly via merge-forward (independent sibling fix-commits had briefly made the stacked PRs `CONFLICTING`, which blocks `pull_request` CI entirely). **PR #4** (this Phase-4 branch) was then brought green the same way: a merge-forward from PR #3 carries the fixes (and `internal/search/*` was already golangci-clean), plus two PR-#4-specific `generated` fixes — `protoc-gen-go-grpc` added to the flake devShell (required by `gen-search-proto`'s `--go-grpc_out`, previously missing) and `pb/*` regenerated with nix-matching tooling (protoc 28.3, protoc-gen-go v1.35.1, **protoc-gen-go-grpc v1.3.0** — nixos-24.11 lags the v1.5.x generic-stream API, so the committed v1.5.1 output diffed). **All four PRs (#1–#4) are now green.**

> Phase 1 shipped the **live-migration variant** of the original plan rather than the "fork + stop writing rows" variant described in §Phase 1 below. The goal and disk numbers are unchanged; the _approach_ keeps `torrent_files` populated (dual-write) until an explicit, verification-gated `cleanup` drops it — so rollback is "redeploy the old image" right up until cutover. See the [updated Phase 1 section](#phase-1-hybrid-blob-migration-implemented--zero-downtime-live-migration) for the as-built design.

---

## Disk Savings Assessment

Live production database measured 2026-05-28: **368 GB** (395,623,308,311 bytes) across 48M torrents and 873M file rows. All estimates below **verified with Python** against real sampled data (see `docs/space-savings-verification.md` and `docs/verify_space_savings.py`).

### Current Database Breakdown

| Table                    | Total      | Data                 | Indexes | Rows | % of DB |
| ------------------------ | ---------- | -------------------- | ------- | ---- | ------- |
| torrent_files            | **273 GB** | 119 GB               | 155 GB  | 873M | 74%     |
| torrent_contents         | **61 GB**  | 21 GB (+12 GB TOAST) | 28 GB   | 48M  | 17%     |
| torrents_torrent_sources | **19 GB**  | 8 GB                 | 11 GB   | 75M  | 5%      |
| torrents                 | **14 GB**  | 7 GB                 | 7 GB    | 48M  | 4%      |
| Other                    | **1 GB**   | —                    | —       | —    | <1%     |

Key findings:

- **Only 16.8M of 48M torrents (35%) have file data** in `torrent_files` — critical for blob sizing
- **31 unused indexes** (0 scans since last stats reset) consuming **29 GB** total
- `torrent_files` has 2 unused indexes: `size_idx` (8.2 GB, 0 scans) and `extension_idx` (5.8 GB, 0 scans)
- `torrent_contents.tsv` GIN index: 14 GB (359 scans) — the FTS workhorse Tantivy replaces
- `content.tsv` GIN index: 31 MB (650K scans) — actively used, keep
- tsvector data is **72.1%** of `torrent_contents` row size (~408 bytes avg), 12 GB in TOAST overflow

### Savings by Scenario (Python-verified)

| Scenario                  | PG Size     | Tantivy      | Total          | Savings    | Effort        |
| ------------------------- | ----------- | ------------ | -------------- | ---------- | ------------- |
| **Current**               | 368 GB      | —            | **368 GB**     | —          | —             |
| Quick wins only           | 339-354 GB  | —            | **339-354 GB** | 4-8%       | Trivial       |
| Tantivy only              | 329 GB      | 39-78 GB     | **368-407 GB** | ~0%        | 10 weeks      |
| **Hybrid Blob only (PG)** | **~128 GB** | —            | **~128 GB**    | **66%**    | **2-3 weeks** |
| Hybrid Blob + Tantivy     | ~93 GB      | 39-78 GB     | **132-171 GB** | 54-64%     | 12+ weeks     |
| **Everything + ZFS**      | **~37 GB**  | **16-31 GB** | **53-68 GB**   | **82-86%** | **13+ weeks** |

### Hybrid Blob Disk Accounting (Python-verified)

| Component                      | Current    | After Blob Migration | Change              | Verification                                                                     |
| ------------------------------ | ---------- | -------------------- | ------------------- | -------------------------------------------------------------------------------- |
| torrent_files (data + indexes) | **273 GB** | **0 GB**             | -273 GB eliminated  | —                                                                                |
| File blobs (ZSTD L3 msgpack)   | 0          | **16.2 GB**          | +16.2 GB            | ✅ Measured: 1.0 KB avg blob, 16.8M torrents with files                          |
| `file_extensions JSONB` + GIN  | 0          | **5.6-9.4 GB**       | +5.6-9.4 GB         | ✅ Measured: 3.1 avg extensions/torrent (as-built: JSONB + `jsonb_path_ops` GIN) |
| `torrent_file_summary` table   | 0          | **10.4-14.1 GB**     | +10.4-14.1 GB       | ✅ Measured: 116 bytes avg row                                                   |
| torrent_contents               | 61 GB      | 61 GB                | unchanged           | —                                                                                |
| torrents_torrent_sources       | 19 GB      | 19 GB                | unchanged           | —                                                                                |
| torrents (base table)          | 14 GB      | 14 GB                | unchanged           | —                                                                                |
| Other tables                   | 1 GB       | 1 GB                 | unchanged           | —                                                                                |
| **Database total**             | **368 GB** | **~127-135 GB**      | **-233 to -241 GB** | **66% reduction**                                                                |

> **Key insight:** Only 16.8M of 48M torrents (35.1%) have file data in `torrent_files`. The remaining 31.1M torrents have no files — blob storage applies only to the 16.8M. This is why compressed blob size is ~16 GB, not the naively-calculated ~46 GB (if extrapolated across all 48M).

---

## Architecture Overview

### Current State (Go + PostgreSQL)

```
DHT Crawler (Go) → Processor → PG INSERT ON CONFLICT
                        ↓
                   Classifier (CEL/YAML) → UpdateTsv() → PG tsvector + GIN
                        ↓
GraphQL/Torznab → query.GenericQuery → tsv @@ tsquery → Results + Facets
```

### Target State (Rust + PostgreSQL + Tantivy)

```
DHT Crawler (Rust/tokio) → Processor → PG INSERT (SQLx)
                               ↓
                          Classifier (Rhai/YAML) → Tantivy IndexWriter
                               ↓
GraphQL/Torznab (axum) → Tantivy Searcher → BM25 Results + Facets
                               ↓
                          PG for relational data (hydration, content metadata)
```

### Transition State (Shadow Mode)

```
                  ┌─────────────────────────────────────┐
                  │         bitmagnet (Go)               │
                  │                                      │
  DHT Crawler ──→ │  Processor ──→ persist() ──→ PG DB   │
                  │       │                     (tsv)    │
                  │       └──→ TantivyIndexer ─(gRPC)──→│──→ Tantivy Sidecar (Rust)
                  │                                      │        │
  GraphQL/API ──→ │  SearchRouter ──→ PG Search          │        └──→ Tantivy Index
                  │       │                              │              (on disk)
                  │       └──→ Tantivy Search ─(gRPC)──→│
                  │       └──→ Comparator (log diffs)    │
                  └─────────────────────────────────────┘
```

---

## Why Not tantivy-go FFI?

| Feature Needed                         | tantivy-go     | Rust Sidecar         |
| -------------------------------------- | -------------- | -------------------- |
| Text search with field boosting        | ✅             | ✅                   |
| Numeric fields (seeders, size sorting) | ❌             | ✅                   |
| Date fields (published_at)             | ❌             | ✅                   |
| Faceted search (14 facet types)        | ❌             | ✅                   |
| Aggregations (facet counts)            | ❌             | ✅                   |
| Range queries                          | ❌             | ✅                   |
| Latest Tantivy (0.26)                  | ❌ (0.22)      | ✅                   |
| Build complexity                       | CGo + Rust FFI | Standard Rust binary |
| Reusability for Rust port              | Throwaway      | Foundation           |

---

## Tantivy Index Schema

### Field Mapping (PG → Tantivy)

| PG Source          | tsvector Weight | Tantivy Field                 | Type  | Flags            | Query Boost     |
| ------------------ | --------------- | ----------------------------- | ----- | ---------------- | --------------- |
| info_hash          | A               | `info_hash`                   | Bytes | STORED + INDEXED | — (exact match) |
| torrent name       | A               | `torrent_name`                | Text  | STORED + INDEXED | 4.0             |
| content title      | A               | `content_title`               | Text  | STORED + INDEXED | 4.0             |
| original title     | A               | `original_title`              | Text  | INDEXED          | 4.0             |
| release year       | B               | `release_year`                | U64   | FAST + INDEXED   | 2.0             |
| video resolution   | C               | `video_resolution`            | Text  | FAST + INDEXED   | 1.5             |
| video source/codec | C               | `video_source`, `video_codec` | Text  | FAST + INDEXED   | 1.5             |
| genres             | D               | `genres`                      | Text  | INDEXED          | 0.5             |
| file paths         | D               | `file_paths`                  | Text  | INDEXED          | 0.5             |
| content_type       | —               | `content_type`                | Facet | —                | — (filter only) |
| seeders            | —               | `seeders`                     | U64   | FAST             | — (sort only)   |
| leechers           | —               | `leechers`                    | U64   | FAST             | — (sort only)   |
| size               | —               | `size`                        | U64   | FAST + INDEXED   | — (sort/filter) |
| files_count        | —               | `files_count`                 | U64   | FAST             | — (sort only)   |
| published_at       | —               | `published_at`                | Date  | FAST + INDEXED   | — (sort/filter) |
| languages          | —               | `languages`                   | Text  | FAST             | — (facet)       |
| file_extensions    | —               | `file_extensions`             | Text  | FAST             | — (facet)       |

### Custom Tokenizer (Critical Path)

Must replicate Go's `TokenizeFlat()` (`internal/database/fts/tokenizer.go`):

1. Unicode transliteration via `deunicode` crate (Rust equivalent of go-unidecode)
2. Lowercase normalization
3. CJK: each character becomes a separate token
4. Split on non-alphanumeric boundaries
5. Remove tokens > 255 bytes

---

## Phased Implementation

### Phase 0: Quick Wins (Week 0, no code changes)

Immediate savings from database maintenance — no application changes required.

| Task                               | Description                                                                                                              | Savings               | Risk                                                            |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | --------------------- | --------------------------------------------------------------- |
| Drop `torrent_files_size_idx`      | 8.2 GB, 0 scans since stats reset                                                                                        | 8.2 GB                | Low — no queries use this index                                 |
| Drop `torrent_files_extension_idx` | 5.8 GB, 0 scans since stats reset                                                                                        | 5.8 GB                | Low — facet uses EXISTS on `extension` column, not this index   |
| Audit remaining unused indexes     | 31 indexes with 0 scans = 29 GB total                                                                                    | up to 15 GB more      | Medium — some may be needed by query planner for faceted search |
| `VACUUM ANALYZE` bloated tables    | `torrents_torrent_sources` has 19.2% dead tuples (1.3 GB)                                                                | ~1.3 GB               | None                                                            |
| Tune autovacuum for large tables   | Set `autovacuum_vacuum_scale_factor = 0.01` on `torrent_files` (871M rows × 0.2 = 174M dead rows before vacuum triggers) | Prevents future bloat | None                                                            |

**Conservative estimate: 14 GB immediate. Aggressive: up to 29 GB.**

> **Note on unused index safety:** Only drop indexes that are confirmed unused via `pg_stat_user_indexes.idx_scan = 0` AND are not the sole access path for a query pattern. The `torrent_files` size and extension indexes are safe — the file type facet uses `EXISTS (... AND extension IN (...))` which hits the composite PK or unique index, not these standalone indexes.

### Phase 1: Hybrid Blob Migration (Implemented — Zero-Downtime Live Migration)

The highest-ROI change: replace 873M individual `torrent_files` rows (273 GB) with one ZSTD-compressed MessagePack blob per torrent. **Estimated savings: ~236 GB (368 → ~132 GB).**

**Status: ✅ implemented** in [PR #1](https://github.com/dashed/bitmagnet/pull/1). Shipped as a **zero-downtime live migration** (full design: [`live-migration-design.md`](./live-migration-design.md)) rather than the offline "fork + stop writing rows" approach originally sketched here. The defining choices:

- **Dual-write, not cutover-on-write.** The DHT persist path writes the blob **and** keeps inserting `torrent_files` rows (same transaction). This keeps every existing query working unchanged and makes rollback trivial (redeploy the old image) right up until the explicit cleanup step.
- **`AfterFind` hook for transparent reads.** A single GORM hook on `Torrent` deserializes `files_data` into `t.Files`, so all read paths that load files (tsvector rebuild, processor preload) are covered without touching each call site. The per-resolver / per-facet rewrites the original plan listed are deferred to cutover.
- **Queue-based self-chaining migration** (not a one-shot script): survives restarts, retries, throttles, and reports progress — driven from the CLI.
- **Live consistency verification + operational CLI + rollback safety gates** were added beyond the original Phase 1 scope.

#### 1a. Schema Changes — ✅ done (`migrations/00021`, `00022`)

| Change                           | As built                                                                                                                                                                                                                                                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `torrents.files_data BYTEA`      | ZSTD-compressed MessagePack blob, one per torrent (~1.0 KB avg, stored in TOAST).                                                                                                                                                                                        |
| `torrents.file_extensions JSONB` | Unique file extensions per torrent (`NOT NULL DEFAULT '[]'`). **JSONB**, not `TEXT[]` — matches the codebase `serializer:json` convention and avoids a `lib/pq` dependency. `jsonb_path_ops` GIN index (created `CONCURRENTLY` via `00022`, `-- +goose NO TRANSACTION`). |
| `torrent_file_summary` table     | Keyed by **`info_hash BYTEA`** PK (FK → `torrents`, `ON DELETE CASCADE`): `file_count, total_size, largest_file_size, extensions JSONB, has_video, has_subtitle, has_audio`. Covers filter/facet queries without decompressing blobs.                                    |

#### 1b. Application Changes — ✅ done

| Area              | File(s)                                             | As built                                                                                                                                                                                                                                 |
| ----------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Blob serializer   | `internal/blobmigration/serializer.go`              | `SerializeFiles`/`DeserializeFiles` (MessagePack → ZSTD L3, ~10% ratio), `ExtractUniqueExtensions`, `BuildFileSummary`. Package-level encoder/decoder.                                                                                   |
| Model fields      | `internal/model/torrents.gen.go`                    | `FilesData []byte` (`json:"-"`) and `FileExts []string` (`serializer:json`). Named `FileExts` to avoid collision with the existing `FileExtensions()` method. The `Files []TorrentFile` relation is **kept** (populated by `AfterFind`). |
| Transparent read  | `internal/model/torrents.go`                        | `AfterFind` deserializes `files_data` → `t.Files` via a `FilesDataDeserializer` function var (breaks the `model`↔`blobmigration` import cycle); falls back to preloaded rows on nil/error.                                              |
| Dual-write        | `internal/dhtcrawler/persist.go`                    | `createTorrentModel()` sets `FilesData`/`FileExts`; the upsert adds `files_data`/`file_extensions` to `DoUpdates` **and still writes `torrent_files` rows**.                                                                             |
| Migration handler | `internal/blobmigration/queue/{handler,message}.go` | Self-chaining batch handler (cursor by `info_hash`), follows the `processor/batch` pattern. Per-batch 5% consistency sample; auto-pauses if error rate > 1%. Progress in `key_values`.                                                   |
| CLI               | `internal/app/cmd/blobmigrationcmd/command.go`      | `blob-migration {start,status,pause,resume,verify,cleanup}`.                                                                                                                                                                             |

#### 1c. Live consistency verification — ✅ done (`internal/blobmigration/consistency/`)

`checker.go` (field-by-field `CompareFiles`, `CheckTorrent/Batch/Random`), `live_checker.go` (continuous background sampler, auto-heals a bad blob by NULLing it to trigger re-migration), `metrics.go` (Prometheus counters/gauges), `healthcheck.go` (reports degraded when `errors_total > 0`).

#### 1d. Migration + cutover — ⏳ operational, **cutover deferred behind safety gate**

| Step                      | Status              | Notes                                                                                                                                                                                                                                              |
| ------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Run live migration        | Operational         | `blob-migration start` enqueues the self-chaining job; runs while the DHT crawler keeps ingesting. `status`/`pause`/`resume` for control.                                                                                                          |
| Verify                    | Operational         | `blob-migration verify --full` (or `--sample-rate`) compares blobs vs. rows; records `verified_at`.                                                                                                                                                |
| **Cleanup (destructive)** | **Not yet run**     | `blob-migration cleanup --confirm` drops `torrent_files` + `VACUUM`. Refuses unless **all** gates pass: status `completed`, zero unmigrated torrents, verification < 24h old, `--confirm`.                                                         |
| Facet → JSONB containment | Deferred to cutover | While `torrent_files` exists (dual-write), the file-type facet still uses the `EXISTS` subquery. The switch to `file_extensions` JSONB containment + the GraphQL resolver switch to blob-only reads happen at cutover, after the table is dropped. |

**GO/NO-GO (cutover):** `verify --full` reports 100% blob↔row match, no unmigrated torrents remain, and search/browse/facets validated → run `cleanup --confirm`. Until then, the system runs safely in dual-write mode.

### Phase 2: Rust Infrastructure (Weeks 3-4, can overlap with Phase 1)

**Status: ✅ implemented** on branch `feat/rust-infrastructure` (stacked on PR #1). As-built notes below.

| Task            | As built                                                                                                                                                                                                                                               | Status |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ |
| Rust workspace  | `bitmagnet-rs/` Cargo workspace, 5 crates: `bitmagnet-proto`, `bitmagnet-common`, `bitmagnet-model`, `bitmagnet-db`, `bitmagnet-search`. (classifier/dht/api crates deferred to their phases.)                                                         | ✅     |
| Protobuf schema | `proto/bitmagnet/{common,search}.proto`, package `bitmagnet.v1`. `SearchService` (IndexDocument, BatchIndex, DeleteDocument, Search, GetFacets, HealthCheck). **tonic 0.14** → codegen via `tonic-prost-build`; enums wire-locked to Go by unit tests. | ✅     |
| CI/CD           | `bitmagnet-rs/docker/Dockerfile.search` multi-stage (built, 103 MB, runs non-root); `.github/workflows/rust.yml` (fmt/clippy `-D warnings`/test/docker, path-filtered).                                                                                | ✅     |

> **As-built deltas from the original sketch:** (1) the model crate proved Go↔Rust blob wire-compat with fixtures generated from the real Go `blobmigration.SerializeFiles` — note Rust must use `rmp_serde::to_vec_named` (msgpack MAP keyed `i/p/e/s`) to match `vmihailenco/msgpack`; (2) `bitmagnet-search` implements the Tantivy 0.26 schema now (the rest of search is Phase 3 stubs); (3) `published_at` is `int64` Unix seconds in the proto; (4) CI is a standalone Rust workflow — folding it into the repo's Nix flake + Taskfile is future work. The `rust-toolchain.toml` pins `channel = "stable"`; pin to an explicit version if fully reproducible Docker builds are required.

### Phase 3: Tantivy Search Sidecar MVP

**Status: ✅ implemented** on branch `feat/tantivy-search-sidecar` (stacked on PR #2). As-built notes below.

| Task                          | As built                                                                                                                                                                                                                                                                                                                                                           | Status |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ |
| Custom tokenizer              | `TokenizeFlat` ported exactly — **go-unidecode tables transcoded verbatim** (the `deunicode` crate diverges), plus Go's `IsLetter\|IsDigit` ranges + single-rune `ToLower`; no dedupe; CJK (>U+1FFF) one token each. 4223 Go-generated fixtures, byte-for-byte parity. Registered as the index analyzer (bare, no `LowerCaser` — would break CJK transliteration). | ✅     |
| Index schema + lifecycle      | Weighted text fields `text_a..d` (A=4.0/B=2.0/C=1.5/D=0.5) + keyword facet/fast fields + numerics + `doc_id`; `open_or_create`/reader/writer; tokenizer registered via `analyzer()`.                                                                                                                                                                               | ✅     |
| Write RPCs + indexer          | `IndexDocument`/`BatchIndex`(stream)/`DeleteDocument`/`HealthCheck`(real doc_count). **doc_id** = `hex(info_hash):ct:cs:cid` (= Go `InferID`/PG generated id) — upsert deletes by doc_id so a torrent's multiple classifications coexist; `DeleteDocument` deletes by info_hash (PG cascade).                                                                      | ✅     |
| Query translation             | `run_search`: faithful Go `AppQueryToTsquery` port (byte-for-byte vs `tsquery_test.go`); operators `&`(AND)/`\|`/`.`(→`<->`)/`!`/`*`(→`:*`) with PG precedence; each lexeme → `DisjunctionMaxQuery(0.3)` of `BoostQuery` over `text_a..d`; phrases→`PhraseQuery`, prefix→`PhrasePrefixQuery`; filters/pagination/sort; `SearchHit.id = doc_id`.                    | ✅     |
| Faceted search + aggregations | `run_facets`: all 14 via typed `tantivy::aggregation`; `files_count` range buckets; `file_type` folds `file_extensions`; `tmdb_id` = `content_id` where `content_source=="tmdb"`; `FacetType::ALL` order.                                                                                                                                                          | ✅     |
| Backfill CLI                  | `src/bin/backfill.rs` + `transform.rs`: `bitmagnet-db::stream_torrents_for_index` drives **FROM `torrent_contents`** (one doc per tc = PG `tsv @@ tsquery` parity), keyset by `tc.id`; deserialize blob (Go-compatible) → proto `TorrentDocument` → Tantivy; clap flags, resume on `tc.id`. (48M-doc live run is operational, deferred.)                           | ✅     |
| Read-path integration test    | `tests/read_path.rs` drives `Search`+`GetFacets` through the server (free-text/sort/filter, facet counts, multi-classification doc_id).                                                                                                                                                                                                                            | ✅     |

> **Known limitations for Phase 4 (read-agent–flagged, all degrade sensibly):** (1) the proto `SearchFilters` is flat, so Go's per-facet OR-logic (exclude a facet's own filter when aggregating it) isn't reproduced — all facets aggregate over the one filtered set; (2) no dedicated multi-valued `file_types` field, so the `file_type` facet **overcounts** a torrent with 2+ same-type extensions (folds `file_extensions`); (3) no null/Unknown facet bucket (Go has one for missing content*type/release_year); (4) multi-key sort honours only the first `SortBy` (single-key `TopDocs`; field-sorted hits carry score 0.0); (5) `SearchHit` carries the `doc_id` \_components* (info_hash + content_type/source/id) so the composite is client-derivable, but there's no explicit `doc_id` field on the proto — add one to `SearchHit` for an explicit stable hit id. These matter for shadow-mode ranking/facet parity and are the first tuning items in Phase 4.

> **Original estimates (for reference):** tokenizer 3-5d · schema 2d · gRPC server 3d · query translation 3d · facets 3d · aggregations 2d · index mgmt 2d · backfill 2d.

### Phase 4: Shadow Mode Go Integration

**Status: ✅ implemented** on branch `feat/shadow-mode-integration` (stacked on PR #3). As-built below.

| Task                 | As built                                                                                                                                                                                                                                                                                                                                                                           | Status |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| gRPC client          | `internal/search/tantivy/`: client over generated bindings (`pb/`, protoc-gen-go v1.35.1 + go-grpc; regen `task gen-search-proto`); unix+tcp; `BuildDocument` mirrors the Rust backfill transform (`DocID==TorrentContent.InferID`).                                                                                                                                               | ✅     |
| Comparator + metrics | `internal/search/shadow/`: Jaccard@20/50, RBO_EXT p=0.9, top-1, latency — keyed by `InferID`; Prometheus collectors (`group:"prometheus_collectors"`) + structured logging.                                                                                                                                                                                                        | ✅     |
| SearchRouter         | `internal/search/router/`: implements `search.Search`; modes postgres/shadow/canary/tantivy. Shadow serves PG + async-compares Tantivy (fire-and-forget, never affects served result/latency, honors `sample_rate`). Derives `pb.SearchRequest` from opaque `query.Option` via a recording `OptionBuilder`. Canary/tantivy _serving_ scaffolded (full hydrated serving = Phase 6). | ✅     |
| Dual-write           | `processor/persist.go`: after the tx commits, async `IndexDocument` per torrent_content + `DeleteDocument` per removed info_hash; no-op when disabled; never fails crawling.                                                                                                                                                                                                       | ✅     |
| Config + fx wiring   | `internal/search/searchfx/` configfx `search` section (**disabled by default → postgres mode**); `appfx` `fx.Decorate` (root scope) swaps the router into gql/torznab/processor; doc-count reporter (HealthCheck→`SetTantivyDocCount`) + client lifecycle; `internal/app/app_test.go` `fx.ValidateApp`.                                                                            | ✅     |

> **Resolved a Phase-3 parity bug here:** `video_resolution` was indexed as the raw enum `"V1080p"`, but Go's tsvector + GraphQL facet use the Label `"1080p"`. Fixed in both `transform.rs` (V-strip, like `video_3d`) and Go `BuildDocument` (`.Label()`), so live + backfilled docs agree and search/facet/filter align on `"1080p"`. `doc_id` unaffected.

> **Deferred to Phase 5 (read-agent caveats + new):** shadow comparison currently SKIPS filtered queries (so jaccard/RBO reflect only free-text parity) — bridging `query.Where` filters → `pb.SearchFilters` needs call-site cooperation and is the top Phase-5 item. Plus the 5 Phase-3 proto caveats (per-facet OR-logic, `file_types` overcount, null bucket, single-key sort, explicit `doc_id` on `SearchHit`). Canary/tantivy _serving_ (hydrating hits from PG) completes in Phase 6 cutover. Per-content deletes go by info_hash only (backfill reconciles).

> **Original estimates (for reference):** gRPC client 1d · dual-write 2d · SearchRouter 3d · comparator 2d · metrics 1d · config 1d · fx wiring 1d.

### Phase 5: Shadow Mode Validation (Weeks 11-13)

| Task                | Description                               | Estimate  |
| ------------------- | ----------------------------------------- | --------- |
| Production backfill | Index 48M torrents from PG                | 1-2 days  |
| Shadow mode run     | 2-3 weeks collecting comparison metrics   | 2-3 weeks |
| Tokenizer tuning    | Fix divergences found during shadow mode  | 1-3 days  |
| Quality gate        | Jaccard > 0.7 @ top-20 for 95% of queries | —         |

### Phase 5.5: File-Grained Search — Complete Per-File Parity (Weeks ~13-14, gates Phase 6)

**Status: 📋 Planned** (spec complete; **gates the Phase-6 cutover**). Full spec: [`docs/dev/file-grained-search-spec.md`](./dev/file-grained-search-spec.md); complete-parity analysis: [`docs/dev/perfile-search-complete-parity.md`](./dev/perfile-search-complete-parity.md); innovative-design research: [`docs/dev/perfile-search-innovative-design.md`](./dev/perfile-search-innovative-design.md).

A **second, file-grained Tantivy index — one document per file** — served by the **same sidecar process** (its own directory `/index/files/` alongside `/index/torrents/`, its **own writer/reader/mutex**), restores **true per-file search** that the hybrid-blob migration removes once `torrent_files` is dropped. **This is a distinct index from the WIP torrent-grained Phase-3 sidecar index** (1 doc per torrent_content): same process and PVC, separate directory + separate writer (Tantivy enforces one writer per directory). It answers the per-file **conjunction** "find all `.mkv` files > 1 GB" — the exact `(extension, size)` pairing — which **neither** `torrent_file_summary` (uncorrelated `largest_file_size` + deduped `extensions` set) **nor** the torrent-grained Tantivy doc (one multivalued `file_extensions[]` + a single torrent-total `size`) can express. Tantivy 0.26 has **no nested documents** (proven: `tantivy/src/query/boolean_query/boolean_query.rs:421-447`), so the only structure that carries the pairing at file granularity is **one doc per file**.

> **As-deployed context.** Phase 1 (hybrid blob) **is deployed**: blobs are written on the live crawl path and the backfill is verified (0 mismatches); the destructive cutover (D1, `DROP TABLE torrent_files`) is **deferred**. The Phase-3 torrent-grained search sidecar is **WIP and gated** (draft/uncommitted ansible, placeholder image, nothing serving) — neither search index is live today. This phase therefore describes **net-new** work that bolts a second, file-grained index onto that (future) sidecar; none of it is deployed.

Defining choices: indexed **from the 16 GB `files_data` blob** (never the 873 M `torrent_files` rows → future-proof past the deferred `DROP TABLE`); `doc_id = hex(info_hash):file_index`; **per-torrent replace** (`delete_term(info_hash)` + add all N file docs in one commit); denormalize **immutable fields only** — `content_type[]` + `published_at`, **never `seeders`/`leechers`** (scrape-mutable → would force ~52 doc rewrites per torrent per scrape across 873 M docs; staleness eliminated by construction); store only `doc_id` — `path`/`size`/`extension` are hydrated from the blob at serve time, keeping the index ≈ 8–15 GB. **GraphQL only** (`torrentContent.fileSearch`); **direct serve, not shadow** (no exact PG baseline at serve time); Torznab stays torrent-grained. Gated by a new **`SEARCH_FILE_INDEX_ENABLED` (default `false`)**, independent of `SEARCH_ENABLED`. This is a **forward feature, not a live regression** — pre-cutover `torrent_files` still answers per-file SQL.

#### Complete parity is a composition, not a single store

`torrent_files` fused **five workloads** into one 273 GB table — per-file search · per-torrent listing/browse · distinct-torrent collapse · fleet analytics · arbitrary joins. No single low-disk store reproduces all five (the only one that does is the slim PG per-file table, rejected on **+68–92 GB re-bloat** — exactly what the migration removed). Parity is recovered by **decomposing** the workload across purpose-fit stores, each cheap because it carries only what it must:

| Component                                              | Restores                                                                                             | Latency          | Marginal disk        |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | ---------------- | -------------------- |
| **Blob hydration** (`AfterFind`, already paid)         | list a torrent's files; ORDER BY path/size; per-torrent paginate + totalCount; display values        | in-mem           | 0                    |
| **G1 — extension-from-path** (code)                    | per-file `extension` value + filter + sort, **correct for the live (crawl-path) corpus**             | <50 ms           | 0                    |
| **G2 — file-browser-over-blob** (code)                 | `TorrentQuery.files` listing/sort/paginate after `DROP TABLE torrent_files`                          | <50 ms           | 0                    |
| **Timestamp + index-sort hydration** (code)            | per-file `created_at`/`updated_at` ← `torrent.created_at`; `ORDER BY index`                          | <50 ms           | 0                    |
| **File-grained Tantivy index v1**                      | per-file `(ext ∧ size)` filter + exact file-level count/sort/facet; single-file synthesis            | <50 ms           | 8–15 GB              |
| **Per-(torrent,ext) aggregate** `{max,min,count}` (PG) | **exact** distinct-torrent count + keyset deep paging for one-sided thresholds (incumbent collapse)  | <50 ms           | 3–5 GB               |
| **Bucket-size vector** on the aggregate (~16 log₂)     | two-sided distinct-torrent ranges (interior exact; ≤2 boundary buckets → bounded blob-refine)        | <50 ms           | ~1 GB                |
| **`DistinctTorrentCollector`** (Rust, ~150 LOC, gated) | path-FTS / two-sided collapse, exact deep paging ≤ a selectivity cap; over cap → exact-but-slow scan | <50 ms under cap | 0                    |
| **DuckDB-on-blobs**                                    | exact ad-hoc analytics / arbitrary SQL / joins                                                       | 1–10 s           | 0                    |
| **File index v1.1** (opt-in)                           | per-file **path FTS** (**exceeds** the incumbent — the table had none)                               | <50 ms           | +8–18 GB (uncertain) |

**Marginal cost of complete parity ≈ 12–21 GB** (file index 8–15 + aggregate 3–5 + bucket vector ~1; collector / DuckDB / blob / hydration = 0) vs the **273 GB** table eliminated — all under the existing **200 Gi HEL1 PVC** (the file index is a second directory on the same volume as the torrent index). (+v1.1 path FTS → ~20–39 GB, cost-gated on a smoke-sizing pass.) The price is **more moving parts (4 stores + 3 code-only paths) and a deliberate latency split**, not lost capability.

**Verdict** (from [`perfile-search-complete-parity.md`](./dev/perfile-search-complete-parity.md)): complete _functional_ parity is **achievable as a composition after two prerequisite fixes (G1, G2)**, interactively (<50 ms) and exactly, except a handful of explicitly-documented exceptions (below). Unqualified single-store parity is unreachable below "keep `torrent_files`" or the rejected slim PG table — this is the parity-vs-disk curve, made explicit.

#### v1 prerequisites (code-only, 0 GB) — these gate everything

- **G1 — extension-from-path correctness fix.** This is a **live, accumulating data-at-rest defect — not a current browser bug.** `dhtcrawler/persist.go` builds `TorrentFile{…}` with no `Extension` (the dropped `torrent_files.extension` was a PG **generated** column), so **crawl-path blobs are written with an empty per-file `extension` (`e=""`) today**, and the affected fraction grows with every crawl. **Backfilled blobs are correct** (the blob-migration path reads the generated column), so the corpus is split live-vs-backfilled. **It does not affect today's UI:** the per-torrent file browser still reads `torrent_files` directly (G2 not done) and dual-write keeps that table populated, so `TorrentFile.extension` is correct **now**; the torrent-level file-type facet is path-derived (`ExtractUniqueExtensions`) and likewise unaffected. The defect **activates as a regression at two future points**: **(A)** when G2 re-points the browser at the blob — crawl-path rows then surface NULL `extension` (and wrong `ORDER BY extension`); and **(B)** when the file-grained index/aggregate trust the blob's `e` — the `extension` filter then **silently misses every crawl-path torrent** (and DuckDB `GROUP BY extension` is empty for them). It **hides from parity tests run on backfilled data**, and the consistency checker never compares `extension`, so a backfill-only parity gate silently passes (hence the cutover gate must use crawl-path blobs — see below). Fix (0 GB): derive `extension` via `model.FileExtensionFromPath(path)` at **every** build/read site (Go `BuildFileDocuments`, Rust `backfill_files`, blob-read/hydration, aggregate key) — never trust blob `e`; its regex is byte-identical to the old generated column, so this is exact parity. Add an `extension` field to the consistency checker so the class of bug can't re-hide.
- **G2 — re-point `TorrentQuery.files` at the blob.** The per-torrent file browser still runs raw SQL `FROM torrent_files` (`gqlmodel/torrent_files.go` → `search.TorrentFiles`); the file-search spec adds a _different_ surface and does **not** reimplement it, so **post-`DROP TABLE torrent_files` the file browser errors.** Fix (0 GB): reimplement `TorrentQuery.files` over `torrent.FilesData` (`AfterFind` already hydrates `t.Files`) — in-memory `orderBy`/pagination/`totalCount`/`hasNextPage`, PG NULL-ordering for the `extension` sort, and the multi-`infoHash` merge. Depends on G1 + the timestamp/index-sort hydration.
- **Per-(torrent,ext) aggregate** `{max,min,count}` (PG, 3–5 GB) — **promoted to v1**: it _is_ the literal incumbent distinct-torrent collapse capability (`WHERE extension='mkv' AND max_size > 1e9` → exact distinct-torrent result with trivial keyset deep paging + arbitrary joins). Built from the same blob pass as the file-index backfill.

#### v1.5 / v1.1 (additive, gated, after v1)

- **v1.5 — completes distinct-torrent collapse** (and exceeds the incumbent, which never exposed these in search): (a) **bucket-size vector** on the aggregate (~1 GB) for two-sided ranges; (b) **`DistinctTorrentCollector`** (Rust, ~150 LOC, gated by a selectivity cap) for path-FTS / two-sided / over-cap collapse — exact-but-slow above the cap. The "no deep distinct-torrent paging in Tantivy 0.26" floor is **downgraded** to "achievable via a gated custom collector."
- **v1.1 — path FTS (opt-in, +8–18 GB uncertain):** tokenized `path` field for per-file path search, after a `backfill_limit` smoke-sizing pass confirms the text cost and demand. The expensive axis is **path FTS, not the denorm scalars.**

#### Documented exceptions (the residue after G1 + G2)

- **E1** — compound predicate crossing per-file `(ext,size)` AND a **non-denormalized** torrent attribute (resolution/tag/title~/seeders>): no exact _interactive_ single-store answer; exact via DuckDB-on-blobs at 1–10 s. The denorm list (`content_type` + `published_at`) is the boundary of interactive compound parity; widen on demand.
- **E2** — per-file timestamps hydrate from `torrent.created_at` (stable), not independent (the blob never stored them; a pre-existing blob-migration gap, display-only, no timestamp sort/filter exists).
- **E3** — distinct-torrent exactness has a selectivity cap (mirrors the incumbent, which also seq-scanned broad queries); over cap → exact-but-slow scan (default) or opt-in `totalCountIsEstimate=true`.
- **E4** — browse is read-your-write (blob is a synchronous `torrents` column); file **search** lags ingest (post-commit fire-and-forget index write) — standard search posture.
- **E5** (non-functional) — uniqueness enforced by builder correctness (per-torrent replace), not a DB constraint.
- **E6** (parity-neutral) — `over_threshold` files (`save_files_threshold`) absent from blob/index/DuckDB — but `torrent_files` truncated them too; `files_count` holds the true total in both.

#### Tasks

| Task                             | Scope                                                                                                                                                                           | Disk     | Status      |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ----------- |
| G1 extension-from-path fix       | `FileExtensionFromPath` at every build/read/aggregate site (Go + Rust); add `extension` to the consistency checker                                                              | 0 GB     | 📋 (prereq) |
| G2 file-browser-over-blob        | reimplement `TorrentQuery.files` over `torrent.FilesData` (in-mem orderBy/paginate/totalCount/hasNextPage + multi-infoHash)                                                     | 0 GB     | 📋 (prereq) |
| Timestamp + index-sort hydration | per-file `created_at`/`updated_at` ← `torrent.created_at`; resolver re-sorts `.Index`                                                                                           | 0 GB     | 📋          |
| Proto + regen                    | `FileSearchService` (`IndexFiles`/`BatchIndexFiles`/`DeleteFiles`/`SearchFiles`/`GetFileFacets`/`HealthCheck`) in `search.proto`; `task gen-search-proto`                       | 0 GB     | 📋          |
| Rust file index v1               | `file_schema` (no path) + `file_indexer` + `bin/backfill_files` (source = blob) + `server` handlers + 2nd-index wiring in `main` (failure-isolated, own heap)                   | 8–15 GB  | 📋          |
| Go surface v1                    | `BuildFileDocuments` + `FileClient` + **guarded** dual-write (change-gated, fire-and-forget; delete fans out to both services) + `filesearch.Service` + GraphQL + searchfx gate | 0 GB     | 📋          |
| Per-(torrent,ext) aggregate      | `{max,min,count}` keyed `(info_hash, extension)` in PG (built from the blob pass) — exact one-sided distinct-torrent                                                            | 3–5 GB   | 📋          |
| DuckDB-on-blobs                  | decode `files_data` → `(info_hash, index, path, extension, size)` on demand (or periodic Parquet) for ad-hoc SQL                                                                | 0 GB     | 📋          |
| Backfill Job + parity gate       | separate Job (serving Deployment scaled-to-0); **set-equality parity vs CRAWL-PATH blobs** before D1 (see GO/NO-GO)                                                             | —        | 📋          |
| v1.5 collapse                    | bucket-size vector (~1 GB) + `DistinctTorrentCollector` (gated)                                                                                                                 | ~1 GB    | 📋 (v1.5)   |
| v1.1 path FTS                    | tokenized `path` field, after smoke-sizing                                                                                                                                      | +8–18 GB | 📋 (opt-in) |

#### This phase gates Phase 6 — `torrent_files` must not be dropped until per-file parity holds

`torrent_files` is the **exact per-file ground truth** the parity gate compares against, and it is also the table Phase-6/D1 drops. So per-file parity must be **proven before** `blob-migration cleanup --confirm` runs `DROP TABLE torrent_files` (deferred-drop **D1** in the Coexistence registry). The gate is a **set-equality** assertion of `(info_hash, file_index)` between a PG query (`WHERE extension=$e AND size>$s`, ∪ single-file synthesis) and `SearchFiles` (precision = recall = 1.0) — stronger than the torrent index's Jaccard/RBO. **It MUST run against CRAWL-PATH blobs, not backfilled ones** — G1 hides on backfilled data, so a backfill-only gate is a false pass. After D1, `torrent_files` is gone and only index-vs-blob self-consistency remains; there is no second chance to validate against the real table. This phase therefore **adds a precondition to D1**: per-file Tantivy set-equality (on crawl-path blobs) joins `verify --full` 100% blob↔row match as a cutover gate.

**GO/NO-GO (per-file parity, before D1 / Phase-6 cutover):** G1 + G2 merged; file index backfilled from the blob with verified 100% blob coverage (no `files_data`-NULL `files_status=multi` rows; ~16.97 M torrents-with-files reconciled); set-equality `(info_hash, file_index)` parity = 1.0 **against crawl-path blobs**; `fileSearch` + per-torrent file browser validated end-to-end. **If NO-GO:** keep `torrent_files` (dual-write posture is unchanged and safe); the file index runs additively with `SEARCH_FILE_INDEX_ENABLED=false` until parity is proven.

### Phase 6: Tantivy Cutover (Weeks 14-15)

| Task                      | Description                                                             | Estimate |
| ------------------------- | ----------------------------------------------------------------------- | -------- |
| Canary rollout            | 5% → 50% → 100% over 2 weeks                                            | 2 weeks  |
| Remove PG tsvector writes | Stop computing tsvector in Go                                           | 1 day    |
| Drop GIN indexes          | Drop `torrent_contents` tsv GIN index (14 GB) + content tsv GIN (31 MB) | 1 day    |

**GO/NO-GO: Week 13** — Is Tantivy stable at 100%? If yes, proceed to Rust port.

### Phase 7: Classifier Rust Port (Weeks 16-21)

| Task                 | Description                                       | Estimate |
| -------------------- | ------------------------------------------------- | -------- |
| YAML parser          | Port workflow YAML parsing (serde_yaml)           | 3 days   |
| Expression engine    | cel-rust or Rhai replacing CEL                    | 5-7 days |
| Classifier actions   | Content type detection, date parsing, video attrs | 5-7 days |
| TMDB integration     | reqwest HTTP client with rate limiting            | 3 days   |
| Golden file testing  | 10K samples, assert Rust output matches Go        | 3 days   |
| Differential testing | Dual-execute in production, log divergence        | 2 days   |
| Cutover              | Rust consumes queue_jobs directly via SQLx        | 2 days   |

**GO/NO-GO: Week 19** — Rust classifier matches Go output with < 0.1% divergence?

### Phase 8: DHT Crawler Rust Port (Weeks 22-27)

| Task                | Description                                   | Estimate  |
| ------------------- | --------------------------------------------- | --------- |
| DHT protocol        | BEP-5/9/33/51 on tokio UDP                    | 5-7 days  |
| K-table             | BTreeMap-based Kademlia routing               | 3-5 days  |
| Bloom filter        | bitvec-based dedup filter                     | 2 days    |
| MetaInfo requester  | TCP metadata fetch (BEP 9)                    | 3-5 days  |
| Batch persist       | tokio channels replacing Go channels          | 3-5 days  |
| Parallel comparison | Both crawlers running, compare discovery rate | 1-2 weeks |
| Cutover             | Disable Go crawler, Rust handles all DHT      | 2 days    |

**GO/NO-GO: Week 27** — Rust pipeline stable? Can stop here (valid end state).

### Phase 9: API Server Rust Port (Weeks 28-33, Optional)

| Task            | Description                                  | Estimate  |
| --------------- | -------------------------------------------- | --------- |
| GraphQL schema  | async-graphql matching gqlgen schema         | 5-7 days  |
| Torznab API     | axum XML handler                             | 2-3 days  |
| Query builder   | Port 827-line query.go (hardest single task) | 7-10 days |
| API conformance | Captured response fixture testing            | 3-5 days  |
| Cutover         | Full Rust stack                              | 2 days    |

---

## Rust Crate Structure

```
bitmagnet-rs/
├── Cargo.toml                    # workspace root
├── proto/bitmagnet/
│   ├── search.proto
│   ├── classifier.proto
│   └── common.proto
├── crates/
│   ├── bitmagnet-proto/          # generated protobuf (tonic)
│   ├── bitmagnet-model/          # domain models (Torrent, Content, TorrentContent)
│   ├── bitmagnet-db/             # SQLx database access
│   ├── bitmagnet-search/         # Tantivy index + gRPC server
│   │   ├── src/
│   │   │   ├── index.rs          # index management
│   │   │   ├── schema.rs         # field definitions
│   │   │   ├── query.rs          # query translation
│   │   │   ├── facets.rs         # faceted search
│   │   │   └── tokenizer.rs      # custom tokenizer matching TokenizeFlat()
│   ├── bitmagnet-classifier/     # workflow engine (Rhai for expressions)
│   ├── bitmagnet-dht/            # DHT crawler (tokio UDP)
│   ├── bitmagnet-api/            # axum + async-graphql + torznab
│   └── bitmagnet-common/         # shared utilities
└── docker/
    ├── Dockerfile.search
    └── docker-compose.yml
```

---

## Risk Matrix

| Risk                                                  | Phase    | Likelihood | Impact       | Mitigation                                                                                                                                                                                                                     |
| ----------------------------------------------------- | -------- | ---------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Blob migration data loss                              | Phase 1  | Low        | **Critical** | ✅ Mitigated: dual-write keeps `torrent_files` intact during migration; `cleanup` is gated on `verify --full` (100% blob↔row match) + `--confirm`. Rollback = redeploy old image until cutover.                               |
| File type facet divergence after blob migration       | Phase 1  | Medium     | **High**     | ✅ Deferred safely: while `torrent_files` exists (dual-write) the facet `EXISTS` subquery is unchanged. The switch to `file_extensions` JSONB containment happens only at cutover; validate facet parity before `cleanup`.     |
| Insufficient disk for `VACUUM` after cleanup          | Phase 1  | Low        | Medium       | `cleanup` runs `VACUUM` after `DROP TABLE torrent_files`; the dropped table frees 273 GB. Schedule during a low-traffic window.                                                                                                |
| tsvector rebuild produces different lexemes from blob | Phase 1  | Low        | **High**     | ✅ Mitigated: `AfterFind` populates `t.Files` from the blob, so `fileSearchStrings()` reads identical data. Covered by the live consistency checker (field-by-field) + E2E equivalence test (`FileExtensions()` blob-vs-rows). |
| Tokenizer mismatch → search divergence                | Phase 3  | Medium     | **High**     | Custom Tantivy tokenizer replicating TokenizeFlat(); exhaustive testing with real torrent names (CJK, Cyrillic)                                                                                                                |
| CEL → Rhai/cel-rust incompatibility                   | Phase 7  | Medium     | **High**     | Evaluate both engines in week 16; golden file testing on 10K+ samples                                                                                                                                                          |
| Tantivy index > 74 GB                                 | Phase 3  | Medium     | Medium       | Monitor during backfill; reduce STORED fields if needed                                                                                                                                                                        |
| Memory pressure (PG + Tantivy + Go + Rust)            | Phase 4+ | Medium     | Medium       | Tantivy uses mmap (OS-managed); deploy on 64GB+ RAM                                                                                                                                                                            |
| Rust learning curve                                   | Phase 2+ | Medium     | Medium       | Search sidecar (greenfield) builds expertise before porting                                                                                                                                                                    |
| PG schema drift during dual-ownership                 | Phase 4+ | Low        | **High**     | Single migration tool; schema validation in CI                                                                                                                                                                                 |

---

## Go/No-Go Decision Points

| Week | Phase   | Checkpoint            | Criteria                                                                                                                    | If No-Go                                           |
| ---- | ------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| 3    | Phase 1 | Hybrid Blob migration | All search/browse functionality verified; file type facet returns same results; tsvector rebuild produces identical lexemes | Keep `torrent_files` table; investigate divergence |
| 7    | Phase 3 | Tantivy Search MVP    | Backfill completes, index size within estimate                                                                              | Tune schema, reduce fields                         |
| 13   | Phase 6 | Tantivy cutover       | Jaccard > 0.7 @ top-20 for 95%, no latency regression                                                                       | Extend shadow mode, tune tokenizer                 |
| 19   | Phase 7 | Classifier port       | Rust classifier < 0.1% divergence from Go                                                                                   | Keep Go classifier, investigate edge cases         |
| 27   | Phase 8 | DHT port              | Rust crawler discovery rate matches Go ± 5%                                                                                 | Keep Go crawler (valid end state)                  |

---

## Key Integration Points in Go Source

### Phase 1: Hybrid Blob Migration — as-built ([PR #1](https://github.com/dashed/bitmagnet/pull/1))

**Done now (dual-write era):**

| Component               | File(s)                                                                   | What was done                                                                                                   |
| ----------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Blob serializer         | `internal/blobmigration/serializer.go`                                    | `SerializeFiles`/`DeserializeFiles` (msgpack+ZSTD), `ExtractUniqueExtensions`, `BuildFileSummary`               |
| Migration handler + CLI | `internal/blobmigration/queue/`, `internal/app/cmd/blobmigrationcmd/`     | Self-chaining batch migration; `blob-migration {start,status,pause,resume,verify,cleanup}`                      |
| Consistency system      | `internal/blobmigration/consistency/`                                     | `CompareFiles`/live checker/metrics/healthcheck (auto-heal on mismatch)                                         |
| Torrent GORM model      | `internal/model/torrents.gen.go`                                          | Added `FilesData []byte`, `FileExts []string` (`serializer:json`); `Files` relation **kept**                    |
| Transparent read        | `internal/model/torrents.go`                                              | `AfterFind` deserializes blob → `t.Files` via `FilesDataDeserializer` var                                       |
| DHT dual-write          | `internal/dhtcrawler/persist.go`                                          | `createTorrentModel()` sets blob + exts; upsert adds them to `DoUpdates`; **still writes `torrent_files` rows** |
| Schema                  | `migrations/00021`, `00022`                                               | `files_data`, `file_extensions JSONB`, `torrent_file_summary`; GIN `CONCURRENTLY`                               |
| fx wiring               | `internal/app/appfx/module.go`, `internal/blobmigration/blobmigrationfx/` | Register config, queue handler, CLI command, consistency worker + collectors                                    |

**Deferred to cutover (after `cleanup` drops `torrent_files`):**

| Component                   | File                                                     | Line(s) | Change at cutover                                                              |
| --------------------------- | -------------------------------------------------------- | ------- | ------------------------------------------------------------------------------ |
| GraphQL file resolver       | `internal/gql/gqlmodel/torrent_files.go`                 | 25      | `TorrentQuery.Files()` — switch from `torrent_files` SQL to blob decompression |
| File type facet             | `internal/database/search/facet_torrent_file_type.go`    | 12, 41  | Switch from `torrent_files` `EXISTS` to `file_extensions` JSONB containment    |
| File extension criteria     | `internal/database/search/criteria_torrent_file_type.go` | 8       | `TorrentFileTypeCriteria()` — rewrite to JSONB-GIN containment                 |
| `TorrentFile` model removal | `internal/model/torrent_files*.go`                       | —       | Remove model/scopes once `torrent_files` is dropped                            |
| Processor persist           | `internal/processor/persist.go`                          | 59-110  | Unchanged (never wrote `torrent_files`)                                        |

### Phases 2-9: Rust Port (files to integrate with)

| Component        | File                                      | Line(s)          | Purpose                                 |
| ---------------- | ----------------------------------------- | ---------------- | --------------------------------------- |
| tsvector build   | `internal/model/torrent_contents.go`      | 66-106           | UpdateTsv() — weights A/B/C/D           |
| Content tsvector | `internal/model/content.go`               | 83-108           | Content.UpdateTsv()                     |
| Tokenizer        | `internal/database/fts/tokenizer.go`      | —                | TokenizeFlat() — must replicate in Rust |
| tsquery builder  | `internal/database/fts/tsquery.go`        | 9-24             | AppQueryToTsquery()                     |
| DB persist       | `internal/processor/persist.go`           | 59-110           | Hook point for dual-write               |
| Search execution | `internal/database/query/query.go`        | 617-619, 646-647 | ts_rank_cd and tsv @@ tsquery           |
| Search interface | `internal/database/search/search.go`      | 9-15             | Central search interface                |
| 14 facet types   | `internal/database/search/facet_*.go`     | —                | All facet implementations               |
| DHT crawler      | `internal/dhtcrawler/crawler.go`          | 61               | Start() — 15 concurrent pipelines       |
| Classifier       | `internal/classifier/classifier.core.yml` | —                | CEL/YAML workflow definitions           |
| DI root          | `internal/app/appfx/module.go`            | 38-76            | Uber fx module composition              |

---

## Shadow Mode Configuration

```yaml
search:
  engine: postgres # postgres | shadow | canary | tantivy
  tantivy:
    enabled: false
    address: "unix:///var/run/bitmagnet/tantivy.sock"
    shadow:
      sample_rate: 1.0
      log_discrepancies: true
      jaccard_threshold: 0.7
    canary:
      percentage: 0.0
      sticky_sessions: true
    backfill:
      batch_size: 10000
      concurrency: 4
```

---

## Shadow Mode Metrics

**Per-query (structured log):**

- Query string, PG latency, Tantivy latency
- Result counts, Jaccard similarity @ top-20/50
- Rank-Biased Overlap (RBO p=0.9), top-1 match

**Prometheus:**

- `bitmagnet_search_shadow_jaccard_histogram`
- `bitmagnet_search_shadow_rbo_histogram`
- `bitmagnet_search_shadow_latency_ratio`
- `bitmagnet_search_tantivy_index_lag_seconds`
- `bitmagnet_search_tantivy_doc_count`
- `bitmagnet_search_tantivy_index_size_bytes`

**Phase transition thresholds:**

- Shadow → Canary: Jaccard > 0.7 @ top-20 for 95% of queries, RBO > 0.8
- Canary → Full: No p99 latency regression, error rate < 0.1%

---

## External References

- [PR #1 — Hybrid Blob migration](https://github.com/dashed/bitmagnet/pull/1) — Phase 1 implementation (12 commits, ~5.6k LOC, 42 unit + 4 E2E tests)
- [Live migration design](./live-migration-design.md) — zero-downtime dual-write + `AfterFind` + queue migration architecture
- [Space-savings verification](./space-savings-verification.md) + [`verify_space_savings.py`](./verify_space_savings.py) — Python verification against production data
- [Database analysis](./bitmagnet-database-analysis.md) — 368 GB PG analysis (live measurements 2026-05-28)
- [bitmagnet source](https://github.com/bitmagnet-io/bitmagnet) — Go, MIT license
- [Tantivy](https://github.com/quickwit-oss/tantivy) — Rust, MIT license
- [tantivy-go](https://github.com/anyproto/tantivy-go) — Go FFI bindings (rejected, see above)
- Discord Go→Rust migration, Vinted ES→Vespa shadow traffic, InfluxData strangler fig pattern

---

## Coexistence / Keep-Everything Mode (no drops)

The phased plan above is written as "migrate → **cutover (drop)** → reclaim." But every phase is _also_ deployable in **keep-everything mode**: run dual-write + Tantivy + BEP-52 v2 while dropping **no** table, trading the disk reclaim for full reversibility. This is the recommended posture for a reversibility-first operator until each cutover is explicitly approved.

**Why it works:** every **new forward-path** migration (`00021`, `00022`, and the rewritten `00023` below) is **additive — zero Up-section `DROP`**. (Historical `00002`–`00020` carry Up-section drops, but they were applied long ago; baseline `main` ships ≤`00020`.) P2/P3/P4 add **no migrations at all** (`00021`+`00022` only); Tantivy's index is **out-of-Postgres**, so adopting it never touches the PG schema. The `SearchRouter` defaults to `engine=postgres`, so Tantivy runs _alongside_ PG FTS (shadow/canary) until you flip it.

**Deferred-drops registry — the complete set of destructive steps (none run in keep-everything mode):**

| #      | Trigger                                                                                                                  | Frees                                                              | Gate                                                                                                    |
| ------ | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| D0a/b  | `DROP INDEX torrent_files_{size,extension}_idx` (0 scans)                                                                | 8.2 + 5.8 GB                                                       | manual                                                                                                  |
| **D1** | `blob-migration cleanup --confirm` → **`DROP TABLE torrent_files`** + facet→JSONB + blob-only reads                      | **276 GB** (DB ~397→~121 GB; `DROP TABLE` frees to OS immediately) | `verify --full` 100% + 0 unmigrated + verify <24 h + `--confirm` + backup. **First irreversible step.** |
| D2     | Phase-6 Tantivy cutover → `DROP INDEX torrent_contents` tsv GIN                                                          | 14 GB                                                              | Tantivy @100% canary, Jaccard >0.7@top-20/95%, no latency regression                                    |
| —      | 🚨 **keep** `content.tsv` GIN (31 MB, **650K scans, actively used**) — Tantivy replaces `torrent_contents` FTS, not this | —                                                                  | —                                                                                                       |

**Disk (order matters):** keep-everything **grows** the DB (every copy is additive) and Tantivy adds **+40–78 GB** → ~440–475 GB with no cutover; cutover D1 first → then Tantivy → ~160–200 GB. The headline win is the **276 GB `torrent_files` drop (D1)**; Tantivy alone is ~disk-neutral (it offsets the 14 GB GIN it lets you drop). With ~1.1 TB free, carrying both copies indefinitely is fine.

**Write amplification:** persist fans out to up to four sinks — `torrent_files` rows · `files_data` blob+`file_extensions` (same tx) · Tantivy `IndexDocument` (post-commit, **fire-and-forget**, no-op when `SEARCH_ENABLED=false`) · `info_hash_v1/v2`+`meta_version` columns. All additive; none on the served read path. _(Optional 5th sink with the file-grained index (Phase 5.5): a `BatchIndexFiles` per torrent_content — post-commit, fire-and-forget, no-op when disabled, **once per ingest/re-classification — NOT per scrape**, because only immutable fields are denormalized.)_

**Rollback:** "redeploy the old image" works right up until the **first** drop (old goose sees ≤`00020` → no-op; GORM ignores unknown columns/tables). After D1 the system is forward-only. 🚫 never `goose down` (its Down drops `files_data` etc.).

## BEP-52 v2 deploy prerequisite: rewrite `00023` (live-safe)

`migrations/00023_v2_infohash.sql` as written is **not deployable on the live 48M-row DB** — it runs, in one startup goose run that gates `/status`: instant `ADD COLUMN` (fine) + **non-concurrent** `CREATE INDEX` ×2 (SHARE-locks the table, blocks the crawler) + `UPDATE torrents SET info_hash_v1=info_hash, meta_version=1` over **all ~48M rows** (48M dead tuples, WAL blowout, long txn). It exceeds the readiness window → **CrashLoopBackOff**. Rewrite it with the Phase-1 playbook (instant DDL + `CONCURRENTLY` + operational batched backfill):

1. **`00023` (transactional, DDL only):** keep just the three `ADD COLUMN` (NULLable, no volatile DEFAULT → metadata-only, instant). Remove the `CREATE INDEX` and `UPDATE` blocks.
2. **New `00024_v2_infohash_indexes.sql` (`-- +goose NO TRANSACTION`, mirrors `00022`):**
   `CREATE INDEX CONCURRENTLY IF NOT EXISTS torrents_info_hash_v2_idx ON torrents (info_hash_v2) WHERE info_hash_v2 IS NOT NULL` — **partial**, since the only filter is the dedup lookup `info_hash_v2 IN (…)` (`internal/dhtcrawler/persist.go:351`), which never matches NULL → the planner uses it and the index holds only hybrid/v2 rows. **Defer the `info_hash_v1` index** — no query filters that column on the v2 branches.
3. **Operational `v2-backfill` CLI** (clone `blobmigrationcmd`'s KV-cursor/status/pause-resume skeleton; **a simple single-keyset loop, NOT the parallel range-worker infra** — the per-row work is a server-side set UPDATE, so workers/zstd are pure overhead): keyset on the PK, per page `UPDATE torrents SET info_hash_v1=info_hash, meta_version=1 WHERE info_hash > $cur AND info_hash <= $last AND meta_version IS NULL` (the `meta_version IS NULL` guard ⇒ idempotent/resumable), batch ~5–20k, sleep between, **runs after startup — never gates `/status`** (~4,800 batches).
4. **No transitional guards needed — read paths are already NULL-tolerant**, so the backfill is for _completeness_, not correctness: `MagnetURI` (`internal/model/torrents.go:99-119`) falls back to the PK `info_hash` when `info_hash_v1`/`v2` are NULL; the dedup `IN (…)` skips NULL rows; GraphQL `infoHashV2`/`metaVersion` are nullable. New crawler rows are **born complete** (`persist.go:243-245` ← `protocol/metainfo/parse.go:54-66`); only pre-existing (and importer-path) rows stay NULL until the backfill, all covered by the PK fallback.
