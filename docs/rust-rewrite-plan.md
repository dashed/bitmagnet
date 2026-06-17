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

| Phase                              | Status                                                                                | Notes                                                                                                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Phase 0: Quick wins                | 📋 Planned                                                                            | Index drops are operational (run against the live DB), not code                                                                                                                                                                                                                                                                                                                                            |
| **Phase 1: Hybrid Blob migration** | ✅ **Implemented** (code merged, [PR #1](https://github.com/dashed/bitmagnet/pull/1)) | Built as a **zero-downtime live migration** (see [`live-migration-design.md`](./live-migration-design.md)) — dual-write + `AfterFind` hook + queue-based migration + live consistency verification + operational CLI. 42 unit tests + 4 E2E tests. **Destructive cutover (drop `torrent_files`, switch facet to JSONB containment) is deferred behind the `cleanup` safety gate and has not run.**         |
| **Phase 2: Rust infrastructure**   | ✅ **Implemented** (branch `feat/rust-infrastructure`, stacked on PR #1)              | `bitmagnet-rs/` Cargo workspace (5 crates: proto, common, model, db, search), `bitmagnet.v1` protobuf (tonic 0.14 / `tonic-prost`), gRPC server skeleton (all RPCs stubbed except HealthCheck), Tantivy 0.26 schema, multi-stage `Dockerfile.search` (built, 103 MB, runs non-root), `.github/workflows/rust.yml` CI. 38 tests pass incl. **Go↔Rust blob wire-compat fixtures**. Domain logic is Phase 3. |
| Phases 3-9: Tantivy + Rust port    | 📋 Planned                                                                            | Unchanged below                                                                                                                                                                                                                                                                                                                                                                                            |

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

### Phase 3: Tantivy Search Sidecar MVP (Weeks 4-7)

Now indexes from blob data (smaller source, faster backfill since blobs are ~16 GB vs 273 GB of `torrent_files` rows).

| Task              | Description                                                                              | Estimate | Depends On        |
| ----------------- | ---------------------------------------------------------------------------------------- | -------- | ----------------- |
| Index schema      | All field types mapped from PG model                                                     | 2 days   | Workspace         |
| Custom tokenizer  | Replicate TokenizeFlat() in Rust                                                         | 3-5 days | —                 |
| gRPC server       | tonic server: IndexDoc, BatchIndex, Delete, Search, Facets                               | 3 days   | Proto             |
| Query translation | PG tsquery → Tantivy BooleanQuery with field boosts                                      | 3 days   | Schema, Tokenizer |
| Faceted search    | 14 facet types from bitmagnet                                                            | 3 days   | Schema            |
| Aggregations      | Facet counts, range aggregations                                                         | 2 days   | Facets            |
| Index management  | Merge policy, warmers, graceful shutdown                                                 | 2 days   | gRPC server       |
| Backfill CLI      | Stream from PG (now reads compressed blobs — faster), batch-index (~60 min for 48M docs) | 2 days   | All above         |

### Phase 4: Shadow Mode Go Integration (Weeks 8-10)

| Task               | Description                                                            | Estimate | Depends On   |
| ------------------ | ---------------------------------------------------------------------- | -------- | ------------ |
| gRPC client        | Go client for Tantivy sidecar                                          | 1 day    | Phase 3      |
| Dual-write         | Async index after PG commit in persist.go                              | 2 days   | gRPC client  |
| SearchRouter       | Shadow/canary/tantivy_only modes                                       | 3 days   | gRPC client  |
| Comparator         | Jaccard similarity, RBO, top-1 match                                   | 2 days   | SearchRouter |
| Prometheus metrics | Latency ratio, jaccard/RBO histograms, index lag                       | 1 day    | Comparator   |
| Configuration      | search.engine, tantivy.address, shadow settings                        | 1 day    | —            |
| fx DI wiring       | Wire into Uber fx module system (`internal/app/appfx/module.go:38-76`) | 1 day    | All above    |

### Phase 5: Shadow Mode Validation (Weeks 11-13)

| Task                | Description                               | Estimate  |
| ------------------- | ----------------------------------------- | --------- |
| Production backfill | Index 48M torrents from PG                | 1-2 days  |
| Shadow mode run     | 2-3 weeks collecting comparison metrics   | 2-3 weeks |
| Tokenizer tuning    | Fix divergences found during shadow mode  | 1-3 days  |
| Quality gate        | Jaccard > 0.7 @ top-20 for 95% of queries | —         |

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
