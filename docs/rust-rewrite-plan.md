# Bitmagnet Rust Rewrite Plan: Hybrid Blob Migration & Tantivy Integration

**Status:** Investigation complete, planning phase  
**Date:** 2026-05-28 (updated with Python-verified production data)  
**Branch:** `feat/rust-rewrite-plan`

---

## Executive Summary

Optimize and rewrite bitmagnet in phases, starting with a **Hybrid Blob migration** that eliminates the 273 GB `torrent_files` table (74% of the database), followed by a **Tantivy search sidecar** and incremental Rust port. Each phase is independently valuable — the project can stop at any checkpoint and still deliver meaningful improvements.

**Key finding from live database analysis (2026-05-28, Python-verified):** Tantivy alone is roughly **disk-neutral** — it replaces ~39 GB of PG FTS data but adds 39-78 GB as its own index. The real space savings come from **Hybrid Blob migration** (368 GB → ~128 GB, a 66% reduction) by replacing 873M individual file rows with ZSTD-compressed blobs per torrent. Only 16.8M of 48M torrents (35%) actually have file data — a critical finding that keeps blob sizes at ~16 GB. Combined with Tantivy and ZFS, total storage drops to 45-56 GB (85% reduction).

**Key architectural decision:** Use a **Rust gRPC sidecar** (not tantivy-go FFI) for search. The tantivy-go bindings lack numeric fields, faceted search, aggregations, and are pinned to Tantivy 0.22 (upstream is 0.26). A gRPC sidecar provides full Tantivy access and establishes the first Rust component of the port.

**Timeline:** ~33 weeks for full port. First value at **week 0** (drop unused indexes, 14-29 GB), biggest impact at **week 3** (Hybrid Blob complete, 368 → ~132 GB), search upgrade at week 7 (Tantivy sidecar).

---

## Disk Savings Assessment

Live production database measured 2026-05-28: **368 GB** (395,623,308,311 bytes) across 48M torrents and 873M file rows. All estimates below **verified with Python** against real sampled data (see `docs/space-savings-verification.md` and `docs/verify_space_savings.py`).

### Current Database Breakdown

| Table | Total | Data | Indexes | Rows | % of DB |
|---|---|---|---|---|---|
| torrent_files | **273 GB** | 119 GB | 155 GB | 873M | 74% |
| torrent_contents | **61 GB** | 21 GB (+12 GB TOAST) | 28 GB | 48M | 17% |
| torrents_torrent_sources | **19 GB** | 8 GB | 11 GB | 75M | 5% |
| torrents | **14 GB** | 7 GB | 7 GB | 48M | 4% |
| Other | **1 GB** | — | — | — | <1% |

Key findings:
- **Only 16.8M of 48M torrents (35%) have file data** in `torrent_files` — critical for blob sizing
- **31 unused indexes** (0 scans since last stats reset) consuming **29 GB** total
- `torrent_files` has 2 unused indexes: `size_idx` (8.2 GB, 0 scans) and `extension_idx` (5.8 GB, 0 scans)
- `torrent_contents.tsv` GIN index: 14 GB (359 scans) — the FTS workhorse Tantivy replaces
- `content.tsv` GIN index: 31 MB (650K scans) — actively used, keep
- tsvector data is **72.1%** of `torrent_contents` row size (~408 bytes avg), 12 GB in TOAST overflow

### Savings by Scenario (Python-verified)

| Scenario | PG Size | Tantivy | Total | Savings | Effort |
|---|---|---|---|---|---|
| **Current** | 368 GB | — | **368 GB** | — | — |
| Quick wins only | 339-354 GB | — | **339-354 GB** | 4-8% | Trivial |
| Tantivy only | 329 GB | 39-78 GB | **368-407 GB** | ~0% | 10 weeks |
| **Hybrid Blob only (PG)** | **~128 GB** | — | **~128 GB** | **66%** | **2-3 weeks** |
| Hybrid Blob + Tantivy | ~93 GB | 39-78 GB | **132-171 GB** | 54-64% | 12+ weeks |
| **Everything + ZFS** | **~37 GB** | **16-31 GB** | **53-68 GB** | **82-86%** | **13+ weeks** |

### Hybrid Blob Disk Accounting (Python-verified)

| Component | Current | After Blob Migration | Change | Verification |
|---|---|---|---|---|
| torrent_files (data + indexes) | **273 GB** | **0 GB** | -273 GB eliminated | — |
| File blobs (ZSTD L3 msgpack) | 0 | **16.2 GB** | +16.2 GB | ✅ Measured: 1.0 KB avg blob, 16.8M torrents with files |
| `file_extensions TEXT[]` + GIN | 0 | **5.6-9.4 GB** | +5.6-9.4 GB | ✅ Measured: 3.1 avg extensions/torrent |
| `torrent_file_summary` table | 0 | **10.4-14.1 GB** | +10.4-14.1 GB | ✅ Measured: 116 bytes avg row |
| torrent_contents | 61 GB | 61 GB | unchanged | — |
| torrents_torrent_sources | 19 GB | 19 GB | unchanged | — |
| torrents (base table) | 14 GB | 14 GB | unchanged | — |
| Other tables | 1 GB | 1 GB | unchanged | — |
| **Database total** | **368 GB** | **~127-135 GB** | **-233 to -241 GB** | **66% reduction** |

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

| Feature Needed | tantivy-go | Rust Sidecar |
|---|---|---|
| Text search with field boosting | ✅ | ✅ |
| Numeric fields (seeders, size sorting) | ❌ | ✅ |
| Date fields (published_at) | ❌ | ✅ |
| Faceted search (14 facet types) | ❌ | ✅ |
| Aggregations (facet counts) | ❌ | ✅ |
| Range queries | ❌ | ✅ |
| Latest Tantivy (0.26) | ❌ (0.22) | ✅ |
| Build complexity | CGo + Rust FFI | Standard Rust binary |
| Reusability for Rust port | Throwaway | Foundation |

---

## Tantivy Index Schema

### Field Mapping (PG → Tantivy)

| PG Source | tsvector Weight | Tantivy Field | Type | Flags | Query Boost |
|---|---|---|---|---|---|
| info_hash | A | `info_hash` | Bytes | STORED + INDEXED | — (exact match) |
| torrent name | A | `torrent_name` | Text | STORED + INDEXED | 4.0 |
| content title | A | `content_title` | Text | STORED + INDEXED | 4.0 |
| original title | A | `original_title` | Text | INDEXED | 4.0 |
| release year | B | `release_year` | U64 | FAST + INDEXED | 2.0 |
| video resolution | C | `video_resolution` | Text | FAST + INDEXED | 1.5 |
| video source/codec | C | `video_source`, `video_codec` | Text | FAST + INDEXED | 1.5 |
| genres | D | `genres` | Text | INDEXED | 0.5 |
| file paths | D | `file_paths` | Text | INDEXED | 0.5 |
| content_type | — | `content_type` | Facet | — | — (filter only) |
| seeders | — | `seeders` | U64 | FAST | — (sort only) |
| leechers | — | `leechers` | U64 | FAST | — (sort only) |
| size | — | `size` | U64 | FAST + INDEXED | — (sort/filter) |
| files_count | — | `files_count` | U64 | FAST | — (sort only) |
| published_at | — | `published_at` | Date | FAST + INDEXED | — (sort/filter) |
| languages | — | `languages` | Text | FAST | — (facet) |
| file_extensions | — | `file_extensions` | Text | FAST | — (facet) |

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

| Task | Description | Savings | Risk |
|---|---|---|---|
| Drop `torrent_files_size_idx` | 8.2 GB, 0 scans since stats reset | 8.2 GB | Low — no queries use this index |
| Drop `torrent_files_extension_idx` | 5.8 GB, 0 scans since stats reset | 5.8 GB | Low — facet uses EXISTS on `extension` column, not this index |
| Audit remaining unused indexes | 31 indexes with 0 scans = 29 GB total | up to 15 GB more | Medium — some may be needed by query planner for faceted search |
| `VACUUM ANALYZE` bloated tables | `torrents_torrent_sources` has 19.2% dead tuples (1.3 GB) | ~1.3 GB | None |
| Tune autovacuum for large tables | Set `autovacuum_vacuum_scale_factor = 0.01` on `torrent_files` (871M rows × 0.2 = 174M dead rows before vacuum triggers) | Prevents future bloat | None |

**Conservative estimate: 14 GB immediate. Aggressive: up to 29 GB.**

> **Note on unused index safety:** Only drop indexes that are confirmed unused via `pg_stat_user_indexes.idx_scan = 0` AND are not the sole access path for a query pattern. The `torrent_files` size and extension indexes are safe — the file type facet uses `EXISTS (... AND extension IN (...))` which hits the composite PK or unique index, not these standalone indexes.

### Phase 1: Hybrid Blob Migration (Weeks 1-3, Go fork)

The highest-ROI change: replace 873M individual `torrent_files` rows (273 GB) with one ZSTD-compressed MessagePack blob per torrent. **Estimated savings: ~236 GB (368 → ~132 GB).**

#### 1a. Schema Changes

| Task | Description | Estimate |
|---|---|---|
| Add `files_data BYTEA` column | ZSTD-compressed MessagePack blob on `torrents` table. Each blob holds all files for one torrent (~18 files avg, ~300-500 bytes compressed). Stored in TOAST. | 1 day |
| Add `file_extensions TEXT[]` column | Array of unique file extensions per torrent on `torrents` table. GIN-indexed for facet queries. Replaces `EXISTS` subquery on `torrent_files`. | 1 day |
| Add `torrent_file_summary` table | `(torrent_id, file_count, total_size, extensions, largest_file_size, has_video, has_subtitle)` — covers 90% of filter/facet queries without decompressing blobs. | 1 day |

#### 1b. Application Changes (Go fork or patch set)

| Task | File(s) | Description | Estimate |
|---|---|---|---|
| Modify DHT persist path | `internal/dhtcrawler/persist.go:150-185` | `createTorrentModel()` currently builds `[]TorrentFile` from `metainfo.Info`. Change to serialize file list as MessagePack → ZSTD compress → store as `files_data` blob. Populate `file_extensions` array. Stop creating `TorrentFile` rows. | 2 days |
| Modify DHT batch insert | `internal/dhtcrawler/persist.go:99-116` | `runPersistTorrents()` currently INSERTs `torrent_files` in batches (lines 111-116). Remove this block; blob is written with the torrent row. | 1 day |
| Update Torrent GORM model | `internal/model/torrents.gen.go:16`, `internal/model/torrents.go` | Add `FilesData []byte` and `FileExtensions []string` fields. Remove `Files []TorrentFile` relation. Add blob serialization/deserialization helpers. | 1 day |
| Update GraphQL file resolver | `internal/gql/gqlmodel/torrent_files.go:25` | `TorrentQuery.Files()` currently calls `t.Search.TorrentFiles()`. Change to decompress blob from `torrents.files_data` and return file structs in-memory. Pagination becomes in-memory slice. | 1 day |
| Update file type facet | `internal/database/search/facet_torrent_file_type.go`, `criteria_torrent_file_type.go` | `TorrentFileTypeCriteria()` currently delegates to `TorrentFileExtensionCriteria()` which does `EXISTS` on `torrent_files`. Change to use `torrents.file_extensions && ARRAY[...]` (GIN-indexed array containment). | 1 day |
| Update tsvector construction | `internal/model/torrent_contents.go:101-103`, `internal/model/torrents.go:186` | `UpdateTsv()` calls `t.Torrent.fileSearchStrings()` which reads from `t.Torrent.Files`. Change `fileSearchStrings()` to deserialize from `files_data` blob instead. | 1 day |
| Remove `TorrentFile` model usage | `internal/model/torrent_files.go`, `internal/model/torrent_files.gen.go` | `BeforeCreate` hook sets `ON CONFLICT DO NOTHING` for `torrent_files` INSERT. Model and related GORM scopes can be removed after migration. | 1 day |

#### 1c. Backfill Migration

| Task | Description | Estimate |
|---|---|---|
| Write backfill script | Stream existing `torrent_files` grouped by `info_hash` → serialize as MessagePack → ZSTD compress → write to `torrents.files_data`. Batch 1000 torrents per transaction. Also populate `file_extensions` and `torrent_file_summary`. | 2 days |
| Run backfill | 48M torrents × ~18 files avg. Estimate 4-8 hours at 1000 torrents/sec (I/O bound on reading 273 GB of `torrent_files`). | 1 day |
| Verify completeness | Assert every torrent with `files_status != 'no_info'` has a non-null `files_data` blob. Spot-check decompressed blobs match original rows. | 0.5 day |
| Drop `torrent_files` table | `DROP TABLE torrent_files;` — reclaims 273 GB | 1 min |
| `VACUUM FULL torrents` | Reclaim TOAST space, compact table after adding blob column. Requires brief downtime + temporary disk (~2x torrents table = ~28 GB). | 1-2 hours |

**GO/NO-GO: Week 3** — All search/browse functionality verified? File type facet returns same results? tsvector rebuild produces identical lexemes? If yes, drop `torrent_files` and proceed.

### Phase 2: Rust Infrastructure (Weeks 3-4, can overlap with Phase 1)

| Task | Description | Estimate |
|---|---|---|
| Rust workspace | Cargo workspace with crates: proto, model, search, common | 1 day |
| Protobuf schema | search.proto (IndexDoc, Search, Facets RPCs) | 1 day |
| CI/CD | Docker multi-stage build, cargo test/clippy/fmt in CI | 1 day |

### Phase 3: Tantivy Search Sidecar MVP (Weeks 4-7)

Now indexes from blob data (smaller source, faster backfill since blobs are ~16 GB vs 273 GB of `torrent_files` rows).

| Task | Description | Estimate | Depends On |
|---|---|---|---|
| Index schema | All field types mapped from PG model | 2 days | Workspace |
| Custom tokenizer | Replicate TokenizeFlat() in Rust | 3-5 days | — |
| gRPC server | tonic server: IndexDoc, BatchIndex, Delete, Search, Facets | 3 days | Proto |
| Query translation | PG tsquery → Tantivy BooleanQuery with field boosts | 3 days | Schema, Tokenizer |
| Faceted search | 14 facet types from bitmagnet | 3 days | Schema |
| Aggregations | Facet counts, range aggregations | 2 days | Facets |
| Index management | Merge policy, warmers, graceful shutdown | 2 days | gRPC server |
| Backfill CLI | Stream from PG (now reads compressed blobs — faster), batch-index (~60 min for 48M docs) | 2 days | All above |

### Phase 4: Shadow Mode Go Integration (Weeks 8-10)

| Task | Description | Estimate | Depends On |
|---|---|---|---|
| gRPC client | Go client for Tantivy sidecar | 1 day | Phase 3 |
| Dual-write | Async index after PG commit in persist.go | 2 days | gRPC client |
| SearchRouter | Shadow/canary/tantivy_only modes | 3 days | gRPC client |
| Comparator | Jaccard similarity, RBO, top-1 match | 2 days | SearchRouter |
| Prometheus metrics | Latency ratio, jaccard/RBO histograms, index lag | 1 day | Comparator |
| Configuration | search.engine, tantivy.address, shadow settings | 1 day | — |
| fx DI wiring | Wire into Uber fx module system (`internal/app/appfx/module.go:38-76`) | 1 day | All above |

### Phase 5: Shadow Mode Validation (Weeks 11-13)

| Task | Description | Estimate |
|---|---|---|
| Production backfill | Index 48M torrents from PG | 1-2 days |
| Shadow mode run | 2-3 weeks collecting comparison metrics | 2-3 weeks |
| Tokenizer tuning | Fix divergences found during shadow mode | 1-3 days |
| Quality gate | Jaccard > 0.7 @ top-20 for 95% of queries | — |

### Phase 6: Tantivy Cutover (Weeks 14-15)

| Task | Description | Estimate |
|---|---|---|
| Canary rollout | 5% → 50% → 100% over 2 weeks | 2 weeks |
| Remove PG tsvector writes | Stop computing tsvector in Go | 1 day |
| Drop GIN indexes | Drop `torrent_contents` tsv GIN index (14 GB) + content tsv GIN (31 MB) | 1 day |

**GO/NO-GO: Week 13** — Is Tantivy stable at 100%? If yes, proceed to Rust port.

### Phase 7: Classifier Rust Port (Weeks 16-21)

| Task | Description | Estimate |
|---|---|---|
| YAML parser | Port workflow YAML parsing (serde_yaml) | 3 days |
| Expression engine | cel-rust or Rhai replacing CEL | 5-7 days |
| Classifier actions | Content type detection, date parsing, video attrs | 5-7 days |
| TMDB integration | reqwest HTTP client with rate limiting | 3 days |
| Golden file testing | 10K samples, assert Rust output matches Go | 3 days |
| Differential testing | Dual-execute in production, log divergence | 2 days |
| Cutover | Rust consumes queue_jobs directly via SQLx | 2 days |

**GO/NO-GO: Week 19** — Rust classifier matches Go output with < 0.1% divergence?

### Phase 8: DHT Crawler Rust Port (Weeks 22-27)

| Task | Description | Estimate |
|---|---|---|
| DHT protocol | BEP-5/9/33/51 on tokio UDP | 5-7 days |
| K-table | BTreeMap-based Kademlia routing | 3-5 days |
| Bloom filter | bitvec-based dedup filter | 2 days |
| MetaInfo requester | TCP metadata fetch (BEP 9) | 3-5 days |
| Batch persist | tokio channels replacing Go channels | 3-5 days |
| Parallel comparison | Both crawlers running, compare discovery rate | 1-2 weeks |
| Cutover | Disable Go crawler, Rust handles all DHT | 2 days |

**GO/NO-GO: Week 27** — Rust pipeline stable? Can stop here (valid end state).

### Phase 9: API Server Rust Port (Weeks 28-33, Optional)

| Task | Description | Estimate |
|---|---|---|
| GraphQL schema | async-graphql matching gqlgen schema | 5-7 days |
| Torznab API | axum XML handler | 2-3 days |
| Query builder | Port 827-line query.go (hardest single task) | 7-10 days |
| API conformance | Captured response fixture testing | 3-5 days |
| Cutover | Full Rust stack | 2 days |

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

| Risk | Phase | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| Blob migration data loss | Phase 1 | Low | **Critical** | Backfill with verification; keep `torrent_files` until 100% blob coverage confirmed; spot-check decompressed blobs against original rows |
| File type facet divergence after blob migration | Phase 1 | Medium | **High** | Compare `file_extensions TEXT[]` facet results against current `EXISTS` subquery on `torrent_files` for a sample of 10K queries before dropping old table |
| Insufficient disk for `VACUUM FULL` | Phase 1 | Low | Medium | `VACUUM FULL torrents` needs ~2x table size temporarily (~28 GB). Current PVC has ~132 GB free after blob migration. Schedule during low-traffic window |
| tsvector rebuild produces different lexemes from blob | Phase 1 | Low | **High** | `fileSearchStrings()` must produce identical output from deserialized blob as from `[]TorrentFile`. Test with golden file comparison on 10K torrents |
| Tokenizer mismatch → search divergence | Phase 3 | Medium | **High** | Custom Tantivy tokenizer replicating TokenizeFlat(); exhaustive testing with real torrent names (CJK, Cyrillic) |
| CEL → Rhai/cel-rust incompatibility | Phase 7 | Medium | **High** | Evaluate both engines in week 16; golden file testing on 10K+ samples |
| Tantivy index > 74 GB | Phase 3 | Medium | Medium | Monitor during backfill; reduce STORED fields if needed |
| Memory pressure (PG + Tantivy + Go + Rust) | Phase 4+ | Medium | Medium | Tantivy uses mmap (OS-managed); deploy on 64GB+ RAM |
| Rust learning curve | Phase 2+ | Medium | Medium | Search sidecar (greenfield) builds expertise before porting |
| PG schema drift during dual-ownership | Phase 4+ | Low | **High** | Single migration tool; schema validation in CI |

---

## Go/No-Go Decision Points

| Week | Phase | Checkpoint | Criteria | If No-Go |
|---|---|---|---|---|
| 3 | Phase 1 | Hybrid Blob migration | All search/browse functionality verified; file type facet returns same results; tsvector rebuild produces identical lexemes | Keep `torrent_files` table; investigate divergence |
| 7 | Phase 3 | Tantivy Search MVP | Backfill completes, index size within estimate | Tune schema, reduce fields |
| 13 | Phase 6 | Tantivy cutover | Jaccard > 0.7 @ top-20 for 95%, no latency regression | Extend shadow mode, tune tokenizer |
| 19 | Phase 7 | Classifier port | Rust classifier < 0.1% divergence from Go | Keep Go classifier, investigate edge cases |
| 27 | Phase 8 | DHT port | Rust crawler discovery rate matches Go ± 5% | Keep Go crawler (valid end state) |

---

## Key Integration Points in Go Source

### Phase 1: Hybrid Blob Migration (files to modify)

| Component | File | Line(s) | Change Required |
|---|---|---|---|
| DHT torrent model builder | `internal/dhtcrawler/persist.go` | 150-185 | `createTorrentModel()` — serialize file list as blob instead of `[]TorrentFile` |
| DHT batch persist | `internal/dhtcrawler/persist.go` | 99-116 | `runPersistTorrents()` — remove `torrent_files` INSERT block (lines 111-116) |
| Torrent GORM model | `internal/model/torrents.gen.go` | 16 | Add `FilesData []byte`, `FileExtensions []string` fields |
| Torrent business logic | `internal/model/torrents.go` | 107, 186 | `FileExtensions()` and `fileSearchStrings()` — read from blob instead of `Files` relation |
| TorrentFile model | `internal/model/torrent_files.go` | 11 | `BeforeCreate` hook — model can be removed after migration |
| TorrentFile generated model | `internal/model/torrent_files.gen.go` | 16 | `TorrentFile` struct — model can be removed after migration |
| GraphQL file resolver | `internal/gql/gqlmodel/torrent_files.go` | 25 | `TorrentQuery.Files()` — decompress blob instead of SQL query |
| File type facet | `internal/database/search/facet_torrent_file_type.go` | 12, 41 | Use `torrents.file_extensions` array containment instead of `torrent_files` EXISTS |
| File extension criteria | `internal/database/search/criteria_torrent_file_type.go` | 8 | `TorrentFileTypeCriteria()` — rewrite to use GIN-indexed `TEXT[]` |
| tsvector construction | `internal/model/torrent_contents.go` | 101-103 | `UpdateTsv()` calls `fileSearchStrings()` — must deserialize blob |
| File type enum | `internal/model/file_type.go` | 19, 143 | `FileType`, `FileTypeFromExtension()` — unchanged but referenced by facet |
| Processor persist | `internal/processor/persist.go` | 59-110 | Classification persist — unchanged (doesn't write `torrent_files`) |

### Phases 2-9: Rust Port (files to integrate with)

| Component | File | Line(s) | Purpose |
|---|---|---|---|
| tsvector build | `internal/model/torrent_contents.go` | 66-106 | UpdateTsv() — weights A/B/C/D |
| Content tsvector | `internal/model/content.go` | 83-108 | Content.UpdateTsv() |
| Tokenizer | `internal/database/fts/tokenizer.go` | — | TokenizeFlat() — must replicate in Rust |
| tsquery builder | `internal/database/fts/tsquery.go` | 9-24 | AppQueryToTsquery() |
| DB persist | `internal/processor/persist.go` | 59-110 | Hook point for dual-write |
| Search execution | `internal/database/query/query.go` | 617-619, 646-647 | ts_rank_cd and tsv @@ tsquery |
| Search interface | `internal/database/search/search.go` | 9-15 | Central search interface |
| 14 facet types | `internal/database/search/facet_*.go` | — | All facet implementations |
| DHT crawler | `internal/dhtcrawler/crawler.go` | 61 | Start() — 15 concurrent pipelines |
| Classifier | `internal/classifier/classifier.core.yml` | — | CEL/YAML workflow definitions |
| DI root | `internal/app/appfx/module.go` | 38-76 | Uber fx module composition |

---

## Shadow Mode Configuration

```yaml
search:
  engine: postgres  # postgres | shadow | canary | tantivy
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

- [bitmagnet source](https://github.com/bitmagnet-io/bitmagnet) — Go, MIT license
- [Tantivy](https://github.com/quickwit-oss/tantivy) — Rust, MIT license
- [tantivy-go](https://github.com/anyproto/tantivy-go) — Go FFI bindings (rejected, see above)
- [Database analysis](./bitmagnet-database-analysis.md) — 368 GB PG analysis (live measurements 2026-05-28)
- Discord Go→Rust migration, Vinted ES→Vespa shadow traffic, InfluxData strangler fig pattern
