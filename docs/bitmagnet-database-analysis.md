# Bitmagnet Database Analysis

Deep investigation into the 367 GB PostgreSQL database on FSN1. Conducted 2026-05-24.

## Executive Summary

The bitmagnet database is **genuinely large, not bloated**. The primary driver is `save_files_threshold: 500000` (5000x the default of 100), which stores up to 500K file records per torrent. This has produced **871 million rows** in `torrent_files`, with **indexes (154 GB) exceeding data (118 GB)** due to a wide composite primary key. Bloat is negligible (~2 GB). Reducing the threshold or dropping unused indexes are the highest-impact actions.

## Database Overview

| Property                   | Value                                                             |
| -------------------------- | ----------------------------------------------------------------- |
| PostgreSQL                 | 16.13 (Alpine)                                                    |
| Database size              | 367 GB                                                            |
| PVC size                   | 398 GB (includes 31 GB stale `bitmagnet_backup.dump` from Jan 14) |
| PVC allocation             | 500 Gi                                                            |
| Total rows (torrent_files) | 871,727,107                                                       |
| Total torrents             | 47,928,340                                                        |

## Size Breakdown by Table

### Top 4 Tables (366 GB / 99.7% of database)

| Table                    | Total      | Data                 | Indexes | Rows | % of DB |
| ------------------------ | ---------- | -------------------- | ------- | ---- | ------- |
| torrent_files            | **272 GB** | 118 GB               | 154 GB  | 871M | 74%     |
| torrent_contents         | **61 GB**  | 21 GB (+12 GB TOAST) | 28 GB   | 48M  | 17%     |
| torrents_torrent_sources | **19 GB**  | 8 GB                 | 11 GB   | 75M  | 5%      |
| torrents                 | **14 GB**  | 7 GB                 | 7 GB    | 48M  | 4%      |

### Remaining Tables (<1 GB each)

| Table                       | Total  | Rows |
| --------------------------- | ------ | ---- |
| torrent_hints               | 493 MB | 3.9M |
| queue_jobs                  | 261 MB | —    |
| content                     | 202 MB | —    |
| content_attributes          | 117 MB | —    |
| content_collections_content | 66 MB  | —    |

## Root Cause: `save_files_threshold: 500000`

Our deployment sets `save_files_threshold: 500000` in the bitmagnet config. The default is **100**.

This controls how many file records are saved per torrent. Each torrent can contain thousands of files (e.g., a season pack with 20 episodes, each with subtitles). With threshold=500K, essentially ALL files from every torrent are stored.

**Impact**: The `torrent_files` table has **871 million rows** (18x the torrent count of 48M). At the default threshold of 100, upstream users report ~50-80 GB total database size for similar torrent counts. Our 5000x threshold is directly responsible for the 272 GB `torrent_files` table.

### Upstream data points for comparison

| User                     | Torrents | save_files_threshold | DB Size                     |
| ------------------------ | -------- | -------------------- | --------------------------- |
| Upstream FAQ estimate    | 10M      | 100 (default)        | ~50 GB                      |
| GitHub user (issue #191) | 22M      | 1000                 | 159 GB (torrent_files only) |
| **Our deployment**       | **48M**  | **500,000**          | **367 GB**                  |

## Index Analysis

Indexes consume **more space than data** in the largest table:

### torrent_files Indexes (154 GB total)

| Index                                  | Size      | Purpose                                    |
| -------------------------------------- | --------- | ------------------------------------------ |
| torrent_files_pkey `(info_hash, path)` | **95 GB** | Composite PK — wide because `path` is TEXT |
| torrent_files_info_hash_index_key      | **45 GB** | Unique index on `(info_hash, index)`       |
| torrent_files_size_idx                 | 8 GB      | Search by file size                        |
| torrent_files_extension_idx            | 6 GB      | Search by file extension                   |

**The composite PK is the single biggest space consumer** (95 GB). The `info_hash` (20 bytes) is stored 3x per file row: once in the data, twice in the two indexes. Upstream issue #191 notes that replacing the composite PK with a serial integer would save ~40% of index space (~60 GB).

### torrent_contents Indexes (28 GB total)

15+ indexes including GIN indexes for full-text search, content type, languages. The `content_type_tsv_idx` GIN index alone is 14 GB.

## Bloat Assessment

**Bloat is NOT the main issue.** Estimated table bloat is ~2 GB total:

| Table                    | Dead Tuples | Dead % | Estimated Bloat | Last Autovacuum |
| ------------------------ | ----------- | ------ | --------------- | --------------- |
| torrents_torrent_sources | 14.4M       | 19.2%  | 1.3 GB          | 2026-04-23      |
| torrent_files            | 1.1M        | 0.1%   | 147 MB          | 2026-01-14      |
| torrent_contents         | 817K        | 1.7%   | 363 MB          | 2026-01-14      |
| torrents                 | 868K        | 1.8%   | 129 MB          | 2026-01-14      |

### Autovacuum Concern

Default `autovacuum_vacuum_scale_factor = 0.2` means vacuum triggers after 20% of rows are dead. For `torrent_files` (871M rows), that's **174M dead rows** before vacuum fires — far too high. The table hasn't been autovacuumed since January.

**Recommendation**: Set per-table autovacuum thresholds:

```sql
ALTER TABLE torrent_files SET (autovacuum_vacuum_scale_factor = 0.01);
ALTER TABLE torrent_contents SET (autovacuum_vacuum_scale_factor = 0.02);
ALTER TABLE torrents_torrent_sources SET (autovacuum_vacuum_scale_factor = 0.02);
```

## Built-in Cleanup Mechanisms

**Almost none** — this is a known gap in bitmagnet:

| Mechanism                  | Exists? | Scope                                                     |
| -------------------------- | ------- | --------------------------------------------------------- |
| Queue job GC               | Yes     | Only `queue_jobs` table                                   |
| Classifier delete rules    | Yes     | Can delete by content type (e.g., XXX) or banned keywords |
| Time-based retention / TTL | **No**  | All torrents kept forever                                 |
| Dead torrent pruning       | **No**  | Zero-seeder torrents not cleaned                          |
| Database size limit        | **No**  | No auto-pause at size threshold                           |
| VACUUM scheduling          | **No**  | Relies on default autovacuum                              |
| Index maintenance          | **No**  | No REINDEX scheduling                                     |

## Upstream Known Issues (All OPEN)

| Issue                                                        | Title                             | Key Insight                                                             |
| ------------------------------------------------------------ | --------------------------------- | ----------------------------------------------------------------------- |
| [#191](https://github.com/bitmagnet-io/bitmagnet/issues/191) | Database size optimizations       | Composite PK wastes ~40% of index space. Maintainer: "not top priority" |
| [#187](https://github.com/bitmagnet-io/bitmagnet/issues/187) | Allow setting database size limit | Request to auto-pause DHT at limit. No implementation                   |
| [#186](https://github.com/bitmagnet-io/bitmagnet/issues/186) | Document disk space requirements  | Real-world: 5-30 KB per torrent depending on config                     |
| [#495](https://github.com/bitmagnet-io/bitmagnet/issues/495) | Create a way to limit disk usage  | User ran out of 100GB. No solution beyond disabling crawler             |

## Recommended Actions

### Immediate (no data loss, no downtime)

1. **Delete stale `bitmagnet_backup.dump`** — 31 GB, from January 14

   ```bash
   kubectl exec -n bitmagnet sts/bitmagnet-postgres -- rm /var/lib/postgresql/data/bitmagnet_backup.dump
   ```

2. **VACUUM torrents_torrent_sources** — reclaim 1.3 GB bloat

   ```sql
   VACUUM ANALYZE torrents_torrent_sources;
   ```

3. **Tune autovacuum** for large tables (see SQL above)

### Medium-term (configuration change, requires restart)

4. **Reduce `save_files_threshold`** from 500,000 to 100-1000

   - Won't delete existing data but stops future growth
   - At 1000: still captures full file lists for most torrents
   - At 100 (default): only stores first 100 files per torrent
   - **Estimated savings on new data**: 50-80% reduction in `torrent_files` growth rate

5. **Enable classifier delete rules** for unwanted content types
   - Reduces `torrent_contents` and related tables

### Long-term (significant effort)

6. **Purge old torrent_files data** for torrents already stored above new threshold

   ```sql
   -- Example: keep only first 1000 files per torrent, delete the rest
   -- CAUTION: This deletes data. Test on backup first.
   DELETE FROM torrent_files WHERE (info_hash, index) IN (
     SELECT info_hash, index FROM torrent_files
     WHERE index > 1000
   );
   -- Follow with VACUUM FULL torrent_files; (requires downtime + 2x disk temporarily)
   ```

7. **VACUUM FULL** on torrent_files — requires downtime and temporary 2x disk space (236 GB free currently). Would reclaim any internal fragmentation.

8. **Replace composite PK with serial** (per issue #191) — saves ~60 GB in indexes but requires schema migration and application changes.

## Capacity Projection

| Scenario                 | Growth Rate     | Time to Fill 500 GB PVC |
| ------------------------ | --------------- | ----------------------- |
| Current (threshold=500K) | ~2-3 GB/day     | ~1-2 months             |
| Threshold=1000           | ~0.5-1 GB/day   | ~6-12 months            |
| Threshold=100 (default)  | ~0.2-0.5 GB/day | ~1-2 years              |
| DHT crawler disabled     | 0               | Never                   |

## Configuration Reference

Current deployment settings (`ansible/inventory/group_vars/k3s_cluster/bitmagnet.yml`):

- `bitmagnet_dht_save_files_threshold: 500000`
- `bitmagnet_dht_save_pieces: false` (good)
- `bitmagnet_postgres_memory_limit: 16Gi`
- `bitmagnet_postgres_shared_memory: 1Gi`
- PVC: 500 Gi

---

## Storage Optimization Options

Investigation conducted 2026-05-24. All options preserve 100% data completeness.

### Option Comparison

| Approach                      | Est. DB Size | Savings | Effort      | Risk    | Notes                                |
| ----------------------------- | ------------ | ------- | ----------- | ------- | ------------------------------------ |
| **Current baseline**          | **367 GB**   | —       | —           | —       | PVC: 398 GB (incl. 31 GB stale dump) |
| **A. ZFS ZSTD-1**             | ~147 GB      | 60%     | Moderate    | Low     | Transparent, no app changes          |
| **B. Hybrid Blob (DB only)**  | ~111-117 GB  | 68-70%  | 2-3 weeks   | Low     | Best ROI, stays in PG                |
| **B + PG summary**            | ~123-132 GB  | 64-66%  | 2-3 weeks   | Low     | Covers 90% of queries                |
| **B + Manticore**             | ~161-197 GB  | 46-56%  | 3-4 weeks   | Low-Med | Full FTS on file paths               |
| **B + Tantivy**               | ~148-191 GB  | 48-60%  | 3-4 weeks   | Low-Med | Embedded, no ext. service            |
| **B + A (ZFS)**               | ~48-53 GB    | 86%     | 3-4 weeks   | Low-Med | Blob + filesystem compression        |
| **B + Manticore + A**         | ~65-79 GB    | 78-82%  | 4-5 weeks   | Medium  | Full solution + ZFS                  |
| **C. Normalize info_hash FK** | ~287 GB      | 22%     | Significant | Medium  | Stacks with others                   |
| **D. ClickHouse**             | 22-28 GB     | 92-94%  | 4-6 weeks   | Medium  | Best compression, complex ops        |

### Hybrid Blob: Precise Disk Accounting

The earlier "40-50 GB" estimate was incorrect — it didn't fully account for `torrent_contents` (61 GB) which stays unchanged. Here's the precise breakdown:

| Component                          | Current    | After Blob Migration | Change                 |
| ---------------------------------- | ---------- | -------------------- | ---------------------- |
| **torrent_files** (data + indexes) | **272 GB** | **0 GB**             | **-272 GB eliminated** |
| File blobs (ZSTD in TOAST)         | 0          | 12-16 GB             | +12-16 GB              |
| `file_extensions TEXT[]` + GIN     | 0          | 4-6 GB               | +4-6 GB                |
| torrent_contents                   | 61 GB      | 61 GB                | unchanged              |
| torrents_torrent_sources           | 19 GB      | 19 GB                | unchanged              |
| torrents (base table)              | 14 GB      | 14 GB                | unchanged              |
| Other tables                       | 1 GB       | 1 GB                 | unchanged              |
| **Database total**                 | **367 GB** | **~111-117 GB**      | **-250 to -256 GB**    |

With optional additions:

| Addition                        | Extra Disk | Extra RAM | Purpose                                           |
| ------------------------------- | ---------- | --------- | ------------------------------------------------- |
| PG summary table                | +12-15 GB  | shared    | Extension/size filters, facet counts              |
| Manticore Search (columnar)     | +50-80 GB  | 2-4 GB    | Full FTS on file paths                            |
| Tantivy (embedded)              | +37-74 GB  | 8-16 GB   | Full FTS, no external service                     |
| DuckDB analytics                | +0 GB      | 2-8 GB    | Analytical queries on Parquet blobs               |
| File-grained Tantivy (per-file) | +8-15 GB   | +2-4 GB   | True per-file search (ext+size pairing, path FTS) |

### Projected Total with Search Engine

| Configuration               | DB Size | Search Index | Total Disk      | Total RAM |
| --------------------------- | ------- | ------------ | --------------- | --------- |
| **Blob + PG summary**       | ~120 GB | included     | **~120 GB**     | shared    |
| **Blob + Manticore**        | ~115 GB | 50-80 GB     | **~165-195 GB** | +2-4 GB   |
| **Blob + Tantivy**          | ~115 GB | 37-74 GB     | **~150-190 GB** | +8-16 GB  |
| **Blob + PG summary + ZFS** | ~48 GB  | included     | **~48 GB**      | shared    |
| **Blob + Manticore + ZFS**  | ~46 GB  | 20-32 GB     | **~66-78 GB**   | +2-4 GB   |

Note: ZFS compression applies to both the database and any on-disk search index (Manticore columnar compresses further under ZFS).

### Per-File Search Capability — chosen path

The hybrid-blob migration trades fleet-wide per-file SQL for blob compactness. To **restore true per-file search** ("the .mkv _itself_ > 1 GB" — a conjunction the summary table and the torrent-grained Tantivy index cannot express, since Tantivy 0.26 has no nested docs), the chosen path is **P2: a second, file-grained Tantivy index (1 doc/file) on the existing sidecar** (+8–15 GB, +2–4 GB RAM) — _not_ a slim PG `torrent_file` table (which would re-bloat 68–92 GB, the exact cost the blob migration removed). It is backfilled from the 16 GB blob (not the 873 M rows) and denormalizes only immutable torrent fields. See [`dev/perfile-search-with-blob-design.md`](./dev/perfile-search-with-blob-design.md) (option matrix) and [`dev/file-grained-search-spec.md`](./dev/file-grained-search-spec.md) (implementation spec).

### Approach Details

#### A. ZFS Filesystem Compression (no app changes)

Move PostgreSQL data directory to a ZFS volume with ZSTD compression. Completely transparent to PostgreSQL and bitmagnet — no code changes, no schema migration.

| Algorithm | Est. Size | Savings | CPU Impact |
| --------- | --------- | ------- | ---------- |
| LZ4       | ~216 GB   | 41%     | Near-zero  |
| ZSTD-1    | ~147 GB   | 60%     | Low        |
| ZSTD-3    | ~130 GB   | 65%     | Moderate   |

**Implementation**: Create ZFS pool on the NVMe drives (currently ext4 on md2 RAID-1), set `recordsize=8K` for PostgreSQL alignment, `pg_basebackup` to new volume, update mount points.

**Stacks with everything** — compression is multiplicative with schema changes.

#### B. Hybrid Blob Storage (best ROI)

Instead of 871M rows (1 per file) in `torrent_files`, store the entire file list as a **compressed blob per torrent** in PostgreSQL:

- 48M torrents × 1 blob each (avg ~18 files/torrent)
- Per file: `{index, path, extension, size}` serialized as MessagePack/CBOR → ~50 bytes avg
- Intra-torrent paths share prefixes heavily → 2-3x prefix sharing
- ZSTD compression per blob → another 3-5x
- **Eliminates**: 154 GB of indexes, ~19 GB row overhead, info_hash redundancy across 871M rows

**File data**: 871M records → ~12-16 GB as compressed blobs (stored in TOAST on the `torrents` table)
**Eliminated**: 272 GB (torrent_files data 118 GB + indexes 154 GB)
**Added**: ~16-22 GB (blobs 12-16 GB + file_extensions array + GIN index 4-6 GB)
**Remaining unchanged**: torrent_contents 61 GB + torrents_torrent_sources 19 GB + torrents 14 GB + other 1 GB = 95 GB
**New DB total**: ~111-117 GB (see precise accounting in "Hybrid Blob: Precise Disk Accounting" below)

**Trade-off**: Can't SQL-query individual files anymore. Need to deserialize the blob in application code. For file-level search (e.g., "find all .mkv files > 1 GB"), add a search engine (Manticore recommended) or use the PostgreSQL summary table for most queries.

**Implementation**: Fork bitmagnet (Go), change `torrent_files` table to a `files_data BYTEA` column on `torrents`, update the GORM model and file hydration code. 2-3 weeks.

#### C. Normalize info_hash to INTEGER FK

Replace 20-byte BYTEA `info_hash` in torrent_files (and other tables) with a 4-byte INTEGER foreign key to the `torrents` table. Saves space in data AND all indexes.

**Savings**: ~80 GB (22%) — torrent_files PK: -21 GB, UNIQUE index: -30 GB, data: -17 GB, other tables: -12 GB
**New total**: ~287 GB standalone, **~106 GB combined with ZFS ZSTD-1**

**Implementation**: Add `torrent_id SERIAL` to torrents table, add `torrent_id INTEGER` FK to torrent_files/torrent_contents/torrents_torrent_sources, backfill, rebuild indexes. Requires bitmagnet fork. Multi-hour migration for 871M rows.

#### D. ClickHouse for torrent_files

Move only `torrent_files` to ClickHouse (columnar database). Keep everything else in PostgreSQL.

**Estimated size**: 22-28 GB for 871M rows:

- info_hash (20B random): ~14-15 GB (incompressible)
- path (TEXT, redundant prefixes): ~6-10 GB (ZSTD excels here)
- extension (~100 unique): ~50 MB (LowCardinality dictionary encoding)
- index/size/timestamps: ~2-3 GB (Delta encoding)
- No separate indexes needed (sparse primary index is tiny)

**Trade-off**: Separate service to operate. ClickHouse excels at analytics/bulk reads, poor at point lookups (ms vs µs). Adds operational complexity.

#### E. Parquet Cold Tier

Two-tier strategy: recent data in PostgreSQL, older data exported as compressed Parquet files on disk or S3.

**Estimated size**: 20-30 GB for the full dataset as Parquet (columnar + ZSTD).

Can be queried by DuckDB on demand. Excellent for archival. Combine with Hybrid Blob (B) for hot data — best of both worlds.

#### F-H. Other Options

- **DuckDB**: 30-45 GB, embedded columnar. Single-writer limitation is a concern for concurrent DHT crawler writes. Weaker string compression than ClickHouse.
- **RocksDB (Rust)**: 35-60 GB with tiered ZSTD compression. Excellent write throughput but KV-only — must build query layer manually. High effort.
- **Full Rust rewrite**: 3-6 months, high risk. Not recommended. Selective Rust rewrite (classifier + storage) is 6-10 weeks if needed.

### PostgreSQL-Specific Quick Wins (No Fork Needed)

These can be applied today without forking bitmagnet:

| Action                                   | Savings               | Effort  | Risk                       |
| ---------------------------------------- | --------------------- | ------- | -------------------------- |
| Drop `torrent_files_size_idx`            | 8 GB                  | Trivial | Low (ORDER BY size slower) |
| LZ4 TOAST on torrent_contents            | ~2 GB                 | Trivial | None                       |
| Tune autovacuum (scale_factor 0.01-0.02) | Prevents future bloat | Trivial | None                       |
| Delete stale `bitmagnet_backup.dump`     | 31 GB                 | Trivial | None                       |
| VACUUM ANALYZE torrents_torrent_sources  | 1.3 GB                | Trivial | None                       |

**Total quick wins: ~42 GB with zero risk.**

### Query Pattern Analysis

From the bitmagnet Go source code, `torrent_files` is accessed via:

- **Writes**: `INSERT ON CONFLICT DO NOTHING` (batch 100) — append-only, never updated
- **Reads**: `WHERE info_hash IN (...)` for hydration (preloading files for a set of torrents)
- **Filters**: `EXISTS (SELECT 1 FROM torrent_files WHERE info_hash = t.info_hash AND extension IN (...))` for content type search
- **Ordering**: `ORDER BY index, path, extension, size`

Key insight: the append-only write pattern and info_hash-grouped reads make this data ideal for **columnar storage** (ClickHouse) or **blob-per-torrent** (Hybrid Blob). The per-file SQL queries are limited to EXISTS checks, which can be replaced by application-level filtering on deserialized blobs.

### Rust Rewrite Assessment

| Component      | Rust Benefit                   | Effort    | Worth It?                |
| -------------- | ------------------------------ | --------- | ------------------------ |
| DHT Crawler    | Moderate (~50% memory savings) | 2-4 weeks | Only if rewriting anyway |
| Classifier     | High (DFA regex, faster)       | 2-3 weeks | Best ROI for Rust        |
| Database Layer | Depends on target store        | 4-8 weeks | Only if changing store   |
| API Server     | Marginal                       | 3-4 weeks | No                       |
| Web UI         | N/A (React)                    | 0         | Keep as-is               |

**Recommendation**: Fork-and-patch in Go (2-3 weeks) over full Rust rewrite (3-6 months). Optionally add Rust classifier via FFI later.

### Recommended Strategy: Hybrid Blob Fork + Manticore Search

**Phase 1 — Hybrid Blob fork (2-3 weeks)**:

- Fork bitmagnet (Go), replace `torrent_files` (871M rows, 272 GB) with ZSTD-compressed blobs per torrent
- Add `file_extensions TEXT[]` + GIN index on `torrents` for facet queries
- Add `torrent_file_summary` table for extension/size/count queries (12-15 GB)
- Full-text search: unchanged (tsvector on `torrent_contents` pre-computed)
- File browsing: blob decompression in GraphQL resolver
- **Result: 367 GB → ~115 GB database**

**Phase 2 — Add Manticore Search (1-2 weeks)**:

- Deploy Manticore with columnar storage for file-path FTS
- Index file paths from blobs at crawl time
- Single C++ binary, 2-4 GB RAM, SQL-like queries, official Go SDK
- **Result: +50-80 GB search index. Total: ~165-195 GB**

**Phase 3 — Optional: ZFS compression (1-2 days)**:

- Transparent 60% compression on both database and search index
- **Result: ~66-78 GB total. 79-82% reduction from original 367 GB**

**Optional — DuckDB analytics**:

- Embedded, queries compressed Parquet blobs directly — no storage overhead
- Covers analytical queries: extension distributions, size histograms, aggregations
- 2-8 GB RAM, official Go driver. Add when analytical queries are needed.

### End-State Projections

| Configuration              | DB      | Search   | **Total**       | Reduction |
| -------------------------- | ------- | -------- | --------------- | --------- |
| **Blob + PG summary**      | ~120 GB | included | **~120 GB**     | 67%       |
| **Blob + Manticore**       | ~115 GB | 50-80 GB | **~165-195 GB** | 47-55%    |
| **Blob + Manticore + ZFS** | ~46 GB  | 20-32 GB | **~66-78 GB**   | 79-82%    |
| **Blob + Quickwit**        | ~115 GB | 37-74 GB | **~150-190 GB** | 48-59%    |
| Current (do nothing)       | 367 GB  | —        | **367 GB**      | 0%        |

---

## Search Capability Analysis (Hybrid Blob Impact)

Investigation of what search features survive the Hybrid Blob migration, what breaks, and how to restore full search capabilities.

### Current Search Architecture

Bitmagnet uses PostgreSQL full-text search via `tsvector` + GIN indexes. The main search index is `torrent_contents.tsv` (14 GB GIN index), which is **pre-computed at classification time** and stores combined lexemes from:

- Content metadata (title, genre, year) — weight A/B
- Torrent name — weight A
- Info hash — weight A
- Video attributes (resolution, codec, source) — weight C
- **File path strings** — weight D (lowest priority, deduplicated prefixes)

Search queries execute `WHERE torrent_contents.tsv @@ tsquery` with `ts_rank_cd()` for relevance. This does NOT query `torrent_files` at search time.

### Impact Assessment

| Feature                                    | Tables Used                          | Query-time? | Breaks with Blobs?                | Criticality | Mitigation                               |
| ------------------------------------------ | ------------------------------------ | ----------- | --------------------------------- | ----------- | ---------------------------------------- |
| **Full-text search** (main search bar)     | `torrent_contents.tsv` GIN           | Yes         | **NO** — tsvector is pre-computed | Critical    | None needed                              |
| **Content type facet** (movie/tv/music)    | `torrent_contents.content_type`      | Yes         | **NO**                            | Critical    | None needed                              |
| **Language/genre/resolution facets**       | `torrent_contents` + `content`       | Yes         | **NO**                            | High        | None needed                              |
| **File Type facet** (video/audio/subtitle) | `torrent_files.extension` via EXISTS | Yes         | **YES**                           | High        | Denormalize into `torrents.extensions[]` |
| **File browsing** (per-torrent file list)  | `torrent_files` direct SELECT        | Yes         | **YES**                           | Medium      | Decompress blob in GraphQL resolver      |
| **tsvector rebuild** (reprocessing)        | `torrent_files` via Preload          | No (async)  | **YES** — needs adaptation        | Medium      | Decompress blob during reprocessing      |
| **DHT persistence** (write path)           | `torrent_files` INSERT               | No (write)  | **YES** — this is the benefit     | N/A         | Replace row INSERTs with blob writes     |
| **Torznab API**                            | No torrent_files refs                | Yes         | **NO**                            | Medium      | None needed                              |

### Key Finding: Full-Text Search Survives

The tsvector is **computed at index time, not query time**. File paths are baked into `torrent_contents.tsv` as weight-D lexemes during classification. The 14 GB GIN index on `torrent_contents` is the actual search workhorse — it has no dependency on the `torrent_files` table at query time.

**What breaks**: Only the File Type sidebar facet (EXISTS subquery on `torrent_files.extension`) and the per-torrent file browser (paginated SELECT). Both are fixable.

### Mitigation: File Type Facet

Add a `file_extensions TEXT[]` column to the `torrents` table, populated at crawl time:

```sql
ALTER TABLE torrents ADD COLUMN file_extensions TEXT[] DEFAULT '{}';
CREATE INDEX ON torrents USING GIN (file_extensions);
```

The facet query changes from:

```sql
EXISTS (SELECT 1 FROM torrent_files WHERE info_hash = t.info_hash AND extension IN ('mkv','mp4'))
```

To:

```sql
torrents.file_extensions && ARRAY['mkv','mp4']
```

**Size**: 48M rows × ~50 bytes avg = ~2.4 GB (vs current 154 GB of torrent_files indexes for the same query).

### Mitigation: File Browsing

The GraphQL resolver decompresses the blob and returns file structs in-memory. Since file lists are paginated (default 10 per page), decompressing a single blob per request is fast — MessagePack decompression of ~50 KB (typical torrent file list) takes <1ms.

### Search Engine Options (for Advanced File Search)

If per-file full-text search on paths is needed (e.g., "find all files matching 'pilot 1080p'"), a dedicated search engine can be added. However, the PostgreSQL Summary Table approach covers most practical use cases without any external service.

#### Option A: PostgreSQL Summary Table (No External Service) — RECOMMENDED

Add a 48M-row summary table alongside the blob:

```sql
CREATE TABLE torrent_file_summary (
    torrent_id       INT PRIMARY KEY,
    file_count       INT,
    total_size        BIGINT,
    extensions        TEXT[],
    largest_file_size BIGINT,
    has_video         BOOLEAN,
    has_subtitle      BOOLEAN
);
```

**Size**: ~12-15 GB with GIN index on extensions. Covers extension filtering, size filtering, facet counts — the most common queries. No external service.

#### Option B: Manticore Search (Lightweight External Engine)

Best fit for homelab if full file-path FTS is needed:

| Metric        | Value                               |
| ------------- | ----------------------------------- |
| Index size    | 50-80 GB (columnar mode)            |
| RAM           | 2-4 GB                              |
| Query latency | <50ms                               |
| Features      | FTS, facets, filters, range queries |
| Complexity    | Low (single C++ binary)             |

4x faster than Elasticsearch at 1.7B documents. Columnar storage keeps RAM minimal. SQL-like query language. Official Go SDK.

#### Option C: Tantivy via tantivy-go (Embedded, No External Service)

Best if zero external dependencies is a priority:

| Metric        | Value                               |
| ------------- | ----------------------------------- |
| Index size    | 37-74 GB                            |
| RAM           | 8-16 GB                             |
| Query latency | <50ms                               |
| Features      | FTS, facets, filters, range queries |
| Complexity    | Very low (embedded library)         |

Production-tested Go bindings (`anyproto/tantivy-go`). Rust FFI adds build complexity. Immutable data model (delete + reindex to update).

#### Search Engine Comparison

| Engine         | Index Size | RAM        | Latency  | Facets  | API             | Effort  | Verdict                    |
| -------------- | ---------- | ---------- | -------- | ------- | --------------- | ------- | -------------------------- |
| **Manticore**  | 50-80 GB   | 2-4 GB     | <50ms    | Full    | SQL + ES-compat | Low-Med | **Best overall**           |
| **Quickwit**   | 37-74 GB   | Very low   | <50ms    | Full    | ES-compatible   | Med     | **Best API, Datadog risk** |
| **Tantivy**    | 37-74 GB   | 8-16 GB    | <50ms    | Full    | Library (FFI)   | Medium  | **Best for Rust rewrite**  |
| **PG Summary** | 12-15 GB   | Shared     | 10-100ms | Partial | SQL             | Low     | **Best default**           |
| DuckDB         | 0 GB extra | 2-8 GB     | 1-10s    | Manual  | SQL             | Low     | Analytics only             |
| OpenSearch     | ~240 GB    | 64-96 GB   | <50ms    | Full    | ES-native       | High    | Overkill                   |
| Typesense      | 100-150 GB | 120-160 GB | 2-10ms   | Full    | REST            | Low     | Too much RAM               |
| Meilisearch    | 370-740 GB | 120-250 GB | <50ms    | Full    | REST            | Medium  | Not viable at 871M         |
| Bleve          | 3.7-7.4 TB | Massive    | 50-100ms | Basic   | Go lib          | Low     | Not viable                 |
| Sonic          | Small      | <100 MB    | <10ms    | No      | Custom          | Low     | Missing features           |

#### Quickwit (Tantivy-as-a-service)

Quickwit is a distributed search engine built on Tantivy, with an Elasticsearch-compatible API:

| Metric         | Value                                                               |
| -------------- | ------------------------------------------------------------------- |
| Index size     | 37-74 GB (same Tantivy compression)                                 |
| RAM            | Very low (stateless compute, index on object storage)               |
| Query latency  | <50ms (with hotcache)                                               |
| Features       | FTS, facets, filters, range queries, ES-compatible API              |
| Storage        | Requires S3-compatible layer (Garage or MinIO)                      |
| Benchmarked    | 6.1B documents, 300 QPS single node                                 |
| Go integration | Any ES Go client (massive ecosystem)                                |
| Risk           | **Acquired by Datadog (early 2025)** — open-source future uncertain |

**Pros vs Manticore**: ES-compatible API (huge Go ecosystem), Tantivy-level compression (37-74 GB vs 50-80 GB), very low RAM (stateless).
**Cons vs Manticore**: Requires S3-compatible layer (Garage/MinIO = extra service), Datadog acquisition risk, batch indexing (not real-time), designed for logs (may lack general search polish).

#### Manticore vs Quickwit vs Tantivy: Decision Matrix

| Factor                 | Manticore              | Quickwit                       | Tantivy              |
| ---------------------- | ---------------------- | ------------------------------ | -------------------- |
| **Deployment**         | **Single binary**      | 2 services (Quickwit + Garage) | Embedded (FFI)       |
| **RAM**                | **2-4 GB**             | Very low                       | 8-16 GB              |
| **Index size**         | 50-80 GB               | **37-74 GB**                   | **37-74 GB**         |
| **API**                | SQL + ES-compat        | **Full ES-compat**             | Library only         |
| **Real-time indexing** | **Native**             | Batch (seconds)                | Manual commits       |
| **Go integration**     | Official SDK           | **Any ES client**              | CGo + Rust FFI       |
| **Build complexity**   | **None**               | None (separate service)        | Rust toolchain in Go |
| **Operational**        | **Simple** (1 service) | Moderate (2 services)          | None (in-process)    |
| **Acquisition risk**   | None (independent)     | **Datadog (2025)**             | None (library)       |
| **Best for**           | **Go fork (simplest)** | ES ecosystem fans              | Future Rust rewrite  |

#### Option D: DuckDB on Parquet Blobs (Embedded Analytics)

If blobs are stored as Parquet format, DuckDB can query them directly — no import needed:

| Metric        | Value                                                    |
| ------------- | -------------------------------------------------------- |
| Index size    | 0 GB additional (queries Parquet directly)               |
| RAM           | 2-8 GB (configurable)                                    |
| Query latency | 1-10s analytical, not interactive FTS                    |
| Features      | Full SQL aggregations, filters, GROUP BY — no facets/FTS |
| Embedded      | Yes (in-process via official Go driver)                  |

Handles analytical queries well ("how many .mkv > 1GB?", "extension distribution", size histograms). Not suitable for interactive full-text search — use Manticore/Tantivy for that.

### Recommended Strategy: Hybrid Blob + Search Engine

**Phase 1 — Hybrid Blob fork (2-3 weeks)**:

- Fork bitmagnet, replace `torrent_files` (871M rows, 272 GB) with ZSTD-compressed blobs per torrent
- Add `file_extensions TEXT[]` column + GIN index on `torrents` for facet queries
- Full-text search: unchanged (tsvector on `torrent_contents` is pre-computed)
- File type facet: array containment on `torrents.file_extensions`
- File browsing: blob decompression in GraphQL resolver
- **Result: 367 GB → ~115 GB database**

**Phase 2 — Add Manticore Search (1-2 weeks)**:

- Deploy Manticore with columnar storage for file-path FTS
- Index file paths from blobs at crawl time
- Enables full Elasticsearch-like search on individual file paths
- **Result: +50-80 GB index, 2-4 GB RAM. Total: ~165-195 GB**

**Phase 3 — Optional: ZFS compression (1-2 days)**:

- Transparent 60% compression on both database and search index
- **Result: ~66-78 GB total. 79-82% reduction from original 367 GB**

**Optional — DuckDB analytics**:

- Embedded, queries compressed Parquet blobs directly — no storage overhead
- Covers analytical queries: extension distributions, size histograms, aggregations
- 2-8 GB RAM, official Go driver. Add when analytical queries are needed.

### Conclusion

The Hybrid Blob fork + Manticore Search is the recommended path:

1. **All critical search capabilities preserved**. Full-text search on torrent names/content is unaffected (pre-computed tsvector in `torrent_contents`). File Type facet restored via `TEXT[]` array column (2.4 GB vs 154 GB of current indexes). File browsing via blob decompression (faster than 45 GB index scan).

2. **Manticore provides Elasticsearch-grade file search** at homelab scale: single C++ binary, 2-4 GB RAM, 50-80 GB columnar index, SQL-like queries, official Go SDK, 4x faster than ES at 1.7B docs. No JVM, no cluster management.

3. **367 GB → 165-195 GB** with full search, or **→ 66-78 GB** with ZFS on top. All 871M file records preserved with complete search capability.

4. **Quickwit is an alternative** if ES-compatible API is preferred (Tantivy compression, very low RAM), but adds Garage dependency and carries Datadog acquisition risk. **Tantivy** is best saved for a future Rust rewrite.

5. **No Elasticsearch needed**. Manticore covers 100% of the use case at 1/20th the resource footprint.
