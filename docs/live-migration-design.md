# Hybrid Blob Live Migration Design

**Date:** 2026-05-28  
**Status:** Investigation complete, source-code verified  
**Branch:** `feat/rust-rewrite-plan`

---

## Overview

Zero-downtime live migration of the 273 GB `torrent_files` table (873M rows) into ZSTD-compressed blobs. Deploy the forked image → migration runs automatically in the background → DHT crawler runs normally throughout.

## User Experience

```
1. Build forked Docker image
2. Update K8s StatefulSet image tag → pod restarts
3. Goose migration adds columns/tables (seconds)
4. App starts normally — DHT crawler, search, API all working
5. Run: bitmagnet blob-migration start
6. Background migration runs (~4-5 hours for 16.8M torrents)
7. Monitor: bitmagnet blob-migration status
8. After completion: bitmagnet blob-migration cleanup
9. Done — 273 GB reclaimed
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Forked bitmagnet (single deploy)                           │
│                                                             │
│  ┌─ DHT Crawler ─────────────────────────────────────────┐  │
│  │ Dual-write: blob + torrent_files rows (same tx)       │  │
│  │ File: dhtcrawler/persist.go:99 transaction             │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─ Queue Migration Worker (self-chaining batches) ──────┐  │
│  │ Reads old torrent_files → serializes → writes blobs   │  │
│  │ 1000 torrents/batch, configurable throttle             │  │
│  │ Progress tracked in key_values table                   │  │
│  │ Pattern: processor/batch/queue/handler.go              │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─ Transparent Read (AfterFind hook) ───────────────────┐  │
│  │ Torrent.AfterFind(): if FilesData != nil → deserialize │  │
│  │ Covers: file browser, processor Preload, tsvector       │  │
│  │ Fallback: GORM Preload from torrent_files (unchanged)  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─ Search/Facets (UNCHANGED during migration) ──────────┐  │
│  │ File type facet: EXISTS on torrent_files (dual-write)  │  │
│  │ Full-text search: torrent_contents.tsv (unaffected)    │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Modification Surface (Source-Code Verified)

### Files that CHANGE

| File                                                  | Change                                                     | Purpose                        |
| ----------------------------------------------------- | ---------------------------------------------------------- | ------------------------------ |
| `model/torrents.gen.go`                               | Add `FilesData []byte` + `FileExtensions []string` fields  | New GORM columns               |
| `model/torrents.go` AfterFind (line 15)               | Add blob deserialization → populate `t.Files`              | Transparent read for ALL paths |
| `dhtcrawler/persist.go` createTorrentModel (line 150) | Serialize files to blob, set `FilesData`                   | Dual-write                     |
| `dhtcrawler/persist.go` transaction (line 100)        | Add `FilesData`, `FileExtensions` to ON CONFLICT DoUpdates | Persist blob in same tx        |
| `migrations/00021_blob_storage.sql`                   | ADD COLUMN, CREATE TABLE                                   | Schema changes                 |
| `migrations/00022_blob_indexes.sql`                   | CREATE INDEX CONCURRENTLY (NO TRANSACTION)                 | GIN index                      |

### New files

| File                                               | Purpose                                      |
| -------------------------------------------------- | -------------------------------------------- |
| `internal/blobmigration/config.go`                 | Config struct: enabled, batch_size, sleep_ms |
| `internal/blobmigration/serializer.go`             | MessagePack+ZSTD serialize/deserialize       |
| `internal/blobmigration/queue/handler.go`          | Self-chaining batch migration queue handler  |
| `internal/blobmigration/blobmigrationfx/module.go` | fx module registration                       |
| `internal/app/cmd/blobmigrationcmd/command.go`     | CLI: start, status, cleanup                  |

### Files that DON'T change

| File                                        | Why unchanged                           |
| ------------------------------------------- | --------------------------------------- |
| `processor/persist.go`                      | Doesn't write to torrent_files          |
| `importer/importer.go`                      | Doesn't touch torrent_files             |
| `torznab/adapter/`                          | Doesn't load files                      |
| `search/criteria_torrent_file_extension.go` | Dual-write keeps torrent_files current  |
| `search/facet_torrent_file_type.go`         | Uses extension criteria (unchanged)     |
| `search/search_torrent_files.go`            | GraphQL files query works via AfterFind |
| `gql/resolvers/`                            | No resolver changes needed              |

## Detailed Design

### Stage 1: Schema Migration (Goose, on startup)

`migrations/00021_blob_storage.sql`:

```sql
-- +goose Up
ALTER TABLE torrents ADD COLUMN IF NOT EXISTS files_data BYTEA;
ALTER TABLE torrents ADD COLUMN IF NOT EXISTS file_extensions TEXT[] DEFAULT '{}';

CREATE TABLE IF NOT EXISTS torrent_file_summary (
    info_hash BYTEA PRIMARY KEY REFERENCES torrents(info_hash),
    file_count INT NOT NULL DEFAULT 0,
    total_size BIGINT NOT NULL DEFAULT 0,
    largest_file_size BIGINT NOT NULL DEFAULT 0,
    extensions TEXT[] DEFAULT '{}',
    has_video BOOLEAN NOT NULL DEFAULT FALSE,
    has_subtitle BOOLEAN NOT NULL DEFAULT FALSE,
    has_audio BOOLEAN NOT NULL DEFAULT FALSE
);

-- +goose Down
DROP TABLE IF EXISTS torrent_file_summary;
ALTER TABLE torrents DROP COLUMN IF EXISTS file_extensions;
ALTER TABLE torrents DROP COLUMN IF EXISTS files_data;
```

`migrations/00022_blob_indexes.sql`:

```sql
-- +goose NO TRANSACTION

-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS torrents_file_extensions_idx
    ON torrents USING GIN (file_extensions);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS torrents_file_extensions_idx;
```

### Stage 2: Dual-Write (immediate, all new data)

In `dhtcrawler/persist.go` `createTorrentModel()` (line 150):

```go
// After building []TorrentFile slice...
if len(files) > 0 {
    blob, err := blobmigration.SerializeFiles(files)
    if err == nil {
        t.FilesData = blob
        t.FileExtensions = extractUniqueExtensions(files)
    }
}
```

The blob write lands in the same transaction (line 99) as the torrent upsert. Add `FilesData` and `FileExtensions` to the ON CONFLICT DoUpdates clause.

### Stage 3: Transparent Read (AfterFind hook)

In `model/torrents.go` `AfterFind()` (line 15):

```go
func (t *Torrent) AfterFind(_ *gorm.DB) error {
    // Prefer blob over Preloaded rows
    if t.FilesData != nil && len(t.FilesData) > 0 {
        files, err := blobmigration.DeserializeFiles(t.FilesData)
        if err == nil {
            t.Files = files
        }
    }
    // Existing sort logic
    if t.Files != nil {
        sort.Slice(t.Files, func(i, j int) bool {
            return t.Files[i].Path < t.Files[j].Path
        })
    }
    // ... rest of existing AfterFind
}
```

This transparently handles:

- Processor Preload (`processor.go:50-56`) → `t.Files` populated from blob
- GraphQL file browser → blob deserialized automatically
- tsvector rebuild → `fileSearchStrings()` reads blob-sourced `t.Files`

### Stage 4: Background Migration (queue-based)

Queue handler following the `processor/batch/queue/handler.go` self-chaining pattern:

```go
func (h *Handler) Handle(ctx context.Context, job model.QueueJob) error {
    params := decodeMigrationParams(job.Payload)

    // Read batch of unmigrated torrents
    rows := h.dao.TorrentFile.Where(
        h.dao.TorrentFile.InfoHash.Gt(params.LastInfoHash),
    ).Group(h.dao.TorrentFile.InfoHash).
      Order(h.dao.TorrentFile.InfoHash).
      Limit(params.BatchSize).Find()

    // For each torrent: serialize files → update blob
    for _, group := range groupByInfoHash(rows) {
        blob := blobmigration.SerializeFiles(group.Files)
        h.dao.Torrent.Where(
            h.dao.Torrent.InfoHash.Eq(group.InfoHash),
        ).Updates(map[string]any{
            "files_data": blob,
            "file_extensions": extractExtensions(group.Files),
        })
        // Also insert into torrent_file_summary
    }

    // Self-chain: enqueue next batch if more remain
    if len(rows) == params.BatchSize {
        params.LastInfoHash = lastInfoHash
        h.queue.Enqueue(newMigrationJob(params))
    } else {
        // Migration complete
        h.keyValues.Set("blob_migration_status", "completed")
    }

    // Update progress
    h.keyValues.Set("blob_migration_progress", progressJSON)

    return nil
}
```

### Stage 5: Cleanup (manual CLI command)

`bitmagnet blob-migration cleanup`:

1. Verify migration complete: `SELECT COUNT(*) FROM torrents WHERE files_data IS NULL AND files_status NOT IN ('no_info') = 0`
2. Switch extension criteria from EXISTS subquery to array containment
3. `DROP TABLE torrent_files` (273 GB reclaimed)
4. `VACUUM` to release space

## Race Condition Analysis

| Race                                       | Scenario                                                               | Resolution                                             |
| ------------------------------------------ | ---------------------------------------------------------------------- | ------------------------------------------------------ |
| DHT re-discovers existing torrent          | Overwrites blob with complete file list from metadata                  | Correct — metadata always contains complete file list  |
| Migrator reads while DHT inserts new files | READ COMMITTED sees consistent snapshot; worst case misses a few files | DHT rediscovery will overwrite blob with complete data |
| Concurrent blob writes (migrator + DHT)    | Last writer wins; both produce correct data                            | Safe — both serialize the complete file list           |
| Read during migration (half-migrated)      | AfterFind checks blob first, falls back to rows                        | Correct — unmigrated torrents use rows                 |

## Performance Impact

| Aspect                 | Impact                                                                |
| ---------------------- | --------------------------------------------------------------------- |
| DHT crawler throughput | ~5% overhead (blob serialization per torrent)                         |
| Search latency         | Zero (no search code changes during migration)                        |
| Disk during migration  | +28 GB temporary (blobs + summary coexist with torrent_files)         |
| Migration duration     | ~4-5 hours at 1K torrents/sec (16.8M with files)                      |
| DB load                | ~18K reads/sec + ~1K writes/sec (modest vs DHT crawler's normal load) |

## Rollback Strategy

| When                              | How                           | Data loss                                        |
| --------------------------------- | ----------------------------- | ------------------------------------------------ |
| During migration (before cleanup) | Roll back to old image        | None — torrent_files still complete (dual-write) |
| After DROP TABLE                  | Cannot roll back to old image | None — blob data is complete and verified        |

## Configuration

```yaml
blob_migration:
  batch_size: 1000
  sleep_between_batches_ms: 100
```

Environment variables: `BLOB_MIGRATION__BATCH_SIZE=1000`

## CLI Commands

```bash
bitmagnet blob-migration start    # Enqueue migration jobs
bitmagnet blob-migration status   # Show progress (migrated/total, %, ETA)
bitmagnet blob-migration cleanup  # Drop torrent_files after verification
```

---

## Coexistence beyond Phase 1 (keep-everything mode)

The dual-write pattern here generalizes: later phases add **more** parallel sinks without removing the legacy one, so the system can run _all_ phases with **no table drop** (full reversibility) until each cutover is explicitly approved. Persist fans out to up to four sinks — `torrent_files` rows · `files_data` blob + `file_extensions` (same tx) · Tantivy `IndexDocument` (Phase 4, **post-commit fire-and-forget**, no-op when `SEARCH_ENABLED=false`) · `info_hash_v1/v2` + `meta_version` (BEP-52 v2, same tx). All additive; none on the served read path. The only destructive steps are the explicit, CLI-gated cutovers — see **“Coexistence / Keep-Everything Mode”** and the **`00023` live-safe rewrite** in [`rust-rewrite-plan.md`](./rust-rewrite-plan.md).

A later phase (**Phase 5.5 — File-Grained Search**, see [`rust-rewrite-plan.md`](./rust-rewrite-plan.md) and [`dev/perfile-search-complete-parity.md`](./dev/perfile-search-complete-parity.md)) adds a **fifth** sink — a per-torrent `BatchIndexFiles` to a file-grained Tantivy index (post-commit, fire-and-forget, no-op when `SEARCH_FILE_INDEX_ENABLED=false`, fired only on file-set/classification change, not per scrape). Two notes for this design: (1) once `torrent_files` is dropped, the `AfterFind` blob path becomes the _only_ source for the per-torrent file browser, so the resolver must be re-pointed at `files_data` (**G2**) — it currently still reads `torrent_files`; (2) the per-file `extension` stored in the blob is **empty for crawl-path torrents** (it was a PG generated column, never set before serialization — **G1**), a live data-at-rest defect that is invisible today (the browser reads `torrent_files`) but surfaces once any consumer reads extension from the blob; every blob/index read must derive extension from the path (`FileExtensionFromPath`, byte-identical to the old generated column).
