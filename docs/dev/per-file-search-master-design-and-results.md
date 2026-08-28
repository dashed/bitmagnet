# bitmagnet Per-File Data & Search — Master Design, Rationale & Results

**Date:** 2026-06-08 · **Branch:** `feat/file-grained-search` · **Scope:** the full arc of bitmagnet's per-file-data rework — the **deployed** Hybrid Blob migration, the **measured** benchmark/experiment suite, and the **proposed** L2 (DuckDB-on-Parquet + PG aggregate) search architecture.

This document is **self-contained**: load-bearing numbers and code anchors are restated inline so it can be read cold. It is **secret-clean** (hosts are placeholders `<fsn1-host>`/`<hel1-host>`; no IPs, DSNs, or credentials) because it lives in a public repo.

### Status legend (used throughout)

| Tag                    | Meaning                                                                 |
| ---------------------- | ----------------------------------------------------------------------- |
| **DEPLOYED**           | live in production now (the Hybrid Blob migration)                      |
| **MEASURED**           | a real number from a benchmark/experiment on the full 879.5M-row corpus |
| **PROPOSED**           | designed and specced, **not yet built** (all of L2)                     |
| **DEFERRED / PENDING** | deliberately not done (the `torrent_files` DROP, the code-build hold)   |

---

## Revisions — 2026-06-09 (external review)

This document was reviewed externally; the full triage + verdicts are in **[`../feedback/feedback_1-response.md`](../feedback/feedback_1-response.md)** (every claim re-verified against the code + the DuckDB source). Accepted changes that **supersede parts of §5/§7** below (the inline prose fold-in is in progress — tasks FB-A0…FB-DOC):

- **DROP gate — use the deployed `file_extensions` JSONB; `agg_torrent_ext` dropped.** ✅ **MEASURED (FB-A1, 2026-06-09 — [`fba1-jsonb-dropgate-results.md`](./fba1-jsonb-dropgate-results.md)):** `file_extensions @>` beats both the `torrent_files` EXISTS and a built `agg_torrent_ext` on every axis — disk **+119 MB vs +9.5 GB** (already deployed), **real-time** freshness, filter **44–114 ms**, facet **~1 ms** (budgeted `EXPLAIN`, with better estimates), and **exact set-parity** (50–110× faster to evaluate). So L2-P0 collapses to a flag-gated `EXISTS→file_extensions @>` swap + parity check; **`agg_torrent_ext`, its seed, delta-upsert, readers, and agg checker are removed** (kept only as a future option if `ext ∧ max_size` is ever needed).
- **L3 free-text path index — per-torrent path-bag = 13.54 GiB measured (not the per-file ~90 GB); the index is no longer a footprint-tripler.** ✅ **MEASURED (PS-MB1, 2026-06-09 — [`pathsearch-microbench-RESULTS.md`](./pathsearch-microbench-RESULTS.md); full investigation [`pathsearch-master-investigation.md`](./pathsearch-master-investigation.md) + threads `pathsearch-T1…T5`):** the _realtime per-keystroke <50 ms free-text PATH search_ investigation (PS-T1–T5) concluded **NO-GO by default** — nice-to-have, no demonstrated demand, purely additive, **never gates the DROP**. The gated micro-bench then ran on the full 879.5 M-row restore: a **per-torrent path-bag char-ngram(2,3) `WithFreqs`** index measures **13.54 GiB** (vs the per-FILE ~90 GB — the unlock is indexing the path field `WithFreqs`, dropping positions, which are **83.5 % dead weight** for ngram), `ascii3` warm p50 **24.71 ms** (p95/p99 tail **~55–65 ms** on the broadest substrings — median-interactive, not uniformly <50 ms), CJK **sub-ms**, recall **1.0000**. Edge-ngram and external engines (Meilisearch/Typesense/Quickwit/Manticore/pg_trgm) were weighed and rejected (PS-T2). ⟹ adding L3 now drops the saving only **87 % → ~83 %** (was −55 % on the per-file figure). **This supersedes the per-file L3 numbers in §4.11 below.** The `WithFreqs`-not-`WithFreqsAndPositions` fix (lossless 83.5 % cut) is a standing recommendation for any path-ngram field, including the existing Tantivy sidecar.
- **Freshness — tombstone the base+delta anti-join.** Anti-join base against a **`delta_changed_torrents` key set** (one row per changed `info_hash`, incl. deletes / zero-row), not the delta _file_ rows — else a deleted/zero-file torrent's base rows leak (confirmed: the classifier's `ErrDeleteTorrent` hard-`DELETE`s torrents).
- **DuckDB fact — v1 = file-facts-only.** `content_type`/`published_at` change outside `files_data` and don't bump `torrents.updated_at` → the denorm goes stale; resolve them via a live PG join, defer denorm to a v2 multi-table change-stream (`created_at` is the lone immutable denorm).
- **DuckDB deploy — immutable read-only generations.** DuckDB's file lock (1 RW xor N RO) _forbids_ a CronJob mutating the server's live DB; build generations offline + swap a `current` pointer + open `READ_ONLY`. Per-query deadlines return from the caller at the deadline and use `Connection::Interrupt()` to unwind the running DuckDB query — **DuckDB has no `statement_timeout`**. Treat SQL as code (lockdown `enable_external_access` etc.); escape `%`/`_`/`\` before `ILIKE`.
- **Doc — provenance + space accounting** (applied below, §4).

**Revised build order:** A0 semantic hardening → **A1 JSONB DROP-gate experiment** → A2 (`agg` only if A1 fails) → A3 DROP-readiness → B1 DuckDB sidecar (tombstone + file-facts-only + immutable RO generations + lockdown) → B2 optional CJK.

---

## 0. Executive Summary

**bitmagnet** is a self-hosted BitTorrent DHT crawler, content classifier, and torrent search engine (web UI + GraphQL + Torznab/Servarr) that indexes torrent _metadata_ discovered on the public DHT. To make every torrent's contents searchable and browsable, it stored one row per file across all torrents in a single `torrent_files` table that had grown to **~276 GB — 74% of a ~397 GB database**. This effort replaces that table while _keeping_ every per-file capability, under one hard rule: **prove-then-retire — nothing is dropped until each replacement layer is deployed and proven in production.** (Full background in §1.)

- **Phase 1 — Hybrid Blob (DEPLOYED).** Each torrent's file list is now also stored as a single compressed blob (`files_data` = zstd+msgpack, ~4.96× compression, **~19 GB total** vs 276 GB). Dual-write is live, the historical backfill is **complete (16,976,700 torrents, 0 left)**, and `verify --full` **passed (0 mismatches)**. The `DROP TABLE torrent_files` cutover is **DEFERRED** — the table remains the live fallback.
- **The question.** Dropping `torrent_files` removes three live query shapes (an extension/file-type filter, a per-torrent browser, content-result hydration) and forecloses a _net-new_ cross-file search. What restores them, cheaply, after the drop?
- **The answer was measured, not guessed.** A benchmark suite on the real 879.5M-row corpus **overturned the original assumption** that a per-file search index is required: the 873M-doc Tantivy structured index gives **no latency win** over DuckDB, costs +14–25 GB, and is scan-bound. The winner is a **DuckDB-on-Parquet + PostgreSQL-aggregate composition** — near-complete parity at **+4–12 GB** and **<150 ms** (most <35 ms).
- **L2 (PROPOSED, all-Rust).** `agg_torrent_ext` (a tiny PG rollup) restores the filter/facet — the **DROP gate** — while a DuckDB-on-Parquet **sidecar** provides the net-new cross-file search, kept **minute-fresh** by a base+delta pipeline. A prove-then-retire **checker** (all-Rust, by invariant composition) proves `agg ⟺ torrent_files` before any flip.
- **The one expensive carve-out.** Interactive **broad / CJK free-text** path search is the sole workload nothing cheap makes fast; a CJK-correct inverted index costs **~+90 GB** (it nearly triples the replacement footprint, cutting savings from −93% to −55%). It is **gated behind a hard, measured product need**, not built by default.

**Where things stand:** Phase 1 is deployed and verified; L2 is fully specified and even verified down to the sqlx bind layer; **no L2 code is built yet** (a deliberate hold); the DROP is deferred. The first implementation brick is L2-P0 (the `agg_torrent_ext` migration + the Rust checker).

---

## 1. Background & Problem Context

### What bitmagnet is

bitmagnet is a **self-hosted BitTorrent indexer, DHT crawler, content classifier, and torrent search engine** with a web UI, GraphQL API, and a **Torznab** endpoint for Sonarr/Radarr/Prowlarr (Servarr) integration (`README.md`). It crawls the public BitTorrent **DHT** to discover torrents, fetches each torrent's **metainfo** (info-hash + file list), classifies the content (movie / TV / music / …), and makes it searchable. It indexes **only metadata** — it is not a tracker, hosts no content, and stores no `.torrent` files or media — building a personal, ad-free torrent search index from what peers gossip on the DHT.

### How it works (the pipeline)

A single Go binary (urfave/cli + uber/fx) runs long-lived stages under `worker run`:

1. **DHT crawl** (`internal/dhtcrawler`) — sample info-hashes off the DHT, triage new ones (bloom filters), fetch metainfo, persist the torrent + file list (`persist.go`).
2. **Queue** (`internal/queue`) — a Postgres-backed `queue_jobs` table decouples crawl from processing.
3. **Process + classify** (`internal/processor` → `internal/classifier`) — upsert into `torrent_contents`; a CEL/YAML workflow infers `content_type`, parses release names, extracts keywords, and enriches movies/TV via **TMDB** (`internal/tmdb`).
4. **Store** — everything in **PostgreSQL**; full-text search is Postgres `tsvector`.
5. **Serve** (`internal/httpserver`) — the **GraphQL** API (`internal/gql`) feeds the web UI, plus the **Torznab** XML endpoint.

A separate **bulk importer** (`internal/importer`) ingests metadata dumps into the same pipeline (no file list — torrents enter `FilesStatusNoInfo`, filled later by the crawler).

### The data model

Everything is keyed off the 20-byte BitTorrent info-hash (`migrations/00001_init.sql`):

- **`torrents`** — one row per torrent (the spine): PK `info_hash bytea`, `name`, `size`, `files_status`, `files_count`, a path-derived generated `extension` (single-file) — **plus the migration's `files_data BYTEA` blob and `file_extensions JSONB`** (§2).
- **`torrent_files`** — **one row per file**: PK `(info_hash, path)`, `index`, `path`, `size`, and a **GENERATED** `extension` path-derived via `[^/.]\.([a-z0-9]+)$` (`00001_init.sql:65-78`). **~857M rows / ~276 GB** vs ~46M torrents — the giant this work replaces.
- **`torrent_contents`** — **one row per classified content item = the search target**: `content_type`, denormalized release/video attributes, and a generated **`tsv tsvector` + GIN index** (the ~14 GB FTS structure).
- **`torrent_file_summary`** — a per-torrent digest (`file_count`, `total_size`, `largest_file_size`, distinct `extensions`, `has_video/audio/subtitle`) added by the migration.
- **`torrents_torrent_sources`** (provenance + mutable seeders/leechers/published_at), **`torrent_hints`** (classifier hints), **`content`** (TMDB/IMDB metadata).

### Why `torrent_files` exists, and how search uses it

`torrent_files` is the **per-file surface** — the only place a torrent's contents are individually addressable. It powers (1) the **file browser** (the `torrent.files` GraphQL query), (2) **extension / file-type filtering + faceting** on search — the file-type facet resolves a `FileType` → its extensions and filters via `EXISTS(SELECT … FROM torrent_files WHERE info_hash = … AND extension IN (…))` (`criteria_torrent_file_extension.go:23-34`), and (3) exact per-file size/path/count.

**Crucially, _main_ search is DROP-independent.** It runs over `torrent_contents` via PostgreSQL FTS (`tsv @@ tsquery` + `ts_rank_cd` ranking, `query.go:618,647`) — never `torrent_files`. So dropping the per-file table leaves the primary search query untouched; only the **file-type facet/filter** and the **browser** need re-pointing at the blob — exactly what this work supplies.

### The problem

`torrent_files` had grown to **~857M rows / ~276 GB (heap ~119 + indexes ~157)** — **74% of the ~397 GB database** of ~46M torrents (only ~17M / ~35% carry file lists). It is the single largest object and the dominant cost of running bitmagnet. The goal: **shrink the database dramatically without losing any per-file capability** — and ideally _gain_ real cross-file search ("find every `.mkv` over 1 GB"), which `torrent_files` could serve only at its 276 GB cost.

### The constraint: prove-then-retire

Keep `torrent_files` as the live source-of-truth / fallback and retire it **only** once every replacement layer is **deployed AND proven in production**, layer by layer — not on benchmarks. The `DROP` is the _last_ step, gated on that proof + a fresh off-host backup. So L2 is additive, reversible, and runs _beside_ the live table.

### The layered solution map

| Layer                              | What                                                                                                                                   | Status                                        | Footprint        |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- | ---------------- |
| **L1 — the blob**                  | per-torrent `files_data` (the DROP gate's data)                                                                                        | **DEPLOYED**                                  | ~19 GB           |
| **L2 — cheap search composition**  | PG extension filter/facet (deployed `file_extensions` JSONB; `agg_torrent_ext` only if needed) + DuckDB-on-Parquet (cross-file search) | **PROPOSED**                                  | ~+8–18 GB all-in |
| **L3 — broad CJK free-text index** | a char-ngram inverted index                                                                                                            | **GATED** (build only on a hard product need) | ~+90 GB          |

The rest of this document is: what L1 does (§2), what the DROP removes (§3), what the suite measured (§4), the L2 design (§5), the reasoning (§6), and where we are (§7).

---

## 2. The Hybrid Blob Migration (DEPLOYED)

> **Status:** Phase 1 is **DEPLOYED** on the FSN1 node (`<fsn1-host>`) — dual-write live, historical backfill **COMPLETE**, `verify --full` **PASSED**. The cutover (`DROP TABLE torrent_files`) and all index drops are **DEFERRED indefinitely** under a no-deletion constraint; nothing in the deployed state removes data.

### Why: the `torrent_files` problem

The legacy schema stores one row per file across all torrents in a single `torrent_files` table. On the live database this had grown to **~276 GB** (heap ~119 GB + indexes ~157 GB) across **~857M rows** — **74% of a ~397 GB / 372 GiB database** of ~46.0M torrents. Only **~16.97M torrents (~35%)** actually carry file lists; the rest are single-file or metadata-only. The migration replaces those ~857M individual rows with **one compressed blob per torrent**, projected to shrink the database from **~397 GB → ~121 GB (≈70%)** once the table is eventually dropped — the blob replacement itself is only **~19 GB** (≈16 GB `files_data` + ~3.3 GB `torrent_file_summary`).

### The blob format and serialization algorithm

Each torrent's file list is serialized into a single `torrents.files_data BYTEA` column as **`zstd( msgpack_array[ {i,p,e,s}, … ] )`**:

- **MessagePack** (`vmihailenco/msgpack/v5`) encodes each file as a _named map_ keyed by the compact tags `i` (index, int), `p` (path, string), `e` (extension, string), `s` (size, uint) — `internal/blobmigration/serializer.go:21-45` (`compactFile` + `SerializeFiles`).
- **ZSTD** at `SpeedDefault` (**≈ level 3**) wraps the MessagePack bytes (`serializer.go:17`). Measured compression is **≈4.96×**, yielding **~1.0 KB average per blob** and **~16 GB** total `files_data` (stored in TOAST), versus the 273–276 GB table.
- The Rust side mirrors this **byte-for-byte** (`bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:30-63`): `#[serde(rename = "i"/"p"/"e"/"s")]` + `rmp_serde::to_vec_named` (the default positional array would be rejected by the Go decoder), ZSTD level 3, magic `0x28B52FFD`. An empty `e` corresponds to a SQL `NULL` extension.

Two derived structures are computed alongside the blob:

- **`file_extensions JSONB`** on `torrents` (default `'[]'`) — the set of unique extensions, GIN-indexed for facet/filter queries.
- **`torrent_file_summary`** — a per-torrent rollup row: `file_count`, `total_size`, `largest_file_size`, `extensions JSONB`, and `has_video`/`has_audio`/`has_subtitle` flags (`serializer.go:99-134`, `BuildFileSummary`).

#### The G1 rule: extension is derived from the path, never from the stored `e`

Unique-extension extraction (`ExtractUniqueExtensions`, `serializer.go:74-97`) and the summary derive every extension from the **file path** via `model.FileExtensionFromPath` — the regex **`[^/.]\.([a-z0-9]+)$`** over the lowercased path (`internal/model/torrent_files.go:33-42`). This deliberately mirrors the legacy PostgreSQL generated column (`migrations/00002_files_status.sql:10-12`) so blob-derived extensions exactly match the old query semantics.

> ⚠️ **Known data-at-rest defect (G1, fix deferred, no current UI impact):** the blob's per-file `e` field is filled from `f.Extension.String` (`serializer.go:34`), which for **crawl-path** torrents is the legacy generated column that was _never populated before serialization_ → those blobs carry an **empty `e`**. Backfilled blobs are correct, so the corpus is split. Because `file_extensions`/summary derive from the _path_ (G1-correct) and the live file browser still reads `torrent_files`, this is **not a live regression** — it only activates when a consumer reads extension _from the blob `e`_ (the future browser re-point or a file-search index). The fix is code-only (derive `e` from path); it is tracked as a precondition for the eventual cutover, not for the deployed state.

### Schema migrations 00021–00023

Migrations are **auto-applied by goose on startup** during fx boot, **before `/status` serves** (`internal/database/migrations/decorator.go:35` → `goose.UpContext`), so a slow migration gates readiness/liveness. The deployed image was built to include **only 00021 + 00022**:

| Migration                                         | What it adds                                                                                                                                                                    | Live-DB behavior                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **00021** `blob_storage`                          | `ADD COLUMN files_data BYTEA` (nullable) + `file_extensions JSONB NOT NULL DEFAULT '[]'` + `CREATE TABLE torrent_file_summary` (PK `info_hash` FK→torrents `ON DELETE CASCADE`) | Metadata-only on PG16 (non-volatile default) → **instant**, brief `ACCESS EXCLUSIVE` only                                                                                                                                                                                                                                                                        |
| **00022** `blob_indexes`                          | `CREATE INDEX CONCURRENTLY … GIN (file_extensions jsonb_path_ops)` on `torrents` and on `torrent_file_summary.extensions`; **`-- +goose NO TRANSACTION`**                       | Non-blocking to writes; column is 100% `'[]'` so the index is tiny                                                                                                                                                                                                                                                                                               |
| **00023** `v2_infohash` (**EXCLUDED — DEFERRED**) | BEP-52 v2: `info_hash_v1`/`info_hash_v2`/`meta_version` columns + 2 indexes + a full-table `UPDATE`                                                                             | 🚫 **Crashloops the live DB**: 2× _non-concurrent_ `CREATE INDEX` (SHARE-locks 48M rows) + a full-table `UPDATE` in **one startup goose transaction** → exceeds the readiness window → pod killed → txn rolls back → infinite CrashLoopBackOff. Must be rewritten to instant DDL + `CONCURRENTLY` + a batched post-startup backfill before any v2 phase deploys. |

Rollback during the entire dual-write window = **redeploy the official image** (old goose sees only ≤00020 → clean no-op; GORM ignores the unknown columns/table). **Never `goose down`** — its Down section drops `files_data` and would destroy migrated blobs.

### Dual-write: keeping blob ≡ `torrent_files`

The invariant is that the blob is always derived from the _same_ file slice that produces the `torrent_files` rows, written in the **same transaction**. There are three write sites:

1. **Crawler (`internal/dhtcrawler/persist.go:165-262`).** `createTorrentModel` builds one `[]model.TorrentFile` slice (`files`), capped at **`SAVE_FILES_THRESHOLD` (default 100)** files per torrent (`internal/dhtcrawler/config.go:32`). That _same capped slice_ feeds both the legacy `torrent_files` rows (`Files: files`) **and** the blob (`persist.go:225-230`). Because both copies come from one slice, they cannot diverge. Classification: **single-file** → no file rows; **multi-file** under the cap → `FilesStatusMulti`; **over the cap** → slice truncated at 100, marked `FilesStatusOverThreshold` (`persist.go:200-217`).
2. **Backfill (`internal/blobmigration/queue/handler.go`).** For each historical `info_hash` it reads **all** `torrent_files` rows verbatim (`processBatch`, ordered by `Index`), serializes them, and writes `files_data` + `file_extensions` + upserts the summary (`handler.go:149-211`). No 100-cap on the historical path → blobs faithfully reproduce the existing table.
3. **Importer (`internal/importer/importer.go:257`).** Writes **no file rows and no blob** — torrents enter as `FilesStatusNoInfo` (`importer.go:263`); files are filled later by the crawler. Nothing to keep consistent here.

Reads are transparent: the `AfterFind` GORM hook on `Torrent` decompresses `files_data` into `t.Files` whenever a blob is present (`internal/model/torrents.go:19-39`), so callers see files regardless of source.

### Consistency checker and live self-heal

- **`CompareFiles` (`consistency/checker.go:37-98`):** compares blob-decoded files against `torrent_files` rows — count first, then per-file **index / path / size** sorted by `Index`.
- **`CheckRandom` / `CheckBatch` (`checker.go:145-184`):** samples N random migrated torrents and aggregates a match/mismatch/error `Summary`.
- **Inline backfill sampling:** each batch checks **5%** of migrated torrents and **auto-pauses** if the error rate exceeds **1%** (`handler.go:95-101`).
- **`verify` subcommand:** on-demand `--sample-rate` (default 0.1) or `--full`; on success stamps `blob_migration:verified_at` (`blobmigrationcmd/command.go:294-381`).
- **`LiveChecker` self-heal (`consistency/live_checker.go`):** a periodic ticker runs `CheckRandom` and on any mismatch **NULLs `files_data`** for the offending torrent (`healTorrent`, `live_checker.go:103-114`) → it re-enters the backfill frontier and is re-migrated rather than served. _(This self-heal is why a future agg-parity check must use a separate counter — see §5.)_

### The DROP path (DEFERRED — never run)

Cutover lives behind `blob-migration cleanup` (`blobmigrationcmd/command.go:384-502`), which enforces **four safety gates**: (1) status `completed`; (2) **zero unmigrated** torrents; (3) a verification that **passed within 24 h**; (4) explicit **`--confirm`**. Only then does it run **`DROP TABLE IF EXISTS torrent_files`** (`command.go:419`) + plain `VACUUM` — `DROP TABLE` frees the ~276 GB to the OS immediately (no `VACUUM FULL`, no 2× trap). Per the no-deletion constraint this is **held indefinitely** behind a separate go-ahead and a fresh off-host backup, additionally gated on per-file search parity being proven (since `torrent_files` is both the parity ground truth _and_ the table being dropped).

### Current DEPLOYED state (operational truth)

- **Image:** fork `dashed/bitmagnet` from the Phase-1 commit (migrations **00021 + 00022 only**, 00023 excluded), iterated `blob-phase1b → blob-phase1h`; single StatefulSet on `<fsn1-host>` behind a gluetun VPN sidecar + a separate `bitmagnet-postgres` (PG 16-alpine). Search stays `postgres` (no Tantivy sidecar).
- **Backfill COMPLETE (2026-06-06):** `torrent_file_summary` = **16,976,700 rows**, **0** with-files torrents unmigrated, `status = completed`.
- **Verified:** `verify --full` **PASSED — 16,977,232 checked, 0 mismatches / 0 errors** (~11 min).
- **Throughput:** **~6 t/s → peak ~13,730 t/s (≈2,300×)** after fixing 5 fork correctness bugs + ~10 perf/PG bottlenecks (key wins: drive discovery from `torrents WHERE files_data IS NULL` not `SELECT DISTINCT … NOT EXISTS` over `torrent_files`; real K-way concurrency via dispatcher `CheckInterval` + a pgx pool of 80; PG CPU limit to 12 cores; WAL/autovacuum tuning). Final ceiling = the app pod's 8-core CPU (ZSTD) + node saturation.
- 🚨 **K=32 PG-crash incident + recovery:** parallelism 32 saturated the node and tripped PostgreSQL's **liveness** probe → CrashLoopBackOff (WAL-recovery loop). Recovered and made sustainable at **K=16** by adding a PG **startupProbe** (slow WAL recovery no longer trips liveness) and raising **`max_wal_size`** (16 GB). Steady config: parallelism 16, pgx pool 80, PG limits **12 CPU / 24 Gi**.
- **DEFERRED:** the `DROP TABLE torrent_files` cutover and **all** index drops remain unrun.

---

## 3. What the DROP Removes (the per-file query surface)

> **PROPOSED.** This specifies the replacement for the per-file read surface that disappears when `torrent_files` is eventually dropped. Per the prove-then-retire constraint, L2 runs _beside_ the live table, proves parity, then becomes primary; the DROP is a separate, later, still-deferred decision.

Surveying the Go search path, the per-file read surface is exactly **three live query shapes** plus **one net-new capability**. They do **not** all belong to L2 — distinguishing them is what makes the replacement cheap:

| #         | Query shape                                                                                                                                       | Current source                                                                          | Where it goes                                               |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| **(a)**   | **file-extension / file-type EXISTS filter + facet**: `EXISTS(torrent_files … extension IN (:exts))` (`criteria_torrent_file_extension.go:24-34`) | **live `torrent_files` read** — the _only_ one in the search hot path                   | **L2a → PG `agg_torrent_ext`** (the DROP-gate parity piece) |
| **(b)**   | **per-torrent file list / browser**: `SELECT * FROM torrent_files` (`search_torrent_files.go:17-29`, via `TorrentQuery.files`)                    | already migrated → `files_data` blob via `AfterFind`; residual SELECT needs re-pointing | **L1 / Phase-A (G2)** — _not_ L2                            |
| **(c)**   | **content-result file hydration**: preload the `Files` has-many (`hydrator_torrent_content_torrent.go`)                                           | blob `AfterFind` for migrated rows                                                      | **L1 / Phase-A** — _not_ L2                                 |
| **(NEW)** | **cross-file search** — "find `.mkv` > 1 GB across all torrents", path substring, file-level analytics / collapse / ranges                        | **does not exist today** (the EXISTS only answers "does _this_ torrent contain ext X")  | **L2b → DuckDB-on-Parquet sidecar** (additive)              |

Two facts from the code make shape (a) far smaller than it looks:

- **The facet rides the same criteria.** `TorrentFileTypeCriteria` flattens `FileType → []extension` and **delegates to `TorrentFileExtensionCriteria`**; the facet runs one `BudgetedCount(*)` per file-type value over the base query filtered by that EXISTS. So it is **distinct-torrent _presence_ semantics** — _not_ a `GROUP BY` / `COUNT(DISTINCT)`. ⟹ swap the one criterion and the facet is restored **for free**, and the rollup needs only `(info_hash, extension)` presence.
- **Shapes (b)/(c) are already L1 (blob)** — listed only so the parity gate covers them; no L2 obligation.

> **L2 = L2a (PG `agg_torrent_ext`, restores shape (a), the DROP gate) + L2b (DuckDB-on-Parquet sidecar, the net-new cross-file search).** The live-parity obligation is **almost entirely L2a**. L2b is net-new → a _correctness_ gate (vs offline `torrent_files` ground truth) but **no live-parity** obligation.

---

## 4. Empirical Foundation — Benchmarks & Experiments (MEASURED)

> Every number here is **measured on the real 879.5M-row corpus** on a **throwaway PostgreSQL restored to an idle host (`<hel1-host>`)**; **production (`<fsn1-host>`) was never touched**. Extrapolations are flagged.

### Headline verdict

The suite was built to replace estimates with measurements before committing to a design. The measurements **overturned the central assumption** (that a per-file Tantivy index is needed for `<50 ms` search):

- **Ship the cheap composition** — code fixes (G1/G2/hydration) + a per-(torrent,ext) PG aggregate + **DuckDB-on-Parquet** — for near-complete parity at **+3.9–12.3 GB** and sub-150 ms.
- **Reject / defer the 873M-doc structured Tantivy file index** — _no_ latency win over DuckDB, +14–25 GB, _slower_ on the common broad filter at scale.
- **Gate any inverted index strictly on a hard product need** for interactive broad/CJK free-text — the one workload nothing cheap makes interactive, at **~+90 GB** for a CJK-correct index.

### Measurement provenance (why the live and bench numbers differ)

The live (production) figures and the benchmark figures come from **different snapshots** — a reader should not expect them to match exactly:

| Metric                    | Value                     | Source / time            | Meaning                                   |
| ------------------------- | ------------------------- | ------------------------ | ----------------------------------------- |
| live `torrent_files` size | ~276 GB                   | FSN1, migration planning | live DB object size (with index bloat)    |
| bench `torrent_files`     | 879,474,880 rows / 261 GB | HEL1 restored dump       | benchmark corpus (fresh-packed, no bloat) |
| live `torrent_files` rows | ~857M                     | FSN1                     | a later/other production count            |
| blobs verified            | 16,977,232                | FSN1 `verify --full`     | torrents checked                          |
| summary rows              | 16,976,700                | FSN1 backfill complete   | summary rows at completion                |

The bench corpus is the **pre-cutover restored dump** (`torrent_files` intact, blobs empty); the deployed state is production at a later time. The small count deltas (857M vs 879.5M; 16,976,700 vs 16,977,232; 276 vs 261 GB) are this provenance gap, not an inconsistency.

### 1. Bench harness + safe data source

A **35 GB pre-cutover `pg_dump -Fc`** (taken _before_ the blob backfill) was restored to a disposable PostgreSQL on the idle `<hel1-host>`. Restore 35 GB → **353 GB**, then `ANALYZE`. Verified: **`torrent_files` 879,474,880 rows / 261 GB**, 48.13M torrents, **16,973,470** with-files. 🚨 Because the dump pre-dates the backfill, its blobs are empty — but it carries the full `torrent_files`, whose `extension` is a **path-derived GENERATED column = G1-correct**; so benches source per-file data **directly from `torrent_files`** (no blob regen). Corpus shape: **avg 51.79 files/torrent** (p50 6, p90 54, p99 743, **max 88,561**); blob corpus **14.5 GB** (zstd **4.96×**); **8.06% single-file**, **6.04% over-threshold**, null-ext **6.8%**.

### 2. RUN-2 — DuckDB-on-Parquet latency

Export from `torrent_files` → **slim Parquet 3.86 GB** (confirms +3–5 GB) / full+path 11.71 GB.

| query                               | warm p50   | cold   |
| ----------------------------------- | ---------- | ------ |
| `mkv > 1GB` paginated LIMIT 1000    | **35 ms**  | 199 ms |
| `GROUP BY extension` (all 879M)     | 1.29 s     | 1.20 s |
| `COUNT DISTINCT info_hash` collapse | 1.27 s     | 1.28 s |
| path-FTS `ILIKE '%S01E%'` LIMIT 100 | **142 ms** | 178 ms |
| single-torrent hydrate (point)      | **17 ms**  | 27 ms  |

**Conclusion:** every realistic query is **0.015–1.3 s** on the full corpus; **cold ≈ warm** (15.5 GB stays resident in 125 GB RAM). The only >2 s form (return _all_ 5.7M matching rows = 14.2 s) is Python `fetchall()` materialization, not scan — UIs paginate (35 ms) or count (1.27 s).

### 3. RUN-3 — sizing (aggregate vs slim PG table)

- **Per-(torrent,ext) aggregate** = **~55M rows** (3.30 pairs/torrent, validated); **5.5 GB** surrogate / **8.4 GB** natural with full `{max,min,count}`; **~5–6 GB for a `max_size`-only natural-key form**. Distinct extensions **47,628** → needs int4.
- **Slim per-file PG table REJECTED** — +**78–113 GB** structural overhead (~10–15× the aggregate).

### 4. RUN-4 — the 873M-doc Tantivy file index (decisive negative)

`bitmagnet-search-bench` (tantivy 0.26.1), all 11 schema variants at 1M/10M/50M docs; per-doc figures flat → extrapolate to 879.5M.

- **Size:** V10 FAST-only ≈ **25.4 GB**; optimized (FAST identity, no fieldnorms) ≈ **13.7 GB**; +path v1.1 ≈ 44.9 GB. A **STORED `doc_id` was 12 GB** (spec guessed 1–2 GB). Size never says NO-GO.
- **Latency — `<50 ms` REFUTED:** the common `ext∧size` filter (~950k matches) is **scan-bound** (no selective text term → no early-termination): **p50 72.9 ms @50M → ~1.3 s p50 / ~3.7 s p95 @879.5M**. INDEXED ≈ FAST (zero range benefit). ➡️ **No latency win over DuckDB**, at +14–25 GB. **GATE: reject the file index for v1.**

### 5. ARCH-C — DuckDB latency optimization ("can we add indexes?")

| query                         | unsorted 3.86 GB | sorted(ext,size) 10.3 GB | rollup table | winner                   |
| ----------------------------- | ---------------- | ------------------------ | ------------ | ------------------------ |
| distinct-torrent **collapse** | 1311 ms          | 132 ms                   | **5.2 ms**   | **rollup**               |
| **GROUP BY** extension        | 1425 ms          | 751 ms                   | **2.3 ms**   | **rollup**               |
| exact COUNT (ext∧size)        | 1024 ms          | **17 ms**                | —            | sorted (zonemap)         |
| paginated FIND (common)       | **30 ms**        | 56 ms                    | —            | already fast (early-out) |

**Three levers, ranked:** (1) **native rollup TABLES = THE `<50 ms` lever** (+~2 GB; work even unsorted); (2) **sort-by-(ext,size)** → row-group min/max pruning for ranges/counts (+6.4 GB, as sorting decorrelates `info_hash` RLE); (3) **ART `CREATE INDEX` does NOT accelerate analytical scans** (EXPLAIN: `seq_scan`; DuckDB has no analytical IndexScan — the speedup is zonemaps, not ART; +50 GB ❌). **FTS/BM25 on path: 23 s → 150 ms but +34.9 GB, no CJK segmentation** — the sole genuine inverted-index carve-out. **Recommended layout ≈ 12.3 GB** (sorted slim + 2 rollup tables) → every structured query **<150 ms (most <35 ms)**. **ARCH-F:** 6/8 future-query classes are _just new SQL_ on existing Parquet; only BEP-52 v2 per-file merkle needs new per-file data.

### 6. EXP-A — write path + main-search wall (DROP-independence)

`torrent_contents` upsert **0.19 ms/row warm** (dominated by a 14 GB `content_type_tsv` GIN); the Go tsvector build is **super-linear O(n²)** in files/torrent (0.42 ms @52 → 387 ms @5000). Main-search latency is governed by **`ts_rank_cd` over the match set, NOT the GIN**: rare ranked 23 ms; broad `x264` ranked **49 s** (the GIN match alone is 482 ms). ✅ **DROP-independence confirmed:** served search touches only `torrent_contents` — **zero `torrent_files`**. (The broad-ranked wall is a separate, pre-existing issue, FIND-2.)

### 7. EXP-B — DuckDB base+delta freshness

| delta (torrents)        | collapse p50 | find p50 |
| ----------------------- | ------------ | -------- |
| 0 (base)                | 141 ms       | 56 ms    |
| +100k (~hours of crawl) | **230 ms**   | 91 ms    |

**Conclusion:** **~minute freshness at <250 ms, no full rebuild.** 🚨 Supersession is **TORRENT-granular via predicate-then-ANTI-JOIN** (re-crawl replaces the whole fileset). `row_number()…=1` is **WRONG** (drops a torrent's other files); window-max is **80× slower** (19 s vs 230 ms). Compaction ≈ 1M-torrent delta → 83 s rebuild + atomic swap.

### 8. EXP-D — CJK path tokenizer (50M)

**15.2% of files carry a CJK codepoint.** Default tokenizer mid-run CJK recall **0.0037** (broken) → **ngram 1.0** recall+precision, at path-field **21.5 → 103 B/doc (~4.8×)** → **~18.9 → ~90.9 GB** at full corpus.

### 9. EXP-D2 — full 879.5M ngram build + latency

**94 GB** index. **CJK free-text is interactive at full scale:** warm p50 **0.07 ms** / cold 0.86 ms / p99 80 ms; broadest ASCII grams (5.6M hits) **100–145 ms p50** — all **sub-second vs DuckDB-ILIKE's ~23 s**. The `<50 ms` premise **holds for free-text** — but only at **~+90 GB**.

### 10. EXP-E — inverted-index freshness under live dual-write

Base 20M with default `LogMergePolicy`: **fresh-lag ≈ 2 ms, flat** across +1k/+10k/+100k; segments **bounded** (29→17–21); supersession via `delete_term` + re-add **11 ms**. This millisecond freshness is the inverted index's genuine edge over batch DuckDB (whose freshness = ~minute flush cadence, EXP-B).

### 11. Space savings vs `torrent_files` (~276 GB)

| layer                                   | footprint                                                                                                                                                           |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **L1 — blob (deployed)**                | `files_data` ~16 GB + summary 3.3 GB = **~19 GB**                                                                                                                   |
| **L2 — cheap search** (per component)   | **DuckDB** slim +3.9 GB / optimized +12.3 GB · **PG agg** +3–6 GB _(or +0 if the JSONB path clears the gate — see the Revisions banner)_ → **all-in L2 ≈ +8–18 GB** |
| **L3 — CJK free-text index (optional)** | **per-torrent path-bag 13.54 GiB** (PS-MB1, measured) · ~~per-file ~90 GB~~ (superseded)                                                                            |

_(All-in L1+L2 ≈ **27–37 GB**, matching the scenario table below. The earlier "+4–12 GB" conflated DuckDB-only with the all-in figure.)_

| scenario                                     | total       | vs 276 GB |
| -------------------------------------------- | ----------- | --------- |
| Migration only (blob)                        | ~19 GB      | **−93%**  |
| + cheap search                               | ~27 GB      | **−90%**  |
| + optimized search                           | ~35 GB      | **−87%**  |
| + free-text index (**per-torrent**, PS-MB1)  | ~**48 GB**  | **−83%**  |
| ~~+ free-text index (per-FILE, superseded)~~ | ~~~125 GB~~ | ~~−55%~~  |

**Headline:** the migration alone is **~93%**; complete per-file _search_ parity barely dents it (**~87%**). The free-text index used to read as the swing factor (−55%) on the **per-file** ngram — but **PS-MB1 measured the per-torrent path-bag form at 13.54 GiB**, so even _with_ interactive free-text the saving is **~83%**. The index stays **NO-GO by default** (gate on a hard demonstrated demand) — not because it's expensive anymore, but because no demand has been shown; it remains purely additive and never gates the DROP. See §12.

### Synthesis — structured vs broad free-text

| Workload                                                                 | DuckDB-on-Parquet                         | PG                     | Inverted index                                                                                                                             |
| ------------------------------------------------------------------------ | ----------------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Structured** (ext∧size, collapse, ranges, counts, analytics, faceting) | **<250 ms** (most <35 ms) at +3.9–12.3 GB | <50 ms (PG aggregate)  | no advantage (+14–50 GB)                                                                                                                   |
| **Broad free-text** (ranked / leading-wildcard substring)                | ~23 s (ILIKE)                             | 15–49 s (`ts_rank_cd`) | **<1 ms–sub-second** (only winner) — +35 GB ASCII (DuckDB-FTS) · per-file ~90 GB · **per-torrent path-bag 13.54 GiB CJK-correct (PS-MB1)** |

The existing PG main search already lives with the broad-ranked wall and is DROP-independent — so the file-search decision introduces nothing new. **Ship the cheap composition; reject the structured Tantivy index; gate any inverted index on a measured `<50 ms` broad/CJK free-text need** — and if it ever is built, the cheap form is the **per-torrent path-bag char-ngram `WithFreqs`** index (§12), not the per-file one.

### 12. PS-MB1 — per-torrent path-bag (the L3 reframing, MEASURED 2026-06-09)

The realtime per-keystroke `<50 ms` free-text **path** search question got its own 5-thread investigation (PS-T1–T5, [`pathsearch-master-investigation.md`](./pathsearch-master-investigation.md)) + a gated micro-bench that **ran on the full 879.5 M-row restore** ([`pathsearch-microbench-RESULTS.md`](./pathsearch-microbench-RESULTS.md), spec [`pathsearch-microbench-spec.md`](./pathsearch-microbench-spec.md)).

- **Decision: NO-GO by default.** Greenfield on both UI and backend, zero demand signal; `<50 ms` is structurally met only at the median (per-keystroke generates the _broadest_ match-sets first); purely additive; **never gates the `torrent_files` DROP** (the build-gate is unchanged).
- **The cost case flipped (measured).** Per-FILE ngram = ~90 GB (873 M docs, EXP-D2) — a footprint-tripler. The **per-torrent path-bag** (one doc per torrent, ~17 M docs; each file path a separate field value so no cross-file boundary grams) measured **13.54 GiB** production (`WithFreqs`; as-built 81.86 GiB − **83.5 % dead-weight positions** dropped — the lossless unlock). Latency `ascii3` warm **p50 24.71 ms**, CJK **0.21 ms**, recall **1.0000**; the broadest synthetic substrings breach 50 ms only at the **p95/p99 tail (~55–65 ms)** — median-interactive, mitigated by min-chars≥3 + real-query selectivity + debounce + top-k.
- **Alternatives rejected (PS-T2/PS-MB1):** edge-ngram (bigger in prod 21.3 GiB _and_ misses substrings, `264`→0.19 recall); external engines (Meilisearch/Typesense are _prefix not infix_; Quickwit misses local `<50 ms`; Manticore a lone gated-spike; pg_trgm loses); per-file ngram (90 GB, breaks the gate at scale).
- **Net:** adding L3 now costs ~13.5 GiB and drops the saving only **87 % → ~83 %** (was −55 %). It's a cheap, viable add-on **if** a real product demand + an in-prod ILIKE wall ever fire — otherwise deferred.

### 13. PSX campaign — built confirmation + production-shape latency + the L2 gap closed (MEASURED 2026-06-09)

After the user committed to building L3, a follow-up campaign ([`psx-campaign-RESULTS.md`](./psx-campaign-RESULTS.md)) **built** the production artifact and closed the last measurement gap:

- **L3 WithFreqs index BUILT (was computed by `.pos` subtraction):** **13.32 GiB** measured (vs the 13.54 GiB prediction, −1.6 %; positions ≈ 0), recall **1.0000**, 16,973,470 docs — confirming the lossless ~83 % cut (82 GB → 13.3 GiB) and that `WithFreqs` is latency-neutral warm / better cold. **Deployable artifact.**
- 🔧 **Production-shape latency correction:** the broad-substring **p95 ≈ 55–65 ms** cited above is a `Count`-collector **lower bound**. The real production page collector (`TopDocs` ordered by a fast field, e.g. seeders) adds **+20–57 %** with no early-termination → **broad-single-gram p95 ≈ 77–94 ms**. Realistic _multi-word_ queries stay **< 50 ms** (two-word 21.9 ms, dotted 47.8 ms, CJK 1.7 ms); only the degenerate single broad gram tails, and that tail is **engine-irreducible** (source-proven in Tantivy 0.26.1: index-sort removed, `order_by_fast_field` full-scans, `collect` can't abort) → a **UX** matter (debounce / min-chars / loading-state), not an engine one.
- ✅ **The L2 blob→Parquet pipeline is now validated on REAL blobs** (D1 — closes "every L2 number was sourced from `torrent_files` by proxy"): bench-encoded prod-format blobs decode with **0 errors**, blob-sourced Parquet == `torrent_files`-sourced **byte-for-byte** (slim + full incl. `path`), decode **0.746 µs/file** (in the predicted 0.6–0.94 band), encode 0.458 µs/file (Rust-indicative). _(Sample: 5.15 M files; full-corpus run is bench-read-bound ~8 h — a NodePort+sqlx artifact, not production — so not executed.)_
- ↩ **`agg_torrent_ext` RETIRED:** `ext ∧ max_size` (the one query `file_extensions` JSONB can't serve) is served by the DuckDB tier at 5–132 ms with **+0 PG disk** — re-adding ~10 GB + a pipeline to PG is against the DROP goal.
- 🔎 **FIND-2 (separate, DROP-independent):** the main search's broad-term relevance wall (`ts_rank_cd`, x264 ≈ 49 s) has a **cheap code-only fix** — default broad typed queries to popularity sort (`seeders`/`published_at` btree backward scan = **1.9–4.9 ms**); offer relevance as an honest opt-in. RUM **deferred** (write-amp on the upsert-heavy table + 30–50 GB).

### 14. CB campaign — concurrency/load (MEASURED 2026-06-10; closes the last gap)

The one remaining unmeasured production dimension — concurrency — is now measured ([`cb-campaign-RESULTS.md`](./cb-campaign-RESULTS.md)); **single-client latency survives production concurrency**:

- **L3 readers (E1):** graceful to **24 concurrent readers** — gate-row p95 grows only **1.86–2.6×** at 24× load (QPS 26→291, plateaus at core count, no collapse); N=1 reproduces the PSX baselines exactly.
- **L3 readers + live writer (E2a/E2b):** the always-on single writer is **invisible to readers** (p95 ≤1.05× baseline even at 50 torrents/s); commit ~13–17 ms keeps 50 t/s with headroom; **fresh-lag sub-ms p95 under full read load**; per-torrent `delete_term` supersession **correct + 5.2 ms under load** (closes the deferred freshness item); segments bounded. 📐 **Deployable L3 index size = 14.0 GiB** (15,017,420,811 B — the keyed build with the mandatory `info_hash` delete-key; the 13.32 GiB figure is the keyless variant; the key adds **no read cost**).
- **L2 DuckDB (E3):** cursors of one connection **parallelize** (QPS scales 10→27; the docs' contradiction resolved empirically; separate connections add nothing — same instance). The `<250 ms @ N=8` bar **holds for rollup-backed/light shapes to N=16**; unbounded `COUNT(DISTINCT)`/full-scan shapes are CPU-bound and breach at N=2–4 → **route heavy shapes through rollups**. Memory never the constraint (zero spill). _(A hydrate "breach" was a cold-harness `disable_object_cache` artifact — warm sidecar ≈17 ms.)_
- **Sidecar config that falls out:** one instance + cursor pool, per-query `threads≈4`, semaphore at the knee (~4–8), heavy ranges via rollups, serve the optimized Parquet (the raw native table measured 100–1000× slower), run warm. gRPC-layer overhead remains the only deploy-time validation.

---

## 5. L2 Architecture (PROPOSED)

### The three-tier model

| Tier                                | Covers                                                                      | Store                                                                     | Freshness                | Latency                   |
| ----------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------ | ------------------------- |
| **0 — served, in-app**              | browse (b), hydration (c), **ext/file-type EXISTS filter + facet** (a)      | the **blob** + **PG `agg_torrent_ext`**                                   | real-time / minute       | <50 ms                    |
| **1 — interactive per-file search** | cross-file `ext∧size`, ranges, path substring, exact counts, collapse       | **DuckDB** over **sorted slim Parquet + native rollup tables** (~12.3 GB) | ≤ delta cadence (~1 min) | **<150 ms** (most <35 ms) |
| **2 — analytics / arbitrary SQL**   | histograms, percentiles, GROUP BY, cross-store JOINs, dedup, future queries | same DuckDB                                                               | ≤ delta cadence          | 0.03–few s                |

**Tier 0 alone clears the DROP.** Marginal disk ≈ **+4–16 GB** total — an **~10× smaller** deploy surface than the rejected Tantivy file-index sidecar.

### Components

**1. `agg_torrent_ext` (PG rollup — L2a, the DROP gate).** Per-`(info_hash, extension)`, derived **from the blob** (the post-DROP source), modeled on the existing `torrent_file_summary`. Stores **only valid extensions** with `max_size`. Restores shape (a) by a one-line flag-gated EXISTS swap.

**2. `bitmagnet-parquet` (new crate — export/refresh).** Productionizes the throwaway `bench/blob_export`. (i) **Base export** = a `files` fact Parquet `(info_hash, file_index, path, extension, size)` **+ denorm** `content_type, published_at, created_at`, **`ORDER BY (extension, size)`** (zone-map pruning: collapse 1311→132 ms, count 1024→17 ms; +6.4 GB), `ROW_GROUP_SIZE 1M`, ZSTD, **bloom OFF**; plus **native rollup TABLES** (`agg_ext` + `agg_torrent_ext` mirror) — the `<50 ms` lever (GROUP BY→2.3 ms, +2 GB); **atomic swap** via versioned dirs + a `current` pointer. (ii) **Minute delta job** — carve recent torrents → tiny `delta.parquet` + delta rollups + PG agg upsert → ping `Reload`. (iii) **Compaction job** — periodic full base rebuild + atomic swap.

**3. `bitmagnet-filesearch` (new crate — DuckDB sidecar, L2b).** Embeds DuckDB via the `duckdb` crate **`bundled`** feature (Rust is not CGO/musl-constrained like the Go build — the exact reason a sidecar beats embedded go-duckdb). ONE bounded instance (`memory_limit`/`threads` are global-per-instance), served via a **`spawn_blocking` pool + semaphore + statement timeout**. **base+delta query view** (below); **dual reload** (frequent delta swap / rare base swap) behind an `RwLock`. **Safe SQL:** structured filters → bound params, never interpolated; path substring `ILIKE ?` paginated + timed.

**4. `file_search.proto` (`FileSearchService`).** Separate from the torrent-grained `SearchService`. `SearchFiles` (filters: extensions/file_types/size range/path_query/content_types/published range; keyset pagination; sort; **`collapse_to_torrent` default true** — matches the torrent-centric UI, avoids mega-torrent flooding). Plus `CountFiles`, `Facets`, `Reload`, `HealthCheck`.

### Base+delta freshness algorithm (EXP-B)

`files_data` is upsert-with-`DoUpdates`, so a re-crawl supersedes a torrent's _whole fileset_ → supersession is **TORRENT-granular**, an **anti-join**:

```sql
CREATE VIEW files AS
  SELECT * FROM read_parquet('…/base/current/fact.parquet') b
    WHERE NOT EXISTS (SELECT 1 FROM read_parquet('…/delta/current/fact.parquet') d
                      WHERE d.info_hash = b.info_hash)
  UNION ALL
  SELECT * FROM read_parquet('…/delta/current/fact.parquet');
```

🚨 `row_number() … = 1` is **WRONG** (keeps one _file_ per torrent → drops the rest); window-max is **80× slower** (19 s vs 230 ms). The delta job (~1 min) carves `torrents WHERE updated_at > :watermark` (**needs an index on `torrents.updated_at`**); delta-append is sub-second in prod. Collapse stays gentle (141 → 230 ms at +100k). Compaction daily or at ~1M delta torrents. **Freshness SLA = the delta-flush interval (~1 min).** Agg gets the same supersession via **DELETE-then-INSERT per changed `info_hash`**. _(Real-time Rust processor dual-write is roadmap — sub-minute, not a Go hot-path change.)_

### The prove-then-retire checker (all-Rust, invariant composition)

The DROP gate is **L2a (`agg_torrent_ext` vs `torrent_files` EXISTS)**, proven by composing invariants — **no Go request-path shadow** (strictly better and all-Rust). A `verify` subcommand of `bitmagnet-parquet`:

- **Job A — one-time, direct, pre-flip:** `agg` vs `torrent_files` on the restore — proves the chain + **G1 parity** directly.
- **Job B — continuous, durable:** `agg` vs the **blob** it was built from. No `torrent_files` → survives the DROP; the build code self-verifies (DRY).
- **existing blob ⟺ `torrent_files`** consistency closes the loop ⟹ `agg ⟺ torrent_files` **transitively**.

**Correctness rules:** expected extension is **ALWAYS path-derived** (`file_extension_from_path(BlobFile.path)`, skip empties) — **NEVER** `BlobFile.extension` (empty for crawl-path = G1). Both sides keep **valid exts only**. `u64` vs `i64` widen carefully. **sqlx 0.9 verified against canonical source:** `Vec<Vec<u8>>` binds as `bytea[]` (sqlx sends the OID in the Parse message → the **`::bytea[]` cast is OPTIONAL**); guard empty key slices; `extension` → `String` (`IS NOT NULL`-filtered), `max_size` → exact `i64`; the `macros` feature is inert for the runtime `query()` path.

**Cap-divergence = structurally ZERO** (settled from code): all three write sites build blob and `torrent_files` from the _same_ `files` slice → `agg(blob) ≡ torrent_files` **by construction**; any mismatch is a **bug**, expect ~100% exact. Require a sustained zero-mismatch window before flipping primary.

> **Safety hazard:** the `LiveChecker` self-heals by **NULL-ing `files_data`** on mismatch → agg-parity drift must ride a **separate counter/flag**, never the blob-heal path.

### L2-P0 detail (`agg_torrent_ext` migration + checker)

**Migration `00024_agg_torrent_ext.sql`** (goose v3, auto-applied):

```sql
CREATE TABLE IF NOT EXISTS agg_torrent_ext (
    info_hash  BYTEA  NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
    extension  TEXT   NOT NULL,                 -- valid (non-null) extensions only
    max_size   BIGINT NOT NULL,
    PRIMARY KEY (info_hash, extension)
);
CREATE INDEX IF NOT EXISTS idx_agg_torrent_ext_extension
    ON agg_torrent_ext (extension, info_hash);
```

- **PK `(info_hash, extension)`** serves the correlated EXISTS (the only strictly-required index); the secondary covers a selective IN-list.
- **`max_size` included now** (decision): future-proofs a torrent-grain `ext∧size` collapse served by PG — `EXISTS(… extension='mkv' AND max_size > 1e9)`.
- **No-extension bucket omitted** → exactly matches `ExtractUniqueExtensions` (skips empties), so parity is clean.

**EXISTS swap** (flag-gated): mirror `criteria_torrent_file_extension.go:24-34`, `q.TorrentFile` → `q.AggTorrentExt` (a generated model; the single-file OR-branch untouched). **Seed (Rust, G1):** stream blobs → per-ext `MAX(size)` → `INSERT … ON CONFLICT DO UPDATE`. **`bitmagnet-db` readers:** `batch_torrent_files_ext_agg` + `batch_agg_torrent_ext` beside the streamers; the compare fn is a **pure fn unit-tested with `.blob` fixtures (CI, no DB)**; readers `#[ignore]` integration + `*_sql_shape` guards.

### Deployment (PROPOSED — HEL1)

A **new** `roles/bitmagnet-filesearch` (**keep** the Tantivy main-search role): node **`<hel1-host>`** (FSN1 ~83% mem-committed); RWO `local-path` PVC **~50 Gi**; ClusterIP **:50052**, `Recreate`, tcpSocket probes; **one image, three entrypoints** (server + delta-refresh + compaction) built on `<fsn1-host>`; **two CronJobs** (delta ~1 min + compaction); Go wiring `BITMAGNET_FILESEARCH_ADDR` + CNP, default off until shadow; new GraphQL `fileSearch` query.

### Phasing (L2-P0 … L2-P4)

- **L2-P0** — `agg_torrent_ext` migration + gen/criteria seam + Rust seed + the **Rust `verify` checker** + `bitmagnet-db` readers. _The DROP gate._
- **L2-P1** — `bitmagnet-parquet`: base export + minute delta job + compaction + `torrents.updated_at` index.
- **L2-P2** — `file_search.proto` + `bitmagnet-filesearch` sidecar + Go gRPC client.
- **L2-P3** — homelab role + 2 CronJobs + image (HEL1) + Go shadow wiring. **Keep the Tantivy role.**
- **L2-P4** — sustained zero-mismatch window → **flip** the filter to `agg_torrent_ext`; **GA** `fileSearch`. **DROP still deferred.**

---

## 6. Rationale & Decision Log

| Decision                      | Choice                                                                | Why / evidence                                                                                                                    |
| ----------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Replace `torrent_files` with… | **per-torrent blob** (not a slim per-file PG table)                   | blob ~19 GB vs slim table **+78–113 GB** (RUN-3); 14× compression                                                                 |
| Per-file search engine        | **DuckDB-on-Parquet** (not a 873M-doc Tantivy index)                  | index gives **no latency win** + is scan-bound ~1.3 s p50 @full + costs +14–25 GB (RUN-4); DuckDB 0.015–1.3 s at +3.86 GB (RUN-2) |
| DuckDB speed lever            | **rollup tables + sorted layout** (not ART indexes)                   | ART `CREATE INDEX` doesn't accelerate analytical scans (EXPLAIN seq_scan); rollups → <50 ms at +2 GB (ARCH-C)                     |
| Freshness                     | **base+delta anti-join** (not row_number / window-max / full rebuild) | minute freshness <250 ms; row_number WRONG, window-max 80× slower (EXP-B)                                                         |
| The DROP-gate parity piece    | **PG `agg_torrent_ext`** (stays in PG, not DuckDB)                    | shape (a) is the only live `torrent_files` read; facet is presence-EXISTS; main search is DROP-independent (EXP-A)                |
| `agg` columns                 | **`(info_hash, extension, max_size)`**                                | facet needs presence only; `max_size` future-proofs torrent-grain `ext∧size` at one cheap column                                  |
| Checker                       | **all-Rust invariant composition** (no Go request-path shadow)        | Job A+B+existing-consistency ⟹ transitive; checks invariants directly+continuously; cap-divergence is structurally zero           |
| DuckDB runtime                | **Rust sidecar** (not embedded go-duckdb)                             | Go build is pure-Go/CGO-disabled/musl; Rust embeds DuckDB cleanly (`bundled`)                                                     |
| Language                      | **Rust** for durable L2 (Go only for the flag-gated EXISTS swap)      | aligns with the rust-rewrite; verified down to the sqlx bind layer                                                                |
| CJK free-text index           | **gated, not built**                                                  | ~+90 GB nearly triples the footprint (−93%→−55%); only broad/CJK free-text needs it (EXP-D2)                                      |
| Node                          | **HEL1**                                                              | FSN1 ~83% mem-committed; heavy DuckDB scans off the live crawler/PG node                                                          |
| Cutover                       | **DROP deferred**                                                     | prove-then-retire; `torrent_files` is both the parity ground truth and the fallback                                               |

---

## 7. Status & Roadmap

**DEPLOYED:** Phase 1 Hybrid Blob — dual-write live, backfill complete (16.97M), `verify --full` 0 mismatches. `torrent_files` retained.

**MEASURED:** the full benchmark/experiment suite (§4) on the real 879.5M-row corpus — verdict: ship the cheap composition, reject the file index, gate the CJK index.

**SPECCED (not built):** all of L2 (§5), down to the sqlx-verified Rust readers and the `00024` migration. Detailed specs: `L2-duckdb-parquet-search-rust-spec.md`, `L2-P0-agg-torrent-ext-and-checker-spec.md`.

**PENDING / DEFERRED:**

- A deliberate **no-code hold** — no L2 code is built yet. First brick when lifted: **L2-P0** (`00024` migration → `bitmagnet-db` readers → the Rust `verify` checker).
- **Phase-A code fixes** that precede cutover: **G1** (derive blob `e` from path everywhere + extend the checker), **G2** (re-point the residual `TorrentQuery.files` SELECT at the blob), per-file timestamp/index-sort hydration, the C6 retired-PG-path guard.
- **The `torrent_files` DROP** — gated on every layer proven live + a fresh off-host backup.
- Bench-env teardown on `<hel1-host>` (RUN-6); the throwaway bench creds were already scrubbed before this repo was made public.

**Build order:** L2-P0 (the gate) → L2-P1 (`bitmagnet-parquet`) → L2-P2 (proto + sidecar + Go client) → L2-P3 (homelab deploy + shadow) → L2-P4 (flip; DROP still deferred).

---

## 8. Appendix

### Task index (tracked)

- **Phase-A prerequisites:** G1 (extension-from-path + checker), G2 (re-point browser at blob), per-file hydration, C6 guard.
- **L2-P0:** `00024` DDL · gen model + criteria seam · Rust blob seed · Rust `verify` (Job A/B) · `bitmagnet-db` batch readers · sqlx-verification (done).
- **L2-P1:** `bitmagnet-parquet` (base export + minute delta + compaction).
- **L2-P2:** `file_search.proto` + `bitmagnet-filesearch` sidecar + GraphQL `fileSearch`.
- **L2-P3:** homelab `bitmagnet-filesearch` role + 2 CronJobs + image + monitoring.
- **L2-P4:** shadow window → flip filter to `agg_torrent_ext` → GA `fileSearch` (DROP deferred).
- **Separate:** FIND-1 (importer O(n²) tsv guard), FIND-2 (broad-ranked PG-FTS wall).

### Source documents (this doc synthesizes them)

- **Deployed migration:** `homelab-infra/docs/bitmagnet-fork-deploy-plan.md`, `bitmagnet-backfill-bottlenecks.md`, `bitmagnet-database-analysis.md`; `bitmagnet/docs/live-migration-design.md`.
- **Benchmarks/experiments:** `docs/dev/{file-grained-search-benchmark-results, file-index-bench-RESULTS, arch-c-parity-and-optimization-results, arch-f-future-query-catalog, experiments-write-read-and-freshness-results, exp-a-write-read-path, exp-b-base-delta-freshness, cjk-tokenizer-and-incremental-merge-bench-RESULTS, space-savings-vs-torrent-files}.md`.
- **L2 architecture:** `docs/dev/{L2-duckdb-parquet-search-rust-spec, L2-P0-agg-torrent-ext-and-checker-spec, duckdb-parquet-parity-architecture, duckdb-integration-arch, duckdb-parquet-pipeline-arch-A, duckdb-future-query-catalog-arch-F, file-grained-search-team-review}.md`; `homelab-infra/docs/{bitmagnet-duckdb-parquet-arch, bitmagnet-tantivy-phase3-deploy-plan}.md`.
