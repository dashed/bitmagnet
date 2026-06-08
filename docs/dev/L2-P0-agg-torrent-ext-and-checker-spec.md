# L2-P0 — `agg_torrent_ext` migration + parity checker: detailed spec

**Date:** 2026-06-08
**Status:** SPEC (design only — no code; "do not drop `torrent_files` until proven" in force).
**Parent:** [`L2-duckdb-parquet-search-rust-spec.md`](./L2-duckdb-parquet-search-rust-spec.md) (this refines its §2a). L2-P0 is the smallest, foundational brick and **the DROP gate**.
**Grounded by** the `bitmagnet-bench` opus team (migration-mapper, checker-mapper, schema-grounder) — all anchors below are verbatim from the fork @ `feat/file-grained-search`.

---

## 1. Purpose

The only live `torrent_files` read in the search path is the **file-extension / file-type filter + facet** (`criteria_torrent_file_extension.go:24-34`). L2-P0 replaces that one read with a small PG rollup, `agg_torrent_ext`, **derived from the `files_data` blob** (the post-DROP source), and adds a **parity checker** that proves the replacement before the EXISTS is flipped. Nothing is dropped here.

---

## 2. Key grounding findings (they shape the design)

1. **Direct precedent exists** — `torrent_file_summary` (`migrations/00021_blob_storage.sql`) is a per-torrent rollup with exactly the pattern we need: `info_hash BYTEA PRIMARY KEY REFERENCES torrents(info_hash) ON DELETE CASCADE, file_count INT, total_size BIGINT, …`. `agg_torrent_ext` is its **per-(torrent,extension) sibling**.
2. **The facet does NOT aggregate.** `facets.go:241-336` runs **one `BudgetedCount(*)` per file-type value** over the base query filtered by the same EXISTS predicate (`criteria_torrent_file_type.go` flattens FileType→extensions and delegates to the extension criteria). It is **distinct-torrent *presence*** semantics, not a `GROUP BY extension` / `COUNT(DISTINCT)`. ⟹ **`agg_torrent_ext` needs only `(info_hash, extension)` presence** to serve both the filter and the facet. `file_count/total_size/max_size` are **not required by the gate** (deferred — §4).
3. **The G1 generated-extension expression** (replicate exactly) — `torrent_files.extension` is `generated always as (substring(lower(path) from '[^/.]\.([a-z0-9]+)$')) stored` (`00001_init.sql:68`). NULL when no extension. The Go `model.FileExtensionFromPath` (`torrent_files.go:33`) is the byte-identical port; `blobmigration.ExtractUniqueExtensions` (`serializer.go:74`) already uses it and **skips empties**.
4. **`torrents.info_hash` = `bytea NOT NULL PRIMARY KEY`** (`00001_init.sql:20`) → the FK + cascade is feasible (identical to `torrent_files:67`).
5. **Checker safety hazard:** the background `LiveChecker` *self-heals* on any `Summary.Mismatches` by **NULL-ing `files_data`** (`live_checker.go` `healTorrent`). Agg-parity drift must therefore ride a **separate counter/flag**, never the blob-heal path.

---

## 3. The migration — `migrations/00024_agg_torrent_ext.sql`

goose v3, single file, StatementBegin/End blocks, auto-applies on pod start (fx decorator `appfx/module.go:80`). Recommended **minimal** DDL (presence-only, per finding #2):

```sql
-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS agg_torrent_ext (
    info_hash  BYTEA NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
    extension  TEXT  NOT NULL,                 -- only valid (non-null) extensions; see §4
    PRIMARY KEY (info_hash, extension)
);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_agg_torrent_ext_extension
    ON agg_torrent_ext (extension, info_hash); -- reverse semi-join when the IN-list is selective
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS agg_torrent_ext;
-- +goose StatementEnd
```

- **PK `(info_hash, extension)`** directly serves the correlated EXISTS (`info_hash = torrents.info_hash AND extension IN (…)`) — the only strictly-required index.
- **Secondary `(extension, info_hash)`** lets the planner drive the semi-join from a selective extension IN-list (mirrors `torrent_files`' standalone `(extension)` index, but covering). Optional; add if the planner prefers it.
- **FK ON DELETE CASCADE** keeps agg consistent when a torrent is purged (matches `torrent_files`).

---

## 4. Schema rationale + the no-extension bucket

- **Store only valid extensions.** The EXISTS predicate always filters `extension IN (<real exts>)` — you never search for "no extension". So no-ext files are **not searchable** and `agg_torrent_ext` simply **omits them** (no `''` row, no NULL — PK forbids NULL anyway). This **exactly matches `ExtractUniqueExtensions`** (which skips empties) → the parity comparison is clean with **no empty-bucket reconciliation** (resolves the risk checker-mapper flagged).
- **One row per distinct valid extension** present in the torrent's **multi-file** set. Single-file torrents keep their extension on `torrents.extension` (the separate OR-branch of the criteria, unchanged) → agg need not cover single-file torrents.
- **`file_count/total_size/max_size` deferred.** Not needed for the filter or the facet (finding #2). Add them later (cheap migration) only if a **torrent-grain `ext∧size` collapse** ("torrents having an `.mkv` > 1 GB") should be served by PG without DuckDB — that's an L2b/collapse concern, out of the P0 gate. **Decision for the user (§9).**

---

## 5. The EXISTS swap (flag-gated, Go)

Mirror `criteria_torrent_file_extension.go:24-34`, swapping the multi-file branch's `q.TorrentFile` → `q.AggTorrentExt`:
```go
gen.Exists(q.AggTorrentExt.Where(
    q.AggTorrentExt.InfoHash.EqCol(q.Torrent.InfoHash),
    q.AggTorrentExt.Extension.In(extensions...)))
```
- **Needs a generated `q.AggTorrentExt`** (the criteria uses gorm-gen objects, not raw SQL). Add `g.GenerateModel("agg_torrent_ext", infoHashType, infoHashReadOnly, …)` near `gen.go:397` + an `ApplyBasic` entry (`gen.go:444`), apply the migration to a dev DB, run `bitmagnet gorm gen`, commit `internal/model/agg_torrent_ext.gen.go` + `internal/database/dao/agg_torrent_ext.gen.go` + the `dao/gen.go` registry edit. **`extension` here is a plain written column** (no `<-:false` generated tag — agg is app-populated, not PG-generated).
- *Fallback:* a raw `EXISTS` via `query.RawCriteria` avoids the gen round-trip but is less idiomatic; prefer gen (the dual-write DAO wants `q.AggTorrentExt` too).
- **Behind a shadow flag**, default off — the single-file OR-branch (`torrents.extension`) is untouched.

---

## 6. Initial backfill / seed (Rust, from the blob, G1)

A one-time seed so the checker has data to validate (distinct from the minute delta-upsert of L2-P1):
- Reuse the `bitmagnet-db` blob streamer (`stream_torrents_with_files`, keyset on info_hash). Per torrent: decode `files_data` → for each file derive `file_extension_from_path` (G1, skip empties) → `DISTINCT` set → `INSERT INTO agg_torrent_ext (info_hash, extension) … ON CONFLICT DO NOTHING`.
- Multi-file torrents only (single-file ext stays on `torrents.extension`).
- Idempotent; resumable by info_hash cursor. (Becomes a mode of the L2-P1 `bitmagnet-parquet` job.)

---

## 7. The parity checker (the gate)

Extend `internal/blobmigration/consistency/` — a **new** `CheckAggBatch` / `compareAgg` beside `CheckBatch`/`CompareFiles` (the existing functions compare only index/path/size; extension parity is greenfield). **Batch** form (right for `--full` ≈ 17M):
- Fetch agg: `SELECT info_hash, extension FROM agg_torrent_ext WHERE info_hash = ANY($1)`.
- **Expected (uncapped) from `torrent_files`** (G1-correct via its generated column): `SELECT info_hash, extension FROM torrent_files WHERE info_hash = ANY($1) AND extension IS NOT NULL GROUP BY 1,2` — or in Go from `rowFiles` via `ExtractUniqueExtensions`.
- Compare the per-torrent extension **sets** and classify:

| class | meaning | gate |
|---|---|---|
| **exact** | `agg == torrent_files` ext-set | pass |
| **cap-subset** | `agg ⊂ torrent_files` (blob capped at `save_files_threshold`, over-threshold torrents only) | **quantify** — count torrents + the missing extensions' rarity |
| **genuine mismatch** | any other difference (incl. `agg ⊄ torrent_files`) | **must be 0** — a bug |

- **Separate mismatch channel** (new counter/result type), **NOT** `Summary.Mismatches` → `LiveChecker` must never NULL a blob over agg drift.
- Wire into the `verify` subcommand behind a new **`--agg-parity`** flag (reuse `--full`/`--sample-rate`); print the 3-class table + non-zero exit only on class-3.
- Run on the **HEL1 restore** (has full, uncapped `torrent_files`) = the offline half of the gate; the live Go shadow-compare (L2-P0b/§4 of the parent) is the online half.

---

## 8. The real content of the gate: cap-induced divergence

`agg_torrent_ext` is **blob-derived**; the blob is capped at `save_files_threshold` (~100 files). The current EXISTS is **uncapped** (`torrent_files`). So for **over-threshold torrents (~6% of the corpus** — p90=54 files, p99=743, max 88,561) the agg ext-set may be a **subset** → flipping the filter could lose a rare-extension match in a large torrent.

- **Open empirical question the checker answers:** is the divergence ≈0 (blobs backfilled from full `torrent_files` → uncapped) or material (forward crawls cap the blob below `torrent_files`)? Depends on the crawler/importer blob-capping behavior (confirm: does `save_files_threshold` cap the *blob* or only `torrent_files`?). The checker's **class-2 count on the real corpus is the answer.**
- **If material → a product decision:** accept losing rare-ext matches in huge torrents, raise `save_files_threshold`, or keep `torrent_files` for over-threshold torrents. This *is* "prove before drop."

### Other risks
| risk | mitigation |
|---|---|
| LiveChecker nukes blobs over agg drift | separate counter/flag (§2 #5, §7) |
| G1 mismatch | replicate the generated-col regex exactly; checker proves it |
| empty-extension false mismatch | store only valid exts; matches `ExtractUniqueExtensions` (§4) |
| FK cascade cost on bulk torrent deletes | mirrors `torrent_files`' existing FK — no new behavior |
| gorm-gen round-trip (needs dev DB) | one-time; or raw-SQL fallback |

---

## 9. Tasks + open decisions

**Tasks (created):** #43 (this spec) · #44 (00024 DDL) · #45 (gen model + criteria seam) · #46 (Rust seed) · #47 (checker `--agg-parity`). Parent: #23 (agg/L2a), #41 (parity harness), #42 (flip).

**Open decisions for the user:**
1. **Schema:** minimal presence `(info_hash, extension)` for P0 *(recommended)*, or include `max_size` now to future-proof torrent-grain `ext∧size` collapse (one extra column, cheap)?
2. **Confirm the cap question** — should we read the crawler to settle whether the blob is capped below `torrent_files` (forward divergence), or let the checker's class-2 measurement settle it empirically?
3. **Checker language** — extend the **Go** checker (recommended: it's the existing `verify` tooling, runs in the migration cmd, and `torrent_files` is being retired so a Rust checker for it is throwaway), with the durable L2 pieces (agg maintenance, sidecar) in Rust as planned?
