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
    info_hash  BYTEA  NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
    extension  TEXT   NOT NULL,                 -- only valid (non-null) extensions; see §4
    max_size   BIGINT NOT NULL,                 -- max size of any file of this ext in this torrent
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
**DECISION (user, 2026-06-08): include `max_size` now.** It's not needed by the EXISTS filter or the facet (presence only), but it future-proofs the **torrent-grain `ext∧size` collapse** at one cheap column: "torrents having an `.mkv` > 1 GB" = `EXISTS(… extension='mkv' AND max_size > 1000000000)` — served by PG/agg without DuckDB. `file_count`/`total_size` remain trivially addable later if a count/sum surface ever needs them.

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
- Reuse the `bitmagnet-db` blob streamer (`stream_torrents_with_files`, keyset on info_hash). Per torrent: decode `files_data` → for each file derive `file_extension_from_path` (G1, skip empties) → **group by extension, taking `MAX(size)`** → `INSERT INTO agg_torrent_ext (info_hash, extension, max_size) … ON CONFLICT (info_hash, extension) DO UPDATE SET max_size = EXCLUDED.max_size`.
- Multi-file torrents only (single-file ext stays on `torrents.extension`).
- Idempotent; resumable by info_hash cursor. (Becomes a mode of the L2-P1 `bitmagnet-parquet` job.)

---

## 7. The checker — ALL RUST (DECISION, user 2026-06-08): a `verify` subcommand of `bitmagnet-parquet`

Two verifications by data-lifetime, **both Rust**, living as a `verify` subcommand of the planned `bitmagnet-parquet` crate (DRY — it streams blobs + builds agg). Workspace conventions (grounded): compare/diff = a **pure fn in the crate `lib.rs`** (unit-tested with the committed `.blob` fixtures → runs in CI, no DB); the **DB readers in `bitmagnet-db`** beside the existing streamers; the bin follows the `bitmagnet-search/src/bin/backfill.rs` `main()`(parse+`init_tracing`)→`run()` split with clap-derive, `anyhow .context()`, per-row warn-and-skip. CLI: `bitmagnet-parquet verify --source <agg-blob|agg-torrent-files> --mode <full|sample> --batch-size 1000 --postgres-dsn ""` (a `#[derive(ValueEnum)]` for `--source`, mirroring the bench crate's `Source` enum).

### Job A — DROP-gate parity: `agg` vs `torrent_files` (one-time, direct, pre-flip)
Proves the swap changes no results + establishes **G1 parity** directly. Bound to `torrent_files` (retiring) so it's throwaway in *lifetime* — but Rust per the calibration, and it reuses everything (`bitmagnet-db` readers + the pure compare fn). New **`bitmagnet-db` batch readers** (task #48; sqlx 0.9 binds `Vec<Vec<u8>>` to `bytea[]` — confirmed, no workaround; use an explicit `::bytea[]` cast):
```rust
// expected (source of truth)
const BATCH_TORRENT_FILES_AGG_SQL: &str =
  "SELECT info_hash, extension, max(size) AS max_size FROM torrent_files \
   WHERE info_hash = ANY($1::bytea[]) AND extension IS NOT NULL GROUP BY info_hash, extension";
pub async fn batch_torrent_files_ext_agg(pool, &[InfoHash]) -> Result<Vec<FileExtAgg>>;
// actual (rollup under test)
const BATCH_AGG_TORRENT_EXT_SQL: &str =
  "SELECT info_hash, extension, max_size FROM agg_torrent_ext WHERE info_hash = ANY($1::bytea[])";
pub async fn batch_agg_torrent_ext(pool, &[InfoHash]) -> Result<Vec<AggTorrentExtRow>>;
```
Keyset-page torrents (reuse the bench crate's raw-sqlx `torrent_files (info_hash,"index")` keyset), batch the hashes, compare per-torrent ext-**set** + `max_size`. Run on the **HEL1 restore** (full, uncapped `torrent_files`) / a prod snapshot. Non-zero exit only on a genuine mismatch.

### Job B — durable invariant: `agg` vs the **blob** (continuous)
"Does agg correctly summarize the blob it was built from?" Needs **no `torrent_files`** → survives the DROP, runs forever. Recompute the expected agg from the decoded blob and compare to the stored `agg_torrent_ext`. The code that BUILDS agg self-verifies its output — DRY in `bitmagnet-parquet`.

### The gate = invariant composition (NO Go request-path shadow)
The earlier plan used a Go live shadow-compare in the request path. Going Rust enables a **strictly better** gate via composition:
- **Job A** (one-time, direct, full) — `agg ⟺ torrent_files` proven directly pre-flip (incl. G1).
- **Job B** (continuous, durable) — `agg ⟺ blob` maintained forever.
- **existing blob ⟺ `torrent_files` consistency** (already running) — closes the loop ⟹ `agg ⟺ torrent_files` in prod, transitively.

No double-running content searches in the hot path; invariants checked directly + continuously; all-Rust.

### Correctness rules (both jobs — load-bearing)
- 🚨 **Expected extension ALWAYS path-derived** via `file_extension_from_path(BlobFile.path)`, skipping empties — **NEVER** `BlobFile.extension` (the stored `e`, which is **empty for crawl-path torrents** = the G1 bug). This matches `torrent_files.extension` (the path-derived generated column) and the agg table (valid exts only, §4).
- **Null/empty-ext symmetry**: the `torrent_files` reader filters `extension IS NOT NULL`; the blob side skips empty path-derived exts → **both sides = valid exts only**, or the checker false-positives on every no-ext torrent.
- **Size types**: `BlobFile.size` is `u64`, `torrent_files.max(size)` and `agg.max_size` are `i64` — widen/compare carefully.
- **Cap-subset is structurally zero (§8)** → any difference is a **bug** (G1/decode/build), not an accepted loss. Expect ~100% exact.

| class | meaning | gate |
|---|---|---|
| **exact** | ext-set + max_size match | pass — expected ~100% |
| **any mismatch** | any difference | **a BUG to fix** — must be 0 |

### Tests (workspace convention)
- **Pure compare fn** → unit tests over in-memory `Vec<BlobFile>` / agg-row fixtures (+ the committed `.blob` fixtures for Go-wire parity) → runs in CI (no DB).
- **DB readers** → `#[tokio::test] #[ignore = "requires a live PostgreSQL"]` integration (run on the HEL1 restore) + a cheap `*_sql_shape` `.contains()` guard, exactly like `bitmagnet-db`'s existing reader tests.

---

## 8. Cap-induced divergence — SETTLED FROM CODE: structurally zero

The earlier worry was that the blob (hence agg) is capped at `save_files_threshold` while the EXISTS is uncapped `torrent_files`, losing rare-ext matches in big torrents. **Reading the three write sites settles it — the blob always mirrors `torrent_files`, so there is no divergence:**
- **Crawler** (`internal/dhtcrawler/persist.go:203-251`): the blob `FilesData` (`SerializeFiles(files)`, :226) and the `torrent_files` rows (`Files: files`, :250) are built from the **same `files` slice**, capped together in the same loop (`if i >= saveFilesThreshold { break }`, :206). Both capped identically (or both full for legacy) → **identical sets**.
- **Backfill** (`internal/blobmigration/queue/handler.go:150-167`): reads **all** `torrent_files` rows for the torrent (`Find()`, no limit) and serializes them **verbatim** → blob == `torrent_files` exactly (even for legacy 88,561-file torrents — the blob just gets big).
- **Importer** (`internal/importer/importer.go:257-263`): writes **no files** (`FilesStatusNoInfo`); file rows + blob arrive later via the crawler. (The "importer bypasses the cap" note refers to the processor's `torrent_contents` tsvector — FIND-1 — not `torrent_files`/blob.)

⟹ **`agg(blob)` and `torrent_files` are the same file set by construction.** The cap-subset class is structurally empty; the checker (Job A) now serves as **build-correctness confirmation** (expect ~100% exact; any mismatch = a bug in the agg-build/G1/decode, not an accepted product loss). The DROP gate is materially de-risked.

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

**Decisions (all settled 2026-06-08):**
1. **Schema** — ✅ **include `max_size`** now (§3): `(info_hash, extension, max_size)`, PK `(info_hash, extension)`. Future-proofs torrent-grain `ext∧size` collapse at one cheap column.
2. **Cap question** — ✅ **settled from code (§8): structurally zero divergence** — blob mirrors `torrent_files` at all three write sites. The checker is build-correctness confirmation, not a cap-risk quantifier.
3. **Checker language** — ✅ **all Rust** (user override, 2026-06-08; §7). Both Job A (agg-vs-`torrent_files`, one-time direct) and Job B (agg-vs-blob, continuous) are a `verify` subcommand of `bitmagnet-parquet`; readers in `bitmagnet-db`; compare fn pure + unit-tested. **No Go request-path shadow** — the gate is invariant composition (Job A direct + Job B continuous + existing blob⟺torrent_files), which is strictly better and all-Rust.
