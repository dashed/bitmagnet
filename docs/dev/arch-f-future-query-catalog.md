# ARCH-F — Future-Query Catalog + Schema/Layout Implications

**Owner:** `duckdb-bench` (team `bitmagnet-bench`, ARCH-F / task #19)
**Date:** 2026-06-07
**Method:** source-grounded (fork `feat/file-grained-search`, v2 branches) + empirical spot-checks on the live 879.5M-row HEL1 bench Parquet (see [`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md)). sequential-thinking + ultrathink.

## Key framing — the structural DuckDB advantage

A **search index** (Tantivy) bakes its query surface into the schema: each new query *type* (a new facet, a new sortable field, a new filter) needs a **schema change + a full re-index** of 873M docs. **DuckDB-on-Parquet** stores the *data*, not a query plan — so **most future queries are just new SQL on the existing files**, runnable the day they're asked, no migration. Only queries needing **new per-file _data_** (a column the blob doesn't carry) require a Parquet **re-export** — measured at **~83 s** for the whole corpus (RUN-2), i.e. cheap and non-disruptive.

The blob is the floor of what's available: `BlobFile { index:u32, path:String, extension:String, size:u64 }` and nothing else (`bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:31-46`). No per-file hash, no per-file mtime. So the classification axis is:

- **🟢 Just SQL** — answerable on the recommended columns; new query = new SELECT.
- **🟡 Denorm column** — needs a torrent-level attribute copied into the Parquet (cheap, stable, include now).
- **🔵 Live PG join** — needs a *mutable* attribute (seeders); never snapshot, always join live PG.
- **🔴 New per-file DATA** — a column the blob lacks (per-file merkle hash, mtime) → schema evolution + re-export.

---

## Catalog (8 categories)

| # | Future workload | DuckDB SQL approach | Needs | Class | Measured / projected |
| - | --------------- | ------------------- | ----- | ----- | -------------------- |
| 1 | **Multi-file-within-torrent**: season-packs (≥N similar videos), "has video AND subtitle", sample-file detection | `GROUP BY info_hash HAVING count(*) FILTER(...)>=N`; conditional aggregates (`bool_or(extension IN videos)`, `bool_or(extension IN subs)`); sample = `count(*) FILTER(size<300MB AND ext IN videos)>0 AND maxsize>4GB` | core cols | 🟢 | **season-pack ≥8 mkv>300MB = 1.3 s** (→ <35 ms via per-torrent rollup) |
| 2 | **Cross-torrent analytics**: top exts by count/size, size-dist per content_type, ext co-occurrence, **ingestion/file-type TRENDS over time** | `GROUP BY extension`; `GROUP BY content_type,…`; self-join for co-occurrence; trends `GROUP BY date_trunc('month', created_at)` | core + **content_type, created_at/published_at** (denorm) for the by-type & trend variants | 🟢 / 🟡 | **faceting 2.7 s**, GROUP BY ext **1.4 s → 2.3 ms (rollup)**; trends need a time col |
| 3 | **JOINs to torrent/content metadata**: "4K movies", "files in torrents with seeders>X", language/genre filters | `files JOIN pg.torrent_contents/content …`; or pre-denormalized columns for stable attrs | content_type/published_at = 🟡 denorm; **seeders/leechers = 🔵 live PG (mutable)** | 🟡/🔵 | **movie∧mkv>1GB JOIN→PG = 1.5 s** (728,574 torrents) |
| 4 | **Dedup / similarity**: same fileset across info_hashes, find-torrent-by-filename, near-dup | exact-fileset: a derived per-torrent `fileset_hash` → `GROUP BY fileset_hash HAVING count(DISTINCT info_hash)>1`; raw `(path,size)` dup = `GROUP BY path,size`; by-filename = `path ILIKE`/FTS | optional derived `fileset_hash` (cheap at export) turns the batch dup into O(n) | 🟢 (+opt derived) | raw **(path,size) dup = 134 s BATCH**; `fileset_hash` GROUP BY ≈ collapse-tier (~1 s / <35 ms rollup) |
| 5 | **Fuzzy / advanced path**: regex, tokenized FTS, accent-fold | `regexp_matches(path,…)` (full scan); `ILIKE` (CJK-correct, slow); accent = `strip_accents()`+ILIKE; tokenized = FTS/BM25 | none (compute) / FTS index for speed | 🟢 (slow) | regex/ILIKE = **~23 s full scan**; BM25 **150 ms but +35 GB**, no CJK-seg — **the lone Tantivy carve-out** |
| 6 | **BEP-52 v2**: find-torrent-by-exact-file-hash, per-file identity, v1↔v2 hybrid dedup | `WHERE file_merkle_root = :hash` — trivial once the column exists | **🔴 NEW per-file merkle column** — NOT in the blob (`BlobFile` has no hash); v2 file-tree roots exist in the protocol (`internal/dhtcrawler/persist.go:184`, `00023_v2_infohash.sql`, `feat/bittorrent-v2-*`) but aren't persisted per-file yet | 🔴 | needs schema evolution + re-export (~83 s) once v2 per-file hashes land |
| 7 | **Quality heuristics**: mislabeled (movie content_type but no video file), archive-only, risky-exe present | per-torrent conditional aggregates: `GROUP BY info_hash HAVING bool_or(ext IN videos)=false AND content_type='movie'`; `bool_or(ext='exe')` | core + content_type (denorm) | 🟢/🟡 | season-pack-tier (~1.3 s; rollup-able) |
| 8 | **Faceting** (result-set facet counts) | `GROUP BY extension` (+`count(DISTINCT info_hash)`) over the filtered set | core | 🟢 | **2.7 s → ms via rollup** |

**Tally: 6 of 8 categories are pure SQL on the recommended columns; only trends (cat 2) + by-content-type (cat 3) need the denorm columns (include now), and only v2-per-file-hash (cat 6) needs genuinely new per-file data.**

---

## Recommended Parquet schema (include now vs later)

```
-- CORE (from the blob — blob.rs BlobFile)
info_hash    VARCHAR(40 hex)   -- torrent identity / collapse / dedup key
file_index   UINTEGER          -- BlobFile.i
path         VARCHAR           -- BlobFile.p  (full parquet only; ~+7.85 GB)
extension    VARCHAR           -- PATH-DERIVED (G1), NOT blob.e
size         UBIGINT           -- BlobFile.s

-- DENORM from the torrent (stable, cheap, ENABLES cat 2/3/7 without a live join)
content_type VARCHAR           -- "4K movies", by-type analytics, mislabel heuristics
published_at TIMESTAMP/INT     -- time filtering + trends
created_at   TIMESTAMP/INT     -- ingestion trends (torrent_files.created_at exists today;
                               --   blob carries no per-file time → use torrent-level)

-- OPTIONAL derived at export (cheap, unlock specific workloads)
file_category VARCHAR          -- coarse video/audio/subtitle/… bucket (cat 1/7/coarse-partition)
fileset_hash  VARCHAR          -- hash of sorted (path,size) per torrent → O(n) exact-fileset dedup (cat 4)

-- FUTURE (🔴 schema evolution — needs NEW per-file data the blob lacks; re-export ~83 s)
file_merkle   VARCHAR          -- BEP-52 v2 per-file merkle root → find-by-file-hash (cat 6)
mtime         TIMESTAMP        -- per-file mtime (not in blob; only if a real need appears)
```

**Do NOT snapshot into Parquet:** `seeders`/`leechers` and any other mutable/fast-changing field — these go stale; serve via a **live PG join** (cat 3) at query time.

**Cost note:** the denorm columns are torrent-level and low-cardinality → they dictionary/RLE-compress to near-zero in the info_hash-ordered Parquet (and `content_type`/`published_at` are tiny). Adding all three is cheap; they future-proof cats 2/3/7 so those never need a re-export.

---

## Coordination with ARCH-C (empirical — DONE)

ARCH-C already spot-checked the representative future queries on the live Parquet:
- season-pack (cat 1) **1.3 s**, faceting (cat 8) **2.7 s**, cross-torrent dup (cat 4) **134 s batch** — confirming the classification (interactive vs batch) and that all are plain SQL.
- The per-torrent **rollup tables** (ARCH-C lever 2) collapse cat 1/2/7/8 aggregate variants to **<35 ms** at +2 GB.
- Path-FTS (cat 5) is the **sole** index-needing workload (ILIKE 23 s / BM25 150 ms+35 GB) — the genuine Tantivy carve-out.

**Net:** the DuckDB-on-Parquet schema below accommodates **7 of 8** future categories as just-SQL (some with the cheap denorm columns); only **BEP-52 v2 per-file hash** needs new per-file data, and even that is a one-line schema add + an ~83 s re-export — versus a search index, where each of these would be a separate schema migration + 873M-doc re-index.
