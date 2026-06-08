# ARCH-F — Future-Query Catalog + Forward-Compatible Parquet Schema

**Owner:** `pg-data-bench` (team bitmagnet-bench) · **Task #19** · Design/docs only.
**Question (user):** *"consider potential queries we may want to add in the FUTURE."*
**Thesis:** **DuckDB-on-Parquet makes the per-file analytics surface OPEN-ENDED.** Almost every plausible future query is **just a new SQL string** over the Parquet that ARCH-A already exports — **zero re-index, zero schema migration, instant**. Only a query needing a brand-new *per-file column that isn't in the blob* (BEP-52 merkle, mtime) requires a Parquet **re-export** (~18 s slim / ~83 s full — RUN-2), and even that is a one-shot, not per-query. Contrast the rejected 873 M-doc Tantivy index: **every new filterable/sortable dimension = a new schema field + a full ~32 min re-backfill (RUN-4) + a sidecar redeploy.** This asymmetry is a first-order argument for the DuckDB choice.
**Grounded in:** `bitmagnet-model/src/blob.rs` (`BlobFile{index,path,extension,size}`), `bitmagnet-db/src/stream.rs` (`TorrentForIndex` = the torrent-level columns available for joins), the `feat/bittorrent-v2-*` branches (v2 identity, *not* per-file merkle), ARCH-A (pipeline) + ARCH-B (SQL-gen/dim-join) + ARCH-C (measured latencies).

---

## 0. The two axes every future query falls on

1. **"New SQL" vs "New DATA":**
   - **New SQL only** — answerable from columns already exported (`files`: info_hash, file_index, path, extension, size; `torrents` dim: content_type, published_at, seeders, video_*, …). **Ship it the day it's asked.** ~90% of the catalog.
   - **New per-file DATA** — needs a column not derivable from the blob today (per-file merkle root, per-file mtime, piece layout). Requires: capture it (crawler/blob change) → re-export the Parquet. Rare.
2. **Latency tier (measured, RUN-2/ARCH-C):**
   - **Interactive early-out** (paginated find, point hydrate): 17–142 ms.
   - **Full-corpus scan/aggregate** (GROUP BY, COUNT DISTINCT, histogram, HAVING): ~1.2–1.4 s warm (cold≈warm, RAM-resident).
   - **Unprunable substring/regex on `path`**: best-effort — ~0.1 s common+`LIMIT`, ~23 s rare/exhaustive (leading-wildcard, no pruning — ARCH-C).

---

## 1. Forward-compatible schema to export NOW

Two Parquets, both produced in one refresh-Job pass (ARCH-A §5 seam). Exporting the **torrent dim** now is the single highest-leverage forward-compat move: it unlocks every join/time-trend/quality/content future query with **no re-export**.

### `files` (per-file fact — keep slim; +3.86 GB)
| col | from | already exported? |
|---|---|---|
| `info_hash` | blob torrent | ✅ |
| `file_index` | blob `i` | ✅ |
| `path` | blob `p` | ✅ (full Parquet, +7.85 GB; needed for filename/path/regex/season queries) |
| `extension` | **G1 path-derived** | ✅ |
| `size` | blob `s` | ✅ |
| *`merkle_root`* | **BEP-52 v2 file tree — NOT captured today** | ❌ needs new data (see §2.6) |
| *`mtime`* | not crawled | ❌ needs new data |

### `torrents` (torrent-level dim — **add now**; ~1–2 GB, ~48 M rows or ~17 M with-files)
`info_hash, info_hash_v2, meta_version, name, files_status, files_count, total_size, created_at, published_at, content_type, content_source, content_id, content_title, release_year, video_resolution, video_source, video_codec, video_3d, video_modifier, release_group, languages[], genres[]`
— every column already available via `stream.rs:STREAM_FOR_INDEX_SQL` (`TorrentForIndex`). `created_at`/`published_at` are the **time axis** for all trend queries; the `content_*`/`video_*` columns power content joins + quality heuristics. **ARCH-B owns the dim-join SQL-gen; ARCH-A emits the dim Parquet.**

> 🚨 **Mutable vs stable — do NOT freeze swarm stats in the Parquet dim.** `seeders`/`leechers` change continuously and would be **stale up to the refresh cadence** if baked into a periodic export. Queries filtering/ranking on *current* seeders (2.3, 2.7) should **join live PG** (`torrents_torrent_sources`) at query time — cheap, since the per-file filter already narrows to a small `info_hash` set that the live join hydrates. The Parquet dim carries only **stable** attrs (content/video/time/identity). This mirrors the freshness split: immutable facts → Parquet (lag OK); mutable swarm → live PG. (If approximate/“was-popular” ranking is acceptable, a stale seeders snapshot in the dim is fine — but make it an explicit choice, not a default.)

> **Recommendation:** export `files` (slim) + `torrents` dim from day one; add the `path` column (full Parquet) if filename/path/season queries are in scope. With those, the entire catalog below — except §2.6 (merkle) — is **new SQL only**.

---

## 2. The catalog

### 2.1 Multi-file predicates / season-packs — **NEW SQL**
*"torrents with a complete S01 (E01–E10)", "≥8 `.mkv` files each >300 MB", "has both a video and a matching `.srt`".*
```sql
-- ≥8 mkv files >300MB in one torrent
SELECT info_hash FROM files
WHERE extension='mkv' AND size > 3e8
GROUP BY info_hash HAVING count(*) >= 8;
-- complete S01E01..E10 (path regex per episode, then require 10 distinct episodes)
SELECT info_hash FROM files
WHERE regexp_matches(lower(path), 's0?1e(0[1-9]|10)')
GROUP BY info_hash HAVING count(DISTINCT regexp_extract(lower(path),'s0?1e([0-9]{2})',1)) >= 10;
```
Needs: `info_hash, extension, size` (+`path` for episode parsing). Tier: **measured 1,317 ms** (≥8 mkv>300MB, scan-bound ≈Q4) → **<35 ms with a pre-agg variant** (the `per_torrent_ext` rollup already carries per-(torrent,ext) count; `WHERE ext='mkv' AND count>=8` is a rollup lookup). The regex episode variant adds an unprunable `path` scan (best-effort). **Tantivy cannot express the per-file conjunction at all (no nested docs — the spec's whole reason for the file index); DuckDB does it in plain SQL.**

### 2.2 Cross-torrent analytics + time-trends — **NEW SQL (needs the `torrents` dim time column)**
*"avg/median file size by extension", ".mkv vs .avi share by year", "codec adoption over time".*
```sql
SELECT t.published_at::date AS day, f.extension,
       count(*) n, approx_quantile(f.size,0.5) p50
FROM files f JOIN torrents t USING (info_hash)
WHERE f.extension IN ('mkv','avi','mp4')
GROUP BY 1,2 ORDER BY 1;
```
Needs: `extension, size` + **`published_at`/`created_at` from the dim** (the reason to export it now). Tier: full GROUP BY ~1.3 s. **Without the dim this would need a re-export to add a time column — exporting the dim now avoids that.**

### 2.3 JOINs to content / seeders / video attrs — **NEW SQL (needs dim)**
*"most-seeded torrents containing a `.flac`", "files inside 1080p x265 movies", "anime (genre) with `.ass` subs".*
```sql
SELECT t.info_hash, t.content_title, t.seeders
FROM files f JOIN torrents t USING (info_hash)
WHERE f.extension='flac' AND t.content_type='movie'
ORDER BY t.seeders DESC LIMIT 50;
```
Needs: `extension` + dim `content_type, video_resolution, video_codec, genres`; **`seeders` is mutable → join live PG** (see §1 note) rather than the frozen dim. Tier: filtered join + order, paginated → sub-second to ~1.3 s (DuckDB can scan the per-file filter, then a live PG lookup ranks the narrowed `info_hash` set by current seeders). **All stable dim columns already exist in `TorrentForIndex` — zero new data.**

### 2.4 Dedup / find-by-filename — **NEW SQL**
*"every torrent containing this exact file", "files appearing in >1 torrent (cross-seeded/dup)".*
```sql
SELECT path, size, count(DISTINCT info_hash) AS torrents
FROM files GROUP BY path, size HAVING torrents > 1 ORDER BY torrents DESC LIMIT 50;
```
Needs: `path, size, info_hash`. Tier: ⚠️ **measured 134 s (ARCH-C)** — a GROUP BY over ~800 M near-distinct `(path,size)` keys is a pathological all-pairs aggregate; it **completes (disk-spills, won't OOM)** but is a **BATCH job, not interactive**. This is **the one future query that is NOT cheap** — run it offline/scheduled and cache the result (e.g. a "popular files" materialized table), or scope it (only `size > 1 GB`, or within a content_type). Still possible in plain SQL — just not a per-request path. **Everything else stays ≤2.7 s or <35 ms via rollups.**

### 2.5 Fuzzy / regex path — **NEW SQL (best-effort latency)**
*`regexp_matches(path, …)`, `ILIKE '%pattern%'`, Levenshtein-ish via `jaccard`/`damerau_levenshtein` (DuckDB built-ins).*
Needs: `path`. Tier: **unprunable full scan** (~0.08 s common+`LIMIT` / ~22–23 s rare/exhaustive — leading-wildcard, ARCH-C). Works (incl. CJK substrings — byte-level), but **not per-keystroke**.
**The index option, now measured (ARCH-C):** a DuckDB **FTS/BM25** index on `path` → extrapolated **~27 min build, ~34.9 GB**, query **147–186 ms** (`1080p`/`bluray`/CJK `电影`). So an index *does* take path search 23 s → ~150 ms — but **+34.9 GB** (≈ the rejected Tantivy file index's whole cost) and 🚨 **NOT CJK-robust**: DuckDB FTS does **no CJK segmentation**, so it matched the exact `电影` token but a sub-token CJK query misses. ⟹ **`ILIKE` is the only CJK-*correct* substring option (but ~23 s); BM25 is fast but ASCII-token-only.** Interactive **AND** CJK-correct path search needs a **CJK-aware tokenizer** — the genuine, narrow Tantivy(path-only) carve-out (ARCH-A §7), gated on an explicit product requirement.

### 2.6 BEP-52 per-file merkle / content-identity dedup — **🚨 NEEDS NEW DATA**
*"find the same file across torrents by cryptographic identity (not path/size collision)", "verify a file's piece layer".*
- **Not available today.** The `feat/bittorrent-v2-*` branches add **torrent-level** v2 identity only (`infoHashV2`, `metaVersion`, btmh magnets, hybrid v1/v2 dedup — commits `8601766`/`c1a822c`/`6a2f77b`/`2f4e273`); they do **not** extract the **per-file merkle root** from the v2 file tree. The blob (`BlobFile`) carries only `{index,path,extension,size}`.
- **To enable:** (1) crawler parses the BEP-52 file tree's per-file `pieces root`; (2) add `merkle_root` to the blob format (a v2 blob-format bump — note the spec's C3 "BEP-52 v2 blob-format bump for per-file merkle" already anticipates this) + persist; (3) backfill; (4) the ARCH-A Job re-exports with the new column (~83 s). Then dedup-by-identity is plain SQL: `GROUP BY merkle_root HAVING count(DISTINCT info_hash)>1`.
- **Cost framing:** a one-time blob/crawler change + one re-export — *still* dramatically cheaper than the Tantivy path (a new FAST field + 32 min re-backfill **per** such feature). And `info_hash_v2` (torrent-level, already on the v2 branches) can ship in the dim **now** for torrent-identity dedup without any per-file change.

### 2.7 Quality heuristics — **NEW SQL (+ dim)**
*"likely-fake" (a `.exe`/`.lnk` in a 'movie'; total size ≪ runtime expectation; sample-file ratio), "best release of X" (resolution+codec+seeders), "padding-file bloat".*
```sql
-- movies carrying executables (fake-ish)
SELECT t.info_hash, t.content_title FROM torrents t
WHERE t.content_type='movie' AND EXISTS (
  SELECT 1 FROM files f WHERE f.info_hash=t.info_hash AND f.extension IN ('exe','lnk','scr'));
```
Needs: `extension, size` + dim `content_type, video_*, seeders`. Tier: semi-join ~sub-second to ~1.3 s. **New SQL; heuristics iterate freely without touching the data.**

### 2.8 Faceting — **NEW SQL (aggregate-accelerated)**
*facet counts by extension / size-bucket / file_category / resolution for a result set.*
```sql
SELECT extension, count(*) files, count(DISTINCT info_hash) torrents
FROM files GROUP BY extension ORDER BY files DESC LIMIT 30;
```
Needs: `extension, size, info_hash`. Tier: **measured 2,734 ms** (the `count(DISTINCT info_hash)` per extension is heavier than a plain count) → **<35 ms via the `per_ext` rollup** (which pre-computes per-extension file + distinct-torrent counts; ARCH-C `per_ext` = 47,628 rows / ~1 MB). **Facets are the rollup's home turf** — ship `per_ext`/`per_torrent_ext` and faceting is instant.

---

## 3. Summary table

| Future query class | New SQL? | New per-file data? | Schema cols needed | Latency tier (measured where shown) |
|---|---|---|---|---|
| Multi-file / season-packs (2.1) | ✅ | — | files(ih,ext,size,path) | **1.32 s** → **<35 ms** via `per_torrent_ext` rollup |
| Time-trends / analytics (2.2) | ✅ | — | files + **dim.published_at** | ~1.3 s |
| Content/video JOINs (2.3) | ✅ | — | files + **dim.content_*/video_***; **seeders → live PG** | ≤1.3 s |
| Dedup / find-by-filename (2.4) | ✅ | — | files(path,size,ih) | ⚠️ **134 s — BATCH, not interactive** |
| Fuzzy / regex path (2.5) | ✅ | — | files.path | best-effort (~0.08 s…23 s); BM25 index → 150 ms @ **+34.9 GB**, ASCII-only |
| **BEP-52 per-file merkle (2.6)** | ✅ *(after)* | **🚨 yes — blob bump + re-export** | files.**merkle_root** | ~1.3 s once present |
| Quality heuristics (2.7) | ✅ | — | files + dim | ≤1.3 s |
| Faceting (2.8) | ✅ | — | files + `per_ext` rollup | **2.73 s** → **<35 ms** via `per_ext` rollup |

**All 8 classes = new SQL on what ARCH-A exports today** (given the `torrents` dim + `path` column); only per-file merkle/mtime needs new data (one-shot capture + re-export, not a per-query cost). Two latency caveats from ARCH-C's measurements: **(a)** the `per_ext` + `per_torrent_ext` rollups (+~1.4 GB, emitted by the refresh Job) take season-packs/faceting/collapse/counts/histograms to **<35 ms**; **(b)** cross-torrent dup-by-(path,size) is a **134 s batch** job (cache it, don't serve it live) and path-FTS is best-effort/CJK-limited — these two are the only non-interactive future workloads, and only path-FTS could ever justify a (CJK-aware) index.

---

## 4. Why this is the strategic case for DuckDB-on-Parquet

- **Open-ended surface.** The Parquet is queried by *arbitrary SQL*. New product questions become one-line queries — no migration, no backfill, no redeploy, same day. The corpus's whole value (a 17 M-torrent / 879 M-file DHT archive) is *analytical exploration*, which an index's closed, pre-declared schema actively fights.
- **The index's cost is per-feature, recurring.** Each new Tantivy filterable/sortable dimension = a new field (more bytes/doc — RUN-4 showed every field adds GB), a full ~32 min re-backfill, and a sidecar roll. DuckDB pays that (a ~18–83 s re-export) **only** for the rare new-per-file-column case, and **once**.
- **Forward-compat is cheap to buy now.** Exporting the `torrents` dim (+1–2 GB) and the `path` column (+7.85 GB) up front converts ~all of §2 into pure SQL. The only thing we *can't* pre-provision is data that isn't crawled yet (merkle/mtime) — and the spec's C3 already earmarks the blob bump for it.
- **Composes with the live tiers.** Hot facets/collapse → the PG per-(torrent,ext) aggregate (<50 ms, live); per-torrent browse → the blob via G2 (live); everything else → DuckDB (≤1.3 s, ≤6 h-stale). Future queries slot into whichever tier fits with no new infrastructure.

**Bottom line:** the future-query surface is **"write a SELECT,"** not "design a field and re-backfill." That is precisely the flexibility the rejected index could never offer, and it should be stated explicitly in the parity architecture (ARCH-E).
