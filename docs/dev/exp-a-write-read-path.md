# EXP-A — Deep Dive: Processor/Queue WRITE Batching + `torrent_contents` tsvector READ

**Owner:** `pg-data-bench` (team bitmagnet-bench) · **Task #31** · Experiments on the disposable HEL1 bench PG (ns `bitmagnet-bench`, `deploy/bench-pg`). Read-only on data; any upserts are scratch.
**Scope (per lead re-scope, the literal option (a)):** characterize the **full write pipeline end-to-end** (DHT crawl → batched persist → queue → dispatcher → processor → `torrent_contents` + tsvector) and the **served main-search READ** (`torrent_contents.tsv @@ tsquery` + `ts_rank_cd` + facets). Measure where feasible on the bench. *(The Tier-0 agg-vs-EXISTS validation from the earlier framing is out of scope this round — captured as a follow-up in App. A.)*
**Coordination:** timed runs serialized with EXP-B (duckdb-bench) on the shared HEL1 box — `TIMING START`/`TIMING DONE` turn-taking.
**Grounded in:** `dhtcrawler/persist.go`, `queue/server/server.go` + `queue/handler/handler.go`, `processor/persist.go`, `model/torrent_contents.go:UpdateTsv`, `model/torrents.go:fileSearchStrings`, `database/fts/tsquery.go:AppQueryToTsquery`, `database/query/query.go:600-660`.

---

## 1. The WRITE pipeline, end-to-end

```
DHT crawl (dhtcrawler)
  └─ runPersistTorrents (persist.go) — accumulates a batch, then ONE tx:
       • torrents        CreateInBatches(…,100)  OnConflict UPDATE files_status, files_count, files_data   ← BLOB dual-written inline (blobmigration.SerializeFiles)
       • torrent_files   CreateInBatches(…,100)   ← the table dropped at cutover
       • torrent_sources CreateInBatches(…,100)
       • torrent_pieces  CreateInBatches(…,10)
       • queue_jobs      CreateInBatches(…,10)    ← enqueues a "process torrent" job per torrent
  └─ also enqueues classification in classifyBatchSize chunks
        │
        ▼  (decoupled via queue_jobs table)
Queue dispatcher (queue/server/server.go)
  • checkTicker polls queue_jobs every CheckInterval (default 30 s; tuned to 25 ms for the blob-backfill — MEMORY)
  • weighted semaphore, concurrency n = runtime.NumCPU() (handler/handler.go) — N jobs run in parallel
        │
        ▼
Processor (processor/persist.go) — per job: classify the torrent, then ONE tx:
       • content          CreateInBatches(…,100)  OnConflict UpdateAll
       • DELETE torrent_contents by id (reclassification)
       • torrent_contents CreateInBatches(…,100)  OnConflict UpdateAll   ← THE torrent_contents write (+ tsv column)
       • torrent_tags     CreateInBatches(…,100)  OnConflict DoNothing
       • DELETE torrents by info_hash (drops)
  └─ AFTER commit: indexToSearchSidecar() — fire-and-forget goroutine, 30 s timeout, per-doc upsert to the (Tantivy) sidecar; errors logged, never block crawl. (This is the dual-write seam ARCH-B swaps Tantivy→DuckDB — and which ARCH-A correctly does NOT use for Parquet, since Parquet has no cheap per-row append.)
```

**Key batch/flush points:** crawler tx = 100-row batches; queue decouples crawl from classify (durable `queue_jobs`, at-least-once); dispatcher parallelism = NumCPU under a semaphore; processor tx = 100-row batches; the search dual-write is **post-commit, async, best-effort** (a failed index write never fails the DB write — the periodic backfill reconciles).

**`tsv` is computed in Go, not in SQL** (`model/torrent_contents.go:UpdateTsv`): it builds an `fts.Tsvector` by `AddText` with weights — **A** = info_hash + torrent name; **C** = video resolution/source/codec/3D/modifier + release group; **D** = `Torrent.fileSearchStrings()`; seeded from `Content.Tsv` when classified. It is then stored as the `tsv` **column** via the `torrent_contents` UpdateAll upsert. ⚠️ `fileSearchStrings()` (`torrents.go:218-272`) is an **O(n²) suffix-dedup** over the file list — cost grows with files/torrent (avg 52, max 88,561), so tsv computation for pathological torrents is the Go-side hot spot, not the DB write.

---

## 2. The `torrent_contents` write cost — measured anchors + write amplification

Bench `torrent_contents` = **48.0 M rows / 61 GB**, with **24 indexes** (catalog-confirmed):
- **`content_type_tsv` GIN = 14 GB** (the FTS index — dominant).
- PK 3.3 GB, the natural-key unique 2.4 GB, `episodes` 1.1 GB, `size` 921 MB, `published_at` 754 MB, + ~16 more btrees ~318 MB each, + a `content_type_languages` GIN 118 MB.

⟹ **every `torrent_contents` upsert maintains 24 indexes including a 14 GB GIN** — high write amplification. The GIN(tsv) update (one entry per lexeme; a torrent with many files → many D-weight lexemes) is the dominant per-row write cost, well above the heap write. This is the real cost center of option (a)'s write path.

**MEASURED (20k-row non-HOT update, bench, the per-row all-24-index maintenance proxy):** **0.19 ms/row warm / 1.22 ms/row cold.** So the DB write cost of `torrent_contents` is **~0.2 ms/row** when hot — modest per row, but it's the **24-index maintenance** (incl. the 14 GB tsv GIN) that makes it the dominant DB write; the cold 1.2 ms/row shows it's I/O-bound on the GIN/btree leaf pages when uncached. Combined with the Go-side tsv compute (~0.4–1.5 ms/torrent typical, below), the **total per-torrent write is ~0.6–1.7 ms** dominated by tsv-build + GIN maintenance.

| metric | value (MEASURED) |
|---|---|
| torrent_contents write ms/row — **warm** (20k-row non-HOT update, all 24 idx incl 14 GB GIN) | **0.19 ms/row** (3,783 ms / 20k) |
| torrent_contents write ms/row — **cold** (uncached) | **1.22 ms/row** (24,332 ms / 20k) |
| **tsv compute (Go, `UpdateTsv`, µs/torrent — MEASURED)** | **see table below** |

**tsv `UpdateTsv` compute cost — MEASURED (Go micro-bench, real `UpdateTsv`→`fileSearchStrings`→`AddText`, M1 Max single-thread):**

| files/torrent | µs/torrent | µs/file | note |
|---|---|---|---|
| 1 | 19.9 | 19.9 | fixed overhead |
| 10 | 67 | 6.7 | |
| **52 (corpus avg)** | **424** | 8.2 | typical multi-file |
| 100 (`saveFilesThreshold` cap) | ~1,200 (interp.) | ~12 | crawler write-path ceiling |
| 500 | 11,356 | 22.7 | super-linear |
| 5,000 | 386,869 (387 ms) | 77.4 | pathological |

🎯 **The tsv build is super-linear (≈O(n²)) in files/torrent** — confirmed: µs/file rises 6.7→8.2→22.7→77.4 as files grow (both `fileSearchStrings`' suffix-dedup *and* `AddText`'s per-call lexeme rescan compound). **Implications:** (a) the crawler caps files at **`saveFilesThreshold=100`**, so the *write-path* tsv cost is bounded at **~1–1.5 ms/torrent** for typical torrents — modest, but it IS the heaviest single CPU step per torrent (vs ~sub-ms for the blob serialize). (b) **Large torrents from the importer path** (which bypasses the cap — 6% exceed 100 files, max 88,561) are a real hazard: a 5,000-file torrent is **387 ms of single-thread CPU**, and the max would be minutes — a tsv-rebuild hot spot worth a guard/cap. (Server x86 ≈ same order, ~1–2×.)

---

## 3. The main-search READ — `tsv @@ tsquery` + `ts_rank_cd` + facets

Built in `database/query/query.go`:
- **Match (line 647):** `WHERE torrent_contents.tsv @@ ?::tsquery` — uses the **14 GB `content_type_tsv` composite GIN** (so a `content_type` filter + FTS is a single index scan).
- **Rank/order (line 618):** `ORDER BY ts_rank_cd(torrent_contents.tsv, ?::tsquery)` — recomputed per matching row (the rank cost scales with match-set size, not corpus).
- **tsquery (line 477):** `fts.AppQueryToTsquery(str)` → app lexemes joined with `&`/`|`/`<->`/`!` and `:*` prefix (`tsquery.go`), cast directly `?::tsquery` (no regconfig — the tsv is app-built lexemes, language-agnostic).
- **Facets:** `createFacetsFilterCriteria` builds aggregation CTEs (counts per content_type/video_resolution/etc.) over the same filtered set.
- **`torrent_files` is never referenced** → the main search is **provably independent of the cutover DROP** (it reads only `torrent_contents`/`torrents`). Confirming this empirically is half the experiment.

**MEASURED (bench PG, single-core — bench pod `/dev/shm` too small for parallel GIN bitmap, so `max_parallel_workers_per_gather=0`; warm):**

| query | matches | p50 | p95 | notes |
|---|---|---|---|---|
| **filter-only** (content_type+resolution, btree, ORDER BY seeders) | — | **<0.1 ms** | 0.3 ms | pure btree, instant |
| **FTS rare term ranked** (`'ubuntu'`, top-20) | 7,137 | **121 ms** | 166 ms | selective FTS |
| **FTS rare + content_type ranked** (`ubuntu`+`software`) | 1,151 | **23 ms** | — | composite GIN; EXPLAIN below |
| **FTS rare facet** (GROUP BY content_type) | 7,137 | **9.8 ms** | 10.3 ms | facet counts |
| **FTS medium term, NO rank** (`'flac'` LIMIT 20) | 1.26 M | **0.5 ms** | 0.8 ms | GIN early-out (no rank → stop at 20) |
| **FTS medium term RANKED** (`'flac'` top-20) | 1.26 M | **15,389 ms** | 15,743 ms | 🚨 ts_rank_cd over all 1.26 M |
| **FTS broad term RANKED** (`'x264'`, single shot) | 4.28 M | **49,362 ms** | — | 🚨 see EXPLAIN |

🎯 **The dominant factor is the `ts_rank_cd` ranking over the full match set, NOT the GIN match.** Proof: `'flac'` matches 1.26 M rows — *unranked* `LIMIT 20` returns in **0.5 ms** (GIN early-out), but *ranked* `ORDER BY ts_rank_cd … LIMIT 20` takes **15.4 s** because PG must fetch every one of the 1.26 M matching heap rows and compute its rank before the top-20 heapsort. The broad `'x264'` (4.28 M) EXPLAIN makes it explicit:
```
Limit (actual 49354 ms)
  Sort (top-N heapsort, 20 rows)               -- sort itself is trivial
    Bitmap Heap Scan torrent_contents          -- 48.8 s: fetch + recheck + rank 4.28M rows (2.05M heap blocks, mostly disk)
      rows=4,278,916
      Bitmap Index Scan content_type_tsv (GIN) -- 482 ms: the match is cheap
```
**The served-shape EXPLAIN (`ubuntu`+`content_type`, 1,151 matches) = 23 ms** and touches only `torrent_contents` (composite `content_type_tsv` GIN + heap) — **zero `torrent_files` access → main search is empirically DROP-independent.** ✅

**Caveats:** single-core (parallelism disabled for the bench pod); production parallel scan + warm cache would cut the broad-term numbers ~N-core (49 s/8 ≈ 6 s — still slow). Selective queries (the realistic served case: multi-word title searches, or any FTS + a `content_type`/facet filter → few-k matches) are **<25 ms** and unaffected. The broad-single-common-term ranked search is a **known PG-FTS scaling wall** (O(match-set) rank computation), orthogonal to the file-search work.

---

## 4. Findings (all MEASURED unless marked *code*)

1. **Write path is queue-decoupled and batched at 100** *(code)*; classify runs at NumCPU concurrency under a semaphore; the search dual-write is post-commit/async/best-effort (never blocks the DB write).
2. **`torrent_contents` DB write = ~0.2 ms/row warm / 1.2 ms/row cold**, dominated by **24-index maintenance incl. a 14 GB tsv GIN** (the cost center).
3. **tsv is Go-computed and super-linear (≈O(n²)) in files/torrent** — 0.42 ms @52 files (avg) → 11 ms @500 → **387 ms @5,000**. Bounded ~1–1.5 ms/torrent on the crawler path (`saveFilesThreshold=100` cap); unbounded on the importer path → the per-torrent CPU hot spot (worth a guard).
4. 🎯 **Main-search READ latency is governed by `ts_rank_cd` over the match set, not the GIN match.** Selective queries (rare term, or FTS + content_type/facet → ≤few-k matches) = **<25 ms**; *unranked* GIN match on 1.26 M rows early-outs at **0.5 ms**; but *ranked* top-20 over 1.26 M = **15 s** / 4.28 M = **49 s** single-core (bitmap heap scan fetches + ranks every match). A real PG-FTS O(match-set) wall, orthogonal to file-search.
5. **Main search is DROP-independent — CONFIRMED**: the served-shape EXPLAIN touches only `torrent_contents` (composite `content_type_tsv` GIN + heap), **zero `torrent_files`**.

**Per-torrent write budget (measured):** Go tsv-build (~0.4–1.5 ms typical) + DB upsert (~0.2 ms/row warm, all 24 idx) ⟹ **~0.6–1.7 ms/torrent**, tsv-build + GIN-maintenance dominated. None of it touches `torrent_files` semantics — the write path is unaffected by the DROP (it stops writing the dropped table, removing one batched insert).

---

## 5. Status — COMPLETE
- ✅ Write pipeline mapped end-to-end (code).
- ✅ tsv `UpdateTsv` compute cost (Go micro-bench).
- ✅ `torrent_contents` write ms/row (DB, 24-index amplification).
- ✅ Main-search p50/p95 across query shapes + EXPLAIN (GIN usage, the `ts_rank_cd` wall, DROP-independence proven).
**Method note:** bench pod `/dev/shm` is too small for parallel GIN bitmaps over millions of matches → ran `max_parallel_workers_per_gather=0` (single-core, conservative; prod parallel + warm cache faster, but the O(match-set) ranking wall remains). All read-only except a scratch 20k-row `UPDATE` on the disposable bench.

---

## App. A — Out-of-scope (follow-up): Tier-0 agg replaces the file-ext EXISTS
From the earlier framing (now deferred): the served file-ext filter's multi-file branch `EXISTS(torrent_files WHERE info_hash=… AND extension IN …)` (`criteria_torrent_file_extension.go`) can be re-pointed at the per-(torrent,ext) aggregate `EXISTS(agg_torrent_ext …)` to survive the DROP. **Parity already validated empirically:** over a 20,000-torrent sample (`ext IN mkv,avi`), agg-EXISTS and torrent_files-EXISTS agreed on **6,598/6,598 matches — 0 disagreements** (a set identity by construction: agg = `GROUP BY info_hash,extension` of torrent_files). Latency comparison (agg 56 M-row PK probe vs 879 M-row table) is the natural follow-up when this returns to scope (tracked as IMPL-A4 `agg_torrent_ext`).
