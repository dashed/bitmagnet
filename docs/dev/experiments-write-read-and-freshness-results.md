# Experiments — Write/Read Path (a) + Base+Delta Freshness (b): Results

**Date:** 2026-06-07
**Status:** Both experiments COMPLETE on the live throwaway bench env (879.5M-row corpus on HEL1). Real measurements; production untouched.
**Threads:** EXP-A (`pg-data-bench`, [`exp-a-write-read-path.md`](./exp-a-write-read-path.md)) · EXP-B (`duckdb-bench`, [`exp-b-base-delta-freshness.md`](./exp-b-base-delta-freshness.md)) · lead synthesis (EXP-C).
**Feeds:** [`duckdb-parquet-parity-architecture.md`](./duckdb-parquet-parity-architecture.md).

---

## Experiment (a) — write pipeline + `torrent_contents` tsvector read

### Write path
DHT crawl → `dhtcrawler/persist.go` batched tx **@100** (blob dual-written **inline**) → `queue_jobs` decouples → dispatcher (weighted semaphore, concurrency = NumCPU) → `processor/persist.go` classify → `torrent_contents` upsert **@100** → **post-commit async best-effort** search dual-write (the seam where the DuckDB/agg writes ride).

| Write cost (measured) | value |
|---|---|
| `torrent_contents` upsert | **0.19 ms/row warm, 1.22 ms/row cold** (20k-row update) — dominated by **24 indexes incl. a 14 GB `content_type_tsv` GIN** |
| tsvector `UpdateTsv` (Go-computed, **super-linear ≈O(n²)** in files/torrent) | 0.42 ms @52 (avg) → 11 ms @500 → **387 ms @5,000 files** |
| **per-torrent write total** | **~0.6–1.7 ms** (tsv-build + GIN-maintenance dominated) |

- The crawler caps files at `saveFilesThreshold=100` → tsv stays ~1–1.5 ms/torrent on the hot path. **The importer path bypasses the cap** → a 5,000-file torrent = 387 ms single-thread, the max (88,561 files) = *minutes* — a pathological-torrent CPU hot spot **worth a guard/cap** (two compounding O(n): `fileSearchStrings`' suffix-dedup + `AddText` rescanning lexemes for `nextPos`).
- **Implication for the architecture:** the Tier-0 `agg_torrent_ext` dual-write (~3.23 small PK upserts/torrent) is **cheap on top of** an upsert already paying a 14 GB GIN — it won't move the write budget.

### Read path — the headline finding
🎯 **Main-search latency is governed by `ts_rank_cd` over the match set, NOT the GIN match.**

| query | matches | p50 |
|---|---|---|
| filter-only (btree) | — | **<0.1 ms** |
| FTS rare + content_type, ranked | 1,151 | **23 ms** |
| FTS rare, ranked | 7,137 | 121 ms |
| FTS medium, **no rank** (GIN early-out) | 1.26M | **0.5 ms** |
| FTS medium, **ranked** | 1.26M | **15.4 s** |
| FTS broad (`x264`), ranked | 4.28M | **49 s** |

- Proof it's the *ranking*: unranked `flac` (1.26M matches) early-outs at 0.5 ms, but ranked top-20 = 15 s — PG heap-fetches + `ts_rank_cd` **every** match before the top-20 heapsort (EXPLAIN: bitmap heap scan of 4.28M = 48.8 s; the GIN match itself is only 482 ms; the sort is trivial).
- **Realistic served queries** (multi-word/title + `content_type` → ≤ few-k matches) are **<25 ms**. **Broad single-common-term *ranked* search is a known PG-FTS O(match-set) wall** (measured single-core; prod parallel + warm cache is faster, but the wall persists). → a separate optimization opportunity (limit/short-circuit ranking on huge match sets, or a different ranker), **independent of the file-data work**.
- ✅ **DROP-independence CONFIRMED:** the served search EXPLAIN touches only `torrent_contents` (composite `content_type_tsv` GIN + heap) — **zero `torrent_files`**. The main search is unaffected by the `torrent_files` cutover.

---

## Experiment (b) — base+delta incremental freshness

**Answer: DuckDB-on-Parquet is NOT limited to batch-refresh staleness — base+delta gives ~minute freshness at <250 ms query cost, no full rebuild.**

| delta size (torrents) | collapse p50 | find p50 |
|---|---|---|
| 0 (base only) | 141 ms | 56 ms |
| +1k | 179 ms | — |
| +10k | 193 ms | — |
| **+100k (~hours of crawl)** | **230 ms** | 91 ms |

- **Delta-append is sub-second in production** (the processor already holds the new torrents + decoded blobs; the 60–73 s the bench saw was the "pick recent torrents" `ORDER BY created_at` sort artifact, not the append).
- **Compaction** ≈ 1M delta torrents → an 83 s full rebuild + atomic swap.
- **Freshness SLA = the delta-flush interval** (1-min flush → ~1-min freshness, <250 ms query cost up to a 100k-torrent delta).

### 🚨 Correct supersession pattern (EXP-B-proven)
`files_data` is upsert-with-`DoUpdates` (`persist.go:113-123`), so a re-crawl supersedes a torrent's **whole fileset**. Latest-wins is **TORRENT-granular, via an ANTI-JOIN:**
```sql
-- exclude any base info_hash present in the delta, then UNION the delta:
SELECT * FROM base b WHERE NOT EXISTS (SELECT 1 FROM delta d WHERE d.info_hash=b.info_hash)
UNION ALL SELECT * FROM delta;
```
- ❌ **`row_number() OVER (PARTITION BY info_hash ORDER BY delta_ts DESC)=1` is WRONG** — it keeps one *file* per torrent → silently drops a multi-file torrent's other files.
- ❌ window-max over the whole set = **80× slower** (19 s vs 230 ms).
- The anti-join lets the base predicate prune via zonemaps and hash-anti-joins the tiny delta (`physical_hash_join.cpp:188`). Supersession correctness verified (a re-crawled torrent's old fileset is excluded from collapse counts, not double-counted).

---

## Net for the architecture

- **(a)** The served main search is DROP-independent; its write cost is tsv-build + GIN-maintenance (~0.6–1.7 ms/torrent), so the Tier-0 `agg_torrent_ext` dual-write is essentially free on top. Two pre-existing, *separate* hot spots surfaced: the importer-path O(n²) tsv build (cap/guard it) and the broad-ranked PG-FTS O(match-set) wall (a future ranking optimization).
- **(b)** Freshness is no longer a reason to prefer anything else: **incremental base+delta gets cross-file search to minute-scale at <250 ms**, with correct torrent-granular supersession. The architecture doc's freshness section now carries the measured curve + the anti-join pattern.

---

## 🎯 Unifying conclusion (across all the work — benchmarks + architecture + experiments)

**Everything splits cleanly into "structured" vs "broad free-text", and that split — not the file-data design — is what determines whether an inverted index is worth it.**

| Workload | DuckDB-on-Parquet | PG | Inverted index (Tantivy / DuckDB BM25) |
|---|---|---|---|
| **Structured** — ext∧size, distinct-torrent collapse, ranges, exact counts, analytics, faceting | **<250 ms** (most <35 ms) at **+3.9–11.7 GB** (sort + rollups) | <50 ms (PG aggregate, Tier-0) | no advantage (slower, +14–50 GB) |
| **Broad free-text** — ranked full-text / leading-wildcard substring | ~23 s (`ILIKE` full scan, unprunable) | **15–49 s** (`ts_rank_cd` over the match set) | **<50 ms** (the only thing that wins) — +34.9 GB, CJK-token-only |

- The same shape holds **for the per-file search AND the existing main torrent search**: structured filtering is cheap everywhere; **broad free-text *ranked/substring* search is the sole workload that hits an O(match-set) wall in *both* engines** and the sole case an inverted index earns its disk + tokenizer cost.
- Crucially, **the existing PG main search already lives with this exact wall** (broad ranked FTS = seconds) — and it's **DROP-independent**. So the file-search decision introduces *nothing new*: it inherits the same "structured = cheap, free-text-ranked = index-or-wait" trade the product already accepts.
- ⟹ **Final synthesis:** reject the per-file Tantivy index (structured per-file search is cheaper/faster on DuckDB-on-Parquet at minute-scale freshness); gate *any* inverted index — file-path-FTS or a future main-search accelerator — strictly on a measured product need for **<50 ms broad free-text search**, knowing it costs an index + a CJK-aware tokenizer and is the one place that cost is unavoidable.
