According to the 2026-06-08 design doc, I’d **keep the core strategy**: blob as the durable replacement, prove-before-drop, DuckDB-on-Parquet for net-new structured file search, and no default Tantivy/CJK index. The benchmarks are strong enough to justify rejecting the 873M-doc structured Tantivy path, and the doc correctly separates the actual DROP gate from net-new cross-file search.  

But yes: **we can do better.** The biggest improvement is to shrink the critical path and make the correctness model harder to accidentally violate.

## Top audit findings

| Priority | Finding                                                                            | What I’d change                                                                                                                                                       |
| -------- | ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **P0**   | The DROP gate may not need `agg_torrent_ext` at all.                               | First benchmark the already-deployed `torrents.file_extensions` / `torrent_file_summary.extensions` JSONB path as the replacement for the extension/file-type EXISTS. |
| **P0**   | Delta supersession is missing an explicit tombstone / changed-key set.             | Anti-join against `changed_torrents`, not just delta file rows, or torrents that change to zero files/no valid rows can leak stale base rows.                         |
| **P0**   | Denormalized DuckDB fields can go stale independently of file updates.             | Either remove `content_type/published_at` denorm from v1 or add watermarks/outbox events for `torrent_contents` and source-table changes.                             |
| **P0**   | Blob extension authority is still easy to misuse.                                  | Treat blob `e` as deprecated/advisory; all readers/exporters/checkers derive extension from path. Move G1/G2 before L2-P0.                                            |
| **P1**   | `agg_torrent_ext.max_size` is “future-proofed” in data, but not fully in indexing. | Either add a measured `(extension, max_size, info_hash)` index later, or narrow the claim to “data-ready, not query-ready.”                                           |
| **P1**   | DuckDB sidecar + CronJobs can become a multi-process write hazard.                 | Build immutable versioned bundles offline; server opens read-only; swap by generation pointer; never mutate the live DuckDB DB from CronJobs.                         |
| **P1**   | Space numbers mix DuckDB-only and all-in L2 cost.                                  | Rewrite as: DuckDB +3.9–12.3 GB, PG agg +3–6 GB, all-in L2 roughly +8–18 GB.                                                                                          |
| **P2**   | Path substring search needs explicit product limits.                               | Minimum length, wildcard escaping, timeouts, no broad/CJK promise unless the optional ngram index is enabled.                                                         |

## The biggest opportunity: try deleting `agg_torrent_ext` from the DROP gate

The doc says the live search hot path only needs **distinct-torrent extension/file-type presence**, not file counts or per-file grouping. It also says the migration already created path-derived `file_extensions` JSONB on `torrents`, plus `torrent_file_summary.extensions`.  

That means the cheapest possible DROP-gate replacement is probably:

```sql
-- multi-file presence via existing blob-derived JSONB
torrents.file_extensions @> '["mkv"]'::jsonb

-- OR, if using the summary table:
torrent_file_summary.extensions @> '["mkv"]'::jsonb
```

For multiple extensions, do an OR of singleton containment clauses, because the existing `jsonb_path_ops` GIN index is good for containment (`@>`), not the `?|` existence-any operator. PostgreSQL’s current docs say `jsonb_path_ops` supports `@>`, `@?`, and `@@`, and is usually smaller and more specific than default `jsonb_ops`. ([PostgreSQL][1])

So I’d add a **L2a-0 experiment** before building a 55M-row aggregate:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT ...
FROM torrent_contents tc
JOIN torrents t ON t.info_hash = tc.info_hash
WHERE ...
  AND (
    t.extension = ANY($1)
    OR t.file_extensions @> '["mkv"]'::jsonb
    OR t.file_extensions @> '["mp4"]'::jsonb
  );
```

Run it for common, rare, and broad queries: `mkv`, `mp4`, `srt`, `jpg`, `zip`, “video file type” extension lists, and the current facet loop. If it is within budget, **skip `agg_torrent_ext` for the DROP gate**. Keep `agg_torrent_ext` only if JSONB plans are unstable or if you want PG to serve `extension ∧ max_size` later.

That could save ~5–6 GB, a new seed pipeline, a new drift surface, FK/index maintenance, and a whole checker class. The existing doc already says the aggregate is ~55M rows and ~5–6 GB for the max-size form, so it is worth proving the zero-new-table path first. 

## Fix the source-of-truth contract before any new layer

The doc correctly identifies G1: extension must be derived from the path, never from blob `e`; it also says crawl-path blobs can have empty `e`, while backfilled blobs are correct, producing split data-at-rest. 

I’d make that stricter:

```text
BlobFile.extension is non-authoritative compatibility data.
Canonical extension = file_extension_from_path(path), always.
Every consumer MUST ignore BlobFile.extension unless explicitly rendering legacy/debug data.
```

Then add shared Go/Rust fixture tests for edge cases:

```text
"Movie.MKV"          => "mkv"
"foo.tar.gz"         => "gz"
".bashrc"            => null
"dir.with.dot/file"  => null
"file."              => null
"file.srt"           => "srt"
"file.S01E01.mkv"    => "mkv"
"file.7z"            => "7z"
"file.x_y"           => null
"日本語.mkv"          => "mkv"
```

This is a small change with a huge payoff: exporters, browser hydration, PG aggregate, JSONB checks, and DuckDB all share one semantic contract.

## The base+delta algorithm needs a tombstone file

The current DuckDB view anti-joins base rows against `delta.fact.parquet` keys. That is correct only if every changed torrent has at least one row in the delta fact. 

It breaks when a torrent changes to:

```text
files_data = NULL
zero files
only extensionless files, if a later query reads only extension-qualified fact rows
deleted torrent
status changed to no-info
```

The safer design is:

```sql
CREATE VIEW files AS
  SELECT b.*
  FROM base_fact b
  WHERE NOT EXISTS (
    SELECT 1
    FROM delta_changed_torrents k
    WHERE k.info_hash = b.info_hash
  )

  UNION ALL

  SELECT *
  FROM delta_fact;
```

So each delta generation should publish at least two files:

```text
delta_fact.parquet               -- actual replacement file rows
delta_changed_torrents.parquet   -- one row per changed info_hash, including zero-row replacements
```

That also makes the anti-join cheaper and clearer, because the right side is distinct torrent keys rather than many file rows.

## Watermarks need to cover more than `torrents.updated_at`

The design says the delta job carves `torrents WHERE updated_at > :watermark` and needs an index on `torrents.updated_at`.  That is fine only for file-list changes if `updated_at` is guaranteed to advance on every relevant file/blob write.

But the proposed DuckDB fact also denormalizes `content_type`, `published_at`, and `created_at`.  Those can change outside `torrents.files_data`:

```text
torrent_contents classification changes
torrent source published_at changes
torrent deletion / merge behavior
metadata refresh without file-list refresh
```

So either:

1. **v1 DuckDB fact only stores file facts**: `info_hash, file_index, path, extension, size`. Then Go/PG joins or post-filters torrent/content metadata.

or:

2. Add a proper change stream:

```text
changed_files(info_hash, reason, updated_at)
changed_content(info_hash, reason, updated_at)
changed_sources(info_hash, reason, updated_at)
```

Use a two-part cursor `(updated_at, info_hash)` and capture an upper bound at the start of each run:

```sql
upper := SELECT max(updated_at), max(info_hash at that updated_at)
export WHERE cursor < (updated_at, info_hash) <= upper
publish atomically
advance cursor only after publish succeeds
```

Also set Kubernetes `concurrencyPolicy: Forbid` and use a PG advisory lock so minute jobs cannot overlap.

## DuckDB deployment should be immutable/read-only

The doc proposes a sidecar plus separate delta/compaction CronJobs.  Current DuckDB docs say one process can read-write, while multiple processes can read the same DB only in read-only mode; write concurrency across multiple processes is a different, newer model and not what you want for this simple sidecar. ([DuckDB][2])

So the production rule should be:

```text
CronJobs build a new immutable generation in staging.
Server opens current generation read-only.
Reload swaps to a new generation.
Old generations are retained until no in-flight query uses them.
No CronJob ever mutates the live DuckDB DB file opened by the server.
```

Bundle layout:

```text
/file-search/
  generations/
    2026-06-08T12-00-00Z/
      base_fact.parquet
      delta_fact.parquet
      delta_changed_torrents.parquet
      rollups.duckdb
      manifest.json
    2026-06-08T12-01-00Z/
      ...
  current -> generations/2026-06-08T12-01-00Z
```

`manifest.json` should include row counts, changed torrent counts, min/max watermark, source DB snapshot marker, schema version, and checksums.

## Harden DuckDB as if SQL is code

The doc already says structured filters are parameterized and path substring is bound.  Good. I’d still add explicit sandboxing because DuckDB can read/write files, access network-backed data sources, load extensions, and consume large memory/CPU; DuckDB’s own security guidance says untrusted SQL should be treated like code and isolated with sandboxing, restricted capabilities, timeouts, and input validation. ([DuckDB][3])

Practical additions:

```text
No arbitrary SQL endpoint.
Read-only DB open.
Container filesystem read-only except the mounted generation directory.
No network egress from filesearch except needed service calls.
Statement timeout.
Memory limit.
Thread limit.
Semaphore around queries.
Escape %, _, and backslash in user path_query before ILIKE.
Minimum path_query length.
Hard max page size.
```

For substring search:

```sql
path ILIKE '%' || escape_like($1) || '%' ESCAPE '\'
```

Otherwise a user searching `%` or `_` can accidentally force a broad scan.

## If `agg_torrent_ext` survives, change its rollout

The proposed DDL is reasonable for a table that starts empty.  But for production seed and future rebuilds, I’d change the operational plan:

```text
1. Create table without the secondary index.
2. Bulk seed through COPY/staging, sorted by info_hash.
3. Build secondary index after seed.
4. Run ANALYZE.
5. Run parity checker.
6. Only then enable shadow reads.
```

PostgreSQL’s `CREATE INDEX CONCURRENTLY` cannot run inside a transaction block and failed concurrent builds can leave invalid indexes, so any later index build needs explicit migration/runbook handling. ([PostgreSQL][4])

Also reconsider the FK during initial seed. A FK to `torrents(info_hash)` is semantically nice, but 55M FK checks during seed are not free. Options:

```text
Option A: no FK; rely on checker + delete path.
Option B: seed first, then add FK NOT VALID, then VALIDATE later.
Option C: keep FK, but budget/measure WAL, CPU, and lock behavior.
```

For the future `max_size` use case, the current secondary index `(extension, info_hash)` does not really optimize `extension = 'mkv' AND max_size > ...`. If PG is expected to serve that query, test and likely add:

```sql
CREATE INDEX CONCURRENTLY idx_agg_torrent_ext_ext_size_hash
ON agg_torrent_ext (extension, max_size, info_hash);
```

If not, remove the “future-proofs ext∧size” claim and say it only stores the data needed for a future measured index.

## Tighten the public doc’s numbers

Several numbers are individually plausible but look inconsistent in one document:

```text
857M live rows vs 879.5M benchmark rows
276 GB live table vs 261 GB restored benchmark table
16,976,700 summary rows vs 16,977,232 verified rows
L2 +4–12 GB vs optimized all-in +16-ish GB
```

The doc explains some of this implicitly: benchmark corpus came from a pre-cutover dump, while deployed state is production later/elsewhere.  But a reader will still stumble.

Add a “measurement provenance” table:

```text
Metric                    Value        Source/time              Meaning
live torrent_files size    ~276 GB      FSN1, migration planning DB object size
bench torrent_files rows   879.5M       HEL1 restored dump       benchmark corpus
blob verified torrents     16,977,232   FSN1 verify --full       checked blobs
summary rows               16,976,700   FSN1 backfill complete   summary rows at completion
```

Also rewrite the L2 space language:

```text
DuckDB slim:      +3.9 GB
DuckDB optimized: +12.3 GB
PG agg:           +3–6 GB
All-in L2:        +8–18 GB
Total after L1+L2: roughly 27–37 GB
```

That matches the later table showing blob-only ~19 GB, cheap search ~27 GB, optimized search ~35 GB. 

## Revised build order

I’d change the roadmap to this:

```text
A0 — Semantic hardening
  - G1: derive extension from path everywhere.
  - Mark blob e non-authoritative.
  - G2: browser reads blob.
  - Hydration sorted/deterministic.
  - C6 retired-PG-path guard.
  - Shared Go/Rust extension fixtures.

A1 — Cheapest DROP-gate experiment
  - Replace torrent_files EXISTS with file_extensions JSONB behind a flag.
  - Run EXPLAIN/BUFFERS on real hot queries.
  - Run parity checker: JSONB/file_extensions vs torrent_files.

A2 — Only if A1 fails: build agg_torrent_ext
  - Bulk seed.
  - Analyze.
  - Parity checker.
  - Shadow result comparison.
  - Flip extension/file-type filter.

A3 — DROP-readiness gate
  - No live code path reads torrent_files.
  - Zero mismatch window.
  - Fresh backup ID recorded.
  - Rollback plan documented.
  - DROP still manual.

B1 — DuckDB sidecar for net-new fileSearch
  - Immutable read-only generations.
  - changed_torrents tombstone set.
  - reliable watermarks.
  - no arbitrary SQL.
  - query-class limits.

B2 — Optional CJK/free-text index
  - Only after product evidence that broad/CJK path search matters.
```

## Bottom line

The design is directionally excellent, but I would **not start by building `agg_torrent_ext`**. I’d first prove whether the already-deployed JSONB extension set clears the only live DROP-gate query shape. If it does, the project gets smaller, cheaper, and safer.

The main correctness changes I’d make before implementation are: **canonical path-derived extension everywhere, delta tombstones, reliable multi-table watermarks, immutable/read-only DuckDB generations, and clearer all-in space accounting**.

[1]: https://www.postgresql.org/docs/current/datatype-json.html?utm_source=chatgpt.com "Documentation: 18: 8.14. JSON Types"
[2]: https://duckdb.org/docs/current/connect/concurrency.html "Concurrency – DuckDB"
[3]: https://duckdb.org/docs/lts/operations_manual/securing_duckdb/overview.html "Securing DuckDB – DuckDB"
[4]: https://www.postgresql.org/docs/current/sql-createindex.html "PostgreSQL: Documentation: 18: CREATE INDEX"
