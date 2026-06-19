# Response to `feedback_1.md` (external design review)

**Date:** 2026-06-09 · **Reviewed doc:** `docs/dev/per-file-search-master-design-and-results.md` (2026-06-08).
**Verdict:** the review is **high quality and substantially correct** — I accept essentially all of it. Every load-bearing claim below was **re-verified against the bitmagnet code and the canonical DuckDB source** (an opus verification team: `fb-jsonb`, `fb-freshness`, `fb-duckdb`). Nothing was rejected; refinements are noted. Tasks **FB-A0…FB-DOC (#60–#68)** track the work; the build order is revised (§ "Revised build order").

> **One correction the review itself needs:** DuckDB has **no `statement_timeout`** setting (it's a Postgres-ism). Per-query time limits use a hard caller deadline backed by the client **`Connection::Interrupt()`** API, with the connection kept out of the pool until DuckDB unwinds. Everything else in the DuckDB section verified.

---

## Per-finding verdicts

### P0-1 — The DROP gate may not need `agg_torrent_ext` at all → ✅ **MEASURED & CONFIRMED (2026-06-09): use JSONB, drop `agg`**

> **FB-A1 ran on the real 879.5M-row restore — full results in [`../dev/fba1-jsonb-dropgate-results.md`](../dev/fba1-jsonb-dropgate-results.md).** `file_extensions @>` **wins on every axis vs both the current `torrent_files` EXISTS and the `agg_torrent_ext` fallback**: disk **+119 MB vs +9.5 GB** (~80×, and already deployed); freshness **real-time** (already dual-written); served filter **44–114 ms** (competitive); facet **~1 ms** (production `budgeted_count` just `EXPLAIN`s) _with better estimates_ (realistic 2.4–15.7M vs the impossible ~24–27M from EXISTS); **exact set-parity** with `torrent_files` (and 50–110× faster to evaluate). ⟹ **`agg_torrent_ext` is dropped from the gate** — retained only as a future option if an `ext ∧ max_size` query is ever committed. L2-P0 collapses to a flag-gated `EXISTS→file_extensions @>` swap + a parity confirmation.

The single most valuable finding, and one we missed. **Confirmed (`fb-jsonb`):**

- `torrents.file_extensions JSONB` exists (`00021_blob_storage.sql:5`), is **`jsonb_path_ops` GIN-indexed** (`00022_blob_indexes.sql:4`), **G1-path-derived** (`ExtractUniqueExtensions` → `file_extension_from_path`, skips empties — `serializer.go:74-97`), and is **dual-written + backfilled** → populated for all with-files torrents.
- 🔑 **Coverage-equivalence:** `file_extensions` and the `torrent_files` rows are built from the **same capped `files` slice** (`persist.go:203-228`) — same ≤100 truncation, no drift. Swapping the multi-file `EXISTS torrent_files … extension IN (…)` for OR-of-`file_extensions @> '["x"]'` changes the _source_, not the _result set_.
- **Our own L2-P0 spec already concedes the gate needs only `(info_hash, extension)` presence** — exactly what `file_extensions` encodes.

**Resulting change:** new **FB-A1 experiment** — `EXPLAIN(ANALYZE, BUFFERS)` the JSONB path _before_ building `agg_torrent_ext`. Build `agg` **only if** (a) broad-facet plans are unstable, or (b) we commit to a future `ext ∧ max_size` (JSONB carries no size). Single-file branch (`torrents.extension`) stays regardless.

**Refinements (mine):**

- The **facet is the cost multiplier**: the file-type facet runs _one_ `BudgetedCount(*)` per file-type value (8 values), each an up-to-**11-way** `@>` GIN-BitmapOr (`video` = 11 exts). The experiment **must** include the full facet loop, not just the filter — that's where JSONB could lose to a purpose-built `(extension, info_hash)` agg index.
- `jsonb_path_ops` supports `@>` but **not** `?|` (reviewer correct) → multi-ext = OR-of-`@>` singletons.
- 🚨 **Experiment data caveat:** the HEL1 restore is the _pre-backfill_ dump, so its `file_extensions` is **empty** — the JSONB plans are meaningless until we **populate `file_extensions` from `torrent_files` on the restore** (or use a blob-backfilled snapshot). Folded into FB-A1.

### P0-2 — Delta supersession needs a tombstone / changed-key set → **ACCEPT (confirmed bug)**

**Confirmed (`fb-freshness`):** our view `base WHERE NOT EXISTS(delta_fact d WHERE d.info_hash=b.info_hash) UNION ALL delta_fact` **leaks stale base rows** whenever a changed torrent emits zero delta_fact rows. The unambiguous case is the **delete path** — the classifier can return `ErrDeleteTorrent` → torrents are hard-`DELETE`d (`processor/processor.go:147-149`, `processor/persist.go:103-113`); a deleted torrent has no `files_data` → no delta rows → its base rows survive forever. Also NULL-`files_data` / no-info / over-threshold transitions. No compensating DB trigger exists.

**Resulting change (FB-B1a):** each delta generation publishes **`delta_changed_torrents.parquet`** (one row per changed `info_hash`, **including deletes and zero-row replacements**); the view anti-joins on that key set: `base WHERE info_hash NOT IN (delta_changed_torrents) UNION ALL delta_fact`. Cheaper (distinct keys) _and_ correct. The changed-set source must capture deletes — not just torrents that still have files.

### P0-3 — Denormalized DuckDB fields go stale independently of file updates → **ACCEPT (confirmed)**

**Confirmed (`fb-freshness`):** `content_type` lives in `torrent_contents`; a reclassification upserts only `torrent_contents`/`content` and **never bumps `torrents.updated_at`** (`processor/persist.go:83-91`). `published_at` lives in `torrents_torrent_sources` and advances _its_ `updated_at`, not the torrent's (`dhtcrawler/persist.go:395-415`). `files_data` writes **do** bump `torrents.updated_at` (so it's a sound watermark for **file facts**); `created_at` is immutable.

**Resulting change (FB-B1b):** **v1 DuckDB fact = file-facts-only** `(info_hash, file_index, path, extension, size)` + immutable `created_at`; **drop the mutable `content_type`/`published_at` denorm**, resolve them via a live PG join at query time. A full denorm requires a **multi-table change stream** (`changed_files`/`changed_content`/`changed_sources`) + a **two-part `(updated_at, info_hash)` cursor** with an upper bound captured at run start and publish-then-advance — deferred to v2. Add k8s `concurrencyPolicy: Forbid` + a PG advisory lock so minute jobs can't overlap.

### P0-4 — Blob extension authority is easy to misuse → **ACCEPT (strengthen G1 + sequence first)**

The doc already flags G1; the review makes it a **contract**. **Resulting change (FB-A0):** treat `BlobFile.extension` (`e`) as **non-authoritative compatibility data** — canonical extension = `file_extension_from_path(path)`, _always_; every reader/exporter/checker ignores `e` except legacy/debug rendering. Add **shared Go/Rust fixture tests** for the edge cases (`foo.tar.gz`→`gz`, `.bashrc`→null, `dir.with.dot/file`→null, `file.`→null, `日本語.mkv`→`mkv`, …). Move semantic hardening (G1/G2 + fixtures) **before** the DROP-gate work.

### P1-1 — `max_size` is data-ready but not query-ready → **ACCEPT (narrow the claim)**

If `agg_torrent_ext` survives FB-A1: the `(extension, info_hash)` index does **not** optimize `extension='mkv' AND max_size > …`; either add a measured `(extension, max_size, info_hash)` index or relabel `max_size` **"data-ready, not query-ready."** Moot if FB-A1 deletes agg. (FB-A2.)

### P1-2 — DuckDB sidecar + CronJobs = multi-process write hazard → **ACCEPT (verified; effectively forced)**

**Confirmed (`fb-duckdb`, canonical source):** the DB file lock is access-mode-driven and enforced via `fcntl` — read-only → `F_RDLCK` (shared), read-write → `F_WRLCK` (exclusive) (`storage/database_handle.cpp:23-31`, `common/local_file_system.cpp:368-424`). So **one RW holder XOR N read-only holders** — a CronJob opening the live DB read-write while the server holds it read-only **throws**. The single-file format is single-writer (the multi-process-write story is DuckLake/external catalogs, not a simple sidecar).

**Resulting change (FB-B1c):** CronJobs build a **new immutable generation** offline (`base_fact.parquet` + `delta_fact.parquet` + `delta_changed_torrents.parquet` + `rollups.duckdb` + `manifest.json`); the server opens the current generation **`access_mode=READ_ONLY`** and swaps a `current` pointer on reload; old generations are retained until in-flight queries drain; **no CronJob ever mutates the live DB.** `manifest.json` = row counts, changed-torrent count, min/max watermark, source-snapshot marker, schema version, checksums.

### "Harden DuckDB as if SQL is code" + P2 (path-substring limits) → **ACCEPT (verified + one correction)**

**Confirmed (`fb-duckdb`):** DuckDB SQL can read/write files, load extensions, and reach the network (`enable_external_access` default **true**) → untrusted SQL is code. Grounded lockdown (open-time, then `lock_configuration=true`): `enable_external_access=false`, `autoload/autoinstall_known_extensions=false`, `allow_unsigned/community_extensions=false`, `allow_persistent_secrets=false`, optional `disabled_filesystems`/`allowed_directories`; resource caps `memory_limit`/`threads`/`operator_memory_limit` + an app-level query **semaphore**; container FS read-only except the generation mount; no network egress. **Never expose raw SQL** — only structured params compiled server-side.

⚠️ **Correction:** **there is no `statement_timeout` in DuckDB.** Enforce per-query deadlines by returning from the caller at the deadline, calling **`Connection::Interrupt()`** (`main/connection.hpp:57`, `client_context.hpp:121`) on the running connection, and reusing that connection only after DuckDB unwinds.

**Path substring (P2) — confirmed necessary:** binding stops SQL _injection_ but **not wildcard injection** — a user-supplied `%`/`_` in the bound value is still a wildcard (`function/scalar/string/like.cpp:184-221`). So **escape `\`, `%`, `_`** before binding and use `path ILIKE '%' || escape_like($1) || '%' ESCAPE '\'`; plus **minimum `path_query` length**, **hard max page size**, the `Interrupt()` deadline, and **no broad/CJK promise** unless the optional ngram index is enabled. (FB-B1d.)

### Tighten the doc's numbers (provenance + space) → **ACCEPT (doc clarity)**

Add a **measurement-provenance** table (live-prod FSN1 vs restored-bench HEL1 vs verify counts) and rewrite the space accounting: **DuckDB slim +3.9 GB / optimized +12.3 GB · PG agg +3–6 GB · all-in L2 +8–18 GB · L1+L2 ~27–37 GB.** (FB-DOC — applied to the master doc.)

### `agg_torrent_ext` rollout (if it survives FB-A1) → **ACCEPT (conditional ops)**

Create the table **without** the secondary index → **COPY-seed** sorted by `info_hash` → build the index after seed → `ANALYZE` → parity checker → shadow → flip. FK: **`NOT VALID` then `VALIDATE`** later, or none + checker, or measure WAL/CPU/lock. `CREATE INDEX CONCURRENTLY` can't run in a txn and can leave an invalid index on failure → explicit runbook handling. (FB-A2.)

---

## Revised build order (supersedes the master doc §7)

```
A0  Semantic hardening (BEFORE the DROP gate)
    G1 path-derived extension everywhere · blob `e` non-authoritative (contract)
    · G2 browser reads blob · deterministic hydration · C6 retired-PG-path guard
    · shared Go/Rust extension fixtures.                                   [FB-A0]

A1  Cheapest DROP-gate experiment
    EXPLAIN(ANALYZE,BUFFERS) file_extensions @> (filter + the 8-value facet loop)
    on a file_extensions-populated restore; parity JSONB vs torrent_files.[FB-A1]

A2  Only if A1 fails: agg_torrent_ext (hardened rollout).                  [FB-A2]

A3  DROP-readiness gate
    no live path reads torrent_files · zero-mismatch window · fresh backup id
    · rollback plan · DROP stays manual + deferred.

B1  DuckDB sidecar (net-new fileSearch)
    immutable read-only generations + manifest · changed_torrents tombstone
    · file-facts-only v1 + reliable watermark · no raw SQL + lockdown + limits.
                                                          [FB-B1a/b/c/d]
B2  Optional CJK/free-text ngram index — only on measured product demand.
```

## Bottom line

The design is directionally validated; the review makes it **smaller, cheaper, and safer**. The biggest win is **not starting by building `agg_torrent_ext`** — prove the already-deployed `file_extensions` JSONB clears the only live DROP-gate query shape first. The correctness changes adopted before any implementation: **canonical path-derived extension everywhere, a changed-torrents delta tombstone, file-facts-only v1 (defer mutable denorm), immutable read-only DuckDB generations, SQL-as-code lockdown (with `Interrupt()` not `statement_timeout`), and clearer provenance/space accounting.**
