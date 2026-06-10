# DV-2 — L2 stack build notes (`bitmagnet-parquet` + `bitmagnet-filesearch`)

**Author:** `dv2-l2` (team `bitmagnet-deploy`, task #2 — BUILD wave, local code only)
**Branch/bookmark:** `feat/l2-filesearch` (off `feat/file-grained-search`; **not** rebased onto the `rust_rewrite` git-chain — `feat/rust-infrastructure` is 8 commits behind `rust-rewrite-plan`, a known pending rebase, and this work stays independent of it).
**Date:** 2026-06-10
**Status:** Skeleton-with-critical-paths COMPLETE. `cargo build` + `cargo test` green on the default workspace (no DuckDB). The production DuckDB engine compiles behind a feature flag (see §2). Nothing deployed/applied.

> Implements **L2** of the prove-then-retire plan ([`L2-duckdb-parquet-search-rust-spec.md`](./L2-duckdb-parquet-search-rust-spec.md)). L2 runs *beside* the live `torrent_files` path, proves parity, and only then becomes primary; the `torrent_files` DROP stays deferred until every replacement layer is proven live (the standing sequencing constraint).

---

## 0. TL;DR — what was built

Two new workspace crates + one new proto + one new DB reader:

| Artifact | What it does | State |
|---|---|---|
| `crates/bitmagnet-parquet` | Productionized `bench/blob_export`: blobs → sorted `(ext,size)` slim **fact** Parquet + 2 **rollup** Parquets, **minute delta** (tombstone supersession incl. deletes), **compaction**, **atomic generation swap**. Lib + `bitmagnet-parquet` CLI. | ✅ built + tested |
| `crates/bitmagnet-filesearch` | DuckDB-on-Parquet **gRPC sidecar** (`FileSearchService`): immutable read-only generations, FB-B1d safe-SQL, CB concurrency config, per-query deadlines. Lib + `bitmagnet-filesearch` server. | ✅ logic built + tested; real DuckDB engine behind `--features duckdb-engine` |
| `proto/bitmagnet/file_search.proto` | New `FileSearchService` (kept separate from torrent-grained `SearchService`). | ✅ wired into `bitmagnet-proto` |
| `bitmagnet_db::stream_changed_torrents` | The delta carve reader (`updated_at > watermark`, keyset on `info_hash`). | ✅ built + tested |

**Test counts (default build):** bitmagnet-parquet 17, bitmagnet-filesearch 11, bitmagnet-db 24 (2 DB-gated ignored), bitmagnet-proto 2 — all green. The full DuckDB end-to-end (build a real generation, query it through `DuckEngine`) runs under `--features duckdb-engine` (duck.rs `tests`).

---

## 1. Architecture & data flow

```
                      bitmagnet-parquet (export/refresh CLI + CronJobs)
 torrents.files_data ──► decode (G1 ext-from-path) ──► Sinks ─┬─► fact.parquet  (sorted ext,size; rg 1M; ZSTD; bloom OFF)
   (blob, keyset read)         │ decode errors counted (V3)   ├─► agg_ext.parquet         (per-ext rollup)
                               │                               ├─► agg_torrent_ext.parquet (per-(torrent,ext) rollup; PG-table mirror)
                               │                               └─► tombstones.parquet      (delta only: changed+deleted info_hash)
                               ▼
            generation tree:  <root>/{base,delta}/v<ts>/…  +  current symlink (atomic rename swap)  +  watermark
                               │
                               ▼
                      bitmagnet-filesearch (DuckDB sidecar, gRPC :50052)
   GenerationManager (RwLock<Arc<LoadedGeneration>>, reload-on-swap)
   Engine: base+delta anti-join VIEW over read_parquet(); rollups for collapse/facets/count
   service: proto → domain query → SafeQuery → DuckDB; semaphore + spawn_blocking + interrupt deadline
```

### Module map

`bitmagnet-parquet`:
- `decode` — `FileRow`, **G1** extension-from-path (never the blob `e`), `DecodeStats` (V3 error count).
- `schema` — Arrow schemas: fact `(info_hash, file_index, path, extension, size)`, `agg_ext`, `agg_torrent_ext`.
- `fact` — streaming `FactWriter`; `SortMode::{None, InMemory}`; ZSTD, **row_group 1M, bloom OFF** (sorted ⇒ zone-map prunes).
- `rollup` — `AggExt` (in-memory map, ~47k ext) + `AggTorrentExt` (streamed one torrent at a time, bounded memory).
- `generation` — `Layout`: versioned dirs, **atomic `current` symlink swap** (`rename(2)`), watermark read/write.
- `delta` — `TombstoneWriter` (deduped info_hash key set, incl. deletes — FB-B1a).
- `export` — `Sinks` fan-out (fact + both rollups + tombstone); `run_base` / `run_delta` / `run_compaction` async jobs.

`bitmagnet-filesearch`:
- `query` — validated, engine-agnostic intent (`Filters`/`Sort`/`FileQuery`/`CountQuery`); limit clamps.
- `sql` — **FB-B1d safe SQL**: server-controlled paths/identifiers vs bound `?` params; `escape_like`; the base+delta **anti-join** `files`/`att` CTEs; rollup-served collapse/facet/count.
- `generation` — `GenerationManager`: resolve current base+delta, `reload()` swap behind `RwLock<Arc<…>>`.
- `engine` — `Engine` trait + `InMemoryEngine` (reference/tests) + `DuckEngine` (feature `duckdb-engine`).
- `service` — gRPC `FileSearchService`: proto mapping, **semaphore + `spawn_blocking`** concurrency, per-query deadline.

---

## 2. The DuckDB feature-flag decision (read this)

The `duckdb` crate with `bundled` statically compiles libduckdb (a large C++ amalgamation, needs a C++ toolchain). To keep the BUILD-wave gate (`cargo build` + `cargo test`) **fast and offline**, the real engine is gated behind the crate feature **`duckdb-engine` (OFF by default)**:

- **Default build** compiles `sql` + `query` + `generation` + `service` + `InMemoryEngine` and runs the whole service end-to-end against the in-memory engine. No DuckDB, no C++ toolchain, no network.
- **Production image** builds `cargo build -p bitmagnet-filesearch --release --features duckdb-engine`. The `DuckEngine` (`engine/duck.rs`) is written to the duckdb-rs 1.3 API; the `duck::tests` module builds a real generation via `bitmagnet-parquet` and asserts the SafeQuery SQL runs on actual DuckDB.
- The default `bitmagnet-filesearch` binary still builds/starts; with no engine it **bails at startup** with a clear message pointing here (so an accidental non-feature image fails loud, not silent).

> This is the one deliberate structural stub. Everything security- and correctness-critical (SQL construction, ILIKE escaping, parameterization, the anti-join shape, concurrency, proto mapping, generation swap) is in the **default** build and tested there.

---

## 3. Key design points (grounded in the campaign results)

- **G1 everywhere** — extension is `file_extension_from_path(path)`, never the blob `e` (empty for crawl-path torrents). Matches the live PG generated column byte-for-byte.
- **Sorted fact, bloom OFF** — `(extension, size)` order ⇒ Parquet row-group min/max zone-maps prune ranges/counts (ARCH-C: exact-count 1024→17 ms); a bloom would be dead weight on a sorted file.
- **Rollups are Parquet, not native DuckDB tables** — the CB campaign found serving a native DuckDB table 100–1000× slower; rollup Parquet is the `<3 ms` facet/collapse/group-by lever.
- **base+delta = TORRENT-granular anti-join** (`sql::files_cte`): `NOT EXISTS (tombstone) UNION ALL delta`. EXP-B proved `row_number() PARTITION BY info_hash = 1` is WRONG (keeps one file/torrent) and window-max is 80× slower. A **delete** is a tombstone with no delta fact rows ⇒ the torrent vanishes.
- **FB-B1d safe SQL** — no user value is ever interpolated; extensions/sizes/path become bound `?` params; the path substring is `ILIKE`-escaped (`%`/`_`/`\`) + `ESCAPE '\'`. `read_parquet` paths are server-controlled generation paths only.
- **FB-B1d external-access tension, resolved explicitly** (`engine/duck.rs` header): `enable_external_access=false` would block `read_parquet` (the engine's whole job), so it stays ON; the surface is constrained instead — server-only paths, bound params, `autoload/autoinstall_known_extensions=false`, and `lock_configuration=true` set **last** so a query can't re-open anything.
- **CB serving config** — ONE DuckDB instance + a cloned-connection cursor pool; per-query `threads≈4`; a tokio **semaphore (default 6, the measured knee)**; heavy `COUNT(DISTINCT)`/collapse routed through the rollups; object cache ON (warm). DuckDB has **no `statement_timeout`** → each query arms an **interrupt watchdog** (`InterruptHandle`) for the deadline.

---

## 4. CLI surface

`bitmagnet-parquet` (env `BITMAGNET_PARQUET_ROOT`, `BITMAGNET_POSTGRES_DSN`):
- `base    --sort memory --fail-on-decode-error` — full export; **V3** run (prints decode-error count; non-zero exit on any error).
- `delta   --watermark <epoch> --deleted-file <f>` — minute carve + tombstone + watermark advance + swap.
- `compact` — full rebuild + empty-delta reset.
- `from-hex --input <psv>` — OFFLINE smoke (no DB), the CI/local path.
- `verify` — **STUB** (agg-vs-`torrent_files` parity, Job A/B; see §5 + L2-P0 spec §7).

`bitmagnet-filesearch` (env `BITMAGNET_FILESEARCH_ADDR=:50052`, `BITMAGNET_PARQUET_ROOT`, concurrency/deadline/threads/memory).

---

## 5. What's STUBBED (explicit list)

> **2026-06-10 update (L2-D3):** stubs **2** and **5** are CLOSED, and a serving
> bug was fixed alongside — `DuckEngine` routed collapse/facet/count through the
> rollup unconditionally, silently dropping `path_query` and mis-handling size
> bounds (`sql::rollup_plan` + fact-path builders + per-group hydration fix it;
> **`l2-1` images carry the bug, deploy `l2-2`+**). See
> [`l2-verify-and-shadow-runbook.md`](./l2-verify-and-shadow-runbook.md).

1. **`DuckEngine` not compiled by the default gate** — behind `--features duckdb-engine` (§2). Code written + a gated e2e test; the heavy bundled compile is the production image's job.
2. ~~**`bitmagnet-parquet verify`** — a CLI stub.~~ ✅ **BUILT (2026-06-10):** Job A implemented — expected agg recomputed **from the blob** (the actual post-DROP source; the L2-P0 PG `agg_torrent_ext` table was superseded by the JSONB gate), compared against `torrent_files GROUP BY info_hash, extension` via the new `bitmagnet_db::batch_torrent_files_ext_agg` batched reader. `--mode full|sample --after <hex> --batch-size`; exit ≠ 0 on any mismatch/decode error. Pure compare fn unit-tested.
3. **Full-scale `(extension,size)` global sort** — `FactWriter::SortMode::InMemory` sorts bounded inputs (delta, tests, `--limit`). The full ~856 M-row base needs a spilling external sort; the deploy compaction job runs the sort in DuckDB (`COPY (SELECT * FROM read_parquet(...) ORDER BY extension, size) TO …`) — a one-line post-pass, noted but not wired into the streaming writer. (The first prod export therefore runs `--sort none` — the unsorted slim base; queries stay correct, row-group pruning arrives with the sort.)
4. **Keyset pagination resumption** — the first page is fully implemented (filter/sort/limit/`has_next` via overfetch/collapse/preview/count/facets). `next_cursor` is returned as an (empty) token; **resuming** a deep page (applying the cursor predicate) is a follow-up: add an `after` keyset predicate to `sql::build_search_files` and thread the opaque cursor through the service.
5. ~~**Deletion audit source** — a deploy-time input.~~ ✅ **BUILT (2026-06-10):** `deleted_torrents` audit table + `AFTER DELETE` trigger (DDL = homelab playbook `bitmagnet_deleted_audit.yml`; deliberately not a goose migration — image digest-pinned, `00023` contested), `bitmagnet_db::read_deleted_torrents` window reader, and `delta --deleted-source none|file|audit` consuming the same half-open lagged carve window.
6. **`content_type`/`published_at` denorm columns** — deferred per the L2 spec revision (they go stale vs the `updated_at` watermark); v1 fact is file-facts only. The proto reserves `content_types` for v2.
7. **PG `agg_torrent_ext` migration + the Go shadow client** — these are the **DV-4 (Go-side)** deliverable, not this crate. The DuckDB-side `agg_torrent_ext` rollup (parity mirror) IS built here.

---

## 6. V2 — dual-read shadow harness (design)

**Goal:** prove the L2b cross-file search returns the *same set* as the equivalent live `torrent_files` SQL, at acceptable latency, before anything flips primary.

**Shape (offline-first, then optional live shadow):**
1. **Query pairs.** For each L2b shape (`ext∧size` paginated find, distinct-torrent collapse, size ranges/counts, per-ext facet, single-torrent hydrate, path-ILIKE), define the equivalent `torrent_files` SQL (ARCH-C already produced these pairs) and the `FileSearchService` request.
2. **Source of truth.** Run the `torrent_files` SQL against the live PG (read-only) **and** the `FileSearchService` against the sidecar reading a generation exported from the *same* snapshot.
3. **Compare.** For each pair: assert the **info_hash set** (collapse) or **(info_hash, file_index) set** (file rows) is identical; record the latency of each side. Tolerate ordering (compare as sets), and for `total_count` allow the documented estimate path.
4. **Window.** Run the pair-suite over a sweep of realistic filters for a sustained window; emit a `filesearch_parity_mismatch` counter + a latency histogram per shape. Require a sustained **zero-mismatch** window before considering L2b GA.

**Runbook outline (gated, read-only — no prod mutation):**
```
# on the HEL1 restore (throwaway PG) — same snapshot the generation is exported from
bitmagnet-parquet base --dsn <restore-dsn> --root /scratch/gen --sort memory --fail-on-decode-error   # V3
bitmagnet-filesearch --root /scratch/gen --addr 127.0.0.1:50052 &                                       # sidecar
v2-shadow --pg <restore-dsn> --sidecar 127.0.0.1:50052 --pairs v2_pairs.json --window 30m --csv v2.csv  # (harness — to build)
#   each row: shape, filter, pg_set_size, sidecar_set_size, set_equal, pg_ms, sidecar_ms
# GATE: 0 mismatches across the suite; sidecar latency within the CB envelope (<250ms structured).
```
~~The `v2-shadow` driver itself is **not** built here (it's a deploy-wave harness); the sidecar side it drives **is**.~~ ✅ **BUILT (2026-06-10):** the `bitmagnet-shadow` workspace crate (bin `v2-shadow`) implements exactly this — five shapes, exact comparison (ordered rows/groups, counts incl. the `estimated` flag failing the gate, facet maps), `COLLATE "C"`/hex/ILIKE-escape mirror rules, CSV + non-zero exit on mismatch, a built-in suite covering every sidecar routing class plus `--pairs` JSON. See [`l2-verify-and-shadow-runbook.md`](./l2-verify-and-shadow-runbook.md). The Go in-request shadow for L2a is moot — the JSONB gate already flipped with direct SQL parity proven.

---

## 7. V3 — first production base export = the 0-errors validation

The first full `bitmagnet-parquet base` over the real corpus **is** the "0 decode errors across all ~16.97 M with-blob torrents" validation. The tool makes this first-class:
- `DecodeStats.decode_errors` is counted per torrent (a bad blob is counted, never fatal — the export still completes and reports).
- `BuildStats::is_clean()` ⇔ `decode_errors == 0`.
- `base --fail-on-decode-error` exits non-zero if any blob failed ⇒ a CI/CronJob gate.
- `report()` prints `torrents_ok / decode_errors / file_rows / agg_ext / agg_torrent_ext / clean`.

Run: `bitmagnet-parquet base --dsn <dsn> --root <root> --fail-on-decode-error` → expect `decode_errors=0 clean=true`. (Cross-check `file_rows` against the known **856.79 M** torrent_files rows and `torrents_ok` against **16,992,238** with-blob torrents.)

---

## 8. Homelab deploy deltas (DESCRIBED — for the deploy wave, not applied)

Model on the existing `roles/bitmagnet-search` (Tantivy sidecar). New role **`roles/bitmagnet-filesearch`**:

- **Node:** HEL1 (`kubernetes.io/hostname: alberto-hetzner`) — FSN1 is ~83% mem-committed.
- **PVC:** RWO `local-path`, **~50 Gi** (DuckDB tier ≈12 GB + headroom for the versioned generation swap; NOT the rejected 200 Gi file-index).
- **Image:** new `ghcr.io/dashed/bitmagnet-filesearch` (one image, **three entrypoints**: `filesearch` server + `parquet-refresh` delta + `parquet-compact`), built on FSN1 (mirror `bitmagnet-search-image-build`). **The builder needs a C++ toolchain** for the DuckDB `bundled` build, and the image must build `--features duckdb-engine`. Pin the libduckdb version; cache the build layer.
- **Deployment:** `bitmagnet-filesearch`, ClusterIP **:50052** (gRPC), `Recreate`, tcpSocket probes on 50052, securityContext 65532, bounded resources (req 250m/1Gi → lim 4000m/6Gi; matches `threads≈4` + 4 GB memory_limit + headroom).
- **Generation PVC mount:** the sidecar mounts the generation root read-only-ish at `BITMAGNET_PARQUET_ROOT=/var/lib/bitmagnet/parquet`; the refresh/compact CronJobs mount it read-write. (Both need the SAME PVC — single-writer/many-reader; the sidecar only reads.)
- **Two CronJobs** (replace the Tantivy one-shot backfill Job):
  - **delta refresh** every ~1 min → `bitmagnet-parquet delta` (carve `updated_at>watermark` + deleted list → delta gen + tombstones + watermark advance) → then call the sidecar `Reload` RPC (cheap delta swap). Export `filesearch_delta_age` from `HealthCheck.delta_age_seconds`.
  - **compaction** daily or when the delta exceeds ~1 M torrents → `bitmagnet-parquet compact` (full base rebuild + empty-delta reset + atomic base swap) → sidecar `Reload`.
  - The first base export is a one-shot **Job** (the V3 run) before the sidecar is scaled up.
- **PG index prereq:** an index on `torrents.updated_at` (or `created_at`) for the delta carve — confirm/add (a Go-repo migration; coordinate with DV-4).
- **Go wiring (DV-4):** `BITMAGNET_FILESEARCH_ADDR=bitmagnet-filesearch.bitmagnet.svc:50052` on the bitmagnet StatefulSet + a CiliumNetworkPolicy allowing bitmagnet → :50052 (mirror `bitmagnet_search_allow_bitmagnet_ingress`), default off until shadow.
- **Make targets** (mirror the Tantivy ones): `bitmagnet-filesearch[-check|-status|-logs|-image-build]`, `bitmagnet-parquet-base-run`, `bitmagnet-parquet-delta-run`, `bitmagnet-parquet-compact-run`.

---

## 9. Build / test commands

```bash
cd bitmagnet-rs
cargo build                                                   # default workspace (no DuckDB)
cargo test  -p bitmagnet-parquet -p bitmagnet-filesearch \
            -p bitmagnet-db -p bitmagnet-proto                # all green
cargo build -p bitmagnet-filesearch --features duckdb-engine  # production engine (needs C++ toolchain)
cargo test  -p bitmagnet-filesearch --features duckdb-engine  # DuckDB end-to-end round-trip

# offline smoke of the export pipeline (no DB):
cargo run -p bitmagnet-parquet --bin bitmagnet-parquet -- --root /tmp/gen from-hex --input sample.psv
```

> The workspace `arrow`/`parquet` are pinned to **55.x** (not the bench's 53.x): arrow 53's `arrow-arith` fails to compile against chrono ≥0.4.45 (a `quarter` method-ambiguity fixed in arrow 54+).
