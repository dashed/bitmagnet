# L2 — DuckDB-on-Parquet search + PG aggregate: Rust implementation spec

> ⚠️ **Revised 2026-06-09 by external review — read [`../feedback/feedback_1-response.md`](../feedback/feedback_1-response.md) alongside this.** Superseded points (fold-in in progress): (1) the **DROP-gate filter** should first try the deployed `torrents.file_extensions` JSONB (`@>` containment) — `agg_torrent_ext` is now _conditional_ (build only if JSONB plans are unstable or `ext∧max_size` is needed); (2) the base+delta view must anti-join a **`delta_changed_torrents` tombstone** key set, not delta file rows; (3) the DuckDB fact is **file-facts-only in v1** (`content_type`/`published_at` denorm goes stale vs the `updated_at` watermark); (4) the sidecar uses **immutable read-only generations** + hard caller deadlines backed by `Connection::Interrupt()` (there is **no `statement_timeout`**) + `ILIKE` escaping + the `enable_external_access=false` lockdown.

**Date:** 2026-06-08
**Status:** SPEC (design only — no code). Implements **L2** of the prove-then-retire plan ([[space-savings-vs-torrent-files]]): the per-file search layer that lets `torrent_files` eventually be dropped, deployed **in parallel** and **proven at parity** first.
**Language:** Rust (aligns with the `bitmagnet-rs` rewrite direction).
**Grounded in:** the `bitmagnet-rs` crate map + the Go file-search surface (both surveyed 2026-06-08) and the measured architecture ([`duckdb-parquet-parity-architecture.md`](./duckdb-parquet-parity-architecture.md), [`arch-c-parity-and-optimization-results.md`](./arch-c-parity-and-optimization-results.md), [`space-savings-vs-torrent-files.md`](./space-savings-vs-torrent-files.md)).

---

## 0. The constraint that shapes everything

> Per the deployment decision (2026-06-08): **do not drop `torrent_files` until each layer is deployed AND proven in production.** L2 must therefore run _beside_ the live `torrent_files` path, prove parity, and only then become primary. The DROP remains a separate, later decision. L2 is **additive and reversible** by design.

---

## 1. What L2 actually is (the split)

The Go survey shows the per-file read surface is **3 query shapes** — and they do **not** all belong to L2:

| shape                                                                                                                                                                 | current source                                                                                                                 | L2 disposition                                                                                                                             |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **(1) file-extension / file-type filter + facet** in the main content search — `EXISTS torrent_files … extension IN (…)` (`criteria_torrent_file_extension.go:24-34`) | **live `torrent_files` table read**                                                                                            | **L2a — PG `agg_torrent_ext`** (the real DROP-gate parity piece; stays in PG, the content search is PG/GIN and DROP-independent per EXP-A) |
| **(2) per-torrent file list/browser** — `SELECT * FROM torrent_files` (`search_torrent_files.go:17-29`)                                                               | already migrated → served from the `files_data` blob via `torrents.go AfterFind`; residual table SELECT just needs re-pointing | **L1 / Phase-A (G2)** — _not_ L2                                                                                                           |
| **(3) content-result file hydration** — preload `Files` has-many (`hydrator_torrent_content_torrent.go:52-64`)                                                        | blob `AfterFind` for migrated rows                                                                                             | **L1 / Phase-A** — _not_ L2                                                                                                                |
| **(NEW) cross-file search** — "find `.mkv` > 1 GB across all torrents", path substring, file-level analytics/collapse/ranges                                          | **does not exist today** (the EXISTS only answers "does this torrent have ext X")                                              | **L2b — DuckDB-on-Parquet sidecar** (additive capability; +12 GB vs the 273 GB a `torrent_files`-backed version would cost)                |

So:

> **L2 = L2a (PG `agg_torrent_ext`, restores shape-1, the DROP gate) + L2b (DuckDB-on-Parquet sidecar, the net-new cross-file search).**
> Shapes 2/3 are already L1 (blob); they are listed here only so the parity gate covers them.

This split matters: **the live-parity obligation is almost entirely L2a** (small PG rollup vs the `torrent_files` EXISTS). L2b is net-new, so it carries a _correctness_ gate (vs offline `torrent_files` ground truth) but no live-parity obligation.

---

## 2. Component specs

### 2a. PG `agg_torrent_ext` (L2a) — the DROP-gate parity piece

> **Detailed design: [`L2-P0-agg-torrent-ext-and-checker-spec.md`](./L2-P0-agg-torrent-ext-and-checker-spec.md).** Refinement from grounding: the file-type facet does **presence-`EXISTS`** (no `GROUP BY`/`COUNT(DISTINCT)`), so the gate needs only `(info_hash, extension)` — `file_count/total_size/max_size` below are **optional/deferred** (only for a future torrent-grain `ext∧size` collapse).

**Schema** (PG migration in the Go repo's migration dir — shared schema):

```sql
CREATE TABLE agg_torrent_ext (
  info_hash   bytea  NOT NULL,
  extension   text   NOT NULL,        -- '' = files with no extension
  file_count  int    NOT NULL,
  total_size  bigint NOT NULL,
  max_size    bigint NOT NULL,
  PRIMARY KEY (info_hash, extension)
);
-- serves: EXISTS(... extension IN (:exts)); per-(torrent,ext) counts for the facet.
```

- **Extension derivation = G1**: always `file_extension_from_path(path)` (`bitmagnet-model enums.rs:293`), **never** the blob's stored `e` (empty for crawl-path torrents). Matches the live PG generated column byte-for-byte. RUN-3 sizing: **+3–5 GB**, ~55M rows, 47,628 distinct extensions (an `ext→int4` dict is an optional further shrink).
- **The swap** (one-line, flag-gated, Go): in `criteria_torrent_file_extension.go` the multi-file branch's `EXISTS torrent_files` → `EXISTS agg_torrent_ext` (same predicate, same shape). The file-type facet (`facet_torrent_file_type.go`) rides the same criteria → restored for free.
- **Maintenance — MINUTE-FRESH via base + per-minute delta-upsert** (v1; no Go hot-path change):
  - The same minute-cadence **delta job** (§2b) carves recently-changed torrents (`torrents WHERE updated_at > :watermark`), decodes their blobs, and **upserts their agg rows with torrent-granular supersession**: `DELETE FROM agg_torrent_ext WHERE info_hash = ANY(:changed); INSERT … ` (re-crawl replaces a torrent's _whole_ per-ext set — matches EXP-B's TORRENT-granular rule). The delta is a minute of crawl (≪ 100k torrents) → sub-second upsert.
  - **Watermark** = max `updated_at` of the last successful delta run (covers both new _and_ re-crawled torrents, since `persist.go` advances `updated_at` on `DoUpdates`). **Requires `torrents_updated_at_info_hash_idx`**, source-owned by `migrations/00024_l1_l2_l3_follow_contract.sql`; homelab's `bitmagnet_pg_optimize.yml` remains an idempotent adoption overlay for already-deployed clusters.
  - **Real-time (roadmap, next increment): Rust processor/crawler dual-write** at persist time (sub-minute) — lands with the Rust processor (the IMPL-A4 "importer dual-write"), layered on top of the minute delta. On the roadmap, not indefinitely deferred; not a Go hot-path change.

### 2b. `bitmagnet-parquet` (new workspace crate) — export / refresh job

Productionizes the throwaway `bench/blob_export` (which already does blob→Parquet via `arrow`/`parquet`, ZSTD, dict, G1 extension, keyset order — `bench/blob_export/src/main.rs:109-173`). Adds:

- **Fact Parquet schema:** `info_hash, file_index, path, extension, size` **+ denorm** `content_type, published_at, created_at` (ARCH-F: 6/8 future queries become pure SQL with these; do NOT snapshot seeders/leechers — mutable, live-PG-join).
- **Sort by `(extension, size)`** → row-group min/max zone-map pruning (ARCH-C measured: collapse 1311→132 ms, exact-count 1024→**17 ms**; cost +6.4 GB as sorting decorrelates `info_hash` RLE). `row_group=1M`, ZSTD, **bloom OFF** (sorted → min/max already prunes).
- **Native rollup TABLES** built into a sidecar `.duckdb` file (ARCH-C: the `<50 ms` lever — GROUP BY 1425→**2.3 ms**, +2 GB): `agg_ext` (per-extension) and `agg_torrent_ext` (mirror of the PG table, for DuckDB-side collapse/facets).
- **MINUTE FRESHNESS via base + delta (EXP-B, v1):** two jobs, not one cadence —
  - **Delta job (every ~1 min):** carve `torrents WHERE updated_at > :watermark` → decode blobs → append a small **`delta.parquet`** (+ rebuild the tiny delta rollups) and upsert PG agg (§2a). EXP-B: delta-append is **sub-second** in prod (the processor already holds the new torrents+blobs; the 60–73 s the bench saw was an `ORDER BY created_at` sort artifact, not the append). Then ping the sidecar `Reload` (delta swap — cheap).
  - **Compaction job (periodic — daily or when the delta exceeds ~1 M torrents):** full-rebuild the **sorted base** Parquet + base rollups from all blobs (ARCH-A, ~3–5 min; EXP-B compaction ≈83 s for a 1 M-torrent delta), reset delta to empty, atomic **base swap**.
  - **Freshness SLA = the delta-flush interval** (1 min → ~1 min freshness, at <250 ms query cost up to a 100 k-torrent delta — EXP-B measured 141 ms base → 230 ms at +100 k).
- **Atomic swap:** versioned dirs `…/parquet/{base/v<ts>/…, delta/v<ts>/…}` + a `current` pointer; the sidecar swaps behind an `RwLock` (§2c). One streamer, two sinks (Parquet + PG agg).

### 2c. `bitmagnet-filesearch` (new workspace crate) — the DuckDB sidecar

- **DuckDB embed:** the `duckdb` crate with the **`bundled`** feature (statically compiles libduckdb — Rust is _not_ CGO/musl-constrained like the Go build, the exact reason a sidecar beats embedded go-duckdb; ARCH-B/D). Needs a C++ toolchain in the builder image.
- **Instance/connection model** (ARCH-C/D: `memory_limit`/`threads` are GLOBAL-per-instance → isolate): ONE persistent DuckDB instance, `memory_limit`/`threads` set once (bounded to the pod, e.g. 4 GB / 4 threads). duckdb-rs is sync → serve queries via a **bounded `spawn_blocking` worker pool + semaphore** + a **per-query statement timeout**.
- **base+delta query view (EXP-B anti-join — the supersession-correct shape):**
  ```sql
  CREATE VIEW files AS
    SELECT * FROM read_parquet('…/base/current/fact.parquet') b
      WHERE NOT EXISTS (SELECT 1 FROM read_parquet('…/delta/current/fact.parquet') d
                        WHERE d.info_hash = b.info_hash)
    UNION ALL
    SELECT * FROM read_parquet('…/delta/current/fact.parquet');
  ```
  🚨 **latest-wins is TORRENT-granular** (a re-crawl replaces a torrent's whole fileset; `files_data` is upsert-with-update, not pure-append). EXP-B proved a per-row `row_number() PARTITION BY info_hash = 1` is **WRONG** (keeps one _file_ per torrent → drops the rest) and window-max is **80× slower** (19 s vs 230 ms). The anti-join lets the base predicate prune via zonemaps and hash-anti-joins the tiny delta (`physical_hash_join.cpp:188`). Rollup queries hit base rollups + a tiny delta rollup, reconciled the same way.
- **Reload (two kinds):** a frequent **delta swap** (re-point the delta Parquet, ~per-minute, cheap) and a rare **base swap** (after compaction). Both swap the `Arc<Connection>`/view behind an `RwLock`, draining in-flight queries before dropping the old.
- **Safe SQL:** all RPC filters are **structured** → bound parameters (`?`), **never** string-interpolated. Path substring = `path ILIKE :q` with a bound `%q%`. (ARCH-C/EXP-D2: broad path-ILIKE is ~23 s unprunable → always paginate + statement-timeout; the ngram inverted index is the **L3** escalation, explicitly out of L2.)
- **Query patterns** (all proven in ARCH-C): `ext ∧ size` paginated find (35 ms), distinct-torrent collapse (rollup → 5–32 ms), size ranges/counts (zone-map → 17–132 ms), per-ext/type facets + size histogram (rollup → <3 ms), single-torrent hydrate (17 ms), path-FTS ILIKE (paginated).

### 2d. Proto — new `file_search.proto`, service `FileSearchService`

Kept **separate** from the torrent-grained `SearchService` (different grain, different engine, no existing file RPC — `bitmagnet-proto` survey). In `bitmagnet-rs/proto/bitmagnet/file_search.proto`, package `bitmagnet.v1`:

- `SearchFiles(SearchFilesRequest) → SearchFilesResponse` — filters: `extensions[]`, `file_types[]`, `size_min/size_max`, `path_query` (ILIKE substring), `content_types[]`, `published_from/to`; `keyset pagination`; `sort_by` (size|path|published); **`collapse_to_torrent: bool` (default true)**. Default (collapse) returns **one row per torrent** with `info_hash`, a matching-file `count`, and a small preview of matching files (served via the `agg_torrent_ext`/rollup path — ARCH-C 5–32 ms); `collapse_to_torrent=false` returns **file rows** `(info_hash, file_index, path, extension, size, content_type, published_at)`. Plus `total_count` (optional/estimate) + `has_next`.
- `CountFiles(...) → count` · `Facets(FacetsRequest) → {per_ext, per_type, size_histogram}` (rollup-served) · `Reload(version)` · `HealthCheck`.
- Generated via the existing `tonic-prost-build` pipeline (`bitmagnet-proto/build.rs:16-19`).

### 2e. Go integration — shadow client (Phase-4 style)

- Go gains a `FileSearchService` gRPC client + a config env (`BITMAGNET_FILESEARCH_ADDR=bitmagnet-filesearch.bitmagnet.svc:50052`), default **off**.
- **Shadow flag** for L2a: when set, the content-search file-extension criteria runs BOTH `EXISTS torrent_files` and `EXISTS agg_torrent_ext`, compares the info_hash sets + facet counts, emits a `filesearch_parity_mismatch` metric — the live half of the parity gate (§4).
- New cross-file search (L2b) is exposed via a new GraphQL query (e.g. `fileSearch`) wired to the sidecar — additive, no parity obligation.

---

## 3. Crate / workspace layout + deps

```
bitmagnet-rs/
  proto/bitmagnet/file_search.proto        # new — FileSearchService
  crates/
    bitmagnet-parquet/                      # new — export/refresh lib+bin (productionized blob_export)
    bitmagnet-filesearch/                   # new — DuckDB sidecar lib+bin
```

- New workspace deps (pin in workspace `Cargo.toml`): `duckdb` (feature `bundled`), `arrow`/`parquet` (promote from `bench/blob_export`). Reuse existing `tonic 0.14`, `prost 0.14`, `sqlx 0.9`, `zstd`, `rmp-serde`, `clap`, `tokio`, `bitmagnet-{db,model,common,proto}`.
- PG migration `agg_torrent_ext` → Go repo migration dir (shared schema). `bench/blob_export` is superseded by `bitmagnet-parquet` (can be retired or kept as a thin bench shim).

---

## 4. Prove-then-retire harness (the gate)

The DROP gate is **L2a (`agg_torrent_ext` vs `torrent_files` EXISTS)** — proven by **all-Rust invariant composition** (no Go request-path shadow; detail in [`L2-P0-…-checker-spec.md`](./L2-P0-agg-torrent-ext-and-checker-spec.md) §7):

1. **Job A — one-time, direct** (Rust `bitmagnet-parquet verify`): full `agg` vs `torrent_files` on the HEL1 restore/snapshot — proves the chain + G1 parity before flip.
2. **Job B — continuous, durable** (Rust): `agg` vs the blob it was built from; survives the DROP.
3. **existing blob ⟺ `torrent_files` consistency** closes the loop ⟹ `agg ⟺ torrent_files` in prod, transitively.

The cap-induced divergence is **structurally zero** (the blob mirrors `torrent_files` at all 3 write sites — checker §8), so any mismatch is a bug; require a sustained zero-mismatch window before flipping primary.

**L2b correctness gate** (no live parity, net-new): its `ext∧size` / collapse / range results validated against the equivalent `torrent_files` SQL on the offline restore — ARCH-C already produced these query pairs; fold them into a repeatable parity check.

Only after L2a is primary-and-proven **and** shapes 2/3 (blob, G2) are proven does `torrent_files` become DROP-eligible — a **separate** decision, still deferred.

---

## 5. Deployment (homelab, HEL1)

Model on the existing `roles/bitmagnet-search` (Tantivy sidecar) — a new role `roles/bitmagnet-filesearch`:

- **Node:** HEL1 (`kubernetes.io/hostname: alberto-hetzner`) — FSN1 is ~83% mem-committed (same rationale as the Tantivy sidecar).
- **PVC:** RWO `local-path`, **~50 Gi** (DuckDB tier ≈12 GB + headroom for the versioned swap — _not_ 200 Gi; that was the rejected file-index).
- **Deployment:** `bitmagnet-filesearch`, ClusterIP **:50052** (gRPC), `Recreate`, tcpSocket probes, securityContext 65532, bounded resources (e.g. req 250m/1Gi → lim 4000m/6Gi).
- **Two CronJobs** (replace the Tantivy one-shot backfill Job): a **delta refresh every ~1 min** (carve recent torrents → append delta Parquet + delta rollups + PG agg upsert → ping sidecar `Reload`) and a **compaction job** (daily or delta>~1 M torrents → full base rebuild + atomic base swap). Minute freshness, EXP-B-validated.
- **Image:** new `ghcr.io/dashed/bitmagnet-filesearch` (one image, two entrypoints: `filesearch` server + `parquet-refresh` job), built on FSN1 (the existing `bitmagnet-search-image-build` pattern). Needs a C++ toolchain in the builder for the DuckDB `bundled` build.
- **Go wiring:** `BITMAGNET_FILESEARCH_ADDR` env on the bitmagnet StatefulSet + a CiliumNetworkPolicy allowing bitmagnet → :50052 (mirrors the `bitmagnet_search_allow_bitmagnet_ingress` toggle), default off until shadow.
- **Make targets:** `bitmagnet-filesearch[-check|-status|-logs|-image-build|-refresh-run]`.

---

## 6. Risks & mitigations

| risk                                                 | mitigation                                                                                                                                                                                                               |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| DuckDB `bundled` build time/size in the Rust image   | pin libduckdb version, cache the build layer; consider a prebuilt lib if too slow                                                                                                                                        |
| `memory_limit`/`threads` global-per-instance         | one bounded instance + `spawn_blocking` worker pool + semaphore + statement timeout                                                                                                                                      |
| Atomic-swap under in-flight queries                  | versioned dir + `RwLock<Arc<Connection>>` + drain-then-drop                                                                                                                                                              |
| **supersession correctness (base+delta)**            | TORRENT-granular **anti-join** only (EXP-B); `row_number()=1` is WRONG (drops a torrent's other files), window-max 80× slower; agg delta = DELETE-then-INSERT per changed `info_hash`                                    |
| freshness staleness                                  | minute-fresh via the delta job (SLA = delta-flush interval); export a `filesearch_delta_age` metric; `torrent_files` is the live fallback during prove; real-time importer dual-write is a later sub-minute optimization |
| delta carve cost                                     | needs source-owned `torrents_updated_at_info_hash_idx`; EXP-B's 60–73 s was an `ORDER BY` sort artifact — prod floor is sub-second                                                                                       |
| empty blob `e` for crawl-path torrents               | **G1**: derive extension from PATH everywhere (already the blob_export behavior + the agg rule)                                                                                                                          |
| broad path-FTS ILIKE ~23 s (ARCH-C/EXP-D2)           | statement-timeout + mandatory pagination; ngram index is the **L3** escalation, not L2                                                                                                                                   |
| introducing DuckDB + a new sidecar = new ops surface | it _replaces_ the rejected 200 Gi Tantivy file-index plan (~10× smaller); reuses the proven sidecar deploy pattern                                                                                                       |

---

## 7. Phasing + task breakdown

- **L2-P0** — PG `agg_torrent_ext` migration + the **Rust** `verify` checker (Job A/B) + `bitmagnet-db` batch readers. _(L2-P0-1…5; detailed in the P0 spec.)_
- **L2-P1** — `bitmagnet-parquet` crate: base export (sort+denorm+rollups+atomic swap) **+ the minute delta job (delta Parquet + delta rollups + agg delta-upsert, anti-join supersession) + compaction job** + the source-owned follow-contract migration (`00024_l1_l2_l3_follow_contract.sql`). _(IMPL-B1/B2.)_
- **L2-P2** — `file_search.proto` + `bitmagnet-filesearch` crate (DuckDB sidecar, safe SQL builder, reload) + Go gRPC client. _(IMPL-B3/B4.)_
- **L2-P3** — homelab `bitmagnet-filesearch` role + CronJob + image (HEL1) + Go shadow wiring. _(IMPL-B5/B6.)_
- **L2-P4** — sustained zero-mismatch window (Job A + B) → flip the content-search filter to `agg_torrent_ext`; GA the `fileSearch` cross-file API. **DROP still deferred.**

All future — **no code in this change**; this document is the spec.

---

## 8. Decisions (all settled 2026-06-08)

1. **agg maintenance** — ✅ **minute-fresh via the delta-upsert job** in v1 (Rust, no Go hot-path change). **Real-time importer dual-write IS on the roadmap** (sub-minute) — it lands with the **Rust processor/crawler** (not a Go hot-path change), as the next increment on top of the minute delta job; not indefinitely deferred.
2. **Refresh cadence v1** — ✅ **minute freshness via base+delta** (EXP-B). §2a/§2b/§2c + the delta CronJob & compaction.
3. **Image** — ✅ **one image, three entrypoints** (server + delta-refresh + compaction), like the Tantivy sidecar.
4. **`fileSearch` default grain** — ✅ **collapse-to-torrent** (each result = a torrent with ≥1 matching file + a per-file count/preview; matches bitmagnet's torrent-centric model, avoids mega-torrent flooding). Individual file-rows mode also supported via `collapse_to_torrent=false`. So `collapse_to_torrent` **defaults to true**.
5. **L2 home** — ✅ **HEL1** (for now; consistent with the Tantivy sidecar + FSN1 saturation).
