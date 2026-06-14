# L3 pathsearch implementation tasks

**Date:** 2026-06-12
**Status:** fork-side vertical slice started. No production image/deploy yet.

This task ledger follows the reconciled deploy plan in
[`pathsearch-T4-deploy-ops.md`](./pathsearch-T4-deploy-ops.md). The active L3
shape is the keyed per-torrent path-bag index, not the retired per-file index.

## Task list

### Fork code

- [x] Add `bitmagnet.v1.PathSearchService` proto with torrent-candidate
  `PathCandidates` and path-specific health response.
- [x] Wire `path_search.proto` into `bitmagnet-proto` generation and re-exports.
- [x] Add production path-bag schema: `path` char-ngram(2,3) `WithFreqs`,
  stored/indexed `info_hash`, and fast metadata fields.
- [x] Add path-bag document builder from `TorrentWithBlob`/`files_data`.
- [x] Add pathsearch upsert/delete keyed by `info_hash`.
- [x] Add ngram conjunction query path with min-length guard and candidate
  collection.
- [x] Add `PathSearchServer` with `PathCandidates` and `HealthCheck`.
- [x] Add `bitmagnet-pathsearch-backfill` binary for full blob-sourced backfill.
- [x] Add `bitmagnet-pathsearch` binary with optional PG-tail follow loop.
- [x] Update `Dockerfile.search` to carry pathsearch server/backfill binaries.
- [x] Add focused unit tests for schema, blob path-bag construction,
  upsert/delete, query, and service behavior.

### Fork code still open

- [x] **DECIDED + FROZEN (A2, 2026-06-13): `PathSearchService.HealthCheck`
  keeps reusing the shared empty `HealthCheckRequest`.** Rationale: it is an
  empty marker with zero divergence surface; `path_search.proto` already imports
  `search.proto` for the generic `SortBy` building block so reuse adds no new
  dependency; no Go client exists yet, so a later switch to a dedicated
  `PathSearchHealthCheckRequest` is a pure mechanical rename with no consumer
  lock-in. Matches the `SearchService` precedent. The `PathSearchHealth` response
  is already path-specific and uniquely named, avoiding the single
  `bitmagnet.v1`-module name collision. Reversible; not a product fork.
- [x] Add a gRPC smoke test for `PathSearchServiceClient` over TCP, mirroring
  the existing `SearchService` smoke test. *(Implemented in `e0d37b43`:
  `tests/pathsearch_smoke.rs` — boots `PathSearchServer` on an ephemeral port,
  exercises `HealthCheck`+`PathCandidates`.)*
- [x] Add a live-Postgres ignored test/smoke for
  `bitmagnet-pathsearch-backfill --limit`. *(Implemented in `e0d37b43`:
  `#[ignore]` `capped_backfill_indexes_path_bag_documents` — asserts docs>0 and
  the partial `--limit` run seeds no watermark.)*
- [x] Add a follow-loop smoke against live PG or a small fixture DB to prove
  watermark advancement, changed upserts, and deleted-torrent tombstones.
  *(Implemented in `e0d37b43`: `#[ignore]` `follow_window_processes_a_recent_window`
  plus deterministic in-RAM `apply_changed_row_*` / `live_tombstones_*` units for
  supersession + tombstones. Extended on `feat/l3-pathsearch-v3` with `#[ignore]`
  `follow_watermark_file_advances_monotonically_across_ticks` — an end-to-end
  watermark-FILE round-trip proving the carve origin advances across two ticks
  and never re-carves from the old origin, guarding the L2 l2-7→l2-8 class.)*
- [x] **DECIDED + FROZEN (A2, 2026-06-13): seeders stay backend-hydrated; the
  L3 index indexes `seeders = 0`.** Rationale: L3 is a candidate engine that
  oversamples and hands `info_hash` to the backend, which already holds fresh
  swarm seeders for ranking/hydration; seeders are mutable swarm state that must
  not be snapshotted into a content-change-triggered follow index (staleness +
  per-swarm-tick re-upsert write-amplification). The `seeders` FAST field is
  **retained** in the Tantivy schema, so future denormalization needs only a
  stream-SQL JOIN + rebuild — no proto/schema/API change. Reversible; not a
  product fork.
- [ ] Add candidate exact-refine integration in the Go backend:
  L3 `info_hash` candidates -> L1/L2 exact substring/structured filters.
- [ ] Add feature flags and UI/typeahead/backend routing switches:
  `SEARCH_PATHSEARCH_ENABLED`, `SEARCH_PATH_TYPEAHEAD_ENABLED`,
  `SEARCH_PATH_COLLAPSE_L3_ENABLED`.

### Homelab / deploy

- [ ] Add `bitmagnet-pathsearch` image-build target.
- [ ] Pin pathsearch image digest in homelab inventory.
- [ ] Add HEL1 `bitmagnet-pathsearch` role/manifests: PVC, Deployment, Service,
  Cilium policies, backfill Job, status/log targets.
- [ ] Verify HEL1 `kubernetes.io/hostname` before creating the local-path PVC.
- [ ] Deploy empty sidecar with backend flags off.
- [ ] Run limited backfill smoke and compare projected size to the 14.0 GiB
  keyed baseline.
- [ ] Run full backfill.
- [ ] Prove production gates: readiness, doc count, index size, freshness,
  candidate recall, exact-refine parity, broad-query tail, and stability.

## Verification so far

```text
cargo check -p bitmagnet-search
cargo test -p bitmagnet-search
cargo test -p bitmagnet-db
cargo check -p bitmagnet-parquet
```

All passed locally on 2026-06-12.
