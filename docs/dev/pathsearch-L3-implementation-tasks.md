# L3 pathsearch implementation tasks

**Date:** 2026-06-12 · **updated 2026-06-15**
**Status:** ✅ **LIVE IN PROD.** The gate-7 L3 path-search route is deployed on the user-facing
URL via a "serve-split" read pod (homelab repo; image `gate7-9` = v13 `76201662`), parity-proven
(recall 1.0 / precision 100%). The boxes below were authored before that deploy and are now stale —
checked off with notes. Deploy/runbook live in the **homelab** repo
(`docs/bitmagnet/gate7-l3-LIVE-status-and-roadmap.md` + `gate7-7-l3-serve-split-runbook.md`), not here.

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
      the existing `SearchService` smoke test. _(Implemented in `e0d37b43`:
      `tests/pathsearch_smoke.rs` — boots `PathSearchServer` on an ephemeral port,
      exercises `HealthCheck`+`PathCandidates`.)_
- [x] Add a live-Postgres ignored test/smoke for
      `bitmagnet-pathsearch-backfill --limit`. _(Implemented in `e0d37b43`:
      `#[ignore]` `capped_backfill_indexes_path_bag_documents` — asserts docs>0 and
      the partial `--limit` run seeds no watermark.)_
- [x] Add a follow-loop smoke against live PG or a small fixture DB to prove
      watermark advancement, changed upserts, and deleted-torrent tombstones.
      _(Implemented in `e0d37b43`: `#[ignore]` `follow_window_processes_a_recent_window`
      plus deterministic in-RAM `apply*changed_row*_`/`live*tombstones*_`units for
supersession + tombstones. Extended on`feat/l3-pathsearch-v3`with`#[ignore]`
`follow_watermark_file_advances_monotonically_across_ticks` — an end-to-end
      watermark-FILE round-trip proving the carve origin advances across two ticks
      and never re-carves from the old origin, guarding the L2 l2-7→l2-8 class.)_
- [x] **DECIDED + FROZEN (A2, 2026-06-13): seeders stay backend-hydrated; the
      L3 index indexes `seeders = 0`.** Rationale: L3 is a candidate engine that
      oversamples and hands `info_hash` to the backend, which already holds fresh
      swarm seeders for ranking/hydration; seeders are mutable swarm state that must
      not be snapshotted into a content-change-triggered follow index (staleness +
      per-swarm-tick re-upsert write-amplification). The `seeders` FAST field is
      **retained** in the Tantivy schema, so future denormalization needs only a
      stream-SQL JOIN + rebuild — no proto/schema/API change. Reversible; not a
      product fork.
- [x] Add candidate exact-refine integration in the Go backend:
      L3 `info_hash` candidates -> L1/L2 exact substring/structured filters. _(DONE = the gate-7
      route in `internal/search/pathsearch/composer.go` `TorrentContent`: L3 candidates → PG requery →
      **L1 blob** decode → exact-refine (substring/ext/size) → paginate. Note: the live route refines
      against **L1 blobs in-process**, NOT the L2 DuckDB sidecar; the L2 `fileSearch` Go consumer is a
      separate open thread, see `dv4-go-integration-notes.md`.)_
- [x] Add feature flags and UI/typeahead/backend routing switches:
      `SEARCH_PATHSEARCH_ENABLED`, `SEARCH_PATH_TYPEAHEAD_ENABLED`,
      `SEARCH_PATH_COLLAPSE_L3_ENABLED`. _(DONE — set on the live read pod as
      `SEARCH_PATHSEARCH_ENABLED` / `SEARCH_PATH_TYPEAHEAD_ENABLED` / `SEARCH_PATH_COLLAPSE_ENABLED`.)_

### Homelab / deploy

> **✅ ALL DONE (2026-06-15)** — the `bitmagnet-pathsearch` sidecar is deployed/serving on HEL1
> (`:50053`, full index, follow loop ticking), and the gate-7 Go route is live on the user URL via
> the serve-split read pod. Lives in the **homelab** repo (roles `bitmagnet-pathsearch` +
> `bitmagnet-l3-serve`); see `docs/bitmagnet/gate7-l3-LIVE-status-and-roadmap.md`.

- [x] Add `bitmagnet-pathsearch` image-build target.
- [x] Pin pathsearch image digest in homelab inventory.
- [x] Add HEL1 `bitmagnet-pathsearch` role/manifests: PVC, Deployment, Service,
      Cilium policies, backfill Job, status/log targets.
- [x] Verify HEL1 `kubernetes.io/hostname` before creating the local-path PVC.
- [x] Deploy empty sidecar with backend flags off.
- [x] Run limited backfill smoke and compare projected size to the 14.0 GiB
      keyed baseline.
- [x] Run full backfill.
- [x] Prove production gates: readiness, doc count, index size, freshness,
      candidate recall, exact-refine parity, broad-query tail, and stability.
      _(Gates 5/6/8 PASS per `l3-gate5-6-verdict.md`; exact-refine parity proven by the gate-7 run.)_

## Verification so far

```text
cargo check -p bitmagnet-search
cargo test -p bitmagnet-search
cargo test -p bitmagnet-db
cargo check -p bitmagnet-parquet
```

All passed locally on 2026-06-12.
