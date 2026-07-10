# 02 — Existing Rust Estate Inventory

Inventory of the Rust workspace already living in the fork at `bitmagnet-rs/`,
assessed as the foundation a full Rust rewrite of bitmagnet would grow from.
All paths are relative to the repo root (`bitmagnet-rs/…`) unless noted.

Branch inspected: `rust-rewrite-plan-20260710` (worktree of
`/Users/me/aaa/github/bitmagnet`). No code changed.

---

## 0. TL;DR

- **8 crates, ~30k lines of Rust src**, `resolver = "2"` workspace, edition
  2021, toolchain pinned (`rust-toolchain.toml`), rustc/cargo 1.95.
- **~290 test functions**, `cargo test --workspace` green offline; a clean
  `#[ignore]`-gated live-PG pattern for the DB integration tier.
- **The entire search read-path is already Rust** and **LIVE in prod**: main
  Tantivy search + follow loop, L2 DuckDB-on-Parquet file search, L3 Tantivy
  pathsearch, plus the export/refresh pipeline and a dual-read shadow gate.
- **What remains in Go is the bulk of the app**: DHT crawler, processor/
  classifier, the GraphQL/API + webui backend, the queue, and — critically —
  **schema ownership** (goose migrations, generated columns). Rust is a set of
  read-side satellites hanging off a Go-owned PostgreSQL.
- **Strong patterns already standardized**: tonic 0.14 gRPC, tokio `full`,
  `thiserror` (libs) + `anyhow` (bins), `clap` derive w/ `env`, `tracing`,
  centrally-pinned workspace deps, UDS-or-TCP listener, `serve_with_shutdown`.
- **Biggest gaps**: **zero metrics/instrumentation**, **no shared config or
  server-bootstrap framework** (each of 8 bins re-derives its own `clap` struct
  and wiring, incl. per-bin copies of the SIGINT+SIGTERM shutdown select),
  **no grpc.health.v1 except on the main-search bin**, and the **migrations are
  Go-owned** so Rust cannot stand up its own schema.

---

## 1. Crate-by-crate inventory

Workspace members (`Cargo.toml`): `bitmagnet-proto`, `bitmagnet-common`,
`bitmagnet-model`, `bitmagnet-db`, `bitmagnet-search`, `bitmagnet-parquet`,
`bitmagnet-filesearch`, `bitmagnet-shadow`. Dependency layering (leaf → root):

```
common ─┐
model ──┼─→ db ─→ parquet ─┐
proto ──┘                  ├─→ filesearch
        └─→ search         └─→ shadow
```

| Crate | src LOC | Bins | Tests* | Role |
|-------|--------:|------|-------:|------|
| bitmagnet-common | 71 | — | 1 | Shared `Error`, `Config`, `init_tracing` |
| bitmagnet-proto | 71 + gen | — | 2 | tonic/prost codegen from `proto/` |
| bitmagnet-model | 1,101 | — | 24 | Domain types + ZSTD/MessagePack blob codec |
| bitmagnet-db | 1,253 | — | 20 | SQLx PG access layer (read-side) |
| bitmagnet-parquet | 5,211 | 1 | ~64 | L2 export/refresh/compaction pipeline |
| bitmagnet-filesearch | 4,737 | 2 | ~58 | L2b DuckDB-on-Parquet file-search sidecar |
| bitmagnet-search | 16,553 | 4 | ~110 | Main Tantivy search + follow loop + L3 pathsearch |
| bitmagnet-shadow | 1,007 | 1 | 9 | V2 dual-read parity gate (L2 DROP gate) |
| **Total** | **~30,000** | **8** | **~290** | |

\*Test counts are `#[test]`/`#[tokio::test]` attribute counts across `src/` and
`tests/`; `cargo test --workspace` reports ~290.

Centrally-pinned key deps (`[workspace.dependencies]`), verified against
crates.io 2026-05-28: **tonic/tonic-prost/tonic-health/prost 0.14**,
**tokio 1 (`full`)**, **tantivy 0.26**, **sqlx 0.9** (`postgres, runtime-tokio,
tls-rustls, macros, json`, `default-features=false`), **arrow/parquet 55**,
**duckdb 1.3** (`bundled, parquet`), **thiserror 2 / anyhow 1**, **clap 4**
(`derive, env`), tracing 0.1, rmp-serde 1, zstd 0.13, futures 0.3.

### bitmagnet-common (71 LOC)
`src/lib.rs` only. The shared substrate: a 3-variant `Error` enum
(`Config`/`Io`/`Other`) + `Result` alias, a tiny runtime `Config`
(`listen_addr`, `log_level` — **barely used**; the real config lives in each
bin's clap struct), and `init_tracing()` (env-filter `fmt` subscriber, `info`
fallback). This is the seed of a shared framework but is currently anemic.

### bitmagnet-proto (71 LOC + generated)
`build.rs` compiles four protos via `tonic_prost_build` (server+client) into a
single `bitmagnet.v1` module (all protos share the package). `lib.rs` re-exports
the three service client/server pairs and the shared enums, and — notably —
**locks enum discriminants to the Go definitions with unit tests**
(`content_type_values_match_go`, `file_type_values_match_go`): the wire contract
guardrail between Go and Rust.

### bitmagnet-model (1,101 LOC)
`blob.rs`, `content.rs`, `enums.rs`, `info_hash.rs`, `torrent.rs`. Domain
types (`InfoHash`, `Torrent`, `Content`, `BlobFile`) and the **ZSTD +
MessagePack `.blob` (de)serializer that mirrors Go's
`internal/blobmigration`** — the crux of Go↔Rust data sharing. Parity is proven
byte-for-byte against Go-produced fixtures (`tests/blob_fixture.rs`,
`tests/file_extension_parity.rs`). This is the most reusable, framework-neutral
crate.

### bitmagnet-db (1,253 LOC)
`config.rs`, `pool.rs`, `stream.rs`, `agg.rs`, `deleted.rs`, `error.rs`. The PG
access layer, **read-only** (streams + aggregates; no writes, no DDL).

- **Config** (`DbConfig`): mirrors Go's `postgres.Config`; DSN-or-fields,
  `BITMAGNET_POSTGRES_*` env, password-free `log_target()` for logs.
- **Pool** (`pool.rs`): `PgPoolOptions::max_connections().connect_with()`,
  eager connect + `ping()` (`SELECT 1`).
- **Query style**: hand-written SQL string constants + the **runtime
  `sqlx::query` API with manual `try_get` row decoding** — deliberately *not*
  the compile-time `query!` macros, so the workspace builds with no live DB.
  Keyset pagination on `info_hash`/`tc.id` (`stream_torrents_with_files`,
  `stream_torrents_for_index`, `stream_changed_torrent_keys`, etc.).
- **Error**: dedicated `DbError` (`Sqlx #[from]`, `Config`, `Decode`) with a
  `From<DbError> for bitmagnet_common::Error` bridge.

### bitmagnet-parquet (5,211 LOC, bin `bitmagnet-parquet`)
The L2 **export/refresh pipeline** (CronJob CLI): reads `torrents.files_data`
blobs → sorted Parquet fact + rollups, minute delta w/ tombstone supersession,
compaction, atomic generation swap, seal/fold segmented store. Modules:
`export`, `delta`, `fact`, `rollup`, `seal`, `generation`, `manifest`,
`compact`(in generation), `verify` (Job-A parity), `schema`, `decode`. Feature
`duckdb-sort` (off by default) embeds DuckDB for the spilling external
`(extension, size)` sort of the ~880M-row base fact. Arrow/parquet 55 (53 has a
chrono-compat compile break, documented inline).

### bitmagnet-filesearch (4,737 LOC, bins `bitmagnet-filesearch`, `bitmagnet-parity`)
The **L2b DuckDB-on-Parquet file-search sidecar** — gRPC `FileSearchService`
(`SearchFiles`/`CountFiles`/`Facets`/`Reload`/`HealthCheck`) serving immutable
read-only generations with hot **`Reload` RPC** generation swap. Modules:
`service`, `engine` + `engine/duck` (real engine behind `duckdb-engine`
feature; `InMemoryEngine` for offline tests), `generation` (GenerationManager,
pin/reload), `sql` (25 tests — pure SQL builder), `query`, `parity`. Second bin
`bitmagnet-parity` is the G1/G2 parity+latency gate harness.

### bitmagnet-search (16,553 LOC, bins `bitmagnet-search`, `backfill`, `bitmagnet-pathsearch`, `bitmagnet-pathsearch-backfill`)
The largest crate — **two Tantivy services in one**:

- **Main search** (`server.rs`, `index.rs`, `indexer.rs`, `schema.rs`,
  `query.rs`, `facets.rs`, `transform.rs`, `tokenizer.rs` + `tokenizer/tables`):
  `SearchService` (Index/BatchIndex/Delete/Search/GetFacets/HealthCheck). The
  `tokenizer` is a byte-for-byte Go port proven against a fixture corpus
  (`tests/tokenizer_parity.rs`).
- **`follow.rs`** — the in-process **PostgreSQL-tail follow loop** that keeps
  the index fresh (keys on `torrents.updated_at`, carve windows, watermark
  file, backoff). Runs inside the serving process because Tantivy holds one
  writer lock per index dir.
- **`pathsearch/`** — the **L3 path-bag candidate sidecar** (`PathSearchService`)
  with its own document/index/indexer/query/schema/server/watermark, plus a
  standalone backfill bin.
- **`backfill.rs`** — the main-search bulk indexer (k8s Job entrypoint).

Only this crate depends on `tonic-health` and only its `main.rs` registers the
standard **`grpc.health.v1.Health`** service (NotServing→Serving transition).

### bitmagnet-shadow (1,007 LOC, bin `v2-shadow`)
The **V2 dual-read shadow harness / L2 DROP gate**: runs equivalent
`torrent_files` SQL (raw sqlx, `pg.rs`) and `FileSearchService` gRPC query pairs
(`grpcmap.rs`) and compares results exactly. This is the correctness instrument
that gates retiring the Go `torrent_files` read path.

---

## 2. Patterns already established (a rewrite should standardize on these)

- **Async runtime**: tokio `full` everywhere; bins use `#[tokio::main]`, libs
  drive no runtime of their own (tokio is a dev-dep in `bitmagnet-db`).
- **Error handling**: **`thiserror` per-crate enums in libraries**
  (`DbError`, blob `BlobError`, etc.) with `#[from]` and cross-crate `From`
  bridges to `bitmagnet_common::Error`; **`anyhow` in binaries** with
  `.context(...)` at the boundaries. Clean, conventional split.
- **Config/env**: **`clap` derive with `#[arg(long, env = "...")]`** as the
  per-bin config surface; DB layer additionally reads `BITMAGNET_POSTGRES_*`
  via `DbConfig::from_env()`. No layered config file — env + flags only.
- **Logging/tracing**: `tracing` macros throughout; every bin calls
  `bitmagnet_common::init_tracing()` (env-filter, `RUST_LOG`, `info` default).
  Structured fields used (`db = %cfg.log_target()`), password redaction handled
  at the `log_target()` boundary.
- **gRPC transport**: tonic 0.14, **UDS-or-TCP listener** parsed from a single
  `--addr` string (`unix:` prefix or `HOST:PORT`), served with
  `serve_with_shutdown` / `serve_with_incoming_shutdown` over
  `UnixListenerStream`. UDS socket file is unlinked-then-bound.
- **Graceful shutdown**: signal-driven. As of `463d0d32` (2026-07-09 hardening
  batch) **all three serving binaries — `bitmagnet-search`,
  `bitmagnet-pathsearch`, and `bitmagnet-filesearch` — handle both SIGINT and
  SIGTERM** (proper k8s behavior), and the `bitmagnet-parquet follow` sidecar
  loop selects on both between ticks. [Team-lead review correction: an earlier
  draft called filesearch SIGINT-only; that was true before 463d0d32.]
- **Health checks**: two-tier and **inconsistent**. Every service exposes a
  *custom* `HealthCheck` RPC (returns live counts/generation state). Only
  `bitmagnet-search` additionally serves the standard `grpc.health.v1.Health`
  reporter; filesearch (no `tonic-health` dep) and pathsearch do not.
- **PG access**: `sqlx` runtime API (not compile-time macros) + hand-written
  SQL constants + manual row decode; `PgPoolOptions` pool, keyset pagination,
  batch `= ANY($1::bytea[])` reads. Deliberately DB-free at build time.
- **Build/release**: workspace at `bitmagnet-rs/`, deps centrally pinned with
  additive per-crate features; heavy C++ engines (DuckDB `bundled`) behind
  opt-in features (`duckdb-engine`, `duckdb-sort`) so the default build/test
  gate is fast and C++-free. Two Dockerfiles under `docker/`
  (`Dockerfile.search`, `Dockerfile.filesearch`) — multi-stage
  `rust:1.95-slim-bookworm` → `debian:bookworm-slim`, nonroot uid 65532,
  **one image with multiple entrypoints** (filesearch image ships filesearch +
  parquet + parity bins). Release profile is cargo default (no custom profile
  tuning present).
- **Cross-language parity as a first-class discipline**: enum-discriminant
  tests, blob-fixture parity, tokenizer-fixture parity, and a live dual-read
  shadow gate. A rewrite inherits a strong correctness-vs-Go harness.

---

## 3. What the Rust estate already REPLACES from Go

The **search read-path is substantially Rust already** and **live in prod**:

| Capability | Go original | Rust replacement | Status |
|-----------|-------------|------------------|--------|
| Main full-text search | PG `tsvector` + `internal/search` | Tantivy index + `follow` loop (`bitmagnet-search`) | Machinery built; follow loop + backfill exist (P3/P4 shadow dormant per project notes) |
| File search (per-file queries) | `torrent_files` table queries | L2b DuckDB-on-Parquet sidecar (`bitmagnet-filesearch`) | **LIVE** on HEL1 |
| Path / candidate search | (path substring over `torrent_files`) | L3 Tantivy pathsearch (`bitmagnet-search/pathsearch`) | **LIVE** on HEL1 |
| `torrent_files` → columnar | (implicit in PG) | L2 export/refresh pipeline (`bitmagnet-parquet`) | **LIVE** (CronJobs) |
| Blob (files_data) codec | `internal/blobmigration` | `bitmagnet-model::blob` | Parity-proven |

**Quantifying what remains in Go.** The Go app is **~88k non-test LOC** under
`internal/`. The Rust crates replace the *read/query* satellites hanging off the
database; they do **not** touch the ingest, classification, or API tiers. The
Go runtime work still 100% Go:

- **DHT crawler** (`internal/dhtcrawler`) — the ingest firehose.
- **Processor + classifier** (`internal/processor` ~0.8k, `internal/classifier`
  ~4.2k) — metadata resolution, TMDB, content classification.
- **Queue / worker** (`internal/queue` ~0.8k, `internal/worker`) — the async
  job engine.
- **GraphQL / API + webui backend** (`internal/gql` ~30k LOC — by far the
  largest subsystem — plus `httpserver`, `torznab`, `webui`).
- **Database write-side + schema** (`internal/database` ~16k) — **all writes,
  migrations, and generated columns**.

So the Rust estate covers the **read/search query path** — roughly the search
subsystem and its feed pipeline — while the **write path, ingest, enrichment,
and the entire API/UI surface (the majority of runtime work and ~75%+ of the
code) remain Go**. A rewrite is far from half-done; it has a strong, proven
read-side spine and an untouched ingest/API body.

---

## 4. Gaps / liabilities as a rewrite foundation

1. **No metrics / instrumentation — at all.** A workspace-wide grep for
   `metric`/`prometheus`/`counter!`/`histogram!` returns **zero hits**. The Go
   app has `internal/metrics` + `internal/telemetry`; the Rust services are
   observability-blind beyond `tracing` logs. Any rewrite must add a metrics
   layer (e.g. `metrics` + a Prometheus exporter, or `opentelemetry`) as
   foundational, not bolt-on. **This is the single biggest gap.**
2. **No shared config / server-bootstrap framework.** Each of the **8 bins**
   re-derives its own `clap` `Args` struct and re-implements listener parsing,
   tracing init, shutdown wiring, and health registration. `bitmagnet-common`
   has a `Config` type that is essentially unused. This duplication will
   compound badly as more services are added — a rewrite should extract a
   `serve(service, opts)` bootstrap + a layered config crate first.
3. **Signal handling — RESOLVED but hand-rolled per bin.** As of `463d0d32`
   all serving bins handle SIGINT+SIGTERM, but each carries its own copy of the
   select-on-signals wiring. Standardize one shutdown helper across all bins
   (belongs in the same shared bootstrap crate as #2).
4. **Inconsistent health surface.** Only `bitmagnet-search` serves
   `grpc.health.v1.Health`; filesearch/pathsearch expose only bespoke
   `HealthCheck` RPCs. Orchestrator/liveness probes see three different
   contracts. Standardize on `grpc.health.v1` everywhere plus optional rich
   custom health.
5. **Migrations are Go-owned (goose).** The schema, generated columns
   (`torrent_files.extension`), and all DDL live in Go/goose. The Rust `db`
   crate is **read-only and schema-blind** (runtime queries, `::text`/`::bigint`
   casts to survive enum/int OID differences). A Rust-authoritative rewrite must
   either take over migrations (sqlx-migrate / refinery) or formalize the Go
   schema as an external contract — currently it's the latter, implicitly.
6. **Per-bin code duplication beyond config**: listener parsing, watermark
   handling, shutdown, and health wiring are copy-adapted across bins rather
   than shared.
7. **`bitmagnet-common` is too thin to be the shared kernel** it's positioned
   as — 3 error variants and an unused `Config`. It needs to grow into (or be
   split into) config, server-runtime, observability, and error crates.
8. **Proto organization**: all four protos collapse into one `bitmagnet.v1`
   module (fine now, but service-per-file discipline in `proto/bitmagnet/` isn't
   reflected in module boundaries; versioning is single-`v1`).
9. **Heavy native deps (DuckDB `bundled`, tantivy) inflate build/CI time** and
   require a C++ toolchain in the image builders; feature-gating mitigates but
   the release images still compile libduckdb from source.
10. **No custom cargo release profile** (LTO/codegen-units/strip) — an easy,
    unclaimed win for binary size and runtime.

---

## 5. CI / test infrastructure

**What exists** (`.github/workflows/rust.yml`, path-filtered to `bitmagnet-rs/**`):

- Four jobs: **`fmt`** (`cargo fmt --all --check`), **`clippy`**
  (`cargo clippy --workspace --all-targets -- -D warnings` — note: workspace
  lints are `warn`, CI promotes to `-D`), **`test`** (`cargo test --workspace`),
  **`docker`** (builds `Dockerfile.search` only, no push, gha cache).
- Toolchain via `dtolnay/rust-toolchain@stable` + `arduino/setup-protoc@v3` +
  `Swatinem/rust-cache@v2`. A comment flags intended future convergence on the
  Go pipeline's `nix develop` + `task`.
- **~290 tests, offline-first**: default-off feature flags (`duckdb-engine`,
  `duckdb-sort`) keep `cargo test` C++-free and DB-free; an in-memory engine
  substitutes for DuckDB in filesearch tests.
- **Clean live-PG gating**: `bitmagnet-db/tests/integration_pg.rs` uses
  **`#[ignore = "requires a live PostgreSQL (set BITMAGNET_POSTGRES_DSN)"]`**;
  run with `cargo test -p bitmagnet-db -- --ignored`. This is the only DB-gated
  tier and it's well-documented in-file.
- Strong **cross-language parity fixtures** committed as test corpora (blob,
  file-extension, tokenizer).

**What a growing Rust codebase would need**:

- **A live-PG / integration CI lane** (a Postgres service container running the
  `--ignored` tests) — today the integration tier only runs manually.
- **Docker CI for the filesearch image** (only `Dockerfile.search` is built in
  CI; the DuckDB image is unbuilt/untested there) and image publish
  (the `ghcr.yml` path).
- **Coverage + a metrics/health smoke lane** once §4.1/§4.4 are addressed.
- **`cargo deny`/audit** for the growing native-dep surface (duckdb, tantivy,
  arrow) — no supply-chain/license gate today.
- **Nix/task convergence** with the Go pipeline (already flagged as intended).

---

## Appendix — key file references

- Workspace + pinned deps: `bitmagnet-rs/Cargo.toml`
- Shared kernel: `bitmagnet-rs/crates/bitmagnet-common/src/lib.rs`
- Proto codegen + wire-contract tests: `bitmagnet-rs/crates/bitmagnet-proto/{build.rs,src/lib.rs}`, `bitmagnet-rs/proto/bitmagnet/*.proto`
- DB config/pool/query style: `bitmagnet-rs/crates/bitmagnet-db/src/{config.rs,pool.rs,stream.rs,agg.rs,error.rs}`
- Live-PG test pattern: `bitmagnet-rs/crates/bitmagnet-db/tests/integration_pg.rs`
- Search entry + follow loop + health: `bitmagnet-rs/crates/bitmagnet-search/src/{main.rs,follow.rs,server.rs}`
- Filesearch service + shutdown (SIGINT-only): `bitmagnet-rs/crates/bitmagnet-filesearch/src/{main.rs,service.rs}`
- Shadow gate: `bitmagnet-rs/crates/bitmagnet-shadow/src/{pg.rs,grpcmap.rs}`
- Build/release: `bitmagnet-rs/docker/Dockerfile.{search,filesearch}`
- CI: `.github/workflows/rust.yml`
