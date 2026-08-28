# Phase 0 — Foundations: task ledger (started 2026-07-10)

Execution ledger for `05-roadmap-and-gates.md §1`. Three parallel lanes with
**disjoint file ownership**; each lane is one agent stream on its own branch off
`rust-rewrite-phase0-20260710`, built/tested in its own Coder workspace, with
WIP commits pushed before/after every codex stage.

**Done-definition (from the roadmap):** all five golden-file harnesses green in
CI against the current Go app + Rust sidecars; the metrics layer exports from at
least one live sidecar (listener default-OFF, env-enabled); `cargo test
--workspace` + new CI lanes green. No cutover, no behavior change: every
retrofit must be bit-identical with existing envs.

## Lane A — Rust foundation crates (branch `p0a-foundations`)
Owns: `bitmagnet-rs/crates/bitmagnet-common/**`, every `bitmagnet-rs` bin's
main/wiring, `bitmagnet-rs/Cargo.toml` workspace deps. OFF-LIMITS: `.github/**`,
`internal/**`, new `bitmagnet-diff` crate.

| # | Task | Status |
|---|------|--------|
| A1 | Layered config module in bitmagnet-common (figment: defaults < file < env) + **strcase env-key port** matching Go's iancoleman/strcase mapping; parity test reads `testdata/parity/config-env-map.golden` (format §Contracts) | ✅ done (merged 2026-07-10) |
| A2 | Bootstrap module: UDS-or-TCP listener parse, tracing init, SIGINT+SIGTERM select, tonic-health (grpc.health.v1) registration, `serve_with_shutdown(service, opts)` | ✅ done (merged 2026-07-10) |
| A3 | Metrics module: prometheus registry + optional HTTP `/metrics` listener (env `BITMAGNET_METRICS_ADDR`, DEFAULT OFF), process + per-service metric hooks | ✅ done (merged 2026-07-10) |
| A4 | Retrofit serving bins (search serve, pathsearch, filesearch serve, parquet follow) onto A1-A3 — behavior-identical with existing envs (same flags/defaults); first real metric: `search_follow_watermark_age_seconds` on main-search | ✅ done (merged 2026-07-10) |

## Lane B — Go golden files + ALL CI wiring (branch `p0b-goldens`)
Owns: `graphql/**` (snapshot artifact + test only), `internal/config*` golden
generator test files, `internal/telemetry` golden test, `migrations/` manifest
test, `testdata/parity/**` (creates it), **`.github/workflows/**` exclusively**.
OFF-LIMITS: `bitmagnet-rs/**` except adding CI jobs that call lane-A/C-provided
scripts; `internal/classifier/**`.

| # | Task | Status |
|---|------|--------|
| B1 | GraphQL SDL golden: normalized concatenation of `graphql/schema/*.graphqls` → `testdata/parity/schema.graphql` + regeneration-assert test | ✅ done (merged 2026-07-10) |
| B2 | Config env-map golden: generator walking the config spec → `testdata/parity/config-env-map.golden` (§Contracts format) + assert test. **Commit+push the golden EARLY — Lane A consumes it** | ✅ done (merged 2026-07-10) |
| B3 | Metric-name golden: register all collectors, dump sorted `name{sorted,label,keys}` → `testdata/parity/metric-names.golden` + assert test | ✅ done (merged 2026-07-10) |
| B4 | goose-history manifest: `migrations/*.sql` ordered names + sha256 → `testdata/parity/migrations.golden` + assert test (protects renumber/edit) | ✅ done (merged 2026-07-10) |
| B5 | CI: golden-file jobs in the Go workflow; rust.yml gains cargo-deny + filesearch-image build + live-PG `#[ignore]` lane; corpus-runner job calling Lane C's runner | ✅ done (merged 2026-07-10) |

## Lane C — classifier corpus + differential harness (branch `p0c-diffharness`)
Owns: `internal/classifier/**` (fixtures + generator test only, no logic
changes), new `internal/parity/**` (Go driver), new
`bitmagnet-rs/crates/bitmagnet-diff/**`, `testdata/parity/classifier/**`.
OFF-LIMITS: `.github/**` (hand a runnable `go test ./internal/parity/...`
target to Lane B), bitmagnet-common, bins.

| # | Task | Status |
|---|------|--------|
| C1 | Deterministic classifier fixture corpus (~200-500 synthetic torrents covering DSL branches + release-name edge cases; NO network — assert the classifier path used is pure) + Go generator test emitting `testdata/parity/classifier/*.golden` (input → full Classification JSON) | ✅ done (merged 2026-07-10) |
| C2 | Differential harness: fixture format (JSONL), Go driver package (`internal/parity`) + Rust crate skeleton (`bitmagnet-diff`) with driver trait + normalizing differ; prove with ONE ported example pair (blob codec or tokenizer fixtures re-expressed in the harness) | ✅ done (merged 2026-07-10) |

## Contracts (cross-lane, fixed here so no lane waits)
- **config-env-map.golden format**: sorted unique lines, `ENV_KEY\tdot.path`,
  LF endings, single trailing newline. Includes every env key the Go config
  walker resolves (the ~25 deployed ones plus the rest of the spec).
- **testdata/parity/** is the shared root; each lane creates only its own files
  under it; the DIRECTORY is created by Lane B first (B2 early push).
- **CI jobs** are owned by Lane B; Lanes A/C expose plain commands
  (`cargo test -p bitmagnet-common`, `go test ./internal/parity/...`).

## Rules of engagement (from tonight's session — binding)
- WIP-commit + push before/after every codex stage and any long wait.
- All builds/tests in your Coder workspace via a self-healing remote-test.sh
  (toolchains under $HOME — dev-k8s rootfs is ephemeral).
- Stay in your lane; if you need a file another lane owns, STOP and message
  the team lead.
- Honest failure reports with log evidence; never force-commit red.
- Team lead reviews every lane before merge into rust-rewrite-phase0-20260710.
