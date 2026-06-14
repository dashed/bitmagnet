# ps-harness — L3 pathsearch latency + recall harness

Self-contained, uv-managed Python harness for the deployed **L3
`bitmagnet-pathsearch`** gRPC sidecar. Proves **D6 gate 5 (latency)** and
**D6 gate 6 (candidate recall)**. The harness is the *tool only* — it takes a
target address + a ground-truth file as inputs and makes **read-only**
`PathCandidates` / `HealthCheck` RPCs. It performs no production discovery,
SSH, or kubectl; the gated live run is driven separately by the lead.

## Setup

```bash
cd bench/pathsearch-harness
uv sync                 # creates .venv, installs grpcio + grpcio-tools
uv run ps-harness gen   # generate gRPC stubs from vendored proto/ (also lazy-auto)
```

gRPC stubs are generated from the **vendored** copies under `proto/bitmagnet/`
(`common.proto`, `search.proto`, `path_search.proto`) into
`src/ps_harness/_generated/` (gitignored, regenerable). To refresh the vendored
protos from the source tree, re-copy from
`bitmagnet-rs/proto/bitmagnet/` and run `ps-harness gen --force`.

## Connection

`--addr HOST:PORT` (default from `BITMAGNET_PATHSEARCH_ADDR`, else
`127.0.0.1:50053`), **plaintext**. For prod, point at a port-forward /
tailscale-reachable endpoint the lead sets up (H2).

```bash
uv run ps-harness health --addr 127.0.0.1:50053
```

## Latency (gate 5)

Single-client, sequential. Per query: `--warm-reps` untimed priming calls, then
`--reps` timed `PathCandidates` RPCs; reports per-query and per-group
p50/p95/p99/min/max/mean using the **same nearest-rank percentile** as
`bench-file-index` (`docs/dev/pathsearch-microbench-spec.md`). The default query
set is the broad per-keystroke prefix sweep (`queries/ps_prefix_sweep.tsv`) — the
`ascii3` / `cjk3` rows are the **gate rows** (< 50 ms warm p50 is the G3 bar).

```bash
uv run ps-harness latency \
  --addr 127.0.0.1:50053 \
  --queries-file queries/ps_prefix_sweep.tsv \
  --warm-reps 5 --reps 30 --limit 50 --oversample 200 \
  --json-out out/latency.json
```

`*trunc` marks queries whose match-set exceeded the returned page;
`*cap` marks queries that hit the 5000-candidate cap.

## Recall (gate 6)

Implements the canonical rev2 gate
(`docs/dev/l3-recall-gate-query-set-and-truth.md`). **Single method — sample
membership, gated on the 5000-cap:**

- request L3 with `limit = 5000` → `returned = min(candidate_total, 5000)`;
- **`candidate_total <= 5000`** ⇒ `membership_valid` ⇒ L3 returned its FULL
  match-set ⇒ `recall = |truth ∩ returned| / |truth|` **must be 1.0** — any single
  real miss FAILS the gate (the un-returned truth hashes are surfaced for §4c
  triage);
- **`candidate_total > 5000`** ⇒ `membership_valid = false` ⇒ the query is
  **auto-dropped** from the recall metric (latency-only); a truth hash "absent"
  here is below the TopDocs cap, not a miss.

`info_hash` is compared as 40-char **lowercase hex** (proto = 20 raw bytes →
`.hex()`). The truth is page-sampled and **freshness-filtered** (`updated_at <=
watermark_bound_epoch`), so a miss is never staleness. The harness reads
`watermark_epoch` from `HealthCheck` at run start and records
`watermark_bound_epoch = watermark_epoch − margin` (default 60s) into the output
meta (the value the lead's truth SQL `$2` must use).

```bash
uv run ps-harness recall \
  --addr 127.0.0.1:50053 \
  --truth-file docs/dev/l3-recall-truth.json \
  --write-truth out/l3-recall-truth.filled.json \
  --json-out out/recall.json
```

Exit code is `0` only if the gate passes (`5` on gate fail) so a gated run can
branch on it.

### Truth-file format (rev2)

Canonical `{meta, queries:[...]}` authored by recall-engineer. Each query is
class `recall`, carries an `expected` hint (`selective` / `uncertain` /
`…overcap…`) and `truth_info_hashes` (40-char lowercase hex, filled by
`populate`/the lead). `meta.l3_request` drives the request page; `meta.
watermark_margin_secs` the freshness margin. A flat `{query: [hex, …]}` dict is
also accepted for ad-hoc use. See `docs/dev/l3-recall-truth.json`.

## Populate (truth from PostgreSQL — LEAD-GATED, read-only)

Fills `truth_info_hashes` via the §5 rev2 ground-truth SQL: `torrent_files
TABLESAMPLE SYSTEM (2.0) REPEATABLE (4242)` JOIN `torrents` with
`position(lower($q) IN lower(path)) > 0 AND updated_at <= to_timestamp($2)`,
`LIMIT 500`. **Serial single connection**, `statement_timeout` (default 15s, the
sidecar has `MAX_CONNECTIONS=2`), and the **password is read from `PGPASSWORD`
only** — never the DSN/argv (a password in `--pg` is refused).

```bash
export PGPASSWORD=...   # from the k8s secret, set by access-engineer at launch
uv run ps-harness populate \
  --truth-file docs/dev/l3-recall-truth.json \
  --pg 'postgresql://postgres@127.0.0.1:5432/bitmagnet' \
  --grpc 127.0.0.1:50053 \            # read watermark_epoch → watermark_bound_epoch
  --out out/l3-recall-truth.filled.json
```

`--grpc` reads the L3 watermark and subtracts the margin; or pass
`--watermark-bound-epoch N` directly. Lower `--sample-pct` (→1.0/0.5) if a query
times out; `--no-freshness` skips the filter (not recommended).

## Offline self-test (no prod)

```bash
uv run pytest -q        # spins an in-process mock PathSearchService
```

The mock reproduces the server's match semantics (lowercase substring, 2-char
guard, uncapped `candidate_total`, 5000-cap page) and asserts the recall math +
all three miss classifications.
