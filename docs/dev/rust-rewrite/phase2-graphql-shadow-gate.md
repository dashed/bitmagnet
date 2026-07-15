# Phase-2 GraphQL shadow — numeric gate & soak runbook (Lane P, P3)

The machine-evaluable gate for the async-graphql read-API rewrite. The live Go
GraphQL server captures its already-computed response and pre-write
response-generation duration, then a bounded embedded hook calls the dark Rust
GraphQL service for sampled eligible searches and emits
`bitmagnet_graphql_shadow_*` metrics. This gate decides, from those metrics
alone, whether the ≥7-day soak passed and cutover is safe.

- **Comparator + gate feed:** `internal/search/graphqlshadow` (P2).
- **Rules + alerts + unit test:** `observability/rules/graphql-shadow-gate.rules.yml`,
  `observability/rules/graphql-shadow-gate.test.yml`.
- **Dashboard:** `observability/grafana-dashboards/graphql-shadow-gate.json`.

## Embedded hook runtime contract

The hook is installed on the real gqlgen response path and registered as the
`graphql_shadow` config section. Its environment surface is:

- `GRAPHQL_SHADOW_ENABLED=false` — the single kill switch;
- `GRAPHQL_SHADOW_ENDPOINT=http://bitmagnet-graphql.bitmagnet.svc.cluster.local:3337/graphql`;
- `GRAPHQL_SHADOW_SAMPLE_RATE=0` — a second independent default-off guard;
- `GRAPHQL_SHADOW_TIMEOUT=5s`;
- `GRAPHQL_SHADOW_MAX_CONCURRENT=4`;
- `GRAPHQL_SHADOW_LOG_DISCREPANCIES=true`.

Only a selected read-only `torrentContent.search` operation is comparable. Its
resolved input must set `totalCount: true`; its response must select the
unaliasable `totalCount`, `totalCountIsEstimate`, `items`, and `aggregations`
fields; and each item must select either canonical `id` or all four InferID
components. Any selected recognized facet must include the complete unaliased
`value`, `label`, `count`, and `isEstimate` projection. Mutations, subscriptions,
parse failures, ambiguous operations, unrelated queries, aliased/incomplete
projections, sampling misses, and concurrency saturation all make zero Rust
calls. The hook detaches admitted work with `context.WithoutCancel`, applies its
own hard timeout, and compares Rust with the captured Go response; it never
issues a second Go request.

The dark response must carry
`X-Bitmagnet-Graphql-Handler-Duration-Us: <positive integer>`. A missing,
malformed, zero, or negative duration makes that admitted attempt a Rust error;
client round-trip time is deliberately not used. The dark client does not follow
redirects and accepts only `application/json` or
`application/graphql-response+json` responses (media-type parameters are
allowed); any redirect or other/malformed/missing media type is a Rust error.
Both histograms represent the
closest shared server-side boundary: HTTP request entry through pre-write
GraphQL response generation. On Go, the clock is sealed immediately after the
gqlgen response handler returns, before `ServeHTTP` writes the response. This
includes parsing, validation, resolver execution, and field-data JSON generation,
but excludes gqlgen's final outer response-envelope serialization and the socket
write. Rust may include final envelope serialization if it builds the complete
body before stamping the header; keep that small remaining serialization nuance
in mind when investigating near-equal latency results.

## Thresholds (phase6 §5 / `phase2-tasks.md §Gate thresholds`)

| KPI | Recording rule | Threshold |
|---|---|---|
| Top-1 match ratio | `graphql_shadow:top1_match:ratio` | ≥ 0.98 |
| Jaccard@20 mean | `graphql_shadow:jaccard20:mean` | ≥ 0.90 |
| RBO mean (p=0.9) | `graphql_shadow:rbo:mean` | ≥ 0.92 |
| Exact total-count match | `graphql_shadow:total_count_match:ratio` | ≥ 0.95 |
| Served-path latency | `graphql_shadow:rust_latency:p99` vs `:reference_latency:p99` | Rust p99 ≤ Go p99 |
| Rust execution validity | `graphql_shadow:rust_success:ratio` | ≥ 0.99 |
| Soak-validity floor | `graphql_shadow:comparisons:increase1h` | ≥ 20 samples / 1h window |

Each KPI has a 1/0 `graphql_shadow:<kpi>:pass` companion; their product is
`graphql_shadow:gate_pass`. All KPIs are computed over a **1h rolling window**;
the gate requires `gate_pass` to hold **continuously** for the whole soak.

Every raw gate input is scoped to the production Go L3 scrape labels
`namespace="bitmagnet",service="bitmagnet-l3"`. An isolated canary must use a
different Service label. The promtool fixture includes a deliberately divergent
canary series and proves it cannot change the production recording rules.

### Sample-zero admission profile

Homelab commit `65d8d7b` provides a separate
`bitmagnet-graphql-shadow-canary` profile for validating the deployment path
before sampling begins. It is serve-only (`worker run --keys=http_server`), has
no Ingress, is excluded from production recording rules by its distinct Service
identity, and hard-bounds both the configured and admitted shadow sample rate to
zero. The profile also fails closed unless its tag-only main image exists on the
selected node with the expected containerd digest, `linux/amd64` platform, and
pinned label; the dark GraphQL EndpointSlice must have a ready TCP 3337 address
before any canary object is applied.

The profile and reciprocal Cilium manifests are offline-rendered through
Ansible and kubeconform. Importing the image, creating the SELECT-only canary
database role, and deploying the sample-zero profile are still separate
`CONFIRM=1` production mutations. Raising the admitted sample ceiling above zero
is a later gate and must not be folded into initial deployment.

**Estimate totals are excluded** from the count-match KPI (the `estimate="false"`
selector): a budgeted-estimate total legitimately differs between engines, so only
exact totals are held to the 0.95 bar. The `estimate="true"` rows are still
recorded and visible on the dashboard.

**Facets** (`graphql_shadow:all_facets_match:ratio`) are reported and alerted on
(`GraphQLShadowFacetMismatch`, info) but are **not** part of `gate_pass` — the
per-facet diff is a supplementary signal, not a phase6 §5 threshold. The
per-facet breakdown is `bitmagnet_graphql_shadow_facet_match_total{facet,matched}`.
A facet enters that denominator when it is present and non-null on either side;
one-sided presence, including a one-sided empty list, is an observed mismatch.
Only absence/null on both sides is unobserved. The all-facets series is emitted
only when the union of observed keys covers all nine facets, so ordinary partial-
facet traffic contributes valid per-facet samples without manufacturing an
all-facets result.

## Soak verdict — the single query

The soak passes iff the composite gate held for every evaluation across the full
soak window:

```promql
min_over_time(graphql_shadow:gate_pass[7d]) == 1
```

`1` → PASS (safe to proceed to a user-gated cutover decision). `0` or empty →
FAIL / insufficient data — do **not** cut over. Widen `[7d]` to the actual soak
length if it ran longer.

To see *which* KPI failed and when, plot each `graphql_shadow:<kpi>:pass` over the
window; the metric that dropped to 0 points at the defect (phase6 §5: "the failing
metric points at the defect").

## Safety-gate observability (the mutation guard)

The load-bearing safety property is unit-tested at both the response-hook and
driver boundaries in `internal/search/graphqlshadow`: mutations, subscriptions,
parse failures, and ambiguous documents make **zero Rust calls**, while the
primary Go handler executes exactly once. The attempt counters make each stage
auditable:

- `bitmagnet_graphql_shadow_sampled_total` increments after an eligible search
  wins the sampling draw;
- `bitmagnet_graphql_shadow_admitted_total` increments after the captured Go
  result is valid and the non-blocking concurrency slot is acquired;
- `bitmagnet_graphql_shadow_comparisons_total` increments only after Rust
  returns a valid response and duration header and a comparison is recorded.

`bitmagnet_graphql_shadow_dropped_total` counts requests rejected by the safety
operation gate; `bitmagnet_graphql_shadow_saturated_total` counts sampled
comparisons shed by the non-blocking concurrency limit. A rising dropped counter
is expected when live traffic contains mutations and is not an error. The
recorded Rust-success ratio is comparisons divided by admitted attempts; a value
below 0.99 invalidates `gate_pass` and raises `GraphQLShadowRustErrors` once the
window has enough admitted attempts.

## Node-contention abort signals (06 R5 / P2-6)

The embedded hook reuses the primary Go result and adds only one sampled Rust/PG
read. On the single HEL1 node these are hard soak abort signals independent of
the numeric gate:

- `NodeDiskIOSaturation` (md2/HEL1) sustained high → cut the shadow sample rate or
  flip the kill-switch (Lane I `bitmagnet_graphql_shadow_enabled: false`).
- Live `/graphql` p99 (the full served Go path, not only the hook's pre-write
  response-generation histogram) regressing → same response.

`GraphQLShadowReferenceErrors` (>5% captured-response projection failures) means
the comparison sample is degraded; its ratio uses 15-minute counter increases
over sampled attempts. Fix the query/projection contract before trusting the
verdict. `GraphQLShadowRustErrors` means admitted attempts are failing, timing
out, returning invalid GraphQL data, or omitting the required positive duration
header; fix that path before trusting the numeric parity ratios.

## Composer-bound backstop (Lane C metrics)

The `graphql_shadow_composer_bounds` group alerts on the L1 refine pipeline's
gate-7 caps (`bitmagnet_search_pathsearch_refine_*`). These reference **Lane C's**
C6 metric names — reconcile them against `internal/search/pathsearch/metrics.go`
once C6 lands (a sustained cap spike is a memory/latency-bound regression the
numeric gate alone would miss; C7's bound tests are the offline backstop).

## Running the checks offline

```bash
task test-prometheus-rules
```

Both are wired into CI-equivalent local verification; the unit test drives
synthetic pass / fail / low-volume scenarios and asserts `gate_pass` and the
alerts behave as specified. The Task invokes both `promtool check rules` and
`promtool test rules`; the Nix development shell provides `promtool` through the
Prometheus package.
