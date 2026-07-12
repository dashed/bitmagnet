# Phase-2 GraphQL shadow — numeric gate & soak runbook (Lane P, P3)

The machine-evaluable gate for the async-graphql read-API rewrite. The dark Rust
GraphQL service (or the Go-embedded fallback) runs a self-shadow over sampled
live `/graphql` traffic and emits `bitmagnet_graphql_shadow_*` metrics; this gate
decides, from those metrics alone, whether the ≥7-day soak passed and cutover is
safe.

- **Comparator + gate feed:** `internal/search/graphqlshadow` (P2).
- **Rules + alerts + unit test:** `observability/rules/graphql-shadow-gate.rules.yml`,
  `observability/rules/graphql-shadow-gate.test.yml`.
- **Dashboard:** `observability/grafana-dashboards/graphql-shadow-gate.json`.

## Thresholds (phase6 §5 / `phase2-tasks.md §Gate thresholds`)

| KPI | Recording rule | Threshold |
|---|---|---|
| Top-1 match ratio | `graphql_shadow:top1_match:ratio` | ≥ 0.98 |
| Jaccard@20 mean | `graphql_shadow:jaccard20:mean` | ≥ 0.90 |
| RBO mean (p=0.9) | `graphql_shadow:rbo:mean` | ≥ 0.92 |
| Exact total-count match | `graphql_shadow:total_count_match:ratio` | ≥ 0.95 |
| Served-path latency | `graphql_shadow:rust_latency:p99` vs `:reference_latency:p99` | Rust p99 ≤ Go p99 |
| Soak-validity floor | `graphql_shadow:comparisons:increase1h` | ≥ 20 samples / 1h window |

Each KPI has a 1/0 `graphql_shadow:<kpi>:pass` companion; their product is
`graphql_shadow:gate_pass`. All KPIs are computed over a **1h rolling window**;
the gate requires `gate_pass` to hold **continuously** for the whole soak.

**Estimate totals are excluded** from the count-match KPI (the `estimate="false"`
selector): a budgeted-estimate total legitimately differs between engines, so only
exact totals are held to the 0.95 bar. The `estimate="true"` rows are still
recorded and visible on the dashboard.

**Facets** (`graphql_shadow:all_facets_match:ratio`) are reported and alerted on
(`GraphQLShadowFacetMismatch`, info) but are **not** part of `gate_pass` — the
per-facet diff is a supplementary signal, not a phase6 §5 threshold. The
per-facet breakdown is `bitmagnet_graphql_shadow_facet_match_total{facet,matched}`.

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

The load-bearing safety property — a mutation is NEVER re-issued to the Go
reference — is proven by the unit test in `internal/search/graphqlshadow`
(`TestShadowOnceMutationMakesZeroReferenceCalls`, zero reference calls on any
non-query document). In production, `bitmagnet_graphql_shadow_dropped_total`
counts every mirrored request the gate hard-dropped; every increment is a request
that reached the dark service and made **zero** Go reference calls. A rising
`dropped_total` is expected (live traffic contains mutations) and is healthy, not
an error.

## Node-contention abort signals (06 R5 / P2-6)

The self-shadow doubles PG read load on the sampled slice (it queries both its own
resolvers and the live Go reference). On the single HEL1 node these are hard soak
abort signals independent of the numeric gate:

- `NodeDiskIOSaturation` (md2/HEL1) sustained high → cut the mirror sample rate or
  flip the kill-switch (Lane I `bitmagnet_graphql_shadow_enabled: false`).
- Live `/graphql` p99 (Traefik/Go, not the shadow's `reference_latency`)
  regressing → same response.

`GraphQLShadowReferenceErrors` (>5% reference-call failures) means the comparison
sample is degraded; fix the reference path before trusting the verdict.

## Composer-bound backstop (Lane C metrics)

The `graphql_shadow_composer_bounds` group alerts on the L1 refine pipeline's
gate-7 caps (`bitmagnet_search_pathsearch_refine_*`). These reference **Lane C's**
C6 metric names — reconcile them against `internal/search/pathsearch/metrics.go`
once C6 lands (a sustained cap spike is a memory/latency-bound regression the
numeric gate alone would miss; C7's bound tests are the offline backstop).

## Running the checks offline

```bash
promtool check rules observability/rules/graphql-shadow-gate.rules.yml
promtool test rules  observability/rules/graphql-shadow-gate.test.yml
```

Both are wired into CI-equivalent local verification; the unit test drives
synthetic pass / fail / low-volume scenarios and asserts `gate_pass` and the
alerts behave as specified.
