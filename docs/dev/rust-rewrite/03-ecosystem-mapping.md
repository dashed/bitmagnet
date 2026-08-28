# 03 — Rust Ecosystem Mapping for the bitmagnet Rewrite

> Scope: a subsystem-by-subsystem survey of the Rust crate landscape (state as of
> mid-2026) for a full rewrite of bitmagnet (Go). For every subsystem: what the Go
> side actually uses, 2–3 candidate Rust approaches with maturity/maintenance/risk
> verdicts, and a recommendation. Maturity claims are grounded in dated
> 2025/2026 sources (linked inline). No code changes; this document feeds the
> synthesis agent.

## 0. Grounding: what the Go app uses, what the Rust workspace already picked

Pulled from `/Users/me/aaa/github/bitmagnet/go.mod` and `internal/*`:

| Subsystem | Go dependency (go.mod) |
|---|---|
| DHT crawl primitives | `anacrolix/dht/v2 v2.22.0` (bootstrap), `anacrolix/torrent v1.58.0` (bencode, metainfo, `peer_protocol`/ut_metadata) — **but the crawler, k-table, BEP-5 responder, BEP-51 sampler are all bespoke** (`internal/dhtcrawler`, `internal/protocol/dht/{ktable,responder,server,scrape}`) |
| GraphQL | `99designs/gqlgen v0.17.64` + `vektah/gqlparser/v2 v2.5.22` (code-gen from `internal/gql/*.graphql` schema, resolvers wired via gin) |
| PG access | `jackc/pgx/v5 v5.7.2` + `gorm.io/gorm v1.25.12` + `gorm.io/gen v0.3.26` (typed query gen) + `gorm.io/plugin/dbresolver` (read replica) + `mgdigital/gorm-cache/v2` |
| Migrations | `pressly/goose/v3 v3.24.1` — 25 SQL migrations in `migrations/*.sql`, `goose_db_version` table |
| Queue | bespoke PG-backed queue (`internal/queue/{manager,server,handler}`), priority + dedup |
| Classifier | `google/cel-go v0.23.2` (+ `antlr4-go/antlr/v4`, `cel.dev/expr`) — CEL over a protobuf `Torrent`/`Classification` env; YAML workflow DSL; JSON-schema draft-07 validation |
| HTTP | `gin-gonic/gin v1.10.0`, `rs/cors` |
| Torznab | bespoke XML (`internal/torznab`, `encoding/xml`) |
| WebUI embed | Go `embed` + `?frontend` cookie switch (`internal/webui/httpserver.go`) between Angular + React SPAs |
| Config | `go-viper/mapstructure/v2` + `iancoleman/strcase` + `go-playground/validator/v10` + custom resolver chain (`internal/config`) |
| DI | `go.uber.org/fx v1.23.0` (every module has an `*fx` sub-package) |
| Telemetry | `prometheus/client_golang v1.20.5`, `go.uber.org/zap`, pyroscope godeltaprof |
| TMDB client | `go-resty/resty/v2 v2.16.5`, `hashicorp/golang-lru/v2` (cache), `golang.org/x/time` (rate limit) |

**Already committed in `bitmagnet-rs/` (prod sidecars, verified against crates.io 2026-05-28, toolchain 1.95.0):**
tokio (`full`), **tonic 0.14** (+ `tonic-prost`/`tonic-prost-build` split), prost 0.14,
tracing + tracing-subscriber, thiserror 2 / anyhow 1, serde + **rmp-serde** + zstd (the
MessagePack→ZSTD blob format mirrors `internal/blobmigration`), **sqlx 0.9** (postgres,
runtime-tokio, tls-rustls, macros, json), **tantivy 0.26**, clap 4, **arrow/parquet 55**,
**duckdb 1.3** (bundled, feature-gated). No axum, async-graphql, DHT, CEL, or reqwest
crate has been chosen yet — those are the open decisions this doc informs.

**Governing principle:** prefer consistency with the choices already in prod. sqlx (not an
ORM), tonic/prost for all gRPC, tokio as the sole runtime, thiserror/anyhow for errors.
That biases several recommendations below (e.g. sqlx over sea-orm, axum over actix because
axum shares tokio/tower/hyper with tonic).

---

## 1. DHT / BitTorrent protocol — **BUILD on primitives, do NOT buy a crawler**

### What bitmagnet actually needs
This is *not* a torrent client. The crawler needs exactly three protocol capabilities:
1. **BEP-5** DHT node (k-table, `find_node`/`get_peers`/`ping` responder) — to be a well-connected participant.
2. **BEP-51 `sample_infohashes`** — the discovery primitive. bitmagnet *samples* infohashes from other nodes' storage rather than passively sniffing `get_peers`. `internal/dhtcrawler/sample_infohashes.go` is the heart of the crawler.
3. **BEP-9 (ut_metadata) over BEP-10 extension protocol** — to fetch the info-dictionary (name + file list) for a discovered infohash, validated against the infohash. `internal/protocol/metainfo/metainforequester`.

Plus bencode (used pervasively), and v2 infohash support (`infohash-v2`, migration `00023`).

### Candidate approaches

**A. `mainline` crate (pubky/mainline).** Latest **v7.0.0, 2026-06-09**; runs in production
for the Pkarr/Pubky projects; actively maintained. **Verdict: not sufficient alone.** It
implements BEP-5, BEP-42 (security), BEP-43 (read-only), BEP-44 (mutable/immutable storage
— the Pkarr use case). It has **no BEP-51 `sample_infohashes` and no BEP-9/ut_metadata** — I
confirmed this against the current docs.rs listing. Its data model is oriented at
put/get of arbitrary values, not swarm-metadata crawling. Usable as a *reference* and for
the routing-table/bencode-wire layer, but the discovery + metadata paths — the whole point
— are absent. ([docs.rs/mainline](https://docs.rs/mainline), [crates.io/crates/mainline](https://crates.io/crates/mainline/1.5.0))

**B. `rbit` (pure-Rust BitTorrent building blocks).** Covers BEP-3/5/6/9/10/11/14/15/23/52 —
notably **it does have BEP-9 ut_metadata and BEP-10 extension protocol and BEP-5 DHT**,
which is more than `mainline`. **Verdict: too immature to depend on.** First published
**2025-12-02**, latest **0.2.2 (2025-12-03)**, ~**26 downloads/month**, one dependent
(`dht-crawler`), self-described as "low-level building blocks rather than a complete
client," and — critically — its BEP list **skips BEP-51** (jumps BEP-23 → BEP-52). Worth
watching and mining for code, but betting the crawler on a two-month-old, single-maintainer,
26-downloads/month crate is a supply-chain and correctness risk. ([lib.rs/crates/rbit](https://lib.rs/crates/rbit), [docs.rs/rbit](https://docs.rs/rbit))

**C. `librqbit` component crates + `rustydht-lib` as references, build the crawler in-tree.**
`librqbit` (the rqbit client, [github.com/ikatson/rqbit](https://github.com/ikatson/rqbit)) is the
most mature/active Rust BitTorrent codebase and has a real DHT + peer/extension-protocol +
ut_metadata implementation, but it is a *downloader* — its DHT is tuned for peer-finding for a
known infohash, not for BEP-51 sampling or being a high-throughput crawler, and it is not
published as clean standalone crates for this use. `rustydht-lib` is BEP-5 only and stale
(last meaningful activity ~2022, "work in progress"). **Verdict: reference material, not a
dependency.** ([lib.rs/crates/rqbit](https://lib.rs/crates/rqbit), [github.com/raptorswing/rustydht-lib](https://github.com/raptorswing/rustydht-lib))

### Recommendation
**Build the DHT crawler in-tree on thin, well-maintained primitives — mirroring what the Go
side already did over anacrolix.** Concretely:
- **Bencode: `bendy`** — the clear 2025 leader. Very active (0.6.1 2025-11-10, 0.6.0
  2025-09-17, 0.5.0/0.4.0 Sept 2025), enforces **canonical encoding** (load-bearing: the
  infohash is SHA-1 of the *canonical* bencoded info-dict, so canonicalization is a
  correctness requirement, not a nicety), optional serde. Fallback `bt_bencode` (serde-JSON-like,
  no_std). ([crates.io/crates/bendy](https://crates.io/crates/bendy), [github.com/P3KI/bendy](https://github.com/P3KI/bendy))
- **UDP DHT wire (BEP-5/BEP-51):** implement the message codec + k-bucket routing table +
  responder in a `bitmagnet-dht` crate over tokio `UdpSocket`. This is the bulk of the port
  but it is *mechanical* — the Go `internal/protocol/dht` is already a from-scratch
  implementation, so there is a byte-exact reference and a parity-test corpus
  (`msg_test.go`, `nodeaddr_test.go`). Port those fixtures as Rust parity tests.
- **BEP-9/BEP-10 metadata fetch:** implement the extension-protocol handshake + 16 KiB
  ut_metadata block reassembly + infohash verification over a tokio TCP peer connection.
  Again mechanical; `rbit`/`librqbit` are readable references. Metadata blocks are 16 KiB,
  last block short, reject on missing — well specified in BEP-9. ([bittorrent.org BEP-9](https://www.bittorrent.org/beps/bep_0009.html))
- **v2 infohash:** the Go side uses `anacrolix/torrent/types/infohash-v2` (SHA-256); in Rust
  use `sha2` + the standard BEP-52 merkle-root computation — no crate needed.

**Risk callout:** this is the single highest-effort, highest-risk subsystem of the whole
rewrite. There is *no* buy option that covers BEP-51 sampling + BEP-9 fetch. Budget it as
the tentpole. Mitigant: the Go implementation is bespoke and fixture-tested, so it is a
spec you can port and diff against, not a black box. The 3–6x first-service rewrite-cost
multiplier ([JJetBrains/Nexumo migration notes](https://medium.com/@Nexumo_/go-vs-rust-for-services-a-decision-tree-1b798f9f7fd3)) lands hardest here.

---

## 2. GraphQL server — **async-graphql, code-first, schema pinned by SDL diff**

### Constraint
Two SPAs (Angular + React) consume the *existing* gqlgen schema — introspection shape,
custom scalars (e.g. `DateTime`, `Void`, `Hash`), enums, and any unions/interfaces must stay
compatible or both frontends break. gqlgen is **schema-first** (SDL is source of truth,
resolvers generated). The naive hope is "feed the same `.graphql` into a Rust schema-first
codegen and get byte-identical output."

### Candidates
**A. `async-graphql`.** The de-facto modern Rust GraphQL server: native async, integrates
with axum/actix/poem, Apollo Federation v2, subscriptions, `#[derive(SimpleObject)]` /
`#[Object]` macros, and — importantly — it can **export SDL** from the code-first schema
(`schema.sdl()`). Actively maintained, the recommended choice for new Rust GraphQL projects
in 2025/2026. **It is code-first, not schema-first** — there is no mature "generate resolver
stubs from `schema.graphql`" tool for Rust equivalent to gqlgen or graphql-code-generator
(those are TS/JS/Java-only). ([async-graphql.github.io](https://async-graphql.github.io/), [requestly GraphQL-in-Rust](https://requestly.com/blog/graphql-rust/))

**B. `juniper`.** Older (2016), stable, but synchronous-first (async via wrappers), **no
federation**, and its schema-first story is a third-party `juniper-from-schema`. Lags
async-graphql on every axis that matters here. **Verdict: no.** ([github.com/graphql-rust/juniper](https://github.com/graphql-rust/juniper))

**C. Preserve gqlgen unchanged behind a Go shim.** Keep the Go GraphQL layer alive during a
phased migration and rewrite everything *behind* it. Viable as a *transition* tactic, not an
end state.

### Recommendation
**`async-graphql`, code-first, with the existing SDL demoted to a golden-file contract.**
Because Rust has no schema-first resolver codegen, you rebuild the types with derive macros,
then **enforce parity by diffing `schema.sdl()` against the committed gqlgen `schema.graphql`
in CI** (normalize ordering/formatting first). This inverts gqlgen's direction (code produces
SDL instead of SDL producing code) but gives an exact, automated compatibility gate — which
is what actually protects the two SPAs. Custom scalars map to `async-graphql`'s `#[Scalar]`
impls; introspection parity falls out of SDL parity. Budget real effort for the resolver
surface (bitmagnet's `internal/gql/resolvers` is large) — this is mechanical but voluminous.
Pair async-graphql with **axum** (§6) since both sit on tower/hyper alongside tonic.

---

## 3. PG access + migrations — **stay on sqlx (already chosen); continue goose via refinery or run goose-as-tool**

### Access layer
The Go side is GORM + gorm-gen (typed queries) + a large hand-written query surface + a
dbresolver read/write split. The Rust workspace **already uses sqlx 0.9** in
`bitmagnet-db`, `bitmagnet-parquet`, `bitmagnet-shadow` — the keyset-pagination streamers
(`stream_torrents_with_files`, `stream_changed_torrents`) are already raw sqlx and the shadow
harness deliberately mirrors the sidecar's SQL predicate. That is a decision already made and
proven in prod.

**Candidates:** sqlx (raw SQL, compile-time-checked, async-first, no relationship magic);
sea-orm 2.0 (ActiveRecord, built on sqlx, can scaffold entities from an existing DB — Jan
2026 GA, "production-ready," but heaviest compile + runtime "magic"); diesel-async
(most extensive compile-time checking, **query pipelining ~20% throughput win**, but async
bolted-on and a sync-shaped API). All three are production-grade in 2026.
([byteiota ORM 2026](https://byteiota.com/rust-orms-2026-sqlx-vs-diesel-vs-seaorm-comparison/), [rustify ORM 2026](https://rustify.rs/articles/rust-sqlx-vs-diesel-vs-seaorm-2026), [SeaORM 2.0](https://techpreneurr.medium.com/seaorm-vs-sqlx-the-rust-orm-war-ends-with-seaorm-1-0-2026-production-ready-87e219ae6fab))

**Recommendation: stay on sqlx.** (a) Consistency with the shipped sidecars; (b) bitmagnet's
value is a *large hand-tuned query surface* (bloom-filter joins, tsvector search, budgeted
counts, blob storage) that maps to raw SQL far better than to an ActiveRecord abstraction —
GORM was already used mostly as a thin mapper, not for relationship graphs; (c) sqlx's
compile-time query checking gives most of the safety with none of the ORM weight. Replace
gorm-gen's typed queries with sqlx `query_as!` macros. If diesel-async's **query pipelining**
proves necessary for the crawler's high write throughput, it is the only lever the others
lack — flag it as a *targeted, measured* option for the persist hot-path only, not a
wholesale ORM switch.

### Migrations — the goose continuity problem
25 existing migrations live in `migrations/*.sql` under **`pressly/goose`**, tracked in the
**`goose_db_version`** table on a live production DB (500Gi PG). The rewrite must **not**
re-run or renumber history. No Rust migrator natively reads `goose_db_version`.

**Options:**
- **Keep goose as an external tool.** goose ships a standalone binary; the Rust app shells to
  it (or the deploy playbook runs it) and the Rust process only *asserts* the expected
  version at boot. Zero schema-table risk, zero re-port of 25 files. **Recommended for the
  transition** — it fully decouples the migration engine from the language rewrite.
- **Rust migrator respecting the table.** sqlx-migrate and refinery both use their *own*
  version tables (`_sqlx_migrations`, `refinery_schema_history`) and would not see goose's
  history — adopting either means a one-time reconciliation (seed their table as if all 25
  ran) and freezing goose. refinery is the more mature ([rust-db/refinery](https://github.com/rust-db/refinery)); sqlx's
  built-in `migrate!` is simplest if you standardize on it. Reversible up/down supported via
  `sqlx migrate add -r`. **Choose this only once the Go tree is fully retired.**

**Recommendation:** **run goose-as-a-tool during and after the rewrite** (it is a build-time/
deploy-time concern, not a runtime one); revisit a Rust-native migrator only as a
post-cutover cleanup, and if so, refinery, seeding its history table to match goose. Note the
`.sql`-hash line-ending gotcha (`.gitattributes` → LF) if you ever move to sqlx's macro.

---

## 4. Job queue — **port the existing PG table semantics; do NOT adopt a queue framework**

### What bitmagnet has
A bespoke PG-backed queue (`internal/queue/{manager,server,handler,prometheus}`) with
**priority ordering** (migration `00015_queue_priority`), a **duplicate-key dedup fix**
(`00019_queue_fix_duplicate_key`), and its own metrics. The queue *schema* is part of the
production database and is coupled to the crawler/processor/classifier pipeline semantics.

### Candidates
- **`sqlxmq`** — LISTEN/NOTIFY + advisory locks over sqlx; mature-ish but the maintainer has
  signalled limited activity. ([github.com/Diggsey/sqlxmq](https://github.com/Diggsey/sqlxmq))
- **`apalis` (+ `apalis-pgmq`/`apalis-postgres`)** — the most feature-rich/actively maintained
  Rust background-job framework in 2026 (workers, middleware, board UI). But `apalis-pgmq`
  imposes the **PGMQ extension** and its own `apalis_pgmq` schema + BYTEA payloads — a
  *different* storage contract than bitmagnet's tables. ([lib.rs/crates/apalis-pgmq](https://lib.rs/crates/apalis-pgmq))
- **`pgmq`** — solid, but again a prescribed schema/extension, not bitmagnet's.
- **Port the existing table + `SELECT ... FOR UPDATE SKIP LOCKED` loop in sqlx.**

### Recommendation
**Port the existing queue schema and its claim/priority/dedup semantics directly onto sqlx
with `FOR UPDATE SKIP LOCKED`.** Adopting apalis/pgmq would mean *migrating the production
queue table to a new contract* on a live 500Gi DB for no functional gain — pure risk. The Go
queue is a few hundred lines of well-understood SQL; re-expressing it in sqlx keeps the
schema, the metrics names, and the pipeline coupling intact. `SKIP LOCKED` is the canonical
Postgres pattern and is exactly what a hand-rolled Rust queue uses ([kerkour PG job queue](https://kerkour.com/rust-job-queue-with-postgresql), [aminediro PG job queue](https://aminediro.com/posts/pg_job_queue/)). Keep LISTEN/NOTIFY for
wakeups (sqlx `PgListener`). Reserve apalis as a reference for worker/middleware ergonomics
only.

---

## 5. Classifier — **cel-interpreter, keep rules files byte-compatible; rhai as the fallback if CEL diverges**

### What bitmagnet has
A CEL-based rules engine (`google/cel-go v0.23.2`): a YAML **workflow DSL**
(`classifier.core.yml`) whose `if_else`/`find_match`/`set_content_type`/`delete`/`run_workflow`
actions embed **CEL expressions** evaluated against a protobuf `Torrent`/`Classification`
environment, with a custom `Lists()` extension, `flags.*`/`keywords.*`/`extensions.*`
pseudo-namespaces (implemented via dotted variable/constant tricks in `cel_env.go`),
`ext.Strings`, and **JSON-schema draft-07** validation of the workflow files. Example
expression: `torrent.files.map(f, f.extension in extensions.audio ? f.size : - f.size).sum() > 50*mb`.
**User-authored `classifier.yml` files are a public contract** — they should keep working.

### Candidates
- **`cel-interpreter` (cel-rust, clarkmcc fork).** The most production-ready Rust CEL: parser
  (lalrpop) + tree-walk interpreter, published as `cel-interpreter` (v0.10.x as of mid-2026,
  wheels via the Python binding shipped 2026-05-12), a FOSDEM 2026 talk on it, active repo.
  **Verdict: the right target, with caveats.** Risks: (a) CEL *conformance gaps* vs cel-go —
  bitmagnet leans on `ext.Strings`, list `.map/.filter/.sum`, protobuf message types, and
  custom functions; each must be checked against cel-rust's supported subset and custom-function
  API. (b) The protobuf-typed environment: cel-go binds `bitmagnet.Torrent` message types
  directly; in Rust you bind CEL to the prost-generated types or to a `serde_json::Value`-style
  dynamic map — a real adapter to write. ([github.com/cel-rust/cel-rust](https://github.com/cel-rust/cel-rust), [crates.io/crates/cel-interpreter](https://crates.io/crates/cel-interpreter), [FOSDEM 2026 CEL-in-Rust](https://fosdem.org/2026/schedule/event/DBGZAU-rust-cel/))
- **`rhai`.** Mature, embeddable Rust scripting engine — but **not CEL**. Adopting it means
  *rewriting every rules file* in a new syntax, breaking the public contract. Only sane if
  cel-rust proves unable to reach parity.
- **Custom port of the CEL subset.** bitmagnet uses a bounded CEL feature set; a hand-written
  evaluator for exactly that subset is possible but re-derives what cel-rust already does.

### Recommendation
**Target `cel-interpreter`, and gate it with a parity corpus before committing.** The moment
you can, take the Go classifier's test fixtures (`classifier_test.go`, `json_schema_test.go`)
and the `classifier.core.yml` workflow and build a **Go-vs-Rust classification parity harness**
(same torrent inputs → same `Classification` output). This is culturally aligned with how this
repo already ships (shadow-comparators, parity gates — see `bitmagnet-shadow`, tokenizer/
extension parity fixtures). If cel-rust clears the corpus (custom functions + list ops +
string ext + the `flags/keywords/extensions` namespace trick reproduced via its variable
provider), ship it. If a specific operator is missing, contribute it upstream or shim it as a
custom function — far cheaper than abandoning CEL. Keep the JSON-schema draft-07 validation
using `jsonschema` (the maintained Rust crate). **Do not switch to rhai unless cel-rust is
proven inadequate** — the rules-file compatibility contract dominates this decision.

---

## 6. HTTP / API layer — **axum + quick-xml + rust-embed**

### Web framework
gin on the Go side. In Rust the real choice is axum vs actix-web vs poem.
**Recommendation: `axum`.** It sits on **tower/hyper**, the same stack tonic uses — so the
gRPC sidecars, the GraphQL server, and the REST/Torznab endpoints share one connection/
middleware model, and async-graphql ships first-class axum integration. This maximizes reuse
with the existing tonic 0.14 investment. (actix is fast but a parallel ecosystem; poem is
async-graphql's sibling but less widely deployed.) ([requestly GraphQL-in-Rust](https://requestly.com/blog/graphql-rust/))

### Torznab XML
Bespoke `encoding/xml` on the Go side. **Recommendation: `quick-xml` with the `serialize`
feature.** ~10x faster than `serde-xml-rs`, active, serde derive for the response structs, and
handles the attribute-vs-element distinction Torznab needs (`$text`/`$value`). The Torznab
schema is small and stable; a set of `#[derive(Serialize)]` structs reproduces the caps/
categories/results XML. If you later add an axum XML extractor, `axum-xml-up` wraps quick-xml.
Watch: serde's JSON-centric model can't express *every* XML nuance, so a few responses may
need manual `Writer` calls — Torznab is simple enough that this is minor.
([docs.rs/quick-xml](https://docs.rs/quick-xml/latest/quick_xml/), [github.com/tafia/quick-xml](https://github.com/tafia/quick-xml))

### WebUI embedding + `?frontend` cookie switch
Go uses `embed` + `resolveFrontend(query, cookie, config, reactEnabled)` to serve Angular or
React from one binary (`internal/webui/httpserver.go`, `frontendCookieName`).
**Recommendation: `rust-embed`** to bake both SPA build outputs into the binary at compile
time (feature-gate the React bundle exactly like the Go `-tags webuireact` build tag), and a
small axum handler replicating the cookie/query resolution + `Set-Cookie` logic. rust-embed
is the standard, mature choice for compile-time static assets and supports feature-gated
folders. Port the `resolveFrontend` precedence (query > cookie > config, warn-if-react-disabled)
verbatim as an axum layer.

---

## 7. Telemetry / config / DI

### Prometheus
Go: `prometheus/client_golang`, metric names like `persisted_total`, `torrents_dropped_total`,
`dht_bootstrap_nodes` — subsystem-prefixed snake_case. **Preserving exact metric names is a
dashboard/alert contract** (this homelab has kube-prometheus-stack + Grafana dashboards +
Loki alert rules keyed on them).
**Candidates:** the **`prometheus`** crate (tikv/rust-prometheus) — direct analog of
client_golang, explicit `Opts{name, help}`, `CounterVec`/`HistogramVec`, a `Registry`;
strict Prometheus-format validation. vs **`metrics`** (metrics-rs) facade + a prometheus
exporter — ergonomic but adds an abstraction and its bridge (`metrics-prometheus`) **forbids
dot-namespaced names** and only maps a subset of types. vs **`prometheus-client`** (official
OpenMetrics, compile-time-typed labels via `EncodeLabelSet`).
**Recommendation: the `prometheus` crate.** It is the 1:1 port target for client_golang —
same mental model, same explicit naming, so you can reproduce every metric name/label/bucket
exactly and keep dashboards green. bitmagnet's names are already snake_case so the "no dots"
constraints of the alternatives are moot, but the direct port is lowest-risk. Use
`prometheus-client` only if you want compile-time label typing and are willing to re-verify
every exposition string. ([docs.rs/prometheus](https://docs.rs/prometheus), [prometheus/client_rust](https://github.com/prometheus/client_rust), [oneuptime Rust+Prometheus 2026](https://oneuptime.com/blog/post/2026-01-07-rust-prometheus-custom-metrics/view))

### Logging / tracing
Go: zap. **Recommendation: `tracing` + `tracing-subscriber`** — already the workspace default;
structured, async-aware, and the ecosystem standard. (pyroscope godeltaprof profiling has a
`pyroscope-rs` analog if continuous profiling is still wanted — lower priority.)

### Config — the strcase env-var contract
This is the subtle one. Go resolves config through `mapstructure/v2` + **`iancoleman/strcase`**
+ `validator/v10` + a custom resolver chain (`internal/config/config.go`,
`configresolver`), which defines the **exact env-var → nested-key mapping** users depend on
(e.g. `dht_crawler.scaling_factor` ↔ some `BITMAGNET_...` env var via a specific case walk).
**Reproducing that mapping byte-for-byte is a user-facing contract**, not an implementation
detail.
**Candidates:** `figment` (provider-stack: defaults → file → `Env::prefixed(...)` → CLI;
strong types, profiles, best-in-class for merging serde-shaped sources incl. clap args) vs
`config-rs` (mature builder, ordered layering, prefix env source). ([docs.rs/figment](https://docs.rs/figment), [config-rs](https://docs.rs/config/latest/config/), [Leapcell Rust config](https://leapcell.io/blog/flexible-configuration-for-rust-applications-beyond-basic-defaults))
**Recommendation: `figment`**, because it composes clap (CLI) + env + file into one typed
struct — closest to the Go resolver-chain shape — **but the strcase mapping is the real work,
not the crate.** Neither crate reproduces `iancoleman/strcase`'s exact walk automatically:
port the case-conversion logic explicitly (there is a `heck` crate for case conversions, but
validate it against `iancoleman/strcase`'s output on the actual config keys) and **build a
parity test that asserts every documented env var resolves to the same node as the Go
binary.** Treat the env-var contract like the GraphQL SDL and the classifier rules — a golden
file, diffed in CI.

### DI
Go: uber `fx` (every subsystem has an `*fx` module, runtime graph). Rust has no reflection, so
there is no `fx` equivalent — and you don't want one.
**Recommendation: explicit constructor wiring at a composition root** (a single `main`/`app`
module that constructs each service passing its dependencies as arguments). This is the
idiomatic, zero-cost, most-readable Rust approach and is explicitly favored over `shaku`-style
macro DI for a codebase this size. `shaku` (compile-time, macro-driven) is the only real DI
library but its "magic" buys little here and costs clarity. **Explicit wiring, no DI crate.**
([chesedo manual DI in Rust](https://chesedo.me/blog/manual-dependency-injection-rust/), [Rust forum shaku vs manual](https://users.rust-lang.org/t/comparing-dependency-injection-libraries-shaku-nject/102619))

---

## 8. TMDB client + misc

Go: `go-resty/resty/v2` (HTTP), `hashicorp/golang-lru/v2` (LRU cache), `golang.org/x/time/rate`
(rate limiting) for the TMDB metadata client.
**Recommendation:**
- **HTTP: `reqwest` 0.12** (`json` feature) — the default, mature async client, integrates with
  tokio/rustls (matches the sqlx tls-rustls choice).
- **Rate limiting: `governor`** — production-grade token-bucket/GCRA, in-process (no Redis),
  the standard analog to `x/time/rate`. Wire it via `reqwest-ratelimit`/`reqwest-middleware`,
  or call the limiter directly. ([governor via reqwest](https://docs.rs/reqwest-rate-limit), [oneuptime Rust rate limiting 2026](https://oneuptime.com/blog/post/2026-01-07-rust-rate-limiting/view))
- **Caching: `moka`** (async, high-performance in-memory cache) as the LRU analog; or
  `http-cache-reqwest` with `manager-moka` if you want HTTP-semantics caching keyed on
  Cache-Control. Note both buffer in memory (fine for small TMDB JSON). ([http-cache-reqwest](https://lib.rs/crates/http-cache-reqwest), [moka](https://crates.io/crates/moka))
- Levenshtein/fuzzy match (`agnivade/levenshtein`, `facette/natsort`, `mozillazg/go-unidecode`)
  → `strsim`, `natord`/manual natural-sort, `deunicode`/`unidecode` crates respectively — all
  small, mature, mechanical swaps.

---

## 9. Cross-cutting: rewrite accelerators & risks

**Accelerators**
- **gqlgen SDL → async-graphql parity is a *gate*, not codegen.** No Rust schema-first
  resolver generator exists; instead export `schema.sdl()` from the code-first async-graphql
  schema and diff it against the committed `schema.graphql` in CI. Automatable, exact, protects
  both SPAs. (§2)
- **The Go tree is a spec, not a black box.** The highest-risk subsystems (DHT, classifier,
  tokenizer, blob format) already ship with **fixture corpora and shadow-comparators**
  (`bitmagnet-shadow`, `tokenizer_fixtures.json`, `file-extension-fixtures.json`, the
  MessagePack `.blob` byte-parity tests). This repo's engineering culture is already
  parity-gate-driven — every rewritten subsystem gets a Go-vs-Rust differential harness. That
  is the single biggest de-risker and it is *already established practice here*.
- **Prod sidecars prove the stack.** tonic/sqlx/tantivy/duckdb/arrow are already live on HEL1
  (search, filesearch, pathsearch, parquet, shadow crates) — the runtime, DB, and gRPC
  choices are validated, not speculative.

**Risks / watch-items**
- **DHT is the tentpole** — no buy option covers BEP-51 + BEP-9; it is a from-scratch port
  (§1). Expect the 3–6x first-service rewrite multiplier to land here.
- **CEL conformance** — cel-rust may not cover 100% of the operators/extensions cel-go gives
  bitmagnet; parity corpus must run *before* committing, with upstream contributions as the
  cheap fix (§5).
- **Contract surfaces that silently break users if wrong:** the GraphQL SDL (§2), the
  classifier rules-file syntax (§5), the config env-var strcase mapping (§7), the Prometheus
  metric names (§7), and the goose migration history (§3). Each should be a golden-file diff in
  CI, mirroring the existing parity-gate culture.
- **Compile time** — the full workspace (tonic + sqlx macros + async-graphql + duckdb bundled +
  tantivy) will have heavy cold builds; keep the duckdb/tantivy-heavy crates feature-gated and
  workspace-split as they already are.

**Footprint expectations (favorable).** The Go binary is ~64 MB (statically linked runtime +
GC). Rust equivalents typically run **2–5 MB** (musl) and **50–80 MB RAM** vs Go's
**100–320 MB** for comparable services, with flat tail latency under load (no GC pauses) —
Tokio work-stealing scheduling. Against the current 32Gi pod limit (usually far under),
the rewrite should *reduce* steady-state memory and give more predictable latency; the
crawler's allocation-heavy hot path is exactly where Rust's deterministic memory management
helps most. Measure per-subsystem against the Go baseline before cutover (identical load
tests, p50/p95/p99 + RSS) — the repo already runs this discipline. ([Rust vs Go 2026 benchmarks](https://www.danilchenko.dev/posts/rust-vs-go/), [markaicode Rust vs Go microservices 2025](https://markaicode.com/rust-vs-go-performance-benchmarks-microservices-2025/), [Nexumo Go vs Rust decision tree](https://medium.com/@Nexumo_/go-vs-rust-for-services-a-decision-tree-1b798f9f7fd3))

---

## Recommendation summary

| Subsystem | Go today | Rust recommendation | Verdict |
|---|---|---|---|
| DHT/BEP-5/51/9 | anacrolix + bespoke | **build in-tree** on `bendy` + tokio UDP/TCP; mine `rbit`/`librqbit` as refs | BUILD — tentpole risk |
| Bencode | anacrolix/torrent/bencode | **`bendy`** (canonical) | Low risk |
| GraphQL | gqlgen (schema-first) | **`async-graphql`** code-first + SDL-diff CI gate | Medium (volume) |
| PG access | GORM + gorm-gen + pgx | **`sqlx`** (already chosen) + `query_as!` | Low — decided |
| Migrations | goose | **run goose-as-tool**; refinery only post-cutover | Low |
| Queue | bespoke PG | **port schema** onto sqlx `FOR UPDATE SKIP LOCKED` | Low |
| Classifier | cel-go | **`cel-interpreter`** + parity corpus (rhai only if it fails) | Medium — conformance |
| HTTP | gin | **`axum`** (shares tower/hyper w/ tonic) | Low |
| Torznab XML | encoding/xml | **`quick-xml`** serialize | Low |
| WebUI embed | Go embed + cookie | **`rust-embed`** + ported `resolveFrontend` | Low |
| Metrics | client_golang | **`prometheus`** crate (exact-name port) | Low |
| Logging | zap | **`tracing`** (already default) | Low |
| Config | mapstructure+strcase+fx | **`figment`** + explicit strcase port + env-var parity test | Medium — contract |
| DI | uber fx | **explicit composition root**, no DI crate | Low |
| TMDB client | resty + x/time + lru | **`reqwest` + `governor` + `moka`** | Low |
