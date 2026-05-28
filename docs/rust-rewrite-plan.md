# Bitmagnet Rust Rewrite Plan: Tantivy Integration & Phased Port

**Status:** Investigation complete, planning phase  
**Date:** 2026-05-28  
**Branch:** `feat/rust-rewrite-plan`

---

## Executive Summary

Rewrite bitmagnet from Go to Rust in phases, starting with a **Tantivy search sidecar** that replaces PostgreSQL's tsvector full-text search. Each phase is independently valuable — the project can stop at any checkpoint and still deliver meaningful improvements.

**Key architectural decision:** Use a **Rust gRPC sidecar** (not tantivy-go FFI). The tantivy-go bindings lack numeric fields, faceted search, aggregations, and are pinned to Tantivy 0.22 (upstream is 0.26). A gRPC sidecar provides full Tantivy access and establishes the first Rust component of the port.

**Timeline:** ~30 weeks for full port. First value at week 4 (search sidecar), first production impact at week 10 (Tantivy serving 100% of search).

---

## Architecture Overview

### Current State (Go + PostgreSQL)
```
DHT Crawler (Go) → Processor → PG INSERT ON CONFLICT
                        ↓
                   Classifier (CEL/YAML) → UpdateTsv() → PG tsvector + GIN
                        ↓
GraphQL/Torznab → query.GenericQuery → tsv @@ tsquery → Results + Facets
```

### Target State (Rust + PostgreSQL + Tantivy)
```
DHT Crawler (Rust/tokio) → Processor → PG INSERT (SQLx)
                               ↓
                          Classifier (Rhai/YAML) → Tantivy IndexWriter
                               ↓
GraphQL/Torznab (axum) → Tantivy Searcher → BM25 Results + Facets
                               ↓
                          PG for relational data (hydration, content metadata)
```

### Transition State (Shadow Mode)
```
                  ┌─────────────────────────────────────┐
                  │         bitmagnet (Go)               │
                  │                                      │
  DHT Crawler ──→ │  Processor ──→ persist() ──→ PG DB   │
                  │       │                     (tsv)    │
                  │       └──→ TantivyIndexer ─(gRPC)──→│──→ Tantivy Sidecar (Rust)
                  │                                      │        │
  GraphQL/API ──→ │  SearchRouter ──→ PG Search          │        └──→ Tantivy Index
                  │       │                              │              (on disk)
                  │       └──→ Tantivy Search ─(gRPC)──→│
                  │       └──→ Comparator (log diffs)    │
                  └─────────────────────────────────────┘
```

---

## Why Not tantivy-go FFI?

| Feature Needed | tantivy-go | Rust Sidecar |
|---|---|---|
| Text search with field boosting | ✅ | ✅ |
| Numeric fields (seeders, size sorting) | ❌ | ✅ |
| Date fields (published_at) | ❌ | ✅ |
| Faceted search (14 facet types) | ❌ | ✅ |
| Aggregations (facet counts) | ❌ | ✅ |
| Range queries | ❌ | ✅ |
| Latest Tantivy (0.26) | ❌ (0.22) | ✅ |
| Build complexity | CGo + Rust FFI | Standard Rust binary |
| Reusability for Rust port | Throwaway | Foundation |

---

## Tantivy Index Schema

### Field Mapping (PG → Tantivy)

| PG Source | tsvector Weight | Tantivy Field | Type | Flags | Query Boost |
|---|---|---|---|---|---|
| info_hash | A | `info_hash` | Bytes | STORED + INDEXED | — (exact match) |
| torrent name | A | `torrent_name` | Text | STORED + INDEXED | 4.0 |
| content title | A | `content_title` | Text | STORED + INDEXED | 4.0 |
| original title | A | `original_title` | Text | INDEXED | 4.0 |
| release year | B | `release_year` | U64 | FAST + INDEXED | 2.0 |
| video resolution | C | `video_resolution` | Text | FAST + INDEXED | 1.5 |
| video source/codec | C | `video_source`, `video_codec` | Text | FAST + INDEXED | 1.5 |
| genres | D | `genres` | Text | INDEXED | 0.5 |
| file paths | D | `file_paths` | Text | INDEXED | 0.5 |
| content_type | — | `content_type` | Facet | — | — (filter only) |
| seeders | — | `seeders` | U64 | FAST | — (sort only) |
| leechers | — | `leechers` | U64 | FAST | — (sort only) |
| size | — | `size` | U64 | FAST + INDEXED | — (sort/filter) |
| files_count | — | `files_count` | U64 | FAST | — (sort only) |
| published_at | — | `published_at` | Date | FAST + INDEXED | — (sort/filter) |
| languages | — | `languages` | Text | FAST | — (facet) |
| file_extensions | — | `file_extensions` | Text | FAST | — (facet) |

### Custom Tokenizer (Critical Path)

Must replicate Go's `TokenizeFlat()` (`internal/database/fts/tokenizer.go`):
1. Unicode transliteration via `deunicode` crate (Rust equivalent of go-unidecode)
2. Lowercase normalization
3. CJK: each character becomes a separate token
4. Split on non-alphanumeric boundaries
5. Remove tokens > 255 bytes

---

## Phased Implementation

### Phase 0: Infrastructure Setup (Week 1)

| Task | Description | Estimate |
|---|---|---|
| Rust workspace | Cargo workspace with crates: proto, model, search, common | 1 day |
| Protobuf schema | search.proto (IndexDoc, Search, Facets RPCs) | 1 day |
| CI/CD | Docker multi-stage build, cargo test/clippy/fmt in CI | 1 day |

### Phase 1: Tantivy Search Sidecar MVP (Weeks 1-4)

| Task | Description | Estimate | Depends On |
|---|---|---|---|
| Index schema | All field types mapped from PG model | 2 days | Workspace |
| Custom tokenizer | Replicate TokenizeFlat() in Rust | 3-5 days | — |
| gRPC server | tonic server: IndexDoc, BatchIndex, Delete, Search, Facets | 3 days | Proto |
| Query translation | PG tsquery → Tantivy BooleanQuery with field boosts | 3 days | Schema, Tokenizer |
| Faceted search | 14 facet types from bitmagnet | 3 days | Schema |
| Aggregations | Facet counts, range aggregations | 2 days | Facets |
| Index management | Merge policy, warmers, graceful shutdown | 2 days | gRPC server |
| Backfill CLI | Stream from PG, batch-index (~80 min for 48M docs) | 2 days | All above |

### Phase 2: Shadow Mode Go Integration (Weeks 5-7)

| Task | Description | Estimate | Depends On |
|---|---|---|---|
| gRPC client | Go client for Tantivy sidecar | 1 day | Phase 1 |
| Dual-write | Async index after PG commit in persist.go | 2 days | gRPC client |
| SearchRouter | Shadow/canary/tantivy_only modes | 3 days | gRPC client |
| Comparator | Jaccard similarity, RBO, top-1 match | 2 days | SearchRouter |
| Prometheus metrics | Latency ratio, jaccard/RBO histograms, index lag | 1 day | Comparator |
| Configuration | search.engine, tantivy.address, shadow settings | 1 day | — |
| fx DI wiring | Wire into Uber fx module system | 1 day | All above |

### Phase 3: Shadow Mode Validation (Weeks 8-10)

| Task | Description | Estimate |
|---|---|---|
| Production backfill | Index 48M torrents from PG | 1-2 days |
| Shadow mode run | 2-3 weeks collecting comparison metrics | 2-3 weeks |
| Tokenizer tuning | Fix divergences found during shadow mode | 1-3 days |
| Quality gate | Jaccard > 0.7 @ top-20 for 95% of queries | — |

### Phase 4: Tantivy Cutover (Weeks 11-12)

| Task | Description | Estimate |
|---|---|---|
| Canary rollout | 5% → 50% → 100% over 2 weeks | 2 weeks |
| Remove PG tsvector writes | Stop computing tsvector in Go | 1 day |
| Drop GIN indexes | Reclaim ~14 GB disk | 1 day |

**GO/NO-GO: Week 10** — Is Tantivy stable at 100%? If yes, proceed to Rust port.

### Phase 5: Classifier Rust Port (Weeks 13-18)

| Task | Description | Estimate |
|---|---|---|
| YAML parser | Port workflow YAML parsing (serde_yaml) | 3 days |
| Expression engine | cel-rust or Rhai replacing CEL | 5-7 days |
| Classifier actions | Content type detection, date parsing, video attrs | 5-7 days |
| TMDB integration | reqwest HTTP client with rate limiting | 3 days |
| Golden file testing | 10K samples, assert Rust output matches Go | 3 days |
| Differential testing | Dual-execute in production, log divergence | 2 days |
| Cutover | Rust consumes queue_jobs directly via SQLx | 2 days |

**GO/NO-GO: Week 16** — Rust classifier matches Go output with < 0.1% divergence?

### Phase 6: DHT Crawler Rust Port (Weeks 19-24)

| Task | Description | Estimate |
|---|---|---|
| DHT protocol | BEP-5/9/33/51 on tokio UDP | 5-7 days |
| K-table | BTreeMap-based Kademlia routing | 3-5 days |
| Bloom filter | bitvec-based dedup filter | 2 days |
| MetaInfo requester | TCP metadata fetch (BEP 9) | 3-5 days |
| Batch persist | tokio channels replacing Go channels | 3-5 days |
| Parallel comparison | Both crawlers running, compare discovery rate | 1-2 weeks |
| Cutover | Disable Go crawler, Rust handles all DHT | 2 days |

**GO/NO-GO: Week 24** — Rust pipeline stable? Can stop here (valid end state).

### Phase 7: API Server Rust Port (Weeks 25-30, Optional)

| Task | Description | Estimate |
|---|---|---|
| GraphQL schema | async-graphql matching gqlgen schema | 5-7 days |
| Torznab API | axum XML handler | 2-3 days |
| Query builder | Port 827-line query.go (hardest single task) | 7-10 days |
| API conformance | Captured response fixture testing | 3-5 days |
| Cutover | Full Rust stack | 2 days |

---

## Rust Crate Structure

```
bitmagnet-rs/
├── Cargo.toml                    # workspace root
├── proto/bitmagnet/
│   ├── search.proto
│   ├── classifier.proto
│   └── common.proto
├── crates/
│   ├── bitmagnet-proto/          # generated protobuf (tonic)
│   ├── bitmagnet-model/          # domain models (Torrent, Content, TorrentContent)
│   ├── bitmagnet-db/             # SQLx database access
│   ├── bitmagnet-search/         # Tantivy index + gRPC server
│   │   ├── src/
│   │   │   ├── index.rs          # index management
│   │   │   ├── schema.rs         # field definitions
│   │   │   ├── query.rs          # query translation
│   │   │   ├── facets.rs         # faceted search
│   │   │   └── tokenizer.rs      # custom tokenizer matching TokenizeFlat()
│   ├── bitmagnet-classifier/     # workflow engine (Rhai for expressions)
│   ├── bitmagnet-dht/            # DHT crawler (tokio UDP)
│   ├── bitmagnet-api/            # axum + async-graphql + torznab
│   └── bitmagnet-common/         # shared utilities
└── docker/
    ├── Dockerfile.search
    └── docker-compose.yml
```

---

## Risk Matrix

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Tokenizer mismatch → search divergence | Medium | **High** | Custom Tantivy tokenizer replicating TokenizeFlat(); exhaustive testing with real torrent names (CJK, Cyrillic) |
| CEL → Rhai/cel-rust incompatibility | Medium | **High** | Evaluate both engines in week 13; golden file testing on 10K+ samples |
| Tantivy index > 74 GB | Medium | Medium | Monitor during backfill; reduce STORED fields if needed |
| Memory pressure (PG + Tantivy + Go + Rust) | Medium | Medium | Tantivy uses mmap (OS-managed); deploy on 64GB+ RAM |
| Rust learning curve | Medium | Medium | Search sidecar (greenfield) builds expertise before porting |
| PG schema drift during dual-ownership | Low | **High** | Single migration tool; schema validation in CI |

---

## Go/No-Go Decision Points

| Week | Checkpoint | Criteria | If No-Go |
|---|---|---|---|
| 4 | Search MVP | Backfill completes, index size within estimate | Tune schema, reduce fields |
| 10 | Tantivy cutover | Jaccard > 0.7 @ top-20 for 95%, no latency regression | Extend shadow mode, tune tokenizer |
| 16 | Classifier port | Rust classifier < 0.1% divergence from Go | Keep Go classifier, investigate edge cases |
| 24 | DHT port | Rust crawler discovery rate matches Go ± 5% | Keep Go crawler (valid end state) |

---

## Key Integration Points in Go Source

| Component | File | Line(s) | Purpose |
|---|---|---|---|
| tsvector build | `internal/model/torrent_contents.go` | 66-106 | UpdateTsv() — weights A/B/C/D |
| Content tsvector | `internal/model/content.go` | 83-108 | Content.UpdateTsv() |
| Tokenizer | `internal/database/fts/tokenizer.go` | — | TokenizeFlat() — must replicate in Rust |
| tsquery builder | `internal/database/fts/tsquery.go` | 9-24 | AppQueryToTsquery() |
| DB persist | `internal/processor/persist.go` | 59-110 | Hook point for dual-write |
| Search execution | `internal/database/query/query.go` | 617-619, 646-647 | ts_rank_cd and tsv @@ tsquery |
| Search interface | `internal/database/search/search.go` | 9-15 | Central search interface |
| 14 facet types | `internal/database/search/facet_*.go` | — | All facet implementations |
| DHT crawler | `internal/dhtcrawler/crawler.go` | 61 | Start() — 15 concurrent pipelines |
| Classifier | `internal/classifier/classifier.core.yml` | — | CEL/YAML workflow definitions |
| DI root | `internal/app/appfx/module.go` | 38-76 | Uber fx module composition |

---

## Shadow Mode Configuration

```yaml
search:
  engine: postgres  # postgres | shadow | canary | tantivy
  tantivy:
    enabled: false
    address: "unix:///var/run/bitmagnet/tantivy.sock"
    shadow:
      sample_rate: 1.0
      log_discrepancies: true
      jaccard_threshold: 0.7
    canary:
      percentage: 0.0
      sticky_sessions: true
    backfill:
      batch_size: 10000
      concurrency: 4
```

---

## Shadow Mode Metrics

**Per-query (structured log):**
- Query string, PG latency, Tantivy latency
- Result counts, Jaccard similarity @ top-20/50
- Rank-Biased Overlap (RBO p=0.9), top-1 match

**Prometheus:**
- `bitmagnet_search_shadow_jaccard_histogram`
- `bitmagnet_search_shadow_rbo_histogram`
- `bitmagnet_search_shadow_latency_ratio`
- `bitmagnet_search_tantivy_index_lag_seconds`
- `bitmagnet_search_tantivy_doc_count`
- `bitmagnet_search_tantivy_index_size_bytes`

**Phase transition thresholds:**
- Shadow → Canary: Jaccard > 0.7 @ top-20 for 95% of queries, RBO > 0.8
- Canary → Full: No p99 latency regression, error rate < 0.1%

---

## External References

- [bitmagnet source](https://github.com/bitmagnet-io/bitmagnet) — Go, MIT license
- [Tantivy](https://github.com/quickwit-oss/tantivy) — Rust, MIT license
- [tantivy-go](https://github.com/anyproto/tantivy-go) — Go FFI bindings (rejected, see above)
- [Database analysis](./bitmagnet-database-analysis.md) — 367 GB PG analysis
- Discord Go→Rust migration, Vinted ES→Vespa shadow traffic, InfluxData strangler fig pattern
