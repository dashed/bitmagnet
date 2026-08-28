# 01 — Go Codebase Inventory (for the planned Rust rewrite)

**Repo:** `github.com/bitmagnet-io/bitmagnet`, fork branch `alberto/my-fork` (studied at the `rust-rewrite-plan-20260710` worktree).
**Upstream merge-base:** `2b9e8ea` (`author/main` = `bitmagnet-io/bitmagnet`). The fork is **+218 commits / 656 files / +161k −2.5k lines** ahead of upstream.
**Scope:** map the *entire* Go application as rewrite-relevant inventory. No code changes. Companion docs: `02-rust-assets.md` (existing Rust crates), `03-ecosystem-mapping.md`.

**Total Go:** ~88.4k LOC non-test, ~105.8k incl. tests. **BUT the single generated file `internal/gql/gql.gen.go` is 26,470 LOC** and `internal/database/dao/*.gen.go` is ~7.6k LOC — so hand-written, rewrite-relevant Go is closer to **~50k LOC**. Existing Rust (`bitmagnet-rs/`) is ~31.6k LOC already covering the search sidecars.

---

## 0. TL;DR for the rewrite planner

- **The DHT protocol engine is hand-rolled and anacrolix-free except for bencode + info-dict parsing/hashing.** It ports to Rust cleanly (tokio UDP + a bencode crate), but the info-dict v1/v2 (BEP-52) hashing has *no direct Rust equivalent* and must be re-verified byte-for-byte.
- **The classifier is a bespoke YAML DSL whose leaf conditions are CEL expressions** (`google/cel-go`). This is the single hardest semantic port: it needs either a Rust CEL engine or a re-modeling of `classifier.core.yml`, plus faithful reproduction of the regex/keyword release-name parsing tables.
- **Persistence is GORM + gorm-gen (95% generated DAO) over Postgres**, with goose migrations 00001–00025. The schema — not the Go — is the durable contract. `sqlx` already covers much of this in `bitmagnet-db`.
- **The search stack is a fork-built multi-tier system**: PG (authoritative) + 3 Rust sidecars (Tantivy main-search, L3 pathsearch, L2 DuckDB filesearch) already implemented in `bitmagnet-rs/`. The Go-only pieces are the **tier-routing decision logic, the L1 blob-refine composer, the feature-flag drop-gates, and the shadow comparator.**
- **fx (uber-go/fx) is load-bearing** — the whole app is one DI graph with lifecycle hooks. A Rust rewrite replaces this with explicit wiring / a runtime like tokio + manual startup ordering.
- **Wire contracts that must be preserved:** PG schema+migrations, GraphQL schema (2 webui consumers + Hermes), Torznab XML (Prowlarr/Sonarr/Radarr), the 3 gRPC sidecar protos, ~40 Prometheus metric names, ~25 env-var config keys, `/status` + `/livez`.

---

## 1. Subsystem map

Per-package non-test LOC (from `wc -l`), largest first. Generated code flagged.

| Package | LOC | Purpose | Key inbound → outbound |
|---|---|---|---|
| `internal/gql` | 29,992 | GraphQL API (gqlgen). **`gql.gen.go` = 26,470 generated.** | httpserver → search, dao, health, queue, processor |
| `internal/database` | 15,974 | Persistence: GORM, gorm-gen DAO, PG search query builder, migrations | everything → Postgres |
| `internal/search` | 10,001 | **Fork.** Tier router + L1 composer + L2/L3/Tantivy clients + shadow | gql, torznab, processor → sidecars, dao |
| `internal/protocol` | 5,915 | DHT KRPC (`dht/` 5,920 incl test) + metainfo BEP-9/10 (`metainfo/`) | dhtcrawler → UDP/TCP sockets |
| `internal/model` | 5,589 | Domain structs (15 gorm-gen models + hand-written release-parsing enums) | everyone |
| `internal/classifier` | 4,155 | Content classifier: YAML DSL + CEL engine | processor → cel-go, model, tmdb |
| `internal/blobmigration` | 2,445 | **Fork.** torrent_files→blob dual-write + backfill + consistency | dhtcrawler, processor, cli → dao |
| `internal/dhtcrawler` | 1,621 | Crawler pipeline (discovery → metadata → persist) | worker → protocol, dao, processor |
| `internal/app` | 1,589 | fx root + CLI commands (urfave/cli) | main → all fx modules |
| `internal/torznab` | 1,427 | Torznab/Newznab XML API (Prowlarr/*arr contract) | httpserver → search |
| `internal/tmdb` | 1,367 | TMDB API client (content metadata attach) | classifier → resty |
| `internal/health` | 1,025 | Health checker; `/status` + `/livez` (fork liveness) | httpserver; **fork: peer/federated health** |
| `internal/protobuf` | 933 | `bitmagnet.proto` (Torrent/Classification msgs) + transformer | classifier ⇄ CEL (NOT a sidecar RPC) |
| `internal/processor` | 828 | Queue consumer → classifier → persist torrent_content | queue → classifier, search, dao |
| `internal/queue` | 799 | PG-backed job queue (poll/lock/retry/backoff) | processor, blobmigration, dhtcrawler |
| `internal/config` | 671 | Reflection config walker; env/yaml/default layering | all fx modules |
| `internal/importer` | 550 | Bulk import endpoints | cli |
| `internal/logging` | 472 | zap logging setup | all |
| `internal/httpserver` | 448 | gin server + pluggable `Option` mount seam + CORS | app → gql, torznab, webui |
| `internal/metrics` | 292 | DB-query metric clients for the GraphQL dashboard (NOT prometheus) | gql |
| `internal/worker` | 275 | Worker registry (enable/disable/start/stop) | app, dhtcrawler, httpserver |
| `internal/blocking` | 270 | Blocklist manager (banned hashes) | dhtcrawler, processor |
| `internal/keywords` | 227 | Keyword-glob DSL → compiled regex (release-name matching) | classifier, model |
| `internal/webui` | 209 | **Fork dual-frontend.** Embeds Angular + React; `?frontend` cookie switch | httpserver |
| `internal/concurrency` | 202 | Worker-pool primitives (BufferedConcurrentChannel, BatchingChannel, KeyedLimiter) | dhtcrawler, search |
| `internal/dev`, `telemetry`, `lexer`, `maps`, `regex`, `slice`, `bloom`, `lazy`, `validation`, `version` | <200 each | utilities; `telemetry/` = prometheus registry + pprof; `lexer`+`regex` = release-name tokenizing | — |

### 1.1 DHT crawler (`internal/protocol/dht`, `internal/protocol/metainfo`, `internal/dhtcrawler`)

**Almost entirely hand-rolled.** anacrolix libs are used only for: `anacrolix/torrent/bencode` (serialization — the one deep dependency), `metainfo.Info` + v1/v2 hashing, `peer_protocol` constants, and `anacrolix/dht/v2` **only for its bootstrap host list**. The KRPC server, Kademlia routing trie, responder, BEP-51 sampling, and BEP-9 metadata leech are all reimplemented in-repo.

- **KRPC wire types** — `dht/msg.go:163` (`Msg`, `Bep51Return`, compact node/infohash codecs via reflection at `nodeinfo.go:110`), `dht/error.go`, `dht/nodeaddr.go`, `dht/scrape.go` (BEP-33 256-byte bloom).
- **UDP transaction server** — `dht/server/server.go:288`: single blocking `SOCK_DGRAM`/`AF_INET` socket via raw `x/sys/unix` syscalls (`socket_unix.go`), read loop, per-query crypto-random 2-byte TID map, response matched by **TID + source-addr** (anti-forgery, `server.go:177`). **IPv4-only.** Port 3334, 4s query timeout.
- **Typed client** — `dht/client/server_adapter.go`: `Ping/FindNode/GetPeers/GetPeersScrape/SampleInfoHashes`.
- **Inbound responder** — `dht/responder/responder.go:157`: bitmagnet answers queries too (real DHT node); md5 announce token (`responder.go:120`).
- **Kademlia routing** — `dht/ktable/`: custom binary trie with bucket-splitting (`btree/node.go:412`), two keyspaces (nodes/hashes) + reverse addr index, single RWMutex, k=80.
- **BEP-9/10 metadata leech** — `metainfo/metainforequester/requester.go:409`: hand-rolled TCP BT handshake + LTEP extension handshake + `ut_metadata` piece reassembly (16 KiB pieces), 10 MiB DoS guard, banning checks (min name len 8, min size 1024, valid UTF-8).
- **Info-dict verify** — `metainfo/parse.go:37`: rejects bytes whose hash ≠ requested infohash under **both** SHA-1 (v1) and truncated SHA-256 (v2/BEP-52).
- **Concurrency** — custom pools in `internal/concurrency/`: `BufferedConcurrentChannel` (buffered chan + `semaphore.Weighted`), `BatchingChannel`, `KeyedLimiter` (per-IP `x/time/rate` in an expirable LRU). Pool sizes scale off `ScalingFactor=10` (`dhtcrawler/factory.go:96`).
- **Pipeline** (`dhtcrawler/`): discovery (BEP-51 sample) → infohash triage (DB-existence routing, `infohash_triage.go:22`) → get_peers → metadata leech → persist. **The processor boundary is a DB row insert, not a Go interface**: `persist.go` writes torrents + a delayed `queue_job` row transactionally; the processor polls the queue.

### 1.2 Queue / processor (`internal/queue`, `internal/processor`)

- **PG-backed queue.** Table `queue_jobs` (migrations 00012/00015/00019): `id`, `fingerprint = sha256(queue+payload)` (dedup), `queue`, `status enum(pending/processed/retry/failed)`, `payload jsonb`, `retries/max_retries`, `run_after`, `priority`, `deadline`, `archival_duration`. Unique partial index `(fingerprint) WHERE status IN (pending,retry)` prevents duplicate in-flight jobs.
- **Dequeue** (`queue/server/server.go:193`): transaction, `WHERE queue=? AND status IN (pending,retry) AND run_after<=now() ORDER BY (status=retry), priority, run_after` with **`FOR UPDATE SKIP LOCKED`** (`clause.Locking{Strength:"UPDATE", Options:"SKIP LOCKED"}`).
- **Polling**: per-handler `time.Ticker` (default 30s) + `semaphore.Weighted(Concurrency)`; self-chains a tight loop while jobs exist. **LISTEN/NOTIFY listener is disabled** (commented out `server.go:42`; crawler jobs carry a delay so notify is moot).
- **Retry/backoff** (`server.go:227`, `helpers.go`): Sidekiq-style `retries^4 + 15 + rand(30)*retries + 1` seconds; deadline check; panic recovery with file:line. GC deletes processed/failed past `ran_at + archival_duration` (default 7d).
- **Processor** (`processor/processor.go:55`): two queues (`process_torrent`, `process_torrent_batch`). Loads torrents (preloading Hint+Sources but **NOT Files** — Files come from the `files_data` blob via `AfterFind`), runs the classifier per-torrent in parallel goroutines, persists `TorrentContent`/`Content`/`TorrentTag` in one transaction, deletes on `ErrDeleteTorrent`. After commit, fire-and-forget **dual-write to the Tantivy sidecar** (`persist.go:138`, no-op if indexer nil). Batch handler keyset-paginates and fans out `process_torrent` jobs.

### 1.3 Classifier (`internal/classifier`, `internal/lexer`, `internal/keywords`, `internal/regex`)

- **DSL = custom YAML tree + CEL leaf conditions.** Not pure CEL. Default rules embedded at `internal/classifier/classifier.core.yml` (280 lines) via `//go:embed` (`source_core.go:7`). Six sections: `workflows`, `flag_definitions`, `flags`, `keywords`, `extensions`, `$schema`.
- **Vocabulary** (`features.go:26`): conditions `and/not/or/expression`; actions `set_content_type`, `delete`, `find_match`, `if_else`, `run_workflow`, `add_tag`, `parse_date`, `parse_video_content`, `attach_{local,tmdb}_content_by_{id,search}`, `unmatched`.
- **Load order** (`source_provider.go`): core embed → XDG `~/.config/bitmagnet/classifier.yml` → CWD `./classifier.yml` → config-injected keywords/extensions/flags. User-overridable without recompiling.
- **Compile → eval**: `classifier.go:88` compiles each workflow's action tree + CEL programs (type-checked to bool output, `condition_expression.go:23`). `runner.go:20` runs the named workflow, binding `torrent`/`result`/`flags.*` protobuf into CEL.
- **The battle-tested release-name parsing** (hardest to reproduce): `keywords/parser.go:227` (keyword-glob DSL → regex via `hedhyw/rex`), `regex/util.go` (Unicode NFD/NFC tokenizing), `classifier/parsers/video.go:221` (title/year/episode extraction), and the `internal/model/` enum+regex libraries (`episodes_parser.go`, `video_resolution.go`, `video_codec.go` incl. release-group extraction, `language.go` + `languages.csv`).
- **Boundary to processor** is narrow (`classifier.go:16`): `Compiler.Compile(Source) → Runner`; `Runner.Run(ctx, workflow, flags, Torrent) → Result`.
- **CEL is used ONLY here** — `cel_env.go:124` builds a custom env registering `torrent`/`result` as protobuf types plus `keywords.*`/`extensions.*`/`flags.*` constants.

### 1.4 Persistence (`internal/database`, `internal/model`, migrations)

- **GORM + gorm-gen.** `dao/` = 8,053 LOC, **~95% generated** (15 `*.gen.go`, each `// Code generated by gorm.io/gen. DO NOT EDIT.`). Generator config: `internal/database/gen/gen.go:468` (introspects a live DB, maps SQL→Go domain types, declares all relations). Hand-written: `dao/torrent_tags.go`, `dao/budgeted_count.go`.
- **`exclause/`** = vendored `gorm-extra-clause-plugin` + added MATERIALIZED CTE support (CTEs/UNION/INTERSECT/EXCEPT). **NOT an upsert helper** — upserts use stock `clause.OnConflict`.
- **`fts/`** = hand-rolled Postgres `tsvector`/`tsquery` modeling in Go (`Tsvector` implements `driver.Valuer`/`Scanner`; `AppQueryToTsquery` has its own lexer/tokenizer). App writes tsvectors (migration 00006 dropped the DB-generated columns).
- **`cache/`** = forked `mgdigital/gorm-cache/v2` query cache (TTL 10m, EaserEnabled=false due to zero-results bug).
- **`postgres/`** = pgxpool + `stdlib.OpenDBFromPool`; **`migrations/`** = goose runner.
- **Models** (`internal/model/`, 5,589 LOC): 15 gorm-gen structs + hand-written companions (`torrents.go` `AfterFind` decodes `files_data`→`Files`, enums via go-generate, `date/languages/episodes/duration/maybe/null`).

### 1.5 GraphQL API (`internal/gql`, `graphql/`)

- **Schema**: `graphql/schema/*.graphqls`, 10 files / 830 LOC / **107 defs (54 type, 30 input, 16 enum, 7 scalar; 0 unions/interfaces)**. `enums.graphqls` is *generated from Go* by `internal/gql/enums/gen/genenums.go`.
- **Top-level Query**: `version`, `workers`, `health`, `queue`, `torrent`, `torrentContent`. Leaves: `TorrentContentQuery.{search, fileSearch*, pathTypeahead*, collapsePaths*}` (`*`=fork), `TorrentQuery.{files, listSources, suggestTags, metrics}`, `QueueQuery.{jobs, metrics}`, `HealthQuery.{status, checks}`.
- **Top-level Mutation**: `TorrentMutation.{delete, putTags, setTags, deleteTags, reprocess}`, `QueueMutation.{enqueueReprocessTorrentsBatch, purgeJobs}`.
- **gqlgen** v0.17.64: exec → `gql.gen.go` (26,470 LOC generated), models autobound to `internal/gql/gqlmodel` + `internal/model` (mostly hand-written domain types, only 508 LOC of true generated DTOs in `gen/model.gen.go`). Config: `internal/gql/gqlgen.yml`.
- **NO dataloaders.** N+1 avoided by eager hydration in one SQL round-trip (`TorrentContentCoreJoins`, `Hydrate*`). The one field resolver that could N+1 (`FileSearchItem.torrentContent`) is batch-hydrated up front (`file_search.go:239`).
- **37 hand resolvers** in `internal/gql/resolvers/` (886 LOC). Resolver struct (`resolver.go:23`) injects `Dao`, `Search`, `Workers`, `Checker`, `QueueManager`, `Processor`, `BlockingManager`, plus fork fields `Pathsearch *pathsearch.Composer` and `FileSearch filesearch.Client`.
- **Tier routing lives in the resolver, not a central router**: `gqlmodel/torrent_content.go:196` — try L3 pathsearch (if eligible/healthy) → else PG.

### 1.6 Torznab (`internal/torznab`) — external wire contract

- Torznab/Newznab XML API that **Prowlarr/Sonarr/Radarr depend on** — must be byte-preserved. Hardcoded namespaces (`result.go:24`), RSS 2.0, `torznab:attr` pairs, magnet enclosures.
- Caps advertise `search`/`tv-search`/`movie-search`/`music-search`/`book-search` (`profile.go:44`). Categories tree is generated (`categories.gen.go`, Newznab 2000/3000/4000/5000/6000/7000/8000).
- **Sibling adapter over the same PG search backend** (`internal/database/search`), NOT over GraphQL: `adapter/adapter.go:21` translates Torznab params → `search` options → `search.Search.TorrentContent`. Total-count disabled.

### 1.7 HTTP server + webui embedding (`internal/httpserver`, `internal/webui`)

- **gin**, built as an fx worker with OnStart/OnStop graceful shutdown. **Pluggable mount seam**: each subsystem provides an `httpserver.Option{Key, Apply(*gin.Engine)}` into fx group `http_server_options` (`server.go:119`). Keys: `graphql` (`POST/GET /graphql` + playground), `torznab` (`GET /torznab/*any`), `webui`, `cors` (`rs/cors`).
- **Dual-frontend (fork)**: Angular (`webui/embed.go`, always) + React (`webui-react/embed.go`, **build-tag `webuireact` gated**, stub otherwise). `GET /` reads `?frontend` query + `bitmagnet-frontend` cookie (precedence query > cookie > `WEBUI_DEFAULT_FRONTEND`), 301-redirects to `/webui` (angular) or `/app/` (react). React handler 404s missing hashed assets (fail-loud on stale deploys); Angular does SPA index fallback.

### 1.8 Search stack (`internal/search`, `internal/database/search`) — fork-built

Four surfaces (see §4 of the search deep-dive):
- **L1** = in-process exact-refine over the `files_data` blob (`pathsearch/composer.go:764`, `refine.go`) — source of truth after the `torrent_files` DROP.
- **L2** = filesearch DuckDB-on-Parquet sidecar (`FileSearchService`, :50052), Go client `filesearch/client.go`. Per-file grain.
- **L3** = pathsearch Tantivy path-bag sidecar (`PathSearchService`, :50053), Go **composer** `pathsearch/composer.go`. Torrent-grained ngram recall.
- **Tantivy main-search** (`SearchService`, unix socket) — shadow/canary only, not yet a serving path (Phase-6 TODO).
- **Router** (`search/router/router.go:106`) is a `search.Search` decorator concerning ONLY the Tantivy shadow/canary; it always serves PG and fires sampled background comparisons.
- **PG query builder** (`internal/database/search/`, 4,707 LOC) is the authoritative engine: `search_torrent_content.go:24`, 9-facet aggregation, cursor pagination, criteria builders, JSONB `file_extensions` filter, `FileCounts` probe.

### 1.9 Config (`internal/config`)

- Reflection walker: each feature registers a typed struct spec under a string key (`configfx.NewConfigModule[T]`). `config.go:122` walks struct fields → `strcase.ToSnake` keys; env key = `UPPER(join(path,"_"))` so `search` + `SampleRate` → **`SEARCH_SAMPLE_RATE`**.
- **Precedence** (`configfx/module.go`): `EXTRA_CONFIG_FILES` yaml > env > `./config.yml` > XDG yaml > struct default.
- **Fork float-coercion fix** (`coerce.go:33`): upstream had no float case → `SEARCH_SAMPLE_RATE=0.05` crash-looped startup; regression test `coerce_float_test.go`.

### 1.10 Telemetry / health / fx / worker / CLI

- **Prometheus** registry `telemetry/prometheus/registry.go:22` (namespace `bitmagnet`), collects fx group `prometheus_collectors`, served `/metrics`. pprof at `httpserver/pprof.go`.
- **Health** (`internal/health/`): `GET /status` (full checker, 200/503) + `GET /livez` (**fork liveness**, always 200). `peer_config.go` = fork federated peer health.
- **fx is load-bearing** (`app/appfx/module.go:41`): ~26 sub-modules + two root decorators (`migrations.NewDecorator` auto-runs migrations on startup; `searchfx.Decorator` wraps PG search in the Tantivy Router). Heavy lifecycle use — gRPC client close hooks, background pollers (`registerDocCountReporter`, `registerPathsearchHealthReporter`), `lazy.Lazy[T]` for deferred construction/cycle-breaking.
- **CLI** (urfave/cli/v2, `app/cli/cli.go`): `worker run/list`, `classifier`, `config`, `process`, `reprocess`, `blobmigration`, `migrate`.
- **Worker registry** (`worker/worker.go:49`): map of named workers each wrapping an `fx.Hook`; enable/disable/start/stop, injected via fx groups.

### 1.11 TMDB (`internal/tmdb`)

Resty-based TMDB API client feeding `attach_tmdb_content_by_{id,search}` classifier actions. External HTTP dependency; a rewrite needs an equivalent client. (Recent upstream fix `e31b30d7` upgraded go-resty to fix TMDB.)

---

## 2. External contracts a rewrite MUST preserve

These are the hard boundaries. Breaking any of them breaks a deployed consumer, dashboard, or the on-disk database.

### 2.1 Postgres schema + migrations
- **goose**, version table default **`goose_db_version`** (no custom name; `internal/database/migrations/migrator.go:46`). Migrations embedded via `//go:embed *.sql` (`migrations/migrations.go`), **auto-run on startup** (`decorator.go`). Sequenced by `NNNNN_` filename.
- A rewrite must keep the same goose version table and never re-order/renumber applied migrations. New migrations continue at 00026+.
- **Migration ledger** (fork-specific flagged 🔶):

| # | One-line |
|---|---|
| 00001 | Base schema: pg_trgm, torrents, torrents_torrent_sources, torrent_files, content(+attributes/collections), torrent_contents, metadata_sources, all indexes + generated tsv/extension |
| 00002 | `FilesStatus` enum + `torrents.files_status`; drop `single_file` |
| 00003 | `torrent_tags` (info_hash,name PK, CHECK, trigram gist) |
| 00004 | updated_at indexes |
| 00005 | `bloom_filters(key,bytes)` |
| 00006 | Drop generated tsv columns; re-add app-written `tsv` + GIN |
| 00007 | `torrent_hints`; move title/ids to content |
| 00008 | Data fix: delete pre-0.1.0 adult movies (no Down) |
| 00009 | `key_values(key,value)` |
| 00010 | `budgeted_count()` plpgsql (EXPLAIN-estimate vs exact count) |
| 00011 | btree_gin composite GIN indexes |
| 00012 | `queue_job_status` enum + `queue_jobs` + `queue_announce_job()` pg_notify trigger |
| 00013 | Split `torrent_pieces` out of torrents |
| 00014 | Data: `book`→`ebook` |
| 00015 | `queue_jobs.priority`; rebuild dequeue index |
| 🔶 00016 | `torrents.files_count` |
| 🔶 00017 | Drop bloom cols; denormalize seeders/leechers/published_at onto torrent_contents |
| 🔶 00018 | torrent_contents.size/files_count + coalesce ordering indexes |
| 🔶 00019 | Dedup retry/pending jobs; replace unique index with `(fingerprint) WHERE status IN (pending,retry)` |
| 🔶 00020 | `bloom_filters.bytes bytea` → PG **large object** `oid` |
| 🔶 00021 | **Blob storage**: `torrents.files_data BYTEA` + `file_extensions JSONB` + `torrent_file_summary` table |
| 🔶 00022 | `CREATE INDEX CONCURRENTLY` (NO TRANSACTION) jsonb_path_ops GIN on file_extensions |
| 🔶 00023 | **v2/hybrid (BEP-52)**: `info_hash_v1/v2/meta_version` + resumable batched backfill procedure (48.2M rows, NO TRANSACTION) |
| 🔶 00024 | **L1/L2/L3 follow contract**: `deleted_torrents` audit table + AFTER DELETE trigger + updated_at index |
| 🔶 00025 | `torrents_torrent_sources.seen_count` |

- **Blob dual-write state (current):** dual-write ON everywhere (crawler + processor write both `torrent_files` AND `files_data`/`torrent_file_summary`); reads still default to legacy `torrent_files`; **DROP not yet done.** All cutover behind `SEARCH_FEATURES_*` flags, **all default OFF**. `cleanup` refuses `DROP TABLE torrent_files` unless `DropCompatibleReads=true` at runtime (`blobmigrationcmd/command.go:833`).

### 2.2 GraphQL schema
- Consumers: **Angular webui, React webui** (both codegen'd), **federated health peers** (`resolvers/health_peer.go`), and **Hermes** (homelab agent queries the API). Enum values are generated *from Go domain enums* (`enums/gen/genenums.go`) — a rewrite must keep enum value strings identical.
- Custom scalars: `DateTime`, `Duration`, `Hash20`(→20-byte infohash), `Hash32`(→v2), `Year`, `Void`, `Date`.

### 2.3 Torznab XML API
- `GET /torznab/{profile}/api` — caps/search/tvsearch/moviesearch/music/book. Byte-format contract for the *arr ecosystem (§1.6). Category IDs, `torznab:attr` names (infohash, magneturl, size, seeders, leechers, peers, files, year, season, ep, tmdb, imdb), RSS date format `Mon, 02 Jan 2006 15:04:05 -0700`.

### 2.4 gRPC sidecar protos (`bitmagnet-rs/proto/bitmagnet/*.proto`, package `bitmagnet.v1`)
Go bindings generated into `internal/search/tantivy/pb/`. Three services:
- **`SearchService`** (Tantivy main-search): `IndexDocument`, `BatchIndex(stream)`, `DeleteDocument`, `Search`, `GetFacets`, `HealthCheck`. Central msg `TorrentDocument` (24 fields, PG→Tantivy field map). `HealthCheckResponse{status, doc_count, watermark_epoch}`.
- **`PathSearchService`** (L3): `PathCandidates`, `HealthCheck`. `PathCandidate{info_hash, score, sort_value}`; response `candidate_total` + `estimated`. `PathSearchHealth{status, doc_count, index_bytes, watermark_epoch, writable}`.
- **`FileSearchService`** (L2): `SearchFiles`, `CountFiles`, `Facets`, `Reload`, `HealthCheck`. `FileFilters`, `FilePagination{limit,cursor}` (keyset), `FileHealthCheckResponse{status, base_version, delta_version, delta_age_seconds}`.
- Separately, `internal/protobuf/bitmagnet.proto` is **NOT a sidecar RPC** — it's the classifier's `Torrent`/`Classification` message contract fed to CEL (`transformer.go`).

### 2.5 Prometheus metric names (dashboards + alerts depend on these)
Namespace `bitmagnet` unless noted. ~40 series:
- **Shadow** (`search_shadow_*`): `jaccard{k}`, `rbo`, `latency_ratio`, `top1_match_total{matched}`, `result_count_delta`, `comparisons_total`, `dropped_total`; `search_tantivy_doc_count`.
- **Pathsearch** (`search_pathsearch_*`): `doc_count`, `healthy`, `watermark_epoch_seconds`, `last_success_epoch_seconds`, `health_checks_total{result}`, `route_total{result}`, `refine_declined_oversized_total`, `refine_retained_capped_total`, `refine_deadline_capped_total`, `refine_shed_total`, `refine_agg_error_total`.
- **Queue** (`bitmagnet_queue_jobs_total{queue,status}`) — live GROUP BY collector.
- **DHT**: `dht_ktable_*`, `dht_server_{query_duration_seconds, query_success_total, query_error_total, query_concurrency, response_dropped_total}`, `dht_responder_*`, `meta_info_requester_*`.
- **Crawler**: `dht_crawler_{persisted_total, torrents_dropped_total}`.
- **Blob**: `blob_consistency_{checks_total, errors_total, last_check_at, last_error_at}`.

### 2.6 Env-var config surface (~25 vars set by deployment manifests)
Derived by the config walker (§1.9). Load-bearing families: `POSTGRES_*`, `DHT_SERVER_*`, `METAINFO_*`, `CLASSIFIER_*` (`DELETE_XXX`, `CONCURRENCY`, `WORKFLOW`), `WEBUI_DEFAULT_FRONTEND`, and the fork search surface:
- `SEARCH_ENABLED`, `SEARCH_ENGINE` (`postgres|shadow|canary|tantivy`), `SEARCH_ADDRESS`, `SEARCH_SAMPLE_RATE` (float!), `SEARCH_DUAL_WRITE_ENABLED` (default **true**), `SEARCH_SHADOW_MAX_CONCURRENT`, `SEARCH_SHADOW_TIMEOUT`, `SEARCH_CANARY_PERCENT`, `SEARCH_LOG_DISCREPANCIES`.
- `SEARCH_FILE_SEARCH_{ENABLED,ADDRESS,TIMEOUT,MAX_ROWS,ROUTE_TEXT}` (L2).
- `SEARCH_PATHSEARCH_*` / `SEARCH_PATH_*` (L3 — addresses, timeouts, oversample, budgets, health interval, concurrency).
- `SEARCH_FEATURES_{DROP_COMPATIBLE_READS, GATE_FILE_EXTENSIONS_JSONB, POPULARITY_SORT_DEFAULT, FILE_BROWSER_FROM_BLOB, FILE_SEARCH_ENABLED}` (all default OFF).

### 2.7 HTTP endpoints
`/graphql` (POST+GET playground), `/torznab/*`, `/metrics`, `/status`, `/livez`, `/webui`, `/app`, `/`, pprof. Queue tables are read by the GraphQL dashboard only (no external queue consumers). The `deleted_torrents` audit table + `key_values` blob-migration checkpoints are read by the Rust follow-loop sidecars.

---

## 3. Hairy-parts ranking (top 8 hardest to rewrite)

1. **Classifier DSL + CEL semantics** (`internal/classifier`). A bespoke YAML action tree with `google/cel-go` leaf expressions binding protobuf `torrent`/`result`/`flags`. No mature Rust CEL implementation → either port a CEL engine or re-model `classifier.core.yml` as native Rust logic and re-verify classification output torrent-by-torrent. **Highest semantic risk.**
2. **Release-name parsing tables** (`internal/keywords`, `internal/regex`, `classifier/parsers/video.go`, `internal/model/{episodes,video_*,language}.go` + `languages.csv`). Years of accreted scene-naming heuristics compiled from a keyword-glob DSL to Go regex. Must be reproduced essentially byte-for-byte or classification/search silently drifts.
3. **DHT protocol edge cases** (`internal/protocol/dht`, `metainfo`). Hand-rolled KRPC + Kademlia + BEP-9/10/33/51 with battle-tested anti-forgery (TID+addr matching), BEP-9 piece-size quirks, BEP-51 6-hour-backoff workaround, token management, banning. Porting is mechanical but the edge cases are only documented in code comments; regressions surface as slow crawl/poisoning.
4. **v1/v2 (BEP-52) hybrid dedup + info-dict hashing** (`metainfo/parse.go`, `dhtcrawler/persist.go`, migration 00023). Truncated-SHA-256 v2 hashing has no direct Rust equivalent; the hybrid-collapse dedup (first-one-wins, fail-open) and the resumable 48M-row backfill are subtle and correctness-critical.
5. **gorm-gen query surface + GORM-specific behaviors** (`internal/database/dao`, `exclause`, `fts`, `cache`). 8k LOC of generated fluent queries, custom CTE/tsvector clauses, `AfterFind` blob hydration, gorm-cache. `sqlx` (already used in `bitmagnet-db`) is lower-level → every query must be hand-translated and the `AfterFind` blob-decode-on-read behavior reproduced.
6. **L1 blob-refine composer** (`pathsearch/composer.go`, 5,982 LOC incl. tests). The chunked exact-refine pipeline with all the gate-7 memory/latency bounds (max-refine-files, retained-file-budget, route deadline, recall-preserving degradation, fail-loud accounting). Go-only, no Rust equivalent, and the correctness constraints are intricate (P0-1..P0-5 documented inline).
7. **gqlgen resolver semantics + schema binding** (`internal/gql`). 26k generated LOC, tight autobinding of GraphQL types to domain structs, `Omittable[T]` nullable-input wrappers, tier-routing embedded in resolvers, enum-schema-generated-from-Go. A Rust GraphQL lib (async-graphql) has different null/omittable semantics → schema must be re-derived and kept wire-identical for two webui consumers.
8. **fx lifecycle graph** (`internal/app`, ~26 modules). Startup ordering, root decorators (auto-migrate, search router wrap), OnStart/OnStop hooks (gRPC client close, background health pollers), `lazy.Lazy[T]` cycle-breaking. The wiring is implicit — a rewrite must reconstruct the dependency order and lifecycle explicitly.

*(Runners-up: the PG-backed queue's `FOR UPDATE SKIP LOCKED` + backoff + fingerprint dedup; the shadow comparator's RBO/Jaccard math; the config reflection walker's env-key derivation + float coercion; the Torznab XML byte-format.)*

---

## 4. Fork-specific deltas a rewrite must carry

The fork is +218 commits over upstream. `internal/search`, `bitmagnet-rs/`, `webui-react/`, and `internal/blobmigration` alone are +110k lines across 305 files (essentially all net-new). Enumerated additions:

- **Blob storage / dual-write** (`internal/blobmigration`, migrations 00016/00020/00021/00022): `SerializeFiles`/`DeserializeFiles` (MessagePack→ZSTD), `BuildFileSummary`, `files_data`+`file_extensions`+`torrent_file_summary`, resumable K-worker range backfill, consistency checker, `blobmigration start/status/pause/resume/verify/backfill-ext/cleanup` CLI. Goal: retire `torrent_files`.
- **BitTorrent v2 / hybrid (BEP-52)** (migration 00023, `metainfo/parse.go`, `dhtcrawler/persist.go`): `info_hash_v1/v2/meta_version`, v1-or-v2 hash verification, hybrid-collapse dedup (`torrents_dropped_total{reason="v2_duplicate"}`), v2-single-file classification fix.
- **DHT seen-count + safe source upsert** (migration 00025, `dhtcrawler/persist.go:451`): `seen_count` increment, `WHERE EXISTS` FK-safe raw upsert, `torrent_file_summary` denormalized rollup.
- **L1 follow contract** (migration 00024, `dhtcrawler`): `deleted_torrents` audit table + AFTER DELETE trigger + updated_at index — feeds the Rust sidecars' incremental follow loops.
- **L1 blob refine composer** (`internal/search/pathsearch/composer.go`): chunked exact-refine over blobs, `CollapsePaths`, gate-7 bounds, recall-preserving degradation.
- **L2 filesearch client + routing** (`internal/search/filesearch`): DuckDB sidecar gRPC client, hygiene, `fileSearch`/`pathTypeahead` GraphQL fields.
- **L3 pathsearch client + composer** (`internal/search/pathsearch`): Tantivy path-bag sidecar client, tier routing.
- **Tantivy main-search client + shadow/canary router** (`internal/search/tantivy`, `router`, `shadow`): dual-write indexer, `Router` decorator, shadow comparator (Jaccard/RBO/Top1), `SEARCH_SAMPLE_RATE`, `bitmagnet_search_shadow_*` metrics.
- **FIND-2 popularity sort** (`gqlmodel/torrent_content.go:514`): rewrites lone-relevance+query to `seeders DESC` to dodge ~49s `ts_rank_cd` walls (flag-gated).
- **Feature-flag drop-gate system** (`internal/database/search/featureflags.go`): `DropCompatibleReads`, `GateFileExtensionsJSONB`, `PopularitySortDefault`, `FileBrowserFromBlob`, `FileSearchEnabled` — atomic-pointer snapshot, static test asserting no served path reads `torrent_files`.
- **JSONB file_extensions facet/filter** (`criteria_torrent_file_extension.go`): `@>` containment gated by `GateFileExtensionsJSONB` (survives `torrent_files` DROP).
- **React webui + dual-frontend serving** (`webui-react/`, `internal/webui`): Vite/React/Mantine app at `/app` behind `webuireact` build tag, `?frontend` cookie switch, i18n (14 locales), full P0-P4 feature parity with Angular.
- **Federated health/worker merging** (`gql/resolvers/health_peer.go`, `health/peer_config.go`): multi-instance health aggregation across peers.
- **`/livez` liveness endpoint** (`health/factory.go:54`).
- **Config float coercion fix** (`config/coerce.go:33`): `SEARCH_SAMPLE_RATE` float parsing.
- **The entire `bitmagnet-rs/` Rust workspace** (8 crates, 31.6k LOC) — the sidecar servers the Go clients talk to; see `02-rust-assets.md`.

---

## 5. Quantitative summary table

Columns: **LOC** (non-test unless noted) · **Contract-criticality** (how much external stuff breaks if wire behavior changes) · **Rewrite difficulty** (S/M/L/XL) · **Existing Rust overlap** (in `bitmagnet-rs/`).

| Subsystem | LOC | Contract-criticality | Difficulty | Existing Rust overlap |
|---|---|---|---|---|
| DHT protocol (`protocol/dht`, `metainfo`) | 5,915 | Med (BitTorrent wire, not our API) | **XL** | None |
| DHT crawler pipeline (`dhtcrawler`) | 1,621 | Low (internal) | L | None |
| Classifier DSL + CEL (`classifier`, `lexer`, `keywords`, `regex`) | 4,700 | Med (classification stability) | **XL** | None |
| Release-name parsing tables (`model/video_*`, `episodes`, `language`) | ~2,200 | Med | L | None |
| Persistence GORM/dao (`database/dao`, `gen`, `exclause`, `fts`, `cache`) | 15,974 | **High** (PG schema) | L (mechanical, bulk) | `bitmagnet-db` (partial, sqlx) |
| Domain model (`model`) | 5,589 | High (blob format, enum discriminants) | M | `bitmagnet-model` ✅ (byte-parity blob) |
| Migrations (00001–00025) | ~25 files | **Critical** (goose version table, on-disk data) | M (keep as-is / reuse) | Reused verbatim (shared DB) |
| PG queue (`queue`) | 799 | Med (dashboard reads) | M | None |
| Processor (`processor`) | 828 | Low | M | None |
| Blob migration (`blobmigration`) | 2,445 | High (dual-write correctness) | L | `bitmagnet-parquet` (export side) + `bitmagnet-shadow` (parity) ✅ |
| GraphQL (`gql`, `graphql/`) | 29,992 (~3.5k hand) | **Critical** (2 webui + Hermes) | L | None |
| Torznab (`torznab`) | 1,427 | **Critical** (Prowlarr/*arr) | M | None |
| Search tier routing + composer (`search/router`, `pathsearch`, `filesearch`, `shadow`) | 10,001 | High (search behavior) | **XL** (composer L1-refine) | Sidecar *servers* ✅ (`bitmagnet-search`, `-filesearch`); **routing/composer/shadow Go-only** |
| PG search query builder (`database/search`) | 4,707 | High (authoritative results) | L | Partial (`bitmagnet-db` index rows) |
| gRPC proto bindings (`protobuf`, `tantivy/pb`) | 933 | High (sidecar wire) | S | `bitmagnet-proto` ✅ (discriminant-locked) |
| HTTP server + webui embed (`httpserver`, `webui`) | 657 | Med (routes, dual-frontend) | M | None |
| Config (`config`) | 671 | Med (env-var surface) | M | `bitmagnet-common` (partial) |
| Telemetry/metrics/health (`telemetry`, `metrics`, `health`) | 1,317 | High (dashboards/alerts + `/livez`) | M | None (metric names must match) |
| fx wiring + CLI (`app`, `worker`) | 1,864 | Low (internal) | L (implicit ordering) | None |
| TMDB (`tmdb`) | 1,367 | Low (external API client) | M | None |
| Blocking/importer/utilities | ~1,400 | Low | S–M | Partial (`bitmagnet-common`) |

**Bottom line:** the durable contract is the **Postgres schema + migrations + the 3 gRPC protos + GraphQL/Torznab wire formats + metric names** — a rewrite can share the live DB and the existing Rust sidecars. The genuinely from-scratch, high-difficulty Go-only work is: **DHT protocol engine, classifier DSL/CEL + release parsing, the L1 blob-refine composer + tier routing, the GraphQL/Torznab adapters, and the fx lifecycle.** The persistence and search-sidecar layers have substantial head-start in `bitmagnet-rs/` (see `02-rust-assets.md`).
