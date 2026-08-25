# DHT crawler policy parity contract

This crate owns database- and policy-dependent crawler behavior above the
protocol, runtime, scheduler, route, and maintenance primitives in
`bitmagnet-dht`. Go remains the production implementation and source of truth.
The bounded published checkpoint contains one Rust info-hash-triage worker,
one strict differential consumer, one concrete PostgreSQL lookup adapter, and
a thin adapter for the persistent blocking manager. It is not an application
composition or a live-database integration.

The crate boundary is intentional. `bitmagnet-dht` owns the typed
`DhtInfoHashTriageRequest`, its bounded input route, and the bounded get-peers
and scrape output routes. `bitmagnet-model` supplies the shared `FilesStatus`
domain enum. This crate owns the higher-level batching and routing decision,
plus the blocking-policy and database-projection seams and the concrete SQLx
lookup and blocking-manager adapters. It does not make the protocol crate
depend on PostgreSQL, the persistent blocking manager, crawler application
state, or deployment configuration.

The implementation checkpoint is
`102f290e9d50d779c4b5ad05adf0f02f7d825d45`. The strict-consumer checkpoint is
`3099790291597d4aed4888601d5b184f173f9bdf`. The PostgreSQL lookup checkpoint is
`18cc4ac47cb4b6186494dde2c244884d105ab749`. The blocking-manager adapter
checkpoint is `c8e22493e03850d5a61712476dc000459b414438`. Together they publish
the API and evidence below.

## Public Rust boundary

`DhtInfoHashTriageWorker::new` consumes the unique
`DhtInfoHashTriageReceiver`, consumes cloneable `DhtGetPeersInput` and
`DhtScrapeInput` capabilities, and accepts shared
`Arc<dyn DhtInfoHashBlockFilter>` and
`Arc<dyn DhtTorrentTriageLookup>` collaborators. It returns the owned worker
and a cloneable, sender-free `DhtInfoHashTriageStatsHandle`. The production
constructor uses `SystemDhtInfoHashTriageClock` and the default policy.

`DhtInfoHashTriageWorker::with_config` additionally accepts an injected
`Arc<dyn DhtInfoHashTriageClock>` and `DhtInfoHashTriageConfig`. The public
configuration fixes these defaults:

- `DHT_INFO_HASH_TRIAGE_BATCH_LIMIT = 1_000`;
- `DHT_INFO_HASH_TRIAGE_BATCH_INTERVAL = 20s`;
- `DHT_INFO_HASH_TRIAGE_SAVE_FILES_THRESHOLD = 100`; and
- `DHT_INFO_HASH_TRIAGE_RESCRAPE_THRESHOLD = 30 days`.

The batch limit is a `NonZeroUsize`; the other values can be overridden by an
explicit Rust composition. Custom values are a Rust capability, not evidence
that arbitrary configurations reproduce Go production.

`DhtInfoHashBlockFilter::filter` receives the first-occurrence-ordered unique
input hashes and returns eligible hashes. It may preserve collaborator-defined
order and duplicate behavior, but every returned hash must occur in the input.
Any foreign returned hash is a contract violation and fails the whole batch
closed before lookup or routing.

`DhtTorrentTriageLookup::lookup` receives the complete filtered vector, so
filter-produced duplicates remain visible to the lookup. It returns
`DhtTorrentTriageRow` values containing the info hash, `FilesStatus`, optional
file count, optional DHT seeder and leecher counts, and an optional DHT update
timestamp in Unix microseconds. Duplicate rows are accepted and the final row
for a hash wins. Both async collaborators return
`TriageCollaboratorError = Box<dyn Error + Send + Sync + 'static>`.

`DhtInfoHashTriageClock::now_unix_micros` is read lazily only for a candidate
that reaches the timestamp-staleness predicate. The system implementation
projects wall time into a signed Unix-microsecond value; deterministic callers
can inject an alternate clock.

### Persistent blocking-manager adapter

`BlockingManager` directly implements `DhtInfoHashBlockFilter`. A public
wrapper would add no ownership or lifecycle semantics because this crate owns
the collaborator trait, so the direct implementation is the smallest public
surface. An application can share one manager as
`Arc<dyn DhtInfoHashBlockFilter>` while retaining a typed clone for blocking
and flush operations.

The adapter converts `Id20` to `InfoHash` byte-for-byte, delegates exactly once
to `BlockingManager::filter`, boxes `BlockingError` behind
`TriageCollaboratorError`, and converts the result byte-for-byte back to
`Id20`. It does not sort, deduplicate, validate against the input, retry, or
flush independently; returned order and duplicates are preserved for the
worker's existing contract validation. Its deterministic tests use a private
single-use delegate seam and a lazy pool solely for construction, without
polling a manager operation or acquiring a PostgreSQL connection.

At the adapter checkpoint, all four focused adapter tests and all 34 crawler
release tests passed. Release all-target checking, Clippy with warnings denied,
rustdoc with warnings denied, formatting, and diff checks also passed. These
are offline source and compile gates, not live PostgreSQL evidence.

### PostgreSQL lookup adapter

`PgDhtTorrentTriageLookup::new` wraps a cheap clone of an already configured,
application-owned `PgPool` and implements `DhtTorrentTriageLookup`. It neither
creates nor closes the pool, starts tasks, retries queries, opens an explicit
transaction, nor imposes a statement timeout. An empty input returns without
accessing the pool.

The static query projects the six Go triage columns. It keeps source `dht` in
the `LEFT JOIN` predicate so torrents without a DHT source row remain visible,
and tests membership with `info_hash = ANY($2::bytea[])`. The input bind keeps
every occurrence in order; query result order remains unspecified. The array
bind deliberately differs from Go's dynamic `IN` SQL text and bind
cardinality, without changing membership semantics.

SQLx rows are converted into `DhtTorrentTriageRow` with checked domain
boundaries. File status text must name a current `FilesStatus`, hashes must be
exactly 20 bytes, and non-null counts must fit `u64`. Rejecting malformed hash
lengths and negative counts is intentional fail-closed Rust hardening: Go's
scanners copy hash bytes and cast signed counts more permissively. Missing
left-joined counts and timestamp remain `None`; the worker treats a missing
timestamp as scrape-eligible, while the strict Go oracle does not cover that
nullable timestamp path. SQLx and decode failures remain typed `DbError`
values behind the collaborator error boundary.

The adapter tests freeze the exact SQL string, source and array binding model,
occurrence preservation, enum/count/hash/timestamp conversion, nullable join
values, empty-input short circuit, clone and trait-object shape, sendability,
and a closed-pool SQLx error. They are offline tests: they do not prepare or
execute the query against PostgreSQL and do not prove live array encoding,
schema compatibility, indexes or query plans, server-side cancellation, or
application pool configuration.

`DhtInfoHashTriageWorker::run` owns the worker until one typed terminal result:

- `InputClosed` means every input clone is gone and all queued input, including
  the final partial batch, was processed;
- `Shutdown { queued_dropped, batch_dropped }` means caller shutdown won;
- `GetPeersClosed { request, queued_dropped, batch_dropped }` returns the exact
  get-peers request that could not commit; and
- `ScrapeClosed { request, queued_dropped, batch_dropped }` returns the exact
  scrape request that could not commit.

Construction starts no task. `run` is the single owned worker future and does
not detach a batcher or child. Dropping that future drops its owned receiver
and capabilities, but produces no typed exit or terminal-accounting guarantee.

## Bounded Go behavior

The Go factory creates `infoHashTriage` as a batching channel with input
capacity 100, maximum batch size 1,000, maximum wait 20 seconds, and batch
output capacity one. Its get-peers and scrape routes each have input capacity
100 and configured concurrency 200. The default scaling factor is 10, the
save-files threshold is 100, and the rescrape threshold is 30 days.

For each delivered Go batch, `runInfoHashTriage` retains the first request for
each info hash and passes first-occurrence-ordered unique hashes to the
blocking manager before accessing the database. A filter error abandons the
current batch and continues the outer loop. An empty filter result skips the
database and both output routes.

The filtered vector, including duplicates, becomes the GORM `IN` argument
list. The query selects:

- `torrents.info_hash`;
- `torrents.files_status`;
- `torrents.files_count`;
- `torrents_torrent_sources.seeders`;
- `torrents_torrent_sources.leechers`; and
- `torrents_torrent_sources.updated_at`.

It left-joins `torrents_torrent_sources` by info hash with source `dht`.
Duplicate result rows use the final row. A database error abandons the current
batch and continues the outer loop.

Each distinct filtered hash is routed to get-peers when no row exists, when
`files_status` is `no_info`, when the status is not `single` and file count is
absent, or when the status is `over_threshold` and file count is at most the
save-files threshold. For each hash, the get-peers decision precedes the scrape
decision.

Otherwise the hash is routed to scrape when either DHT swarm count is absent,
or when `updated_at` is strictly before `time.Now() - rescrapeThreshold`. Go
reads `time.Now()` for each item that reaches that final predicate. A row with
known counts and an update timestamp equal to the cutoff is not stale. All
remaining hashes are discarded.

The Go output sends select against context cancellation. The production loop
is started as a detached goroutine, has no worker join and no stats surface,
and receives from the batching output without checking the channel-open
boolean. Those lifecycle facts are source evidence, not behavior reproduced by
the Rust worker.

## Deliberate Rust ownership and hardening deltas

Rust preserves the first-request duplicate rule, filter-before-lookup order,
filtered lookup arguments, final-row-wins lookup projection, decision matrix,
per-hash get-peers decision precedence, and strict-before staleness predicate.
It deliberately differs at the surrounding ownership boundary:

- one owned `run` future replaces the detached Go triage goroutine;
- input EOF is explicit and typed instead of permitting repeated zero-value
  batches from a closed Go output;
- batching is first-item-relative inside the worker, with no detached batcher
  and no one-batch output buffer;
- each completed or EOF-flushed batch starts a fresh delay, so there is no
  catch-up schedule;
- filtered hashes route in first-filtered order instead of Go map iteration
  order;
- the worker consumes abstract async collaborators; this crate supplies a SQLx
  lookup adapter, but not the persistent Go manager or live PostgreSQL
  composition;
- collaborator failures are counted, drop only the current batch, and continue
  without claiming Go log delivery;
- shutdown, input EOF, and both downstream closures have typed results and
  exact suffix accounting;
- downstream sends use cancellation-safe send futures and biased shutdown;
- a foreign filter hash fails the batch closed instead of creating Go's
  zero-value request lookup; and
- the injected clock makes reached staleness checks deterministic without
  claiming Go's exact wall-clock schedule.

The Rust route queues also bound only committed queued requests. A pending
upstream send owns its request outside the input queue; closing the receiver
wakes that sender and lets it recover its own unsent request. Such external
pending requests are not part of the worker's `queued_dropped` count.

## Lifecycle, cancellation, and accounting

Shutdown is biased before first input, batch-delay completion, input receive,
filter completion, lookup completion, and output-send commitment. During a
final EOF-flushed batch, an already-ready shutdown is polled once more before
returning `InputClosed`. A shutdown while collecting classifies the dequeued
local batch separately from the still-queued suffix.

On shutdown, the worker closes its unique input receiver, drains already
queued requests, and returns their count as `queued_dropped`. If cancellation
occurs before deduplication, `batch_dropped` counts the raw dequeued local
batch. After deduplication it counts only unique requests not already
classified as duplicate, suppressed, failed, routed, or discarded. During
routing it counts the current uncommitted request and untouched routing suffix,
never an already committed prefix.

On downstream closure, the worker similarly closes and drains the input,
returns the exact current unsent request, and classifies that request plus the
untouched routing suffix as `batch_dropped`. A get-peers closure never becomes
a scrape result, and a scrape closure never becomes a get-peers result.

`DhtInfoHashTriageStatsHandle::snapshot` independently reads saturating atomic
counters. At a terminal snapshot, every dequeued input occurrence has exactly
one terminal classification:

```text
dequeued
  = input_duplicates_dropped
  + filter_suppressed
  + filter_failure_dropped
  + filter_contract_dropped
  + lookup_failure_dropped
  + get_peers_queued
  + scrape_queued
  + discarded
  + shutdown_batch_dropped
  + route_closed_batch_dropped
```

`shutdown_queued_dropped` and `route_closed_queued_dropped` are excluded from
that identity because those requests were drained before dequeue. The
diagnostic counters `batches`, `filter_calls`, `filter_failures`,
`filter_hashes_returned`, `lookup_calls`, `lookup_failures`,
`unknown_filtered_hashes_dropped`, and `route_closures` also do not add new
terminal outcomes. Because a snapshot is not an atomic multi-counter
transaction, the conservation identity is a terminal-state contract rather
than a guarantee for every concurrently observed intermediate snapshot.

## Differential oracle and strict consumer

Go oracle commit `6aece7ac7605507aaf5ccdcc9adf2497170b071d` generated
`testdata/parity/dht/dht_crawler_info_hash_triage.jsonl`. The checked fixture's
SHA-256 is
`52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8`.
It contains exactly seven ordered rows:

1. `production_source_factory_and_lifecycle_contract` (`SOURCE_ONLY`);
2. `dedup_filter_lookup_and_decision_matrix` (`RUNTIME_EXACT`);
3. `empty_filter_result_skips_database_and_outputs` (`RUNTIME_EXACT`);
4. `filter_error_drops_batch_and_continues` (`RUNTIME_EXACT`);
5. `database_error_drops_batch_and_continues` (`RUNTIME_EXACT`);
6. `cancellation_at_blocked_get_peers_send` (`RUNTIME_EXACT`); and
7. `cancellation_at_blocked_scrape_send` (`RUNTIME_EXACT`).

The source row binds exact SHA-256 values for fifteen live Go files:

- `internal/blocking/manager.go`:
  `d32ef7b0fb1eeadaeb1134f49b1046911c27312d2383b402d5989c8bc830130f`;
- `internal/concurrency/batching_channel.go`:
  `72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975`;
- `internal/concurrency/buffered_concurrent_channel.go`:
  `4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c`;
- `internal/database/dao/torrents.gen.go`:
  `59dd2534bdf02f356230ba602015a1ee8f9fc55d7203660776feeab4293981a3`;
- `internal/database/dao/torrents_torrent_sources.gen.go`:
  `8efbb42ea9fa9aee021ef41528d0821600ebf703db8c76a4dc706a22e64ca31a`;
- `internal/dhtcrawler/config.go`:
  `b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef`;
- `internal/dhtcrawler/crawler.go`:
  `ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6`;
- `internal/dhtcrawler/factory.go`:
  `ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6`;
- `internal/dhtcrawler/infohash_triage.go`:
  `7950da30f12ec9d54ba830c7465a749d4625ad0fd7e0aa2bebbdc4cef2027f02`;
- `internal/dhtcrawler/sample_infohashes.go`:
  `483b9037673dce82f9026f2aec9448812f804c13484fd0bd2f55fcfc70a52983`;
- `internal/model/files_status_enum.go`:
  `5f723e62282dcc82e2037c96d1423f81075cddca24b14e29a544340f5650e9a0`;
- `internal/model/null.go`:
  `b9c3762d286201140c51cd3ca2630361fb35fb76464c297a37d85037d1be782d`;
- `internal/model/torrents.gen.go`:
  `3c3fb6debefdca25530b9f3cecd818e8b98817528f36ff87a76dfee79cad84e0`;
- `internal/model/torrents_torrent_sources.gen.go`:
  `a5431060dd68f51ac77aced27f4a3c1481124054bef43365d368bded4a405b41`;
  and
- `internal/protocol/id.go`:
  `e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f`.

It also binds six normalized AST digests for the default config,
`crawler.nodeHasPeersForHash`, `crawler.start`, `factory.New`,
`runInfoHashTriage`, and `triageResult`, plus the exact `go.mod` and `go.sum`
entries for `github.com/DATA-DOG/go-sqlmock v1.5.2`.

The source row is not executed as Rust behavior. The other six rows drive the
actual Go `crawler.runInfoHashTriage` with manually controlled interface lanes,
a scripted blocking manager, real GORM DAO query construction over sqlmock,
and fixed observations away from the staleness boundary. Action order is
recorded as a sorted multiset because Go routes over a map.

The strict Rust consumer uses `deny_unknown_fields` on every fixture shape,
recomputes the fixture digest, requires the exact row order and
classifications, recomputes all fifteen current source digests, and checks the
six normalized AST values and sqlmock dependency lines. It independently
reconstructs every row's expected schema values before replay.

The decision-matrix replay runs the actual Rust worker over twelve dequeued
requests in two batches. It proves first-request duplicate retention, filter
and lookup calls, five get-peers actions, two scrape actions, two policy
discards, three lazy clock reads, and `InputClosed`. Its other nonzero stats are
one input duplicate, two filter calls, nine returned hashes, two suppressed
hashes, and one lookup call. The empty-filter replay reaches `InputClosed` with
two dequeued requests, two batches, two filter calls, two suppressed hashes,
and no lookup, output, or clock access.

The filter-error replay reaches `InputClosed` with two dequeued requests, two
batches, two filter calls, one filter failure, one failure-classified hash, and
one hash suppressed by the following batch. The lookup-error replay similarly
continues to `InputClosed` with two dequeued requests, two batches, two filter
calls, one returned and one suppressed hash, one lookup failure, and one
failure-classified routing hash. Every replay asserts terminal conservation.

The two cancellation replays fill the selected Rust output route to its exact
capacity, observe the target send attempt through a private test hook, then
make shutdown ready. Each proves no target commit, preservation of the
preexisting route prefix, `Shutdown { queued_dropped: 0, batch_dropped: 1 }`,
one `shutdown_batch_dropped`, and terminal conservation. The private hook is a
deterministic test seam, not public API.

The committed checkpoint passed the sixteen built-in worker tests, all seven
strict-consumer tests, the combined twenty-three-test crate suite and doctests,
the strict-consumer focus one hundred consecutive times, all-target and
all-feature `cargo check`, strict all-target and all-feature Clippy with
warnings denied, rustdoc with warnings denied, formatting checks, and diff
whitespace checks.

The PostgreSQL adapter checkpoint passed all seven focused adapter tests, the
combined thirty-test crate suite and doctests in release mode, all-target and
all-feature `cargo check`, strict all-target and all-feature Clippy with
warnings denied, rustdoc with warnings denied, formatting checks, and diff
whitespace checks. Those gates use no live PostgreSQL instance.

## Evidence boundaries and nonclaims

The Go fixture does not claim map iteration, SQL result, or downstream
delivery order; the exact rescrape boundary or wall-clock schedule; a live
PostgreSQL schema, plan, indexes, transactions, or result order; production
blocking-filter state, buffering, or flushes; production batching timer, input
close, or output close behavior; downstream callback concurrency or
completion; select-tie resolution, fairness, or side effects beyond the
recorded lane-access observation; log delivery; total work retention,
throughput, or backpressure; closed Go triage-output behavior; live network or
external-service behavior; upstream proof that the responding node has peers
for a sampled hash; ignore-hash provenance; or any Rust API, stats,
supervision, application, deployment, or production-readiness fact.

The Rust consumer, worker, PostgreSQL adapter, and blocking-manager adapter do
not claim exact Go map, SQL-result, or delivery order; exact Go SQL text or
bind cardinality; exact Go wall-clock values or per-item `time.Now()` schedule;
live PostgreSQL array encoding, schema compatibility, indexes, query plans,
server-side cancellation, transactions, retries, statement timeouts, or pool
configuration; a live persistent production blocking Bloom state; Go's detached
batcher timing, output boundary, or close behavior; downstream get-peers or
scrape execution; Go select ties, eager lane operands, or fairness; Go logging;
cross-route retention or waiter fairness; closed Go output behavior; live DHT
traffic; upstream sample provenance; concurrent upstream pending-send drain
accounting; supervisor/application/deployment wiring; or production readiness.

The oracle has no live PostgreSQL, network, DNS, UDP, DHT, or deployment
dependency. Passing it establishes the bounded source and deterministic replay
contract only.

## Pending integration

The following remain deliberately outside this checkpoint:

- application ownership and shutdown flushing of the persistent blocking
  manager, plus metrics and operator-facing failure policy;
- application construction of `PgDhtTorrentTriageLookup` with a configured
  pool, plus live schema/codec/query-plan validation and database
  observability;
- ownership of the unique triage receiver by an application or higher-level
  crawler supervisor;
- application construction and shutdown wiring between the existing DHT
  sample-infohashes maintenance path and this worker;
- production get-peers and scrape consumers, their concurrency, callbacks,
  persistence outputs, retries, and failure policy;
- a producer-side `closed()` waiter on the typed triage input route;
- configuration loading, health reporting, metrics export, and operator-facing
  diagnostics; and
- live traffic, deployment configuration, rollout, migration, or operational
  readiness.

The existing DHT maintenance supervisor borrows a triage input capability for
the sample-infohashes worker but does not own this crate's unique triage
receiver, construct `DhtInfoHashTriageWorker`, or monitor it as a child. The
typed get-peers and scrape routes are handoff primitives; this checkpoint does
not add their downstream workers. Explicit tests can compose the isolated
worker, but no current production application path completes this pipeline.
