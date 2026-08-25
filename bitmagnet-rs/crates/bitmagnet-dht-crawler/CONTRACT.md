# DHT crawler policy parity contract

This crate owns database- and policy-dependent crawler behavior above the
protocol, runtime, scheduler, route, and maintenance primitives in
`bitmagnet-dht`. Go remains the production implementation and source of truth.
The bounded published checkpoint contains owned Rust info-hash-triage,
get-peers, scrape, and request-metainfo workers, strict differential consumers
for all four stages, one concrete
PostgreSQL lookup adapter, and a thin adapter for the persistent blocking
manager, plus one offline concrete triage composition test and taskless typed
metainfo-request, scraped-source, and torrent-persistence handoffs. The
get-peers worker publishes successful nonempty peer vectors through the
metainfo-request handoff. The scrape worker publishes successful raw BEP-33
Bloom filters through the scraped-source handoff. The request-metainfo worker
publishes allowed verified metainfo through the torrent-persistence handoff.
No concrete peer-wire metainfo requester, request-stage persistent-blocking
adapter, scraped-source or torrent-persistence worker, or Rust production
application composition exists yet. This is not a live-database integration.

The crate boundary is intentional. `bitmagnet-dht` owns the typed
`DhtInfoHashTriageRequest`, its bounded input route, and the bounded get-peers
and scrape output routes. `bitmagnet-model` supplies the shared `FilesStatus`
domain enum. `bitmagnet-metainfo` owns verified v1/v2 info-dictionary parsing,
the parsed metainfo domain, normalized file projection, and default
side-effect-free banning policy. This crate owns the higher-level batching and
routing decision, the owned get-peers, scrape, and request-metainfo stages,
their taskless downstream handoffs, the blocking-policy and
database-projection seams, and the concrete SQLx lookup and triage
blocking-manager adapters. It does not make the protocol or metainfo crate
depend on PostgreSQL, the persistent blocking manager, crawler application
state, or deployment configuration.

The implementation checkpoint is
`102f290e9d50d779c4b5ad05adf0f02f7d825d45`. The strict-consumer checkpoint is
`3099790291597d4aed4888601d5b184f173f9bdf`. The PostgreSQL lookup checkpoint is
`18cc4ac47cb4b6186494dde2c244884d105ab749`. The blocking-manager adapter
checkpoint is `c8e22493e03850d5a61712476dc000459b414438`. The offline concrete
composition checkpoint is `846c4c0f813da949db02a34ce20a78d73eb72a3b`.
The Go get-peers oracle checkpoint is
`19f568e01c637a8ae1b94f38e3db2c9f95734d8c`, and the request-metainfo route
checkpoint is `73a4d867b41f4a4e7933d527c633b044736300c6`. The owned get-peers
worker checkpoint is `b52c174eec3a58cc02d09a18435abf5e3f31f64b`, and its strict
Rust-consumer checkpoint is `7c49c4be0a5d8465d234b4c63d2ac5bf190572ca`.
The scraped-source route checkpoint is
`a76591e92430ceb65fc7eb62af4ffbbaa791dad7`. The frozen Go scrape-oracle
checkpoint is `c6b365ab0a62000351baf76ec78cfca38506b5ee`, and its fixture SHA-256
is `d434306fd60678be95cabd53d59ea152f6a013bf2e486f4bb2456aa8da2c6d9b`.
The owned scrape-worker checkpoint is
`c9921be38a9d68a9812d4647bf1c33b74812ad5f`, and its strict Rust-consumer
checkpoint is `6f45f7b6eeb29b0c3d41c327246d9a27df0f5ac5`.
The frozen Go request-metainfo oracle checkpoint is
`2f1f7c7292b749b8ef8af3aae6bf1214d2d26651`, and its fixture SHA-256 is
`03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5`.
The Rust metainfo parser, normalized-file projection, and default banning
policy checkpoints are respectively
`96b0fcafd846ac6458e01407d50de7487eea2bff`,
`2409898e398107a2372bab62feae80ce6d200877`, and
`73d72c9c0bcb001b7aacfbdc7b5c9ea6fc51ae40`. The torrent-persistence route
checkpoint is `53d020c5c338c28f2a573e2bcad7e99cb902bf3a`, the owned
request-metainfo worker checkpoint is
`918fa935274fa72ad2207f586f34f6858187152d`, and its strict Rust-consumer
checkpoint is `d73de2fca1a39aca0d61fc7685b0aa41c6b3a041`.
Together they publish the API and evidence below.

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

### Offline concrete composition proof

The integration test at the concrete-composition checkpoint uses only public
APIs to construct a lazy caller-owned `PgPool`, one
`Arc<BlockingManager>` shared through `Arc<dyn DhtInfoHashBlockFilter>`, one
`Arc<PgDhtTorrentTriageLookup>` shared through
`Arc<dyn DhtTorrentTriageLookup>`, the concrete triage/get-peers/scrape routes,
and one real `DhtInfoHashTriageWorker`. The route, manager, lookup, and worker
constructors start no application worker, and the lazy pool opens no
connection. SQLx may own an internal pool-maintenance task.

The test queues three triage requests and gives `run` an already-ready shutdown
future. The worker's biased shutdown branch wins before intake, so it returns
exactly `Shutdown { queued_dropped: 3, batch_dropped: 0 }`; its stats are the
default value except for three `shutdown_queued_dropped`. This proves that the
concrete filter and lookup collaborators were not polled. It also proves that
the worker drops its sole output capabilities, closes and drains the unique
triage receiver, and releases its collaborator clones. A subsequent input send
recovers its exact rejected request.

The retained typed manager then performs an empty `flush` while the lazy pool
is still open and has size zero. The test drops the remaining collaborators,
closes the pool last, and observes the closed state. This is a bounded ownership
and shutdown-order proof only: the empty flush is a no-op, and the test does not
exercise PostgreSQL, a nonempty persistent Bloom write, UDP, an application
pipeline task, downstream processing, a supervisor, or a production
application.

At the concrete-composition checkpoint, the 34 crawler unit tests, the one
integration test, and doctests passed in release mode. All-target checking,
Clippy with warnings denied, rustdoc with warnings denied, formatting, and diff
checks also passed. These gates used no database service or live network.

### Request-metainfo handoff route

`dht_request_meta_info_channel` constructs one taskless bounded Tokio MPSC
queue with fixed capacity `DHT_REQUEST_META_INFO_ROUTE_CAPACITY = 100`, matching
the Go request-metainfo input capacity at default scaling. Its cloneable
`DhtRequestMetaInfoInput` and unique `DhtRequestMetaInfoReceiver` own the queue;
the final input clone controls drain-then-EOF.

Each `DhtMetaInfoRequest` owns the original `info_hash`, the queried DHT
`source_node_addr`, and the complete ordered `peers` vector. IPv4 and scoped
IPv6 addresses, order, and duplicates are preserved. A pending send owns that
payload outside the queue and is cancellation-safe. Explicit receiver close or
receiver destruction returns the exact unsent request through
`DhtRequestMetaInfoInputClosed::into_request`; explicit close retains the
already committed prefix for draining.

Seven focused route tests freeze the public constant and type traits, payload
identity, exact hundred-item FIFO and the blocked 101st send, pending-send
cancellation, clone-controlled EOF, close-and-drain behavior, and receiver-drop
recovery. At the route checkpoint, all 41 crawler unit tests, the concrete
composition integration test, and doctests passed in release mode. All-target
checking, strict Clippy, rustdoc, formatting, and diff checks also passed.

At this route-only checkpoint, the queue did not reproduce Go's 400 concurrent
request-metainfo callbacks or claim a total-retention bound. It constructed no
get-peers or metainfo worker and performed no DHT query, TCP request, parsing,
banning, blocking, persistence, database, classifier, supervisor, or
application work. Later checkpoints add owned producer and consumer workers;
the route itself remains taskless and retains exactly the ownership contract
above.

### Torrent-persistence handoff route

`dht_persist_torrent_channel` constructs one taskless bounded Tokio MPSC queue
with fixed capacity `DHT_PERSIST_TORRENT_ROUTE_CAPACITY = 1_000`, matching the
Go persist-torrents input capacity at default scaling. Its cloneable
`DhtPersistTorrentInput` and unique `DhtPersistTorrentReceiver` own the queue;
the final input clone controls drain-then-EOF.

Each `DhtPersistTorrentRequest` owns the original requested `info_hash`, the
supplying DHT `source_node_addr`, and one shared `Arc<ParsedInfo>` containing
the decoded info domain and verified v1 and optional v2 identities; normalized
files can be projected fallibly from `Info`. IPv4 and scoped IPv6 source
addresses are retained. The route does not recompute identity, copy parsed
metainfo per input clone, or replace the original requested hash with an
identity parsed from the payload.

A pending `send` owns its exact request outside the queue and is
cancellation-safe. A successful send is an irrevocable FIFO commit. Explicit
receiver close or destruction rejects later and pending sends and returns the
exact unsent request through `DhtPersistTorrentInputClosed::into_request`;
explicit close retains the committed prefix for draining. Receiver EOF occurs
only after explicit close or the final input clone is dropped and that prefix
has drained.

Seven focused tests freeze the public constant and type traits, exact parsed
payload sharing and route identity, exact thousand-item FIFO and the blocked
1,001st send, pending-send cancellation, clone-controlled EOF,
close-and-drain behavior, and receiver-drop recovery. At the route checkpoint,
all 101 then-registered crawler unit tests, the concrete composition
integration test, and doctests passed in release mode. Release all-target
checking, strict Clippy, rustdoc, formatting, and diff checks also passed.

Construction starts no task. The route does not implement Go's maximum-1,000,
60-second persist-torrents batcher, its output-capacity-one writer boundary,
deduplication, model projection, database writes, retries, metrics,
supervision, or application ownership.

### Scraped-source handoff route

`dht_persist_source_channel` constructs one taskless bounded Tokio MPSC queue
with fixed capacity `DHT_PERSIST_SOURCE_ROUTE_CAPACITY = 1_000`, matching Go's
raw persist-sources input capacity at default scaling. The function returns one
cloneable `DhtPersistSourceInput` and one unique
`DhtPersistSourceReceiver`; every input clone shares the same FIFO.

Each `DhtPersistSourceRequest` owns the original `info_hash`, the supplying
DHT `source_node_addr`, and exact `seeders_bloom` and `peers_bloom`
`ScrapeBloomFilter` values. IPv4 and scoped IPv6 source addresses are retained.
The peers filter is the filter a future persistence writer will project as the
DHT leecher count, but this route preserves both raw 256-byte filters and
performs no count projection.

A pending `send` owns its exact request outside the queue and is
cancellation-safe. A successful send is an irrevocable queue commit. Explicit
`DhtPersistSourceReceiver::close` or receiver destruction rejects pending and
later sends and returns each exact unsent request through
`DhtPersistSourceInputClosed::into_request`; explicit close retains the
already-committed FIFO prefix for draining. `recv` awaits the next FIFO item and
`try_recv` observes only a currently queued item. Receiver EOF occurs only
after explicit close or the final input clone is dropped and the committed
prefix is drained.

Seven focused route tests freeze the public constant and type traits, raw empty
and patterned filter identity, scoped source identity, exact thousand-item
FIFO and backpressured 1,001st send, pending-send cancellation,
clone-controlled drain-then-EOF, close-and-drain recovery, and receiver-drop
recovery. Construction starts no task. The route does not batch, estimate
counts, persist sources, access a database, or define worker, supervisor, or
application ownership.

At the scraped-source route checkpoint, the seven focused route tests and all
71 crawler unit tests, the concrete-composition integration test, and doctests
passed in release mode. These tests use no live network or database.

### Owned get-peers worker

`DhtGetPeersWorker::new` consumes the unique `DhtGetPeersReceiver` and owns
the supplied `DhtRuntimeClient`, `KTable`, `DhtRequestMetaInfoInput`, and
`DhtDiscoverySender` handles. It returns the unstarted worker and a cloneable,
sender-free `DhtGetPeersWorkerStatsHandle`. `with_config` accepts a
`DhtGetPeersWorkerConfig`; its nonzero `max_inflight` defaults to 200, matching
Go's configured callback concurrency at default scaling.

`run` owns one `JoinSet` and never polls the input while that set is at
capacity, so it retains no extra acquire waiter outside the capacity-100 input
route. Route EOF joins every accepted child before returning `InputClosed`.
Biased shutdown closes and drains the input, marks cancellation accounting,
aborts every accepted child, joins every cancellation, and returns
`Shutdown { queued_dropped, tasks_cancelled, recursive_nodes_dropped,
meta_info_requests_dropped }`. A panicking child causes all siblings to be
aborted and joined before the original panic payload resumes. Dropping the run
future aborts its children and closes the input but deliberately provides no
typed exit or terminal shutdown counters.

Each child that starts invokes `get_peers` exactly once with the request's
source address and info hash. Error applies one `DropAddr`; Rust KTable reverse
identity uses the IP and numeric IPv6 scope but excludes port, while no Go error
reason is stored. Success first classifies the returned peer vector for
terminal accounting. Its first KTable or downstream-route effect then applies
one synchronous `PutNode` using the response ID, request address, and
`Responded`, before any later await or discovery fanout.

Response nodes retain response order under one absolute one-second deadline.
Each node requires an owned discovery reservation; an equal-ready deadline
wins, and receiver closure or timeout deterministically classifies the entire
current-and-remaining suffix. An empty peer vector stops after responder and
discovery effects. A nonempty vector preserves occurrences, order, duplicates,
IPv4, and scoped IPv6 into `PutHash`, then sends the same vector with the
original info hash and source address to the metainfo route. `PutHash` is
synchronous and precedes that cancellation-safe send. Normal discovery or
metainfo receiver closure is counted and does not stop the worker.

At quiescent normal EOF, the exact statistics equations are:

```text
dequeued = queries_started = tasks_completed
queries_started = queries_succeeded + queries_failed
put_node_commands = queries_succeeded
drop_addr_commands = queries_failed
queries_succeeded = responses_without_peers + responses_with_peers
recursive_nodes
  = recursive_nodes_queued
  + recursive_nodes_closed_dropped
  + recursive_nodes_timed_out_dropped
put_hash_commands = responses_with_peers
responses_with_peers = meta_info_queued + meta_info_closed_dropped
```

After typed shutdown, `dequeued = tasks_completed +
shutdown_tasks_cancelled`; `shutdown_recursive_nodes_dropped` extends the
recursive equation, and `shutdown_meta_info_dropped` extends the metainfo
equation. `put_hash_commands = responses_with_peers` is normal-only because a
successful nonempty response can be cancelled during recursive fanout before
its hash put. Peer-value counters count occurrences before KTable's IP-and-scope
normalization.

Sixteen focused worker tests freeze defaults and saturation, pre-ready drain,
scoped address drop, exact success order and duplicates, absolute and
equal-ready deadlines, capacity and EOF joining, three shutdown positions,
normal output closure, never-polled accepted work, child-panic cleanup, and
active-run drop. Seven strict-consumer tests pin the complete Go source row and
replay rows two through seven through the actual worker core, KTable,
discovery route, and metainfo route. The two downstream shutdown replays reach
the actual full route waits rather than stopping in their observation hooks.

At the worker checkpoint, all 57 then-registered crawler unit tests, the
concrete composition integration test, and doctests passed in release mode.
At the strict-consumer checkpoint, all 64 crawler unit tests, the concrete
composition integration test, and doctests passed in release mode. The seven
strict tests also passed in 25 consecutive focused runs. Release all-target and
all-feature checking, strict Clippy, rustdoc, formatting, and diff checks
passed. These tests inject query results and do not open UDP, DNS, PostgreSQL,
or any live service.

### Owned scrape worker

`DhtScrapeWorker::new` consumes the unique `DhtScrapeReceiver` and owns the
supplied `DhtRuntimeClient`, `KTable`, `DhtPersistSourceInput`, and
`DhtDiscoverySender` handles. It returns the unstarted worker and a cloneable,
sender-free `DhtScrapeWorkerStatsHandle`. `with_config` accepts a
`DhtScrapeWorkerConfig`; its nonzero `max_inflight` defaults to 200, matching
Go's configured callback concurrency at default scaling. Construction starts
no task.

`run` owns one `JoinSet` and does not poll input at capacity, so it retains no
extra acquire waiter beyond accepted children and the capacity-100 input
route. Route EOF joins every accepted child before returning `InputClosed`.
Biased shutdown closes and drains input, marks shutdown accounting, aborts
every accepted child, joins every cancellation, and returns
`Shutdown { queued_dropped, tasks_cancelled, recursive_nodes_dropped,
persist_source_requests_dropped }`. A panicking child aborts and joins all
siblings before its original panic payload resumes. Dropping the run future
closes input and aborts children but deliberately produces no typed exit or
terminal shutdown counters.

Each started child invokes `get_peers_scrape` exactly once with the request's
source address and info hash. An error applies one `DropAddr`; Rust KTable
reverse identity uses the IP and numeric IPv6 scope but excludes the port, and
the Go error cause is not stored. Success counts every returned peer occurrence
as deliberately ignored, then applies one synchronous `PutNode` using the
response ID, request address, and `Responded`, before discovery fanout or any
downstream await. It never applies `PutHash` for ignored scrape peer values.

Response nodes retain response order and duplicates under one absolute
one-second deadline. Each node requires an owned discovery reservation; an
equal-ready deadline wins, and receiver closure or timeout deterministically
classifies the complete current-and-remaining suffix. Every successful query
produces one exact `DhtPersistSourceRequest` containing the original info hash
and source address, with `seeders_bloom` and `peers_bloom` preserved in their
original directions as raw 256-byte filters. On normal completion, if the child
reaches the handoff, it attempts the cancellation-safe send after discovery,
including when the node list or either filter is empty. Normal discovery or
scraped-source receiver closure is counted and remains local to that child
rather than stopping the worker.

At quiescent normal EOF, the exact statistics equations are:

```text
dequeued = queries_started = tasks_completed
queries_started = queries_succeeded + queries_failed
put_node_commands = queries_succeeded
drop_addr_commands = queries_failed
recursive_nodes
  = recursive_nodes_queued
  + recursive_nodes_closed_dropped
  + recursive_nodes_timed_out_dropped
queries_succeeded
  = persist_source_queued
  + persist_source_closed_dropped
```

`peer_values_ignored` counts response occurrences, including duplicates, and
has no equality to the number of successful queries. After typed shutdown,
`dequeued = tasks_completed + shutdown_tasks_cancelled`;
`shutdown_recursive_nodes_dropped` extends the recursive-node equation; and
`shutdown_persist_source_dropped` extends the successful-query/persist-source
equation. The exit's four fields correspond respectively to
`shutdown_queued_dropped`, `shutdown_tasks_cancelled`,
`shutdown_recursive_nodes_dropped`, and
`shutdown_persist_source_dropped`. Every counter is saturating and monotonic;
`DhtScrapeWorkerStatsHandle::snapshot` reads fields independently with relaxed
ordering, so cross-field conservation is promised only after worker exit.

The strict consumer records these deliberate Rust deltas from the frozen Go
oracle: Rust owns and joins bounded tasks; input EOF is typed; shutdown closes
and drains input before aborting and joining accepted tasks; shutdown during a
pending query applies none of Go's post-cancellation prefix; shutdown after a
discovery prefix accounts the exact unattempted suffix; shutdown at
scraped-source backpressure preserves the table and discovery prefix; discovery
uses cancellation-safe owned reservations and one suffix deadline; the
scraped-source send future is owned by worker shutdown; typed EOF, shutdown,
and accounting replace Go's swallowed lane error; and Rust drops KTable
addresses by IP and numeric scope without storing Go's error cause.

Sixteen focused worker tests freeze defaults and saturation, pre-ready drain,
scoped address drop, success ordering, duplicate discovery, raw Bloom
direction, absolute and equal-ready deadlines, capacity and EOF joining, three
shutdown positions, normal downstream closure, never-polled accepted work,
child-panic cleanup, active-run drop, and core-drop request recovery. Seven
strict-consumer tests pin the complete Go source row and replay rows two through
seven through the actual worker core, KTable, discovery route, and
scraped-source route. Rows five
through seven assert the deliberate owned-shutdown deltas; the two downstream
shutdown replays enter the actual full-route waits rather than stopping in
their observation hooks. Row one remains source-only, and row eight remains a
Go-only manual-lane behavior.

At the worker checkpoint, all 87 then-registered crawler unit tests, the
concrete composition integration test, and doctests passed in release mode.
At the strict-consumer checkpoint, all 94 crawler unit tests, the concrete
composition integration test, and doctests passed in release mode. The seven
strict tests also passed in 25 consecutive focused runs. Release all-target and
all-feature checking, strict Clippy, rustdoc, formatting, and diff checks
passed. These tests inject scrape results and do not open UDP, DNS, PostgreSQL,
or any live service.

### Owned request-metainfo worker

`DhtRequestMetaInfoWorker::new` consumes the unique
`DhtRequestMetaInfoReceiver` and owns the supplied `DhtPersistTorrentInput`,
`Arc<dyn DhtMetaInfoRequester>`,
`Arc<dyn DhtMetaInfoBanningChecker>`, and
`Arc<dyn DhtInfoHashBlocker>`. It returns the unstarted worker and a cloneable,
sender-free `DhtRequestMetaInfoWorkerStatsHandle`. The requester returns one
already verified `ParsedInfo`; the checker is synchronous and side-effect-free;
the blocker receives an ordered hash slice and explicit flush flag.
`DefaultDhtMetaInfoBanningChecker` adapts the default policy from
`bitmagnet-metainfo` without blocking as a side effect. No concrete peer-wire
requester or persistent blocking-manager adapter is implied by these seams.

`with_config` accepts a `DhtRequestMetaInfoWorkerConfig`; its nonzero
`max_inflight` defaults to 400, matching Go's configured callback concurrency
at default scaling. `run` owns one `JoinSet` and does not poll input at
capacity, so it retains no acquire waiter beyond accepted children and the
capacity-100 input route. Route EOF joins every accepted child before returning
`InputClosed`. Biased shutdown closes and drains input, aborts and joins every
accepted child, and returns exact queued, task, peer-occurrence, pending
request, pending block, and pending persistence counts. A panicking child
aborts and joins its siblings before the original panic resumes. Dropping the
run future closes input and aborts children but deliberately produces no typed
exit; non-graceful drop does not manufacture terminal shutdown counters.

Every task tries peer occurrences sequentially in original order, including
duplicates. Request errors fall through. The first success stops attempts and
classifies the complete unattempted suffix as skipped. An empty peer vector is
dropped as Rust hardening, and an all-failed vector is dropped after retaining
ordered failure display and cause identity within the worker's private parity
seam. Neither condition emits a zero-valued `ParsedInfo`.

Successful metainfo is checked once. A rejection invokes one block call on the
exact original requested hash with `flush = false`, ignores and counts that
call's error, emits no persistence request, and never tries the suffix. An
allowed result emits one `DhtPersistTorrentRequest` with the original hash,
source address, and `Arc<ParsedInfo>`. Normal torrent-route closure is local to
that child and counted. The worker does not retry, compare the parsed v1 hash
with the requested hash, persist blocking state itself, or own the downstream
receiver.

At quiescent normal EOF, the exact statistics equations are:

```text
dequeued = tasks_completed
peer_occurrences
  = request_attempts_failed
  + request_attempts_succeeded
  + peer_occurrences_skipped
request_attempts_started
  = request_attempts_failed + request_attempts_succeeded
request_attempts_succeeded = allowed + banned
tasks_completed = empty_peers_dropped + all_peers_failed + allowed + banned
banned = block_calls_started = block_succeeded + block_failed_ignored
allowed = persist_queued + persist_closed_dropped
```

After typed shutdown:

```text
dequeued = tasks_completed + shutdown_tasks_cancelled
peer_occurrences
  = request_attempts_failed
  + request_attempts_succeeded
  + peer_occurrences_skipped
  + shutdown_peer_occurrences_dropped
request_attempts_started
  = request_attempts_failed
  + request_attempts_succeeded
  + shutdown_request_attempts_cancelled
request_attempts_succeeded
  = allowed
  + banned
  + shutdown_block_calls_cancelled
  + shutdown_persist_requests_dropped
block_calls_started = banned + shutdown_block_calls_cancelled
```

Completed tasks, completed block calls, and completed persistence sends retain
their normal classification equations. Queued shutdown drops were never
dequeued and remain separate. Every counter is saturating and monotonic;
snapshots read fields independently with relaxed ordering, so conservation is
promised only after exit.

Fifteen focused worker tests freeze defaults, type and default-checker shape,
saturation, empty/all-failed behavior, ordered duplicates and first success,
exact banned and allowed effects, output closure, capacity and EOF joining,
shutdown during a request, block, and persistence send, pre-ready drain,
child-panic cleanup, and active-run drop. At the worker checkpoint, all 116
then-registered crawler unit tests, the concrete composition integration test,
and doctests passed in release mode; the focused worker suite passed 25
consecutive runs. At the strict-consumer checkpoint, all 125 crawler unit
tests, the integration test, and doctests passed, and all nine focused strict
tests passed 100 consecutive runs. Release all-target and all-feature checks,
strict Clippy, rustdoc, formatting, and diff checks passed. These are scripted
collaborator and in-memory route tests; they open no TCP, UDP, DNS, PostgreSQL,
or other live service.

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

## Frozen Go get-peers behavior

The strict Go get-peers oracle froze the downstream stage before implementation
and now supplies source and differential evidence to the Rust consumer. With
default scaling ten, Go's get-peers input has capacity 100 and concurrency 200.
The request-metainfo input has capacity 100 and concurrency 400.
`BufferedConcurrentChannel.Run` dequeues before acquiring its
semaphore, can therefore retain one additional acquire waiter, starts detached
callbacks, does not join them, and does not check the input channel-open
boolean. The crawler starts `runGetPeers` as an unjoined goroutine and ignores
the lane's returned error.

One callback calls `GetPeers` with the shared context, queried source address,
and info hash. A query error synchronously issues one IP-and-scope keyed
`DropAddr`; the source port is excluded, the reason is exactly
`failed to get peers: <cause>`, and the wrapped cause remains identifiable. No
success-side command or route is reached. A successful response instead puts
the response ID at the queried address with `NodeResponded` before any
cancellation check, discovery fanout, or peer-presence check.

Response nodes are visited in order under one child context with a one-second
timeout. Go's cancellation arm contains an unlabeled `break` inside the
`select`, so it exits only that `select`, not the enclosing node loop. Remaining
iterations still evaluate the discovery input accessor; when cancellation and
a send are both ready, the selected arm is unspecified. The oracle records
accessor calls separately from committed deliveries and makes no deterministic
post-timeout suffix claim.

After discovery, absent or empty peer values return the exact helper error
`no peers found`; the responder put and any discovery prefix remain, while no
address drop, hash put, or metainfo handoff occurs. Nonempty values are copied
in order, with duplicates, into both KTable hash peers and the metainfo request.
The hash put is synchronous and precedes the cancellation-aware metainfo send,
so cancellation cannot retract the responder or hash mutations. The KTable may
later collapse peers by IP and keep the final port, while the metainfo payload
retains the full ordered vector.

The eight-line fixture is
`testdata/parity/dht/dht_crawler_get_peers.jsonl`, with SHA-256
`82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844`.
It fixes one source-only row, three exact runtime rows, three deliberate
Rust-owned-shutdown-delta rows, and one Go-only lane-error row. Runtime rows
execute the actual `runGetPeers` and `requestPeersForHash` through a manual
callback lane, scripted client, tracing wrapper over the actual KTable, and
controlled discovery/metainfo inputs. The source row pins normalized AST
digests, sixteen full repository-source digests, five prerequisite-fixture
digests, six evidence commits, and eighteen nonclaims.

The generator freshness gate passed once and one hundred consecutive times;
its race-enabled focus passed ten times. `go vet`, Go formatting, manual fixture
hash verification, and diff checks also passed. These are deterministic source
and controlled-collaborator gates: they do not execute a server adapter, UDP,
DNS, a one-second wall-clock timeout, a metainfo requester, banning/blocking,
persistence, PostgreSQL, a supervisor, or an application.

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
- the worker consumes abstract async collaborators; this crate supplies the
  SQLx lookup and persistent-manager adapters, and proves their public
  construction in an offline test, but not a live PostgreSQL or production
  application composition;
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

For get-peers, Rust replaces Go's detached callback lane with one owned,
bounded, joined task set. It makes EOF typed, does not dequeue Go's additional
pre-semaphore waiter, applies deterministic timeout priority, and stops a
cancelled recursive suffix instead of continuing Go's unlabeled-break loop.
Its pending query is abandoned without Go's post-cancellation table effects;
after one recursive delivery it abandons the remaining nodes, future hash put,
and future metainfo request; after `PutHash` it can abandon only the blocked
metainfo request. The guards account those owned deltas exactly. Rust stores no
Go error cause in KTable and does not reproduce the swallowed Go lane error.

## Lifecycle, cancellation, and accounting

For `DhtInfoHashTriageWorker`, shutdown is biased before first input,
batch-delay completion, input receive,
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

## Frozen Go scrape behavior

Go oracle checkpoint `c6b365ab0a62000351baf76ec78cfca38506b5ee`
generated `testdata/parity/dht/dht_crawler_scrape.jsonl` with SHA-256
`d434306fd60678be95cabd53d59ea152f6a013bf2e486f4bb2456aa8da2c6d9b`.
It contains exactly eight ordered rows:

1. `production_source_factory_and_lifecycle_contract` (`SOURCE_ONLY`) pins the
   production worker, factory, shared context, channel, dependency, and
   prerequisite source contract without executing a runtime row;
2. `scrape_error_drops_request_ip_and_preserves_cause` (`RUNTIME_EXACT`) freezes
   the wrapped error identity and exact reason plus the request-IP-and-scope
   `DropAddr`, with no discovery or persistence handoff;
3. `success_present_empty_filters_ignores_values_and_hands_off_raw_blooms`
   (`RUNTIME_EXACT`) freezes responder `PutNode`, ignored peer values, and one
   raw present-empty seeders/peers handoff;
4. `success_preserves_node_order_and_bloom_direction_before_persist`
   (`RUNTIME_EXACT`) freezes responder-first ordering, distinct Bloom
   directions, and discovery order with the exact duplicate sequence first,
   second, first before one raw handoff;
5. `cancelled_before_client_return_still_puts_responder_but_abandons_fanout_and_persist`
   (`RUNTIME_WITH_OWNED_SHUTDOWN_DELTA`) retains the responder put and recorded
   eager route-accessor calls but commits no discovery or persistence output;
6. `cancel_after_one_discovery_retains_prefix_but_abandons_suffix_and_persist`
   (`RUNTIME_WITH_OWNED_SHUTDOWN_DELTA`) retains exactly the first discovery,
   records the eagerly evaluated suffix and persistence accessors, and commits
   neither the discovery suffix nor persistence handoff;
7. `cancellation_when_persist_send_is_unavailable_keeps_table_and_discovery_prefix`
   (`RUNTIME_WITH_OWNED_SHUTDOWN_DELTA`) retains the responder and complete
   discovery prefix, evaluates `persistSources.In()` while its unbuffered send
   is unavailable, cancels during that eager operand, and commits no
   persistence request; and
8. `lane_error_is_swallowed` (`GO_ONLY_LANE`) freezes `runScrape` returning
   after its manual input lane errors without invoking a callback.

Each production callback calls `GetPeersScrape` with the shared context and
the request address and info hash. An error applies one request-IP-and-numeric-
scope `DropAddr` without the port, using the exact reason
`failed to get peers from p: <cause>` while preserving the wrapped cause.
Success first applies `PutNode` with the response ID at the original request
address and `NodeResponded`, before any cancellation check, and ignores every
response `Values` item. It then visits response nodes in order, including
duplicates, under one configured one-second child context. The unlabeled
`break` exits only the `select`, so cancellation still scans the suffix and
eagerly evaluates each discovery `In()` accessor; ready send/cancellation ties
remain unspecified. After all discovery attempts, the raw handoff retains the
original request, maps `BfSeeders` to BFsd and `BfPeers` to BFpe, and eagerly
evaluates `persistSources.In()`.

Row seven proves eager persistence-accessor evaluation and zero delivery while
the unbuffered send is unavailable. It does not prove elapsed blocked-send
time or a particular ready-select winner.

The source row pins 21 normalized AST digests, 18 source-file digests, seven
prerequisite fixture digests, the Bloom dependency lines from `go.mod` and
`go.sum`, and the evidence commits. At default scaling it records Go scrape
input capacity 100 and callback concurrency 200; discovered-node input
capacity 1,000 with maximum batch size ten, configured 10 ms interval, and
output capacity one; and raw persist-sources input capacity 1,000 with maximum
batch size 1,000, configured 60-second interval, and output capacity one. No
batching-ticker schedule or delivery is runtime-proven. Go dequeues before
acquiring its semaphore, can retain one acquire waiter beyond input capacity
plus callback concurrency, starts detached callbacks, and does not join them.
`crawler.start` starts its worker detached, waits only for `stopped`, defers
shared-context cancellation, and joins neither the worker nor callbacks. The
source row also freezes Go's unchecked closed-input loop as source evidence
rather than executing it.

Runtime rows execute actual Go `runScrape` and `requestScrape` through a
manual callback lane, scripted client, tracing wrapper over an actual KTable,
and controlled discovery and raw persist-sources inputs. They do not execute
`runPersistSources`. No UDP, DNS, live DHT, PostgreSQL, model conversion, or
database writer participates.

The generator freshness test passed once and in 100 consecutive runs, its race
focus passed ten consecutive runs, and `go vet ./internal/dhtcrawler` passed at
the oracle checkpoint.

## Frozen Go request-metainfo behavior

Go oracle checkpoint `2f1f7c7292b749b8ef8af3aae6bf1214d2d26651`
generated `testdata/parity/dht/dht_crawler_request_meta_info.jsonl` with
SHA-256
`03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5`.
It contains exactly eight ordered rows:

1. `production_source_factory_and_lifecycle_contract` (`SOURCE_ONLY`) pins the
   production worker, factory, route, requester, banning, blocking, and
   persistence-input source contract without executing a runtime row;
2. `zero_peers_returns_nil_error_and_emits_zero_parsed_info`
   (`RUNTIME_WITH_OWNED_SHUTDOWN_DELTA`) freezes Go's empty `errors.Join`
   result and resulting zero-valued parsed-info handoff as a bug that the owned
   Rust worker deliberately hardens by dropping rather than reproducing;
3. `ordered_duplicate_peers_fail_through_to_first_allowed_hybrid_success`
   (`RUNTIME_EXACT`) freezes sequential duplicate attempts, failure
   fallthrough, first allowed success, and exact original-route plus parsed
   v1/v2 hybrid identity in one persistence-input handoff;
4. `all_peer_failures_join_in_attempt_order_and_preserve_causes`
   (`RUNTIME_EXACT`) calls `doRequestMetaInfo` directly and freezes the joined
   error text in peer-attempt order plus `errors.Is` identity for every cause;
5. `banned_success_invokes_block_hash_false_ignores_block_error_stops_and_emits_none`
   (`RUNTIME_EXACT`) freezes the actual combined default banning checker
   reporting short name, small size, and invalid UTF-8; one
   `Block(ctx, [hash], false)` call; ignored block failure; no later peer; and
   no persistence-input handoff;
6. `cancellation_during_first_request_error_continues_remaining_peers_with_same_cancelled_context`
   (`RUNTIME_WITH_OWNED_SHUTDOWN_DELTA`) freezes Go continuing to the next peer
   with the same already-cancelled context after the pending first request
   returns `context.Canceled`;
7. `cancelled_before_scripted_success_still_checks_ban_and_eagerly_evaluates_unavailable_persist_in`
   (`RUNTIME_WITH_OWNED_SHUTDOWN_DELTA`) freezes that continuation reaching a
   scripted success, still running the banning check, eagerly evaluating
   `persistTorrents.In()`, and delivering nothing while the unbuffered send is
   unavailable; and
8. `lane_error_is_swallowed` (`GO_ONLY_LANE`) freezes `runRequestMetaInfo`
   returning after its manual input lane errors without invoking a callback.

`doRequestMetaInfo` attempts peers sequentially in input order, including
duplicates, and passes the shared callback context, original hash, and exact
peer address to every requester call. Request failures fall through. The first
request success is checked for banning and, if allowed, returned immediately;
remaining peers are not attempted. If banning fails, Go calls
`Block(ctx, []protocol.ID{hash}, false)`, ignores that call's error, returns the
ban error, and stops. The `false` argument disables an explicit flush request;
it does not prove that the real manager avoids a flush when its independent
`shouldFlush` policy is true.

When every requester attempt fails, `errors.Join` retains failure order and
cause identity. With zero peers, `errors.Join` over the empty slice is `nil`,
so the worker treats the zero response as success and offers a handoff with
zero `ParsedInfo`. After a non-error result, the worker's output `select`
eagerly evaluates `persistTorrents.In()` and races that send against the shared
context. The fixture records the accessor call and unavailable-send outcome,
but does not choose a winner for an equal-ready select.

The hybrid row loads the pinned torrent fixture and executes the actual
`ParseMetaInfoBytes` verifier before scripting its requester response. It pins
the v1 identity
`631a31dd0a46257d5078c0dee4e66e26f73e42ac`, full v2 identity
`d8dd32ac93357c368556af3ac1d95c9d76bd0dff6fa9833ecdac3d53134efabb`,
metadata version two, and parsed name. It does not execute a TCP requester or
wire exchange.

The source row pins 20 normalized AST digests, 16 source-file digests, four
prerequisite fixture digests, six full evidence commits, and 17 explicit
nonclaims. At default scaling it records request-metainfo input capacity 100,
callback concurrency 400, and persist-torrents input capacity 1,000 with
maximum batch size 1,000, configured 60-second interval, and output capacity
one. Go dequeues before semaphore acquisition, can retain one acquire waiter
beyond input capacity plus callback concurrency, starts detached callbacks,
and does not join them. `crawler.start` starts the worker detached, waits only
for `stopped`, defers shared-context cancellation, and joins neither the worker
nor callbacks. Its unchecked closed-input loop is source-only evidence; it is
not executed by the oracle.

Runtime rows execute actual Go `runRequestMetaInfo` or `doRequestMetaInfo`
through a manual callback lane, scripted requester and blocking manager, and
controlled persistence input. Separately, hybrid fixture setup executes the
actual parser, while the banned runtime row wraps the actual default combined
banning checker. They do not execute `runPersistTorrents`, TCP, DNS, live
networking, PostgreSQL, model conversion, database writes, or nonempty
blocking-manager persistence. The generator freshness test passed once and in
100 consecutive runs, its race focus passed ten consecutive runs, and
`go vet ./internal/dhtcrawler` passed at the oracle checkpoint.

### Strict Rust request-metainfo consumer

The strict consumer recursively rejects unknown JSON fields, pins the exact
fixture SHA-256, final LF, eight-row order and IDs, and the one source-only,
three runtime-exact, three owned-shutdown-delta, and one Go-only-lane class
partition. It also pins all 20 normalized AST digests, 16 source-file digests,
four prerequisite fixture digests, six evidence commits, and the source row's
17 ordered nonclaims; it separately pins 17 Rust nonclaims. Exact source,
prerequisite, and evidence maps and both ordered nonclaim vectors must match;
extra, missing, reordered, or duplicated entries fail closed.

The six Rust runtime rows exercise the actual owned worker or its private
ordered-attempt seam, the real typed routes, the actual Rust parser, and the
default banning checker:

1. row two proves the empty-peer hardening drop and absence of a zero
   `ParsedInfo` handoff;
2. row three parses the pinned hybrid torrent and reproduces ordered duplicate
   failures, scoped IPv6 identity, first success, skipped suffix, exact event
   order, original route identity, parsed v1/v2 identity, and route EOF;
3. row four reproduces ordered all-failure display and every original error
   identity, then proves the worker's local all-failed drop and terminal stats;
4. row five parses raw invalid-name bytes, reproduces the exact short-name,
   small-size, and invalid-UTF-8 default ban, blocks the original requested hash
   with `false`, ignores the scripted block error, and emits no output;
5. row six cancels actual owned work during the pending first request, proving
   that Rust drops the current and remaining suffix instead of continuing with
   a cancelled context; and
6. row seven fills the real 1,000-slot torrent route, reaches its actual blocked
   send, and proves shutdown cancellation preserves the committed prefix while
   the pending persistence request never commits.

Row seven is deliberately a conceptual mapping from Go's unavailable-output
observation to owned Rust output cancellation; it is not an exact replay of
Go's peer sequence. Row eight partitions Go's swallowed manual-lane error from
Rust's typed `InputClosed` behavior and does not claim that the latter is a Go
runtime result. The source-only row is never executed as Rust runtime behavior.

The banned row's scripted parsed v1 hash
`80b26192d4afd1a76f8a52d1899bc59af904c0b8` intentionally differs from its
requested hash ending in `00cc`. It proves worker routing and blocking identity,
not end-to-end requester hash verification. Go's `U+FFFD` is only the lossy
JSON display projection; Rust retains the original raw `0xff` name byte.
Passing `flush = false` proves the worker argument, not that a future concrete
blocking-manager adapter cannot flush under its own policy.

The nine focused strict-consumer tests passed in 100 consecutive runs. The
current Go generator freshness test regenerated the same fixture SHA, and the
full crawler suite contained 125 passing unit tests plus the concrete
composition integration test. Strict Clippy, rustdoc, formatting, and diff
checks passed at the consumer checkpoint. This evidence uses only pinned
fixtures, scripted collaborators, and in-memory routes.

## Evidence boundaries and nonclaims

The triage Go fixture does not claim map iteration, SQL result, or downstream
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

The Rust consumers, workers, PostgreSQL adapter, blocking-manager adapter, and
offline composition test do not claim exact Go map, SQL-result, or delivery
order; exact Go SQL text or bind cardinality; exact Go wall-clock values or
per-item `time.Now()` schedule; live PostgreSQL array encoding, schema
compatibility, indexes, query plans, server-side cancellation, transactions,
retries, statement timeouts, or pool configuration; a live persistent
production blocking Bloom state or nonempty flush durability; Go's detached
batcher timing, output boundary, or close behavior; downstream scraped-source
persistence or metainfo processing; Go select ties, eager lane operands, or
fairness; Go logging; cross-route retention or waiter fairness; closed Go
output behavior; live DHT traffic; upstream sample provenance; concurrent
upstream pending-send drain accounting; supervisor/application/deployment
wiring; or production readiness.

The get-peers strict consumer executes its six runtime rows through the actual
Rust worker core. Rows two through four reproduce the bounded Go command and
handoff observations; rows five through seven freeze the deliberate owned
shutdown deltas. The source-only row and Go-only lane-error row are not Rust
runtime behaviors. The source row's pinned
`Rust_public_API_or_owned_task_lifecycle_no_Rust_consumer_exists_in_this_slice`
nonclaim describes the earlier Go-oracle checkpoint, not the current
repository.

The get-peers fixture and consumer do not claim Go ready-select winners or
eager-operand side effects; callback scheduling or completion order; semaphore
or channel fairness; closed Go input execution; callback joining; actual
elapsed one-second timing; send-to-closed Go-channel behavior; exact
`NodeResponded` timestamps; KTable iteration, eviction, or internal layout;
opaque Go node-option identity or a stored Rust error cause; live DNS, UDP,
DHT, client, or wire behavior; downstream discovery processing; metainfo
requesting, banning, blocking, or persistence; torrent-source database or
nonempty durability behavior; production throughput, retention, or waiter
fairness; application supervision, deployment, or readiness; arbitrary textual
IPv6 zones beyond numeric scopes; Go lane-error semantics; or concurrent
external pending-send accounting.

The scrape strict consumer executes its six runtime rows through the actual
Rust worker core. Rows two through four reproduce the bounded command,
discovery, ignored-peer, and raw-filter handoff observations. Rows five through
seven freeze the deliberate owned-shutdown deltas with exact terminal and stats
accounting. The source-only row and Go-only lane-error row are not Rust runtime
behaviors. The source row's historical no-Rust-consumer nonclaim describes the
Go-oracle checkpoint, not the current repository.

The Rust consumer does not reproduce exact Go ready-select winners or eager
operand effects. The Go fixture freezes only the recorded `In()` accessor call
counts and claims no arbitrary operand side effects beyond them; neither
surface claims production callback scheduling or completion order, semaphore
fairness, closed-input execution, or callback joining. They also do not claim
actual elapsed one-second timing; send-to-closed Go-channel behavior; exact
`NodeResponded` timestamps; KTable iteration, eviction, or internal layout;
opaque Go node-option identity or Rust storage of the Go error cause; Bloom
capacity, hash count, set-bit count, or `ApproximatedSize` runtime assertions;
high-density `ApproximatedSize` projection before the database persistence
writer; live DNS, UDP, DHT, client, or wire behavior; downstream discovered-node
processing; `runPersistSources` batching, deduplication, model conversion, or
database behavior; torrent-source database or nonempty durability behavior;
production throughput, total retention, or waiter fairness; application
supervision, deployment, or readiness; arbitrary textual IPv6 zones beyond
numeric scopes; Go lane-error semantics in the owned Rust route; or concurrent
external pending-send accounting beyond prequeued fixture inputs. Runtime
handoffs assert exact raw 256-byte Bloom identity and direction only.

The request-metainfo strict consumer executes six owned Rust rows. Rows two
through five prove the bounded hardening, ordering, parsed identity, default
policy, blocking, and handoff observations described above. Rows six and seven
freeze deliberate owned-shutdown deltas with exact terminal and stats
accounting; row seven remains a conceptual owned-output cancellation mapping,
not an exact Go peer-sequence replay. The source-only and Go-only lane rows are
not Rust runtime behaviors. The source row's pinned
`Rust_public_API_or_owned_task_lifecycle_no_Rust_consumer_exists_in_this_slice`
nonclaim is historical evidence about the earlier Go-oracle checkpoint, not
the current repository.

Neither fixture nor Rust consumer claims exact Go ready-select winners or
eager-operand side effects beyond the recorded input accessor; Go callback
scheduling or completion order; semaphore or channel fairness; closed Go input
execution; callback joining; send-to-closed Go-channel behavior; production
throughput, total retention, or waiter fairness; arbitrary textual IPv6 zones
beyond numeric scope; or concurrent external pending-send accounting outside
the fixture's prequeued inputs. Rust's typed input EOF does not reproduce Go's
manual lane-error semantics.

They also do not claim a live metainfo TCP handshake, extension negotiation,
piece transfer, requester, or end-to-end requester hash verification;
production banning behavior beyond the frozen default-checker row; real
blocking-manager buffering, Bloom state, policy flush, database, or durability;
`runPersistTorrents` batching, deduplication, model conversion, or database
behavior; batching ticks, logs, metrics, or persisted-counter delivery; or
application supervision, deployment, or production readiness. The scripted
banned-row hash mismatch, raw-byte versus lossy `U+FFFD` distinction, and row
seven's conceptual mapping remain explicit nonclaims rather than gaps hidden by
the runtime assertions.

The oracle has no live PostgreSQL, network, DNS, UDP, DHT, or deployment
dependency. Passing it establishes the bounded source and controlled Go-oracle
contract only. The implemented strict Rust consumer separately establishes the
bounded deterministic replay and deliberate deltas documented above; neither
gate alone establishes production composition or readiness.

## Pending integration

The following remain deliberately outside this checkpoint:

- production application ownership and nonempty shutdown flushing of the
  persistent blocking manager, plus metrics and operator-facing failure policy;
- application construction of `PgDhtTorrentTriageLookup` with a configured
  pool, plus live schema/codec/query-plan validation and database
  observability;
- ownership of the unique triage receiver by an application or higher-level
  crawler supervisor;
- application construction and shutdown wiring between the existing DHT
  sample-infohashes maintenance path, the triage worker, and the get-peers
  worker;
- production application construction, supervision, metrics, and operator
  policy for the existing Rust get-peers worker;
- production application construction, supervision, metrics, retry, and
  operator failure policy for the existing Rust scrape worker, plus the
  downstream database persistence writer that will own high-density
  `ApproximatedSize` projection;
- a concrete peer-wire `DhtMetaInfoRequester` implementing TCP, BEP-10/BEP-9
  extension negotiation and piece transfer, plus end-to-end requested-hash
  verification;
- a concrete `DhtInfoHashBlocker` adapter to the persistent blocking manager,
  including production flush and failure policy;
- the downstream torrent-persistence batcher, deduplication, model projection,
  database writer, and ownership of the unique torrent-persistence receiver;
- production construction, supervision, shutdown wiring, metrics, retry, and
  operator failure policy for the existing Rust request-metainfo worker;
- a producer-side `closed()` waiter on the typed triage input route;
- configuration loading, health reporting, metrics export, and operator-facing
  diagnostics; and
- live traffic, deployment configuration, rollout, migration, or operational
  readiness.

The existing DHT maintenance supervisor borrows a triage input capability for
the sample-infohashes worker but does not own this crate's unique triage
receiver, construct `DhtInfoHashTriageWorker`, or monitor it as a child. The
typed scrape input and scraped-source output routes now have an owned Rust
scrape consumer between them, but no production application path constructs,
runs, or supervises it. The get-peers route likewise has an owned Rust
consumer without production application ownership. The concrete offline test
composes the isolated triage worker and its persistent collaborators without
polling them, but no current production application path constructs or
supervises the triage, get-peers, and scrape workers as a pipeline. The
request-metainfo route has the get-peers worker as a producer and the owned
Rust request-metainfo worker as its consumer; allowed results flow through the
typed torrent-persistence route. No production application constructs or
supervises that worker, no concrete peer-wire requester or persistent blocker
adapter supplies it, and no downstream persistence worker owns the unique
torrent-persistence receiver in this checkpoint.
