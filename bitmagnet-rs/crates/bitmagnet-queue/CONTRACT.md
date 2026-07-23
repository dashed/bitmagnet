# Lane Q — PG job-queue substrate

Owns the queue-job wire/DB contract: the job fingerprint, the three job types
and their payload JSON, the `QueueJob` row + status enum, and dequeue
ordering / retry-backoff / GC / poll semantics. This is the substrate the
Phase-4 producer seam and the Lane P processor build on.

Contract: [`docs/dev/rust-rewrite/phase3-contracts.md`](../../../docs/dev/rust-rewrite/phase3-contracts.md) §1 (Queue-job wire/DB contract).

## Live PostgreSQL runtime milestone

The crate now owns the transactional `FOR UPDATE SKIP LOCKED` claim and
settlement path, preserving the frozen pending-before-retry ordering and Go
retry/deadline semantics while retaining the row lock through handler
execution. The serial poll-only consumer self-chains after a claimed job and
sleeps only when the queue is empty; handler timeouts cancel the Rust future.

The processed-row mirror copies the stored JSONB value into
`process_torrent_shadow`, uses deterministic source-ID sampling, serializes
mirror writers with a PostgreSQL advisory transaction lock, and enforces a hard
active-depth cap. Admission is fail-closed: only payloads with
`ClassifierWorkflow="default"` and
`local_search_enabled`, `apis_enabled`, and `tmdb_enabled` all explicitly
`false` in `ClassifierFlags` may enter the shadow queue while the Rust
`attach_*` actions remain unsupported. An omitted workflow is rejected because
Go resolves it through mutable deployment configuration. Every requested
info-hash must also still exist, must not have changed since the source job
settled, none may have any `torrent_hints` row, and none may have a
source-backed `torrent_contents` association. Omitted/default-true, deleted,
explicit-hint, and source-backed rows are scanned and checkpointed but never
inserted. Rust does not yet model all fields of an explicit hint, so even a
type-only hint is excluded. This
admission query requires read access to `torrents`, `torrent_hints`, and
`torrent_contents`; it takes no live-row locks. Migration 28 adds a durable cursor keyed by the
`(source_queue,shadow_queue)` mirror identity. Each page locks and reads that
row after acquiring the target-queue advisory lock, then commits its scratch
inserts and cursor advance in one transaction. Waiting or restarted replicas
therefore cannot replay a process-local stale cursor or skip committed source
rows. When the active cap blocks a sampled candidate, the transaction does not
advance past that candidate.

The production-safe bootstrap creates a new mirror identity at
`clock_timestamp()` and does not silently replay the retained archive. Scanning
from the oldest retained row or from an explicit `(ran_at,id)` position requires
an explicit bootstrap choice; once created, the durable row remains authoritative
and later process configuration cannot reset it.

The runtime is not production-deployable until queue writes are constrained by
reviewed RLS or security-definer operations. Broad `INSERT`/`UPDATE` grants on
`queue_jobs` would let a faulty shadow process mutate live `process_torrent`
rows despite the queue-name checks in Rust. Production negative controls must
reject live-queue writes as well as live-table writes.

Rust deliberately uses PostgreSQL `clock_timestamp()` for eligibility,
deadline, settlement, and retry scheduling instead of binding the application
clock as Go does. This keeps every decision on the database clock while still
preserving the frozen ordering and retry-count semantics. The consumer returns
database errors to its caller and currently requires an external supervisor to
restart or back off; it does not silently spin on an unavailable database.

The migration-backed PostgreSQL gate covers frozen dequeue ordering,
`SKIP LOCKED` concurrency, retry and deadline settlement, JSONB-preserving
scratch insertion, handler panic recovery, delay, cursor advancement, and
depth-cap behavior. The gate also verifies durable restart/resume behavior and
that a stale replica observes the committed cursor.

Still outstanding before Go queue retirement: the general producer API,
bounded multi-worker orchestration for queues that require concurrency greater
than one, terminal-row garbage collection, runtime metrics, and the Lane P
shadow processor/runtime deployment.
