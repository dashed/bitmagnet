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
active-depth cap. Its `(ran_at,id)` cursor is caller-owned because the frozen
shadow permission boundary grants writes only to `queue_jobs`.

This cursor is not atomic with scratch insertion. The mirror is therefore
single-replica and **not deployable yet**: a durable cursor/source ledger must
be approved and committed in the same transaction before a crash-safe runtime
can be enabled. The advisory lock protects the depth cap, not stale cursors
held by separate replicas.

Rust deliberately uses PostgreSQL `clock_timestamp()` for eligibility,
deadline, settlement, and retry scheduling instead of binding the application
clock as Go does. This keeps every decision on the database clock while still
preserving the frozen ordering and retry-count semantics. The consumer returns
database errors to its caller and currently requires an external supervisor to
restart or back off; it does not silently spin on an unavailable database.

The migration-backed PostgreSQL gate covers frozen dequeue ordering,
`SKIP LOCKED` concurrency, retry and deadline settlement, JSONB-preserving
scratch insertion, handler panic recovery, delay, cursor advancement, and
depth-cap behavior.

Still outstanding before Go queue retirement: the general producer API,
bounded multi-worker orchestration for queues that require concurrency greater
than one, terminal-row garbage collection, runtime metrics, and a deployment
decision for durable mirror cursor checkpointing.
