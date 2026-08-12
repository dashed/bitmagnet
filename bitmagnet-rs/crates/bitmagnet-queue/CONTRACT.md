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

Migration 29 supplies the row-scoped write boundary as `SECURITY DEFINER`
capabilities. They hardcode `process_torrent_shadow` for enqueue/claim/settle
and the single `(process_torrent,process_torrent_shadow)` cursor identity,
qualify every database object, run with `search_path=pg_catalog,pg_temp`, and
revoke `PUBLIC` execution. The runtime role needs read-only table grants plus
explicit `EXECUTE` on these functions; it must receive no direct write grant on
`queue_jobs` or `queue_mirror_cursors`. Before granting runtime execution,
deployment automation must transfer all seven functions away from the Goose
executor to a dedicated non-login, non-superuser owner with only the table
rights needed by the fixed function bodies. This prevents a future function
bug from executing with the migration superuser's authority. Deployment
remains gated until ownership and the minimal runtime grants are catalog-proven
and the negative controls pass in-cluster.

The capabilities protect live data, not shadow telemetry from a compromised
shadow credential. Within the fixed scratch boundary, that credential can
advance or rewind the cursor, enqueue arbitrary scratch payloads, and settle
other scratch jobs. Those are accepted shadow-integrity risks for this dark
pilot and must never be described as tenant isolation.

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
that a stale replica observes the committed cursor. Its permission negative
controls prove a minimally granted role can use the shadow capabilities but
cannot directly mutate either queue or any cursor identity.

Migrations 31 and 32 bound the batch handler's database access behind fixed
`SECURITY DEFINER` capabilities. Selection hardcodes the `torrents` and
`torrent_contents` query, strict info-hash cursor, snapshot cutoff, nullable
content filter, orphan exclusion, ascending order, and page limit. Plan enqueue
accepts no queue name or fingerprint: it hardcodes `process_torrent` children
and at most one final `process_torrent_batch` continuation, computes each
fingerprint from the exact raw JSON text before casting it to JSONB, and fixes
pending status, retry count, maximum retries, archival duration, and null
settlement fields. Both functions qualify database objects, pin
`search_path=pg_catalog,pg_temp`, and revoke `PUBLIC` execution. The batch
runtime therefore needs explicit `EXECUTE` on these functions but no direct
`SELECT` on source tables or `INSERT` on `queue_jobs`. As with migration 29,
deployment remains blocked until automation transfers function ownership to a
reviewed least-privilege NOLOGIN owner and proves the runtime grants in-cluster.

Planning the returned ordered hashes into child and continuation queue jobs is
byte/fingerprint-gated against Go. Callers materialize each logical job's
absolute `run_after` when the job is planned; one application-clock
`created_at` is shared by the atomic multi-row insert, matching GORM
slice-create behavior. Migration 32 issues one INSERT and never uses
`ON CONFLICT`: an active pending/retry fingerprint collision returns SQLSTATE
23505 and inserts no siblings. A processed or failed row does not block the
same fingerprint from being queued again. The general strict producer remains
available for non-batch callers and tests, but the batch handler uses only the
fixed migration-32 capability.

The strict producer runs as its own database statement/transaction. This
preserves Go's surprising parent/child boundary: child jobs may commit before
the parent batch job is settled, and a later parent retry can then encounter
the active-fingerprint constraint. A future Rust batch handler must therefore
use a pool with at least two connections while `consume_one` retains the parent
row transaction, and it must materialize children immediately after each page
instead of assigning every child the same `run_after` at final insertion.

The crate now exposes that select-plan-insert orchestration as a callable batch
handler without registering any runtime consumer. It decodes one constructor-
normalized payload, selects and plans each page, captures each child timestamp
at that page boundary, captures the continuation timestamp after finalization,
and then invokes the strict producer once. Zero `BatchSize` or `ChunkSize` is
rejected fail-closed because every supported producer injects those defaults
before persistence. Migration-backed tests pin chunk overshoot, job order,
timestamps, active-fingerprint retry failure, and the empty-result no-op. An
activated production consumer remains deliberately absent, so this milestone
cannot compete with the live Go `process_torrent_batch` worker.

A standalone `bitmagnet-process-torrent-batch` binary composes the callable
handler with the serial queue consumer for offline/runtime testing. A dedicated
`Dockerfile.process-torrent-batch` now builds only that executable from the
repository-root context required by the compile-time classifier-core embed,
runs it as UID/GID 65532, and binds exact commit/tree provenance plus a versioned
`process_torrent_batch/v1` consumer contract into validated OCI labels and
environment variables. CI builds the Linux/amd64 production-shaped image,
asserts its identity, non-root user, queue-consumer label, and entrypoint, then
executes `--help` through the image's default entrypoint in a read-only,
networkless, capability-free container without requiring a database. The image
has no default arguments and does not by itself claim queue ownership or
concurrency exclusion; those remain deployment concerns. The binary pins the Go
handler's defaults of a 30-second idle poll and 10-minute job timeout, bounds
PostgreSQL pool acquisition, and refuses a configured connection maximum below
two because the retained parent transaction and the handler's independent
selection/insertion need separate connections. The binary is not copied into a
shared serving image or wired into deployment automation. Its current shutdown
path stops polling and drains the in-flight job through parent settlement; the
migration-backed gate proves shutdown cannot cancel that handler. The 10-minute
handler timeout still cancels work at its deadline. If a child committed first,
it survives independently, the parent settles to retry, and the retry observes
the strict SQLSTATE 23505 collision; this boundary is also migration-backed
tested and preserves Go's independent child transaction semantics.

The PostgreSQL cleanup primitive now preserves Go's one-pass terminal-row
predicate exactly: only `processed`/`failed` rows with
`ran_at + archival_duration < cutoff` are deleted, null `ran_at` values survive,
and the affected-row count is returned. Periodic immediate-then-10-minute
scheduling is implemented as an unactivated, shutdown-aware loop: each attempt
starts immediately or ten minutes after the preceding attempt finishes, and a
database error is logged without ending the loop. Go remains the sole live
global cleanup owner until an explicit ownership handoff. Shutdown interrupts
the wait but drains an already-started sweep; SQLx future drop does not prove a
server-side PostgreSQL cancellation, so the loop never returns while its DELETE
may still be completing.

Migration 33 bounds the batch worker's async queue-depth snapshot behind a
no-argument `SECURITY DEFINER` capability. It hardcodes both the selected queue
and returned `process_torrent_batch` label, returns only nonempty status groups,
qualifies `queue_jobs`, pins `search_path=pg_catalog,pg_temp`, and revokes
`PUBLIC` execution. The runtime needs explicit `EXECUTE` but no direct table
read; owner transfer and runtime grants remain deployment-gated. The queue
adapter builds a fresh, unregistered
`bitmagnet_queue_jobs_total{queue,status}` gauge family from each awaited
batch-only snapshot and emits no family when that queue is empty. Other queues
and synthetic zero groups never appear. The common metrics server can await
such families per scrape. When explicitly configured with
`BITMAGNET_METRICS_ADDR`, the still-non-deployed batch worker now awaits that
fresh family per scrape; query failures omit only the queue family and leave the
rest of the HTTP 200 metrics response intact. At the minimum two-connection pool
size a scrape can contend with a retained parent transaction plus handler query;
the bounded acquisition failure degrades only that scrape's queue family.

Still outstanding before Go queue retirement: guarded single-consumer deployment
wiring for the batch image, bounded multi-worker orchestration for queues that
require concurrency greater than one, terminal-row cleanup ownership handoff,
deployment metrics plumbing, and the Lane P shadow processor/runtime deployment.
