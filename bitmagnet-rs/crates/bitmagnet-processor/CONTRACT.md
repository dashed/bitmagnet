# Lane P — processor orchestration + write-shadow

Owns processor orchestration: wiring Lane Q (queue), Lane R (release parse),
and Lane C (classifier) into the full processing path, and the write-shadow
strategy (the Go processor's write-set, the shadow mechanism, bounded
resource caps, fail-open safety semantics, and the operating-rule cutover).
The write-shadow design was reviewed and approved on 2026-07-17, including the
archived-payload mirror amendment. See the frozen contract before implementing
the remaining runtime.

Contract: [`docs/dev/rust-rewrite/phase3-contracts.md`](../../../docs/dev/rust-rewrite/phase3-contracts.md) §5 (FULL write-shadow strategy); orchestration also draws on §4 (summary-write) and §1 (queue).

## Milestone 1: write-set materialization

The crate now implements the pure classifier-to-write-set boundary and gates it
against a 330-record Go oracle generated through the public Go classifier
factory plus the package-private `newTorrentContent` helper. The stable output
contains `torrent_contents`, stale-ID deletes, whole-info-hash delete signals,
tag/content placeholders, and failed hashes for queue republish.

This milestone intentionally stops before DB behavior:

- `LoadedTorrent.classifier_input` must already contain the effective hint from
  the read/hydration stage;
- runtime classifier overrides currently accept the core bool flags only;
- at this historical checkpoint attached-content persistence was rejected; the
  later persistence and disconnected-writer milestones now carry the base
  content TSV and complete association rows; and
- persistence, Tantivy dual-write, queue polling/mirroring, live-row diffing,
  and the SELECT-only role negative control require the Coder+PG milestone.

The frozen core classifier uses no `add_tag` actions, and the flags-off corpus
never attaches content, so these limitations do not weaken the 330/330 first
milestone gate. They are explicit blockers for claiming the DB/shadow milestones
complete.

## Milestone 2: supported-path PostgreSQL transaction kernel

`persist_write_set` now reproduces the Go transaction order for the currently
supported unattached-content path: blocker call before the transaction, stale
`torrent_contents` deletes, 100-row classified-content upserts, 100-row
`torrent_tags` inserts with `ON CONFLICT DO NOTHING`, and whole-torrent deletes.
It requires the source-derived `seeders`/`leechers`/`published_at`/`tsv` image
explicitly because those fields are intentionally excluded from the stable M1
comparison projection. It also converts the normalized episode string back to
Go's JSONB map shape and validates Go's tag-name hook.

The milestone-2 live-PostgreSQL gate ran against goose 26 and proved upsert/delete/tag
semantics, blocker-before-delete ordering, exact microsecond timestamps, and
full rollback when a late tag write fails.

The concrete persistent-manager adapter checkpoint is
`5f4f1d92c85222ab92476dcee977ce27f7addacc`. It parses the complete deletion
batch before mutation, delegates exactly once to the shared blocking manager
with `flush = false`, and preserves input order and duplicates until the
manager-owned set buffer. Parse or manager failure aborts before the processor
transaction and retains the original error in the existing boxed source chain.
At that checkpoint, all 52 processor release tests passed, with the one
explicitly opt-in PostgreSQL test ignored; release all-target checking, Clippy
with warnings denied, rustdoc with warnings denied, formatting, and diff checks
also passed. These are offline source and compile gates for the adapter, not a
new live-PostgreSQL or large-object validation.

Rust deliberately handles a deletion-only write set even though Go currently
returns early when a batch produces no `torrent_contents`, dropping deletion
hashes collected from that batch before `persist` can block or delete them.
`PreparedWriteSet::is_empty` includes whole-info-hash deletes, and an offline
regression test freezes that correctness improvement. Exact parity is claimed
for the pre-transaction blocking boundary once Go reaches `persist`, not for
that Go orchestration bug.

The writer-plan checkpoint composes the bounded writer loader,
the real materializer, and the pure volatile-field projection into the exact
`WriteSet` plus `torrent_contents.id`-keyed metadata image accepted by
`persist_write_set`. It borrows hydrated classifier inputs rather than cloning
their potentially large file lists, requires unique matching source keys, and
requires an explicit `default` workflow with all three attach flags false. It
fails closed on any unresolved hint/enrichment path, attached content,
incomplete projection keyset, or transaction-kernel input validation error.
The immutable plan retains failed hashes as retry intent; matching Go requires
the future persisting runtime to enqueue those retries successfully before
persisting its successful rows. The ingest-shadow runtime now calls the composer
inside its read-only comparison transaction, but neither the composer nor that
runtime calls persistence.

The dormant `persist_writer_plan` coordinator now closes that ordering seam
without taking queue ownership. A partial-success plan carries a Go-compatible
`process_torrent` replacement job (same classify mode, explicit workflow,
classifier flags, exact missing-hash prefix, and max-retries value). The stable
`WriteSet.failed_info_hashes` remains sorted and deduplicated for comparison, but
the plan now carries separate fingerprint-significant retry order: every missing
requested hash first, in original request order and including duplicates, followed
by loaded classifier failures in Rust's first-request processing order. Go appends
that classifier-failure suffix from concurrent goroutines, so its concrete order
is nondeterministic. Rust's suffix is the same logical retry set and a valid Go
completion order. Its fingerprint follows the shared queue contract for that
concrete order, but production mixed-owner dedup must not rely on Rust and Go
constructing the same fingerprint for a batch with loaded classifier failures.

The coordinator requires a `RetryPublisher` to durably insert that job, accepting
an active-fingerprint conflict as Go's `ON CONFLICT DO NOTHING`, before it invokes
the existing blocker/transaction kernel. `QueueStore` now implements that boundary
with a one-statement auto-commit insert and returns a typed receipt distinguishing
the new row ID from an active-fingerprint conflict. The coordinator exposes that
receipt on success and carries it on any later persistence error instead of
discarding evidence of the durable side effect. Publish failure prevents any blocker or
live-table mutation; a later persistence failure deliberately does not retract the
already-durable retry. When every classification failed it returns an error without
publishing or persisting so the source queue job owns ordinary retry handling. It
also freezes Go's second early return: a plan with no classified
`torrent_contents` and no failed hashes is acknowledged without invoking the
kernel, even if it contains delete signals. The kernel's independently callable
deletion-only correctness improvement described above remains unchanged.

The plan also retains a structured ordered failure ledger separate from its
canonical comparison set. One aggregate missing-hash cause preserves the original
request order and duplicates and renders exactly `missing N info hashes`; loaded
classifier errors follow in Rust's first-request processing order with their raw
messages. `AllFailed` renders those causes newline-separated like Go's
`errors.Join`. Retry-publication failure appends the publisher error to the same
image, while a persistence failure after successful publication carries only the
persistence error plus the durable retry receipt. The classifier suffix remains
one valid Go goroutine-completion order, not a claim that mixed owners choose the
same concrete order.
This preserves Go's visible joined text and exposes every structured cause, but
Rust's single-source `Error` trait does not reproduce Go's multi-error
`Unwrap() []error` / `errors.Is` traversal. Retry-job construction is infallible
for the currently admitted bool-only payload; if that payload grows, a build
failure must join this ledger before mutation activation.

Focused fake-based tests freeze exact retry JSON/fingerprint bytes, missing-hash
order and multiplicity independent of the canonical comparison set, publish-
before-persist ordering, exact joined failure rendering, publish-failure
short-circuiting, retry survival and receipt retention after a late persistence
error, all-failed handling, the deletion-only early return, and
the public seam's publish-failure short circuit before both the blocker and a
closed database pool. No consumer, application wiring, migration, Tantivy
dual-write, or live PostgreSQL mutation was added by this checkpoint. Direct
`QueueStore` publication still requires table `INSERT` and therefore remains a
source/test seam; production activation requires a fixed least-privilege enqueue
capability.

This is not yet the complete runtime persistence milestone:

- the persistence kernel now accepts complete attached `content` images and
  owns their additive attribute, collection, and link rows; the disconnected
  writer admits reused rows only after bounded full hydration;
- the plan still needs a persisting writer runtime and live-queue lifecycle after
  the remaining safety gates pass;
- application composition still needs to share the manager across callers and
  flush it after all producers stop but before the PostgreSQL pool closes; and
- at milestone 2, queue retry wiring, post-commit Tantivy dual-write, the
  poll-mirror, and the full write-set comparator remained outstanding. The
  later shadow-only pieces are now covered by milestones 4 and 5.

## Milestone 3: live snapshot and permission fail-safe

`read_live_snapshot` now reads the stable comparison image with non-locking
`SELECT`s over `content`, `torrent_contents`, `torrent_tags`, and `torrents`.
The runtime hydration path additionally reads `torrent_hints`. It canonicalizes
languages, episodes, content rows, and tags, and
represents a Go-deleted torrent as the first-class `live_absent` outcome.

The PostgreSQL gate creates a restricted test role with SELECT on those five
stable-image tables, `torrents_torrent_sources` for the writer projection, and
`queue_jobs` for exact source identity. It proves the runtime can read and plan
inside that boundary, settle a scratch job through the scoped functions, and
cannot update a live torrent: PostgreSQL rejects the negative control with
`insufficient_privilege` (`42501`). It also proves the role cannot read the
undeclared `content_attributes` table. Direct queue-table writes are absent;
§5.4's reviewed row-scoped functions remain the only mutation boundary.

Attached-content identifiers remain intentionally empty in the snapshot. The
identifiers live in `content_attributes`, but §5.4 grants the shadow role no
access to that table. Expanding that frozen permission boundary requires an
explicit contract decision; the reader does not silently widen it.

## Milestone 4: pure stable-image comparator

`compare_write_set` now canonicalizes and compares the materialized write-set
against the settled `LiveSnapshot` per §5.2(c). It treats a Rust delete signal
and `live_absent` as a match, reports the opposite outcomes as
`delete_signal` drift, and emits a closed `DriftField` vocabulary for
low-cardinality per-field metrics. Row-level drift is retained alongside
field-level labels so changes in field association cannot be hidden by equal
column multisets.

The stable comparator's types contain only the frozen stable image: content identity
and metadata, `torrent_contents` classification fields including `InferID()`,
canonical languages/episodes, tags, and the delete outcome. Volatile database
and crawl fields (`created_at`/`updated_at`, generated `tsv`,
seeders/leechers/published-at snapshots, and unrelated surrogate IDs) never
enter that API.

A separate bounded writer comparator keys the plan's persistence image by exact
`torrent_contents.id` and compares expected-row presence, seeders, leechers,
and generated `tsv`. PostgreSQL evaluates the text projection using
`tc.tsv = expected.tsv::tsvector`, matching the transaction kernel's
`::tsvector` persistence cast; it does not run `to_tsvector` over
already-serialized tsvector text. The writer reader selects only expected IDs.
Missing expected rows are writer `row_presence` drift, while unexpected or stale
rows remain solely in the stable comparator's ownership. Writer verdict and
drift metrics add only the closed `row_presence`, `seeders`, `leechers`, and
`tsv` labels and do not alter the existing stable metric names.

The persistence projection still computes `published_at` for every insert.
On conflict, however, the Go writer's GORM `UpdateAll` excludes this generated
model field because it carries a database-default tag, preserving the existing
row's value. The Rust transaction kernel deliberately mirrors that behavior by
omitting `published_at` from its conflict-update list. A post-only reader cannot
distinguish an inserted row from a conflict without prior state, so the writer
comparator neither selects `published_at` nor emits a drift label for it. The
bounded campaign controller's before/after row evidence owns proof that an
existing `published_at` value was preserved.

Expected tags use additive semantics: every tag Rust would add must exist in
the live set, while unrelated pre-existing tags remain valid because Go never
deletes them in this path.

Inputs without a trustworthy comparison are rejected rather than counted as
matches: retryable failed hashes, contradictory delete/write outcomes, orphan
content rows, missing live states, or live hashes without a materialized
outcome. Runtime metrics, sampling, mismatch capture, and scratch-job
settlement remain the shadow-consumer milestone.

## Milestone 5: durable supported-subset shadow runtime

The runtime now supplies a bounded read/hydration path, a durable processed-row
mirror, a non-persisting comparator consumer, low-cardinality outcome metrics,
and the `bitmagnet-ingest-shadow` executable with separate `mirror` and
`consume` subcommands. Migration 28 owns the database cursor. The mirror wraps
each admitted payload in a strict version-1 envelope containing the exact source
job ID, settled `ran_at`, and unnormalized JSONB payload; the scratch fingerprint
covers that full envelope. The loader uses per-torrent and per-job
compressed/decompressed/file-count limits and rejects oversized inputs before
transferring their blob.

Admission and runtime checks deliberately support only payloads whose workflow
is explicitly `default`, whose three attach flags are explicitly false, whose
source torrents still exist and have not changed since the source job settled,
which have either no explicit `torrent_hints` row or one bare type-only hint,
and which have no reusable source-backed `torrent_contents` association. A bare
hint has no source, ID, title/date, episode/language/video, or release-group
fields; sourced or enriched hints remain excluded. Unsupported inputs settle
only their scratch job and are counted by a closed reason vocabulary; transient
database/runtime errors retain queue retry behavior. Rematch preserves explicit
Go hint semantics internally while disabling only content-association reuse.
The consumer
revalidates the envelope against the exact retained `process_torrent` row and
rejects a missing/changed source, any other overlapping attempted run at or
after the captured timestamp, any overlapping nonterminal `pending`/`retry` job
regardless of its `run_after`, or a newly ineligible live input. The nonterminal
guard closes the interval where Go is executing a handler but its row has not
yet committed a terminal status or `ran_at`. The runtime also rejects post-source
`updated_at` values in `torrents`, `torrents_torrent_sources`,
`torrent_contents`, `torrent_tags`, and an admitted bare hint; sourced/enriched
hint presence is a categorical rejection. Source validation, bounded writer
loading, `WriterPlan` composition
inputs, stable live comparison, and volatile writer comparison share one
read-only repeatable-read snapshot, preventing a concurrent Go transaction from
creating a torn or cross-run image. The returned causal result binds both
evidence planes to the envelope's exact source job ID and `ran_at`. This is
causal live-state evidence, not historical event replay: row deletion has no
tombstone.

That source-backed exclusion is production-runtime policy, not a remaining
writer-planning limitation. The disconnected `load_writer_plan` path keeps all
attach flags explicitly false and the `NullContentResolver`, but can reuse the
single association selected by Go hint synthesis after its scalar content row,
attributes, collection links, and linked collection rows are all hydrated under
bounded sentinel scans in the same repeatable-read transaction. Sourced or
enriched explicit hints remain categorically unsupported, and the production
runtime's `SourceBackedContentPresent` rejection and shadow-role ACL are
unchanged.

The version-1 rollout can be staged with mirror sampling fixed at zero: the new
consumer remains non-persisting and the only DB mutation is still scoped scratch
settlement. Before enabling a nonzero sample, deployment must grant SELECT on
`torrents_torrent_sources`, confirm the scratch queue is empty of legacy raw
payloads, and re-run the existing row-boundary negative controls. This checkpoint
does not authorize persistence, live-queue ownership, a second queue consumer,
or a nonzero sample.

Migration 29 already supplies the deployed `SECURITY DEFINER` cursor and
scratch-queue mutation boundary, so this checkpoint is not waiting on direct
`queue_jobs` privileges and can deploy with sampling fixed at zero. A nonzero
production sample remains proof-gated: the runtime role and function ownership
must be catalog-verified, direct live-queue mutation must remain denied, and the
negative controls above must pass in-cluster. Deleted-input parity also remains
unmeasurable without pre-delete hydration or a durable write-set ledger. A
nonzero soak additionally requires proof that Go's effective merged classifier
configuration matches Rust's embedded core source and defaults. The
authoritative value is the structured Go log emitted when the live processor
initializes the exact cached source used by its classifier runner:

```text
msg="classifier runner initialized"
effective_config_digest="sha256:..."
default_workflow="..."
```

Do not substitute a digest recomputed by a separate process: the live runner
caches its source, while mounted configuration files can change afterward.
`bitmagnet classifier digest` is only a deployment preflight/cross-check. When
using that command, run it inside the Go processor container, or in a one-shot
container with the same image, environment, config mounts, working directory,
and `XDG_CONFIG_HOME`. Go resolves embedded core YAML, XDG and
working-directory `classifier.yml` overlays, application config
flags/keywords/extensions, TMDB enablement, and the configured default workflow
before hashing. Pass the runtime-loaded log value unchanged to the Rust
consumer as `BITMAGNET_INGEST_SHADOW_EXPECTED_CLASSIFIER_CONFIG_DIGEST` (or
`--expected-classifier-config-digest`). The consume command refuses to start
when it is missing or differs from Rust's embedded effective configuration.

The digest wire contract is version 1. Hash the UTF-8 bytes of one compact JSON
object (no insignificant whitespace and no HTML escaping) with SHA-256, then
emit `sha256:` plus lowercase hexadecimal. U+2028 and U+2029 are encoded as the
six ASCII bytes `\u2028` and `\u2029`; floating-point values are outside v1 and
must fail digest construction. Integer values use their shortest decimal form.
Keys within configuration maps are lexicographically sorted and array order is
preserved. The document's fixed field order and shape are:

```json
{"version":1,"default_workflow":"...","source":{"workflows":{},"flag_definitions":{},"flags":{},"keywords":{},"extensions":{}}}
```

The five `source` values are the fully merged/defaulted Go values. `$schema` is
excluded because it does not affect behavior; classifier concurrency is also
excluded because it affects scheduling rather than classification results.
Changing the selected default workflow or any workflow, flag definition,
effective flag value, keyword, or extension changes the digest.

This gate does not authorize future classifier-core or overlay support by
itself. In particular, Go compiles `content_type_list: [unknown]` as enum value
zero while the current Rust classifier drops `unknown` from that list. Any
future core/default or Rust overlay work must align that behavior and extend
the cross-language parity corpus before its digest can be accepted in a soak.

## Milestone 6: persisting `process_torrent` worker

`ProcessTorrentWorker` and the non-deployed `bitmagnet-process-torrent-worker`
binary are the first runtime that calls `persist_writer_plan`. Each iteration
claims one job through `QueueStore::consume_admitted_process_torrent`, composes
the plan with `load_writer_plan`, persists it (retry publication, blocker,
transaction kernel), and settles the row while still holding the claim lock.
Go's handler defaults are kept: concurrency one, a ten-minute job timeout, a
30-second idle poll, and self-chaining after any claim. Stored errors use Go's
text: `job failed to process: <error>` for handler failures, `job exceeded its
<Go duration> timeout: context deadline exceeded` for timeouts, and the bare
deadline error for expired jobs.

Within a job, the torrents are classified on a dedicated pool of
`--classify-parallelism` threads (default 4,
`BITMAGNET_PROCESS_TORRENT_WORKER_CLASSIFY_PARALLELISM`; 1 is the serial
path), as Go runs one goroutine per torrent. Results are folded into the
write-set in requested order, so the plan, including which error wins and the
classifier-failure (retry fingerprint) order, is identical for any
parallelism. The default is explicit because `available_parallelism` sees the
node's cores, not the pod's CPU request; size the pod's request to match it,
since the worker shares HEL1 with PostgreSQL.

Only explicitly supported payloads are ever locked: `ClassifierWorkflow` must
be `default`, the three attach flags must be JSON `false`, and the payload may
have no top-level key other than `ClassifyMode`, `ClassifierWorkflow`,
`ClassifierFlags`, and `InfoHashes` (Go's decoder also binds case-insensitive
key matches such as `classifierWorkflow`, which serde ignores). The embedded core
configuration defaults all three flags to `true`, so an omitted flag can never
resolve to the supported value; the organic DHT payload (`InfoHashes` only) is
therefore left to Go, unless Milestone 9's `--admit-organic` is on. A claimed job that the kernel still cannot
reproduce (a sourced or enriched explicit hint, an input over the loader's
bounds, any composition invariant) is abstained before any side effect: the
claim transaction rolls back, the row keeps its status, retries, `run_after`,
and error, and the worker excludes that ID from its own claims for a cool-off
period. A payload serde cannot decode is abstained as well: Go's decoder
accepts inputs serde rejects (one `0x` hash prefix, JSON `null` for
`InfoHashes`), so only Go can settle it with Go's outcome. A database error
while loading, a timeout, and every error once persistence has started settle
through Go's retry lifecycle; every other loader or composer error abstains. A request of
more than 100 info hashes (the loader bound, equal to Go's DHT batch and the
default reprocess batch size) is therefore always left to Go.

The binary refuses to start unless `--expected-classifier-config-digest`
matches the embedded configuration and the Goose head is at least 35. The
loader, writer, and generic queue SQL need only head 26; 35 adds
`queue_jobs_admitted_process_torrent_claim_idx`, a partial index whose
predicate is the claim's admission predicate, so a claim reads only admitted
rows. Without it the claim steps over every organic row ahead of the first
admitted one in Go's order, and reads the whole organic backlog on every poll
when none is admitted. It needs no `SECURITY DEFINER` capability from
migrations 29–34: the claim and settlement are the generic `queue_jobs`
statements, and the writer needs the same table rights as the Go processor. It flushes the shared blocking manager
on shutdown, as Go does when the application stops.

A database error while claiming or settling (an unreachable server, an
exhausted pool) never stops the worker. As in Go's `serverHandler`, it is
logged as `error handling process_torrent job`, the claim transaction rolls
back, and the worker polls again after one check interval. Each such attempt
increments `bitmagnet_process_torrent_worker_jobs_total{outcome="error"}`.

Shutdown is bounded. On `SIGTERM` the worker claims nothing new and gives an
in-flight job `--shutdown-grace-seconds` (default 20) to settle. After that the
job is dropped and its claim transaction rolls back, so the row keeps its
status, retries, and error and is claimed again later. Go behaves the same way:
stopping the queue server cancels the handler's context, the settle update
fails, and the claim rolls back. Side effects the writer already committed stay
in place, and the next run of the job redoes them. The 20-second default plus
the blocking-manager flush fits Kubernetes' default 30-second
`terminationGracePeriodSeconds`. Without the bound, a job could run for up to
its ten-minute timeout and be `SIGKILL`ed mid-write, without the flush.

Migration-backed gates (`tests/worker_pg.rs`) prove: a processed job leaves
exactly the live image the direct `load_writer_plan` + `persist_writer_plan`
path leaves; non-admitted payloads are never touched and Go's claim still
takes them in its order; admitted claim order is Go's; the claim's custom
and generic plans scan the admitted-only partial index behind a 20,000-row
organic backlog; Go's retry count,
`calculate_backoff` envelope, terminal `failed` status, and error text; a
partial failure publishes Go's replacement job before persisting; abstention
leaves the row byte-identical and unlocked; an abstaining job at the head of
the queue neither blocks the admitted job behind it nor causes a re-claim
loop, and its exclusion expires and is capped; and two Rust workers plus Go's
exact claim never double-claim; and on shutdown an in-flight job settles if it
finishes within the grace, or else is abandoned with its row byte-identical
and claimable again; and database errors are counted while the worker keeps
polling, then processes the job once the database recovers. `tools/parity/process_torrent_worker/run.sh`
drains the same Go-built jobs with Go alone (A), with Rust then Go for its
abstentions (B), and with both concurrently (C), and diffs the resulting
images. At Goose 29 over the 330 synthetic classifier cases plus all 119,991
real-name replay inputs (120,321 torrents, 1,224 seeded jobs including file
lists, explicit hints, deletion-only and mixed deletions, partial failures,
and all-missing jobs), B and C are byte-identical to A across all 241,982 dump
lines: 120,300 `torrent_contents`, 120,310 surviving torrents, 11
`deleted_torrents`, and all 1,346 `queue_jobs` (status, retries, error text,
payload, and fingerprint, including the 122 Go-shaped partial-failure
replacements). In B the Rust worker settled every admitted job except the four
with sourced explicit hints, which it abstained from and Go then processed.
`run_after`, `ran_at`, write timestamps, generated IDs, and bloom-filter bytes
are excluded (Go's backoff jitter and filter decrement start are random).
The first run measured B's Rust phase at 1,209 s (about 100 torrents per
second) against 91 s for A. Profiling traced that to the classifier, not the
database: cel-rust's `matches` recompiled the multi-kilobyte `keywords.*`
regexes on every call, every CEL binding was deep-copied for every expression,
and, once those were fixed, the admitted claim's per-flag payload comparisons
pushed the planner off `queue_jobs_claim_idx` into a sequential scan and sort
of every pending payload per claim. With those fixed the same harness gives
B's Rust phase 42 s against A's 100 s in round 1 and 42 s against 42 s in
round 2 (reprocessing), and the settled-job span over the 120k corpus is
25-31 s for both workers in both rounds (20-26 ms per 100-torrent job), with
B and C still byte-identical in both rounds. What remained of classification
CPU time was cel-rust value conversion (the torrent once per run, `result` once
per expression). Spreading it over four threads cut classification of a
100-torrent job from 9.9 ms to 2.8 ms on a 4-core workspace (20,000 replay
torrents, identical output at 1-4 threads); the loader and persistence stay
serial. In the 120k differential at Goose 35 on the same workspace, the Rust
worker's settled-job span went from 21-24 ms per job at one thread to 14-15 ms
at four (two runs of each, both rounds; one four-thread round of 44 ms/job was
a noisy outlier that its repeat did not show), and B and C stayed
byte-identical to A in every run (241,982 lines in round 1, 243,179 in round
2). Go's A phase took 90-100 s in round 1 against 33-34 s for B's Rust phase.
The `result` conversion was then 45% of serial classification CPU: converting
it twice per expression took the 119,991 replay inputs from 10.0 to 14.5 ms per
100 torrents. A condition now converts only the `result` fields it selects,
one that never names `result` binds nothing, and only a use of the whole object
still gets the full map, so serial classification takes 5.1 ms per 100
torrents, with byte-identical output. In the 120k differential, three runs of
each interleaved with the previous build, the settled-job span at one thread
went from 23.5-24.2 to 16.9-19.2 ms per job in round 1 (23.4-31.0 to 18.7-29.0
in the noisier round 2); at four threads, where classification was already a
small share of a job, the change is within noise (14.8-18.3 against 15.4-16.8
in round 1, leaving out a uniform 46-48 ms/job slowdown that hit both builds'
third runs back to back, and 15.4-17.3 against 14.8-16.4 in round 2). B and C
were byte-identical to A in every run.

The differential found three persistence-kernel divergences that the
canonicalizing comparators had hidden: empty `episodes` were stored as `{}`
where Go stores SQL `NULL` (every such Rust row, 1,981 in the first run); `languages`
were stored in code order where Go stores `Languages.Slice()` (natsort by
name) order; and, once the first was fixed, the one Go case that does store
`{}` — an episodes regex match that allocates the map but adds nothing, such
as the backwards range `S01E79 - 9` — was stored as `NULL` (1 of 20,310).
`bitmagnet_release::Episodes` now tracks Go's nil-versus-allocated bit
(equality still ignores it) and the writer plan carries it to persistence.
The canonical comparison image is unchanged.

Known differences from Go, all outside the admitted subset's write-set:
no post-commit Tantivy dual-write (the search follower keys on
`torrents.updated_at`, which the processor does not touch, so a Rust-written
reclassification reaches the sidecar only at the next backfill or torrent
update); a timeout cancels the Rust handler, while
Go's handler goroutine keeps running after its job is settled;
database-driver and panic error texts differ; a job with several classifier
failures may publish its retry hashes in a different (valid) order, and
therefore a different fingerprint, than a given Go run.

## Milestone 7: flags-on admission (`--admit-flags-on`)

`--admit-flags-on` (`BITMAGNET_PROCESS_TORRENT_WORKER_ADMIT_FLAGS_ON`, default
off) lets the worker also settle explicit flags-on jobs. Without it the worker
is Milestone 6 unchanged: the same claim statement and 00035 index, the
`NullContentResolver`, no TMDB client, Goose head 35, and the composition on
the runtime thread. `--tmdb-api-key` is refused without the switch, so a
deployment cannot believe it enabled TMDB when it did not. The start-up line
logs the admission (`FlagsOff` or `FlagsOffOrOn`) in both modes, and the TMDB
base URL when the switch is on.

With the switch the claim is
`ProcessTorrentAdmission::FlagsOffOrOn`: Milestone 6's predicate, with a
second accepted containment in which all three attach flags are JSON `true`.
The workflow must still be explicitly `default`, and the key allow-list is
unchanged. Mixed, omitted, non-boolean, or inherited flags are never claimed,
so organic payloads stay with Go. Both subsets come out of one claim in Go's
order. Migration 00036's `queue_jobs_admitted_flags_on_process_torrent_claim_idx`
repeats that predicate verbatim, so the binary requires Goose head 36 with the
switch. The composer re-checks the same subset (`validate_supported_params`):
a live materializer accepts uniform explicit flags, and the flags-off
materializer still accepts only explicit `false`.

The classifier gets Go's resolver stack, `LiveContentResolver`:
- **Local search:** `PgContentSearch::with_associations` on the worker's pool.
- **TMDB:** `TmdbClient` with `--tmdb-api-key` (`BITMAGNET_TMDB_API_KEY`; required, and redacted in `Debug`) and `--tmdb-base-url` (`BITMAGNET_TMDB_BASE_URL`, default `https://api.themoviedb.org/3`).

The client is the existing Rust port:
- It validates the key lazily on first use and latches a 401.
- It allows two requests in flight and 20 requests per second (burst 5).
- It retries transport errors only.

The pool must allow three connections: the claimed row, loading and persisting,
and local search. Only two are in use within a job. The third covers a
composition abandoned at its timeout: it keeps running read-only on the blocking
pool, and its local search can hold a connection while the next job claims and
loads.

Local-search candidates are hydrated, as Go's are. Go's search preloads each
candidate's attributes and collections, and `UpdateTsv` appends genre
collection names and `id` attribute values at weight D in slice order. That
order therefore decides the lexeme positions of the `torrent_contents.tsv` the
processor writes. `with_associations` reads the candidates and their
associations in one `REPEATABLE READ READ ONLY` transaction. It uses the
writer loader's statements, now shared from `bitmagnet_content_search`:
attributes by `(source, key)` and collections by the collection's
`(type, source, id)`, both compared under the database collation, within the
loader's 1,024-per-content ceilings. Go's loader has no `ORDER BY`; the
harness's Go side is given this same order (M1a). `PgContentSearch::new`
stays scalar-only for its other callers.

The classifier engine is synchronous and drives the resolver's futures to
completion itself. With the switch:
- The composition runs on tokio's blocking pool (`load_live_writer_plan`).
- Classification still spreads over `--classify-parallelism` threads.
- Each classification's resolver futures run through `Handle::block_on` on the worker's runtime.

`Handle::block_on` panics on a runtime thread, so `load_writer_plan` refuses a
live materializer (`LiveCompositionOnRuntimeThread`) instead. Results still
fold in requested order, so the plan is identical for any parallelism.

Resolver failures whose Go text Rust cannot rebuild abstain the whole job
before any side effect. (Milestone 8 hands every other answered TMDB status to
the torrent that made the call, as Go does.) Each classification's resolver
calls run in a per-job scope. The first abstaining `ResolveError` is recorded
with its info hash and call. That error is a local search error, a TMDB
transport error, a 401, a status whose reason phrase cannot be rebuilt, an
undecodable body, or a disabled client. Recording it stops the scope, so every
resolver call in the job that starts later fails without I/O. The
composition then returns `MaterializeError::ResolverFailed` ahead of any other
result. The worker logs `abstaining from process_torrent job after a content
resolver failure` (job ID, info hash, call, error) and abstains: the claim
rolls back, the row is untouched, and the ID cools off. A TMDB details 404
is not an error: as in Go, the torrent stays unmatched. The engine's own error
texts are unchanged. Dropping the job future (timeout or shutdown) also stops
the scope, so an abandoned composition makes no further TMDB or search calls.

Gates:
- **Unit** (`src/live.rs`, `src/writer_plan.rs`, the binary):
  - Every abstaining fault (search 401, key-validation 401, disabled client, timeout, undecodable body, unrenderable reason phrase, local search error) abstains the job, both serial and at four threads. Milestone 8 lists the faults that fail only the torrent.
  - A details 404 succeeds as unmatched.
  - One abstaining torrent abstains the job, whatever else failed.
  - A call outside a classification, or after the first failure, never reaches I/O.
  - Payload acceptance covers partly-true, omitted, string, numeric, and inherited flags, empty and custom workflows, and extra flags, in both modes.
  - The key is refused without the switch, required with it, and never printed.
- **Migration-backed** (`tests/worker_pg.rs`):
  - Hydration keeps the order of the database's own `ORDER BY` under its default collation, and the resulting tsv positions follow it.
  - On an en_US-like (glibc) collation the gate also proves that order is not a bytewise re-sort (`a7` < `B8`, `éxtra` < `f-key` < `id`, `imdb` < `TVDB`). CI's musl `postgres:16-alpine` collates bytewise, so there the test reports that this half did not run.
  - The flags-on claim takes both explicit subsets in Go's order, and nothing else.
  - Its custom and generic plans scan 00036's index behind a 20,000-row organic backlog.
  - A flags-on job attaches seeded local content with its ordered associations.
  - An abstaining resolver failure abstains with zero writes and the row byte-identical.
  - A composition abandoned at its job timeout makes no further resolver calls once it is unblocked.
  - A flags-on job with a sourced hint abstains with the attach-hint reason, with zero writes and zero resolver calls, because the hint check precedes classification.
  - A flags-off job processed by the switched worker leaves the same image as without the switch.
- **Regression, switch off:**
  - The Milestone 6 120k differential at Goose 35 stays byte-identical: B and C equal A in both rounds (241,982 and 243,179 lines), and every Rust worker logged `admission=FlagsOff`.
  - The 119,991-input classifier replay, the 2,000-subject flags-on desync gate, and the cross-language tape rerun are unchanged.
- **Differential:** M1a's harness (stub TMDB, seeded content) compares Go-alone with Rust-alone dumps that include the content tables.

Known differences from Go, beyond Milestone 6's:
- **Resolver failure scope.** An abstaining resolver failure (see above) abstains the whole job. Go fails only the affected torrents, republishes them, and persists the rest. The Rust worker therefore needs Go's worker beside it to settle such jobs. While the failure persists (a revoked key, a TMDB outage that times out), every flags-on job is left to Go. Milestone 8 narrowed this class.
- **401 fallback.** Go falls back to its default TMDB key after a 401 at key validation; Rust latches the 401 and abstains. A 401 on a later call is per torrent in Go (`TMDB request failed: 401 Unauthorized`, then latched); Rust abstains there too, and latches.
- **Enriched hints (M3).** Sourced and enriched explicit hints still abstain, before any resolver call, as do attached-content images that conflict within a job.
- **Tantivy.** There is still no Tantivy dual-write, so content attached by Rust reaches the search sidecar only at the next backfill or torrent update.

## Milestone 8: per-torrent TMDB status failures

With `--admit-flags-on`, a TMDB request answered with its own non-2xx status
other than 401 now fails only the torrent whose classification made it, as in
Go.
The other torrents keep resolving, and the job settles through Milestone 6's
classifier-failure path. Under Milestone 7 such a status abstained the whole
job.

Rust rebuilds Go's whole chain byte for byte:
- **Requester** (`internal/tmdb/requester.go`): a non-2xx becomes `TMDB request failed: <res.Status()>`. 401 and 404 are sentinels with fixed text (`401 Unauthorized`, `404 Not Found`), whatever phrase the server sent. A details 404 is still unmatched; a search or find 404 is an error.
- **Classifier:** search and find errors pass through unwrapped. `find_match` wraps them as `runtime error at Path <path>: <cause>`, which stops that torrent's workflow.
- **Processor** (`processor.go`): the errors are `missing N info hashes` (when hashes are missing), then each failed torrent's error.
  - When no torrent succeeded, the job fails with the errors joined by newlines, behind the handler's `job failed to process: ` prefix, and retries with Go's backoff up to its max retries.
  - Otherwise Go publishes a retry job before persisting the rest. The retry job holds the missing hashes, then the failed torrents, and is inserted `ON CONFLICT DO NOTHING`.

For example:
`job failed to process: missing 1 info hashes\nruntime error at Path workflows.default.[5].if_else.if_action.find_match: TMDB request failed: 503 Slow Down`.

**Status lines.** On HTTP/1.x, Go's `res.Status()` is the status line as the
server sent it. hyper keeps a reason phrase (`hyper::ext::ReasonPhrase`) only
when it differs from the canonical one. `HttpResponse::reason_phrase` carries
that phrase, so `TmdbError::Http`'s `status_line` is exact: `599 status code
599` from a Go server, `503 Slow Down`. hyper reports three different status
lines as the same empty phrase:
- a missing phrase (`HTTP/1.1 500`), which Go renders `500`;
- an empty one (`HTTP/1.1 500 `), which Go renders `500 `;
- one with obs-text bytes, which Go renders as sent.

An empty or non-UTF-8 phrase is therefore `TmdbError::UnrenderableStatus`, and
abstains.

**Mechanism:**
- `From<TmdbError>` maps 401, 404 and `Http` to `ResolveError::TmdbStatus { status, status_line }`, which renders exactly like `ResolveError::Tmdb`.
- `TmdbClient`'s seam calls hand on a latched key-validation failure as `ResolveError::Tmdb` instead (see below).
- `live::abstains` is false only for a `TmdbStatus` other than 401.
- `AbstainingResolver` records only abstaining errors, and only they stop the scope. A per-torrent error reaches the engine unchanged.

**Log events.** The parity harness attributes TMDB requests to the failure
that stopped a job from these lines, all carrying `info_hash` and `call`:
- `content resolver call started` (debug): a call that goes ahead, logged before its request;
- `content resolver failure recorded; later calls are refused` (info): an abstaining failure, with `error`;
- `content resolver call refused` (debug): a call made after the scope stopped, which makes no request.

The decision to admit or refuse a call, and its log line, share the scope's
lock with recording a failure. So at several threads no call is logged as
started after the failure that stopped its scope, although calls that started
earlier may still be in flight. A refused call is still kept as the failure
when none was recorded (a scope the worker stopped at a timeout or shutdown),
as in Milestone 7.

**Retries.** resty v2.17.2, with `SetRetryCount(3)` and no retry conditions,
retries only when `err != nil`: a transport error. That is four attempts with
2–20 s of jittered backoff, each with a 10 s timeout. An answered status is
one request. The Rust client's `execute_with_retry` matches, so a status
failure is never retried within a job.

**Key validation.** Both sides validate the key lazily, once per process, and
latch the outcome. Go returns a failed validation's error for every later call
of the process. Its text could be rebuilt, but the job abstains on it whatever
its status: otherwise a transient 500 at start-up would fail every torrent,
and burn every flags-on job's retries, until a restart. A latched 404 abstains
too; it is not treated as unmatched. Once the initialiser has failed, no call
reaches the network, so the seam knows that an error seen under a failed latch
is the latched one.

**Retry workflow.** `compose_retry_job` rebuilds the message from Go's four
fields, with the workflow Go resolved (`resolved_workflow`: the job's
workflow, or `default` when it is empty), as Go's
`NewQueueJob(MessageParams{...})` does. That closes Milestone 7's "Retry
workflow" difference.

The job still abstains, untouched, for:
- a 401;
- a failed key validation, whatever its status;
- a transport error or timeout, whose Go text embeds the URL and so the API key;
- an unrenderable status;
- an undecodable body;
- a disabled client;
- a local search error;
- before any resolver call: an undecodable or unsupported payload, or a sourced hint.

Failed torrents appear in the job's input order, both in the retry job and in
the joined error. Go appends them under a mutex in goroutine completion order,
so that order is not deterministic in Go and is not emulated.

Gates:
- **Unit** (`bitmagnet-tmdb`):
  - The server's phrase survives: `599 status code 599`, Go's `413 Request Entity Too Large`, `503 Slow Down`.
  - The sentinels keep their fixed text.
  - A non-UTF-8 or empty phrase is unrenderable.
  - Answered statuses keep their code through the seam.
  - Over a real socket, the live transport keeps a non-canonical phrase, reports a canonical one as `None`, and reports `500`, `500 ` and an obs-text phrase as empty.
  - The client renders a 599's own phrase, and a 404's sentinel whatever the server's phrase.
  - A failed key validation crosses the seam as `ResolveError::Tmdb`: a 500, with one request across two calls, and a 404 on a details call. A validated client's same statuses stay answered ones.
- **Unit** (`src/live.rs`, serial and at four threads):
  - These fail only the torrent, with Go's text: 5xx, 429, a search or find 404, a non-canonical phrase.
  - These abstain: 401s, a key validation answered 500, or 404 on a details call, a disabled client, a transport error, unrenderable phrases, an undecodable body, a local search error.
  - A details 404 and an empty search resolve.
  - One failing torrent fails alone while another resolves.
  - An abstaining error abstains the job whatever else failed.
  - The first recorded failure is the one kept.
  - The guard logs a started call, the recorded failure, and a refused call, in that order, with their fields.
- **Unit** (`src/writer_plan.rs`): the retry job names the resolved workflow.
- **Migration-backed** (`tests/worker_pg.rs`; `LiveContentResolver` over a scripted TMDB transport):
  - Partial failure, at one and at four threads: the failing torrent goes to the retry job, while seeded local content and TMDB-resolved content persist. Each request is counted.
  - Missing hashes lead the retry job, ahead of a TMDB failure.
  - All failed: the job stores Go's error text and fails at its max retries, with one search per run, no new job and no writes.
  - A 401 (one attempt), a transport failure (four attempts) and a key validation answered 500 (no search at all) abstain with zero writes, the row unchanged, and no call after the failing one.
- **Mutations:** 19, all killed. They are:
  - every error abstains;
  - the guard records per-torrent errors;
  - a 401 per torrent;
  - a transport error per torrent;
  - the status code is lost at the seam;
  - the client ignores the phrase;
  - the transport drops the phrase;
  - a non-UTF-8 phrase renders lossily;
  - an empty phrase renders with a trailing space;
  - the retry job clones the named workflow;
  - `find_match` moves on after any error;
  - missing hashes follow the classifier failures;
  - a stopped scope still calls the resolver;
  - a latched validation crosses as an answered status;
  - each of the three log events is dropped (three mutations);
  - a later failure overwrites the first;
  - a refusal is not kept as the failure.

Known differences from Go, beyond Milestone 7's:
- **HTTP/2 status text.** The Rust transport speaks HTTP/1.1 only, because reqwest is built without `http2`. Go negotiates HTTP/2 with TLS servers such as api.themoviedb.org (resty sets `ForceAttemptHTTP2`), and on HTTP/2 `res.Status()` is the code plus Go's `http.StatusText`, not the server's phrase.
  - For the common codes (500, 502, 503, 504, 429) the two texts agree.
  - They differ when a server's HTTP/1.1 phrase differs from Go's table (413 or 422 on a current server), and when a code has no table entry (Go writes `599 `).
  - The harness's plain-HTTP fake TMDB runs HTTP/1.1 on both sides, so it does not exercise this.
- **Failure order.** With two or more failed torrents in one job, Go's order is goroutine completion order and Rust's is input order (see above).
- **Calls in flight.** With classification parallelism above one, calls already in flight when an abstaining error is recorded still complete; only calls that start afterwards are refused.

## Milestone 9: organic admission (`--admit-organic`)

`--admit-organic` (`BITMAGNET_PROCESS_TORRENT_WORKER_ADMIT_ORGANIC`, default
off) lets a flags-on worker claim organic payloads: the shapes Go's producers
write, whose omitted attach flags the classifier resolves to the core
defaults. `--organic-max-priority`
(`BITMAGNET_PROCESS_TORRENT_WORKER_ORGANIC_MAX_PRIORITY`, default 0) is the
highest job priority it claims.
- `--admit-organic` refuses to start without `--admit-flags-on`, so it inherits Milestone 7's resolver stack and Goose head 36. It adds no migration.
- A negative bound is refused, and so is a bound without `--admit-organic`.
- The start-up line logs `admission=Organic { max_priority: N }`.

**Claim.** `ProcessTorrentAdmission::Organic { max_priority }` runs
`ORGANIC_PROCESS_TORRENT_CLAIM_SQL`: Milestone 6's select list, `ORDER BY` and
locking, with `priority <= $2` and `ORGANIC_PAYLOAD_PREDICATE` in place of the
containment. The predicate admits `MessageParams` exactly as Go's producers
write it:
- only the four exact key names; Go's case-insensitive aliases are still rejected;
- `ClassifierWorkflow` absent, `""` or `"default"`;
- `ClassifyMode` absent, `0` or `1`, compared as jsonb text: numeric jsonb equality would also admit `1.0`, which neither Go's `int` nor serde's `i32` decodes;
- `InfoHashes` an array of 1 to 100 (the loader's bound) lowercase 40-digit hex strings, a strict subset of what both decoders accept;
- `ClassifierFlags` absent, or an object holding only the three attach flags, each a JSON boolean, so `delete_xxx` and `delete_content_types` are rejected;
- no JSON `null` anywhere. No producer writes one (`message.go` uses `omitempty`); Go would read it as absent, so such a job is left to Go.

The predicate is a `CASE` whose `WHEN`s run in order, so `jsonb -` (an error
on a scalar) and the array functions only see the types an earlier `WHEN`
established; PostgreSQL may reorder `AND`ed quals. A payload outside it is
never claimed. `validate_supported_params(params, Organic)` re-checks the
decoded params as a backstop: a workflow that resolves to `default`, and no
flag but the attach flags, as booleans.

**Flags.** Each attach flag is the payload's value, else the core default,
`true`, and an empty or absent workflow is `default`. That is the classifier's
existing per-flag merge, as in Go's `runner.go`, so there is no resolution
code: a DHT `{"InfoHashes":[…]}` job classifies exactly as the explicit
flags-on job does.

**Index.** No partial index can carry the predicate (PostgreSQL allows no
subquery in an index predicate), and none is needed. The claim walks Go's
`queue_jobs_claim_idx` on `(queue, (status = 'retry'), priority, run_after)`,
whose order is the claim's `ORDER BY`, with `priority <= $2` in its Index
Cond. Rows above the bound are stepped over on the index tuples without a
heap fetch; the predicate runs only on rows within the bound.
`EXPLAIN (ANALYZE, BUFFERS)` at bound 0 on musl PostgreSQL 16.15, prod's
major version:
- **(a)** No row within the bound, behind 100,000 at priority 10: 605 shared buffers, 3.2 ms (custom plan) and 4.6 ms (generic). PostgreSQL 16 has no skip scan, so this reads every higher-band index tuple, about 6 buffers per 1,000 rows: roughly 2,700 buffers per idle poll, 30 s apart, behind a whole-database batch backlog of 450,000.
- **(b)** As (a), plus 1,024 rows within the bound, all excluded: 649 buffers, 4.6 ms (custom) and 12.2 ms (generic, which compares each row with the exclusion list linearly).
- PostgreSQL 18.6's skip scan reads 9 and 50 buffers (two index searches).

**Bound.** Go's producers write these priorities:
- the DHT crawler (`persist.go`): 0;
- every processor retry (`processor.go`'s `NewQueueJob`): 0;
- batch children (`planner.go`): 10, or 4 with `apis_enabled` false;
- the importer (`importer.go`): 20.

Bound 0 therefore takes the DHT and retry jobs, and steps over the importer
and batch jobs, where the hinted torrents are. Raise it only after M3b, once
the `hint` count below is clean. Organic admission is not a superset of
`FlagsOffOrOn`: an explicit flags-on payload above the bound stays with Go.

**Retries.** A retry job is rebuilt from Go's four fields (Milestone 8): the
resolved workflow `default`, the job's `ClassifyMode`, and its flags as given
(omitted when absent or empty), at the default priority 0. It is therefore an
organic job within any bound.

**Abstention reasons.** `bitmagnet_process_torrent_worker_jobs_total` has a
`reason` label. It is empty except on `outcome="abstained"`, where it is one
of:
- `payload`: undecodable, or a workflow or flags outside the admission;
- `hint`: a sourced or enriched explicit hint (M3);
- `resolver`: an abstaining resolver failure;
- `conflict`: `WriterLoadError::ConflictingReusableContent`;
- `unsupported`: any other loader or composer limit.

Under organic admission `payload` should stay at 0, because SQL rejects those
shapes before any claim. A non-zero count means the SQL predicate and the
validator disagree.

**Operations.** The TMDB client latches its key validation and a 401 for the
life of the process. After rotating the TMDB key, restart the Rust worker:
until then every job that reaches TMDB abstains and is left to Go.

Gates:
- **Unit:**
  - (`bitmagnet-queue`) The flags-off and flags-on claims are pinned byte for byte. The organic claim is the flags-off claim's select list, `ORDER BY` and locking around the bound and predicate. Each admission maps to its claim.
  - (`src/writer_plan.rs`) Organic params accept an empty or `default` workflow and any boolean subset of the attach flags. They refuse other workflows; `delete_xxx`, `delete_content_types`, unknown and case-variant flags; and string, numeric and null flag values.
  - (`src/worker.rs`) Each abstention maps to its reason label.
  - (the binary) `--admit-organic` requires `--admit-flags-on`. A negative bound, and a bound without the switch, are refused. The defaults are off and 0.
- **Migration-backed** (`tests/worker_pg.rs`, on the workspace's PostgreSQL 18.6 and on musl 16.15):
  - Each producer shape, at its priority, is claimed in Go's order exactly within bounds 0, 3, 4, 9, 10, 19 and 20; the Go-only shapes never are. The flags-on claim still takes only the explicit subsets, the probes leave every row unchanged, and the worker's admission follows its configuration only with a live materializer.
  - The predicate, evaluated over the stored column, agrees with an independent Rust reference on 181 payloads, 33 of them admitted. They cover every producer shape; each field missing, null, retyped, or under a case-variant name; `ClassifyMode` as `-0`, `1e0`, `1.0` and others; 0, 1, 2, 99, 100, 101 and 250 hashes; 39- and 41-character, `0x`, upper-case, newline-terminated and unicode-escaped hashes; each flag true, false, null, 0, 1, `"true"`, an array or an object; `delete_xxx`, `delete_content_types` and unknown flags; and duplicate keys. Every admitted payload decodes and passes the validator, and the claim takes exactly the admitted rows.
  - The plan: an Index Scan on `queue_jobs_claim_idx` with the bound in its Index Cond, no sort, and neither 00035's nor 00036's index, as custom and generic plans, for (a), (b), and 20,000 DHT rows within the bound.
  - An organic DHT job leaves the same image, and makes the same TMDB requests, as the explicit flags-on job.
  - Explicit organic flags override the defaults: `tmdb_enabled: false` makes no TMDB request while local search still attaches, and `local_search_enabled: false` sends the local movie to TMDB.
  - The retry jobs of a DHT job, of an explicit empty workflow with empty flags, and of a rematch batch child at priority 10 carry `default`, the mode and the flags, at priority 0, and bound 0 claims them.
  - A hinted organic job abstains with reason `hint` (`payload` stays 0), stays excluded while the job behind it is processed, and Go then claims it untouched.
  - A type-only hint with `languages = '[]'`, as every prod hint has, and SQL NULL episodes classifies as one with NULL `languages` does, and the hint is used. A non-empty `languages` abstains.
- **Mutations:** 21, all killed. They are:
  - an absent attach flag resolves to `false`;
  - the core defaults override the payload's attach flags;
  - the SQL admits a custom workflow, extra or case-variant keys, a non-boolean flag, `delete_xxx`, more than 100 hashes, or any hash element (six mutations);
  - the bound is exclusive, or dropped;
  - the validator skips the flag key set;
  - organic admission starts without flags-on, or a non-live worker claims organic payloads;
  - a retry keeps the named workflow, or loses `ClassifyMode`;
  - the flags-off or the flags-on claim drifts by a byte;
  - the worker ignores its exclusions under organic admission, or the claim drops them;
  - the `CASE` flattened to `AND NOT`s, or to `IS NOT TRUE`s. The second keeps every verdict, because PostgreSQL happened to run each type guard first, so the agreement test passes. Only the plan test catches it: the `AND`ed selectivities move the generic plan to a bitmap scan and a sort.

Known differences from Go, beyond Milestones 7 and 8:
- **Nulls and numbers.** A payload with a JSON `null` field, or a `ClassifyMode` other than the integers 0 and 1, is left to Go.
- **Hinted organic jobs (M3).** A DHT or retry job whose torrent has a sourced or enriched hint abstains, as in Milestone 7; the `hint` count measures how often.
