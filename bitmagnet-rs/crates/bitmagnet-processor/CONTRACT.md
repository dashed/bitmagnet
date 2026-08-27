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
- attached-content persistence is rejected because the current writer image
  does not carry the base content TSV and association rows; and
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

This is not yet the complete runtime persistence milestone:

- attached `content` remains rejected until the writer projection carries the
  base content TSV and the persistence kernel owns its association rows;
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
which have no explicit `torrent_hints` row, and which have no reusable
source-backed `torrent_contents` association. Unsupported inputs settle only
their scratch job and are counted by a closed reason vocabulary; transient
database/runtime errors retain queue retry behavior. Rematch preserves explicit
Go hint semantics internally while disabling only content-association reuse,
although all explicit hints remain excluded from this milestone. The consumer
revalidates the envelope against the exact retained `process_torrent` row and
rejects a missing/changed source, any other overlapping attempted run at or
after the captured timestamp, any overlapping nonterminal `pending`/`retry` job
regardless of its `run_after`, or a newly ineligible live input. The nonterminal
guard closes the interval where Go is executing a handler but its row has not
yet committed a terminal status or `ran_at`. The runtime also rejects post-source
`updated_at` values in `torrents`, `torrents_torrent_sources`,
`torrent_contents`, and `torrent_tags`; hint presence is already a categorical
rejection. Source validation, bounded writer loading, `WriterPlan` composition
inputs, stable live comparison, and volatile writer comparison share one
read-only repeatable-read snapshot, preventing a concurrent Go transaction from
creating a torn or cross-run image. The returned causal result binds both
evidence planes to the envelope's exact source job ID and `ran_at`. This is
causal live-state evidence, not historical event replay: row deletion has no
tombstone.

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
