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
- attached-content enrichment is rejected because Lane C's normalized JSON
  exposes only `contentAttached`, not the `Content` row; and
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

This is not yet the complete runtime persistence milestone:

- attached `content` remains rejected until Lane C exposes the structured
  content row and its associations;
- the runtime loader still needs to carry the volatile persistence metadata and
  construct the weighted FTS vector;
- the concrete stable-bloom blocking manager is not yet ported; and
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
live tables and direct SELECT/INSERT/UPDATE on `queue_jobs`. It proves the
runtime can read the snapshot, insert and settle a scratch job, and cannot
update a live torrent: PostgreSQL rejects the negative control with
`insufficient_privilege` (`42501`). It also proves the role cannot read the
undeclared `content_attributes` table. The direct queue-table grants are a test
harness capability, not a production-safe isolation claim; §5.4 requires a
reviewed row-scoped queue boundary before deployment.

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

The comparator's types contain only the frozen stable image: content identity
and metadata, `torrent_contents` classification fields including `InferID()`,
canonical languages/episodes, tags, and the delete outcome. Volatile database
and crawl fields (`created_at`/`updated_at`, generated `tsv`,
seeders/leechers/published-at snapshots, and unrelated surrogate IDs) never
enter the API.

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
`consume` subcommands. Migration 28 owns the database cursor. The loader uses
per-torrent and per-job compressed/decompressed/file-count limits and rejects
oversized inputs before transferring their blob.

Admission and runtime checks deliberately support only payloads whose workflow
is explicitly `default`, whose three attach flags are explicitly false, whose
source torrents still exist and have not changed since the source job settled,
which have no explicit `torrent_hints` row, and which have no reusable
source-backed `torrent_contents` association. Unsupported inputs settle only
their scratch job and are counted by a closed reason vocabulary; transient
database/runtime errors retain queue retry behavior. Rematch preserves explicit
Go hint semantics internally while disabling only content-association reuse,
although all explicit hints remain excluded from this milestone. Hydration and
the live comparison share one read-only repeatable-read snapshot, preventing a
concurrent Go transaction from creating a torn multi-table image.

This milestone is code-complete but not production-deployable. Direct
`INSERT`/`UPDATE` privileges on `queue_jobs` would allow a faulty shadow process
to mutate live `process_torrent` rows. Production needs reviewed RLS or
security-definer queue operations, negative controls against live-queue
mutation, and coordinated application of migrations 27 and 28 (including the
Go services' expected goose head). Deleted-input parity also remains
unmeasurable without pre-delete hydration or a durable write-set ledger. A
nonzero soak also requires proof that Go's effective merged classifier
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
