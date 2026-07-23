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

The live-PostgreSQL gate runs against goose 26 and proves upsert/delete/tag
semantics, blocker-before-delete ordering, exact microsecond timestamps, and
full rollback when a late tag write fails.

This is not yet the complete runtime persistence milestone:

- attached `content` remains rejected until Lane C exposes the structured
  content row and its associations;
- the runtime loader still needs to carry the volatile persistence metadata and
  construct the weighted FTS vector;
- the concrete stable-bloom blocking manager is not yet ported; and
- queue retry wiring, post-commit Tantivy dual-write, the poll-mirror, and the
  full write-set comparator remain outstanding.

## Milestone 3: live snapshot and permission fail-safe

`read_live_snapshot` now reads the stable comparison image with non-locking
`SELECT`s over exactly `content`, `torrent_contents`, `torrent_tags`, and
`torrents`. It canonicalizes languages, episodes, content rows, and tags, and
represents a Go-deleted torrent as the first-class `live_absent` outcome.

The goose-26 PostgreSQL gate creates the frozen shadow role with SELECT on those
four live tables and SELECT/INSERT/UPDATE on `queue_jobs`. It proves the role can
read the snapshot, insert and settle a scratch job, and cannot update a live
torrent: PostgreSQL rejects the negative control with `insufficient_privilege`
(`42501`). It also proves the role cannot read the undeclared
`content_attributes` table.

Attached-content identifiers remain intentionally empty in the snapshot. The
identifiers live in `content_attributes`, but §5.4 grants the shadow role no
access to that table. Expanding that frozen permission boundary requires an
explicit contract decision; the reader does not silently widen it.
