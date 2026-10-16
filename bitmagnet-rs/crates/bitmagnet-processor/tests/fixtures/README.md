# Processor write-set parity fixture

`write_set.golden.jsonl` is a 330-record Go oracle generated from the frozen
classifier corpus in `testdata/parity/classifier/inputs.jsonl`.

The throwaway generator lived in `internal/processor` with
`package processor`, which is required to call the unexported
`newTorrentContent`. It built the classifier through the public
`classifier.New(Params{...})` path with flags off, assigned each fixture a
deterministic info-hash (`sha256(fixture ID)[:20]`), and projected the stable
classification-derived fields from Go's `TorrentContent`. The generator was
deleted after producing the fixture so Lane P remains confined to
`bitmagnet-rs/crates/bitmagnet-processor/**`.

The fixture deliberately covers:

- all 330 classifier corpus torrents;
- 310 classified writes and 20 delete-torrent outcomes;
- `files_count=1` inference for single-file torrents;
- stale `torrent_contents` deletion;
- preservation of an existing row whose ID equals `InferID()` (every seventh
  classified fixture); and
- the canonical write-shadow projection from Phase-3 contract §5.2(c).

The flags-off corpus never attaches a `Content` row and core.yml never emits
tags, so those arrays/maps are empty in this milestone. DB-bound enrichment and
the SELECT-only shadow negative control remain later Lane P milestones.
