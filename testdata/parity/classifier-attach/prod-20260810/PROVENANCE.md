# Classifier attach tape

Recording of the observations the Go classifier made against its impure
dependencies (local content search, TMDB) while classifying the subjects below.
It exists because the Go classifier is not a pure function of
(torrent, database snapshot): the local content search orders candidates by a
ts_rank_cd that ties, so the candidate window and its order are decided by the
query plan, and the levenshtein selection that follows is first-wins. Only the
ordered candidate list that was actually observed is replayable.

## Run

- Command: bitmagnet classifier (CLASSIFIER_TAPE_DIR set)
- Host: bitmagnet-0
- Generated at: 2026-08-10T07:11:41Z
- Content database: postgres bitmagnet-postgres.bitmagnet.svc.cluster.local:5432/bitmagnet
- Effective classifier config digest: sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae
- Records: 2000
- Observations: 653
- Incomplete records: 9
- Truncated: true

  Those classifications were still running when the tape was written, so
  their observation lists are prefixes. A replay excludes them and reports
  a miss for those subjects rather than serving a short answer.

  The record cap was reached and recording stopped. This tape is not a
  complete oracle for the population it was drawn from; replaying that whole
  population against it will report misses.

## Flag state

- `default {"apis_enabled":true,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":true,"tmdb_enabled":true}`: 2000

## Observation kinds

- `local.content_by_id (ok)`: 13
- `local.content_by_search (ok)`: 386
- `tmdb.request (ok)`: 254

## What a green replay against this tape does NOT prove

**The desync guarantee stops at the `searchString` boundary.**

`local.content_by_search` records the search string the classifier hands to the
query builder, not the tsquery that string is compiled into. The tsquery is
built inside the query builder and is not exposed at this seam. So an
implementation that derives a *different* tsquery from the same base title does
**not** desync: replay matches on the search string, hands back Go's recorded
candidates, and the divergence is invisible. This is the one class of bug the
request half of the tape was built to catch and cannot.

That is not hypothetical. Rust's word-character predicate (`char::is_alphanumeric`)
disagrees with Go's `unicode.IsLetter || unicode.IsDigit` at 12,322 code points --
see `bitmagnet-rs/crates/bitmagnet-fts/src/lib.rs` and
`bitmagnet-rs/crates/bitmagnet-search/src/query.rs`, both of which document the
gap as harmless. In the search path it silently narrows the query, turning an
`&` into an adjacency `<->`, so Rust returns a strict subset of Go's rows with
no error at all.

Tsquery construction is therefore **out of scope for this tape** and has to be
proven separately, by an all-scalar test over the two word-character predicates.
A green replay here must not be read as covering it.

Extending the seam down to the tsquery would be a query-builder change and a
separate decision; it has deliberately not been made.

## Notes

Recorded live from the running classifier.

## Export (added out-of-band, 2026-08-10)

`inputs.json` is NOT written by the recorder. It is a read-only export of the
classifier input for each recorded subject, taken from production immediately
after the recording, and it is what lets the tape be replayed offline.

* **1,921 of 2,000 subjects.** The other 79 torrents were deleted from the
  database between recording and export. Ordinary churn; they have no input and
  a replay skips them.
* 🚨 **File lists come from `torrent_files`, not the `files_data` blob the
  processor actually hydrates from.** They are expected to agree, but that is an
  assumption rather than a proof.
* **`contents` is new in this corpus** and carries the hydrated `content` row
  for each `torrent_contents` association. Without it a replay cannot reproduce
  Go's PRE-ATTACH (T9): `processor.go` synthesises a hint from the first sourced
  association whenever the stored hint has no content source, and `runner.Run`
  then attaches that content BEFORE the workflow, suppressing the whole
  enrichment branch.
* `hint` is the STORED `torrent_hints` row, NOT the processor's synthesised
  effective hint. The synthesis is logic, not data, and belongs in the replay.

## 🚨 No outcomes in this corpus

The deployed `bitmagnet-0` image (`p0-f4412986`) predates `tape.RecordOutcome`,
so every record's outcome is UNKNOWN and nothing here is "authoritative" in the
sense `Record::authoritative()` means. Recording outcomes requires deploying the
production WRITER, which is a materially larger operation than setting a tape
flag. Until then a zero-observation record cannot be told apart from a
classification that ended early.
