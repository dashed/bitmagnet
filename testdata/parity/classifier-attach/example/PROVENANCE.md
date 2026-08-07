# Classifier attach tape

Recording of the observations the Go classifier made against its impure
dependencies (local content search, TMDB) while classifying the subjects below.
It exists because the Go classifier is not a pure function of
(torrent, database snapshot): the local content search orders candidates by a
ts_rank_cd that ties, so the candidate window and its order are decided by the
query plan, and the levenshtein selection that follows is first-wins. Only the
ordered candidate list that was actually observed is replayable.

## Run

- Command: go test ./internal/classifier -run TestTapeExampleGolden -update-tape-example
- Host: fixture
- Generated at: 2026-07-25T00:00:00Z
- Content database: none (fixtures, not a database)
- Effective classifier config digest: sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae
- Records: 4
- Observations: 5
- Incomplete records: 0
- Truncated: false

## Flag state

- `default {"apis_enabled":false,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":false,"tmdb_enabled":false}`: 1
- `default {"apis_enabled":true,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":true,"tmdb_enabled":true}`: 2
- `default {"apis_enabled":true,"tmdb_enabled":true}`: 1

## Observation kinds

- `local.content_by_search (ok)`: 2
- `tmdb.request (error)`: 1
- `tmdb.request (ok)`: 2

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

Generated from fixtures so the format can be committed and reviewed. A real tape is recorded by running the classifier with CLASSIFIER_TAPE_DIR set; see the README next to this directory.
