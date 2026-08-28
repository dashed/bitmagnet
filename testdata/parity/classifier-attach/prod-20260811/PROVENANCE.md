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
- Generated at: 2026-08-11T06:19:21Z
- Content database: postgres bitmagnet-postgres.bitmagnet.svc.cluster.local:5432/bitmagnet
- Effective classifier config digest: sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae
- Records: 2000
- Observations: 715
- Incomplete records: 0
- Authoritative records: 2000
- Truncated: true
  - ended completed: 1940
  - ended deleted: 60

  The record cap was reached and recording stopped. This tape is not a
  complete oracle for the population it was drawn from; replaying that whole
  population against it will report misses.

## Flag state

- `default {"apis_enabled":true,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":true,"tmdb_enabled":true}`: 2000

## Observation kinds

- `local.content_by_id (ok)`: 17
- `local.content_by_search (ok)`: 418
- `tmdb.request (ok)`: 280

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

## Deployment evidence

- Source commit: `97304a42bf17b205f4f60e6278fd27dc7a0532d5`
- Source tree: `31f8280617f140cddcdd8d663fb7a06e6a668e7e`
- Writer image: `ghcr.io/dashed/bitmagnet:p0-97304a42`
- Imported content digest: `sha256:476a69504a9ef56e9524647d9360214d33de46dac5206ea0e61157c3f8b5b198`
- Runtime image/config ID: `sha256:8a54c2544906446cd01cde7c8df046642f909dbf0745de97a0ca9becac63c0cf`
- Image tar SHA-256: `7a3bde592f441086d17967ca64071f222781689582ed44b857ff3c93435b07b2`
- Pod UID: `62e5f6a2-07fd-4b93-9b70-5f718292a66c`
- Pod start: `2026-08-11T04:46:05Z`
- Successful on-full write observed: `2026-08-11T06:17:57Z`
- Final clean-shutdown write: `2026-08-11T06:19:21Z`
- Intended maximum: 2,000 records
- PVC: `bitmagnet/bitmagnet-classifier-tape`
- PV: `pvc-150b5cf5-cae2-4520-884b-524107b269b5`

The cap snapshot was copied before shutdown. It contained nine in-flight
records. The artifact committed here is the later clean-shutdown generation,
which closes all nine and is therefore the authoritative source generation.
The writer pod had zero restarts for the complete run.

After the final write, production was restored to
`ghcr.io/dashed/bitmagnet:p0-f4412986` with tape environment, mount and volume
absent. The retained PVC remains Bound and unmounted.

## Artifact acceptance

- `manifest.json` SHA-256: `f998d9f91240d218df137a2fdaf8a75bbc22ea0be23b39cf0ac17b230ab71c93`
- `tape.jsonl` SHA-256: `143031cfb9312cd71854988a828c55b52b5a658380a90e716f5521077b11d0ec`
- `PROVENANCE.md` source-generation SHA-256 before this evidence appendix:
  `2b113cd6d78c40a04511ab0db32c3d8e8b21f552e504cdaaa8b983878e692665`
- Tape lines: 2,000
- Maximum JSONL line: 358,186 bytes, below the 16 MiB reader limit
- Every `(subject, attempt)` is unique; every record has non-null embedded input
  whose ID equals the subject; every outcome is terminal and authoritative.
- Credential-shaped field, DSN, bearer token and private-key scan: clean.

No `inputs.json` exists for this corpus. The writer captured classifier-time
input directly, so no post-hoc production SQL export or reconstruction was
performed.
