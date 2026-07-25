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

## Notes

Generated from fixtures so the format can be committed and reviewed. A real tape is recorded by running the classifier with CLASSIFIER_TAPE_DIR set; see the README next to this directory.
