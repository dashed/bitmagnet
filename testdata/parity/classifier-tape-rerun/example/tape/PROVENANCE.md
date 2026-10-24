# Classifier attach tape

Recording of the observations the Go classifier made against its impure
dependencies (local content search, TMDB) while classifying the subjects below.
It exists because the Go classifier is not a pure function of
(torrent, database snapshot): the local content search orders candidates by a
ts_rank_cd that ties, so the candidate window and its order are decided by the
query plan, and the levenshtein selection that follows is first-wins. Only the
ordered candidate list that was actually observed is replayable.

## Run

- Command: go test ./internal/classifier -run TestTapeRerunExampleGolden -update-tape-rerun-example
- Host: fixture
- Generated at: 2026-08-12T00:00:00Z
- Content database: none (fixtures, not a database)
- Effective classifier config digest: sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae
- Acquisition plan digest: sha256:c6febd6d4dbcc762050d5a4d38d401dc0d56f50f901b88fc252a382a83b455fe
- Records: 9
- Observations: 8
- Incomplete records: 0
- Authoritative records: 9
- Action entries: 10
- Truncated: false
  - ended completed: 6
  - ended deleted: 2
  - ended unmatched: 1

## Flag state

- `default {"apis_enabled":false,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":false,"tmdb_enabled":false}`: 1
- `default {"apis_enabled":true,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":true,"tmdb_enabled":true}`: 5
- `tape_evidence_action_entries {"apis_enabled":false,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":false,"tmdb_enabled":false}`: 1
- `tape_evidence_deleted {"apis_enabled":false,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":false,"tmdb_enabled":false}`: 1
- `tape_evidence_unmatched {"apis_enabled":false,"delete_content_types":[],"delete_xxx":false,"local_search_enabled":false,"tmdb_enabled":false}`: 1

## Observation kinds

- `local.content_by_id (ok)`: 2
- `local.content_by_search (ok)`: 2
- `tmdb.request (ok)`: 4

## Action entries

- `attach_local_content_by_id`: 3
- `attach_local_content_by_search`: 3
- `attach_tmdb_content_by_id`: 2
- `attach_tmdb_content_by_search`: 2

## Notes

Generated cross-language same-input, same-observation write-set fixture.
