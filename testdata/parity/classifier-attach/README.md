# Classifier attach tape

This directory is where a recording of the Go classifier's **flags-ON**
enrichment observations lands. `example/` holds a committed fixture-generated
tape that pins the format. The dated `prod-*` directories are separately
authorised recordings of real production traffic; `prod-20260811` is the first
one whose writer captured classifier-time input and terminal outcomes directly.

## Why a tape

The Go classifier is not a pure function of `(torrent, database snapshot)`.
`internal/classifier/search.go` asks the local content search for the top ten
candidates ordered by `ts_rank_cd`, and that rank is degenerate for the phrase
queries the classifier issues -- a real production query for `"cinderella"`
returns dozens of rows all ranked exactly `1`. Which rows land in the `LIMIT 10`
window, and in what order, is then decided by the query plan. The levenshtein
selection that picks the winner is strictly first-wins with an early exit on an
exact match, so the plan's order decides the answer.

Freezing the database is therefore not enough to build an oracle: re-running the
query re-rolls the dice. The only replayable artifact is the ordered candidate
list that was **actually observed**, which is what this tape holds.

## What is recorded

Each classification record embeds the exact, language-neutral classifier input
captured at `runner.Run` entry: name and size, file status/list in slice order,
the **effective** hint the processor handed to the classifier, and existing
content associations (including hydrated content rows) in slice order. This is
the authoritative input for that specific `(subject, attempt)`; it avoids
reconstructing pre-classification state from a database export taken after the
classification has already written its result.

The `input` field is optional within the v1 schema so existing tapes remain
loadable. A replay may use its legacy out-of-band input only when the field is
absent. A present-but-null or present-but-undecodable input fails closed rather
than falling back. New v1 records therefore require the updated reader; older
strict Go readers reject the new field instead of silently replaying it wrong.

Per classification, the following dependency observations are then recorded in
the order the classification made them:

| Kind | Seam | Recorded |
| --- | --- | --- |
| `local.content_by_search` | `internal/classifier/search.go`, on the raw result of the `LIMIT 10` query, **before** levenshtein | search string, content type, year, the release-date range the year expands into, the window's order and limit; every candidate with its `ts_rank_cd` |
| `local.content_by_id` | `internal/classifier/search.go`, on the `LIMIT 1` query | content ref, whether the canonical or an alternative identifier was matched, the ordering applied; the row, or an explicit empty list |
| `tmdb.request` | `internal/tmdb`, at the `Requester` seam | method, path and every query parameter; the HTTP status and the raw response body |

Every observation records **the request as well as the response**. On replay the
incoming request is compared with the recorded one and a mismatch is a hard
error. That is what catches a port asking a *different question* -- a different
search string, a dropped year filter, a missing query parameter -- even when the
answers happen to coincide.

The `api_key` is set once on the TMDB HTTP client and never passes through the
recorded parameter map, so a tape carries no credential.

## 🚨 What a green replay against this tape does NOT prove

**The desync guarantee stops at the `searchString` boundary.**

`local.content_by_search` records the search string the classifier hands to the
query builder, not the tsquery that string is compiled into. The tsquery is
built inside the query builder and is not exposed at this seam. So an
implementation that derives a *different* tsquery from the same base title does
**not** desync: replay matches on the search string, hands back Go's recorded
candidates, and the divergence is invisible. This is the one class of bug the
request half of the tape was built to catch and cannot.

That is not hypothetical. Rust's word-character predicate
(`char::is_alphanumeric`) disagrees with Go's `unicode.IsLetter ||
unicode.IsDigit` at 12,322 code points -- see
`bitmagnet-rs/crates/bitmagnet-fts/src/lib.rs` and
`bitmagnet-rs/crates/bitmagnet-search/src/query.rs`, both of which currently
document the gap as harmless. In the search path it silently narrows the query,
turning an `&` into an adjacency `<->`, so Rust returns a strict subset of Go's
rows with no error at all.

Tsquery construction is therefore **out of scope for this tape** and has to be
proven separately, by an all-scalar test over the two word-character predicates.
A green replay here must not be read as covering it.

Extending the seam down to the tsquery would be a query-builder change and a
separate decision; it has deliberately not been made. The text above is also
written into every recorded tape's `PROVENANCE.md`, from
`classifier.TapeScopeLimits`, so a tape read in isolation still carries its own
limits.

## Empty is not missing

- An observation with `"outcome":"ok"` and an empty result is a genuine empty
  answer: the query ran and matched nothing.
- A classification that consulted nothing appears as a record with
  `"observations":[]` -- "classified, observed nothing" is a fact.
- An observation that was never recorded is **absent**, and a replay that reaches
  for it gets a distinct miss error. A replay must never read a gap as a
  legitimate empty answer.

The reader enforces this: a record whose observation list is `null`, or an `ok`
observation whose response is absent or `null`, is rejected rather than loaded.

A record marked `"incomplete":true` was still being classified when the tape was
written -- possible for a snapshot taken at the record cap while other
classifications were in flight. Its observation list is a prefix, so the reader
excludes it: asking about that subject reports a miss rather than serving a
short answer that looks like a classification which stopped asking. The write on
a clean shutdown supersedes the snapshot with the complete records.

## Files

| File | |
| --- | --- |
| `tape.jsonl` | one JSON object per classification, sorted by `(subject, attempt)` |
| `manifest.json` | schema version, effective classifier config digest, counts, truncation flag |
| `PROVENANCE.md` | what ran, when, on what host, against which database, under which flags |

`manifest.json`'s `effectiveConfigDigest` pins the classifier configuration the
tape was recorded under (`classifier.EffectiveConfigDigest`). Loading a tape
against a different digest fails closed.

Ordering under concurrency: the classifier runs classifications concurrently
(`classifier.concurrency`, default 10) while serialising local searches to one
at a time. Each classification holds its own session, so its observation
sequence is unaffected by whatever else is running; the tape is then sorted by
subject at write time, which is what makes the bytes deterministic.

## Recording a tape

Recording is off unless a tape directory is configured. Set:

```
CLASSIFIER_TAPE_DIR=/path/to/tape
CLASSIFIER_TAPE_MAX_RECORDS=5000
```

and run the classifier over the population you want to capture. The recorder
accepts exactly that many records; the next classification is refused a tape
session and synchronously writes the cap snapshot. A clean shutdown writes
again, closing any classifications that were in flight at the cap. Exceeding
the cap marks the tape truncated: a truncated tape is not a complete oracle for
the population it was drawn from, and replaying that whole population against
it will report misses.

**This is an offline evidence-gathering mode, not a serving mode.** Recording a
production run is a separate, explicitly authorised step.

## Regenerating the example

```
go test ./internal/classifier -run TestTapeExampleGolden -update-tape-example
```

The example is generated from fixtures, with a pinned timestamp, so it is
byte-reproducible and diffs cleanly. It deliberately includes a tied candidate
window, a genuine empty answer, a classification with no observations at all,
and a recorded TMDB failure.
