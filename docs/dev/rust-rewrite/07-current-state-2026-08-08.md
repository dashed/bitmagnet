# Rust rewrite — current state, 2026-08-08

The other documents in this directory (`00-overview` … `06-risks-open-questions`,
`phase*-tasks`, `phase3-contracts`) describe the *plan*. They were last revised in
mid-July and several of their claims are now stale. **This document is the
authoritative statement of where things actually are.** Where it contradicts a
sibling document, this one is correct.

## One-line status

Phases 0–3 are landed and deployed. The read path is in production. The remaining
blocker for cutover is **B′ — flags-ON classifier parity**. The oracle now works
end to end — Go records the tape, Rust reads it — so the concrete gap has moved:
nothing *consumes* the replay yet. Wiring the classifier's `ContentResolver` seam
through it is the next step.

## What is deployed right now

| Component | Image | Notes |
|---|---|---|
| `bitmagnet-0` (DHT crawler / processor) | `bitmagnet:p0-f4412986` | my-fork tip. The **only pod that writes**. |
| `bitmagnet-l3` (serves all user traffic) | `bitmagnet:shadow-canary-4ef7ad45` | IngressRoute backend. |
| `bitmagnet-graphql` (dark Rust) | `bitmagnet-graphql:graphql-rss-gated-20260718-a86a8b4b` | Not user-routed. |
| `bitmagnet-torznab` (dark Rust) | `bitmagnet-torznab:torznab-20260718-4ef7ad45` | No IngressRoute — genuinely dark. |
| `bitmagnet-search` (Tantivy) | `bitmagnet-search:p0-20260710` | Shadow only; PG still serves. |
| `bitmagnet-filesearch` (L2 DuckDB) | `bitmagnet-filesearch:l2-17` | Engine errors are now logged. |
| `bitmagnet-pathsearch` (L3) | `bitmagnet-pathsearch:l3-5` | |

## Corrections to the older documents

- **Phase-3 lanes Q/R/C/P are landed.** Some arrived by *patch-equivalence* rather
  than branch ancestry, so `git merge-base --is-ancestor` reports a false negative.
  Use `git cherry` when auditing lane state.
- **`files-attr` is no longer a route-flip blocker** — merged 2026-07-18
  (`3d3cadfe`).
- **The live ingest-shadow pilot is dead** (torn down 2026-07-28). It was
  structurally incapable of producing evidence: both the mirror SQL and the
  consumer runtime required an archived payload to *echo* the classifier config,
  and no production producer ever writes those keys, so P(0 comparable outcomes)
  was 1.000 at any sample rate. The `phase3-contracts.md` §5.4/§5.6 predicate it
  enforced is still the contract; only the live instrument is gone.
  Its **PG roles and DSN secrets survive** and are what the offline replay uses.
- **Offline write-set replay replaced it**, and is the write-path evidence now.

## Write-set parity — current evidence

Re-run 2026-08-08 against my-fork tip, read-only vs production Postgres:

```
enrichmentIndependent   29,177 compared   29,177 matched   0 mismatched   rate 1.0
enrichmentDependent      3,082 compared        0 matched   3,082 mismatched (expected)
unbucketedCompared 0
```

🚨 **Read the buckets separately; never average them.** `enrichmentIndependent`
(live `content_source IS NULL`) is the real port-fidelity evidence — flags-on and
flags-off agree there by construction, so drift is real. `enrichmentDependent`
divergence is expected: a flags-off Rust write set structurally cannot reproduce
content Go attached with flags on.

Run it with `make bitmagnet-writeset-replay-gate CONFIRM=1 [LIMIT_JOBS=n]`
(homelab-infra). The harness is read-only at three independent layers: session
`default_transaction_read_only`, a startup refusal if the connecting role holds
INSERT/UPDATE/DELETE, and SELECT-only code under `REPEATABLE READ READ ONLY`.

This result supersedes the older 146,025/146,025 figure, which was invalidated
when the processor write set changed (see below).

## The processor write-set change you must know about

`preservedRuleDerivedContentType` (Go `internal/processor/processor.go`, mirrored
in Rust `bitmagnet-processor/src/load.rs::effective_hint`) preserves a stored
`content_type` when the torrent has **no stored file list** and the type is
**rule-derived** (no `content_source`).

Why it exists: the existing hint reuse requires `content_source` to be valid, so
it only ever covered *sourced* (tmdb/imdb) matches. A rule-derived type such as
`xxx` was never carried over, and with `files_status` of `no_info` /
`over_threshold` the rule cannot re-derive it either — so a reprocess silently
cleared it. Measured on production: **2,511,677 of 49,113,935 `torrent_contents`
rows (5.1%)** satisfy that precondition.

It is deliberately **not** gated on `ClassifyMode`: rematch means *re-derive*, and
when derivation is impossible `unknown` is data loss rather than a fresher answer.
Only the type is carried over — never source/ID — so it cannot resurrect a stale
content match.

## B′ — flags-ON classifier parity (THE remaining blocker)

There is no other B′ document in this repo; this section is it.

**Why it is required.** Rust implements the classifier with the three enrichment
flags OFF (`local_search_enabled`, `apis_enabled`, `tmdb_enabled`) because it does
not implement the `attach_*` actions. Production Go runs all three ON. Until Rust
reproduces flags-ON behaviour, the `enrichmentDependent` bucket can never match,
and the write path cannot be cut over.

**The hard part is the oracle.** Flags-ON behaviour depends on external state — a
TMDB API and a local content search over a live table. You cannot diff against it
deterministically without capturing it first.

**What is DONE and deployed** (all merged into `alberto/my-fork` and live on
`bitmagnet-0` as of 2026-08-08):

- **B′-0 seam** — `ContentResolver` dependency seam in the classifier.
- **B′-1 textmatch** — Go's Levenshtein matcher + string normalisation ported.
- **Recorder / observation tape** — `internal/tape/` (`recorder.go`, `replay.go`,
  `session.go`, `format.go`, `write.go`) plus the TMDB seam
  (`internal/tmdb/requester_recorder.go`) and the local-search seam
  (`internal/classifier/tape_local_search.go`).
  🔑 The tape is **inert in production**: with no tape session on the context it
  delegates straight through, costing a context lookup and a nil check. It is
  therefore safe that it is already deployed.

- **Rust-side tape replay** — `bitmagnet-rs/crates/bitmagnet-tape`. Loads a
  Go-written tape and answers observations from it, preserving the three
  properties that make a replay evidence: requests are asserted (not just
  answers), empty is distinct from missing, and incomplete records are excluded.
  🔑 Requests are compared BY BYTES, so the crate carries a `GoFormatter` that
  reproduces Go's canonical JSON. The escaping was determined empirically, not
  assumed: Go and serde_json agree on everything — including `\b` and `\f` —
  except **U+2028/U+2029**, which Go escapes unconditionally. That single
  divergence is the crate's whole reason for not calling `serde_json::to_string`.
  Pinned by `testdata/parity/tape-canonical/escapes.json`, generated FROM Go.

**What is NOT done — the remaining gap:**

1. **Nothing consumes the replay yet.** `bitmagnet-tape` can answer observations,
   but `bitmagnet-classifier` does not yet route its `ContentResolver` seam
   through it. That wiring is the next concrete step, and it is what turns the
   oracle into an actual parity run.
2. The four remaining lanes of the seven-lane carve.
3. No flags-ON parity gate exists, so there is no pass/fail criterion yet.
4. 🚨 **Tsquery construction is out of scope for the tape and must be proven
   separately.** The seam records the search string, not the tsquery it compiles
   into, so two implementations that agree on the string and disagree on the
   tsquery replay identically and never desync. Rust's `char::is_alphanumeric`
   disagrees with Go's `unicode.IsLetter || unicode.IsDigit` at 12,322 code
   points — an all-scalar test over the two predicates is the only thing that
   covers it.

**Suggested next milestone.** T9 "pre-attach" — compare classifier state *before*
the enrichment attach actions fire. It is the largest slice reachable without a
working oracle, so it can proceed in parallel with the Rust tape-replay work.

## Known hazards

- **Bulk reprocess is still unsafe at scale** in the sense that any fleet-wide
  rematch should be re-examined first; the specific `content_type`-clearing bug is
  fixed, but the class of "re-derivation loses data that was previously derivable"
  may affect other rules.
- **L2 filesearch degrades as segments accumulate.** DuckDB returns
  `Out of Memory Error: failed to pin block` at its 3 GB engine limit. Raising that
  limit is *not* the fix — 4 GB was tried and OOM-killed the container; 3 GB is
  chosen so the engine degrades gracefully instead. Segment count grows from each
  hourly seal and only resets at the monthly merge-base. A `*/5` probe CronJob now
  alerts on it.
- **`bitmagnet-l3` bypasses Postgres for free-text search** via the pathsearch
  composer, so PG-path fixes do not affect user-facing search there. `bitmagnet-0`
  has no composer and does hit the PG path.
