# Rust rewrite — current state, 2026-08-09

Supersedes [`07-current-state-2026-08-08.md`](07-current-state-2026-08-08.md),
which is now stale in several load-bearing places (it lists TMDB replay, the
corpus harness and the parity gate as not done, and flags a unicode/tsquery
divergence that is actually merged). **This document is the authoritative
statement of where things are.** Where any sibling disagrees, this one is
correct.

## One-line status

Phases 0–3 are landed and deployed. **B′ — flags-ON classifier parity — is
functionally complete and measured**: the oracle works end to end for both
seams, all four `attach_*` actions and the pre-attach path are implemented, and
the live PostgreSQL + TMDB backends exist. What remains is not classifier logic;
it is evidence at scale, one structural limit in the write-set oracle, and
production wiring.

## What is deployed right now

Unchanged from 07 — **nothing from this work is deployed**:

| Component | Image | Notes |
|---|---|---|
| `bitmagnet-0` (DHT crawler / processor) | `bitmagnet:p0-f4412986` | The **only pod that writes**. |
| `bitmagnet-l3` (serves all user traffic) | `bitmagnet:shadow-canary-4ef7ad45` | IngressRoute backend. |
| `bitmagnet-graphql` (dark Rust) | `bitmagnet-graphql:graphql-rss-gated-20260718-a86a8b4b` | Not user-routed. |
| `bitmagnet-torznab` (dark Rust) | `bitmagnet-torznab:torznab-20260718-4ef7ad45` | Genuinely dark. |
| `bitmagnet-search` (Tantivy) | `bitmagnet-search:p0-20260710` | Shadow only. |
| `bitmagnet-filesearch` (L2 DuckDB) | `bitmagnet-filesearch:l2-17` | |
| `bitmagnet-pathsearch` (L3) | `bitmagnet-pathsearch:l3-5` | |

`bitmagnet-0` was rolled twice on 2026-08-09 to record a classifier tape and
then to revert it. Verified after the revert: no `CLASSIFIER_TAPE_*` env, no
tape volume, pod 2/2, crawler/http/queue workers up. The 2Gi PVC was left Bound
on purpose (an `emptyDir` would have destroyed the tape on the revert roll); the
corpus is committed, so the PVC is now redundant.

The write-set replay gate image pin is still `wsr-f4412986`. Newer images
(`wsr-446ac39d`, `wsr-d8859cad`) are built and imported into HEL1 containerd but
**deliberately not pinned** — a pin change should follow a decision, not ride
along with a measurement. Gate runs used `-e bitmagnet_writeset_replay_image_tag=…`.

## Corrections to 07

- **TMDB replay is wired** (`4739f0be`). 07 calls it "the next concrete step".
- **The corpus harness and a pass/fail gate exist** (`0a4cd4f3`, `df705322`).
- **The tsquery / unicode-class divergence is CLOSED.** 07 lists it as an open
  gap needing an all-scalar test; the fix (`0db30a33`) is an ancestor of
  `alberto/my-fork`, and `bitmagnet-fts`'s tokenizer uses a table generated
  *from Go*. Do not re-open it.
- **T9 pre-attach is implemented**, not merely "the suggested next milestone".

Everything else in 07 — the deployed table, the `preservedRuleDerivedContentType`
change, the dead ingest-shadow pilot, the read-only guarantees — still holds.

## B′ — what now exists

- **Observation tape**, recorded from Go and replayed in Rust
  (`internal/tape/`, `crates/bitmagnet-tape`). Requests are asserted BY BYTES,
  empty is distinct from missing, incomplete records are excluded.
- **Both seams replay.** Local (`local.content_by_id` / `local.content_by_search`)
  and TMDB (`tmdb.request`, recorded at the HTTP level).
- **All four `attach_*` actions** plus the **T9 pre-attach**.
- **Live backends**: `bitmagnet-content-search` (PostgreSQL) and
  `bitmagnet-tmdb` (HTTP), composed by `bitmagnet-resolver`'s
  `LiveContentResolver`.

### The two gates, and how to read them

**Desync gate — `crates/bitmagnet-classifier/tests/prod_corpus_gate.rs`**, against
300 classifications recorded live from `bitmagnet-0`:

```
subjects=284 matched=282 desynced=0 missed=0 unconsumed=0 errored=0
not_authoritative=2  observations=120/120
```

Every recorded observation consumed — all 72 `local.content_by_search` and all
48 `tmdb.request` — each matching Go's request byte for byte, in order.

🚨 **Only 72 of the 284 subjects observed anything at all.** The other 212
recorded nothing and "match" by both sides asking nothing, which is weak
evidence. The load-bearing part is the 72 subjects / 120 observations that
actually exercised a seam. Do not quote 284 as 284 confirmations.

**Write-set gate — `make bitmagnet-writeset-replay-gate CONFIRM=1`** (read-only
at three independent layers), 200 jobs:

```
                       pre-T9         T9 only              + structured materialize
enrichmentIndependent  2073/2073      2058/2058            2054/2054   rate 1.0
enrichmentDependent    240 MISMATCH   0 compared           231 compared, 200 matched,
                                      (236 unsupported)    31 mismatch — rate 0.866
```

Read the buckets separately, never averaged. `enrichmentIndependent` is the real
port-fidelity evidence and has held at 1.0 throughout.

### 🚨 The write-set oracle has a structural ceiling at ~86.6%

The residual 31 are **not** a port defect. Verified by running **Go's own
classifier** on the replay state: for one subject Go itself yields
`languages=[ja]` when content is already attached, because `AttachContent` folds
in `OriginalLanguage` while `len(Languages)==0`, before `parse_video_content`
runs. The live row says `["en","it"]` — what Go produced on its **first** run,
when nothing was attached and inference went first.

Go is not write-set-idempotent, so a **replay** and a **snapshot of a first run**
legitimately differ. Rust faithfully reproduces Go-on-replay. Closing this needs
a harness where **both** implementations re-run against the same state, not
Rust-replay against a snapshot. That is an apparatus change and it is what a
cutover argument will ultimately rest on.

## What is NOT done

1. **Nothing constructs a `LiveContentResolver` in production.** No wiring into
   the processor or serving path, no config plumbing for the TMDB key or the PG
   pool. The capability is built and tested; it is not reachable. This is the
   first step that performs real TMDB I/O with flags ON, so it wants a decided
   blast radius and a way back.
2. **The live backends' ANSWERS are unproven.** A tape replay shows the
   classifier asks the right questions; it cannot show these backends answer
   them as Go's do. The SQL has been validated read-only against production
   (statements run, plausible rows) — that is not the same as answer parity.
3. **The corpus is ~0.3% of the stated gate.** T1 asks for ≥25,000 stratified
   classifications with each attach action entered ≥2,000× and each terminal
   outcome ≥500×. Today: 300 records, 72 subjects exercising a seam.
   Re-recording is now cheap — outcomes are recorded, TMDB replays, and the
   export needs `contents` anyway.
4. **The both-re-run write-set harness** (see the ceiling above).
5. `attach_hint_unsupported` still makes the *live shadow runtime* refuse a job
   outright. The write-set replay binary does not consult it (it has its own
   per-hash reasons), so it did not gate these measurements.

## Known hazards

Carried forward from 07 and still true: bulk reprocess is unsafe at scale; L2
filesearch degrades as segments accumulate (3 GB DuckDB engine limit, raising it
OOM-kills the container); `bitmagnet-l3` bypasses Postgres for free-text search
via the pathsearch composer.

New, and each one cost real time to find:

- 🚨 **`#[serde(default)]` is not Go's `encoding/json`.** `default` covers an
  *absent* field, not an explicit `null`. Go documents that unmarshalling `null`
  into a non-pointer has no effect; serde rejects it. TMDB sends `null` freely.
  Every response DTO needs `deserialize_with = "null_to_default"`. Found by the
  gate: four real subjects errored on `invalid type: null, expected a string`.
- 🚨 **A tape record with an empty observation list is ambiguous** unless it
  carries an outcome, because a classification that ends early *closes* its
  session and is written as a COMPLETE record with nothing in it. `Incomplete`
  only tracks still-open sessions. Fixed by `tape.RecordOutcome` (`9581fe59`);
  the 2026-08-09 corpus predates it, so all its outcomes are `unknown`.
- 🚨 **Two different render paths for one language set.**
  `Languages.Slice()` is natsort by language NAME (display); the
  `torrent_contents.languages` COLUMN is alpha-2 CODE order. They disagree —
  `["de","fr"]` by code is German/English, i.e. `["fr","de"]` by name. The Go
  write-set oracle fixture `language-0001-multi-french-german` pins this.
- 🚨 **An empty tsquery does not mean "no results".** A base title with no word
  characters compiles to an empty tsquery, and Go then *drops* the `tsv @@`
  filter (`query.go:751`) and ranks literally `0` (`query.go:717-724`) —
  returning the first 10 rows of the content type. A careless port returns none.
- 🚨 **The processor SYNTHESISES the classifier's hint** from the first sourced
  `torrent_contents` association whenever the stored hint has **no** content
  source (`processor.go:119-134`). A NULL `content_source` in `torrent_hints` is
  the *precondition* for that path, not evidence against it. Getting this
  backwards produced two wrong diagnoses before the right one.
- 🚨 **TMDB `queryParams` is a Go map**, so `encoding/json` sorts its keys, and
  the tape compares requests byte for byte. A `HashMap` on the Rust side
  desyncs nondeterministically; use a `BTreeMap`.

## Runbook pointers

- Desync gate: `cargo test -p bitmagnet-classifier --release --test prod_corpus_gate -- --nocapture`
- Flags-off goldens: `cargo test --release --test classifier_pair` (330/330)
- Go write-set oracle: `cargo test -p bitmagnet-processor --release --test write_set_parity`
- Write-set gate (prod, read-only): `make bitmagnet-writeset-replay-gate CONFIRM=1 [LIMIT_JOBS=n]`
- Replay image: `make bitmagnet-image-import CONFIRM=1 REF=<40-char sha> IMAGE=writeset-replay TAG=wsr-<short>`
  — the gate playbook has no tag passthrough, so pass
  `-e bitmagnet_writeset_replay_image_tag=…` to the playbook directly.
