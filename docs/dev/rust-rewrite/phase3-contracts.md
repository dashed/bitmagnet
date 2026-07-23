# Phase 3 — frozen contracts (base-prep)

Lead-owned base-branch prep for the Phase-3 ingest port, per
`rust-phase3-plan-draft-20260717.md §1` (user-approved) and `phase2-tasks.md §0`
(freeze shared contracts before lanes race). Every frozen fact cites
`file:line` evidence read from the base worktree at
`origin/alberto/my-fork` = `1b57442d` (branch `codex/phase3-base-20260717`).

Scope of this document: the four hard contracts (queue wire/DB, classifier
corpus, release-parse output, summary-write) plus the **FULL write-shadow
strategy** (user decision #4). It does **not** implement any lane; it is the
written target the lanes build against.

> **✅ REVIEW STATUS:** §5 (the write-shadow mechanism) was **reviewed and
> APPROVED by the team-lead on 2026-07-17**, with one amendment to §5.2(B)
> (sample archived `processed` rows and copy the payload verbatim — now folded
> in). §§1–4 are frozen facts with code citations.

Companion artifacts checked in with this doc:

- `internal/parity/queue_gen_test.go` — fingerprint + backoff generator/verifier
  (runs in `go test ./...`).
- `internal/parity/queue_dequeue_gen_test.go` — live-PG dequeue-ordering
  differential (`//go:build integration`).
- `testdata/parity/queue/{fingerprints,backoff,dequeue_ordering}.jsonl` — the
  frozen goldens.
- `bitmagnet-rs/crates/bitmagnet-diff/tests/classifier_pair.rs` — classifier
  `Driver` stub + normalizer round-trip over the 330-fixture corpus.

---

## 0. Corrections to the plan draft (found during code verification)

Three facts in `rust-phase3-plan-draft-20260717.md` are wrong or imprecise
against the code at `1b57442d`. They are corrected below and must be treated as
the frozen truth:

1. **Dedup index is on `(fingerprint)` ALONE, not `(fingerprint, status)`.**
   Plan §1.1 describes the pre-`00019` shape. `00019_queue_fix_duplicate_key.sql`
   **drops** the `(fingerprint, status)` partial index and creates
   `queue_jobs_fingerprint_idx ON queue_jobs (fingerprint) WHERE status IN
   ('pending','retry')` — on `fingerprint` only (`00019:8`). It also first
   `DELETE`s `retry` rows that collide with a `pending` row of the same
   fingerprint (`00019:4`). **Frozen contract: at most one active (`pending` OR
   `retry`) job may exist per fingerprint** — strictly stronger than the plan's
   claim. See §1.4.

2. **The proto `Classification` message DROPS `year` and `video3d`.**
   `transformer.go NewClassification` never sets `Year` (commented out at
   `internal/protobuf/transformer.go:113-117,165`) nor `Video3d` (proto field 10
   declared at `bitmagnet.proto:53` but never assigned). So the CEL `result`
   variable (a `bitmagnet.Classification`) exposes **no year and no video3d**,
   even though the classifier corpus `expected` DOES capture both (they live on
   `ContentAttributes`, not the proto). These are two distinct output surfaces —
   see §3.2. Lane R/C must not "fix" the transformer to emit them; parity means
   reproducing the omission.

3. **Custom CEL functions actually invoked by `classifier.core.yml` are
   `.sum()`, `.join()`, `matches()` — nothing else.** Plan §1.2's "only join +
   sum" is right about the *hot path*, but the registered surface is wider:
   `join`/`matches` come from `ext.Strings` (`cel_env.go:20`); `sum` is one of
   **six** functions in the vendored `k8s.lists` library (`isSorted`, `sum`,
   `max`, `min`, `indexOf`, `lastIndexOf` — `cel_lists.go:138-168`), of which
   core.yml uses only `sum`. The other five and the wider `ext.Strings` surface
   matter only for user-supplied `classifier.yml` extensibility and land
   incrementally (§2.4).

---

## 1. Queue-job wire/DB contract (blocks Lane Q + the Phase-4 producer seam)

Oracle: `internal/queue/**`, `internal/model/queue_jobs.go`,
`migrations/00012_queue.sql`, `00015_queue_priority.sql`,
`00019_queue_fix_duplicate_key.sql`.

### 1.1 Fingerprint

`model.NewQueueJob` (`internal/model/queue_jobs.go:11-38`):

```
payloadStr = string(json.Marshal(payload))          // :12
fingerprint = hex(sha256(queue + payloadStr))       // :18-26  io.WriteString(h, queue+payloadStr); "%x"
```

- **Byte-identical or dual-run double-processes** (`06 R4`). The Rust marshaller
  must reproduce Go's `encoding/json` output exactly: struct-field declaration
  order, per-type key casing, `omitempty` semantics, **and Go's map-key
  alphabetical sort** (`ClassifierFlags` is a map — see below).
- Frozen goldens: `testdata/parity/queue/fingerprints.jsonl` (subsystem
  `queue_fingerprint`), one fixture per representative payload; `expected` pins
  the exact `payload` bytes AND the `fingerprint`. The generator self-checks that
  `hex(sha256(queue||payload)) == constructor fingerprint`
  (`queue_gen_test.go`).

### 1.2 The three job types + payload JSON (the fingerprint input)

| Queue | `MessageName` | Struct | Key casing | Defaults injected by constructor |
|---|---|---|---|---|
| `process_torrent` | `internal/processor/message.go:9` | `MessageParams` `:22-27` | **PascalCase** | `MaxRetries(2)` `:33` |
| `process_torrent_batch` | `internal/processor/batch/message.go:12` | `MessageParams` `:14-24` | **PascalCase** | `BatchSize=100` `:43`, `ChunkSize=10000` `:47`, `MaxRetries(2)` `:53` |
| `blob_migration` | `internal/blobmigration/queue/message.go:5` | `MessageParams` `:13-24` | **camelCase** | `ChunkSize=DefaultChunkSize(2000)` `:28` + `handler.go:39`, `NumRanges=1` `:32`, `MaxRetries(2)` `:38` |

Field-level subtleties the goldens pin (all verified in `fingerprints.jsonl`):

- **`process_torrent`** — declaration order `ClassifyMode, ClassifierWorkflow,
  ClassifierFlags, InfoHashes` (`message.go:23-26`). `InfoHashes` has **no
  `omitempty`** → always emitted; the other three are `omitempty`. `InfoHashes`
  array order is **preserved, not sorted** (fingerprint is order-sensitive).
  `ClassifierFlags` is `classifier.Flags = map[string]any`
  (`internal/classifier/flag.go:36`) → Go emits map keys **alphabetically
  sorted**. `ClassifyMode` is an int enum (`ClassifyModeDefault=0`,
  `ClassifyModeRematch=1`, `message.go:13-20`).
- **`process_torrent_batch`** — `InfoHashGreaterThan` is `protocol.ID` ( a
  `[20]byte` array) with `omitempty`, and `UpdatedBefore` is `time.Time` with
  `omitempty`; **Go never omits struct/array types for `omitempty`**, so both are
  **always serialized** — the zero info-hash as
  `"0000000000000000000000000000000000000000"` and the zero time as
  `"0001-01-01T00:00:00Z"`. This is a classic Rust-port trap (serde `Option`
  would skip them). See fixture `process_torrent_batch_defaults_injected`.
- **`blob_migration`** — camelCase; `infoHashGreaterThan`, `rangeId`,
  `numRanges`, `chunkSize` have **no `omitempty`** (always present);
  `infoHashLessOrEqual` is `omitempty`. `InfoHashGreaterThan` here is a **hex
  string**, not a `protocol.ID` (`message.go:15`).

`protocol.ID` JSON marshaling: `MarshalJSON` → `json.Marshal(id.String())` and
`String()` → `hex.EncodeToString(id[:])` (`internal/protocol/id.go:171-173,
105-107`) — i.e., a 40-char lowercase hex string.

### 1.3 QueueJob row + status enum

`model.QueueJob` (`internal/model/queue_jobs.gen.go:15-30`): columns `id`
(text PK, `gen_random_uuid()`, read-only), `fingerprint`, `queue`, `status`,
`payload` (jsonb), `retries`, `max_retries`, `run_after`, `ran_at`, `error`,
`deadline`, `archival_duration` (interval), `created_at`, `priority`. Default
`ArchivalDuration = 7*24h` (`queue_jobs.go:31`); default `Status = pending`
(`.gen.go:19`). Status enum: `pending`, `processed`, `retry`, `failed`
(`internal/model/queue_job_status_enum.go:19-22`; DDL `00012_queue.sql`).

### 1.4 Dedup, priority, dequeue ordering, retry/backoff, GC, poll

**Dedup (corrected — see §0.1):** partial-unique on `(fingerprint) WHERE status
IN ('pending','retry')` (`00019:8`). At most one active job per fingerprint. DB
enforces this, so overlapping Go/Rust producers are safe **iff** the fingerprint
matches (`04 §3.3` rule 2).

**Priority:** ASC = lower-first; default 0; importer enqueues at 20
(`internal/importer/importer.go`, `model.QueueJobPriority(20)`).

**Dequeue** (`internal/queue/server/server.go:193-215`, GORM `handleJob`):

```
WHERE queue = ? AND status IN ('pending','retry') AND run_after <= now()   -- :201-207
ORDER BY (status = 'retry'), priority, run_after                           -- :208-211
FOR UPDATE SKIP LOCKED                                                      -- :212-214
LIMIT 1                                                                     -- .First()  :215
```

- `(status='retry')` is a boolean: `false` (pending) sorts before `true`
  (retry), so **all pending drain before all retry**; within a group, priority
  ASC then run_after ASC.
- **No `id` tiebreak.** Ties in `(status='retry', priority, run_after)` make the
  order nondeterministic — beware the `sqlx LIMIT $n` generic-plan trap
  (`sqlx-parameterized-limit-plan-trap`); **use a literal `LIMIT 1`** and seed
  goldens with distinct sort keys.
- Frozen golden: `testdata/parity/queue/dequeue_ordering.jsonl` (subsystem
  `queue_dequeue`) — a live-PG differential (proved against the local PG16) whose
  crafted seed yields `[p-d, p-a, p-b, p-c, r-a, r-b, r-c]`, exercising
  pending-before-retry, priority-ASC, run_after-ASC, and exclusion of future /
  terminal-status / wrong-queue rows.

**Retry/backoff** (`server.go:227-256` + `internal/queue/helpers.go:17-25`):
deadline check (`:227`); `retries++` only if the claimed job was not `pending`
(`:233-235`); on handler error with `retries < max_retries` →
`status=retry, run_after = CalculateBackoff(retries)` (`:245-247`), else
`status=failed` (`:249`); on success → `status=processed` (`:254`). One txn per
job.

`CalculateBackoff(retryCount)` = `now().UTC() + (round(retryCount^4) + 15 +
RandInt(30)*retryCount + 1) seconds` (`helpers.go:17-25`). **`RandInt(30)` ∈
[0,29] is nondeterministic** (`helpers.go:10-13`, rand seeded by wall clock). The
golden pins only the **deterministic part** `retryCount^4 + 16` and **bounds the
jitter** `[0, 29*retryCount]`, never an exact timestamp
(`testdata/parity/queue/backoff.jsonl`, subsystem `queue_backoff`, retries 0..5);
`TestQueueBackoffJitterWithinBounds` proves the real function stays inside that
envelope.

**GC** (`server.go:125-146`): deletes `status IN ('processed','failed') AND
ran_at + archival_duration < now()` on a ticker (default archival 7d).

**Poll model** (`internal/queue/handler/handler.go:59-83`, `server.go:157-191`):
`CheckInterval` default 30s (`handler.go:75`), `Concurrency` default `NumCPU`
(`handler.go:81-83`). The dispatcher starts a `time.NewTicker(1)` (1 ns), resets
to `CheckInterval` when idle, and **self-chains tight (`Reset(1)`) after any hit**
(`server.go:158,178,185-187`). Per-job handler timeout via `handler.Exec`
(`handler.go:94-148`). **LISTEN/NOTIFY is disabled consumer-side** — the whole
listener path is commented out (`server.go:1-3, 42-107`); the consumer is
**poll-only**. The **producer-side `queue_announce_job()` `pg_notify` trigger
still exists** (`00012_queue.sql`, `AFTER INSERT ... WHEN (run_after <=
created_at)`), but crawler jobs are enqueued with `QueueJobDelayBy(time.Minute)`
(`internal/dhtcrawler/persist.go:53`) so `run_after > created_at` and the trigger
does **not** fire for them. A Rust **producer** must keep the trigger intact and
must not rely on NOTIFY for dispatch.

### 1.5 The Phase-4 producer seam (why the fingerprint is load-bearing beyond Phase 3)

The Go crawler's enqueue is `tx.QueueJob.CreateInBatches(queueJobsToPersist, 10)`
with **no `OnConflict` clause** (`internal/dhtcrawler/persist.go:172`): a
duplicate fingerprint **aborts the whole crawl-persist transaction**, so a Rust
producer emitting a byte-divergent fingerprint doesn't merely double-process — it
can **wedge crawl persistence**. (Contrast: the consumer-side republish DOES use
`OnConflict{DoNothing}` — `internal/processor/processor.go:184-186`.) The
fingerprint golden is therefore load-bearing for the Phase-4 crawler port, not
only Phase 3's consumer.

---

## 2. Classifier corpus contract (blocks Lane C)

Oracle: `internal/classifier/**`, `internal/protobuf/**`,
`testdata/parity/classifier/**`. No standalone `CONTRACT.md` existed; this
section is it.

### 2.1 The corpus (the frozen oracle)

`testdata/parity/classifier/{inputs.jsonl, corpus.golden.jsonl}` — **330
fixtures** each, shared schema `{id, subsystem:"classifier", input, expected}`
(`corpus.golden.jsonl` is already in the `bitmagnet-diff` fixture shape).
Generated by `internal/classifier/corpus_test.go` (`TestClassifierCorpus`,
regen with `-update-corpus`).

- **Input** = `classifierInput` (`corpus_test.go:28-51`): `id`, `name`, `size`,
  `filesStatus`, `extension?`, `filesCount?`, `files[]{index,path,extension,
  size}`, `hint?{contentType,contentSource?,contentId?}`.
- **Expected** = `classifierExpected` (`corpus_test.go:53-70`): `contentType`,
  `baseTitle`, `date{year,month,day}?`, `languages[]`, `languageMulti`,
  `episodes` (string form of `model.Episodes`), `videoResolution`, `videoSource`,
  `videoCodec`, `video3d`, `videoModifier`, `releaseGroup`, `contentAttached`,
  **`outcome` ∈ {classified, deleted, unmatched, error}**, `error?`. (Note this
  is a **superset** of the plan's field list — `languageMulti` and
  `videoModifier` are also frozen.)
- Encoding: `SetEscapeHTML(false)`, one JSON object per line
  (`corpus_test.go` `encodeClassifierCorpus`).

### 2.2 Purity precondition (Phase-0 C1)

The golden path is pure **iff the three enrichment flags are OFF** —
`local_search_enabled=false`, `apis_enabled=false`, `tmdb_enabled=false`
(`corpus_test.go` flags block). Enforcement: **all mock expectations left empty**
except `LocalSearch.ContentByID` → `ErrUnmatched`, which stubs the **one
unconditional** attach branch (`attach_local_content_by_id`, reached via the
hinted-content-id `find_match` at `classifier.core.yml:66-78`) so it runs
network-free; any real `ContentBySearch`/TMDB call fails the test. The four
`attach_*` actions and their flag-gating:

| Action | Gating in `classifier.core.yml` | Under flags-off |
|---|---|---|
| `attach_local_content_by_id` | inside a hinted-id `if_else` (`:66-78`); the `find_match` call is unconditional (`:74`) | mocked → `ErrUnmatched` |
| `attach_tmdb_content_by_id` | `flags.apis_enabled && flags.tmdb_enabled` (`:76`) | false → `unmatched` |
| `attach_local_content_by_search` | `flags.local_search_enabled` (`:96`) | false → `unmatched` |
| `attach_tmdb_content_by_search` | `flags.apis_enabled && flags.tmdb_enabled` (`:100`) | false → `unmatched` |

### 2.3 Result / outcome types

- `classification.Result` (`internal/classifier/classification/result.go:5-9`):
  embeds `ContentAttributes`, plus `Content *model.Content` and
  `Tags map[string]struct{}`.
- `ContentAttributes` (`result.go:31-44`): `ContentType`, `BaseTitle`, `Date`,
  `Languages`, `LanguageMulti`, `Episodes`, `VideoResolution`, `VideoSource`,
  `VideoCodec`, `Video3D`, `VideoModifier`, `ReleaseGroup`.
- Outcome sentinels (`classification/errors.go:30-36`):
  `ErrUnmatched = WorkflowError{key:"unmatched"}`,
  `ErrDeleteTorrent = WorkflowError{key:"delete_torrent"}`. `normalizeClassifierResult`
  maps `ErrDeleteTorrent→"deleted"`, `ErrUnmatched→"unmatched"`, any other
  error→`"error"`, nil→`"classified"` (`corpus_test.go`).

### 2.4 CEL engine + action/condition vocabulary (RESOLVED GREEN, spike 2026-07-17)

Port on the **`cel` crate v0.14.0 (cel-rust)** with **serde-bound
`torrent`/`result` objects** (plan §1.2). Custom env (`internal/classifier/cel_env.go`):

- Base vars `torrent : bitmagnet.Torrent`, `result : bitmagnet.Classification`
  (`cel_env.go:22-23`); libs `StdLib` (`:16`), `Lists()` (k8s.lists, `:17`),
  `ext.Strings` (`:20`).
- Dotted namespaces registered as constants + a null-map placeholder for
  type-checking: `flags.*` (`:28-38`), `keywords.*` (`:40-55`), `extensions.*`
  (`:56-70`), `fileType.*` (`:71-90`), `contentType.*` (`:91-110`), plus size
  units `kb/mb/gb` (`:111-122`).
- **Functions core.yml actually calls:** `.sum()` (k8s.lists Go impl,
  `cel_lists.go:143-151,212-253`), `.join()` and `matches()` (`ext.Strings`,
  `cel_env.go:20`). Grep of `classifier.core.yml`: only `.sum()`, `.join()`,
  `matches()` appear. The rest of `k8s.lists` (`isSorted/max/min/indexOf/
  lastIndexOf`) and the wider `ext.Strings` surface are needed **only for user
  `classifier.yml`** and can land incrementally.
- 🚨 **ASCII regex mode.** Go `regexp` `\w`/`\W` are ASCII; Rust `regex` defaults
  to Unicode. Every `matches()`-style keyword regex must compile in **ASCII mode
  (`(?-u)`)**. Note the Go keyword compiler avoids literal `\w` (it uses
  `[\p{L}0-9]` helpers, `internal/regex/util.go:14-20`) and sidesteps case-fold
  with explicit `[Aa]` classes (`internal/keywords/parser.go:201-214`), so
  case-folding is not an extra risk — but the CEL `matches()` calls in core.yml
  are the surface that must be ASCII-pinned.

Action vocabulary (`internal/classifier/features.go:26-48`; each `name()` a const
string): `set_content_type`, `delete`, `find_match`, `if_else`, `run_workflow`,
`add_tag`, `parse_date`, `parse_video_content`, `attach_local_content_by_id`,
`attach_local_content_by_search`, `attach_tmdb_content_by_id`,
`attach_tmdb_content_by_search`, `unmatched` (13 actions). Conditions: `and`,
`not`, `or`, `expression` (`condition_{and,not,or,expression}.go`).

Source load/merge order (`internal/classifier/source_provider.go:11-23`): **core
embed → XDG (`bitmagnet/classifier.yml`) → CWD (`./classifier.yml`) → config
injection**, folded via `.merge()` (`:33-50`). Config injection **force-disables
`tmdb_enabled`** when TMDB is unconfigured (`:134-136`). `classifier.core.yml` is
279 lines and stays **byte-compatible** (it is a public contract).

**TMDB scope (user decision #3): IN.** The live fleet runs with `TMDB_API_KEY`
set, so Lane C ports the TMDB client + the four `attach_*` actions. Under the
flags-off corpus they are inert (§2.2); parity for the real-sample gate (flags
per the fleet) requires them.

### 2.5 Validation harness (wired here)

`bitmagnet-rs/crates/bitmagnet-diff/tests/classifier_pair.rs` loads the 330-fixture
`corpus.golden.jsonl`, asserts the schema (`outcome`+`contentType` keys present),
proves the canonical normalizer round-trips every `expected`, and runs a
`ClassifierDriver` **stub** (errors on every fixture) through the shared harness —
so the plumbing compiles and executes now, and **Lane C only replaces
`ClassifierDriver::run`'s body** and flips the assertion to `report.ok()`. Gate:
**100% on the 330 corpus, ≥0.999 on the real-name replay sample** (§2.6).

### 2.6 Real-name replay corpus (user decision #2) — FROZEN

The 330 synthetic fixtures prove branch coverage, not distributional fidelity
over ~52.8M real torrents (`06 R2`). Decision #2 adds a frozen **119,991-name**
production-sampled replay corpus as the **≥0.999 agreement gate** for the Rust
classifier (Lane R/C), alongside the 100%-synthetic gate. Frozen artifacts under
`testdata/parity/classifier-replay/` (raw sampling TSVs gitignored;
`names.jsonl` + `PROVENANCE.md` preserve regeneration):

- `names.jsonl` — `{"id":N,"name":…}` frozen name list, sha256
  `d333d48c…`; **119,991 unique names** (deduped, sorted by UTF-8 bytes).
- `inputs.jsonl` — classifier harness inputs, sha256 `fb848626…`.
- `oracle.golden.jsonl` — full `ContentAttributes` per name from the **pure
  flags-off Go classifier**, sha256 `34d831e4…`; **119,990 classified / 1
  deleted / 0 errors**. Determinism verified: regenerated in a clean env,
  byte-identical (SHA unchanged).
- Oracle generator: `TestClassifierReplayOracle` in
  `internal/classifier/corpus_replay_test.go` (skips by default; `-update-replay`
  regenerates; subsystem tag `classifier-replay`; strict LocalSearch/tmdb mocks =
  the same flags-off purity assertion as the base corpus, `ContentByID` stubbed
  `.Maybe()` since replay inputs carry no hint).

**🔑 Input-shaping contract (REQUIRED for CEL rules to fire — freeze this).**
Each real name is wrapped in a minimal **single-file** synthetic torrent (the
base `classifierInput` shape, §2.1):

- `filesStatus = "single"` carrying the **real production `size`**.
- `extension` = name-derived via Go's exact single-file extension regex
  `[^/.]\.([a-z0-9]+)$` applied to the **lowercased** name
  (`model.FileExtensionFromPath`, `internal/model/torrent_files.go:33-43`; the
  same value `internal/model/torrents.go:143` derives for a single-file torrent).
  45,907 names (38.3%) yield an extension.

**Why `single` + real size, not `no_info`:** the `no_info` proto path gives the
synthetic file `size = 0`, which **zeroes every size-gated CEL rule** (the
`torrent.files.map(f, f.extension in extensions.X ? f.size : -f.size).sum()`
pattern) and suppresses all content-type classification. `single` carries the
real size into the file so the classifier exercises its real name-parsing paths.
The Rust port's replay harness must reproduce this exact input shaping or the
≥0.999 comparison is meaningless. This is a **name-replay** corpus: content-type
derives from the name + its own trailing extension, not prod file topology
(multi-file torrents are intentionally reshaped as single-file; the gate only
requires Rust == Go over these identical frozen inputs).

---

## 3. Release-parse output shape (blocks Lane R → Lane C)

Oracle: `internal/keywords/parser.go`, `internal/regex/util.go`,
`internal/lexer/lexer.go`, `internal/classifier/parsers/video.go`,
`internal/model/{episodes_parser,video_resolution,video_codec,video_source,
language}.go` + `languages.csv`, `internal/protobuf/**`.

### 3.1 Enum vocabularies (frozen)

- **VideoResolution** (`video_resolution_enum.go:19-27`): `V360p, V480p, V540p,
  V576p, V720p, V1080p, V1440p, V2160p, V4320p`; aliases
  (`video_resolution.go:18-29`): `1080i, 1920x1080, 3840x2160, 2k, 4k, 8k, sd,
  hd, fhd, uhd`.
- **VideoCodec** (`video_codec_enum.go:19-25`): `H264, x264, x265, XviD, DivX,
  MPEG2, MPEG4`; alias `avc→H264` (`video_codec.go:20-22`). (No `h265`/`hevc`
  constant — only `x265`.)
- **VideoSource** (`video_source.go:12`): `CAM, TELESYNC, TELECINE, WORKPRINT,
  DVD, TV, WEBDL, WEBRip, BluRay`; aliases `video_source.go:19-33`.
- **ContentType** (`content_type_enum.go:19-27`): `movie, tv_show, music, ebook,
  comic, audiobook, game, software, xxx`.
- **Languages**: `internal/model/languages.csv` — **63 lines (62 data rows)**,
  header `alpha2,alpha3,name,aliases`; `//go:embed` + `init()` parse
  (`language.go:315-355`); regex vocabulary per language emits `alpha2+"dub"`,
  `alpha3`, lowercased name, and lowercased aliases (`language.go:83-95`).
- **Episodes**: `model.Episodes` built via `AddSeason`/`AddEpisode`;
  `EpisodesMatchToEpisodes`/`ParseEpisodes` (`episodes_parser.go:88-162`) handle
  regular `SNN[ENN]`, `x`-format `NNxNN[-NN]`, ranges, and comma lists.

### 3.2 Output surface (two distinct images — do NOT conflate)

1. **Classifier corpus `expected`** (§2.1) — the flags-off oracle. Captures
   `date` (year/month/day), `video3d`, `videoModifier`, `languageMulti`,
   `episodes` (string). This is Lane C's gate image.
2. **Proto `Classification` message** (`internal/protobuf/bitmagnet.proto:43-70`)
   — what CEL sees as `result`. 13 fields, but the transformer
   (`transformer.go:112-175`) **does NOT populate `year` (field 4) or `video3d`
   (field 10)** (see §0.2). `NewClassification` maps: `contentType`,
   `hasAttachedContent`, `hasBaseTitle`, `languages[]` (via `l.ID()`),
   `episodes[]` (via `SeasonEntries().String()`), `videoResolution`,
   `videoSource`, `videoCodec`, `releaseGroup`, `contentId`, `contentSource`.
   Lane R/C reproduce this omission.

### 3.3 🚨 Alias-precedence determinism trap (the Lane-R fidelity risk)

- `video_source` **is** deterministic: aliases sorted **longest-first with an
  alphabetical tiebreak** (`video_source.go:35-51`, `sort.Slice` at `:41-46`), so
  `web-dl`/`web-rip` precede `web` (fork commit `998ebfc6`).
- `video_resolution` (`video_resolution.go:31-38`) and `video_codec`
  (`video_codec.go:24-28`) **are NOT sorted** — they `for k := range map` and
  append aliases in Go's randomized iteration order. The base enum names come
  first (deterministic slices), but the appended aliases are unordered.
  `video_codec` currently has one alias (`avc`) so the nondeterminism is latent;
  `video_resolution` has ten.

**Frozen requirement:** the Rust port must **enforce longest-first (alpha
tiebreak) alternation order for EVERY alias table** — not just `video_source` —
and the cross-language golden must include **alias-precedence fixtures**
(ambiguous prefixes) so a regression is caught in CI. This plus the corpus-gap
augmentation (CJK/bracketed titles, ambiguous aliases, episode `x`-format/ranges,
multi-language, `releaseGroup`, plus dedicated date/language goldens) is Lane R's
gate beyond the corpus slice.

---

## 4. Summary-write contract (shared substrate; denorm has landed)

The denorm candidate is **already merged** into the base (`00026` present;
base commit `1b57442d` = "keep summary.compressed_bytes in sync with files_data
mutators"), so the post-denorm shape is frozen here:

- Table `torrent_file_summary` (`00021_blob_storage.sql`): `info_hash BYTEA PK`,
  `file_count`, `total_size`, `largest_file_size`, `extensions JSONB`,
  `has_video`, `has_subtitle`, `has_audio`, `created_at`, `updated_at`; **plus
  `compressed_bytes BIGINT` (nullable)** from `00026_summary_compressed_bytes.sql`.
- `BuildFileSummary(infoHash protocol.ID, files []model.TorrentFile,
  compressedBytes int) model.TorrentFileSummary`
  (`internal/blobmigration/serializer.go:117-124`, post-denorm signature):
  counts/sizes/extension-set/`has_*` from `files`; `compressed_bytes =
  octet_length(files_data) = len(zstd blob)` written in the same tx; **NULL =
  "not yet backfilled"** → the Rust read side treats it as a miss and falls back
  to decoding the blob.
- Upsert `ON CONFLICT (info_hash) DO UPDATE` (crawler + backfill).
- **The Phase-3 processor does NOT write the summary** — `persist.go` writes only
  `Content`/`TorrentContent`/`TorrentTag` (§5.1). It may **read**
  `compressed_bytes`. Writing the summary is a Phase-4 crawler concern; freezing
  the contract now de-risks that port and any Rust backfill.

---

## 5. ⚠️ FULL write-shadow strategy (user decision #4) — DESIGN, NEEDS REVIEW

Reads can be shadowed by re-answering and discarding; **writes cannot** — two
consumers persisting the same job double-write the same rows (`06 R4`). The
approved posture (decision #4) is the heavier **sustained dual-consume
comparison**: one persisting consumer per queue (Go), plus a Rust shadow that
materializes its would-be writes, diffs them against what Go actually wrote, and
discards. The concrete mechanism below was **reviewed and approved by the
team-lead (2026-07-17)**; §5.2(B) carries the one approved amendment.

### 5.1 What the Go processor writes (the target write-set)

From `internal/processor/persist.go:27-125` (single tx) and
`internal/processor/processor.go:55-201` (orchestration):

- `Content` — `OnConflict{UpdateAll}`, batches of 100 (`persist.go:66-73`).
- delete stale `TorrentContent` by `deleteIDs` (`:75-81`).
- `TorrentContent` — `OnConflict{UpdateAll}`, batches of 100 (`:83-91`).
- `TorrentTag` — `OnConflict{DoNothing}`, batches of 100 (`:93-101`).
- delete-torrent path (`ErrDeleteTorrent`): `blockingManager.Block(infoHashes)`
  **before** the tx (`:59-63`) then `DELETE torrents` by info-hash **in** the tx
  (`:103-113`).
- **post-commit fire-and-forget Tantivy dual-write** (`:118-124`,
  `indexToSearchSidecar :138-156`): a goroutine with
  `context.WithoutCancel(ctx)` + 30s timeout, **errors logged only, never
  returned**. Known gap replicated for parity: `deleteIDs` (individual
  `torrent_content` removals) are **not** mirrored to the sidecar — only whole
  info-hash deletes (`:134-137`).

Orchestration builds the write-set by fanning out per-torrent classify
(`runner.Run(ctx, workflow, flags, torrent)`, `processor.go:136`), collecting
`persistPayload{torrentContents, deleteIDs, deleteInfoHashes, addTags}`
(`:195-200`); failed hashes are republished with `OnConflict{DoNothing}`
(`:184-186`). The `process_torrent` handler runs at **Concurrency 1, JobTimeout
10min** (`internal/processor/queue/handler.go:42-43`); `process_torrent_batch`
same (`batch/queue/handler.go:154-155`).

### 5.2 The mechanism (proposed)

**(a) Mirroring — how jobs reach the shadow.** A small **mirror-writer** samples
ingest and inserts copies into a **scratch queue name `process_torrent_shadow`**
that **no Go worker consumes** (the Go dispatcher filters `WHERE queue =
'process_torrent'`, §1.4, so scratch rows are invisible to it). Because the
fingerprint is `sha256(queue || payload)` (§1.1) and the queue name differs, a
shadow copy's fingerprint **cannot collide** with the live job's — no dedup
interference. Two options considered:

- **(A) Tee-at-enqueue** — hook the Go producer so every Nth `process_torrent`
  insert also writes a scratch row. Exact same payloads, but touches the Go
  producer hot path.
- **(B) Poll-mirror off archived rows** — a separate sidecar samples from
  **already-`processed` `process_torrent` rows** (`queue_jobs WHERE
  queue='process_torrent' AND status='processed'`) and **copies the original
  payload verbatim** into a scratch row, changing **only** the queue name. Zero
  change to the Go producer; sampling is one external knob.

**Recommend (B) — REVISED per team-lead review (2026-07-17): sample archived rows
and copy the payload verbatim; do NOT reconstruct it.** The processed queue row
**retains the exact original payload** for the 7-day archival window (§1.4 GC
contract), so copying it preserves **classify-config fidelity** — a live job
enqueued with non-default `ClassifyMode`/`ClassifierWorkflow`/`ClassifierFlags`
(reprocess flows, batch runs with explicit flags) is shadowed with those exact
params, not defaults. (Reconstructing payloads from info-hashes — the original
draft — would shadow every job with default config and diff spuriously.) Because
only the queue name changes and the fingerprint is `sha256(queue||payload)`
(§1.1), the copy **re-fingerprints automatically** with no dedup collision. This
also **naturally samples the real job mix** (crawler vs batch vs reprocess) in
its true proportions. Cost: `SELECT` on `queue_jobs` processed rows + `INSERT` of
scratch rows — both within the fail-safe DB role (§5.4). **Critical ordering
property:** sampling *already-processed* rows means the Go processor has, by
definition, already persisted (or deleted) the info-hash, so the shadow diffs
against a **settled** live row, not a race; enqueue the scratch job with a small
delay (`QueueJobDelayBy`, §1.4) as belt-and-suspenders for read-replica lag.

🔑 **Corollary (deleted info-hashes).** Because the shadow replays a payload the
Go processor *already handled*, some info-hashes will have been **deleted** by
Go's delete path (`ErrDeleteTorrent` → `DELETE torrents`, §5.1). The shadow must
treat "no live row for this info-hash" as a **first-class diff outcome
(`live_absent`)**, not an error — and compare it against whether its own pipeline
also produced a delete signal (a real match/mismatch), rather than aborting.

**(b) Shadow consumer (Rust).** A `bitmagnet-processor` in **shadow mode**: it
claims from `process_torrent_shadow` via a `bitmagnet-queue` consumer bound to
that queue name, runs the identical classify+build pipeline to produce the
in-memory `persistPayload`, then — instead of persisting — **reads the current
live rows** for those info-hashes (non-locking `SELECT`s on
`content`/`torrent_contents`/`torrent_tags`), canonically normalizes both its
would-be write-set and the live rows, **diffs**, emits metrics, and **discards**.
The only write it performs is marking its own scratch-queue job `processed`.

**(c) The comparison image.** Canonicalize (à la `bitmagnet-diff`: sorted keys,
stable projection) and compare per info-hash:

- `torrent_content`: `content_type/source/id`, `languages` (sorted), `episodes`
  (canonical), `video_resolution/source/codec/3d/modifier`, `release_group`,
  `size`, `files_count`, and the **`InferID()` value** (`processor.go:151`, a
  pure function of the classification — a mismatch there is real drift).
- `content` rows keyed by `(type,source,id)`: title/release_year/identifiers.
- `torrent_tags`: sorted name set.
- delete signal: whether Go deleted the torrent (`ErrDeleteTorrent`).
- **Excluded (volatile):** `id` surrogate, `created_at`/`updated_at`, `tsv`,
  `seeders`/`leechers`/`published_at` snapshots — time- or source-derived, not
  classification output.

Metrics `bitmagnet_ingest_shadow_*`: match/mismatch counters labeled by
content-type, per-field drift, and first-N mismatch samples for offline triage.

### 5.3 Bounded resource caps (`06 R5`)

Sample rate (start 1–5% of ingest) as the primary throttle; semaphore-capped
shadow concurrency (mirror the Go `Concurrency 1`, or a small fixed N); off-peak
scheduling via the mirror's `run_after`; a **scratch-queue depth cap** (the
mirror stops enqueuing above a backlog threshold so a stalled shadow can't grow
unboundedly); short `archival_duration` on scratch rows (e.g., 1h) so GC reaps
them fast. Deploy posture reuses Phase-2 dark: internal-only ClusterIP, no
ingress, non-root, read-only rootfs, token-automount off, fail-closed inventory
flags.

### 5.4 Fail-open semantics (shadow failure never blocks Go) — the safety core

1. **Physical queue separation.** Scratch rows live under a distinct queue name
   the Go worker never selects, so a shadow backlog cannot starve the live
   `process_torrent` queue.
2. **Separate process.** The mirror-writer and shadow consumer are independent
   Deployments; if either dies, real ingest is untouched (they only add scratch
   rows / read live tables).
3. **No live locks.** The shadow's live-table access is non-locking `SELECT`; it
   holds nothing the Go processor's tx needs. A crash returns its claimed scratch
   job to `pending`/`retry` (shared-queue semantics) — harmless.
4. **DB-permission fail-safe (strongest guarantee).** Run the shadow under a DB
   role **granted `SELECT` only** on `content`/`torrent_contents`/`torrent_tags`/
   `torrents` and write access **only** to `queue_jobs` (for its scratch claims).
   Then even a bug **cannot** mutate live data — "discards" is enforced at the
   permission layer, not just in code. A negative-control test asserts an
   attempted live write is rejected.

### 5.5 The operating rule + cutover (`04 §3.3`, `06 R4`)

**Exactly one persisting consumer per queue name, ever.** The live
`process_torrent` queue keeps its single Go persisting consumer throughout the
soak; Rust persists only into the scratch comparison (its own scratch-queue
status column), never the live tables. **At cutover**, in one change: disable the
Go `process_torrent` worker, enable the Rust processor to consume the **live**
`process_torrent` queue, and tear down the mirror/scratch apparatus. Rollback is
the inverse toggle; the shared `queue_jobs` table means a job the Rust consumer
claimed and did not finish returns to `pending`/`retry` and Go picks it up.

### 5.6 Honest limitation to weigh in review

The live row the shadow reads is a **moving target** — torrents get reprocessed
(crawler re-enqueue, reclassification), so even with the delay in §5.2(a) the
shadow match-rate is a **distribution with expected noise**, not a hard 100%.
Therefore: the **hard correctness gates remain the golden corpus (100%) and the
real-name replay sample (≥0.999)** (§2.5); the dark mirror soak is a
**supplementary live-distribution confidence signal**, not the primary proof.
This matches the plan's `05 P3`/`06 R8` ordering (golden-replay is the backstop).
If review agrees, the soak's exit criterion should be phrased as "match-rate ≥ X
after excluding info-hashes whose live row changed during the compare window",
not a bare 100%.

---

## 6. Coordination note

The real-name replay oracle (user decision #2, §2.6) was produced by a separate
`p3-corpus` agent and folded into this branch as a dedicated commit
(`internal/classifier/corpus_replay_test.go` + `testdata/parity/classifier-replay/`;
raw sampling TSVs gitignored). Note `oracle.golden.jsonl` is 52.78 MB — over
GitHub's 50 MB soft-warning threshold (under the 100 MB hard limit); it pushes
with a warning. If the fork later wants it off the main history, Git LFS is the
migration path, but the maintainer directed a plain commit.
