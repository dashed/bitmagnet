# G1 — Blob `e` (extension) canonicalization: write-path fix + e-backfill (DESIGN)

Status: DESIGN for LEAD review (read-only phase, no code changes yet).
Lineage read: deployed app = bookmark `feat/l3-pathsearch-v13`; all file:line refs below are `git show feat/l3-pathsearch-v13:<path>`.

## Problem (recap)

The per-torrent blob `files_data = zstd(msgpack([]compactFile{i,p,e,s}))` stores `e`
(extension) **empty for crawl-path torrents**. Documented at
`internal/blobmigration/consistency/checker.go:95-98`. The DROP of `torrent_files`
makes the blob the source of truth, so `e` must become correct/complete and must
**stop accumulating empty** on new crawls. This is data canonicalization + a DROP
gate, **not** a live-search bug (every live consumer derives ext from the path).

---

## 1. Exact write site(s)

`compactFile` is constructed in **exactly one production location**:
`internal/blobmigration/serializer.go:28-44` (`SerializeFiles`), line 34:

```go
compact[i] = compactFile{
    Index:     int(f.Index),
    Path:      f.Path,
    Extension: f.Extension.String,   // <-- writes whatever the input carried (empty for crawl)
    Size:      f.Size,
}
```

(`git grep 'compactFile{'` → only `serializer.go:31` and `serializer_test.go:273`.)

Two production callers feed it:

| Caller                     | file:line                                                                 | Input `f.Extension`?                                                                   | Resulting `e`              |
| -------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------- |
| **Live crawl dual-write**  | `internal/dhtcrawler/persist.go:226` (`SerializeFiles(files)`)            | **NOT set** — `persist.go:217-221` builds `TorrentFile{InfoHash,Index,Path,Size}` only | **EMPTY** ← the bug source |
| **blobmigration backfill** | `internal/blobmigration/queue/handler.go:351` (`SerializeFiles(t.files)`) | **SET** — loader `handler.go:267` `SELECT tf.extension` (the generated column)         | already correct            |

**Conclusion:** empty `e` is written **only by the live crawl** path; the original
backfill already wrote correct `e` from `torrent_files.extension`. New crawls since
the backfill have been re-introducing empty `e`. The fix point is **inside
`SerializeFiles`** — one site covers both callers.

Note `ExtractUniqueExtensions` directly below (`serializer.go:79`) **already** does
the right thing (`model.FileExtensionFromPath(f.Path)`); `SerializeFiles` is the lone
inconsistency.

---

## 2. The fix

Self-canonicalize in `SerializeFiles` — ignore the caller's `f.Extension`, always
derive from the path:

```go
Extension: model.FileExtensionFromPath(f.Path).String,
```

(`FileExtensionFromPath` returns `NullString`; `.String` is `""` when invalid, which
is the correct empty-`e` for extensionless files.)

Why in `SerializeFiles` (not the callers):

- Single site → covers crawl + backfill, can never be missed by a future caller.
- **No-op for the backfill caller**: `torrent_files.extension` is the generated column
  `substring(lower(path) from '[^/.]\.([a-z0-9]+)$')` (`migrations/00001_init.sql:70`),
  which is byte-identical to `FileExtensionFromPath(path)` — so re-deriving yields the
  same value it already passed in.
- Makes the serializer robust to any caller passing wrong/stale/empty `e`.
- Matches `ExtractUniqueExtensions` immediately below.

### Go ↔ PG ↔ Rust parity (the correctness backbone)

All three derivations are the same regex on the lowercased path, capture group 1
(extension without the dot):

- **PG generated column** `migrations/00001_init.sql:70`:
  `substring(lower(path) from '[^/.]\.([a-z0-9]+)$')`
- **Go** `internal/model/torrent_files.go:33-42`:
  `regexp [^/.]\.([a-z0-9]+)$` over `strings.ToLower(path)`.
- **Rust** `bitmagnet-rs/crates/bitmagnet-model/src/enums.rs:293-309`
  (`file_extension_from_path`): `rfind('.')` → char before dot must be non-`/` non-`.`
  → suffix must be non-empty all-`[a-z0-9]` → lowercased.

**Parity finding: equivalent, no blocker.** The Go regex `$`-anchors the extension to
the final `[a-z0-9]+` run, and `[a-z0-9]+` cannot cross a `.`, so the operative dot is
always the _last_ dot — exactly Rust's `rfind('.')`. I hand-checked the divergence-prone
cases and all agree: `a/b/c.tar.gz`→`gz`, `a..gz`→∅, `file.mkv.`→∅, `file.mkv.@`→∅,
`a.b/c`→∅, `.gitignore`/`dir/.hidden`→∅, `disc/track.001`→`001`, uppercase→lowercased,
`音楽/曲.flac`→`flac`.

The only theoretical gap is **exotic-Unicode lowercasing**: Go `strings.ToLower`
(simple 1:1 case-fold) vs Rust `to_lowercase` (full Unicode, can expand 1→N) vs PG
`lower()` (locale-dependent). This **cannot affect the result class** for the
extension run, because the extension must be ASCII `[a-z0-9]` after folding — any
non-ASCII there is rejected by all three. The char-before-dot check is `==/` or `==.`
only (ASCII), also fold-invariant. ⇒ **negligible, not a blocker.**

**Fold-in (dv4 open item — "Rust fixture-parity test"):** the shared fixtures
`testdata/file-extension-fixtures.json` are consumed by Go
(`internal/blobmigration/file_extension_fixtures_test.go`) but **not yet by Rust**
(Rust has only 3 inline cases at `enums.rs:407-415`; its other parity tests are
`blob_fixture.rs` and `tokenizer_parity.rs`). Add a Rust test that loads the **same**
JSON and asserts `file_extension_from_path(path) == expected_extension` for every case
(incl. the dotfile/trailing-dot/numeric/CJK cases). Lock parity permanently in CI.

---

## 3. Deploy implication — crawler roll, v2-neutral

The blob dual-write runs in **bitmagnet-0 (crawler/processor)**, image `go-flags-2`,
deliberately untouched. The write-fix **does** require rolling bitmagnet-0 (without it,
new crawls keep writing empty `e` → re-break the DROP gate after any backfill).

**Surgical patch design:**

- The change is the **single line** at `serializer.go:34` (`Extension:` value). It
  touches no v2 code, no schema, no migration, no Go API. `serializer.go` is shared
  infra and the `compactFile` build is identical across lineages, so the same
  one-liner applies on the `go-flags-2` source commit.
- Build a **`go-flags-2 + G1`** image: take the exact commit `go-flags-2` was built
  from, apply only this line, rebuild, roll bitmagnet-0.
- **MUST NOT** switch bitmagnet-0 to a `gate7-*` / `l3-pathsearch-*` image — those
  activate the unproven **v2-infohash WRITE path**. The patch is **v2-neutral**:
  `persist.go` already routes v1/v2 files through `info.UpvertedFiles()`
  (`persist.go:194`) _before_ serialization; the fix is downstream of that and only
  changes how `e` is computed from an already-resolved path. No behavioral change to
  v1 or v2 ingestion, magnets, or dedup.

Confirm at build time: `git diff go-flags-2..go-flags-2+G1` is exactly the one
`serializer.go` line (plus optionally the new Rust parity test, which the crawler
image doesn't ship).

---

## 4. Backfill of existing blobs — e-ONLY, reuse blobmigration infra

**Recommendation: e-only in-place rewrite of `files_data`.** Read/modify/write only the
~16 GB `files_data` column; do **not** touch the 276 GB `torrent_files`. This is
`torrent_files`-independent ⇒ DROP-order-independent, and reuses the proven
range-worker + KV-checkpoint machinery. Full-rebuild-from-`torrent_files` is rejected
(276 GB, wasteful, and re-couples to the table we're about to DROP).

### Mechanism — the round-trip identity

With the §2 fix in place, the correction for one blob is literally:

```
new_blob = SerializeFiles(DeserializeFiles(old_blob))
```

`DeserializeFiles` (`serializer.go:46`) rebuilds `[]TorrentFile{Path,...}`; the **fixed**
`SerializeFiles` re-derives `e` from each `Path`. Same code as the write fix ⇒ one
implementation, no second derivation to keep in sync. zstd at a fixed level + msgpack
struct-map encoding are deterministic, so the output is byte-stable.

### Delivery — new `blob-migration` subcommand

Add `bitmagnet blob-migration backfill-ext` to
`internal/app/cmd/blobmigrationcmd/command.go:50-58` (alongside
`start/status/pause/resume/verify/cleanup`). Reuse the existing **parallel
info_hash-range workers + KV cursors** pattern (`startCmd`/`seedRanges`, KV keys
`blob_migration:cursor:*` etc.). Differences from `start`:

- **Scan source:** `torrents WHERE files_data IS NOT NULL` ordered by `info_hash`
  (NOT `torrent_files`). Ranges over the `torrents` PK.
- **Per row:** decode → re-serialize (round-trip above) → `UPDATE torrents SET
files_data = ? WHERE info_hash = ?`. Does **not** touch `torrent_file_summary` or
  `file_extensions` (those are already path-derived and correct).
- **Per-batch COMMIT**, batch size tuned like the original backfill.

### Idempotency / resumability

- **Skip-write when already correct:** if `new_blob == old_blob` bytes, skip the
  `UPDATE` (no WAL, no churn). Already-backfilled and extensionless-only blobs are
  thus free on re-run.
- **Resumable** via the per-range KV cursor (same as `start --resume`); a re-run picks
  up where it stopped and the skip-write makes re-processing a completed range cheap.
- Naturally idempotent: the round-trip is a fixed-point once `e` is canonical.

### Safety (from the original backfill's hard lessons)

- **K=16 concurrency, NOT K=32** (K=32 crashed PG: liveness probe tripped under WAL
  load → unrecoverable WAL-recovery loop). Cap workers at 16.
- PG **startupProbe** (not liveness) so WAL recovery can't be killed; `max_wal_size`
  raised (the GUCs from `make bitmagnet-pg-optimize`).
- Per-batch COMMIT; bounded chunk rows for memory.
- This workload is **lighter** than the original: no zstd-of-fresh-data CPU storm, no
  `torrent_file_summary` INSERTs, no GIN index churn — just decode+re-encode+single
  column UPDATE, and skip-write elides the no-op majority. Expect well under the
  original's load envelope. References: homelab `make bitmagnet-backfill-parallel`,
  `docs/bitmagnet/bitmagnet-backfill-bottlenecks.md`.

---

## 5. Correctness invariant + verification gate

**Invariant:** for every file in every blob,
`blob.e == torrent_files.extension == FileExtensionFromPath(path)` (all three equal by
construction after the fix).

**Verify by extending the consistency checker.** `CompareFiles`
(`checker.go:104-115`) currently compares the **path-derived** ext on _both_ sides
(`bExt := FileExtensionFromPath(b.Path)` vs `rExt := FileExtensionFromPath(r.Path)`) and
**deliberately ignores the raw stored `e`** (so it won't flag legit empty-`e` crawl
blobs today). Add a **strict-`e` mode** that _additionally_ asserts the **raw stored
field**:

```go
// strict-e: prove the backfill populated e canonically
if b.Extension.String != bExt.String {   // raw stored e vs path-derived
    // mismatch: blob still carries non-canonical e
}
```

Wire it as a `--strict-e` flag on `verifyCmd`. **0-mismatch gate** across the full
corpus (≈16.99 M torrents-with-blob) = G1 done. Also assert no NEW empty-`e` appears
after the crawler roll (run strict-`e` verify on a recent-crawl sample post-roll).

Belt-and-suspenders: the existing path-derived check already catches blob round-trip
path corruption; strict-`e` adds the "stored `e` is canonical" assertion.

---

## 6. Consumer impact — ZERO live impact

Every **live** consumer already derives the extension from the path, never the raw `e`:

- **Live L2/L3 (parquet/DuckDB sidecar = `bitmagnet-parquet`)** — `decode.rs:85-94`,
  `rows_from_files` sets `extension: file_extension_from_path(&f.path)`; the module doc
  (`decode.rs:6-11`) is explicit: "derived from the file PATH … **never** the [blob `e`]".
  All downstream (`fact.rs`, `rollup.rs`, `export.rs`, `verify.rs`) consume that
  path-derived `FileRow.extension`.
- **Go consistency checker** — path-derived both sides (`checker.go:104`).
- **Go blob consumers** — `ExtractUniqueExtensions` / `BuildFileSummary`
  (`serializer.go:79,113`) derive from path.

⇒ The empty-`e` has **no effect on anything serving today**, which is why this is a
canonicalization/DROP-gate task. The fix + backfill are safe to land without touching
read paths.

**One latent (non-live) exception to flag:** the **Tantivy** `bitmagnet-search` crate
`transform.rs:62-65` reads the **raw** `f.extension` for its `file_extensions` facet
(`filter(|f| !f.extension.is_empty()).map(|f| f.extension.clone())`). This is the
**superseded Phase-3 Tantivy plan** (replaced by the `bitmagnet-parquet`/DuckDB sidecar)
and is **not in the live serving lineage**, so no live impact today. The G1 write-fix +
backfill make this code correct automatically (e becomes canonical); if Tantivy were
ever revived it should also be switched to `file_extension_from_path(&f.path)` for
defense-in-depth. **Flagged, not blocking.**

---

## 7. Ordering vs the DROP

- The e-backfill reads/writes **only `files_data`** and never reads `torrent_files`, so
  it is **independent of and not blocked by** the DROP. It can run before, after, or
  interleaved with DROP prep.
- **How G1 advances the DROP-track:** the DROP makes the blob the source of truth.
  Today the blob's `e` is non-canonical (empty for crawl) — acceptable only because all
  consumers re-derive from path. G1 closes the gate by (a) **stopping new empty-`e`**
  (crawler roll) and (b) **canonicalizing the existing corpus** (e-backfill), so that
  post-DROP the blob is _self-sufficient_ (e == path-derived everywhere) and the
  strict-`e` 0-mismatch verify proves it. Sequencing: **roll crawler first** (stop the
  bleed), **then backfill** (heal the corpus), **then strict-`e` verify** (gate), and
  only then is the `e` precondition for DROP satisfied.

---

## Open risks / blockers

1. **None blocking.** Go↔Rust↔PG parity holds; the only residual is exotic-Unicode
   folding, which is result-class-invariant for ASCII extensions. Mitigate by folding
   in the dv4 Rust fixtures-parity test.
2. **Crawler roll discipline:** the build must be `go-flags-2 + the one serializer line`
   only — verify the diff; do not pick up a `gate7-*` image (would activate the v2
   write path).
3. **Backfill load:** honor K=16 + startupProbe + `max_wal_size`; skip-write keeps it
   light, but watch PG under the column-rewrite UPDATE volume (HOT-update unlikely since
   `files_data` is large/TOASTed — expect dead tuples; schedule autovacuum / consider
   running in waves).
4. **`transform.rs` raw-`e` reader** is latent (non-live) — fix opportunistically if
   Tantivy is revived; not on the G1 critical path.

## Suggested task split (for #2 implement)

- (a) `serializer.go:34` one-line write-fix.
- (b) Rust fixtures-parity test loading `testdata/file-extension-fixtures.json` (dv4).
- (c) `blob-migration backfill-ext` subcommand (range workers + KV cursor + round-trip
  - skip-write).
- (d) checker `--strict-e` mode + `verifyCmd` flag.
- (e) (optional) `transform.rs` switch to path-derived (defense-in-depth, non-live).
