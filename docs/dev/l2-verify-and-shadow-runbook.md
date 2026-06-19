# L2 verify + v2-shadow — the DROP-gate proof runbook

**Date:** 2026-06-12 · **Status:** L2 gate tooling built, GATE A passed, frozen GATE C accepted, and l2-11 proven live over a production window. The DROP remains deferred until the remaining replacement layers, especially L3 pathsearch, are deployed and proven.
**Parent:** [`dv2-l2-build-notes.md`](./dv2-l2-build-notes.md) (§5 stubs 2/5 + §6 are closed by this) · [`L2-P0-agg-torrent-ext-and-checker-spec.md`](./L2-P0-agg-torrent-ext-and-checker-spec.md) §7 (adapted — see below) · sequencing rule: [`torrent-files-replacement-options.md`](./torrent-files-replacement-options.md) §"The hard rule".

The `torrent_files` DROP needs a **proof**, not a vibe. This doc is the map of that proof: what each tool checks, how the pieces compose, and the exact gate criteria.

---

## 1. The proof, composed

```
(A) blob ⟺ torrent_files            bitmagnet-parquet verify        (one-time direct, pre-DROP)
(B) generation = f(blob)            same G1 decode code + duck e2e   (by construction + tested)
(C) sidecar(generation) ⟺ torrent_files SQL    v2-shadow            (the end-to-end DROP gate)
(D) blob ⟺ torrent_files, forever   Go consistency LiveChecker      (already live in prod)
```

- **(A) `verify`** proves the post-DROP per-file source (the blob) agrees with the
  retiring table at the per-`(torrent, extension)` aggregate grain — including G1
  (the extension is path-derived on BOTH sides: the PG generated column and the
  blob decode). L2-P0 §8 settled that divergence is structurally zero, so **any**
  mismatch is a bug, never an accepted loss.
- **(C) `v2-shadow`** proves the whole serving stack — export → Parquet generation
  → DuckDB SQL → gRPC — returns exactly what the equivalent `torrent_files` SQL
  returns, shape by shape. This is the gate that catches writer/reader/SQL bugs
  (A) cannot see.
- (B) and (D) close the loop continuously: the generation is built from the blob
  by the same code `verify` exercises, and the live checker keeps blob ⟺
  `torrent_files` true while both exist.

**Adaptation vs the L2-P0 spec:** the spec's Job A compared a PG `agg_torrent_ext`
TABLE against `torrent_files`. That table was **superseded** for the DROP gate by
the (already flipped + verified) `file_extensions` JSONB. `verify` therefore
recomputes the expected aggregate **from the blob** — which is strictly better:
it tests the actual post-DROP data path. The spec's settled decisions (all-Rust,
readers in `bitmagnet-db`, pure compare fn, no Go request-path shadow) carry over.

---

## 2. `bitmagnet-parquet verify` (Job A)

```bash
# smoke (default): 100k torrents from the start of the keyset
bitmagnet-parquet verify --dsn "$DSN"

# the gate run: full corpus, supervised, read-only on both sides
bitmagnet-parquet verify --dsn "$DSN" --mode full --batch-size 1000

# resume after an interruption (cursor = last hex printed in the progress log)
bitmagnet-parquet verify --dsn "$DSN" --mode full --after <info_hash_hex>
```

Per torrent: decode `files_data` → `ext -> max(size)` (G1, empties skipped) vs
`torrent_files … WHERE extension IS NOT NULL GROUP BY info_hash, extension`
(one batched `= ANY($1::bytea[])` read per page). Exit ≠ 0 on **any** mismatch
or blob decode error; summary line mirrors the export jobs:

```
verify: torrents_checked=… exact=… mismatched=0 decode_errors=0 clean=true
```

**GATE A: `clean=true` over `--mode full` (~16.99 M with-blob torrents).**
Read-only; pace with `--batch-size` if prod PG matters (the per-page cost is one
keyset page + one PK-driven aggregate per 1 000 torrents).

## 3. `v2-shadow` (the C gate)

```bash
# on the SAME snapshot: a restore + a generation exported from it
bitmagnet-parquet base --dsn "$RESTORE_DSN" --root /scratch/gen --fail-on-decode-error   # V3, also GATE A input
bitmagnet-filesearch --root /scratch/gen --addr 127.0.0.1:50052 &
v2-shadow --pg "$RESTORE_DSN" --sidecar http://127.0.0.1:50052 --csv v2.csv [--repeat N]
```

Five shapes, each compared **exactly** (`find` ordered rows · `collapse` ordered
groups · two count grains · the extension facet as a map). The built-in suite
covers every shape AND every sidecar routing class (rollup-exact / rollup-set +
hydration / fact-only); `--pairs pairs.json` extends it. CSV per pair:
`run,shape,label,filter,pg_n,sidecar_n,equal,pg_ms,sidecar_ms,detail`.

Mirror rules that make the comparison honest (in `bitmagnet-shadow/src/pg.rs`):
path ordering is `COLLATE "C"` (DuckDB orders by UTF-8 code point — a locale
collation would produce a different, correct-looking order); `info_hash` is
compared as lowercase hex (order-isomorphic to the bytea); `"index"`/`sum()` are
cast to `bigint` in SQL (sqlx exact-OID decode); the ILIKE escape map is
byte-identical to the sidecar's. **Keep `path_query` pairs ASCII** — non-ASCII
ILIKE case-folding legitimately differs between PG and DuckDB.

**GATE C: `mismatches=0` across the suite on the same-snapshot run** (and
sidecar latency within the CB envelope — the CSV carries both timings). Against
live prod the run is indicative only: `torrent_files` moves while the generation
is as fresh as its last delta.

### GATE C result (2026-06-11): 12/13 exact + 1 EXPLAINED superset → accepted

Run on a truly frozen snapshot (crawler scaled to 0; compact export 06:07–06:25Z;
suite immediately after). **12 of 13 pairs exactly equal** — every `find` (incl.
path-sort under `COLLATE "C"`), every `collapse` (all three routing classes),
every count, and the 5 086-bucket size facet. The 13th (`facet:video`) differed
by **+10 avi files on the sidecar** — root-caused, corpus-quantified, and the
LEGACY side is the wrong one:

- `torrent_files`' **primary key is `(info_hash, path)`** (live schema:
  `torrent_files_pkey ON (info_hash, path)`), and the crawler persists file rows
  with `OnConflict{DoNothing}` — so **duplicate-path files (BEP-47 `.pad/N`
  padding, identical path at many indexes) are silently dropped**. The blob
  keeps the faithful list (matches `torrents.files_count`).
- Corpus-wide (frozen): **18 torrents, +18 726 blob-side files (0.002 % of
  882.8 M)**, 99.9 % NULL-extension `.pad` entries; one torrent holds 17 229 of
  them. Affected torrents span 2025-01 → today: long-standing upstream
  behavior, not a dual-write regression.
- GATE A was structurally blind to it (per-(torrent, ext) presence+max survives
  dropped duplicates) — the count-grain facet pair is what caught it.

> **2026-06-11 follow-up:** padding is now retained-but-filtered — see
> [`l2-padding-files.md`](./l2-padding-files.md) (`is_padding` fact column,
> padding-free rollups, default `NOT is_padding`, proto `include_padding`,
> comparator mirror). The remaining strict-equality residue is the ~18 real
> dup-path files below.

**Disposition: the sidecar is a strict SUPERSET of `torrent_files` — files the
legacy PK cannot represent. No capability regresses at the DROP; the gate's
intent is met.** Optional strict mode (if exact equality is ever preferred):
dedup `(info_hash, path)` at blob decode, making L2 bug-compatible with the
legacy table. Also caught and fixed live by this gate run, on the serving side:
the DuckDB spill dir (relative `.tmp` vs cwd `/` → `workingDir` = the PVC) and
an OOMKill (engine `memory_limit` bounds the buffer pool, not the process —
now 3 GB engine / 8 Gi container).

**Latency note:** ~~unsorted v1 base: finds 11–18 s vs the 10 s deadline~~ —
the external sort SHIPPED and ran in prod (2026-06-11): finds now 0.75–3.4 s,
counts <1 s. Trade-off + the collapse regression it exposed (info_hash
locality; the l2-10 batch-probe fix) and the l2-11 watchdog fix: see
[`l2-sorted-layout-results.md`](./l2-sorted-layout-results.md).

### l2-11 live prod-window result (2026-06-12)

This is not a replacement for same-snapshot GATE C; it proves the deployed
l2-11 service behaves correctly while prod is moving.

Window: **2026-06-12T01:18:38Z -> 01:32:51Z**.

- Readiness stayed stable: Deployment `1/1` Available, pod
  `bitmagnet-filesearch-d57454d78-m88w2` Ready/Running, **0** restarts,
  endpoint `10.42.2.33:50052`, no warning events.
- Deadline behavior stayed hard: direct `collapse:path` (`S01E01`, limit 50)
  returned gRPC `DeadlineExceeded` / `query exceeded deadline` in
  **10.36-10.37 s**; HealthCheck stayed `SERVING_STATUS_SERVING`.
- Freshness stayed healthy: refresh jobs every minute, **4-5 s** duration,
  `decode_errors=0`, `clean=true`, sidecar reloads every minute around
  `:15.727Z`, deltas advanced `v1781227141 -> v1781227921`, final
  `delta_mark=2026-06-12T01:31:31Z`, final `delta_age_seconds=50-54`.
- Live structured `v2-shadow`, excluding path-query shapes, was **9/11 exact**.
  The accepted residues were the known `facet:video` `avi +10` dup-path
  superset and moving-prod freshness drift (`facet:>1g` changed from `mkv -3`
  to `mp4 -2` after the next delta).
- Path-query shadow is intentionally deadline-guarded until L3 candidate routing
  exists: `find:path 1080p` took PG **144 s** and the sidecar returned the 10 s
  deadline.

## 4. The deletion audit (closes delta stub #5)

`deleted_torrents` (bytea PK + `deleted_at`, upsert) is fed by an
`AFTER DELETE` trigger on `torrents`; DDL ships as the homelab playbook
`bitmagnet_deleted_audit.yml` (`make bitmagnet-deleted-audit`, idempotent —
deliberately NOT a goose migration: the deployed Go image is digest-pinned and
`00023` is contested by the v2 branch; a future adopting migration is a no-op
over the `IF NOT EXISTS` DDL). The delta job consumes it over the SAME
half-open lagged window as the change carve:

```bash
bitmagnet-parquet delta --dsn "$DSN" --deleted-source audit
```

(`--deleted-source file --deleted-file <f>` and `none` remain for manual runs.)
Rows older than the last full base rebuild are prunable after compaction.

### Freshness loop — LIVE (2026-06-11) + the cumulative-delta contract

The DDL is applied (trigger functionally verified with a synthetic
insert+delete riding a real tick's tombstones) and the minute cadence runs:
CronJob `*/1` → `delta --deleted-source=audit` → atomic swap → the `l2-7`+
sidecar **self-reloads** (`BITMAGNET_FILESEARCH_RELOAD_SECS`, default 30 s).
Freshness SLA ≈ cadence + reload + the 30 s carve lag ≈ ≤2 min.

🚨 **The cumulative-delta contract (a live catch, fixed in `l2-8`):** delta
ticks must NEVER advance the carve origin. The first implementation advanced
the watermark per tick, so each tick carved a 1-minute sliver that REPLACED
the whole delta — un-hiding stale base rows for everything earlier ticks had
carved (observed live: tick 2's 15 torrents evicted tick 1's 1,303). Correct
semantics (EXP-B): `watermark` = the BASE's cut, written only by
base/compaction; every tick re-carves the whole `(watermark, now − 30 s]`
window (changes + audit deletes) and idempotently replaces `delta/current`;
`delta_mark` (new file) carries freshness for `HealthCheck.delta_age_seconds`.
Tick cost grows with the window until a compaction folds it into a new base —
schedule compacts accordingly (~18.5 min each; the delta stays trivially small
for days at current crawl rates). Verified live: consecutive ticks grow
monotonically (39→56→70; after origin heal 1,589→1,617), origin pinned. The
2026-06-12 l2-11 window confirmed the same contract under serving load:
`delta v1781227141 -> v1781227921`, final `delta_age_seconds=50-54`, and recent
refresh logs stayed `decode_errors=0 clean=true`.

## 5. Fixed alongside (found BY this work): DuckEngine rollup routing

Writing the comparator forced the question "what exactly does each path
return?" — and the inspection caught a real serving bug in `l2-1`:
`DuckEngine::collapse`/`facet_ext`/`count` routed through the `agg_torrent_ext`
rollup **unconditionally**, but the rollup has no `path` column (a collapsed
search with a path filter silently DROPPED the filter) and only per-(torrent,
ext) `max_size` for size (`size_max` ⇒ wrong set membership; `size_min` ⇒
inflated counts). The fix (`sql::rollup_plan` + fact-path builders + per-group
exact hydration) keeps the <50 ms rollup wins where they are exact and routes
the rest to the fact CTE; the `InMemoryEngine` reference semantics + new unit
and duck-e2e tests pin it. **Deploy `l2-3`+ only (see the supersession note below).** This is precisely the class of bug GATE C exists to catch; the
shadow suite exercises all three routing classes.

**Second catch (the FSN1 e2e run itself, 2026-06-10):** the duckdb crate's bare
`bundled` feature compiles libduckdb **without the parquet extension** —
`read_parquet` is not in the catalog at runtime, and the FB-B1d lockdown
(`autoinstall/autoload_known_extensions=false`) rightly blocks loading it, so
EVERY sidecar query fails. All three duck-e2e tests (including the two
pre-existing ones, whose "tested" status was never actually exercised on a real
toolchain) failed with exactly this. Fix: `duckdb = { features = ["bundled",
"parquet"] }` — the extension is statically linked, the lockdown stays. With it,
all 33 filesearch tests pass on real DuckDB on the FSN1 builder.

**Third catch (the first GATE A smoke run, live):** `torrents.files_count` is
`int4`; the base/delta stream readers decoded it as `i64` without a `::bigint`
cast — sqlx 0.9 errors on the OID mismatch, so `verify` (and the base export,
same reader) crashed on the first page. These readers had never run against a
real database (the for-index reader casts everywhere, which is why the Tantivy
backfill never hit it). Fixed + SQL-shape-test-guarded.

**Image supersession:** `l2-1` = routing bug · `l2-2` = routing fixed but NO
parquet extension (cannot serve at all) · `l2-3` = sidecar fine but the
`bitmagnet-parquet` CLI crashes on the int4 read · `l2-4` = correct but
pre-padding-filter · `l2-5` = padding filter with a 2-convention classifier
(misses libtorrent's 7.9 M rows) · **`l2-6`+ = good (use only this).**

### FINAL GATE C cycle (2026-06-11, l2-6, frozen snapshot): 12/13 + the known residue

Export `compact: torrents_ok=48,200,070 decode_errors=0 file_rows=882,894,529
padding_rows=33,040,027 clean=true` (~18.5 min). Suite: **12/13 exactly equal
with padding invisible by default on BOTH sides** (incl. `find:nullext`, now
genuinely pad-free). The single mismatch is precisely the documented dup-path
residue: `facet:video` avi `9,263,039 (pg) vs 9,263,049 (sidecar)` = the +10
real dup-path avi files the legacy `(info_hash, path)` PK cannot store. The
harness reports it as a mismatch BY DESIGN (it is a true data difference);
the disposition is the accepted, bounded, fully-explained superset. Ops note: the GHCR package is PRIVATE (anonymous pull 401s) — the
homelab role wires an imagePullSecret from the vaulted PAT; flipping the
package public in the GitHub UI would make that unnecessary.

**GATE A status: ✅ PASSED (2026-06-11).** The full run swept the ENTIRE
`torrents` table — **48,195,834 checked, 48,195,834 exact, mismatched=0,
decode_errors=0, clean=true** — in 3 h 25 m (~3.9 k torrents/s, single
Job on HEL1, read-only, crawler live throughout). That is ~2.8× the ~17 M
with-blob estimate: the keyset walks every torrent, so the run also proved
every no-blob torrent has no stray `torrent_files` rows (the backfill's
"0 with-files torrents left" claim re-proven). Job
`bitmagnet-filesearch-verify`, 2026-06-10T21:12:11Z → 2026-06-11T00:36:54Z.

## 6. Order of operations (per the hard rule)

1. ✅ DONE — image `l2-3` built on FSN1 from the duck-e2e-green tree (33/33
   tests on real DuckDB there; the macOS toolchain can't compile bundled
   libduckdb).
2. ✅ DONE — serving role deployed to HEL1 (PVC Bound, Service, CNPs; pod
   NotReady-by-design pending the first export).
3. ✅ DONE — **GATE A PASSED** (48,195,834/48,195,834 exact, 0 mismatches, 0 decode errors; 2026-06-11).
4. ✅ DONE — supervised compact export (V3: `decode_errors=0`) published the
   first serving generation and the sidecar became Ready.
5. ✅ DONE — **GATE C** (`v2-shadow`) on a frozen snapshot: 12/13 exact plus the
   accepted, bounded dup-path superset.
6. ✅ DONE — deletion audit + minute delta CronJob live; l2-11 prod-window proof
   confirmed freshness, readiness, deadline behavior, and live shadow residue.
7. Next replacement-layer work: deploy/prove L3 pathsearch for fast path
   candidates, then revisit the DROP plan. The DROP stays deferred until then.
