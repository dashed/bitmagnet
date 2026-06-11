# L2 verify + v2-shadow — the DROP-gate proof runbook

**Date:** 2026-06-10 · **Status:** tooling BUILT (this commit); the gate runs are pending.
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

* **(A) `verify`** proves the post-DROP per-file source (the blob) agrees with the
  retiring table at the per-`(torrent, extension)` aggregate grain — including G1
  (the extension is path-derived on BOTH sides: the PG generated column and the
  blob decode). L2-P0 §8 settled that divergence is structurally zero, so **any**
  mismatch is a bug, never an accepted loss.
* **(C) `v2-shadow`** proves the whole serving stack — export → Parquet generation
  → DuckDB SQL → gRPC — returns exactly what the equivalent `torrent_files` SQL
  returns, shape by shape. This is the gate that catches writer/reader/SQL bugs
  (A) cannot see.
* (B) and (D) close the loop continuously: the generation is built from the blob
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
`bitmagnet-parquet` CLI crashes on the int4 read · **`l2-4`+ = good (use only
this).** Ops note: the GHCR package is PRIVATE (anonymous pull 401s) — the
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
4. Supervised base export (V3: `decode_errors=0`) → sidecar Ready.
5. **GATE C** (`v2-shadow`) on the HEL1 restore snapshot; optionally an
   indicative run against prod + delta freshness.
6. `make bitmagnet-deleted-audit` + flip the delta CronJob on → freshness SLA.
7. Only after A+C pass and the layers run proven-in-prod does the DROP
   conversation start. The DROP stays deferred indefinitely until then.
