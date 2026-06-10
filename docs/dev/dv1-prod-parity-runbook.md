# DV-1 / V1 — production `file_extensions` ext-parity confirmation runbook

**Date:** 2026-06-10 · **Owner:** `dv1-parity` (team `bitmagnet-deploy`) · **Status:** DESIGN — commands written, **NOT executed**. Every prod run is **explicitly user-gated** (see §7).

**Goal.** Prove, on **LIVE prod (FSN1) current data**, that the deployed `torrents.file_extensions` JSONB array exactly equals the extension-set derivable from `torrent_files` for every multi-file torrent. This is the **precondition** for flipping the DROP-gate criteria (the flag-gated swap of the multi-file `EXISTS torrent_files … extension IN (…)` → `file_extensions @> '["x"]'`, per [`fba1-jsonb-dropgate-results.md`](./fba1-jsonb-dropgate-results.md)).

**Relationship to FB-A1.** FB-A1 proved parity on the **bench restore** — a 2026-06-05 *pre-backfill* snapshot whose `file_extensions` was **populated synthetically** from `torrent_files`. That proves the *backend* (the `@>` filter returns the same set as the EXISTS) but it is **circular for the column itself**: it can't prove that prod's *real, dual-written* `file_extensions` matches `torrent_files`, because in the bench it was *derived from* `torrent_files`. **V1 closes that gap on prod's actual live column.**

---

## 0. TL;DR — recommendation

**Use Method (a): a GENTLE, READ-ONLY, PURE-SQL live-prod checker** — a rate-limited keyset scan that, per multi-file torrent, compares the stored `file_extensions` JSONB against `array_agg(DISTINCT torrent_files.extension)`. No blob decode, no new binary, no image rebuild, no write path, never touches the blob self-heal. Run a fast **Tier-1 random sample** (minutes) for an early signal, then a **Tier-2 full keyset pass** (off-peak, resumable, ~2.5–5 h) for certainty before the flip.

**Reject Method (b)** (fresh post-backfill dump → bench restore → FB-A1-style check) as the *primary* path: it requires a NEW multi-hour MVCC `pg_dump` (itself a heavy prod read over ~400 GB), a multi-hour HEL1 restore, and still only proves a **stale snapshot**, not live data — the exact thing V1 exists to improve over FB-A1. Keep (b) only as an optional zero-prod-**write**-risk fallback if the user vetoes any prod read load (§6).

---

## 1. What "parity" means here (the exact invariant)

Two PostgreSQL-side derivations of the same underlying file paths:

| side | how it is produced | code anchor |
|---|---|---|
| **`torrents.file_extensions`** (JSONB) | `ExtractUniqueExtensions(files)` → for each file `FileExtensionFromPath(path)` (Go regex on `lower(path)`), **skip empties, dedupe, sort** → JSON array | `serializer.go:74`; dual-written in the crawler upsert next to `files_data` (`persist.go:121,228`) and recomputed by the backfill (`queue/handler.go:172,208`) |
| **`torrent_files`-derived set** | `SELECT DISTINCT extension FROM torrent_files WHERE info_hash=? AND extension IS NOT NULL`, sorted. `torrent_files.extension` is the **generated column** `substring(lower(path) from '[^/.]\.([a-z0-9]+)$')` | `00001_init.sql:68` |

**The two regexes are byte-identical.** Go: `regexp.MustCompile(`[^/.]\.([a-z0-9]+)$`)` applied to `strings.ToLower(path)`, returning capture group 1 (`torrent_files.go:33-42`). PG: the same POSIX pattern via `substring(lower(path) from …)`, returning the same capture group. Both lower first, both capture `[a-z0-9]+`, both yield NULL/empty on no-match. ⟹ **parity is structural**; V1 is a *build-correctness confirmation* (expect **100 % exact**), and any mismatch is a concrete bug or a known edge (§5), not an accepted product loss.

**Scope of comparison:** **multi-file torrents only** — those with `files_data IS NOT NULL` (equivalently `files_status IN (multi, over_threshold)`). Single-file torrents carry their extension on `torrents.extension` and are served by the **unchanged single-file OR-branch** of the criteria; they have no `torrent_files` rows and `file_extensions = '[]'`, so they are **excluded by design** and require no parity check.

---

## 2. The invariant, as one SQL predicate

A torrent **matches** (is in parity) iff its sorted JSONB array equals its sorted `torrent_files`-derived array:

```sql
-- canonical per-torrent comparison (mismatch = TRUE means a parity violation)
(
  SELECT array_agg(e ORDER BY e)
  FROM jsonb_array_elements_text(t.file_extensions) AS e
)
IS DISTINCT FROM
COALESCE(
  (SELECT array_agg(DISTINCT tf.extension ORDER BY tf.extension)
   FROM torrent_files tf
   WHERE tf.info_hash = t.info_hash AND tf.extension IS NOT NULL),
  ARRAY[]::text[]
)
```

`IS DISTINCT FROM` is NULL-safe (handles the both-empty case: `array_agg` over zero rows is NULL on the JSONB side and `COALESCE`→`'{}'` on the TF side; an all-no-extension torrent yields `[]` JSONB → NULL array → must compare equal to `'{}'`, so we also `COALESCE` the JSONB side in the batch query below). The JSONB array is already sorted+unique at write time, but we re-sort defensively so the comparison is order-independent.

---

## 3. Method (a) — gentle live-prod checker (RECOMMENDED)

### 3.1 Why pure SQL (and why NOT extend the Go/Rust tooling)

The task brief asks to prefer reusing the deployed **blob consistency checker** (`internal/blobmigration/consistency`) or the **fork Rust verify tooling**. I inspected both:

- **Go blob `LiveChecker`** (`consistency/live_checker.go`, `checker.go`) compares **blob vs `torrent_files`** on **index/path/size only** — it does **not** compare `file_extensions` (the G1 proposal to add `extension` checking was never merged; current `CompareFiles` has no extension branch). Worse, on **any** mismatch it **self-heals by `UPDATE torrents SET files_data = NULL`** (`healTorrent`, `live_checker.go:103-114`) to trigger blob re-migration. Extending it to also assert `file_extensions` would (1) require a code change + FSN1 image rebuild + redeploy, (2) decode every blob (CPU), and (3) risk entangling ext-parity drift with the **destructive blob-heal path** — explicitly forbidden by the L2-P0 spec (§2 #5: "Agg-parity drift must ride a separate counter/flag, never the blob-heal path"). **Rejected.**
- **Rust verify tooling** — the planned `bitmagnet-parquet verify` subcommand (L2-P0-4/-5 readers) **was deleted from the plan by FB-A1** (it targeted `agg_torrent_ext`, which is no longer built). The only Rust binary on the deploy branch is `bitmagnet-search/src/bin/backfill.rs` (Tantivy indexer) — not a parity tool. **Nothing to reuse.**

Pure SQL is therefore the minimal, zero-deploy, **read-only** expression of the invariant. It honours the same precedent the brief cites — the blob checker already samples live prod continuously — **without** its code, its blob decode, or its heal path. It is strictly the cheapest correct option.

### 3.2 Sizing (FSN1 PG = 12 CPU / 24 Gi, continuously written by dhtcrawler+processor)

- **Population:** ~**17.0 M** multi-file torrents with a blob (FB-A1: 16,959,775; live probe: 16,992,238).
- **`torrent_files`:** ~**856.8 M** rows / **277 GB** (live empirical) — note the bench *dump* has 879.5 M (pre-dedup snapshot). Avg **51.79** files/torrent (p50 6 / p90 54 / p99 743 / max 88,561 — heavy skew).
- **Work profile:** the per-torrent subquery is a PK-ordered index range scan on `torrent_files (info_hash, index)`. A full pass touches ~857 M index tuples ≈ **one logical pass of the `torrent_files` PK index**. It is **read-only**; it writes nothing and takes no heavy locks.
- **Cache reality:** 24 Gi RAM ≪ 277 GB → mostly cold, disk-bound, but PK-ordered so largely sequential on the index. The blob backfill already did exactly this read pattern.
- **Pacing target:** single connection, `max_parallel_workers_per_gather = 0` (avoid the FB-A1 §6 `/dev/shm`=1 Gi parallel-bitmap hazard — though our query uses **no** GIN `@>`, so the risk is already nil), modest `work_mem`, batches of 1–2 k torrents, optional `pg_sleep` between batches. At a deliberately gentle **~1,000–3,000 torrents/s** the full pass is **~1.6–4.7 h** (17 M ÷ rate). Run **off-peak**, watch `pg_stat_activity` + node load, abort instantly if crawler write latency degrades.

### 3.3 Tier-1 — fast random-sample pre-check (minutes; high-confidence early signal)

Because drift would be **systematic** (a regex divergence or a stale class affects a *pattern* of torrents, not one), a bounded random sample catches it with near-certainty long before the full pass. This is the quick "is anything obviously wrong?" gate.

```bash
# GATE 1 required before running (see §7). Read-only. ~1–3 min.
ssh ansible@<FSN1_PROD_IP> 'sudo k3s kubectl exec -n bitmagnet bitmagnet-postgres-0 -c postgres -- \
  psql -U postgres -d bitmagnet -v ON_ERROR_STOP=1 -P pager=off -At <<'"'"'SQL'"'"'
SET max_parallel_workers_per_gather = 0;
SET work_mem = '"'"'64MB'"'"';
SET statement_timeout = '"'"'20min'"'"';
WITH sample AS (
    -- TABLESAMPLE SYSTEM is O(pages), not O(rows): ~0.6% of torrents, blob-only.
    SELECT info_hash, file_extensions
    FROM torrents TABLESAMPLE SYSTEM (0.6)
    WHERE files_data IS NOT NULL
)
SELECT
  count(*)                                  AS sampled,
  count(*) FILTER (WHERE mismatch)          AS mismatches
FROM (
  SELECT
    COALESCE(
      (SELECT array_agg(e ORDER BY e) FROM jsonb_array_elements_text(s.file_extensions) e),
      ARRAY[]::text[]
    )
    IS DISTINCT FROM
    COALESCE(
      (SELECT array_agg(DISTINCT tf.extension ORDER BY tf.extension)
       FROM torrent_files tf
       WHERE tf.info_hash = s.info_hash AND tf.extension IS NOT NULL),
      ARRAY[]::text[]
    ) AS mismatch
  FROM sample s
) q;
SQL'
```

- **Expected:** `mismatches = 0`. A non-zero count → **stop, do not proceed to Tier-2 or the flip**; dump examples (§3.5) and triage via §5.
- Re-run 2–3× (TABLESAMPLE re-seeds each run) to widen coverage cheaply. ~0.6 % ≈ 100 k torrents/run.

### 3.4 Tier-2 — full keyset pass (off-peak, resumable, paced; CERTAINTY)

Driver script kept **on FSN1's shell** (loops `psql` over keyset windows so each statement is small and the cursor is durable). Writes only mismatches to a local file. **GATE 2 required.**

```bash
# GATE 2 required. Read-only. Off-peak. ~1.6–4.7 h. Resumable via $CURSOR.
# Run inside: ssh ansible@<FSN1_PROD_IP>  (then the loop execs psql in the PG pod)
BATCH=2000              # torrents per window
SLEEP=0.2               # seconds between windows (raise to throttle further)
CURSOR=''               # hex info_hash; set to last printed cursor to resume
OUT=/tmp/dv1_mismatches.tsv
: > "$OUT"

while : ; do
  RES=$(sudo k3s kubectl exec -n bitmagnet bitmagnet-postgres-0 -c postgres -- \
    psql -U postgres -d bitmagnet -v ON_ERROR_STOP=1 -P pager=off -At \
    -v cur="${CURSOR}" -v lim="${BATCH}" <<'SQL'
SET max_parallel_workers_per_gather = 0;
SET work_mem = '64MB';
SET statement_timeout = '5min';
WITH batch AS (
    SELECT info_hash, file_extensions
    FROM torrents
    WHERE files_data IS NOT NULL
      AND (:'cur' = '' OR info_hash > decode(:'cur','hex'))
    ORDER BY info_hash
    LIMIT :lim
),
cmp AS (
    SELECT
      b.info_hash,
      COALESCE((SELECT array_agg(e ORDER BY e)
                FROM jsonb_array_elements_text(b.file_extensions) e),
               ARRAY[]::text[]) AS jsonb_exts,
      COALESCE((SELECT array_agg(DISTINCT tf.extension ORDER BY tf.extension)
                FROM torrent_files tf
                WHERE tf.info_hash = b.info_hash AND tf.extension IS NOT NULL),
               ARRAY[]::text[]) AS tf_exts
    FROM batch b
)
SELECT
  -- line 1: control row (rows scanned, max cursor)
  'CTRL'||E'\t'||count(*)||E'\t'||COALESCE(encode(max(info_hash),'hex'),'')
  FROM cmp
UNION ALL
SELECT
  encode(info_hash,'hex')||E'\t'||
  array_to_string(jsonb_exts,',')||E'\t'||
  array_to_string(tf_exts,',')
FROM cmp
WHERE jsonb_exts IS DISTINCT FROM tf_exts;
SQL
  )
  CTRL=$(printf '%s\n' "$RES" | grep '^CTRL' | head -1)
  SCANNED=$(printf '%s' "$CTRL" | cut -f2)
  NEXT=$(printf '%s' "$CTRL" | cut -f3)
  printf '%s\n' "$RES" | grep -v '^CTRL' | grep . >> "$OUT" || true
  echo "scanned=${SCANNED} cursor=${NEXT} mismatches_so_far=$(wc -l < "$OUT")"
  [ -z "$NEXT" ] && break               # no rows left → done
  [ "$SCANNED" -lt "$BATCH" ] && { CURSOR="$NEXT"; break; }   # last partial window
  CURSOR="$NEXT"
  sleep "$SLEEP"
done
echo "DONE. total mismatches=$(wc -l < "$OUT"). cursor=${CURSOR}"
```

- **Pass criterion:** `total mismatches = 0` over the full population → **parity proven on live prod; the flip's parity precondition is satisfied.**
- **Resumable:** if interrupted (or you must stop for a load spike), restart with `CURSOR=<last printed cursor>`.
- **Throttle live:** raise `SLEEP` or lower `BATCH` mid-run; or pause entirely and resume off-peak.
- The keyset (`info_hash > decode(:cur,'hex') ORDER BY info_hash LIMIT`) rides the `torrents` PK — O(batch) per window, no OFFSET scan.

### 3.5 Inspect any mismatch

```bash
# Show the first few mismatches with their decoded blob ext-set vs torrent_files, for triage.
head -20 /tmp/dv1_mismatches.tsv   # columns: info_hash_hex  jsonb_exts(csv)  tf_exts(csv)
# Per-hash deep look (substitute HEX):
ssh ansible@<FSN1_PROD_IP> "sudo k3s kubectl exec -n bitmagnet bitmagnet-postgres-0 -c postgres -- \
  psql -U postgres -d bitmagnet -At -c \"
    SELECT files_status, file_extensions FROM torrents WHERE info_hash = decode('HEX','hex');
    SELECT index, right(path,40) AS path_tail, extension
      FROM torrent_files WHERE info_hash = decode('HEX','hex') ORDER BY index;\""
```

The `(jsonb_exts, tf_exts)` columns make the class obvious at a glance (which side is the superset, whether it's a single odd extension = regex drift, or a whole-set zero = stale).

---

## 4. Method (b) — fresh dump → bench restore → FB-A1 check (FALLBACK ONLY)

**When to use:** only if the user **vetoes any prod read load** (Tier-2 is a heavy read). Otherwise it is strictly worse for V1.

Procedure: `make bitmagnet-backup-now` (multi-hour MVCC `pg_dump -Fc`, ~100 GB+, **reads ~400 GB off prod** — itself a non-trivial prod load) → ship to HEL1 → `make bitmagnet-bench-pg-setup` + `bitmagnet-bench-restore DUMP=<new>` (the current bench holds the *pre-backfill* dump whose `file_extensions` is empty, so a **NEW post-backfill dump is required**) → run the §2 invariant over the restore (zero further prod risk).

**Costs / caveats:**
- The dump step is a **multi-hour heavy prod read** — it does **not** avoid prod load, it just front-loads it into one big scan instead of a paced one. Method (a)'s paced pass is *gentler*, not heavier.
- Proves a **snapshot**, not **live** data — V1's entire reason for existing over FB-A1 is to validate *current* data. (b) reintroduces the staleness V1 set out to remove.
- ~35 GB+ transfer + multi-hour restore on HEL1, plus teardown (`make bitmagnet-bench-pg-teardown`).

**Net:** (b) is the zero-prod-**write**-risk option but it is slower, costs *more* prod read I/O up front, and answers a weaker question. Recommend (a); offer (b) only on explicit user preference for "nothing paced against live PG."

---

## 5. Mismatch semantics + remediation

Every mismatch falls into one of these classes. **Expected count of all of them on current prod: 0** (backfill complete; both sides share one regex; §8 of L2-P0 settled the cap to structurally-zero).

| # | class | what the `(jsonb_exts, tf_exts)` pair looks like | root cause | real? | remediation |
|---|---|---|---|---|---|
| 1 | **stale / un-backfilled** | `jsonb=[]`, `tf=non-empty` | a torrent written before the dual-write whose `file_extensions` was never populated AND that the backfill missed. Backfill is reported COMPLETE → **expected empty**. | only if backfill incomplete | targeted re-backfill over the affected `info_hash` window (`bitmagnet blob-migration start` is idempotent/resumable; or a one-off `UPDATE … SET file_extensions = <derived>`). Does **not** self-heal (see §5.1). |
| 2 | **G1 path-derivation drift** | same length, one differing member; or a systematic pattern (uppercase, unicode, multi-dot, digit-only ext) | Go `FileExtensionFromPath` diverged from the PG generated column on some path. Regexes are **byte-identical** → expected zero. | a genuine **bug** | fix the Go port to match `substring(lower(path) from '[^/.]\.([a-z0-9]+)$')` exactly, redeploy, re-backfill affected rows. |
| 3 | **crawler @cap vs uncapped backfill** | — | L2-P0 §8: **structurally zero** — `file_extensions`, `files_data`, and `torrent_files` rows are all built from the **same** (identically capped or identically full) `files` slice at every write site (crawler, backfill, importer). | **not a real class** | none — if it ever appears, it's actually class 4. |
| 4 | **re-crawl-after-backfill skew** (edge) | `tf` ⊋ `jsonb` (TF superset), only on `over_threshold` torrents | `torrent_files` inserts are `ON CONFLICT DO NOTHING` (`torrent_files.go:11`) while `file_extensions` is `DO UPDATE`. A re-crawl that re-derives a *capped/changed* set updates `file_extensions` but leaves old `torrent_files` rows. Extremely rare. | rare, benign-ish | re-backfill recomputes `file_extensions` from current `torrent_files` (DO UPDATE). Since `torrent_files` is being **dropped**, `file_extensions` becomes source-of-truth anyway; we just need equality *now* to prove no behaviour change at flip. |
| 5 | **importer bypass** | `jsonb=[]`, `tf=[]` | importer writes `FilesStatusNoInfo` — **no files, no blob, no rows** (`importer.go`). Both sides empty (and `files_data IS NULL` → excluded by our filter). The "importer bypass" caveat (FIND-1) concerns the **tsvector**, not `file_extensions`. | **not a mismatch** | none. |
| 6 | **single-file** | n/a (excluded) | `files_status = single`: ext on `torrents.extension`, served by the unchanged OR-branch; `files_data IS NULL` → excluded. | **not a mismatch** | none — single-file branch is untouched by the gate. |

### 5.1 Does the live checker self-heal `file_extensions`? — **No (verified in code).**

The blob `LiveChecker.healTorrent` (`live_checker.go:103-114`) only `UPDATE torrents SET files_data = NULL`. In steady state (backfill cursor at end) NULLing `files_data` does **not** re-enqueue the torrent — the backfill draws from `torrent_files` via `queryDistinctInfoHashes`, not from `files_data IS NULL` — so `file_extensions` is **not** automatically recomputed. ⟹ remediation for classes 1/2/4 is an **explicit re-backfill** (or targeted `UPDATE`), not a passive heal. This also confirms our checker (pure SQL, read-only) **cannot** trip the destructive heal path — satisfying the L2-P0 §2 #5 safety requirement by construction.

---

## 6. Prod read-load safety (why Tier-2 is acceptable)

- **Read-only**: no writes, no row locks, no `VACUUM` interaction, no schema change.
- **No GIN `@>`**: the parity query uses the `torrent_files` PK and the `file_extensions` column directly — it **never** executes the broad `jsonb_path_ops @>` parallel bitmap scan that exhausted the 1 Gi `/dev/shm` in FB-A1 §6. `max_parallel_workers_per_gather = 0` removes even the theoretical risk.
- **Paced + abortable**: single connection, small windows, `SLEEP` throttle, resumable cursor. Monitor with the §8 queries; abort on any crawler write-latency regression.
- **Bounded statement**: `statement_timeout = 5min` per window caps any pathological torrent (e.g. the 88,561-file outlier).

---

## 7. User-gate points (explicit confirmation required)

Per the homelab `CLAUDE.md` ("NEVER make changes to real servers… server-safety; Ansible-first") **and** because Tier-2 is a heavy read against the live DB:

- **GATE 0 — design sign-off.** User approves this runbook + Method (a). *(this deliverable)*
- **GATE 1 — Tier-1 sample run.** User approves running the §3.3 read-only sample against prod FSN1. Output: `mismatches` count.
- **GATE 2 — Tier-2 full pass.** User approves the §3.4 paced full scan **and the off-peak window**. Output: `total mismatches`.
- **GATE 3 — remediation (only if mismatches > 0).** User approves any re-backfill / `UPDATE` (these are *writes* → Ansible-first; use `bitmagnet blob-migration start`, not ad-hoc SQL, where possible).
- **GATE 4 — the flip & the DROP (out of scope for V1; tracked elsewhere).** Flipping the criteria flag and the eventual `make bitmagnet-blob-migration-cleanup CONFIRM=1` are **separate** gated steps owned by DV-4 / the cutover plan; V1 only *clears the parity precondition*.

No SQL in this runbook mutates state. All prod interaction is `ssh ansible@<FSN1_PROD_IP> "sudo k3s kubectl exec … psql"` (read-only), matching the CLAUDE.md "SSH kubectl is acceptable for verification" rule. Any remediation is a separate, Ansible-first, separately-gated action.

---

## 8. Appendix — connection + monitoring

**Prod PG (FSN1):** StatefulSet `bitmagnet-postgres`, pod **`bitmagnet-postgres-0`**, ns `bitmagnet`, container `postgres`, DB `bitmagnet`, user `postgres`, PG16, 12 CPU / 24 Gi, `shm_size = 1Gi`. Local `psql` in-pod authenticates over the unix socket as superuser (no password needed). Reach it read-only via:

```bash
ssh ansible@<FSN1_PROD_IP> "sudo k3s kubectl exec -n bitmagnet bitmagnet-postgres-0 -c postgres -- \
  psql -U postgres -d bitmagnet -At -c '<read-only query>'"
```

**Live-load watch (run in a second shell during Tier-2):**
```bash
ssh ansible@<FSN1_PROD_IP> "sudo k3s kubectl exec -n bitmagnet bitmagnet-postgres-0 -c postgres -- \
  psql -U postgres -d bitmagnet -At -c \"
    SELECT state, count(*), max(now()-query_start) AS max_age
    FROM pg_stat_activity WHERE datname='bitmagnet' GROUP BY state;\""
# node load:
ssh ansible@<FSN1_PROD_IP> "uptime; free -g | head -2"
```

**Population sanity (cheap, run once before Tier-1):**
```sql
SELECT count(*) AS multifile_with_blob FROM torrents WHERE files_data IS NOT NULL;
-- expect ~17.0 M; this is the Tier-2 denominator.
```

**Anchors verified for this runbook** (fork @ deploy branch): `serializer.go:74` (`ExtractUniqueExtensions`), `persist.go:121,228` (crawler dual-write), `queue/handler.go:172,208` (backfill recompute), `torrent_files.go:33` (Go regex), `00001_init.sql:68` (PG generated column), `00021_blob_storage.sql:5` (`file_extensions` JSONB DDL), `consistency/live_checker.go:103` (blob heal = `files_data=NULL`, no ext handling), `fba1-jsonb-dropgate-results.md` (bench parity + `/dev/shm` caveat).

---

## 8. RESULTS — Tier-1 (RAN 2026-06-10, user-gated)

Three independent Tier-1 runs (fresh `TABLESAMPLE` seed each) against **live prod** (`bitmagnet-postgres-0`), read-only, serial, `statement_timeout=8min`; each completed in ~1–2 min with no observable impact:

| run | sampled (blob torrents) | mismatches |
|---|---|---|
| 1 | 101,867 | **0** |
| 2 | 102,231 | **0** |
| 3 | 103,077 | **0** |

**≈307 k torrents (~1.8 % of the ~17 M blob population) sampled, zero mismatches.** Since any drift would be systematic (regex divergence / a stale class), this is a strong PASS of the Tier-1 gate. **Tier-2 (the full paced keyset pass, §3.4) remains open** — required for certainty before flipping `SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB` in prod, per GATE 2.
