# File Search — Path to COMPLETE `torrent_files` Parity

**Date:** 2026-06-07
**Status:** Design + plan (read-only analysis). No code changed by this document.
**Question:** Can the file-search _baseline_ be improved to reach **complete** functional parity with the dropped `torrent_files` table — and what exactly does it take?
**Method:** 6-agent opus team (`matrix` / `datares` / `distinct` / `compose` + adversarial `audit` + lead synthesis), source-verified against the fork, Tantivy v0.26, PostgreSQL 16.
**Inputs / supersedes-detail-in:** [`file-grained-search-spec.md`](./file-grained-search-spec.md), [`perfile-search-with-blob-design.md`](./perfile-search-with-blob-design.md), [`perfile-search-innovative-design.md`](./perfile-search-innovative-design.md).

---

## 0. Verdict

**Complete _functional_ parity is achievable — as a composition, after two prerequisite fixes — but not as a single low-disk store.**

- **As specified today: NOT-YET.** Two HIGH regressions block it (G1: the file index trusts an extension value that is empty for crawl-path torrents; G2: the per-torrent file browser was never re-pointed off `torrent_files`). Both are cheap, non-architectural code fixes.
- **After G1 + G2: ACHIEVED-WITH-DOCUMENTED-EXCEPTIONS.** Every functional capability of `torrent_files` is restored, interactively (<50 ms) and exactly, except four explicitly-documented exceptions (compound-non-denorm predicates, per-file timestamp drift, the distinct-torrent selectivity cap, search-vs-browse lag) and one near-zero-signal semantic loss (independent per-file timestamps) + a non-functional integrity downgrade.
- **Unqualified "complete parity" is unreachable** below "keep `torrent_files`" (+273 GB) or the slim PG per-file table (+68–92 GB re-bloat). This is the §13.4 curve, re-confirmed.

**Cost of parity:** ~**12–21 GB** marginal (file index 8–15 + aggregate 3–5 + bucket vector ~1; collector/DuckDB/blob/hydration = 0) vs the **273 GB** table eliminated. The price is _more moving parts_ (4 stores + 3 code-only paths) and a deliberate latency split — not lost capability.

---

## 1. Why one store can't do it (the irreducible truth)

`torrent_files` fused **five workloads** into one 273 GB table: per-file search · per-torrent listing/browse · distinct-torrent collapse · fleet analytics · arbitrary joins. The per-file `(extension, size)` conjunction is irreducible (Tantivy has no nested docs; PG has no index-organized table → 10.5 GB of btree overhead alone for 873 M file rows). The only single-store full-parity replacement is the slim PG per-file table, which re-bloats 68–92 GB — exactly what the blob migration removed. **Parity is therefore recovered by _decomposing_ the workload across purpose-fit stores, each cheap because it carries only what it must.**

---

## 2. The two HIGH regressions (must fix for parity)

### G1 — Blob per-file `extension` is empty for crawl-path torrents [HIGH, correctness prerequisite]

- **Root cause:** `dhtcrawler/persist.go:211-216` builds `TorrentFile{InfoHash, Index, Path, Size}` with **no `Extension`** — `torrent_files.extension` is a PG **generated** column (`migrations/00001_init.sql:70`), computed by the DB, never set in Go. `blobmigration/serializer.go:34` then serializes `f.Extension.String == ""` → blob `e=""` → `DeserializeFiles` yields `Extension.Valid=false`.
- **Blast radius (one root cause, five victims):** (1) file-index `extension` filter silently misses **every crawl-path torrent**; (2) GraphQL `TorrentFile.extension` is NULL in the browser; (3) `ORDER BY extension` wrong; (4) DuckDB-on-blobs `GROUP BY extension` empty (can't even distinguish a real no-ext file from the bug); (5) the per-(torrent,ext) aggregate, if keyed off blob `e`, drops live torrents into a NULL bucket and **under-returns** distinct torrents.
- **Why it hides:** the **migration** path (`queue/handler.go:150-167`) reads the generated column, so **backfilled** blobs carry extension — only **crawl-path** blobs are empty → a live-vs-backfill split. The consistency checker (`consistency/checker.go:63-93`) compares only index/path/size, **never extension**, so it never flags this. _Parity tests run on backfilled data would silently pass._
- **Fix (0 GB, code only):** re-derive extension from path via `model.FileExtensionFromPath(path)` at **every** build/read site — Go `BuildFileDocuments`, Rust `backfill_files`, blob-read/hydration, and the aggregate's ext key. **Never trust blob `e`.** This is exact parity, not a workaround: `FileExtensionFromPath`'s regex `[^/.]\.([a-z0-9]+)$` on `lower(path)` is **byte-identical to the `torrent_files.extension` generated column**, and `ExtractUniqueExtensions`/`FileExtensions()` already derive this way. (`fileType` already derives from path; `extension` is the lone outlier.)
- **Spec correction:** `file-grained-search-spec.md` §5.1/D8 ("extension comes from the stored blob value, not re-derived from path") is **wrong on both counts** and must say: derive from path.

### G2 — The per-torrent file browser was never re-pointed at the blob [HIGH]

- **Root cause:** `resolvers/query.resolvers.go:109` → `gqlmodel/torrent_files.go:25` → `search.TorrentFiles` → `query.GenericQuery` over `TableNameTorrentFile` = raw SQL **`FROM torrent_files`**. The webui file browser (`torrents/torrent-files.datasource.ts`) drives it with server-side `limit/page/offset`, `orderBy ∈ {index,path,extension,size}` asc/desc, `totalCount`, `hasNextPage`, and a multi-`infoHash` cross-torrent form.
- **The miss:** the file-search spec adds a **different** surface (`torrentContent.fileSearch`, returning torrent-grained `matchedFiles`); it does **not** reimplement `TorrentQuery.files`. §13 _asserts_ "file listing via the blob" but no task concretely re-points this resolver. **Post-`DROP TABLE torrent_files`, the file browser errors.**
- **Fix (0 GB, code only):** reimplement `TorrentQuery.files` over `torrent.FilesData` — `AfterFind` already hydrates `t.Files` (and path-sorts); apply `orderBy`/pagination/`totalCount`/`hasNextPage` **in memory**, replicate PG NULL ordering for the `extension` sort (NULLS LAST on asc), and handle the multi-`infoHash` merge. Depends on G1 (extension correctness) and the timestamp/index-sort hydration below.

---

## 3. The complete-parity composition

| Component                                              | Restores                                                                                                                                           | Latency          | Marginal disk        |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- | -------------------- |
| **Blob hydration** (`AfterFind`, retained)             | list a torrent's files; ORDER BY path/size; per-torrent paginate + totalCount; display-value hydration                                             | in-mem           | 0 (paid)             |
| **G1 extension-from-path** (code)                      | per-file `extension` value + filter + sort — **correct for the live corpus**                                                                       | <50 ms           | 0                    |
| **G2 file-browser-over-blob** (code)                   | `TorrentQuery.files` listing/sort/paginate post-cutover                                                                                            | <50 ms           | 0                    |
| **Timestamp hydration** (code)                         | per-file `created_at`/`updated_at` ← **`torrent.created_at`** (stable)                                                                             | <50 ms           | 0                    |
| **Index re-sort** (code)                               | `ORDER BY index` (resolver re-sorts `.Index`; AfterFind defaults to path-sort)                                                                     | <50 ms           | 0                    |
| **File-grained Tantivy index v1**                      | per-file `(ext∧size)` filter + exact file-level count/sort/facet; single-file synth (**exceeds** incumbent)                                        | <50 ms           | 8–15 GB              |
| **Per-(torrent,ext) aggregate** `{max,min,count}` (PG) | **exact** distinct-torrent count + keyset deep paging, one-sided size + ext-only (the literal incumbent collapse); join-friendly                   | <50 ms           | 3–5 GB               |
| **Bucket-size vector** on the aggregate (~16 log₂)     | two-sided distinct-torrent ranges: interior exact+sublinear, ≤2 boundary buckets → bounded blob-refine                                             | <50 ms           | ~1 GB                |
| **`DistinctTorrentCollector`** (code, gated)           | path-FTS collapse + any predicate ≤ cap: exact deep paging; over cap → guarded **exact-but-slow** scan (the cell `torrent_files` also seq-scanned) | <50 ms under cap | 0                    |
| **DuckDB-on-blobs**                                    | exact ad-hoc analytics / arbitrary SQL / joins                                                                                                     | 1–10 s           | 0                    |
| **File index v1.1** (opt-in)                           | per-file **path FTS** (**exceeds** incumbent — table had none)                                                                                     | <50 ms           | +8–18 GB (uncertain) |

**Total v1-for-parity ≈ 12–21 GB** (+v1.1 path FTS → ~20–39 GB). All ≪ 273 GB; under the 200 Gi PVC.

---

## 4. Documented exceptions (after G1+G2, this is the residue)

- **E1 — Compound predicate crossing per-file `(ext,size)` AND a _non-denormalized_ torrent attribute** (resolution/tag/title~/seeders>): no exact _interactive_ single-store answer (the file index denorms only `content_type`+`published_at`; the old SQL JOIN did it). Exact via DuckDB-on-blobs + PG scan at 1–10 s. **The denorm list is the boundary of interactive compound parity** — state it. (Widen the denorm set later only on demand.)
- **E2 — Per-file timestamp drift:** hydrate both `created_at` and `updated_at` from **`torrent.created_at`** (stable), not `torrent.updated_at` (which upserts to last crawl). Per-file timestamps are display-only (no timestamp sort/filter exists); truly independent per-file timestamps existed only via the raise-threshold re-crawl append path — no real signal. The one literal column value the composition can't independently reproduce; a pre-existing blob-migration gap.
- **E3 — Distinct-torrent exactness has a selectivity cap:** file-level `total_hits` exact; collapse exact for one-sided via the aggregate and for two-sided/path-FTS via the gated collector up to the cap; above the cap, exact-but-slow scan (default) or opt-in `totalCountIsEstimate=true`. Mirrors the incumbent (PG also seq-scanned broad).
- **E4 — Browse is read-your-write; file _search_ lags ingest** (post-commit fire-and-forget index write, `processor/persist.go:150`). Standard search posture; browser is unaffected (blob is a synchronous `torrents` column).
- **E5 (non-functional) — integrity downgrade:** `torrent_files` had `PK(info_hash,path)` + `UNIQUE(info_hash,index)`; the composition enforces uniqueness by **builder correctness** (D4 per-torrent replace), not a DB constraint. Not a query-capability loss.
- **E6 (parity-neutral) — over_threshold files:** absent from blob/index/DuckDB — but `torrent_files` was truncated there too (`createTorrentModel` truncates the slice; `files_count` holds the true total in both). Documented, not a regression. (Upstream default `save_files_threshold` = 100.)

**Parity-or-better confirmations (audit):** sort determinism (blob full-sort+slice is _more_ page-stable than the tiebreaker-less SQL `ORDER BY size`); Torznab (already `len(Files)`, blob-hydrated, torrent-grained); tsvector rebuild (AfterFind sorts blob and rows identically before `fileSearchStrings`); read-after-write (v1 info_hash is content-addressed → file list immutable per hash → replace ≡ append-once-frozen).

---

## 5. Phased rollout

**v1 — MANDATORY for complete functional parity**

1. **G1** extension-from-path correctness fix (0 GB) — _prerequisite_; without it the live corpus is invisible to the ext filter and NULL in the browser.
2. **G2** reimplement `TorrentQuery.files` over the blob (0 GB) — _prerequisite_; without it the browser breaks at cutover.
3. Timestamp (E2, from `created_at`) + index-sort hydration (0 GB).
4. File-grained Tantivy index v1 (ext + size + immutable denorm) — 8–15 GB.
5. Per-(torrent,ext) aggregate `{max,min,count}` in PG — exact one-sided distinct-torrent — 3–5 GB. _(Promoted from "optional": it IS the literal incumbent collapse capability.)_
6. DuckDB-on-blobs — exact analytics — 0 GB.
7. Add an `extension` field to the consistency checker (so G1-class bugs can't hide again).
8. Guard `criteria_torrent_file_extension.go` (`EXISTS torrent_files`) so the retired PG search mode can't run post-cutover.

**v1.5 — required to _complete_ distinct-torrent collapse (also exceeds the incumbent, which never exposed these in search)**

9. Bucket-size vector on the aggregate (~1 GB) — two-sided distinct-torrent ranges.
10. `DistinctTorrentCollector` (0 GB, ~150 LOC, gated) — path-FTS / over-cap collapse, exact-but-slow guard.

**Optional / forward**

11. File index v1.1 path FTS (+8–18 GB) — cost-gated on smoke-sizing + demand.
12. Composite-term interim field on the existing torrent index (~0.3 GB) — torrent-grained bucketed ext+size _before_ the file index ships; retire after.
13. Widen the file-index denorm set (E1) only if interactive compound predicates are demanded.

Every step is additive, reversible, and gated by `SEARCH_FILE_INDEX_ENABLED=false`.

---

## 6. Spec edits this analysis triggers

- `file-grained-search-spec.md` §5.1/D8: replace "extension from the stored blob value" with **derive-from-path** (G1).
- Add **G2** (file-browser-over-blob) and **G1** as v1 line items in §11.
- Promote the per-(torrent,ext) aggregate to v1 (it is incumbent parity, not optional).
- Add the consistency-checker `extension` field + the `criteria_torrent_file_extension` post-cutover guard to the cutover runbook.

---

## 7. Source references

- **G1:** `dhtcrawler/persist.go:211-216`, `blobmigration/serializer.go:34`, `migrations/00001_init.sql:70`, `blobmigration/queue/handler.go:150-167`, `blobmigration/consistency/checker.go:63-93`, `model/torrent_files.go` (`FileExtensionFromPath`), `model/torrents.go` (`FileExtensions`/`ExtractUniqueExtensions`).
- **G2:** `internal/gql/resolvers/query.resolvers.go:109`, `internal/gql/gqlmodel/torrent_files.go:25`, `internal/database/search/search_torrent_files.go`, `internal/database/search/order_torrent_files.go`, webui `torrents/torrent-files.datasource.ts`.
- **Composition / numbers:** `docs/dev/file-grained-search-spec.md` (§5, §11, §13), `docs/dev/perfile-search-innovative-design.md`, `docs/space-savings-verification.md`.
- **Blob format:** `bitmagnet-rs/crates/bitmagnet-model/src/blob.rs:31-46,60`.
