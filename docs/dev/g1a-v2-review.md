# G1.6 — Adversarial review of the G1a (BitTorrent v2/hybrid foundation) IMPLEMENTATION

**Scope:** read-only review of implemented code on `feat/bittorrent-v2-foundation`.
**Verdict: 🔴 NO-GO** until two required fixes land. Everything else is solid.

Both defects are reachable in normal crawling and are _masked_ by tests that exercise the
wrong shape, so the green suite does not catch them.

---

## 🔴 REQUIRED #1 — `UNIQUE(info_hash_v2)` + hybrid cross-discovery ⇒ whole-batch persist failure / data loss

**Reachability:** a hybrid torrent is announced on the DHT under **both** its v1 infohash
**and** its truncated-v2 infohash, so the crawler discovers it twice with two _different_
20-byte PKs. The PK is the discovery hash (`persist.go:222` `InfoHash: hash`), and
`ParseMetaInfoBytes` records the **full** v2 hash whenever `HasV2()` — i.e. for _both_
discoveries (`parse.go:63-66`). Result: two `torrents` rows, different PK, **identical
`info_hash_v2`**.

**Defect:** the persist upsert's only conflict arbiter is `info_hash`
(`persist.go:101-111`). A unique violation on the _non-arbiter_ `info_hash_v2` index
(`migrations/00023_v2_infohash.sql:19`) is **not** absorbed by `ON CONFLICT (info_hash)` →
the whole `CreateInBatches(torrentsToPersist, 100)` statement aborts → the surrounding
`dao.Transaction` rolls back **all** of that batch (torrents + files + sources + pieces +
queue jobs) → it is merely logged and dropped (`persist.go:134-135`). It recurs every time
that hybrid is re-seen, so it is sticky data loss, not a one-off.

**Why the suite misses it:** `TestV2UniqueInfoHashV2`
(`persist_v2_integration_test.go:108-128`) confirms the constraint _fires_ via single-row
`db.Create`, but never drives the batched `runPersistTorrents` upsert path, so the
batch-rollback behavior is untested.

**Required fix:** for G1a, make `info_hash_v2` a **plain (non-unique) index**. Exact
v2-identity dedupe / hybrid-row merge is **G1b** scope (look up `info_hash_v2` before
insert, or merge the two rows). Update `TestV2UniqueInfoHashV2` accordingly.
_(Note: the original spec recommended this UNIQUE; I retract it — in the batch path it turns
a benign duplicate row into active data loss. Alternative — keep UNIQUE and make persist
resilient to a second arbiter — is more work and against "foundation only".)_

---

## 🔴 REQUIRED #2 — v2/hybrid **single-file** torrents misclassified as multi (test is vacuous)

**Defect:** classification gates on `info.IsDir()` (`persist.go:178`). For v2, `Info.IsDir()`
delegates to `FileTree.IsDir()` = `NumEntries() != 0` (anacrolix `info.go:146-152`,
`file-tree.go:89-94`). A **real** v2 single-file torrent has
`file tree = {"movie.mkv": {"": {…}}}` — the root tree has **one entry** → `IsDir() == true`.

Empirically verified (probe against anacrolix v1.58.0): a realistic
`Info{MetaVersion:2, FileTree.Dir:{"movie.mkv":{File:{Length}}}}` returns **`IsDir()==true`**
(while v1 single-file returns `false`). So real v2/hybrid single-file torrents take the
**multi** branch → `FilesStatusMulti`, `FilesCount=1`, a spurious `torrent_files` row, and a
NULL `torrents.extension` (the generated column is `single`-only). This **breaks v1 parity**
and the spec's explicit contract that v2-single behaves like v1-single.

**Why the suite misses it:** `TestCreateTorrentModelV2SingleFile`
(`persist_v2_test.go:113-123`) builds `FileTree{File:{Length}}` with an **empty `Dir`** — a
shape that never occurs in a real torrent (a file-tree root is always a directory of names) —
so `IsDir()` is false and it routes through the Single branch. It even asserts
`require.False(info.IsDir())` on that unreal structure, giving false confidence. The common
case (a single movie/ISO as pure-v2 or hybrid) is broken and uncovered.

**Required fix:** keep `!info.IsDir()` for v1 (byte-identical) but detect a lone root-level
file for v2:

```go
isSingle := !info.IsDir()
if info.HasV2() {
    f := info.UpvertedFiles()
    isSingle = len(f) == 1 && len(f[0].Path) <= 1
}
```

and rewrite the test with a realistic `Dir:{"movie.mkv":{File:{Length}}}` tree.

---

## Confirmation on the 8 points

1. **Canonical-PK invariant — ✅ holds.** PK == discovery hash everywhere (`persist.go:222`;
   files/pieces/sources all use `hash`). FK coherence intact. _Document:_ a hybrid discovered
   via its truncated-v2 hash gets PK=v2-trunc while `info_hash_v1` holds the (different) real
   v1 hash (`parse.go:58-60`) — consistent with PK==discovery, but the "hybrid PK = v1"
   wording only holds when discovered via v1. Not a bug; this is the same scenario behind #1.
2. **Anti-poison ordering — ✅ correct.** Raw-byte v1 and truncated-v2 hashes are computed and
   matched **before** `bencode.Unmarshal` (`parse.go:35-49`); tampered/wrong-hash rejected for
   v1/hybrid/pure-v2 (`parse_test.go:130-172`). Unconditional SHA-256 is over ≤10 MiB
   (`maxMetadataSize`) — sub-ms, not a DoS.
3. **v1 non-regression — ✅ none.** v1-single → `IsDir()==false` → Single, no rows (identical).
   v1-multi → `UpvertedFiles()==UpvertedV1Files()` returns `info.Files` in order, same
   `DisplayPath`/sizes/count, same threshold logic (`persist.go:178-198`). The empty-`Files`
   synthetic single-file is never reached (guarded by `IsDir()`).
4. **FilesStatusSingle semantics — ⚠️ broken for v2/hybrid single-file (see #2).** Intact for
   v1. The re-crawl gate (`infohash_triage.go:85-88`) tolerates the misclassification
   (Multi+FilesCount valid) but UI/`SingleFile()`/`torrents.extension`/`FileType()` diverge
   from v1 for identical content.
5. **Migration safety — ✅ (with #1's index change).** Additive bytea/smallint, no length
   CHECK, generated columns untouched; backfill `info_hash_v1=info_hash, meta_version=1`
   correct; NULL-distinct verified (`TestV2MultipleV1RowsCoexist`); up/down verified
   (`TestV2MigrationUpDown`). Change `info_hash_v2` unique→plain per #1.
6. **Test vacuity / missing cases — ⚠️.** `TestCreateTorrentModelV2SingleFile` is vacuous
   (#2). `TestV2UniqueInfoHashV2` doesn't cover the batch path (#1). Missing: hybrid
   discovered via truncated-v2 (parse + persist — the #1 trigger); v2 over-threshold
   (`FilesStatusOverThreshold`/`FilesCount`); a batched-persist regression test for #1.
7. **Pointer aliasing in ParsedInfo — ✅ none.** `ParseMetaInfoBytes` takes addresses of
   per-call locals (`parse.go:59-65`); `Response`/`ParsedInfo` flow by value through the
   channel (`request_meta_info.go:18-24`); no loop-captured pointers / shared backing arrays.
8. **Generated-code drift — ✅ none.** `internal/database/gen/gen.go:183-185` explicitly maps
   `info_hash_v1→*protocol.ID`, `info_hash_v2→*protocol.InfoHashV2`, `meta_version→NullUint16`;
   `NullUint16` pre-exists (`null.go:391`, used by `content.runtime`). `task gen` reproduces
   the committed `torrents.gen.go` (model + dao). No index tags are emitted, so the
   unique→plain index change won't drift the model.

## Optional

- `InfoHashV2` type is clean: `Scan` enforces 32 bytes (`infohash_v2.go:81-83`),
  `Value`/`String`/`ToShort` correct, no DHT/wire methods (correctly absent for G1a).
- Cosmetic: single-file now returns `Files: nil` vs pre-G1a empty slice — behaviorally
  identical; ignore.
- Down migration leaves v2-discovered rows in place (valid 20-byte PKs) — acceptable; the
  optional `DELETE … WHERE meta_version=2` cleanup is absent.

**Bottom line:** two small, localized required fixes (#1 unique→plain + defer dedupe to G1b;
#2 v2 single-file predicate + de-vacuous the test). Everything else passes adversarial review.
