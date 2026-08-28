# G1a — BitTorrent v2 / hybrid FOUNDATION — adversarial spec & decision record

**Task:** G1.1 (foundation slice of gap **G1**, `docs/bep-compliance-audit.md` §"BEP 52").
**Branch:** `feat/bittorrent-v2-foundation`.
**Role of this doc:** adversarial audit of the proposed _widened dual-hash PK_ design,
plus the concrete, code-grounded spec implementers must follow. No production code is
written here.
**Status:** ✅ Verdict reached. **One required design change** (canonical PK width — see §A).

---

## 0. TL;DR verdict

| #   | Design point                                                                                                                                           | Verdict                                                                                                                                   |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| A   | Dual-hash columns (`info_hash_v1`, `info_hash_v2`, `meta_version`) + backfill                                                                          | ✅ **GO**                                                                                                                                 |
| A   | **Pure-v2 canonical PK = full 32-byte SHA-256**                                                                                                        | ❌ **NO-GO** as written                                                                                                                   |
| A   | **Pure-v2 canonical PK = truncated 20-byte SHA-256** (REQUIRED CHANGE)                                                                                 | ✅ **GO**                                                                                                                                 |
| B   | Go types: `protocol.ID` stays the 20-byte node/peer/infohash+PK handle; add a new 32-byte `protocol.InfoHashV2` **only** for the `info_hash_v2` column | ✅ **GO** (honours the user's "new 32B type" choice without widening the PK or `protocol.ID`)                                             |
| C   | Parser: SHA-256 verify + dual-hash + meta_version                                                                                                      | ✅ **GO** (exact API in §C)                                                                                                               |
| D   | persist: `UpvertedFiles()`                                                                                                                             | 🟡 **GO with mandatory correction** (blanket swap is a bug — §D)                                                                          |
| E   | Rust read path                                                                                                                                         | ✅ **Safe IFF §A change adopted** (truncated ⇒ all PKs stay 20 bytes ⇒ Rust untouched). Full-32 ⇒ **breaks Rust**, must include G1d work. |

**The single most important finding:** the whole crawl pipeline is keyed on the **20-byte
DHT infohash** end-to-end. Making the _stored PK_ 32 bytes for pure-v2 desynchronises that
pipeline, breaks the Rust read path, breaks the GraphQL `Hash20` scalar, and forces a
type-widening that is explicitly out of "foundation" scope. **Using the truncated 20-byte
SHA-256 as the canonical PK (with the full 32-byte hash retained in `info_hash_v2`) achieves
every G1a goal — pure-v2 becomes representable, searchable, and round-trippable to its true
v2 identity — with near-zero blast radius.** This is a _required_ change, not a preference.

---

## A. Migration safety & the canonical-PK decision

### A.1 What is sound in the proposal (✅ GO)

- **`info_hash bytea` has no length constraint.** `migrations/00001_init.sql:20`
  (`info_hash bytea not null primary key`) — verified no later migration adds a
  length/octet CHECK on it. The column can physically hold 20 _or_ 32 bytes today.
- **Generated columns survive any PK width.**
  - `torrents.extension` derives from `name`, not the hash (`00002_files_status.sql:10-12`) — unaffected.
  - `torrent_contents.id = encode(info_hash,'hex')||':'||…` (`00001_init.sql:190-193`) — `encode()`
    works for any byte length; the composite id stays unique. Fine for 20- or 32-byte hashes.
- **All 7 FK children FK the _column_, not a width.** `torrents_torrent_sources`,
  `torrent_files`, `torrent_contents` (00001), `tags`/`torrent_tags` (00003),
  `torrent_hints` (00007), `torrent_pieces` (00013), `blob_storage`/`torrent_file_summary`
  (00021) all `references torrents(info_hash) on delete cascade` against `bytea` — **no FK
  column type change is needed** under either PK strategy.
- **The three new columns + backfill are correct and safe:**
  ```sql
  ALTER TABLE torrents ADD COLUMN info_hash_v1 bytea;       -- 20-byte SHA-1 (v1 + hybrid)
  ALTER TABLE torrents ADD COLUMN info_hash_v2 bytea;       -- 32-byte SHA-256 (hybrid + pure-v2)
  ALTER TABLE torrents ADD COLUMN meta_version smallint;    -- 1 = v1, 2 = v2/hybrid
  CREATE INDEX ON torrents (info_hash_v1);
  CREATE INDEX ON torrents (info_hash_v2);
  -- backfill existing rows (all are v1 today):
  UPDATE torrents SET info_hash_v1 = info_hash, meta_version = 1;
  ```
  Backfill is a single non-locking-heavy `UPDATE`; existing rows are preserved verbatim;
  `meta_version` stays nullable (no NOT NULL/ default churn required for foundation).

### A.2 Why full-32-byte canonical PK is NO-GO for a _foundation_ branch

The proposal keeps `info_hash` as PK but lets it hold **32 bytes for pure-v2**. Adversarial
analysis says this is the wrong cut line:

1. **It desynchronises the crawl pipeline.** Every stage keys on the **20-byte DHT hash**
   (`protocol.ID`, `type ID [20]byte`, `id.go:52`):

   - discovery/triage: `blocking.Filter([]protocol.ID)` (`manager.go:35`), `runInfoHashTriage`
     `map[protocol.ID]` + `Torrent.InfoHash.In(valuers...)` (`infohash_triage.go:28-104`);
   - fetch: `requester.Request(ctx, infoHash protocol.ID, …)` + `btHandshake` 20-byte echo
     compare `if resHash != infoHash` (`requester.go:86,165-200`);
   - verify: `ParseMetaInfoBytes(infoHash protocol.ID, …)` (`parse.go:12`);
   - persist/classify/scrape: `hashMap map[protocol.ID]…`, `hashesToClassify []protocol.ID`,
     scrape channel keyed by the 20-byte hash (`persist.go:37-91,140-147`).

   If the _stored PK_ becomes a 32-byte value for pure-v2, it no longer equals the 20-byte
   hash the pipeline used to fetch, classify and scrape that torrent — the persisted row and
   the in-flight scrape/classify keys diverge. The truncated PK keeps **PK == the hash we
   crawled with**, so the pipeline stays coherent with zero rework.

2. **It breaks the Rust read path** (see §E) — `InfoHash::from_slice` rejects ≠20 bytes.

3. **It breaks the GraphQL `Hash20` scalar.** The scalar maps to `protocol.ID`
   (`gqlgen.yml:116-118`) and is used for _every_ infohash field/arg
   (`gql.gen.go:2453,2474,2499,2594-2602,…`); marshalling emits 40-hex (`id.go:207-209`).
   A 32-byte PK would surface 64-hex IDs → a breaking API/scalar change → squarely G1e.

4. **It changes Go↔Rust DocID parity.** `torrent_contents.id`/Tantivy `doc_id` is built from
   `hex(info_hash)` on both sides (memory: _Rust Search Parity_); variable-width hashes force
   parity edits in both builders — not "foundation".

5. **It forces the type split now.** `protocol.ID.Scan` does `copy(id[:], v)` (`id.go:125-134`)
   — silently truncates a 32-byte read to 20 — and `Value()` returns 20 bytes (`id.go:136-138`).
   So a 32-byte PK _cannot_ be stored/round-tripped through the existing model type; you must
   widen/replace `protocol.ID` across **8 model fields** (`torrents.gen.go:17`,
   `torrent_files.gen.go:17`, `torrent_pieces.gen.go:17`, `torrent_hints.gen.go:17`,
   `torrent_file_summary.go:12`, `torrents_torrent_sources.gen.go:19`, `torrent_tags.gen.go:17`,
   `torrent_contents.gen.go:19`) **and** the importer (`importer.go:259,266` uses `item.InfoHash`).

### A.2b The (i) vs (ii) decision — definitive

The user chose **"Widened dual-hash PK"** (explicit `info_hash_v1`/`info_hash_v2` columns +
a new 32-byte Go type) **and** **"foundation only"**. Those two are reconciled by option **(i)**,
**not** by a 32-byte PK:

- **(i) — RECOMMENDED:** PK column stays **20 bytes always** (SHA-1 for v1/hybrid; truncated
  SHA-256 for pure-v2); the **full 32-byte v2 hash lives in the new `info_hash_v2` column**, for
  which we **still introduce the new 32-byte Go value object** (`protocol.InfoHashV2`). This
  honours the dual-hash-columns + new-type choice _and_ stays within foundation scope.
- **(ii) — REJECTED (violates foundation-only):** full 32-byte PK for pure-v2.

**Why (ii) is out of scope — concrete pipeline breakages (file:line):** the discovery hash that
enters the pipeline is _always_ the 20-byte value (truncated SHA-256 for v2, since the full hash
is only knowable _after_ parsing the info dict). A 32-byte PK forces a truncated→full remap that
breaks three core flows:

1. **Scrape FK orphan.** After persist, `c.scrape.In() <- i.nodeHasPeersForHash`
   (`persist.go:144`) carries the 20-byte hash; the scrape persister writes
   `torrents_torrent_sources` with that 20-byte value (`createTorrentSourceModel` →
   `result.infoHash`, `persist.go:293-296`). If the `torrents` row PK is now 32 bytes, that
   source row FK-violates / orphans. Same for `torrent_files`/`torrent_pieces`, which
   `createTorrentModel` fills with the same `hash` (`persist.go:182-187,201-208`).
2. **Classification miss.** `hashesToClassify []protocol.ID` (20-byte, `persist.go:91`) feeds the
   processor job; the processor then looks torrents up by those 20-byte hashes and would never
   find the 32-byte-keyed pure-v2 row → it stays unclassified.
3. **Triage never dedups.** Re-discovery of the same pure-v2 via DHT yields the 20-byte truncated
   hash; `runInfoHashTriage` queries `Torrent.InfoHash.In(valuers...)` with 20-byte values
   (`infohash_triage.go:53-73`) → no match against a 32-byte PK → the torrent is re-crawled
   forever and never recognised as known.

Each of these is a _foundation_ flow (persist, scrape, classify, re-crawl gate), so (ii) cannot
be confined to "foundation only" — it drags in a pipeline-wide remap that is properly **G1b**
(DHT-wire / v2-identity) scope. **(i) is the definitive recommendation.**

**FK / identity confirmation for (i):** `torrents_torrent_sources`, `torrent_files`,
`torrent_contents`, `torrent_pieces`, `torrent_hints`, `tags` all `references torrents(info_hash)`
(the 20-byte value). Under (i) the pure-v2 PK **is** the same 20-byte truncated hash the pipeline
already inserts everywhere (`persist.go:182-187,221-226,295`), so FK coherence is automatic and
byte-for-byte identical to the v1 path — **no FK or identity issue**. The only added uniqueness
guard is `UNIQUE (info_hash_v2)` for exact v2-identity dedupe.

### A.3 REQUIRED design change — truncated 20-byte canonical PK (= option (i))

> **Canonical `torrents.info_hash` PK is ALWAYS 20 bytes:**
>
> - **v1-only:** the v1 SHA-1 (unchanged; existing rows untouched).
> - **hybrid:** the v1 SHA-1 (preserves v1 swarm identity & existing rows).
> - **pure-v2:** the **truncated (first 20 bytes) SHA-256** — i.e.
>   `infohash_v2.HashBytes(infoBytes).ToShort()`. This is _exactly_ the 20-byte handle BEP 52
>   mandates "where 20 bytes are needed (DHT/trackers)" and is the value we already crawled the
>   torrent with.
>
> The **full 32-byte** v2 hash is preserved in `info_hash_v2`; `meta_version` records the kind.

Consequences (all favourable):

- `protocol.ID` is unchanged; **no Go type split is required in G1a** (deliverable B collapses
  to "add three model fields + a 32-byte value object only for the `info_hash_v2` column").
- GraphQL `Hash20` stays valid (40-hex everywhere). No scalar/API change.
- Rust read path untouched (all PKs 20 bytes). G1d can proceed independently.
- DocID parity unchanged.
- The crawl pipeline's 20-byte key == the PK throughout.

Residual risks of truncation (all acceptable for foundation, documented):

- **20-byte collision** between two distinct pure-v2 torrents (birthday ≈ 2⁻⁸⁰) — _same risk
  class the system already accepts for v1 SHA-1_. Mitigate by adding `UNIQUE` on the full
  `info_hash_v2` so an exact-identity duplicate surfaces instead of silently merging.
- **Cross-space collision** (a truncated-v2 equals some v1 SHA-1) — 160-bit, astronomically
  unlikely; no worse than the existing v1 collision assumption.
- **Hybrid dedup by v2:** a hybrid is keyed by its v1 hash; if the _same_ torrent is later
  seen only by its v2 reference, it inserts a second row. **This duplication exists identically
  under the full-32 design** (hybrid keyed v1, v2-ref keyed v2) — it is a _lookup/dedup_ gap to
  resolve in **G1b** (query `info_hash_v2` before insert), not a PK-width issue. Flag it; out of
  scope for G1a.

### A.4 Down migration (feasible)

```sql
-- +goose Down
DROP INDEX IF EXISTS … (v1/v2 indexes);
ALTER TABLE torrents DROP COLUMN IF EXISTS meta_version;
ALTER TABLE torrents DROP COLUMN IF EXISTS info_hash_v2;
ALTER TABLE torrents DROP COLUMN IF EXISTS info_hash_v1;
-- OPTIONAL hard-clean: remove pure-v2 rows ingested while v2 was enabled, so a
-- post-downgrade v1-only binary never serves rows it can't re-derive:
--   DELETE FROM torrents WHERE meta_version = 2 AND info_hash_v1 IS NULL;
-- (must run BEFORE dropping the columns; cascades via the 7 FK children)
```

Because the truncated PK is a valid 20-byte hash, _leaving_ pure-v2 rows after a downgrade is
harmless (they look like ordinary 20-byte torrents); the DELETE is offered for purists. Under
the rejected full-32 design the DELETE would be **mandatory** (else a downgraded v1-only binary

- 20-byte Rust reader chokes on 32-byte PKs) — another reason truncated is safer.

---

## B. Go type strategy & blast radius

**Decision: keep `protocol.ID` as the 20-byte type for node IDs, peer IDs, and the canonical
infohash PK. Do NOT widen it. Do NOT introduce a variable-length PK type.** With the §A.3
truncated PK, the PK column is always 20 bytes, so the existing `protocol.ID` Scan/Value/
Bencode/JSON/GQL machinery (`id.go:125-209`) is correct as-is.

What G1a _adds_ (minimal):

- A small fixed-size value object for the **full v2 hash** used only by the new
  `info_hash_v2` column — e.g. `protocol.InfoHashV2 [32]byte` with `Scan`/`Value`
  (`bytea` ⇄ 32 bytes) and `String()`/`ParseInfoHashV2()` (64-hex). It is **not** a PK, **not**
  a map key, **not** on the DHT wire. Model field: `InfoHashV2 *protocol.InfoHashV2`
  (nullable). `meta_version` → `MetaVersion model.NullUint8`/`*uint8`. `info_hash_v1` →
  `*protocol.ID` (nullable; equals PK for v1/hybrid).
- These three fields land on the **`Torrent`** model only (`torrents.gen.go`). The other 7 FK
  models keep `protocol.ID` (they FK the 20-byte PK).

**gorm PK note:** `primaryKey;<-:create` on a fixed `[20]byte` needs no custom variable-length
logic — `Scan`/`Value` already exist. (A variable-length PK _would_ have required a bespoke
type; the §A.3 decision removes that need entirely.)

**Call sites that MUST change in G1a (small set):**

- `internal/protocol/metainfo/parse.go:12-23` — verification + return dual hashes/meta_version (§C).
- `internal/dhtcrawler/persist.go:153-228` — `createTorrentModel`: populate the 3 new fields;
  fix file enumeration (§D).
- `internal/model/torrents.gen.go` — add 3 fields (regenerate via `task gen-gorm`; see
  Taskfile) + likely `internal/database/dao` regen.
- `migrations/00023_v2_infohash.sql` (next free number; highest existing is **00022**).
- New value object `protocol.InfoHashV2` (+ unit tests).

**Call sites that explicitly STAY (deferred), thanks to §A.3:**

- `protocol.ID` and all node/peer-ID callers (`RandomNodeID*`, ktable, dht/\*).
- GraphQL `Hash20` scalar & `gqlmodel` (G1e to optionally expose v2 fields).
- Rust crates (G1d).
- Magnet `urn:btmh:` (`torrents.go:92-96`) — G1b.
- Handshake `ExtensionBitV2 = 7` advertise (`requester.go:41,163`) — G1c.
- `internal/importer/importer.go` — v1-only RARBG-style import path; untouched.

---

## C. Parser spec (`parse.go`) — exact anacrolix API

Verified against `anacrolix/torrent@v1.58.0`:

| Need                          | Call                                                                                       | Result                       |
| ----------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------- |
| v1 SHA-1 of raw info bytes    | `mi.HashBytes(metaInfoBytes)` (`metainfo/infohash.go:80`, used today `parse.go:13`)        | `infohash.T` = `[20]byte`    |
| v2 SHA-256 of raw info bytes  | `infohash_v2.HashBytes(metaInfoBytes)` (`types/infohash-v2/infohash-v2.go:85`)             | `infohash_v2.T` = `[32]byte` |
| truncated v2 (first 20 bytes) | `v2hash.ToShort()` (`infohash-v2.go:60-62`)                                                | `*infohash.T` = `[20]byte`   |
| meta version / kind           | unmarshal then `info.MetaVersion` / `info.HasV2()` / `info.HasV1()` (`info.go:29,198-205`) | `int64` / `bool`             |

**New `ParseMetaInfoBytes` contract** (returns the info + a small descriptor so persist needs
no re-hashing):

```
v1   := protocol.ID(mi.HashBytes(metaInfoBytes))          // SHA-1
v2   := infohash_v2.HashBytes(metaInfoBytes)              // SHA-256 (full)
v2t  := protocol.ID(*v2.ToShort())                        // truncated SHA-256
// unmarshal first (cheap) to learn the kind:
bencode.Unmarshal(metaInfoBytes, &info)
hasV1, hasV2 := info.HasV1(), info.HasV2()

// ANTI-POISON verify: the bytes we received must hash to the hash we requested.
// `infoHash` is the 20-byte value from the DHT/ut_metadata fetch.
switch {
case hasV1 && v1 == infoHash:            ok (v1-only OR hybrid; canonical = v1)
case hasV2 && v2t == infoHash:           ok (pure-v2 OR hybrid reached via v2; canonical = v2t)
default:                                  reject "info bytes have wrong hash"
}
```

**Canonical-hash selection (drives the PK and the 3 columns):**

| Kind    | `HasV1` | `HasV2` | PK (`info_hash`)      | `info_hash_v1` | `info_hash_v2` | `meta_version` |
| ------- | ------- | ------- | --------------------- | -------------- | -------------- | -------------- |
| v1-only | ✓       | ✗       | v1 SHA-1              | v1 SHA-1       | `NULL`         | 1              |
| hybrid  | ✓       | ✓       | **v1 SHA-1**          | v1 SHA-1       | full SHA-256   | 2              |
| pure-v2 | ✗       | ✓       | **truncated SHA-256** | `NULL`         | full SHA-256   | 2              |

Notes:

- Keep computing the infohash over the **raw received bytes** (no re-marshal) — preserves the
  existing anti-poisoning property (audit §BEP 3/9).
- `HasV1()` is `true` for hybrids (it checks `Files != nil || Length != 0 || len(Pieces) != 0`,
  `info.go:202-205`), so hybrids correctly take the v1 PK row and still record the v2 columns.
- The `switch` removes the current hard SHA-1-only gate (`parse.go:13`) that silently drops
  pure-v2.

---

## D. persist spec (`createTorrentModel`) — `UpvertedFiles()` correction

**The proposal's "use `info.UpvertedFiles()`" is correct for v2 _enumeration_ but a blanket
`info.Files` → `info.UpvertedFiles()` swap is a BUG** (and is exactly what audit G9 warns
against). Reason: for a **v1 single-file** torrent `UpvertedFiles()` == `UpvertedV1Files()`
returns **one synthetic `FileInfo{Length, Path: nil}`** (`info.go:168-176`). The current code
classifies single vs multi by `len(info.Files) > 0` (`persist.go:169`); a naive swap to
`len(UpvertedFiles()) > 0` would reclassify every v1 single-file torrent as **Multi** and emit
an **empty-path** file row — corrupting `FilesStatusSingle` (load-bearing for
`SingleFile()`/`HasFilesInfo()` `torrents.go:99-105`, the `extension` generated column
`00002:10-12`, transformer/search, and the re-crawl gate `infohash_triage.go:85-88`).

**Correct logic — classify by `info.IsDir()` (byte-identical to today for v1, correct for v2):**

```go
isSingle := !info.IsDir()   // v1: len(Files)==0  → false==dir; v2: FileTree.IsDir()
                            // (info.go:146-152)

if isSingle {
    filesStatus = model.FilesStatusSingle   // NO file rows; filesCount stays null
} else {
    upverted := info.UpvertedFiles()        // v1 → UpvertedV1Files (incl. padding, unchanged);
                                            // v2 → FileTree files with real Paths (info.go:156-164)
    filesStatus = model.FilesStatusMulti
    filesCount  = model.NewNullUint(uint(len(upverted)))
    for i, file := range upverted {
        if i >= int(saveFilesThreshold) { filesStatus = model.FilesStatusOverThreshold; break }
        files = append(files, model.TorrentFile{
            InfoHash: hash, Index: uint(i),
            Path: file.DisplayPath(&info),  // v2-safe: IsDir→BestPath() join; else BestName()
            Size: uint(file.Length),
        })
    }
}
```

**Equivalence proof for v1 (MUST NOT change v1 behavior):**

- v1 single (`Files` empty): `IsDir()`==false → Single, no rows — _identical_ to current.
- v1 multi-with-1-file (`Files`==1, in a dir): `IsDir()`==true → Multi, 1 row — _identical_
  (current `len(Files)>0`→Multi). This is why the predicate is `IsDir()`, **not** `len==1`.
- v1 multi: `IsDir()`==true → Multi; `UpvertedV1Files` returns the same `Files` (adds an unused
  `TorrentOffset`); `DisplayPath`/sizes identical; `filesCount`==`len(Files)`. _Identical._

**v2 behavior (new):**

- pure-v2/hybrid single-file: `IsDir()`==false → Single, extension from name (G9 path) — parity
  with v1 single.
- v2/hybrid multi: real per-file paths from the file tree; correct `filesCount`/threshold.

**Out of scope (do NOT add here):** padding-file filtering by `ExtendedFileAttrs.Attr=="p"`
is **G3**; `UpvertedV1Files` deliberately still includes padding files. Leave it.

Also populate the 3 new fields on the returned `model.Torrent` (from the §C descriptor):
`InfoHashV1`, `InfoHashV2`, `MetaVersion`. `Pieces` continues to store the **v1** `Pieces`
(`persist.go:201-208`); v2 **piece layers** are a separate concern (note for a later slice —
they live outside the info dict, in the `.torrent`'s `piece layers` key, not in ut*metadata
data, so they are \_not* recoverable on the crawl path and are correctly deferred).

---

## E. THE RUST QUESTION — definitive answer

**With the §A.3 truncated PK: G1a lands with ZERO Rust changes and ZERO risk.** Every
`torrents.info_hash` value remains 20 bytes, so the Rust reader never sees a non-20-byte PK.

Evidence the full-32 design **would** break Rust (hence rejected):

- `bitmagnet-db/src/stream.rs:73-75` and `:234-236`:
  `let raw: Vec<u8> = row.try_get("info_hash")?;`
  `InfoHash::from_slice(&raw).map_err(|e| DbError::Decode(...))?;`
- `InfoHash::from_slice` (`bitmagnet-model/src/info_hash.rs:29-34`) `try_into::<[u8;20]>()`
  → `Err(InfoHashError::Length)` for 32 bytes. The `?` propagates `DbError::Decode` and
  **aborts the whole page fetch** (`stream_torrents_with_files`, `stream_torrents_for_index`).
  Worse: pages are **keyset-ordered by `info_hash`** (`stream.rs:45-50`), so a single 32-byte
  pure-v2 row poisons the page _and_ the cursor can't step past it → backfill wedged.

Recommendation: **adopt §A.3 (truncated PK)**. Then no Rust widening, no shadow-mode guard, no
"prevent 32-byte rows from reaching Rust" hack is needed — there are no 32-byte PKs to reach it.
Full v2 parity (reading `info_hash_v2`/`meta_version`, btmh, etc.) is cleanly deferred to **G1d**
because the new columns are additive and ignored by the current Rust SELECTs
(`stream.rs:45-46` selects a fixed column list that does not include them).

> If the team _overrides_ this audit and keeps the full-32 PK, then G1a **must** also: widen the
> Rust `InfoHash` (or add a separate 32-byte type) across `info_hash.rs`, `stream.rs`,
> `torrent.rs`, `content.rs`, and fix DocID/`from_slice` call sites — i.e. drag G1d into G1a.
> That contradicts "foundation only". **NO-GO.**

---

## F. Risks, edge cases & required test matrix

**Risks / edge cases to handle:**

1. v1 single-file reclassification regression (the §D `IsDir()` predicate is mandatory).
2. 20-byte truncation collisions — add `UNIQUE (info_hash_v2)` to surface exact dupes (§A.3).
3. Hybrid-vs-v2 duplicate rows — dedup by `info_hash_v2` is **G1b**; documented gap, not fixed here.
4. `meta_version` nullability — keep nullable; do not backfill-then-NOT-NULL in foundation.
5. Anti-poison `switch` must reject when _neither_ v1 nor truncated-v2 matches the requested
   hash (tamper/poison) — preserve current security property (`parse.go:13`).
6. v2 piece layers are unavailable on the ut_metadata path — do not attempt to store; note it.
7. Generated `torrent_contents.id` length grows only if PK grows — truncated PK keeps it stable.
8. `gen` drift: after editing the model, run `task gen-gorm` + commit generated diff (CI
   `generated` check fails otherwise — see memory _bitmagnet CI Gotchas_).

**Test matrix (implementers MUST cover):**

| Case                             | Fixture / input                              | Assert                                                                                                                               |
| -------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| v1-only multi                    | existing v1 torrent                          | row byte-identical to pre-G1a (PK, files, status, count, extension)                                                                  |
| v1-only single                   | v1 single-file                               | `FilesStatusSingle`, **no** file row, extension from name — unchanged                                                                |
| hybrid                           | `testdata/bittorrent-v2-hybrid-test.torrent` | PK = v1 SHA-1; `info_hash_v1`=PK; `info_hash_v2`=full SHA-256; `meta_version=2`; files from tree                                     |
| pure-v2 multi                    | `testdata/bittorrent-v2-test.torrent`        | **ingested, not dropped**; PK = truncated SHA-256; `info_hash_v1` NULL; `info_hash_v2` full; `meta_version=2`; per-file rows present |
| pure-v2 single                   | synthetic v2 single-file                     | `FilesStatusSingle`, no row, extension from name                                                                                     |
| tampered bytes                   | flip a byte                                  | rejected ("wrong hash") for v1, hybrid, and pure-v2 alike                                                                            |
| anti-poison                      | bytes whose hash ≠ requested                 | rejected                                                                                                                             |
| migration up                     | backfill                                     | all existing rows: `info_hash_v1==info_hash`, `meta_version=1`, `info_hash_v2` NULL                                                  |
| migration down                   | drop columns (+ optional pure-v2 DELETE)     | schema restored; v1 rows intact                                                                                                      |
| `protocol.InfoHashV2` round-trip | 32-byte value                                | `Scan`/`Value` (bytea⇄32B), `String()`/parse 64-hex, JSON, (and Bencode/GQL only if surfaced)                                        |
| Rust backfill smoke              | DB containing a pure-v2 row                  | `stream_torrents_*` reads it **without** `DbError::Decode` (passes precisely because PK is 20 bytes)                                 |
| GraphQL                          | query a pure-v2 torrent's `infoHash`         | returns valid 40-hex `Hash20` (truncated PK)                                                                                         |
| triage re-crawl gate             | pure-v2 row with files                       | not re-queued for getPeers once files known (`infohash_triage.go:85-88`)                                                             |

---

## G. Required design changes (summary for implementers)

1. **Canonical pure-v2 PK = truncated 20-byte SHA-256**, full 32-byte hash in `info_hash_v2`.
   (Do **not** store 32-byte PKs.) ← the one blocking change.
2. **Do not widen/split `protocol.ID`.** Add only a `protocol.InfoHashV2 [32]byte` value object
   for the `info_hash_v2` column.
3. **persist:** classify single/multi by `!info.IsDir()`; enumerate with `UpvertedFiles()`
   only in the multi branch. Never emit a synthetic single-file row.
4. Add `UNIQUE (info_hash_v2)` alongside the v1/v2 indexes.
5. Keep magnets, DHT bit-7 advertise, GraphQL v2 surface, and Rust v2 parity **out** (G1b/c/d/e).

With these, G1a is migration-safe, preserves all existing rows / 7 FK tables / generated
columns, leaves the Go node-ID type and the Rust read path untouched, stops dropping pure-v2,
and records full v2 identity for later slices.
