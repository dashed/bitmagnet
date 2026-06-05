# G1b2 — Hybrid-vs-v2 row dedup in `runPersistTorrents`

**Status:** spec / design audit (no production code yet)
**Branch:** `feat/bittorrent-v2-dedup`
**Audited by:** adversarial spec-auditor (G1b2.1)
**Verdict:** **GO** — design is sound. One required change (drop-before-`hashMap`-insert ordering) and two design decisions locked below. Every premise was checked against code.

---

## 1. Problem (re-confirmed against code)

A hybrid torrent `H` has three identities:

- `A` — 20-byte v1 SHA-1 (`info_hash_v1`)
- `V` — full 32-byte v2 SHA-256 (`info_hash_v2`)
- `Bt` — truncated v2 (first 20 bytes of `V`), the form BEP 52 mandates on the DHT

`H` is announced on the DHT under **both** `A` and `Bt`, so the crawler can discover it
twice and create **two `torrents` rows** with different 20-byte PKs (`A` and `Bt`) but the
**same** `info_hash_v2 = V`.

The `info_hash_v2` index is intentionally **non-unique**
(`migrations/00023_v2_infohash.sql:25`, with the rationale spelled out at lines 17–24): the
batched persist upsert only arbitrates `ON CONFLICT (info_hash)`
(`internal/dhtcrawler/persist.go:101-102`); a UNIQUE violation on `info_hash_v2` would abort
the whole batch transaction → data loss. So the duplicates persist. G1b2 collapses them to
one row at the **persist layer** (the index stays non-unique by design).

**Only hybrids can collide:**

- pure-v2 → every discovery uses the same truncated PK `Bt` → handled by the existing
  `ON CONFLICT (info_hash)` upsert.
- v1-only → `info_hash_v2` is `nil` → never participates.
- hybrid → two distinct PKs (`A`, `Bt`) sharing one `V` → the target of this fix.

---

## 2. Adversarial findings (per design point)

### Point 4 — Is `InfoHashV2` reliably set for a hybrid discovered via EITHER hash? — **GO (premise holds)**

`ParseMetaInfoBytes` (`internal/protocol/metainfo/parse.go:34-70`) hashes the **raw received
bytes** under **both** schemes (`v1 := mi.HashBytes`, `v2 := infohashv2.HashBytes`,
lines 37-39) regardless of which 20-byte hash was requested, and accepts if **either**
matches (lines 41-46). It then sets `parsed.InfoHashV2` purely on `info.HasV2()`
(lines 63-67) — independent of which hash matched.

Therefore a hybrid discovered via `A` **or** via `Bt` yields the **same** `InfoHashV2 = V`.
Confirmed. The dedup key is reliable. ✅

Note (not a blocker): the row keyed by `Bt` still gets `info_hash_v1 = A` populated, because
`info.HasV1()` is true for a hybrid and `v1 == A` (parse.go:58-61). So both candidate rows
carry the full dual identity regardless of which PK wins — which is exactly why FIRST-ONE-WINS
loses no identity information (see Point 1).

### Point 7 — Correctness of the drop predicate `stored != i.infoHash` — **GO**

Predicate: drop iff `v2 != nil` **and** `(existingV2[*v2]` or `batchV2[*v2])` exists **and**
that stored PK `!= i.infoHash`.

| Scenario                                          | stored PK for V | current `i.infoHash` | result                                     | correct?            |
| ------------------------------------------------- | --------------- | -------------------- | ------------------------------------------ | ------------------- |
| pure-v2 re-discovery                              | `Bt`            | `Bt`                 | equal → **keep** (ON CONFLICT upsert)      | ✅                  |
| hybrid re-discovered under same PK (both via `A`) | `A`             | `A`                  | equal → **keep** (upsert)                  | ✅                  |
| hybrid `A` then `Bt`                              | `A`             | `Bt`                 | differ → **drop `Bt`**                     | ✅ collapses to `A` |
| hybrid `Bt` then `A`                              | `Bt`            | `A`                  | differ → **drop `A`**                      | ✅ first-one-wins   |
| two different torrents                            | —               | —                    | distinct `V` (SHA-256 collision-resistant) | ✅ never collide    |
| v1-only                                           | `nil`           | —                    | not in set                                 | ✅ never dropped    |

The `!= i.infoHash` clause is **essential**: without it, a legitimate same-PK re-discovery
(metadata refresh that _must_ upsert) would be wrongly dropped. With it, only true cross-PK
v2 collisions are dropped. ✅

### Point 6 — No partial state for a dropped item — **GO (requires the ordering change in §3)**

If the item is `continue`d **before** `createTorrentModel` _and_ **before** the
`hashMap[i.infoHash] = i` insert, then for that item:

- no `model.Torrent`, no `TorrentFile`, no `TorrentsTorrentSource`, no `TorrentPieces` are
  appended (all built inside the `else` branch, persist.go:67-95);
- it is not appended to `hashesToClassify` (persist.go:91) → no classify job;
- it is **not** in `hashMap`, so the scrape-forward loop (persist.go:140-147) never forwards
  it.

Fully clean. The **required change** is that the v2 drop must sit _after_ the existing PK-dup
skip (persist.go:61-63) but _before_ the `hashMap` insert at persist.go:65. (See §3.) ✅

### Point 5 — Interaction with the existing `hashMap` PK-dedup — **GO, orthogonal**

`hashMap` is keyed by the **20-byte PK** `i.infoHash` and dedups exact-PK repeats within a
batch (persist.go:61-65). The v2 dedup keys on the **full 32-byte V**. They are orthogonal
and compose cleanly when ordered PK-skip → v2-drop → `hashMap` insert. No ordering hazard:
the PK skip handles identical PKs first; the v2 drop then handles distinct PKs sharing `V`.

### Point 3 — IN-query batch size, param limits, chunking — **GO, no chunking required (add a defensive cap)**

- `persistTorrents` is `NewBatchingChannel[infoHashWithMetaInfo](1000, 1000, time.Minute)`
  (`internal/dhtcrawler/factory.go:106-110`): capacity 1000, **maxBatchSize 1000**, 1-minute
  flush.
- `batchingChannel.batch()` flushes when `len(buffer) >= maxBatchSize`, appending one at a
  time (`internal/concurrency/batching_channel.go:56-59`), so a delivered batch is **≤ 1000
  items**.
- Only hybrids contribute to the v2 set, and the set is deduplicated → **≤ 1000 distinct
  `bytea` values** in the `IN` list. (Realistically far fewer.)
- Postgres extended-protocol bind-parameter limit is **65535**. 1000 `bytea` params for a
  single `IN` is ~1.5% of that. gorm/pgx parameterizes the slice. **Safe and performant**;
  the query rides the plain `info_hash_v2` index (migration 00023:25). **No chunking
  required.**

**Recommendation:** add a self-documenting chunk constant (e.g. `const v2LookupChunkSize =
1000`) and chunk the `IN` defensively, so the lookup stays correct if `maxBatchSize` is ever
raised. Cheap insurance; not a correctness blocker today.

(Unrelated: the persist upsert's `CreateInBatches(..., 100)` at persist.go:111 is a separate
gorm row-batching knob and is untouched by this change.)

### Point 1 — FIRST-ONE-WINS vs prefer-v1 — **DECISION: FIRST-ONE-WINS**

Prefer-v1 (always keep the `A`-keyed row) requires, when the `Bt` row is discovered/persisted
first, **re-keying** the PK from `Bt` to `A`. The cost/risk was verified against the schema:

`torrents.info_hash` is referenced `ON DELETE CASCADE` (but **no** `ON UPDATE CASCADE`
anywhere — `grep "on update" migrations/*.sql` → none) by **6 child tables**:

1. `torrents_torrent_sources` (`migrations/00001_init.sql:48`)
2. `torrent_files` (`00001_init.sql:67`)
3. `torrent_contents` (`00001_init.sql:194`)
4. `torrent_tags` (`migrations/00003_tags.sql:6`)
5. `torrent_hints` (`migrations/00007_torrent_hints.sql:6`)
6. `torrent_pieces` (`migrations/00013_torrent_pieces.sql:6`)

(plus the `torrent_contents` → search/content graph downstream).

Re-keying would mean: manually cascade-update the PK across all 6 children inside a
transaction (no `ON UPDATE CASCADE` to lean on), **and** handle the case where the target PK
(`A`) already has child rows that would collide with the re-keyed children. High complexity,
real data-loss risk, for **marginal** benefit — because the surviving row already stores the
full identity (`info_hash_v1 = A`, `info_hash_v2 = V`) regardless of which PK wins, and magnet
generation reads those fields, not the PK (`internal/model/torrents.go:102-113`).

**FIRST-ONE-WINS** avoids all of it: keep whichever row exists/was-seen first; drop the later
collision. Acceptable and strongly recommended. Determinism: within a batch, slice order
(insertion order) decides; across batches, the already-persisted row wins via the `existingV2`
lookup.

### Point 2 — Should a dropped discovery still contribute its source/scrape? — **DECISION: drop entirely**

Tracing the schema and the source-persist path:

- `TorrentsTorrentSource` PK is `(info_hash, source)` and the crawler always writes
  `source = "dht"` (persist.go:243-247, 315-320). The surviving row's single `(survivingPK,
"dht")` source already exists; the dropped discovery's source would key to the **same**
  `(survivingPK, "dht")` (no second row) **or**, if forwarded to scrape under its own PK, to
  `(Bt, "dht")`.
- The scrape→source persist (`runPersistSources`) guards every insert with
  `gen.Exists(Torrent WHERE info_hash = source.info_hash)` (persist.go:294-298). Since the
  dropped row's PK has **no** `torrents` row, a `(Bt, "dht")` source would be **filtered out**.

So forwarding a dropped discovery to scrape is **pure wasted work** (a scrape RPC whose result
is discarded), and it can never create a useful extra source row. **Drop entirely** — skip
model/files/sources/pieces/classify **and** scrape.

**Known limitation (document, don't fix here):** the v2-swarm seeder/leecher counts
(scrapable only under `Bt`) are not merged into the surviving row, because the schema models a
single `(info_hash, "dht")` source and `Bt` has no torrent row to scrape against. We report
the winning swarm's S/L. Acceptable; note for a future enhancement if v1+v2 swarm-count
merging is ever wanted.

---

## 3. Required design (final)

In `runPersistTorrents` (`internal/dhtcrawler/persist.go:60-96`):

**Pre-pass (once per batch, before the loop):**

```go
// 1. Collect the deduplicated set of full v2 hashes present in this batch.
v2Set := make(map[protocol.InfoHashV2]struct{})
for _, i := range is {
    if v2 := i.metaInfo.InfoHashV2; v2 != nil {
        v2Set[*v2] = struct{}{}
    }
}

// 2. One indexed lookup (chunked defensively at v2LookupChunkSize=1000) of any
//    pre-existing rows already holding those v2 hashes under some PK.
//    SELECT info_hash, info_hash_v2 FROM torrents WHERE info_hash_v2 IN (<v2Set>)
existingV2 := map[protocol.InfoHashV2]protocol.ID{}   // V -> stored PK
// 3. Within-batch tracking of the first PK seen per V.
batchV2 := map[protocol.InfoHashV2]protocol.ID{}      // V -> first PK in this batch
```

`protocol.InfoHashV2` is `[32]byte` (`internal/protocol/infohash_v2.go:23`) → comparable →
valid map key. ✅

**In the loop — ordering is mandatory:**

```go
for _, i := range is {
    // (a) existing exact-PK dedup — UNCHANGED (persist.go:61-63)
    if _, ok := hashMap[i.infoHash]; ok {
        continue
    }

    // (b) NEW: v2 cross-PK dedup — BEFORE the hashMap insert.
    if v2 := i.metaInfo.InfoHashV2; v2 != nil {
        if dropV2Duplicate(*v2, i.infoHash, existingV2, batchV2) {
            c.torrentsDropped.WithLabelValues("v2_duplicate").Inc()
            continue // no model/files/sources/pieces/classify; not in hashMap → not scraped
        }
        batchV2[*v2] = i.infoHash
    }

    // (c) existing hashMap insert + model build — UNCHANGED (persist.go:65 onward)
    hashMap[i.infoHash] = i
    // ... createTorrentModel etc.
}
```

**Pure, unit-testable decision helper:**

```go
// dropV2Duplicate reports whether this discovery is a hybrid already represented
// under a DIFFERENT primary key (first-one-wins). v2 is the full 32-byte hash.
func dropV2Duplicate(
    v2 protocol.InfoHashV2,
    pk protocol.ID,
    existing, batch map[protocol.InfoHashV2]protocol.ID,
) bool {
    if stored, ok := existing[v2]; ok && stored != pk {
        return true
    }
    if stored, ok := batch[v2]; ok && stored != pk {
        return true
    }
    return false
}
```

**Metric** — mirror the `responseDropped` pattern
(`internal/protocol/dht/server/prometheus_collector.go:54-59`,
`server.go:199-203`). Add a `*prometheus.CounterVec` on `crawler`
(`internal/dhtcrawler/crawler.go:57` next to `persistedTotal`), constructed in
`factory.go` (alongside persist.go's `persistedTotal`, `factory.go:52-57`) and exported via
the `group:"prometheus_collectors"` `fx.Out` (`factory.go:44`, `Result` struct):

```
bitmagnet_dht_crawler_torrents_dropped_total{reason="v2_duplicate"}
```

---

## 4. Edge cases & non-goals

- **Historical duplicates** (rows written before G1b2) are **not** retro-cleaned by this
  change; the `existingV2` map arbitrarily keeps one PK per `V` if several pre-exist. A
  one-off cleanup/merge migration is **optional future work** (out of scope for G1b2.1).
- **`existingV2` last-write-wins** when the DB already holds duplicate `V` rows — harmless for
  going-forward dedup.
- The integration test `TestV2DuplicateInfoHashV2Coexist`
  (`persist_v2_integration_test.go:115-144`) asserts DB-level coexistence via **direct
  inserts** (not `runPersistTorrents`) and **remains valid** — it documents that the index
  stays non-unique. G1b2 dedups at the persist layer, not the index. No test breakage.

---

## 5. Required test matrix

**Unit (`dropV2Duplicate`, no DB) — `internal/dhtcrawler`:**

1. `v2 == nil` path (caller guards) → never dropped.
2. `existing[V] == pk` → false (legit same-PK upsert preserved).
3. `existing[V] != pk` → true (drop).
4. `batch[V] == pk` → false.
5. `batch[V] != pk` → true (drop).
6. both maps empty → false.

**Parse-layer (`internal/protocol/metainfo`):** 7. Hybrid fixture hashed/requested via `A` → `InfoHashV2 == V`. 8. Same hybrid requested via `Bt` (truncated) → `InfoHashV2 == V` (identical). (Locks the
Point-4 premise.)

**Integration (`//go:build integration`, extend `persist_v2_integration_test.go`):** 9. Batch with both `A` and `Bt` discoveries of one hybrid → exactly **one** `torrents` row;
`torrents_dropped_total{reason="v2_duplicate"}` == 1; exactly one scrape forwarded. 10. Pre-existing DB row at `A`; new batch contains `Bt` → `Bt` dropped via `existingV2`; still
one row. 11. Pure-v2 discovered twice in one batch (same `Bt` PK) → one row, **no** v2-drop
(handled by the PK `hashMap` skip; assert drop counter == 0). 12. Two genuinely different torrents in a batch → both persisted (no false drop). 13. v1-only + hybrid in the same batch → v1-only never dropped.

---

## 6. Verdict summary

| Design point                                    | Verdict                                                                                                                                     |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. FIRST-ONE-WINS vs prefer-v1                  | **FIRST-ONE-WINS** (prefer-v1 = cross-table re-key over 6 FK tables, no `ON UPDATE CASCADE`, collision risk; identity preserved either way) |
| 2. Dropped discovery contributes source/scrape? | **Drop entirely** (same source key = no-op, or filtered by the `Exists` guard; scrape would be wasted)                                      |
| 3. IN-query batch size / chunking               | Batch ≤ **1000**; `IN` is safe (≪ 65535 param limit); **no chunking required**, add a defensive `v2LookupChunkSize` cap                     |
| 4. `InfoHashV2` set for either hash             | **Confirmed** (parse.go sets it on `info.HasV2()`, independent of matched hash)                                                             |
| 5. Interaction with `hashMap`                   | **Orthogonal**, composes with the ordering in §3                                                                                            |
| 6. No partial state on drop                     | **Guaranteed** — drop before model build _and_ before `hashMap` insert                                                                      |
| 7. `stored != pk` predicate                     | **Correct** — the inequality is essential to preserve same-PK upserts                                                                       |

**Overall: GO**, with the single required ordering change (drop **before** the `hashMap`
insert) and the two locked decisions above.
