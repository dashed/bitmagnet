# G1b2.4 — Adversarial code review: hybrid-vs-v2 row dedup

**Branch:** `feat/bittorrent-v2-dedup`
**Reviewer:** adversarial code reviewer (G1b2.4), read-only
**Scope:** `internal/dhtcrawler/{persist.go,crawler.go,factory.go}`, tests
`dedup_v2_test.go`, `dedup_v2_integration_test.go`
**Verdict:** **GO** — no correctness blocker found. 3 OPTIONAL items below.

---

## Verification performed

- `go build ./internal/dhtcrawler/` → OK
- `go vet ./internal/dhtcrawler/` → OK
- `go vet -tags integration ./internal/dhtcrawler/` → OK (integration test compiles)
- `go test ./internal/dhtcrawler/ -run 'TestDropV2Duplicate|TestFilterV2Duplicates'`
  → **PASS** (all 11 subtests)
- `gofmt -l` on all 5 changed/added files → clean
- `golangci-lint` v2 could **not** run locally (installed binary is v1.64.8; repo
  config is `version: "2"`). Reasoned manually instead — see §Lint.

---

## Findings against the hunt list

### 1. Drop ordering / zero partial state — **PASS (better than spec)**

The implementation drops in a **pre-pass** (`filterV2Duplicates`, persist.go:67)
that runs entirely **before** the loop, returning only `kept`. The loop
(persist.go:72) iterates `kept`, so a dropped item:

- never reaches `hashMap[i.infoHash] = i` (persist.go:77),
- never reaches `createTorrentModel` / files / sources / pieces / `hashesToClassify`
  (all inside the loop body, persist.go:79–106),
- is **not** in `hashMap`, so the scrape-forward loop (persist.go:152–159) never
  forwards it.

Zero partial state, not scraped. This is cleaner than the spec's in-loop design
and fully satisfies spec Point 6. ✓

### 2. `lookupExistingV2` correctness — **PASS**

- `t.InfoHashV2.In(values...)` — `field.Field.In(values ...driver.Valuer)`
  (confirmed `gorm.io/gen@v0.3.26/field/field.go:29`). `values` is
  `[]driver.Valuer`, spread is valid Go and compiles (build passed). ✓
- `protocol.InfoHashV2.Value()` has a **value** receiver returning `h[:]`
  (infohash_v2.go:90) → 32-byte `bytea` IN params, rides the non-unique
  `info_hash_v2` index. A value ranged from the map key satisfies `driver.Valuer`. ✓
- `Select(t.InfoHash, t.InfoHashV2)` selects the right two columns; V→PK mapping
  `existing[*row.InfoHashV2] = row.InfoHash` is correct; the `row.InfoHashV2 != nil`
  guard (persist.go:360) is defensively redundant (the `IN` filter excludes NULLs). ✓
- Chunk bounds: `for start := 0; start < len(values); start += v2LookupChunkSize`,
  `end := min(start+chunk, len(values))` — no off-by-one; chunk = 1000 = current
  batch cap, ≪ the 65535 bind-param limit. ✓
- Early return when `len(v2Set) == 0` (persist.go:336) → **no query** for v1-only
  batches. Not N+1: one (chunked) query per batch.
- **Fail-open** (persist.go:353–357): on error returns the partial map
  accumulated so far (empty on first-chunk error). A partial map only **misses**
  drops — it can never cause a **false** drop — and within-batch dedup still runs.
  Never blocks persistence; degrades to pre-G1b2 (non-unique index tolerant)
  behavior. **Correct call.** ✓

### 3. First-one-wins determinism — **PASS** (with one historical-data caveat → O1)

Within a batch: slice order decides via `batch[V]`, deterministic, only true
cross-PK collisions dropped (covered by tests). Across batches: the DB row wins via
`existing`. No legitimate distinct torrent is dropped in normal operation (SHA-256
distinct ⇒ distinct V). See **O1** for the historical-duplicate edge.

### 4. `stored != pk` (same-PK re-discovery never dropped) — **PASS**

`dropV2Duplicate` (persist.go:281–287) returns false when `stored == pk` for both
the `existing` and `batch` maps. Pure-v2 (one truncated PK) and hybrid-via-same-hash
re-discoveries upsert, never drop. Locked by unit test
`existing same pk (legit upsert)` and integration test
`TestV2DedupSamePKUpsertNotDropped`. ✓

### 5. Metric wiring / nil-deref — **PASS**

`torrentsDropped` is: constructed in `New()` (factory.go:60–65), assigned to the
crawler in the `OnStart` hook (factory.go:135), and exported via
`Result.TorrentsDropped prometheus.Collector \`group:"prometheus_collectors"\``(factory.go:45,155). Registered. The increment is guarded by`if droppedV2 > 0`
(persist.go:68), and production has a single construction path that always sets the
field. No nil-deref in any existing/added test (the integration helper builds its
own counter, dedup_v2_integration_test.go:29). Only a hypothetical future
bare-`crawler{}`test that drives`runPersistTorrents` through a drop would panic →
**O3**.

### 6. Concurrency — **PASS**

`runPersistTorrents` is started once (crawler.go:78) → single goroutine; `existingV2`,
`batch`, `hashMap` are batch-local. `prometheus.CounterVec` is itself goroutine-safe.
Cross-**process** TOCTOU (multiple bitmagnet instances) is tolerated by the
non-unique `info_hash_v2` index — a duplicate may slip through, matching pre-G1b2
behavior. ✓

### 7. Performance — **PASS**

One extra **indexed** lookup per batch, skipped entirely for v1-only batches, chunked
defensively. No N+1. Negligible.

### Lint / style — **PASS (local v2 unavailable; reasoned)**

`gofmt -l` clean on all files. `gofumpt` v0.10.0 (local) flags reformatting, but
**only on pre-existing untouched lines** (`flushHashesToClassify` closure,
`createTorrentModel` call, `factory.go` channel constructors) — confirmed via
`git diff main` that this branch added none of those lines. This is local-gofumpt
version drift vs CI's pinned gofumpt, **not introduced by this PR** (those lines are
already in `main` and passing CI). The **new** code (helpers, pre-pass, lookup) is
gofumpt-clean. New code follows the file's existing `wsl`/`whitespace` conventions and
`prealloc`s its slices (`kept`, `values` with `make(..., 0, n)`). No `interface{}`
literals introduced; `struct{}` set + typed `[]driver.Valuer` are idiomatic.

---

## Required fixes

**None.** The implementation matches the spec, the required ordering constraint is
satisfied (stronger: drop happens in a pre-pass), the metric is registered, and all
builds/vets/unit-tests pass.

## Optional fixes

- **O1 — `lookupExistingV2` nondeterminism on pre-existing duplicate V rows.** The
  query has no `ORDER BY`; `existing[*v2] = pk` is last-write-wins. If the DB already
  holds _two_ rows for one V (a historical pre-G1b2 duplicate), a legitimate same-PK
  re-discovery of one of them can be transiently dropped depending on row order
  (`stored` may resolve to the _other_ PK). **No data loss** — the drop only skips the
  discovery's upsert/scrape for that batch, never deletes the row — and it requires the
  exact duplicate state this feature prevents going forward (spec §4 scopes historical
  dupes out). Fix if cheap: add deterministic ordering, or prefer the `stored` PK that
  equals the incoming `pk` before declaring a collision.
- **O2 — Integration coverage gap.** The new integration tests exercise
  `lookupExistingV2` + `filterV2Duplicates` + the metric **directly**, never the
  `runPersistTorrents` loop itself (the pre-pass call site, the kept-iteration, the
  scrape-skip). The spec deliberately scoped out a full-channel harness; the pure
  helpers are well covered. Optional: one test asserting that a dropped item leaves no
  `torrents` row **and** is not forwarded to scrape via the real loop.
- **O3 — Defensive nil-guard.** `c.torrentsDropped` would nil-deref if a future test
  built a bare `crawler{}` and drove `runPersistTorrents` through a drop. Production is
  safe (single init path) and the guard `if droppedV2 > 0` shields no-drop batches.
  Low value; note only.

---

## Verdict: **GO**

Ship as-is. O1–O3 are non-blocking; O1 is the only one with any correctness flavor and
is bounded to a historical-data state the feature itself eliminates.
