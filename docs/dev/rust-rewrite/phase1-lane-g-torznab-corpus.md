# Phase 1 · Lane G — Torznab parity corpus, fixtures & gates (design)

Stage-1 design artifact for Lane G (`p1g-goldens`). Binding parents:
`phase1-tasks.md` (Lane G: G1–G2) and `05-roadmap-and-gates.md §Phase 1`.
Stages 2–3 (the Go golden test G1 and the replay/diff harness G2) implement
against this document; the fixture dataset (`fixtures.jsonl`) and query corpus
(`corpus.jsonl`) committed alongside it are the source of truth.

## 1. What Lane G proves

The roadmap's Phase-1 parity gate has two halves; Lane G owns both harnesses:

- **G1 — golden file (0-diff).** The real Go Torznab adapter, driven over a
  deterministic fixture dataset, must emit XML that is byte-identical (after the
  documented normalization) to a committed golden. The future Rust
  Torznab service (Lane T + Lane Q) normalizes its own output the same way and
  diffs against the same goldens → **0 diffs**.
- **G2 — shadow replay (set/count gates).** A replayer fires the same corpus at
  two live endpoints (Go `:3333/torznab`, Rust service) and diffs the per-query
  infohash **sets + ordering + counts** → **set-match ≥ 0.99, count ≥ 0.98**.
  The live two-endpoint run happens at gate time; Lane G ships the harness plus
  a canned-fixture unit test now.

Lane G owns the **wire format + result-set** contract. It does *not* re-prove the
search-query builder itself — that is Lane Q (`searchquery` crate parity, Q3,
options→infohash list against the same live-PG lane). G1 exercises the builder
only transitively (the adapter's `searchRequestToQueryOptions` → real
`search.TorrentContent` → PG), which is exactly what a Torznab client sees.

## 2. Architecture decision — live-PG golden, in-process gin

The Go Torznab adapter (`internal/torznab/adapter`) is thin: it maps a
`SearchRequest` → `[]query.Option`, calls `search.Search.TorrentContent`, and
maps the `TorrentContentResult` → XML. All filtering, ordering, paging,
full-text relevance and identifier lookup are delegated to the real search over
Postgres. There is **no honest way to exercise the adapter's option mapping
without a real search over real rows** — a mock/in-memory `search.Search` would
have to re-implement search semantics (unverified), and would make every corpus
query return the same rows, defeating the "results are non-trivial per query"
requirement.

Therefore G1's search goldens are a **live-PG integration test**
(`//go:build integration`, `POSTGRES_DSN`, goose-migrated schema), mirroring the
repo's existing `live-PG integration` CI lane (`.github/workflows/rust.yml`) and
Lane Q's Q3. This is the same standard Lane Q uses; the two lanes share the
live-PG lane. "Hermetic" is honored as *deterministic + self-contained*: a fresh
`DROP SCHEMA public CASCADE` + goose-up + seed per run, no wall-clock or network
in the adapter path.

The request path is driven **in-process through the real gin engine**, exactly
like `internal/torznab/httpserver/handler_test.go` constructs it (httptest
recorder + `engine.ServeHTTP`), but with the **real** adapter wired as the
`torznab.Client` instead of a mock:

```
s      := search.New(search.Params{Query: lazy.New(func() (*dao.Query, error) { return dao.Use(db), nil })}).Search  (lazy → search.Search)
client := adapter.New(s)                          // adapter.Adapter implements torznab.Client
engine := gin.New(); httpserver.New(lazy.New(func() (torznab.Client, error) { return client, nil }), torznab.Config{}.MergeDefaults()).Apply(engine)
```

Firing each corpus `path` through `engine.ServeHTTP` covers the full stack:
handler param parsing (`t=`, `q=`, `cat=`, `imdbid=`, `tmdbid=`, `season=`,
`ep=`, `limit=`, `offset=`, CSV/edge parsing) → real adapter → real search → PG
→ XML — the same code a Prowlarr/*arr client hits.

**Caps is split out as a DB-free golden.** `profile.Caps().XML()` needs no
database, so the caps golden lives in a plain (non-integration) test that runs in
the fast `golden-files` CI job, matching the Phase-0 golden discipline. Only the
search corpus needs the live-PG lane.

> Reuse note: this **extends** the classifier-corpus discipline (stable ids,
> JSONL fixtures, `-update` regen, deterministic outputs) rather than reusing its
> mechanism — the classifier corpus is pure/in-memory; Torznab needs DB-backed
> search, so the fixtures seed Postgres.

## 3. Fixture dataset (`fixtures.jsonl`, 25 rows)

One JSON object per row. The seeder (G1) reads this file and, per row, inserts:
`torrents` (Name carries the search tokens, weight A) → optional
`torrents_torrent_sources` (only when `seeders`/`leechers` present — this is what
makes `Torrent.Seeders()/Leechers()` non-null, the source of the seeders/leechers/peers
attrs) → `torrent_files` **and** a `files_data` blob via
`blobmigration.SerializeFiles` (the default Torznab hydrator does **not** preload
file rows and the legacy-read gate blocks it, so the `files` attr comes only from
the blob — import `blobmigration` to register `model.FilesDataDeserializer`) →
optional `content` (canonical `tmdb`, `Title`=`contentTitle`, `ReleaseYear`=`year`;
`UpdateTsv()`) → optional `content_attributes` (`source=imdb, key=id, value=imdb`)
→ `torrent_contents` (never set `id`; call `UpdateTsv()` with Torrent+Content
attached, then detach before Create, per `processor/persist.go`).

Field vocabulary (mapped to model enums by the seeder):
`contentType` `movie|tv_show|music|ebook|comic|audiobook|xxx|software|game` (omit
→ null content_type → category "Unknown"/8000); `videoResolution`
`480p|720p|1080p|1440p|2160p` → `VideoResolutionV*`; `video3d` `3d|3d-sbs|3d-ou`
→ `Video3DV3D|V3DSBS|V3DOU`; `videoCodec` `x264|x265|H264|XviD|...` →
`model.VideoCodec`; `episodes` `{"<season>":[<ep>,...]}` (empty list = whole
season) → `model.Episodes`.

Coverage across the 25 rows:

| dimension | how it is covered |
|---|---|
| content types | movie ×8, tv_show ×6, music ×3, ebook ×2, comic ×1, audiobook ×1, xxx ×1, software ×1, game ×1, null ×1 |
| video resolution | 480p, 720p, 1080p, 2160p (for SD/HD/UHD category buckets) |
| video 3d | `mov-vertex-3d` (3d-sbs) for the Movies/3D bucket |
| seeders/leechers/peers | present on most; `mov-comet-480` has **no source row** (attrs absent); `mus-solaris-lp` has leechers=0 (attr `0`, peers=seeders) |
| files attr | single-file and multi-file (3/5/12/2 files); count = len(files) from the blob |
| year attr | only rows with a `content` row + `year` (movies/tv with tmdb) |
| season/episode attrs | all tv rows carry `episodes` |
| imdb/tmdb attrs + search | movies `101/tt1010101`, `104/tt1040404`, `106`; tv `201/tt2010101`, `202`, `204` |
| team (release_group) | AXIS/LUME/ZONE/FLEET/DRACO/... on a subset |
| video (codec) attr | x264/x265/H264/XviD on a subset |

### Determinism invariants (load-bearing)

1. **Unique `pub` (published_at rank), 1..25.** Default order is
   `published_at DESC` with **no tie-breaker** (`TorrentContentDefaultOption`);
   distinct published_at → a total, stable order. The seeder maps
   `pub` → a distinct timestamp (e.g. `base + pub·24h`). `fixtures.jsonl` is
   listed in descending `pub`, so file order == default search order.
2. **Relevance ordering has no tie-breaker** (`ts_rank_cd` only). The only
   multi-row full-text query, `q=nebula`, matches four rows whose token counts
   are **pairwise distinct** (1/2/3/4 occurrences of `nebula` in the torrent
   Name) → strictly ordered ranks. `contentTitle`s deliberately avoid the query
   tokens so the content→torrent_content tsv copy cannot perturb the counts.
   Every other `q=` query is single-match.
3. **Infohashes are derived deterministically from the fixture id** (the seeder
   hashes the id to 20 bytes), so goldens and the replay harness share stable
   hashes without hard-coding them.

## 4. Query corpus (`corpus.jsonl`, 51 queries)

Each line: `{id, kind: caps|search|error, path, desc, dims, expectIds?}`.
Golden filename derives from id: `caps` → `caps.golden.xml`, else
`q-<id>.golden.xml`. `expectIds` is the **design oracle** — the ordered fixture
ids the query is designed to return.

> **Honesty rule for G1:** the generated golden is the recorded truth, but if the
> real adapter's output diverges from `expectIds`, the implementer must **STOP
> and report** the divergence, not silently accept it — it means either the
> fixture/oracle has a bug (I fix it) or the adapter behaves differently than
> modeled (needs review). Do not `-update` past an `expectIds` mismatch.

Coverage (51 = 1 caps + 48 search + 2 error):

| group | queries |
|---|---|
| caps | `caps` |
| t=search core | all / q(relevance) / q+limit / q(empty) / limit / limit+offset / limit-clamp |
| t=search categories | 2000, 2030, 2040, 2045, 2060, 5000, 5040, 3000, 3030, 4000, 4050, 6000, 7000, 7020, 7030, 2000,5000 (multi), 8000 (noop edge), non-numeric edge |
| t=search identifiers | imdbid, tmdbid, q+cat |
| t=movie | all / q / imdbid / tmdbid / cat=2040 / paging / offset-beyond(empty) |
| t=tvsearch | all / q / season / season+ep / imdbid / tmdbid / cat=5045 / season-nonnumeric edge |
| t=music | all / q |
| t=book | all / q / cat=7020(redundant edge) |
| errors | missing t= (code 200) / unknown function (code 202) |

Deliberately included **behavioral edges** the Rust side must reproduce: the
lossy category maps (`4050`≡PC, `7020`≡Books), the no-op `8000`, non-numeric
`cat`/`season` dropped by `Atoi`, `limit` clamp to MaxLimit, and the redundant
double-criteria on `t=book&cat=7020`.

**v1 scope boundary:** default profile only. Non-default profiles (`Tags`
filtering, alternate limits) would couple fixtures to `torrent_tags` seeding;
deferred and noted here. Profile *caps* parity is Lane T's concern (T1).

## 5. Normalization spec (canonical Torznab XML)

Both sides serialize with different XML writers (Go `encoding/xml` +
`xml.Header`; Rust quick-xml). The golden stores the **canonical normal form**;
each side parses its own output and re-serializes with these rules, then byte-
compares. Implemented once in Go (`internal/parity/torznab_xml.go`,
`NormalizeTorznabXML`) and mirrored in Rust by Lane T. Rules:

1. **Drop the XML declaration** (`<?xml ...?>`). The canonical form has none.
2. **Re-serialize from the parsed tree**, pretty-printed: 2-space indent per
   depth, one element per line, LF endings, single trailing newline (mirrors the
   SDL golden's deterministic layout).
3. **Attributes are sorted by name** (full name including any `xmlns:`/prefix),
   with values escaped canonically (`& < > "` → `&amp; &lt; &gt; &quot;`).
   Attribute order is not semantically meaningful, so sorting is safe and erases
   writer differences (e.g. namespace-attr ordering on `<rss>`).
4. **Child element order is preserved** (document order). Ordering **is** the
   parity contract for `<item>`s (ranked results) and reflects struct field order
   elsewhere — never sort children.
5. **Whitespace-only text nodes are dropped** (indentation is insignificant).
   Leaf text is preserved verbatim after XML-unescape → canonical re-escape.
   Torznab output has no mixed content. CDATA is normalized to escaped text
   (defensive; bitmagnet emits none).
6. **Empty elements** render uniformly as `<name/>` regardless of whether the
   source wrote `<name/>` or `<name></name>`.
7. **`omitempty` presence is not synthesized.** The normalizer never adds or
   removes elements; element presence is the contract and each side must match
   Go's `omitempty` semantics (Lane T's job).

### Parity traps to freeze (watch items for Lane T)

- **Zero-value channel dates.** The adapter leaves `channel/pubDate` and
  `channel/lastBuildDate` at the zero `RSSDate`; `encoding/xml`'s `omitempty`
  does not apply to struct-typed fields, so Go emits them formatted as
  `Mon, 01 Jan 0001 00:00:00 +0000`. Constant and deterministic — the golden
  freezes it; Rust must emit the same.
- **`<error>` attribute name.** `torznab.Error.Code` is tagged
  `xml:"error,attr"`, so the code attribute is literally `error` (not `code`):
  `<error error="200" description="..."/>`. Surprising but real; the golden
  freezes it.
- **Enclosure vs guid.** `guid` = infohash string; `enclosure@url` = magnet URI;
  `torznab:attr name="infohash"` = infohash. G2 extracts infohashes from the
  `torznab:attr name="infohash"` values (falls back to `guid`).

## 6. Gates

- **G1:** normalize(Go adapter XML) == committed golden, per corpus entry →
  **0 diffs**. `-update` regenerates. Assert side re-runs the adapter (needs the
  live-PG lane for search goldens; caps golden is DB-free).
- **G2:** per query, set-match = Jaccard(infohash sets); count parity =
  min(count)/max(count) (1.0 when both empty); ordering diff reported alongside.
  Aggregate over the corpus → **mean set-match ≥ 0.99, mean count ≥ 0.98**. The
  unit test feeds two canned XML pairs (one identical → 1.0/1.0 pass, one
  divergent → below-gate fail) to exercise the diff without HTTP.

## 7. Deliverables & file layout (ownership: Lane G)

```
testdata/parity/torznab/fixtures.jsonl        # this stage — fixture dataset (25 rows)
testdata/parity/torznab/corpus.jsonl          # this stage — query corpus (51)
testdata/parity/torznab/caps.golden.xml       # G1 — generated (DB-free)
testdata/parity/torznab/q-*.golden.xml        # G1 — generated (live-PG)
internal/torznab/caps_parity_test.go          # G1 — caps golden (DB-free, new file)
internal/torznab/search_parity_test.go        # G1 — search goldens (//go:build integration, new file)
internal/parity/torznab_xml.go                # shared — NormalizeTorznabXML + ExtractInfohashes (new file)
internal/parity/torznab_xml_test.go           # shared — normalizer unit tests (new file)
internal/parity/torznab_corpus.go             # shared — corpus/fixture loaders (new file)
internal/parity/torznab_replay.go             # G2 — replay/diff harness (new file)
internal/parity/torznab_replay_test.go        # G2 — canned-XML unit test (new file)
docs/dev/rust-rewrite/phase1-lane-g-torznab-corpus.md  # this doc
```

Only new files; no edits to existing Go under `internal/torznab/**` or the
existing `internal/parity` Phase-0 harness. CI wiring (adding the search golden
to the live-PG job, caps golden to the fast `golden-files` job) touches
`.github/workflows/**`, which is outside Lane G's file set — flagged for the team
lead / Lane I.
