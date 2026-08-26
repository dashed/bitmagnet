# Torznab Go → Rust mirror-spec (Lane T, Phase 1)

Authoritative transcription of the Go Torznab adapter that `bitmagnet-torznab`
mirrors. Source of truth is `internal/torznab/**` at base `611d3177` — every
byte-parity claim below is traced to a Go file/line. Lane G's real goldens
(`testdata/parity/torznab/*.golden.xml`) supersede any disagreement; this doc is
the contract the crate is built against until they land.

**Split reminder:** this crate owns HTTP routing, param parsing, category
mapping, XML rendering, metrics, and the goose boot-assert. Query *construction*
(predicate/order/limit → SQL) is `bitmagnet-search-query` (Lane Q). The seam is
called out in §7.

---

## 1. Routing & request surface

- Single route: `GET /torznab/*any` (`internal/torznab/httpserver/httpserver.go:36`).
- **Profile resolution** (`handler.go:167` `getProfile`): take `c.Param("any")`,
  `strings.Trim(.., "/")`, `strings.Split(.., "/")[0]`, `strings.ToLower`.
  - `""`, `"api"`, `"default"` → `ProfileDefault`.
  - else look up by ID via `Config.GetProfile` (case-insensitive `EqualFold`,
    `config.go:25`). Miss → `profileNotFoundError` → **HTTP 404**, plain-text
    body `profile not found: <name>\n` (NOT XML — `handler.go:137` `writeHTTPError`).
  - So `/torznab/`, `/torznab/api`, `/torznab/default`, `/torznab/test`,
    `/torznab/test/api`, `/torznab/test/api/` all resolve (see `handler_test.go`
    `TestCaps` cases).
- **Dispatch on `t=`** (`handler.go:27`):
  - `t` absent/empty → torznab `Error{200, "missing parameter (t)"}` (see §6).
  - `t=caps` → caps document (§3).
  - anything else → search path (§4/§5); the *function validity* check (is it
    `search`/`movie`/`tvsearch`/`music`/`book`?) happens downstream in the query
    layer, NOT here — an unknown function yields `Error{202, ...}` (§6, §7).

## 2. Accepted query parameters (`internal/torznab/parameters.go`)

Exact keys — **no aliases**. Unknown params are ignored.

| Key | Const | Type / parse rule |
|-----|-------|-------------------|
| `t` | `ParamType` | string; function selector (§1) |
| `q` | `ParamQuery` | string; empty string ⇒ no search-string predicate |
| `cat` | `ParamCat` | repeatable **and** CSV: `QueryArray("cat")` then `strings.Split(v, ",")`; each token `strconv.Atoi`, **non-integer tokens silently dropped** (`handler.go:47`) |
| `imdbid` | `ParamIMDBID` | string; non-empty ⇒ `NullString` set |
| `tmdbid` | `ParamTMDBID` | string; non-empty ⇒ `NullString` set |
| `season` | `ParamSeason` | int via `Atoi`; **only parsed when non-empty AND `Atoi` succeeds**; episode is only read *inside* a valid season block (`handler.go:70`) |
| `ep` | `ParamEpisode` | int via `Atoi`; note the key is **`ep`**, not `episode` |
| `limit` | `ParamLimit` | int via `Atoi`; set only when `err==nil && v>0` (`handler.go:85`) |
| `offset` | `ParamOffset` | int via `Atoi`; set whenever `Atoi` succeeds (incl. 0/negative→uint wrap — mirror Go's `uint(intOffset)` cast) (`handler.go:91`) |

Season/episode coupling: `episode` is captured only if `season` parsed OK first.
A bare `ep=` with no `season=` is dropped.

## 3. Caps document (`t=caps`)

Built by `Profile.Caps()` (`profile.go:41`), marshalled by `objToXML`
(`xmlutil.go:11`): `xml.MarshalIndent(obj, "", "  ")` prefixed with
`xml.Header` = `<?xml version="1.0" encoding="UTF-8"?>\n`. HTTP 200,
`Content-Type: application/xml; charset=utf-8`.

Structure (element order fixed by Go struct field order, `caps.go`):

```
<caps>
  <server title="{Profile.Title}"></server>
  <limits max="{MaxLimit}" default="{DefaultLimit}"></limits>
  <searching>
    <search available="yes" supportedParams="q,imdbid,tmdbid"></search>
    <tv-search available="yes" supportedParams="q,imdbid,tmdbid,season,ep"></tv-search>
    <movie-search available="yes" supportedParams="q,imdbid,tmdbid"></movie-search>
    <music-search available="yes" supportedParams="q"></music-search>
    <audio-search available="no"></audio-search>
    <book-search available="yes" supportedParams="q"></book-search>
  </searching>
  <categories>
    ...TopLevelCategories...
  </categories>
  <tags></tags>
</caps>
```

- `search`/`tv-search`/`movie-search`/`music-search`/`audio-search`/`book-search`
  order is the `CapsSearching` struct order (`caps.go:32`). `supportedParams` is
  `omitempty` — omitted when `available="no"` (audio-search).
- `CapsLimits.Max`/`Default` are `uint,omitempty`; `ProfileDefault` = 100/100.
- **`<tags>` always renders as an empty element** (`Tags string`, no omitempty).
- The repo's `internal/torznab/examples/caps1.xml` is a *foreign reference file*,
  NOT bitmagnet output — different order/params. Do NOT treat it as a golden.

### Category tree (`categories.gen.go`, `TopLevelCategories`)

Emitted in this order, each `<category id name>` with nested `<subcat id name>`:

| id | name | subcats (id/name) |
|----|------|-------------------|
| 2000 | Movies | 2030 Movies/SD, 2040 Movies/HD, 2045 Movies/UHD, 2060 Movies/3D |
| 3000 | Audio | 3030 Audio/Audiobook |
| 4000 | PC | 4050 PC/Games |
| 5000 | TV | 5030 TV/SD, 5040 TV/HD, 5045 TV/UHD |
| 6000 | XXX | 6070 XXX/Other |
| 7000 | Books | 7020 Books/EBook, 7030 Books/Comics |
| 8000 | Other | (none) |

`Profile.Caps()` uses `Categories: CapsCategories{Categories: TopLevelCategories}`.
Leaf categories carry `Subcat: []Subcategory{}` (empty slice ⇒ no child elements).

## 4. Category-ID → search-criteria mapping (`adapter/search_options.go:59`)

Lane T owns this mapping; it produces abstract criteria the query crate turns
into SQL. For each `cat` in `r.Cats`, `switch` on `Category.Has(cat)` (a
top-level matches itself or any subcat, `category.go:15`):

- **CategoryMovies (2000)**: add `ContentType=Movie` *unless* (`t=movie` AND
  `cat != 2000`). Then sub-switch:
  - 2030 → `VideoResolution ∈ {480p}`
  - 2040 → `VideoResolution ∈ {720p,1080p,1440p,2160p}`
  - 2045 → `VideoResolution ∈ {2160p}`
  - 2060 → `Video3D ∈ {3D,3DSBS,3DOU}`
- **CategoryTV (5000)**: add `ContentType=TvShow` unless (`t=tvsearch` AND
  `cat != 5000`). Sub-switch: 5030→480p, 5040→{720p,1080p,1440p,2160p}, 5045→2160p.
- **CategoryXXX (6000)**: `ContentType=Xxx`.
- **CategoryPC (4000)**: `ContentType ∈ {Software,Game}`.
- **CategoryAudioAudiobook (3030)**: `ContentType=Audiobook`.
- **CategoryAudio (3000)**: `ContentType=Music`. (3030 is matched by the
  Audiobook arm *first* — Go `switch` order matters: Audiobook before Audio.)
- **CategoryBooksComics (7030)**: `ContentType=Comic`.
- **CategoryBooks (7000)**: `ContentType ∈ {Ebook,Comic,Audiobook}` — NB this arm
  appends directly to `options` (a top-level `Where`), not to `catCriteria`
  (`search_options.go:136`); a quirk to preserve.

Per-cat criteria are `And`-combined, and the set of cats is `Or`-combined into one
`Where` (`search_options.go:144`). `t=` function itself also injects a content-type
criterion (search=none, movie=Movie, tvsearch=TvShow[+episodes], music=Music,
book={Ebook,Comic,Audiobook}) — that part is the query crate's (§7).

## 5. Search result feed (`adapter/search_result.go`, `result.go`)

`SearchResult` → `objToXML` (same header + 2-space indent). Root:

```
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:torznab="http://torznab.com/schemas/2015/feed">
  <channel>
    <title>{Profile.Title}</title>
    <pubDate>...</pubDate>          (RSSDate, ALWAYS rendered — see §5.3)
    <lastBuildDate>...</lastBuildDate>
    <newznab:response xmlns="http://www.newznab.com/DTD/2010/feeds/attributes/" offset="{req.Offset}"></...>
    <item>...</item>
  </channel>
</rss>
```

Channel fields are mostly `omitempty` (Link/Description/Language/Docs/Generator)
and left unset by the adapter (`search_result.go:20`) — so only `title`,
`pubDate`, `lastBuildDate`, `response`, and `item`s appear. **`Total` is
commented out** (`search_result.go:25`) so `response` carries only `offset`
(itself `omitempty`, so offset=0 ⇒ bare `<response .../>`).

### 5.1 `<item>` fields (`result.go:109`, order fixed by struct)

| element | source | notes |
|---------|--------|-------|
| `title` | `Torrent.Name` | required |
| `guid` | `InfoHash.String()` | omitempty |
| `pubDate` | `RSSDate(PublishedAt)` | RSSDate, omitempty (but struct ⇒ never omitted, §5.3) |
| `category` | content-type `Label()` or `"Unknown"` | omitempty |
| `size` | `Torrent.Size` (uint) | required |
| `enclosure` | url=`MagnetURI()`, length=size, type=`application/x-bittorrent;x-scheme-handler/magnet` | attrs |
| `torznab:attr`* | see §5.2 | repeated |

`link`/`description`/`comments` are omitempty and unset ⇒ absent.

### 5.2 `torznab:attr` emission order (`search_result.go:63`)

Emitted as literal `<torznab:attr name=".." value=".."></torznab:attr>` (the
`torznab` prefix is declared once on `<rss>`; Go does not otherwise bind it).
**Order is load-bearing:**

1. `infohash` — `Torrent.InfoHash.String()` — always
2. `magneturl` — `Torrent.MagnetURI()` — always
3. `category` — the mapped category ID as decimal string — always
4. `size` — `Torrent.Size` decimal — always
5. `publishdate` — `PublishedAt.Format("Mon, 02 Jan 2006 15:04:05 -0700")` — always
6. `seeders` — if `Seeders().Valid`
7. `leechers` — if `Leechers().Valid`
8. `peers` — if seeders **and** leechers valid (= leechers+seeders)
9. `files` — if `len(Torrent.Files) > 0`
10. `year` — if `Content.ReleaseYear` not nil
11. `season` — if `len(Episodes) > 0` (first season entry)
12. `episode` — if that first season has episodes (first episode)
13. `video` — if `VideoCodec.Valid` (codec `Label()`)
14. `resolution` — if `VideoResolution.Valid` (resolution `Label()`)
15. `team` — if `ReleaseGroup.Valid`
16. `tmdb` — if content has a `tmdb` identifier
17. `imdb` — if content has an `imdb` identifier, **value stripped of the `tt`
    prefix** (`imdbID[2:]`, `search_result.go:170`)

The item-level `category` element (§5.1) is the content-type **label** string
("Movies", "TV shows", …, or "Unknown"); the `category` *attr* (#3) is the
numeric **ID** (`CategoryOther=8000` default, mapped per content type at
`search_result.go:38`). Do not conflate them.

### 5.3 RSS date format

`RssDateDefaultFormat = "Mon, 02 Jan 2006 15:04:05 -0700"` (`result.go:52`).
Used for `pubDate`/`lastBuildDate` elements and the `publishdate` attr.
`RSSDate` is a `time.Time` newtype; because it is a struct, Go's `omitempty` never
elides it — a zero `SearchResult{}` still emits `<pubDate>Mon, 01 Jan 0001
00:00:00 +0000</pubDate>` and `<lastBuildDate>…</lastBuildDate>` (confirmed by
`handler_test.go` `TestSearch`, which diffs against `result.XML()` on an empty
result). Rust must reproduce this "always present, zero-time formatted" behaviour.

## 6. Error XML (`errors.go`)

`Error` marshals to `<error error="{Code}" description="{Description}"></error>`
— note the attribute name is literally **`error`** (struct tag `xml:"error,attr"`,
the `//revive:disable-next-line` quirk), NOT `code`. Header + indent as usual.
**Torznab errors are written with HTTP 200** (via `writeXML`, `handler.go:128`),
not 4xx. Known codes this adapter emits:

| trigger | code | description |
|---------|------|-------------|
| missing `t=` | 200 | `missing parameter (t)` |
| unknown function `t=foo` | 202 | `no such function (foo)` (from the query layer, §7) |

Non-torznab errors (encode failure, search backend error) fall to
`writeHTTPError`: HTTP 500 (or 404 for profile-not-found), plain-text body
`<err>\n`. The `fmt.Errorf("failed to search: %w", …)` wrapper is transparent —
`errors.As` unwraps the inner `torznab.Error`, so the 202 XML is emitted cleanly
without the wrapper prefix.

## 7. Cross-lane seam (Lane Q — `bitmagnet-search-query`)

Everything above is Lane T's except **query construction**. The boundary object
is a `TorznabSearchParams`-shaped struct (Lane Q owns its definition, committed
early per the cross-lane contract). Lane T:

1. parses HTTP → fills that struct (function `t`, `q`, resolved cat criteria,
   imdb/tmdb, season/episode, limit clamped to `[…,MaxLimit]` with `DefaultLimit`
   fallback, offset, profile tags);
2. calls Lane Q's builder → gets result rows **or** a typed "unknown function"
   error that Lane T renders as `Error{202,…}`.

Facts Lane T must feed the builder, mirrored from `search_options.go`:
- limit default `Profile.DefaultLimit`, clamped down to `Profile.MaxLimit`
  (`ProfileDefault` 100/100; a profile may raise both);
- `q!=""` ⇒ search-string predicate + order-by (relevance desc, or published-at
  desc when `Profile.DisableOrderByRelevance`) — **FIND-2 default ordering** lives
  here;
- profile `Tags` ⇒ `TorrentTagCriteria`;
- imdb id normalised to a `tt`-prefix before lookup (`search_options.go:155`);
- `TorrentContentDefaultOption()` + `WithTotalCount(false)` baseline
  (`adapter.go:22`) — total is deliberately not computed (why `response` omits it).

Lane Q's production `build_query`/`fetch` API is live, including content-join
hydration for release year, IMDb, and TMDB identifiers. The production
`pg_router` seam uses that real PostgreSQL client; fixture-only tests retain the
injectable `SearchClient` boundary.

## 8. Byte-parity normalization surface (for Lane G goldens)

`encoding/xml` vs `quick-xml` differ in ways the golden diff must normalize
(roadmap: "0 diffs after namespace/whitespace normalisation"):

1. **Empty elements**: Go emits `<foo></foo>`; quick-xml/serde tends to
   self-close `<foo/>`. Force expanded form or normalize both sides.
2. **Attribute/text escaping**: Go escapes `"`→`&#34;`, `'`→`&#39;`, `\t`→`&#x9;`,
   `\n`→`&#xA;`, `\r`→`&#xD;`; quick-xml defaults to `&quot;`/`&apos;`. Torrent
   names (item `title`, `dn=` in the magnet) are the live blast radius.
3. **Namespaced `response`**: Go renders the newznab-namespaced element with an
   `xmlns="http://www.newznab.com/DTD/2010/feeds/attributes/"` attribute; the
   Rust writer must place the same xmlns (and NOT re-declare the torznab prefix
   on every `<torznab:attr>`).
4. **Indent**: 2 spaces, `\n` line endings, `xml.Header` + trailing newline shape.
5. **`torznab:attr` literal prefix**: emit the literal `torznab:attr` QName; do
   not let the serializer rewrite it to a generated `ns0:` prefix.

These are the target invariants for T1's serializer; the marked
`*.golden.xml` integration test is `#[ignore]`/skip-if-absent until G1 lands.
