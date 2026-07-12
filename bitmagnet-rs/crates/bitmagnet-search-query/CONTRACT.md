# `bitmagnet-search-query` — v1 contract (Q1)

The exact predicate / ordering / limit subset the Go Torznab adapter exercises,
traced from `internal/torznab/adapter/*` down into
`internal/database/{search,query}`, and how it maps onto this crate's public
API. This is the binding spec for Q2 (SQL port) and Q3 (parity). Lane T
(`bitmagnet-torznab`) codes against the public API summarised in §5.

Base commit: `611d3177`. Read-only Go reference (no changes there):
`internal/torznab/**`, `internal/database/search/**`,
`internal/database/query/**`, `internal/model/**`.

---

## 1. The Go call path

`adapter.Search` (`internal/torznab/adapter/adapter.go`) builds a `[]query.Option`
and calls `search.TorrentContent(ctx, options...)`:

```go
options := []query.Option{search.TorrentContentDefaultOption(), query.WithTotalCount(false)}
options = append(options, searchRequestToQueryOptions(req)...)   // the Torznab-specific part
searchResult, _ := a.search.TorrentContent(ctx, options...)
return torrentContentResultToTorznabResult(req, searchResult)
```

Two fixed options frame every Torznab query:

- `search.TorrentContentDefaultOption()` = `query.DefaultOption()` (`Limit(10)` +
  aggregation budget 5000) + default hydration + **core joins registered** +
  **default order `torrent_contents.published_at DESC` (single column)**.
- `query.WithTotalCount(false)` — Torznab never counts. (`Total` is also
  commented out in `search_result.go`.) So `doCount` is skipped and no
  `has_next_page` +1 is applied: the page is a plain `LIMIT`/`OFFSET`.

`searchRequestToQueryOptions` then appends the request-specific options. Because
`OptionBuilder.OrderBy` **replaces** (not appends) the order slice, a
request-supplied order overrides the default published_at order.

**Base relation:** `torrent_contents`. `SELECT *` (`query.SelectAll`) plus
per-order-column alias projections (see §4). All of Torznab's filter and order
columns are **denormalised onto `torrent_contents`** (`content_type`,
`video_resolution`, `video_3d`, `episodes`, `tsv`, `seeders`, `leechers`,
`size`, `published_at`, `files_count`, `info_hash`) — so the common query is
**single-table**; the `torrents` and `content` joins are only pulled in when a
criterion or ordering requires them (§3).

---

## 2. Request → predicate mapping (`search_options.go`)

Lane T reproduces this mapping and expresses the result as a [`Criteria`] tree.

### 2.1 Function `t=` (top-level, AND-applied)

| `t=` | Predicate |
|------|-----------|
| `search` | none |
| `movie` | `ContentTypeIn([Movie])` |
| `tvsearch` | `ContentTypeIn([TvShow])` + episodes (if `season` set, §2.4) |
| `music` | `ContentTypeIn([Music])` |
| `book` | `ContentTypeIn([Ebook, Comic, Audiobook])` |
| other | error `202 no such function` (Lane T emits the error XML) |

### 2.2 Categories `cat=` (OR of per-category ANDs)

Each requested category id contributes an AND-group; the groups are OR'd and the
whole disjunction is AND'd onto the query (`query.Where(query.Or(catsCriteria...))`).
Per category (`torznab.Category*`):

- **Movies (2000/2030/2040/2045/2060)** → `ContentTypeIn([Movie])` *unless the
  function is already `movie` and this is a sub-category* (Go guard
  `r.Type != FunctionMovie || CategoryMovies.ID == cat`), plus:
  - 2030 SD → `VideoResolutionIn([V480p])`
  - 2040 HD → `VideoResolutionIn([V720p,V1080p,V1440p,V2160p])`
  - 2045 UHD → `VideoResolutionIn([V2160p])`
  - 2060 3D → `Video3DIn([V3D,V3DSBS,V3DOU])`
- **TV (5000/5030/5040/5045)** → `ContentTypeIn([TvShow])` (same function guard),
  plus 5030 SD / 5040 HD / 5045 UHD resolution groups as above (no 3D bucket).
- **XXX (6000/6070)** → `ContentTypeIn([Xxx])`
- **PC (4000/4050)** → `ContentTypeIn([Software, Game])`
- **Audio/Audiobook (3030)** → `ContentTypeIn([Audiobook])`
- **Audio (3000)** → `ContentTypeIn([Music])`
- **Books/Comics (7030)** → `ContentTypeIn([Comic])`
- **Books (7000)** → `ContentTypeIn([Ebook, Comic, Audiobook])` — **quirk:** this
  case appends to the *top-level AND* (`options`), NOT to the OR group, unlike
  every other category. Lane T must place a bare 7000 in the AND bucket, a 7030
  in the OR bucket.

Category-id → content-type/resolution knowledge is **Torznab-domain and lives in
Lane T**; this crate only sees the resulting `ContentTypeIn`/`VideoResolutionIn`/
`Video3DIn` leaves.

### 2.3 Free-text `q=`

`query.SearchString(q)` sets a tsquery (§6) and adds the `tsv @@ $::tsquery`
predicate. When `q` is present the order is set to relevance, or published_at if
the profile disables relevance ordering (§4). Empty `q` ⇒ no FTS predicate, no
relevance order.

### 2.4 Episodes (`tvsearch` + `season`/`ep`)

Only for `t=tvsearch` with `season` valid. `AddEpisode(season, ep)` if `ep` also
valid, else `AddSeason(season)`. Maps to [`Criteria::Episodes`] →
`TorrentContentEpisodesCriteria`:

- season only: `torrent_contents.episodes #> '{<season>}' = '{}'::jsonb`
- season+episode(s): `torrent_contents.episodes #> '{<season>}' @> '{"<ep>":{}}'::jsonb`
- multiple seasons AND-combined.

### 2.5 IDs `imdbid=` / `tmdbid=`

Content type for the id predicate: `Movie` unless `t=tvsearch` (then `TvShow`);
for `t=search` it is the Go nil type — the `type` predicate is dropped
([`ContentRef::content_type`] = `None`). *(The Go `else if` on `TMDBID`/`IMDBID`
type selection is dead for `t=search` — both branches leave `ct` nil; reproduce
the observable behaviour: nil type for `search`, Movie for `movie`/`music`/`book`,
TvShow for `tvsearch`.)*

- `imdbid=` → normalise to a `tt`-prefixed id, then
  [`Criteria::AlternativeIdentifier`] (`ContentAlternativeIdentifierCriteria`):
  `EXISTS (SELECT FROM content_attributes WHERE content_type=content.type AND
  content_source=content.source AND content_id=content.id AND source='imdb' AND
  value IN (<id>) [AND content_type=<ct>])`. **Requires the `content` join.**
- `tmdbid=` → [`Criteria::CanonicalIdentifier`]
  (`ContentCanonicalIdentifierCriteria`): OR over
  `(content.type=<ct> AND) content.source='tmdb' AND content.id IN (<id>)`.
  **Requires the `content` join.**

### 2.6 Profile tags

`profile.Tags` (default profile: none) → [`Criteria::TorrentTag`]
(`TorrentTagCriteria`): `EXISTS (SELECT FROM torrent_tags WHERE
info_hash=torrents.info_hash AND name IN (...))`. **Requires the `torrents` join.**

### 2.7 Limit / offset

`limit = profile.DefaultLimit` (default 100), overridden by `r.Limit` clamped to
`profile.MaxLimit` (default 100). Always emitted (`query.Limit`). `offset` from
`r.Offset` if set (`query.Offset`). Lane T passes the **already-resolved** values
in [`TorznabSearchParams`] (`limit`, `offset`). Limit 0 is a valid Go state
(returns no items); reproduce it.

---

## 3. Dynamic joins

`torrent_contents` is the base. Joins are registered by
`TorrentContentCoreJoins` but only *applied* when `requiredJoins` references them
(`internal/database/query/query.go` `applyPre`/`extractRequiredJoins`). For the
Torznab subset:

| Needs join | When |
|---|---|
| `torrents` (INNER on `info_hash`) | profile tags present (2.6). *(Also order-by-name, but Torznab never orders by name.)* |
| `content` (LEFT on type/source/id) | `imdbid=` or `tmdbid=` present (2.5) |
| none | everything else — a single-table `torrent_contents` scan |

`content` is a **LEFT** join in core-joins, but the id EXISTS/`content.*`
predicates make the rows effectively inner-filtered. Q2 must reproduce the join
*type* and *on* clauses exactly (they can affect the plan but not the result set
given the predicates). The deeper `content`→`content_collections` joins
(`ContentCoreJoins`) are **never required** on the Torznab path (no facets, no
collection criteria) and must not be emitted.

---

## 4. Ordering

`applySelect` projects each order column as `<expr> AS _order_<i>` and
`applyPost` emits `ORDER BY _order_0 [DESC], _order_1 [DESC], ...` over those
aliases. The Torznab-reachable orders:

1. **Default (no `q`)** — from `TorrentContentDefaultOption`:
   `ORDER BY torrent_contents.published_at DESC` — **single column, NO
   `info_hash` tie-break.** Selected when [`TorznabSearchParams::order`] is
   `None`.
2. **Relevance (`q`, relevance enabled)** — `TorrentContentOrderByRelevance`:
   `ORDER BY query_string_rank DESC`, where the select adds
   `ts_rank_cd(torrent_contents.tsv, $<n>::tsquery) AS _order_0` (or `0 AS _order_0`
   when there is no tsquery). **Single column, NO tie-break.**
3. **Published (`q`, `DisableOrderByRelevance`)** —
   `TorrentContentOrderByPublishedAt`:
   `ORDER BY torrent_contents.published_at DESC, torrent_contents.info_hash DESC`
   (two columns, deterministic).

⚠️ Orders (1) and (2) have **no tie-break**, so ties are resolved by physical row
order — non-deterministic. **Parity fixtures MUST give matched rows distinct
`published_at` (case 1) and distinct `ts_rank_cd` ranks (case 2)** or paginate
unstably. Q3 seed design must guarantee this.

### Deviations

**FIND-2 does NOT apply here.** The lone-relevance→`seeders DESC` popularity-sort
rewrite lives in `internal/gql/gqlmodel/torrent_content.go` (flag
`POPULARITY_SORT_DEFAULT`, default OFF) and only fires on the **GraphQL** path.
The Torznab adapter builds options directly and never traverses gqlmodel, so its
relevance order is the raw `ts_rank_cd` order above. (The phase-1 ledger's "FIND-2
where Torznab hits them" resolves to: it doesn't. Recorded as a deviation.)

- **`files` attr from `files_count`.** The crate projects `files_count` from
  `torrent_contents.files_count`; Lane T emits the `files` attr from it. Live Go instead uses
  `len(Torrent.Files)` which is empty under the D1 `torrent_files` read-disable, so live Go omits
  `files`. Deliberate: the Rust behavior restores data the D1 cutover removed from Go's feed.
- **Ordered path must be lean (two-query hydration).** Literal LIMIT was necessary but not
  sufficient: a single statement that also carries the hydration LEFT JOINs + correlated
  subselects still plans as a parallel Gather Merge that shuffles equal-rank ties per execution.
  `fetch()` therefore runs a LEAN single-table membership query (torrent_contents-resident columns
  + filter joins + literal LIMIT — serial plan, mirrors GORM/Go) and a SEPARATE hydration query
  keyed on `torrent_contents.id = ANY($1)` (torrents.name, content year/imdb/tmdb, sources-max
  seeders/leechers), merged in memory preserving query-1 order. **Single-statement joined
  hydration is FORBIDDEN on any ordered+LIMITed path — binding precedent for Phase-2 Lane S.**
- **OBSERVATION — explicit `published_at` order carries a `, info_hash DESC` tiebreak that Go's
  logged statement lacked.** `build_query` appends `torrent_contents.info_hash` as a secondary
  sort key on the explicit-`published_at` path; a `pg_stat_statements` capture showed Go emitting
  plain `published_at DESC`. Evidence conflicts (`internal/database/search/order_torrent_content.go`
  PublishedAt `Clauses()` DOES include an info_hash tiebreak, so a Go call-path emits it — the
  plain form seen may be a different, e.g. GraphQL, path). No observable divergence in either live
  gate round (every published_at-ordered query was set=1.0 and order-matched on unique data). Not
  changed; first suspect if a future gate shows browse-query order drift on `published_at` ties.

**CTE race strategy** (`doItems` / `shouldTryCteStrategy`): a performance
optimisation that returns identical results to the default strategy. It triggers
for tsquery searches whose order is *not* exactly the single `query_string_rank
DESC` — i.e. the published-at-with-query case (3) — never for the relevance case
(2). Q2 need not replicate the race; a single correctly-ordered+limited query is
result-equivalent. Note it so the shadow harness isn't surprised by identical
output from a simpler query.

---

## 5. Public API (frozen for Lane T)

```rust
// params.rs
pub struct TorznabSearchParams {
    pub query:  Option<String>,          // raw app-query text; None/"" => no FTS
    pub filter: Option<Criteria>,        // predicate tree (T builds from t=/cat=/id/tags)
    pub order:  Option<TorrentContentOrder>, // None => default published_at DESC (single col)
    pub limit:  u32,                     // resolved, clamped to MaxLimit
    pub offset: Option<u32>,
}

// criteria.rs — leaves + combinators (Criteria::and/or/not/content_type_in/…)
pub enum Criteria {
    And(Vec<Criteria>), Or(Vec<Criteria>), Not(Box<Criteria>),
    ContentTypeIn(Vec<ContentType>),
    VideoResolutionIn(Vec<VideoResolution>),
    Video3DIn(Vec<Video3D>),
    Episodes(Episodes),
    CanonicalIdentifier(Vec<ContentRef>),    // tmdbid
    AlternativeIdentifier(Vec<ContentRef>),  // imdbid
    TorrentTag(Vec<String>),
}
pub struct ContentRef { pub content_type: Option<ContentType>, pub source: String, pub id: String }
pub enum VideoResolution { V360p, V480p, V540p, V576p, V720p, V1080p, V1440p, V2160p, V4320p }
pub enum Video3D { V3D, V3DSBS, V3DOU }
pub struct Episodes(pub BTreeMap<i32, Vec<i32>>); // season -> episodes ([] = whole season)

// order.rs
pub struct TorrentContentOrder { pub field: TorrentContentOrderField, pub direction: OrderDirection }
pub enum TorrentContentOrderField { Relevance, PublishedAt }
pub enum OrderDirection { Ascending, Descending }

// query.rs — the entry point + execution
pub fn build_query(params: &TorznabSearchParams) -> Result<SearchQuery, SearchQueryError>;
pub struct SearchQuery { /* sql(): &str ; binds(): &[Bind] */ }
pub enum Bind { Bytea(Vec<u8>), Text(String), Tsquery(String) }
impl SearchQuery {
    pub async fn fetch_info_hashes(&self, pool: &PgPool) -> Result<Vec<InfoHash>>; // Q3 parity output
    pub async fn fetch(&self, pool: &PgPool) -> Result<Vec<SearchResultItem>>;     // rows for XML
}

// result.rs — one hydrated row (non_exhaustive); info_hash is the parity key
pub struct SearchResultItem { pub info_hash: InfoHash, pub name: String, pub size: u64, /* … */ }
```

`ContentType`, `InfoHash` are re-exported from `bitmagnet-model`. All params /
criteria / order types derive `serde` so Q3 fixtures carry a
`TorznabSearchParams` as the `input` JSON.

**Boundary confirmations for Lane T:** category-id interpretation, HTTP param
parsing (`t=`,`q=`,`cat=`,`imdbid=`,`tmdbid=`,`season`,`ep`,`limit`,`offset`),
error XML, caps, and result→XML all live in `bitmagnet-torznab`. This crate never
sees a category id or an HTTP concern.

---

## 6. Full-text: app-query → tsquery (highest parity risk)

`q=` is not passed raw to Postgres. `query.SearchString` runs
`fts.AppQueryToTsquery` (`internal/database/fts/tsquery.go` + `tsvector.go` +
`lexer/`), a small query language compiled to a Postgres `tsquery` string, then
binds it as `torrent_contents.tsv @@ $<n>::tsquery`. Grammar:

- word/phrase tokens → lexemes via `Tokenize`/`TokenizeFlat`, each `quoteLexeme`'d;
- `&`/`|`/`.` operators → `&`/`|`/`<->`; `!` → negation; `*` → `:*` prefix on the
  preceding lexeme; `"quoted"` → words joined `<->`; `()` → grouped recursively;
- default operator between adjacent terms is `&`.

Q2 **ports this tokenizer faithfully** — divergence changes the `@@` match set and
fails parity end-to-end (the fixture input carries the raw `q`, both sides
tokenise independently). The Q3 corpus must include operator/quote/wildcard/paren
cases, not just single words. This is the single largest and riskiest piece of
the port; budget accordingly.

---

## 7. Info-hash representation

`torrent_contents.info_hash` is `bytea` (20-byte v1 hash, Go `protocol.ID`).
Rendered as lowercase hex (`InfoHash: Display`). Parity fixtures carry hex
strings; both sides compare hex. Binds use `Bind::Bytea(20 bytes)`.

---

## 8. Ledger status after Q1

- **Q1 — done:** contract above + public API (`src/{lib,params,criteria,order,
  query,result}.rs`) committed on `p1q-searchquery`. Lane T unblocked.
- **Q2 — open:** implement `build_query` + `SearchQuery::fetch*` + the tsquery
  tokenizer; unit tests assert SQL shape (no DB).
- **Q3 — open:** Go generator (`internal/parity/`, one new file) seeds
  deterministic fixture rows + emits `{options → info-hash list}` via the REAL Go
  builder under the live-PG CI lane; Rust `#[ignore]` integration test consumes
  the same fixtures through `bitmagnet-diff` → 0 diffs.
