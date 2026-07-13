# Lane S — Task S-item: expand SearchResultItem to the full model.TorrentContent

Priority task (unblocks Lane G resolvers + Lane C re-point). Expand the crate's
`SearchResultItem` from the flat Torznab-scalar shape to the FULL Go
`model.TorrentContent` field set the GraphQL API needs — **ADDITIVELY** (the
existing flat fields stay byte-identical; Torznab's output must not change). All
new fields load in the **id-keyed HYDRATION query**, never the lean
ordering/membership query; the `files_data` blob is **hydrator-gated** (off by
default).

## Absolute paths
- Crate you edit ONLY: `/private/tmp/.../scratchpad/bm-p2s/bitmagnet-rs/crates/bitmagnet-search-query/`
  (full path: `/private/tmp/claude-501/-Users-me-aaa-github-homelab-infra/76ef0523-d98e-4a99-85f3-d442371dfa15/scratchpad/bm-p2s/bitmagnet-rs/crates/bitmagnet-search-query/`)
  - `src/result.rs` (SearchResultItem), `src/query.rs` (HYDRATION_BY_ID_SQL, fetch, decode, tests), `src/lib.rs`, `CONTRACT.md`.
- Reuse (READ-ONLY, do NOT edit) `bitmagnet-model` types: `crates/bitmagnet-model/src/{content.rs,torrent.rs}` already define `Torrent`, `Content`, `TorrentContent` with the right fields (Torrent has `files_data`, `file_extensions`, `info_hash_v1/v2`; Content has full metadata; TorrentContent has content_source/id, languages, video_source/3d/codec/modifier, seeders, leechers, created/updated... check them).
- Go reference (READ-ONLY), repo root `/Users/me/aaa/github/bitmagnet/`:
  - `internal/database/search/search_torrent_content.go` (`TorrentContentResultItem` = `query.ResultItem` + embedded `model.TorrentContent`).
  - `internal/database/search/hydrator_torrent_content_torrent.go` (the `files` gate → `HydrateTorrentContentTorrentWithFiles`; preloads Sources+TorrentSource+Hint+Tags, +Files when `files`; the `CheckLegacyTorrentFilesReadAllowed` guard).
  - `internal/database/search/hydrator_torrent_content_content.go`.
  - `internal/gql/gqlmodel/torrent_content.go` `NewTorrentContentFromResultItem` (the EXACT field set the GraphQL TorrentContent needs) + `DHTSeenStatsFromTorrent` (DHT stats from the `source=="dht"` row) + `TorrentSourceInfosFromTorrent`.
  - `internal/model/torrent_contents.gen.go` (torrent_contents columns), `torrents.gen.go` (torrents columns incl. `files_data`), `torrents_torrent_sources.gen.go` (source columns: seeders/leechers/seen_count/created_at/updated_at), `torrent_contents.go` `Title()`.

## Ground rules
1. Edit ONLY `bitmagnet-search-query`. sqlx runtime binds, no ORM/macros, builds with no DB.
2. **ADDITIVE / backward-compatible.** Keep EVERY existing `SearchResultItem`
   field EXACTLY (name, size, content_type, published_at, seeders, leechers,
   files_count, video_resolution, video_3d, video_codec, release_group, episodes,
   release_year, imdb_id, tmdb_id, info_hash_v1/v2, info_hash). Torznab's
   `result_map` (in `bitmagnet-torznab`) + its 66 XML goldens + live parity read
   ONLY these — they must stay green. The flat `seeders`/`leechers` are the
   Torznab **sources-max** derivation (keep the `(SELECT max(s.seeders)...)`
   subqueries as-is); do NOT repoint them to the denormalized column.
3. **Lean-membership rule (BINDING):** the lean ordering/membership query
   (`ORDERING_SELECT` / query 1 in `fetch`) stays UNCHANGED — do NOT add any of the
   new columns (especially the blob) to it. All new fields load in the id-keyed
   HYDRATION query (query 2) or keyed sub-queries (query 3+). The `files_data` blob
   must NEVER touch the ordered path.
4. `SearchResultItem` is already `#[non_exhaustive]`; update its `for_test`
   constructor + any in-crate constructions to default the new fields.

## PART A — expand SearchResultItem (result.rs)

ADD these fields (alongside the existing flat ones), reusing `bitmagnet_model`
types (re-export what you need from lib.rs):
- `pub torrent_content: bitmagnet_model::TorrentContent` — the core torrent_contents
  row (denormalized seeders/leechers/languages/video_*/content_source+id/created_at/
  updated_at/published_at/size/files_count). This is the embedded `model.TorrentContent`.
- `pub torrent: bitmagnet_model::Torrent` — the full torrent sub-object (name, size,
  private, created_at, updated_at, files_status, extension, files_count,
  file_extensions, info_hash_v1/v2, meta_version, sources, tags; `files_data` = the
  gated blob, `None` unless hydrate-files requested).
- `pub content: Option<bitmagnet_model::Content>` — `Some` iff the content row exists
  (content_id present), else `None` (mirror gqlmodel `if item.Content.ID != ""`).
- `pub title: String` — Go `TorrentContent.Title()` derivation: if no content or
  empty content title → `torrent.name`; else `content.title [/ original_title] [(year)] [episodes-label]`.
- `pub dht_seen_count: i32`, `pub dht_first_seen_at: Option<i64>`,
  `pub dht_last_seen_at: Option<i64>` — from the `source == "dht"` row in
  `torrent.sources` (Unix seconds), per `DHTSeenStatsFromTorrent`.
- `pub query_string_rank: f64` — the relevance rank (`ts_rank_cd`), 0.0 when no
  tsquery. (Go `query.ResultItem.QueryStringRank`.) 🚨 **This ONE field comes from
  QUERY-1 (the lean ordering/membership query), NOT the hydration query-2** — see
  the note in PART B. Query-2 has no tsquery binding and CANNOT compute the rank.
Document each field's Go source. Keep the struct `#[non_exhaustive]`. Derive stays
`Debug, Clone, PartialEq` (drop `Eq` if `f64`/`f32` in the model types forbid it —
switch to `PartialEq` only; adjust the existing `#[derive(... Eq)]` accordingly and
fix any downstream `Eq` assumptions in tests).

## PART B — expand the id-keyed hydration query (query.rs)

Extend `HYDRATION_BY_ID_SQL` (keeps `WHERE torrent_contents.id = ANY($1::text[])`,
NO ORDER/LIMIT) to also SELECT the columns needed to build `torrent_content`,
`torrent` (minus blob), and `content`:
- torrent_contents: content_source, content_id, languages (jsonb), episodes (already
  hydrated separately — reuse), video_source, video_modifier, created_at, updated_at,
  seeders, leechers (the DENORMALIZED tc columns → `torrent_content` object; distinct
  from the flat sources-max fields), published_at, size, files_count, content_type,
  video_resolution, video_3d, video_codec, release_group.
- torrents: name, size, private, created_at, updated_at, files_status, extension,
  files_count, file_extensions (jsonb), info_hash_v1, info_hash_v2, meta_version.
- content: type, source, id, title, release_year, original_language, original_title,
  overview, runtime, popularity, vote_average, vote_count (all of `model::Content`).
Render timestamps as `floor(EXTRACT(EPOCH FROM ...))::bigint` (the crate's house
style) where the target is an `i64`; keep `created_at`/`updated_at` as epoch secs.
Decode each row into the model structs + set `torrent_content`/`torrent`/`content`
on the item, preserving query-1 order (the existing HashMap<id,_> merge pattern).

**🚨 query_string_rank is the ONE exception — it comes from QUERY-1, not query-2.**
The rank is `ts_rank_cd(torrent_contents.tsv, $tsquery)`, which needs the tsquery
binding + ordering context that ONLY query-1 (the lean ordering/membership query)
has; query-2 (id-keyed hydration) has no tsquery bound and cannot re-derive it. So:
- When a tsquery is present, project the rank as an aliased column IN QUERY-1:
  `ts_rank_cd(torrent_contents.tsv, $N::tsquery) AS query_string_rank` (this is the
  SAME single-table expression query-1 already ORDER BYs for relevance — projecting
  it adds NO joins/subselects, so the lean serial plan is preserved; the
  membership/order/result-set + Torznab XML output are unchanged).
- Carry it in the membership-row struct (`OrderedRow`) keyed by `tc.id`, then MERGE
  it into the item alongside the query-2 hydration fields (both keyed by tc.id).
- 0.0 / absent for browse (non-tsquery) orders — mirror Go (rank only set when a
  query string is present). Do NOT project it when there's no tsquery.
Keep the lean query otherwise byte-identical; the rank is a projected column only,
never a join/subselect. Everything ELSE in PART A lives in query-2.

**files_data gate:** add a bool to a small hydrate-options struct threaded into the
fetch path (see PART D). When ON, add `torrents.files_data` to this SELECT (or a
separate id-keyed query) and populate `torrent.files_data = Some(bytes)`; when OFF,
leave it `None` and do NOT select the column. The blob stays out of query 1.

## PART C — keyed sub-collections: torrent Sources + Tags (query.rs)

The full `model::Torrent` needs `sources` (for DHT stats + GraphQL source infos +
seeders) and `tags`. Load them with info_hash-keyed queries (NOT joined into the
ordered path):
- Sources: `SELECT source, info_hash, import_id, seeders, leechers, published_at,
  created_at, updated_at, seen_count [, torrent_sources.name] FROM
  torrents_torrent_sources [LEFT JOIN torrent_sources ...] WHERE info_hash =
  ANY($1::bytea[])`. Group by info_hash → `torrent.sources: Vec<TorrentsTorrentSource>`
  (add this to the model reuse; if `bitmagnet_model::Torrent` lacks a `sources`
  field, DO NOT edit bitmagnet-model — instead carry sources in a crate-local
  wrapper or a `Vec` field you add to the item, e.g. `pub torrent_sources:
  Vec<...>` — but PREFER reusing an existing model field; inspect torrent.rs first).
- Tags: `SELECT info_hash, name FROM torrent_tags WHERE info_hash = ANY($1::bytea[])`
  → `torrent.tags`.
Then derive `dht_seen_count`/`dht_first_seen_at`/`dht_last_seen_at` from the
`source=="dht"` source row. **NOTE:** inspect `bitmagnet_model::Torrent` — if it has
no `sources`/`tags` fields, add crate-local `torrent_sources: Vec<TorrentSourceInfo>`
+ `tags: Vec<String>` fields on `SearchResultItem` instead of forcing them onto the
model struct (staying out of the bitmagnet-model crate). Define a small
`TorrentSourceInfo` in result.rs if needed (key, name, seeders, leechers, seen_count,
first_seen_at, last_seen_at) mirroring gqlmodel `TorrentSourceInfo`. DHT stats can be
computed here even without a full sources model.
(If loading Sources/Tags meaningfully complicates this task, prioritize: the
`torrent` core + `content` + `torrent_content` + `files_data` gate + DHT stats are
the critical-path unblock; Sources/Tags collections may be a thin follow-up — but
DHT stats REQUIRE the dht source row, so load at least that.)

## PART D — the files_data hydrate gate (query.rs)

Add a hydrate-options input threaded into the fetch path:
```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HydrateOptions { pub files_data: bool }  // default: false
```
- Keep `SearchQuery::fetch(&self, pool) -> Result<Vec<SearchResultItem>>` WORKING and
  UNCHANGED in signature (Lane T calls it) — it delegates to the new
  `fetch_with(pool, HydrateOptions::default())` (files_data OFF). Lane T's items get
  the expanded fields with `files_data = None`, which it ignores.
- Add `pub async fn fetch_with(&self, pool, opts: HydrateOptions) ->
  Result<Vec<SearchResultItem>>` that performs the gated hydration. Lane C will call
  this with `files_data: true` for refine.
Re-export `HydrateOptions` from lib.rs.

## PART E — keep Torznab + lean path green
- `build_query`, `ORDERING_SELECT`, query 1 in fetch, and the flat-field decode: UNCHANGED.
- The flat `seeders`/`leechers` sources-max subqueries: UNCHANGED.
- `for_test` + any test constructors: default the new fields (empty Torrent/None
  content/0 rank/empty title).

## PART F — CONTRACT.md + tests + S5 note
- CONTRACT.md §Phase-2: document the expanded `SearchResultItem` (flat Torznab
  fields UNCHANGED + new nested `torrent_content`/`torrent`/`content`/title/DHT/rank),
  the `HydrateOptions.files_data` gate (blob off the ordered path, D1-note), and the
  seeders semantic split (flat=sources-max for Torznab; `torrent_content.seeders`=
  denormalized for GraphQL).
- Unit tests: assert the expanded HYDRATION_BY_ID_SQL selects the new columns and
  that `files_data` is absent unless the gate is on; a serde/round-trip for the
  expanded item; the `Title()` derivation cases (content title vs torrent name);
  DHT-stats derivation from a dht source row. No DB (the SQL-shape + pure-logic
  parts); the executing paths are S5-gated.
- Add a comment noting S5 differential parity must compare the full expanded item
  field set (not just info-hash), incl. the µs-precision published_at note.

## Acceptance criteria
1. `cargo build -p bitmagnet-search-query` compiles; `cargo test` all green
   (existing + new); `parity_pg` stays `#[ignore]`.
2. `cargo fmt` + `cargo clippy --all-targets -- -D warnings` clean.
3. `fetch(pool)` signature UNCHANGED; Torznab flat fields UNCHANGED; lean query 1
   UNCHANGED; blob never in the ordered path.
4. No edits outside the crate (no bitmagnet-model edits).
When done: list changed files + cargo outputs; note any part deferred (e.g. if
Sources/Tags collections were thinned) so the reviewer knows the exact surface.
