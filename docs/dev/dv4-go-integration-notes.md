# DV-4 — Go-side integration notes (DROP-gate cutover)

Branch: `feat/deploy-go-integration` (jj, off `feat/file-grained-search`).
Scope: **local Go only**. Nothing deployed. Every behaviour change is behind a
feature flag that is **OFF by default**; the app behaves exactly as upstream
until an operator flips a flag — and a flag is only flipped in prod *after* its
listed validation passes. This honours the standing sequencing constraint:
*do not drop `torrent_files` until each replacement layer is deployed and proven
in production, layer by layer; keep `torrent_files` as the live fallback
throughout.*

## Feature-flag mechanism

- Config section **`search_features`** → env vars `SEARCH_FEATURES_*`
  (`internal/database/search/featureflags_config.go`), registered in
  `databasefx` via `configfx.NewConfigModule[...]` + `fx.Invoke(ApplyFeatureFlags)`.
- The resolved config is published to a package-level atomic snapshot in
  `internal/database/search/featureflags.go` (`SetFeatureFlags` /
  `FeatureFlagsValue`), mirroring the established `model.FilesDataDeserializer`
  indirection. This lets the pure criteria/order functions and the GraphQL layer
  read the flags without threading config through every signature. Tests set/reset
  the snapshot directly.
- All flags default **false**. An unset env = upstream behaviour.

## FLAG → VALIDATION map

| Flag (`SEARCH_FEATURES_*`) | Default | What it does | MUST pass before flipping in prod |
|---|---|---|---|
| `GATE_FILE_EXTENSIONS_JSONB` | OFF | Multi-file branch of `TorrentFileExtensionCriteria` switches from `EXISTS(torrent_files …)` to `torrents.file_extensions @> '["ext"]'::jsonb` (OR-of-`@>` for multi-ext; jsonb_path_ops GIN supports `@>` not `?|`). Single-file `Torrent.Extension.In` branch unchanged. | **DV-1 prod ext-parity**: for a representative sample, the JSONB-column result set == the per-row `EXISTS` result set (no false +/−). Confirm the `file_extensions` GIN index is present and `file_extensions` is backfilled for all with-files torrents. This is the DROP-gate for the per-extension filter. |
| `POPULARITY_SORT_DEFAULT` | OFF | FIND-2. When a query-string search's order is **exactly** the web-UI default lone `relevance`, rewrite it to `seeders DESC` (drops `ts_rank_cd` from the sort). Any non-relevance field, or relevance + an explicit extra field, is passed through (the opt-in). No query string ⇒ untouched. | Product sign-off that popularity is the right *default* sort for typed queries; spot-check that explicit "Sort: Relevance" / multi-field orders still return ranked results. Measure p95 on a broad term (e.g. `x264`) drops from ~tens-of-seconds to ms. |
| `FILE_BROWSER_FROM_BLOB` | OFF | G2. `TorrentQuery.Files` (per-torrent file browser) is served from the AfterFind-hydrated `files_data` blob instead of `SELECT FROM torrent_files`; extension is path-derived (G1). Orders/paginates in memory. | Blob proven a faithful source of truth in prod (DV-1 / `verify --full` 0 mismatches — already ✅ on FSN1) **and** a dual-read spot-check that blob-served file lists == `torrent_files`-served lists for a sample (incl. over-threshold + CJK paths). This is the per-torrent-browser DROP-gate. |
| `FILE_SEARCH_ENABLED` | OFF | Master switch for the GraphQL `fileSearch` + `pathTypeahead` resolvers (`FileSearchQuery`). OFF ⇒ both return `filesearch.ErrDisabled`. ON ⇒ delegate to the injected `filesearch.Client`. | DV-2 DuckDB file-search sidecar **and** DV-3 path-FTS sidecar deployed, reachable, and a real `filesearch.Client` wired (the default is `filesearch.Disabled()`). Until then ON still returns disabled if no client is injected (double-gated). |

> The `torrent_files` **DROP itself is gated on ALL of the above being flipped and
> proven live** — it remains the LAST step and stays deferred indefinitely. These
> flags let each replacement layer run in parallel (dual-read/compare) first.

## What changed

1. **JSONB gate** — `internal/database/search/criteria_torrent_file_extension.go`.
   `fileExtensionsJSONBContains` builds the index-friendly OR-of-`@>` with
   `json.Marshal`ed single-element array args (no injection). Single-file branch
   untouched.
2. **FIND-2** — `internal/gql/gqlmodel/torrent_content.go`
   (`find2PopularitySortDefault`). Rewrites *only* the exact web-UI default; the
   `seeders` order already carries an `info_hash` tiebreak.
   **Decision — Go-side flip vs UI-side default:** the *root cause* is the web UI
   (`torrents-search.component.ts`) defaulting **every** typed query to
   `relevance`; the cleanest long-term fix is UI-side (default typed searches to
   `seeders DESC`, offer "Relevance" as an explicit choice). We still implement
   the **Go-side flip** because it is the server-side safety net for clients the
   UI deploy doesn't cover — Torznab, the GraphQL API, and older web builds — so
   operators can kill the `ts_rank_cd`-over-the-match-set wall without shipping a
   frontend. True relevance stays fully available opt-in. Recommend doing **both**:
   UI default change + this flag ON.
3. **G1 / FB-A0 hardening**
   - `internal/blobmigration/consistency/checker.go`: `CompareFiles` now checks
     an **`extension`** field, derived from **PATH on both sides** (never the raw
     blob `e`, which is legitimately empty for crawl-path torrents). Catches a
     path that round-trips to a different extension; does **not** false-flag
     crawl-path empties.
   - `serializer.go ExtractUniqueExtensions` already derives from path (verified) —
     unchanged.
   - Shared fixtures: **`testdata/file-extension-fixtures.json`** (repo root) is
     the single source of truth for the contract, consumed by
     `internal/blobmigration/file_extension_fixtures_test.go`. **Rust counterpart
     to add** (mirror): a test in `bitmagnet-rs/crates/bitmagnet-model` that reads
     `../../../testdata/file-extension-fixtures.json` (relative to the crate's
     `CARGO_MANIFEST_DIR`) and asserts `file_extension_from_path(path) ==
     expected_extension` and that `transform.rs`'s `file_extensions` facet derives
     from path (so the Rust Tantivy/Parquet path never emits an empty
     `file_extensions` for crawl-path torrents — the G1 deploy-time bug). The JSON
     carries `blob_e` precisely so the Rust side can assert it is **ignored**.
4. **G2** — `internal/gql/gqlmodel/torrent_files.go`. `model.Torrent.AfterFind`
   already hydrates `Files` from the blob; the residual `SELECT FROM torrent_files`
   only remains in the `FILE_BROWSER_FROM_BLOB`-OFF path. ON ⇒ blob-backed,
   path-derived extension, in-memory order/paginate.
5. **GraphQL resolver stubs** — `internal/search/filesearch` (transport-neutral
   `Client` interface + `Disabled()` default + all input hygiene) and
   `internal/gql/gqlmodel/file_search.go` (`FileSearchQuery`, double-gated by flag
   + client). **Input hygiene (FB-B1d):** `EscapeLikePattern` escapes `\ % _`
   (backslash first; pair with SQL `ESCAPE '\'`); query capped at 256 runes,
   typeahead prefix at 128 runes; `MinPrefixChars=2` (UI should mirror + debounce
   ~150–250ms); extensions normalised/deduped/capped; limits clamped.

### GraphQL wiring — remaining trivial step (deferred)

`FileSearchQuery` is **not yet bound into the GraphQL schema** — deliberately,
until the DV-2/DV-3 protos are frozen (coordination messages sent). To finish:
1. Add `fileSearch(...)` + `pathTypeahead(...)` fields to
   `graphql/schema/*.graphqls` returning types bound to `filesearch.FileSearchResult`
   / `PathTypeaheadResult` (gqlgen `autobind` already covers `gqlmodel`).
2. `go generate` / run gqlgen → regenerates `internal/gql/gql.gen.go`.
3. Implement the generated resolver methods by delegating to `FileSearchQuery`.
4. Provide a real `filesearch.Client` (DV-2/DV-3 gRPC) via fx, replacing
   `filesearch.Disabled()`, and flip `FILE_SEARCH_ENABLED`.
Because all logic + validation already lives behind the `Client` interface, this
is mechanical.

## Verification

- `go build ./...` ✅  ·  `go vet` (touched pkgs) ✅
- `go test ./internal/database/search/... ./internal/gql/gqlmodel/...
  ./internal/search/filesearch/... ./internal/blobmigration/...` ✅
- Tests cover: flag default-off + apply, JSONB SQL/args (incl. JSON escaping),
  FIND-2 rewrite/opt-in/no-query cases, checker extension (crawl-path empty-e ⇒
  match, divergent path ⇒ mismatch, stale e ⇒ ignored), shared-fixture
  extension contract, and all input-hygiene rules.
