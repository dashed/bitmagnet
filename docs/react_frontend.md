# BitMagnet React Frontend — Specification v2.1 (2026-07-03)

A ground-up rewrite of the web UI in **React + Vite**, guided by three design
values, in priority order:

1. **Snappy** — measurable performance budgets, not vibes.
2. **Pedestrian** — boring, conventional UI patterns; nothing clever where
   ordinary works. Familiarity beats novelty.
3. **Mobile friendly** — mobile is a first-class layout, not a degraded
   desktop table.

v2 supersedes the 2026-01 spec (same file, git history); v2.1 folds a
25-finding independent architecture critique (gpt-5.5, 2026-07-03) — notable
corrections: exact legacy-URL contract, P1 split into four gates, Mantine 9,
paginate-first instead of mandatory virtualization, `cached:false` refresh
semantics preserved, i18n phasing de-contradicted, and several parity items
the first draft missed (name sort, date-selector depth, files-tab states,
sponsor/document-title chrome).

---

## 1. Ground rules

- **Location**: `webui-react/` parallel to `webui/`. Angular keeps serving at
  `/webui/` until sunset; React mounts at **`/app/`** via a second `go:embed`.
  Coexistence until parity is proven, then a config-gated default redirect.
- **Backend contract**: same-origin `/graphql`, schema from
  `graphql/schema/**/*.graphqls`, operations shared at repo-root
  `graphql/{queries,mutations,fragments}/`. No new backend endpoints are
  required for core scope (the Jan spec's SSE + settings-API asks are cut —
  polling and localStorage are pedestrian and sufficient; the Angular UI
  already polls).
- **Deep links are a public contract.** The legacy URL serialization is
  preserved exactly or redirected exactly: Angular elides defaults and
  encodes `query`, `content_type`, per-facet params, `order`, `desc`, size
  units, `published_at`, `torrent`, and `tab`; the permalink route is
  `/torrents/permalink/:infoHash` (NOT bare `/:infoHash`). **P1a delivers a
  written redirect matrix** (every legacy URL shape → its `/app/`
  equivalent) with tests. TanStack Router's JSON search-param encoding must
  not silently change public URLs.

  **Recorded waivers (P1a gate, 2026-07-04):** _(the custom published-date editor waiver was CLOSED 2026-07-04: full custom-range editor shipped, `<date> to <date>` backend grammar; a page-size selector [10/20/50/100] also landed)_ (1) the legacy `?facets=`
  active-panel-list param is not preserved — panel open/closed state is
  ephemeral component state in React; selected filter _values_ fully
  round-trip. (2) Angular pre-checks all facet checkboxes when nothing is
  selected; React shows them unchecked (clearer signifier of "no filter").
  (3) P2 gate 2026-07-04: the always-visible health toolbar widget is
  waived — Health ships as a full page (content parity: checks, workers,
  degraded banner) reachable from the dashboard; a nav status dot can be
  revisited with P3 a11y work.

## 2. Stack (reaffirmed from v1 where still current — boring on purpose)

| Layer     | Choice                                                             | Notes                                                                                                                                                                                                                                          |
| --------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Framework | **React 19** + TypeScript 5 (strict)                               | React Compiler **off at baseline** — budgets must hold without it; enable later only if measured and Mantine-compatible                                                                                                                        |
| Build     | **Vite (current major)** + pnpm                                    | Vite's build pipeline is Rolldown-backed as of v8 — chunking/visualizer assumptions must be validated in P0, along with `base: "/app/"` and deep-link refresh under the Go embed                                                               |
| UI kit    | **Mantine 9** + Tabler icons                                       | v2 said 8; current is 9.x — pin the current major. Granular component imports + granular CSS imports are budget requirements, not suggestions                                                                                                  |
| Data      | **TanStack Query** + `graphql-request` + **codegen client-preset** | codegen with `documentMode: "string"` (no AST bloat), strict scalars (Date/DateTime/Duration/Hash20/Hash32/Void/Year), and an explicit fragment-masking decision in P0. **Query owns ALL server state**; Router owns URL state + prefetch only |
| Routing   | **TanStack Router**                                                | typed, validated search params replace the Angular 594-LOC URL/facet/order controller — under the §1 legacy-URL contract                                                                                                                       |
| Tables    | TanStack Table (desktop)                                           | **paginated-first everywhere** (default 20/page, matching Angular). Virtualization is NOT mandatory: apply TanStack Virtual only to measured hotspots (long file/path lists), since variable-height cards + row expansion fight virtualizers   |
| Charts    | Recharts, lazy-loaded                                              | dashboard-only chunk; theme-aware and container-sized (the Angular fixed 400×550 px charts are a named defect)                                                                                                                                 |
| i18n      | react-i18next                                                      | **all 14 languages, all lazy, from P1**: catalog plumbing + `en` inline; the other 13 as lazy chunks. Parity also means: browser-language detection, `bitmagnet-language` persistence, fallback-marker stripping, dynamic keys (§5 acceptance) |
| Forms     | none at core                                                       | TanStack Form is deferred until a real validation-heavy workflow exists — search "forms" are URL-state controls; dialogs are simple                                                                                                            |

Cut from v1 core (parking lot, §8): cmd-K palette, saved searches, fuse.js,
SSE live updates, offline support, TanStack Form.

## 3. "Snappy" — the budgets (CI-enforced, not aspirational)

| Metric                                             | Budget                | Enforcement                          |
| -------------------------------------------------- | --------------------- | ------------------------------------ |
| Initial JS for the search route                    | **≤ 250 KB gzip**     | size-limit in CI + bundle visualizer |
| Any lazy route chunk                               | ≤ 150 KB gzip         | size-limit                           |
| INP (interaction to next paint), p75               | ≤ 200 ms              | Lighthouse CI on the built embed     |
| Repeat search → first result paint                 | ≤ 300 ms (cache-warm) | Playwright trace assertion           |
| LCP on cold `/app/` load, mid-tier phone emulation | ≤ 2.5 s               | Lighthouse CI                        |

The 250 KB number is achievable **only with discipline** (critique finding
#3): granular Mantine CSS/component imports, `documentMode: "string"`
codegen, and route-level lazy loading of the detail panel, mutation dialogs,
and the date-picker widget. **P0's exit includes a real measured search-route
bundle** — if the skeleton already busts the budget, the stack decision
reopens before P1.

Request/caching semantics (corrected from v2): the Angular search runs
Apollo `fetchPolicy: "no-cache"` and controls freshness via the backend's
`cached: true/false` input — manual refresh sends `cached:false`. The React
port keeps that contract (TanStack Query keys + `cached:false` on explicit
refresh), and uses `placeholderData: keepPreviousData` for instant page
flips. `aggregationBudget` exists in the schema but Angular does NOT send it
— if React adopts it, that is a **new behavior** with its own estimated-total
tests, not parity.

## 4. "Mobile friendly" — the layout contract

- **Mobile-first CSS**; breakpoints are declarative (CSS/Mantine), never
  imperative `sizeAtLeast()` checks in components.
- **< md**: bottom navigation (**Torrents / Dashboard** — parity; a Health
  nav destination would be a product change, see §10), search results as
  cards (paginated, like Angular's compact table), facets in a bottom sheet,
  detail as a full-screen route. Touch targets ≥ 44 px. No horizontal
  scroll, ever.
- **≥ md**: top bar + left facet rail, results as a table, inline row
  expansion for detail.
- Fix the known iOS Enter-to-search bug (semantic `<form>` +
  `<input type="search">` + submit handler).
- Magnet links get explicit, deliberate behavior (protocol `href`,
  long-press friendly, adjacent copy affordance).

## 5. Parity surface (from the 2026-07-03 Angular inventory + critique)

Everything below exists today and must work in React before any sunset talk.

### Torrent search (the heavyweight)

- Query box (Enter-to-search, clear); content-type nav with estimate-aware
  counts (`~N` sig-figs util ported from `intEstimate`); size filter
  (dual-unit KB…TiB, apply/clear); **published-date selector at full Angular
  depth** — quick presets, extended presets, custom date range, custom
  timeframe expression, active-filter chips, error chips (critique #10; a
  simplified version requires an explicit parity waiver); dynamic facets
  (torrent_source, torrent_tag, file_type, language, genre,
  video_resolution, video_source — content-type-relevant, nullable-bucket
  aware, counts estimate-aware).
- Sort: relevance (only with a query), **name** (critique #11),
  published_at, updated_at, size, files_count, seeders, leechers +
  direction.
- Results: content-type icon, title/name, chips (tags, languages, 3d,
  resolution, source, codec, modifier), size, published (timeAgo +
  tooltip), **DHT seen** (last-seen + count; first/last/count tooltip),
  seeders/leechers, magnet.
- Multi-select + bulk actions: copy magnets/hashes, set/put/delete tags
  (suggest-tags autocomplete), reprocess (local/apis/force-rematch),
  guarded delete.
- Pagination: per-list behavior defined explicitly (critique #14) — search
  shows no last-page control (totals may be estimates); files and jobs
  lists may show last-page (exact counts).
- Detail (inline row on desktop / full-screen on mobile) + permalink
  `/torrents/permalink/:infoHash`: TMDB poster, metadata, sources with seen
  counts, episodes, genres, rating, external links, info-hash copy; tabs:
  **Files** — with all three Angular states: multi-file paginated table,
  **single-file synthesized row** (`filesStatus === "single"`), and
  no-info/over-threshold ("showing x of y") (critique #15) — Edit tags,
  Reprocess, Delete.
- **Error/loading conventions ship in P1a, not P3** (critique #16):
  snack-bar-equivalent GraphQL error surface, list progress indicators,
  empty states, request cancellation on param change.

### Admin & chrome

- **Dashboard home** — health card + shortcut cards.
- **Queue visualize + Torrent metrics** — ONE shared `MetricsControls`
  component (auto-refresh interval/pause/last-updated, timeframe + paginate,
  resolution multiplier/bucket, queue-or-source filter, event filter) and a
  **tested data-normalization module** (client-side bucketing, status/latency
  aggregation — critique #21) feeding timeline/totals chart adapters.
- **Queue jobs** — facets (queue, status) w/ counts, sort, table, row-expand
  pretty-printed JSON payload, full pagination.
- **Queue admin** — purge-jobs + enqueue-reprocess-batch dialogs.
- **Health** — toolbar widget (status color) + summary dialog (checks +
  workers, errors when degraded); poll-driven.
- **Chrome** — logo + `Version` (tooltip), language menu (14), theme toggle,
  external-links menu, **sponsor link**, **document-title behavior**
  (critique #22), not-found page.

### GraphQL operations (all existing)

Queries: `TorrentContentSearch`, `TorrentFiles`, `TorrentMetrics`
(+`listSources`), `QueueMetrics`, `QueueJobs`, `TorrentSuggestTags`,
`HealthCheck`, `Version`. Mutations: `TorrentDelete`, `TorrentSetTags`,
`TorrentPutTags`, `TorrentDeleteTags`, `TorrentReprocess`, `QueuePurgeJobs`,
`QueueEnqueueReprocessTorrentsBatch`.

### i18n acceptance checks (beyond catalog conversion — critique #9)

Browser-language auto-detection; `bitmagnet-language` localStorage
persistence; missing-key fallback to `en` with marker-value stripping;
dynamic/interpolated keys; translated facet/order/content-type labels; RTL
for `ar`.

### Themes — explicit parity waiver (critique #23)

v2.1 ships light + dark (system-pref + manual toggle, persisted), designed
as successors of Classic/Tundra. Neon/Clean are dropped deliberately —
recorded here as intentional non-parity with its own visual acceptance pass
(including RTL).

## 6. New product surface (backend live; Angular never wired it)

Flag-gated, after parity. Reserved IA: a search-mode switch
(Torrents | Files | Paths).

1. **Path typeahead** (`pathTypeahead`, L3) — debounced suggestions, ≥3
   chars (backend guard).
2. **File search** (`fileSearch`, L2) — find files by name/ext/size across
   torrents.
3. **Collapse paths** (`collapsePaths`, L3) — path-grouped result browsing.

Each gets its own small spec + review when picked up.

## 7. Phases (P1 split per critique #2)

| Phase                    | Scope                                                                                                                                                                                         | Exit gate                                                                                                                             |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **P0 scaffold**          | Vite app embedded at `/app/` (build-flag), codegen pipeline (documentMode string), CI (typecheck, vitest, size-limit, Lighthouse), theme + lazy-i18n plumbing (en), error/loading conventions | binary serves `/app/`; **measured search-route bundle** vs budget; `base`/deep-link refresh verified; Authentik pass-through verified |
| **P1a read-only search** | query/facets/sort/pagination + URL-state schema + legacy redirect matrix + results table/cards                                                                                                | parity vs Angular on read-only flows; redirect matrix tested; budgets green                                                           |
| **P1b detail**           | row expansion + permalink route + files tab (3 states) + chips                                                                                                                                | parity incl. deep links                                                                                                               |
| **P1c mutations**        | bulk actions, tags, reprocess, delete, dialogs                                                                                                                                                | mutation parity, optimistic/refresh semantics defined                                                                                 |
| **P1d mobile**           | bottom nav, cards polish, bottom-sheet facets, full-screen detail, touch/scroll audit                                                                                                         | mobile layout contract (§4) verified on device emulation                                                                              |
| **P2 admin parity**      | dashboard home, queue visualize/jobs/admin, torrent metrics (normalization module first), health                                                                                              | parity checklist; charts theme-aware/container-sized                                                                                  |
| **P3 i18n + polish**     | 13 lazy catalogs enabled + i18n acceptance checks (§5), RTL pass, a11y pass (keyboard + SR on search flow)                                                                                    | i18n acceptance green; axe clean on core flows                                                                                        |
| **P4 new surface**       | §6 features, flag-gated                                                                                                                                                                       | per-feature review                                                                                                                    |
| **P5 flip**              | config-gated default redirect `/webui/` → `/app/`, Angular sunset decision                                                                                                                    | user acceptance + a soak week                                                                                                         |

## 8. Later-work parking lot (deliberately not core)

cmd-K palette · saved searches · fuzzy client-side search · SSE/live push ·
offline/PWA · theme gallery beyond light/dark · bulk-select across pages ·
TanStack Form · broad virtualization.

## 9. Delivery notes (model routing)

- Mechanical, clear-spec work → **gpt-5.5 via codex**: Transloco→i18next
  catalog conversion, codegen setup, chart-adapter ports, MetricsControls
  consolidation, redirect-matrix implementation + tests.
- UX-critical components (search page, mobile layouts) and spec evolution →
  **opus/fable** (taste ≥ 7), with adversarial review at each phase gate.
- The Angular app is the executable parity oracle: Playwright drives both
  UIs against the same backend and diffs rendered results during P1/P2.

## 10. Risks / open decisions

- **Mantine 9 bundle weight vs the 250 KB budget** — P0 measures; fallback
  is a trimmed component subset. Decision point, not a blocker.
- **Health as a mobile nav destination** — product change vs Angular's
  toolbar-widget-only. Decide at P1d; default is parity (widget only).
- **Estimate-aware pagination UX on mobile** — explicit pages first
  (pedestrian, matches Angular); revisit with usage.
- **Vite/Rolldown chunking behavior** — validate visualizer + manual-chunk
  assumptions in P0.
- **React Compiler** — off at baseline; adopt only with measured benefit and
  no Mantine interactions.
