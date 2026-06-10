
## 10. THE FLIP — SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB=true (DEPLOYED + VERIFIED 2026-06-10)

Image `go-flags-1` rolled out with the flag ON. Verification:

- **Rollout:** sts/bitmagnet Running, 0 restarts, `goose: no migrations to run. current version: 22`, all workers started, GraphQL serving, env `SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB=true` present, 0 error/fatal/panic lines.
- **SQL path switched (pg_stat_activity sampling under live probes):** the served filter now issues `torrents.file_extensions @> …`; **zero** `EXISTS(torrent_files)` for the file-type filter. Latency 0.10–0.23 s round-trip (≈ baseline).
- **🎯 EXACT SET PARITY (definitive, SQL-level, read-only):** for the full video-extension multi-file filter, `EXISTS(torrent_files …)` and `file_extensions @> (OR-of-containment)` both return **11,629,070** torrents with **0 symmetric difference** (0 old∖new, 0 new∖old). The filter result set is byte-identical between the two encodings on live prod.
- **Note on `totalCount`:** bitmagnet's `totalCount` is a **budgeted estimate** (`budgeted_count` → planner `Plan Rows` when cost > budget), not an exact count. The pre/post-flip GraphQL `totalCount` for `ubuntu+video` differed (52,669 → 31,005) **only because the JSONB plan yields a different, more accurate planner estimate** (FB-A1 §4) — the underlying matched set is identical (proven above). This is an estimator improvement, not a result change.

**⟹ The JSONB DROP-gate is LIVE and behaviorally correct. The per-file-extension filter no longer reads `torrent_files` — one of the live query shapes the `torrent_files` DROP removes is now served from the blob-derived `file_extensions` column.**

## 11. G2 — FILE_BROWSER_FROM_BLOB flip (DEPLOYED 2026-06-10, after one rollback)

**First attempt (go-flags-1) PANICKED and was rolled back within minutes:** every file-browser query hit a nil-pointer in `filesFromBlob` — the `Files` FIELD resolver (`query.resolvers.go`) constructed a fresh `TorrentQuery{Search}` ignoring `obj` and omitting `Dao` (a latent upstream DI gap, harmless for the legacy Search-only path, fatal for the blob path). Rollback = flag→false + redeploy; verified byte-identical to baseline. Lesson: unit tests covered the pure functions, not the resolver's DI — gqlgen field resolvers that reconstruct their parent object need integration coverage.

**Fix (go-flags-2 @ 4f64274):** wire `Dao` in the field resolver (same deps as `queryResolver.Torrent`) + a defensive nil-Dao error in `filesFromBlob` (degrade, never panic). Re-flipped.

**Verification (3 multi-file torrents, 192 files; ordered/default/paged probes, flag-off baseline vs blob path):**
- totalCount equal (192) and **result SETS exactly equal** in all probes.
- **Single-torrent index-ordered sequences byte-exact** (33/65/94 files) — the real UI case (the browser queries one info_hash).
- Only difference: **inter-torrent ordering on tied sort keys** in multi-hash queries (index collides across torrents; equal sizes swap) — undefined behavior in BOTH paths (PG's tie order was plan-dependent), not a contract change.
- Pod healthy, 0 error lines, both flags ON (`GATE_FILE_EXTENSIONS_JSONB`, `FILE_BROWSER_FROM_BLOB`).

**⟹ Two of the DROP-removed query shapes now run blob-backed in prod: (a) the ext/file-type filter (JSONB) and (b) the per-torrent file browser (blob). `filesFromBlob` reads only `torrents.files_data` — no `torrent_files` access by construction.**
