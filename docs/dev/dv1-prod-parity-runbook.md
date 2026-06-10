
## 11. G2 — FILE_BROWSER_FROM_BLOB flip (DEPLOYED 2026-06-10, after one rollback)

**First attempt (go-flags-1) PANICKED and was rolled back within minutes:** every file-browser query hit a nil-pointer in `filesFromBlob` — the `Files` FIELD resolver (`query.resolvers.go`) constructed a fresh `TorrentQuery{Search}` ignoring `obj` and omitting `Dao` (a latent upstream DI gap, harmless for the legacy Search-only path, fatal for the blob path). Rollback = flag→false + redeploy; verified byte-identical to baseline. Lesson: dv4's unit tests covered the pure functions, not the resolver's DI — gqlgen field resolvers that reconstruct their parent object need integration coverage.

**Fix (go-flags-2 @ 4f64274):** wire `Dao` in the field resolver (same deps as `queryResolver.Torrent`) + a defensive nil-Dao error in `filesFromBlob` (degrade, never panic). Re-flipped.

**Verification (3 multi-file torrents, 192 files; ordered/default/paged probes, flag-off baseline vs blob path):**
- totalCount equal (192) and **result SETS exactly equal** in all probes.
- **Single-torrent index-ordered sequences byte-exact** (33/65/94 files) — the real UI case (the browser queries one info_hash).
- Only difference: **inter-torrent ordering on tied sort keys** in multi-hash queries (index collides across torrents; equal sizes swap) — undefined behavior in BOTH paths (PG's tie order was plan-dependent), not a contract change.
- Pod healthy, 0 error lines, both flags ON (`GATE_FILE_EXTENSIONS_JSONB`, `FILE_BROWSER_FROM_BLOB`).

**⟹ Two of the DROP-removed query shapes now run blob-backed in prod: (a) the ext/file-type filter (JSONB) and (b) the per-torrent file browser (blob). `filesFromBlob` reads only `torrents.files_data` — no `torrent_files` access by construction.**
