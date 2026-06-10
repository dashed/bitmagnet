
## 10. THE FLIP — SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB=true (DEPLOYED + VERIFIED 2026-06-10)

Image `go-flags-1` rolled out with the flag ON. Verification:

- **Rollout:** sts/bitmagnet Running, 0 restarts, `goose: no migrations to run. current version: 22`, all workers started, GraphQL serving, env `SEARCH_FEATURES_GATE_FILE_EXTENSIONS_JSONB=true` present, 0 error/fatal/panic lines.
- **SQL path switched (pg_stat_activity sampling under live probes):** the served filter now issues `torrents.file_extensions @> …`; **zero** `EXISTS(torrent_files)` for the file-type filter. Latency 0.10–0.23 s round-trip (≈ baseline).
- **🎯 EXACT SET PARITY (definitive, SQL-level, read-only):** for the full video-extension multi-file filter, `EXISTS(torrent_files …)` and `file_extensions @> (OR-of-containment)` both return **11,629,070** torrents with **0 symmetric difference** (0 old∖new, 0 new∖old). The filter result set is byte-identical between the two encodings on live prod.
- **Note on `totalCount`:** bitmagnet's `totalCount` is a **budgeted estimate** (`budgeted_count` → planner `Plan Rows` when cost > budget), not an exact count. The pre/post-flip GraphQL `totalCount` for `ubuntu+video` differed (52,669 → 31,005) **only because the JSONB plan yields a different, more accurate planner estimate** (FB-A1 §4) — the underlying matched set is identical (proven above). This is an estimator improvement, not a result change.

**⟹ The JSONB DROP-gate is LIVE and behaviorally correct. The per-file-extension filter no longer reads `torrent_files` — one of the live query shapes the `torrent_files` DROP removes is now served from the blob-derived `file_extensions` column.**
