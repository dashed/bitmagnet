# L2 padding files — retain, flag, filter by default

**Date:** 2026-06-11 · **Status:** ✅ **LIVE (image `l2-6`)** — final quiesced cycle verified: export `padding_rows=33,040,027`, suite 12/13 exact with only the documented dup-path residue. Found by GATE C; design settled with the user ("retain the files, make search better by filtering").
**Parent docs:** [`l2-verify-and-shadow-runbook.md`](./l2-verify-and-shadow-runbook.md) (the GATE C finding) · [`torrent-files-replacement-options.md`](./torrent-files-replacement-options.md).

## What they are, and what they actually cost (measured, frozen snapshot 2026-06-11)

BitTorrent **padding files** are alignment filler, not content. THREE
conventions exist in the corpus (inventoried exhaustively on the new
generation): BEP-47's `.pad/<size>` directory (the same path repeated at many
indexes in hybrid torrents); BitComet's `_____padding_file…` (5 underscores)
name markers; and libtorrent's older `.____padding_file/` (4 underscores)
directory, possibly nested. The first classifier shipped only the first two —
the `padding_rows=25,104,231` of the first l2-5 export exposed the 7.9 M-row
libtorrent variant against the 33.0 M corpus scan; the 3-convention classifier
covers 33,039,281 rows with a residue of ~759 coincidental substrings (1 in
~1.2 M — accepted).

| Dimension                          | Measured                                                                                       |
| ---------------------------------- | ---------------------------------------------------------------------------------------------- |
| Rows in the corpus                 | **33,035,140 = 3.74 %** of 882.8 M, across **779,071 torrents** (~4.6 % of with-blob torrents) |
| Share of the NULL-extension bucket | **55.28 %** (33.0 M of 59.8 M) — the "no extension" facet/filter was majority junk             |
| Nominal declared bytes             | 101 TB = 0.137 % of total                                                                      |
| Disk in the fact                   | ~3.7 % of rows ≈ 0.3–0.5 GB of the 13 GB fact (RLE/dict compress well)                         |
| Scan latency                       | ~3.7 % of any full-scan query                                                                  |

So the **resource** cost is minor; the **result-quality** cost (facet counts,
NULL-ext and small-size-range queries, per-torrent matching counts) is the real
one. Note the legacy `torrent_files` carries most of these rows too (one per
distinct pad path — its `(info_hash, path)` PK dedups the repeats): the junk
predates L2; L2 just made it visible by being faithful.

## The design: classify once at export, filter by default, opt back in

**Nothing is deleted.** The blob stays byte-faithful (storage of record) and the
fact keeps every row — flagged:

1. **`decode::is_padding_path(path)`** — the single classification point
   (`.pad/` prefix, OR contains `_____padding_file`, OR contains
   `.____padding_file/`), computed once per row at export and materialized as
   the **`is_padding` BOOLEAN fact column** (RLE ≈ free on disk). Queries
   never pattern-match 880 M paths.
2. **Rollups (`agg_ext`, `agg_torrent_ext`) are built padding-free** — facet /
   collapse / distinct-count numbers are clean on the fast path, which is where
   the junk was most visible.
3. **Every fact-path query defaults to `NOT is_padding`**
   (`sql::predicate`); the proto gains **`FileFilters.include_padding`**
   (default `false`) for opt-in visibility.
4. **`include_padding=true` forces `RollupPlan::FactOnly`** — the rollups
   cannot serve a pads-included query (they no longer contain them); only the
   fact (which kept the flagged rows) can.
5. **The `InMemoryEngine` reference applies the SAME classifier fn** in
   `Filters::matches`, so both engines agree by construction; the **v2-shadow
   PG mirror** expresses the identical classification as exact string ops
   (`left(path,5)='.pad/' OR strpos(path,'_____padding_file')>0` — LIKE would
   treat the underscores as wildcards).
6. The export's stats line now reports `padding_rows=` (expect ~33.0 M).

## Deploy sequencing (schema change!)

The new SQL references `is_padding`; **old generations don't have the column**.
Order: build+pin the new image → **run the export first** (new generation
carries the column; the old sidecar ignores extra columns) → **then** roll the
sidecar. Never roll a new sidecar onto an old generation.

## What this does and does not fix

- Fixes: junk-free facets/counts/finds by default, −3.7 % rows on every scan,
  a NULL-ext bucket that means what it says (~26.7 M real files).
- Does NOT fix the G2 file browser (it reads the blob directly in Go — showing
  pads there is arguably correct for a _file browser_; a display toggle is a
  separate small Go-side decision).
- Does NOT fully close the GATE C facet mismatch: of the +18,726 blob-side
  superset rows, ~18,708 were pads — but **18 are real dup-path files** (10
  avi, 6 mp4, 1 url, 1 3gp) the legacy `(info_hash, path)` PK structurally
  cannot store. A strict re-run still shows that residue; the sidecar remains
  a (now ~18-file) documented superset where it is simply more correct than
  the table being retired.
