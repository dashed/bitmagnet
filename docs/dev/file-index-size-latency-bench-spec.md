# File-Grained Index — Size & Latency Smoke-Pass Bench Spec

**Owner:** `index-bench` (team `bitmagnet-bench`, TASK #2)
**Date:** 2026-06-07
**Status:** DESIGN ONLY. No index built, no backfill run, no deploy. Execution is gated on user go-ahead, targeted at **HEL1** (idle).
**Settles:** the GATE between Phase A (cheap composition) and Phase B (the 873 M-doc file index) in
[`file-grained-search-spec.md`](./file-grained-search-spec.md) §6/§11 and
[`file-grained-search-team-review.md`](./file-grained-search-team-review.md) §5 (B3, H1).

---

## 0. The two questions the gate turns on

1. **SIZE** — is the v1 file index really ~10–16 GB (review) / 8–12 GB (spec §6), or larger once you count the
   two things §6 omits: the **INDEXED numeric term-dict for `size`** and the **`published_at` FAST column** (~3.3 GB
   alone)? And does **dropping `INDEXED` on `size`/`published_at`** (range comes from FAST,
   `range_query.rs:102-117`) actually save what we think?
2. **LATENCY** — does the index deliver the **<50 ms** per-file `ext ∧ size` query that is its _sole_ advantage
   over DuckDB-on-blobs (+0 GB, 1–10 s)? If not, the index does not earn its ~15–24 GB + 2nd writer + backfill ops.

**Re-framing the GO/NO-GO:** the spec's gate ("index ≤ 74 GB → GO") is the wrong gate — every realistic estimate
fits the 200 Gi PVC trivially. The decisive gate is **(a)** the FAST-only size (does dropping INDEXED hold the index
under ~20 GB) and **(b)** the latency delta vs the cheap composition. Size alone never says NO-GO here; **latency can.**

---

## 1. Corpus constants (from the deploy plan + fork code)

| Quantity            | Value                                                                      | Source                                      |
| ------------------- | -------------------------------------------------------------------------- | ------------------------------------------- |
| Torrents with files | 16,976,700                                                                 | backfill complete (MEMORY / deploy plan §0) |
| Target file docs    | ~873 M                                                                     | spec §1/§6                                  |
| Avg files/torrent   | ~51.8                                                                      | 873 M ÷ 16.98 M                             |
| Distinct extensions | ~100                                                                       | spec §6 / `schema.rs:191` analogue          |
| Blob source bytes   | ~16 GB total (`files_data`)                                                | spec §6 (≈1 KB/torrent)                     |
| Tantivy             | **0.26.1**                                                                 | `bitmagnet-rs/Cargo.toml:62`, `Cargo.lock`  |
| `size` range        | 0 … ~10¹¹ B                                                                | ~37 bits → bitpacked FAST                   |
| `published_at`      | Unix secs, denorm **per-torrent** (all files of a torrent share one value) | spec D7/§4.4                                |

**Cheapest possible validation of the 873 M claim — run this FIRST, it builds nothing:**

```sql
-- one bounded read-only aggregate; confirms the doc-count target before any index work
SELECT count(*) AS torrents_with_files,
       sum(files_count) AS total_file_docs,
       avg(files_count)::numeric(10,2) AS avg_files
FROM torrents
WHERE files_status IN ('single','multi') AND files_count IS NOT NULL;
```

If `total_file_docs` ≫ 873 M (a few pathological `save_files_threshold` torrents with 50 k+ files, §4.4) the index is
bigger than headline. Log it.

---

## 2. Size model — what §6 gets right, and the two omissions

Tantivy 0.26 writes one set of files per segment, keyed by component extension. We measure each **directly** from the
on-disk segment files (no estimation): `.store` (doc store), `.fast` (columnar/FAST fields), `.term` (term dicts),
`.idx` (postings), `.pos` (positions — v1.1 path only), `.fieldnorm`. (`meta.json` enumerates segments.)

### Per-component prediction at 873 M (my numbers, to be validated)

| Component                                            | §6 says         | **My prediction**             | Why §6 is off                                                                                                                                                                                                          |
| ---------------------------------------------------- | --------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `size` u64 **FAST** (bitpacked)                      | 4.4 GB          | **4.0–4.6 GB**                | ~37-bit values ⇒ ~4.6 B/doc; OK                                                                                                                                                                                        |
| `info_hash` postings (delete key, INDEXED bytes)     | 1–2 GB          | **1–2 GB**                    | 16.98 M terms, files of a torrent are consecutive doc-ids ⇒ tiny deltas; OK                                                                                                                                            |
| `extension` postings + FAST ordinal                  | 1–2 GB          | **1.5–2.5 GB**                | FAST ordinal ~1 B/doc (~0.9 GB) **plus** skewed postings ~1 GB; slight undercount                                                                                                                                      |
| `content_type[]` FAST (multivalued denorm)           | (lumped 2–4 GB) | **~1 GB**                     | ~1 value/doc + offsets                                                                                                                                                                                                 |
| **`published_at` FAST (denorm)**                     | (lumped 2–4 GB) | **~3.3 GB**                   | **§6 omission #2** — review B3: this field _alone_ ≈ 3.3 GB (873 M × ~30 bits)                                                                                                                                         |
| **`doc_id` STORED-only** (`STRING.set_stored()`)     | 1–2 GB          | **3–8 GB (HIGH UNCERTAINTY)** | **biggest swing** — 46 B/doc raw (40-hex `info_hash` + `:idx`); store is LZ4 block-compressed and the 40-hex prefix repeats across a torrent's ~52 files, so the ratio is **whatever LZ4 achieves — must be measured** |
| **`size` INDEXED term-dict + postings**              | _omitted_       | **+3–5 GB**                   | **§6 omission #1** — review B3: hundreds of millions of _near-unique_ u64 values ⇒ big FST term-dict + 873 M postings. **Not needed for range** (FAST does it).                                                        |
| **`published_at` INDEXED term-dict + postings**      | _omitted_       | **+0.6–1 GB**                 | distinct ≈ #torrents (denorm) so cheaper than `size`, but still real. Not needed for range.                                                                                                                            |
| `path` term-dict + postings + `.pos` (**v1.1 only**) | +2–4 GB         | **+8–18 GB, CJK-broken**      | review H2 — separate project, separate tokenizer decision; out of v1 scope                                                                                                                                             |

### Headline predictions

- **v1 FAST-only (recommended — drop INDEXED on `size`+`published_at`):** **~14–18 GB**
  (size 4.4 + published FAST 3.3 + doc_id store 3–8 + info_hash 1.5 + ext 2 + content_type 1).
- **v1 spec-as-written (INDEXED on `size`+`published_at`):** **~19–24 GB** (+3–6 GB of term dicts, almost all from
  `size`'s near-unique-value dictionary).
- **Net:** review's "10–16 GB, not 8–12" is closer than the spec, and I expect it to land **slightly higher still**
  because of `doc_id` STORED. The two biggest under-weighted costs are **`doc_id` store** and **`size` INDEXED dict** —
  exactly what the variant matrix below isolates.

---

## 3. The experiment — a schema-variant matrix (isolates every component)

Because Tantivy bundles all FAST fields into one `.fast` file (and all term dicts into `.term`/`.idx`), you cannot
attribute a single field's cost from one index. **Build the same N torrents into several schema variants and diff the
component file sizes.** Each variant always includes the mandatory delete key (`info_hash` INDEXED) so deletes work.

| #         | Variant                                                                            | Isolates                                                                      |
| --------- | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| V1        | `doc_id` STORED + `info_hash` INDEXED only                                         | baseline: `.store`(doc_id) + `.term`/`.idx`(info_hash)                        |
| V2        | **identity-as-FAST**: `info_hash` FAST + `file_index` FAST, **no** stored `doc_id` | the cheaper-identity alternative; **compare `.fast` Δ vs V1's `.store`**      |
| V3        | V1 + `size` **FAST**                                                               | `size` FAST cost                                                              |
| **V4**    | V1 + `size` **INDEXED\|FAST**                                                      | **Δ(V4−V3) = `size` INDEXED term-dict+postings — the key two-variant number** |
| V5        | V1 + `extension` STRING\|FAST                                                      | extension cost (FAST ordinal + postings)                                      |
| V6        | V1 + `content_type[]` FAST                                                         | content_type multivalued cost                                                 |
| V7        | V1 + `published_at` **FAST**                                                       | published FAST (validate the ~3.3 GB)                                         |
| **V8**    | V1 + `published_at` **INDEXED\|FAST**                                              | **Δ(V8−V7) = published INDEXED cost**                                         |
| **V9**    | **Full v1, spec-as-written** (size+published INDEXED\|FAST)                        | total INDEXED config                                                          |
| **V10**   | **Full v1, FAST-only** (size+published FAST only)                                  | **the GO/NO-GO size number** = recommended config                             |
| V11 (opt) | V10 + `path` TEXT (WithFreqsAndPositions)                                          | v1.1 path-FTS smoke (review H2) — separate decision                           |

**Two-variant headline (settles "drop INDEXED"):** report **V9 − V10** = the total term-dict tax we remove by
dropping INDEXED on the two numerics. Predict **3.6–6 GB**. Confirm range queries still work on V10 (FAST-only) —
`RangeQuery` on a FAST u64/i64 is served by `FastFieldRangeWeight` (`range_query.rs:102-117`), no INDEXED needed.

**Identity headline (a bonus the spec didn't cost):** report **V1.`.store` − V2.`.fast`**. If FAST identity is cheaper
(my guess: stored doc_id 3–8 GB vs FAST info_hash+file_index ~3.8 GB), recommend FAST identity and read
`(info_hash, file_index)` from the columnar reader at collection time instead of from the doc store.

### Multi-scale extrapolation (don't trust a single ratio)

Term-dict/FST/store-block costs have a **fixed-ish floor + sublinear growth**; FAST columns are **linear**. A single
bytes/doc ratio from small N will _over_-count the linear part and _under_-count nothing useful. So build each variant
at **≥2 scales** and fit per component:

- **N1 = 1 M docs** (~19,300 torrents) — first post-merge index.
- **N2 = 10 M docs** (~193,000 torrents) — enough merges + term cardinality to expose dict growth.
- **N3 = 50 M docs** (~965,000 torrents) — optional, only if N1→N2 slope is non-linear (it will be for `size` INDEXED).

Fit `bytes(N) = a·N + b·N^c` per component (FAST: c≈0, pure `a·N`; term dicts: 0<c<1). Extrapolate to N=873 M.
Report a **range** (linear-upper vs sublinear-fit) per component and a summed v10 total with error bars.

---

## 4. Backfill TIME / CPU / merge RAM (review H1)

Measured on the same runs that build §3:

- **Throughput** docs/s at N1 vs N2 (N2 carries more segment merges ⇒ throughput _drops_; that drop is the merge tax).
  Extrapolate to 873 M. Tantivy single-writer simple-doc ingest is CPU-bound; v1 has **no tokenized path** so it's
  numeric/keyword adds + zstd-blob decode only — fast. Expect the merge tax, not the add, to dominate at 873 M.
- **CPU**: pin the writer heap at the production `WRITER_HEAP_BYTES = 256 MiB` (`index.rs:15`); record cores used and
  user+sys CPU-seconds. The spec sizes two 256 MiB heaps for the live two-index process (§4.2) — the **backfill** runs
  with the serving Deployment scaled to 0 (§7), so it gets the whole box.
- **Peak merge RAM (RSS)**: sample during the final big merge — merging FAST columns + term dicts is the RAM peak.
  Tantivy merges stream/mmap, so expect low-GB, not "load the index". Confirm it.
- **Peak disk**: during merge old+new segments coexist ⇒ up to ~2× the final index. At ~20 GB final that's ~40 GB
  transient — trivial on the 200 Gi PVC, but **log it** so the deploy plan doesn't get surprised.
- **Segment-merge behavior at high doc counts**: confirm no single-segment u32 doc-id overflow (873 M ≪ 4.29 B, fine),
  and record final segment count + largest segment after a forced `merge`/`wait_merging_threads`.

Extrapolation: `time(873M) ≈ (N/throughput) + merge_tax(N)`. Report wall-clock low/high. (The 46 M-doc _torrent_ index
backfilled in ~1.5–6.5 h; the file index is ~19× the docs — H1 expects materially longer; the smoke pass quantifies it.)

---

## 5. Query latency (the decisive number)

Build the **full V10** index at the largest feasible N (≥10 M docs; ideally 50 M so segment count/postings are
realistic — latency at small N flatters the result). Replicate the file read-path (a `BooleanQuery` of a `TermQuery`
on `extension` ∧ a FAST `RangeQuery` on `size`, mirroring `query.rs:185-226`), then:

| Scenario                                  | Query                                                                             | Collector           | Report                                                                                 |
| ----------------------------------------- | --------------------------------------------------------------------------------- | ------------------- | -------------------------------------------------------------------------------------- |
| **A — file-level (primary, D6)**          | `ext ∈ {mkv} ∧ size ≥ 1e9`, sort by size desc, top-20                             | `TopDocs` + `Count` | **p50/p95/p99** over ≥1000 randomized queries                                          |
| **B — collapse-to-torrent (D6, at-risk)** | same predicate, then dedup `info_hash` (terms agg on the FAST field)              | full match-set scan | p50/p95/p99 — **this is the latency risk** (forfeits TopDocs early-termination, §13.2) |
| **C — selectivity sweep**                 | vary ext (common `mkv`/`mp4` vs rare `flac`) × size threshold (1 GB / 100 MB / 0) | both                | latency vs match-set size curve                                                        |

Randomize across real extensions + thresholds; warm the mmap cache first; measure steady-state. **The <50 ms claim is
for A (top-k file-level).** B scales with match-set size (∝ corpus) — extrapolate from C's curve to the 873 M match
sets. If B blows past ~50 ms for common extensions, that confirms the spec's own caveat (collapse is approximate /
gated) and pushes the product toward the §13.2 per-(torrent,ext) PG aggregate for exact torrent-collapse instead of
the index.

---

## 6. 🚨 Dependency resolution — `file_schema`/`backfill_files` don't exist yet (Phase B)

The file-index code is Phase B; gating Phase B on a benchmark that _needs_ Phase B is circular. Two options:

- **(a) Gate on building Phase B first** — accurate (exact production schema) but couples the gate to the build it is
  meant to authorize, and burns the proto/gRPC/Go work _before_ we know the index is worth it. **Rejected.**
- **(b) RECOMMENDED — a minimal standalone Rust harness** that emits the _same_ schema from decoded blobs, no
  proto/gRPC/Go, no changes to the shipped sidecar.

### Sketch of (b) — `crates/bitmagnet-search-bench` (dev-only sibling crate, not in the prod binary)

Reuses what already exists, so it's ~250 LOC:

- **Data source = already shipped:** `bitmagnet_db::stream_torrents_with_files` (`stream.rs:58`) — keyset page over
  `torrents`, returns `(info_hash, name, size, files_status, files_count, files_data)`. Decode the blob with
  `bitmagnet_model::deserialize_files` (`blob.rs:60`).
- **Doc construction = mirror §4.3 + the G1 fix:** one doc per `BlobFile`; derive `extension` via
  `bitmagnet_model::file_extension_from_path` (**never the blob's stored `e`** — G1; the blob's `e` is empty for
  crawl-path torrents). Single-file synthesis (D5): `files_status == "single"` ⇒ one doc
  `{file_index:0, extension: file_extension_from_path(name), size: torrent.size}`.
- **Schema = a literal copy of the §4.1 table**, built with the same flag idioms as `schema.rs:149/152`
  (`keyword_facet = (STRING|STORED).set_fast(None)`; `numeric = (STORED|INDEXED|FAST)`), parameterized so one harness
  emits any of V1–V11 via a `--variant` flag.
- **Denorm fields (`content_type[]`, `published_at`) — synthesize, don't join.** `stream_torrents_with_files` carries
  no content*type/published_at. For \_sizing* only the **cardinality + range** of a FAST/INDEXED column matter, and both
  are well-characterized: `published_at` = distinct ≈ #torrents over a ~3-yr window (derive deterministically from
  `info_hash` bytes → a plausible Unix-sec in `[now-3y, now]`); `content_type` = 1 ordinal drawn from ~10 values on a
  realistic weight. This keeps the harness reading **only the `torrents` table** (lightest possible) and reproduces the
  columnar cost faithfully. (An "accurate" mode can add a small `published_at`/`content_type` SELECT if we want to
  confirm the synthesis — optional.)
- **Index + measure:** open via the existing `index::{open_or_create-equivalent, writer}` idiom (256 MiB heap), commit
  every 10 k docs, force-merge, then `du`-by-extension the segment files for §3 and time the run for §4.
- **Latency:** open a reader, run the §5 queries directly (TermQuery ∧ FAST RangeQuery), no gRPC.

CLI: `bench-file-index --variant V10 --limit-docs 10000000 --index-path /tmp/fidx --batch-size 1000`.

### Zero-production-risk execution (server-safety compliant)

The only server touch is **one bounded read-only export**, then everything runs offline:

1. **One** read-only keyset slice — `COPY (SELECT info_hash,name,size,files_status::text,files_count,files_data
FROM torrents WHERE files_status IN ('single','multi') AND info_hash > '\x00' ORDER BY info_hash LIMIT 965000)
TO STDOUT` (binary) → a ~1 GB file. ~965 k torrents ≈ 50 M docs ceiling for all variants/scales.
2. Copy to **HEL1 (idle)**; the harness reads the dump (add a `--from-dump` source alongside the live `--postgres-dsn`)
   and builds indexes locally. **No index-building CPU on FSN1** (83% mem) and **no live DB load** beyond the single
   bounded SELECT. Needs user go-ahead (it connects once), but it is minimal and read-only.

---

## 7. Deliverables of the run

1. Per-component bytes/doc table at N1/N2(/N3) + extrapolated 873 M totals with ranges (§3).
2. **V9 − V10** (the INDEXED tax) and **V1.store − V2.fast** (the identity choice).
3. Backfill wall-clock / CPU-s / peak RSS / peak disk extrapolated to 873 M (§4).
4. SearchFiles p50/p95/p99 for scenarios A/B/C + the selectivity curve (§5).
5. A one-line GO/NO-GO: **GO** if V10 ≤ ~20 GB **and** scenario A p95 < 50 ms; **re-evaluate vs DuckDB-on-blobs** if A
   is fast but B (collapse) is slow or V10 > ~25 GB.

---

## 8. My size prediction (the bet)

- **v1 FAST-only (V10, recommended): ~14–18 GB** — above the spec's 8–12 and the top of the review's 10–16, driven by
  `doc_id` STORED (3–8 GB, the measurement that matters most) and `published_at` FAST (3.3 GB).
- **v1 INDEXED (V9): ~19–24 GB** — the extra 3.6–6 GB is almost entirely `size`'s near-unique-value term dict.
- **Recommendation regardless of exact number:** (1) **drop INDEXED on `size`+`published_at`** (range via FAST),
  (2) **measure `doc_id` STORED vs FAST identity** and likely switch to FAST identity, (3) treat **latency (scenario A
  vs B), not size, as the real gate** — the index fits the PVC either way; what it must justify is <50 ms over the
  +0 GB DuckDB alternative.
