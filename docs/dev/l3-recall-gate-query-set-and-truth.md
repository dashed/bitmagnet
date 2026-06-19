# L3 Recall Gate (gate 6) — Query Set, Ground-Truth SQL, Truth-File Format

Author: `recall-engineer` (team `bitmagnet-ps-harness`, task H3)
Index under test: **live prod L3 per-torrent path-bag Tantivy index, 25.59M docs**
Truth source: **PROD FSN1 PG** `postgresql://postgres@127.0.0.1:5432/bitmagnet` (port-forward, serial 1 conn,
client `statement_timeout = 15s`). **NOT** the HEL1 bench restore (different/pre-backfill population → false misses).

> Rev 2 — incorporates the team's H3 constraints: 5000-cap → selective-only recall; same-population = prod
> FSN1; freshness watermark filter; 15s timeout bounding. Two corrections vs. the initial guidance are called
> out inline (🔧): the freshness column is **`updated_at`** (not `created_at`), and the I/O bound must be
> **`TABLESAMPLE SYSTEM`** page-sampling (not a PK info_hash-range slice).

## 0. What L3 actually does (verified in source)

- Query guard (`pathsearch/query.rs:52-73`): reject if `raw.trim().chars().count() < 2`
  → **server floor = 2 chars**; we set the **query-set floor at 3 chars** (a single 2-gram is too broad — §3).
- Tokenizer (`pathsearch/schema.rs:65-71`): `NgramTokenizer::new(2, 3, false)` + `LowerCaser`.
  Char-based 2- and 3-grams, all positions, then lowercased. CJK has no case → unaffected.
- Query build: tokenize query into its **distinct** 2/3-grams and AND them (`BooleanQuery`/`Occur::Must`).
- Indexing (`pathsearch/indexer.rs:10-22`): each file path is a **separate value** of the `path` field
  → Tantivy tokenizes each path independently → **no cross-file grams**.
- Doc source (`pathsearch/document.rs`): paths come from the **files_data blob**; single-file torrents with
  no blob fall back to the torrent **name**. Non-empty paths only; **padding files are indexed (not filtered)**.
- Output (`query.rs:165-183`): candidate `info_hash` = **raw 20 bytes**, plus exact `candidate_total`
  (a Tantivy `Count` over the whole index — torrent-doc granular, **not capped**).
- Caps (`query.rs:15-17, 90-102`): returned list = `min(limit + oversample, 5000)`.
  **`MAX_CANDIDATES = 5000` is a hard ceiling on the returned list.**

## 1. Recall guarantee → ground truth = case-insensitive contiguous substring on `torrent_files.path`

Any torrent with a file path containing the exact substring **S** contains every 2/3-gram of S (S is contiguous
within that one path value), so the ngram conjunction matches it. Therefore:

> **{torrents with a per-file path containing S} ⊆ L3 candidate set** (recall ≈ 100%, superset).

L3 may add false positives (grams scattered across files / across the query) — fine, this is recall-FIRST; L1/L2
do exactness. We only ever check **truth ⊆ L3**, never the reverse. (A non-contiguous L3 hit correctly absent
from truth is a *precision* artifact, not a recall miss — confirmed by harness-reviewer.)

## 2. Predicate — `position(lower())`, not raw ILIKE

To mirror L3 (LowerCaser + literal contiguous substring) and be safe against ILIKE metachars (`% _ \`):

```sql
position(lower($1) IN lower(path)) > 0
```

Case-insensitive literal substring (≡ `ILIKE '%'||$1||'%'` for metachar-free $1). PG `lower()` and Rust
`LowerCaser` agree on ASCII and caseless CJK — avoid Turkish-I/ß queries (none in the set). `info_hash bytea`
→ emit **lowercase 40-char hex** via `encode(info_hash,'hex')`; L3's raw bytes → harness `.hex()` lowercase.

## 3. Query set (floor 3 chars; SELECTIVE only — recall is meaningful only when `candidate_total ≤ 5000`)

Broad single-gram terms (`.m`, `01`, `1080`, bare `x264`) match millions → exceed the 5000 cap → **excluded
from recall; they belong to the LATENCY set.** Recall queries are long/compound/CJK substrings biased to be
selective. The harness reads each query's exact `candidate_total` and **keeps it in the recall metric only if
`candidate_total ≤ 5000`** (equivalently `returned_size == candidate_total`); over-cap queries are auto-dropped
to "latency-only". So a few of the candidates below may auto-drop — that's expected; provide a surplus.

### 3a. ASCII compound / realistic (most likely selective)
`1080p.bluray.x264`, `2160p.web-dl`, `dolby.atmos`, `s01e01.1080p`, `directors.cut`, `complete.series`,
`hevc.10bit`, `remux.2160p`, `extended.edition`, `flac.24bit`

### 3b. ASCII single-token (selectivity uncertain — candidate_total decides)
`bdremux`, `webrip`, `hdcam`

### 3d. FINDING (empirical, H4 live probe) — gram-match vs literal-substring divergence

Two distinct match models drive the two halves of the gate, and they diverge widely:
- **L3 `candidate_total`** = ngram(2,3) **token/gram** match: a torrent counts if it contains all the
  query's grams *scattered anywhere*. Dotted-digit compounds are built from ultra-common grams
  (`10`,`it`,`bit`,`.1`), so they balloon: `x265.10bit`=58 522, `web-dl.ddp5.1`=126 836, `bluray.remux`=26 639
  — all far over the 5000 cap despite "looking" specific.
- **PG truth** = `position(lower(q) IN lower(path))` = **literal contiguous substring**. Real paths use varied
  separators, so a literal dotted compound is often *rare*: `complete.series` cand=4745 → **1 truth**;
  `extended.edition` cand=1900 → **2 truth**. Whereas a real on-disk convention matches both:
  `hevc.10bit` cand=3698 → **13 truth**; `第01集` cand=2395 → **11 truth**.

⟹ **Dotted multi-token ASCII compounds are doubly bad recall queries**: grams scatter (inflate
`candidate_total` over the cap) *and* the literal form is rare (starves truth). The **ideal recall query is
literal-in-path**: (a) **CJK markers** (`第NN集`, `真人秀` — no separators, written literally; candidate_total ≈
literal-count) and (b) **distinctive single tokens** (release groups, rare tags — their grams co-occur only in
the token itself, so candidate_total ≈ literal-count → reliable for BOTH `≤5000` membership AND `≥10` truth).
The band is structurally narrow: across ~45 live-probed terms (21 round-1 dotted compounds + 12 round-2 CJK
markers/phrases + 12 round-3 distinctive single tokens) only ~8 landed in `[1500,5000]` with usable literal
truth — and 6 of those are CJK (episode markers `第NN集`, plus `真人秀`/`粤语中字`/`纪录片`) versus just 2 ASCII
(`hevc.10bit`, a real on-disk convention; `qxr`, a distinctive release-group tag). Even single tokens mostly
gram-scatter over the cap (of 12 probed — `tigole`,`framestor`,`remastered`,`criterion`,… — only `qxr`
survived). This token-gram→broad tension is an inherent L3 property, not a harness limitation, and is why the
robust recall sample is necessarily CJK-heavy.

**Gate 6 result (live, widened set):** tested=10 (candidate_total ≤ 5000), total truth hashes=84,
**min_recall = 1.0000, real_misses = 0**, robust ≥10-truth queries spanning both charsets — a ~4× widen over
the initial 2 robust. Recall is a structural guarantee (literal substring ⊆ ngram-conjunction), so 1.0 is the
expected and observed outcome; the value of the widen is coverage breadth/credibility, not a different verdict.

### 3c. CJK — the real recall differentiator (15.2% of corpus)
Longer fragments are the carriers (selective + exercise 2-gram∧3-gram conjunction); the 2-char ones are likely
over-cap and will auto-drop:
- carriers: `蓝光原盘` (Blu-ray disc), `国语中字` (Mandarin + CN subs), `第01集` (episode 01),
  `字幕组` (subtitle group), `アニメ` (katakana "anime"), `繁體中字` (Traditional-CN subs)
- likely-broad (auto-drop expected): `高清` (HD), `电影` (movie)

## 4. Recall method & interpretation (single method — sample membership, gated on the cap)

Per query the harness: reads `watermark_epoch` from HealthCheck **once at run start** (W); issues L3
`path_candidates` with **`limit = 5000`** (→ returned = `min(candidate_total, 5000)`, any sort); records
`candidate_total` + `returned_size` + the returned hex set.

- **Valid iff `candidate_total ≤ 5000`** → L3 returned its FULL match-set → the sampled truth (a subset of the
  whole-index match-set) must be wholly contained → `recall = |truth ∩ returned| / |truth|` **must = 1.0**.
- `candidate_total > 5000` → set `membership_valid = false`, **exclude from the recall metric** (it's a latency
  query). Truth hashes "absent" here are below the TopDocs cap, not misses.

Because truth is sampled (§5), this is a **systematic-gap / correctness gate, not a precise recall %**: even
~10–40 truth hashes/query is a real per-hash test, and **any single real miss fails the gate** → triage:

### 4c. A truth hash absent from returned candidates, with `candidate_total ≤ 5000` (a REAL miss). It is NOT:
- **the cap** (excluded: `candidate_total ≤ 5000`), nor **freshness** (excluded: §5 `updated_at ≤ W − margin`).
- So investigate, in order:
  1. **L3 tombstoned the torrent** — empty/undecodable `files_data` blob, or a re-crawl that lost its files
     (`apply_changed_row` → Tombstoned/BlobError). Re-decode the torrent's blob and diff vs `torrent_files`.
     → a **blob⟷torrent_files divergence** (GATE-A territory), not ngram logic.
  2. **single-file name-fallback mismatch** — `files_status='single'` with empty blob: L3 indexed the torrent
     **name**, truth used the `torrent_files.path`; if they differ textually the substring can miss.
  3. **tokenization/normalization edge** — PG `lower()` vs Rust `LowerCaser` disagree on a query char (avoided by
     query choice; flag if it recurs).
  4. otherwise → genuine coverage/index bug → escalate.

## 5. Ground-truth SQL — bounded by page-sampling + freshness-filtered (read-only, LEAD-GATED, H4)

🔧 **Bounding correction:** a PK `info_hash`-range slice does **NOT** bound I/O. `info_hash` is SHA-1 (uniform,
uncorrelated with heap/insertion order), so any range holds rows scattered across ~every heap page → a bitmap
heap scan touches nearly the whole 277GB table. Bound by **physical-page sampling** instead:
`TABLESAMPLE SYSTEM (p)` reads ~p% of *pages* → genuinely bounded I/O. `REPEATABLE(seed)` fixes the sampled
pages so every query sees the **same** sampled population (clean cross-query comparison + reproducible truth).

🔧 **Freshness correction:** the follow loop carves on **`updated_at`**, not `created_at`
(`stream.rs:111` `WHERE updated_at > to_timestamp($1) AND updated_at <= to_timestamp($2)`; the comment notes
`persist.go` bumps `updated_at` on every `DoUpdates`, so it captures **re-crawls**). And the watermark is
published **only after** the window's rows are committed + reader-reloaded (`bitmagnet-pathsearch.rs:174-180`),
so `watermark_epoch` is a sound "searchable-up-to" bound. A `created_at` filter would miss the re-crawl
false-miss (old torrent re-crawled recently with a new fileset containing q). `created_at` lives on `torrents`
— and so does `updated_at` — so the truth query **joins `torrent_files ⋈ torrents` on `info_hash`** for the
filter. `CARVE_LAG_SECS = 30`; the watermark already trails wall-clock by 30s, so a small extra **margin = 60s**
is belt-and-suspenders (clock skew / commit-visibility). Subtracting margin only shrinks truth (never a false
miss).

```sql
-- H4, read-only, lead-gated. Per query. statement_timeout=15s, serial 1 conn.
--   $1 = query string
--   $2 = (watermark_epoch read from HealthCheck at run start) - 60   [margin]
SELECT DISTINCT encode(s.info_hash, 'hex') AS info_hash_hex
FROM torrent_files TABLESAMPLE SYSTEM (2.0) REPEATABLE (4242) s   -- ~2% of pages → bounded I/O; §5c knob
JOIN torrents t ON t.info_hash = s.info_hash
WHERE position(lower($1) IN lower(s.path)) > 0
  AND t.updated_at <= to_timestamp($2)                           -- freshness: only indexed-and-searchable torrents
LIMIT 500;                                                       -- truth-set bound (selective ⇒ rarely reached)
```

- Cost: dominated by the ~2% page-sample read (~5GB of 277GB), substring-filtered inline; only the few
  survivors join to `torrents` (PK lookup) for the watermark check. Each statement bounded to a few seconds.
- Do **not** add padding/extension filters — L3 indexes padding paths, so truth must too (§0).
- **Truth is a uniform sample of matching torrents.** Soundness holds regardless of sample size: a sampled row
  proving torrent⊇q ⟹ that torrent is an L3 candidate. Sample size ≈ `p% × whole-index match count`.
- Optional one-scan variant: `CREATE TEMP TABLE tf_sample AS SELECT info_hash,path FROM torrent_files
  TABLESAMPLE SYSTEM (2.0) REPEATABLE (4242);` then run the per-query filter against `tf_sample` (cheap). Needs
  a few GB temp space + the `CREATE` must itself finish within 15s — coordinate with access-engineer; the inline
  per-query form above is the safer default.

### 5c. Knobs
- **Sample %** `TABLESAMPLE SYSTEM (p)`: lower p (→1.0/0.5) if a query times out at 15s; raise p (→5) for bigger
  truth samples on very selective queries. Same `REPEATABLE` seed across all queries + the L3 run.
- **Stronger full-truth option (opt-in, gated):** for a few marquee queries, drop `TABLESAMPLE` and raise
  `statement_timeout` deliberately to get the *complete* truth set (full seq scan, minutes/query) — lead +
  access-engineer decide; not the default.

## 6. Truth-file format (JSON) — for harness-builder

Lead fills `truth_info_hashes` (§5) in H4; harness fills `_runtime` at run time. `watermark_bound_epoch` is the
contract value the lead plugs into `$2` (= HealthCheck `watermark_epoch − 60`), recorded so truth + L3 agree.

```json
{
  "meta": {
    "generated_by": "recall-engineer H3 (rev2)",
    "index": "L3 prod path-bag, 25.59M docs",
    "truth_source": "prod FSN1 torrent_files JOIN torrents (postgresql://postgres@127.0.0.1:5432/bitmagnet)",
    "info_hash_encoding": "lowercase_hex_40char_no_prefix",
    "predicate": "position(lower($q) IN lower(path)) > 0",
    "sampling": "TABLESAMPLE SYSTEM (2.0) REPEATABLE (4242)",
    "freshness_filter": "torrents.updated_at <= to_timestamp(watermark_bound_epoch)",
    "watermark_bound_epoch": null,
    "watermark_margin_secs": 60,
    "limit_per_query": 500,
    "query_floor_chars": 3,
    "l3_request": { "limit": 5000, "oversample": 0, "note": "returned = min(candidate_total, 5000)" },
    "membership_valid_when": "candidate_total <= 5000  (== returned_size)"
  },
  "queries": [
    { "id": "ascii_1080p_bluray_x264", "q": "1080p.bluray.x264", "class": "recall", "lang": "ascii",
      "truth_info_hashes": [], "truth_sample_count": null,
      "_runtime": { "candidate_total": null, "returned_size": null, "recall": null, "membership_valid": null } },
    { "id": "cjk_bluray_disc", "q": "蓝光原盘", "class": "recall", "lang": "cjk",
      "truth_info_hashes": [], "truth_sample_count": null,
      "_runtime": { "candidate_total": null, "returned_size": null, "recall": null, "membership_valid": null } }
  ]
}
```

Harness rules:
- Read `watermark_epoch` from HealthCheck once at run start; set `meta.watermark_bound_epoch = watermark_epoch −
  60`; give it to the lead for `$2`.
- Per query: L3 `limit=5000`; record `candidate_total`, `returned_size`. Hex-encode L3 info_hash lowercase.
- `candidate_total ≤ 5000` → `membership_valid=true`; `recall = |truth ∩ returned| / |truth|`; **gate fails if
  recall < 1.0** → §4c triage.
- `candidate_total > 5000` → `membership_valid=false`; drop from recall metric (latency-only).
