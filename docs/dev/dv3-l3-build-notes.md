# DV-3 / L3 — Path-FTS sidecar build notes

**Task:** `bitmagnet-deploy` #3 (DV-3, V4 prereq) · **Agent:** `dv3-l3` · **Date:** 2026-06-10
**Mode:** BUILD only — local code, compile-checked + unit-tested. **Nothing deployed, applied, or run against prod/k3s/HEL1.**
**Branch:** `feat/l3-pathsearch` (off `feat/file-grained-search`), built in an isolated jj workspace (`/Users/me/aaa/github/bitmagnet-l3`) — see [§0 Working-copy note](#0-working-copy-note).
**Design source of truth:** [`pathsearch-T3-index-design.md`](./pathsearch-T3-index-design.md) (schema/levers) + [`pathsearch-T4-deploy-ops.md`](./pathsearch-T4-deploy-ops.md) (deploy/ops). Reference impl: `bench-file-index` crate (ngram schema + `build_path_query`).

---

## 0. Working-copy note (coordination)

The DV agents share one on-disk working copy at `/Users/me/aaa/github/bitmagnet`. jj snapshots the whole working copy on every command, so concurrent edits entangle into whichever change is `@` (my first proto edit there was reverted by a peer's checkout). **Mitigation taken:** I created an isolated `jj workspace add` at `/Users/me/aaa/github/bitmagnet-l3` (separate working-copy dir, shared repo) and did all L3 work there. The bookmark `feat/l3-pathsearch` points at the clean change. When committing/merging, this branch is independent of dv2's `feat/l2-filesearch` and dv4's Go work. Recommend the team isolate per-agent workspaces or serialize commits.

---

## 1. What this is

A **second, narrow engine** added to the existing `bitmagnet-search` sidecar crate: a **per-torrent path-bag** char-ngram(2,3) Tantivy index that answers CJK-correct free-text **path typeahead**. It is independent of (and additive to) the torrent-grained main-search modules in the same crate. It does **not** replace PG-FTS main search, nor the DuckDB-on-Parquet structured per-file tier — it adds the one thing neither offers cheaply: broad, interactive substring path search.

Granularity is **per-torrent** (PS-T3 Lever 5): one doc per torrent, holding *all* its file paths as the bench-validated **14.0 GiB keyed shape** — `WithFreqs` (no positions), info_hash delete-key. A hit identifies a *torrent* (ranked by seeders); display text + which files matched are hydrated downstream by info_hash (blob / DuckDB).

---

## 2. What's built (compile-checked + unit-tested)

All in branch `feat/l3-pathsearch`. `cargo check`/`clippy -p bitmagnet-db -p bitmagnet-search --bins --tests` clean; **87 unit tests pass** (74 search incl. 24 new pathsearch, 13 db) — all in-RAM / SQL-shape, no live DB needed.

### proto — `bitmagnet-rs/proto/bitmagnet/search.proto`
New `PathSearchService` (alongside `SearchService`, same `:50051`):
- `PathTypeahead(PathTypeaheadRequest) → PathTypeaheadResponse` — `query` + `Pagination`; returns `repeated PathHit{info_hash, seeders, files_count, size, score}`.
- `IndexTorrentPaths(stream TorrentPathDocument) → IndexPathsResponse` — optional push source (a) (Phase-4).
- `DeleteTorrent(DeleteDocumentRequest) → DeleteDocumentResponse` — supersession/removal.
- `HealthCheck` — reuses the existing message (doc_count = path-bag docs).

### crate `bitmagnet-search` — new `pathsearch/` module (mirrors the main-search split)
| File | Responsibility |
|---|---|
| `pathsearch/tokenizer.rs` | The **one** ngram analyzer (`NgramTokenizer(2,3,false)` + `LowerCaser`) registered for **both** writer and query (`path_grams`), `WithFreqs`-no-positions rationale. The CJK-correctness invariant. |
| `pathsearch/schema.rs` | Per-torrent path-bag schema: `info_hash` BYTES INDEXED+STORED (delete-key + hit id); `path_grams` ngram(2,3) `WithFreqs` (a test asserts `!has_positions`); `seeders`/`size`/`files_count` FAST. |
| `pathsearch/index.rs` | `open_or_create_path` + the **single-thread, 2 GiB-arena** writer (`path_writer`) — the EXP-D crash-avoidance config (compile-time `const _` asserts ≥2 GiB). |
| `pathsearch/indexer.rs` | `build_document` (one `path_grams` value **per file** = no boundary grams; name fallback when the blob is empty) + `upsert` (delete_term(info_hash)+add) + `delete`. |
| `pathsearch/query.rs` | `path_typeahead`: tokenize → **guard** (`gram_guard_ok`: ≥2 grams OR a single CJK gram = "≥3 ASCII / ≥2 CJK chars") → gram-**conjunction** `BooleanQuery` → `TopDocs::order_by_fast_field(seeders, Desc)`; reads info_hash (stored) + size/files_count (FAST). **No `Count`** on the hot path (`total_hits` = page size). |
| `pathsearch/follow.rs` | The **`--follow` PG-tail watermark loop** (the required fork addition) + `Watermark` persistence + `index_torrent_row` (shared DB-row→doc glue). |
| `pathsearch/server.rs` | `PathSearchServer` impl of `PathSearchService`; shares the SOLE `Arc<Mutex<IndexWriter>>` with the follow loop; exposes `writer_handle()`/`reader()`/`fields()`. |
| `src/bin/pathsearch_server.rs` | Serving binary; spawns the follow loop when `--follow`. TCP/unix listen (reuses the main binary's `Listen` idiom). |
| `src/bin/backfill_files.rs` | One-shot initial build: info_hash-keyset pages → `index_torrent_row`; **no force-merge** (incremental merge retained); seeds the follow watermark on completion. |

### crate `bitmagnet-db` — `pathsearch_stream.rs`
- `TorrentForPathIndex` DTO (info_hash, name, size, files_status, files_count, **seeders**, updated_at_micros, files_data) + `.files()` blob decode.
- `stream_torrents_for_pathsearch` — **backfill**, keyset on `info_hash` PK (also projects `updated_at_micros` to seed the watermark).
- `stream_torrents_for_pathsearch_since` — **follow**, keyset on the `(updated_at_micros, info_hash)` watermark.
- Both drive FROM `torrents` (per-torrent, unlike the torrent_contents-driven main backfill) with a correlated `MAX(tc.seeders)` sub-select. SQL is `&'static str` (sqlx 0.9 `SqlSafeStr`), no dynamic data.

### `bitmagnet-rs/docker/Dockerfile.search`
Added the two new binaries to the runtime image (`pathsearch-server`, `backfill-files`) + `BITMAGNET_PATHSEARCH_*` env + the `search-files` dir. Same `cargo build -p bitmagnet-search` already compiles them.

### Key design decisions (each grounded)
- **Watermark = sidecar file, not index meta.** `<index_dir>/../.pathsearch-watermark`, written atomically (temp+rename), two lines `<micros>\n<hex>`. *Justification:* Tantivy's index meta is owned by the segment/merge lifecycle and is not an app-cursor store — coupling the watermark to it risks loss on a merge and fights Tantivy's atomicity. A standalone rename-on-write file is crash-safe, lifecycle-independent, and inspectable. Missing file ⇒ epoch `(0,[])` ⇒ the loop re-sweeps the corpus (self-healing gap-closer after a backfill).
- **`updated_at` as `bigint` micros**, not a timestamptz — the workspace sqlx has no `chrono`/`time` feature; `EXTRACT(EPOCH …)*1e6` keyset-compares cleanly and micros matches PG `timestamptz` precision.
- **2-char CJK allowed, 2-char ASCII rejected.** A 2-char ASCII query = 1 broad bigram (the measured 100–320 ms worst case); a 2-char CJK query = 1 *selective* bigram (huge gram vocabulary). The guard exempts a lone CJK gram — exactly PS-T3's "min-3-chars ASCII / 2-char CJK".
- **Backfill seeds the watermark to `max(updated_at)` seen** (empty info_hash tiebreak), so the serving pod's follow loop starts incrementally rather than re-sweeping 17 M rows.

---

## 3. What's stubbed / deliberately out of scope

- **No homelab role files written** — the role delta is *described* in §4 (per task; do NOT commit to homelab-infra).
- **No deploy, no image build, no backfill run, no V4 validation run** — this is the BUILD wave; V4 runbook is outlined in §5.
- **Go / GraphQL / web-UI wiring** — the `pathTypeahead` resolver + the default-off `SEARCH_PATH_TYPEAHEAD_ENABLED` gate are a separate, flag-gated step (PS-T4 §8), not built here. (DV-4 owns Go-side work.)
- **Push source (a)** — `IndexTorrentPaths` RPC is *implemented* (so the upgrade needs no proto work) but the Go dual-write that would call it is not. The keep-everything default is the in-pod PG-tail follow loop (zero Go change).
- **`tonic-health`** — not added; probes stay `tcpSocket:50051` (same constraint as the drafted role). A ~10-line add later enables native `readinessProbe.grpc:`.
- **No live-PG integration test** — backfill/follow need a DB; unit tests cover in-RAM index logic + SQL-shape. The existing `--ignored` live-PG pattern (in `bin/backfill.rs`) can be mirrored for a manual check.
- **`seeders` correlated sub-select** — correct but its full-backfill cost is unvalidated (per-torrent `MAX(tc.seeders)` over 17 M rows). It rides the `torrent_contents` PK info_hash prefix; confirm with the V4 smoke gate. If hot, precompute or drop seeders to a `files_count`-only rank.

---

## 4. Homelab role delta (DESCRIBED — not committed)

Clone the drafted `ansible/roles/bitmagnet-search/` → `bitmagnet-pathsearch` (resource *shape* is identical; this is a parameter + lifecycle delta). Concrete changes vs the drafted Phase-3 role:

| File | Drafted (torrent Phase-3) | **L3 path-FTS delta** |
|---|---|---|
| `defaults/main.yml` | torrent index, 200Gi, 6Gi mem, scaled-0 batch | **path-bag knobs** (`BITMAGNET_PATHSEARCH_*`, index `…/search-files`); **PVC recomputed → 60Gi** (see below); mem **requests 2Gi / limit 6Gi** (per-torrent index is far smaller than the 94 GB per-file one — 6Gi is comfortable); **`follow_enabled: true`** + `poll_secs: 15`, `batch: 500`; container command `pathsearch-server` |
| `templates/pvc.yaml.j2` | 200Gi at index dir | **60Gi**, mount the **parent** `/var/lib/bitmagnet` (D0) so the sibling watermark file + any future index land on the PVC, not ephemeral fs |
| `templates/deployment.yaml.j2` | Recreate, tcpSocket, read-only serve, **scaled-0 batch** | **writer SCALED-1 (permanent sole writer)** + **follow-loop env** (`BITMAGNET_PATHSEARCH_FOLLOW=true`, `BITMAGNET_POSTGRES_*`, poll/batch). Recreate strategy + tcpSocket probes **unchanged** (a RollingUpdate surge pod would open a 2nd writer → crash-loop) |
| `templates/backfill-job.yaml.j2` | `bitmagnet-backfill`, torrent_contents | command **`backfill-files`**, single-thread + 2 GiB arena (built into the bin), **no `--force-merge`** |
| `templates/cilium-network-policy.yaml.j2` | serving egress→PG "unused in P3" | **serving egress→PG now REQUIRED** (the follow loop tails PG). `allow_bitmagnet_ingress` only when UI wiring lands |
| `templates/service.yaml.j2` | ClusterIP gRPC :50051 | **unchanged — ClusterIP internal-only**, no Traefik/Authentik/DNS (reaches users via the already-authed web UI) |
| `tasks/{main,backfill}.yml` | PVC→NetPol→Svc→Deploy; scale-0→Job→scale-1 | **unchanged shape**; labels → `…-pathsearch`. Backfill scale-0 dance still mandatory for the *initial* build / full rebuild (single-writer); steady-state freshness comes from the in-pod follow loop, not re-running the Job |
| `group_vars/.../bitmagnet_pathsearch.yml` | torrent pin, 200Gi | path-FTS image pin, 60Gi, follow knobs |

### PVC recompute (the T4 300Gi was for the dead 82–94 GB per-positions per-file era)
- The shipped design is the **per-torrent path-bag, positions dropped = 14.0 GiB** (bench-validated). The 300Gi / 94 GB figure was the per-file `WithFreqsAndPositions` index — **not** what this builds.
- Steady **14.0 GiB**. `LogMergePolicy` background merges transiently ~2× the largest segment (not the whole index); the backfill does **not** force-merge, so there's no full-index transient. Corpus growth headroom (17 M torrents climbing).
- **Recommendation: `bitmagnet_pathsearch_index_storage: 60Gi`** (~4.3× steady — covers a worst-case large merge transient + years of growth). Range **50–100Gi** defensible; pick the high end only if a future force-merge-to-1-segment is ever wanted. local-path is **`ALLOWVOLUMEEXPANSION=false`** (non-expandable) so size up front — but HEL1 has 1.8 TB, so 60Gi is free insurance and ~5× smaller than the drafted 300Gi.

### 🚨 MUST-VERIFY before any deploy — node hostname (read-only, NOT run here)
The drafted role pins `bitmagnet_search_node_hostname: alberto-hetzner`, but the inventory's HEL1 host is **`alberto-hetzner-hel1`** and the real K8s node label may differ from either. A wrong `nodeSelector` strands the node-bound local-path PVC and the pod never schedules. **Verify the actual `kubernetes.io/hostname` label and set it explicitly:**
```bash
ssh ansible@<FSN1_PROD_IP> "sudo k3s kubectl get nodes -o custom-columns=NAME:.metadata.name,HOST:'.metadata.labels.kubernetes\.io/hostname',ROLES:.metadata.labels.node-role\.kubernetes\.io/.* --show-labels" \
  | grep -i hel
# then set bitmagnet_pathsearch_node_hostname to the verified label value.
```

---

## 5. V4 validation runbook (outline — gated, NOT run)

Prereqs: PS-T5 GO (already decided GO for CJK free-text per MEMORY), the node-label verify above, and the image built on FSN1 (`make bitmagnet-search-image-build REF=feat/l3-pathsearch TAG=pathsearch` → digest-pin; the same image carries `pathsearch-server` + `backfill-files`).

1. **Deploy (writer scaled-1, follow idle on empty):** `make bitmagnet-pathsearch-check` (diff) → `make bitmagnet-pathsearch` (PVC at parent `/var/lib/bitmagnet`, NetPol incl. egress→PG, ClusterIP Svc, Deployment). Index empty; follow loop idles.
2. **Smoke gate:** `…-backfill-run LIMIT=100000` (Deployment auto scaled 0→Job→1). Read docs/s + extrapolated index size; **abort unless it projects to ≈14 GiB** at 17 M torrents (PS-T3 GO ceiling) and confirm the `seeders` sub-select isn't a throughput wall.
3. **Full backfill (~62 min measured at this granularity):** `…-backfill-run`. Single-thread + 2 GiB arena (built in), no force-merge. On completion the bin seeds the watermark.
4. **Steady state — follow on:** serving pod scaled-1 with `BITMAGNET_PATHSEARCH_FOLLOW=true`. Verify:
   - `HealthCheck.doc_count ≈` torrents-with-blob (~17 M); index `du -sh ≈ 14 GiB ≤ 60Gi`.
   - A `PathTypeahead` probe (ASCII + CJK substrings) returns plausible seeders-ranked hits; a 2-char ASCII query is rejected, a 2-char CJK query is accepted.
   - **Fresh-lag:** insert/modify a torrent in PG (or wait for a crawl), confirm it becomes typeahead-able within ~`poll_secs` (seconds — this is the keep-everything floor; ms freshness would need the Phase-4 push path).
   - **Supersession:** re-crawl a torrent with a changed fileset → the old fileset's unique substrings stop matching, the new ones match (single `delete_term`+re-add).
   - Web-UI latency unchanged (Go untouched — no wiring yet).
5. **Rollback:** `…-reset CONFIRM=1` deletes only the path-search-labelled resources + the 60Gi PVC. PG/Go/torrent_files untouched. Index is DHT-regenerable (re-backfill).

---

## 6. Status

Code complete on `feat/l3-pathsearch`, `cargo build`/`clippy` green, 87 unit tests pass, committed (conventional-commits, **not pushed**). Homelab role delta + V4 runbook described above; **nothing applied, no image built, no prod touch.**
