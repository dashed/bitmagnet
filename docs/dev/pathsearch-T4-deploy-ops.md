# PS-T4 — Path-FTS Typeahead: Deploy / Integration / Ops Plan

**Status:** 📋 PLAN ONLY — **nothing applied, nothing committed, no prod change.** Read-only investigation per the homelab server-safety rules.
**Date:** 2026-06-09
**Author:** `ps-t4-deploy` (team `bitmagnet-bench`, task #75)
**Mode:** keep-everything (no PG schema change, no table drop, no Go-image change required for the deploy itself)
**Companion docs:**
[`cjk-tokenizer-and-incremental-merge-bench-RESULTS.md`](./cjk-tokenizer-and-incremental-merge-bench-RESULTS.md) (EXP-D/E/D2 — the measured 94 GB / 97 min / ~2 ms-freshness numbers) ·
[`file-grained-search-team-review.md`](./file-grained-search-team-review.md) (§6 file-index deploy delta, D0 parent-mount) ·
[`duckdb-parquet-parity-architecture.md`](./duckdb-parquet-parity-architecture.md) (the structured-search engine this does **not** replace) ·
homelab [`docs/bitmagnet-tantivy-phase3-deploy-plan.md`](../../../homelab-infra/docs/bitmagnet-tantivy-phase3-deploy-plan.md) (the drafted role this diffs against).

> ⚠️ **This document is the HOW, gated on a WHETHER it does not decide.** Whether the +94 GB path-FTS index should be built at all is **PS-T5's** adversarial call. The benchmark suite (EXP-D2 + `file-grained-search-benchmark-results.md`) already concluded that **structured** per-file search is cheap on DuckDB-on-Parquet (+3.9 GB, <250 ms) and the ngram index earns its keep **only** for a hard, product-confirmed need for interactive **broad free-text path search (especially CJK, 15.2 % of the corpus) at <50 ms with realtime freshness.** This plan exists so that *if PS-T5 returns GO*, the deploy is a concrete, costed, reversible delta rather than a research project.

---

## 0. TL;DR

- **The live decision (answered):** the Tantivy sidecar is **resurrected SOLELY as the per-file path-FTS typeahead engine** — a *third*, narrow engine. It does **not** serve the torrent-grained Phase-3 index (PG-FTS still owns main search) and does **not** serve structured per-file search (DuckDB-on-Parquet owns that, per ARCH-A..F). One process, **one index**: a char-ngram path index, one doc per file.
- **Reuse the drafted `bitmagnet-search` role wholesale** — its shape (HEL1-pinned Deployment + ClusterIP gRPC Service + node-bound local-path PVC + CiliumNetworkPolicy + single-writer backfill discipline + FSN1→GHCR image build) is exactly right. PS-T4 is a **parameter + lifecycle delta on that role**, not a new role.
- **Five load-bearing changes** vs the drafted batch Phase-3 plan:
  1. **Index workload:** torrent-grained ≤74 GB read-only → **file-grained ngram path index, 94 GB measured** (EXP-D2: 879.5 M docs, 100.8 GB raw / 94 GB merged).
  2. **Writer lifecycle:** scaled-**0** batch (writer only during backfill) → **scaled-1 always-on writer** consuming an incremental stream (EXP-E: ~2 ms freshness, bounded segments). The serving pod is the **permanent sole writer**.
  3. **PVC:** 200 Gi → **300 Gi** (94 GB index + force-merge/large-merge transient headroom; local-path is **not** expandable).
  4. **Backfill engine:** torrent_contents rows → **blob-sourced file docs**, **single-thread writer + ≥2 GB arena** (ngram multi-thread writer **crashes** — EXP-D), ~**97 min** @150 k docs/s.
  5. **gRPC surface:** a **new file-grained typeahead RPC** (a `FileSearchService`/`PathTypeahead`), distinct from the existing torrent `SearchService` — a fork code addition.
- **Keep-everything holds:** the deploy itself touches neither PG schema nor the Go image. The path-tail incremental writer reads PG read-only; teardown reverts to nothing. Wiring the web UI to call it is a *separate, flag-gated, default-off* Go integration step (described in §8, not part of the deploy).

---

## 1. Scope & non-goals

| In scope (this doc) | Out of scope |
|---|---|
| HEL1 topology for a path-FTS-only sidecar | Whether to build the index at all (→ **PS-T5**) |
| 300 Gi PVC sizing + mem for the 94 GB ngram index | The index schema/tokenizer internals (→ **PS-T3**) |
| Writer-scaled-1 lifecycle + single-writer locking | UX/requirements (→ **PS-T1/T2**) |
| Initial backfill Job + always-on incremental writer | Dropping `torrent_files` (deferred indefinitely, see MEMORY sequencing constraint) |
| gRPC typeahead API shape + GraphQL/UI wiring shape | The DuckDB-on-Parquet structured sidecar (separate, #27/#29) |
| Authentik/Traefik posture; FSN1→GHCR image build | Committing any homelab file / building any image |
| Explicit DIFF vs the drafted `bitmagnet-search` role | Phase-6 PG-FTS cutover |

**Coexistence map (what serves what after a GO):**

| Engine | Serves | Where | Status |
|---|---|---|---|
| PostgreSQL FTS (in the Go app) | main torrent search, torznab | FSN1 PG (deployed) | unchanged, authoritative |
| DuckDB-on-Parquet sidecar | structured per-file search (ext∧size, collapse, analytics) | HEL1 (planned #27) | the cheap parity tier |
| **Tantivy path-FTS sidecar (THIS doc)** | **broad free-text path typeahead (incl. CJK)** | **HEL1** | **gated on PS-T5 GO** |

The three are additive and independent. This sidecar is the *only* one that delivers <50 ms broad free-text + ms freshness; it is also the most expensive (+94 GB).

---

## 2. Topology

```
            HEL1 (alberto-hetzner-hel1 — see §11 MUST-VERIFY; idle i9-12900K / 125 GB RAM / 1.8 TB)
  ┌──────────────────────────────────────────────────────────────────────┐
  │  Deployment bitmagnet-pathsearch (replicas:1, Recreate)                │
  │    ┌─ container: pathsearch-server  ─ serves ─▶ gRPC FileSearchService │
  │    │                                  (ClusterIP bitmagnet-pathsearch:50051)
  │    │     holds the SOLE Tantivy IndexWriter (single-writer invariant)  │
  │    └─ follow loop: PG-tail (created_at watermark) ─ upsert file docs ──┐│
  │                          │                                            ││
  │   mmap ─▶ /var/lib/bitmagnet/search-files  (94 GB ngram path index) ◀─┘│
  │                          ▲ SAME 300Gi local-path PVC (single writer!)  │
  │  Job bitmagnet-pathsearch-backfill  ── writes (INITIAL build only) ────┘│
  │    runs ONLY with the Deployment scaled to 0                            │
  └──────────────────────────────┬─────────────────────────────────────────┘
                                  │ read-only keyset SELECT (cross-node ~25 ms RTT)
                                  ▼
                       FSN1: bitmagnet-postgres:5432
                       (torrents + files_data blobs → one doc per file)
```

**Why HEL1 (unchanged from the drafted plan, re-confirmed for this workload):** FSN1 is ~83 % memory-limit-committed (bitmagnet 8C/32Gi + PG 12C/24Gi) and already carries the live crawl + PG disk. A 94 GB mmap'd index + an always-on writer belongs on the **idle** HEL1 (1.8 TB free). local-path PVC is node-bound regardless, so isolating it on HEL1 is free and avoids repeating the K=32 saturation→PG-crash. Cross-node PG read for the backfill/tail is latency-tolerant (read-only `AccessShareLock`, per-page `fetch_all`, no held snapshot).

**Why ClusterIP gRPC (unchanged):** internal-only. The typeahead is never user-facing directly — it reaches users through the existing bitmagnet web UI (already behind Authentik/Traefik on FSN1). See §9.

**Naming:** I propose a **distinct workload name `bitmagnet-pathsearch`** (not reusing `bitmagnet-search`) so this can coexist with — and not be confused for — the drafted torrent-grained sidecar, and so a future operator never points the torrent-index backfill at the path index or vice-versa. If the team prefers to *retire* the torrent-grained Phase-3 plan entirely (likely, since DuckDB replaced its purpose), then renaming the existing role's resources to `…-pathsearch` and repurposing it is equivalent — call that out in PS-T0. Either way the resource *shape* is identical.

---

## 3. The heart: writer-scaled-1 vs the batch Phase-3 plan

This is the single biggest reconciliation. The drafted role's whole lifecycle is built around **scaled-0 batch**: the backfill is the *only* writer, the serving pod is read-only, and `tasks/backfill.yml` scales the Deployment to 0 → runs the Job → scales back to 1. That is correct for a **static** index that is rebuilt occasionally.

A **typeahead** index must stay fresh as the crawler discovers torrents. EXP-E measured that an always-on incremental writer (`LogMergePolicy`, not force-merge) gives **~2 ms freshness lag, bounded segment count (17–29), ~1 GB peak RSS**, and **11 ms torrent-granular supersession** via `delete_term(info_hash)` + re-add. So the serving pod must **also be the live writer**.

### 3.1 Lifecycle states

| State | replicas | Writer held by | When |
|---|---|---|---|
| **Initial build** | **0** | the backfill Job (sole writer) | once, at first bring-up (~97 min) |
| **Steady state** | **1** | the serving pod (sole writer + reader + follow loop) | normal operation — **permanent** |
| **Full rebuild** | 0 → Job → 1 | the backfill Job, then the serving pod | rare (schema change, corruption, tokenizer change) |

The scale-0 dance is **retained but demoted**: it runs at initial build and full-rebuilds only, not on every refresh. Steady-state freshness comes from the in-pod follow loop, not from re-running the Job.

### 3.2 The follow loop — two stream sources (be precise about freshness)

The mandate phrased this as "consuming the crawler dual-write stream." There are two concrete sources, with **different freshness floors and different keep-everything cost**:

| Source | How | Freshness | Go change? |
|---|---|---|---|
| **(b) PG-tail** *(recommended primary)* | the sidecar polls PG on a watermark (`torrents.created_at`/`updated_at` keyset), decodes new/changed blobs, upserts file docs, `commit()` + `reader.reload()` | **= poll interval (seconds)** | **none — true keep-everything** |
| **(a) gRPC push** *(Phase-4 upgrade)* | the Go app dual-writes each persisted torrent to the sidecar via `BatchIndex`/`IndexDocument` (already in the proto) | **~2 ms** (EXP-E) | **yes** — the shadow-mode Go image |

**Critical honesty for the lead:** "ms freshness needs writer scaled-1" is *necessary but not sufficient*. Scaled-1 is required for **either** source. **Millisecond** freshness specifically requires source (a) gRPC push, which breaks keep-everything (Go image change). Source (b) PG-tail keeps Go untouched and gives **seconds** freshness.

**For per-keystroke <50 ms typeahead, the hard requirement is QUERY latency, not the freshness of brand-new torrents.** A torrent crawled 10 s ago not yet being typeahead-able is invisible to the user; a 300 ms keystroke is not. So **(b) PG-tail at a 10–30 s poll is the right keep-everything default**; (a) gRPC push is a later upgrade to buy ms freshness **only if a product reason ever demands it** (it almost certainly won't for typeahead). I recommend deploying with (b) and treating (a) as out-of-scope.

> 🚩 **Fork-code prerequisite:** the follow loop (a `--follow` / watermark-poll mode on the file indexer) **does not exist yet** in `bitmagnet-rs`. The proto has the *push* RPCs (`IndexDocument`/`BatchIndex`/`DeleteDocument`) but no PG-tail follow mode. This is a required code addition (analogous to task #41 "Job B continuous"), distinct from the one-shot backfill bin. Flagged, not made.

---

## 4. Single-writer invariant & locking

Tantivy permits **exactly one `IndexWriter` per directory** (it takes a `.tantivy-writer.lock`; a second writer errors immediately). With the serving pod as the permanent writer, the invariant becomes:

1. **Recreate strategy** on the Deployment (already in the drafted `deployment.yaml.j2`) — a RollingUpdate surge pod would try to open a second writer and crash-loop. Recreate tears the old pod down first. **Keep it.**
2. **`replicas: 1` + RWO PVC** — K8s + the access mode prevent a second scheduled pod.
3. **Backfill ⟂ serving** — the initial-build / full-rebuild Job is a second writer, so `tasks/backfill.yml`'s scale-0 → wait-for-pod-delete → Job → scale-1 sequence is **still mandatory for those operations**. The existing C1 fix (poll for terminal Complete/Failed, never `kubectl wait --for=condition=complete`) and the C2 `block:/always:` scale-back-up both carry over unchanged and are *more* important now (leaving replicas:0 means no live writer and a stale index).
4. **Backstop:** Tantivy's own directory lock is the last line of defence — even if orchestration races, the second writer fails loudly rather than corrupting the index. Document this; do not rely on it as the primary control.
5. **Follow-loop ⟂ backfill:** because the follow loop lives *inside* the serving pod, scaling the serving pod to 0 (for a rebuild) also stops the follow loop — there is never a follow-writer and a backfill-writer at once. This is the clean property that makes the design safe.

---

## 5. PVC & memory sizing

### 5.1 PVC — 200 Gi → **300 Gi**

- EXP-D2 **measured** the full-corpus ngram path index at **100.8 GB raw → 94 GB** after force-merge to 1 segment (879.5 M docs, path-field 101.6 B/doc, ~89.3 GB postings-dominated).
- The drafted 200 Gi was sized for a **≤74 GB torrent index** (2.7× headroom). For a 94 GB index, 200 Gi is only 2.1× — and a **force-merge transiently needs ~1× the segment set on top** (≈94 GB) → ~188 GB peak, leaving ~12 GB. **Too tight** to be safe, especially since `LogMergePolicy` background merges can also transiently double a large segment.
- **local-path has `ALLOWVOLUMEEXPANSION=false`** — the size is permanent; under-sizing means a destructive re-create. HEL1 has 1.8 TB, so headroom is free.
- **Recommendation: `bitmagnet_pathsearch_index_storage: 300Gi`** (94 GB steady + ~190 GB merge/rebuild headroom). If the team commits to **never force-merging** (serve the bounded multi-segment `LogMergePolicy` index — EXP-E shows path-term lookups stay sub-ms over 17–29 segments), 200 Gi could hold, but 300 Gi removes the foot-gun for a one-time cost of disk HEL1 has in abundance.

### 5.2 Memory

- EXP-E peak RSS at 20 M base = **1006 MB**; the full-corpus writer arena is **2 GB** (single-thread requirement, §6), plus the OS page cache for the 94 GB mmap (reclaimable but wants room — ASCII cold reads touch disk per EXP-D2).
- Drafted serving limit was 6 Gi. The file-grained review (§6) already bumped to 8 Gi for a *dual*-index pod. For a single 94 GB index + 2 GB writer arena + query working set, recommend **requests 2 Gi / limits 10 Gi**. HEL1 is idle so this is comfortable and avoids working-set OOM on a large merge.

### 5.3 Mount point (D0)

Adopt the file-grained review's **D0 fix now**: mount the PVC at the **parent `/var/lib/bitmagnet`**, with the index in a **`search-files/` subdir**, not directly at the index dir. This is forward-compatible (a future second index dir, e.g. a torrent index, lands on the PVC not ephemeral fs) and costs nothing. `BITMAGNET_PATHSEARCH_INDEX=/var/lib/bitmagnet/search-files`.

---

## 6. Backfill (initial build)

| Knob | Drafted (torrent) | **Path-FTS (this)** | Source |
|---|---|---|---|
| Doc unit | 1 / torrent_content (46.0 M) | **1 / file (≈879.5 M)** | EXP-D2 |
| Data source | `torrent_contents` rows | **`torrents` + `files_data` blob** (one blob → N file docs) | review §6 |
| Extension | (n/a) | **G1: `FileExtensionFromPath`, never blob `e`** (empty for crawl-path torrents) | review G1 |
| Cursor | `tc.id` | **`torrents` keyset (info_hash / created_at)**; commit cadence counts **file** docs | review §6 |
| Writer threads | (default) | **single-thread** (`--writer-threads 1`) — ngram multi-thread writer **crashes** ("index writer killed", arena starvation) | EXP-D |
| Writer arena | (default) | **≥2 GB** (`--writer-heap-mb 2000`) | EXP-D |
| Runtime | 1.5–6.5 h | **~97 min @150 k docs/s** | EXP-D2 |
| Final merge | force-merge to 1 seg | **prefer incremental merge** (skip the final force-merge so the steady-state writer keeps merging; saves the 94 GB transient & a long single-threaded merge) | EXP-E |
| Backfill bin | `bitmagnet-backfill` | **`backfill_files`** (3rd image COPY) | review §6 |

- **Hard smoke-gate (carry over + tighten):** run `--limit 100000` first, extrapolate index size & docs/s, and only proceed if the projection lands at the EXP-D2 ~94 GB (PS-T5's GO/NO-GO ceiling, not the PVC's). The drafted `LIMIT=` smoke flow already supports this.
- **PG safety unchanged:** read-only, single-digit connections, per-page fetch (no held snapshot → autovacuum unblocked), gentler than the Phase-1 K=16 migration.
- **Gap-closing is now automatic:** unlike the batch plan (where rows crawled *during* backfill were stranded by random `tc.id`), the steady-state follow loop (§3.2) sweeps up everything after the backfill cursor on its first pass. The initial backfill no longer needs a perfect 100 %.

---

## 7. gRPC typeahead API shape

The existing `bitmagnet.v1.SearchService` (proto §`search.proto`) is **torrent-grained** (`TorrentDocument`, one doc/torrent). Path typeahead needs a **file-grained** surface — a fork code addition, distinct from the torrent service. Proposed contract (PS-T3 owns the exact schema; this is the deploy/wire contract):

```proto
// NEW — file-grained path typeahead. Lives alongside SearchService on :50051.
service FileSearchService {
  // Per-keystroke path typeahead. Must answer <50 ms at 879.5M docs.
  rpc PathTypeahead(PathTypeaheadRequest) returns (PathTypeaheadResponse);
  // Steady-state writer ingress (push source (a); optional for keep-everything).
  rpc IndexFiles(stream FileDocument) returns (BatchIndexResponse);
  rpc DeleteTorrentFiles(DeleteDocumentRequest) returns (DeleteDocumentResponse); // delete_term(info_hash)
  rpc HealthCheck(HealthCheckRequest) returns (FileHealthCheckResponse);          // doc_count = file docs
}

message PathTypeaheadRequest {
  string query = 1;                 // path substring (ngram-tokenized; CJK-correct)
  repeated string file_extensions = 2;   // optional ext filter (FAST facet)
  optional uint64 size_min = 3;     // optional (FAST range)
  optional uint64 size_max = 4;
  Pagination pagination = 5;        // typeahead: small limit (e.g. 10–20)
}
message FileDocument {              // one per file
  bytes info_hash = 1; uint32 file_index = 2; string path = 3;
  string extension = 4;             // G1-derived from path
  uint64 size = 5; int64 published_at = 6; ContentType content_type = 7;
}
message PathTypeaheadResponse { repeated FileHit hits = 1; uint64 total_hits = 2; }
message FileHit { FileDocument document = 1; float score = 2; }
```

**Probes stay `tcpSocket:50051`** — the sidecar has no `grpc.health.v1`/reflection, only the custom `HealthCheck` RPC (same constraint as the drafted role). A `tonic-health` add (~10 lines) would later enable native `readinessProbe.grpc:`; optional.

---

## 8. bitmagnet GraphQL / web-UI wiring (integration phase — NOT the deploy)

Keep-everything deploy = sidecar + backfill, **zero user-facing change**. Surfacing typeahead in the UI is a *separate, flag-gated, default-off* step (mirrors the DuckDB sidecar's `fileSearch` resolver, #28):

1. **GraphQL:** add a `pathTypeahead(query, ext?, sizeMin?, sizeMax?, limit)` query → a Go resolver that dials `bitmagnet-pathsearch.bitmagnet.svc:50051` `FileSearchService/PathTypeahead`, hydrates display fields, returns file rows.
2. **Gate:** `SEARCH_PATH_TYPEAHEAD_ENABLED=false` by default → the web UI search box behaves exactly as today until flipped. Engine stays `postgres`; this is a *parallel* surface, not a cutover.
3. **NetPol toggle:** flip `bitmagnet_pathsearch_allow_bitmagnet_ingress: true` (the drafted role already has the equivalent `allow_bitmagnet_ingress` ingress rule) so the Go pod may reach :50051. Off until wiring lands.
4. **Web UI:** a debounced typeahead control posting the GraphQL query. Frontend work, out of infra scope.

Because the UI is already authenticated, **no new auth surface** is introduced.

---

## 9. Authentik / Traefik exposure

**None.** The sidecar is **ClusterIP, internal-only** — no `IngressRoute`, no Authentik middleware, no Traefik route, no DNS record. The drafted `service.yaml.j2` is already ClusterIP-only; keep it verbatim. Users reach typeahead only via the existing bitmagnet web UI (FSN1), which is already behind Authentik forward-auth + Traefik. Exposing :50051 externally would be a security regression and is explicitly rejected. (Contrast: the Hermes dashboard *is* Authentik-exposed; this sidecar is not user-facing and must stay internal.)

---

## 10. Image build (FSN1 → GHCR)

Reuse `playbooks/bitmagnet_search_image_build.yml` (`make bitmagnet-search-image-build REF=<branch> TAG=<tag>`) verbatim — it builds `Dockerfile.search` with context `bitmagnet-rs`, native amd64 on FSN1, pushes to the public GHCR package, prints the digest to pin. Deltas:

- **REF** = the branch carrying the path-FTS engine + follow mode (`feat/file-grained-search` per MEMORY, on top of `feat/tantivy-search-sidecar`). Confirm the path index + `PathTypeahead` + `backfill_files` + follow loop are all on it.
- **`Dockerfile.search` COPY additions (fork commit, gated):**
  ```diff
   COPY --from=builder /build/target/release/bitmagnet-search /usr/local/bin/bitmagnet-search
  +COPY --from=builder /build/target/release/backfill_files   /usr/local/bin/bitmagnet-backfill-files
  ```
  (and ensure the server binary builds the path index + follow mode behind `BITMAGNET_PATHSEARCH_*` env.) Both bins come from the existing `cargo build --release -p bitmagnet-search`.
- **GHCR package:** if a new `ghcr.io/dashed/bitmagnet-pathsearch` package is used, set it **public once** in the GitHub UI (mirrors the Go fork; avoids an imagePullSecret). Reuses vaulted `vault_ghcr_token`.
- **M1 carry-over:** `bitmagnet-rs/rust-toolchain.toml` `channel = "stable"` overrides the Dockerfile's `rust:1.95` pin (rustup fetches latest stable at build). Consider pinning the channel; non-blocking.

---

## 11. Explicit DIFF vs the drafted `bitmagnet-search` role

The drafted role (`ansible/roles/bitmagnet-search/`, uncommitted) is the baseline. **Keep it for the torrent index if that's ever revived; clone/parameterize it for path-FTS.** Concrete deltas:

| File | Drafted (torrent Phase-3) | **PS-T4 path-FTS delta** |
|---|---|---|
| `defaults/main.yml` | torrent index, 200Gi, 6Gi mem, scaled-0 batch | **add** `*_file_index`/path knobs; **300Gi**; **10Gi limit**; ngram backfill knobs `--writer-threads 1 --writer-heap-mb 2000`; **follow-mode gate** `bitmagnet_pathsearch_follow_enabled: true` + poll interval; index path `…/search-files`; doc target ≈879.5 M |
| `templates/pvc.yaml.j2` | 200Gi | **300Gi**, mount **parent** `/var/lib/bitmagnet` (D0) |
| `templates/deployment.yaml.j2` | Recreate, tcpSocket, read-only serve | **+ follow-loop env** (`BITMAGNET_PATHSEARCH_FOLLOW=true`, PG `BITMAGNET_POSTGRES_*`, watermark interval); mem 10Gi; index path subdir. Recreate/probes **unchanged** |
| `templates/backfill-job.yaml.j2` | `bitmagnet-backfill`, torrent_contents, multi-thread default | **`bitmagnet-backfill-files`**, blob source, **single-thread + 2 GB arena**, file-doc commit cadence, `--no-final-merge` |
| `templates/cilium-network-policy.yaml.j2` | serving egress→PG noted "unused in P3" | **serving egress→PG now REQUIRED** (follow loop tails PG); set `allow_bitmagnet_ingress` only when UI wiring lands |
| `tasks/main.yml` | PVC→NetPol→Svc→Deploy | **unchanged shape**; update digest guard + labels to `…-pathsearch` |
| `tasks/backfill.yml` | scale-0 → Job → scale-1 | **unchanged & still mandatory** for initial build / full rebuild; C1/C2/C3 fixes carry over (more critical now — replicas:0 = no live writer) |
| `group_vars/.../bitmagnet_search.yml` | torrent pin, 200Gi | path-FTS image pin, 300Gi, follow knobs |
| `playbooks/*` | install / backfill / image-build | **reusable as-is** (rename targets if cloning) |
| `Makefile` targets | `bitmagnet-search{,-check,-status,-logs,-image-build,-backfill-run,-backfill-status,-reset}` | clone as `bitmagnet-pathsearch-*`; **add `…-rebuild`** (full-rebuild dance = backfill-run with a fresh index); `-reset` confirm text → "300Gi path index" |

> 🚨 **MUST-VERIFY before any deploy — node hostname.** The drafted role pins `bitmagnet_search_node_hostname: alberto-hetzner`, but the inventory's HEL1 host is **`alberto-hetzner-hel1`** (`hosts.yml`) and most roles pin FSN1 as `alberto-hetzner-fsn1`. `gatus` uses bare `alberto-hetzner`. The actual K8s **`kubernetes.io/hostname` label** must be confirmed (`kubectl get nodes --show-labels`) — a wrong `nodeSelector` strands the node-bound local-path PVC and the pod never schedules. **Verify the real HEL1 node label and set it explicitly.** (Read-only check; not done here.)

---

## 12. Ops / runbook (path-FTS)

**First bring-up (gated, after PS-T5 GO + fork prereqs):**
1. `make bitmagnet-pathsearch-image-build REF=feat/file-grained-search TAG=pathsearch` → pin digest, set GHCR public.
2. `make bitmagnet-pathsearch-check` (diff), then `make bitmagnet-pathsearch` (PVC + NetPol + Svc + Deployment, empty index, follow loop idle on empty).
3. `make bitmagnet-pathsearch-backfill-run LIMIT=100000` → **smoke gate**: read docs/s + extrapolated index size; abort if it won't land near 94 GB.
4. `make bitmagnet-pathsearch-backfill-run` → full ~97 min single-thread backfill (Deployment auto-scaled 0→Job→1).
5. Verify `HealthCheck.doc_count ≈ 879.5 M`, index `du -sh ≈ 94 GB ≤ 300 Gi`, a `PathTypeahead` probe returns plausible hits, **web UI latency unchanged** (Go untouched).
6. Steady state: serving pod stays scaled-1, follow loop keeps the index fresh (seconds). Done.

**Monitoring (follow-up, mirrors review §6):** add a `bitmagnet_pathsearch_file_doc_count` gauge; derive freshness lag from `doc_count` vs PG file estimate; alert on follow-loop watermark stall and on PVC >80 %. No `index_size_bytes` metric exists upstream — derive from a disk check or add the gauge.

**Rollback:** `make bitmagnet-pathsearch-reset CONFIRM=1` deletes only the path-search-labeled resources + the 300 Gi PVC. PG/Go/torrent_files untouched → full revert. The index is DHT-regenerable (a re-backfill).

---

## 13. Risks & open questions

1. **(→ PS-T5) Does the index get built at all?** EXP-D2 says the +94 GB earns its keep *only* for hard interactive broad free-text (esp. CJK). Structured search is already cheap on DuckDB (+3.9 GB). This deploy is wasted effort if PS-T5 returns NO-GO.
2. **Follow mode is unimplemented** (§3.2 🚩). A `--follow`/watermark-poll mode on the file indexer is a required fork code addition; without it the index goes stale and you're back to periodic full rebuilds (acceptable fallback, but not "fresh typeahead").
3. **Single-thread backfill is a throughput floor** — ngram multi-thread writer crashes (EXP-D), so ~97 min is not parallelizable on the writer side. Fine for a one-time build; a concern only if frequent full rebuilds are needed (so prefer incremental merge, §6).
4. **`save_files_threshold`** (review M1) — confirm the deployed crawler value; very large filesets inflate per-torrent doc counts and the backfill/merge cost (heavy skew: p99 743 files/torrent, max 88,561).
5. **CJK correctness depends on the ngram tokenizer** (PS-T3) — the default tokenizer silently returns ~nothing on mid-run CJK (recall 0.0037); the deploy is only as correct as the schema PS-T3 specifies.
6. **300 Gi vs force-merge** — if the team insists on a force-merged single segment (lowest query latency), 300 Gi is the safe floor; if incremental-merge serving is accepted, the PVC could be smaller but 300 Gi removes the non-expandable foot-gun.
7. **Node label** (§11 MUST-VERIFY) — the single most likely first-deploy failure.

---

## 14. Status

Plan complete. **Nothing applied, nothing committed, no image built.** All homelab deltas are described against the existing drafted `bitmagnet-search` role; none are written to disk by this task. Awaiting PS-T5's GO/NO-GO and PS-T0 synthesis.
