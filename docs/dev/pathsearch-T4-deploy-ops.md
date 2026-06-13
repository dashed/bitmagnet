# PS-T4 - Pathsearch L3: Deploy / Integration / Ops Plan

**Status:** PLAN + fork-side implementation started. The Rust proto/schema,
path-bag server, backfill binary, and follow-loop scaffold exist in the fork;
no image built and no prod change.
**Date:** 2026-06-12
**Mode:** keep-everything. `torrent_files` remains the live
fallback/source-of-truth until every replacement layer is deployed and proven.

This document supersedes the older one-doc-per-file path-FTS deploy sketch. The
active L3 target is the measured **per-torrent path-bag** Tantivy index:

* char-ngram(2,3) path bag, `WithFreqs`, no positions
* one document per torrent, not one document per file
* indexed 20-byte `info_hash` delete key for torrent-granular supersession
* production deploy size **14.0 GiB** keyed
* purpose: fast free-text path candidates and fast `collapse:path` candidate
  sets; exact file/path semantics stay in L1 blobs and L2 DuckDB

Companion docs:
[`torrent-files-replacement-options.md`](./torrent-files-replacement-options.md)
(current layer map and hard rule) -
[`psx-campaign-RESULTS.md`](./psx-campaign-RESULTS.md) (13.32 GiB keyless
`WithFreqs` build, recall, broad-query latency) -
[`cb-campaign-RESULTS.md`](./cb-campaign-RESULTS.md) (14.0 GiB keyed build,
concurrency, live writer, supersession) -
[`pathsearch-master-investigation.md`](./pathsearch-master-investigation.md)
(historical decision trail).

---

## 0. TL;DR

* **Deploy target:** `bitmagnet-pathsearch`, a narrow Tantivy sidecar on HEL1
  serving the per-torrent path-bag ngram index. It is not the main torrent
  search engine, and it does not replace L2 structured per-file search.
* **Why this exists:** broad `path ILIKE '%...%'` and `collapse:path` are the
  pathological L2 classes. L3 turns path text into a small candidate
  `info_hash` set; L1/L2 then verify the real substring and structured filters.
* **Measured artifact:** keyed production index is **15,017,420,811 B =
  14.0 GiB**, 16,973,470 docs, one segment. The keyless path-only form was
  13.32 GiB; production needs the keyed form for `delete_term(info_hash)`.
* **Latency shape:** realistic multi-word queries are <50 ms p95
  single-client; broad single-gram TopDocs p95 is about 77-94 ms and is solved
  with UX guards (min chars/debounce/submit fallback), not more engine tuning.
* **Deploy shape:** reuse the drafted `bitmagnet-search` role pattern:
  HEL1-pinned Deployment, ClusterIP gRPC, node-bound local-path PVC,
  CiliumNetworkPolicy, single-writer backfill discipline, FSN1 -> GHCR build.
  Use a distinct workload name, `bitmagnet-pathsearch`.
* **Fork status:** the production pathsearch server, backfill binary, candidate
  RPC, and PG-tail follow-loop scaffold are implemented locally. Backend
  exact-refine routing, homelab manifests, image build, and prod proof remain.

---

## 1. Scope

| In scope | Out of scope |
|---|---|
| HEL1 topology for the L3 path-bag sidecar | Dropping `torrent_files` |
| PVC/memory sizing for the 14.0 GiB keyed index | Replacing PostgreSQL main search |
| initial backfill + full-rebuild lifecycle | Replacing L2 structured file search |
| steady-state one-writer follow loop | exact file-result semantics inside L3 |
| candidate RPC shape for GraphQL/backend routing | public Traefik/Auth exposure |
| deploy proof gates before any DROP discussion | new product UX beyond a flag-gated integration |

The old per-file ngram plan is explicitly retired for this track. Per-file ngram
was the footprint-heavy option; the per-torrent path-bag index is the measured
GO path.

---

## 2. Coexistence Map

| Engine | Serves | Where | Status |
|---|---|---|---|
| PostgreSQL FTS | main torrent search, torznab | FSN1 PG/app | deployed, unchanged |
| L1 blobs | torrent file browser and exact file hydration | PG blob rows | deployed, verified |
| L2 DuckDB-on-Parquet | structured per-file find/collapse/count/facet | HEL1 | deployed, proven |
| L3 pathsearch | free-text path candidates, fast `collapse:path` candidates | HEL1 | GO, deploy pending |

L3 is a candidate engine. It can say "these torrents probably contain paths
matching this text quickly"; L1/L2 still say "these exact files matched after
substring verification and structured filtering."

---

## 3. Topology

```text
             HEL1 (alberto-hetzner-hel1; verify node label before deploy)
  -------------------------------------------------------------------------
  Deployment bitmagnet-pathsearch (replicas: 1, strategy: Recreate)
    container: pathsearch-server
      gRPC :50053, ClusterIP service bitmagnet-pathsearch
      holds the sole Tantivy IndexWriter in steady state
      follows PG by watermark and supersedes torrent docs

    mmap /var/lib/bitmagnet/pathsearch
      keyed per-torrent path-bag index, ~14.0 GiB steady state

  Job bitmagnet-pathsearch-backfill
    initial build / full rebuild only
    runs only while Deployment is scaled to 0

             read-only PG keyset/blob reads across the cluster
                       |
                       v
             FSN1 bitmagnet PostgreSQL
```

**Why HEL1:** FSN1 already carries the live crawler, app, and PostgreSQL load.
HEL1 has enough CPU/RAM/disk for a 14 GiB mmap index plus backfill/rebuild
headroom, and local-path PVCs are node-bound anyway.

**Why ClusterIP:** no direct user exposure. Users reach pathsearch only through
the existing bitmagnet app once a flag-gated resolver is added.

**Naming:** use `bitmagnet-pathsearch`. Do not reuse `bitmagnet-search` for this
workload; the distinct name prevents confusion with the older torrent-grained
Phase-3 sidecar.

---

## 4. Index Contract

| Property | Production L3 value |
|---|---|
| granularity | one document per torrent |
| doc count | about 16.97 M torrent docs at the measured corpus |
| path field | concatenated/path-bag text from decoded file paths |
| tokenizer | char-ngram(2,3) |
| postings | `WithFreqs`, no positions |
| delete key | indexed 20-byte `info_hash`, required |
| primary mutation | `delete_term(info_hash)` then add replacement path-bag doc |
| steady size | 14.0 GiB keyed |
| exactness role | recall-first candidate set; exact file/path verification is L1/L2 |

Do not add per-file extension/size semantics to L3. Extension and size filters
belong to L2 exact-refine. Storing enough metadata for ordering or display is
fine, but the L3 doc should stay torrent-grained.

Recommended document fields:

| Field | Indexed | Stored | Fast | Purpose |
|---|---:|---:|---:|---|
| `path_grams` | yes | no | no | ngram query text |
| `info_hash` | yes | yes | no | identity + delete key |
| `seeders` or sort proxy | no | no | yes | TopDocs ordering |
| `published_at` / `created_at` | no | optional | yes | stable secondary sort/debug |
| `files_count` / total size | no | optional | yes | cheap display/filter hints only |

---

## 5. Writer Lifecycle

Tantivy permits exactly one `IndexWriter` per directory. Keep the invariant
simple:

| State | Deployment replicas | Writer holder | When |
|---|---:|---|---|
| initial build | 0 | backfill Job | first bring-up |
| steady state | 1 | serving pod follow loop | normal operation |
| full rebuild | 0 -> Job -> 1 | backfill Job, then serving pod | schema/tokenizer/corruption |

Required controls:

* Deployment strategy `Recreate`; no rolling surge writer.
* RWO local-path PVC and `replicas: 1`.
* Backfill playbook scales Deployment to 0, waits for the pod to exit, runs the
  Job, then scales the Deployment back to 1 in `always:` cleanup.
* The follow loop lives in the serving pod, so scale-to-0 also stops the live
  writer before any rebuild writer starts.

---

## 6. Follow Loop

The production follow mode is still a fork-code prerequisite. The recommended
first implementation is PG-tail/watermark polling:

| Source | How | Freshness | Go app change |
|---|---|---|---|
| PG-tail (deploy default) | poll changed torrents by watermark, decode blob, `delete_term(info_hash)` + add path-bag doc, commit, reload | seconds, controlled by poll interval | no |
| gRPC push (later) | app dual-writes each persisted torrent to L3 | low milliseconds | yes |

Default to PG-tail. Typeahead query latency is the user-visible requirement; a
new torrent appearing in pathsearch 10-30 seconds later is acceptable unless a
product requirement says otherwise.

CB proved the writer side has headroom at the measured scale:

* keyed supersession is correct under 24-reader load
* reader p95 under writes stayed about 1.0-1.04x baseline
* fresh-lag p99 stayed <=2.2 ms inside the benchmark writer loop
* 5/20/50 torrent/s write targets were achieved

The deploy implementation should still batch commits if real PG-tail traffic or
storage fsync behavior makes commit-per-torrent noisy.

---

## 7. PVC And Memory

The 300 Gi PVC from the older per-file plan is no longer the right default.

Recommended initial sizing:

| Resource | Recommendation | Why |
|---|---:|---|
| PVC | `100Gi` | 14 GiB index + old/new rebuild overlap + merge headroom + non-expandable local-path safety |
| memory request | `2Gi` | enough scheduled reservation for server + writer |
| memory limit | `8Gi` to `10Gi` | writer heap + query working set + mmap/page-cache pressure |
| writer heap | `--writer-heap-mb 2000` | matches the measured safe build shape |

Keep the PVC mounted at parent `/var/lib/bitmagnet`, with the index in
`/var/lib/bitmagnet/pathsearch`. Parent mount leaves room for future index
subdirectories without moving the PVC.

Alert at 80% PVC usage. Do not shrink below 100Gi until a live backfill, rebuild,
and at least one compaction/merge cycle have been observed.

---

## 8. Backfill

Initial backfill builds the per-torrent path-bag index from production blobs.

| Knob | Value |
|---|---|
| source | `torrents` + `files_data` blobs |
| doc unit | one doc per torrent |
| expected docs | about 16.97 M |
| expected index size | about 14.0 GiB keyed |
| tokenizer | char-ngram(2,3), `WithFreqs`, no positions |
| writer | one writer, 2 GiB heap |
| supersession key | indexed `info_hash` |
| measured build wall | about 60-90 minutes class from PSX/CB measurements |

Backfill gates:

1. Run a limited smoke first, e.g. `LIMIT=100000`.
2. Project docs/s and GiB/doc; abort if the projection is far above the 14 GiB
   keyed baseline.
3. Full run only after the smoke lands near the measured size curve.
4. After full build, verify doc count, index size, HealthCheck, and sample
   candidate queries.
5. Start the serving pod and confirm the follow watermark advances.

The backfill should read PG with keyset pagination and short transactions. It is
read-only and must not hold a long snapshot.

---

## 9. gRPC Contract

The old `FileSearchService/PathTypeahead` shape was tied to one-doc-per-file.
The current service should be torrent-candidate oriented.

Draft shape:

```proto
service PathSearchService {
  rpc PathCandidates(PathCandidatesRequest) returns (PathCandidatesResponse);
  rpc HealthCheck(HealthCheckRequest) returns (PathSearchHealth);
}

message PathCandidatesRequest {
  string query = 1;                 // path substring / free text
  uint32 limit = 2;                 // final requested candidates
  uint32 oversample = 3;            // backend exact-refine headroom
  PathSort sort = 4;                // seeders/published/score proxy
}

message PathCandidate {
  bytes info_hash = 1;
  float score = 2;
  uint64 sort_value = 3;
}

message PathCandidatesResponse {
  repeated PathCandidate candidates = 1;
  uint64 candidate_total = 2;       // torrent-doc candidate count, not exact files
  bool estimated = 3;               // exact global file counts are not L3's job
}

message PathSearchHealth {
  uint64 doc_count = 1;
  uint64 index_bytes = 2;
  string watermark = 3;
  bool writable = 4;
}
```

`candidate_total` is not an exact file count. Exact file counts and exact path
matches come from L2 or background caches after candidate narrowing.

Probes can start as `tcpSocket:50053`, matching the existing sidecar pattern.
Adding standard `grpc.health.v1` later would allow native Kubernetes gRPC
readiness probes.

---

## 10. Backend / GraphQL Integration

The deploy itself is internal-only and user-invisible. Integration is a separate
flag-gated step.

Required backend composition:

1. Dial `bitmagnet-pathsearch.bitmagnet.svc:50053`.
2. Ask L3 for oversampled `info_hash` candidates.
3. Exact-refine the real path substring and any `extension`/size filters via L1
   blob decode or L2 DuckDB.
4. Hydrate display rows from existing torrent/blob surfaces.
5. Return exact rows/groups; mark broad global counts as estimated or omit them.

For fast `collapse:path`, the request path becomes:

```text
path query -> L3 info_hash candidates -> L1/L2 exact substring refine
           -> collapse/group/hydrate response
```

Feature flags:

| Flag | Default | Purpose |
|---|---|---|
| `SEARCH_PATHSEARCH_ENABLED` | false | enable backend use of L3 |
| `SEARCH_PATH_TYPEAHEAD_ENABLED` | false | enable UI typeahead |
| `SEARCH_PATH_COLLAPSE_L3_ENABLED` | false | route `collapse:path` through L3 candidates |

The UI should use min-character and debounce guards. Broad single grams are the
known p95 tail; the fix is UX/backpressure, not a different Tantivy query plan.

---

## 11. Auth / Exposure

Do not expose the sidecar directly.

* no IngressRoute
* no DNS record
* no Traefik route
* no Authentik middleware
* ClusterIP only
* Cilium ingress only from the app namespace/pods once integration is enabled
* PG egress only for the follow loop

Users reach it through the existing bitmagnet web UI.

---

## 12. Image Build

Reuse the FSN1 -> GHCR build flow from the drafted search-sidecar role, but
publish a pathsearch-specific image/tag.

Expected fork deliverables before image build:

* ~~server binary that opens/serves the path-bag index~~
* ~~backfill binary for blob -> per-torrent path-bag index~~
* ~~`WithFreqs` no-position schema~~
* ~~indexed `info_hash` delete key~~
* ~~`PathCandidates` RPC~~
* ~~HealthCheck RPC~~
* ~~PG-tail follow mode scaffold~~
* ~~env wiring for DSN/index path/poll interval/writer heap~~
* backend exact-refine integration and production image/deploy wiring

Candidate naming:

```text
ghcr.io/dashed/bitmagnet-pathsearch:<tag>
```

Set the GHCR package visibility/pull secret path once, then pin by digest in
homelab inventory.

---

## 13. Role Delta From `bitmagnet-search`

| File/area | Drafted search role | L3 pathsearch delta |
|---|---|---|
| workload name | `bitmagnet-search` | `bitmagnet-pathsearch` |
| PVC | torrent sidecar size | `100Gi`, parent mount `/var/lib/bitmagnet` |
| deployment | Recreate, one pod | keep Recreate; add follow env and PG egress |
| container | search server | pathsearch server on `:50053` |
| backfill job | old torrent/search job | path-bag backfill, one doc/torrent, 2 GiB writer heap |
| network policy | app ingress optional | keep app ingress disabled until backend flag wiring |
| PG egress | optional/unused in old static serve | required for PG-tail follow |
| Make targets | `bitmagnet-search-*` | clone as `bitmagnet-pathsearch-*` |
| reset | old index delete | confirm text must say it deletes the pathsearch PVC/index |

MUST VERIFY before deploy: the actual HEL1 Kubernetes node label. Inventory names
have varied (`alberto-hetzner`, `alberto-hetzner-hel1`,
`alberto-hetzner-fsn1`). Run:

```sh
kubectl get nodes --show-labels
```

Set the node selector to the real `kubernetes.io/hostname` value before creating
the local-path PVC.

---

## 14. First Bring-up Runbook

Prereqs:

* fork code deliverables in section 12 exist
* image built and digest pinned
* HEL1 node label verified
* Cilium policy rendered/diffed
* backend flags remain off

Steps:

1. `make bitmagnet-pathsearch-image-build REF=<branch> TAG=<tag>`
2. Pin the digest in homelab inventory.
3. `make bitmagnet-pathsearch-check`
4. `make bitmagnet-pathsearch` to create PVC, Service, NetPol, and an empty
   Deployment.
5. `make bitmagnet-pathsearch-backfill-run LIMIT=100000`
6. Check projection: docs/s, GiB/doc, expected full size near 14 GiB keyed.
7. `make bitmagnet-pathsearch-backfill-run`
8. Verify:
   * HealthCheck serving
   * doc_count about 16.97 M
   * `du -sh` about 14 GiB and well below 100Gi
   * sample CJK and ASCII path queries return plausible candidates
   * broad single-gram latency matches the documented tail class
9. Confirm follow-loop watermark advances on fresh/updated torrents.
10. Only after the sidecar is proven, enable backend shadow/flagged routing.

No UI or DROP behavior changes during the infrastructure bring-up.

---

## 15. Proof Gates

The deployment is not "proven" until these pass in prod:

| Gate | Pass condition |
|---|---|
| readiness | pod Ready, HealthCheck serving, no restarts |
| size | index near 14 GiB keyed, PVC comfortably below alert |
| doc count | close to live torrent-with-blob count |
| freshness | follow watermark advances; superseded torrent resolves to new doc only |
| latency | realistic multi-word path queries <50 ms p95 class; broad gram tail documented |
| candidate recall | L3 candidate set contains known PG/L2 matches for sampled path queries |
| exact refine | L3 -> L1/L2 route returns exact path matches for sampled queries |
| stability | live writer does not disturb reader latency or readiness |

Known acceptable classes:

* broad single-gram p95 tail; mitigate with UX guards
* candidate totals differing from exact file totals; L3 counts torrents, not files
* moving-prod freshness drift during live shadows

Not acceptable:

* missing exact matches after L1/L2 refine
* stale superseded torrent docs after follow commit/reload
* unbounded segment growth
* app dependency on L3 while flags are off

---

## 16. Rollback

Rollback remains simple because this is additive:

1. Turn backend flags off.
2. Scale/delete `bitmagnet-pathsearch`.
3. Delete the pathsearch PVC only with an explicit confirm target.

PG, L1, L2, and `torrent_files` are untouched.

---

## 17. Risks

1. **Fork code missing:** server/backfill/follow/RPC are prerequisites, not
   deployment details.
2. **Candidate/exact confusion:** L3 must not become the exact file-result
   source. It returns torrent candidates; L1/L2 verify.
3. **Broad-query tail:** single broad grams are a known 77-94 ms p95
   single-client class and grow under high concurrency. UX/backpressure is part
   of the design.
4. **Node label/PVC pinning:** wrong HEL1 node selector strands the local-path
   volume or pod.
5. **Segment growth:** follow mode must monitor segment count and merge health.
6. **Counts:** exact global file counts for broad path substrings are background
   or cached work, not an interactive L3 request.

---

## 18. Current Next Step

Next: add backend exact-refine routing and the homelab `bitmagnet-pathsearch`
role/manifests from this reconciled plan, then build and prove the image in
prod. The `torrent_files` DROP stays deferred.
