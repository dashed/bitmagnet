# Phase-2 integrated GraphQL RSS gate

This harness measures the boundary that the isolated composer RSS binary does
not cover: the exact `Dockerfile.graphql` image, real sqlx/PostgreSQL hydration,
the L3 gRPC client, async-graphql projection/serialization, four simultaneous
clients, and the service container's cgroup-v2 memory accounting.

It is local-only. It creates a private container network, a tmpfs PostgreSQL,
an L3 test double, and fresh GraphQL containers. It publishes no host ports and
does not read a kubeconfig, contact k3s, or mutate production.

## Prerequisites (fail closed)

- A Linux Docker server using cgroup v2. The `gate` profile requires amd64 to
  match production. Docker Desktop is supported when its Linux VM reports
  cgroup v2 and the remaining prerequisites; an arm64 `smoke` run is structural
  evidence only and cannot satisfy the admission gate.
- At least 4 Docker CPUs and 12 GiB assigned to Docker. The GraphQL container
  gets exactly 4 CPUs and 8 GiB; the extra capacity isolates PostgreSQL, the
  mock, and response clients from the measured cgroup.
- Network access for the Rust/Python/PostgreSQL base images and Cargo crates,
  plus enough disk for a release Rust build.
- A checkout whose current files are the code to measure. Dirty and untracked
  files are allowed and are included in the recorded workspace digest/status.

The runner exits before creating containers when any runtime prerequisite is
missing:

```sh
python3 bench/graphql-rss/run.py --preflight-only
```

## Gate run

From the repository root:

```sh
python3 bench/graphql-rss/run.py \
  --profile gate \
  --repeat 3 \
  --output bench/graphql-rss/evidence/graphql-rss-gate.jsonl
```

The default acceptance ceiling is 6 GiB peak inside the 8 GiB GraphQL cgroup,
leaving 25% headroom. This is a harness guard, not production admission or an
approval to deploy. Override it only when the reviewer has selected a different
gate explicitly:

```sh
python3 bench/graphql-rss/run.py --max-peak-bytes 5905580032
```

A fast structural run scales the same byte-limit relationships down and still
executes all four cases:

```sh
python3 bench/graphql-rss/run.py \
  --profile smoke \
  --repeat 1 \
  --output /tmp/bitmagnet-graphql-rss-smoke.jsonl
```

The runner builds the GraphQL image with the repository's exact command:

```sh
docker build \
  -f bitmagnet-rs/docker/Dockerfile.graphql \
  -t <session-tag> \
  bitmagnet-rs
```

`--graphql-image` and `--helper-image` can reuse prebuilt images. Their immutable
Docker image IDs and layers are recorded, but a supplied GraphQL image has weaker
source provenance than the default build.

## Workload and evidence

The disposable database contains two classes of production-format
`zstd(msgpack[{i,p,e,s}])` blobs:

- `accepted`: four candidate torrents whose combined MessagePack plus decoded
  owned strings remain below the decoded budget. Composer-retained owned strings
  remain below the retained budget while filling at least 80% of it. Evidence
  reports GraphQL's later path-derived extension bytes separately because they
  are not charged to the composer-retained budget.
- `adversarial`: a highly compressible blob whose decompressed MessagePack is
  strictly larger than the one-blob decompression ceiling.

Each repeat starts a new GraphQL cgroup for every combination below:

| Scenario | Projection | Expected result |
| --- | --- | --- |
| accepted | minimal WebUI-like torrent fields, no `torrent.files` | 4 items, no retained file response |
| accepted | the same fields plus `torrent.files` | 4 items and every accepted file serialized |
| adversarial | minimal | bounded rejection, empty estimated result |
| adversarial | `torrent.files` | bounded rejection, empty estimated result |

The HTTP driver and each case's fresh gRPC test double have four-party barriers.
A harness-only forced-RLS policy adds a second barrier on the first
`torrent_contents` read after each request acquires a composer refine permit. A
run is invalid unless the mock records exactly four arrivals, releases, and
responses in one generation and four distinct sqlx backends reach the refine
barrier. The driver retains only response summaries and hashes in the JSONL; it
does not write multi-megabyte response bodies to disk.

The JSONL contains:

- commit, branch, dirty status, tracked-diff and full workspace hashes;
- Dockerfile, Cargo lock/toolchain, migration-set, schema, runner, and helper
  hashes;
- GraphQL/helper/PostgreSQL image IDs, repo digests, platforms, and layers;
- every byte/count/timeout/concurrency configuration value and repeat number;
- seed blob raw/decoded-owned/composer-retained/GraphQL-derived/compressed sizes,
  retained-budget fill ratio, and SHA-256 hashes;
- four response status/size/hash/latency/error summaries and handler duration;
- selected Prometheus process/pathsearch samples plus scrape hashes;
- `memory.current`, `memory.peak`, `memory.events(.local)`, `memory.stat`, swap
  peak, Docker OOM state, and service-log hash/tail;
- per-check evaluation and a terminal pass/fail summary.

A small PID-1 wrapper and cgroup watcher run inside the GraphQL container. The
wrapper forwards termination to the exact GraphQL binary; the watcher atomically
mirrors the kernel files every 100 ms and takes a final sample after the binary
exits. Keeping the tiny wrapper alive also preserves cgroup evidence if the OOM
killer selects the much larger GraphQL child. `memory.peak` itself is
kernel-maintained and exact.

## Cleanup and exit status

Every helper, service, and dependency container has a session-scoped name and is
registered before launch. Containers and the private network are removed and
their absence verified on success and failure; cleanup failure is terminal
evidence. Built images and JSONL evidence remain for review. `--keep` preserves
containers and the network for debugging; remove them manually afterward using
the prefix printed in Docker names.

- exit `0`: every repetition passed response, barrier, metrics, OOM, and peak
  checks;
- exit `1`: the harness completed but one or more gates failed;
- exit `2`: setup, prerequisite, build, or execution evidence was incomplete.

## Local self-checks

These checks need no containers and never touch production:

```sh
python3 -m unittest discover -s bench/graphql-rss -p 'test_*.py'
python3 -m py_compile bench/graphql-rss/run.py bench/graphql-rss/helper.py
```

## Deliberate limitations

- `schema.sql` is a minimal production-compatible schema for the SQL actually
  issued by this path. It avoids replaying non-transactional, large-table Goose
  backfills and is not a migration-parity test.
- The L3 server is a deterministic candidate/health test double. This gate
  measures client/composer/refine behavior, not Tantivy indexing or query RSS.
- The 8 GiB value is the GraphQL cgroup limit, not a whole-node pressure test.
- A passing JSONL is evidence for the P2-5 review; it does not flip the homelab
  RSS admission boolean, change an image pin, or authorize a dark deployment.
