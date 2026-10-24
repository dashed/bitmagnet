# Composer RSS harness

`search-serve-rss` measures the Rust L1 composer's actual blob representation
transition under the standard system allocator:

1. a parent process writes zstd/msgpack blobs to disk, so fixture generation
   cannot contaminate the measured child's high-water mark;
2. the child starts with only fixture metadata resident;
3. its fake PostgreSQL adapter reads the same raw blob bytes the Lane-S adapter
   will hydrate;
4. the real `Composer` decodes, exact-matches, clears `files_data`, and retains
   `Vec<BlobFile>` results; and
5. Linux `/proc/self/status` supplies `VmRSS` and `VmHWM`.

The fixture anchors to the measured live file-count shape (p50 6, p90 54, p99
743, max 88,561 files/torrent) and deliberately exercises the observed
high-fanout tail: three max-files torrents fit below the 300,000-file chunk
budget, while twelve drive the 1,000,000-file retained cap with exactly one
bounded lookahead. Paths are always unique and matching; the scenarios make
their byte shape explicit:

| Scenario | Path bytes | Purpose |
|---|---:|---|
| `chunk` | fixed 39 | original production-tail chunk observation |
| `retained` | fixed 39 | original retained cap + lookahead observation |
| `accepted-byte-boundary` | fixed 650 | high-fanout case accepted just below the byte envelope |
| `variable-path-retained` | cycles 39, 128, 512, 1,024 | sensitivity to a mixed long-path tail |
| `long-path-retained` | fixed 1,024 | adversarial all-long-path upper observation |

The latter two do not claim that 1,024 bytes is a schema maximum: PostgreSQL
`text` and MessagePack impose no per-path byte limit. They measure how the
aggregate decompressed, decoded-allocation, and retained-byte budgets stop a
hostile path distribution rather than treating a single 39-byte fixture as a
hard bound.

Run on Linux in release mode:

```text
CARGO_BUILD_JOBS=4 cargo run --locked --release -p bitmagnet-search-serve \
  --bin search-serve-rss -- --scenario all
```

Each child emits one JSON object with raw KiB readings, elapsed time, fixture
sizes, path-byte statistics, retained files, and the observed RSS delta per peak decoded file. The
300-second harness timeout isolates memory measurement from the independent
8-second route-deadline test; it is not a production recommendation.

This probe is necessary but not sufficient to accept or raise production caps.
Its evidence must be interpreted with the final GraphQL binary's allocator,
container limit, concurrent request count, PostgreSQL driver buffers, and other
resident services. C7 unit tests independently prove that every request stops at
the configured compressed/decompressed, transient decoded, retained-byte,
file-count, deadline, and concurrency bounds.

## 2026-07-13 Coder result

Raw evidence is committed in
`evidence/rss-coder-x86_64-20260713.jsonl`. The release build ran on Linux
x86_64 with Rust 1.97.0, the standard system allocator, 19.9 GiB host memory,
and `CARGO_BUILD_JOBS=4`:

| Scenario | Peak decoded upper bound | Retained | Peak RSS delta | Observed bytes/file |
|---|---:|---:|---:|---:|
| one production-tail chunk | 265,683 | 265,683 | 46,996 KiB | 181.133 |
| retained cap + lookahead | 1,062,732 | 974,171 | 159,324 KiB | 153.517 |

At the more conservative chunk observation, 300,000 files extrapolate to about
51.8 MiB of composer RSS delta. The retained-cap run directly observed the
worst representation transition: eleven live max-files vectors plus the
twelfth bounded lookahead remained below 156 MiB of RSS delta. This is far below
the earlier Go-derived 300 MiB transient + 200 MiB retained assumptions.

Decision from the original two scenarios: retain the existing
`300_000 / 300_000 / 1_000_000` file caps. The measurement supports those
defaults but does not justify raising them, and the observed live maximum
(88,561) remains below the per-torrent cap.

That is a count-bound decision, not yet a hard byte-bound acceptance. A final
integrated GraphQL-container concurrency/RSS run remains required before
deployment because the eventual sqlx pool, GraphQL response, and other resident
state are not in this isolated harness. The variable/long-path follow-up below
also has to remain within the agreed per-request component envelope; otherwise
deployment requires either lower count caps or explicit compressed,
decompressed, and retained decoded-byte bounds. Until then the evidence blocks
raising caps or claiming whole-pod headroom, not merging the bounded route.

## 2026-07-13 path-shape follow-up

This section records the historical count-only result that motivated the P2-5
byte envelope. Its deployment conclusion is superseded by the 2026-07-14
follow-up below, while the raw observation remains valid.

Raw evidence is committed separately in
`evidence/rss-coder-x86_64-20260713-path-shapes.jsonl`, preserving the original
run's provenance. The same Linux x86_64 Coder host, release profile, standard
system allocator, and `CARGO_BUILD_JOBS=4` produced:

| Scenario | Path-byte shape | Peak decoded upper bound | Retained | Peak RSS delta | Observed bytes/file |
|---|---:|---:|---:|---:|---:|
| chunk | fixed 39 | 265,683 | 265,683 | 46,816 KiB | 180.439 |
| retained | fixed 39 | 1,062,732 | 974,171 | 159,436 KiB | 153.625 |
| variable-path-retained | 39/128/512/1,024 cycle | 1,062,732 | 974,171 | 595,748 KiB | 574.036 |
| long-path-retained | fixed 1,024 | 1,062,732 | 974,171 | 1,270,160 KiB | 1,223.868 |

All four routes completed successfully and preserved the count contracts: the
three-torrent chunk retained 265,683 files, while each retained-cap scenario
kept eleven max-files torrents (974,171 files) and decoded only the twelfth
bounded lookahead. The mixed-path run nevertheless raised the per-request RSS
delta to about 581.8 MiB, and the fixed-1,024-byte run raised it to about
1,240.4 MiB. Count limits therefore bound vector cardinality but do not bound
the owned bytes in decoded paths.

The fixed-long fixture's raw compressed blobs total only 5,678,099 bytes.
Because repetitive paths compress well, a compressed-input byte limit alone
would not bound decompressed or retained allocation. The current decoder and
database schema have no independent path-byte maximum.

The existing count caps remain appropriate as defaults and must not be raised
from this evidence. The exact residual P2-5 deployment gate is one of:

1. implement and test bounded decompression plus decoded and retained byte
   budgets, with an explicit error or fallback rather than silent truncation;
   or
2. establish an enforced upstream path-byte maximum, then run the integrated
   GraphQL/sqlx container at `MaxConcurrentRefines` and demonstrate peak RSS
   below the actual pod limit with agreed headroom.

Until one branch of that gate is satisfied, the 1,240.4 MiB single-request
observation means the count caps alone cannot establish a finite worst-case
byte envelope. This blocks deployment acceptance and cap increases, but not
merging the already bounded route and its observability library. Runtime metric
acceptance separately still requires the final composition root to register one
shared `PathsearchMetrics` and pass it to both the composer and health poller.

## 2026-07-14 P2-5 byte-envelope follow-up

Raw evidence is committed in
`evidence/rss-coder-x86_64-20260714-p2-5-byte-bounds.jsonl`. The Linux x86_64
release binary used Rust 1.97.0, the standard system allocator, and the staged
production envelope:

- 64 MiB maximum compressed input and decompressed MessagePack output per blob;
- 128 MiB transient decoded allocation per chunk, charged as raw MessagePack
  plus owned path/extension strings; and
- 64 MiB retained owned-string bytes per request.

| Scenario | Retained files | Cap expected | Peak RSS delta |
|---|---:|---:|---:|
| chunk, fixed 39 | 265,683 | no | 48,620 KiB |
| retained, fixed 39 | 974,171 | yes | 160,944 KiB |
| accepted boundary, fixed 650 | 88,561 | no | 128,076 KiB |
| mixed 39/128/512/1,024 | 88,561 | yes | 89,508 KiB |
| fixed 1,024 | 0 | yes | 69,656 KiB |

Every route completed and matched its expected bounded-prefix contract. The
new fixed-650 high-fanout case is the important positive control: all 88,561
files were retained without tripping a cap, at about 125.1 MiB peak RSS delta.
The hostile fixed-1,024 case that previously retained 974,171 files and peaked
about 1.24 GiB now retains none and peaks about 68.0 MiB while proving the
decompression boundary rejects the highly compressible input.

This closes the count-only byte-envelope defect and supports retaining the
existing count defaults. It does not close deployment admission. The final gate
is the integrated `bench/graphql-rss` run using the exact GraphQL image, real
sqlx/PostgreSQL hydration, four overlapping requests, both GraphQL projections,
and an 8 GiB cgroup with the agreed 6 GiB peak ceiling. Keep the homelab RSS gate
false until that JSONL passes on a host with at least 12 GiB available to Docker;
the disposable Coder Docker runtime used for this component run correctly fails
that preflight and is not valid integrated evidence.
