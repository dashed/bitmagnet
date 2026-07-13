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
| `variable-path-retained` | cycles 39, 128, 512, 1,024 | sensitivity to a mixed long-path tail |
| `long-path-retained` | fixed 1,024 | adversarial all-long-path upper observation |

The latter two do not claim that 1,024 bytes is a schema maximum: PostgreSQL
`text`, MessagePack, and the decoder currently impose no path-byte limit. They
measure how strongly the count caps depend on owned string bytes and keep that
residual visible rather than treating a single 39-byte fixture as a hard bound.

Run on Linux in release mode:

```text
CARGO_BUILD_JOBS=4 cargo run --release -p bitmagnet-search-serve \
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
the configured per-torrent, chunk, retained, deadline, and concurrency bounds.

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
