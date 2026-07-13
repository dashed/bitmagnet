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

The fixture anchors to the measured live corpus shape (p50 6, p90 54, p99 743,
max 88,561 files/torrent) and deliberately exercises the observed high-fanout
tail: three max-files torrents fit below the 300,000-file chunk budget, while
twelve drive the 1,000,000-file retained cap with exactly one bounded lookahead.
Paths are unique, 39-byte media paths so allocator cost is not hidden by
shared strings or unrealistically short values.

Run on Linux in release mode:

```text
CARGO_BUILD_JOBS=4 cargo run --release -p bitmagnet-search-serve \
  --bin search-serve-rss -- --scenario all
```

Each child emits one JSON object with raw KiB readings, elapsed time, fixture
sizes, retained files, and the observed RSS delta per peak decoded file. The
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

Decision: retain the existing `300_000 / 300_000 / 1_000_000` file caps. The
measurement supports those conservative defaults but does not justify raising
them, and the real-world maximum (88,561) remains below the per-torrent cap.
Lane C's component-level allocator gate is satisfied. A final integrated
GraphQL-container concurrency/RSS run remains required before deployment because
the eventual sqlx pool, GraphQL response, and other resident state are not in
this isolated harness; it is specifically a blocker to increasing caps or
claiming whole-pod headroom, not a reason to weaken the current bounded route.
