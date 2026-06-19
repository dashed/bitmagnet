# cb-D2 — DuckDB-on-Parquet **concurrency** spec (E3)

**Owner:** `cb-d2-duck` (team `bitmagnet-bench`, task cb-D2 / "#65")
**Date:** 2026-06-10
**Status:** SPEC + DRAFTED harness (`bench/cb_duckdb_load.py`). **DESIGN-ONLY — nothing run on HEL1.**
**Question:** ARCH-C/RUN-2 proved the L2 DuckDB tier is fast — _for a single client_. Every one of those numbers (structured <250 ms, most <35 ms, collapse 32 ms via rollups) was a **single-threaded, single-cursor, direct call**. Production is a **Rust gRPC sidecar** ([`L2-duckdb-parquet-search-rust-spec.md`](./L2-duckdb-parquet-search-rust-spec.md) §2c) serving **N concurrent queries from ONE in-process DuckDB** over an immutable read-only generation. In DuckDB `threads` and `memory_limit` are **GLOBAL per database instance** — concurrent queries **contend**. Does the L2 latency bar survive concurrency, and where is the knee?

**Success criterion (the L2 bar):** structured queries hold **< 250 ms p95 at N = 8** concurrent workers. Identify the N at which it breaks.

---

## 1. The production object we are modelling

The sidecar (L2 spec §2c) is:

- **ONE persistent DuckDB instance**, `memory_limit`/`threads` set **once** (bounded to the pod, e.g. 4–6 GB / 4 threads).
- duckdb-rs is **sync** → queries served via a **bounded `spawn_blocking` worker pool + semaphore**. So at any instant up to _pool-size_ queries are **executing concurrently inside one engine**.
- Reads an **immutable read-only generation** of Parquet (base + tiny delta), swapped behind an `RwLock`. No writers during a query → pure read concurrency.

The Python driver reproduces exactly this shape: one in-process `duckdb.connect()`, N worker threads each holding a `.cursor()`, closed-loop against the same artifacts. **In-box parallelism is the point** — the concurrency lives inside one process/one engine, driven by one `ssh` invocation. (The Rust `spawn_blocking` pool ≙ the Python thread pool; both hand N concurrent `execute()` calls to one shared `DatabaseInstance`.)

---

## 2. DuckDB concurrency semantics — RESOLVED from docs/source (the design pivots on these)

### 2.1 The Python client releases the GIL during execution → **use threads, not multiprocessing**

DuckDB's Python client **releases the GIL while a query executes** ([duckdb-python free-threading discussion #40](https://github.com/duckdb/duckdb-python/discussions/40)). So N Python threads each calling `execute().fetchall()` achieve **real wall-clock overlap** inside the C++ engine — the GIL is only held for the thin bind/fetch marshalling, not the scan/aggregate. Caveat: paths that **call back into Python** (e.g. scanning a pandas DataFrame, Python UDFs) re-acquire the GIL; **our queries read Parquet and fetch native tuples → no Python callback → GIL stays released for the heavy part.** ⟹ **multiprocessing is unnecessary for the primary measurement**; it is the _fallback_ only if 2.2 shows serialization.

### 2.2 `.cursor()` vs serialization — the one **CONTESTED** point this bench resolves

The docs disagree, and this is the crux:

- The **[Multiple Python Threads guide](https://duckdb.org/docs/current/guides/python/multiple_threads)** says: _"Each thread must use the `.cursor()` method to create a thread-local connection to the same DuckDB file."_ — i.e. cursors are the blessed concurrent-read pattern, **no lock mentioned**.
- **[Discussion #12817](https://github.com/duckdb/duckdb/discussions/12817)** has a maintainer saying _"cursor() just creates a new connection so each thread will have its own"_ (→ independent, parallel) **but also** the behavioral rule _"Only one query execution can be active at a time on a given connection"_ (→ a _bare shared_ connection serializes; cursors are the escape hatch). A separate thread summarizes the opposite reading — that cursors are _handles on one connection_ and _"cannot run queries at the same time."_

So the authoritative docs **literally contradict each other** on whether cursors-of-one-connection overlap or serialize. **This is precisely what E3 must measure**, not assume. The bench's signature distinguishes the two outcomes cleanly:

| Outcome                        | Aggregate-QPS vs N                   | Per-query p50 vs N                              | Reading                          |
| ------------------------------ | ------------------------------------ | ----------------------------------------------- | -------------------------------- |
| **Parallel** (cursors overlap) | rises until CPU-bound, then plateaus | rises as queries share the thread pool          | guide is right                   |
| **Serialized** (cursors queue) | **flat** ≈ single-worker QPS         | p50 ~flat, **p95/p99 inflate ~N×** (queue wait) | #12817's "one at a time" reading |

`bench/cb_duckdb_load.py` runs **both** topologies so the contrast is direct (§3).

### 2.3 `threads` under N concurrent queries — one global morsel pool, work-stealing

`threads` is a **global per-instance** setting (the engine's morsel-driven scheduler pool), **not** per-query. A _single_ analytical query already fans across all `threads` workers. With N concurrent queries, they **share that one pool** — DuckDB's scheduler interleaves morsels across active pipelines (work-stealing), so N queries don't get N× threads; they **time-share the same `threads` workers**. Consequences the sweep makes visible:

- `threads = 24` (= all cores): one heavy scan already saturates the box → adding concurrency **inflates each query's latency ~N×** for CPU-bound shapes; aggregate throughput is ~**flat** (already CPU-bound at N=1). Great at N=1, collapses under load.
- `threads = 4` (per-query cap): each query uses ≤4 workers → ~**6 queries run truly in parallel** before 24 cores saturate → **better multi-tenant throughput and lower tail** at high N, at the cost of slower single heavy queries. The classic "cap per-query parallelism to raise concurrent throughput" trade. **This is the knob a sidecar operator actually tunes** → it is the second sweep axis.

### 2.4 `memory_limit` under concurrent aggregates — the spill / blow-up risk

`memory_limit` is **global per instance** too. N concurrent aggregates (`GROUP BY`, `COUNT(DISTINCT)`, hash joins) each build hash tables **against the same budget**. When the _combined_ footprint exceeds `memory_limit`, DuckDB **spills to `temp_directory`**. Two hazards:

- If `temp_directory` is unset/defaulted onto a **tmpfs / `/dev/shm`**, "spilling" consumes **RAM**, not disk → the **/dev/shm-class blow-up** (OOM-kill of the pod). → the bench **pins `temp_directory` to real disk** (`<scratch>/duckdb_tmp`) and a **bounded `memory_limit`** (default `6GB`, pod-like), and **samples peak RSS + spill bytes** so we see exactly when contention forces spill.
- Our optimized mix is mostly **rollup lookups / zonemap-pruned scans** (small memory), with the heavy `COUNT(DISTINCT)` two-sided range as the pressure case. The sweep shows whether N concurrent copies of the heavy shape breach the budget.

**Design conclusions baked into the harness:** Python **threads** (GIL released) · ONE shared instance + **per-worker `.cursor()`** (primary) · bounded `memory_limit` + on-disk `temp_directory` · sweep **N × `threads`** · measure **p50/p95/p99 per (N, threads, query) + aggregate QPS + peak RSS + spill**.

---

## 3. Experiment E3

### 3.1 Topologies

- **PRIMARY — sidecar-faithful (the headline):** one shared `duckdb.connect()` (in-memory) with views over the Parquet artifacts; **N worker threads, each its own `.cursor()`**. Exactly the Rust embedding (one instance, N concurrent reads over read-only Parquet).
- **SECONDARY — contrast:** **N separate `duckdb.connect(idx_native.duckdb, read_only=True)`**. ⚠️ **Same-process connects to one path SHARE a `DatabaseInstance` via the instance cache** → still **one engine** (one thread pool / one `memory_limit`). So SECONDARY isolates the **Python cursor-vs-separate-connection binding overhead**, **not** engine isolation. If — and only if — PRIMARY shows cursor **serialization** (2.2), the true engine-isolation contrast is **separate processes** (`--multiproc`, the tertiary fallback). State this explicitly in the writeup; do not claim SECONDARY gives independent engines.

### 3.2 Query mix (the ARCH-C interactive shapes → production-layout artifacts)

All from `/home/ansible/bench-scratch` (built by `export_parquet_pg.py` + `arch_c_optimize.py`):

| query                  | artifact                               | shape                                             | ARCH-C single-client | in success bar?                                     |
| ---------------------- | -------------------------------------- | ------------------------------------------------- | -------------------- | --------------------------------------------------- |
| `find_mkv_gt1gb_lim1k` | `v1_sorted.parquet`                    | paginated find, `ext∧size` LIMIT 1000 (early-out) | ~56 ms               | ✅                                                  |
| `collapse_rollup_v7`   | `v7_agg_torrent_ext.parquet`           | distinct-torrent collapse (one-sided, exact)      | ~5 ms                | ✅                                                  |
| `two_sided_range_v1`   | `v1_sorted.parquet`                    | `COUNT(DISTINCT)` mkv∈[1,2]GB (zonemap)           | ~109 ms              | ✅                                                  |
| `groupby_rollup_v6`    | `v6_agg_ext.parquet`                   | rollup `GROUP BY` ext lookup                      | ~12 ms               | ✅                                                  |
| `hydrate_point_v0`     | `files_slim.parquet` (info_hash-order) | single-torrent point lookup                       | ~17 ms               | ✅                                                  |
| `path_ilike_lim100`    | `files_full.parquet` (+path)           | path-ILIKE common-term early-out LIMIT 100        | ~142 ms              | ➖ (acknowledged-slow; measured, excluded from bar) |

> Why these artifacts: the **recommended production layout** (ARCH-C §5) is **sorted slim + the two rollup tables** (≈12.3 GB). `find`/`range` ride the sorted `v1` (zonemap pruning); `collapse`/`groupby` ride the `v6`/`v7` rollups (the `<50 ms` lever); `hydrate` rides the **info_hash-ordered** `v0` (its row-group min/max prune the point lookup → 17 ms, _not_ `v1` which is ext-ordered and would full-scan); `path` needs the `path` column → `files_full`. Path-ILIKE is the known unprunable ~23 s shape **unless** LIMIT + common-term early-out (the 142 ms case) — it is in the mix to stress the tail but **out of the bar** (its slow tail is a known carve-out, L3/Tantivy territory).

**SECONDARY** runs only the structured subset that maps to the native `files` table (no `path`/rollups): `find`, `two_sided_range`, `collapse_count_distinct`, `hydrate_point`, `exact_count`.

The mix is driven **closed-loop, round-robin, no think time** → measures **max throughput + latency-under-load** (the conservative worst case; production skews lighter toward browse/facet, so real tail ≤ what we report).

### 3.3 Axes & fixed cost

- **N (concurrency):** {1, 2, 4, 8, 16}.
- **`threads`:** {24 (= all cores, default), 4 (per-query cap)} — §2.3.
- **`memory_limit`:** fixed pod-bound **6 GB** default (observe spill); optional second value (e.g. `32GB`) if cheap.
- **Wall-clock:** **~60 s per (topology, threads, N) level** (closed loop → thousands of samples for cheap queries, ≥hundreds for heavy → stable p95/p99).
- **Warm steady-state:** each query warmed once per `threads` config before timing (the always-on sidecar serves warm; matches ARCH-C warm p50). Artifacts (~27 GB resident: v0 3.86 + v1 10.3 + v6/v7 1.4 + full 11.7) fit HEL1's 125 GB RAM.

Total primary wall-clock ≈ 2 threads-cfg × 5 N × 60 s ≈ **10 min** + warmups; secondary similar. Comfortably a single session.

### 3.4 Outputs (per level)

- per-query **p50 / p95 / p99** (ms) + sample count
- **aggregate QPS** (= total completed queries / wall) — the parallel-vs-serialize discriminator (§2.2)
- **peak RSS** (GB, `/proc/self/status` VmRSS sampler @50 ms)
- **peak spill** (GB, `temp_directory` size sampler) — the memory-contention signal
- a **KNEE report**: first N (per `threads` cfg) at which **any structured** query breaches **250 ms p95**, with the offending query(ies).

---

## 4. The drafted harness — `bench/cb_duckdb_load.py`

uv-runnable single-file script, style-matched to `bench/bench_duckdb.py` / `arch_c_optimize.py` (`# /// script` header, `pct()`, `duckdb`). Key pieces:

- `primary_topology()` / `secondary_topology()` — the two §3.1 factories; `make_exec` returns a `.cursor()` (primary) or a fresh `read_only` connection (secondary).
- `run_level()` — N worker threads, `threading.Barrier` to start together, `threading.Event` to stop at `--duration`, per-worker latency lists merged at the end. Closed loop, staggered starting query per worker.
- `Sampler` — daemon thread tracking **peak VmRSS** + **peak spill bytes**.
- `configure()` — sets `memory_limit`, `temp_directory` (on disk), `PRAGMA disable_object_cache`, and `threads` once on the shared instance (global per §2.3/2.4).
- `ih_pool()` — pre-fetches 1000 `info_hash` BLOB literals for the point lookup (no per-call round-trip; exercises real keys).
- CSV out + the KNEE report.

**Gate flags (DESIGN-ONLY safety):** the full sweep is **gated** — without `--smoke` (a local toy: workers=1,2 / 5 s) **or** `--i-have-user-ok` it prints the plan and **exits without touching the artifacts**. The real HEL1 run requires the explicit `--i-have-user-ok`.

---

## 5. Run protocol (HEL1 — for when the user OKs RUN-7; NOT run now)

Single `ssh`, in-box parallelism, `setsid` + `.exit` sentinel, gentle polling — per the team's HEL1 ops lessons (tailscale IP; `setsid` survives client-side SSH timeouts; ONE connection at a time; lock+`pgrep` guard against duplicate concurrent runs colliding).

```bash
# HEL1 via tailscale (public IP SSH is flaky 255/124); ssh-agent signing flake → -o IdentityAgent=none
HEL1="ssh -o IdentityAgent=none -i ~/.ssh/id_ed25519 ansible@<HEL1_TAILSCALE_IP>"

# 1) ship the script (idempotent)
scp -o IdentityAgent=none -i ~/.ssh/id_ed25519 \
    bench/cb_duckdb_load.py ansible@<HEL1_TAILSCALE_IP>:/home/ansible/bench-scratch/

# 2) ONE detached launch — lock+pgrep guard, setsid survives the SSH timeout, writes .exit on completion
$HEL1 'cd /home/ansible/bench-scratch && \
  ( set -e; \
    if [ -e cb_d2.lock ] || pgrep -f cb_duckdb_load.py >/dev/null; then echo "ALREADY RUNNING"; exit 9; fi; \
    : > cb_d2.lock; rm -f cb_d2.exit; \
    setsid bash -c "uv run cb_duckdb_load.py \
        --scratch /home/ansible/bench-scratch \
        --workers 1,2,4,8,16 --threads-axis 24,4 --duration 60 \
        --mem 6GB --csv cb_d2_primary.csv --i-have-user-ok \
        > cb_d2_primary.log 2>&1; echo \$? > cb_d2.exit; rm -f cb_d2.lock" \
      </dev/null >/dev/null 2>&1 & \
    echo "LAUNCHED pid=$!" )'

# 3) gentle poll (NOT every few seconds — wait for the completion signal)
$HEL1 'cd /home/ansible/bench-scratch && tail -n 40 cb_d2_primary.log; \
       [ -e cb_d2.exit ] && echo "DONE rc=$(cat cb_d2.exit)" || echo "still running"'

# 4) SECONDARY contrast (separate run; --secondary uses idx_native.duckdb)
$HEL1 '... setsid ... uv run cb_duckdb_load.py --secondary \
        --workers 1,2,4,8,16 --threads-axis 24,4 --duration 60 \
        --mem 6GB --csv cb_d2_secondary.csv --i-have-user-ok > cb_d2_secondary.log 2>&1 ...'

# 5) pull results
scp -o IdentityAgent=none -i ~/.ssh/id_ed25519 \
    ansible@<HEL1_TAILSCALE_IP>:/home/ansible/bench-scratch/cb_d2_*.csv ./
```

**Local sanity (safe, anytime):** `uv run bench/cb_duckdb_load.py --smoke --scratch <dir-with-artifacts>` — needs the artifacts present; on a dev box without them it errors on the first read (expected). It is a wiring check, not a measurement.

**Pre-flight:** confirm `uv` is on PATH for `ansible` (installed userspace per RUN-2); confirm artifacts exist (`ls -la /home/ansible/bench-scratch/{files_slim,v1_sorted,v6_agg_ext,v7_agg_torrent_ext,files_full}.parquet idx_native.duckdb`). The bench env is the still-up HEL1 restore (RUN-6 teardown pending).

---

## 6. What the result tells the L2 deploy decision

| Finding                                                                              | Implication for the sidecar                                                                                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| structured p95 < 250 ms holds **at N=8**                                             | L2 bar **met under concurrency**; bounded `spawn_blocking` pool size ~8 is safe → ship as specced                                                                                                                                                                             |
| breaches **before N=8** at `threads=24` but **holds at `threads=4`**                 | **cap per-query `threads`** in the sidecar config (≈ cores/expected-concurrency); set the semaphore to the knee                                                                                                                                                               |
| aggregate QPS **flat** as N grows (PRIMARY) but **scales** (SECONDARY/`--multiproc`) | cursor-level serialization is real → sidecar must use **separate connections** (still one instance) or accept the single-writer-style throughput ceiling; size the pool to the QPS plateau                                                                                    |
| **spill > 0** / peak RSS approaches `memory_limit` at high N                         | the heavy `COUNT(DISTINCT)` is the contention driver → either **raise `memory_limit`**, **pin a real-disk `temp_directory`** (never tmpfs), or route collapse through the **rollup** (which it already does — `v7`) and keep the unbounded `COUNT(DISTINCT)` off the hot path |

The knee N is the number to set the **`spawn_blocking` pool + semaphore** to, and the `threads` outcome is the **per-query parallelism cap** to put in the pod config — both are direct inputs to the L2 deploy ([`L2-duckdb-parquet-search-rust-spec.md`](./L2-duckdb-parquet-search-rust-spec.md) §2c, §5).

---

## 7. Deliverables status

- ✅ this spec (`bitmagnet/docs/dev/cb-D2-duckdb-concurrency-spec.md`)
- ✅ drafted harness (`bitmagnet/bench/cb_duckdb_load.py`) — ruff-clean, `py_compile`-OK, gated; **not run**
- ⏸ HEL1 sweep (RUN-7) — pending explicit user OK (DESIGN-ONLY per the task)
