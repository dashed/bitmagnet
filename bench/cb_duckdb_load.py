# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""E3 — DuckDB concurrency load driver (team bitmagnet-bench, task cb-D2).

THROWAWAY bench. Answers the one question ARCH-C/RUN-2 left open: those latency
numbers (<250ms structured, most <35ms) were all SINGLE-CLIENT direct calls.
Production = a Rust gRPC sidecar (L2-duckdb-parquet-search-rust-spec §2c) serving
N concurrent queries from ONE in-process DuckDB over an immutable read-only
generation. `threads` and `memory_limit` are GLOBAL-per-instance in DuckDB, so
concurrent queries CONTEND for one thread pool + one memory budget.

This driver runs the ARCH-C query mix from N closed-loop workers for a fixed
wall-clock, reporting per-(N x threads x query) p50/p95/p99, aggregate QPS, peak
RSS, and spill bytes. Two topologies:

  PRIMARY  (sidecar-faithful): ONE shared in-process duckdb.connect() over the
           Parquet artifacts; each worker uses its own .cursor(). This is exactly
           how the Rust sidecar embeds DuckDB (one instance, N concurrent reads).
  SECONDARY(contrast): N separate duckdb.connect(<file>.duckdb, read_only=True).
           NB: same-process connects to one path SHARE a DatabaseInstance via the
           instance cache -> still ONE engine (one thread pool / memory_limit).
           So this isolates the Python cursor-vs-connection binding overhead, NOT
           engine isolation. True engine isolation = separate PROCESSES; offered
           as the --multiproc fallback IF cursors prove to serialize.

DuckDB facts resolved for this design (see cb-D2-duckdb-concurrency-spec.md §2):
  * The Python client RELEASES the GIL during execute() -> threads give real
    parallelism (no multiprocessing needed for the primary measurement).
  * .cursor() is the documented multi-thread pattern; whether cursors of one
    connection truly overlap or serialize on the client context is CONTESTED in
    the docs -> this bench resolves it (flat aggregate QPS as N grows = serialize;
    rising QPS until CPU-bound = parallel).

Usage (style-matched to bench_duckdb.py / arch_c_optimize.py):
  uv run cb_duckdb_load.py --smoke                      # local-safe sanity, ~15s
  uv run cb_duckdb_load.py --scratch /home/ansible/bench-scratch \
      --duration 60 --threads-axis 24,4 --workers 1,2,4,8,16 \
      --mem 6GB --csv cb_d2.csv                         # the full E3 sweep (HEL1)

GATE: the full sweep reads the real 879.5M-row artifacts on HEL1 and pins all
cores for minutes. It is gated behind --i-have-user-ok (or --smoke for the toy
run). Without one of those it prints the plan and exits.
"""
import argparse
import csv as csvmod
import os
import threading
import time
from collections import defaultdict

import duckdb

# ----------------------------------------------------------------------------- #
# Artifacts (HEL1 /home/ansible/bench-scratch — produced by export_parquet_pg.py
# + arch_c_optimize.py build()). Paths are relative to --scratch.
# ----------------------------------------------------------------------------- #
ART = {
    "v0": "files_slim.parquet",            # info_hash-order slim (hydrate point lookup)
    "v1": "v1_sorted.parquet",             # sorted(ext,size) -> zonemap pruning (find/range)
    "v6": "v6_agg_ext.parquet",            # per-ext rollup (GROUP BY lookup)
    "v7": "v7_agg_torrent_ext.parquet",    # per-(torrent,ext) rollup (collapse)
    "pf": "files_full.parquet",            # +path (path-ILIKE)
}
NATIVE = "idx_native.duckdb"               # SECONDARY: native `files` table + 2 ART idx

MKV_GT_1GB = "extension='mkv' AND size>1000000000"


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1))))
    return xs[k]


# ----------------------------------------------------------------------------- #
# Query mix — the ARCH-C interactive shapes, each mapped to its production-layout
# artifact. `sql` is a string, or a callable(ih)->str for the point lookup.
# `structured` flags the ones counted toward the <250ms-p95 success bar (path-FTS
# is acknowledged-slow and excluded from the bar but still measured).
# ----------------------------------------------------------------------------- #
def primary_mix():
    return [
        # paginated find mkv>1GB (sorted layout, early-out LIMIT) — ARCH-C ~56ms
        ("find_mkv_gt1gb_lim1k",
         f"SELECT info_hash,file_index,size FROM v1 WHERE {MKV_GT_1GB} LIMIT 1000", True),
        # distinct-torrent collapse via the v7 rollup (one-sided exact) — ARCH-C ~5ms
        ("collapse_rollup_v7",
         "SELECT count(*) FROM v7 WHERE extension='mkv' AND max_size>1000000000", True),
        # two-sided range distinct torrents on sorted v1 (zonemap) — ARCH-C ~109ms
        ("two_sided_range_v1",
         "SELECT count(DISTINCT info_hash) FROM v1 "
         "WHERE extension='mkv' AND size BETWEEN 1000000000 AND 2000000000", True),
        # rollup GROUP BY on the v6 agg table — ARCH-C ~12ms
        ("groupby_rollup_v6",
         "SELECT extension,n,bytes FROM v6 ORDER BY n DESC LIMIT 30", True),
        # single-torrent hydrate point lookup on info_hash-ordered v0 — ARCH-C ~17ms
        ("hydrate_point_v0",
         lambda ih: f"SELECT file_index,extension,size FROM v0 "
                    f"WHERE info_hash={ih} ORDER BY file_index", True),
        # path-ILIKE w/ LIMIT (common-term early-out) — ARCH-C ~142ms; NOT in the bar
        ("path_ilike_lim100",
         "SELECT info_hash,file_index,path,size FROM pf "
         "WHERE path ILIKE '%1080p%' AND extension='mkv' LIMIT 100", False),
    ]


def secondary_mix():
    # native `files` table (no path/rollups) — the structured subset only.
    return [
        ("find_mkv_gt1gb_lim1k",
         f"SELECT info_hash,file_index,size FROM files WHERE {MKV_GT_1GB} LIMIT 1000", True),
        ("two_sided_range",
         "SELECT count(DISTINCT info_hash) FROM files "
         "WHERE extension='mkv' AND size BETWEEN 1000000000 AND 2000000000", True),
        ("collapse_count_distinct",
         f"SELECT count(DISTINCT info_hash) FROM files WHERE {MKV_GT_1GB}", True),
        ("hydrate_point",
         lambda ih: f"SELECT file_index,extension,size FROM files "
                    f"WHERE info_hash={ih} ORDER BY file_index", True),
        ("exact_count",
         f"SELECT count(*) FROM files WHERE {MKV_GT_1GB}", True),
    ]


# ----------------------------------------------------------------------------- #
# Peak-RSS + spill sampler (Linux /proc; daemon thread).
# ----------------------------------------------------------------------------- #
class Sampler(threading.Thread):
    def __init__(self, temp_dir, interval=0.05):
        super().__init__(daemon=True)
        self.temp_dir = temp_dir
        self.interval = interval
        self.peak_rss = 0
        self.peak_spill = 0
        self._stop = threading.Event()

    def _rss(self):
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024
        except OSError:
            return 0
        return 0

    def _spill(self):
        tot = 0
        try:
            for d, _, fs in os.walk(self.temp_dir):
                for fn in fs:
                    try:
                        tot += os.path.getsize(os.path.join(d, fn))
                    except OSError:
                        pass
        except OSError:
            pass
        return tot

    def run(self):
        while not self._stop.is_set():
            self.peak_rss = max(self.peak_rss, self._rss())
            self.peak_spill = max(self.peak_spill, self._spill())
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()


# ----------------------------------------------------------------------------- #
# Connection setup.
# ----------------------------------------------------------------------------- #
def configure(con, threads, mem, temp_dir):
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{temp_dir}'")
    con.execute("PRAGMA disable_object_cache")  # match bench_duckdb's cold-ish semantics
    if threads:  # 0 = leave default (= all cores)
        con.execute(f"SET threads={threads}")


def register_views(con, scratch):
    for name, fn in ART.items():
        con.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_parquet('{os.path.join(scratch, fn)}')"
        )


def ih_pool(con, n=1000):
    """Pre-fetch a pool of info_hash literals for the point-lookup query.

    Stored as DuckDB BLOB literals ('\\x..'::BLOB) so the hydrate query binds a
    real key without a per-call round-trip. v0 is info_hash-ordered, so this also
    exercises zonemap pruning on the point lookup.
    """
    rows = con.execute(
        "SELECT '\\x' || lower(hex(info_hash)) FROM v0 "
        "WHERE info_hash IN (SELECT info_hash FROM v0 USING SAMPLE 5000 ROWS) "
        "GROUP BY info_hash LIMIT ?", [n]
    ).fetchall()
    return [f"'{r[0]}'::BLOB" for r in rows] or ["''::BLOB"]


# ----------------------------------------------------------------------------- #
# One concurrency level: N workers, closed loop, fixed wall-clock.
# ----------------------------------------------------------------------------- #
def run_level(make_exec, mix, ihs, n_workers, duration, temp_dir):
    stop_evt = threading.Event()
    barrier = threading.Barrier(n_workers + 1)
    results = [None] * n_workers

    def worker(wid):
        ex = make_exec()
        local = defaultdict(list)
        barrier.wait()
        i = wid  # stagger the starting query per worker
        while not stop_evt.is_set():
            name, q, _ = mix[i % len(mix)]
            sql = q(ihs[i % len(ihs)]) if callable(q) else q
            t = time.perf_counter()
            ex.execute(sql).fetchall()
            local[name].append(time.perf_counter() - t)
            i += 1
        results[wid] = local

    threads_ = [threading.Thread(target=worker, args=(w,)) for w in range(n_workers)]
    for t in threads_:
        t.start()
    barrier.wait()  # release all workers together
    t0 = time.perf_counter()
    stop_evt.wait(duration)
    stop_evt.set()
    for t in threads_:
        t.join()
    wall = time.perf_counter() - t0

    merged = defaultdict(list)
    for local in results:
        for name, xs in (local or {}).items():
            merged[name].extend(xs)
    total = sum(len(xs) for xs in merged.values())
    return merged, total, wall


# ----------------------------------------------------------------------------- #
# Topology factories.
# ----------------------------------------------------------------------------- #
def primary_topology(scratch, threads, mem, temp_dir):
    con = duckdb.connect()  # one shared in-memory instance
    configure(con, threads, mem, temp_dir)
    register_views(con, scratch)
    ihs = ih_pool(con)
    mix = primary_mix()

    def warmup():
        for name, q, _ in mix:
            sql = q(ihs[0]) if callable(q) else q
            con.execute(sql).fetchall()

    def make_exec():
        cur = con.cursor()            # the contested path: cursor of one connection
        return cur

    return con, mix, ihs, warmup, make_exec


def secondary_topology(scratch, threads, mem, temp_dir):
    path = os.path.join(scratch, NATIVE)
    base = duckdb.connect(path, read_only=True)
    configure(base, threads, mem, temp_dir)
    ihs = ih_pool_native(base)
    mix = secondary_mix()

    def warmup():
        for name, q, _ in mix:
            sql = q(ihs[0]) if callable(q) else q
            base.execute(sql).fetchall()

    def make_exec():
        c = duckdb.connect(path, read_only=True)  # separate connection (shares instance)
        configure(c, threads, mem, temp_dir)
        return c

    return base, mix, ihs, warmup, make_exec


def ih_pool_native(con, n=1000):
    rows = con.execute(
        "SELECT '\\x' || lower(hex(info_hash)) FROM files "
        "WHERE info_hash IN (SELECT info_hash FROM files USING SAMPLE 5000 ROWS) "
        "GROUP BY info_hash LIMIT ?", [n]
    ).fetchall()
    return [f"'{r[0]}'::BLOB" for r in rows] or ["''::BLOB"]


# ----------------------------------------------------------------------------- #
# Driver.
# ----------------------------------------------------------------------------- #
def main(a):
    workers = [int(x) for x in a.workers.split(",")]
    threads_axis = [int(x) for x in a.threads_axis.split(",")]
    temp_dir = a.temp_dir or os.path.join(a.scratch, "duckdb_tmp")
    os.makedirs(temp_dir, exist_ok=True)
    topo = secondary_topology if a.secondary else primary_topology
    topo_name = "secondary" if a.secondary else "primary"

    print(f"E3 DuckDB concurrency  topology={topo_name}  scratch={a.scratch}")
    print(f"  workers={workers}  threads_axis={threads_axis}  mem={a.mem}  "
          f"duration={a.duration}s  temp_dir={temp_dir}")
    if not (a.smoke or a.i_have_user_ok):
        print("\n[GATE] full sweep not run. Pass --smoke (local toy) or "
              "--i-have-user-ok (HEL1 full). Plan printed above; exiting.")
        return

    rows = []
    for threads in threads_axis:
        base, mix, ihs, warmup, make_exec = topo(a.scratch, threads, a.mem, temp_dir)
        tlabel = "all" if threads == 0 else str(threads)
        print(f"\n{'='*92}\nthreads={tlabel}  (warming caches…)", flush=True)
        warmup()
        for n in workers:
            smp = Sampler(temp_dir)
            smp.start()
            merged, total, wall = run_level(make_exec, mix, ihs, n, a.duration, temp_dir)
            smp.stop()
            qps = total / wall if wall else 0.0
            print(f"\n  N={n:<2d} threads={tlabel:<3} "
                  f"QPS={qps:8.1f}  total={total:>8}  wall={wall:5.1f}s  "
                  f"peakRSS={smp.peak_rss/1e9:5.2f}GB  spill={smp.peak_spill/1e9:5.2f}GB")
            for name, q, structured in mix:
                xs = merged.get(name, [])
                if not xs:
                    continue
                p50, p95, p99 = pct(xs, 50)*1e3, pct(xs, 95)*1e3, pct(xs, 99)*1e3
                bar = ""
                if structured and p95 > 250:
                    bar = "  <-- OVER 250ms p95 (BREACH)"
                print(f"    {name:26s} n={len(xs):>7} "
                      f"p50={p50:8.2f} p95={p95:8.2f} p99={p99:8.2f} ms{bar}")
                rows.append(dict(
                    topology=topo_name, threads=tlabel, workers=n, query=name,
                    structured=structured, samples=len(xs),
                    p50_ms=round(p50, 3), p95_ms=round(p95, 3), p99_ms=round(p99, 3),
                    qps=round(qps, 2), peak_rss_gb=round(smp.peak_rss/1e9, 3),
                    spill_gb=round(smp.peak_spill/1e9, 3), wall_s=round(wall, 2),
                ))
        base.close()

    if a.csv and rows:
        with open(a.csv, "w", newline="") as f:
            w = csvmod.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {a.csv}")

    # Knee report: first N (per threads cfg) where ANY structured query breaches 250ms p95.
    print(f"\n{'='*92}\nKNEE (structured p95 > 250ms):")
    by_cfg = defaultdict(list)
    for r in rows:
        if r["structured"] and r["p95_ms"] > 250:
            by_cfg[r["threads"]].append((r["workers"], r["query"], r["p95_ms"]))
    if not by_cfg:
        print("  none — all structured queries held <250ms p95 across the sweep ✅")
    for tlabel, breaches in sorted(by_cfg.items()):
        first = min(b[0] for b in breaches)
        print(f"  threads={tlabel}: first breach at N={first}  "
              f"({', '.join(f'{q}@N{n}={p:.0f}ms' for n, q, p in sorted(breaches))})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scratch", default="/home/ansible/bench-scratch",
                    help="dir holding the Parquet/.duckdb artifacts")
    ap.add_argument("--workers", default="1,2,4,8,16", help="N sweep (comma list)")
    ap.add_argument("--threads-axis", dest="threads_axis", default="24,4",
                    help="DuckDB `threads` settings to sweep (0 = all cores)")
    ap.add_argument("--duration", type=float, default=60.0, help="wall-clock per level (s)")
    ap.add_argument("--mem", default="6GB", help="memory_limit (pod-bound; e.g. 6GB)")
    ap.add_argument("--temp-dir", dest="temp_dir", default=None, help="spill dir (on disk)")
    ap.add_argument("--secondary", action="store_true",
                    help="SECONDARY topology: N read_only connects to idx_native.duckdb")
    ap.add_argument("--csv", default=None)
    ap.add_argument("--smoke", action="store_true",
                    help="local toy: overrides to workers=1,2 duration=5")
    ap.add_argument("--i-have-user-ok", dest="i_have_user_ok", action="store_true",
                    help="run the full HEL1 sweep (explicit user opt-in)")
    args = ap.parse_args()
    if args.smoke:
        args.workers = "1,2"
        args.threads_axis = args.threads_axis or "0"
        args.duration = 5.0
    main(args)
