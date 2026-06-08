# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""DuckDB-on-blobs query driver — THROWAWAY bench tool (team bitmagnet-bench, RUN-0a/RUN-2).

Runs the 6-query suite against the decoded Parquet produced by `blob_export`,
reporting cold (first hit) and warm p50/p95/p99 over R repetitions, at
threads=1 and threads=all, with rows-scanned + MB/s from EXPLAIN ANALYZE.

Usage:
  uv run bench_duckdb.py --slim files_slim.parquet [--full files_full.parquet]
                         [--reps 30] [--mem 32GB] [--csv out.csv]

Cold-cache note: DuckDB object cache is disabled per connection; for a TRUE
cold OS-page-cache run, drop_caches on the host before the first invocation
(handled by the RUN-2 wrapper, not here). Within one process, run 1 = cold-ish
(Parquet metadata + first read), runs 2..R = warm.
"""
import argparse, statistics, sys, time
import duckdb


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1))))
    return xs[k]


def queries(slim_rel, full_rel):
    """Return {name: (sql, needs_full)}. Q1-Q5 hit slim; Q4(hist)/Q6 noted."""
    q = {
        # Q1b — same discriminator filter but paginated (the realistic interactive form).
        "Q1b_mkv_gt_1gb_limit1k": (
            f"SELECT info_hash, file_index, size FROM {slim_rel} "
            f"WHERE extension='mkv' AND size>1000000000 LIMIT 1000",
            False,
        ),
        # Q2 — extension distribution (GROUP BY over all files).
        "Q2_ext_distribution": (
            f"SELECT extension, count(*) n, sum(size) bytes FROM {slim_rel} "
            f"GROUP BY extension ORDER BY n DESC LIMIT 50",
            False,
        ),
        # Q3 — size histogram (analytics: log2 buckets + percentiles).
        "Q3_size_histogram": (
            f"SELECT floor(log2(greatest(size,1)))::int lg, count(*) n, "
            f"approx_quantile(size,0.5) p50, approx_quantile(size,0.95) p95, "
            f"approx_quantile(size,0.99) p99 FROM {slim_rel} "
            f"WHERE extension='mkv' GROUP BY lg ORDER BY lg",
            False,
        ),
        # Q4 — distinct-torrent collapse (the thing Tantivy 0.26 cannot do).
        "Q4_distinct_torrents": (
            f"SELECT count(DISTINCT info_hash) torrents FROM {slim_rel} "
            f"WHERE extension='mkv' AND size>1000000000",
            False,
        ),
        # Q5 — two-sided range distinct-torrent (only file-grain can answer).
        "Q5_two_sided_range": (
            f"SELECT count(DISTINCT info_hash) FROM {slim_rel} "
            f"WHERE extension='mkv' AND size BETWEEN 1000000000 AND 2000000000",
            False,
        ),
    }
    if full_rel:
        # Q6 — path FTS (needs the `path` column → full Parquet).
        q["Q6_path_fts_S01E"] = (
            f"SELECT info_hash, file_index, path, size FROM {full_rel} "
            f"WHERE path ILIKE '%S01E%' AND extension='mkv' LIMIT 100",
            True,
        )
    return q


def single_torrent_hydrate(con, slim_rel):
    """Q7 — hydrate one torrent's files (point lookup), like the blob AfterFind path."""
    ih = con.execute(f"SELECT info_hash FROM {slim_rel} LIMIT 1").fetchone()[0]
    return (
        "Q7_single_torrent_hydrate",
        f"SELECT file_index, extension, size FROM {slim_rel} "
        f"WHERE info_hash='{ih}' ORDER BY file_index",
    )


def scanned_rows(con, sql):
    """Pull cardinality + timing from EXPLAIN ANALYZE (best-effort)."""
    try:
        plan = con.execute("EXPLAIN ANALYZE " + sql).fetchall()
        return "\n".join(r[-1] for r in plan)
    except Exception as e:  # noqa: BLE001
        return f"(explain failed: {e})"


def run(args):
    qset = queries("slim", "pfull")
    rows = []
    for threads in (0, 1):  # 0 = all cores
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{args.mem}'")
        con.execute("PRAGMA disable_object_cache")
        if threads:
            con.execute(f"SET threads={threads}")
        con.execute(f"CREATE VIEW slim AS SELECT * FROM read_parquet('{args.slim}')")
        if args.full:
            con.execute(f"CREATE VIEW pfull AS SELECT * FROM read_parquet('{args.full}')")
        allq = dict(qset)
        name, sql = single_torrent_hydrate(con, "slim")
        allq[name] = (sql, False)
        for name, (sql, needs_full) in allq.items():
            if needs_full and not args.full:
                continue
            ts = []
            res0 = None
            for i in range(args.reps):
                t = time.perf_counter()
                res = con.execute(sql).fetchall()
                ts.append(time.perf_counter() - t)
                if i == 0:
                    res0 = res
            warm = ts[1:] or ts
            row = dict(
                query=name,
                threads=("all" if threads == 0 else threads),
                cold_ms=ts[0] * 1e3,
                p50_ms=pct(warm, 50) * 1e3,
                p95_ms=pct(warm, 95) * 1e3,
                p99_ms=pct(warm, 99) * 1e3,
                rows_out=len(res0),
            )
            rows.append(row)
            print(
                f"threads={row['threads']:>3} {name:24s} "
                f"cold={row['cold_ms']:8.1f}ms  p50={row['p50_ms']:8.2f}ms  "
                f"p95={row['p95_ms']:8.2f}ms  p99={row['p99_ms']:8.2f}ms  rows_out={row['rows_out']}"
            )
        con.close()
    if args.csv:
        import csv
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--slim", required=True, help="slim Parquet (info_hash,file_index,extension,size)")
    ap.add_argument("--full", default=None, help="full Parquet (adds path) — enables Q6 path-FTS")
    ap.add_argument("--reps", type=int, default=30)
    ap.add_argument("--mem", default="32GB")
    ap.add_argument("--csv", default=None)
    run(ap.parse_args())
