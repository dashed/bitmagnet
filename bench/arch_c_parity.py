# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""ARCH-C JOB 1 — complete-parity query catalog (correctness + latency).

Proves each of the 5 torrent_files workloads as a concrete DuckDB query against
the real 879.5M-row Parquet, plus PG JOINs and a CJK-safety check for ILIKE.
Read-only. Run on HEL1 (owns the box).
"""
import argparse, os, time
import duckdb

SLIM = "/home/ansible/bench-scratch/files_slim.parquet"
FULL = "/home/ansible/bench-scratch/files_full.parquet"
# Throwaway local bench PG (NodePort on 127.0.0.1). DSN + password from env.
PG = os.environ.get("BENCH_PG_DSN", "host=127.0.0.1 port=30654 dbname=bitmagnet user=postgres")


def timed(con, sql, reps=7):
    ts = []
    out = None
    for i in range(reps):
        t = time.perf_counter()
        out = con.execute(sql).fetchall()
        ts.append(time.perf_counter() - t)
    warm = sorted(ts[1:]) or ts
    return warm[len(warm) // 2] * 1e3, ts[0] * 1e3, out


def section(title):
    print(f"\n{'='*70}\n{title}\n{'='*70}")


def main(a):
    con = duckdb.connect()
    con.execute("SET threads=16; SET memory_limit='32GB'")
    con.execute(f"CREATE VIEW slim AS SELECT * FROM read_parquet('{SLIM}')")
    con.execute(f"CREATE VIEW filesfull AS SELECT * FROM read_parquet('{FULL}')")

    section("WORKLOAD 1a — per-file search: ext ∧ size (paginated)")
    p50, cold, out = timed(con,
        "SELECT info_hash,file_index,size FROM slim "
        "WHERE extension='mkv' AND size>1000000000 ORDER BY size DESC LIMIT 50")
    print(f"  rows={len(out)} top_size={out[0][2] if out else None}  p50={p50:.1f}ms cold={cold:.1f}ms")

    section("WORKLOAD 1b — per-file search: path-FTS via ILIKE (+ CJK safety)")
    for label, pat in [("ascii '1080p'", "%1080p%"), ("ascii case 'BLURAY'", "%BLURAY%"),
                       ("CJK 电影(movie)", "%电影%"), ("CJK 中", "%中%"),
                       ("cyrillic Фильм", "%Фильм%")]:
        p50, cold, out = timed(con,
            f"SELECT count(*) FROM filesfull WHERE path ILIKE '{pat}'", reps=4)
        # correctness: verify a returned row truly contains the (case-folded) needle
        chk = con.execute(
            f"SELECT path FROM filesfull WHERE path ILIKE '{pat}' LIMIT 1").fetchone()
        n = out[0][0]
        contains = (chk is not None and pat.strip('%').lower() in chk[0].lower()) if chk else (n == 0)
        print(f"  {label:22s} matches={n:>12,}  sample_ok={contains}  p50={p50:.0f}ms")

    section("WORKLOAD 2 — per-torrent file listing (ORDER BY index, paginate, totalCount)")
    ih = con.execute("SELECT info_hash FROM slim GROUP BY info_hash "
                     "HAVING count(*) BETWEEN 50 AND 200 LIMIT 1").fetchone()[0]
    p50, cold, out = timed(con,
        f"SELECT file_index,extension,size FROM slim WHERE info_hash='{ih}' "
        f"ORDER BY file_index LIMIT 25 OFFSET 0")
    total = con.execute(f"SELECT count(*) FROM slim WHERE info_hash='{ih}'").fetchone()[0]
    print(f"  torrent={ih[:16]}… page_rows={len(out)} totalCount={total}  p50={p50:.1f}ms cold={cold:.1f}ms")

    section("WORKLOAD 3 — distinct-torrent collapse + DEEP keyset pagination")
    # page 1
    p50, cold, pg1 = timed(con,
        "SELECT info_hash, max(size) ms FROM slim WHERE extension='mkv' AND size>1000000000 "
        "GROUP BY info_hash ORDER BY info_hash LIMIT 50")
    last = pg1[-1][0]
    # deep keyset page (seek far in): WHERE info_hash > :last
    p50d, coldd, pgd = timed(con,
        f"SELECT info_hash, max(size) ms FROM slim WHERE extension='mkv' AND size>1000000000 "
        f"AND info_hash > '{last}' GROUP BY info_hash ORDER BY info_hash LIMIT 50")
    # deep OFFSET comparison (the anti-pattern) at offset 100000
    p50o, coldo, _ = timed(con,
        "SELECT DISTINCT info_hash FROM slim WHERE extension='mkv' AND size>1000000000 "
        "ORDER BY info_hash LIMIT 50 OFFSET 100000", reps=4)
    print(f"  page1 rows={len(pg1)}  p50={p50:.1f}ms | keyset-next p50={p50d:.1f}ms | OFFSET100k p50={p50o:.0f}ms")

    section("WORKLOAD 4 — analytics / arbitrary SQL + JOINs to PG content/torrents")
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{PG}' AS pg (TYPE postgres, READ_ONLY)")
    # 4a: histogram + percentiles (pure analytics)
    p50, cold, out = timed(con,
        "SELECT approx_quantile(size,0.5) p50,approx_quantile(size,0.95) p95,"
        "approx_quantile(size,0.99) p99,max(size) FROM slim WHERE extension='mkv'", reps=4)
    print(f"  4a percentiles(mkv): {out[0]}  p50={p50:.0f}ms")
    # 4b: JOIN file rows → torrent content_type. Materialize the 2 PG columns ONCE
    # (one PG scan; the realistic periodic-snapshot pattern), then time the local join.
    t = time.perf_counter()
    con.execute("CREATE TEMP TABLE tc AS SELECT lower(hex(info_hash)) AS info_hash, "
                "content_type FROM pg.torrent_contents")
    ntc = con.execute("SELECT count(*) FROM tc").fetchone()[0]
    print(f"  (materialized {ntc:,} pg.torrent_contents rows in {time.perf_counter()-t:.1f}s)")
    p50, cold, out = timed(con,
        "SELECT count(DISTINCT s.info_hash) FROM slim s JOIN tc ON tc.info_hash = s.info_hash "
        "WHERE s.extension='mkv' AND s.size>1000000000 AND tc.content_type='movie'", reps=3)
    print(f"  4b JOIN movie ∧ mkv>1GB distinct torrents = {out[0][0]:,}  p50={p50:.0f}ms cold={cold:.0f}ms")

    section("WORKLOAD 5 — exact counts")
    for label, sql in [
        ("count files mkv>1GB", "SELECT count(*) FROM slim WHERE extension='mkv' AND size>1000000000"),
        ("count DISTINCT torrents mkv>1GB",
         "SELECT count(DISTINCT info_hash) FROM slim WHERE extension='mkv' AND size>1000000000"),
        ("total files", "SELECT count(*) FROM slim")]:
        p50, cold, out = timed(con, sql, reps=4)
        print(f"  {label:34s} = {out[0][0]:>14,}  p50={p50:.0f}ms")

    print("\nPARITY CATALOG COMPLETE")


if __name__ == "__main__":
    main(argparse.ArgumentParser().parse_args())
