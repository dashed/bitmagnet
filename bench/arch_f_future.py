# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""ARCH-F spot-check — prove representative FUTURE queries are just new SQL on the
existing Parquet (no re-index). Warm p50 over reps. Read-only, HEL1."""
import time, duckdb
S = "/home/ansible/bench-scratch"


def timed(con, sql, reps=8):
    ts = []
    out = None
    for i in range(reps):
        t = time.perf_counter(); out = con.execute(sql).fetchall(); ts.append(time.perf_counter()-t)
    warm = sorted(ts[1:]) or ts
    return warm[len(warm)//2]*1e3, warm[-1]*1e3, ts[0]*1e3, len(out)


con = duckdb.connect()
con.execute("SET threads=16; SET memory_limit='32GB'; SET preserve_insertion_order=false")
con.execute(f"CREATE VIEW files AS SELECT * FROM read_parquet('{S}/files_slim.parquet')")
con.execute(f"CREATE VIEW filesfull AS SELECT * FROM read_parquet('{S}/files_full.parquet')")

print("ARCH-F future-query spot-checks (warm p50 / p95 / cold, full 879M-row corpus)")

q1 = ("season-pack: ≥8 mkv >300MB per torrent",
      "SELECT info_hash FROM files WHERE extension='mkv' AND size>300000000 "
      "GROUP BY info_hash HAVING count(*)>=8 LIMIT 50")
q2 = ("cross-torrent dup (path,size) >1 torrent [path→full parquet]",
      "SELECT path,size,count(DISTINCT info_hash) n FROM filesfull "
      "GROUP BY path,size HAVING n>1 ORDER BY n DESC LIMIT 50")
q3 = ("faceting: ext → count + distinct torrents",
      "SELECT extension,count(*),count(DISTINCT info_hash) FROM files GROUP BY extension ORDER BY 2 DESC LIMIT 30")

for label, sql in [q1, q2, q3]:
    p50, p95, cold, n = timed(con, sql, reps=6)
    print(f"  {label:52s} p50={p50:8.0f}ms p95={p95:8.0f}ms cold={cold:8.0f}ms rows={n}")

print("ARCH-F COMPLETE")
