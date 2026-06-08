# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""EXP-B measure (TIMED — run only with the box turn). base+delta latest-wins:
query latency vs base-only, delta-size→latency curve, supersession correctness.

Latest-wins dedup is TORRENT-granular (a re-crawl replaces the whole fileset):
  RECOMMENDED = predicate-then-anti-join (prune base, anti-join the tiny delta).
  Naive `row_number() OVER (PARTITION BY info_hash)=1` is WRONG (keeps 1 row/torrent).
  `v = max(v) OVER (PARTITION BY info_hash)` is correct but windows ALL rows (slow).
"""
import os, time, duckdb
S = "/home/ansible/bench-scratch"
BASE = f"{S}/v3_sorted_rg100k.parquet"
W = "extension='mkv' AND size>1000000000"


def timed(con, sql, reps=10):
    ts = []; out = None
    for i in range(reps):
        t = time.perf_counter(); out = con.execute(sql).fetchall(); ts.append(time.perf_counter()-t)
    warm = sorted(ts[1:]) or ts
    return warm[len(warm)//2]*1e3, warm[-1]*1e3, out


con = duckdb.connect()
con.execute("SET threads=16; SET memory_limit='32GB'; SET preserve_insertion_order=false")
con.execute(f"CREATE VIEW b AS SELECT info_hash,file_index,extension,size FROM read_parquet('{BASE}')")


def collapse_recommended(delta):
    # predicate-then-anti-join latest-wins distinct-torrent count
    return (f"SELECT count(*) FROM ("
            f"SELECT info_hash FROM b WHERE {W} AND info_hash NOT IN (SELECT info_hash FROM read_parquet('{delta}')) "
            f"UNION SELECT info_hash FROM read_parquet('{delta}') WHERE {W})")


def find_recommended(delta):
    return (f"SELECT info_hash,file_index,size FROM ("
            f"SELECT info_hash,file_index,size FROM b WHERE {W} AND info_hash NOT IN (SELECT info_hash FROM read_parquet('{delta}')) "
            f"UNION ALL SELECT info_hash,file_index,size FROM read_parquet('{delta}') WHERE {W}) LIMIT 1000")


def collapse_window(delta):
    # correct but expensive: window max(v) over ALL rows
    return (f"SELECT count(DISTINCT info_hash) FROM ("
            f"SELECT info_hash,extension,size,v FROM ("
            f"SELECT info_hash,extension,size,0 v FROM b "
            f"UNION ALL SELECT info_hash,extension,size,1 v FROM read_parquet('{delta}')) "
            f"QUALIFY v = max(v) OVER (PARTITION BY info_hash)) WHERE {W}")


print("="*86)
print("BASE-ONLY baselines")
p50,p95,out = timed(con, f"SELECT count(DISTINCT info_hash) FROM b WHERE {W}")
base_count = out[0][0]
print(f"  collapse base-only          p50={p50:8.1f}ms  count={base_count:,}")
p50,p95,_ = timed(con, f"SELECT info_hash,file_index,size FROM b WHERE {W} LIMIT 1000")
print(f"  find base-only              p50={p50:8.1f}ms")

print("="*86)
print("DELTA-SIZE → LATENCY CURVE (latest-wins, recommended anti-join pattern)")
for N in (1000, 10000, 100000):
    d = f"{S}/delta_{N}.parquet"
    if not os.path.exists(d):
        print(f"  delta_{N}: MISSING"); continue
    p50c,p95c,outc = timed(con, collapse_recommended(d))
    p50f,p95f,_ = timed(con, find_recommended(d))
    print(f"  +delta_{N:>6}  collapse p50={p50c:8.1f}ms p95={p95c:8.1f}ms (count={outc[0][0]:,}) | "
          f"find p50={p50f:8.1f}ms p95={p95f:8.1f}ms")

print("="*86)
print("DEDUP PATTERN COST (delta_100000): anti-join vs window-max(v)")
d = f"{S}/delta_100000.parquet"
p50a,_,_ = timed(con, collapse_recommended(d), reps=6)
p50w,_,_ = timed(con, collapse_window(d), reps=4)
print(f"  anti-join(predicate-first) p50={p50a:8.1f}ms   window-max(v)-over-all p50={p50w:8.1f}ms")

print("="*86)
print("SUPERSESSION CORRECTNESS (re-crawl removes the victim's mkv>1GB)")
victim = open(f"{S}/super_victim.txt").read().strip()
sup = f"{S}/delta_super.parquet"
in_base = con.execute(f"SELECT count(*)>0 FROM b WHERE info_hash='{victim}' AND {W}").fetchone()[0]
# base+delta_super collapse: is the victim still counted?
victim_in_result = con.execute(
    f"SELECT count(*)>0 FROM (SELECT info_hash FROM b WHERE {W} AND info_hash NOT IN (SELECT info_hash FROM read_parquet('{sup}')) "
    f"UNION SELECT info_hash FROM read_parquet('{sup}') WHERE {W}) WHERE info_hash='{victim}'").fetchone()[0]
new_count = con.execute(collapse_recommended(sup)).fetchone()[0]
# fileset for victim after dedup should equal the DELTA's rows, not base's
base_fc = con.execute(f"SELECT count(*) FROM b WHERE info_hash='{victim}'").fetchone()[0]
delta_fc = con.execute(f"SELECT count(*) FROM read_parquet('{sup}') WHERE info_hash='{victim}'").fetchone()[0]
print(f"  victim in base mkv>1GB set: {in_base} (expect True)")
print(f"  victim in base+delta result: {victim_in_result} (expect FALSE — re-crawl removed its mkv)")
print(f"  collapse count base-only={base_count:,}  base+delta_super={new_count:,}  Δ={base_count-new_count} (expect 1)")
print(f"  victim fileset: base={base_fc} rows, delta={delta_fc} rows → latest-wins must serve DELTA's {delta_fc}")
ok = (in_base and not victim_in_result and base_count - new_count == 1)
print(f"  SUPERSESSION CORRECT = {ok}")
print("EXP-B MEASURE COMPLETE")
