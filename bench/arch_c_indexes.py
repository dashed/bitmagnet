# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""ARCH-C index question — "can we ADD indexes (disk cost) to DuckDB-on-Parquet
to improve things?" Empirically answers 3 levers the matrix didn't cover:
  1. ART CREATE INDEX (native table) for ext∧size range AND info_hash point —
     does the optimizer USE it? (EXPLAIN) + added disk.
  2. Native rollup TABLES (per-ext, size-histogram, per-(torrent,ext)) —
     do they turn 1.2-1.3s GROUP BY/DISTINCT into <50ms? + disk.
  3. FTS (BM25 inverted index) on path — does it make unbounded path-FTS fast?
     build time, disk/row (extrapolated to 879M), CJK behavior.
Read-only on the source Parquet; writes throwaway .duckdb artifacts to scratch.
"""
import os, time, duckdb
S = "/home/ansible/bench-scratch"
SLIM = f"{S}/files_slim.parquet"
FULL = f"{S}/files_full.parquet"


def timed(con, sql, reps=8):
    ts = []
    out = None
    for i in range(reps):
        t = time.perf_counter(); out = con.execute(sql).fetchall(); ts.append(time.perf_counter()-t)
    warm = sorted(ts[1:]) or ts
    return warm[len(warm)//2]*1e3, warm[-1]*1e3, ts[0]*1e3, out


def gb(p):
    return os.path.getsize(p)/1e9 if os.path.exists(p) else 0


print("="*92)
print("LEVER 1 — ART CREATE INDEX on a native table (range vs point) + EXPLAIN + disk")
print("="*92)
# native table w/ ART on extension AND info_hash (point) — measure if optimizer uses them
p_idx = f"{S}/idx_native.duckdb"
if not os.path.exists(p_idx):
    n = duckdb.connect(p_idx); n.execute("SET threads=16; SET memory_limit='32GB'")
    n.execute(f"CREATE TABLE files AS SELECT * FROM read_parquet('{SLIM}')")
    sz_noidx = gb(p_idx)
    t=time.perf_counter(); n.execute("CREATE INDEX idx_ext_size ON files(extension,size)")
    t_ext=time.perf_counter()-t
    t=time.perf_counter(); n.execute("CREATE INDEX idx_ih ON files(info_hash)")
    t_ih=time.perf_counter()-t
    n.close()
    print(f"  build: native table+2 ART indexes; ext_size idx {t_ext:.0f}s, info_hash idx {t_ih:.0f}s")
n = duckdb.connect(p_idx, read_only=True); n.execute("SET threads=16; SET memory_limit='32GB'")
print(f"  idx_native.duckdb total size = {gb(p_idx):.2f} GB (table + 2 ART indexes)")
# does EXPLAIN show an index scan for the range filter?
plan = "\n".join(r[-1] for r in n.execute(
    "EXPLAIN SELECT count(*) FROM files WHERE extension='mkv' AND size>1000000000").fetchall())
print(f"  ext∧size uses_index_scan = {'INDEX_SCAN' in plan.upper() or 'ART' in plan.upper()} "
      f"(seq_scan={'SEQ_SCAN' in plan.upper()})")
p50,p95,cold,_ = timed(n, "SELECT count(*) FROM files WHERE extension='mkv' AND size>1000000000")
print(f"  range ext∧size count: p50={p50:.0f}ms p95={p95:.0f}ms (vs ~1270ms raw parquet)")
ih = n.execute("SELECT info_hash FROM files LIMIT 1").fetchone()[0]
planp = "\n".join(r[-1] for r in n.execute(
    f"EXPLAIN SELECT * FROM files WHERE info_hash='{ih}'").fetchall())
p50,p95,cold,_ = timed(n, f"SELECT * FROM files WHERE info_hash='{ih}'")
print(f"  info_hash POINT lookup uses_index = {'INDEX' in planp.upper() and 'SEQ_SCAN' not in planp.upper()} "
      f"p50={p50:.1f}ms p95={p95:.1f}ms")
n.close()

print("\n"+"="*92)
print("LEVER 2 — native rollup TABLES (the DuckDB analog of the PG aggregate) + disk")
print("="*92)
p_roll = f"{S}/rollups.duckdb"
if not os.path.exists(p_roll):
    r = duckdb.connect(p_roll); r.execute("SET threads=16; SET memory_limit='32GB'")
    r.execute(f"CREATE TABLE agg_ext AS SELECT extension,count(*) n,sum(size) bytes,min(size) mn,max(size) mx "
              f"FROM read_parquet('{SLIM}') GROUP BY extension")
    r.execute(f"CREATE TABLE size_hist AS SELECT extension, floor(log2(greatest(size,1)))::int lg, count(*) n "
              f"FROM read_parquet('{SLIM}') GROUP BY extension, lg")
    r.execute(f"CREATE TABLE agg_torrent_ext AS SELECT info_hash, extension, max(size) max_size, min(size) min_size, "
              f"count(*) fc FROM read_parquet('{SLIM}') GROUP BY info_hash, extension")
    r.close()
r = duckdb.connect(p_roll, read_only=True); r.execute("SET threads=16; SET memory_limit='32GB'")
sizes = {t: (n_, sz) for t,n_,sz in r.execute(
    "SELECT table_name, estimated_size, estimated_size FROM duckdb_tables()").fetchall()}
print(f"  rollups.duckdb total = {gb(p_roll):.3f} GB; rows: "
      f"agg_ext={r.execute('SELECT count(*) FROM agg_ext').fetchone()[0]:,}, "
      f"size_hist={r.execute('SELECT count(*) FROM size_hist').fetchone()[0]:,}, "
      f"agg_torrent_ext={r.execute('SELECT count(*) FROM agg_torrent_ext').fetchone()[0]:,}")
for label, sql in [
    ("GROUP BY ext (was 1.28s) → agg_ext", "SELECT extension,n,bytes FROM agg_ext ORDER BY n DESC LIMIT 30"),
    ("size histogram (was 1.15s) → size_hist", "SELECT lg,sum(n) FROM size_hist WHERE extension='mkv' GROUP BY lg ORDER BY lg"),
    ("distinct-torrent collapse one-sided (was 1.27s) → agg_torrent_ext",
     "SELECT count(*) FROM agg_torrent_ext WHERE extension='mkv' AND max_size>1000000000"),
    ("two-sided needs file-grain — agg can't (NOTE)", "SELECT 1")]:
    p50,p95,cold,_ = timed(r, sql)
    print(f"  {label:56s} p50={p50:7.2f}ms p95={p95:7.2f}ms")
r.close()

print("\n"+"="*92)
print("LEVER 3 — FTS (BM25 inverted index) on path: build cost / disk / latency / CJK")
print("="*92)
SAMPLE = 20_000_000
p_fts = f"{S}/fts_sample.duckdb"
con = duckdb.connect(p_fts); con.execute("SET threads=16; SET memory_limit='32GB'")
con.execute("INSTALL fts; LOAD fts")
if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='docs'").fetchone()[0]==0:
    con.execute(f"CREATE TABLE docs AS SELECT row_number() OVER () AS id, path "
                f"FROM read_parquet('{FULL}') USING SAMPLE {SAMPLE} ROWS")
    base = gb(p_fts)
    t=time.perf_counter()
    con.execute("PRAGMA create_fts_index('docs','id','path', overwrite=1)")
    t_build=time.perf_counter()-t
    full=gb(p_fts)
    print(f"  built BM25 over {SAMPLE:,} paths in {t_build:.0f}s ({SAMPLE/t_build/1e6:.2f}M/s); "
          f"db {base:.2f}→{full:.2f} GB (index ≈ {full-base:.2f} GB)")
    print(f"  EXTRAPOLATED to 879.5M paths: build ≈ {879.5e6/(SAMPLE/t_build)/60:.0f} min, "
          f"index ≈ {(full-base)*879.5e6/SAMPLE:.1f} GB")
# query latency: BM25 match for an ASCII token + CJK token
for label, term in [("ascii '1080p'","1080p"), ("ascii 'bluray'","bluray"), ("CJK '电影'","电影")]:
    try:
        p50,p95,cold,out = timed(con,
            f"SELECT id FROM (SELECT id, fts_main_docs.match_bm25(id,'{term}') AS score FROM docs) "
            f"WHERE score IS NOT NULL ORDER BY score DESC LIMIT 50", reps=5)
        print(f"  BM25 query {label:16s} p50={p50:.1f}ms hits={len(out)}")
    except Exception as e:
        print(f"  BM25 query {label:16s} ERROR {str(e)[:80]}")
con.close()
print("\nINDEX-QUESTION COMPLETE")
