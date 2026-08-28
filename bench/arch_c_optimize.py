# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""ARCH-C JOB 2 — empirical latency optimization of the heavy DuckDB queries.

Builds layout/index/pre-agg variants from the real 879.5M-row slim Parquet on
HEL1 scratch (idempotent), then measures before→after p50/p95 for the heavy
queries. Goal: can ext∧size + collapse + GROUP BY reach <50ms?

Levers (grounded in duckdb/extension/parquet): sort→row-group min/max + bloom
pruning; write_bloom_filter on/off; row_group_size; hive PARTITION_BY;
native .duckdb table w/ zonemaps + ART index; pre-aggregated per-(ext) and
per-(torrent,ext) tables.
"""
import argparse, os, time
import duckdb

S = "/home/ansible/bench-scratch"
SLIM = f"{S}/files_slim.parquet"

# Coarse file-category bucket for V4 (the ARCH-A "partition by ~5 dirs" idea).
FILECAT = (
    "CASE WHEN extension IN ('mkv','mp4','avi','m4v','mov','wmv','flv','mpg','mpeg','ts','webm','vob','m2ts') THEN 'video' "
    "WHEN extension IN ('mp3','flac','aac','m4a','ogg','wav','wma','opus') THEN 'audio' "
    "WHEN extension IN ('srt','sub','ass','ssa','idx','vtt') THEN 'subtitle' "
    "WHEN extension IN ('zip','rar','7z','gz','tar','iso') THEN 'archive' "
    "WHEN extension IN ('jpg','jpeg','png','gif','bmp','webp') THEN 'image' "
    "ELSE 'other' END"
)


def build(con):
    """Create all variant artifacts (idempotent)."""
    def need(p):
        return not os.path.exists(p)

    # V1 sorted (ext,size), bloom ON (default), default row groups
    if need(f"{S}/v1_sorted.parquet"):
        log("build V1 sorted(ext,size) bloom-on")
        con.execute(f"COPY (SELECT * FROM read_parquet('{SLIM}') ORDER BY extension, size) "
                    f"TO '{S}/v1_sorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)")
    # V2 sorted, bloom OFF (isolate bloom vs min/max)
    if need(f"{S}/v2_sorted_nobloom.parquet"):
        log("build V2 sorted bloom-OFF")
        con.execute(f"COPY (SELECT * FROM read_parquet('{S}/v1_sorted.parquet')) "
                    f"TO '{S}/v2_sorted_nobloom.parquet' (FORMAT parquet, COMPRESSION zstd, "
                    f"ROW_GROUP_SIZE 1000000, WRITE_BLOOM_FILTER false)")
    # V3 sorted, small row groups (finer pruning)
    if need(f"{S}/v3_sorted_rg100k.parquet"):
        log("build V3 sorted rg=100k")
        con.execute(f"COPY (SELECT * FROM read_parquet('{S}/v1_sorted.parquet')) "
                    f"TO '{S}/v3_sorted_rg100k.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
    # V4 hive partition by a COARSE derived file_category (~6 dirs) — NOT by raw
    # extension (47,628 distinct → 47k tiny files / metadata blowup, per ARCH-A).
    if need(f"{S}/v4_hive"):
        log("build V4 hive partition_by file_category (coarse)")
        con.execute(f"COPY (SELECT *, {FILECAT} AS file_category FROM read_parquet('{S}/v1_sorted.parquet')) "
                    f"TO '{S}/v4_hive' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (file_category), OVERWRITE_OR_IGNORE)")
    # V8 sorted, DEFAULT row-group size (122,880) — the row-group sweep point
    if need(f"{S}/v8_sorted_rgdefault.parquet"):
        log("build V8 sorted rg=default(122880)")
        con.execute(f"COPY (SELECT * FROM read_parquet('{S}/v1_sorted.parquet')) "
                    f"TO '{S}/v8_sorted_rgdefault.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)")
    # V5 native .duckdb table sorted + ART index on extension
    if need(f"{S}/v5_native.duckdb"):
        log("build V5 native table + ART index")
        n = duckdb.connect(f"{S}/v5_native.duckdb")
        n.execute("SET threads=16; SET memory_limit='32GB'")
        n.execute(f"CREATE TABLE files AS SELECT * FROM read_parquet('{S}/v1_sorted.parquet')")
        n.execute("CREATE INDEX idx_ext ON files(extension)")
        n.close()
    # V6 pre-agg per-(ext)
    if need(f"{S}/v6_agg_ext.parquet"):
        log("build V6 preagg per-(ext)")
        con.execute(f"COPY (SELECT extension, count(*) n, sum(size) bytes, min(size) mn, max(size) mx "
                    f"FROM read_parquet('{SLIM}') GROUP BY extension) TO '{S}/v6_agg_ext.parquet' (FORMAT parquet)")
    # V7 pre-agg per-(torrent,ext), sorted (ext,max_size) for pruning
    if need(f"{S}/v7_agg_torrent_ext.parquet"):
        log("build V7 preagg per-(torrent,ext)")
        con.execute(f"COPY (SELECT info_hash, extension, max(size) max_size, min(size) min_size, count(*) fc "
                    f"FROM read_parquet('{SLIM}') GROUP BY info_hash, extension ORDER BY extension, max_size) "
                    f"TO '{S}/v7_agg_torrent_ext.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 200000)")


def log(m):
    print(f"[build] {m}", flush=True)


def timed(con, sql, reps=12):
    ts = []
    for i in range(reps):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        ts.append(time.perf_counter() - t)
    warm = sorted(ts[1:]) or ts
    p50 = warm[len(warm)//2]; p95 = warm[min(len(warm)-1, int(len(warm)*0.95))]
    return p50*1e3, p95*1e3, ts[0]*1e3


def rel(con, name, parquet):
    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{parquet}')")


def main(a):
    con = duckdb.connect()
    con.execute("SET threads=16; SET memory_limit='32GB'")
    build(con)

    # register views
    rel(con, "v0", SLIM)
    rel(con, "v1", f"{S}/v1_sorted.parquet")
    rel(con, "v2", f"{S}/v2_sorted_nobloom.parquet")
    rel(con, "v3", f"{S}/v3_sorted_rg100k.parquet")
    rel(con, "v8", f"{S}/v8_sorted_rgdefault.parquet")
    con.execute(f"CREATE OR REPLACE VIEW v4 AS SELECT * FROM read_parquet('{S}/v4_hive/**/*.parquet', hive_partitioning=true)")
    rel(con, "v6", f"{S}/v6_agg_ext.parquet")
    rel(con, "v7", f"{S}/v7_agg_torrent_ext.parquet")
    nat = duckdb.connect(f"{S}/v5_native.duckdb", read_only=True)
    nat.execute("SET threads=16; SET memory_limit='32GB'")

    print(f"\n{'='*92}\nSIZES (GB):", flush=True)
    for p in ["files_slim.parquet","v1_sorted.parquet","v2_sorted_nobloom.parquet","v3_sorted_rg100k.parquet",
              "v8_sorted_rgdefault.parquet","v6_agg_ext.parquet","v7_agg_torrent_ext.parquet","v5_native.duckdb"]:
        fp=f"{S}/{p}"
        if os.path.exists(fp): print(f"  {p:30s} {os.path.getsize(fp)/1e9:7.3f}")
    hive=f"{S}/v4_hive"
    if os.path.exists(hive):
        tot=sum(os.path.getsize(os.path.join(d,f)) for d,_,fs in os.walk(hive) for f in fs)
        print(f"  {'v4_hive (dir)':30s} {tot/1e9:7.3f}")
    # hive dir size + row counts of aggs
    for name,v in [("v6 rows","v6"),("v7 rows","v7")]:
        print(f"  {name:30s} {con.execute(f'SELECT count(*) FROM {v}').fetchone()[0]:,}")

    def row(label, p50, p95, cold, note=""):
        print(f"  {label:46s} p50={p50:8.1f}ms p95={p95:8.1f}ms cold={cold:8.1f}ms {note}", flush=True)

    WHERE = "extension='mkv' AND size>1000000000"

    # ---- Path-FTS 2x2 (completes the {common,rare}×{LIMIT,COUNT} grid for ARCH-A) ----
    con.execute(f"CREATE OR REPLACE VIEW pf AS SELECT * FROM read_parquet('{S}/files_full.parquet')")
    print(f"\n{'='*92}\nP. PATH-FTS ILIKE 2x2 (leading-wildcard = unprunable, full 11.7GB path scan)")
    pf_cases = [
        ("common '1080p' LIMIT100", "SELECT path FROM pf WHERE path ILIKE '%1080p%' LIMIT 100"),
        ("common '1080p' COUNT",    "SELECT count(*) FROM pf WHERE path ILIKE '%1080p%'"),
        ("rare  'zzqxwv' LIMIT100",  "SELECT path FROM pf WHERE path ILIKE '%zzqxwv%' LIMIT 100"),
        ("rare  'zzqxwv' COUNT",     "SELECT count(*) FROM pf WHERE path ILIKE '%zzqxwv%'"),
    ]
    for label, sql in pf_cases:
        row(label, *timed(con, sql, reps=4))

    print(f"\n{'='*92}\nA. ext∧size PAGINATED FIND (LIMIT 1000)  [common ext = mkv]")
    for v in ["v0","v1","v2","v3","v8"]:
        row(f"{v}", *timed(con, f"SELECT info_hash,file_index,size FROM {v} WHERE {WHERE} LIMIT 1000"))
    row("v4_hive(file_category=video)", *timed(con,
        f"SELECT info_hash,file_index,size FROM v4 WHERE file_category='video' AND {WHERE} LIMIT 1000"))
    row("v5_native(ART)", *timed(nat, f"SELECT info_hash,file_index,size FROM files WHERE {WHERE} LIMIT 1000"))

    print(f"\n{'='*92}\nA2. ext∧size FIND, RARE ext (bloom isolation: v1 bloom-on vs v2 bloom-off)")
    RARE = "extension='epub' AND size>1000000"
    for v in ["v0","v1","v2","v3","v8"]:
        row(f"{v}", *timed(con, f"SELECT info_hash,file_index,size FROM {v} WHERE {RARE} LIMIT 1000"))

    print(f"\n{'='*92}\nB. ext∧size DISTINCT-TORRENT COLLAPSE (count distinct)")
    for v in ["v0","v1","v3"]:
        row(f"{v}", *timed(con, f"SELECT count(DISTINCT info_hash) FROM {v} WHERE {WHERE}"))
    row("v5_native", *timed(nat, f"SELECT count(DISTINCT info_hash) FROM files WHERE {WHERE}"))
    row("v7_preagg(torrent,ext) max_size>1e9", *timed(con,
        "SELECT count(*) FROM v7 WHERE extension='mkv' AND max_size>1000000000"),
        "<-- one-sided exact")

    print(f"\n{'='*92}\nC. GROUP BY extension (full histogram)")
    row("v0 scan", *timed(con, "SELECT extension,count(*),sum(size) FROM v0 GROUP BY extension"))
    row("v1 scan(sorted)", *timed(con, "SELECT extension,count(*),sum(size) FROM v1 GROUP BY extension"))
    row("v6_preagg(ext) lookup", *timed(con, "SELECT extension,n,bytes FROM v6 ORDER BY n DESC"), "<-- tiny table")

    print(f"\n{'='*92}\nD. TWO-SIDED RANGE distinct torrents (mkv in [1,2]GB)")
    R2 = "extension='mkv' AND size BETWEEN 1000000000 AND 2000000000"
    for v in ["v0","v1","v3"]:
        row(f"{v}", *timed(con, f"SELECT count(DISTINCT info_hash) FROM {v} WHERE {R2}"))
    row("v5_native", *timed(nat, f"SELECT count(DISTINCT info_hash) FROM files WHERE {R2}"))

    print(f"\n{'='*92}\nE. EXACT COUNT files (ext∧size)")
    for v in ["v0","v1","v3"]:
        row(f"{v}", *timed(con, f"SELECT count(*) FROM {v} WHERE {WHERE}"))
    row("v7_preagg sum(fc)", *timed(con,
        "SELECT sum(fc) FROM v7 WHERE extension='mkv' AND max_size>1000000000"),
        "approx (counts files in torrents w/ a >1GB mkv, not files>1GB)")

    print("\nOPTIMIZATION MATRIX COMPLETE", flush=True)


if __name__ == "__main__":
    main(argparse.ArgumentParser().parse_args())
