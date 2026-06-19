# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""EXP-B build (parallel-safe) — carve delta Parquets + a supersession fixture.

🚨 Uses DuckDB `postgres_query()` so the carve runs SERVER-SIDE in PG (uses the
torrents(created_at) index for "recent N" + the torrent_files PK on info_hash),
instead of the postgres SCANNER which full-scans 261GB per delta. Records the
delta-append latency (freshness floor proxy). Read-only on PG.
"""
import os, time, duckdb
S = "/home/ansible/bench-scratch"
# Throwaway local bench PG (NodePort on 127.0.0.1). DSN + password from env.
PG = os.environ.get("BENCH_PG_DSN", "host=127.0.0.1 port=30654 dbname=bitmagnet user=postgres")


def pg(sql):
    # embed a PG SQL string inside a DuckDB string literal (double the quotes)
    return "postgres_query('pgx', '" + sql.replace("'", "''") + "')"


con = duckdb.connect()
con.execute("SET threads=16; SET memory_limit='32GB'; SET preserve_insertion_order=false")
con.execute("INSTALL postgres; LOAD postgres")
con.execute(f"ATTACH '{PG}' AS pgx (TYPE postgres, READ_ONLY)")

# server-side carve: recent-N torrents' files, index-driven in PG
DELTA_SQL = ("SELECT encode(info_hash,'hex') AS info_hash, index AS file_index, extension, size "
             "FROM torrent_files WHERE info_hash IN "
             "(SELECT info_hash FROM torrents ORDER BY created_at DESC LIMIT {N})")

print("EXP-B build — delta-append latency (server-side carve via postgres_query)")
for N in (1000, 10000, 100000):
    out = f"{S}/delta_{N}.parquet"
    if os.path.exists(out) and os.path.getsize(out) > 0:
        print(f"  delta_{N}: exists"); continue
    t = time.perf_counter()
    con.execute(
        f"COPY (SELECT info_hash, file_index::UINTEGER file_index, extension, size::UBIGINT size "
        f"FROM {pg(DELTA_SQL.format(N=N))}) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
    dt = time.perf_counter() - t
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"  delta_{N:>6}: build {dt:6.2f}s  rows={rows:>10,}  size={os.path.getsize(out)/1e6:6.1f}MB "
          f"({N/dt:,.0f} torrents/s)", flush=True)

# ---- Supersession fixture: re-crawl a base torrent with its mkv>1GB removed ----
base = f"{S}/v3_sorted_rg100k.parquet"
victim = con.execute(
    f"SELECT info_hash FROM read_parquet('{base}') WHERE extension='mkv' AND size>1000000000 LIMIT 1"
).fetchone()[0]
print(f"\nsupersession victim = {victim} (has mkv>1GB in base)")
SUP_SQL = (f"SELECT encode(info_hash,'hex') AS info_hash, index AS file_index, extension, size "
           f"FROM torrent_files WHERE info_hash = decode('{victim}','hex') "
           f"AND NOT (extension='mkv' AND size>1000000000)")
con.execute(
    f"COPY (SELECT info_hash, file_index::UINTEGER file_index, extension, size::UBIGINT size "
    f"FROM {pg(SUP_SQL)}) TO '{S}/delta_super.parquet' (FORMAT parquet, COMPRESSION zstd)")
dn = con.execute(f"SELECT count(*) FROM read_parquet('{S}/delta_super.parquet')").fetchone()[0]
mkv_left = con.execute(
    f"SELECT count(*) FROM read_parquet('{S}/delta_super.parquet') WHERE extension='mkv' AND size>1000000000").fetchone()[0]
print(f"  delta_super rows={dn} (mkv>1GB kept={mkv_left}, expect 0)")
open(f"{S}/super_victim.txt", "w").write(victim)
print("EXP-B BUILD COMPLETE", flush=True)
