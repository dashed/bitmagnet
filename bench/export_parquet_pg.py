# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""RUN-2 source export — Parquet straight from `torrent_files` via DuckDB's
postgres scanner (team bitmagnet-bench).

Why not blob_export here: the HEL1 restore dump PREDATES the blob backfill, so
`torrents.files_data` is empty — but `torrent_files` (~879.5M rows) is fully
present. Its `extension` is the STORED generated column
`substring(lower(path) from '[^/.]\\.([a-z0-9]+)$')` — already PATH-derived
(G1-correct), identical to what decoding the blobs would yield. So the Parquet
content (and therefore DuckDB query latency) is identical to the blob path; the
blob-DECODE cost is already grounded separately (0.6–0.94 µs/file) and is NOT
re-measured here.

Produces, with ONE PG scan:
  * files_full.parquet  — info_hash, file_index, path, extension, size
  * files_slim.parquet  — same minus path (built locally from the full file)

Schema matches blob_export exactly (info_hash hex VARCHAR, file_index UINTEGER,
extension VARCHAR nullable, size UBIGINT) so bench_duckdb.py + RUN-4 consume
either interchangeably.

Usage (on the HEL1 box, AFTER the lead signals RUN-3 is done):
  uv run export_parquet_pg.py \
     --attach "host=127.0.0.1 port=5432 dbname=bitmagnet user=postgres password=…" \
     --out-full /scratch/files_full.parquet --out-slim /scratch/files_slim.parquet \
     --threads 16 --mem 32GB
"""
import argparse, os, time
import duckdb

# extension is the path-derived STORED generated column → no derivation needed.
# hex(info_hash) mirrors blob_export's hex info_hash; READ_ONLY attach = never
# writes the bench PG.
SRC = (
    'SELECT lower(hex(info_hash)) AS info_hash, '
    '"index"::UINTEGER AS file_index, '
    'path, '
    'extension, '
    'size::UBIGINT AS size '
    'FROM pg.torrent_files'
)


def gb(path):
    return os.path.getsize(path) / 1e9


def main(a):
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.mem}'")
    con.execute(f"SET threads={a.threads}")
    con.execute("SET preserve_insertion_order=false")  # lower memory on the big COPY
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{a.attach}' AS pg (TYPE postgres, READ_ONLY)")

    n_pg = con.execute("SELECT count(*) FROM pg.torrent_files").fetchone()[0]
    print(f"source torrent_files rows: {n_pg:,}")

    # Single PG scan → full Parquet.
    t = time.perf_counter()
    con.execute(
        f"COPY ({SRC}) TO '{a.out_full}' "
        f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)"
    )
    dt = time.perf_counter() - t
    print(f"full  export: {dt:7.1f}s  {gb(a.out_full):6.2f} GB  -> {a.out_full}")

    # Slim = full minus path, built LOCALLY from the Parquet (no 2nd PG scan).
    t = time.perf_counter()
    con.execute(
        f"COPY (SELECT info_hash, file_index, extension, size "
        f"FROM read_parquet('{a.out_full}')) TO '{a.out_slim}' "
        f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)"
    )
    dt = time.perf_counter() - t
    print(f"slim  export: {dt:7.1f}s  {gb(a.out_slim):6.2f} GB  -> {a.out_slim}")

    n_pq = con.execute(f"SELECT count(*) FROM read_parquet('{a.out_slim}')").fetchone()[0]
    null_ext = con.execute(
        f"SELECT count(*) FILTER (WHERE extension IS NULL) FROM read_parquet('{a.out_slim}')"
    ).fetchone()[0]
    print(f"parquet rows: {n_pq:,}  (PG match: {n_pq == n_pg})  null-ext: {null_ext:,} "
          f"({100*null_ext/max(n_pq,1):.1f}%)")
    print(f"slim density: {os.path.getsize(a.out_slim)/n_pq:.2f} B/row  "
          f"full density: {os.path.getsize(a.out_full)/n_pq:.2f} B/row")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--attach", required=True, help="postgres ATTACH connstr (READ_ONLY)")
    ap.add_argument("--out-full", required=True)
    ap.add_argument("--out-slim", required=True)
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--mem", default="32GB")
    main(ap.parse_args())
