# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""PSX-D1 / R5 Stage-3 parity: blob-sourced Parquet == torrent_files-sourced Parquet.

Order-INDEPENDENT checksum (SUM of per-row hashes) — avoids an 856M-row sort that
string_agg(ORDER BY) would force. Compares row count, distinct info_hash, sum(size),
null-ext count, and a tuple checksum over (info_hash, file_index, extension, size)
[+ path for --full]. Any mismatch -> anti-join localizer (first 50 divergent rows).

usage:
  uv run d1_parity.py <blob.parquet> <tf.parquet> [--full]
"""
import sys

import duckdb

full = "--full" in sys.argv
args = [a for a in sys.argv[1:] if not a.startswith("--")]
blob_pq, tf_pq = args[0], args[1]

con = duckdb.connect()
con.execute("PRAGMA threads=8;")

# tuple expression — order-independent SUM(hash(...)) as HUGEINT to avoid overflow.
tuple_expr = "info_hash || '|' || file_index || '|' || coalesce(extension,'∅') || '|' || size"
if full:
    tuple_expr += " || '|' || path"


def stats(path: str) -> dict:
    q = f"""
      SELECT count(*)                              AS rows,
             count(DISTINCT info_hash)             AS distinct_ih,
             sum(size)                             AS sum_size,
             count(*) FILTER (WHERE extension IS NULL) AS null_ext,
             sum(hash({tuple_expr})::HUGEINT)      AS checksum
      FROM read_parquet('{path}')
    """
    r = con.execute(q).fetchone()
    return dict(rows=r[0], distinct_ih=r[1], sum_size=r[2], null_ext=r[3], checksum=r[4])


print(f"== D1 parity (full={full}) ==")
print(f"  blob: {blob_pq}")
print(f"  tf  : {tf_pq}")
b = stats(blob_pq)
t = stats(tf_pq)
print(f"\n  {'metric':<14} {'blob':>22} {'torrent_files':>22} {'match':>7}")
ok = True
for k in ("rows", "distinct_ih", "sum_size", "null_ext", "checksum"):
    m = b[k] == t[k]
    ok = ok and m
    print(f"  {k:<14} {str(b[k]):>22} {str(t[k]):>22} {'OK' if m else 'DIFF':>7}")

print(f"\n  VERDICT: {'PARITY ✅ (identical)' if ok else 'MISMATCH ❌'}")

if not ok:
    print("\n  -- anti-join localizer (blob rows not in torrent_files, first 50) --")
    key = "info_hash, file_index, extension, size" + (", path" if full else "")
    rows = con.execute(
        f"""SELECT {key} FROM read_parquet('{blob_pq}')
            EXCEPT SELECT {key} FROM read_parquet('{tf_pq}') LIMIT 50"""
    ).fetchall()
    for row in rows:
        print("   blob-only:", row)
    rows2 = con.execute(
        f"""SELECT {key} FROM read_parquet('{tf_pq}')
            EXCEPT SELECT {key} FROM read_parquet('{blob_pq}') LIMIT 50"""
    ).fetchall()
    for row in rows2:
        print("   tf-only:  ", row)
    sys.exit(1)
