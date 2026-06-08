-- =====================================================================
-- RUN-3 [gated] — Aggregate + slim-table REAL sizing benchmark
-- Owner: pg-data-bench (team bitmagnet-bench)
--
-- 🚨 RUNS ON THE RESTORED BENCH PG ONLY (ns bitmagnet-bench, deploy/bench-pg
--    on HEL1), NEVER production. Source = the 35GB pre-cutover pg_dump
--    (/var/lib/bitmagnet-backups/bitmagnet-pg-20260605-235749.dump) which
--    still contains BOTH torrent_files (856.8M rows) AND the blobs.
--
-- Runner (sequenced AFTER RUN-1 restore is verified, and scheduled so the
-- heavy GROUP BY/CREATE INDEX do NOT overlap the DuckDB/Tantivy latency runs).
-- Pipe the script in over stdin (no pod-name / kubectl cp needed):
--   cat run3-pg-sizing-bench.sql | ssh ansible@<fsn1-host> \
--     "sudo k3s kubectl -n bitmagnet-bench exec -i deploy/bench-pg -- \
--        psql -U postgres -d bitmagnet -X -P pager=off -f -" \
--     2>&1 | tee run3-results.txt
-- Run it under nohup/background — the heavy steps take ~45-120 min total (see notes).
--
-- Validates:
--   • per-(torrent,ext) aggregate  → +3-5GB / ~55M-row claim (natural vs surrogate)
--   • slim per-file PG table         → +68-92GB REJECT claim (natural vs normalized)
-- Predicted from live grounding (pg-data-bench, 2026-06-07):
--   torrent_files = 856.79M rows, 277GB (120 heap + 157 idx)
--   aggregate ≈ 54.8-55.1M rows; natural-key ≈ 5.5-6.5GB; surrogate ≈ 3-3.5GB
--   slim natural-key ≈ 95-100GB; slim normalized (int4 id) ≈ 68-92GB
-- =====================================================================

\timing on
\set ON_ERROR_STOP on

-- Bench-PG session tuning (safe on a throwaway DB; speeds sorts + index builds)
SET work_mem = '2GB';
SET maintenance_work_mem = '4GB';
SET max_parallel_workers_per_gather = 4;
SET max_parallel_maintenance_workers = 4;
SET synchronous_commit = off;

-- ---------------------------------------------------------------------
-- 0. BASELINE — confirm the restore matches production ground truth
-- ---------------------------------------------------------------------
\echo '== 0. BASELINE: torrent_files on the restored bench DB =='
SELECT
  (SELECT reltuples::bigint FROM pg_class WHERE relname='torrent_files') AS tf_reltuples,
  pg_size_pretty(pg_total_relation_size('torrent_files')) AS tf_total,
  pg_size_pretty(pg_relation_size('torrent_files'))       AS tf_heap,
  pg_size_pretty(pg_indexes_size('torrent_files'))        AS tf_idx;

-- Exact distinct-extension cardinality (decides int2 vs int4 ext_id).
-- HEAVY (full scan) but it's the bench DB. Expect ~1k-? (planner est was 975).
\echo '== 0b. distinct extension cardinality (full scan, bench-only) =='
SELECT count(*) AS distinct_extensions,
       count(*) FILTER (WHERE extension IS NULL) AS has_null_bucket
FROM (SELECT DISTINCT extension FROM torrent_files) d;
-- NOTE: surrogate uses int4 ext_id regardless — due to 8-byte alignment of the
-- int8 size column, int2 vs int4 ext_id saves ZERO bytes/row here. int4 is safe
-- even if the long-tail of garbage extensions exceeds int2's 32,767 ceiling.

-- ---------------------------------------------------------------------
-- 1. DIMENSION TABLES (shared by surrogate aggregate + normalized slim)
-- ---------------------------------------------------------------------
\echo '== 1. build surrogate dimension tables =='
DROP TABLE IF EXISTS dim_torrent;
CREATE TABLE dim_torrent AS
SELECT info_hash, (row_number() OVER ())::int4 AS torrent_id
FROM (SELECT DISTINCT info_hash FROM torrent_files) d;
CREATE UNIQUE INDEX dim_torrent_ih ON dim_torrent(info_hash);
CREATE UNIQUE INDEX dim_torrent_id ON dim_torrent(torrent_id);

DROP TABLE IF EXISTS dim_ext;
CREATE TABLE dim_ext AS
SELECT extension, (row_number() OVER ())::int4 AS ext_id
FROM (SELECT DISTINCT extension FROM torrent_files WHERE extension IS NOT NULL) d;
CREATE UNIQUE INDEX dim_ext_ext ON dim_ext(extension);

SELECT (SELECT count(*) FROM dim_torrent) AS n_torrents,
       (SELECT count(*) FROM dim_ext)     AS n_extensions,
       pg_size_pretty(pg_total_relation_size('dim_torrent')) AS dim_torrent_size,
       pg_size_pretty(pg_total_relation_size('dim_ext'))     AS dim_ext_size;

-- ---------------------------------------------------------------------
-- 2. AGGREGATE — NATURAL KEY  (info_hash, extension) + max/min/count + PK
--    Expect ~55M rows, ~5.5-6.5GB total.
-- ---------------------------------------------------------------------
\echo '== 2. aggregate (natural key) — build + measure =='
DROP TABLE IF EXISTS agg_nat;
CREATE TABLE agg_nat AS
SELECT info_hash, extension, max(size) AS mx, min(size) AS mn, count(*)::int4 AS c
FROM torrent_files
GROUP BY info_hash, extension;
CREATE UNIQUE INDEX agg_nat_pk ON agg_nat(info_hash, extension);
-- realistic query index: WHERE extension='mkv' AND mx > T
CREATE INDEX agg_nat_ext_mx ON agg_nat(extension, mx);
ANALYZE agg_nat;
SELECT count(*) AS rows,
       count(*) FILTER (WHERE extension IS NULL) AS null_ext_rows
FROM agg_nat;
SELECT pg_size_pretty(pg_relation_size('agg_nat'))                AS heap,
       pg_size_pretty(pg_relation_size('agg_nat_pk'))             AS pk_idx,
       pg_size_pretty(pg_relation_size('agg_nat_ext_mx'))         AS ext_mx_idx,
       pg_size_pretty(pg_total_relation_size('agg_nat'))          AS total,
       round(pg_total_relation_size('agg_nat')/1e9::numeric, 2)   AS total_GB,
       round(pg_total_relation_size('agg_nat')::numeric
             / NULLIF((SELECT count(*) FROM agg_nat),0), 1)        AS bytes_per_row;

-- ---------------------------------------------------------------------
-- 3. AGGREGATE — SURROGATE KEY  (int4 torrent_id, int4 ext_id) + covering idx
--    Drops NULL-ext rows (useless for the (ext,size) query). Expect ~3-3.5GB.
-- ---------------------------------------------------------------------
\echo '== 3. aggregate (surrogate key) — build + measure =='
DROP TABLE IF EXISTS agg_surr;
CREATE TABLE agg_surr AS
SELECT dt.torrent_id, de.ext_id, max(f.size) AS mx, min(f.size) AS mn, count(*)::int4 AS c
FROM torrent_files f
JOIN dim_torrent dt USING (info_hash)
JOIN dim_ext      de USING (extension)   -- inner join drops NULL extension
GROUP BY dt.torrent_id, de.ext_id;
-- covering index for "WHERE ext_id=X AND mx > T -> torrent_ids" (exact distinct-torrent)
CREATE INDEX agg_surr_cov ON agg_surr(ext_id, mx) INCLUDE (torrent_id);
ANALYZE agg_surr;
SELECT count(*) AS rows FROM agg_surr;
SELECT pg_size_pretty(pg_relation_size('agg_surr'))               AS heap,
       pg_size_pretty(pg_relation_size('agg_surr_cov'))           AS cov_idx,
       pg_size_pretty(pg_total_relation_size('agg_surr'))         AS total,
       round(pg_total_relation_size('agg_surr')/1e9::numeric, 2)  AS total_GB,
       round(pg_total_relation_size('agg_surr')::numeric
             / NULLIF((SELECT count(*) FROM agg_surr),0), 1)       AS bytes_per_row;

-- ---------------------------------------------------------------------
-- 4. SLIM PER-FILE TABLE — NATURAL KEY  (info_hash, extension, size)
--    Path-stripped but keeps 20-byte info_hash. + btree(ext,size) + btree(ih)
--    Expect ~95-100GB. Built + measured + DROPPED to bound peak disk.
-- ---------------------------------------------------------------------
\echo '== 4. slim per-file table (natural key) — build + measure + drop =='
DROP TABLE IF EXISTS slim_nat;
CREATE TABLE slim_nat AS
SELECT info_hash, extension, size FROM torrent_files;
CREATE INDEX slim_nat_ext_size ON slim_nat(extension, size);
CREATE INDEX slim_nat_ih       ON slim_nat(info_hash);
ANALYZE slim_nat;
SELECT pg_size_pretty(pg_relation_size('slim_nat'))               AS heap,
       pg_size_pretty(pg_relation_size('slim_nat_ext_size'))      AS ext_size_idx,
       pg_size_pretty(pg_relation_size('slim_nat_ih'))            AS ih_idx,
       pg_size_pretty(pg_total_relation_size('slim_nat'))         AS total,
       round(pg_total_relation_size('slim_nat')/1e9::numeric, 2)  AS total_GB,
       round(pg_total_relation_size('slim_nat')::numeric
             / NULLIF((SELECT reltuples FROM pg_class WHERE relname='slim_nat'),0)::numeric, 1) AS bytes_per_row;
DROP TABLE slim_nat;   -- free disk before the next big table

-- ---------------------------------------------------------------------
-- 5. SLIM PER-FILE TABLE — NORMALIZED  (int4 torrent_id, extension, size)
--    The EXACT rejected model in perfile-search-with-blob-design.md:
--    int4 id + ext + size + btree(ext,size) + btree(id). Expect 68-92GB.
-- ---------------------------------------------------------------------
\echo '== 5. slim per-file table (normalized int4 id) — build + measure + drop =='
DROP TABLE IF EXISTS slim_norm;
CREATE TABLE slim_norm AS
SELECT dt.torrent_id, f.extension, f.size
FROM torrent_files f JOIN dim_torrent dt USING (info_hash);
CREATE INDEX slim_norm_ext_size ON slim_norm(extension, size);
CREATE INDEX slim_norm_id       ON slim_norm(torrent_id);
ANALYZE slim_norm;
SELECT pg_size_pretty(pg_relation_size('slim_norm'))              AS heap,
       pg_size_pretty(pg_relation_size('slim_norm_ext_size'))     AS ext_size_idx,
       pg_size_pretty(pg_relation_size('slim_norm_id'))           AS id_idx,
       pg_size_pretty(pg_total_relation_size('slim_norm'))        AS total,
       round(pg_total_relation_size('slim_norm')/1e9::numeric, 2) AS total_GB,
       round(pg_total_relation_size('slim_norm')::numeric
             / NULLIF((SELECT reltuples FROM pg_class WHERE relname='slim_norm'),0)::numeric, 1) AS bytes_per_row;
DROP TABLE slim_norm;

-- ---------------------------------------------------------------------
-- 6. SANITY: a real one-sided distinct-torrent query on the surrogate agg
--    (proves the aggregate answers "torrents with an .mkv > 1GB" exactly)
-- ---------------------------------------------------------------------
\echo '== 6. sanity: distinct-torrent count for a sample ext, mx > 1GB =='
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM agg_surr
WHERE ext_id = (SELECT ext_id FROM dim_ext WHERE extension='mkv')
  AND mx > 1000000000;

-- ---------------------------------------------------------------------
-- 7. Leave agg_nat / agg_surr / dims in place for RUN-5 (GATE feed).
--    RUN-6 teardown drops the whole namespace + PVC.
-- ---------------------------------------------------------------------
\echo '== RUN-3 complete. Aggregate + slim sizing measured. =='
