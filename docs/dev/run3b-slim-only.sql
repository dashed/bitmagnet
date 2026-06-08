-- RUN-3b — slim-table re-measure (steps 4-6 only). dim_torrent still present.
-- The first RUN-3 client (ssh) dropped after the slim tables printed server-side;
-- they were auto-dropped, so this rebuilds + re-measures them to capture the totals.
\timing on
\set ON_ERROR_STOP on
SET work_mem = '2GB';
SET maintenance_work_mem = '4GB';
SET max_parallel_workers_per_gather = 4;
SET max_parallel_maintenance_workers = 4;
SET synchronous_commit = off;

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
DROP TABLE slim_nat;

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

\echo '== 6. sanity: distinct-torrent count for mkv, mx > 1GB (on agg_surr) =='
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM agg_surr
WHERE ext_id = (SELECT ext_id FROM dim_ext WHERE extension='mkv')
  AND mx > 1000000000;

\echo '== RUN-3b complete =='
