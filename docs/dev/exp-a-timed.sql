-- EXP-A timed runs v3 (lean) — parallelism OFF, single-core, bench pod.
\timing on
\set ON_ERROR_STOP on
SET max_parallel_workers_per_gather = 0;
SET work_mem = '256MB';
DROP TABLE IF EXISTS _lat;
CREATE TEMP TABLE _lat(q text, ms double precision);

-- 10 reps on the interactive-representative (selective) queries
DO $$
DECLARE i int; t0 timestamptz;
BEGIN
  FOR i IN 1..10 LOOP
    t0 := clock_timestamp();
    PERFORM count(*) FROM (SELECT info_hash FROM torrent_contents
      WHERE tsv @@ 'ubuntu'::tsquery ORDER BY ts_rank_cd(tsv,'ubuntu'::tsquery) DESC LIMIT 20) s;
    INSERT INTO _lat VALUES ('1_fts_rare_ranked(7k)', extract(epoch from clock_timestamp()-t0)*1000);

    t0 := clock_timestamp();
    PERFORM count(*) FROM (SELECT info_hash FROM torrent_contents
      WHERE content_type='movie' AND video_resolution='1080p'
      ORDER BY seeders DESC NULLS LAST LIMIT 20) s;
    INSERT INTO _lat VALUES ('5_filter_only', extract(epoch from clock_timestamp()-t0)*1000);

    t0 := clock_timestamp();
    PERFORM count(*) FROM (SELECT content_type, count(*) FROM torrent_contents
      WHERE tsv @@ 'ubuntu'::tsquery GROUP BY content_type) s;
    INSERT INTO _lat VALUES ('6_fts_rare_facet', extract(epoch from clock_timestamp()-t0)*1000);
  END LOOP;
  -- 4 reps on medium term (1.3M matches) — heavier
  FOR i IN 1..4 LOOP
    t0 := clock_timestamp();
    PERFORM count(*) FROM (SELECT info_hash FROM torrent_contents
      WHERE tsv @@ 'flac'::tsquery ORDER BY ts_rank_cd(tsv,'flac'::tsquery) DESC LIMIT 20) s;
    INSERT INTO _lat VALUES ('2_fts_medium_ranked(1.3M)', extract(epoch from clock_timestamp()-t0)*1000);

    t0 := clock_timestamp();
    PERFORM count(*) FROM (SELECT info_hash FROM torrent_contents
      WHERE tsv @@ 'flac'::tsquery LIMIT 20) s;
    INSERT INTO _lat VALUES ('4_fts_medium_norank', extract(epoch from clock_timestamp()-t0)*1000);
  END LOOP;
END$$;

\echo '== A. main-search latency (ms), single-core, warm =='
SELECT q, count(*) reps,
  round(min(ms)::numeric,1) AS min,
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)::numeric,1) AS p50,
  round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)::numeric,1) AS p95,
  round(max(ms)::numeric,1) AS max
FROM _lat GROUP BY q ORDER BY q;

\echo '== A7. BROAD worst-case single shot: x264 (4.28M) ranked top-20 =='
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT info_hash FROM torrent_contents
WHERE tsv @@ 'x264'::tsquery ORDER BY ts_rank_cd(tsv,'x264'::tsquery) DESC LIMIT 20;

\echo '== A8. served shape EXPLAIN (rare+ctype; GIN + ts_rank_cd + NO torrent_files) =='
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT info_hash FROM torrent_contents
WHERE tsv @@ 'ubuntu'::tsquery AND content_type='software'
ORDER BY ts_rank_cd(tsv,'ubuntu'::tsquery) DESC LIMIT 20;

\echo '== B. torrent_contents write cost: 20000-row UPDATE (all 24 idx incl 14GB tsv GIN) =='
DROP TABLE IF EXISTS _wsample;
CREATE TEMP TABLE _wsample AS SELECT id FROM torrent_contents LIMIT 20000;
\echo '-- B1 cold --'
UPDATE torrent_contents SET updated_at = now() WHERE id IN (SELECT id FROM _wsample);
\echo '-- B2 warm --'
UPDATE torrent_contents SET updated_at = now() WHERE id IN (SELECT id FROM _wsample);
\echo '== EXP-A timed runs complete =='
