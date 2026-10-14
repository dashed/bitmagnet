-- Normalized database image shared by the Go oracle and the Rust replay.
--
-- Every column of every table the processor can touch is kept verbatim except
-- three time-dependent ones. A created_at/updated_at/run_after value at or after
-- 2020-01-01 was written by the run under test (every seed timestamp predates
-- 2016) and becomes the string "fresh"; a seeded value is kept exactly, so an
-- upsert that must preserve created_at is still checked. queue_jobs.id is a
-- random UUID and is dropped; run_after <= created_at (the NOTIFY trigger's
-- immediacy condition) is kept as a boolean. to_jsonb renders both SQL NULL
-- and JSON null as null, so the jsonb columns of torrent_contents also carry
-- an explicit IS NULL flag.
WITH norm(tbl, sort_key, row_json) AS (
  SELECT 'queue_jobs', q.fingerprint || q.status::text,
         to_jsonb(q) - 'id' || jsonb_build_object('run_after_le_created_at', q.run_after <= q.created_at)
    FROM queue_jobs q
  UNION ALL
  SELECT 'torrent_contents', tc.id,
         to_jsonb(tc) || jsonb_build_object('episodes_is_sql_null', tc.episodes IS NULL,
                                            'languages_is_sql_null', tc.languages IS NULL)
    FROM torrent_contents tc
  UNION ALL
  SELECT 'torrent_tags', encode(tt.info_hash, 'hex') || tt.name, to_jsonb(tt) FROM torrent_tags tt
  UNION ALL
  SELECT 'content', c.type || ':' || c.source || ':' || c.id, to_jsonb(c) FROM content c
  UNION ALL
  SELECT 'content_attributes',
         ca.content_type || ':' || ca.content_source || ':' || ca.content_id || ':' || ca.source || ':' || ca.key,
         to_jsonb(ca)
    FROM content_attributes ca
  UNION ALL
  SELECT 'content_collections_content',
         ccc.content_type || ':' || ccc.content_source || ':' || ccc.content_id || ':'
           || ccc.content_collection_type || ':' || ccc.content_collection_source || ':' || ccc.content_collection_id,
         to_jsonb(ccc)
    FROM content_collections_content ccc
  UNION ALL
  SELECT 'torrents', encode(t.info_hash, 'hex'), jsonb_build_object('info_hash', encode(t.info_hash, 'hex'))
    FROM torrents t
  UNION ALL
  SELECT 'deleted_torrents', encode(d.info_hash, 'hex'), jsonb_build_object('info_hash', encode(d.info_hash, 'hex'))
    FROM deleted_torrents d
),
cleaned AS (
  SELECT n.tbl, n.sort_key,
         (SELECT jsonb_object_agg(
                   e.key,
                   CASE
                     WHEN e.key IN ('created_at', 'updated_at', 'run_after')
                          AND jsonb_typeof(e.value) = 'string'
                          AND (e.value #>> '{}')::timestamptz >= '2020-01-01T00:00:00Z'
                       THEN to_jsonb('fresh'::text)
                     ELSE e.value
                   END)
            FROM jsonb_each(n.row_json) e) AS row_json
    FROM norm n
)
SELECT (jsonb_build_object(
  'queue_jobs', '[]'::jsonb, 'torrent_contents', '[]'::jsonb, 'torrent_tags', '[]'::jsonb,
  'content', '[]'::jsonb, 'content_attributes', '[]'::jsonb,
  'content_collections_content', '[]'::jsonb, 'torrents', '[]'::jsonb, 'deleted_torrents', '[]'::jsonb
) || COALESCE(
  (SELECT jsonb_object_agg(tbl, rows)
     FROM (SELECT tbl, jsonb_agg(row_json ORDER BY sort_key) AS rows FROM cleaned GROUP BY tbl) grouped),
  '{}'::jsonb
))::text;
