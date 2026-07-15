-- Minimal production-compatible L1 schema for the isolated GraphQL/sqlx RSS gate.
-- This is deliberately not a replacement for Goose migrations. It contains the
-- exact columns touched by the Phase-2 candidate/refine/hydration path and keeps
-- the harness independent of non-transactional production backfills.

CREATE TABLE goose_db_version (
  id bigserial PRIMARY KEY,
  version_id bigint NOT NULL,
  is_applied boolean NOT NULL,
  tstamp timestamptz NOT NULL DEFAULT now()
);
INSERT INTO goose_db_version (version_id, is_applied) VALUES (25, true);

CREATE TABLE torrent_sources (
  key text PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE metadata_sources (
  key text PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE torrents (
  info_hash bytea PRIMARY KEY,
  info_hash_v1 bytea,
  info_hash_v2 bytea,
  meta_version smallint,
  name text NOT NULL,
  size bigint NOT NULL,
  private boolean NOT NULL,
  files_status text NOT NULL,
  extension text,
  files_count integer,
  files_data bytea,
  file_extensions jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE torrents_torrent_sources (
  source text NOT NULL REFERENCES torrent_sources(key),
  info_hash bytea NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
  import_id text,
  seeders integer,
  leechers integer,
  published_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  seen_count integer NOT NULL DEFAULT 1,
  PRIMARY KEY (source, info_hash)
);

CREATE TABLE torrent_tags (
  info_hash bytea NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
  name text NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (info_hash, name)
);

CREATE TABLE content (
  type text NOT NULL,
  source text NOT NULL,
  id text NOT NULL,
  title text NOT NULL,
  release_year integer,
  original_language text,
  original_title text,
  overview text,
  runtime integer,
  popularity real,
  vote_average real,
  vote_count bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (type, source, id)
);

CREATE TABLE content_attributes (
  content_type text NOT NULL,
  content_source text NOT NULL,
  content_id text NOT NULL,
  source text NOT NULL,
  key text NOT NULL,
  value text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (content_type, content_source, content_id, source, key)
);

CREATE TABLE torrent_contents (
  id text PRIMARY KEY,
  info_hash bytea NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
  content_type text,
  content_source text,
  content_id text,
  languages jsonb,
  episodes jsonb,
  video_resolution text,
  video_source text,
  video_codec text,
  video_3d text,
  video_modifier text,
  release_group text,
  tsv tsvector,
  seeders integer,
  leechers integer,
  published_at timestamptz NOT NULL,
  size bigint NOT NULL,
  files_count integer,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  UNIQUE (info_hash, content_type, content_source, content_id)
);
CREATE INDEX torrent_contents_info_hash_idx ON torrent_contents(info_hash);

CREATE TABLE torrent_file_summary (
  info_hash bytea PRIMARY KEY REFERENCES torrents(info_hash) ON DELETE CASCADE,
  file_count integer NOT NULL,
  total_size bigint NOT NULL,
  largest_file_size bigint NOT NULL,
  extensions jsonb NOT NULL,
  has_video boolean NOT NULL,
  has_subtitle boolean NOT NULL,
  has_audio boolean NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

-- The GraphQL process uses this non-owner role so forced RLS can place a
-- harness-only barrier on the first torrent_contents read after the composer's
-- refine semaphore is acquired. Four separate PostgreSQL backends must arrive
-- before any of them can hydrate/decode, proving the measured requests reached
-- the expensive phase concurrently rather than merely overlapping at L3.
CREATE ROLE bitmagnet_rss_app LOGIN PASSWORD 'rss-gate-only';
GRANT CONNECT ON DATABASE bitmagnet TO bitmagnet_rss_app;
GRANT USAGE ON SCHEMA public TO bitmagnet_rss_app;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bitmagnet_rss_app;

CREATE SEQUENCE rss_refine_barrier_generation;
CREATE SEQUENCE rss_refine_barrier_arrivals;
SELECT setval('rss_refine_barrier_generation', 1, true);
SELECT setval('rss_refine_barrier_arrivals', 1, false);

CREATE FUNCTION rss_refine_barrier_wait()
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
  barrier_generation bigint;
  recorded_generation text;
  arrivals bigint;
  arrivals_called boolean;
  deadline timestamptz;
BEGIN
  SELECT last_value
    INTO barrier_generation
    FROM rss_refine_barrier_generation;
  recorded_generation := current_setting('bitmagnet_rss.generation', true);
  IF recorded_generation = barrier_generation::text THEN
    RETURN true;
  END IF;

  SELECT last_value, is_called
    INTO arrivals, arrivals_called
    FROM rss_refine_barrier_arrivals;
  IF arrivals_called AND arrivals >= 4 THEN
    PERFORM set_config(
      'bitmagnet_rss.generation', barrier_generation::text, false
    );
    RETURN true;
  END IF;

  arrivals := nextval('rss_refine_barrier_arrivals');
  PERFORM set_config(
    'bitmagnet_rss.generation', barrier_generation::text, false
  );
  deadline := clock_timestamp() + interval '60 seconds';
  WHILE arrivals < 4 LOOP
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION
        'refine barrier generation % reached only %/4 arrivals',
        barrier_generation,
        arrivals;
    END IF;
    PERFORM pg_sleep(0.01);
    SELECT last_value INTO arrivals FROM rss_refine_barrier_arrivals;
  END LOOP;
  RETURN true;
END;
$$;

REVOKE ALL ON FUNCTION rss_refine_barrier_wait() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION rss_refine_barrier_wait() TO bitmagnet_rss_app;

ALTER TABLE torrent_contents ENABLE ROW LEVEL SECURITY;
ALTER TABLE torrent_contents FORCE ROW LEVEL SECURITY;
CREATE POLICY rss_refine_barrier_select
  ON torrent_contents
  FOR SELECT
  TO bitmagnet_rss_app
  USING (rss_refine_barrier_wait());
