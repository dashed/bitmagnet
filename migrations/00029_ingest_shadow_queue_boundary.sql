-- +goose Up
-- +goose StatementBegin

-- Narrow database capabilities for the ingest-shadow runtime. These functions
-- deliberately hardcode the live and scratch queue identities. The runtime
-- role receives EXECUTE on these functions from deployment automation, but no
-- INSERT/UPDATE/DELETE privilege on queue_jobs or queue_mirror_cursors.

CREATE FUNCTION public.ingest_shadow_lock_cursor(
  bootstrap_latest boolean,
  bootstrap_ran_at timestamptz,
  bootstrap_source_job_id text
)
RETURNS TABLE (ran_at text, source_job_id text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
  IF bootstrap_latest IS NULL THEN
    RAISE EXCEPTION 'bootstrap mode must be explicit'
      USING ERRCODE = '22023';
  END IF;
  IF bootstrap_latest
     AND (bootstrap_ran_at IS NOT NULL OR bootstrap_source_job_id IS NOT NULL) THEN
    RAISE EXCEPTION 'latest bootstrap does not accept an explicit cursor'
      USING ERRCODE = '22023';
  END IF;
  IF NOT bootstrap_latest
     AND ((bootstrap_ran_at IS NULL) <> (bootstrap_source_job_id IS NULL)) THEN
    RAISE EXCEPTION 'explicit cursor timestamp and source job ID must both be null or non-null'
      USING ERRCODE = '22023';
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('process_torrent_shadow', 0)
  );

  INSERT INTO public.queue_mirror_cursors
    (source_queue, shadow_queue, ran_at, source_job_id)
  VALUES
    (
      'process_torrent',
      'process_torrent_shadow',
      CASE WHEN bootstrap_latest
        THEN pg_catalog.clock_timestamp()
        ELSE bootstrap_ran_at
      END,
      CASE WHEN bootstrap_latest
        THEN ''
        ELSE bootstrap_source_job_id
      END
    )
  ON CONFLICT (source_queue, shadow_queue) DO NOTHING;

  RETURN QUERY
  SELECT cursor_row.ran_at::text, cursor_row.source_job_id
  FROM public.queue_mirror_cursors AS cursor_row
  WHERE cursor_row.source_queue = 'process_torrent'
    AND cursor_row.shadow_queue = 'process_torrent_shadow'
  FOR UPDATE;
END;
$function$;

CREATE FUNCTION public.ingest_shadow_advance_cursor(
  new_ran_at timestamptz,
  new_source_job_id text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  changed_rows bigint;
BEGIN
  IF new_ran_at IS NULL OR new_source_job_id IS NULL THEN
    RAISE EXCEPTION 'cursor timestamp and source job ID must be non-null'
      USING ERRCODE = '22023';
  END IF;

  UPDATE public.queue_mirror_cursors
  SET ran_at = new_ran_at,
      source_job_id = new_source_job_id,
      updated_at = pg_catalog.clock_timestamp()
  WHERE source_queue = 'process_torrent'
    AND shadow_queue = 'process_torrent_shadow';

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  IF changed_rows <> 1 THEN
    RAISE EXCEPTION 'ingest-shadow cursor identity is not initialized'
      USING ERRCODE = 'P0002';
  END IF;
END;
$function$;

CREATE FUNCTION public.ingest_shadow_enqueue_job(
  job_fingerprint text,
  job_payload jsonb,
  job_max_retries bigint,
  delay_seconds bigint,
  archival_seconds bigint,
  job_priority integer
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  changed_rows bigint;
BEGIN
  INSERT INTO public.queue_jobs
    (
      fingerprint, queue, status, payload, retries, max_retries, run_after,
      archival_duration, created_at, priority
    )
  VALUES
    (
      job_fingerprint, 'process_torrent_shadow', 'pending', job_payload, 0,
      job_max_retries,
      pg_catalog.clock_timestamp() + pg_catalog.make_interval(secs => delay_seconds),
      pg_catalog.make_interval(secs => archival_seconds),
      pg_catalog.clock_timestamp(), job_priority
    )
  ON CONFLICT (fingerprint) WHERE status IN ('pending', 'retry') DO NOTHING;

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  RETURN changed_rows = 1;
END;
$function$;

CREATE FUNCTION public.ingest_shadow_claim_job()
RETURNS TABLE (
  id text,
  fingerprint text,
  queue text,
  status text,
  payload text,
  retries bigint,
  max_retries bigint,
  priority integer,
  deadline_exceeded boolean
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
  SELECT
    job.id,
    job.fingerprint,
    job.queue,
    job.status::text,
    job.payload::text,
    job.retries::bigint,
    job.max_retries::bigint,
    job.priority,
    job.deadline IS NOT NULL
      AND job.deadline < pg_catalog.clock_timestamp()
  FROM public.queue_jobs AS job
  WHERE job.queue = 'process_torrent_shadow'
    AND job.status IN ('pending', 'retry')
    AND job.run_after <= pg_catalog.clock_timestamp()
  ORDER BY (job.status = 'retry'), job.priority, job.run_after
  FOR UPDATE SKIP LOCKED
  LIMIT 1
$function$;

CREATE FUNCTION public.ingest_shadow_settle_processed(
  job_id text,
  job_retries bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  changed_rows bigint;
BEGIN
  UPDATE public.queue_jobs
  SET status = 'processed',
      retries = job_retries,
      ran_at = pg_catalog.clock_timestamp()
  WHERE id = job_id
    AND queue = 'process_torrent_shadow'
    AND status IN ('pending', 'retry');

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  IF changed_rows <> 1 THEN
    RAISE EXCEPTION 'shadow queue job is not claimable: %', job_id
      USING ERRCODE = 'P0002';
  END IF;
END;
$function$;

CREATE FUNCTION public.ingest_shadow_settle_retry(
  job_id text,
  job_retries bigint,
  job_error text,
  delay_seconds bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  changed_rows bigint;
BEGIN
  UPDATE public.queue_jobs
  SET status = 'retry',
      retries = job_retries,
      ran_at = pg_catalog.clock_timestamp(),
      error = job_error,
      run_after = pg_catalog.clock_timestamp()
        + pg_catalog.make_interval(secs => delay_seconds)
  WHERE id = job_id
    AND queue = 'process_torrent_shadow'
    AND status IN ('pending', 'retry');

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  IF changed_rows <> 1 THEN
    RAISE EXCEPTION 'shadow queue job is not claimable: %', job_id
      USING ERRCODE = 'P0002';
  END IF;
END;
$function$;

CREATE FUNCTION public.ingest_shadow_settle_failed(
  job_id text,
  job_retries bigint,
  job_error text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  changed_rows bigint;
BEGIN
  UPDATE public.queue_jobs
  SET status = 'failed',
      retries = job_retries,
      ran_at = pg_catalog.clock_timestamp(),
      error = job_error
  WHERE id = job_id
    AND queue = 'process_torrent_shadow'
    AND status IN ('pending', 'retry');

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  IF changed_rows <> 1 THEN
    RAISE EXCEPTION 'shadow queue job is not claimable: %', job_id
      USING ERRCODE = 'P0002';
  END IF;
END;
$function$;

REVOKE ALL ON FUNCTION public.ingest_shadow_lock_cursor(boolean, timestamptz, text)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ingest_shadow_advance_cursor(timestamptz, text)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ingest_shadow_enqueue_job(text, jsonb, bigint, bigint, bigint, integer)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ingest_shadow_claim_job()
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ingest_shadow_settle_processed(text, bigint)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ingest_shadow_settle_retry(text, bigint, text, bigint)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ingest_shadow_settle_failed(text, bigint, text)
  FROM PUBLIC;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP FUNCTION IF EXISTS public.ingest_shadow_settle_failed(text, bigint, text);
DROP FUNCTION IF EXISTS public.ingest_shadow_settle_retry(text, bigint, text, bigint);
DROP FUNCTION IF EXISTS public.ingest_shadow_settle_processed(text, bigint);
DROP FUNCTION IF EXISTS public.ingest_shadow_claim_job();
DROP FUNCTION IF EXISTS public.ingest_shadow_enqueue_job(text, jsonb, bigint, bigint, bigint, integer);
DROP FUNCTION IF EXISTS public.ingest_shadow_advance_cursor(timestamptz, text);
DROP FUNCTION IF EXISTS public.ingest_shadow_lock_cursor(boolean, timestamptz, text);

-- +goose StatementEnd
