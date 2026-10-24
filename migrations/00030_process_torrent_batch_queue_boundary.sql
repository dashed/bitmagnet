-- +goose Up
-- +goose StatementBegin

-- Narrow database capabilities for the standalone Rust process_torrent_batch
-- consumer. The queue identity is deliberately fixed in every function. The
-- runtime role receives EXECUTE on these functions from deployment automation,
-- but no direct UPDATE privilege on queue_jobs.
-- This migration intentionally leaves the Goose executor as function owner;
-- deployment automation must transfer ownership to its reviewed NOLOGIN role
-- before granting a production runtime EXECUTE.

CREATE FUNCTION public.process_torrent_batch_claim_job()
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
  WHERE job.queue = 'process_torrent_batch'
    AND job.status IN ('pending', 'retry')
    AND job.run_after <= pg_catalog.clock_timestamp()
  ORDER BY (job.status = 'retry'), job.priority, job.run_after
  FOR UPDATE SKIP LOCKED
  LIMIT 1
$function$;

CREATE FUNCTION public.process_torrent_batch_settle_processed(
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
    AND queue = 'process_torrent_batch'
    AND status IN ('pending', 'retry');

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  IF changed_rows <> 1 THEN
    RAISE EXCEPTION 'process_torrent_batch job is not claimable: %', job_id
      USING ERRCODE = 'P0002';
  END IF;
END;
$function$;

CREATE FUNCTION public.process_torrent_batch_settle_retry(
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
    AND queue = 'process_torrent_batch'
    AND status IN ('pending', 'retry');

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  IF changed_rows <> 1 THEN
    RAISE EXCEPTION 'process_torrent_batch job is not claimable: %', job_id
      USING ERRCODE = 'P0002';
  END IF;
END;
$function$;

CREATE FUNCTION public.process_torrent_batch_settle_failed(
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
    AND queue = 'process_torrent_batch'
    AND status IN ('pending', 'retry');

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  IF changed_rows <> 1 THEN
    RAISE EXCEPTION 'process_torrent_batch job is not claimable: %', job_id
      USING ERRCODE = 'P0002';
  END IF;
END;
$function$;

REVOKE ALL ON FUNCTION public.process_torrent_batch_claim_job()
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.process_torrent_batch_settle_processed(text, bigint)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.process_torrent_batch_settle_retry(text, bigint, text, bigint)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION public.process_torrent_batch_settle_failed(text, bigint, text)
  FROM PUBLIC;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP FUNCTION IF EXISTS public.process_torrent_batch_settle_failed(text, bigint, text);
DROP FUNCTION IF EXISTS public.process_torrent_batch_settle_retry(text, bigint, text, bigint);
DROP FUNCTION IF EXISTS public.process_torrent_batch_settle_processed(text, bigint);
DROP FUNCTION IF EXISTS public.process_torrent_batch_claim_job();

-- +goose StatementEnd
