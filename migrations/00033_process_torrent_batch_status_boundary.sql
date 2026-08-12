-- +goose Up
-- +goose StatementBegin

-- Read-only metrics capability for the standalone process_torrent_batch
-- runtime. The queue name and returned label are fixed; the runtime role needs
-- EXECUTE on this function but no direct SELECT on queue_jobs.
-- This migration intentionally leaves the Goose executor as function owner;
-- deployment automation must transfer ownership to its reviewed NOLOGIN role
-- before granting a production runtime EXECUTE.

CREATE FUNCTION public.process_torrent_batch_status_counts()
RETURNS TABLE (queue text, status text, count bigint)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
  SELECT
    'process_torrent_batch'::text AS queue,
    job.status::text AS status,
    pg_catalog.count(*)::bigint AS count
  FROM public.queue_jobs AS job
  WHERE job.queue = 'process_torrent_batch'
  GROUP BY job.status
  ORDER BY job.status
$function$;

REVOKE ALL ON FUNCTION public.process_torrent_batch_status_counts()
  FROM PUBLIC;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP FUNCTION IF EXISTS public.process_torrent_batch_status_counts();

-- +goose StatementEnd
