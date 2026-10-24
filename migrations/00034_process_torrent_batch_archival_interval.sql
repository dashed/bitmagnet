-- +goose Up
-- +goose StatementBegin

-- Migration 32 used a seven-day interval component. The Go model scanner only
-- accepts PostgreSQL's hh:mm:ss interval representation, so preserve the same
-- duration in the time/microseconds component. CREATE OR REPLACE retains the
-- existing function owner and grants; PUBLIC remains explicitly revoked.

CREATE OR REPLACE FUNCTION public.process_torrent_batch_enqueue_plan(
  child_payloads text[],
  child_run_afters timestamptz[],
  child_priorities integer[],
  continuation_payload text,
  continuation_run_after timestamptz,
  shared_created_at timestamptz
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  expected_rows bigint;
  inserted_rows bigint;
BEGIN
  IF child_payloads IS NULL
     OR child_run_afters IS NULL
     OR child_priorities IS NULL THEN
    RAISE EXCEPTION 'child plan arrays must be non-null'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.array_ndims(child_payloads) > 1
     OR pg_catalog.array_ndims(child_run_afters) > 1
     OR pg_catalog.array_ndims(child_priorities) > 1 THEN
    RAISE EXCEPTION 'child plan arrays must be one-dimensional'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.cardinality(child_payloads)
       <> pg_catalog.cardinality(child_run_afters)
     OR pg_catalog.cardinality(child_payloads)
       <> pg_catalog.cardinality(child_priorities) THEN
    RAISE EXCEPTION 'child plan arrays must have equal cardinality'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.array_position(child_payloads, NULL) IS NOT NULL
     OR pg_catalog.array_position(child_run_afters, NULL) IS NOT NULL
     OR pg_catalog.array_position(child_priorities, NULL) IS NOT NULL THEN
    RAISE EXCEPTION 'child plan arrays must not contain null elements'
      USING ERRCODE = '22023';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_catalog.unnest(child_payloads) AS child(payload)
    WHERE NOT child.payload IS JSON OBJECT
  ) THEN
    RAISE EXCEPTION 'every child payload must be a JSON object'
      USING ERRCODE = '22023';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_catalog.unnest(child_priorities) AS child(priority)
    WHERE child.priority NOT IN (4, 10)
  ) THEN
    RAISE EXCEPTION 'child priorities must be 4 or 10'
      USING ERRCODE = '22023';
  END IF;
  IF (continuation_payload IS NULL) <> (continuation_run_after IS NULL) THEN
    RAISE EXCEPTION 'continuation payload and run_after must both be null or non-null'
      USING ERRCODE = '22023';
  END IF;
  IF continuation_payload IS NOT NULL
     AND NOT continuation_payload IS JSON OBJECT THEN
    RAISE EXCEPTION 'continuation payload must be a JSON object'
      USING ERRCODE = '22023';
  END IF;
  IF continuation_payload IS NOT NULL
     AND pg_catalog.cardinality(child_payloads) = 0 THEN
    RAISE EXCEPTION 'a continuation requires at least one child'
      USING ERRCODE = '22023';
  END IF;
  IF shared_created_at IS NULL THEN
    RAISE EXCEPTION 'shared_created_at must be non-null'
      USING ERRCODE = '22023';
  END IF;

  -- An empty child plan with no continuation is an explicit successful no-op.
  -- It still executes this one INSERT statement, whose SELECT yields no rows.
  WITH child_rows AS (
    SELECT
      payload.ordinality AS plan_order,
      'process_torrent'::text AS queue,
      payload.value AS raw_payload,
      run_after.value AS run_after,
      priority.value AS priority
    FROM pg_catalog.unnest(child_payloads) WITH ORDINALITY
      AS payload(value, ordinality)
    JOIN pg_catalog.unnest(child_run_afters) WITH ORDINALITY
      AS run_after(value, ordinality) USING (ordinality)
    JOIN pg_catalog.unnest(child_priorities) WITH ORDINALITY
      AS priority(value, ordinality) USING (ordinality)
  ), planned_rows AS (
    SELECT plan_order, queue, raw_payload, run_after, priority
    FROM child_rows
    UNION ALL
    SELECT
      pg_catalog.cardinality(child_payloads)::bigint + 1,
      'process_torrent_batch'::text,
      continuation_payload,
      continuation_run_after,
      0
    WHERE continuation_payload IS NOT NULL
  )
  INSERT INTO public.queue_jobs
    (
      fingerprint, queue, status, payload, retries, max_retries, run_after,
      ran_at, error, deadline, archival_duration, created_at, priority
    )
  SELECT
    pg_catalog.encode(
      pg_catalog.sha256(
        pg_catalog.convert_to(planned.queue || planned.raw_payload, 'UTF8')
      ),
      'hex'
    ),
    planned.queue,
    'pending'::public.queue_job_status,
    planned.raw_payload::jsonb,
    0,
    2,
    planned.run_after,
    NULL,
    NULL,
    NULL,
    pg_catalog.make_interval(secs => 604800),
    shared_created_at,
    planned.priority
  FROM planned_rows AS planned
  ORDER BY planned.plan_order;

  GET DIAGNOSTICS inserted_rows = ROW_COUNT;
  expected_rows := pg_catalog.cardinality(child_payloads)
    + CASE WHEN continuation_payload IS NULL THEN 0 ELSE 1 END;
  IF inserted_rows <> expected_rows THEN
    RAISE EXCEPTION 'batch plan inserted % rows, expected %',
      inserted_rows, expected_rows
      USING ERRCODE = '22023';
  END IF;
  RETURN inserted_rows;
END;
$function$;

REVOKE ALL ON FUNCTION public.process_torrent_batch_enqueue_plan(
  text[], timestamptz[], integer[], text, timestamptz, timestamptz
) FROM PUBLIC;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Restore the exact migration-32 body when explicitly rolling back this fix.
CREATE OR REPLACE FUNCTION public.process_torrent_batch_enqueue_plan(
  child_payloads text[],
  child_run_afters timestamptz[],
  child_priorities integer[],
  continuation_payload text,
  continuation_run_after timestamptz,
  shared_created_at timestamptz
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  expected_rows bigint;
  inserted_rows bigint;
BEGIN
  IF child_payloads IS NULL
     OR child_run_afters IS NULL
     OR child_priorities IS NULL THEN
    RAISE EXCEPTION 'child plan arrays must be non-null'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.array_ndims(child_payloads) > 1
     OR pg_catalog.array_ndims(child_run_afters) > 1
     OR pg_catalog.array_ndims(child_priorities) > 1 THEN
    RAISE EXCEPTION 'child plan arrays must be one-dimensional'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.cardinality(child_payloads)
       <> pg_catalog.cardinality(child_run_afters)
     OR pg_catalog.cardinality(child_payloads)
       <> pg_catalog.cardinality(child_priorities) THEN
    RAISE EXCEPTION 'child plan arrays must have equal cardinality'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.array_position(child_payloads, NULL) IS NOT NULL
     OR pg_catalog.array_position(child_run_afters, NULL) IS NOT NULL
     OR pg_catalog.array_position(child_priorities, NULL) IS NOT NULL THEN
    RAISE EXCEPTION 'child plan arrays must not contain null elements'
      USING ERRCODE = '22023';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_catalog.unnest(child_payloads) AS child(payload)
    WHERE NOT child.payload IS JSON OBJECT
  ) THEN
    RAISE EXCEPTION 'every child payload must be a JSON object'
      USING ERRCODE = '22023';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_catalog.unnest(child_priorities) AS child(priority)
    WHERE child.priority NOT IN (4, 10)
  ) THEN
    RAISE EXCEPTION 'child priorities must be 4 or 10'
      USING ERRCODE = '22023';
  END IF;
  IF (continuation_payload IS NULL) <> (continuation_run_after IS NULL) THEN
    RAISE EXCEPTION 'continuation payload and run_after must both be null or non-null'
      USING ERRCODE = '22023';
  END IF;
  IF continuation_payload IS NOT NULL
     AND NOT continuation_payload IS JSON OBJECT THEN
    RAISE EXCEPTION 'continuation payload must be a JSON object'
      USING ERRCODE = '22023';
  END IF;
  IF continuation_payload IS NOT NULL
     AND pg_catalog.cardinality(child_payloads) = 0 THEN
    RAISE EXCEPTION 'a continuation requires at least one child'
      USING ERRCODE = '22023';
  END IF;
  IF shared_created_at IS NULL THEN
    RAISE EXCEPTION 'shared_created_at must be non-null'
      USING ERRCODE = '22023';
  END IF;

  -- An empty child plan with no continuation is an explicit successful no-op.
  -- It still executes this one INSERT statement, whose SELECT yields no rows.
  WITH child_rows AS (
    SELECT
      payload.ordinality AS plan_order,
      'process_torrent'::text AS queue,
      payload.value AS raw_payload,
      run_after.value AS run_after,
      priority.value AS priority
    FROM pg_catalog.unnest(child_payloads) WITH ORDINALITY
      AS payload(value, ordinality)
    JOIN pg_catalog.unnest(child_run_afters) WITH ORDINALITY
      AS run_after(value, ordinality) USING (ordinality)
    JOIN pg_catalog.unnest(child_priorities) WITH ORDINALITY
      AS priority(value, ordinality) USING (ordinality)
  ), planned_rows AS (
    SELECT plan_order, queue, raw_payload, run_after, priority
    FROM child_rows
    UNION ALL
    SELECT
      pg_catalog.cardinality(child_payloads)::bigint + 1,
      'process_torrent_batch'::text,
      continuation_payload,
      continuation_run_after,
      0
    WHERE continuation_payload IS NOT NULL
  )
  INSERT INTO public.queue_jobs
    (
      fingerprint, queue, status, payload, retries, max_retries, run_after,
      ran_at, error, deadline, archival_duration, created_at, priority
    )
  SELECT
    pg_catalog.encode(
      pg_catalog.sha256(
        pg_catalog.convert_to(planned.queue || planned.raw_payload, 'UTF8')
      ),
      'hex'
    ),
    planned.queue,
    'pending'::public.queue_job_status,
    planned.raw_payload::jsonb,
    0,
    2,
    planned.run_after,
    NULL,
    NULL,
    NULL,
    pg_catalog.make_interval(days => 7),
    shared_created_at,
    planned.priority
  FROM planned_rows AS planned
  ORDER BY planned.plan_order;

  GET DIAGNOSTICS inserted_rows = ROW_COUNT;
  expected_rows := pg_catalog.cardinality(child_payloads)
    + CASE WHEN continuation_payload IS NULL THEN 0 ELSE 1 END;
  IF inserted_rows <> expected_rows THEN
    RAISE EXCEPTION 'batch plan inserted % rows, expected %',
      inserted_rows, expected_rows
      USING ERRCODE = '22023';
  END IF;
  RETURN inserted_rows;
END;
$function$;

REVOKE ALL ON FUNCTION public.process_torrent_batch_enqueue_plan(
  text[], timestamptz[], integer[], text, timestamptz, timestamptz
) FROM PUBLIC;

-- +goose StatementEnd
