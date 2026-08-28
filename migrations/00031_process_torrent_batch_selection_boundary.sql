-- +goose Up
-- +goose StatementBegin

-- Read-only database capability for the standalone Rust
-- process_torrent_batch consumer. The selected tables, keyset ordering, and
-- filter semantics are fixed in the function body. The runtime role receives
-- EXECUTE from deployment automation, but no direct SELECT on source tables.
-- This migration intentionally leaves the Goose executor as function owner;
-- deployment automation must transfer ownership to its reviewed NOLOGIN role
-- before granting a production runtime EXECUTE.

CREATE FUNCTION public.process_torrent_batch_select_page(
  after_exclusive bytea,
  updated_before timestamptz,
  non_null_content_types text[],
  include_null_content_type boolean,
  select_orphans boolean,
  page_limit bigint
)
RETURNS TABLE (info_hash bytea)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
  IF after_exclusive IS NULL
     OR pg_catalog.octet_length(after_exclusive) <> 20 THEN
    RAISE EXCEPTION 'after_exclusive must be exactly 20 bytes'
      USING ERRCODE = '22023';
  END IF;
  IF updated_before IS NULL THEN
    RAISE EXCEPTION 'updated_before must be non-null'
      USING ERRCODE = '22023';
  END IF;
  IF non_null_content_types IS NULL THEN
    RAISE EXCEPTION 'non_null_content_types must be a non-null array without null elements'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.array_ndims(non_null_content_types) > 1 THEN
    RAISE EXCEPTION 'non_null_content_types must be one-dimensional'
      USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.array_position(non_null_content_types, NULL) IS NOT NULL THEN
    RAISE EXCEPTION 'non_null_content_types must be a non-null array without null elements'
      USING ERRCODE = '22023';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_catalog.unnest(non_null_content_types) AS content_type(value)
    WHERE content_type.value <> ALL(ARRAY[
      'movie', 'tv_show', 'music', 'ebook', 'comic', 'audiobook', 'game',
      'software', 'xxx'
    ]::text[])
  ) THEN
    RAISE EXCEPTION 'non_null_content_types contains a non-canonical value'
      USING ERRCODE = '22023';
  END IF;
  IF include_null_content_type IS NULL OR select_orphans IS NULL THEN
    RAISE EXCEPTION 'selection booleans must be non-null'
      USING ERRCODE = '22023';
  END IF;
  IF page_limit IS NULL OR page_limit <= 0 THEN
    RAISE EXCEPTION 'page_limit must be positive'
      USING ERRCODE = '22023';
  END IF;

  RETURN QUERY
  SELECT torrent.info_hash
  FROM public.torrents AS torrent
  WHERE torrent.info_hash > after_exclusive
    AND torrent.updated_at < updated_before
    AND (
      NOT (
        pg_catalog.cardinality(non_null_content_types) > 0
        OR include_null_content_type
      )
      OR EXISTS (
        SELECT 1
        FROM public.torrent_contents AS content
        WHERE content.info_hash = torrent.info_hash
          AND (
            content.content_type = ANY(non_null_content_types)
            OR (include_null_content_type AND content.content_type IS NULL)
          )
      )
    )
    AND (
      NOT select_orphans
      OR NOT EXISTS (
        SELECT 1
        FROM public.torrent_contents AS orphan_content
        WHERE orphan_content.info_hash = torrent.info_hash
      )
    )
  ORDER BY torrent.info_hash ASC
  LIMIT page_limit;
END;
$function$;

REVOKE ALL ON FUNCTION public.process_torrent_batch_select_page(
  bytea, timestamptz, text[], boolean, boolean, bigint
) FROM PUBLIC;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP FUNCTION IF EXISTS public.process_torrent_batch_select_page(
  bytea, timestamptz, text[], boolean, boolean, bigint
);

-- +goose StatementEnd
