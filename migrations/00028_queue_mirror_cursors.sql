-- +goose Up
-- +goose StatementBegin

-- Durable checkpoint for processed-row mirrors. The source/target pair is the
-- mirror identity; the nullable cursor represents a newly initialized mirror.
-- Writers lock this row and advance it in the same transaction as scratch
-- queue inserts, so replicas cannot race a stale process-local checkpoint.
CREATE TABLE queue_mirror_cursors (
  source_queue text NOT NULL,
  shadow_queue text NOT NULL,
  ran_at timestamptz,
  source_job_id text,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (source_queue, shadow_queue),
  CONSTRAINT queue_mirror_cursors_position_check
    CHECK ((ran_at IS NULL) = (source_job_id IS NULL)),
  CONSTRAINT queue_mirror_cursors_distinct_queues_check
    CHECK (source_queue <> shadow_queue)
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE IF EXISTS queue_mirror_cursors;

-- +goose StatementEnd
