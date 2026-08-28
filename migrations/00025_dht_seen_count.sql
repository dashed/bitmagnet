-- +goose Up
-- +goose StatementBegin

ALTER TABLE torrents_torrent_sources
  ADD COLUMN IF NOT EXISTS seen_count integer NOT NULL DEFAULT 1;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE torrents_torrent_sources
  DROP COLUMN IF EXISTS seen_count;

-- +goose StatementEnd
