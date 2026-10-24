-- +goose Up
-- +goose StatementBegin

ALTER TABLE torrent_file_summary ADD COLUMN IF NOT EXISTS compressed_bytes BIGINT;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE torrent_file_summary DROP COLUMN IF EXISTS compressed_bytes;

-- +goose StatementEnd
