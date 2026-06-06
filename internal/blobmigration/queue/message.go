package queue

import "github.com/bitmagnet-io/bitmagnet/internal/model"

const MessageName = "blob_migration"

// MessageParams drives one parallel range-worker's next chunk of the backfill.
//
// info_hash bounds are hex-encoded for readability in the job payload; the handler parses them back
// to protocol.ID (raw 20-byte bytea) for the keyset query. Binding a hex *string* directly would
// compare it against the bytea info_hash column as the ASCII bytes of the hex (wrong ordering) —
// that was the original cursor bug.
type MessageParams struct {
	// Exclusive lower cursor (hex). "" = start of this worker's range.
	InfoHashGreaterThan string `json:"infoHashGreaterThan"`
	// Inclusive upper bound (hex) for this worker's range. "" = no upper bound (single range).
	InfoHashLessOrEqual string `json:"infoHashLessOrEqual,omitempty"`
	// Parallel range index; selects this worker's checkpoint key (blob_migration:cursor:<rangeID>).
	RangeID int `json:"rangeId"`
	// Total number of parallel ranges (for the completion barrier).
	NumRanges int `json:"numRanges"`
	// Torrents (≈ distinct info_hashes) to process per chunk.
	ChunkSize int `json:"chunkSize"`
}

func NewQueueJob(msg MessageParams, options ...model.QueueJobOption) (model.QueueJob, error) {
	if msg.ChunkSize == 0 {
		msg.ChunkSize = DefaultChunkSize
	}

	if msg.NumRanges == 0 {
		msg.NumRanges = 1
	}

	return model.NewQueueJob(
		MessageName,
		msg,
		append([]model.QueueJobOption{model.QueueJobMaxRetries(2)}, options...)...,
	)
}
