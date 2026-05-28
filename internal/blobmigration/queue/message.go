package queue

import "github.com/bitmagnet-io/bitmagnet/internal/model"

const MessageName = "blob_migration"

type MessageParams struct {
	InfoHashGreaterThan string `json:"infoHashGreaterThan"`
	BatchSize           int    `json:"batchSize"`
}

func NewQueueJob(msg MessageParams, options ...model.QueueJobOption) (model.QueueJob, error) {
	if msg.BatchSize == 0 {
		msg.BatchSize = 1000
	}

	return model.NewQueueJob(
		MessageName,
		msg,
		append([]model.QueueJobOption{model.QueueJobMaxRetries(2)}, options...)...,
	)
}
