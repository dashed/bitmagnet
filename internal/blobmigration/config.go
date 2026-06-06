package blobmigration

type Config struct {
	// BatchSize is legacy (the old per-torrent batch). ChunkSize is the streaming-rewrite knob.
	BatchSize             uint              `yaml:"batch_size"`
	ChunkSize             uint              `yaml:"chunk_size"`  // torrents per streaming chunk
	Parallelism           uint              `yaml:"parallelism"` // K parallel info_hash-range workers
	SleepBetweenBatchesMs uint              `yaml:"sleep_between_batches_ms"`
	Consistency           ConsistencyConfig `yaml:"consistency"`
}

type ConsistencyConfig struct {
	Enabled    bool `yaml:"enabled"`
	IntervalMs uint `yaml:"interval_ms"`
	SampleSize int  `yaml:"sample_size"`
}

func NewDefaultConfig() Config {
	return Config{
		BatchSize:             1000,
		ChunkSize:             2000,
		Parallelism:           8,
		SleepBetweenBatchesMs: 100,
		Consistency: ConsistencyConfig{
			Enabled:    false,
			IntervalMs: 30000,
			SampleSize: 100,
		},
	}
}
