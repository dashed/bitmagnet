package blobmigration

type Config struct {
	BatchSize             uint              `yaml:"batch_size"`
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
		SleepBetweenBatchesMs: 100,
		Consistency: ConsistencyConfig{
			Enabled:    false,
			IntervalMs: 30000,
			SampleSize: 100,
		},
	}
}
