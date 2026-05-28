package blobmigration

type Config struct {
	BatchSize             uint `yaml:"batch_size"`
	SleepBetweenBatchesMs uint `yaml:"sleep_between_batches_ms"`
}

func NewDefaultConfig() Config {
	return Config{
		BatchSize:             1000,
		SleepBetweenBatchesMs: 100,
	}
}
