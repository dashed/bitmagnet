package server

// Config controls which queue handlers the Go queue server owns.
//
// The empty default preserves the existing behavior: every registered handler
// is realized and started by the Go queue server.
type Config struct {
	DisabledQueues []string
}

func NewDefaultConfig() Config {
	return Config{}
}
