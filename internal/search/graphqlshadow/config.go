package graphqlshadow

import "time"

const (
	defaultTimeout       = 5 * time.Second
	defaultMaxConcurrent = 4
)

// Config controls the embedded GraphQL shadow hook. It is registered under the
// graphql_shadow config key, producing GRAPHQL_SHADOW_* environment variables.
// The independent Enabled switch and zero default sample rate make the feature
// inert until operators intentionally set both.
type Config struct {
	// Enabled is the single kill switch. False means the gqlgen middleware is a
	// direct passthrough: no sampling draw, projection, goroutine, or Rust call.
	Enabled bool
	// Endpoint is the internal-only dark Rust POST /graphql endpoint.
	Endpoint string
	// SampleRate is the fraction of comparable search queries shadowed. <= 0
	// disables calls; >= 1 shadows every comparable query.
	SampleRate float64
	// Timeout is the hard bound around the detached Rust HTTP request.
	Timeout time.Duration
	// MaxConcurrent bounds in-flight Rust requests. Saturation drops work without
	// blocking the served Go response.
	MaxConcurrent int
	// LogDiscrepancies emits a structured line for completed mismatches.
	LogDiscrepancies bool
}

// NewDefaultConfig returns the dark, default-off configuration.
func NewDefaultConfig() Config {
	return Config{
		Enabled:          false,
		Endpoint:         "http://bitmagnet-graphql.bitmagnet.svc.cluster.local:3337/graphql",
		SampleRate:       0,
		Timeout:          defaultTimeout,
		MaxConcurrent:    defaultMaxConcurrent,
		LogDiscrepancies: true,
	}
}

func (c Config) active() bool {
	return c.Enabled && c.SampleRate > 0 && c.Endpoint != ""
}

func (c Config) timeout() time.Duration {
	if c.Timeout <= 0 {
		return defaultTimeout
	}

	return c.Timeout
}

func (c Config) maxConcurrent() int {
	if c.MaxConcurrent <= 0 {
		return defaultMaxConcurrent
	}

	return c.MaxConcurrent
}
