package router

import "time"

// Mode selects how the SearchRouter blends the legacy PostgreSQL search engine
// with the Tantivy sidecar. Canary and Tantivy modes may serve eligible queries
// from Tantivy only while the sidecar's cached health state is fresh and healthy.
type Mode string

const (
	// ModePostgres serves PostgreSQL only; the Tantivy sidecar is never called.
	ModePostgres Mode = "postgres"
	// ModeShadow serves PostgreSQL and, for a sampled fraction of queries, runs
	// the same query against Tantivy in the background and records how the two
	// result sets compare. The served result and its latency are never affected.
	ModeShadow Mode = "shadow"
	// ModeCanary serves an eligible, sticky percentage of queries from Tantivy
	// while retaining sampled shadow comparisons for PostgreSQL-served queries.
	ModeCanary Mode = "canary"
	// ModeTantivy serves the whole eligible query class from Tantivy while
	// ineligible or failed requests fall back to PostgreSQL.
	ModeTantivy Mode = "tantivy"
)

// Valid reports whether m is a recognised mode.
func (m Mode) Valid() bool {
	switch m {
	case ModePostgres, ModeShadow, ModeCanary, ModeTantivy:
		return true
	default:
		return false
	}
}

// tantivyBacked reports whether the mode may touch the sidecar at all. Only the
// Tantivy-backed modes build a request or draw a sampling decision; ModePostgres
// (and any invalid mode) is a pure passthrough.
func (m Mode) tantivyBacked() bool {
	switch m {
	case ModeShadow, ModeCanary, ModeTantivy:
		return true
	case ModePostgres:
		return false
	default:
		return false
	}
}

// serving reports whether the mode may serve results from Tantivy.
func (m Mode) serving() bool {
	return m == ModeCanary || m == ModeTantivy
}

// Config configures the SearchRouter. The wiring layer (task #4) builds it from
// the application config and supplies it via fx.
type Config struct {
	// Mode selects the routing strategy. An empty/invalid mode is treated as
	// ModePostgres (safe passthrough) by the router.
	Mode Mode
	// SampleRate in [0,1] is the fraction of queries shadow-compared against
	// Tantivy. <= 0 disables shadowing; >= 1 shadows every query. In production
	// this should stay well below 1; saturated shadow comparisons are dropped by
	// ShadowMaxConcurrent as expected back-pressure, not queued.
	SampleRate float64
	// CanaryPercent in [0,100] is the percentage of eligible traffic the canary
	// routes to the Tantivy-serving path, sticky per query.
	CanaryPercent float64
	// ServeTimeout bounds the Tantivy Search RPC on the serving hot path,
	// distinct from the background ShadowTimeout. On a deadline or any error the
	// router fails closed to PostgreSQL. <= 0 falls back to defaultServeTimeout.
	ServeTimeout time.Duration
	// ShadowTimeout bounds the background Tantivy query + comparison so a slow or
	// stuck sidecar can never leak goroutines. <= 0 falls back to defaultShadowTimeout.
	ShadowTimeout time.Duration
	// ShadowMaxConcurrent bounds in-flight shadow comparisons. <= 0 falls back to
	// defaultShadowMaxConcurrent. When full, sampled comparisons are dropped
	// without blocking the serving path.
	ShadowMaxConcurrent int
	// LogDiscrepancies enables a structured log line for each shadow comparison
	// the comparator flags as a discrepancy (see shadow.LogComparison).
	LogDiscrepancies bool
}

// defaultShadowTimeout bounds a background shadow query when Config.ShadowTimeout
// is unset.
const defaultShadowTimeout = 5 * time.Second

// defaultServeTimeout bounds a Tantivy query on the serving hot path when
// Config.ServeTimeout is unset.
const defaultServeTimeout = 800 * time.Millisecond

// defaultShadowMaxConcurrent bounds background shadow comparisons when
// Config.ShadowMaxConcurrent is unset.
const defaultShadowMaxConcurrent = 4

// shadowTimeout returns the configured timeout or the default when unset.
func (c Config) shadowTimeout() time.Duration {
	if c.ShadowTimeout <= 0 {
		return defaultShadowTimeout
	}

	return c.ShadowTimeout
}

// serveTimeout returns the configured serving timeout or the default when unset.
func (c Config) serveTimeout() time.Duration {
	if c.ServeTimeout <= 0 {
		return defaultServeTimeout
	}

	return c.ServeTimeout
}

// shadowMaxConcurrent returns the configured concurrency limit or the default
// when unset.
func (c Config) shadowMaxConcurrent() int {
	if c.ShadowMaxConcurrent <= 0 {
		return defaultShadowMaxConcurrent
	}

	return c.ShadowMaxConcurrent
}
