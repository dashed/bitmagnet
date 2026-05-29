package router

import "time"

// Mode selects how the SearchRouter blends the legacy PostgreSQL search engine
// with the Tantivy sidecar. PostgreSQL is always the source of served results in
// Phase 4; the Tantivy-serving modes (canary/tantivy) are scaffolded but still
// serve PostgreSQL until the hydration path lands in Phase 6 (see router.go).
type Mode string

const (
	// ModePostgres serves PostgreSQL only; the Tantivy sidecar is never called.
	ModePostgres Mode = "postgres"
	// ModeShadow serves PostgreSQL and, for a sampled fraction of queries, runs
	// the same query against Tantivy in the background and records how the two
	// result sets compare. The served result and its latency are never affected.
	ModeShadow Mode = "shadow"
	// ModeCanary is ModeShadow plus a routing decision (sticky per query) for a
	// configurable percentage of traffic. The Tantivy-serving path is a Phase-6
	// TODO, so canary currently still serves PostgreSQL + shadow-compares.
	ModeCanary Mode = "canary"
	// ModeTantivy is ModeCanary at 100%. Serving from Tantivy completes in
	// Phase 6; until then it behaves as shadow over all traffic.
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

// Config configures the SearchRouter. The wiring layer (task #4) builds it from
// the application config and supplies it via fx.
type Config struct {
	// Mode selects the routing strategy. An empty/invalid mode is treated as
	// ModePostgres (safe passthrough) by the router.
	Mode Mode
	// SampleRate in [0,1] is the fraction of queries shadow-compared against
	// Tantivy. <= 0 disables shadowing; >= 1 shadows every query.
	SampleRate float64
	// CanaryPercent in [0,100] is the percentage of traffic the canary routes to
	// the Tantivy-serving path (sticky per query). Unused until the Phase-6
	// serving path lands; the routing decision is computed and observable now.
	CanaryPercent float64
	// ShadowTimeout bounds the background Tantivy query + comparison so a slow or
	// stuck sidecar can never leak goroutines. <= 0 falls back to defaultShadowTimeout.
	ShadowTimeout time.Duration
	// LogDiscrepancies enables a structured log line for each shadow comparison
	// the comparator flags as a discrepancy (see shadow.LogComparison).
	LogDiscrepancies bool
}

// defaultShadowTimeout bounds a background shadow query when Config.ShadowTimeout
// is unset.
const defaultShadowTimeout = 5 * time.Second

// shadowTimeout returns the configured timeout or the default when unset.
func (c Config) shadowTimeout() time.Duration {
	if c.ShadowTimeout <= 0 {
		return defaultShadowTimeout
	}

	return c.ShadowTimeout
}
