package searchfx

import (
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
)

// Config is the application-level "search" config section binding the Tantivy
// sidecar client + the search router. It is registered under the "search" key
// (so env vars are SEARCH_*, e.g. SEARCH_ENABLED, SEARCH_ADDRESS,
// SEARCH_SAMPLE_RATE). It is disabled by default: with Enabled=false the app
// behaves exactly as before (router passes through to Postgres, no dual-write).
type Config struct {
	// Enabled is the master switch. When false the Tantivy client is not built,
	// the router serves Postgres only, and the dual-write is a no-op.
	Enabled bool
	// Address of the sidecar: a Unix socket ("unix:///run/bitmagnet/search.sock")
	// or a TCP "host:port".
	Address string
	// Engine is the router strategy when Enabled: postgres | shadow | canary |
	// tantivy. Ignored (forced to postgres) when Enabled is false.
	Engine string `validate:"omitempty,oneof=postgres shadow canary tantivy"`
	// SampleRate in [0,1] is the fraction of queries shadow-compared. Out-of-range
	// values are handled gracefully by the router (>=1 always, <=0 never).
	SampleRate float64
	// CanaryPercent in [0,100] is the canary's Tantivy-serving share (Phase 6).
	CanaryPercent float64
	// Timeout bounds each unary sidecar RPC.
	Timeout time.Duration
	// BatchTimeout bounds a BatchIndex stream (0 = unbounded).
	BatchTimeout time.Duration
	// ShadowTimeout bounds a background shadow comparison.
	ShadowTimeout time.Duration
	// LogDiscrepancies logs each shadow comparison the comparator flags.
	LogDiscrepancies bool

	// --- L3 pathsearch (separate from the Tantivy main-search above) ---------
	//
	// These gate the L3 per-torrent path-bag candidate sidecar + the backend
	// exact-refine route. They are INDEPENDENT of Enabled above and ALL DEFAULT
	// FALSE: with PathsearchEnabled=false the pathsearch client + composer are
	// never constructed and the search backend behaves byte-identically to today
	// (no L3 dial).

	// PathsearchEnabled is the master switch for backend use of L3
	// (SEARCH_PATHSEARCH_ENABLED).
	PathsearchEnabled bool
	// PathTypeaheadEnabled enables the UI path typeahead route
	// (SEARCH_PATH_TYPEAHEAD_ENABLED). Requires PathsearchEnabled.
	PathTypeaheadEnabled bool
	// PathCollapseEnabled routes the existing collapse:path through the L3
	// candidate path (SEARCH_PATH_COLLAPSE_ENABLED). The "L3" is an implementation
	// detail dropped from the flag name; the flag's job is to route path-collapse
	// through L3 candidates + exact-refine. Requires PathsearchEnabled.
	PathCollapseEnabled bool
	// PathsearchAddress is the L3 sidecar address
	// (SEARCH_PATHSEARCH_ADDRESS); ClusterIP gRPC in production.
	PathsearchAddress string
	// PathsearchTimeout bounds each unary L3 RPC (SEARCH_PATHSEARCH_TIMEOUT).
	PathsearchTimeout time.Duration
	// PathsearchMinQueryLength is the server-side broad-gram guard: shorter
	// queries skip the L3 route (SEARCH_PATHSEARCH_MIN_QUERY_LENGTH).
	PathsearchMinQueryLength int
	// PathsearchOversample multiplies the page window to size the candidate
	// budget for exact-refine headroom (SEARCH_PATHSEARCH_OVERSAMPLE).
	PathsearchOversample uint
	// PathsearchMaxCandidates hard-caps candidates fetched/blob-decoded per
	// request so a broad gram never decodes an unbounded set
	// (SEARCH_PATHSEARCH_MAX_CANDIDATES).
	PathsearchMaxCandidates uint
}

// NewDefaultConfig returns the safe, disabled-by-default search config.
func NewDefaultConfig() Config {
	return Config{
		Enabled:          false,
		Address:          "unix:///run/bitmagnet/search.sock",
		Engine:           string(router.ModePostgres),
		SampleRate:       1,
		CanaryPercent:    0,
		Timeout:          5 * time.Second,
		BatchTimeout:     0,
		ShadowTimeout:    5 * time.Second,
		LogDiscrepancies: true,

		// L3 pathsearch — all switches default false (feature off).
		PathsearchEnabled:        false,
		PathTypeaheadEnabled:     false,
		PathCollapseEnabled:      false,
		PathsearchAddress:        "bitmagnet-pathsearch.bitmagnet.svc:50053",
		PathsearchTimeout:        5 * time.Second,
		PathsearchMinQueryLength: 3,
		PathsearchOversample:     4,
		PathsearchMaxCandidates:  2000,
	}
}

// pathsearchConfig maps the section to the L3 gRPC client config.
func (c Config) pathsearchConfig() pathsearch.Config {
	return pathsearch.Config{
		Address: c.PathsearchAddress,
		Timeout: c.PathsearchTimeout,
	}
}

// composerConfig maps the section to the L3 exact-refine composer config.
func (c Config) composerConfig() pathsearch.ComposerConfig {
	return pathsearch.ComposerConfig{
		MinQueryLength:   c.PathsearchMinQueryLength,
		OversampleFactor: c.PathsearchOversample,
		MaxCandidates:    c.PathsearchMaxCandidates,
		TypeaheadEnabled: c.PathTypeaheadEnabled,
		CollapseEnabled:  c.PathCollapseEnabled,
	}
}

// tantivyConfig maps the section to the gRPC client config.
func (c Config) tantivyConfig() tantivy.Config {
	return tantivy.Config{
		Address:      c.Address,
		Timeout:      c.Timeout,
		BatchTimeout: c.BatchTimeout,
	}
}

// routerConfig maps the section to the router config. When the feature is
// disabled the router is forced to ModePostgres, so a stale/configured Mode can
// never cause the router to touch the (absent) sidecar.
func (c Config) routerConfig() router.Config {
	mode := router.Mode(c.Engine)
	if !c.Enabled {
		mode = router.ModePostgres
	}

	return router.Config{
		Mode:             mode,
		SampleRate:       c.SampleRate,
		CanaryPercent:    c.CanaryPercent,
		ShadowTimeout:    c.ShadowTimeout,
		LogDiscrepancies: c.LogDiscrepancies,
	}
}
