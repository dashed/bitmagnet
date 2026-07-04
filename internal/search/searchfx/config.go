package searchfx

import (
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
)

// Config is the application-level "search" config section binding the Tantivy
// sidecar client + the search router. It is registered under the "search" key
// (so env vars are SEARCH_*, e.g. SEARCH_ENABLED, SEARCH_ADDRESS,
// SEARCH_SAMPLE_RATE, SEARCH_SHADOW_MAX_CONCURRENT). It is disabled by default:
// with Enabled=false the app behaves exactly as before (router passes through to
// Postgres, no dual-write).
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
	// values are handled gracefully by the router (>=1 always, <=0 never). Keep
	// this well below 1 in production; ShadowMaxConcurrent drops are expected
	// back-pressure, not errors, when sampled shadow work exceeds capacity.
	SampleRate float64
	// CanaryPercent in [0,100] is the canary's Tantivy-serving share (Phase 6).
	CanaryPercent float64
	// Timeout bounds each unary sidecar RPC.
	Timeout time.Duration
	// BatchTimeout bounds a BatchIndex stream (0 = unbounded).
	BatchTimeout time.Duration
	// ShadowTimeout bounds a background shadow comparison.
	ShadowTimeout time.Duration
	// ShadowMaxConcurrent bounds in-flight shadow comparisons
	// (SEARCH_SHADOW_MAX_CONCURRENT). Default 4. When saturated, sampled
	// comparisons are dropped without blocking the serving path.
	ShadowMaxConcurrent int
	// LogDiscrepancies logs each shadow comparison the comparator flags.
	LogDiscrepancies bool

	// --- L2 DuckDB filesearch (separate from main search and L3 pathsearch) ---
	//
	// These gate construction of the gRPC client for the structured per-file
	// sidecar. They do NOT expose the GraphQL product surface by themselves; the
	// resolver still requires SEARCH_FEATURES_FILE_SEARCH_ENABLED.

	// FileSearchEnabled dials the L2 filesearch sidecar
	// (SEARCH_FILE_SEARCH_ENABLED).
	FileSearchEnabled bool
	// FileSearchAddress is the L2 sidecar address
	// (SEARCH_FILE_SEARCH_ADDRESS); ClusterIP gRPC in production.
	FileSearchAddress string
	// FileSearchTimeout bounds each unary L2 RPC (SEARCH_FILE_SEARCH_TIMEOUT).
	FileSearchTimeout time.Duration
	// FileSearchMaxRows is the maximum rows the Go client requests to emulate
	// offset pagination while the sidecar cursor is not usable
	// (SEARCH_FILE_SEARCH_MAX_ROWS). 0 → client default (500).
	FileSearchMaxRows uint
	// FileSearchRouteText routes GraphQL fileSearch inputs with non-empty text
	// through the L3 pathsearch candidate + L1 refine pipeline when pathsearch is
	// enabled and healthy (SEARCH_FILE_SEARCH_ROUTE_TEXT). Default true; set false
	// for an ops rollback to the L2 DuckDB sidecar for text shapes.
	FileSearchRouteText bool

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
	// PathsearchHealthInterval is the cadence of the background L3 HealthCheck
	// poller (SEARCH_PATHSEARCH_HEALTH_INTERVAL). The poll publishes the doc-count
	// / healthy / watermark gauges and updates the cached HealthGate that fails the
	// route closed to PostgreSQL when L3 is provably unhealthy (finding #4 / P0-3).
	// <= 0 falls back to a safe default.
	PathsearchHealthInterval time.Duration
	// PathsearchMaxWatermarkLag, when > 0, marks L3 unhealthy if its follow-loop
	// watermark is older than this (now - watermark_epoch > lag)
	// (SEARCH_PATHSEARCH_MAX_WATERMARK_LAG). DEFAULT 0 = DISABLED: a fresh-but-lagging
	// L3 stays healthy. Enabling it trades occasional name-semantics PG fallback
	// during replication lag for never serving from a stale index. The follow loop
	// normally keeps L3 fresh within seconds, so default-off is the safe choice.
	PathsearchMaxWatermarkLag time.Duration
	// PathsearchMaxRefineFiles is the per-torrent file-count SANITY CAP for the L3
	// exact-refine (SEARCH_PATHSEARCH_MAX_REFINE_FILES): a candidate whose
	// authoritative file_count exceeds this is declined (fail-loud, excluded +
	// counted) rather than blob-decoded. Pinned == RefineFileBudget so no single
	// torrent's decode can exceed one chunk's budget. 0 → composer default (300k).
	PathsearchMaxRefineFiles uint
	// PathsearchRefineFileBudget is the cumulative file-count budget per refine
	// chunk (SEARCH_PATHSEARCH_REFINE_FILE_BUDGET): the kept candidates are decoded
	// in chunks summing under this so peak TRANSIENT decode memory ≈ one budget
	// (~1KB/file ⇒ ~300MB), not the whole candidate set. This is the make-or-break
	// memory bound (the transient decode happens in the PG query before the retained
	// cap). 0 → composer default (300k).
	PathsearchRefineFileBudget uint
	// PathsearchMaxChunkTorrents caps the torrents per refine chunk regardless of
	// the file budget (SEARCH_PATHSEARCH_MAX_CHUNK_TORRENTS). 0 → composer default
	// (1024).
	PathsearchMaxChunkTorrents uint
	// PathsearchRetainedFileBudget caps the cumulative decoded files RETAINED
	// across all refined matches of one request
	// (SEARCH_PATHSEARCH_RETAINED_FILE_BUDGET): once exceeded, the route serves the
	// accumulated top-relevance prefix as a memory-capped estimate. This is the
	// downstream bound that holds regardless of match rate (~200B/retained file ⇒
	// ~200MB). 0 → composer default (1M).
	PathsearchRetainedFileBudget uint
	// PathsearchRouteTimeout bounds the WHOLE L3 route end-to-end — candidates +
	// FileCounts + every chunk decode + the Go exact-refine
	// (SEARCH_PATHSEARCH_ROUTE_TIMEOUT) — NOT just the sidecar RPC (that is the
	// distinct PathsearchTimeout, which nests under this). On deadline the route
	// serves the accumulated top-relevance prefix as a deadline-capped estimate,
	// never the resolver's broad-FTS PG fallback. It closes the pathological
	// whole-dir latency tail (apple2_flop ran 14-16s unbounded). 0 → composer
	// default (8s) (gate7-6).
	PathsearchRouteTimeout time.Duration
	// PathsearchMaxConcurrentRefines bounds how many EXPENSIVE multi-chunk refines
	// run concurrently across both routes (one shared semaphore)
	// (SEARCH_PATHSEARCH_MAX_CONCURRENT_REFINES). The cheap single-chunk fast path
	// never acquires a slot, so a saturated limiter never blocks/sheds a
	// normal/selective query. 0 → composer default (runtime.NumCPU()) (gate7-6).
	PathsearchMaxConcurrentRefines int
	// PathsearchSlotWait bounds how long a multi-chunk refine waits for a
	// concurrency slot before SHEDDING (serving an empty fail-loud estimate)
	// (SEARCH_PATHSEARCH_SLOT_WAIT). 0 (default) waits up to the route deadline for
	// a slot (queue rather than shed eagerly); a small positive value sheds fast
	// under a burst (gate7-6).
	PathsearchSlotWait time.Duration
}

// NewDefaultConfig returns the safe, disabled-by-default search config.
func NewDefaultConfig() Config {
	return Config{
		Enabled:             false,
		Address:             "unix:///run/bitmagnet/search.sock",
		Engine:              string(router.ModePostgres),
		SampleRate:          1,
		CanaryPercent:       0,
		Timeout:             5 * time.Second,
		BatchTimeout:        0,
		ShadowTimeout:       5 * time.Second,
		ShadowMaxConcurrent: 4,
		LogDiscrepancies:    true,

		// L2 filesearch — client construction defaults false (feature dark).
		FileSearchEnabled:   false,
		FileSearchAddress:   "bitmagnet-filesearch.bitmagnet.svc:50052",
		FileSearchTimeout:   5 * time.Second,
		FileSearchMaxRows:   filesearch.DefaultMaxRows,
		FileSearchRouteText: true,

		// L3 pathsearch — all switches default false (feature off).
		PathsearchEnabled:         false,
		PathTypeaheadEnabled:      false,
		PathCollapseEnabled:       false,
		PathsearchAddress:         "bitmagnet-pathsearch.bitmagnet.svc:50053",
		PathsearchTimeout:         5 * time.Second,
		PathsearchMinQueryLength:  3,
		PathsearchOversample:      4,
		PathsearchMaxCandidates:   2000,
		PathsearchHealthInterval:  15 * time.Second,
		PathsearchMaxWatermarkLag: 0, // disabled by default (see field doc)

		// gate7-4 byte-bound: per-torrent sanity cap + chunk + retained budgets.
		// MaxRefineFiles == RefineFileBudget so ONE chunk's transient decode
		// (~1KB/file measured) can never exceed the budget → ~300k×1KB ≈ 300MB,
		// fits a 2–3Gi pod (the 3Gi stress OOMed at the old 1.5M budget ≈ 1.5GB).
		PathsearchMaxRefineFiles:     300_000,
		PathsearchRefineFileBudget:   300_000,
		PathsearchMaxChunkTorrents:   1024,
		PathsearchRetainedFileBudget: 1_000_000,

		// gate7-6 CPU/latency bound: bound the whole route (8s, distinct from the 5s
		// sidecar RPC timeout) + the multi-chunk refine concurrency (0 = NumCPU) +
		// the slot-wait (0 = queue up to the route deadline rather than shed eagerly).
		PathsearchRouteTimeout:         8 * time.Second,
		PathsearchMaxConcurrentRefines: 0,
		PathsearchSlotWait:             0,
	}
}

// fileSearchConfig maps the section to the L2 gRPC client config.
func (c Config) fileSearchConfig() filesearch.Config {
	return filesearch.Config{
		Address: c.FileSearchAddress,
		Timeout: c.FileSearchTimeout,
		MaxRows: c.FileSearchMaxRows,
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
		MinQueryLength:       c.PathsearchMinQueryLength,
		OversampleFactor:     c.PathsearchOversample,
		MaxCandidates:        c.PathsearchMaxCandidates,
		TypeaheadEnabled:     c.PathTypeaheadEnabled,
		FileSearchRouteText:  c.FileSearchRouteText,
		CollapseEnabled:      c.PathCollapseEnabled,
		MaxRefineFiles:       c.PathsearchMaxRefineFiles,
		RefineFileBudget:     c.PathsearchRefineFileBudget,
		MaxChunkTorrents:     c.PathsearchMaxChunkTorrents,
		RetainedFileBudget:   c.PathsearchRetainedFileBudget,
		RouteTimeout:         c.PathsearchRouteTimeout,
		MaxConcurrentRefines: c.PathsearchMaxConcurrentRefines,
		SlotWait:             c.PathsearchSlotWait,
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
		Mode:                mode,
		SampleRate:          c.SampleRate,
		CanaryPercent:       c.CanaryPercent,
		ShadowTimeout:       c.ShadowTimeout,
		ShadowMaxConcurrent: c.ShadowMaxConcurrent,
		LogDiscrepancies:    c.LogDiscrepancies,
	}
}
