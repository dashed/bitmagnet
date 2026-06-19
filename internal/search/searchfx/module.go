// Package searchfx wires the Tantivy search sidecar into the application: it
// registers the "search" config section, provides the gRPC client and shadow
// metrics, and decorates the database's search.Search with the SearchRouter so
// every consumer (GraphQL, Torznab, processor) transparently runs through it.
//
// The feature is disabled by default (see Config); when disabled the client is
// nil and the router is a pure Postgres passthrough, so the application is
// unchanged.
package searchfx

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/config/configfx"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/pathsearch"
	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// docCountInterval is how often the sidecar's index document count is polled for
// the Prometheus gauge (readiness / replication-lag monitoring).
const docCountInterval = 30 * time.Second

// New is the fx module for the search feature: the "search" config section, the
// Tantivy client (nil when disabled), the router config, and the shadow metrics.
// The search.Search decoration is registered separately at the application root
// (see Decorator) because fx decorations are module-scoped and must enclose the
// GraphQL/Torznab/processor consumers.
func New() fx.Option {
	return fx.Module(
		"search",
		configfx.NewConfigModule[Config]("search", NewDefaultConfig()),
		fx.Provide(
			newClient,
			newFileSearchClient,
			newPathsearchClient,
			newComposer,
			pathsearch.NewHealthState,
			pathsearch.NewMetricsResult,
			func(c Config) router.Config { return c.routerConfig() },
			shadow.New,
		),
		fx.Invoke(registerDocCountReporter),
		fx.Invoke(registerPathsearchHealthReporter),
	)
}

// newFileSearchClient builds the L2 filesearch gRPC client, or returns the
// intentional disabled implementation when SEARCH_FILE_SEARCH_ENABLED=false.
// GraphQL remains separately gated by SEARCH_FEATURES_FILE_SEARCH_ENABLED.
func newFileSearchClient(lc fx.Lifecycle, cfg Config) (filesearch.Client, error) {
	if !cfg.FileSearchEnabled {
		return filesearch.Disabled(), nil
	}

	client, err := filesearch.NewClient(cfg.fileSearchConfig())
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { return client.Close() },
	})

	return client, nil
}

// newPathsearchClient builds the L3 pathsearch gRPC client, or returns nil when
// the feature is disabled. Like newClient, a nil client signals "off": the
// composer is not built and no L3 dial happens.
func newPathsearchClient(lc fx.Lifecycle, cfg Config) (*pathsearch.Client, error) {
	if !cfg.PathsearchEnabled {
		return nil, nil //nolint:nilnil // disabled: nil client signals "off"
	}

	client, err := pathsearch.NewClient(cfg.pathsearchConfig())
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { return client.Close() },
	})

	return client, nil
}

// newComposer builds the L3 exact-refine composer, or a lazy resolving to nil
// when the feature is disabled. A nil composer is the safe passthrough state:
// the GraphQL search layer routes through it only when both the enable flag is
// set and the composer is non-nil, so with PathsearchEnabled=false behavior is
// byte-identical to today.
func newComposer(
	cfg Config,
	client *pathsearch.Client,
	pg lazy.Lazy[search.Search],
	health *pathsearch.HealthState,
	metrics *pathsearch.Metrics,
	logger *zap.SugaredLogger,
) lazy.Lazy[*pathsearch.Composer] {
	return lazy.New(func() (*pathsearch.Composer, error) {
		if !cfg.PathsearchEnabled || client == nil {
			return nil, nil
		}

		s, err := pg.Get()
		if err != nil {
			return nil, err
		}

		// WithHealthGate wires the cached health signal published by
		// registerPathsearchHealthReporter: the route fails closed to PostgreSQL
		// when L3 is provably unhealthy (finding #4 / P0-3). WithMetrics counts route
		// outcomes so the gate-7 harness can prove L3 was actually served.
		return pathsearch.NewComposer(
			client, s, cfg.composerConfig(), logger,
			pathsearch.WithHealthGate(health.Healthy),
			pathsearch.WithMetrics(metrics),
		), nil
	})
}

// registerDocCountReporter starts a background poller (when the feature is
// enabled) that periodically reads the sidecar's index document count from its
// HealthCheck and publishes it to the Prometheus gauge. It is lifecycle-managed:
// started on app start, cancelled and drained on stop. A nil client (feature
// disabled) is a no-op.
func registerDocCountReporter(
	lc fx.Lifecycle,
	client *tantivy.Client,
	metrics *shadow.Metrics,
	logger *zap.SugaredLogger,
) {
	if client == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	poll := func() {
		resp, err := client.HealthCheck(ctx)
		if err != nil {
			if logger != nil {
				logger.Debugw("search: tantivy health check failed", "error", err)
			}

			return
		}

		metrics.SetTantivyDocCount(int64(resp.GetDocCount()))
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)

				ticker := time.NewTicker(docCountInterval)
				defer ticker.Stop()

				poll()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						poll()
					}
				}
			}()

			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			<-done

			return nil
		},
	})
}

// registerPathsearchHealthReporter starts a background poller (when the L3
// feature is enabled) that periodically calls the pathsearch sidecar's
// HealthCheck and publishes the result two ways (finding #4 / P0-3):
//
//   - the cached *pathsearch.HealthState that backs the composer's HealthGate —
//     so the route fails CLOSED to PostgreSQL when L3 is provably unhealthy
//     (definition: reachable + SERVING + doc_count>0 [+ optionally fresh]); and
//   - the Prometheus gauges/counters (doc_count, healthy, watermark, last
//     success, health_checks_total) so a misconfigured address is VISIBLE rather
//     than silently serving PG on every query.
//
// It is lifecycle-managed (started on app start, drained on stop) and mirrors
// registerDocCountReporter. A nil client (feature disabled) is a no-op. Because
// HealthState defaults fail-closed (Healthy()==false), the route stays on
// PostgreSQL until the first SUCCESSFUL poll flips it healthy.
// pathsearchHealthChecker is the slice of *pathsearch.Client the poller depends
// on, so the per-poll logic can be unit-tested with a fake. *pathsearch.Client
// satisfies it.
type pathsearchHealthChecker interface {
	HealthCheck(ctx context.Context) (*pb.PathSearchHealth, error)
}

// pathsearchPollState carries the cross-poll bookkeeping the poller needs:
// whether ANY poll has ever succeeded (for the loud first-failure log) and the
// last published health (for change-only logging). It lives outside the poll
// function so a single instance threads through the ticker loop.
type pathsearchPollState struct {
	everSucceeded bool
	lastHealthy   *bool
}

// logState invokes log only when the healthy value CHANGED from the last poll
// (or on the very first poll), so a steady state doesn't spam the log.
func (ps *pathsearchPollState) logState(healthy bool, log func()) {
	if ps.lastHealthy != nil && *ps.lastHealthy == healthy {
		return
	}

	log()

	h := healthy
	ps.lastHealthy = &h
}

// pollPathsearchHealth performs ONE health poll: it probes the sidecar, computes
// the trust decision (reachable + SERVING + doc_count>0 [+ optionally fresh]),
// publishes it to the cached HealthState (backing the route's fail-closed gate)
// and to the Prometheus gauges/counters, and logs transitions. nowEpoch is the
// poll time in Unix seconds (injected for testability of the freshness gate).
func pollPathsearchHealth(
	ctx context.Context,
	hc pathsearchHealthChecker,
	state *pathsearch.HealthState,
	metrics *pathsearch.Metrics,
	cfg Config,
	logger *zap.SugaredLogger,
	ps *pathsearchPollState,
	nowEpoch int64,
) {
	resp, err := hc.HealthCheck(ctx)
	if err != nil {
		metrics.IncHealthCheck(false)
		// Flip healthy false but preserve the last-known doc/watermark/success so
		// the gauges show "was N docs, last ok at T" rather than zeroing on a blip.
		_, docCount, watermark, lastSuccess := state.Snapshot()
		state.SetHealthy(false, docCount, watermark, lastSuccess)
		metrics.SetHealth(false, docCount, watermark, lastSuccess)

		if logger != nil {
			// Change-only so a persistently-wrong address doesn't spam every tick,
			// but the FIRST failure (and any healthy→down transition) is loud: a
			// never-succeeded poll is almost always a misconfigured address, the
			// exact silent-PG-fallback that falsely passes the gate-7 proof.
			ps.logState(false, func() {
				if !ps.everSucceeded {
					logger.Errorw(
						"pathsearch: L3 HealthCheck FAILED and has NEVER succeeded — "+
							"check SEARCH_PATHSEARCH_ADDRESS; the L3 route is failing closed to PostgreSQL",
						"address", cfg.PathsearchAddress, "error", err,
					)
				} else {
					logger.Warnw(
						"pathsearch: L3 HealthCheck failed; route now failing closed to PostgreSQL",
						"address", cfg.PathsearchAddress, "error", err,
					)
				}
			})
		}

		return
	}

	metrics.IncHealthCheck(true)

	ps.everSucceeded = true
	docCount := int64(resp.GetDocCount())
	watermark := resp.GetWatermarkEpoch()
	serving := resp.GetStatus() == pb.PathSearchHealth_SERVING_STATUS_SERVING
	healthy := serving && docCount > 0

	// Optional freshness gate (cfg default 0 = off): a fresh-but-lagging index
	// can be treated as unhealthy so the route never serves from a stale L3.
	staleLag := time.Duration(0)

	if healthy && cfg.PathsearchMaxWatermarkLag > 0 && watermark > 0 {
		if lag := time.Duration(nowEpoch-watermark) * time.Second; lag > cfg.PathsearchMaxWatermarkLag {
			healthy = false
			staleLag = lag
		}
	}

	state.SetHealthy(healthy, docCount, watermark, nowEpoch)
	metrics.SetHealth(healthy, docCount, watermark, nowEpoch)

	if logger != nil {
		ps.logState(healthy, func() {
			switch {
			case healthy:
				logger.Infow(
					"pathsearch: L3 healthy",
					"doc_count",
					docCount,
					"watermark_epoch",
					watermark,
				)
			case staleLag > 0:
				logger.Warnw(
					"pathsearch: L3 watermark lag exceeds threshold; route failing closed to PostgreSQL",
					"lag",
					staleLag,
					"threshold",
					cfg.PathsearchMaxWatermarkLag,
					"watermark_epoch",
					watermark,
				)
			default:
				logger.Warnw(
					"pathsearch: L3 reachable but NOT trusted "+
						"(not SERVING or empty index); route failing closed to PostgreSQL",
					"status",
					resp.GetStatus().String(),
					"doc_count",
					docCount,
				)
			}
		})
	}
}

// registerPathsearchHealthReporter starts a background poller (when the L3
// feature is enabled) that periodically calls the pathsearch sidecar's
// HealthCheck and publishes the result two ways (finding #4 / P0-3):
//
//   - the cached *pathsearch.HealthState that backs the composer's HealthGate —
//     so the route fails CLOSED to PostgreSQL when L3 is provably unhealthy
//     (definition: reachable + SERVING + doc_count>0 [+ optionally fresh]); and
//   - the Prometheus gauges/counters (doc_count, healthy, watermark, last
//     success, health_checks_total) so a misconfigured address is VISIBLE rather
//     than silently serving PG on every query.
//
// It is lifecycle-managed (started on app start, drained on stop) and mirrors
// registerDocCountReporter. A nil client (feature disabled) is a no-op. Because
// HealthState defaults fail-closed (Healthy()==false), the route stays on
// PostgreSQL until the first SUCCESSFUL poll flips it healthy.
func registerPathsearchHealthReporter(
	lc fx.Lifecycle,
	client *pathsearch.Client,
	state *pathsearch.HealthState,
	metrics *pathsearch.Metrics,
	cfg Config,
	logger *zap.SugaredLogger,
) {
	if client == nil {
		return
	}

	interval := cfg.PathsearchHealthInterval
	if interval <= 0 {
		interval = docCountInterval
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	ps := &pathsearchPollState{}

	poll := func() {
		pollPathsearchHealth(ctx, client, state, metrics, cfg, logger, ps, time.Now().Unix())
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)

				ticker := time.NewTicker(interval)
				defer ticker.Stop()

				poll()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						poll()
					}
				}
			}()

			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			<-done

			return nil
		},
	})
}

// Decorator is the fx.Decorate target the application root registers to wrap the
// Postgres search.Search in the SearchRouter. It is exported so appfx can apply
// it at root scope (decorations only affect the module they're declared in and
// its descendants, and the search consumers are siblings of this module).
//
// Usage in appfx: fx.Decorate(searchfx.Decorator).
func Decorator(
	pg lazy.Lazy[search.Search],
	client *tantivy.Client,
	metrics *shadow.Metrics,
	logger *zap.SugaredLogger,
	cfg router.Config,
) lazy.Lazy[search.Search] {
	return lazy.New(func() (search.Search, error) {
		s, err := pg.Get()
		if err != nil {
			return nil, err
		}

		return router.New(s, client, metrics, logger, cfg), nil
	})
}

// newClient builds the Tantivy gRPC client, or returns nil when the feature is
// disabled (consumers — the router and the processor dual-write — treat a nil
// client as "indexing/serving off"). When enabled, the connection is closed on
// application shutdown.
func newClient(lc fx.Lifecycle, cfg Config) (*tantivy.Client, error) {
	if !cfg.Enabled {
		return nil, nil //nolint:nilnil // disabled: nil client signals "off"
	}

	client, err := tantivy.NewClient(cfg.tantivyConfig())
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { return client.Close() },
	})

	return client, nil
}
