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
	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/bitmagnet-io/bitmagnet/internal/search/shadow"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
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
			func(c Config) router.Config { return c.routerConfig() },
			shadow.New,
		),
		fx.Invoke(registerDocCountReporter),
	)
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
