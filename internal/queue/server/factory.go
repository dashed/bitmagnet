package server

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Params struct {
	fx.In
	Config Config
	Query  lazy.Lazy[*dao.Query]
	// PgxPool  lazy.Lazy[*pgxpool.Pool]
	Handlers []RegisteredHandler `group:"queue_handlers"`
	Logger   *zap.SugaredLogger
}

type Result struct {
	fx.Out
	Worker worker.Worker `group:"workers"`
}

func New(p Params) Result {
	stopped := make(chan struct{})

	return Result{
		Worker: worker.NewWorker(
			"queue_server",
			fx.Hook{
				OnStart: func(context.Context) error {
					logger := p.Logger.Named("queue")
					enabled, disabled, err := selectHandlerRegistrations(
						p.Handlers,
						p.Config.DisabledQueues,
					)
					if err != nil {
						return err
					}
					// pool, err := p.PgxPool.Get()
					// if err != nil {
					// 	return err
					// }
					query, err := p.Query.Get()
					if err != nil {
						return err
					}
					logDisabledQueues(logger, disabled)
					handlers, err := realizeHandlers(enabled)
					if err != nil {
						return err
					}
					srv := server{
						stopped: stopped,
						query:   query,
						// pool:       pool,
						handlers:   handlers,
						gcInterval: time.Minute * 10,
						logger:     logger,
					}
					// todo: Fix!
					//nolint:contextcheck
					return srv.Start(context.Background())
				},
				OnStop: func(context.Context) error {
					close(stopped)
					return nil
				},
			},
		),
	}
}
