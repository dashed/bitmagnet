package blobmigrationfx

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration/consistency"
	"github.com/bitmagnet-io/bitmagnet/internal/config/configfx"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type consistencyParams struct {
	fx.In
	Config blobmigration.Config
	Dao    lazy.Lazy[*dao.Query]
	Logger *zap.SugaredLogger
}

type consistencyResult struct {
	fx.Out
	Worker     worker.Worker        `group:"workers"`
	Collector1 prometheus.Collector `group:"prometheus_collectors"`
	Collector2 prometheus.Collector `group:"prometheus_collectors"`
	Collector3 prometheus.Collector `group:"prometheus_collectors"`
	Collector4 prometheus.Collector `group:"prometheus_collectors"`
}

func newConsistency(p consistencyParams) consistencyResult {
	metrics := consistency.NewMetrics()
	var lc *consistency.LiveChecker

	return consistencyResult{
		Worker: worker.NewWorker(
			"blob_consistency_checker",
			fx.Hook{
				OnStart: func(context.Context) error {
					if !p.Config.Consistency.Enabled {
						return nil
					}
					q, err := p.Dao.Get()
					if err != nil {
						return err
					}
					interval := time.Duration(p.Config.Consistency.IntervalMs) * time.Millisecond
					lc = consistency.NewLiveChecker(q, interval, p.Config.Consistency.SampleSize, p.Logger, metrics)
					lc.Start()
					return nil
				},
				OnStop: func(context.Context) error {
					if lc != nil {
						lc.Stop()
					}
					return nil
				},
			},
		),
		Collector1: metrics.ChecksTotal,
		Collector2: metrics.ErrorsTotal,
		Collector3: metrics.LastCheckAt,
		Collector4: metrics.LastErrorAt,
	}
}

func New() fx.Option {
	return fx.Module(
		"blob_migration",
		configfx.NewConfigModule[blobmigration.Config]("blob_migration", blobmigration.NewDefaultConfig()),
		fx.Provide(newConsistency),
	)
}
