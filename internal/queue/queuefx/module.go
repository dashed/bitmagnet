package queuefx

import (
	"github.com/bitmagnet-io/bitmagnet/internal/config/configfx"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/manager"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/prometheus"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/server"
	"go.uber.org/fx"
)

func New() fx.Option {
	return fx.Module(
		"queue",
		configfx.NewConfigModule[server.Config]("queue_server", server.NewDefaultConfig()),
		fx.Provide(
			server.New,
			manager.New,
			prometheus.New,
		),
	)
}
