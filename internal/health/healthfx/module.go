package healthfx

import (
	"github.com/bitmagnet-io/bitmagnet/internal/config/configfx"
	"github.com/bitmagnet-io/bitmagnet/internal/health"
	"go.uber.org/fx"
)

func New() fx.Option {
	return fx.Module(
		"health",
		configfx.NewConfigModule[health.PeerConfig]("health", health.NewDefaultPeerConfig()),
		fx.Provide(
			health.New,
		),
	)
}
