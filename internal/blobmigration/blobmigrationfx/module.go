package blobmigrationfx

import (
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/config/configfx"
	"go.uber.org/fx"
)

func New() fx.Option {
	return fx.Module(
		"blob_migration",
		configfx.NewConfigModule[blobmigration.Config]("blob_migration", blobmigration.NewDefaultConfig()),
	)
}
