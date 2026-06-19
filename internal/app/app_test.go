package app

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/app/appfx"
	"github.com/bitmagnet-io/bitmagnet/internal/app/cli/hooks"
	"github.com/bitmagnet-io/bitmagnet/internal/logging/loggingfx"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// TestAppGraphValid validates the whole fx dependency graph (mirroring New) without
// running constructors or lifecycle. It guards the wiring — in particular the
// root-scope search.Search decoration added by searchfx — so an unsatisfiable
// dependency fails here in tests rather than at application startup.
func TestAppGraphValid(t *testing.T) {
	t.Parallel()

	err := fx.ValidateApp(
		appfx.New(),
		loggingfx.WithLogger(),
		fx.Invoke(func(*zap.SugaredLogger, *cli.App, hooks.AttachedHooks) {}),
	)
	require.NoError(t, err)
}
