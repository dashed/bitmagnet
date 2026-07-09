package searchfx

import (
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/config"
	"github.com/bitmagnet-io/bitmagnet/internal/config/configresolver"
	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/go-playground/validator/v10"
	"github.com/iancoleman/strcase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultConfigIsDisabledPassthrough(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	assert.False(t, c.Enabled, "search must be off by default")
	assert.True(t, c.DualWriteEnabled, "dual-write must preserve its existing default-on behavior")
	assert.Equal(t, 4, c.ShadowMaxConcurrent)
	assert.Equal(t, router.ModePostgres, c.routerConfig().Mode,
		"a disabled, default config is a pure Postgres passthrough")
}

func TestDualWriteEnabledConfigResolution(t *testing.T) {
	t.Parallel()

	resolve := func(t *testing.T, env map[string]string) Config {
		t.Helper()

		result, err := config.New(config.Params{
			Specs: []config.Spec{{Key: "search", DefaultValue: NewDefaultConfig()}},
			Resolvers: []configresolver.Resolver{
				configresolver.NewEnv(env),
			},
			Validate: validator.New(),
		})
		require.NoError(t, err)

		resolved, ok := result.Resolved.NodeMap["search"].Value.(Config)
		require.True(t, ok)

		return resolved
	}

	t.Run("unset defaults true", func(t *testing.T) {
		assert.True(t, resolve(t, nil).DualWriteEnabled)
	})

	t.Run("environment false", func(t *testing.T) {
		resolved := resolve(t, map[string]string{"SEARCH_DUAL_WRITE_ENABLED": "false"})
		assert.False(t, resolved.DualWriteEnabled)
	})
}

func TestRouterConfigForcesPostgresWhenDisabled(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	c.Enabled = false
	c.Engine = string(router.ModeShadow) // configured mode is ignored when disabled

	assert.Equal(t, router.ModePostgres, c.routerConfig().Mode)
}

func TestRouterConfigUsesModeWhenEnabled(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	c.Enabled = true
	c.Engine = string(router.ModeShadow)
	c.SampleRate = 0.25
	c.ShadowMaxConcurrent = 2
	c.LogDiscrepancies = true

	rc := c.routerConfig()
	assert.Equal(t, router.ModeShadow, rc.Mode)
	assert.InDelta(t, 0.25, rc.SampleRate, 0)
	assert.Equal(t, 2, rc.ShadowMaxConcurrent)
	assert.True(t, rc.LogDiscrepancies)
}

func TestShadowMaxConcurrentEnvVarName(t *testing.T) {
	t.Parallel()

	gotEnv := "SEARCH_" + strings.ToUpper(strcase.ToSnake("ShadowMaxConcurrent"))
	assert.Equal(t, "SEARCH_SHADOW_MAX_CONCURRENT", gotEnv)
}

func TestTantivyConfigMapsFields(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	c.Address = "127.0.0.1:3334"

	tc := c.tantivyConfig()
	assert.Equal(t, "127.0.0.1:3334", tc.Address)
	assert.Equal(t, c.Timeout, tc.Timeout)
	assert.Equal(t, c.BatchTimeout, tc.BatchTimeout)
}
