package searchfx

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/search/router"
	"github.com/stretchr/testify/assert"
)

func TestNewDefaultConfigIsDisabledPassthrough(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	assert.False(t, c.Enabled, "search must be off by default")
	assert.Equal(t, router.ModePostgres, c.routerConfig().Mode,
		"a disabled, default config is a pure Postgres passthrough")
}

func TestRouterConfigForcesPostgresWhenDisabled(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	c.Enabled = false
	c.Mode = string(router.ModeShadow) // configured mode is ignored when disabled

	assert.Equal(t, router.ModePostgres, c.routerConfig().Mode)
}

func TestRouterConfigUsesModeWhenEnabled(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	c.Enabled = true
	c.Mode = string(router.ModeShadow)
	c.SampleRate = 0.25
	c.LogDiscrepancies = true

	rc := c.routerConfig()
	assert.Equal(t, router.ModeShadow, rc.Mode)
	assert.InDelta(t, 0.25, rc.SampleRate, 0)
	assert.True(t, rc.LogDiscrepancies)
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
