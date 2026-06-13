package searchfx

import (
	"reflect"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/iancoleman/strcase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPathsearchFlagsDefaultFalse locks the hard B2 invariant: every pathsearch
// switch is false in the default config, so the feature is off out of the box.
func TestPathsearchFlagsDefaultFalse(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	assert.False(t, c.PathsearchEnabled, "SEARCH_PATHSEARCH_ENABLED must default false")
	assert.False(t, c.PathTypeaheadEnabled, "SEARCH_PATH_TYPEAHEAD_ENABLED must default false")
	assert.False(t, c.PathCollapseEnabled, "SEARCH_PATH_COLLAPSE_ENABLED must default false")
}

// TestPathsearchEnvVarNames pins the EXACT env var names the three flags resolve
// to. The config loader derives a field's env key as
// ToUpper("search_" + strcase.ToSnake(FieldName)) (see internal/config/config.go
// + envresolver.go), so this asserts both that the fields exist on Config and
// that their names ToSnake-map to the contract-required env vars. (The collapse
// flag is SEARCH_PATH_COLLAPSE_ENABLED, not ..._L3_...: strcase.ToSnake splits a
// letter→digit boundary, so a literal "L3" token can't survive — this test is
// the guard against that and future strcase surprises.)
func TestPathsearchEnvVarNames(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"PathsearchEnabled":    "SEARCH_PATHSEARCH_ENABLED",
		"PathTypeaheadEnabled": "SEARCH_PATH_TYPEAHEAD_ENABLED",
		"PathCollapseEnabled":  "SEARCH_PATH_COLLAPSE_ENABLED",
	}

	ct := reflect.TypeOf(Config{})

	for field, wantEnv := range want {
		_, ok := ct.FieldByName(field)
		require.Truef(t, ok, "Config must have field %s", field)

		gotEnv := "SEARCH_" + strings.ToUpper(strcase.ToSnake(field))
		assert.Equalf(t, wantEnv, gotEnv, "field %s must resolve to env %s", field, wantEnv)
	}
}

// TestNewComposerNilWhenDisabled proves the wiring's safe state: with the feature
// off, the provider yields a nil composer, which the GraphQL layer treats as the
// byte-identical passthrough path.
func TestNewComposerNilWhenDisabled(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig() // PathsearchEnabled == false

	lz := newComposer(cfg, nil, lazy.New(func() (search.Search, error) { return nil, nil }), nil)

	c, err := lz.Get()
	require.NoError(t, err)
	assert.Nil(t, c, "disabled feature must yield a nil composer")
	assert.False(t, c.TypeaheadEnabled(), "nil composer TypeaheadEnabled must be false")
	assert.False(t, c.CollapseEnabled(), "nil composer CollapseEnabled must be false")
}
