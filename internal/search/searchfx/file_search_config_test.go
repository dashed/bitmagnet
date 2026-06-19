package searchfx

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/filesearch"
	"github.com/iancoleman/strcase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSearchConfigDefaultsDisabled(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	assert.False(t, c.FileSearchEnabled, "SEARCH_FILE_SEARCH_ENABLED must default false")
	assert.Equal(t, "bitmagnet-filesearch.bitmagnet.svc:50052", c.FileSearchAddress)
	assert.Equal(t, 5*time.Second, c.FileSearchTimeout)
	assert.Equal(t, uint(filesearch.DefaultMaxRows), c.FileSearchMaxRows)
}

func TestFileSearchEnvVarNames(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"FileSearchEnabled": "SEARCH_FILE_SEARCH_ENABLED",
		"FileSearchAddress": "SEARCH_FILE_SEARCH_ADDRESS",
		"FileSearchTimeout": "SEARCH_FILE_SEARCH_TIMEOUT",
		"FileSearchMaxRows": "SEARCH_FILE_SEARCH_MAX_ROWS",
	}

	ct := reflect.TypeOf(Config{})

	for field, wantEnv := range want {
		_, ok := ct.FieldByName(field)
		require.Truef(t, ok, "Config must have field %s", field)

		gotEnv := "SEARCH_" + strings.ToUpper(strcase.ToSnake(field))
		assert.Equalf(t, wantEnv, gotEnv, "field %s must resolve to env %s", field, wantEnv)
	}
}

func TestFileSearchConfigMapsFields(t *testing.T) {
	t.Parallel()

	c := NewDefaultConfig()
	c.FileSearchAddress = "127.0.0.1:50052"
	c.FileSearchTimeout = 2 * time.Second
	c.FileSearchMaxRows = 123

	fc := c.fileSearchConfig()
	assert.Equal(t, "127.0.0.1:50052", fc.Address)
	assert.Equal(t, 2*time.Second, fc.Timeout)
	assert.Equal(t, uint(123), fc.MaxRows)
}

func TestNewFileSearchClientDisabledReturnsDisabledClient(t *testing.T) {
	t.Parallel()

	c, err := newFileSearchClient(nil, NewDefaultConfig())
	require.NoError(t, err)

	_, err = c.FileSearch(context.Background(), filesearch.FileSearchInput{})
	assert.ErrorIs(t, err, filesearch.ErrDisabled)
}

func TestNewFileSearchClientEnabledEmptyAddressErrors(t *testing.T) {
	t.Parallel()

	cfg := NewDefaultConfig()
	cfg.FileSearchEnabled = true
	cfg.FileSearchAddress = ""

	c, err := newFileSearchClient(nil, cfg)
	assert.Nil(t, c)
	assert.Error(t, err)
}
