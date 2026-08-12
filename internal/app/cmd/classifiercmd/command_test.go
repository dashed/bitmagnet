package classifiercmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteCreateOnlyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, writeCreateOnlyJSON(path, map[string]string{"value": "<exact>"}))

	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{\"value\":\"<exact>\"}\n", string(encoded))

	require.Error(t, writeCreateOnlyJSON(path, map[string]string{"value": "replacement"}))
	encoded, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{\"value\":\"<exact>\"}\n", string(encoded))
}

func TestWriteCreateOnlyJSONUsesCanonicalUnicodeSeparatorEscapes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, writeCreateOnlyJSON(path, struct {
		Value string `json:"value"`
	}{Value: "before\u2028between\u2029after"}))

	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(
		t,
		"{\"value\":\"before\\u2028between\\u2029after\"}\n",
		string(encoded),
	)
}
