package tantivy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTarget(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		address string
		want    string
		wantErr bool
	}{
		{
			name:    "unix scheme single slash",
			address: "unix:/run/bitmagnet.sock",
			want:    "unix:/run/bitmagnet.sock",
		},
		{
			name:    "unix scheme triple slash",
			address: "unix:///run/bitmagnet.sock",
			want:    "unix:///run/bitmagnet.sock",
		},
		{
			name:    "bare absolute path becomes unix",
			address: "/run/bitmagnet.sock",
			want:    "unix:///run/bitmagnet.sock",
		},
		{name: "tcp host:port", address: "127.0.0.1:3334", want: "127.0.0.1:3334"},
		{name: "tcp localhost", address: "localhost:3334", want: "localhost:3334"},
		{name: "trims surrounding whitespace", address: "  localhost:3334  ", want: "localhost:3334"},
		{name: "empty is an error", address: "", wantErr: true},
		{name: "whitespace-only is an error", address: "   ", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseTarget(tc.address)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNewClientLazyDialAndClose(t *testing.T) {
	t.Parallel()

	// grpc.NewClient is lazy: it builds the connection without dialing, so this
	// succeeds even though nothing is listening, and Close tears it down cleanly.
	c, err := NewClient(Config{Address: "127.0.0.1:3334"})
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.NoError(t, c.Close())
}

func TestNewClientUnixTarget(t *testing.T) {
	t.Parallel()

	c, err := NewClient(Config{Address: "unix:///tmp/bitmagnet-search.sock"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	assert.NotNil(t, c)
}

func TestNewClientEmptyAddressErrors(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{Address: ""})
	assert.Error(t, err)
}
