package metainfo_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	ami "github.com/anacrolix/torrent/metainfo"
	infohashv2 "github.com/anacrolix/torrent/types/infohash-v2"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadInfoBytes reads a .torrent fixture and returns the raw info dictionary
// bytes (exactly what ut_metadata / the DHT fetch would yield).
func loadInfoBytes(t *testing.T, path string) []byte {
	t.Helper()

	b, err := os.ReadFile(path)
	require.NoError(t, err)

	mi, err := ami.Load(bytes.NewReader(b))
	require.NoError(t, err)

	return []byte(mi.InfoBytes)
}

// syntheticV1InfoBytes builds a minimal v1-only single-file info dict.
func syntheticV1InfoBytes(t *testing.T) []byte {
	t.Helper()

	info := ami.Info{
		Name:        "synthetic-single.bin",
		PieceLength: 32768,
		Length:      4096,
		Pieces:      make([]byte, 20),
	}

	b, err := bencode.Marshal(info)
	require.NoError(t, err)

	return b
}

func v1Hash(b []byte) protocol.ID {
	return protocol.ID(ami.HashBytes(b))
}

func v2Full(b []byte) protocol.InfoHashV2 {
	return protocol.InfoHashV2(infohashv2.HashBytes(b))
}

func v2Short(b []byte) protocol.ID {
	v2 := infohashv2.HashBytes(b)
	return protocol.ID(*v2.ToShort())
}

func TestParseMetaInfoBytes(t *testing.T) {
	t.Parallel()

	v1OnlyBytes := syntheticV1InfoBytes(t)
	hybridBytes := loadInfoBytes(t, "testdata/bittorrent-v2-hybrid-test.torrent")
	pureV2Bytes := loadInfoBytes(t, "testdata/bittorrent-v2-test.torrent")

	tests := []struct {
		name            string
		infoBytes       []byte
		discoveryHash   protocol.ID
		wantMetaVersion uint8
		wantV1          bool
		wantV2          bool
	}{
		{
			name:            "v1-only (synthetic)",
			infoBytes:       v1OnlyBytes,
			discoveryHash:   v1Hash(v1OnlyBytes),
			wantMetaVersion: 1,
			wantV1:          true,
			wantV2:          false,
		},
		{
			name:            "hybrid (discovered via v1)",
			infoBytes:       hybridBytes,
			discoveryHash:   v1Hash(hybridBytes),
			wantMetaVersion: 2,
			wantV1:          true,
			wantV2:          true,
		},
		{
			name:            "pure-v2 (discovered via truncated SHA-256)",
			infoBytes:       pureV2Bytes,
			discoveryHash:   v2Short(pureV2Bytes),
			wantMetaVersion: 2,
			wantV1:          false,
			wantV2:          true,
		},
		{
			// A hybrid can be announced on the DHT under its truncated-v2 hash;
			// it must still verify and record BOTH identities.
			name:            "hybrid (discovered via truncated SHA-256)",
			infoBytes:       hybridBytes,
			discoveryHash:   v2Short(hybridBytes),
			wantMetaVersion: 2,
			wantV1:          true,
			wantV2:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := metainfo.ParseMetaInfoBytes(tt.discoveryHash, tt.infoBytes)
			require.NoError(t, err)

			assert.Equal(t, tt.wantMetaVersion, parsed.MetaVersion)

			if tt.wantV1 {
				require.NotNil(t, parsed.InfoHashV1)
				assert.Equal(t, v1Hash(tt.infoBytes), *parsed.InfoHashV1)
			} else {
				assert.Nil(t, parsed.InfoHashV1)
			}

			if tt.wantV2 {
				require.NotNil(t, parsed.InfoHashV2)
				assert.Equal(t, v2Full(tt.infoBytes), *parsed.InfoHashV2)
			} else {
				assert.Nil(t, parsed.InfoHashV2)
			}
		})
	}
}

// TestParseMetaInfoBytesTampered flips a byte of each fixture and confirms the
// anti-poisoning hash check rejects it for v1-only, hybrid and pure-v2 alike.
func TestParseMetaInfoBytesTampered(t *testing.T) {
	t.Parallel()

	v1OnlyBytes := syntheticV1InfoBytes(t)
	hybridBytes := loadInfoBytes(t, "testdata/bittorrent-v2-hybrid-test.torrent")
	pureV2Bytes := loadInfoBytes(t, "testdata/bittorrent-v2-test.torrent")

	tests := []struct {
		name          string
		infoBytes     []byte
		discoveryHash protocol.ID
	}{
		{name: "v1-only", infoBytes: v1OnlyBytes, discoveryHash: v1Hash(v1OnlyBytes)},
		{name: "hybrid", infoBytes: hybridBytes, discoveryHash: v1Hash(hybridBytes)},
		{name: "pure-v2", infoBytes: pureV2Bytes, discoveryHash: v2Short(pureV2Bytes)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tampered := append([]byte(nil), tt.infoBytes...)
			tampered[len(tampered)-1] ^= 0xFF

			_, err := metainfo.ParseMetaInfoBytes(tt.discoveryHash, tampered)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "wrong hash")
		})
	}
}

// TestParseMetaInfoBytesWrongRequestedHash confirms valid bytes are rejected
// when the requested hash matches neither the v1 nor the truncated-v2 hash.
func TestParseMetaInfoBytesWrongRequestedHash(t *testing.T) {
	t.Parallel()

	pureV2Bytes := loadInfoBytes(t, "testdata/bittorrent-v2-test.torrent")

	var wrong protocol.ID // zero value matches nothing

	_, err := metainfo.ParseMetaInfoBytes(wrong, pureV2Bytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong hash")
}
