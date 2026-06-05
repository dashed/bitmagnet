package model

import (
	"strings"
	"testing"

	infohashv2 "github.com/anacrolix/torrent/types/infohash-v2"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMagnetURI(t *testing.T) {
	t.Parallel()

	v1 := protocol.MustParseID("0123456789abcdef0123456789abcdef01234567")

	var v2 protocol.InfoHashV2
	for i := range v2 {
		v2[i] = byte(i)
	}

	v2Topic := "urn:btmh:" + infohashv2.ToMultihash(infohashv2.T(v2)).HexString()
	// sha2-256 multihash is 0x12 (code) 0x20 (length) followed by the 32-byte hash.
	require.True(t, strings.HasPrefix(infohashv2.ToMultihash(infohashv2.T(v2)).HexString(), "1220"))

	tests := []struct {
		name        string
		torrent     Torrent
		wantBtih    bool
		wantBtmh    bool
		wantPK      protocol.ID // PK that must NOT leak as a btih topic
		forbidPKBih bool
	}{
		{
			name:     "v1-only",
			torrent:  Torrent{InfoHash: v1, InfoHashV1: &v1, Name: "ubuntu.iso", Size: 100},
			wantBtih: true,
			wantBtmh: false,
		},
		{
			name:     "hybrid",
			torrent:  Torrent{InfoHash: v1, InfoHashV1: &v1, InfoHashV2: &v2, Name: "hybrid", Size: 200},
			wantBtih: true,
			wantBtmh: true,
		},
		{
			// pure-v2: PK is the truncated SHA-256; it must NOT be emitted as a btih.
			name:        "pure-v2",
			torrent:     Torrent{InfoHash: v2.ToShort(), InfoHashV2: &v2, Name: "v2only", Size: 300},
			wantBtih:    false,
			wantBtmh:    true,
			wantPK:      v2.ToShort(),
			forbidPKBih: true,
		},
		{
			// legacy / importer row with no v2 columns populated: fall back to PK.
			name:     "legacy (no v2 columns)",
			torrent:  Torrent{InfoHash: v1, Name: "legacy", Size: 400},
			wantBtih: true,
			wantBtmh: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri := tt.torrent.MagnetURI()

			assert.True(t, strings.HasPrefix(uri, "magnet:?"))
			assert.Contains(t, uri, "&dn=")
			assert.Contains(t, uri, "&xl=")

			if tt.wantBtih {
				assert.Contains(t, uri, "xt=urn:btih:")
			} else {
				assert.NotContains(t, uri, "urn:btih:", "pure-v2 must not emit a btih topic")
			}

			if tt.wantBtmh {
				assert.Contains(t, uri, v2Topic)
			} else {
				assert.NotContains(t, uri, "urn:btmh:")
			}

			if tt.forbidPKBih {
				assert.NotContains(t, uri, "urn:btih:"+tt.wantPK.String(),
					"the truncated-SHA256 PK must never appear as a v1 btih topic")
			}
		})
	}
}
