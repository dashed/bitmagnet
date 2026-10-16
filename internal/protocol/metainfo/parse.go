package metainfo

import (
	"errors"
	"fmt"

	"github.com/anacrolix/torrent/bencode"
	mi "github.com/anacrolix/torrent/metainfo"
	infohashv2 "github.com/anacrolix/torrent/types/infohash-v2"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

// ParsedInfo is the result of parsing and verifying a raw info dictionary: the
// decoded info plus its BitTorrent v1/v2 (BEP 52) identity.
type ParsedInfo struct {
	Info Info
	// MetaVersion is 1 for v1-only torrents and 2 for v2 / hybrid torrents.
	MetaVersion uint8
	// InfoHashV1 is the 20-byte SHA-1 info hash; set for v1-only and hybrid
	// torrents, nil for pure-v2.
	InfoHashV1 *protocol.ID
	// InfoHashV2 is the full 32-byte SHA-256 info hash; set for hybrid and
	// pure-v2 torrents, nil for v1-only.
	InfoHashV2 *protocol.InfoHashV2
}

// ParseMetaInfoBytes decodes and verifies a raw info dictionary received for the
// given (20-byte) info hash.
//
// The bytes are accepted only if they hash to the requested info hash under
// either the v1 (SHA-1) or the truncated v2 (SHA-256) scheme — preserving the
// anti-poisoning property while no longer silently dropping BitTorrent v2 /
// hybrid (BEP 52) torrents, which previously failed the SHA-1-only check.
func ParseMetaInfoBytes(infoHash protocol.ID, metaInfoBytes []byte) (ParsedInfo, error) {
	// Hash the raw received bytes (never a re-marshal) under both schemes and
	// require a match against the requested hash BEFORE trusting the contents.
	v1 := protocol.ID(mi.HashBytes(metaInfoBytes))
	v2 := infohashv2.HashBytes(metaInfoBytes)
	v2Short := protocol.ID(*v2.ToShort())

	matchedV1 := v1 == infoHash
	matchedV2 := v2Short == infoHash

	if !matchedV1 && !matchedV2 {
		return ParsedInfo{}, errors.New("info bytes have wrong hash")
	}

	var info Info
	if unmarshalErr := bencode.Unmarshal(metaInfoBytes, &info); unmarshalErr != nil {
		return ParsedInfo{}, fmt.Errorf("error unmarshaling info bytes: %w", unmarshalErr)
	}

	parsed := ParsedInfo{
		Info:        info,
		MetaVersion: 1,
	}

	if info.HasV1() {
		hashV1 := v1
		parsed.InfoHashV1 = &hashV1
	}

	if info.HasV2() {
		hashV2 := protocol.InfoHashV2(v2)
		parsed.InfoHashV2 = &hashV2
		parsed.MetaVersion = 2
	}

	return parsed, nil
}
