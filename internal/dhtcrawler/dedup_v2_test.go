package dhtcrawler

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	pmetainfo "github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/stretchr/testify/assert"
)

func mkV2(b byte) protocol.InfoHashV2 {
	var h protocol.InfoHashV2
	for i := range h {
		h[i] = b
	}

	return h
}

func mkPK(b byte) protocol.ID {
	var id protocol.ID
	for i := range id {
		id[i] = b
	}

	return id
}

func mkItem(pkByte byte, v2 *protocol.InfoHashV2) infoHashWithMetaInfo {
	return infoHashWithMetaInfo{
		nodeHasPeersForHash: nodeHasPeersForHash{infoHash: mkPK(pkByte)},
		metaInfo:            pmetainfo.ParsedInfo{InfoHashV2: v2},
	}
}

func TestDropV2Duplicate(t *testing.T) {
	t.Parallel()

	v := mkV2(0xAA)
	a := mkPK(0x01)
	b := mkPK(0x02)

	tests := []struct {
		name            string
		existing, batch map[protocol.InfoHashV2]protocol.ID
		pk              protocol.ID
		want            bool
	}{
		{"empty maps", nil, nil, a, false},
		{"existing same pk (legit upsert)", map[protocol.InfoHashV2]protocol.ID{v: a}, nil, a, false},
		{"existing different pk (drop)", map[protocol.InfoHashV2]protocol.ID{v: a}, nil, b, true},
		{"batch same pk", nil, map[protocol.InfoHashV2]protocol.ID{v: a}, a, false},
		{"batch different pk (drop)", nil, map[protocol.InfoHashV2]protocol.ID{v: a}, b, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, dropV2Duplicate(v, tt.pk, tt.existing, tt.batch))
		})
	}
}

func TestFilterV2Duplicates(t *testing.T) {
	t.Parallel()

	v := mkV2(0xAA)
	w := mkV2(0xBB)

	t.Run("hybrid discovered twice in one batch drops the second", func(t *testing.T) {
		t.Parallel()

		is := []infoHashWithMetaInfo{mkItem(0x01, &v), mkItem(0x02, &v)}
		kept, dropped := filterV2Duplicates(is, nil)

		assert.Equal(t, 1, dropped)
		assert.Len(t, kept, 1)
		assert.Equal(t, mkPK(0x01), kept[0].infoHash, "first-one-wins")
	})

	t.Run("pre-existing DB row drops the new cross-pk discovery", func(t *testing.T) {
		t.Parallel()

		is := []infoHashWithMetaInfo{mkItem(0x02, &v)}
		kept, dropped := filterV2Duplicates(is, map[protocol.InfoHashV2]protocol.ID{v: mkPK(0x01)})

		assert.Equal(t, 1, dropped)
		assert.Empty(t, kept)
	})

	t.Run("same-pk re-discovery is kept (must upsert)", func(t *testing.T) {
		t.Parallel()

		is := []infoHashWithMetaInfo{mkItem(0x01, &v), mkItem(0x01, &v)}
		kept, dropped := filterV2Duplicates(is, nil)

		// Not a cross-PK collision; the downstream hashMap handles the exact-PK repeat.
		assert.Equal(t, 0, dropped)
		assert.Len(t, kept, 2)
	})

	t.Run("v1-only torrents are never dropped", func(t *testing.T) {
		t.Parallel()

		is := []infoHashWithMetaInfo{mkItem(0x01, nil), mkItem(0x02, nil)}
		kept, dropped := filterV2Duplicates(is, nil)

		assert.Equal(t, 0, dropped)
		assert.Len(t, kept, 2)
	})

	t.Run("distinct torrents are both kept", func(t *testing.T) {
		t.Parallel()

		is := []infoHashWithMetaInfo{mkItem(0x01, &v), mkItem(0x02, &w)}
		kept, dropped := filterV2Duplicates(is, nil)

		assert.Equal(t, 0, dropped)
		assert.Len(t, kept, 2)
	})
}
