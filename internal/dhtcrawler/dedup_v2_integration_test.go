//go:build integration

package dhtcrawler

import (
	"context"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	pmetainfo "github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// newDedupTestCrawler builds the minimal crawler that lookupExistingV2 and the
// drop-metric path need: a dao backed by the test DB, a no-op logger, and the
// torrents_dropped counter. No pipeline goroutines are started — these tests
// exercise lookupExistingV2 + filterV2Duplicates directly, not the full channel
// machinery (deliberately, per the spec: no fragile full-crawler harness).
func newDedupTestCrawler(db *gorm.DB) *crawler {
	return &crawler{
		dao:    dao.Use(db),
		logger: zap.NewNop().Sugar(),
		torrentsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bitmagnet",
			Subsystem: "dht_crawler",
			Name:      "torrents_dropped_total",
			Help:      "A counter of torrents dropped before persistence, by reason.",
		}, []string{"reason"}),
	}
}

// hybridIdentities loads the hybrid fixture and returns its three DHT identities:
// A  — the 20-byte v1 SHA-1 (the primary key when discovered via v1),
// V  — the full 32-byte v2 SHA-256 (the dedup key, stored in info_hash_v2),
// Bt — the truncated v2 (first 20 bytes of V; the primary key when discovered
//
//	via the BEP 52 truncated hash on the DHT),
//
// along with the ParsedInfo produced by the real verifier.
func hybridIdentities(t *testing.T) (a protocol.ID, v protocol.InfoHashV2, bt protocol.ID, parsed pmetainfo.ParsedInfo) {
	t.Helper()

	hash, p := loadFixtureParsed(t, "testdata/bittorrent-v2-hybrid-test.torrent", false)
	require.NotNil(t, p.InfoHashV2, "hybrid fixture must carry a full v2 hash")

	a = hash
	v = *p.InfoHashV2
	bt = v.ToShort()

	require.NotEqual(t, a, bt, "a hybrid's v1 and truncated-v2 PKs must differ")

	return a, v, bt, p
}

// itemDiscoveredAs builds the channel item the crawler would produce for a
// discovery under primary key pk carrying full v2 identity v.
func itemDiscoveredAs(pk protocol.ID, parsed pmetainfo.ParsedInfo) infoHashWithMetaInfo {
	return infoHashWithMetaInfo{
		nodeHasPeersForHash: nodeHasPeersForHash{infoHash: pk},
		metaInfo:            parsed,
	}
}

// TestV2DedupExistingDBDropsCrossPK: a hybrid already stored under its v1 PK (A)
// is re-discovered on the DHT under its truncated-v2 PK (Bt). lookupExistingV2
// must map V -> A from the DB, and filterV2Duplicates must then drop the Bt
// discovery (cross-PK collision, first-one-wins keeps A).
func TestV2DedupExistingDBDropsCrossPK(t *testing.T) {
	db := setupV2TestDB(t)
	ctx := context.Background()

	a, v, bt, parsed := hybridIdentities(t)

	// Seed the DB with the row keyed by A (info_hash_v2 = V).
	existingRow, err := createTorrentModel(a, parsed, false, 1000)
	require.NoError(t, err)
	require.NoError(t, insertTorrentModel(db, &existingRow))

	c := newDedupTestCrawler(db)

	// The incoming batch is the SECOND discovery, under the truncated-v2 PK.
	batch := []infoHashWithMetaInfo{itemDiscoveredAs(bt, parsed)}

	existing := c.lookupExistingV2(ctx, batch)
	require.Len(t, existing, 1, "lookup should find the pre-existing v2 row")
	gotPK, ok := existing[v]
	require.True(t, ok, "lookup must be keyed by the full v2 hash V")
	assert.Equal(t, a, gotPK, "stored PK for V must be A (the first-discovered row)")

	kept, dropped := filterV2Duplicates(batch, existing)
	assert.Equal(t, 1, dropped, "the cross-PK Bt discovery must be dropped")
	assert.Empty(t, kept, "nothing survives — the DB already holds this v2 identity")
}

// TestV2DedupNoFalsePositiveEmptyDB: with an empty torrents table the lookup
// returns nothing, so a legitimate first discovery is kept (no false drop).
func TestV2DedupNoFalsePositiveEmptyDB(t *testing.T) {
	db := setupV2TestDB(t)
	ctx := context.Background()

	a, _, _, parsed := hybridIdentities(t)

	c := newDedupTestCrawler(db)

	batch := []infoHashWithMetaInfo{itemDiscoveredAs(a, parsed)}

	existing := c.lookupExistingV2(ctx, batch)
	assert.Empty(t, existing, "empty DB must yield no pre-existing v2 rows")

	kept, dropped := filterV2Duplicates(batch, existing)
	assert.Equal(t, 0, dropped, "a first discovery must never be dropped")
	require.Len(t, kept, 1)
	assert.Equal(t, a, kept[0].infoHash)
}

// TestV2DedupSamePKUpsertNotDropped: a row already exists under PK Bt (v2 = V),
// and a new discovery arrives under the SAME PK Bt. lookupExistingV2 returns
// V -> Bt; filterV2Duplicates must KEEP it (stored == pk), so the legitimate
// metadata-refresh upsert is preserved. This is the path a pure-v2 re-discovery
// also takes (one truncated PK), and it must not be mistaken for a collision.
func TestV2DedupSamePKUpsertNotDropped(t *testing.T) {
	db := setupV2TestDB(t)
	ctx := context.Background()

	_, v, bt, parsed := hybridIdentities(t)

	// Seed a row keyed by Bt (info_hash_v2 = V).
	existingRow, err := createTorrentModel(bt, parsed, false, 1000)
	require.NoError(t, err)
	require.NoError(t, insertTorrentModel(db, &existingRow))

	c := newDedupTestCrawler(db)

	// Same-PK re-discovery.
	batch := []infoHashWithMetaInfo{itemDiscoveredAs(bt, parsed)}

	existing := c.lookupExistingV2(ctx, batch)
	require.Len(t, existing, 1)
	assert.Equal(t, bt, existing[v], "stored PK for V is Bt itself")

	kept, dropped := filterV2Duplicates(batch, existing)
	assert.Equal(t, 0, dropped, "same-PK re-discovery must upsert, not drop")
	require.Len(t, kept, 1)
	assert.Equal(t, bt, kept[0].infoHash)
}

// TestV2DedupCounterIncrements wires the lookup + filter to the metric exactly as
// runPersistTorrents does, asserting torrents_dropped_total{reason="v2_duplicate"}
// reflects the number dropped. Driving the full channel pipeline is intentionally
// out of scope (the unit tests + the lookup integration tests above already cover
// the logic); this just locks the metric/label contract end-to-end against the DB.
func TestV2DedupCounterIncrements(t *testing.T) {
	db := setupV2TestDB(t)
	ctx := context.Background()

	a, _, bt, parsed := hybridIdentities(t)

	existingRow, err := createTorrentModel(a, parsed, false, 1000)
	require.NoError(t, err)
	require.NoError(t, insertTorrentModel(db, &existingRow))

	c := newDedupTestCrawler(db)

	batch := []infoHashWithMetaInfo{itemDiscoveredAs(bt, parsed)}

	existing := c.lookupExistingV2(ctx, batch)
	_, dropped := filterV2Duplicates(batch, existing)
	if dropped > 0 {
		c.torrentsDropped.WithLabelValues("v2_duplicate").Add(float64(dropped))
	}

	got := testutil.ToFloat64(c.torrentsDropped.WithLabelValues("v2_duplicate"))
	assert.Equal(t, float64(1), got,
		`torrents_dropped_total{reason="v2_duplicate"} must equal the dropped count`)
}
