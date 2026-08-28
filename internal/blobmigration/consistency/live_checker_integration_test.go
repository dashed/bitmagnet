//go:build integration

package consistency

import (
	"context"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestHealTorrentClearsSummaryCompressedBytes verifies the repair path nulls the
// summary's compressed_bytes together with files_data, so a covered summary row
// can't keep a stale byte length for a blob that no longer exists.
func TestHealTorrentClearsSummaryCompressedBytes(t *testing.T) {
	t.Cleanup(func() { search.SetFeatureFlags(search.FeatureFlags{}) })
	// Default flags => AllowTorrentFilesRepair() is true => repair runs.
	search.SetFeatureFlags(search.FeatureFlags{})

	db := setupIntegrationDB(t)
	q := dao.Use(db)
	ctx := context.Background()

	infoHash := hashFromBytes(0x42, 0x13)
	now := time.Now()

	require.NoError(t, db.Exec(
		"INSERT INTO torrents (info_hash, name, size, private, files_status, created_at, updated_at, files_data) "+
			"VALUES (?, 't', 0, false, 'multi', ?, ?, decode('01020304', 'hex'))",
		infoHash, now, now,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO torrent_file_summary (info_hash, file_count, compressed_bytes, created_at, updated_at) "+
			"VALUES (?, 2, 4, ?, ?)",
		infoHash, now, now,
	).Error)

	lc := &LiveChecker{dao: q, logger: zap.NewNop().Sugar()}
	lc.healTorrent(ctx, infoHash)

	// Assert via IS NULL booleans: gorm can't scan a bytea column into []byte.
	var filesDataNull bool
	require.NoError(t, db.Raw(
		"SELECT files_data IS NULL FROM torrents WHERE info_hash = ?", infoHash[:],
	).Scan(&filesDataNull).Error)
	require.True(t, filesDataNull, "files_data must be cleared for re-migration")

	var compressedBytesNull bool
	require.NoError(t, db.Raw(
		"SELECT compressed_bytes IS NULL FROM torrent_file_summary WHERE info_hash = ?", infoHash[:],
	).Scan(&compressedBytesNull).Error)
	require.True(t, compressedBytesNull, "summary compressed_bytes must be nulled in the same repair tx")
}
