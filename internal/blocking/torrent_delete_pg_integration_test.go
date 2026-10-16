//go:build integration

package blocking

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	torrentDeleteParityDatabase = "bitmagnet_graphql_torrent_delete_test"
	torrentDeleteGoWriterRole   = "bitmagnet_graphql_torrent_delete_go_writer_ci"
	torrentDeleteFilterBytes    = 25_000_091

	torrentDeleteHashA = "00000000000000000000000000000000000000a1"
	torrentDeleteHashB = "00000000000000000000000000000000000000b2"
	torrentDeleteHashC = "00000000000000000000000000000000000000c3"
	torrentDeleteHashD = "00000000000000000000000000000000000000d4"
	torrentDeleteHashE = "00000000000000000000000000000000000000e5"
	torrentDeleteHashZ = "00000000000000000000000000000000000000f6"
)

type torrentDeleteParitySnapshot struct {
	Torrents       []string
	Tags           []string
	Deleted        []string
	BloomOID       uint32
	BloomOwner     string
	BloomCreatedAt time.Time
	BloomUpdatedAt time.Time
	BloomEncoded   []byte
}

func TestTorrentDeleteParityGoSeed(t *testing.T) {
	adminPool := openTorrentDeleteParityPool(
		t,
		"BITMAGNET_GRAPHQL_TORRENT_DELETE_TEST_ADMIN_DATABASE_URL",
		2,
	)
	writerPool := openTorrentDeleteParityPool(
		t,
		"BITMAGNET_GRAPHQL_TORRENT_DELETE_TEST_GO_DATABASE_URL",
		1,
	)
	requireTorrentDeleteParityDatabase(t, adminPool)
	requireTorrentDeleteParityDatabase(t, writerPool)

	ctx := context.Background()
	var writerRole string
	require.NoError(t, writerPool.QueryRow(ctx, "SELECT current_user::text").Scan(&writerRole))
	require.Equal(t, torrentDeleteGoWriterRole, writerRole)

	var blockedRows, largeObjects int64
	require.NoError(t, adminPool.QueryRow(ctx,
		"SELECT count(*)::bigint FROM bloom_filters WHERE key = $1",
		blockedTorrentsBloomFilterKey,
	).Scan(&blockedRows))
	require.NoError(t, adminPool.QueryRow(ctx,
		"SELECT count(*)::bigint FROM pg_catalog.pg_largeobject_metadata",
	).Scan(&largeObjects))
	require.Zero(t, blockedRows, "the chained parity database must start without blocking metadata")
	require.Zero(t, largeObjects, "the chained parity database must start without large objects")

	seedTorrentDeleteParityRows(t, adminPool)
	manager := newTorrentDeleteParityManager(t, writerPool)
	a := protocol.MustParseID(torrentDeleteHashA)
	b := protocol.MustParseID(torrentDeleteHashB)
	c := protocol.MustParseID(torrentDeleteHashC)
	z := protocol.MustParseID(torrentDeleteHashZ)
	require.NoError(t, manager.Block(ctx, []protocol.ID{a, a, b, z}, true))
	filtered, err := manager.Filter(ctx, []protocol.ID{a, b, z, c})
	require.NoError(t, err)
	require.Equal(t, []protocol.ID{c}, filtered)

	snapshot := readTorrentDeleteParitySnapshot(t, adminPool)
	requireTorrentDeleteParityRows(
		t,
		snapshot,
		[]string{torrentDeleteHashC, torrentDeleteHashD, torrentDeleteHashE},
		[]string{torrentDeleteHashC, torrentDeleteHashD, torrentDeleteHashE},
		[]string{torrentDeleteHashA, torrentDeleteHashB},
	)
	require.NotZero(t, snapshot.BloomOID)
	require.Equal(t, torrentDeleteGoWriterRole, snapshot.BloomOwner)
	require.Len(t, snapshot.BloomEncoded, torrentDeleteFilterBytes)
	require.Equal(t, snapshot.BloomCreatedAt, snapshot.BloomUpdatedAt)
}

func TestTorrentDeleteParityGoVerifyAfterRust(t *testing.T) {
	adminPool := openTorrentDeleteParityPool(
		t,
		"BITMAGNET_GRAPHQL_TORRENT_DELETE_TEST_ADMIN_DATABASE_URL",
		2,
	)
	writerPool := openTorrentDeleteParityPool(
		t,
		"BITMAGNET_GRAPHQL_TORRENT_DELETE_TEST_GO_DATABASE_URL",
		1,
	)
	requireTorrentDeleteParityDatabase(t, adminPool)
	requireTorrentDeleteParityDatabase(t, writerPool)

	before := readTorrentDeleteParitySnapshot(t, adminPool)
	requireTorrentDeleteParityRows(
		t,
		before,
		[]string{torrentDeleteHashE},
		[]string{torrentDeleteHashE},
		[]string{torrentDeleteHashA, torrentDeleteHashB, torrentDeleteHashC, torrentDeleteHashD},
	)
	require.Equal(t, torrentDeleteGoWriterRole, before.BloomOwner)
	require.Len(t, before.BloomEncoded, torrentDeleteFilterBytes)

	manager := newTorrentDeleteParityManager(t, writerPool)
	c := protocol.MustParseID(torrentDeleteHashC)
	d := protocol.MustParseID(torrentDeleteHashD)
	e := protocol.MustParseID(torrentDeleteHashE)
	filtered, err := manager.Filter(context.Background(), []protocol.ID{c, d, e, e})
	require.NoError(t, err)
	require.Equal(t, []protocol.ID{e, e}, filtered)

	after := readTorrentDeleteParitySnapshot(t, adminPool)
	require.Equal(t, before, after,
		"a fresh production Go manager must decode and re-encode Rust state without drift")
}

func openTorrentDeleteParityPool(t *testing.T, envName string, maxConnections int32) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(envName)
	if dsn == "" {
		t.Skipf("%s is not set", envName)
	}
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	config.MaxConns = maxConnections
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func requireTorrentDeleteParityDatabase(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var databaseName string
	require.NoError(t, pool.QueryRow(context.Background(),
		"SELECT current_database()::text",
	).Scan(&databaseName))
	require.Equal(t, torrentDeleteParityDatabase, databaseName,
		"torrent-delete parity refuses any database without its exact disposable-name sentinel")
}

func newTorrentDeleteParityManager(t *testing.T, writerPool *pgxpool.Pool) Manager {
	t.Helper()
	wait := &sync.WaitGroup{}
	result := New(Params{
		Pool: lazy.New(func() (*pgxpool.Pool, error) {
			return writerPool, nil
		}),
		PgxPoolWait: wait,
	})
	manager, err := result.Manager.Get()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, result.AppHook.OnStop(context.Background()))
		wait.Wait()
	})
	return manager
}

func seedTorrentDeleteParityRows(t *testing.T, adminPool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for index, rawHash := range []string{
		torrentDeleteHashA,
		torrentDeleteHashB,
		torrentDeleteHashC,
		torrentDeleteHashD,
		torrentDeleteHashE,
	} {
		hash := protocol.MustParseID(rawHash)
		_, err := adminPool.Exec(ctx,
			`INSERT INTO torrents (info_hash, name, size, private, created_at, updated_at)
			 VALUES ($1, $2, 0, false, $3, $3)`,
			hash[:], rawHash, time.Date(2026, time.August, 27, 12, index, 0, 0, time.UTC),
		)
		require.NoError(t, err)
		_, err = adminPool.Exec(ctx,
			`INSERT INTO torrent_tags (info_hash, name, created_at, updated_at)
			 VALUES ($1, $2, $3, $3)`,
			hash[:], "delete-parity", time.Date(2026, time.August, 27, 13, index, 0, 0, time.UTC),
		)
		require.NoError(t, err)
	}
}

func readTorrentDeleteParitySnapshot(t *testing.T, adminPool *pgxpool.Pool) torrentDeleteParitySnapshot {
	t.Helper()
	ctx := context.Background()
	snapshot := torrentDeleteParitySnapshot{
		Torrents: readTorrentDeleteParityHashes(t, adminPool,
			"SELECT encode(info_hash, 'hex') FROM torrents ORDER BY info_hash"),
		Tags: readTorrentDeleteParityHashes(t, adminPool,
			"SELECT encode(info_hash, 'hex') FROM torrent_tags ORDER BY info_hash"),
		Deleted: readTorrentDeleteParityHashes(t, adminPool,
			"SELECT encode(info_hash, 'hex') FROM deleted_torrents ORDER BY info_hash"),
	}
	require.NoError(t, adminPool.QueryRow(ctx,
		`SELECT bf.oid, r.rolname::text, bf.created_at, bf.updated_at,
		        pg_catalog.lo_get(bf.oid, 0::bigint, $2::integer)
		 FROM bloom_filters bf
		 JOIN pg_catalog.pg_largeobject_metadata lom ON lom.oid = bf.oid
		 JOIN pg_catalog.pg_roles r ON r.oid = lom.lomowner
		 WHERE bf.key = $1`,
		blockedTorrentsBloomFilterKey, torrentDeleteFilterBytes+1,
	).Scan(
		&snapshot.BloomOID,
		&snapshot.BloomOwner,
		&snapshot.BloomCreatedAt,
		&snapshot.BloomUpdatedAt,
		&snapshot.BloomEncoded,
	))
	return snapshot
}

func readTorrentDeleteParityHashes(
	t *testing.T,
	adminPool *pgxpool.Pool,
	query string,
) []string {
	t.Helper()
	rows, err := adminPool.Query(context.Background(), query)
	require.NoError(t, err)
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var hash string
		require.NoError(t, rows.Scan(&hash))
		result = append(result, hash)
	}
	require.NoError(t, rows.Err())
	return result
}

func requireTorrentDeleteParityRows(
	t *testing.T,
	snapshot torrentDeleteParitySnapshot,
	wantTorrents []string,
	wantTags []string,
	wantDeleted []string,
) {
	t.Helper()
	require.Equal(t, wantTorrents, snapshot.Torrents)
	require.Equal(t, wantTags, snapshot.Tags)
	require.Equal(t, wantDeleted, snapshot.Deleted)
}
