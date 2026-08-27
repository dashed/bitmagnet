//go:build integration

package parity

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestQueueJobArchivalDurationRepresentation exercises the same full-model
// hydration used by queue/server.handleJob. PostgreSQL intervals preserve
// months, days, and time independently; model.Duration.Scan supports only the
// default hh:mm:ss text emitted by a time-component interval. The temporary
// table and single connection keep the fixture session-local for any test DSN.
func TestQueueJobArchivalDurationRepresentation(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping queue duration PostgreSQL regression")
	}

	ctx := context.Background()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open fixture PostgreSQL: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("unwrap fixture PostgreSQL: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })

	if err := db.WithContext(ctx).Connection(func(tx *gorm.DB) error {
		assertQueueJobArchivalDurationRepresentation(t, tx)
		return nil
	}); err != nil {
		t.Fatalf("reserve fixture PostgreSQL connection: %v", err)
	}
}

func assertQueueJobArchivalDurationRepresentation(t *testing.T, db *gorm.DB) {
	t.Helper()

	if err := db.Exec("SET intervalstyle TO postgres").Error; err != nil {
		t.Fatalf("set deterministic interval style: %v", err)
	}
	if err := db.Exec(`CREATE TEMP TABLE queue_jobs (
		id text NOT NULL PRIMARY KEY,
		fingerprint text NOT NULL,
		queue text NOT NULL,
		status text NOT NULL,
		payload jsonb NOT NULL,
		retries integer NOT NULL DEFAULT 0,
		max_retries integer NOT NULL DEFAULT 0,
		run_after timestamptz NOT NULL,
		ran_at timestamptz,
		error text,
		deadline timestamptz,
		archival_duration interval NOT NULL,
		created_at timestamptz NOT NULL,
		priority integer NOT NULL DEFAULT 0
	)`).Error; err != nil {
		t.Fatalf("create private queue_jobs table: %v", err)
	}
	if err := db.Exec(`INSERT INTO queue_jobs
		(id, fingerprint, queue, status, payload, run_after, archival_duration, created_at)
		VALUES
		('safe-seconds', 'safe-seconds', 'process_torrent_batch', 'pending', '{}', now(),
		 make_interval(secs => 604800), now()),
		('unsafe-days', 'unsafe-days', 'process_torrent_batch', 'pending', '{}', now(),
		 make_interval(days => 7), now())`).Error; err != nil {
		t.Fatalf("seed duration representations: %v", err)
	}

	var safeText, unsafeText string
	if err := db.Raw(
		`SELECT
		 max(archival_duration::text) FILTER (WHERE id = 'safe-seconds'),
		 max(archival_duration::text) FILTER (WHERE id = 'unsafe-days')
		 FROM queue_jobs`,
	).Row().Scan(&safeText, &unsafeText); err != nil {
		t.Fatalf("read interval representations: %v", err)
	}
	if safeText != "168:00:00" || unsafeText != "7 days" {
		t.Fatalf("interval text = (%q, %q), want (%q, %q)", safeText, unsafeText, "168:00:00", "7 days")
	}

	query := dao.Use(db)
	safe, err := query.QueueJob.Where(query.QueueJob.ID.Eq("safe-seconds")).First()
	if err != nil {
		t.Fatalf("hydrate safe full queue model: %v", err)
	}
	if got, want := time.Duration(safe.ArchivalDuration), 7*24*time.Hour; got != want {
		t.Fatalf("safe archival duration = %s, want %s", got, want)
	}

	_, err = query.QueueJob.Where(query.QueueJob.ID.Eq("unsafe-days")).First()
	if err == nil {
		t.Fatal("day-component interval unexpectedly hydrated through model.Duration.Scan")
	}
	const knownFailure = `time: unknown unit " dayss" in duration "7 dayss"`
	if !strings.Contains(err.Error(), knownFailure) {
		t.Fatalf("unsafe hydration error = %q, want substring %q", err, knownFailure)
	}
}
