//go:build integration

package parity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor/batch"
	batchqueue "github.com/bitmagnet-io/bitmagnet/internal/processor/batch/queue"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const queueBatchSelectionSubsystem = "process_torrent_batch_selection_pg"

type queueBatchSelectionSeed struct {
	InfoHash     string    `json:"infoHash"`
	UpdatedAt    time.Time `json:"updatedAt"`
	ContentTypes []*string `json:"contentTypes"`
}

type queueBatchSelectionCase struct {
	ID        string          `json:"id"`
	Selection batch.Selection `json:"selection"`
}

type queueBatchSelectionInput struct {
	Seed  []queueBatchSelectionSeed `json:"seed"`
	Cases []queueBatchSelectionCase `json:"cases"`
}

type queueBatchSelectionResult struct {
	ID         string        `json:"id"`
	InfoHashes []protocol.ID `json:"infoHashes"`
}

type queueBatchSelectionExpected struct {
	Results []queueBatchSelectionResult `json:"results"`
}

func batchSelectionHash(value uint16) protocol.ID {
	return protocol.MustParseID(fmt.Sprintf("%040x", value))
}

func nullableContentType(value *string) model.NullContentType {
	if value == nil {
		return model.NewNullContentType(nil)
	}
	return model.NewNullContentType(*value)
}

func queueBatchSelectionCorpus() queueBatchSelectionInput {
	old := time.Date(2026, time.August, 11, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, time.August, 12, 0, 0, 0, 0, time.UTC)
	future := time.Date(2026, time.August, 13, 0, 0, 0, 0, time.UTC)
	movie, tvShow := "movie", "tv_show"
	seed := []queueBatchSelectionSeed{
		{batchSelectionHash(1).String(), old, []*string{&movie}},
		{batchSelectionHash(2).String(), old, []*string{nil}},
		{batchSelectionHash(3).String(), old, []*string{&movie, &tvShow}},
		{batchSelectionHash(4).String(), cutoff, []*string{&movie}},
		{batchSelectionHash(5).String(), old, []*string{}},
		// Decoy NULL row catches an uncorrelated `OR content_type IS NULL`.
		{batchSelectionHash(6).String(), future, []*string{nil}},
		{batchSelectionHash(255).String(), old, []*string{}},
		{batchSelectionHash(256).String(), old, []*string{}},
	}
	selection := func(after uint16, contentTypes []model.NullContentType, orphans bool, limit uint) batch.Selection {
		if contentTypes == nil {
			contentTypes = []model.NullContentType{}
		}
		return batch.Selection{
			AfterExclusive: batchSelectionHash(after),
			UpdatedBefore:  cutoff,
			ContentTypes:   contentTypes,
			Orphans:        orphans,
			OrderBy:        "info_hash_asc",
			Limit:          limit,
		}
	}
	return queueBatchSelectionInput{Seed: seed, Cases: []queueBatchSelectionCase{
		{"cursor_limit_order", selection(0, nil, false, 2)},
		{"strict_cutoff_short_page", selection(3, nil, false, 10)},
		{"content_movie_duplicate_rows", selection(0, []model.NullContentType{nullableContentType(&movie)}, false, 10)},
		{"content_null_only", selection(0, []model.NullContentType{nullableContentType(nil)}, false, 10)},
		{"content_movie_or_null_correlated", selection(0, []model.NullContentType{nullableContentType(&movie), nullableContentType(nil)}, false, 10)},
		{"orphans", selection(0, nil, true, 10)},
		{"content_and_orphan_is_empty", selection(0, []model.NullContentType{nullableContentType(&movie), nullableContentType(nil)}, true, 10)},
		{"byte_order_ff_before_0100", selection(255, nil, false, 10)},
		{"empty_page", selection(256, nil, false, 10)},
	}}
}

func TestGenerateQueueBatchSelectionPgFixture(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping batch selection fixture generation")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open fixture PostgreSQL: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("open fixture sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)

	ctx := context.Background()
	const schema = "phase3_queue_batch_selection"
	if err := db.WithContext(ctx).Exec(
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE; CREATE SCHEMA " + schema + "; SET search_path TO " + schema,
	).Error; err != nil {
		t.Fatalf("reset parity schema: %v", err)
	}
	for _, statement := range []string{
		`CREATE TABLE torrents (info_hash bytea PRIMARY KEY, updated_at timestamptz NOT NULL)`,
		`CREATE TABLE torrent_contents (info_hash bytea NOT NULL, content_type text NULL)`,
	} {
		if err := db.WithContext(ctx).Exec(statement).Error; err != nil {
			t.Fatalf("apply selector DDL: %v", err)
		}
	}

	input := queueBatchSelectionCorpus()
	for _, torrent := range input.Seed {
		if err := db.WithContext(ctx).Exec(
			`INSERT INTO torrents (info_hash, updated_at) VALUES (decode(?, 'hex'), ?)`,
			torrent.InfoHash, torrent.UpdatedAt,
		).Error; err != nil {
			t.Fatalf("seed torrent %s: %v", torrent.InfoHash, err)
		}
		for _, contentType := range torrent.ContentTypes {
			if err := db.WithContext(ctx).Exec(
				`INSERT INTO torrent_contents (info_hash, content_type) VALUES (decode(?, 'hex'), ?)`,
				torrent.InfoHash, contentType,
			).Error; err != nil {
				t.Fatalf("seed torrent content %s: %v", torrent.InfoHash, err)
			}
		}
	}

	selector := batchqueue.NewPostgresSelector(dao.Use(db))
	expected := queueBatchSelectionExpected{Results: make([]queueBatchSelectionResult, 0, len(input.Cases))}
	for _, scenario := range input.Cases {
		first, err := selector.Select(ctx, scenario.Selection)
		if err != nil {
			t.Fatalf("select scenario %s: %v", scenario.ID, err)
		}
		second, err := selector.Select(ctx, scenario.Selection)
		if err != nil {
			t.Fatalf("repeat scenario %s: %v", scenario.ID, err)
		}
		if fmt.Sprint(first) != fmt.Sprint(second) {
			t.Fatalf("scenario %s is nondeterministic: %v vs %v", scenario.ID, first, second)
		}
		expected.Results = append(expected.Results, queueBatchSelectionResult{scenario.ID, first})
	}

	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	reconcileQueueFixtures(t, "process_torrent_batch_selection.jsonl", []Fixture{{
		ID:        "postgres_batch_selection",
		Subsystem: queueBatchSelectionSubsystem,
		Input:     inputJSON,
		Expected:  expectedJSON,
	}})
}
