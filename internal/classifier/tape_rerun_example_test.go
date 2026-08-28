package classifier

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
)

var updateTapeRerunExample = flag.Bool(
	"update-tape-rerun-example",
	false,
	"regenerate testdata/parity/classifier-tape-rerun/example/tape",
)

const tapeRerunExampleDir = "../../testdata/parity/classifier-tape-rerun/example/tape"

// TestTapeRerunExampleGolden generates a traced, processor-state-aware tape
// with real core-workflow executions and valid info hashes. The processor
// packages on both sides use it as their cross-language report oracle.
func TestTapeRerunExampleGolden(t *testing.T) {
	source, err := yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}.source()
	if err != nil {
		t.Fatalf("load core classifier source: %v", err)
	}
	digest, err := EffectiveConfigDigest(source, "default")
	if err != nil {
		t.Fatalf("effective config digest: %v", err)
	}

	searchBody, detailsBody := tmdbBodies(t, 11224, "Cinderella")
	findBody, err := json.Marshal(map[string]any{
		"movie_results": []map[string]any{{"id": 11224}},
	})
	if err != nil {
		t.Fatalf("encode TMDB find response: %v", err)
	}
	requester := stubRequester{bodies: map[string][]byte{
		"/search/movie":        searchBody,
		"/find/tt0042332":      findBody,
		"/find/tt0042332-copy": findBody,
		"/movie/11224":         detailsBody,
	}}
	byIDContent := testContent("11224", "Cinderella", 1950)
	byIDContent.Attributes = []model.ContentAttribute{{
		ContentType:   model.ContentTypeMovie,
		ContentSource: model.SourceTmdb,
		ContentID:     "11224",
		Source:        model.SourceImdb,
		Key:           "id",
		Value:         "tt0042332",
	}}
	contentSearch := &stubContentSearch{results: []search.ContentResult{
		tiedResult("Cinderella", "Cinderella (1950)", "Cinderella"),
		{},
		{Items: []search.ContentResultItem{{Content: byIDContent}}},
		{},
	}}
	recorder := tape.NewRecorder(digest, 0, tape.Provenance{
		Command:               "go test ./internal/classifier -run TestTapeRerunExampleGolden -update-tape-rerun-example",
		Host:                  "fixture",
		Database:              "none (fixtures, not a database)",
		AcquisitionPlanDigest: tapePlanFixtureDigest,
		Notes:                 "Generated cross-language same-input, same-observation write-set fixture.",
	})
	runner := newTapeRunner(t, localSearchDeps(contentSearch, requester), recorder)

	fixtures := []struct {
		subject string
		flags   Flags
		torrent model.Torrent
	}{
		{
			subject: "0000000000000000000000000000000000000001",
			flags:   testFlagsOn,
			torrent: tapeRerunTorrent(
				"Cinderella.1950.1080p.BluRay.x264-GROUP",
				"stale:local:z",
				"stale:local:a",
			),
		},
		{
			subject: "0000000000000000000000000000000000000002",
			flags:   testFlagsOn,
			torrent: tapeRerunTorrent(
				"Cinderella.1950.720p.WEB-DL.h264",
				"stale:tmdb",
			),
		},
		{
			subject: "0000000000000000000000000000000000000003",
			flags:   testFlagsOff,
			torrent: tapeRerunTorrent("Cinderella.1950.2160p.UHD.BluRay.x265"),
		},
		{
			subject: "0000000000000000000000000000000000000004",
			flags:   testFlagsOn,
			torrent: tapeRerunTorrent("Synthetic pthc marker.txt"),
		},
		{
			subject: "0000000000000000000000000000000000000005",
			flags:   testFlagsOn,
			torrent: tapeRerunHintedTorrent("tt0042332"),
		},
		{
			subject: "0000000000000000000000000000000000000006",
			flags:   testFlagsOn,
			torrent: tapeRerunHintedTorrent("tt0042332-copy"),
		},
	}
	for _, fixture := range fixtures {
		ctx := tape.WithSubject(context.Background(), fixture.subject)
		_, runErr := runner.Run(ctx, "default", fixture.flags, fixture.torrent)
		if fixture.subject == "0000000000000000000000000000000000000004" {
			if runErr == nil {
				t.Fatalf("deleted fixture %s unexpectedly completed", fixture.subject)
			}
			continue
		}
		if runErr != nil {
			t.Fatalf("classify %s: %v", fixture.subject, runErr)
		}
	}

	// Keep a compact permanent regression for every private acquisition-only
	// workflow. The production plan executes thousands of these; one of each is
	// enough to pin Go/Rust replay, plan-digest reporting, and serving isolation.
	plan, err := LoadTapeAcquisitionPlan(tapePlanFixturePath, tapePlanFixtureDigest)
	if err != nil {
		t.Fatalf("load tape acquisition plan: %v", err)
	}
	evidenceRunner, err := compileTapeEvidenceRunner(source, recorder)
	if err != nil {
		t.Fatalf("compile tape evidence runner: %v", err)
	}
	for entryIndex, entry := range plan.Entries {
		subject := tapePlanSubject(plan.digest, entryIndex, 0)
		torrent := rekeyTapePlanTorrent(entry.torrent, protocol.MustParseID(subject))
		ctx := withTapeEvidenceCapability(
			tape.WithSubject(context.Background(), subject),
			tapeEvidencePlanCapability,
		)
		_, runErr := evidenceRunner.Run(ctx, entry.Workflow, entry.Flags, torrent)
		if !tapePlanOutcomeMatches(tapeEvidenceWorkflowQuotas[entryIndex].outcome, runErr) {
			t.Fatalf("run %s fixture: %v", entry.Workflow, runErr)
		}
	}

	generatedAt := time.Date(2026, time.August, 12, 0, 0, 0, 0, time.UTC)
	if *updateTapeRerunExample {
		if err := recorder.Write(tapeRerunExampleDir, generatedAt); err != nil {
			t.Fatalf("write tape rerun example: %v", err)
		}
		return
	}

	actualDir := t.TempDir()
	if err := recorder.Write(actualDir, generatedAt); err != nil {
		t.Fatalf("write generated tape rerun example: %v", err)
	}
	for _, name := range []string{tape.TapeFileName, tape.ManifestFileName, tape.ProvenanceFileName} {
		expected, readErr := os.ReadFile(filepath.Join(tapeRerunExampleDir, name))
		if readErr != nil {
			t.Fatalf("read committed %s (regenerate with -update-tape-rerun-example): %v", name, readErr)
		}
		actual, readErr := os.ReadFile(filepath.Join(actualDir, name))
		if readErr != nil {
			t.Fatalf("read generated %s: %v", name, readErr)
		}
		if !bytes.Equal(expected, actual) {
			t.Errorf("tape rerun example %s differs (regenerate with -update-tape-rerun-example)", name)
		}
	}
}

func tapeRerunTorrent(name string, existingIDs ...string) model.Torrent {
	torrent := movieTorrent(name)
	torrent.Contents = make([]model.TorrentContent, 0, len(existingIDs))
	for _, id := range existingIDs {
		torrent.Contents = append(torrent.Contents, model.TorrentContent{ID: id})
	}
	return torrent
}

func tapeRerunHintedTorrent(contentID string) model.Torrent {
	torrent := tapeRerunTorrent("Cinderella.1950.1080p.BluRay.x264")
	torrent.Hint = model.TorrentHint{
		ContentType:   model.ContentTypeMovie,
		ContentSource: model.NewNullString(model.SourceImdb),
		ContentID:     model.NewNullString(contentID),
	}
	return torrent
}
