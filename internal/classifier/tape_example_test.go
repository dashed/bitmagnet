package classifier

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
)

// updateTapeExample regenerates the committed example tape.
var updateTapeExample = flag.Bool(
	"update-tape-example",
	false,
	"regenerate testdata/parity/classifier-attach/example",
)

const tapeExampleDir = "../../testdata/parity/classifier-attach/example"

// TestTapeExampleGolden pins the on-disk tape format.
//
// The example is generated from fixtures, not from production, so it can be
// committed and read by anyone porting the classifier: it shows the exact bytes
// a replay has to consume, including the shapes that are easy to get wrong --
// a tied candidate window, a genuine empty answer, a recorded failure, and a
// classification that observed nothing at all.
func TestTapeExampleGolden(t *testing.T) {
	source, err := yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}.source()
	if err != nil {
		t.Fatalf("load core classifier source: %v", err)
	}

	digest, err := EffectiveConfigDigest(source, "default")
	if err != nil {
		t.Fatalf("effective config digest: %v", err)
	}

	searchBody, detailsBody := tmdbBodies(t, 11224, "Cinderella")
	requester := stubRequester{bodies: map[string][]byte{
		"/search/movie": searchBody,
		"/movie/11224":  detailsBody,
	}}
	contentSearch := &stubContentSearch{results: []search.ContentResult{
		// A tied window: three candidates, one rank, order decided by the plan.
		tiedResult("Cinderella", "Cinderella (1950)", "Cinderella"),
		// A genuine empty answer, which sends find_match on to TMDB.
		{},
	}}

	recorder := tape.NewRecorder(digest, 0, tape.Provenance{
		Command:     "go test ./internal/classifier -run TestTapeExampleGolden -update-tape-example",
		Host:        "fixture",
		Database:    "none (fixtures, not a database)",
		ScopeLimits: TapeScopeLimits,
		Notes: "Generated from fixtures so the format can be committed and reviewed. " +
			"A real tape is recorded by running the classifier with CLASSIFIER_TAPE_DIR set; " +
			"see the README next to this directory.",
	})

	runner := newTapeRunner(t, localSearchDeps(contentSearch, requester), recorder)

	for _, fixture := range []struct {
		subject string
		flags   Flags
		torrent model.Torrent
	}{{
		// Reaches the local search and wins from a tied window.
		subject: "tied-window",
		flags:   testFlagsOn,
		torrent: movieTorrent("Cinderella.1950.1080p.BluRay.x264-GROUP"),
	}, {
		// The local search comes back empty, so the classification falls through
		// to TMDB: three observations, and the empty one is an answer.
		subject: "empty-then-tmdb",
		flags:   testFlagsOn,
		torrent: movieTorrent("Cinderella.1950.720p.WEB-DL.h264"),
	}, {
		// Every enrichment flag is off, so nothing impure is consulted at all.
		// The record still exists, with no observations: "classified, observed
		// nothing" is a different fact from "never classified".
		subject: "no-observations",
		flags:   testFlagsOff,
		torrent: movieTorrent("Cinderella.1950.2160p.UHD.BluRay.x265"),
	}} {
		ctx := tape.WithSubject(context.Background(), fixture.subject)
		if _, err := runner.Run(ctx, "default", fixture.flags, fixture.torrent); err != nil {
			t.Fatalf("classify %s: %v", fixture.subject, err)
		}
	}

	// A recorded failure, so the example shows the error shape too.
	failingClient := tmdb.NewClient(stubRequester{err: tmdb.ErrUnauthorized})
	failureCtx := recorder.Begin(context.Background(), "tmdb-failure", "default", map[string]any{
		"apis_enabled": true, "tmdb_enabled": true,
	}, newTapeClassifierInput("tmdb-failure", movieTorrent("Cinderella (1950)")))

	if _, err := failingClient.SearchMovie(failureCtx, tmdb.SearchMovieRequest{
		Query:        "Cinderella",
		IncludeAdult: true,
	}); err == nil {
		t.Fatal("the failure fixture did not fail")
	}

	tape.EndSession(failureCtx, tape.RecordOutcome{Kind: tape.RecordCompleted})

	// A pinned timestamp keeps the example byte-reproducible.
	generatedAt := time.Date(2026, time.July, 25, 0, 0, 0, 0, time.UTC)

	if *updateTapeExample {
		if err := recorder.Write(tapeExampleDir, generatedAt); err != nil {
			t.Fatalf("write example tape: %v", err)
		}

		t.Logf("wrote example tape to %s", tapeExampleDir)

		return
	}

	actualDir := t.TempDir()
	if err := recorder.Write(actualDir, generatedAt); err != nil {
		t.Fatalf("write example tape: %v", err)
	}

	for _, name := range []string{tape.TapeFileName, tape.ManifestFileName, tape.ProvenanceFileName} {
		expected, err := os.ReadFile(filepath.Join(tapeExampleDir, name))
		if err != nil {
			t.Fatalf("read committed example %s (run with -update-tape-example to create it): %v", name, err)
		}

		actual, err := os.ReadFile(filepath.Join(actualDir, name))
		if err != nil {
			t.Fatalf("read generated example %s: %v", name, err)
		}

		if !bytes.Equal(expected, actual) {
			t.Errorf("example tape %s differs (run with -update-tape-example to regenerate)", name)
		}
	}

	// The committed example must load, which exercises every reader invariant
	// against real bytes rather than hand-written ones.
	if _, err := tape.Load(tapeExampleDir, digest); err != nil {
		t.Fatalf("committed example tape does not load: %v", err)
	}
}
