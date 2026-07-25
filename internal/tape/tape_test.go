package tape

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const digest = "sha256:abc"

func newTestRecorder(t *testing.T, maxRecords int) *Recorder {
	t.Helper()

	return NewRecorder(digest, maxRecords, Provenance{
		Command:     "go test",
		Host:        "test",
		Database:    "fixture",
		ScopeLimits: "this tape does not prove anything about frobnication",
	})
}

func writeAndLoad(t *testing.T, recorder *Recorder, wantDigest string) (*Replay, string) {
	t.Helper()

	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatalf("write: %v", err)
	}

	replay, err := Load(dir, wantDigest)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	return replay, dir
}

// TestNoSessionWithoutRecorder pins the disabled state: a nil Recorder never
// puts a session on the context, so every seam stays on its normal path.
func TestNoSessionWithoutRecorder(t *testing.T) {
	var recorder *Recorder

	ctx := recorder.Begin(context.Background(), "subject", "default", nil)
	if SessionFrom(ctx) != nil {
		t.Fatal("a nil recorder opened a session")
	}
}

// TestEmptyRecordSurvives is the empty-versus-missing guarantee at the format
// level: a classification that observed nothing is a record with an empty
// observation list, and it must not degrade into a null on the way to disk.
func TestEmptyRecordSurvives(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	SessionFrom(recorder.Begin(context.Background(), "quiet", "default", map[string]any{"a": true})).End()

	replay, dir := writeAndLoad(t, recorder, digest)

	raw, err := os.ReadFile(filepath.Join(dir, TapeFileName))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(raw), `"observations":[]`) {
		t.Fatalf("empty observation list did not survive encoding: %s", raw)
	}

	// The record exists, so the subject was classified; the first question asked
	// of it is still a miss, because no observation was recorded.
	session := SessionFrom(replay.Begin(context.Background(), "quiet", 0))

	_, _, err = session.Next("any", struct{}{})
	if !errors.Is(err, ErrMiss) {
		t.Fatalf("got %v, want a miss", err)
	}
}

func TestRecordedEmptyResponseIsAnAnswer(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil))
	session.Observe("k", map[string]string{"q": "x"}, map[string][]string{"items": {}})
	session.End()

	replay, _ := writeAndLoad(t, recorder, digest)
	replaySession := SessionFrom(replay.Begin(context.Background(), "s", 0))

	response, observationErr, err := replaySession.Next("k", map[string]string{"q": "x"})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if observationErr != nil {
		t.Fatalf("replay returned an error observation: %v", observationErr)
	}

	if string(response) != `{"items":[]}` {
		t.Fatalf("replayed %s, want the recorded empty answer", response)
	}
}

func TestDesyncOnDifferentRequest(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil))
	session.Observe("k", map[string]any{"query": "cinderella", "year": 1950}, map[string]any{"ok": true})
	session.End()

	replay, _ := writeAndLoad(t, recorder, digest)

	for name, request := range map[string]map[string]any{
		"different value":   {"query": "cinderella", "year": 1951},
		"missing parameter": {"query": "cinderella"},
		"extra parameter":   {"query": "cinderella", "year": 1950, "region": "US"},
	} {
		t.Run(name, func(t *testing.T) {
			replaySession := SessionFrom(replay.Begin(context.Background(), "s", 0))

			_, _, err := replaySession.Next("k", request)
			if !errors.Is(err, ErrDesync) {
				t.Fatalf("got %v, want a desync", err)
			}
		})
	}

	t.Run("matching request", func(t *testing.T) {
		replaySession := SessionFrom(replay.Begin(context.Background(), "s", 0))
		// Key order in the caller's map must not matter: both sides are
		// canonically encoded before comparison.
		if _, _, err := replaySession.Next("k", map[string]any{"year": 1950, "query": "cinderella"}); err != nil {
			t.Fatalf("identical request desynced: %v", err)
		}
	})
}

func TestDigestDriftFailsClosed(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	SessionFrom(recorder.Begin(context.Background(), "s", "default", nil)).End()

	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(dir, "sha256:different"); err == nil {
		t.Fatal("a tape recorded under a different config digest loaded without complaint")
	}
}

func TestReaderRejectsMalformedRecords(t *testing.T) {
	for name, line := range map[string]string{
		"null observations": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"observations":null}`,
		"missing request": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"observations":` +
			`[{"kind":"k","outcome":"ok","response":{}}]`,
		"ok without response": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"observations":` +
			`[{"kind":"k","request":{},"outcome":"ok"}]}`,
		"ok with null response": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"observations":` +
			`[{"kind":"k","request":{},"outcome":"ok","response":null}]}`,
		"error without error": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"observations":` +
			`[{"kind":"k","request":{},"outcome":"error"}]}`,
		"unknown outcome": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"observations":` +
			`[{"kind":"k","request":{},"outcome":"maybe"}]}`,
		"empty subject": `{"subject":"","attempt":0,"workflow":"w","flags":{},"observations":[]}`,
		"unknown field": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"observations":[],"extra":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRecords(strings.NewReader(line + "\n")); err == nil {
				t.Fatal("malformed record decoded without complaint")
			}
		})
	}
}

func TestTruncationIsRecorded(t *testing.T) {
	recorder := newTestRecorder(t, 2)

	var full int

	recorder.OnFull(func() { full++ })

	for _, subject := range []string{"a", "b", "c", "d"} {
		ctx := recorder.Begin(context.Background(), subject, "default", nil)
		if SessionFrom(ctx) == nil && subject < "c" {
			t.Fatalf("subject %s was refused a session before the cap", subject)
		}

		EndSession(ctx)
	}

	if !recorder.Truncated() {
		t.Fatal("the recorder did not report truncation")
	}

	if full != 1 {
		t.Fatalf("the full callback ran %d times, want exactly 1", full)
	}

	replay, _ := writeAndLoad(t, recorder, digest)
	if !replay.Manifest().Truncated {
		t.Fatal("the manifest does not report truncation")
	}

	if replay.Manifest().RecordCount != 2 {
		t.Fatalf("tape holds %d records, want the cap of 2", replay.Manifest().RecordCount)
	}
}

// TestUnfinishedRecordIsExcluded covers writing a tape while classifications
// are still running, which is what a cap-reached snapshot does. Such a record
// holds a prefix of its observations, and serving that prefix would look like
// the classification legitimately stopped asking questions.
func TestUnfinishedRecordIsExcluded(t *testing.T) {
	recorder := newTestRecorder(t, 0)

	finished := SessionFrom(recorder.Begin(context.Background(), "finished", "default", nil))
	finished.Observe("k", map[string]any{"q": 1}, map[string]any{})
	finished.End()

	// Begun, observed once, and deliberately not ended: still in flight.
	unfinished := SessionFrom(recorder.Begin(context.Background(), "unfinished", "default", nil))
	unfinished.Observe("k", map[string]any{"q": 1}, map[string]any{})

	replay, dir := writeAndLoad(t, recorder, digest)

	if replay.Manifest().IncompleteRecordCount != 1 {
		t.Fatalf("manifest reports %d incomplete records, want 1",
			replay.Manifest().IncompleteRecordCount)
	}

	raw, err := os.ReadFile(filepath.Join(dir, TapeFileName))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(raw), `"incomplete":true`) {
		t.Fatalf("the in-flight record is not marked incomplete: %s", raw)
	}

	session := SessionFrom(replay.Begin(context.Background(), "unfinished", 0))
	if _, _, err := session.Next("k", map[string]any{"q": 1}); !errors.Is(err, ErrMiss) {
		t.Fatalf("an incomplete record answered with %v, want a miss", err)
	}

	// The finished record beside it is unaffected.
	session = SessionFrom(replay.Begin(context.Background(), "finished", 0))
	if _, _, err := session.Next("k", map[string]any{"q": 1}); err != nil {
		t.Fatalf("the finished record did not replay: %v", err)
	}

	// Ending it and writing again promotes it to a usable answer.
	unfinished.End()

	replay, _ = writeAndLoad(t, recorder, digest)
	if replay.Manifest().IncompleteRecordCount != 0 {
		t.Fatal("the record is still marked incomplete after the session ended")
	}

	session = SessionFrom(replay.Begin(context.Background(), "unfinished", 0))
	if _, _, err := session.Next("k", map[string]any{"q": 1}); err != nil {
		t.Fatalf("the completed record did not replay: %v", err)
	}
}

// TestConcurrentSessionsAreIndependent is the concurrency claim: sessions
// interleave freely, but each subject's sequence is its own and the tape sorts
// deterministically.
func TestConcurrentSessionsAreIndependent(t *testing.T) {
	const subjects = 32

	encode := func(concurrent bool) string {
		recorder := newTestRecorder(t, 0)

		observe := func(i int) {
			subject := string(rune('a'+i/26)) + string(rune('a'+i%26))
			session := SessionFrom(recorder.Begin(context.Background(), subject, "default", nil))

			for step := range 3 {
				session.Observe("k", map[string]any{"subject": subject, "step": step},
					map[string]any{"step": step})
			}

			session.End()
		}

		if concurrent {
			var wg sync.WaitGroup

			for i := range subjects {
				wg.Add(1)

				go func() {
					defer wg.Done()
					observe(i)
				}()
			}

			wg.Wait()
		} else {
			for i := range subjects {
				observe(i)
			}
		}

		records, err := recorder.Records()
		if err != nil {
			t.Fatalf("records: %v", err)
		}

		encoded, err := EncodeRecords(records)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}

		return string(encoded)
	}

	sequential := encode(false)
	if got := encode(true); got != sequential {
		t.Fatalf("concurrent tape differs\nsequential:\n%s\nconcurrent:\n%s", sequential, got)
	}

	records, err := DecodeRecords(strings.NewReader(sequential))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, record := range records {
		for step, observation := range record.Observations {
			var request struct {
				Subject string `json:"subject"`
				Step    int    `json:"step"`
			}

			if err := json.Unmarshal(observation.Request, &request); err != nil {
				t.Fatal(err)
			}

			if request.Subject != record.Subject || request.Step != step {
				t.Fatalf("record %s step %d holds %+v: sequences crossed between sessions",
					record.Subject, step, request)
			}
		}
	}
}

func TestRepeatSubjectGetsDistinctAttempt(t *testing.T) {
	recorder := newTestRecorder(t, 0)

	for range 2 {
		session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil))
		session.Observe("k", map[string]any{"attempt": session.Attempt()}, map[string]any{})
		session.End()
	}

	records, err := recorder.Records()
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	if len(records) != 2 || records[0].Attempt != 0 || records[1].Attempt != 1 {
		t.Fatalf("repeat classifications collided: %+v", records)
	}

	// Both attempts have to survive the round trip; a duplicate (subject,
	// attempt) pair is rejected on load.
	writeAndLoad(t, recorder, digest)
}

func TestProvenanceDocumentNamesTheRun(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", map[string]any{
		"local_search_enabled": true,
	}))
	session.Observe("local.content_by_search", map[string]any{}, map[string]any{})
	session.End()

	_, dir := writeAndLoad(t, recorder, digest)

	document, err := os.ReadFile(filepath.Join(dir, ProvenanceFileName))
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"go test",
		"fixture",
		digest,
		`"local_search_enabled":true`,
		"local.content_by_search (ok)",
		// A tape read in isolation has to carry its own limits, or a pass gets
		// read as broader evidence than it is.
		"does NOT prove",
		"this tape does not prove anything about frobnication",
	} {
		if !strings.Contains(string(document), want) {
			t.Errorf("PROVENANCE.md does not mention %q:\n%s", want, document)
		}
	}
}
