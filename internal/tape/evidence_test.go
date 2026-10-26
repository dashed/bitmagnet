package tape

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const mutatedTestValue = "mutated"

func TestActionEntriesRoundTripAndReplayInOrder(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil, nil))

	for _, name := range []string{"attach_local_content_by_id", "attach_tmdb_content_by_id"} {
		if err := session.EnterAction(name); err != nil {
			t.Fatalf("record action %q: %v", name, err)
		}
	}
	session.Observe("dependency", map[string]any{"q": 1}, map[string]any{"ok": true})
	session.End(RecordOutcome{Kind: RecordCompleted})

	replay, _ := writeAndLoad(t, recorder, digest)
	manifest := replay.Manifest()
	if manifest.ActionEntryCount == nil || *manifest.ActionEntryCount != 2 {
		t.Fatalf("actionEntryCount = %v, want 2", manifest.ActionEntryCount)
	}
	if got, want := manifest.ActionEntryCounts, map[string]int{
		"attach_local_content_by_id": 1,
		"attach_tmdb_content_by_id":  1,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("actionEntryCounts = %v, want %v", got, want)
	}

	recorded := replay.Subjects()
	if len(recorded) != 1 {
		t.Fatalf("got %d records, want 1", len(recorded))
	}
	if got := []string{recorded[0].ActionEntries[0].Name, recorded[0].ActionEntries[1].Name}; !reflect.DeepEqual(got, []string{"attach_local_content_by_id", "attach_tmdb_content_by_id"}) {
		t.Fatalf("ordered actions = %v", got)
	}

	replaySession := SessionFrom(replay.Begin(context.Background(), "s", 0))
	if remaining, known := replaySession.RemainingActions(); !known || remaining != 2 {
		t.Fatalf("remaining actions = %d, known=%t; want 2, true", remaining, known)
	}
	if replaySession.RemainingObservations() != 1 {
		t.Fatalf("remaining observations = %d, want 1", replaySession.RemainingObservations())
	}

	if err := replaySession.EnterAction("attach_local_content_by_id"); err != nil {
		t.Fatalf("first action: %v", err)
	}
	if err := replaySession.EnterAction("attach_tmdb_content_by_id"); err != nil {
		t.Fatalf("second action: %v", err)
	}
	if _, _, err := replaySession.Next("dependency", map[string]any{"q": 1}); err != nil {
		t.Fatalf("observation: %v", err)
	}
	if err := replaySession.VerifyComplete(); err != nil {
		t.Fatalf("complete replay: %v", err)
	}
}

func TestActionReplayFailsImmediatelyAndReportsUnconsumed(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil, nil))
	_ = session.EnterAction("first")
	_ = session.EnterAction("second")
	session.Observe("dependency", map[string]any{}, map[string]any{})
	session.End(RecordOutcome{Kind: RecordCompleted})
	replay, _ := writeAndLoad(t, recorder, digest)

	t.Run("desync", func(t *testing.T) {
		s := SessionFrom(replay.Begin(context.Background(), "s", 0))
		if err := s.EnterAction("wrong"); !errors.Is(err, ErrActionDesync) {
			t.Fatalf("got %v, want action desync", err)
		}
	})

	t.Run("miss", func(t *testing.T) {
		s := SessionFrom(replay.Begin(context.Background(), "s", 0))
		_ = s.EnterAction("first")
		_ = s.EnterAction("second")
		if err := s.EnterAction("third"); !errors.Is(err, ErrActionMiss) {
			t.Fatalf("got %v, want action miss", err)
		}
	})

	t.Run("unconsumed", func(t *testing.T) {
		s := SessionFrom(replay.Begin(context.Background(), "s", 0))
		_ = s.EnterAction("first")

		if remaining, known := s.RemainingActions(); !known || remaining != 1 {
			t.Fatalf("remaining actions = %d, known=%t; want 1, true", remaining, known)
		}
		if s.RemainingObservations() != 1 {
			t.Fatalf("remaining observations = %d, want 1", s.RemainingObservations())
		}
		if err := s.VerifyComplete(); !errors.Is(err, ErrUnconsumed) {
			t.Fatalf("got %v, want unconsumed", err)
		}
	})
}

func TestLegacyTapeLeavesActionTraceUnknown(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	SessionFrom(recorder.Begin(context.Background(), "legacy", "default", nil, nil)).
		End(RecordOutcome{Kind: RecordCompleted})
	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	removeManifestFields(t, dir, "actionEntryCount", "actionEntryCounts")
	replay, err := Load(dir, digest)
	if err != nil {
		t.Fatalf("load legacy tape: %v", err)
	}

	session := SessionFrom(replay.Begin(context.Background(), "legacy", 0))
	if err := session.EnterAction("an_action_the_old_writer_could_not_record"); err != nil {
		t.Fatalf("legacy action trace should be unchecked: %v", err)
	}
	if remaining, known := session.RemainingActions(); known || remaining != 0 {
		t.Fatalf("legacy remaining actions = %d, known=%t; want 0, false", remaining, known)
	}
	if err := session.VerifyComplete(); err != nil {
		t.Fatalf("legacy empty observation stream should be complete: %v", err)
	}
}

func TestProcessorStateCapturedImmediatelyAndEmitsKnownEmpty(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	ids := []string{"tc-b", "tc-a"}
	session := SessionFrom(recorder.Begin(
		context.Background(),
		"s",
		"default",
		nil,
		nil,
		ProcessorState{ExistingContentIDs: ids},
	))
	ids[0] = mutatedTestValue
	session.End(RecordOutcome{Kind: RecordCompleted})

	empty := SessionFrom(recorder.Begin(
		context.Background(),
		"empty",
		"default",
		nil,
		nil,
	))
	empty.End(RecordOutcome{Kind: RecordCompleted})

	replay, dir := writeAndLoad(t, recorder, digest)
	bySubject := make(map[string]Record)
	for _, record := range replay.Subjects() {
		bySubject[record.Subject] = record
	}

	if got := bySubject["s"].ProcessorState.ExistingContentIDs; !reflect.DeepEqual(got, []string{"tc-b", "tc-a"}) {
		t.Fatalf("processor state changed after Begin: %v", got)
	}
	if got := bySubject["empty"].ProcessorState.ExistingContentIDs; got == nil || len(got) != 0 {
		t.Fatalf("known empty processor state = %#v, want non-nil empty", got)
	}

	raw, err := os.ReadFile(filepath.Join(dir, TapeFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"processorState":{"existingContentIds":[]}`) {
		t.Fatalf("known empty processor state was not emitted: %s", raw)
	}
}

func TestManifestAggregatesAreRecomputed(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil, nil))
	_ = session.EnterAction("attach_local_content_by_search")
	session.Observe("dependency", map[string]any{}, map[string]any{})
	session.End(RecordOutcome{Kind: RecordCompleted})

	sourceDir := t.TempDir()
	if err := recorder.Write(sourceDir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(map[string]any){
		"record count":        func(m map[string]any) { m["recordCount"] = 2 },
		"observation count":   func(m map[string]any) { m["observationCount"] = 2 },
		"incomplete count":    func(m map[string]any) { m["incompleteRecordCount"] = 1 },
		"authoritative count": func(m map[string]any) { m["authoritativeRecordCount"] = 0 },
		"outcome counts": func(m map[string]any) {
			m["recordOutcomeCounts"] = map[string]any{"completed": 2}
		},
		"action total": func(m map[string]any) { m["actionEntryCount"] = 2 },
		"action counts": func(m map[string]any) {
			m["actionEntryCounts"] = map[string]any{"attach_local_content_by_search": 2}
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			dir := copyTapeFiles(t, sourceDir)
			manifest := readManifestMap(t, dir)
			mutate(manifest)
			writeManifestMap(t, dir, manifest)

			if _, err := Load(dir, digest); err == nil {
				t.Fatal("tape loaded with a stale manifest aggregate")
			}
		})
	}
}

func TestManifestRejectsNullCapabilitiesAndUnknownFields(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	SessionFrom(recorder.Begin(context.Background(), "s", "default", nil, nil)).
		End(RecordOutcome{Kind: RecordCompleted})
	sourceDir := t.TempDir()
	if err := recorder.Write(sourceDir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	for _, field := range []string{
		"acquisitionPlanDigest",
		"authoritativeRecordCount",
		"recordOutcomeCounts",
		"actionEntryCount",
		"actionEntryCounts",
	} {
		t.Run("null "+field, func(t *testing.T) {
			dir := copyTapeFiles(t, sourceDir)
			manifest := readManifestMap(t, dir)
			manifest[field] = nil
			writeManifestMap(t, dir, manifest)
			if _, err := Load(dir, digest); err == nil {
				t.Fatalf("tape loaded with null manifest field %s", field)
			}
		})
	}

	t.Run("unknown field", func(t *testing.T) {
		dir := copyTapeFiles(t, sourceDir)
		manifest := readManifestMap(t, dir)
		manifest["actionEntyrCount"] = 0
		writeManifestMap(t, dir, manifest)
		if _, err := Load(dir, digest); err == nil {
			t.Fatal("tape loaded with an unknown manifest field")
		}
	})
}

func TestAcquisitionPlanDigestRoundTripsAndLegacyAbsenceRemainsValid(t *testing.T) {
	const planDigest = "sha256:c6febd6d4dbcc762050d5a4d38d401dc0d56f50f901b88fc252a382a83b455fe"
	recorder := NewRecorder(digest, 10, Provenance{
		Command:               "go test",
		AcquisitionPlanDigest: planDigest,
	})
	SessionFrom(recorder.Begin(context.Background(), "s", "default", nil, nil)).
		End(RecordOutcome{Kind: RecordCompleted})
	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	replay, err := Load(dir, digest)
	if err != nil {
		t.Fatal(err)
	}
	if got := replay.Manifest().AcquisitionPlanDigest; got != planDigest {
		t.Fatalf("acquisition plan digest = %q, want %q", got, planDigest)
	}
	if got := recorder.Progress().AcquisitionPlanDigest; got != planDigest {
		t.Fatalf("progress acquisition plan digest = %q, want %q", got, planDigest)
	}
	provenance, err := os.ReadFile(filepath.Join(dir, ProvenanceFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(provenance), "- Acquisition plan digest: "+planDigest+"\n") {
		t.Fatalf("provenance does not bind acquisition plan: %s", provenance)
	}

	t.Run("legacy absent", func(t *testing.T) {
		legacy := copyTapeFiles(t, dir)
		removeManifestFields(t, legacy, "acquisitionPlanDigest")
		if _, err := Load(legacy, digest); err != nil {
			t.Fatalf("load legacy manifest: %v", err)
		}
	})

	for name, value := range map[string]any{
		"empty":      "",
		"uppercase":  "sha256:C6FEBD6D4DBCC762050D5A4D38D401DC0D56F50F901B88FC252A382A83B455FE",
		"unprefixed": "c6febd6d4dbcc762050d5a4d38d401dc0d56f50f901b88fc252a382a83b455fe",
	} {
		t.Run(name, func(t *testing.T) {
			invalid := copyTapeFiles(t, dir)
			manifest := readManifestMap(t, invalid)
			manifest["acquisitionPlanDigest"] = value
			writeManifestMap(t, invalid, manifest)
			if _, err := Load(invalid, digest); err == nil {
				t.Fatal("loaded malformed acquisition plan digest")
			}
		})
	}
}

func TestReplaySubjectsReturnsDeepCopies(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(
		context.Background(),
		"s",
		"default",
		map[string]any{"list": []any{"original"}},
		map[string]any{"id": "s"},
		ProcessorState{ExistingContentIDs: []string{"tc-original"}},
	))
	_ = session.EnterAction("attach_local_content_by_search")
	session.Observe("dependency", map[string]any{"q": "original"}, map[string]any{"v": "original"})
	session.End(RecordOutcome{Kind: RecordCompleted})

	replay, _ := writeAndLoad(t, recorder, digest)
	want := replay.Subjects()[0]
	mutated := replay.Subjects()
	mutated[0].Input[0] = 'x'
	mutated[0].Flags["list"].([]any)[0] = mutatedTestValue
	mutated[0].ActionEntries[0].Name = mutatedTestValue
	mutated[0].ProcessorState.ExistingContentIDs[0] = mutatedTestValue
	mutated[0].Observations[0].Request[0] = 'x'
	mutated[0].Observations[0].Response[0] = 'x'
	mutated[0].Outcome.Kind = RecordDeleted

	if got := replay.Subjects()[0]; !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay.Subjects mutation escaped copy:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestActionCapabilityMustBeDeclaredByManifest(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil, nil))
	_ = session.EnterAction("attach_local_content_by_search")
	session.End(RecordOutcome{Kind: RecordCompleted})
	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(0, 0).UTC()); err != nil {
		t.Fatal(err)
	}

	removeManifestFields(t, dir, "actionEntryCount", "actionEntryCounts")
	if _, err := Load(dir, digest); err == nil {
		t.Fatal("action entries loaded without a manifest capability declaration")
	}
}

func TestRecorderProgressDistinguishesCapSnapshotFromFinalWrite(t *testing.T) {
	recorder := newTestRecorder(t, 0)

	finished := SessionFrom(recorder.Begin(context.Background(), "finished", "default", nil, nil))
	_ = finished.EnterAction("attach_local_content_by_search")
	finished.Observe("dependency", map[string]any{}, map[string]any{})
	finished.End(RecordOutcome{Kind: RecordCompleted})

	cancelled := SessionFrom(recorder.Begin(context.Background(), "cancelled", "default", nil, nil))
	cancelled.End(RecordOutcome{Kind: RecordCanceled, Error: "context canceled"})

	open := SessionFrom(recorder.Begin(context.Background(), "open", "default", nil, nil))
	_ = open.EnterAction("attach_tmdb_content_by_search")

	progress := recorder.Progress()
	if progress.RegisteredRecords != 3 || progress.OpenSessions != 1 ||
		progress.AuthoritativeRecords != 1 || progress.NonAuthoritativeRecords != 1 {
		t.Fatalf("unexpected live progress: %+v", progress)
	}
	if progress.ObservationCount != 1 || progress.ActionEntryCount != 2 {
		t.Fatalf("unexpected evidence progress: %+v", progress)
	}

	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(1, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	progress = recorder.Progress()
	if !progress.LastWrite.Succeeded || progress.LastWrite.Final || progress.LastWrite.OpenSessions != 1 {
		t.Fatalf("cap-style write was not reported as non-final: %+v", progress.LastWrite)
	}

	open.End(RecordOutcome{Kind: RecordCompleted})
	if err := recorder.Write(dir, time.Unix(2, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	progress = recorder.Progress()
	if !progress.LastWrite.Succeeded || !progress.LastWrite.Final || progress.OpenSessions != 0 {
		t.Fatalf("quiescent write was not reported as final: %+v", progress)
	}
	if progress.LastWrite.Attempt != 2 || progress.LastWrite.AuthoritativeRecords != 2 ||
		progress.LastWrite.NonAuthoritativeRecords != 1 {
		t.Fatalf("unexpected final write progress: %+v", progress.LastWrite)
	}
}

func TestRecorderProgressMarksArtifactStaleOnRefusedBegin(t *testing.T) {
	recorder := newTestRecorder(t, 1)
	session := SessionFrom(recorder.Begin(context.Background(), "kept", "default", nil, nil))
	session.End(RecordOutcome{Kind: RecordCompleted})

	dir := t.TempDir()
	if err := recorder.Write(dir, time.Unix(1, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	if progress := recorder.Progress(); !progress.LastWrite.Final || progress.Truncated {
		t.Fatalf("quiescent pre-cap write = %+v, want final and untruncated", progress)
	}

	// The cap becomes a fact only on the first refused Begin. No record count
	// changes, but the already-written manifest is now stale because it says
	// truncated=false.
	if got := SessionFrom(recorder.Begin(context.Background(), "refused", "default", nil, nil)); got != nil {
		t.Fatalf("refused Begin unexpectedly returned a session: %+v", got)
	}
	progress := recorder.Progress()
	if !progress.Truncated || progress.LastWrite.Final {
		t.Fatalf("refused Begin did not stale the artifact: %+v", progress)
	}

	if err := recorder.Write(dir, time.Unix(2, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	progress = recorder.Progress()
	if !progress.LastWrite.Final || !progress.LastWrite.Truncated {
		t.Fatalf("rewritten cap artifact is not final: %+v", progress.LastWrite)
	}
}

func TestRecorderProgressExposesLatchedAndWriteErrors(t *testing.T) {
	recorder := newTestRecorder(t, 0)
	session := SessionFrom(recorder.Begin(context.Background(), "s", "default", nil, nil))
	if err := session.EnterAction(""); err != nil {
		t.Fatalf("recording hook reports through the recorder latch: %v", err)
	}

	if progress := recorder.Progress(); progress.Error == "" {
		t.Fatal("latched recorder error is absent from progress")
	}
	if err := recorder.Write(t.TempDir(), time.Unix(1, 0).UTC()); err == nil {
		t.Fatal("write succeeded despite the latched recorder error")
	}
	progress := recorder.Progress()
	if progress.LastWrite.Succeeded || progress.LastWrite.Error == "" {
		t.Fatalf("failed write is absent from progress: %+v", progress.LastWrite)
	}
}

func TestCapCallbackAndShutdownWritesPublishOneConsistentGeneration(t *testing.T) {
	recorder := newTestRecorder(t, 1)
	session := SessionFrom(recorder.Begin(context.Background(), "kept", "default", nil, nil))
	session.End(RecordOutcome{Kind: RecordCompleted})

	dir := t.TempDir()
	capGeneratedAt := time.Unix(1, 0).UTC()
	shutdownGeneratedAt := time.Unix(2, 0).UTC()
	capWriteDone := make(chan error, 1)
	recorder.OnFull(func() {
		capWriteDone <- recorder.Write(dir, capGeneratedAt)
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_ = recorder.Begin(context.Background(), "refused", "default", nil, nil)
	}()
	shutdownWriteDone := make(chan error, 1)
	go func() {
		defer wg.Done()
		<-start
		shutdownWriteDone <- recorder.Write(dir, shutdownGeneratedAt)
	}()
	close(start)
	wg.Wait()

	if err := <-capWriteDone; err != nil {
		t.Fatalf("cap callback write: %v", err)
	}
	if err := <-shutdownWriteDone; err != nil {
		t.Fatalf("shutdown write: %v", err)
	}

	replay, err := Load(dir, digest)
	if err != nil {
		t.Fatalf("load concurrently written tape: %v", err)
	}
	manifest := replay.Manifest()
	provenance, err := os.ReadFile(filepath.Join(dir, ProvenanceFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(provenance), "- Generated at: "+manifest.GeneratedAt+"\n") {
		t.Fatalf(
			"manifest and provenance came from different generations: manifest=%s\n%s",
			manifest.GeneratedAt,
			provenance,
		)
	}

	progress := recorder.Progress()
	if progress.LastWrite.Attempt != 2 || !progress.LastWrite.Succeeded || !progress.LastWrite.Final {
		t.Fatalf("concurrent writes did not finish as two serialized generations: %+v", progress.LastWrite)
	}
}

func TestReaderRejectsMalformedActionAndProcessorState(t *testing.T) {
	lines := map[string]string{
		"null actions":          `{"subject":"s","attempt":0,"workflow":"w","flags":{},"actionEntries":null,"observations":[]}`,
		"empty action name":     `{"subject":"s","attempt":0,"workflow":"w","flags":{},"actionEntries":[{"name":""}],"observations":[]}`,
		"null processor state":  `{"subject":"s","attempt":0,"workflow":"w","flags":{},"processorState":null,"observations":[]}`,
		"null existing IDs":     `{"subject":"s","attempt":0,"workflow":"w","flags":{},"processorState":{"existingContentIds":null},"observations":[]}`,
		"empty existing ID":     `{"subject":"s","attempt":0,"workflow":"w","flags":{},"processorState":{"existingContentIds":[""]},"observations":[]}`,
		"duplicate existing ID": `{"subject":"s","attempt":0,"workflow":"w","flags":{},"processorState":{"existingContentIds":["tc","tc"]},"observations":[]}`,
	}

	for name, line := range lines {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRecords(strings.NewReader(line + "\n")); err == nil {
				t.Fatal("malformed record decoded without complaint")
			}
		})
	}
}

func readManifestMap(t *testing.T, dir string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func writeManifestMap(t *testing.T, dir string, manifest map[string]any) {
	t.Helper()
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ManifestFileName), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func removeManifestFields(t *testing.T, dir string, fields ...string) {
	t.Helper()
	manifest := readManifestMap(t, dir)
	for _, field := range fields {
		delete(manifest, field)
	}
	writeManifestMap(t, dir, manifest)
}

func copyTapeFiles(t *testing.T, sourceDir string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{ManifestFileName, TapeFileName} {
		raw, err := os.ReadFile(filepath.Join(sourceDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
