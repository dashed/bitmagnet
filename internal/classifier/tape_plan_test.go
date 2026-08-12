package classifier

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const (
	tapePlanFixturePath   = "../../testdata/parity/classifier-attach/t1/acquisition-plan.json"
	tapePlanFixtureDigest = "sha256:c6febd6d4dbcc762050d5a4d38d401dc0d56f50f901b88fc252a382a83b455fe"
	coreConfigDigest      = "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae"
)

func TestTapeAcquisitionPlanFixtureIsPinnedAndStrict(t *testing.T) {
	plan, err := LoadTapeAcquisitionPlan(tapePlanFixturePath, tapePlanFixtureDigest)
	require.NoError(t, err)
	require.Equal(t, TapeAcquisitionPlanSchema, plan.Schema)
	require.Equal(t, []int{2000, 500, 500}, []int{
		plan.Entries[0].Repeat,
		plan.Entries[1].Repeat,
		plan.Entries[2].Repeat,
	})
	require.Equal(t, []string{
		tapeEvidenceActionEntriesWorkflow,
		tapeEvidenceUnmatchedWorkflow,
		tapeEvidenceDeletedWorkflow,
	}, []string{
		plan.Entries[0].Workflow,
		plan.Entries[1].Workflow,
		plan.Entries[2].Workflow,
	})

	raw, err := os.ReadFile(tapePlanFixturePath)
	require.NoError(t, err)

	t.Run("digest mismatch", func(t *testing.T) {
		_, err := LoadTapeAcquisitionPlan(
			tapePlanFixturePath,
			"sha256:0000000000000000000000000000000000000000000000000000000000000000",
		)
		require.ErrorContains(t, err, "digest mismatch")
	})

	tests := map[string]string{
		"unknown top-level field": strings.Replace(string(raw), "\n  \"entries\"", "\n  \"unexpected\": true,\n  \"entries\"", 1),
		"unknown entry field":     strings.Replace(string(raw), "\n      \"workflow\"", "\n      \"unexpected\": true,\n      \"workflow\"", 1),
		"unsafe flag":             strings.Replace(string(raw), "\"apis_enabled\": false", "\"apis_enabled\": true", 1),
		"null delete list":        strings.Replace(string(raw), "\"delete_content_types\": []", "\"delete_content_types\": null", 1),
		"invalid input id":        strings.Replace(string(raw), "0000000000000000000000000000000000000001", "not-an-info-hash", 1),
		"wrong repeat":            strings.Replace(string(raw), "\"repeat\": 2000", "\"repeat\": 1999", 1),
		"trailing JSON":           string(raw) + "{}\n",
	}
	for name, invalid := range tests {
		t.Run(name, func(t *testing.T) {
			path, digest := writeTapePlanTestFile(t, []byte(invalid))
			_, err := LoadTapeAcquisitionPlan(path, digest)
			require.Error(t, err)
		})
	}
	for _, field := range []string{"flags", "input"} {
		t.Run("null "+field, func(t *testing.T) {
			invalid := mutateTapePlanTestJSON(t, raw, func(document map[string]any) {
				entries := document["entries"].([]any)
				entries[0].(map[string]any)[field] = nil
			})
			path, digest := writeTapePlanTestFile(t, invalid)
			_, err := LoadTapeAcquisitionPlan(path, digest)
			require.ErrorContains(t, err, "must be an explicit object")
		})
	}
}

func TestTapeAcquisitionPlanImageContractIsExact(t *testing.T) {
	dockerfile, err := os.ReadFile("../../ci.Dockerfile")
	require.NoError(t, err)
	contract := string(dockerfile)
	require.NotContains(t, contract, "LABEL io.bitmagnet.classifier-tape-contract =")
	require.Contains(t, contract,
		`LABEL io.bitmagnet.classifier-tape-contract="action-progress-processor-state-plan-v1"`)
	require.Contains(t, contract,
		`LABEL io.bitmagnet.classifier-tape-acquisition-plan="${TAPE_ACQUISITION_PLAN_SHA256}"`)
	require.Contains(t, contract,
		`ARG TAPE_ACQUISITION_PLAN_SHA256=`+tapePlanFixtureDigest)
	require.Contains(t, contract,
		`COPY --link testdata/parity/classifier-attach/t1/acquisition-plan.json /opt/bitmagnet/t1/acquisition-plan.json`)
}

func TestTapeAcquisitionPlanConfigurationIsAllOrNothing(t *testing.T) {
	tests := []struct {
		name       string
		config     Config
		configured bool
		wantError  string
	}{
		{name: "inactive"},
		{
			name:       "configured",
			config:     Config{TapeDir: "tape", TapePlanPath: "plan", TapePlanSHA256: tapePlanFixtureDigest},
			configured: true,
		},
		{name: "path only", config: Config{TapeDir: "tape", TapePlanPath: "plan"}, wantError: "configured together"},
		{name: "digest only", config: Config{TapeDir: "tape", TapePlanSHA256: tapePlanFixtureDigest}, wantError: "configured together"},
		{
			name:      "plan without tape",
			config:    Config{TapePlanPath: "plan", TapePlanSHA256: tapePlanFixtureDigest},
			wantError: "requires classifier tape recording",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configured, err := tapePlanConfigured(tt.config)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.configured, configured)
		})
	}
}

func TestTapePlanSubjectCrossLanguageVectors(t *testing.T) {
	require.Equal(t, "2e7249954964aa67073e5f384840690e117aeaae", tapePlanSubject(tapePlanFixtureDigest, 0, 0))
	require.Equal(t, "343fa65779e1386ea27aa237ab5f346d9890a2ca", tapePlanSubject(tapePlanFixtureDigest, 2, 499))
}

func TestTapeEvidenceWorkflowsRequirePrivateCapability(t *testing.T) {
	source := loadCoreTapePlanSource(t)
	for _, quota := range tapeEvidenceWorkflowQuotas {
		require.NotContains(t, source.Workflows, quota.name)
	}

	plan, err := LoadTapeAcquisitionPlan(tapePlanFixturePath, tapePlanFixtureDigest)
	require.NoError(t, err)
	runner, err := compileTapeEvidenceRunner(source, nil)
	require.NoError(t, err)
	for i, quota := range tapeEvidenceWorkflowQuotas {
		t.Run(quota.name, func(t *testing.T) {
			_, err := runner.Run(context.Background(), quota.name, plan.Entries[i].Flags, plan.Entries[i].torrent)
			require.ErrorContains(t, err, "reserved for validated classifier tape evidence")
		})
	}
}

func TestTapeAcquisitionPlanExecutesAndReplaysExactEvidence(t *testing.T) {
	source := loadCoreTapePlanSource(t)
	recorder := tape.NewRecorder(coreConfigDigest, 4000, tape.Provenance{
		Command:               "classifier tape plan test",
		AcquisitionPlanDigest: tapePlanFixtureDigest,
	})
	config := Config{
		TapeDir:        t.TempDir(),
		TapeMaxRecords: 4000,
		TapePlanPath:   tapePlanFixturePath,
		TapePlanSHA256: tapePlanFixtureDigest,
	}
	executor, err := newTapeAcquisitionPlanExecutor(config, source, recorder)
	require.NoError(t, err)
	require.NoError(t, executor.Run(context.Background()))

	progress := recorder.Progress()
	require.Equal(t, tapePlanFixtureDigest, progress.AcquisitionPlanDigest)
	require.Equal(t, 3000, progress.RegisteredRecords)
	require.Zero(t, progress.OpenSessions)
	require.Equal(t, 3000, progress.AuthoritativeRecords)
	require.Zero(t, progress.NonAuthoritativeRecords)
	require.Zero(t, progress.ObservationCount)
	require.Equal(t, 8000, progress.ActionEntryCount)
	require.Equal(t, map[string]int{
		attachLocalContentByIDName:     2000,
		attachTMDBContentByIDName:      2000,
		attachLocalContentBySearchName: 2000,
		attachTmdbContentBySearchName:  2000,
	}, progress.ActionEntryCounts)
	require.Equal(t, map[string]int{
		string(tape.RecordCompleted): 2000,
		string(tape.RecordUnmatched): 500,
		string(tape.RecordDeleted):   500,
	}, progress.RecordOutcomeCounts)

	records, err := recorder.Records()
	require.NoError(t, err)
	require.Len(t, records, 3000)
	seenSubjects := make(map[string]struct{}, len(records))
	workflowCounts := make(map[string]int)
	for _, record := range records {
		require.Len(t, record.Subject, 40)
		require.Regexp(t, "^[0-9a-f]{40}$", record.Subject)
		require.Zero(t, record.Attempt)
		_, duplicate := seenSubjects[record.Subject]
		require.False(t, duplicate, record.Subject)
		seenSubjects[record.Subject] = struct{}{}
		workflowCounts[record.Workflow]++
	}
	require.Equal(t, map[string]int{
		tapeEvidenceActionEntriesWorkflow: 2000,
		tapeEvidenceUnmatchedWorkflow:     500,
		tapeEvidenceDeletedWorkflow:       500,
	}, workflowCounts)

	require.NoError(t, recorder.Write(config.TapeDir, time.Unix(0, 0).UTC()))
	replay, err := tape.Load(config.TapeDir, coreConfigDigest)
	require.NoError(t, err)
	require.Equal(t, tapePlanFixtureDigest, replay.Manifest().AcquisitionPlanDigest)
	provenance, err := os.ReadFile(filepath.Join(config.TapeDir, tape.ProvenanceFileName))
	require.NoError(t, err)
	require.Contains(t, string(provenance), "Acquisition plan digest: "+tapePlanFixtureDigest)

	replayer, err := NewTapeReplayer(replay)
	require.NoError(t, err)
	replayExamples := make(map[string]tape.Record)
	for _, record := range replay.Subjects() {
		if _, exists := replayExamples[record.Workflow]; !exists {
			replayExamples[record.Workflow] = record
		}
	}
	for _, workflow := range []string{
		tapeEvidenceActionEntriesWorkflow,
		tapeEvidenceUnmatchedWorkflow,
		tapeEvidenceDeletedWorkflow,
	} {
		t.Run("replay "+workflow, func(t *testing.T) {
			_, err := replayer.Run(context.Background(), replayExamples[workflow])
			require.NoError(t, err)
		})
	}
}

func TestTapeAcquisitionPlanFactoryOnStartSeedsSynchronouslyWithoutLiveDependencies(t *testing.T) {
	tapeDir := t.TempDir()
	config := NewDefaultConfig()
	config.TapeDir = tapeDir
	config.TapeMaxRecords = 4000
	config.TapePlanPath = tapePlanFixturePath
	config.TapePlanSHA256 = tapePlanFixtureDigest

	searchRequested := false
	tmdbRequested := false
	lifecycle := fxtest.NewLifecycle(t)
	logCore, logs := observer.New(zap.InfoLevel)
	_ = New(Params{
		Config:     config,
		TmdbConfig: tmdb.NewDefaultConfig(),
		Search: lazy.New(func() (search.Search, error) {
			searchRequested = true
			return nil, errors.New("live database search must not initialize for tape plan")
		}),
		TmdbClient: lazy.New(func() (tmdb.Client, error) {
			tmdbRequested = true
			return nil, errors.New("live TMDB client must not initialize for tape plan")
		}),
		Lifecycle: lifecycle,
		Logger:    zap.New(logCore).Sugar(),
	})

	require.NoError(t, lifecycle.Start(context.Background()))
	require.False(t, searchRequested)
	require.False(t, tmdbRequested)
	progressLogs := logs.FilterMessage("classifier tape progress").All()
	require.Len(t, progressLogs, 1)
	fields := progressLogs[0].ContextMap()
	require.EqualValues(t, 3000, fields["registered_records"])
	require.EqualValues(t, 3000, fields["authoritative_records"])
	require.EqualValues(t, 8000, fields["action_entry_count"])
	require.Equal(t, tapePlanFixtureDigest, fields["acquisition_plan_digest"])

	require.NoError(t, lifecycle.Stop(context.Background()))
	replay, err := tape.Load(tapeDir, coreConfigDigest)
	require.NoError(t, err)
	require.Len(t, replay.Subjects(), 3000)
}

func TestTapeAcquisitionPlanRequiresOrganicCapacityAndHonorsCancellation(t *testing.T) {
	source := loadCoreTapePlanSource(t)
	baseConfig := Config{
		TapeDir:        "tape",
		TapePlanPath:   tapePlanFixturePath,
		TapePlanSHA256: tapePlanFixtureDigest,
	}

	tooSmall := tape.NewRecorder(coreConfigDigest, 3000, tape.Provenance{AcquisitionPlanDigest: tapePlanFixtureDigest})
	_, err := newTapeAcquisitionPlanExecutor(baseConfig, source, tooSmall)
	require.ErrorContains(t, err, "leave room for organic traffic")

	recorder := tape.NewRecorder(coreConfigDigest, 3001, tape.Provenance{AcquisitionPlanDigest: tapePlanFixtureDigest})
	executor, err := newTapeAcquisitionPlanExecutor(baseConfig, source, recorder)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, executor.Run(ctx), context.Canceled)
	require.Zero(t, recorder.Progress().RegisteredRecords)
}

func loadCoreTapePlanSource(t *testing.T) Source {
	t.Helper()
	source, err := (yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}).source()
	require.NoError(t, err)
	return source
}

func writeTapePlanTestFile(t *testing.T, raw []byte) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acquisition-plan.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	digest := tapePlanDigestForTest(raw)
	return path, digest
}

func tapePlanDigestForTest(raw []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

func mutateTapePlanTestJSON(t *testing.T, raw []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	var document map[string]any
	require.NoError(t, json.Unmarshal(raw, &document))
	mutate(document)
	encoded, err := json.Marshal(document)
	require.NoError(t, err)
	return encoded
}
