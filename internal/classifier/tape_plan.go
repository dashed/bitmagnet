package classifier

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	"github.com/go-resty/resty/v2"
)

const (
	TapeAcquisitionPlanSchema   = "bitmagnet.classifier-tape-acquisition-plan/v1"
	maxTapeAcquisitionPlanBytes = 1 << 20

	tapeEvidenceActionEntriesWorkflow = "tape_evidence_action_entries"
	tapeEvidenceUnmatchedWorkflow     = "tape_evidence_unmatched"
	tapeEvidenceDeletedWorkflow       = "tape_evidence_deleted"
)

type tapeEvidenceWorkflowQuota struct {
	name    string
	repeat  int
	outcome tape.RecordOutcomeKind
	actions []string
}

var tapeEvidenceWorkflowQuotas = []tapeEvidenceWorkflowQuota{
	{
		name:    tapeEvidenceActionEntriesWorkflow,
		repeat:  2000,
		outcome: tape.RecordCompleted,
		actions: []string{
			attachLocalContentByIDName,
			attachTMDBContentByIDName,
			attachLocalContentBySearchName,
			attachTmdbContentBySearchName,
		},
	},
	{name: tapeEvidenceUnmatchedWorkflow, repeat: 500, outcome: tape.RecordUnmatched},
	{name: tapeEvidenceDeletedWorkflow, repeat: 500, outcome: tape.RecordDeleted},
}

// TapeAcquisitionPlan is a reviewed, digest-pinned list of synthetic evidence
// strata. Entries are deliberately not a generic classifier driver: exactly
// the three private tape_evidence workflows are accepted.
type TapeAcquisitionPlan struct {
	Schema  string                     `json:"schema"`
	Entries []TapeAcquisitionPlanEntry `json:"entries"`

	digest string
}

type TapeAcquisitionPlanEntry struct {
	Workflow string          `json:"workflow"`
	Flags    Flags           `json:"flags"`
	Repeat   int             `json:"repeat"`
	Input    json.RawMessage `json:"input"`

	torrent model.Torrent
}

// LoadTapeAcquisitionPlan reads at most one MiB, verifies the SHA-256 of the
// exact file bytes, strict-decodes the plan, and validates the fixed seed quota.
func LoadTapeAcquisitionPlan(path, expectedDigest string) (TapeAcquisitionPlan, error) {
	if path == "" || expectedDigest == "" {
		return TapeAcquisitionPlan{}, errors.New("classifier tape acquisition plan path and SHA-256 are both required")
	}
	if err := validateTapePlanDigest(expectedDigest); err != nil {
		return TapeAcquisitionPlan{}, err
	}

	file, err := os.Open(path)
	if err != nil {
		return TapeAcquisitionPlan{}, fmt.Errorf("open classifier tape acquisition plan: %w", err)
	}
	defer func() { _ = file.Close() }()
	raw, err := io.ReadAll(io.LimitReader(file, maxTapeAcquisitionPlanBytes+1))
	if err != nil {
		return TapeAcquisitionPlan{}, fmt.Errorf("read classifier tape acquisition plan: %w", err)
	}
	if len(raw) > maxTapeAcquisitionPlanBytes {
		return TapeAcquisitionPlan{}, fmt.Errorf("classifier tape acquisition plan exceeds %d bytes", maxTapeAcquisitionPlanBytes)
	}
	actualDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	if actualDigest != expectedDigest {
		return TapeAcquisitionPlan{}, fmt.Errorf(
			"classifier tape acquisition plan digest mismatch: got %s, want %s",
			actualDigest,
			expectedDigest,
		)
	}

	var plan TapeAcquisitionPlan
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return TapeAcquisitionPlan{}, fmt.Errorf("decode classifier tape acquisition plan: %w", err)
	}
	if err := requireTapePlanEOF(decoder); err != nil {
		return TapeAcquisitionPlan{}, err
	}
	plan.digest = actualDigest
	if err := validateTapeAcquisitionPlan(&plan, tapeEvidenceWorkflowQuotas); err != nil {
		return TapeAcquisitionPlan{}, err
	}
	return plan, nil
}

func validateTapeAcquisitionPlan(plan *TapeAcquisitionPlan, quotas []tapeEvidenceWorkflowQuota) error {
	if plan.Schema != TapeAcquisitionPlanSchema {
		return fmt.Errorf("classifier tape acquisition plan schema is %q, want %q", plan.Schema, TapeAcquisitionPlanSchema)
	}
	if len(plan.Entries) != len(quotas) {
		return fmt.Errorf("classifier tape acquisition plan has %d entries, want %d", len(plan.Entries), len(quotas))
	}
	for i := range quotas {
		entry, quota := &plan.Entries[i], quotas[i]
		if entry.Workflow != quota.name {
			return fmt.Errorf("classifier tape acquisition plan entry %d workflow is %q, want %q", i, entry.Workflow, quota.name)
		}
		if entry.Repeat != quota.repeat {
			return fmt.Errorf("classifier tape acquisition plan entry %d repeat is %d, want %d", i, entry.Repeat, quota.repeat)
		}
		if entry.Flags == nil {
			return fmt.Errorf("classifier tape acquisition plan entry %d flags must be an explicit object", i)
		}
		if err := validateTapeEvidenceFlags(entry.Flags); err != nil {
			return fmt.Errorf("classifier tape acquisition plan entry %d flags: %w", i, err)
		}
		if len(bytes.TrimSpace(entry.Input)) == 0 || bytes.Equal(bytes.TrimSpace(entry.Input), []byte("null")) {
			return fmt.Errorf("classifier tape acquisition plan entry %d input must be an explicit object", i)
		}
		torrent, err := DecodeTapeClassifierInput(entry.Input)
		if err != nil {
			return fmt.Errorf("classifier tape acquisition plan entry %d input: %w", i, err)
		}
		entry.torrent = torrent
	}
	return nil
}

func validateTapeEvidenceFlags(flags Flags) error {
	if len(flags) != 5 {
		return fmt.Errorf("must contain exactly the five fail-closed classifier flags")
	}
	for _, name := range []string{"local_search_enabled", "apis_enabled", "tmdb_enabled", "delete_xxx"} {
		value, present := flags[name].(bool)
		if !present || value {
			return fmt.Errorf("%s must be explicitly false", name)
		}
	}
	deleteTypes, present := flags["delete_content_types"]
	if !present {
		return errors.New("delete_content_types must be explicitly empty")
	}
	values, ok := deleteTypes.([]any)
	if !ok || values == nil || len(values) != 0 {
		return errors.New("delete_content_types must be explicitly empty")
	}
	return nil
}

func requireTapePlanEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing classifier tape acquisition plan: %w", err)
	}
	return errors.New("classifier tape acquisition plan contains more than one JSON value")
}

func validateTapePlanDigest(digest string) error {
	if len(digest) != len("sha256:")+64 || !strings.HasPrefix(digest, "sha256:") {
		return errors.New("classifier tape acquisition plan SHA-256 must be sha256 followed by 64 lowercase hexadecimal characters")
	}
	if _, err := hex.DecodeString(digest[len("sha256:"):]); err != nil || strings.ToLower(digest) != digest {
		return errors.New("classifier tape acquisition plan SHA-256 must be sha256 followed by 64 lowercase hexadecimal characters")
	}
	return nil
}

func tapePlanConfigured(config Config) (bool, error) {
	pathSet, digestSet := config.TapePlanPath != "", config.TapePlanSHA256 != ""
	if pathSet != digestSet {
		return false, errors.New("classifier tape acquisition plan path and SHA-256 must be configured together")
	}
	if !pathSet {
		return false, nil
	}
	if config.TapeDir == "" {
		return false, errors.New("classifier tape acquisition plan requires classifier tape recording")
	}
	return true, nil
}

type tapeEvidenceCapability int

const (
	tapeEvidencePlanCapability tapeEvidenceCapability = iota + 1
	tapeEvidenceReplayCapability
)

type tapeEvidenceCapabilityKey struct{}

func withTapeEvidenceCapability(ctx context.Context, capability tapeEvidenceCapability) context.Context {
	return context.WithValue(ctx, tapeEvidenceCapabilityKey{}, capability)
}

func hasTapeEvidenceCapability(ctx context.Context) bool {
	capability, _ := ctx.Value(tapeEvidenceCapabilityKey{}).(tapeEvidenceCapability)
	return capability == tapeEvidencePlanCapability || capability == tapeEvidenceReplayCapability
}

func isTapeEvidenceWorkflow(workflow string) bool {
	switch workflow {
	case tapeEvidenceActionEntriesWorkflow, tapeEvidenceUnmatchedWorkflow, tapeEvidenceDeletedWorkflow:
		return true
	default:
		return false
	}
}

func rejectReservedTapeEvidenceWorkflows(source Source) error {
	for workflow := range source.Workflows {
		if isTapeEvidenceWorkflow(workflow) {
			return fmt.Errorf("classifier source defines reserved tape evidence workflow %q", workflow)
		}
	}
	return nil
}

func augmentTapeEvidenceSource(source Source) (Source, error) {
	if err := rejectReservedTapeEvidenceWorkflows(source); err != nil {
		return Source{}, err
	}
	augmented := source
	augmented.Workflows = source.Workflows.merge(workflowSources{
		tapeEvidenceActionEntriesWorkflow: []any{
			map[string]any{findMatchName: []any{attachLocalContentByIDName}},
			map[string]any{findMatchName: []any{attachTMDBContentByIDName}},
			map[string]any{findMatchName: []any{attachLocalContentBySearchName}},
			map[string]any{findMatchName: []any{attachTmdbContentBySearchName}},
		},
		tapeEvidenceUnmatchedWorkflow: unmatchedName,
		tapeEvidenceDeletedWorkflow:   deleteName,
	})
	return augmented, nil
}

func compileTapeEvidenceRunner(source Source, recorder *tape.Recorder) (Runner, error) {
	augmented, err := augmentTapeEvidenceSource(source)
	if err != nil {
		return nil, err
	}
	return (compiler{
		options: []compilerOption{compilerFeatures(defaultFeatures), celEnvOption},
		dependencies: dependencies{
			search:     tapePlanNoIO{},
			tmdbClient: tmdb.NewClient(tapePlanNoIO{}),
		},
		recorder: recorder,
	}).Compile(augmented)
}

type tapePlanNoIO struct{}

func (tapePlanNoIO) ContentByID(context.Context, model.ContentRef) (model.Content, error) {
	return model.Content{}, errors.New("classifier tape evidence workflow escaped to local content by ID")
}

func (tapePlanNoIO) ContentBySearch(context.Context, model.ContentType, string, model.Year) (model.Content, error) {
	return model.Content{}, errors.New("classifier tape evidence workflow escaped to local content search")
}

func (tapePlanNoIO) Content(context.Context, ...query.Option) (search.ContentResult, error) {
	return search.ContentResult{}, errors.New("classifier tape evidence workflow escaped to database search")
}

func (tapePlanNoIO) Request(context.Context, string, map[string]string, any) (*resty.Response, error) {
	return nil, errors.New("classifier tape evidence workflow escaped to TMDB")
}

// TapeAcquisitionPlanExecutor synchronously seeds and verifies a recorder.
type TapeAcquisitionPlanExecutor struct {
	plan     TapeAcquisitionPlan
	runner   Runner
	recorder *tape.Recorder
}

func newTapeAcquisitionPlanExecutor(config Config, source Source, recorder *tape.Recorder) (*TapeAcquisitionPlanExecutor, error) {
	configured, err := tapePlanConfigured(config)
	if err != nil || !configured {
		return nil, err
	}
	if recorder == nil {
		return nil, errors.New("classifier tape acquisition plan requires an initialized recorder")
	}
	plan, err := LoadTapeAcquisitionPlan(config.TapePlanPath, config.TapePlanSHA256)
	if err != nil {
		return nil, err
	}
	total := planExecutionCount(plan)
	if total >= recorder.MaxRecords() {
		return nil, fmt.Errorf(
			"classifier tape acquisition plan has %d executions but recorder cap %d must leave room for organic traffic",
			total,
			recorder.MaxRecords(),
		)
	}
	if recorder.Progress().AcquisitionPlanDigest != plan.digest {
		return nil, fmt.Errorf("classifier tape recorder is not bound to acquisition plan digest %s", plan.digest)
	}
	runner, err := compileTapeEvidenceRunner(source, recorder)
	if err != nil {
		return nil, fmt.Errorf("compile classifier tape evidence runner: %w", err)
	}
	return &TapeAcquisitionPlanExecutor{plan: plan, runner: runner, recorder: recorder}, nil
}

func planExecutionCount(plan TapeAcquisitionPlan) int {
	total := 0
	for _, entry := range plan.Entries {
		total += entry.Repeat
	}
	return total
}

// Run executes the reviewed seed sequentially and returns only after exact
// before/after evidence validation.
func (e *TapeAcquisitionPlanExecutor) Run(ctx context.Context) error {
	if e == nil {
		return nil
	}
	before, err := e.recorder.Records()
	if err != nil {
		return fmt.Errorf("snapshot classifier tape before acquisition plan: %w", err)
	}
	total := planExecutionCount(e.plan)
	if len(before)+total >= e.recorder.MaxRecords() {
		return errors.New("classifier tape acquisition plan would leave no room for organic traffic")
	}
	type expectedRecord struct {
		workflow string
		outcome  tape.RecordOutcomeKind
		actions  []string
	}
	expected := make(map[string]expectedRecord, total)

	for entryIndex, entry := range e.plan.Entries {
		quota := tapeEvidenceWorkflowQuotas[entryIndex]
		for iteration := range entry.Repeat {
			if err := ctx.Err(); err != nil {
				return err
			}
			subject := tapePlanSubject(e.plan.digest, entryIndex, iteration)
			if _, duplicate := expected[subject]; duplicate {
				return fmt.Errorf("classifier tape acquisition plan derived duplicate subject %s", subject)
			}
			for _, record := range before {
				if record.Subject == subject {
					return fmt.Errorf("classifier tape acquisition plan subject %s already exists", subject)
				}
			}
			torrent := rekeyTapePlanTorrent(entry.torrent, protocol.MustParseID(subject))
			runCtx := withTapeEvidenceCapability(tape.WithSubject(ctx, subject), tapeEvidencePlanCapability)
			_, runErr := e.runner.Run(runCtx, entry.Workflow, entry.Flags, torrent)
			if !tapePlanOutcomeMatches(quota.outcome, runErr) {
				if runErr == nil {
					return fmt.Errorf(
						"classifier tape acquisition plan %s iteration %d ended without an error, want %s",
						entry.Workflow,
						iteration,
						quota.outcome,
					)
				}
				return fmt.Errorf(
					"classifier tape acquisition plan %s iteration %d ended with %w, want %s",
					entry.Workflow,
					iteration,
					runErr,
					quota.outcome,
				)
			}
			expected[subject] = expectedRecord{entry.Workflow, quota.outcome, quota.actions}
		}
	}

	after, err := e.recorder.Records()
	if err != nil {
		return fmt.Errorf("snapshot classifier tape after acquisition plan: %w", err)
	}
	if len(after)-len(before) != total {
		return fmt.Errorf("classifier tape acquisition plan recorded %d records, want %d", len(after)-len(before), total)
	}
	seen := 0
	for _, record := range after {
		want, planned := expected[record.Subject]
		if !planned {
			continue
		}
		seen++
		if record.Attempt != 0 || record.Workflow != want.workflow || record.Incomplete || !record.Authoritative() ||
			record.Outcome == nil || record.Outcome.Kind != want.outcome || len(record.Observations) != 0 {
			return fmt.Errorf("classifier tape acquisition plan record failed invariants: %+v", record)
		}
		gotActions := make([]string, 0, len(record.ActionEntries))
		for _, action := range record.ActionEntries {
			gotActions = append(gotActions, action.Name)
		}
		if !equalTapePlanStrings(gotActions, want.actions) {
			return fmt.Errorf("classifier tape acquisition plan record %s actions %v, want %v", record.Subject, gotActions, want.actions)
		}
	}
	if seen != total {
		return fmt.Errorf("classifier tape acquisition plan verified %d records, want %d", seen, total)
	}
	return nil
}

func tapePlanOutcomeMatches(want tape.RecordOutcomeKind, err error) bool {
	switch want {
	case tape.RecordCompleted:
		return err == nil
	case tape.RecordUnmatched:
		return errors.Is(err, classification.ErrUnmatched)
	case tape.RecordDeleted:
		return errors.Is(err, classification.ErrDeleteTorrent)
	default:
		return false
	}
}

func tapePlanSubject(planDigest string, entryIndex, iteration int) string {
	// Cross-language contract: SHA-256 over the UTF-8 bytes of
	// "<sha256:plan-hex>|<zero-based-entry>|<zero-based-iteration>", truncated
	// to its first 20 bytes and encoded as lowercase hex. The value is an
	// identifier, not an integrity primitive; the full plan digest above is.
	seed := fmt.Sprintf("%s|%d|%d", planDigest, entryIndex, iteration)
	digest := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(digest[:20])
}

func rekeyTapePlanTorrent(torrent model.Torrent, subject protocol.ID) model.Torrent {
	rekeyed := torrent
	rekeyed.InfoHash = subject
	rekeyed.Files = append([]model.TorrentFile(nil), torrent.Files...)
	for i := range rekeyed.Files {
		rekeyed.Files[i].InfoHash = subject
	}
	rekeyed.Contents = append([]model.TorrentContent(nil), torrent.Contents...)
	for i := range rekeyed.Contents {
		rekeyed.Contents[i].InfoHash = subject
	}
	if !rekeyed.Hint.IsNil() {
		rekeyed.Hint.InfoHash = subject
	}
	return rekeyed
}

func equalTapePlanStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
