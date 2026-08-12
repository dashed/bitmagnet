package classifier

import (
	"context"
	"fmt"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/database/query"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	"github.com/go-resty/resty/v2"
)

// TapeReplayer runs Go's real classifier against a previously recorded
// dependency session. The fallback dependencies always fail: a replay that
// escapes the tape cannot silently reach PostgreSQL or TMDB.
type TapeReplayer struct {
	tape   *tape.Replay
	runner Runner
}

// TapeReplayResult is the deterministic classifier boundary consumed by the
// processor write-set parity harness.
type TapeReplayResult struct {
	Record         tape.Record
	Torrent        model.Torrent
	Classification classification.Result
	Outcome        tape.RecordOutcome
}

// NewTapeReplayer compiles the embedded Go classifier once and binds it to a
// fail-closed tape replay.
func NewTapeReplayer(replay *tape.Replay) (*TapeReplayer, error) {
	if replay == nil {
		return nil, fmt.Errorf("classifier tape replay is nil")
	}

	source, err := yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}.source()
	if err != nil {
		return nil, fmt.Errorf("load core classifier source: %w", err)
	}

	for _, record := range replay.Subjects() {
		if isTapeEvidenceWorkflow(record.Workflow) {
			source, err = augmentTapeEvidenceSource(source)
			if err != nil {
				return nil, fmt.Errorf("augment core classifier for tape evidence replay: %w", err)
			}
			break
		}
	}

	compiled, err := (compiler{
		options: []compilerOption{
			compilerFeatures(defaultFeatures),
			celEnvOption,
		},
		dependencies: dependencies{
			search: localSearchSemaphore{
				search:    localSearch{replayOnlyContentSearch{}},
				semaphore: make(chan struct{}, 1),
			},
			tmdbClient: tmdb.NewClient(replayOnlyTMDBRequester{}),
		},
	}).Compile(source)
	if err != nil {
		return nil, fmt.Errorf("compile core classifier for tape replay: %w", err)
	}

	return &TapeReplayer{tape: replay, runner: compiled}, nil
}

// Run replays one exact record. A successful return means Go reproduced the
// record's terminal outcome and consumed every recorded observation and, when
// available, every recorded action entry.
func (r *TapeReplayer) Run(ctx context.Context, record tape.Record) (TapeReplayResult, error) {
	if !record.Authoritative() {
		return TapeReplayResult{}, fmt.Errorf(
			"classifier tape record %s#%d is not authoritative",
			record.Subject,
			record.Attempt,
		)
	}
	if len(record.Input) == 0 {
		return TapeReplayResult{}, fmt.Errorf(
			"classifier tape record %s#%d has no embedded input",
			record.Subject,
			record.Attempt,
		)
	}

	torrent, err := DecodeTapeClassifierInput(record.Input)
	if err != nil {
		return TapeReplayResult{}, fmt.Errorf(
			"classifier tape record %s#%d input: %w",
			record.Subject,
			record.Attempt,
			err,
		)
	}
	if torrent.InfoHash.String() != record.Subject {
		return TapeReplayResult{}, fmt.Errorf(
			"classifier tape record subject %q does not match input id %q",
			record.Subject,
			torrent.InfoHash.String(),
		)
	}

	replayCtx := r.tape.Begin(ctx, record.Subject, record.Attempt)
	if isTapeEvidenceWorkflow(record.Workflow) {
		replayCtx = withTapeEvidenceCapability(replayCtx, tapeEvidenceReplayCapability)
	}
	result, runErr := r.runner.Run(replayCtx, record.Workflow, Flags(record.Flags), torrent)
	actualOutcome := classificationOutcome(replayCtx, runErr)
	if record.Outcome == nil || actualOutcome.Kind != record.Outcome.Kind {
		want := tape.RecordOutcomeKind("unknown")
		if record.Outcome != nil {
			want = record.Outcome.Kind
		}
		return TapeReplayResult{}, fmt.Errorf(
			"classifier tape record %s#%d outcome mismatch: Go replay got %q (error %v), recorded %q",
			record.Subject,
			record.Attempt,
			actualOutcome.Kind,
			runErr,
			want,
		)
	}

	session := tape.SessionFrom(replayCtx)
	if session == nil {
		return TapeReplayResult{}, fmt.Errorf(
			"classifier tape record %s#%d did not open a replay session",
			record.Subject,
			record.Attempt,
		)
	}
	if err := session.VerifyComplete(); err != nil {
		return TapeReplayResult{}, fmt.Errorf(
			"classifier tape record %s#%d did not consume the recording: %w",
			record.Subject,
			record.Attempt,
			err,
		)
	}

	return TapeReplayResult{
		Record:         record,
		Torrent:        torrent,
		Classification: result,
		Outcome:        actualOutcome,
	}, nil
}

type replayOnlyContentSearch struct{}

func (replayOnlyContentSearch) Content(
	context.Context,
	...query.Option,
) (search.ContentResult, error) {
	return search.ContentResult{}, fmt.Errorf("classifier tape replay escaped to live content search")
}

type replayOnlyTMDBRequester struct{}

func (replayOnlyTMDBRequester) Request(
	context.Context,
	string,
	map[string]string,
	any,
) (*resty.Response, error) {
	return nil, fmt.Errorf("classifier tape replay escaped to live TMDB")
}
