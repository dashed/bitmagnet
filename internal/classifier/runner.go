package classifier

import (
	"context"
	"errors"
	"fmt"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protobuf"
	"github.com/bitmagnet-io/bitmagnet/internal/tape"
	"github.com/google/cel-go/common/types/ref"
)

type runner struct {
	dependencies
	flagDefinitions
	compiledFlags
	workflows map[string]action
	// recorder is nil unless observation recording is configured, in which case
	// Run opens a session for each classification. Nil is the only state a
	// serving deployment is ever in.
	recorder *tape.Recorder
}

func (r runner) Run(
	ctx context.Context,
	workflow string,
	flags Flags,
	t model.Torrent,
	// Named so the deferred session end can read how the classification
	// actually finished; see the tape.EndSession call below.
) (result classification.Result, err error) {
	w, ok := r.workflows[workflow]
	if !ok {
		return classification.Result{}, fmt.Errorf("workflow not found: %s", workflow)
	}

	cfs := make(map[string]ref.Val, len(r.flagDefinitions))

	for k, d := range r.flagDefinitions {
		if runtimeRawVal, ok := flags[k]; ok {
			rcf, err := d.celVal(runtimeRawVal)
			if err != nil {
				return classification.Result{}, fmt.Errorf(
					"invalid value for runtime flag '%s': %w",
					k,
					err,
				)
			}

			cfs[k] = rcf
		} else {
			cfs[k] = r.compiledFlags[k]
		}
	}

	cl := classification.Result{}
	if !t.Hint.IsNil() {
		cl.ApplyHint(t.Hint)
	}
	// if possible, attach the existing content to the result to save some work:
	if !t.Hint.IsNil() && t.Hint.ContentSource.Valid {
		for _, tc := range t.Contents {
			if tc.ContentType.Valid &&
				tc.ContentType.ContentType == t.Hint.ContentType &&
				tc.ContentSource.Valid &&
				tc.ContentSource.String == t.Hint.ContentSource.String &&
				tc.ContentID.String == t.Hint.ContentID.String &&
				tc.Content.Source == tc.ContentSource.String {
				content := tc.Content
				cl.AttachContent(&content)

				break
			}
		}
	}

	// Opening the session here, once per classification, is what keys the tape:
	// every observation the workflow goes on to make belongs to this subject and
	// is numbered in the order it was made. Classifications run concurrently but
	// each holds its own session, so the interleaving between them cannot
	// disturb the sequence within one.
	if r.recorder != nil {
		subject := r.subject(ctx, t)
		ctx = r.recorder.Begin(
			ctx,
			subject,
			workflow,
			effectiveFlagValues(cfs),
			newTapeClassifierInput(subject, t),
		)
		// Closing the session is what lets a tape written mid-run tell a
		// finished classification from one whose observations are still
		// arriving; the outcome is what lets it tell a classification that
		// consulted nothing from one that never got the chance. Both are needed:
		// an early exit CLOSES its session, so it would otherwise be written as
		// a complete record with an empty observation list.
		sessionCtx := ctx
		defer func() { tape.EndSession(sessionCtx, classificationOutcome(sessionCtx, err)) }()
	}

	exCtx := executionContext{
		Context:      ctx,
		dependencies: r.dependencies,
		workflows:    r.workflows,
		flags:        cfs,
		torrent:      t,
		torrentPb:    protobuf.NewTorrent(t),
		result:       cl,
	}

	return w.run(exCtx)
}

// classificationOutcome maps how the workflow ended onto the tape's vocabulary.
//
// It keys on the SENTINELS rather than on message text, because the distinction
// the tape needs is whether a replay of the same input would stop in the same
// place: `unmatched` and `delete` are the workflow's own deterministic endings
// and it would, while a cancellation is a property of the process shutting down
// and it would not.
func classificationOutcome(ctx context.Context, err error) tape.RecordOutcome {
	// The workflow's own vocabulary first: `delete` and `unmatched` are endings
	// a replay of the same input reaches too, and they mean what they say even
	// if the context has since gone away.
	switch {
	case errors.Is(err, classification.ErrDeleteTorrent):
		return tape.RecordOutcome{Kind: tape.RecordDeleted}
	case errors.Is(err, classification.ErrUnmatched):
		return tape.RecordOutcome{Kind: tape.RecordUnmatched}
	}

	// 🚨 A cancelled context makes the observation list untrustworthy whether or
	// not the classification itself noticed: a seam can short-circuit without
	// surfacing an error, and a workflow that never touched the context returns
	// a clean nil while having done less than it would have. Conservative on
	// purpose -- excluding a sound record costs a little coverage, whereas
	// trusting a truncated one fabricates a divergence, which is the exact
	// failure this field exists to prevent.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return tape.RecordOutcome{Kind: tape.RecordCanceled, Error: ctxErr.Error()}
	}

	switch {
	case err == nil:
		return tape.RecordOutcome{Kind: tape.RecordCompleted}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return tape.RecordOutcome{Kind: tape.RecordCanceled, Error: err.Error()}
	default:
		return tape.RecordOutcome{Kind: tape.RecordFailed, Error: err.Error()}
	}
}

// subject identifies the classification in the tape. The info hash is the
// natural key in production; corpora whose fixtures share a placeholder info
// hash stamp their own id with tape.WithSubject.
func (r runner) subject(ctx context.Context, t model.Torrent) string {
	if subject, ok := tape.SubjectFrom(ctx); ok {
		return subject
	}

	return t.InfoHash.String()
}

// effectiveFlagValues renders the flag state the classification actually ran
// under -- compiled defaults with the run's overrides applied -- rather than
// just the overrides, since it is the effective state that decides which
// enrichment actions execute.
func effectiveFlagValues(flags map[string]ref.Val) map[string]any {
	values := make(map[string]any, len(flags))
	for name, value := range flags {
		values[name] = value.Value()
	}

	return values
}
