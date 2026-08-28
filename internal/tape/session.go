package tape

import (
	"context"
	"encoding/json"
	"sync"
)

type contextKey struct{}

// SessionFrom returns the tape session carried by ctx, or nil.
//
// This is the whole of the enable check at an observation seam: with no session
// on the context -- the state of every normally configured process -- a seam
// does nothing but this lookup and a nil comparison.
func SessionFrom(ctx context.Context) *Session {
	session, _ := ctx.Value(contextKey{}).(*Session)
	return session
}

// WithSubject stamps an explicit subject identifier onto ctx, overriding the
// identity the recording would otherwise derive (in the classifier, the torrent
// info hash). It exists for corpora whose fixtures carry a stable id but no
// meaningful info hash.
func WithSubject(ctx context.Context, subject string) context.Context {
	return context.WithValue(ctx, subjectKey{}, subject)
}

type subjectKey struct{}

// SubjectFrom returns the explicit subject stamped by [WithSubject], if any.
func SubjectFrom(ctx context.Context) (string, bool) {
	subject, ok := ctx.Value(subjectKey{}).(string)
	return subject, ok
}

// Session is the per-classification handle a seam observes through. It is
// either recording or replaying; see [Recorder.Begin] and [Replay.Begin].
//
// A session belongs to a single classification and is never shared between
// them, so its observation sequence is per-subject and is unaffected by other
// classifications running concurrently. The session itself is mutex-guarded
// because nothing in the classifier's contract forbids a workflow from
// observing concurrently within one classification.
type Session struct {
	subject  string
	attempt  int
	recorder *Recorder

	// replay state; a nil recorder means this is a replay session.
	mu              sync.Mutex
	recorded        []Observation
	cursor          int
	recordedActions []ActionEntry
	actionCursor    int
	actionsKnown    bool
}

// End marks the classification finished with the given outcome, so a tape
// written from now on records its observation list as complete AND says how the
// classification ended. Ending twice, or ending a replay session, is harmless.
//
// The outcome is not optional: an observation list without one cannot be told
// apart from a prefix left by an early exit. See [Outcome].
func (s *Session) End(outcome RecordOutcome) {
	if s == nil {
		return
	}

	s.recorder.endSession(s.subject, s.attempt, outcome)
}

// EndSession ends the session carried by ctx, if there is one. It is the form
// callers want at the end of a classification:
// `defer func() { tape.EndSession(ctx, outcomeFor(err)) }()`. It costs a context
// lookup and a nil check when recording is off.
func EndSession(ctx context.Context, outcome RecordOutcome) {
	SessionFrom(ctx).End(outcome)
}

// Subject reports the identity this session's observations are keyed to.
func (s *Session) Subject() string { return s.subject }

// Attempt reports which classification of [Session.Subject] this is within the
// run, counting from zero.
func (s *Session) Attempt() int { return s.attempt }

// Replaying reports whether this session answers from a recording instead of
// recording live observations.
func (s *Session) Replaying() bool { return s.recorder == nil }

// EnterAction records or verifies one workflow action invocation. Recording
// appends the action to this classification's ordered trace. Replay compares it
// immediately with the next recorded entry, so a wrong branch fails at the
// branch boundary rather than being inferred later from dependency traffic.
func (s *Session) EnterAction(name string) error {
	if s == nil {
		return nil
	}

	if s.recorder != nil {
		s.recorder.appendActionEntry(s.subject, s.attempt, ActionEntry{Name: name})
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.actionsKnown {
		return nil
	}

	sequence := s.actionCursor
	s.actionCursor++

	if sequence >= len(s.recordedActions) {
		return &ActionMissError{
			Subject:  s.subject,
			Attempt:  s.attempt,
			Sequence: sequence,
			Name:     name,
		}
	}

	want := s.recordedActions[sequence].Name
	if want != name {
		return &ActionDesyncError{
			Subject:  s.subject,
			Attempt:  s.attempt,
			Sequence: sequence,
			WantName: want,
			GotName:  name,
		}
	}

	return nil
}

// EnterAction records or verifies an action on the session carried by ctx. A
// normally configured serving process has no session, so this is one context
// lookup and a nil check outside evidence mode.
func EnterAction(ctx context.Context, name string) error {
	return SessionFrom(ctx).EnterAction(name)
}

// RemainingObservations reports how many recorded dependency observations a
// replay has not consumed. It is zero for a recording session.
func (s *Session) RemainingObservations() int {
	if s == nil || s.recorder != nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return remaining(len(s.recorded), s.cursor)
}

// RemainingActions reports the unconsumed ordered action entries and whether
// the tape knows the trace. known=false is the legacy-v1 case: absence means
// unknown and must not be treated as an empty trace.
func (s *Session) RemainingActions() (remainingActions int, known bool) {
	if s == nil || s.recorder != nil {
		return 0, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return remaining(len(s.recordedActions), s.actionCursor), s.actionsKnown
}

// VerifyComplete checks the two independent replay streams. Observation
// exhaustion is always known; action exhaustion is enforced only when the
// manifest declares action-entry capability.
func (s *Session) VerifyComplete() error {
	if s == nil || s.recorder != nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	remainingObservations := remaining(len(s.recorded), s.cursor)
	remainingActions := remaining(len(s.recordedActions), s.actionCursor)
	if remainingObservations == 0 && (!s.actionsKnown || remainingActions == 0) {
		return nil
	}

	return &UnconsumedError{
		Subject:               s.subject,
		Attempt:               s.attempt,
		RemainingObservations: remainingObservations,
		RemainingActions:      remainingActions,
		ActionsKnown:          s.actionsKnown,
	}
}

func remaining(total, consumed int) int {
	if consumed >= total {
		return 0
	}

	return total - consumed
}

// Observe appends a successful observation.
//
// A nil or empty response value is still encoded explicitly, so a genuine empty
// answer is recorded as such and can never be confused with a gap.
func (s *Session) Observe(kind string, request, response any) {
	requestJSON, responseJSON, err := marshalPair(request, response)
	if err != nil {
		s.recorder.fail(err)
		return
	}

	s.append(Observation{
		Kind:     kind,
		Request:  requestJSON,
		Outcome:  OutcomeOK,
		Response: responseJSON,
	})
}

// ObserveError appends a failed observation. errKind is a stable discriminator
// the replay uses to reconstruct the original error value, because the
// classifier's control flow depends on error identity and not on error text.
func (s *Session) ObserveError(kind string, request any, errKind, message string) {
	s.ObserveErrorDetail(kind, request, errKind, message, nil)
}

// ObserveErrorDetail is [Session.ObserveError] with optional evidence attached
// to the failure. detail is encoded but never consulted by a replay.
func (s *Session) ObserveErrorDetail(kind string, request any, errKind, message string, detail any) {
	requestJSON, err := Marshal(request)
	if err != nil {
		s.recorder.fail(err)
		return
	}

	observationError := &ObservationError{Kind: errKind, Message: message}

	if detail != nil {
		detailJSON, err := Marshal(detail)
		if err != nil {
			s.recorder.fail(err)
			return
		}

		observationError.Detail = detailJSON
	}

	s.append(Observation{
		Kind:    kind,
		Request: requestJSON,
		Outcome: OutcomeError,
		Error:   observationError,
	})
}

func marshalPair(request, response any) (requestJSON, responseJSON json.RawMessage, err error) {
	requestJSON, err = Marshal(request)
	if err != nil {
		return nil, nil, err
	}

	responseJSON, err = Marshal(response)
	if err != nil {
		return nil, nil, err
	}

	return requestJSON, responseJSON, nil
}

func (s *Session) append(observation Observation) {
	s.recorder.appendObservation(s.subject, s.attempt, observation)
}

// Next returns the response recorded at the session's current position,
// asserting that the caller is asking the recorded question.
//
// It returns a [*MissError] (matching [ErrMiss]) when the recording holds no
// observation at this position, and a [*DesyncError] (matching [ErrDesync])
// when the kind or the request differs from what was recorded. A recorded
// failure is returned as a [*ObservationError] in the second result with a nil
// error, so the caller can rebuild the dependency's original error value.
func (s *Session) Next(kind string, request any) (json.RawMessage, *ObservationError, error) {
	requestJSON, err := Marshal(request)
	if err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	sequence := s.cursor
	s.cursor++
	s.mu.Unlock()

	if sequence >= len(s.recorded) {
		return nil, nil, &MissError{
			Subject:  s.subject,
			Attempt:  s.attempt,
			Sequence: sequence,
			Kind:     kind,
		}
	}

	observation := s.recorded[sequence]
	if observation.Kind != kind || !equalJSON(observation.Request, requestJSON) {
		return nil, nil, &DesyncError{
			Subject:         s.subject,
			Attempt:         s.attempt,
			Sequence:        sequence,
			WantKind:        observation.Kind,
			GotKind:         kind,
			WantRequestJSON: string(observation.Request),
			GotRequestJSON:  string(requestJSON),
		}
	}

	if observation.Outcome == OutcomeError {
		return nil, observation.Error, nil
	}

	return observation.Response, nil, nil
}

// equalJSON compares two canonically encoded values. Both sides come from
// [Marshal], which sorts map keys and disables HTML escaping, so a byte
// comparison is a value comparison.
func equalJSON(a, b json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
