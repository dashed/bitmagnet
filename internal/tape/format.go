package tape

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Schema identifies the on-disk tape format. A reader refuses a tape whose
// schema it does not recognise.
const Schema = "bitmagnet.classifier-attach-tape/v1"

// Observation outcomes. Exactly one of Response / Error is populated, selected
// by Outcome; a reader rejects any other combination. OutcomeOK with an empty
// response body is a genuine empty answer, which is deliberately distinct from
// the observation being absent from the tape altogether (see [ErrMiss]).
const (
	OutcomeOK    = "ok"
	OutcomeError = "error"
)

// Observation is a single interaction with an impure dependency.
//
// Request is the canonically encoded question that was asked. It is the field
// a replay asserts against; see [ErrDesync].
type Observation struct {
	Kind     string            `json:"kind"`
	Request  json.RawMessage   `json:"request"`
	Outcome  string            `json:"outcome"`
	Response json.RawMessage   `json:"response,omitempty"`
	Error    *ObservationError `json:"error,omitempty"`
}

// ObservationError carries an error in a form a replay can turn back into the
// same error value the dependency originally produced. Kind is a stable
// discriminator owned by the recording package (for example the TMDB recorder
// distinguishes an unauthorized response from a not-found response, because
// the classifier's find_match fallthrough treats them differently); Message is
// the original error text.
// Detail is optional evidence about the failure -- an HTTP status and error
// body, say. Replay must not need it to reconstruct the error; it is there so a
// human reading the tape can see what actually came back.
type ObservationError struct {
	Kind    string          `json:"kind"`
	Message string          `json:"message"`
	Detail  json.RawMessage `json:"detail,omitempty"`
}

// Record is one classification: the subject that was classified, the flag
// state it ran under, and the observations it made, in the order it made them.
//
// One line of the tape file is one Record. Observation order is the slice
// order; a replay consumes observations by position, so a port that consults
// its dependencies in a different order desyncs.
//
// Attempt disambiguates repeat classifications of the same subject within a
// single recording run. In a normal run every subject is classified once and
// Attempt is always 0.
// Incomplete marks a record whose classification had not finished when the tape
// was written -- possible when a run is snapshotted at its record cap while
// other classifications are still in flight. Its observation list is a prefix,
// so it is not an oracle for that subject and a reader excludes it: asking
// about the subject then reports a miss rather than a short answer.
type Record struct {
	Subject  string         `json:"subject"`
	Attempt  int            `json:"attempt"`
	Workflow string         `json:"workflow"`
	Flags    map[string]any `json:"flags"`
	// Input is the classifier input as it existed when this record began. It is
	// encoded immediately by Recorder.Begin, before the workflow can attach
	// content or otherwise make a later database snapshot disagree with the
	// state Go actually classified.
	//
	// Nil is valid for tapes recorded before input capture existed. Readers must
	// use their legacy out-of-band input only in that case; a present value is
	// the authoritative input for this specific subject AND attempt.
	Input        json.RawMessage `json:"input,omitempty"`
	Observations []Observation   `json:"observations"`
	Incomplete   bool            `json:"incomplete,omitempty"`
	// Outcome is how the classification ended. Nil means either that the record
	// was still open when the tape was written (see Incomplete) or that the tape
	// predates outcome recording -- in both cases the outcome is UNKNOWN, which
	// is not the same as "it finished normally".
	Outcome *RecordOutcome `json:"outcome,omitempty"`
}

// RecordOutcomeKind is how a classification ended.
type RecordOutcomeKind string

const (
	// RecordCompleted: the workflow ran to the end and returned a result.
	RecordCompleted RecordOutcomeKind = "completed"
	// RecordUnmatched: the workflow ended with ErrUnmatched.
	RecordUnmatched RecordOutcomeKind = "unmatched"
	// RecordDeleted: the workflow ended with ErrDeleteTorrent.
	RecordDeleted RecordOutcomeKind = "deleted"
	// RecordCanceled: the context was cancelled or timed out -- e.g. the
	// process was shutting down. NOT reproducible, so the observation list is a
	// prefix that happens to stop wherever the cancellation landed.
	RecordCanceled RecordOutcomeKind = "canceled"
	// RecordFailed: anything else went wrong.
	RecordFailed RecordOutcomeKind = "error"
)

// RecordOutcome is how a classification ended.
//
// 🚨 Why the tape needs this at all. Without it, a record holding an EMPTY
// observation list is ambiguous between two opposite claims:
//
//   - the workflow ran to completion and legitimately consulted nothing, so a
//     replay that consults nothing agrees and one that consults something has
//     diverged; and
//   - the classification never got that far -- cancelled at shutdown, or stopped
//     by an error -- so the list is a PREFIX and proves nothing either way.
//
// Reading the second as the first manufactures a divergence out of a recording
// artifact, which is exactly what happened to two subjects of the 2026-08-09
// production corpus: the gate reported them as misses, and running Go's own
// classifier over their input showed Go asks precisely what the replay asked.
//
// [Record.Incomplete] does not cover this. It is set only while a session is
// still OPEN when the tape is written, and a classification that ends early
// closes its session on the way out -- so it is written as a complete record
// with nothing in it.
type RecordOutcome struct {
	Kind RecordOutcomeKind `json:"kind"`
	// Error is the message for the failing kinds, kept for diagnosis only.
	// Consumers must key on Kind: the text is not part of the contract.
	Error string `json:"error,omitempty"`
}

// Authoritative reports whether this record's observation list can be read as a
// COMPLETE account of what the classification asked.
//
// It is true only for the endings a replay of the same input reaches too:
// running to completion, or stopping at `unmatched` / `delete`, which are the
// workflow's own deterministic vocabulary. A cancellation is not reproducible,
// an error may not be, and an unknown outcome is not a claim at all -- for those
// the list is a prefix, and "the replay asked more" says nothing about parity.
func (r Record) Authoritative() bool {
	if r.Incomplete || r.Outcome == nil {
		return false
	}

	switch r.Outcome.Kind {
	case RecordCompleted, RecordUnmatched, RecordDeleted:
		return true
	case RecordCanceled, RecordFailed:
		return false
	default:
		return false
	}
}

// Manifest is the tape header, written alongside the records.
//
// EffectiveConfigDigest pins the classifier configuration the tape was recorded
// under. A replay that loads a tape recorded under a different digest fails
// closed rather than comparing against a stale oracle.
type Manifest struct {
	Schema                string `json:"schema"`
	EffectiveConfigDigest string `json:"effectiveConfigDigest"`
	GeneratedAt           string `json:"generatedAt"`
	Recorder              string `json:"recorder"`
	RecordCount           int    `json:"recordCount"`
	ObservationCount      int    `json:"observationCount"`
	// IncompleteRecordCount is how many of those records were still being
	// classified when the tape was written. They are excluded from replay.
	IncompleteRecordCount int `json:"incompleteRecordCount"`
	// AuthoritativeRecordCount is how many records are a COMPLETE account of
	// what their classification asked -- see [Record.Authoritative]. The rest
	// hold a prefix, so for them "the replay asked more" is not evidence of a
	// divergence.
	//
	// It is in the manifest because it is the honest size of the oracle, and a
	// reader who only sees RecordCount will overstate it.
	AuthoritativeRecordCount int `json:"authoritativeRecordCount"`
	// RecordOutcomeCounts breaks the records down by how they ended, so a tape
	// with an unexpected number of cancellations is visible without reading
	// every line.
	RecordOutcomeCounts map[string]int `json:"recordOutcomeCounts,omitempty"`
	// Truncated is set when the recording hit its observation cap and stopped
	// recording. A truncated tape is not a complete oracle and a replay of the
	// full population against it will report misses.
	Truncated bool `json:"truncated"`
}

var (
	// ErrMiss reports that the tape holds no observation at the requested
	// position. It is deliberately distinct from a recorded empty response: a
	// miss means the recording never saw this question, so the replay has no
	// answer and must not invent one.
	ErrMiss = errors.New("tape: observation not recorded")

	// ErrDesync reports that the caller asked a different question from the one
	// recorded at this position -- a different kind, or a different request.
	ErrDesync = errors.New("tape: request does not match the recording")
)

// DesyncError describes a specific desync. It is the highest-signal failure the
// replay produces: it says the port asked the wrong question, independently of
// whether the answer would have matched.
type DesyncError struct {
	Subject         string
	Attempt         int
	Sequence        int
	WantKind        string
	GotKind         string
	WantRequestJSON string
	GotRequestJSON  string
}

func (e *DesyncError) Error() string {
	if e.WantKind != e.GotKind {
		return fmt.Sprintf(
			"tape: desync at %s#%d[%d]: recorded kind %q, replay asked %q",
			e.Subject, e.Attempt, e.Sequence, e.WantKind, e.GotKind,
		)
	}

	return fmt.Sprintf(
		"tape: desync at %s#%d[%d] (%s): recorded request %s, replay asked %s",
		e.Subject, e.Attempt, e.Sequence, e.WantKind, e.WantRequestJSON, e.GotRequestJSON,
	)
}

func (e *DesyncError) Is(target error) bool { return target == ErrDesync }

// MissError describes a specific miss.
type MissError struct {
	Subject  string
	Attempt  int
	Sequence int
	Kind     string
}

func (e *MissError) Error() string {
	return fmt.Sprintf(
		"tape: no observation recorded at %s#%d[%d] (%s)",
		e.Subject, e.Attempt, e.Sequence, e.Kind,
	)
}

func (e *MissError) Is(target error) bool { return target == ErrMiss }

// Marshal encodes a value for the tape.
//
// HTML escaping is disabled so the bytes stay language-neutral: encoding/json
// would otherwise rewrite <, > and & in titles and search strings, which
// serde_json does not. Map keys are sorted by encoding/json, so the output is
// stable and tapes diff cleanly.
func Marshal(value any) (json.RawMessage, error) {
	var buf bytes.Buffer

	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(value); err != nil {
		return nil, err
	}

	return json.RawMessage(bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})), nil
}

// validate checks the invariants a reader relies on. It is applied to every
// record read from disk so a malformed or hand-edited tape fails closed instead
// of silently answering questions wrongly.
func (r Record) validate() error {
	if r.Subject == "" {
		return errors.New("record has an empty subject")
	}

	if r.Attempt < 0 {
		return fmt.Errorf("record %q has a negative attempt %d", r.Subject, r.Attempt)
	}

	if r.Observations == nil {
		return fmt.Errorf("record %q has a null observations list; an empty list must encode as []", r.Subject)
	}

	if bytes.Equal(bytes.TrimSpace(r.Input), []byte("null")) {
		return fmt.Errorf("record %q has a null input; an unavailable legacy input must be absent", r.Subject)
	}

	for i, obs := range r.Observations {
		if err := obs.validate(); err != nil {
			return fmt.Errorf("record %q observation %d: %w", r.Subject, i, err)
		}
	}

	return nil
}

func (o Observation) validate() error {
	if o.Kind == "" {
		return errors.New("observation has an empty kind")
	}

	if len(o.Request) == 0 {
		return errors.New("observation has no request; the request is what a replay asserts against")
	}

	switch o.Outcome {
	case OutcomeOK:
		if o.Error != nil {
			return errors.New(`observation has outcome "ok" but carries an error`)
		}
		// A response of "null" is rejected as well as an absent one: an absent
		// or null response is indistinguishable from a gap, and a gap must
		// never be read as a legitimate empty answer.
		if len(o.Response) == 0 || bytes.Equal(o.Response, []byte("null")) {
			return errors.New(
				`observation has outcome "ok" but no response; ` +
					"a genuine empty answer must encode its emptiness explicitly",
			)
		}
	case OutcomeError:
		if len(o.Response) != 0 {
			return errors.New(`observation has outcome "error" but carries a response`)
		}

		if o.Error == nil {
			return errors.New(`observation has outcome "error" but no error`)
		}

		if o.Error.Kind == "" {
			return errors.New("observation error has an empty kind")
		}
	default:
		return fmt.Errorf("observation has an unknown outcome %q", o.Outcome)
	}

	return nil
}
