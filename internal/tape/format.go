package tape

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	Input json.RawMessage `json:"input,omitempty"`
	// ActionEntries records the attach actions the workflow entered, in exact
	// execution order. It is deliberately separate from Observations: entering
	// an action and consulting an impure dependency are different facts, because
	// an action can return unmatched before it reaches its dependency seam.
	//
	// Absent on legacy v1 tapes. A present list is recorder-only evidence; replay
	// still consumes dependency observations independently.
	ActionEntries []ActionEntry `json:"actionEntries,omitempty"`
	// ProcessorState is the pre-classification processor state needed to
	// reconstruct classification-derived writes that is intentionally not part
	// of the classifier input. Absent means unknown on a legacy tape; a new
	// writer emits the object even when ExistingContentIDs is empty.
	ProcessorState *ProcessorState `json:"processorState,omitempty"`
	Observations   []Observation   `json:"observations"`
	Incomplete     bool            `json:"incomplete,omitempty"`
	// Outcome is how the classification ended. Nil means either that the record
	// was still open when the tape was written (see Incomplete) or that the tape
	// predates outcome recording -- in both cases the outcome is UNKNOWN, which
	// is not the same as "it finished normally".
	Outcome *RecordOutcome `json:"outcome,omitempty"`

	// actionEntriesPresent distinguishes a legacy record that predates action
	// tracing from an explicit empty actionEntries array. It is populated only
	// while decoding; a new run's manifest-level ActionEntryCount is the
	// capability signal when omitempty elides a traced empty list.
	actionEntriesPresent bool
}

// UnmarshalJSON preserves the absent/null distinction for ActionEntries. A
// missing field is valid legacy v1 data, while an explicit null is malformed:
// a known empty trace must be [] (or be omitted under a manifest that declares
// action-trace capability), never null.
func (r *Record) UnmarshalJSON(data []byte) error {
	type recordWire Record

	var decoded recordWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	rawActions, actionsPresent := fields["actionEntries"]
	if actionsPresent && bytes.Equal(bytes.TrimSpace(rawActions), []byte("null")) {
		return errors.New("record has null actionEntries; an empty action trace must encode as [] or be omitted")
	}
	rawProcessorState, processorStatePresent := fields["processorState"]
	if processorStatePresent && bytes.Equal(bytes.TrimSpace(rawProcessorState), []byte("null")) {
		return errors.New("record has null processorState; unavailable legacy state must be absent")
	}

	*r = Record(decoded)
	r.actionEntriesPresent = actionsPresent

	return nil
}

// ActionEntry is one workflow action invocation. The object shape leaves room
// for future action evidence without changing the ordered Record field from a
// list of strings into a different type.
type ActionEntry struct {
	Name string `json:"name"`
}

// ProcessorState is the portion of the processor's classifier-time state that
// affects write-set materialization but not classification. IDs preserve the
// torrent.Contents slice order observed by Go; downstream canonical write-set
// comparison may sort its own delete list explicitly.
type ProcessorState struct {
	ExistingContentIDs []string `json:"existingContentIds"`
}

// cloneRecord returns a replay-safe copy whose mutable slices, maps, raw JSON,
// and nested error/outcome objects do not alias recorder or replay state.
func cloneRecord(record Record) Record {
	cloned := record
	cloned.Input = bytes.Clone(record.Input)
	cloned.Flags = make(map[string]any, len(record.Flags))
	for key, value := range record.Flags {
		cloned.Flags[key] = cloneJSONValue(value)
	}
	cloned.ActionEntries = append([]ActionEntry(nil), record.ActionEntries...)
	if record.ProcessorState != nil {
		state := *record.ProcessorState
		state.ExistingContentIDs = append([]string{}, record.ProcessorState.ExistingContentIDs...)
		cloned.ProcessorState = &state
	}
	cloned.Observations = make([]Observation, len(record.Observations))
	for index, observation := range record.Observations {
		cloned.Observations[index] = observation
		cloned.Observations[index].Request = bytes.Clone(observation.Request)
		cloned.Observations[index].Response = bytes.Clone(observation.Response)
		if observation.Error != nil {
			observationError := *observation.Error
			observationError.Detail = bytes.Clone(observation.Error.Detail)
			cloned.Observations[index].Error = &observationError
		}
	}
	if record.Outcome != nil {
		outcome := *record.Outcome
		cloned.Outcome = &outcome
	}
	return cloned
}

func cloneJSONValue(value any) any {
	switch typed := value.(type) {
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneJSONValue(item)
		}
		return cloned
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		for key, item := range typed {
			cloned[key] = cloneJSONValue(item)
		}
		return cloned
	default:
		return value
	}
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
	// AcquisitionPlanDigest is absent for legacy and organic-only tapes. When
	// present it pins the exact acquisition-plan file used to seed the run.
	AcquisitionPlanDigest string `json:"acquisitionPlanDigest,omitempty"`
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
	// ActionEntryCount and ActionEntryCounts summarize Record.ActionEntries.
	// ActionEntryCount is a pointer so a reader can distinguish a legacy v1
	// manifest (field absent: action entry recording was unavailable) from a new
	// recording that entered zero attach actions (field present with value 0).
	// The per-name map is omitted when that known total is zero.
	ActionEntryCount  *int           `json:"actionEntryCount,omitempty"`
	ActionEntryCounts map[string]int `json:"actionEntryCounts,omitempty"`
	// Truncated is set when the recording hit its observation cap and stopped
	// recording. A truncated tape is not a complete oracle and a replay of the
	// full population against it will report misses.
	Truncated bool `json:"truncated"`

	authoritativeRecordCountPresent bool
}

// UnmarshalJSON remembers which post-v1-extension aggregate fields were
// actually present. An older writer's absent authoritative count is unknown,
// not a declaration of zero; a present value is recomputed and enforced.
func (m *Manifest) UnmarshalJSON(data []byte) error {
	type manifestWire Manifest

	var decoded manifestWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	for _, name := range []string{
		"acquisitionPlanDigest",
		"authoritativeRecordCount",
		"recordOutcomeCounts",
		"actionEntryCount",
		"actionEntryCounts",
	} {
		if raw, present := fields[name]; present && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("manifest has null %s; unavailable legacy aggregates must be absent", name)
		}
	}

	*m = Manifest(decoded)
	_, m.authoritativeRecordCountPresent = fields["authoritativeRecordCount"]
	if _, present := fields["acquisitionPlanDigest"]; present {
		if err := validateSHA256Digest(m.AcquisitionPlanDigest); err != nil {
			return fmt.Errorf("manifest acquisitionPlanDigest: %w", err)
		}
	}

	return nil
}

func validateSHA256Digest(digest string) error {
	const prefix = "sha256:"
	if len(digest) != len(prefix)+64 || !strings.HasPrefix(digest, prefix) {
		return fmt.Errorf("must be sha256 followed by 64 lowercase hexadecimal characters")
	}
	for _, char := range digest[len(prefix):] {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return fmt.Errorf("must be sha256 followed by 64 lowercase hexadecimal characters")
		}
	}
	return nil
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

	// ErrActionMiss reports that a replay entered an action after exhausting the
	// recorded ordered action trace.
	ErrActionMiss = errors.New("tape: action entry not recorded")

	// ErrActionDesync reports that a replay entered a different action at the
	// current position in the ordered action trace.
	ErrActionDesync = errors.New("tape: action entry does not match the recording")

	// ErrUnconsumed reports that a replay stopped with recorded observations or
	// known action entries left over.
	ErrUnconsumed = errors.New("tape: recorded evidence was not fully consumed")
)

// ActionMissError describes a replay that entered more actions than Go
// recorded for the subject.
type ActionMissError struct {
	Subject  string
	Attempt  int
	Sequence int
	Name     string
}

func (e *ActionMissError) Error() string {
	return fmt.Sprintf(
		"tape: no action entry recorded at %s#%d[%d] (replay entered %q)",
		e.Subject,
		e.Attempt,
		e.Sequence,
		e.Name,
	)
}

func (*ActionMissError) Is(target error) bool { return target == ErrActionMiss }

// ActionDesyncError describes a replay whose next action differs from the
// recorded action at the same position.
type ActionDesyncError struct {
	Subject  string
	Attempt  int
	Sequence int
	WantName string
	GotName  string
}

func (e *ActionDesyncError) Error() string {
	return fmt.Sprintf(
		"tape: action desync at %s#%d[%d]: recorded %q, replay entered %q",
		e.Subject,
		e.Attempt,
		e.Sequence,
		e.WantName,
		e.GotName,
	)
}

func (*ActionDesyncError) Is(target error) bool { return target == ErrActionDesync }

// UnconsumedError describes evidence the replayed classification did not ask
// for or enter. ActionsKnown is false for legacy v1 tapes, whose absent
// actionEntries field is unknown rather than an empty trace.
type UnconsumedError struct {
	Subject               string
	Attempt               int
	RemainingObservations int
	RemainingActions      int
	ActionsKnown          bool
}

func (e *UnconsumedError) Error() string {
	if !e.ActionsKnown {
		return fmt.Sprintf(
			"tape: %s#%d left %d recorded observations unconsumed (legacy action trace unknown)",
			e.Subject,
			e.Attempt,
			e.RemainingObservations,
		)
	}

	return fmt.Sprintf(
		"tape: %s#%d left %d recorded observations and %d action entries unconsumed",
		e.Subject,
		e.Attempt,
		e.RemainingObservations,
		e.RemainingActions,
	)
}

func (*UnconsumedError) Is(target error) bool { return target == ErrUnconsumed }

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

func (*DesyncError) Is(target error) bool { return target == ErrDesync }

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

func (*MissError) Is(target error) bool { return target == ErrMiss }

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

	for i, action := range r.ActionEntries {
		if action.Name == "" {
			return fmt.Errorf("record %q action entry %d has an empty name", r.Subject, i)
		}
	}

	if r.ProcessorState != nil {
		if r.ProcessorState.ExistingContentIDs == nil {
			return fmt.Errorf(
				"record %q processorState has a null existingContentIds list; an empty list must encode as []",
				r.Subject,
			)
		}

		seen := make(map[string]struct{}, len(r.ProcessorState.ExistingContentIDs))
		for i, id := range r.ProcessorState.ExistingContentIDs {
			if id == "" {
				return fmt.Errorf("record %q processorState existingContentIds[%d] is empty", r.Subject, i)
			}
			if _, duplicate := seen[id]; duplicate {
				return fmt.Errorf("record %q processorState repeats existing content ID %q", r.Subject, id)
			}
			seen[id] = struct{}{}
		}
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
