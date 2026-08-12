package tape

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// DefaultMaxRecords bounds a recording run so a long-lived process cannot grow
// without limit. Once the cap is reached no further classification opens a
// session, and the tape is marked truncated.
const DefaultMaxRecords = 5000

// Provenance is the human-facing description of a recording run, written to
// PROVENANCE.md next to the tape. None of it is load-bearing for replay --
// [Manifest.EffectiveConfigDigest] is what fails a replay closed -- but a tape
// whose origin nobody can reconstruct is not evidence.
type Provenance struct {
	// Command is what was run to produce the tape.
	Command string
	// Host is where it ran.
	Host string
	// Database identifies the content database the local searches were served
	// from, precisely enough to tell two snapshots apart.
	Database string
	// AcquisitionPlanDigest binds synthetic seed records to the exact reviewed
	// plan bytes that produced them. Empty means no acquisition plan was used.
	AcquisitionPlanDigest string
	// ScopeLimits states what a green replay against this tape does NOT prove.
	// It is written into PROVENANCE.md verbatim.
	//
	// An oracle that does not carry its own limits invites the reader to treat a
	// pass as broader evidence than it is, and the limits belong in the artifact
	// rather than in a report nobody re-reads.
	ScopeLimits string
	// Notes is free-form context.
	Notes string
}

// Recorder collects the observations of a recording run and writes them out as
// a tape. The zero value is not usable; construct one with [NewRecorder].
//
// A Recorder is safe for concurrent use: classifications run concurrently and
// each holds its own [Session], but they all append into the one Recorder.
type Recorder struct {
	digest     string
	provenance Provenance
	maxRecords int

	mu       sync.Mutex
	records  []*Record
	index    map[recordKey]*Record
	attempts map[string]int
	// open holds the sessions that have begun and not yet ended. A tape written
	// while they are in flight marks their records incomplete.
	open map[recordKey]struct{}
	// truncated is set when a classification was refused a session because the
	// record cap was reached. A truncated tape is not a complete oracle.
	truncated bool
	// err latches the first encoding failure. Rather than write a tape with a
	// hole in it, [Recorder.Write] refuses to write at all.
	err error
	// onFull runs once, when the record cap is first reached.
	onFull func()
	// revision changes whenever evidence or artifact-relevant recorder state
	// changes. A write remembers the revision it snapshotted so Progress can
	// distinguish a genuinely final artifact from one made stale without a
	// record-count change (for example, the first refused Begin setting
	// Truncated after an otherwise quiescent write).
	revision uint64

	// writeMu prevents the cap callback and lifecycle shutdown from interleaving
	// their three-file generations. The directory write is not atomic, so two
	// concurrent writers would otherwise manufacture a mixed artifact.
	writeMu   sync.Mutex
	lastWrite recorderWriteProgress
}

// Progress is a read-only, point-in-time view suitable for a future metrics or
// status exporter. Counts describe the in-memory recorder now; LastWrite
// describes the most recent attempted artifact generation.
type Progress struct {
	AcquisitionPlanDigest   string
	RegisteredRecords       int
	OpenSessions            int
	AuthoritativeRecords    int
	NonAuthoritativeRecords int
	ObservationCount        int
	ActionEntryCount        int
	ActionEntryCounts       map[string]int
	RecordOutcomeCounts     map[string]int
	Truncated               bool
	Error                   string
	LastWrite               WriteProgress
}

// WriteProgress makes a successful quiescent generation distinguishable from
// the cap snapshot that may still contain open sessions. Final is true only if
// the last successful write contained every currently registered record and no
// session was open in that generation or has opened since.
type WriteProgress struct {
	Attempt                 int
	GeneratedAt             time.Time
	RecordCount             int
	OpenSessions            int
	AuthoritativeRecords    int
	NonAuthoritativeRecords int
	Truncated               bool
	Succeeded               bool
	Final                   bool
	Error                   string
}

type recorderWriteProgress struct {
	WriteProgress
	revision uint64
}

type recordKey struct {
	subject string
	attempt int
}

// NewRecorder returns a Recorder that pins its tape to effectiveConfigDigest.
// maxRecords <= 0 selects [DefaultMaxRecords].
func NewRecorder(effectiveConfigDigest string, maxRecords int, provenance Provenance) *Recorder {
	if maxRecords <= 0 {
		maxRecords = DefaultMaxRecords
	}

	return &Recorder{
		digest:     effectiveConfigDigest,
		provenance: provenance,
		maxRecords: maxRecords,
		index:      make(map[recordKey]*Record),
		attempts:   make(map[string]int),
		open:       make(map[recordKey]struct{}),
	}
}

// Begin opens a recording session for one classification and returns a context
// carrying it.
//
// The record is registered immediately rather than on completion, so a
// classification that ends in an error still contributes its observations, and
// a classification that consults nothing at all still appears in the tape with
// an empty observation list. That distinction matters: "classified, observed
// nothing" and "never classified" are different facts, and only the first one
// is an answer.
//
// Begin returns ctx unchanged once the record cap is reached, which leaves
// every seam on the no-session path.
func (r *Recorder) Begin(
	ctx context.Context,
	subject string,
	workflow string,
	flags map[string]any,
	input any,
	processorState ...ProcessorState,
) context.Context {
	if r == nil || subject == "" {
		return ctx
	}

	// The cheap first check avoids encoding inputs after the cap has already
	// been reached. A second check under the registration lock below handles
	// concurrent Begin calls racing for the last slot.
	r.mu.Lock()
	if len(r.records) >= r.maxRecords {
		alreadyFull := r.truncated
		r.truncated = true
		if !alreadyFull {
			r.revision++
		}
		onFull := r.onFull
		r.mu.Unlock()

		if !alreadyFull && onFull != nil {
			onFull()
		}

		return ctx
	}
	r.mu.Unlock()

	var (
		encodedInput []byte
		encodeErr    error
		// New writers always declare processor state. The variadic argument is
		// retained only for source compatibility with existing direct recorder
		// callers; omitting it means a known empty pre-classification state, not
		// legacy unknown state.
		capturedState = &ProcessorState{ExistingContentIDs: []string{}}
	)
	if len(processorState) > 1 {
		r.fail(fmt.Errorf("tape: Begin received %d processor states, want at most one", len(processorState)))
		return ctx
	}
	if len(processorState) == 1 {
		state := processorState[0]
		state.ExistingContentIDs = append([]string{}, state.ExistingContentIDs...)
		capturedState = &state
	}
	if input != nil {
		encodedInput, encodeErr = Marshal(input)

		if encodeErr == nil && bytes.Equal(bytes.TrimSpace(encodedInput), []byte("null")) {
			encodeErr = errors.New("encoded as null")
		}
	}

	r.mu.Lock()

	if len(r.records) >= r.maxRecords {
		alreadyFull := r.truncated
		r.truncated = true
		if !alreadyFull {
			r.revision++
		}
		onFull := r.onFull
		r.mu.Unlock()

		if !alreadyFull && onFull != nil {
			onFull()
		}

		return ctx
	}

	if encodeErr != nil {
		r.err = errors.Join(
			r.err,
			fmt.Errorf("tape: encode classifier input for %q: %w", subject, encodeErr),
		)
		r.revision++
		r.mu.Unlock()
		return ctx
	}

	attempt := r.attempts[subject]
	r.attempts[subject] = attempt + 1

	record := &Record{
		Subject:        subject,
		Attempt:        attempt,
		Workflow:       workflow,
		Flags:          copyFlags(flags),
		Input:          encodedInput,
		ProcessorState: capturedState,
		// Never nil: an empty observation list must survive to disk as [].
		Observations: []Observation{},
	}
	key := recordKey{subject, attempt}
	r.records = append(r.records, record)
	r.index[key] = record
	r.open[key] = struct{}{}
	r.revision++
	r.mu.Unlock()

	return context.WithValue(ctx, contextKey{}, &Session{
		subject:  subject,
		attempt:  attempt,
		recorder: r,
	})
}

func copyFlags(flags map[string]any) map[string]any {
	copied := make(map[string]any, len(flags))
	for k, v := range flags {
		copied[k] = v
	}

	return copied
}

func (r *Recorder) appendObservation(subject string, attempt int, observation Observation) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.index[recordKey{subject, attempt}]
	if !ok {
		r.err = errors.Join(r.err, errors.New("tape: observation for an unregistered session"))
		r.revision++
		return
	}

	record.Observations = append(record.Observations, observation)
	r.revision++
}

func (r *Recorder) appendActionEntry(subject string, attempt int, action ActionEntry) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.index[recordKey{subject, attempt}]
	if !ok {
		r.err = errors.Join(r.err, errors.New("tape: action entry for an unregistered session"))
		r.revision++
		return
	}

	if action.Name == "" {
		r.err = errors.Join(r.err, errors.New("tape: action entry has an empty name"))
		r.revision++
		return
	}

	record.ActionEntries = append(record.ActionEntries, action)
	r.revision++
}

// endSession closes a session and stamps how the classification ended.
//
// Recording the outcome is what makes an empty observation list readable: see
// [RecordOutcome]. The stamp lands on the record itself rather than being derived at
// snapshot time, because by then the classification is gone and only the record
// remains.
func (r *Recorder) endSession(subject string, attempt int, outcome RecordOutcome) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := recordKey{subject, attempt}
	_, wasOpen := r.open[key]
	delete(r.open, key)
	changed := wasOpen

	// A session whose record was never registered -- the cap was already
	// reached at Begin -- has nothing to stamp.
	if record, ok := r.index[key]; ok {
		// First ending wins: a double End must not overwrite the real outcome
		// with whatever the second caller happened to pass.
		if record.Outcome == nil {
			stamped := outcome
			record.Outcome = &stamped
			changed = true
		}
	}
	if changed {
		r.revision++
	}
}

func (r *Recorder) fail(err error) {
	if r == nil || err == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.err = errors.Join(r.err, err)
	r.revision++
}

// Records returns the recorded classifications in tape order: sorted by subject
// then attempt, with each record's observations in the order they were made.
//
// Sorting at this point is what makes the tape deterministic under concurrency.
// Classifications interleave, so append order is a race; (subject, attempt) is
// not, and the sequence within a record is fixed by the single classification
// that produced it.
func (r *Recorder) Records() ([]Record, error) {
	records, _, _, err := r.snapshotRecords()
	return records, err
}

// snapshotRecords returns the records and artifact-relevant recorder state from
// one lock acquisition. Write uses it so a racing refused Begin cannot make the
// tape lines and manifest.Truncated describe different instants.
func (r *Recorder) snapshotRecords() ([]Record, bool, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return nil, r.truncated, r.revision, r.err
	}

	records := make([]Record, 0, len(r.records))

	for _, record := range r.records {
		copied := cloneRecord(*record)
		// Snapshotting a run that is still going catches whatever
		// classifications happen to be mid-flight; their observation lists are
		// prefixes, and saying so is the difference between a short answer and
		// a known gap.
		_, copied.Incomplete = r.open[recordKey{copied.Subject, copied.Attempt}]
		records = append(records, copied)
	}

	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Subject != records[j].Subject {
			return records[i].Subject < records[j].Subject
		}

		return records[i].Attempt < records[j].Attempt
	})

	for _, record := range records {
		if err := record.validate(); err != nil {
			return nil, r.truncated, r.revision, err
		}
	}

	return records, r.truncated, r.revision, nil
}

// OnFull registers a callback to run the first time Begin refuses a record
// after the cap has been filled.
//
// The Nth Begin fills a cap of N. The next Begin proves the live stream would
// have continued, marks the tape truncated, and runs this callback. Waiting for
// a clean shutdown to write would throw the whole run away if the process is
// killed instead. The callback runs on that classifying goroutine, outside the
// Recorder's lock.
func (r *Recorder) OnFull(callback func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.onFull = callback
}

// Truncated reports whether the record cap cut the recording short.
func (r *Recorder) Truncated() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.truncated
}

// Progress returns an internally consistent snapshot without exposing mutable
// recorder maps or records. It is intentionally transport-neutral: the
// classifier package can export it through metrics or status later without the
// tape package depending on an observability stack.
func (r *Recorder) Progress() Progress {
	if r == nil {
		return Progress{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	progress := Progress{
		AcquisitionPlanDigest: r.provenance.AcquisitionPlanDigest,
		RegisteredRecords:     len(r.records),
		OpenSessions:          len(r.open),
		ActionEntryCounts:     make(map[string]int),
		RecordOutcomeCounts:   make(map[string]int),
		Truncated:             r.truncated,
		LastWrite:             r.lastWrite.WriteProgress,
	}

	for _, record := range r.records {
		progress.ObservationCount += len(record.Observations)
		for _, action := range record.ActionEntries {
			progress.ActionEntryCount++
			progress.ActionEntryCounts[action.Name]++
		}

		key := recordKey{record.Subject, record.Attempt}
		if _, open := r.open[key]; open {
			progress.RecordOutcomeCounts["unknown"]++
			continue
		}

		outcome := "unknown"
		if record.Outcome != nil {
			outcome = string(record.Outcome.Kind)
		}
		progress.RecordOutcomeCounts[outcome]++

		if record.Authoritative() {
			progress.AuthoritativeRecords++
		} else {
			progress.NonAuthoritativeRecords++
		}
	}

	if r.err != nil {
		progress.Error = r.err.Error()
	}

	progress.LastWrite.Final = progress.LastWrite.Succeeded &&
		progress.LastWrite.Error == "" &&
		progress.Error == "" &&
		progress.LastWrite.OpenSessions == 0 &&
		progress.OpenSessions == 0 &&
		progress.LastWrite.RecordCount == progress.RegisteredRecords &&
		r.lastWrite.revision == r.revision

	progress.ActionEntryCounts = copyCountMap(progress.ActionEntryCounts)
	progress.RecordOutcomeCounts = copyCountMap(progress.RecordOutcomeCounts)

	return progress
}

// MaxRecords returns the configured record cap. Acquisition-plan validation
// uses it to guarantee that deterministic seed records leave room for organic
// traffic before any classification runs.
func (r *Recorder) MaxRecords() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxRecords
}

func (r *Recorder) finishWrite(
	generatedAt time.Time,
	summary recordSummary,
	truncated bool,
	revision uint64,
	writeErr error,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	attempt := r.lastWrite.Attempt + 1
	nonAuthoritative := summary.recordCount -
		summary.authoritativeRecordCount -
		summary.incompleteRecordCount
	if nonAuthoritative < 0 {
		nonAuthoritative = 0
	}

	r.lastWrite.WriteProgress = WriteProgress{
		Attempt:                 attempt,
		GeneratedAt:             generatedAt.UTC(),
		RecordCount:             summary.recordCount,
		OpenSessions:            summary.incompleteRecordCount,
		AuthoritativeRecords:    summary.authoritativeRecordCount,
		NonAuthoritativeRecords: nonAuthoritative,
		Truncated:               truncated,
		Succeeded:               writeErr == nil,
	}
	r.lastWrite.revision = revision

	if writeErr != nil {
		r.lastWrite.Error = writeErr.Error()
	}
}
