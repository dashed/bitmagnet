package tape

import (
	"context"
	"errors"
	"sort"
	"sync"
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
) context.Context {
	if r == nil || subject == "" {
		return ctx
	}

	r.mu.Lock()

	if len(r.records) >= r.maxRecords {
		alreadyFull := r.truncated
		r.truncated = true
		onFull := r.onFull
		r.mu.Unlock()

		if !alreadyFull && onFull != nil {
			onFull()
		}

		return ctx
	}

	attempt := r.attempts[subject]
	r.attempts[subject] = attempt + 1

	record := &Record{
		Subject:  subject,
		Attempt:  attempt,
		Workflow: workflow,
		Flags:    copyFlags(flags),
		// Never nil: an empty observation list must survive to disk as [].
		Observations: []Observation{},
	}
	key := recordKey{subject, attempt}
	r.records = append(r.records, record)
	r.index[key] = record
	r.open[key] = struct{}{}
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
		return
	}

	record.Observations = append(record.Observations, observation)
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
	delete(r.open, key)

	// A session whose record was never registered -- the cap was already
	// reached at Begin -- has nothing to stamp.
	if record, ok := r.index[key]; ok {
		// First ending wins: a double End must not overwrite the real outcome
		// with whatever the second caller happened to pass.
		if record.Outcome == nil {
			stamped := outcome
			record.Outcome = &stamped
		}
	}
}

func (r *Recorder) fail(err error) {
	if r == nil || err == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.err = errors.Join(r.err, err)
}

// Records returns the recorded classifications in tape order: sorted by subject
// then attempt, with each record's observations in the order they were made.
//
// Sorting at this point is what makes the tape deterministic under concurrency.
// Classifications interleave, so append order is a race; (subject, attempt) is
// not, and the sequence within a record is fixed by the single classification
// that produced it.
func (r *Recorder) Records() ([]Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return nil, r.err
	}

	records := make([]Record, 0, len(r.records))

	for _, record := range r.records {
		copied := *record
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
			return nil, err
		}
	}

	return records, nil
}

// OnFull registers a callback to run the first time the record cap is reached.
//
// A bounded recording run is done at that moment, and waiting for a clean
// shutdown to write the tape would throw the whole run away if the process is
// killed instead. The callback runs on the classifying goroutine, outside the
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
