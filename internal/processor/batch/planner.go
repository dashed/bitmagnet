package batch

import (
	"bytes"
	"errors"

	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

var errPlannerAlreadyFinalized = errors.New("batch planner is already finalized")

var errInvalidPlannerPage = errors.New("batch planner page violates the ordered keyset contract")

// JobSpec is one logical queue job. It deliberately excludes RunAfter, which
// model.NewQueueJob derives from time.Now when the handler materializes it.
type JobSpec struct {
	ProcessTorrent *processor.MessageParams
	Continuation   *MessageParams
	Priority       int
}

func (s JobSpec) QueueJob() (model.QueueJob, error) {
	switch {
	case s.ProcessTorrent != nil && s.Continuation == nil:
		return processor.NewQueueJob(*s.ProcessTorrent, model.QueueJobPriority(s.Priority))
	case s.ProcessTorrent == nil && s.Continuation != nil:
		return NewQueueJob(*s.Continuation)
	default:
		return model.QueueJob{}, errors.New("batch job spec must select exactly one job kind")
	}
}

// Plan is the deterministic logical job image produced by one batch-handler run.
// Database selection supplies ordered pages; Planner owns grouping, priorities,
// the chunk boundary, and the continuation job.
type Plan struct {
	Jobs        []JobSpec
	MaxInfoHash protocol.ID
	ChunkSize   uint
	Done        bool
}

// Planner incrementally reproduces the process_torrent_batch handler's pure
// queue-planning boundary.
type Planner struct {
	message   MessageParams
	jobs      []JobSpec
	max       protocol.ID
	chunkSize uint
	done      bool
	finalized bool
}

func NewPlanner(message MessageParams) *Planner {
	return &Planner{message: message, max: message.InfoHashGreaterThan}
}

func (p *Planner) MaxInfoHash() protocol.ID {
	return p.max
}

func (p *Planner) ShouldQuery() bool {
	return !p.done && (p.chunkSize == 0 || p.chunkSize < p.message.ChunkSize)
}

// AddPage accepts the next ordered page returned by the database query. A
// non-nil result is the child job that the handler must materialize immediately
// so its RunAfter timestamp retains the original handler's timing.
func (p *Planner) AddPage(infoHashes []protocol.ID) (*JobSpec, error) {
	if p.finalized {
		return nil, errPlannerAlreadyFinalized
	}
	if len(infoHashes) == 0 {
		p.done = true
		return nil, nil
	}
	if p.message.BatchSize > 0 && len(infoHashes) > int(p.message.BatchSize) {
		return nil, errInvalidPlannerPage
	}
	previous := p.max
	for _, infoHash := range infoHashes {
		if bytes.Compare(infoHash[:], previous[:]) <= 0 {
			return nil, errInvalidPlannerPage
		}
		previous = infoHash
	}

	priority := 10
	if p.message.ApisDisabled() {
		priority = 4
	}
	message := processor.MessageParams{
		ClassifyMode:       p.message.ClassifyMode,
		ClassifierWorkflow: p.message.ClassifierWorkflow,
		ClassifierFlags:    p.message.ClassifierFlags,
		InfoHashes:         append([]protocol.ID(nil), infoHashes...),
	}
	spec := JobSpec{ProcessTorrent: &message, Priority: priority}
	p.jobs = append(p.jobs, spec)
	p.max = infoHashes[len(infoHashes)-1]
	p.chunkSize += uint(len(infoHashes))
	if len(infoHashes) < int(p.message.BatchSize) {
		p.done = true
	}
	return &spec, nil
}

// Finalize appends the keyset-continuation job when the chunk boundary stopped
// the handler before the source query was exhausted.
func (p *Planner) Finalize() (Plan, error) {
	if p.finalized {
		return Plan{}, errPlannerAlreadyFinalized
	}
	p.finalized = true
	if !p.done {
		continuation := MessageParams{
			InfoHashGreaterThan: p.max,
			UpdatedBefore:       p.message.UpdatedBefore,
			ClassifyMode:        p.message.ClassifyMode,
			ClassifierWorkflow:  p.message.ClassifierWorkflow,
			ClassifierFlags:     p.message.ClassifierFlags,
			ChunkSize:           p.message.ChunkSize,
			BatchSize:           p.message.BatchSize,
			ContentTypes:        p.message.ContentTypes,
			Orphans:             p.message.Orphans,
		}
		p.jobs = append(p.jobs, JobSpec{Continuation: &continuation})
	}
	return Plan{
		Jobs:        append([]JobSpec(nil), p.jobs...),
		MaxInfoHash: p.max,
		ChunkSize:   p.chunkSize,
		Done:        p.done,
	}, nil
}
