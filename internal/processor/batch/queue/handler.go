package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor/batch"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/handler"
	"go.uber.org/fx"
)

type Params struct {
	fx.In
	Dao lazy.Lazy[*dao.Query]
}

type Result struct {
	fx.Out
	Handler lazy.Lazy[handler.Handler] `group:"queue_handlers"`
}

func New(p Params) Result {
	return Result{
		Handler: lazy.New(func() (handler.Handler, error) {
			d, err := p.Dao.Get()
			if err != nil {
				return handler.Handler{}, err
			}
			return handler.New(
				batch.MessageName,
				func(ctx context.Context, job model.QueueJob) (err error) {
					msg := &batch.MessageParams{}
					if err := json.Unmarshal([]byte(job.Payload), msg); err != nil {
						return err
					}
					planner := batch.NewPlanner(*msg)
					selector := NewPostgresSelector(d)
					var queueJobs []*model.QueueJob
					for planner.ShouldQuery() {
						selection := planner.Selection()
						infoHashes, findErr := selector.Select(ctx, selection)
						if findErr != nil {
							return findErr
						}
						spec, planErr := planner.AddPage(infoHashes)
						if planErr != nil {
							return planErr
						}
						if spec != nil {
							queueJob, jobErr := spec.QueueJob()
							if jobErr != nil {
								return jobErr
							}
							queueJobs = append(queueJobs, &queueJob)
						}
					}
					plan, planErr := planner.Finalize()
					if planErr != nil {
						return planErr
					}
					for _, spec := range plan.Jobs[len(queueJobs):] {
						job, jobErr := spec.QueueJob()
						if jobErr != nil {
							return jobErr
						}
						queueJobs = append(queueJobs, &job)
					}
					if len(queueJobs) > 0 {
						if createErr := d.QueueJob.
							WithContext(ctx).
							Create(queueJobs...); createErr != nil {
							return createErr
						}
					}
					return nil
				},
				handler.JobTimeout(time.Second*60*10),
				handler.Concurrency(1),
			), nil
		}),
	}
}
