package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor/batch"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/handler"
	"go.uber.org/fx"
	"gorm.io/gen"
	"gorm.io/gen/field"
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
					var scopes []func(gen.Dao) gen.Dao
					if len(msg.ContentTypes) > 0 {
						scopes = append(scopes, contentTypesScope(d, msg.ContentTypes))
					}
					if msg.Orphans {
						scopes = append(scopes, func(tx gen.Dao) gen.Dao {
							return tx.Not(
								gen.Exists(
									d.TorrentContent.Where(
										d.TorrentContent.InfoHash.EqCol(
											d.Torrent.InfoHash,
										),
									),
								),
							)
						})
					}
					planner := batch.NewPlanner(*msg)
					var queueJobs []*model.QueueJob
					for planner.ShouldQuery() {
						torrents, findErr := d.Torrent.WithContext(ctx).
							Scopes(scopes...).
							Where(
								d.Torrent.InfoHash.Gt(planner.MaxInfoHash()),
								d.Torrent.UpdatedAt.Lt(msg.UpdatedBefore),
							).
							Select(d.Torrent.InfoHash).
							Order(d.Torrent.InfoHash).
							Limit(int(msg.BatchSize)).
							Find()
						if findErr != nil {
							return findErr
						}
						var infoHashes []protocol.ID
						for _, t := range torrents {
							infoHashes = append(infoHashes, t.InfoHash)
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

func contentTypesScope(
	d *dao.Query,
	contentTypeFilters []model.NullContentType,
) func(gen.Dao) gen.Dao {
	var contentTypes []string
	var unknownContentType bool
	for _, contentType := range contentTypeFilters {
		if !contentType.Valid {
			unknownContentType = true
		} else {
			contentTypes = append(contentTypes, contentType.ContentType.String())
		}
	}
	return func(tx gen.Dao) gen.Dao {
		var contentTypeCondition field.Expr
		switch {
		case len(contentTypes) > 0 && unknownContentType:
			contentTypeCondition = field.Or(
				d.TorrentContent.ContentType.In(contentTypes...),
				d.TorrentContent.ContentType.IsNull(),
			)
		case len(contentTypes) > 0:
			contentTypeCondition = d.TorrentContent.ContentType.In(contentTypes...)
		default:
			contentTypeCondition = d.TorrentContent.ContentType.IsNull()
		}
		sq := d.TorrentContent.Where(
			d.TorrentContent.InfoHash.EqCol(d.Torrent.InfoHash),
			contentTypeCondition,
		)
		return tx.Where(gen.Exists(sq))
	}
}
