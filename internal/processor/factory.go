package processor

import (
	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Params struct {
	fx.In
	ClassifierConfig classifier.Config
	Search           lazy.Lazy[search.Search]
	Workflow         lazy.Lazy[classifier.Runner]
	Dao              lazy.Lazy[*dao.Query]
	BlockingManager  lazy.Lazy[blocking.Manager]
	Logger           *zap.SugaredLogger
	// Tantivy is the search sidecar client for the dual-write; nil when the
	// "search" feature is disabled (provided by searchfx).
	Tantivy *tantivy.Client `optional:"true"`
}

type Result struct {
	fx.Out
	Processor lazy.Lazy[Processor]
}

func New(p Params) Result {
	return Result{
		Processor: lazy.New(func() (Processor, error) {
			s, err := p.Search.Get()
			if err != nil {
				return nil, err
			}
			d, err := p.Dao.Get()
			if err != nil {
				return nil, err
			}
			bm, err := p.BlockingManager.Get()
			if err != nil {
				return nil, err
			}
			w, err := p.Workflow.Get()
			if err != nil {
				return nil, err
			}

			return processor{
				dao:             d,
				search:          s,
				blockingManager: bm,
				runner:          w,
				defaultWorkflow: p.ClassifierConfig.Workflow,
				tantivy:         p.Tantivy,
				logger:          p.Logger,
			}, nil
		}),
	}
}
