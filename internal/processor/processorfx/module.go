package processorfx

import (
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	batchqueue "github.com/bitmagnet-io/bitmagnet/internal/processor/batch/queue"
	processorqueue "github.com/bitmagnet-io/bitmagnet/internal/processor/queue"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"go.uber.org/fx"
)

func New() fx.Option {
	return fx.Module(
		"processor",
		fx.Provide(
			processor.New,
			processorqueue.New,
			batchqueue.New,
			provideSearchIndexer,
		),
	)
}

// provideSearchIndexer adapts the (optional) Tantivy client into the processor's
// SearchIndexer. A nil client (search feature disabled) yields a nil interface —
// returned explicitly so it is a true nil (not a typed-nil that would defeat the
// processor's nil check).
func provideSearchIndexer(client *tantivy.Client) processor.SearchIndexer {
	if client == nil {
		return nil
	}

	return client
}
