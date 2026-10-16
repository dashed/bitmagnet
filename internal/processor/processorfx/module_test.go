package processorfx

import (
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/search/searchfx"
	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy"
	"github.com/stretchr/testify/assert"
)

func TestProvideSearchIndexerHonorsDualWriteConfig(t *testing.T) {
	t.Parallel()

	client := &tantivy.Client{}
	cfg := searchfx.NewDefaultConfig()

	assert.Same(t, client, provideSearchIndexer(client, cfg),
		"default config must preserve processor dual-write")

	cfg.DualWriteEnabled = false
	assert.Nil(t, provideSearchIndexer(client, cfg),
		"disabled dual-write must withhold the client from the processor")
}
