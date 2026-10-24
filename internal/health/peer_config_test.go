package health

import (
	"strings"
	"testing"

	"github.com/iancoleman/strcase"
	"github.com/stretchr/testify/assert"
)

func TestPeerConfigEnvNames(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		"HEALTH_PEER_GRAPHQL_URLS",
		"HEALTH_"+strings.ToUpper(strcase.ToSnake("PeerGraphqlUrls")),
	)
	assert.Equal(
		t,
		"HEALTH_PEER_TIMEOUT",
		"HEALTH_"+strings.ToUpper(strcase.ToSnake("PeerTimeout")),
	)
}
