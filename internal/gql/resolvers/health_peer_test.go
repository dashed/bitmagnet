package resolvers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergePeerHealthPrefersActivePeerOverInactiveLocal(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	peer := newHealthPeerServer(t, healthPeerResponse{
		Data: struct {
			Health  gen.HealthQuery "json:\"health\""
			Workers struct {
				ListAll gen.WorkersListAllQueryResult "json:\"listAll\""
			} "json:\"workers\""
		}{
			Health: gen.HealthQuery{
				Status: gen.HealthStatusUp,
				Checks: []gen.HealthCheck{
					{
						Key:       "dht",
						Status:    gen.HealthStatusUp,
						Timestamp: now,
					},
					{
						Key:       "postgres",
						Status:    gen.HealthStatusUp,
						Timestamp: now,
					},
				},
			},
			Workers: struct {
				ListAll gen.WorkersListAllQueryResult "json:\"listAll\""
			}{
				ListAll: gen.WorkersListAllQueryResult{
					Workers: []gen.Worker{
						{Key: "dht_crawler", Started: true},
						{Key: "queue_server", Started: true},
					},
				},
			},
		},
	})
	defer peer.Close()

	resolver := Resolver{
		HealthPeerConfig: health.PeerConfig{
			PeerGraphqlURLs: []string{peer.URL},
			PeerTimeout:     time.Second,
		},
	}
	localHealth := gen.HealthQuery{
		Status: gen.HealthStatusUp,
		Checks: []gen.HealthCheck{
			{
				Key:       "dht",
				Status:    gen.HealthStatusInactive,
				Timestamp: now.Add(time.Second),
			},
			{
				Key:       "postgres",
				Status:    gen.HealthStatusUp,
				Timestamp: now.Add(time.Second),
			},
		},
	}

	mergedHealth := resolver.mergePeerHealth(context.Background(), localHealth)
	require.Equal(t, gen.HealthStatusUp, mergedHealth.Status)
	assert.Equal(t, gen.HealthStatusUp, healthCheckByKey(t, mergedHealth.Checks, "dht").Status)
	assert.Nil(t, healthCheckByKey(t, mergedHealth.Checks, "status_peer"))

	localWorkers := []gen.Worker{
		{Key: "dht_crawler", Started: false},
		{Key: "http_server", Started: true},
		{Key: "queue_server", Started: false},
	}
	mergedWorkers := resolver.mergePeerWorkers(context.Background(), localWorkers)

	assert.True(t, workerByKey(t, mergedWorkers, "dht_crawler").Started)
	assert.True(t, workerByKey(t, mergedWorkers, "http_server").Started)
	assert.True(t, workerByKey(t, mergedWorkers, "queue_server").Started)
}

func TestMergePeerHealthReportsPeerFailure(t *testing.T) {
	t.Parallel()

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer peer.Close()

	resolver := Resolver{
		HealthPeerConfig: health.PeerConfig{
			PeerGraphqlURLs: []string{peer.URL},
			PeerTimeout:     time.Second,
		},
	}

	merged := resolver.mergePeerHealth(context.Background(), gen.HealthQuery{
		Status: gen.HealthStatusUp,
		Checks: []gen.HealthCheck{
			{
				Key:       "postgres",
				Status:    gen.HealthStatusUp,
				Timestamp: time.Now().UTC(),
			},
		},
	})

	require.Equal(t, gen.HealthStatusDown, merged.Status)
	peerCheck := healthCheckByKey(t, merged.Checks, healthPeerCheckKey)
	assert.Equal(t, gen.HealthStatusDown, peerCheck.Status)
	require.NotNil(t, peerCheck.Error)
	assert.Contains(t, *peerCheck.Error, "503")
}

func newHealthPeerServer(t *testing.T, response healthPeerResponse) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		w.Header().Set("content-type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(response))
	}))
}

func healthCheckByKey(t *testing.T, checks []gen.HealthCheck, key string) *gen.HealthCheck {
	t.Helper()

	for i := range checks {
		if checks[i].Key == key {
			return &checks[i]
		}
	}

	return nil
}

func workerByKey(t *testing.T, workers []gen.Worker, key string) gen.Worker {
	t.Helper()

	for _, worker := range workers {
		if worker.Key == key {
			return worker
		}
	}

	t.Fatalf("worker %q not found", key)
	return gen.Worker{}
}
