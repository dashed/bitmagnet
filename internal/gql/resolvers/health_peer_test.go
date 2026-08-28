package resolvers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
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
	peerClient := newHealthPeerClient(t, healthPeerResponse{
		Data: healthPeerDataResponse{
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
			Workers: healthPeerWorkerPayload{
				ListAll: gen.WorkersListAllQueryResult{
					Workers: []gen.Worker{
						{Key: "dht_crawler", Started: true},
						{Key: "queue_server", Started: true},
					},
				},
			},
		},
	})

	resolver := Resolver{
		HealthPeerConfig: health.PeerConfig{
			PeerGraphqlUrls: []string{"http://peer.test/graphql"},
			PeerTimeout:     time.Second,
		},
		healthPeerHTTPClient: peerClient,
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

	peerClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Body:       io.NopCloser(bytes.NewBufferString("not ready")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	resolver := Resolver{
		HealthPeerConfig: health.PeerConfig{
			PeerGraphqlUrls: []string{"http://peer.test/graphql"},
			PeerTimeout:     time.Second,
		},
		healthPeerHTTPClient: peerClient,
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newHealthPeerClient(t *testing.T, response healthPeerResponse) *http.Client {
	t.Helper()

	return &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPost, req.Method)
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))

			body, err := json.Marshal(response)
			if err != nil {
				return nil, err
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader(body)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}
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
