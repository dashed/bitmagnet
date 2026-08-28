package resolvers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
)

const (
	healthPeerCheckKey = "status_peer"
	healthPeerQuery    = `query HealthPeer {
  health {
    status
    checks {
      key
      status
      timestamp
      error
    }
  }
  workers {
    listAll {
      workers {
        key
        started
      }
    }
  }
}`
)

type healthPeerSnapshot struct {
	Health  gen.HealthQuery
	Workers []gen.Worker
}

type healthPeerResponse struct {
	Data   healthPeerDataResponse    `json:"data"`
	Errors []healthPeerErrorResponse `json:"errors,omitempty"`
}

type healthPeerDataResponse struct {
	Health  gen.HealthQuery         `json:"health"`
	Workers healthPeerWorkerPayload `json:"workers"`
}

type healthPeerWorkerPayload struct {
	ListAll gen.WorkersListAllQueryResult `json:"listAll"`
}

type healthPeerErrorResponse struct {
	Message string `json:"message"`
}

func (r *Resolver) mergePeerHealth(ctx context.Context, local gen.HealthQuery) gen.HealthQuery {
	snapshots, errs := r.fetchHealthPeerSnapshots(ctx)

	checks := local.Checks
	for _, snapshot := range snapshots {
		checks = mergeHealthChecks(checks, snapshot.Health.Checks)
	}

	if len(errs) > 0 {
		checks = mergeHealthChecks(checks, []gen.HealthCheck{healthPeerErrorCheck(errs)})
	}

	return gen.HealthQuery{
		Status: aggregateHealthStatus(checks),
		Checks: checks,
	}
}

func (r *Resolver) mergePeerWorkers(ctx context.Context, local []gen.Worker) []gen.Worker {
	snapshots, _ := r.fetchHealthPeerSnapshots(ctx)
	if len(snapshots) == 0 {
		return sortWorkers(local)
	}

	byKey := make(map[string]gen.Worker, len(local))
	for _, worker := range local {
		byKey[worker.Key] = worker
	}

	for _, snapshot := range snapshots {
		for _, worker := range snapshot.Workers {
			existing, ok := byKey[worker.Key]
			if !ok || (!existing.Started && worker.Started) {
				byKey[worker.Key] = worker
			}
		}
	}

	workers := make([]gen.Worker, 0, len(byKey))
	for _, worker := range byKey {
		workers = append(workers, worker)
	}

	return sortWorkers(workers)
}

func (r *Resolver) fetchHealthPeerSnapshots(ctx context.Context) ([]healthPeerSnapshot, []error) {
	if r == nil || len(r.HealthPeerConfig.PeerGraphqlUrls) == 0 {
		return nil, nil
	}

	timeout := r.HealthPeerConfig.PeerTimeout
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}

	client := r.healthPeerClient(timeout)
	snapshots := make([]healthPeerSnapshot, 0, len(r.HealthPeerConfig.PeerGraphqlUrls))
	errs := make([]error, 0)

	for _, rawURL := range r.HealthPeerConfig.PeerGraphqlUrls {
		peerURL := strings.TrimSpace(rawURL)
		if peerURL == "" {
			continue
		}

		snapshot, err := fetchHealthPeerSnapshot(ctx, client, peerURL)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", peerURL, err))
			continue
		}

		snapshots = append(snapshots, snapshot)
	}

	return snapshots, errs
}

func (r *Resolver) healthPeerClient(timeout time.Duration) *http.Client {
	if r.healthPeerHTTPClient != nil {
		return r.healthPeerHTTPClient
	}

	return &http.Client{Timeout: timeout}
}

func fetchHealthPeerSnapshot(
	ctx context.Context,
	client *http.Client,
	peerURL string,
) (healthPeerSnapshot, error) {
	body, err := json.Marshal(map[string]string{"query": healthPeerQuery})
	if err != nil {
		return healthPeerSnapshot{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peerURL, bytes.NewReader(body))
	if err != nil {
		return healthPeerSnapshot{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return healthPeerSnapshot{}, err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return healthPeerSnapshot{}, fmt.Errorf("unexpected status %s", resp.Status)
	}

	var decoded healthPeerResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return healthPeerSnapshot{}, err
	}

	if len(decoded.Errors) > 0 {
		messages := make([]string, 0, len(decoded.Errors))
		for _, gqlErr := range decoded.Errors {
			messages = append(messages, gqlErr.Message)
		}

		return healthPeerSnapshot{}, fmt.Errorf("graphql errors: %s", strings.Join(messages, "; "))
	}

	return healthPeerSnapshot{
		Health:  decoded.Data.Health,
		Workers: decoded.Data.Workers.ListAll.Workers,
	}, nil
}

func mergeHealthChecks(local []gen.HealthCheck, peer []gen.HealthCheck) []gen.HealthCheck {
	byKey := make(map[string]gen.HealthCheck, len(local)+len(peer))
	for _, check := range local {
		byKey[check.Key] = check
	}

	for _, check := range peer {
		existing, ok := byKey[check.Key]
		if !ok || shouldReplaceHealthCheck(existing, check) {
			byKey[check.Key] = check
		}
	}

	checks := make([]gen.HealthCheck, 0, len(byKey))
	for _, check := range byKey {
		checks = append(checks, check)
	}

	sort.Slice(checks, func(i, j int) bool {
		return checks[i].Key < checks[j].Key
	})

	return checks
}

func shouldReplaceHealthCheck(existing gen.HealthCheck, candidate gen.HealthCheck) bool {
	existingSeverity := healthStatusSeverity(existing.Status)
	candidateSeverity := healthStatusSeverity(candidate.Status)

	if existing.Status == gen.HealthStatusInactive && candidate.Status != gen.HealthStatusInactive {
		return true
	}

	if candidateSeverity != existingSeverity {
		return candidateSeverity > existingSeverity
	}

	return candidate.Timestamp.After(existing.Timestamp)
}

func aggregateHealthStatus(checks []gen.HealthCheck) gen.HealthStatus {
	if len(checks) == 0 {
		return gen.HealthStatusUnknown
	}

	status := gen.HealthStatusUp

	for _, check := range checks {
		switch check.Status {
		case gen.HealthStatusDown:
			return gen.HealthStatusDown
		case gen.HealthStatusUnknown:
			status = gen.HealthStatusUnknown
		}
	}

	return status
}

func healthStatusSeverity(status gen.HealthStatus) int {
	switch status {
	case gen.HealthStatusDown:
		return 3
	case gen.HealthStatusUnknown:
		return 2
	case gen.HealthStatusUp:
		return 1
	case gen.HealthStatusInactive:
		return 0
	default:
		return 2
	}
}

func healthPeerErrorCheck(errs []error) gen.HealthCheck {
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}

	message := strings.Join(messages, "; ")

	return gen.HealthCheck{
		Key:       healthPeerCheckKey,
		Status:    gen.HealthStatusDown,
		Timestamp: time.Now(),
		Error:     &message,
	}
}

func sortWorkers(workers []gen.Worker) []gen.Worker {
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].Key < workers[j].Key
	})

	return workers
}
