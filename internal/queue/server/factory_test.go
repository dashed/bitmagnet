package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/worker"
	"go.uber.org/zap"
)

func TestWorkerStartupGetsQueryBeforeRealizingEnabledHandlers(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("query unavailable")
	queryGets := 0
	handlerGets := make(map[string]int)
	result := New(Params{
		Config: NewDefaultConfig(),
		Query: lazy.New(func() (*dao.Query, error) {
			queryGets++

			return nil, wantErr
		}),
		Handlers: []RegisteredHandler{testRegistration("enabled", handlerGets)},
		Logger:   zap.NewNop().Sugar(),
	})

	err := startWorker(t, result.Worker)
	if !errors.Is(err, wantErr) {
		t.Fatalf("worker start error = %v, want wrapping %v", err, wantErr)
	}
	if queryGets != 1 {
		t.Fatalf("query Get calls = %d, want 1", queryGets)
	}
	if len(handlerGets) != 0 {
		t.Fatalf("handler factories were realized before query Get failed: %v", handlerGets)
	}
}

func TestWorkerStartupValidatesOwnershipBeforeGettingQuery(t *testing.T) {
	t.Parallel()

	queryGets := 0
	handlerGets := make(map[string]int)
	result := New(Params{
		Config: Config{DisabledQueues: []string{"unknown"}},
		Query: lazy.New(func() (*dao.Query, error) {
			queryGets++

			return nil, errors.New("query unexpectedly requested before ownership validation")
		}),
		Handlers: []RegisteredHandler{testRegistration("enabled", handlerGets)},
		Logger:   zap.NewNop().Sugar(),
	})

	err := startWorker(t, result.Worker)
	if err == nil || !strings.Contains(err.Error(), "unknown queue") {
		t.Fatalf("worker start error = %v, want unknown queue", err)
	}
	if queryGets != 0 {
		t.Fatalf("query Get calls = %d, want 0", queryGets)
	}
	if len(handlerGets) != 0 {
		t.Fatalf("handler factories were realized before validation failed: %v", handlerGets)
	}
}

func startWorker(t *testing.T, w worker.Worker) error {
	t.Helper()

	result, err := worker.NewRegistry(worker.RegistryParams{
		Workers: []worker.Worker{w},
		Logger:  zap.NewNop().Sugar(),
	})
	if err != nil {
		t.Fatalf("create worker registry: %v", err)
	}
	if err := result.Registry.Enable("queue_server"); err != nil {
		t.Fatalf("enable queue server worker: %v", err)
	}

	return result.Registry.Start(context.Background())
}
