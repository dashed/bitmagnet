package server

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/handler"
	"go.uber.org/zap"
)

func TestResolveHandlersPreservesOrderAndDoesNotRealizeDisabled(t *testing.T) {
	t.Parallel()

	getCalls := make(map[string]int)
	registrations := []RegisteredHandler{
		testRegistration("first", getCalls),
		testRegistration("disabled", getCalls),
		testRegistration("last", getCalls),
	}

	got, err := resolveHandlers(registrations, []string{"disabled"}, zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("resolveHandlers returned error: %v", err)
	}

	gotQueues := make([]string, len(got))
	for i, h := range got {
		gotQueues[i] = h.Queue
	}

	if want := []string{"first", "last"}; !reflect.DeepEqual(gotQueues, want) {
		t.Fatalf("resolved queue order = %v, want %v", gotQueues, want)
	}
	if getCalls["first"] != 1 || getCalls["last"] != 1 {
		t.Fatalf("enabled Get calls = %v, want one call for each enabled handler", getCalls)
	}
	if getCalls["disabled"] != 0 {
		t.Fatalf("disabled handler Get calls = %d, want 0", getCalls["disabled"])
	}
}

func TestSelectHandlerRegistrationsDoesNotRealizeEnabledHandlers(t *testing.T) {
	t.Parallel()

	getCalls := make(map[string]int)
	registrations := []RegisteredHandler{
		testRegistration("first", getCalls),
		testRegistration("second", getCalls),
	}

	enabled, disabled, err := selectHandlerRegistrations(registrations, []string{"second"})
	if err != nil {
		t.Fatalf("selectHandlerRegistrations returned error: %v", err)
	}
	if len(getCalls) != 0 {
		t.Fatalf("selection realized handler factories: %v", getCalls)
	}
	if got := []string{enabled[0].Name}; !reflect.DeepEqual(got, []string{"first"}) {
		t.Fatalf("enabled queues = %v, want [first]", got)
	}
	if !reflect.DeepEqual(disabled, []string{"second"}) {
		t.Fatalf("disabled queues = %v, want [second]", disabled)
	}
}

func TestResolveHandlersDefaultKeepsEveryRegistration(t *testing.T) {
	t.Parallel()

	getCalls := make(map[string]int)
	registrations := []RegisteredHandler{
		testRegistration("first", getCalls),
		testRegistration("second", getCalls),
	}

	got, err := resolveHandlers(registrations, nil, zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("resolveHandlers returned error: %v", err)
	}

	if gotQueues := []string{got[0].Queue, got[1].Queue}; !reflect.DeepEqual(gotQueues, []string{"first", "second"}) {
		t.Fatalf("resolved queue order = %v, want [first second]", gotQueues)
	}
}

func TestResolveHandlersRejectsInvalidDisabledQueuesBeforeRealization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		disabled []string
		wantErr  string
	}{
		{name: "blank", disabled: []string{""}, wantErr: "blank queue name"},
		{name: "whitespace", disabled: []string{" \t"}, wantErr: "blank queue name"},
		{name: "duplicate", disabled: []string{"first", "first"}, wantErr: "duplicate queue"},
		{name: "unknown", disabled: []string{"missing"}, wantErr: "unknown queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			getCalls := make(map[string]int)
			registrations := []RegisteredHandler{
				testRegistration("first", getCalls),
				testRegistration("second", getCalls),
			}

			_, err := resolveHandlers(registrations, tt.disabled, zap.NewNop().Sugar())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("resolveHandlers error = %v, want containing %q", err, tt.wantErr)
			}
			if len(getCalls) != 0 {
				t.Fatalf("handler factories were realized before validation failed: %v", getCalls)
			}
		})
	}
}

func TestResolveHandlersRejectsInvalidRegistrationsBeforeRealization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		registrations func(map[string]int) []RegisteredHandler
		wantErr       string
	}{
		{
			name: "blank",
			registrations: func(calls map[string]int) []RegisteredHandler {
				return []RegisteredHandler{
					testRegistration("first", calls),
					testRegistration("", calls),
				}
			},
			wantErr: "blank name",
		},
		{
			name: "duplicate",
			registrations: func(calls map[string]int) []RegisteredHandler {
				return []RegisteredHandler{
					testRegistration("first", calls),
					testRegistration("first", calls),
				}
			},
			wantErr: "registered more than once",
		},
		{
			name: "nil factory",
			registrations: func(calls map[string]int) []RegisteredHandler {
				return []RegisteredHandler{
					testRegistration("first", calls),
					{Name: "nil"},
				}
			},
			wantErr: "nil factory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			getCalls := make(map[string]int)
			_, err := resolveHandlers(tt.registrations(getCalls), nil, zap.NewNop().Sugar())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("resolveHandlers error = %v, want containing %q", err, tt.wantErr)
			}
			if len(getCalls) != 0 {
				t.Fatalf("handler factories were realized before validation failed: %v", getCalls)
			}
		})
	}
}

func TestResolveHandlersRejectsRealizedQueueMismatch(t *testing.T) {
	t.Parallel()

	registration := RegisteredHandler{
		Name: "registered",
		Handler: lazy.New(func() (handler.Handler, error) {
			return handler.Handler{Queue: "realized"}, nil
		}),
	}

	_, err := resolveHandlers([]RegisteredHandler{registration}, nil, zap.NewNop().Sugar())
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("resolveHandlers error = %v, want queue-name mismatch", err)
	}
}

func TestResolveHandlersWrapsFactoryError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("factory failed")
	registration := RegisteredHandler{
		Name: "broken",
		Handler: lazy.New(func() (handler.Handler, error) {
			return handler.Handler{}, wantErr
		}),
	}

	_, err := resolveHandlers([]RegisteredHandler{registration}, nil, zap.NewNop().Sugar())
	if !errors.Is(err, wantErr) {
		t.Fatalf("resolveHandlers error = %v, want wrapping %v", err, wantErr)
	}
}

func testRegistration(name string, getCalls map[string]int) RegisteredHandler {
	return RegisteredHandler{
		Name: name,
		Handler: lazy.New(func() (handler.Handler, error) {
			getCalls[name]++

			return handler.Handler{Queue: name}, nil
		}),
	}
}
