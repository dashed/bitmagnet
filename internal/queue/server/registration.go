package server

import (
	"fmt"
	"strings"

	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/queue/handler"
	"go.uber.org/zap"
)

// RegisteredHandler pairs a queue name with its lazy handler factory. Keeping
// the name outside the lazy value lets the server exclude an externally-owned
// queue without realizing any of that handler's dependencies.
type RegisteredHandler struct {
	Name    string
	Handler lazy.Lazy[handler.Handler]
}

func resolveHandlers(
	registrations []RegisteredHandler,
	disabledQueues []string,
	logger *zap.SugaredLogger,
) ([]handler.Handler, error) {
	enabled, disabled, err := selectHandlerRegistrations(registrations, disabledQueues)
	if err != nil {
		return nil, err
	}

	logDisabledQueues(logger, disabled)

	return realizeHandlers(enabled)
}

func selectHandlerRegistrations(
	registrations []RegisteredHandler,
	disabledQueues []string,
) ([]RegisteredHandler, []string, error) {
	registered := make(map[string]struct{}, len(registrations))

	for _, registration := range registrations {
		if strings.TrimSpace(registration.Name) == "" {
			return nil, nil, fmt.Errorf("queue handler registration has a blank name")
		}
		if registration.Handler == nil {
			return nil, nil, fmt.Errorf(
				"queue handler %q has a nil factory",
				registration.Name,
			)
		}
		if _, exists := registered[registration.Name]; exists {
			return nil, nil, fmt.Errorf(
				"queue handler %q is registered more than once",
				registration.Name,
			)
		}

		registered[registration.Name] = struct{}{}
	}

	disabled := make(map[string]struct{}, len(disabledQueues))

	for _, queueName := range disabledQueues {
		if strings.TrimSpace(queueName) == "" {
			return nil, nil, fmt.Errorf("queue_server.disabled_queues contains a blank queue name")
		}
		if _, exists := disabled[queueName]; exists {
			return nil, nil, fmt.Errorf(
				"queue_server.disabled_queues contains duplicate queue %q",
				queueName,
			)
		}
		if _, exists := registered[queueName]; !exists {
			return nil, nil, fmt.Errorf(
				"queue_server.disabled_queues contains unknown queue %q",
				queueName,
			)
		}

		disabled[queueName] = struct{}{}
	}

	enabled := make([]RegisteredHandler, 0, len(registrations)-len(disabled))

	for _, registration := range registrations {
		if _, isDisabled := disabled[registration.Name]; !isDisabled {
			enabled = append(enabled, registration)
		}
	}

	return enabled, append([]string(nil), disabledQueues...), nil
}

func realizeHandlers(registrations []RegisteredHandler) ([]handler.Handler, error) {
	handlers := make([]handler.Handler, 0, len(registrations))

	for _, registration := range registrations {
		h, err := registration.Handler.Get()
		if err != nil {
			return nil, fmt.Errorf("realizing queue handler %q: %w", registration.Name, err)
		}
		if h.Queue != registration.Name {
			return nil, fmt.Errorf(
				"queue handler registration name %q does not match realized queue %q",
				registration.Name,
				h.Queue,
			)
		}

		handlers = append(handlers, h)
	}

	return handlers, nil
}

func logDisabledQueues(logger *zap.SugaredLogger, disabledQueues []string) {
	for _, queueName := range disabledQueues {
		logger.Infow("queue handler disabled", "queue", queueName)
	}
}
