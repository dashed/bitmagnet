package graphqlshadow

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type captureContextKey struct{}

// Capture holds the request-entry timestamp and the comparable raw response
// captured by gqlgen middleware. CaptureResponse seals the Go reference
// duration immediately after response generation; Finish only dispatches the
// dark comparison after gql.ServeHTTP returns.
type Capture struct {
	started time.Time

	mu       sync.Mutex
	response *capturedResponse
}

type capturedResponse struct {
	request           Request
	data              json.RawMessage
	referenceDuration time.Duration
	hasError          bool
	err               error
}

// Hook coordinates capture of the primary Go response with default-off,
// sampled, non-blocking execution against the dark Rust endpoint.
type Hook struct {
	cfg     Config
	driver  *Driver
	metrics *Metrics
	logger  *zap.SugaredLogger
	sem     *semaphore.Weighted

	// Injectable nondeterminism/asynchrony/clock keeps focused tests reliable.
	sample func() float64
	run    func(func())
	now    func() time.Time
}

// NewHook constructs the production hook.
func NewHook(
	cfg Config,
	rust *HTTPExecutor,
	metrics *Metrics,
	logger *zap.SugaredLogger,
) *Hook {
	return newHook(cfg, rust, metrics, logger)
}

func newHook(
	cfg Config,
	rust Executor,
	metrics *Metrics,
	logger *zap.SugaredLogger,
) *Hook {
	return &Hook{
		cfg:     cfg,
		driver:  NewDriver(rust, metrics),
		metrics: metrics,
		logger:  logger,
		sem:     semaphore.NewWeighted(int64(cfg.maxConcurrent())),
		sample:  rand.Float64,
		run: func(f func()) {
			go f()
		},
		now: time.Now,
	}
}

// Begin records Go HTTP request entry and installs a request-scoped Capture for
// CaptureResponse. When the feature is inactive it returns the original context
// and a nil capture with no clock read.
func (h *Hook) Begin(ctx context.Context) (context.Context, *Capture) {
	if h == nil || !h.cfg.active() {
		return ctx, nil
	}

	capture := &Capture{
		started: h.now(),
	}

	return context.WithValue(ctx, captureContextKey{}, capture), capture
}

// CaptureResponse is gqlgen response middleware. It executes the primary
// response exactly once and records request-entry-through-response-generation
// duration immediately after next returns. It captures only an eligible request
// plus raw response; it never samples, admits, launches, or calls Rust.
func (h *Hook) CaptureResponse(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
	response := next(ctx)

	capture, ok := ctx.Value(captureContextKey{}).(*Capture)
	if !ok || capture == nil {
		return response
	}

	referenceDuration := h.now().Sub(capture.started)

	if response == nil || !graphql.HasOperationContext(ctx) {
		return response
	}

	opCtx := graphql.GetOperationContext(ctx)

	opType, err := ClassifyOperation(opCtx.RawQuery, opCtx.OperationName)
	if err != nil || opType != OperationQuery {
		// Safety classification happens before Finish can sample or call Rust.
		h.metrics.incDropped()

		return response
	}

	if !IsComparableSearchOperation(opCtx.RawQuery, opCtx.OperationName, opCtx.Variables) {
		return response
	}

	variables, marshalErr := json.Marshal(opCtx.Variables)
	captured := &capturedResponse{
		request: Request{
			Query:         opCtx.RawQuery,
			OperationName: opCtx.OperationName,
			Variables:     variables,
		},
		data:              append(json.RawMessage(nil), response.Data...),
		referenceDuration: referenceDuration,
		hasError:          len(response.Errors) != 0,
		err:               marshalErr,
	}

	capture.mu.Lock()
	if capture.response == nil {
		capture.response = captured
	}
	capture.mu.Unlock()

	return response
}

// Finish samples and launches any eligible dark comparison after the served
// handler returns. The Go duration was already sealed by CaptureResponse, so
// response writing and client/socket delay cannot inflate it.
func (h *Hook) Finish(ctx context.Context, capture *Capture) {
	if h == nil || capture == nil {
		return
	}

	capture.mu.Lock()
	captured := capture.response
	capture.mu.Unlock()

	if captured == nil || h.sample() >= h.cfg.SampleRate {
		return
	}

	h.metrics.incSampled()

	if captured.err != nil || captured.hasError {
		h.metrics.incReferenceError()

		return
	}

	reference, err := SearchResultFromResponseData(captured.data)
	if err != nil {
		h.metrics.incReferenceError()

		if h.logger != nil {
			h.logger.Debugw("GraphQL shadow: captured Go response is not comparable", "error", err)
		}

		return
	}

	// Admission is deliberately non-blocking after the Go handler has returned.
	// Saturation sheds the comparison immediately; it never queues behind Rust.
	if !h.sem.TryAcquire(1) {
		h.metrics.incSaturated()

		return
	}

	h.metrics.incAdmitted()

	background := context.WithoutCancel(ctx)

	h.run(func() {
		defer h.sem.Release(1)

		shadowCtx, cancel := context.WithTimeout(background, h.cfg.timeout())
		defer cancel()

		outcome, comparison, shadowErr := h.driver.ShadowCaptured(
			shadowCtx,
			captured.request,
			reference,
			captured.referenceDuration,
		)
		if shadowErr != nil {
			if h.logger != nil {
				h.logger.Debugw(
					"GraphQL shadow: dark comparison failed",
					"outcome", outcome,
					"error", shadowErr,
				)
			}

			return
		}

		if comparison != nil && comparison.IsDiscrepancy() && h.cfg.LogDiscrepancies && h.logger != nil {
			h.logger.Infow(
				"GraphQL shadow discrepancy",
				"operation_name", captured.request.OperationName,
				"go_count", comparison.PGCount,
				"rust_count", comparison.TantivyCount,
				"total_count_delta", comparison.TotalCountDelta,
				"facets_observed", comparison.FacetsObserved,
				"facets_matched", comparison.FacetsMatched,
				"jaccard_at_20", comparison.JaccardAt20,
				"rbo", comparison.RBO,
				"top1_match", comparison.Top1Match,
			)
		}
	})
}
