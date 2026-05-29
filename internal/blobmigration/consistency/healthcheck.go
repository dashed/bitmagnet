package consistency

import (
	"context"
	"fmt"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/health"
	dto "github.com/prometheus/client_model/go"
)

func NewHealthCheck(metrics *Metrics) health.Check {
	return health.Check{
		Name:    "blob_consistency",
		Timeout: time.Second,
		Check: func(context.Context) error {
			var m dto.Metric
			if err := metrics.ErrorsTotal.Write(&m); err != nil {
				return fmt.Errorf("reading error metric: %w", err)
			}
			if m.GetCounter() != nil && m.GetCounter().GetValue() > 0 {
				return fmt.Errorf("blob consistency errors detected: %.0f", m.GetCounter().GetValue())
			}
			return nil
		},
	}
}
