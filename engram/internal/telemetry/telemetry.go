// Package telemetry is tracing, metrics and the label policy of PLAN.md section 2.2.23 (register D13, N62). The shard
// label is mandatory; a `tenant` or `namespace` label panics at registration. M0.1 declares the signatures only.
package telemetry

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
)

// ShardLabels is the pre-rendered label set of one shard (`shard="7"`), carried by router.ShardHandle.
type ShardLabels struct {
	Shard string
}

// Counter is a monotonically increasing metric.
type Counter interface {
	Add(delta float64, labelValues ...string)
}

// Histogram observes a distribution.
type Histogram interface {
	Observe(v float64, labelValues ...string)
}

// Gauge is a metric that goes up and down.
type Gauge interface {
	Set(v float64, labelValues ...string)
}

// Registry creates metrics. The shard label is mandatory and `tenant`/`namespace` labels panic at registration (N62).
type Registry interface {
	Counter(name string, labels ...string) Counter
	Histogram(name string, b []float64, labels ...string) Histogram
	Gauge(name string, labels ...string) Gauge
}

// UsageReport is a per-tenant token and cost report from token_usage (replaces a tenant metric label, N62).
type UsageReport struct {
	Tenant                         id.TenantID
	From, To                       time.Time
	PromptTokens, CompletionTokens int64
	CostMicros                     int64
}

// TenantMeter reports per-tenant usage from token_usage; it replaces a tenant label (N62).
type TenantMeter interface {
	Report(ctx context.Context, t id.TenantID, from, to time.Time) (*UsageReport, error)
}

// StartStage opens a span for one pipeline stage and returns the function that ends it with the stage's error.
func StartStage(ctx context.Context, stage string) (context.Context, func(err error)) {
	panic("stub")
}
