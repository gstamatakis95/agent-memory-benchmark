// Package txn is the capability handle of the caller's open transaction (PLAN.md section 2.1; register N133a, N140). It
// is a leaf: quota.Meter takes a txn.Tx and records usage inside the caller's transaction without importing store, and
// store.Tx implements it (Tx.Txn()). It contains no I/O.
package txn

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
)

// Tx is implemented by store.Tx. quota.Meter takes this, never store.Tx, so quota does not import store.
type Tx interface {
	Scope() id.Scope
	Usage() Usage
}

// UsageKey is sha256(operation_id || activity || item key): the exactly-once key of token_usage_events (PD-1).
type UsageKey [32]byte

// UsageEvent is one gateway call as metered in the shard (table token_usage_events). Cost is computed at write time
// with PriceVersion (N20).
type UsageEvent struct {
	Op               string // extract | summarize | embed | consolidate | adjudicate | reflect | page | rerank
	Model            string
	PriceVersion     string
	Operation        id.OperationID // zero for calls outside an operation
	Day              time.Time      // UTC day the call is billed to
	PromptTokens     int64
	CompletionTokens int64
	CostMicros       int64
	Cached           bool
}

// Usage is the metering handle of one transaction.
type Usage interface {
	// Record is exactly-once through usage_key (PD-1): a second call with the same key inserts nothing and reports
	// inserted = false.
	Record(ctx context.Context, key UsageKey, e UsageEvent) (inserted bool, err error)
	// Day is the rollup read for quota.Meter.Remaining: tokens (prompt + completion) and facts used on a UTC day.
	Day(ctx context.Context, day time.Time) (tokens, facts int64, err error)
}
