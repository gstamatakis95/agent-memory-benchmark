// Package quota is rates, token metering and admission (PLAN.md section 2.2.21; register D13, N130, N135, PD-1).
// Pattern: Gate, one Reserve in front of every gateway call class. A refused Reserve defers a workflow (DEFERRED,
// resume_at = the window reset, written through store.OperationRepo.Defer) and returns RESOURCE_EXHAUSTED to a
// synchronous Reflect. M0.1 declares the signatures only.
package quota

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/gateway"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/txn"
)

// Bucket names a rate bucket of the interceptor: recalls_per_min, retains_per_min, request bytes, namespaces (D13).
type Bucket string

// The rate buckets.
const (
	BucketRecall Bucket = "recalls_per_min"
	BucketRetain Bucket = "retains_per_min"
	BucketNone   Bucket = ""
)

// Key identifies a token bucket: (tenant, namespace, bucket).
type Key struct {
	Scope  id.Scope
	Bucket Bucket
}

// Decision is the answer of Limiter.Allow.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
}

// CallClass is a gateway call class: extract | summarize | route | write | refresh | reflect-iteration (N130).
type CallClass string

// The call classes.
const (
	CallExtract     CallClass = "extract"
	CallSummarize   CallClass = "summarize"
	CallRoute       CallClass = "route"
	CallWrite       CallClass = "write"
	CallRefresh     CallClass = "refresh"
	CallReflectIter CallClass = "reflect-iteration"
)

// Reservation is a granted admission; a refusal is an error (errs.QuotaExceeded) the caller turns into DEFERRED.
type Reservation struct {
	Tokens int64
}

// Limits are the effective per-namespace limits Meter.Remaining reads against.
type Limits struct {
	LLMTokensPerDay int64
	MaxFacts        int64
}

// Limiter holds in-process token buckets (recalls and retains per minute); the per-process share is limit / N_api.
type Limiter interface {
	Allow(ctx context.Context, k Key, n int64) (Decision, error)
}

// Reserver is admission for a gateway call class.
type Reserver interface {
	// Reserve admits a call of the class: extract | summarize | route | write | refresh | reflect-iteration.
	Reserve(ctx context.Context, sc id.Scope, call CallClass, tokens int64) (Reservation, error)
}

// Meter records usage inside the caller's transaction. It takes the leaf txn handle, so quota does not import store
// (N140).
type Meter interface {
	// Record is exactly-once through usage_key (PD-1).
	Record(ctx context.Context, tx txn.Tx, op string, u gateway.Usage, day time.Time) error
	Remaining(ctx context.Context, tx txn.Tx, l Limits, day time.Time) (tokens, facts int64, err error)
}
