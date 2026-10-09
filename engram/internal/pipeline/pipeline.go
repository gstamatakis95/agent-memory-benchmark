// Package pipeline is the generic ordered Step runner of PLAN.md section 2.1 (register N132, N140). A request is a
// fixed sequence of named steps with per-step timing and skip rules; the recall planner's Fuse, Rerank, Boost, Pack and
// Stream stages are Steps over *recall.State. It is a leaf: the standard library and go.opentelemetry.io/otel (the
// span) are its only imports, and it performs no I/O.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ErrSkip is returned by a Step to record itself as skipped (deadline, disabled, empty window) and let the pipeline go
// on. Any other error stops the pipeline.
var ErrSkip = errors.New("pipeline: step skipped")

// Step is one named stage of a request. Run mutates the shared state S; returning ErrSkip records the stage as skipped
// and continues; any other error stops the pipeline.
type Step[S any] interface {
	Name() string
	Run(ctx context.Context, s S) error
}

// StepFunc adapts a function to a Step.
type StepFunc[S any] struct {
	StepName string
	Fn       func(ctx context.Context, s S) error
}

// Name implements Step.
func (f StepFunc[S]) Name() string { return f.StepName }

// Run implements Step.
func (f StepFunc[S]) Run(ctx context.Context, s S) error { return f.Fn(ctx, s) }

// StageTiming is the record of one step. Every executed step produces exactly one, in order. A step that failed has Err
// set and is the last element; steps after it do not run and have no timing.
type StageTiming struct {
	Name     string
	Start    time.Time
	Duration time.Duration
	Skipped  bool
	Err      error
}

// TracerName is the OpenTelemetry instrumentation name of the pipeline span.
const TracerName = "github.com/gstamatakis95/engram/internal/pipeline"

// clock is replaced in tests so that StageTiming can be asserted exactly.
var clock = time.Now

// Run executes the steps in order, opening one OpenTelemetry span ("pipeline.run", with the step names as an attribute
// and one event per step) and recording one StageTiming per executed step. It returns the timings of the steps that ran
// and, when a step failed, that step's error wrapped with its name (errors.Is and errors.As see through the wrap). A
// cancelled context stops the run before the next step with the context's error.
func Run[S any](ctx context.Context, s S, steps ...Step[S]) ([]StageTiming, error) {
	ctx, span := otel.Tracer(TracerName).Start(ctx, "pipeline.run")
	defer span.End()

	names := make([]string, len(steps))
	for i, st := range steps {
		names[i] = st.Name()
	}
	span.SetAttributes(attribute.StringSlice("pipeline.steps", names))

	timings := make([]StageTiming, 0, len(steps))
	for _, st := range steps {
		if err := ctx.Err(); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return timings, fmt.Errorf("pipeline before step %q: %w", st.Name(), err)
		}
		start := clock()
		err := st.Run(ctx, s)
		t := StageTiming{Name: st.Name(), Start: start, Duration: clock().Sub(start)}
		switch {
		case err == nil:
		case errors.Is(err, ErrSkip):
			t.Skipped = true
		default:
			t.Err = err
		}
		timings = append(timings, t)
		span.AddEvent("step", trace.WithAttributes(attribute.String("name", t.Name),
			attribute.Int64("duration_ns", int64(t.Duration)), attribute.Bool("skipped", t.Skipped)))
		if t.Err != nil {
			span.RecordError(t.Err)
			span.SetStatus(codes.Error, t.Err.Error())
			return timings, fmt.Errorf("pipeline step %q: %w", t.Name, t.Err)
		}
	}
	return timings, nil
}
