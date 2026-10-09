package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type state struct{ log []string }

func step(name string, d time.Duration, err error) Step[*state] {
	return StepFunc[*state]{StepName: name, Fn: func(_ context.Context, s *state) error {
		s.log = append(s.log, name)
		fakeNow = fakeNow.Add(d)
		return err
	}}
}

var fakeNow time.Time

func useFakeClock(t *testing.T) {
	t.Helper()
	fakeNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	old := clock
	clock = func() time.Time { return fakeNow }
	t.Cleanup(func() { clock = old })
}

func TestRun_StageTimingPerStep(t *testing.T) {
	useFakeClock(t)
	s := &state{}
	start := fakeNow
	timings, err := Run(context.Background(), s,
		step("fuse", 5*time.Millisecond, nil), step("rerank", 90*time.Millisecond, nil),
		step("pack", 3*time.Millisecond, nil))
	if err != nil {
		t.Fatal(err)
	}
	want := []StageTiming{
		{Name: "fuse", Start: start, Duration: 5 * time.Millisecond},
		{Name: "rerank", Start: start.Add(5 * time.Millisecond), Duration: 90 * time.Millisecond},
		{Name: "pack", Start: start.Add(95 * time.Millisecond), Duration: 3 * time.Millisecond},
	}
	if len(timings) != len(want) {
		t.Fatalf("got %d timings, want %d", len(timings), len(want))
	}
	for i := range want {
		if timings[i] != want[i] {
			t.Errorf("timing %d = %+v, want %+v", i, timings[i], want[i])
		}
	}
}

func TestRun_ErrSkipContinues(t *testing.T) {
	useFakeClock(t)
	s := &state{}
	wrapped := errors.Join(errors.New("deadline below the 106 ms reserve"), ErrSkip)
	timings, err := Run(context.Background(), s, step("fuse", 0, nil), step("rerank", time.Millisecond, wrapped),
		step("pack", 0, nil))
	if err != nil {
		t.Fatalf("ErrSkip must not stop the pipeline, got %v", err)
	}
	if got := s.log; len(got) != 3 || got[2] != "pack" {
		t.Fatalf("steps run = %v, want all three", got)
	}
	if !timings[1].Skipped || timings[0].Skipped || timings[2].Skipped || timings[1].Err != nil {
		t.Errorf("only rerank is skipped (and not failed): %+v", timings)
	}
}

func TestRun_ErrorStopsAndIsWrapped(t *testing.T) {
	useFakeClock(t)
	boom := errors.New("boom")
	s := &state{}
	timings, err := Run(context.Background(), s, step("a", 0, nil), step("b", time.Millisecond, boom),
		step("c", 0, nil))
	if !errors.Is(err, boom) {
		t.Fatalf("error %v must wrap boom", err)
	}
	if len(s.log) != 2 || len(timings) != 2 {
		t.Fatalf("step c must not run: log %v, %d timings", s.log, len(timings))
	}
	if timings[1].Err == nil || timings[1].Skipped || timings[0].Err != nil {
		t.Errorf("failed step must carry Err: %+v", timings)
	}
	if want := `pipeline step "b": boom`; err.Error() != want {
		t.Errorf("error text = %q, want %q", err.Error(), want)
	}
}

func TestRun_NoSteps(t *testing.T) {
	timings, err := Run[*state](context.Background(), &state{})
	if err != nil || len(timings) != 0 {
		t.Fatalf("empty pipeline = %v, %v", timings, err)
	}
}

func TestRun_CancelledContextStopsBeforeNextStep(t *testing.T) {
	useFakeClock(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := &state{}
	cancelling := StepFunc[*state]{StepName: "a", Fn: func(context.Context, *state) error { cancel(); return nil }}
	timings, err := Run(ctx, s, Step[*state](cancelling), step("b", 0, nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(timings) != 1 || len(s.log) != 0 {
		t.Fatalf("b must not run: %d timings, log %v", len(timings), s.log)
	}
}

func TestRun_OpensOneSpanWithAnEventPerStep(t *testing.T) {
	useFakeClock(t)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	steps := []Step[*state]{step("a", 0, nil), step("b", 0, ErrSkip), step("c", 0, nil)}
	if _, err := Run(context.Background(), &state{}, steps...); err != nil {
		t.Fatal(err)
	}
	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Name() != "pipeline.run" {
		t.Fatalf("want exactly one span pipeline.run, got %d", len(spans))
	}
	if n := len(spans[0].Events()); n != 3 {
		t.Errorf("span has %d step events, want 3", n)
	}
}
