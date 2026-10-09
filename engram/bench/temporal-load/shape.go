package main

import (
	"context"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// The shape of RetainDocument (PLAN.md sections 2.2.17 and 5.1.2): four activities before the fan-out (LoadItem, Chunk,
// SummarizeDocument, PlanChunks), MarkProgress before each wave of at most 32 chunks, five activities per chunk
// (ExtractChunk, EmbedChunk, ResolveEntities, BuildLinks, CommitChunk) run as one coroutine per chunk, then
// FinalizeVersion and MarkOperation. Payload sizes follow N59: every activity result is at most 4 KiB (a cache or
// staging blob key plus counters), CommitChunk's input is keys-only, ChunkWork carries a header, a context, metadata
// and entity hints (about 1.8 KB) and no text. The activities do no work: the load test measures Temporal, not Engram.
const maxWave = 32

type stage struct {
	name string
	in   []int // sizes of the activity arguments in bytes
	out  int   // size of the result in bytes
}

var (
	loadItem  = stage{"LoadItem", []int{1000}, 500}
	chunkAct  = stage{"Chunk", []int{300}, 2000}
	summarize = stage{"SummarizeDocument", []int{300}, 500}
	planChunk = stage{"PlanChunks", []int{600}, 1500}
	progress  = stage{"MarkProgress", []int{200}, 100}
	finalize  = stage{"FinalizeVersion", []int{500}, 300}
	perChunk  = []stage{
		{"ExtractChunk", []int{150, 1800}, 3500},
		{"EmbedChunk", []int{150, 1800, 300}, 400},
		{"ResolveEntities", []int{150, 300}, 1000},
		{"BuildLinks", []int{150, 300, 400, 1000}, 500},
		{"CommitChunk", []int{800}, 300},
	}
)

// Input of the load-test workflow (and of the restore drill).
type shapeInput struct {
	Chunks     int
	StartNanos int64
	Sentinel   string // text that must only ever appear encrypted in the history (the drill and the encryption test)
}

var pool = func() []byte { b := make([]byte, 8192); _, _ = rand.Read(b); return b }()

func blob(n int) []byte { return pool[:n] }

var actOpts = workflow.ActivityOptions{
	StartToCloseTimeout: 2 * time.Minute,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
}

func run(ctx workflow.Context, s stage) error {
	args := make([]any, len(s.in))
	for i, n := range s.in {
		args[i] = blob(n)
	}
	var out []byte
	return workflow.ExecuteActivity(ctx, s.name, args...).Get(ctx, &out)
}

// retainShape is the workflow. It returns the history length it saw when it scheduled its last activity.
func retainShape(ctx workflow.Context, in shapeInput) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, actOpts)
	for _, s := range []stage{loadItem, chunkAct, summarize, planChunk} {
		if err := run(ctx, s); err != nil {
			return 0, err
		}
	}
	for next := 0; next < in.Chunks; next += maxWave {
		if err := run(ctx, progress); err != nil {
			return 0, err
		}
		wg := workflow.NewWaitGroup(ctx)
		var firstErr error
		for c := next; c < in.Chunks && c < next+maxWave; c++ {
			wg.Add(1)
			workflow.Go(ctx, func(ctx workflow.Context) {
				defer wg.Done()
				for _, s := range perChunk {
					if err := run(ctx, s); err != nil {
						firstErr = err
						return
					}
				}
			})
		}
		wg.Wait(ctx)
		if firstErr != nil {
			return 0, firstErr
		}
	}
	if err := run(ctx, finalize); err != nil {
		return 0, err
	}
	n := int(workflow.GetInfo(ctx).GetCurrentHistoryLength())
	err := workflow.ExecuteActivity(ctx, "MarkOperation", blob(300), n, in.StartNanos).Get(ctx, nil)
	return n, err
}

// counters are what the activities observe; the sampler reads them.
type counters struct {
	completed atomic.Int64
	events    atomic.Int64 // history events of completed workflows, counted at the last activity (+8: its own three
	// events, the final workflow task and the completion; drill-seed checks the constant against real histories)
	activities atomic.Int64
	mu         sync.Mutex
	latencies  []time.Duration // workflow start to MarkOperation
}

func (c *counters) activityFns() map[string]any {
	fns := map[string]any{}
	wrap := func(n int) { c.activities.Add(1); _ = n }
	one := func(s stage) any {
		return func(_ context.Context, _ []byte) ([]byte, error) { wrap(1); return blob(s.out), nil }
	}
	two := func(s stage) any {
		return func(_ context.Context, _, _ []byte) ([]byte, error) { wrap(2); return blob(s.out), nil }
	}
	three := func(s stage) any {
		return func(_ context.Context, _, _, _ []byte) ([]byte, error) { wrap(3); return blob(s.out), nil }
	}
	four := func(s stage) any {
		return func(_ context.Context, _, _, _, _ []byte) ([]byte, error) { wrap(4); return blob(s.out), nil }
	}
	for _, s := range append([]stage{loadItem, chunkAct, summarize, planChunk, progress, finalize}, perChunk...) {
		switch len(s.in) {
		case 1:
			fns[s.name] = one(s)
		case 2:
			fns[s.name] = two(s)
		case 3:
			fns[s.name] = three(s)
		case 4:
			fns[s.name] = four(s)
		}
	}
	fns["MarkOperation"] = func(ctx context.Context, _ []byte, histLen int, startNanos int64) error {
		_ = activity.GetInfo(ctx)
		c.activities.Add(1)
		c.completed.Add(1)
		c.events.Add(int64(histLen) + 8)
		if startNanos > 0 {
			d := time.Since(time.Unix(0, startNanos))
			c.mu.Lock()
			c.latencies = append(c.latencies, d)
			c.mu.Unlock()
		}
		return nil
	}
	return fns
}
