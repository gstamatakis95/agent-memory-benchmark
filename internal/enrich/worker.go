package enrich

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Worker rate-limit defaults (docs/03-temporal.md section 3: 32 goroutines x
// ~50ms unary embed ~= 640/s, so 600/s task-queue-wide protects the
// embedder across ALL workers).
const (
	DefaultMaxConcurrentActivities      = 64
	DefaultTaskQueueActivitiesPerSecond = 600.0
)

// WorkerConfig tunes the enrichment worker; zero values mean the defaults
// above and the "enrichment" task queue.
type WorkerConfig struct {
	TaskQueue string
	// MaxConcurrentActivities caps parallel activity slots on THIS worker.
	MaxConcurrentActivities int
	// TaskQueueActivitiesPerSecond is the server-enforced, task-queue-wide
	// activity rate cap — the single global embedder throttle.
	TaskQueueActivitiesPerSecond float64
}

// NewWorker builds the enrichment worker with the sweep workflow and
// activities registered. The caller starts it (w.Run / w.Start) and MUST do
// so before EnsureSchedule, or the first schedule fire finds no worker.
func NewWorker(c client.Client, a *Activities, cfg WorkerConfig) worker.Worker {
	if cfg.TaskQueue == "" {
		cfg.TaskQueue = TaskQueue
	}
	if cfg.MaxConcurrentActivities <= 0 {
		cfg.MaxConcurrentActivities = DefaultMaxConcurrentActivities
	}
	if cfg.TaskQueueActivitiesPerSecond <= 0 {
		cfg.TaskQueueActivitiesPerSecond = DefaultTaskQueueActivitiesPerSecond
	}
	w := worker.New(c, cfg.TaskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: cfg.MaxConcurrentActivities,
		TaskQueueActivitiesPerSecond:       cfg.TaskQueueActivitiesPerSecond,
		WorkerActivitiesPerSecond:          0, // unlimited; rely on the task-queue cap
	})
	w.RegisterWorkflow(EnrichmentSweepWorkflow)
	w.RegisterActivity(a) // registers CountBacklog, PlanRanges, ProcessBatch
	return w
}
