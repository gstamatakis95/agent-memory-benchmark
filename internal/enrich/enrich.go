// Package enrich is the Temporal enrichment layer (docs/03-temporal.md): a
// Schedule fires every minute (overlap Skip) and runs EnrichmentSweepWorkflow,
// which loops { CountBacklog -> PlanRanges -> fan out ProcessBatch } until the
// backlog drains or a ~50s soft deadline of workflow time passes, then exits
// cleanly so the next tick continues with a fresh, small event history.
//
// Persistence is the append-only ledger in internal/store: activities only
// ever INSERT (done/failed events); "pending" is derived by the absence of a
// done event, so retried and duplicated batches are harmless
// (docs/02-storage.md E.5, docs/04-append-only.md).
package enrich

import "time"

// TaskQueue is the Temporal task queue shared by the worker, the schedule
// action and the sweep's activities (docs/03-temporal.md section 3).
const TaskQueue = "enrichment"

// ScheduleID identifies the 1-minute sweep schedule.
const ScheduleID = "enrichment-sweep"

// DefaultVersion is the current enrichment pipeline version; bumping it
// re-enqueues every memory for re-enrichment for free (docs/04-append-only.md).
var DefaultVersion int16 = 1

// Sweep tuning (docs/03-temporal.md section 3 constants).
const (
	// DefaultBatchSize is the number of memory ids one ProcessBatch handles.
	DefaultBatchSize = 128
	// DefaultFanOut is how many ProcessBatch activities run per wave.
	DefaultFanOut = 8
	// SoftDeadline bounds one sweep run in WORKFLOW time; on expiry the run
	// exits cleanly and the next scheduled tick picks up the remainder.
	SoftDeadline = 50 * time.Second
	// interWavePause is a short workflow timer between waves. It exists so
	// workflow.Now advances between iterations: the soft deadline is measured
	// in workflow time, which only moves via timers/events — this both keeps
	// real runs polite and makes the deadline reachable in the auto
	// timer-skipping test environment (docs/06-testing.md Tier 1 gotcha).
	interWavePause = time.Second
	// DefaultEmbedConcurrency is the bounded errgroup fan-out inside one
	// ProcessBatch; it recovers throughput over the unary-only embedder
	// (docs/02-storage.md E.3, docs/03-temporal.md).
	DefaultEmbedConcurrency = 32
)
