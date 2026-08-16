package enrich

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// SweepInput parameterizes one sweep run. Zero values mean the package
// defaults, so the schedule can start the workflow with an empty input.
type SweepInput struct {
	Version   int16 // enrichment version to sweep (0 => DefaultVersion)
	BatchSize int   // ids per ProcessBatch (0 => DefaultBatchSize)
	FanOut    int   // ProcessBatch activities per wave (0 => DefaultFanOut)
}

// PermanentErrorType is matched by the activity retry policy's
// NonRetryableErrorTypes (docs/03-temporal.md section 3).
const PermanentErrorType = "PermanentEnrichmentError"

// sweepActivityOptions are the doc's exact activity options: generous
// StartToClose for a whole batch, short HeartbeatTimeout so a crashed worker
// is detected within ~15s, exponential retry capped at 5 attempts.
func sweepActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        30 * time.Second,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{PermanentErrorType},
		},
	}
}

// EnrichmentSweepWorkflow is the scheduled sweeper (docs/03-temporal.md
// section 3): loop { CountBacklog; if 0 exit; PlanRanges; fan out
// ProcessBatch futures; await all } until the backlog drains or ~50s of
// workflow time elapse. On the soft deadline it exits cleanly — the next
// 1-minute tick continues — which keeps every run's event history tiny and
// avoids ContinueAsNew entirely.
func EnrichmentSweepWorkflow(ctx workflow.Context, in SweepInput) error {
	version := in.Version
	if version == 0 {
		version = DefaultVersion
	}
	batchSize := in.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	fanOut := in.FanOut
	if fanOut <= 0 {
		fanOut = DefaultFanOut
	}

	ctx = workflow.WithActivityOptions(ctx, sweepActivityOptions())
	logger := workflow.GetLogger(ctx)
	var a *Activities // method references only; the worker holds the real deps

	deadline := workflow.Now(ctx).Add(SoftDeadline)
	for workflow.Now(ctx).Before(deadline) {
		var backlog int
		if err := workflow.ExecuteActivity(ctx, a.CountBacklog, version).Get(ctx, &backlog); err != nil {
			return err
		}
		if backlog == 0 {
			return nil // drained; the next tick re-checks
		}

		var batches []BatchInput
		if err := workflow.ExecuteActivity(ctx, a.PlanRanges, version, batchSize, fanOut).Get(ctx, &batches); err != nil {
			return err
		}
		if len(batches) == 0 {
			// Backlog counted but nothing claimable right now (e.g. rows
			// slipped back into their backoff window). Let the next tick try.
			return nil
		}

		futures := make([]workflow.Future, len(batches))
		for i, b := range batches {
			futures[i] = workflow.ExecuteActivity(ctx, a.ProcessBatch, b)
		}
		// Collect all futures. A failed batch leaves its rows pending for the
		// next wave/tick — log and continue, never fail the whole sweep.
		for i, f := range futures {
			var r BatchResult
			if err := f.Get(ctx, &r); err != nil {
				logger.Warn("enrich: batch failed; rows stay pending for next sweep",
					"batch", i, "error", err)
				continue
			}
			logger.Debug("enrich: batch finished",
				"batch", i, "done", r.Done, "failed", r.Failed, "permanent", r.Permanent)
		}

		// Advance workflow time between waves so the soft deadline is
		// reachable both in production and in the timer-skipping test env.
		if err := workflow.Sleep(ctx, interWavePause); err != nil {
			return err
		}
	}
	return nil // soft deadline hit; exit cleanly, next tick continues
}
