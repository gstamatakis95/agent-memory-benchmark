package snapshotflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// BuildSnapshotWorkflow runs the three-step build pipeline (package doc
// comment): AllocateVersion -> BuildAndSeal -> Publish. The snapshot task
// queue is served by exactly one worker (the server binary), so
// BuildAndSeal's local tarball is guaranteed to still be on disk for
// Publish; Activities.Publish fails non-retryably if that invariant is
// ever violated (e.g. a worker restart between the two activities).
func BuildSnapshotWorkflow(ctx workflow.Context, in BuildInput) (PublishResult, error) {
	var a *Activities // method references only; the worker holds the real deps

	var version int64
	allocateCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	if err := workflow.ExecuteActivity(allocateCtx, a.AllocateVersion, in.EnrichmentVersion).Get(ctx, &version); err != nil {
		return PublishResult{}, err
	}

	var built BuildResult
	buildCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: BuildStartToClose,
		HeartbeatTimeout:    BuildHeartbeat,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	})
	if err := workflow.ExecuteActivity(buildCtx, a.BuildAndSeal, in, version).Get(ctx, &built); err != nil {
		return PublishResult{}, err
	}

	var out PublishResult
	publishCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: PublishStartToClose,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	if err := workflow.ExecuteActivity(publishCtx, a.Publish, built).Get(ctx, &out); err != nil {
		return PublishResult{}, err
	}
	return out, nil
}
