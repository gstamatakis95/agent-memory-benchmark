package snapshotflow

import (
	"context"
	"strconv"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// run starts (or joins) the build workflow for in.EnrichmentVersion and
// blocks until it completes. The workflow id is fixed per enrichment
// version (WorkflowIDPrefix + version), so a second concurrent request for
// the same version joins the already-running workflow instead of starting
// a duplicate (WorkflowIdConflictPolicy USE_EXISTING); once that run has
// completed, a later request starts a fresh one (WorkflowIdReusePolicy
// ALLOW_DUPLICATE) rather than being rejected.
func run(ctx context.Context, c client.Client, in BuildInput) (PublishResult, error) {
	id := WorkflowIDPrefix + strconv.FormatInt(int64(in.EnrichmentVersion), 10)
	wr, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       id,
		TaskQueue:                TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, BuildSnapshotWorkflow, in)
	if err != nil {
		return PublishResult{}, err
	}

	var out PublishResult
	if err := wr.Get(ctx, &out); err != nil {
		return PublishResult{}, err
	}
	return out, nil
}
