package snapshotflow

import (
	"context"

	"go.temporal.io/sdk/client"
)

// STUB — replaced by workflow.go / activities.go / worker.go.

func run(ctx context.Context, c client.Client, in BuildInput) (PublishResult, error) {
	panic("snapshotflow: not implemented")
}
