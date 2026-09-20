package snapshotflow

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// NewWorker builds the snapshot-build worker with BuildSnapshotWorkflow and
// its activities registered. taskQueue "" defaults to TaskQueue. The build
// is heavy (one Bleve writer, streaming the whole corpus) so concurrency is
// capped low: this worker should not compete for CPU with a build already
// in flight, and the workflow id prefix already keeps concurrent builds of
// the SAME enrichment version from overlapping.
func NewWorker(c client.Client, a *Activities, taskQueue string) worker.Worker {
	if taskQueue == "" {
		taskQueue = TaskQueue
	}
	w := worker.New(c, taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: 2,
	})
	w.RegisterWorkflow(BuildSnapshotWorkflow)
	w.RegisterActivity(a) // registers AllocateVersion, BuildAndSeal, Publish
	return w
}
