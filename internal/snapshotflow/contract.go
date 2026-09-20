// Package snapshotflow is the Temporal side of snapshot publishing
// (docs/07-snapshot-serving.md section 3):
//
//	BuildSnapshotWorkflow(BuildInput)
//	  ├─ AllocateVersion(enrichmentVersion)      → version (Postgres sequence)
//	  ├─ BuildAndSeal(version, enrichmentVersion) → BuildResult (local tar.zst)
//	  └─ Publish(version, tarPath)               → PublishResult (S3 key; pointer flipped)
//
// BuildAndSeal streams every enriched row at the pinned enrichment version
// straight from Postgres into a snapshot.Builder on local disk (no
// per-row activity payloads), heartbeating progress; Publish uploads the
// artifact, appends a 'published' snapshot_events row and flips
// current.json strictly after the upload completes. The workflow id is
// fixed per enrichment version so there is exactly one writer at a time
// (Bleve is single-writer; rebuilds are atomic and reproducible).
package snapshotflow

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"

	"example.com/agentmem/internal/snapshot"
)

// TaskQueue is the Temporal task queue for snapshot builds (separate from
// the enrichment sweep so a long build never starves the sweeper).
const TaskQueue = "snapshot"

// WorkflowIDPrefix + enrichment version is the workflow id, so concurrent
// build requests for the same version collapse into one run.
const WorkflowIDPrefix = "build-snapshot-v"

// BuildInput parameterizes one build.
type BuildInput struct {
	EnrichmentVersion int16
	// VectorDim is the stored vector width. 0 => embed.Dims (768, no
	// truncation). Values below embed.Dims slice the leading dims and
	// re-normalize (Matryoshka); only valid when the served model applies
	// its final layer-norm before the slice — see docs/07 section 1.
	VectorDim int
}

// BuildResult is what BuildAndSeal returns to the workflow: the local
// tarball path on the worker that built it plus the manifest. The path is
// only meaningful on the same worker, which is why Publish runs on the
// same task queue and tolerates a missing file by failing the workflow.
type BuildResult struct {
	Version  int64
	TarPath  string
	Manifest snapshot.Manifest
}

// PublishResult is the workflow's output.
type PublishResult struct {
	Version  int64
	Key      string
	Manifest snapshot.Manifest
}

// Run starts (or joins) the build workflow for in.EnrichmentVersion and
// blocks until it completes.
func Run(ctx context.Context, c client.Client, in BuildInput) (PublishResult, error) {
	return run(ctx, c, in)
}

// Activity/workflow timing knobs.
const (
	// BuildStartToClose bounds one BuildAndSeal attempt; a LongMemEval-S
	// corpus (~250k rows) streams and indexes in minutes, not hours.
	BuildStartToClose = 2 * time.Hour
	// BuildHeartbeat: the builder heartbeats after every Bleve batch. This is
	// liveness only (detect a dead worker); a retried attempt rebuilds from
	// scratch — see docs/07 section 3 for why that is the cheaper contract.
	BuildHeartbeat = 60 * time.Second
	// PublishStartToClose bounds the upload.
	PublishStartToClose = 30 * time.Minute
)
