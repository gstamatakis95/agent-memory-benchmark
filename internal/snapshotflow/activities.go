package snapshotflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"example.com/agentmem/internal/embed"
	"example.com/agentmem/internal/snapshot"
	"example.com/agentmem/internal/store"
)

// SnapshotTarballMissingErrorType is the non-retryable error type Publish
// raises when the built tarball is not on this worker's local disk — the
// snapshot task queue is served by exactly one worker (the server), so a
// missing tarball means BuildAndSeal ran on a different process instance
// (e.g. a restart moved work between attempts) and the whole workflow must
// simply be re-run from AllocateVersion.
const SnapshotTarballMissingErrorType = "SnapshotTarballMissing"

// Activities carries the snapshot-build activities' dependencies. Register
// one instance on the worker (see NewWorker).
type Activities struct {
	DB store.DB
	S3 *snapshot.S3Store
	// WorkDir is where BuildAndSeal writes a snapshot directory and the
	// sealed tarball before Publish uploads it.
	WorkDir string
	// DefaultVectorDim is used when a BuildInput carries VectorDim 0
	// (0 here too => embed.Dims).
	DefaultVectorDim int
	// EmbedModel is recorded in the manifest ("" => embed.Model).
	EmbedModel string
}

func (a *Activities) vectorDim(in BuildInput) int {
	if in.VectorDim > 0 {
		return in.VectorDim
	}
	if a.DefaultVectorDim > 0 {
		return a.DefaultVectorDim
	}
	return embed.Dims
}

func (a *Activities) embedModel() string {
	if a.EmbedModel != "" {
		return a.EmbedModel
	}
	return embed.Model
}

// AllocateVersion draws the next monotonic snapshot version.
func (a *Activities) AllocateVersion(ctx context.Context, enrichmentVersion int16) (int64, error) {
	v, err := store.AllocateSnapshotVersion(ctx, a.DB)
	if err != nil {
		return 0, fmt.Errorf("snapshotflow: allocate version: %w", err)
	}
	return v, nil
}

// recordFailure best-effort appends a 'failed' snapshot_events row and
// returns cause unchanged, so callers can `return BuildResult{},
// a.recordFailure(ctx, ..., err)`.
func (a *Activities) recordFailure(ctx context.Context, version int64, enrichmentVersion int16, cause error) error {
	_ = store.InsertSnapshotEvent(ctx, a.DB, store.SnapshotEvent{
		SnapshotVersion:   version,
		EnrichmentVersion: enrichmentVersion,
		Event:             "failed",
		ErrorMessage:      cause.Error(),
	})
	return cause
}

// snapshotVector converts one packed embedding column into the vector a
// snapshot row stores: validated against the native embed.Dims, optionally
// truncated to a smaller Matryoshka width and re-normalized (contract.go:
// BuildInput.VectorDim). Pure and side-effect-free so it can be unit tested
// without a builder or database.
func snapshotVector(packed []byte, dim int) ([]float32, error) {
	vec, err := embed.UnpackVector(packed)
	if err != nil {
		return nil, fmt.Errorf("snapshotflow: unpack embedding: %w", err)
	}
	if len(vec) != embed.Dims {
		return nil, fmt.Errorf("snapshotflow: embedding has %d dims, want %d", len(vec), embed.Dims)
	}
	if dim >= embed.Dims {
		return vec, nil
	}
	truncated := append([]float32(nil), vec[:dim]...)
	return embed.L2Normalize(truncated), nil
}

// BuildAndSeal streams every enriched row at in.EnrichmentVersion straight
// from Postgres into a snapshot.Builder on local disk, heartbeating
// progress every snapshot.BatchSize rows, then seals the finished directory
// into a tar.zst and appends a 'built' snapshot_events row.
func (a *Activities) BuildAndSeal(ctx context.Context, in BuildInput, version int64) (BuildResult, error) {
	dim := a.vectorDim(in)
	if dim <= 0 || dim > embed.Dims {
		err := fmt.Errorf("snapshotflow: vector dim %d out of range (want 1..%d)", dim, embed.Dims)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}

	dir := filepath.Join(a.WorkDir, fmt.Sprintf("build-v%d", version))
	if err := os.RemoveAll(dir); err != nil {
		err = fmt.Errorf("snapshotflow: clear stale build dir %s: %w", dir, err)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}
	if err := os.MkdirAll(a.WorkDir, 0o755); err != nil {
		err = fmt.Errorf("snapshotflow: mkdir work dir %s: %w", a.WorkDir, err)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}

	b, err := snapshot.NewBuilder(dir, snapshot.Manifest{
		Version:           version,
		EnrichmentVersion: in.EnrichmentVersion,
		VectorDim:         dim,
		EmbedModel:        a.embedModel(),
		Metric:            snapshot.Metric,
	})
	if err != nil {
		err = fmt.Errorf("snapshotflow: new builder: %w", err)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}

	var added, skipped int64
	streamErr := store.StreamAtVersionAll(ctx, a.DB, in.EnrichmentVersion, func(row store.EnrichedRow) error {
		if len(row.Embedding) == 0 {
			skipped++
			return nil
		}
		vec, err := snapshotVector(row.Embedding, dim)
		if err != nil {
			// A malformed embedding is a data bug in one row, not a reason
			// to fail the whole build: skip it like a missing embedding.
			skipped++
			return nil
		}
		doc := snapshot.Doc{
			MemoryID:       row.MemoryID,
			ConversationID: row.ConversationID,
			SessionID:      row.SessionID,
			ContentHash:    row.ContentHash,
			S3Key:          row.S3Key,
		}
		if row.TS != nil {
			doc.TSUnix = row.TS.Unix()
		}
		if err := b.Add(doc, row.Lexemes, vec); err != nil {
			return fmt.Errorf("snapshotflow: add memory %d: %w", row.MemoryID, err)
		}
		added++
		if added%snapshot.BatchSize == 0 {
			activity.RecordHeartbeat(ctx, added)
		}
		return nil
	})
	if streamErr != nil {
		err = fmt.Errorf("snapshotflow: stream enriched corpus at version %d: %w", in.EnrichmentVersion, streamErr)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}
	if skipped > 0 {
		activity.GetLogger(ctx).Warn("snapshotflow: skipped rows with missing/invalid embeddings",
			"version", version, "skipped", skipped, "added", added)
	}
	if added == 0 {
		err = fmt.Errorf("snapshotflow: build v%d added zero rows (an empty snapshot is a bug, not a result)", version)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}

	manifest, err := b.Close()
	if err != nil {
		err = fmt.Errorf("snapshotflow: close builder: %w", err)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}

	tarPath := filepath.Join(a.WorkDir, fmt.Sprintf("snapshot-v%d.tar.zst", version))
	if err := snapshot.Seal(dir, tarPath); err != nil {
		err = fmt.Errorf("snapshotflow: seal: %w", err)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}
	if err := os.RemoveAll(dir); err != nil {
		activity.GetLogger(ctx).Warn("snapshotflow: remove build dir after seal", "dir", dir, "error", err)
	}

	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		err = fmt.Errorf("snapshotflow: marshal manifest: %w", err)
		return BuildResult{}, a.recordFailure(ctx, version, in.EnrichmentVersion, err)
	}
	if err := store.InsertSnapshotEvent(ctx, a.DB, store.SnapshotEvent{
		SnapshotVersion:   version,
		EnrichmentVersion: in.EnrichmentVersion,
		Event:             "built",
		DocCount:          manifest.DocCount,
		Manifest:          manifestJSON,
	}); err != nil {
		return BuildResult{}, fmt.Errorf("snapshotflow: insert built event: %w", err)
	}

	return BuildResult{Version: version, TarPath: tarPath, Manifest: manifest}, nil
}

// Publish uploads the sealed tarball, records the 'published' event, and
// flips current.json strictly after the upload completes. A missing
// tarball (the build ran on another worker) fails non-retryably: retrying
// Publish alone can never recover it, only re-running BuildAndSeal can.
func (a *Activities) Publish(ctx context.Context, r BuildResult) (PublishResult, error) {
	if _, err := os.Stat(r.TarPath); err != nil {
		return PublishResult{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("snapshotflow: build artifact not found on this worker: %s", r.TarPath),
			SnapshotTarballMissingErrorType, err)
	}

	key, err := a.S3.Publish(ctx, r.Version, r.TarPath)
	if err != nil {
		return PublishResult{}, fmt.Errorf("snapshotflow: publish artifact: %w", err)
	}

	manifestJSON, err := json.Marshal(r.Manifest)
	if err != nil {
		return PublishResult{}, fmt.Errorf("snapshotflow: marshal manifest: %w", err)
	}
	if err := store.InsertSnapshotEvent(ctx, a.DB, store.SnapshotEvent{
		SnapshotVersion:   r.Version,
		EnrichmentVersion: r.Manifest.EnrichmentVersion,
		Event:             "published",
		S3Key:             key,
		DocCount:          r.Manifest.DocCount,
		Manifest:          manifestJSON,
	}); err != nil {
		return PublishResult{}, fmt.Errorf("snapshotflow: insert published event: %w", err)
	}

	if err := a.S3.FlipPointer(ctx, snapshot.Pointer{Version: r.Version, Key: key}); err != nil {
		return PublishResult{}, fmt.Errorf("snapshotflow: flip pointer: %w", err)
	}

	if err := os.Remove(r.TarPath); err != nil {
		activity.GetLogger(ctx).Warn("snapshotflow: remove tarball after publish", "path", r.TarPath, "error", err)
	}

	return PublishResult{Version: r.Version, Key: key, Manifest: r.Manifest}, nil
}
