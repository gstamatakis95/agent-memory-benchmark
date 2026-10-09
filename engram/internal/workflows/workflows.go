// Package workflows holds all Temporal workflow definitions and the activity INTERFACES they call, task-queue naming,
// ContinueAsNew rules, retry policies and the API's client wrapper (PLAN.md section 2.2.17; register D11, N59, N69,
// N97, N99, N100, N136, N140, N157). Concrete activities are structs in cmd/engram-worker that wire service packages
// into these interfaces; this package imports no concrete service package. Pattern: Pipeline (the workflow body is the
// ordered list of activity groups) and Saga for the move only; the expunge and tenant delete are forward-only
// workflows. Each activity is idempotent by key and fenced by the epoch, so a restart resumes instead of repeating.
// M0.1 declares the signatures only.
package workflows

import (
	"context"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	workflowv1 "github.com/gstamatakis95/engram/gen/go/engram/private/workflow/v1"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/workflows/codec"
)

// MaxInlineResultBytes is the largest activity result that travels inline; anything above goes by blob key (N59).
const MaxInlineResultBytes = 4 << 10

// TaskQueue is "shard-{id}".
func TaskQueue(s id.ShardID) string { panic("stub") }

// OpID is "ns/{ns}/op/{op}": retain, export, namespace delete (refresh is singleton-backed: PageRefreshID).
func OpID(ns id.NamespaceID, op id.OperationID) id.WorkflowID { panic("stub") }

// ExpungeID is "ns/{ns}/expunge": the singleton behind DELETE_DOCUMENT (N136).
func ExpungeID(ns id.NamespaceID) id.WorkflowID { panic("stub") }

// ConsolidateID is "ns/{ns}/consolidate".
func ConsolidateID(ns id.NamespaceID) id.WorkflowID { panic("stub") }

// PageRefreshID is "ns/{ns}/page/{page}": scheduled, delete-driven and manual refreshes (the operation_id rides in the
// nudge).
func PageRefreshID(ns id.NamespaceID, p id.PageID) id.WorkflowID { panic("stub") }

// MoveWorkflowID is "move/{ns}/{epoch}" on shard-{target}.
func MoveWorkflowID(ns id.NamespaceID, e id.Epoch) id.WorkflowID { panic("stub") }

// RetainDocument is the retain pipeline: Prepare once, ChunkPipeline per chunk (at most 32 in flight, a workflow-side
// semaphore), then Finish. Inputs and results are protos in engram.internal.workflow.v1; every input carries
// schema_version 2.
func RetainDocument(ctx workflow.Context,
	in *workflowv1.RetainDocumentInput) (*workflowv1.RetainDocumentResult, error) {
	panic("stub")
}

// Consolidate is long-lived: Nudge signal; debounce 30 s; ContinueAsNew every round.
func Consolidate(ctx workflow.Context, in *workflowv1.ConsolidateInput) error { panic("stub") }

// Expunge also targets NAMESPACE (5.4).
func Expunge(ctx workflow.Context, in *workflowv1.ExpungeInput) (*workflowv1.ExpungeResult, error) {
	panic("stub")
}

// PageRefresh is the per-page singleton.
func PageRefresh(ctx workflow.Context, in *workflowv1.RefreshPageInput) (*workflowv1.RefreshPageResult, error) {
	panic("stub")
}

// Move is the saga.
func Move(ctx workflow.Context, in *workflowv1.MoveInput) (*workflowv1.MoveResult, error) {
	panic("stub")
}

// ExportSnapshot, TenantDelete (fences every namespace, then stamps acknowledged_at, N182), RetainBackfill,
// ReembedNamespace and the schedules (SweeperInput) follow the same shape.
func ExportSnapshot(ctx workflow.Context, in *workflowv1.ExportInput) (*workflowv1.ExportResult, error) {
	panic("stub")
}

// TenantDelete fences every namespace of the tenant, then stamps acknowledged_at (N182).
func TenantDelete(ctx workflow.Context, in *workflowv1.TenantDeleteInput) error { panic("stub") }

// RetainBackfill is started by engramctl's own client.
func RetainBackfill(ctx workflow.Context, in *workflowv1.RetainBackfillInput) error { panic("stub") }

// ReembedNamespace is started by engramctl's own client.
func ReembedNamespace(ctx workflow.Context, in *workflowv1.ReembedNamespaceInput) error {
	panic("stub")
}

// Sweeper runs the shard-wide schedules.
func Sweeper(ctx workflow.Context, in *workflowv1.SweeperInput) error { panic("stub") }

// Prepare is the first retain activity group.
type Prepare interface {
	// LoadItem reads the APPEND base under the documents row lock (N56) and writes the single owner-keyed
	// ver/{document_id}/v{n} body blob (N104).
	LoadItem(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.LoadItemResult, error)
	Chunk(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.ChunkPlan, error)
	SummarizeDocument(ctx context.Context,
		in *workflowv1.RetainDocumentInput) (*workflowv1.SummarizeDocumentResult, error)
	// PlanChunks classifies each chunk: member | live | tombstoned | stale_extraction | absent.
	PlanChunks(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.ChunkPlan, error)
	// MarkProgress is written once per wave (N69).
	MarkProgress(ctx context.Context, in *workflowv1.MarkProgressInput) error
}

// ChunkPipeline is the per-chunk activity group.
type ChunkPipeline interface {
	ExtractChunk(ctx context.Context, sc *workflowv1.WorkflowScope,
		w *workflowv1.ChunkWork) (*workflowv1.ExtractChunkResult, error)
	EmbedChunk(ctx context.Context, sc *workflowv1.WorkflowScope, w *workflowv1.ChunkWork,
		x *workflowv1.ExtractChunkResult) (*workflowv1.EmbedChunkResult, error)
	ResolveEntities(ctx context.Context, sc *workflowv1.WorkflowScope,
		x *workflowv1.ExtractChunkResult) (*workflowv1.ResolveEntitiesResult, error)
	BuildLinks(ctx context.Context, sc *workflowv1.WorkflowScope, x *workflowv1.ExtractChunkResult,
		e *workflowv1.EmbedChunkResult, r *workflowv1.ResolveEntitiesResult) (*workflowv1.BuildLinksResult, error)
	// CommitChunk inserts only; DocumentBusy is retryable; InputBlobMissing re-runs extract+embed at most twice (N100).
	CommitChunk(ctx context.Context, in *workflowv1.CommitChunkInput) (*workflowv1.CommitChunkResult, error)
}

// Finish is the last retain activity group.
type Finish interface {
	// FinalizeVersion inserts chunk tombstones and fact_hidden(reextract), flags the affected observations stale
	// (evidence rows are kept, derived versions stay visible, N135); superseded_by (N49).
	FinalizeVersion(ctx context.Context, in *workflowv1.FinalizeVersionInput) (*workflowv1.FinalizeVersionResult, error)
	// ReembedChunk inserts one chunk_vectors row (N60, N111).
	ReembedChunk(ctx context.Context, in *workflowv1.ChunkWork) error
	MarkOperation(ctx context.Context, in *workflowv1.MarkOperationInput) error
}

// ConsolidateSteps are the consolidation activities. The quota gate is a method of quota.Reserver, called inside the
// LLM activities, not a separate activity (N130). ApplyBatch is one of the two derivation transactions that obey the
// commit rule of N120 (the other is the page refresh's CommitPageVersion).
type ConsolidateSteps interface {
	RouteBatch(ctx context.Context, in *workflowv1.RouteBatchInput) (*workflowv1.RouteBatchResult, error)
	WriteObservation(ctx context.Context,
		in *workflowv1.WriteObservationInput) (*workflowv1.WriteObservationResult, error)
	StoreProposal(ctx context.Context, in *workflowv1.StoreProposalInput) (*workflowv1.StoreProposalResult, error)
	ApplyBatch(ctx context.Context, in *workflowv1.ApplyBatchInput) (*workflowv1.ApplyBatchResult, error)
	// StampFailed writes INSERT fact_consolidation(..., note = 'failed') for a fact whose batch failed alone.
	StampFailed(ctx context.Context, sc *workflowv1.WorkflowScope, fact id.FactID) error
}

// Starter is the Temporal client, split by capability (N140): engram-api and the delete handlers start workflows
// through it. AlreadyStarted is nil.
type Starter interface {
	StartRetain(ctx context.Context, in *workflowv1.RetainDocumentInput) error // ns/{ns}/op/{op}
	StartExport(ctx context.Context, in *workflowv1.ExportInput) error         // ExportSnapshot at ns/{ns}/op/{op}
	// StartNamespaceDelete starts Expunge{NAMESPACE} at ns/{ns}/op/{op}.
	StartNamespaceDelete(ctx context.Context, in *workflowv1.ExpungeInput) error
	// StartTenantDelete starts tenant/{tenant}/delete on the cell-wide control queue.
	StartTenantDelete(ctx context.Context, in *workflowv1.TenantDeleteInput) error
}

// Signaller is SignalWithStart of the per-namespace singletons. A manual page refresh is SignalPageRefresh with the
// operation_id in the nudge. The move is started by move.Orchestrator.Start (api.Deps.Mover).
type Signaller interface {
	SignalConsolidate(ctx context.Context, t *workflowv1.ConsolidateTrigger) error // ns/{ns}/consolidate
	SignalExpunge(ctx context.Context, in *workflowv1.ExpungeInput) error          // ns/{ns}/expunge
	SignalPageRefresh(ctx context.Context, in *workflowv1.RefreshPageInput) error  // ns/{ns}/page/{page}
}

// Waiter cancels and awaits workflows.
type Waiter interface {
	// Cancel is refused for DELETE_* operations before it gets here (N136).
	Cancel(ctx context.Context, wf id.WorkflowID) error
	// WaitResult is the WaitOperation long-poll (N70).
	WaitResult(ctx context.Context, wf id.WorkflowID, timeout time.Duration) (done bool, err error)
}

// DataConverter is the AES-256-GCM codec, per-namespace data key wrapped by the shard key (N59, N99). It is only the
// data half: error messages and stack traces travel through the failure converter, so clients and workers should
// install Converters, which returns both.
func DataConverter(keys KeyProvider) converter.DataConverter { return codec.NewDataConverter(keys) }

// Converters returns the encrypting data converter and the failure converter that seals failures under the same
// namespace key; set both on every Temporal client and worker (Converters.Apply). Decoding is strict unless
// codec.Options.AllowPlaintext is set, which only dev tools do.
func Converters(keys KeyProvider, o codec.Options) codec.Converters { return codec.New(keys, o) }

// KeyProvider supplies the per-namespace data keys: DataKey(ctx, ns, keyID), CurrentKeyID(ns) and Shred(ctx, ns), the
// shredding step of a namespace or tenant delete. It lives in internal/workflows/codec, which this package imports.
type KeyProvider = codec.KeyProvider
