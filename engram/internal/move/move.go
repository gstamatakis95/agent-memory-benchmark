// Package move is the namespace move saga (PLAN.md section 2.2.19, decision D5; register N93, N97, N123, N125, N143,
// N147, N160, N161, N169 to N173, N179, N180, N184). The protocol runs as Temporal workflow `move/{ns}/{epoch}` on
// `shard-{target}` (N2): freeze, then copy every table from the static source, verify by equality, build the indexes
// and seal the copy under the freeze, then cut over with a `ready` state and a catalog CAS. Pattern: Saga with a state
// machine: every step is idempotent, every step before the catalog CAS (a”) has a compensation, and (a”) is the point
// of no return. It is the only code path that holds two shard handles. M0.1 declares the signatures only.
package move

import (
	"context"
	"time"

	workflowv1 "github.com/gstamatakis95/engram/gen/go/engram/private/workflow/v1"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// Fence is the verified identity of a move that every activity re-checks first.
type Fence struct {
	Ns     id.NamespaceID
	Tenant id.TenantID
	Source id.ShardID
	Target id.ShardID
	Epoch  id.Epoch // e; the target holds e+1
	Move   id.MoveID
	// SrcRow and DstRow are the exact source and target namespace_ownership rows the activity VERIFIED (state,
	// freeze_reason, epoch, move_id, target hint).
	SrcRow, DstRow store.OwnershipRow
}

// PlanResult, CopyResult, DrainResult, CleanupResult, Ref and Status are the step results.
type (
	PlanResult struct {
		SourceSystemID int64
		Timeline       int32
		WEst           time.Duration
	}
	CopyResult struct {
		Tables int
		Rows   int64
		WFinal int64
	}
	DrainResult struct {
		Terminated []id.WorkflowID
	}
	CleanupResult struct {
		Move  id.MoveID
		State string
	}
	// Ref is the answer of Start; with EstimateOnly only WindowEstimate is set.
	Ref struct {
		Move           id.MoveID
		WindowEstimate time.Duration
	}
	// Status is the answer of Status.
	Status struct {
		Move  id.MoveID
		State string
		Step  string
	}
)

// StartOptions carries Window, FreezeNotBefore, EstimateOnly, DrainWait, CopyRangeRows, CopyStreams and the cutover
// retry.
type StartOptions struct {
	Window          time.Duration
	FreezeNotBefore time.Time
	EstimateOnly    bool
	DrainWait       time.Duration
	CopyRangeRows   int
	CopyStreams     int
	CutoverRetry    time.Duration
	RequestID       string
}

// Copier copies and verifies. The source is static from Freeze to thaw or cleanup, so no catch-up or merge exists: the
// target's sequence is advanced past the source's final value W_final ONCE, at the start of FrozenCopy (N147, N160).
// Every activity first re-checks the fence against the catalog row, both ownership rows and the source session's
// system_identifier/timeline (MoveFenced). Every STEP, including the catalog CAS, is a compare-and-set on the exact
// rows it verified (N143): zero rows = MoveFenced (TestMove_CatalogCAS).
type Copier interface {
	// Plan runs the plan_target / start_move edges; records system_id, timeline, W_est; pauses expunge; blob pre-warm
	// (the only pre-freeze work).
	Plan(ctx context.Context, f Fence) (*PlanResult, error)
	// FrozenCopy: engram_seq_advance(W_final) once, then classes 1 and 2 and token_usage_events once by PK ranges of at
	// most 100 k rows, 4 streams, WAL-paced; resumable at (table, last_key).
	FrozenCopy(ctx context.Context, f Fence) (*CopyResult, error)
	// VerifyFrozen checks per PK range (at most 100 k rows, bit_xor combined in Go, each statement inside the 30 s role
	// timeout) and per-table counts, VerifyFK, blob existence; one re-copy of mismatching tables, then
	// MoveVerifyFailed.
	VerifyFrozen(ctx context.Context, f Fence) (*workflowv1.VerifyFrozenReport, error)
}

// Readier builds indexes, seals the copy and awaits the outbox consumers.
type Readier interface {
	// BuildIndexes runs under the freeze (maintenance_work_mem = min(2.4 KB x v, 24 GB), shm 32g): it requests the
	// target's partial indexes for every table and waits for engram_move_indexes_valid; a failed build retries once.
	BuildIndexes(ctx context.Context, f Fence) (*workflowv1.BuildIndexesReport, error)
	// SealCopy runs pg_switch_wal() -> copy_end_lsn; pgbackrest check; store.Replication.Replayed (a standby always
	// exists, N190); catalog.MoveStamps.RecordFloor (N179).
	SealCopy(ctx context.Context, f Fence) (*workflowv1.SealCopyResult, error)
	// AwaitConsumers waits for every source outbox consumer cursor to pass the namespace's final max(seq), bound 120 s.
	AwaitConsumers(ctx context.Context, f Fence) error
}

// Freezer freezes and drains the source.
type Freezer interface {
	// Freeze takes the exclusive fence, one 35 s attempt, the freeze_move edge; it returns the freeze deadline the
	// workflow arms as a timer until (a'').
	Freeze(ctx context.Context, f Fence) (deadline time.Time, err error)
	// Drain reads the in-flight set from the static source operations rows, backfill parents included (N97, C-20).
	Drain(ctx context.Context, f Fence) (*DrainResult, error)
}

// Cutover is the cut up to the point of no return.
type Cutover interface {
	Begin(ctx context.Context, f Fence) error // (a) intent only
	// ReadyTarget is (b'): ready_target(floor_lsn); the ownership trigger refuses `ready` unless
	// engram_move_indexes_valid, last_value > w_final and a floor (N147, N160, N179).
	ReadyTarget(ctx context.Context, f Fence) error
	// Commit is (a''): catalog Moves.Commit, the CAS cutover -> committed on the VERIFIED source/target rows, THE point
	// of no return; then it waits for Replicated(CommitLSN) (10 s per attempt) and re-reads the row (N171).
	Commit(ctx context.Context, f Fence) error
}

// Handover runs only after Commit returned; (c) is fenced on the source row and the replicated `committed` only
// (N169(4)).
type Handover interface {
	Source(ctx context.Context, f Fence) error // (c) cutover_c: frozen/move -> moved_out + target hint
	// ActivateTarget is (b''): activate_target, unconditional while the target row reads `ready` and the move
	// `committed`; retried indefinitely; stamps activated_at, clears move_id (N180).
	ActivateTarget(ctx context.Context, f Fence) error
	// Catalog is (d): shard, epoch, state, NOTIFY, retried indefinitely; success is `shard = target and epoch >= e_t +
	// 1`; the end test reads the target row (N180).
	Catalog(ctx context.Context, f Fence) error
}

// Closer restarts, cleans up and rolls back.
type Closer interface {
	// Restart restarts ns/{ns}/op/{op} on shard-{target}, TERMINATE_IF_RUNNING, memo epoch e+1; singleton-backed kinds
	// by SignalWithStart.
	Restart(ctx context.Context, f Fence, d *DrainResult) error
	// Cleanup drives the batches once the row reads `cleaning` (CleanupMove, N170): DROP INDEX by name,
	// engram_cleanup_namespace; records `done` via Moves.Advance after one more zero-row call (N184); blob prefix at
	// finished_at + 28 d; the moved_out row stays.
	Cleanup(ctx context.Context, f Fence) error
	// Rollback is only before (a''): it wins the cutover -> rolled_back CAS, waits for it to replicate, then thaws; run
	// at the freeze deadline (MoveWindowExceeded); after (a'') the page MoveFrozenPastDeadline instead (N171).
	Rollback(ctx context.Context, f Fence, why string) error
}

// Orchestrator is MoveService (api imports move, N157).
type Orchestrator interface {
	// Start plans the move; o.EstimateOnly returns Ref.WindowEstimate without planning.
	Start(ctx context.Context, ns id.NamespaceID, target id.ShardID, o StartOptions) (*Ref, error)
	Status(ctx context.Context, m id.MoveID) (*Status, error)
	Abort(ctx context.Context, m id.MoveID) error // rollback before (a''), else an error
	// Cleanup is MoveService.CleanupMove: it takes `committed -> cleaning` only (the trigger refuses before
	// activated_at + 24 h); Closer.Cleanup does the rest (N167, N170, N184).
	Cleanup(ctx context.Context, m id.MoveID) (*CleanupResult, error)
}
