// Package catalog owns the engram_catalog tables (D4) and the in-process Resolver (PLAN.md section 2.2.3; register D4,
// N125, N163, N171, N173, N179, N180, N184). Pattern: Repository for the control plane, split by capability (at most 5
// methods each); the Resolver is a read-through cache (LRU 100 k, TTL 60 s, negative 5 s, LISTEN catalog_changes;
// existing namespaces are served indefinitely while the catalog is unreachable, only misses fail UNAVAILABLE). The
// catalog has ONE asynchronous hot standby (N163): every promotion and every restore runs `engramctl catalog reconcile
// --from-shards` before the catalog serves writes. M0.1 declares the signatures only; the allowed imports are fsm and
// config (plus the leaves and the generated protos).
package catalog

import (
	"context"
	"errors"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/config"
	"github.com/gstamatakis95/engram/internal/fsm"
	"github.com/gstamatakis95/engram/internal/id"
)

// NamespaceState is the catalog state of a namespace (namespace_state).
type NamespaceState string

// The namespace states: creating | active | moving | frozen | restoring | deleting | deleted.
const (
	StateCreating  NamespaceState = "creating"
	StateActive    NamespaceState = "active"
	StateMoving    NamespaceState = "moving"
	StateFrozen    NamespaceState = "frozen"
	StateRestoring NamespaceState = "restoring"
	StateDeleting  NamespaceState = "deleting"
	StateDeleted   NamespaceState = "deleted"
)

// Entry is what the Resolver returns: placement, state and the vector space (fixed at creation, N111).
type Entry struct {
	Namespace      id.NamespaceID
	Tenant         id.TenantID
	Name           string
	Shard          id.ShardID
	Epoch          id.Epoch
	State          NamespaceState
	EmbeddingModel string
	EmbeddingDims  int
	Config         config.Layer
	TenantEntry    *TenantEntry
}

// EpochReason says why an epoch was bumped (move cutover, restore, failover).
type EpochReason string

// CreateParams, NamespaceQuery and NamespacePatch are the inputs of Namespaces and NamespaceAdmin.
type (
	CreateParams struct {
		Tenant         id.TenantID
		Name           string
		Shard          id.ShardID
		EmbeddingModel string
		EmbeddingDims  int
		Config         config.Layer
		RequestID      string
	}
	NamespaceQuery struct {
		PageSize  int32
		PageToken string
	}
	NamespacePatch struct {
		DisplayName *string
		Config      *config.Layer
		Mask        []string
	}
)

// Namespaces is the directory.
type Namespaces interface {
	Resolve(ctx context.Context, ns id.NamespaceID) (*Entry, error)
	ResolveByName(ctx context.Context, t id.TenantID, name string) (*Entry, error)
	// Create also inserts namespace_ownership(active, epoch 1) via the shard bootstrapper.
	Create(ctx context.Context, p CreateParams) (*Entry, error)
	// SetState moves creating | active | moving | frozen | restoring | deleting | deleted.
	SetState(ctx context.Context, ns id.NamespaceID, from, to NamespaceState) error
	BumpEpoch(ctx context.Context, ns id.NamespaceID, expected id.Epoch, why EpochReason) (id.Epoch, error)
}

// NamespaceAdmin backs NamespaceService.ListNamespaces and UpdateNamespace.
type NamespaceAdmin interface {
	List(ctx context.Context, t id.TenantID, q NamespaceQuery) ([]Entry, string, error)
	Update(ctx context.Context, ns id.NamespaceID, p NamespacePatch, etag string) (*Entry, error)
}

// MoveRow is a namespace_moves row; the DDL is the authority for the column list.
type MoveRow struct {
	Move          id.MoveID
	Namespace     id.NamespaceID
	Tenant        id.TenantID
	Source        id.ShardID
	Target        id.ShardID
	FromEpoch     id.Epoch
	ToEpoch       id.Epoch
	State         fsm.MoveState
	WEstSeconds   int
	WindowSeconds int
	WFinal        int64
	FrozenAt      *time.Time
	FreezeBy      *time.Time
	CommittedAt   *time.Time
	ActivatedAt   *time.Time
	CopyEndLSN    id.LSN
	CopyTimeline  int32
	CopySealedAt  *time.Time
	FinishedAt    *time.Time
}

// PlanParams, CommitParams, CutoverParams and MoveQuery are the inputs of the move ledger.
type (
	PlanParams struct {
		Window    time.Duration
		NotBefore time.Time
		CreatedBy string
		RequestID string
	}
	// CommitParams carries the verified shards, epochs, timeline and catalog namespace row of the CAS (a'') (N143).
	CommitParams struct {
		Move            id.MoveID
		Source, Target  id.ShardID
		Epoch           id.Epoch
		SourceSystemID  int64
		SourceTimeline  int32
		NamespaceState  NamespaceState
		ExpectedFreezer string
	}
	CutoverParams struct {
		Move   id.MoveID
		Target id.ShardID
		Epoch  id.Epoch
	}
	MoveQuery struct {
		Namespace id.NamespaceID
		States    []fsm.MoveState
		PageSize  int32
		PageToken string
	}
)

// ErrLost and ErrCommitted are the CAS outcomes of Commit and Rollback (N125): exactly one of them wins the row.
var (
	ErrLost      = errors.New("catalog: move row was won by a rollback or restore, or a verified value changed")
	ErrCommitted = errors.New("catalog: move is committed; rollback is no longer possible")
)

// Moves is the move ledger: every transition is a CAS on the current state (five methods; the rest is MoveStamps,
// Replication).
type Moves interface {
	// Plan records source_system_id, source_timeline_id, w_est_seconds, window_seconds; window rules and archive
	// health: PreconditionFailed{MOVE_WINDOW_REQUIRED | MOVE_WINDOW_TOO_SHORT | MOVE_TARGET_ARCHIVE_UNHEALTHY |
	// MOVE_TARGET_NOT_HA} (N173, N179, N190).
	Plan(ctx context.Context, ns id.NamespaceID, target id.ShardID, p PlanParams) (*MoveRow, error)
	// Advance stamps frozen_at, freeze_deadline = frozen_at + max(1.5 x w_est, w_est + 10 min) capped by
	// window_seconds, and w_final on planned -> frozen.
	Advance(ctx context.Context, m id.MoveID, from, to fsm.MoveState) error
	// Commit is (a'') the CAS cutover -> committed on the verified shards, epochs, timeline and catalog namespace row
	// (source, e, frozen) (N143): THE point of no return; ErrLost if a rollback or restore won the row (N125) or a
	// verified value changed.
	Commit(ctx context.Context, p CommitParams) error
	// Cutover is step (d) only: shard, epoch, state, NOTIFY; success also when already (target, e + 1).
	Cutover(ctx context.Context, p CutoverParams) error
	// Rollback is the CAS cutover -> rolled_back (or any earlier state); ErrCommitted if (a'') won.
	Rollback(ctx context.Context, m id.MoveID, reason string) error
}

// StampKind is a same-state timestamp of a move: ready | moved_out | activated | finished.
type StampKind string

// The stamp kinds (`activated` is the 24 h gate's input, N170, N180).
const (
	StampReady     StampKind = "ready"
	StampMovedOut  StampKind = "moved_out"
	StampActivated StampKind = "activated"
	StampFinished  StampKind = "finished"
)

// Outcome is the arbiter outcome a replication wait is recorded for.
type Outcome string

// The outcomes.
const (
	OutcomeCommitted  Outcome = "committed"
	OutcomeRolledBack Outcome = "rolled_back"
)

// MoveStamps: every non-derived `namespace_moves` column has exactly one writer (N184).
type MoveStamps interface {
	// Stamp is a same-state update of k in {ready, moved_out, activated, finished}.
	Stamp(ctx context.Context, m id.MoveID, k StampKind, at time.Time) error
	// RecordFloor writes copy_end_lsn, copy_end_timeline, copy_sealed_at: what the `cutover -> ...` CHECK reads (N179).
	RecordFloor(ctx context.Context, m id.MoveID, lsn id.LSN, timeline int32, sealedAt time.Time) error
	// RecordReplicated writes committed_replicated_at / rolled_back_replicated_at (N171).
	RecordReplicated(ctx context.Context, m id.MoveID, o Outcome, at time.Time) error
}

// Replication is catalog_replicated(lsn) and the commit LSN (N171(1)).
type Replication interface {
	Replicated(ctx context.Context, lsn id.LSN) (bool, error) // a streaming standby has replay_lsn >= lsn
	CommitLSN(ctx context.Context) (id.LSN, error)            // pg_current_wal_lsn() after COMMIT, same session
}

// AckAfterReplay decorates the client-acknowledged catalog writes (the 4.1.3 list, N171(3), N184): it runs write, then
// acks only after Replicated(CommitLSN); on timeout it returns UNAVAILABLE{RetryInfo 2 s}, the retry idempotent by
// request_id. Step-writes do not wait.
func AckAfterReplay(ctx context.Context, rep Replication, write func(ctx context.Context) error) error {
	panic("stub")
}

// OwnershipObservation is one shard ownership row as the reconcile reads it.
type OwnershipObservation struct {
	Shard       id.ShardID
	Namespace   id.NamespaceID
	State       string
	Epoch       id.Epoch
	TargetHint  id.ShardID
	TargetEpoch id.Epoch
	FloorLSN    id.LSN
}

// ReconcileReport lists what a reconcile changed and which shards it could not read (ReconcileIncomplete, N185).
type ReconcileReport struct {
	RoutingFixed []id.NamespaceID
	MovesFixed   []id.MoveID
	Skipped      []id.ShardID
}

// Reconciler is `engramctl catalog reconcile --from-shards`: the first step of every catalog promotion and restore
// (N163).
type Reconciler interface {
	// FromShards treats the shards' ownership rows as the source of truth (a moved_out source with a hint implies the
	// move is at least committed; no target row and no moved_out source implies rolled_back); it raises each epoch to
	// the max over its `active`/`frozen/*` rows (`frozen/restore` included; the higher epoch wins a torn snapshot) and
	// a `moved_out` row's target_epoch (N172, N180); a re-derived `committed` takes its floor from the target's
	// `floor_lsn`, stamps `reconciled_at`, writes routing only (N184); an unreadable shard is skipped
	// (`ReconcileIncomplete`, N185).
	FromShards(ctx context.Context, rows []OwnershipObservation) (*ReconcileReport, error)
}

// MoveReader backs MoveService.GetMove and ListMoves.
type MoveReader interface {
	Get(ctx context.Context, m id.MoveID) (*MoveRow, error)
	List(ctx context.Context, q MoveQuery) ([]MoveRow, string, error)
}

// ShardState is the catalog state of a shard.
type ShardState string

// The shard states (shard_state).
const (
	ShardProvisioning ShardState = "provisioning"
	ShardActive       ShardState = "active"
	ShardFull         ShardState = "full"
	ShardDraining     ShardState = "draining"
	ShardReadonly     ShardState = "readonly"
	ShardRetired      ShardState = "retired"
)

// Shard is a registered shard.
type Shard struct {
	ID       id.ShardID
	Cell     id.CellID
	State    ShardState
	Capacity int64
}

// ShardPatch is the input of Registry.UpdateShard.
type ShardPatch struct {
	Cell     *id.CellID
	Capacity *int64
	State    *ShardState
}

// Registry is the shard registry (admin surface, section 9).
type Registry interface {
	RegisterShard(ctx context.Context, s Shard) error
	GetShard(ctx context.Context, s id.ShardID) (*Shard, error)
	ListShards(ctx context.Context, cell id.CellID) ([]Shard, error)
	SetShardState(ctx context.Context, s id.ShardID, to ShardState) error
	UpdateShard(ctx context.Context, s id.ShardID, p ShardPatch) error
}

// TenantEntry is a tenants row.
type TenantEntry struct {
	Tenant      id.TenantID
	DisplayName string
	State       string // active | suspended | deleting | deleted
	Isolation   string // shared | dedicated
	Config      config.Layer
}

// TenantQuery and TenantPatch are the inputs of Tenants.
type (
	TenantQuery struct {
		PageSize  int32
		PageToken string
	}
	TenantPatch struct {
		DisplayName *string
		Config      *config.Layer
		Limits      *config.Quota
		Isolation   *string
		State       *string
	}
)

// Tenants is the tenant directory (admin surface).
type Tenants interface {
	Create(ctx context.Context, t TenantEntry) error
	Get(ctx context.Context, t id.TenantID) (*TenantEntry, error)
	List(ctx context.Context, q TenantQuery) ([]TenantEntry, string, error)
	Update(ctx context.Context, t id.TenantID, p TenantPatch) (*TenantEntry, error)
	// BeginDelete sets tenants.state = 'deleting' + delete_operation_id: the TENANT_DELETING read barrier (N122).
	BeginDelete(ctx context.Context, t id.TenantID, op id.OperationID) error
}

// TenantOps derives DELETE_TENANT from tenants.state / deleted_at (N127, N133d).
type TenantOps interface {
	Operation(ctx context.Context, t id.TenantID, op id.OperationID) (*memoryv1.Operation,
		[]*memoryv1.Operation /* per-namespace DELETE_NAMESPACE created so far */, error)
}

// ReplayFloor is catalog.shards.replay_floor: outside the restorable shard state (N134, N163(4)); the effective floor
// is min(this, the _control/restores/ markers).
type ReplayFloor interface {
	// Get is the catalog value; a replay takes min(this, intent.Log.RestoreFloor) so that a catalog that lost the
	// lowered floor cannot raise it.
	Get(ctx context.Context, s id.ShardID) (time.Time, error)
	// Lower is min(); there is NO method that raises it while intents are retained (35 days).
	Lower(ctx context.Context, s id.ShardID, restoreTarget time.Time) (time.Time, error)
}

// Resolver is the read-through cache over Namespaces.
type Resolver interface {
	// Resolve never blocks on the catalog with a fresh entry.
	Resolve(ctx context.Context, ns id.NamespaceID) (*Entry, error)
	// ResolveFresh bypasses the cache (after WrongShardOrEpoch).
	ResolveFresh(ctx context.Context, ns id.NamespaceID) (*Entry, error)
	Invalidate(ns id.NamespaceID)
	Run(ctx context.Context) error // LISTEN loop; full flush on reconnect
}

// ResolverOptions configure the Resolver: MaxEntries 100 000, TTL, NegativeTTL and StaleMax (0 = unbounded).
type ResolverOptions struct {
	MaxEntries  int
	TTL         time.Duration
	NegativeTTL time.Duration
	StaleMax    time.Duration
}
