package fsm

// OwnershipState is the state of a namespace_ownership row on a shard (PLAN.md section 2.1, N101, N125): `incoming |
// ready | active | frozen/{move,delete,restore} | moved_out`. The SQL type is (ownership_state, freeze_reason); here
// the two are one string, `frozen/<reason>`. OwnershipNone is the state before `create` and after a deleting edge.
type OwnershipState string

// The ownership states.
const (
	OwnershipNone          OwnershipState = ""
	OwnershipIncoming      OwnershipState = "incoming"
	OwnershipReady         OwnershipState = "ready"
	OwnershipActive        OwnershipState = "active"
	OwnershipFrozenMove    OwnershipState = "frozen/move"
	OwnershipFrozenDelete  OwnershipState = "frozen/delete"
	OwnershipFrozenRestore OwnershipState = "frozen/restore"
	OwnershipMovedOut      OwnershipState = "moved_out"
)

// OwnershipEdge names a row group of ownership_transitions.edge.
type OwnershipEdge string

// The ownership edges, exactly the `edge` values of ownership_transitions.
const (
	EdgeCreate         OwnershipEdge = "create"
	EdgePlanTarget     OwnershipEdge = "plan_target"
	EdgeStartMove      OwnershipEdge = "start_move"
	EdgeAbortMove      OwnershipEdge = "abort_move"
	EdgeFreezeMove     OwnershipEdge = "freeze_move"
	EdgeFreezeDelete   OwnershipEdge = "freeze_delete"
	EdgeFreezeRestore  OwnershipEdge = "freeze_restore"
	EdgeRestoreDelete  OwnershipEdge = "restore_delete"
	EdgeThawMove       OwnershipEdge = "thaw_move"
	EdgeReadyTarget    OwnershipEdge = "ready_target"
	EdgeUnreadyTarget  OwnershipEdge = "unready_target"
	EdgeCutoverC       OwnershipEdge = "cutover_c"
	EdgeActivateTarget OwnershipEdge = "activate_target"
	EdgeReturnMove     OwnershipEdge = "return_move"
	EdgeReturnAbort    OwnershipEdge = "return_abort"
	EdgeReconcileOut   OwnershipEdge = "reconcile_out"
	EdgeEpochBump      OwnershipEdge = "epoch_bump"
	EdgeRestoreDone    OwnershipEdge = "restore_done"
	EdgeRollbackTarget OwnershipEdge = "rollback_target"
	EdgePurgeDeleted   OwnershipEdge = "purge_deleted"
)

// The database roles that may take an ownership edge (the role_name column; N133e).
const (
	RoleApp   = "engram_app"
	RoleAdmin = "engram_admin"
	RoleMove  = "engram_move"
)

// OwnershipRows is the edge table of the ownership state machine, transcribed from ownership_transitions in
// docs/plan/sql/shard_schema.sql (TestFSM_OwnershipMatchesSQL pins the two together until M0.8 generates this file from
// the migration). The epoch, move and target effects of the SQL table are enforced by the database trigger and are not
// part of the state table.
var OwnershipRows = []Row[OwnershipState, OwnershipEdge]{
	{From: OwnershipNone, Edge: EdgeCreate, Role: RoleApp, To: OwnershipActive},
	{From: OwnershipNone, Edge: EdgeCreate, Role: RoleAdmin, To: OwnershipActive},
	{From: OwnershipNone, Edge: EdgePlanTarget, Role: RoleMove, To: OwnershipIncoming},
	{From: OwnershipActive, Edge: EdgeStartMove, Role: RoleMove, To: OwnershipActive},
	{From: OwnershipActive, Edge: EdgeAbortMove, Role: RoleMove, To: OwnershipActive},
	{From: OwnershipActive, Edge: EdgeAbortMove, Role: RoleAdmin, To: OwnershipActive},
	{From: OwnershipActive, Edge: EdgeFreezeMove, Role: RoleMove, To: OwnershipFrozenMove},
	{From: OwnershipActive, Edge: EdgeFreezeDelete, Role: RoleApp, To: OwnershipFrozenDelete},
	{From: OwnershipActive, Edge: EdgeFreezeRestore, Role: RoleAdmin, To: OwnershipFrozenRestore},
	{From: OwnershipIncoming, Edge: EdgeFreezeRestore, Role: RoleAdmin, To: OwnershipFrozenRestore},
	{From: OwnershipReady, Edge: EdgeFreezeRestore, Role: RoleAdmin, To: OwnershipFrozenRestore},
	{From: OwnershipFrozenRestore, Edge: EdgeRestoreDelete, Role: RoleAdmin, To: OwnershipFrozenDelete},
	{From: OwnershipFrozenMove, Edge: EdgeThawMove, Role: RoleMove, To: OwnershipActive},
	{From: OwnershipFrozenMove, Edge: EdgeThawMove, Role: RoleAdmin, To: OwnershipActive},
	{From: OwnershipIncoming, Edge: EdgeReadyTarget, Role: RoleMove, To: OwnershipReady},
	{From: OwnershipReady, Edge: EdgeUnreadyTarget, Role: RoleMove, To: OwnershipIncoming},
	{From: OwnershipFrozenMove, Edge: EdgeCutoverC, Role: RoleMove, To: OwnershipMovedOut},
	{From: OwnershipReady, Edge: EdgeActivateTarget, Role: RoleMove, To: OwnershipActive},
	{From: OwnershipMovedOut, Edge: EdgeReturnMove, Role: RoleMove, To: OwnershipIncoming},
	{From: OwnershipIncoming, Edge: EdgeReturnAbort, Role: RoleMove, To: OwnershipMovedOut},
	{From: OwnershipActive, Edge: EdgeReconcileOut, Role: RoleAdmin, To: OwnershipMovedOut},
	{From: OwnershipFrozenMove, Edge: EdgeReconcileOut, Role: RoleAdmin, To: OwnershipMovedOut},
	{From: OwnershipFrozenRestore, Edge: EdgeReconcileOut, Role: RoleAdmin, To: OwnershipMovedOut},
	{From: OwnershipActive, Edge: EdgeEpochBump, Role: RoleAdmin, To: OwnershipActive},
	{From: OwnershipFrozenRestore, Edge: EdgeRestoreDone, Role: RoleAdmin, To: OwnershipActive},
	{From: OwnershipIncoming, Edge: EdgeRollbackTarget, Role: RoleMove, To: OwnershipNone},
	{From: OwnershipIncoming, Edge: EdgeRollbackTarget, Role: RoleAdmin, To: OwnershipNone},
	{From: OwnershipReady, Edge: EdgeRollbackTarget, Role: RoleAdmin, To: OwnershipNone},
	{From: OwnershipFrozenRestore, Edge: EdgeRollbackTarget, Role: RoleAdmin, To: OwnershipNone},
	{From: OwnershipFrozenDelete, Edge: EdgePurgeDeleted, Role: RoleAdmin, To: OwnershipNone},
}

// Ownership is the ownership transition table.
var Ownership = New("ownership", OwnershipRows)
