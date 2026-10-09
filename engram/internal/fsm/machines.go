package fsm

// OperationState is the state of an operation (N35, section 4): exactly the proto's enum names.
type OperationState string

// The operation states. A FAILED operation is final: a retry is a new operation, so there is no FAILED -> PENDING edge
// (N139).
const (
	OpPending   OperationState = "PENDING"
	OpRunning   OperationState = "RUNNING"
	OpDeferred  OperationState = "DEFERRED"
	OpSucceeded OperationState = "SUCCEEDED"
	OpFailed    OperationState = "FAILED"
	OpCancelled OperationState = "CANCELLED"
)

// IsTerminal reports whether no edge leaves the state.
func (s OperationState) IsTerminal() bool {
	return s == OpSucceeded || s == OpFailed || s == OpCancelled
}

// OperationEdge names an operation transition.
type OperationEdge string

// The operation edges.
const (
	OpEdgeStart   OperationEdge = "start"   // PENDING -> RUNNING
	OpEdgeDefer   OperationEdge = "defer"   // RUNNING -> DEFERRED (quota.Reserve refused, resume_at set)
	OpEdgeResume  OperationEdge = "resume"  // DEFERRED -> RUNNING
	OpEdgeSucceed OperationEdge = "succeed" // RUNNING -> SUCCEEDED
	OpEdgeFail    OperationEdge = "fail"    // PENDING | RUNNING | DEFERRED -> FAILED
	OpEdgeCancel  OperationEdge = "cancel"  // PENDING | RUNNING | DEFERRED -> CANCELLED
)

// Operation is the operation table: `PENDING -> RUNNING <-> DEFERRED -> SUCCEEDED | FAILED | CANCELLED`, monotone
// (section 5.1: the guard `WHERE state NOT IN (terminal)` makes a late activity harmless). Any role may take an edge.
// Reading of the arrow chain: a deferred operation resumes through RUNNING before it can succeed; a PENDING one can be
// cancelled or failed (op-sweeper, delete) without ever running.
var Operation = New("operation", []Row[OperationState, OperationEdge]{
	{From: OpPending, Edge: OpEdgeStart, To: OpRunning},
	{From: OpPending, Edge: OpEdgeFail, To: OpFailed},
	{From: OpPending, Edge: OpEdgeCancel, To: OpCancelled},
	{From: OpRunning, Edge: OpEdgeDefer, To: OpDeferred},
	{From: OpRunning, Edge: OpEdgeSucceed, To: OpSucceeded},
	{From: OpRunning, Edge: OpEdgeFail, To: OpFailed},
	{From: OpRunning, Edge: OpEdgeCancel, To: OpCancelled},
	{From: OpDeferred, Edge: OpEdgeResume, To: OpRunning},
	{From: OpDeferred, Edge: OpEdgeFail, To: OpFailed},
	{From: OpDeferred, Edge: OpEdgeCancel, To: OpCancelled},
})

// MoveState is the state of a namespace move (catalog namespace_moves.state; D5, N125, N160, N183).
type MoveState string

// The move states. `committed` is the catalog CAS (a”), the point of no return (N125). The plan's section 2.1 list
// omits `lost`, which the DDL and N183 add (a move-in lost to a restore).
const (
	MovePlanned    MoveState = "planned"
	MoveFrozen     MoveState = "frozen"
	MoveCopied     MoveState = "copied"
	MoveCutover    MoveState = "cutover"
	MoveCommitted  MoveState = "committed"
	MoveCleaning   MoveState = "cleaning"
	MoveDone       MoveState = "done"
	MoveRolledBack MoveState = "rolled_back"
	MoveLost       MoveState = "lost"
)

// MoveEdge names a move transition.
type MoveEdge string

// The move edges.
const (
	MoveEdgeFreeze   MoveEdge = "freeze"   // planned -> frozen
	MoveEdgeCopy     MoveEdge = "copy"     // frozen -> copied
	MoveEdgeCutover  MoveEdge = "cutover"  // copied -> cutover
	MoveEdgeCommit   MoveEdge = "commit"   // cutover -> committed: the catalog CAS (a''), the point of no return
	MoveEdgeCleanup  MoveEdge = "cleanup"  // committed -> cleaning (the 24 h gate is a trigger, not part of the table)
	MoveEdgeFinish   MoveEdge = "finish"   // cleaning -> done
	MoveEdgeRollback MoveEdge = "rollback" // planned | frozen | copied | cutover -> rolled_back; none after commit
	MoveEdgeLose     MoveEdge = "lose"     // committed | cleaning -> lost (N183)
)

// Move is the move table, identical to the pairs accepted by catalog_check_move_transition in
// docs/plan/sql/catalog_schema.sql (TestFSM_MoveMatchesSQL pins them together). Note the absence of any edge from
// committed to rolled_back: after the CAS neither a rollback nor a restore reconcile can win.
var Move = New("move", []Row[MoveState, MoveEdge]{
	{From: MovePlanned, Edge: MoveEdgeFreeze, To: MoveFrozen},
	{From: MoveFrozen, Edge: MoveEdgeCopy, To: MoveCopied},
	{From: MoveCopied, Edge: MoveEdgeCutover, To: MoveCutover},
	{From: MoveCutover, Edge: MoveEdgeCommit, To: MoveCommitted},
	{From: MoveCommitted, Edge: MoveEdgeCleanup, To: MoveCleaning},
	{From: MoveCleaning, Edge: MoveEdgeFinish, To: MoveDone},
	{From: MovePlanned, Edge: MoveEdgeRollback, To: MoveRolledBack},
	{From: MoveFrozen, Edge: MoveEdgeRollback, To: MoveRolledBack},
	{From: MoveCopied, Edge: MoveEdgeRollback, To: MoveRolledBack},
	{From: MoveCutover, Edge: MoveEdgeRollback, To: MoveRolledBack},
	{From: MoveCommitted, Edge: MoveEdgeLose, To: MoveLost},
	{From: MoveCleaning, Edge: MoveEdgeLose, To: MoveLost},
})
