package store

import (
	"context"
	"time"

	eventsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/events/v1"
	"github.com/gstamatakis95/engram/internal/fsm"
	"github.com/gstamatakis95/engram/internal/id"
)

// Ops groups operations and metering. Usage is Tx.Txn().Usage().
type Ops interface {
	Operations() OperationRepo
	Idempotency() IdempotencyRepo
	Ledger() LedgerRepo
	Outbox() OutboxWriter
}

// OutboxWriter is the writer half of the transactional outbox (D6). It lives in store so that store does not import
// outbox (N140); internal/outbox holds the relay, cursors and sinks and imports store. Append runs inside the same
// transaction as the state change and is the LAST statement of every write transaction.
type OutboxWriter interface {
	Append(ctx context.Context, ev *eventsv1.Event) (seq int64, err error)
	// AppendAll is ONE multi-row INSERT: still the last statement.
	AppendAll(ctx context.Context, evs []*eventsv1.Event) ([]int64, error)
}

// OperationRepo writes operation rows.
type OperationRepo interface {
	Insert(ctx context.Context, o OperationRow) error
	// Transition is monotone and fsm-checked.
	Transition(ctx context.Context, op id.OperationID, from, to fsm.OperationState, r Result) error
	SetProgress(ctx context.Context, op id.OperationID, p Progress) error // absolute values, once per wave (N69)
	Defer(ctx context.Context, op id.OperationID, until time.Time, d DeferredInfo) error
	// PendingWithoutWorkflow feeds the op-sweeper (N3).
	PendingWithoutWorkflow(ctx context.Context, olderThan time.Duration) ([]OperationRow, error)
}

// IdempotencyRepo keeps request_id keys for 24 h: the stored response returns on a replay.
type IdempotencyRepo interface {
	Begin(ctx context.Context, method, requestID string, hash [32]byte) (IdempotencyState, error)
	Commit(ctx context.Context, method, requestID string, op id.OperationID, response []byte) error
}

// LedgerRepo is the append-only ingest ledger: no update or delete exists, a trigger rejects both. Ledger rows are
// deleted only by an explicit document, namespace or tenant delete under the admin role (5.4.2), never by a replace.
type LedgerRepo interface {
	Append(ctx context.Context, e LedgerEntry) error
	Get(ctx context.Context, l id.LedgerID) (*LedgerEntry, error)
}
