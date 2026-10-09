// Package ledger is the append-only ingest ledger (PLAN.md section 2.2.24; register N104). There is no update or delete
// method and a trigger rejects both. Inline body up to 64 KiB, else the owner-keyed blob ledger/{ledger_id}, which dies
// with its row. Ledger rows are deleted only by an explicit document, namespace or tenant delete under the admin role
// (the expunge, 5.4.2), never by a replace or append retire. The only implementation is the append-only table, so there
// is no swappable seam (section 2.3). M0.1 declares the signatures only.
package ledger

import (
	"context"

	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// Entry is one ledger row.
type Entry = store.LedgerEntry

// MaxInlineBytes is the largest body kept inline in the ledger row.
const MaxInlineBytes = 64 << 10

// Writer appends to the ledger inside the caller's write transaction.
type Writer interface {
	Append(ctx context.Context, tx store.Tx, e Entry) error
}

// Reader reads the ledger.
type Reader interface {
	Get(ctx context.Context, tx store.ReadTx, l id.LedgerID) (*Entry, error)
	ListByDocument(ctx context.Context, tx store.ReadTx, doc id.DocumentID) ([]Entry, error)
}
