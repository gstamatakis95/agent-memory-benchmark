// Package intent is the delete-intent log (PLAN.md section 2.2.28; register N122, N123, N134, N143, N150, N159, N162).
// Pattern: append-only log in the blob store: an acknowledged delete exists in two places before the ack, the committed
// marker and its intent object. M0.1 declares the signatures only.
package intent

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
)

// Key is _control/deletes/{tenant}/{ns}/{deleted_at}-{operation_id}.json.
type Key string

// Kind is the intent kind: document | namespace | tenant | invalidate | restore.
type Kind string

// The intent kinds.
const (
	KindDocument   Kind = "document"
	KindNamespace  Kind = "namespace"
	KindTenant     Kind = "tenant"
	KindInvalidate Kind = "invalidate"
	KindRestore    Kind = "restore"
)

// Intent records the marker's EXACT effect, taken from the store.DeletionRecord that the marker transaction returned.
type Intent struct {
	Kind Kind
	// Subject is typed {Class, Key} (N162): the memory subject is (document_id, content_hash), so a fact and its twins
	// share one chain.
	Subject   id.Subject
	Tenant    id.TenantID
	Namespace id.NamespaceID
	Operation id.OperationID
	// Prev is the subject's last deletion_log entry read under the document lock: the chain predecessor (zero = first).
	Prev      id.OperationID
	DeletedAt time.Time
	// Epoch is the namespace epoch the marker committed under (N143): replay stores THIS recorded epoch (never the
	// shard's current one; applied_epoch is informational) and skips an intent older than an applied entry of its
	// subject (N150).
	Epoch id.Epoch
	// UpToVersion is for the document kind: the tombstone's up_to_version, applied verbatim by the replay.
	UpToVersion id.DocVersion
	// MemoryIDs are for the invalidate and restore kinds: the resolved set the marker hid (every fact of the subject)
	// or removed (the stamped set).
	MemoryIDs []id.FactID
}

// Log is the intent object store.
type Log interface {
	// Put is put-if-absent under the marker's own name and content (If-None-Match: *). The attempt that committed the
	// marker calls it after the commit and before the ack; a duplicate attempt that finds the subject already deleted
	// calls it with the marker it observed, so an ack always implies an intent and a duplicate never writes a second
	// object. After Put the handler re-reads the marker and acks only if it is still present, else UNAVAILABLE (N143,
	// TestIntent_AckRereadsMarker).
	Put(ctx context.Context, in Intent) (Key, error)
	// List returns intents in name order; replay order is Order, not name order.
	List(ctx context.Context, ns id.NamespaceID, since time.Time) ([]Intent, error)
	Trim(ctx context.Context, olderThan time.Duration) (int, error) // 35 days
	// PutRestore writes _control/restores/{shard}/{restore_target}.json, before a replay starts (N163).
	PutRestore(ctx context.Context, shard id.ShardID, target, floor time.Time) error
	// RestoreFloor is the min over the shard's restore markers; the replay floor is min(catalog.ReplayFloor.Get, this).
	RestoreFloor(ctx context.Context, shard id.ShardID) (time.Time, error)
}

// HelpPrevious (N159) runs under the subject lock, before the caller's own marker transaction: if the subject's latest
// deletion_log entry (either kind; rebuilt from the row's effect, epoch, operation_id, prev_operation_id and
// deleted_at) has no intent object, it puts it (put-if-absent under that entry's own name). It reports whether it had
// to put.
func HelpPrevious(ctx context.Context, l Log, latest Intent) (put bool, err error) {
	panic("stub")
}

// Order returns the replay order: per subject in Prev-chain order (a broken chain starts at its oldest present member),
// subjects in any order. It reads no clock, so no clock-sync bound is needed (C-16, P-13).
func Order(in []Intent) []Intent {
	panic("stub")
}
