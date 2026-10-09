// Package store is the per-shard Postgres layer of PLAN.md section 2.2.7: pools, the fenced transaction and one
// repository per table family (register N82, N113, N140, N157). It is the only package besides internal/index that
// contains SQL. M0.1 declares the interfaces and value types; implementations arrive with M0.8 and M1.x.
package store

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/fsm"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/txn"
)

// Store is the only entry point to a shard. Reads take no lock; writes run the fence prelude (N82). Five methods.
type Store interface {
	// InNamespace runs fn in one WRITE transaction: SET LOCAL engram.namespace_id/tenant_id/epoch and statement_timeout
	// (30 s = idle_in_transaction_session_timeout, A-F1), engram_try_ns_fence (shared try-lock, never waits: a refusal
	// is NamespaceFrozen{200 ms}), then a plain SELECT state, epoch FROM namespace_ownership; abort with
	// WrongShardOrEpoch unless state = 'active' AND epoch = ns.Epoch.
	InNamespace(ctx context.Context, ns id.Scope, fn func(tx Tx) error) error
	// ReadNamespace opens a ReadSession for a request: no transaction yet, no lock. Each Session.Tx is one short READ
	// transaction (below); a recall is N of them, not one unit of work (N140).
	ReadNamespace(ctx context.Context, ns id.Scope, fn func(s ReadSession) error) error
	// Admin runs fn as engram_admin (BYPASSRLS, 30 s timeouts): sweepers that enumerate namespaces, the expunge's
	// purge, the restore replay.
	Admin(ctx context.Context, fn func(tx AdminTx) error) error
	// Direct returns a dedicated, non-pooled connection as engram_relay for the outbox relay's advisory-lock election
	// and ordered outbox reads (N4, N15); outbox.Relay takes a Store and calls this.
	Direct(ctx context.Context) (DirectConn, error)
	Close() error
}

// DirectConn is the relay's dedicated connection.
type DirectConn interface {
	// TryLock is the session-level pg_try_advisory_lock; the lock is lost when the connection drops.
	TryLock(ctx context.Context, key int64) (bool, error)
	// ReadOutbox reads in seq order, every namespace.
	ReadOutbox(ctx context.Context, after int64, limit int) ([]OutboxRow, error)
	// Cursors is outbox_cursors: guarded UPDATE, gap watchlist.
	Cursors() CursorRepo
	Close() error
}

// SubjectLocker (N150, N159, N162) is the second interface the Store implementation satisfies (Store itself stays at
// five methods). Lock takes the SESSION-level subject lock pg_advisory_lock(engram_subject_lock_key(ns, subject)) on a
// dedicated direct connection of the `engram_subject` alias (2 per API process per shard, a role holding only
// pg_advisory_lock on the subject key space). The lease is taken with pg_try_advisory_lock polled every 50 ms for at
// most 3 s, so one blocked lease never holds a slot; pool exhaustion is an immediate retryable UNAVAILABLE. The marker
// writer holds the lease from before its marker transaction until the intent put and the marker re-read are done;
// Release is called on every exit path and a crashed handler's lease dies with its connection (tcp_user_timeout 30 s).
type SubjectLocker interface {
	Lock(ctx context.Context, ns id.NamespaceID, subject id.Subject) (SubjectLease, error)
}

// SubjectLease is a held subject lock.
type SubjectLease interface {
	// Latest is the subject's last deletion_log entry of either kind, read while the lease is held (help-previous
	// input).
	Latest(ctx context.Context) (DeletionRecord, bool, error)
	Release()
}

// Stores is the per-shard directory the API's Deps needs: a process serves up to 32 shards (D3).
type Stores interface {
	For(s id.ShardID) (Store, bool)
	Local() []id.ShardID
}

// Replication is the seam of the move seal's standby wait: a streaming standby has replayed up to lsn (N179).
type Replication interface {
	Replayed(ctx context.Context, lsn id.LSN) (bool, error)
}

// ReadSession is the scope of one request on one shard. It holds no connection; every Tx opens its own, so arms that
// run concurrently (N54) never share one (a pgx connection executes one statement at a time). A semaphore shared by the
// process admits at most 32 concurrent arm transactions per shard (N155, N164), and a second semaphore of 2 inside it
// admits the filtered arms (E >= theta); TestReadSession_ConcurrentArms asserts both bounds.
type ReadSession interface {
	Scope() id.Scope
	// Tx runs fn in ONE short READ transaction on its own pooled connection: SET LOCAL scope GUCs, the read fence
	// (SELECT state FROM namespace_ownership: 'active' and 'frozen/move' are accepted, epoch ignored; 'frozen/delete',
	// 'frozen/restore', 'incoming', 'ready' and 'moved_out' are refused, a moved_out row also returns the internal
	// MovedOutHint), then o.SetLocal (enable_seqscan = off, max_parallel_workers_per_gather = 0, hnsw.ef_search, an arm
	// statement_timeout; N138). Marker sets, $allowed_docs and the plan choice are values passed in, not re-read.
	Tx(ctx context.Context, o TxOptions, fn func(tx ReadTx) error) error
}

// Tx groups the repositories by role; every accessor returns an interface of at most five methods. It does not embed
// txn.Tx: Txn() hands out the leaf capability handle, so quota.Meter can record usage inside the caller's transaction
// without importing store (N157, A-7).
type Tx interface {
	Txn() txn.Tx      // Scope() and Usage(): what quota.Meter takes
	Read() ReadTx     // the read side, also usable inside a write transaction
	Write() Writes    // insert-only content, markers, document tags
	Derived() Derived // observations, pages, consolidation bookkeeping, derivation lock (N117, N120)
	Ops() Ops         // operations, idempotency keys, ledger, outbox
}

// Writes groups the content writers.
type Writes interface {
	Insert() Inserts  // insert-only content writers (N113): no method updates a content row
	Markers() Markers // tombstones, fact_hidden, curation_log, derived_hidden, expunge progress (N115, N119)
	Tags() DocumentTags
}

// ReadTx is what a ReadSession arm and a write transaction's Read() see. It is split to stay at four accessors plus the
// derived readers: GetMemory and GetPage of an observation or page version use Derived() and so are served during a
// move freeze, not through a fenced write transaction (N157, A-9).
type ReadTx interface {
	Content() ContentReader      // documents, chunks, facts, graph, tags
	Visible() MarkerReader       // the marker sets and curation state behind the visibility predicate
	Derived() DerivedReader      // Observations().Served, Pages().Served
	Operations() OperationReader // OperationService.GetOperation / ListOperations
}

// ContentReader groups the content readers.
type ContentReader interface {
	Documents() DocumentReader
	Chunks() ChunkReader
	Facts() FactReader
	Graph() GraphReader // links and entities
	Tags() TagLister    // ListTags, from the tag_counts table
}

// DerivedReader groups the readers of derived versions.
type DerivedReader interface {
	Observations() ObservationReader
	Pages() PageReader
}

// ObservationReader serves the version CURRENT at asOf if visible, nothing if hidden; zero visible sources are not
// served; fail closed (N117, N135).
type ObservationReader interface {
	Served(ctx context.Context, os []id.ObservationID, asOf time.Time) ([]ObservationVersion, error)
}

// PageReader serves page versions under the same rule as ObservationReader.
type PageReader interface {
	Served(ctx context.Context, ps []id.PageID, asOf time.Time) ([]PageVersion, error)
}

// OperationReader backs OperationService.GetOperation and ListOperations.
type OperationReader interface {
	Get(ctx context.Context, op id.OperationID) (*OperationRow, error)
	List(ctx context.Context, q OperationQuery) ([]OperationRow, string, error)
}

// AdminTx is the engram_admin transaction.
type AdminTx interface {
	// Ownership: Get, Transition(edge, ...) generated from section 3.3.1, FenceExclusive (one 35 s attempt).
	Ownership() OwnershipRepo
	// Sweep lists namespaces with pending facts, stale observations, pending tombstones and deferred operations.
	Sweep() Sweepers
	Restore() RestoreRepo // apply an intent verbatim through the admin variant of the marker transaction (N122)
	// Indexes is vector_indexes for the index runner: requested, building, ready | failed, lease, purged_since_build
	// (N138).
	Indexes() IndexRepo
	Purge() Purger // Materialize, purge batches and DerivedPurge as engram_admin (N119, N136)
}

// OwnershipRepo reads and moves a namespace_ownership row. Transition takes an edge of the generated fsm table, so the
// Go side and the ownership_transitions trigger cannot disagree.
type OwnershipRepo interface {
	Get(ctx context.Context, ns id.NamespaceID) (*OwnershipRow, error)
	Transition(ctx context.Context, ns id.NamespaceID, edge fsm.OwnershipEdge,
		p TransitionParams) (*OwnershipRow, error)
	// FenceExclusive is the exclusive taker of the namespace fence: ONE 35 s attempt (N82).
	FenceExclusive(ctx context.Context, ns id.NamespaceID) error
}
