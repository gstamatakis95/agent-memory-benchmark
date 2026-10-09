package store

import (
	"context"
	"errors"

	"github.com/gstamatakis95/engram/internal/id"
)

// ErrRefused is returned by a try-lock that was refused (the derivation lock, N120); the caller retries.
var ErrRefused = errors.New("store: lock refused")

// ErrBaseLost is returned by InsertVersion when the base compare-and-set matched zero rows: the writer lost, rolls back
// and discards (N143, BaseCurrentAtCommit, TestApply_BaseVersionCAS).
var ErrBaseLost = errors.New("store: derivation base lost")

// Markers writes the delete and curation markers. Every marker method also inserts the shard-local deletion_log row in
// the same transaction and returns the marker's exact effect as a DeletionRecord{Subject, Operation, Prev, DeletedAt,
// UpToVersion, MemoryIDs}: Subject is the typed id.Subject and Prev the subject's chain tip read BY ins_seq under the
// subject lock (never deleted_at; the predecessor, N122, N162). The intent object is built from that record.
type Markers interface {
	// TombstoneDocument writes a document_tombstones 'pending' row covering (document, t.UpToVersion = highest version
	// assigned, N133c); event_seq is NULL until the final statement, WITH s AS (INSERT INTO outbox ... RETURNING seq)
	// UPDATE document_tombstones SET event_seq = s.seq (A-10).
	TombstoneDocument(ctx context.Context, t DocumentTombstone) (DeletionRecord, error)
	// TombstoneChunks: reason is 'replace' | 'reextract'.
	TombstoneChunks(ctx context.Context, reason ChunkReason, cs []id.ChunkID) error
	// Hide writes fact_hidden keyed (memory_id, cause), no FK to facts (N145): 'invalidate' | 'reextract'; for
	// 'invalidate', under the subject lock of (document_id, content_hash) that the caller holds until its intent is put
	// (SubjectLocker, N150, N159, N162) it hides EVERY live fact of the subject, each stamped invalidation_op = the
	// operation id (rec.MemoryIDs); a second invalidate is changed = false and a success (N139), but it still inserts
	// its own deletion_log row (N143).
	Hide(ctx context.Context, f id.FactID, cause HideCause, reason string) (rec DeletionRecord, changed bool, err error)
	// Unhide resolves I = fact_hidden(f).invalidation_op and deletes fact_hidden WHERE invalidation_op = I and the
	// derived_hidden rows of those ids (N162); a 'reextract' row is never touched (N135); it works after the fact's own
	// purge (N145), NOT_FOUND{DOCUMENT_DELETED} after its document's purge.
	Unhide(ctx context.Context, f id.FactID) (rec DeletionRecord, changed bool, err error)
	// Expunge is expunge_progress, derived_hidden and the consumer-cursor check.
	Expunge() ExpungeRepo
}

// MarkerReader reads the marker sets behind the visibility predicate.
type MarkerReader interface {
	// Sets returns doc_tomb and doc_pending ({document -> up_to_version}, N133c) and chunk_tomb: two indexed selects,
	// bounded by expunge lag (N116). fact_hidden is NOT in the set: every arm tests it per candidate by primary-key
	// anti-join.
	Sets(ctx context.Context) (MarkerSets, error)
	// Curation is the last action for the subject (document_id, content_hash) with its invalidation_op, re-applied at
	// CommitChunk; a later restore row suppresses the re-application (N162).
	Curation(ctx context.Context, doc id.DocumentID, hash [32]byte) (CurationState, error)
}

// Derived groups the repositories of derived state (observations, pages, consolidation bookkeeping) and the derivation
// lock.
type Derived interface {
	// Observations, Pages and Consolidation: Served lives on the read side (DerivedReader).
	Observations() ObservationRepo
	Pages() PageRepo
	Consolidation() ConsolidationRepo
	TryDerivationLock(ctx context.Context) error       // shared; ErrRefused -> retry (N120)
	DerivationLockExclusive(ctx context.Context) error // Materialize and Restore: one 35 s attempt
}

// ObservationRepo writes observation versions. The derivation commit rule (N120, N143, N144) is two repository calls
// under the shared lock, in this order: Verify (a fresh statement), then InsertVersion, whose FIRST statement is the
// idempotency lookup (engram_derivation_commit_seen), then the base compare-and-set
// (engram_derivation_base_cas($expected, check_visible)).
type ObservationRepo interface {
	// Verify checks every rendered fact input against the FULL marker sets (all open tombstones, chunk_tomb,
	// fact_hidden of both causes), observation-version inputs with engram_obs_version_hidden against ALL open
	// tombstones; an early, advisory base read only (the base check that counts is the CAS in InsertVersion).
	Verify(ctx context.Context, in VerifyInput) (VerifyResult, error)
	// InsertVersion: FIRST `commit_key` seen -> return the committed version, touch nothing (N144); THEN the base CAS
	// `UPDATE observations SET current_version = $expected + 1 WHERE ... AND current_version = $expected` for EVERY
	// writer, root rebuilds included (engram_derivation_base_cas), zero rows = ErrBaseLost, the caller rolls back and
	// discards (N143); then root_version, observation_inputs, observation_version_sources, vector (with obs_tags),
	// meta.superseded_at; the stale flags clear only WHERE stale_seq = $captured; no FK to facts (N135).
	InsertVersion(ctx context.Context, v ObservationVersion, inputs []id.FactID, sources []Source) error
	// ReplaceSources rewrites the working set; only under the derivation lock; add before drop (N57).
	ReplaceSources(ctx context.Context, o id.ObservationID, add, drop []id.FactID) error
	// MarkStale is a narrow mutable row; it hides nothing.
	MarkStale(ctx context.Context, os []id.ObservationID, k StaleKind) error
}

// PageRepo follows the same shape as ObservationRepo; Served is on PageReader. InsertVersion also writes `text`,
// `commit_key` and the `page_version_vectors` row, and its markdown key is attempt-unique and deleted on a failed
// commit only when no row names it (N144).
type PageRepo interface {
	Verify(ctx context.Context, in VerifyInput) (VerifyResult, error)
	InsertVersion(ctx context.Context, v PageVersion, inputs []id.FactID, sources []Source) error
	SetSources(ctx context.Context, p id.PageID, add, drop []id.FactID) error
	MarkStale(ctx context.Context, ps []id.PageID, k StaleKind) error
}

// ConsolidationRepo is the consolidation bookkeeping: proposals are insert-only keyed (batch_key, attempt) and a
// discard or a capacity retry writes the next attempt (nothing is deleted); Batches holds state in {routed, stored,
// applied, discarded, capacity} and `applied` is written in the same transaction as the `done` stamps; the watermark is
// bounded by ins_seq < engram_seq_floor(now()), not an id timestamp (N95, C-19).
type ConsolidationRepo interface {
	Watermark(ctx context.Context) (int64, error)
	Advance(ctx context.Context, to int64) error
	Proposals() ProposalRepo
	Applied(ctx context.Context, opKey [32]byte) (bool, error)
	Batches() BatchRepo
}

// ProposalRepo stores consolidation proposals, insert-only, keyed (batch_key, attempt).
type ProposalRepo interface {
	Insert(ctx context.Context, p ProposalRow) (stored ProposalRow, err error)
	Get(ctx context.Context, key [32]byte, attempt int) (ProposalRow, error)
}

// BatchRepo records the state of consolidation batches.
type BatchRepo interface {
	Record(ctx context.Context, key [32]byte, attempt int, st BatchState) error
}

// ExpungeRepo is the expunge bookkeeping (expunge_progress).
type ExpungeRepo interface {
	Next(ctx context.Context) (*ExpungeUnit, error)
	Record(ctx context.Context, u ExpungeUnit) error
	// ConsumersPassed is engram_consumers_passed(event_seq): every registered consumer cursor is past seq.
	ConsumersPassed(ctx context.Context, eventSeq int64) (bool, error)
	Finish(ctx context.Context, ns id.NamespaceID) error
}

// Purger runs as engram_admin. Batch deletes rows, vectors and owner-keyed blobs but never evidence rows (N135);
// Derived replaces each covered observation or page version by a content-free stub in one transaction, naming every NOT
// NULL column with a documented placeholder (`pv_id` fresh, `markdown_blob_key = '_stub'`, `evidence_hash = '\x00...'`;
// N136, N157). Materialize discovers its work from `fact_hidden WHERE cause = 'invalidate' AND materialized_at IS NULL`
// and the open tombstones, never from the signal, and stamps `materialized_at` in the batch that writes the
// `derived_hidden` rows (N145).
type Purger interface {
	Materialize(ctx context.Context, ns id.NamespaceID, limit int) (MaterializeResult, error)
	Batch(ctx context.Context, ns id.NamespaceID, target PurgeTarget, limit int) (PurgeResult, error)
	Derived(ctx context.Context, ns id.NamespaceID, limit int) (PurgeResult, error)
}
