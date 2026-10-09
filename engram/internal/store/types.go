package store

import (
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/id"
)

// The value types below are the vocabulary of the repositories. M0.1 fixes their names and the fields the section 2.2
// signatures and the register name; the owning milestones (M0.8, M1.1, M1.3, M1.8, M1.10) add columns as the DDL needs.

// TxOptions are the per-arm settings of ReadSession.Tx.
type TxOptions struct {
	// SetLocal are the SET LOCAL statements of the arm: enable_seqscan = off, max_parallel_workers_per_gather = 0,
	// hnsw.ef_search and an arm statement_timeout (N138).
	SetLocal []string
	// Filtered marks an arm of the filtered class (E >= theta): it passes the second semaphore (2 per process, N164).
	Filtered bool
}

// LockMode selects the per-document lock: LockShared is a try-lock (DocumentBusy after 100 ms); LockExclusive is one 35
// s attempt (N83).
type LockMode uint8

// The document lock modes.
const (
	LockShared LockMode = iota + 1
	LockExclusive
)

// Filter is the structural filter shared by the fact readers; it has the shape of index.Filter, and one converts to the
// other.
type Filter struct {
	AsOf        *time.Time
	FactTypes   []memoryv1.FactType
	AllowedDocs []id.DocumentID
	Markers     MarkerSets
}

// ReadOptions are the options of FactReader.ByIDs.
type ReadOptions struct {
	AsOf *time.Time
	// IncludeInvalidated returns an invalidated fact with InvalidatedAt set (GetMemory, BatchGetMemories, N157).
	IncludeInvalidated bool
}

// MarkerSets are the values loaded once per recall and passed to every arm (N116): doc_tomb, doc_pending (document to
// up_to_version) and chunk_tomb. fact_hidden is never in the sets.
type MarkerSets struct {
	DocTomb    map[id.DocumentID]id.DocVersion
	DocPending map[id.DocumentID]id.DocVersion
	ChunkTomb  []id.ChunkID
}

// CurationState is the last curation action for a subject (document_id, content_hash) (N162).
type CurationState struct {
	Hidden         bool
	InvalidationOp id.OperationID
}

// HideCause is the fact_hidden cause.
type HideCause uint8

// The hide causes.
const (
	HideInvalidate HideCause = iota + 1
	HideReextract
)

// ChunkReason is the chunk_tomb reason.
type ChunkReason uint8

// The chunk tombstone reasons.
const (
	ChunkReplace ChunkReason = iota + 1
	ChunkReextract
)

// DeletionRecord is the exact effect of a marker, read back from the deletion_log row written in the same transaction.
// Prev is the subject's chain tip read BY ins_seq under the subject lock (never deleted_at; N122, N162). The intent
// object is built from this record.
type DeletionRecord struct {
	Subject     id.Subject
	Operation   id.OperationID
	Prev        id.OperationID
	DeletedAt   time.Time
	UpToVersion id.DocVersion
	MemoryIDs   []id.FactID
}

// DocumentDraft is the input of DocumentWriter.BeginVersion.
type DocumentDraft struct {
	Document id.DocumentID
	Mode     memoryv1.UpdateMode
	Ledger   id.LedgerID
	Tags     []string
	Metadata map[string]any
	Context  string
}

// DocumentTombstone is the input of Markers.TombstoneDocument.
type DocumentTombstone struct {
	Document    id.DocumentID
	UpToVersion id.DocVersion
	Operation   id.OperationID
	At          time.Time
}

// Document is a document row, or its content-free tombstone view when Deleting (N136).
type Document struct {
	ID         id.DocumentID
	Current    id.DocVersion
	Deleting   bool
	Tags       []string
	TagGen     int64
	Metadata   map[string]any
	CreatedAt  time.Time
	UpdatedAt  time.Time
	LastLedger id.LedgerID
}

// DocVersion is one document_versions row (the store type; id.DocVersion is the version number).
type DocVersion struct {
	Document   id.DocumentID
	Version    id.DocVersion
	Status     string // ingesting | active | superseded | deleted
	ChunkCount int
	FactCount  int
	CreatedAt  time.Time
}

// DocumentQuery is the filter and paging of DocumentReader.List.
type DocumentQuery struct {
	Tags      []string
	TagMode   memoryv1.TagMatchMode
	PageSize  int32
	PageToken string
}

// BodyRef is the owner-keyed body key and hash of a document version.
type BodyRef struct {
	Key string
	Sum [32]byte
}

// TagQuery is the paging of ListTags.
type TagQuery struct {
	Prefix    string
	PageSize  int32
	PageToken string
}

// TagCount is one row of tag_counts.
type TagCount struct {
	Tag   string
	Count int64
}

// TagPatch is the input of DocumentTags.Update.
type TagPatch struct {
	Add, Remove []string
	Expected    int64 // tag_generation, 0 = unconditional
}

// ChunkInsert is the input of ChunkWriter.Insert.
type ChunkInsert struct {
	Document    id.DocumentID
	Version     id.DocVersion
	ContentHash [32]byte
	Text        string
	Header      string
	MentionedAt time.Time
}

// Chunk is a chunk row.
type Chunk struct {
	ID          id.ChunkID
	ContentHash [32]byte
	Text        string
	Header      string
	MentionedAt time.Time
}

// ChunkState classifies a content hash for PlanChunks: member | live | tombstoned | stale_extraction | absent.
type ChunkState struct {
	Hash  [32]byte
	State string
	Chunk id.ChunkID
}

// VectorRow is a packed vector with its model and effective time (embedding_effective_at, N111).
type VectorRow struct {
	Model       string
	Dims        int
	Vector      []float32
	EffectiveAt time.Time
}

// Fact is a fact row.
type Fact struct {
	ID            id.FactID
	Document      id.DocumentID
	Chunk         id.ChunkID
	Type          memoryv1.FactType
	Text          string
	ContentHash   [32]byte
	ExtractionKey [32]byte
	MentionedAt   time.Time
	OccurredStart *time.Time
	OccurredEnd   *time.Time
	InvalidatedAt *time.Time
}

// StampNote is the payload of a fact_consolidation stamp (N95).
type StampNote struct {
	State string // done | failed
	Note  string
}

// LinkKind is the fact_links type.
type LinkKind uint8

// The link kinds.
const (
	LinkEntity LinkKind = iota + 1
	LinkTemporal
	LinkSemantic
	LinkCausal
)

// Link is one fact_links edge; undirected edges are stored once with Src < Dst.
type Link struct {
	Src, Dst id.FactID
	Kind     LinkKind
	Weight   float32
}

// MemoryQuery is the structural filter of ListMemories.
type MemoryQuery struct {
	Filter             Filter
	IncludeInvalidated bool
	PageSize           int32
	PageToken          string
}

// EntityUpsert, Alias, Mention and EntityCandidate are the entity writer and reader values.
type (
	EntityUpsert struct {
		Name, Type, CanonicalNorm string
	}
	Alias struct {
		Entity   id.EntityID
		Alias    string
		Document id.DocumentID
	}
	Mention struct {
		Entity      id.EntityID
		Fact        id.FactID
		MentionedAt time.Time
	}
	EntityCandidate struct {
		Entity id.EntityID
		Name   string
		Score  float32
	}
)

// ObservationVersion and PageVersion are version rows of derived state (the store types; id.ObsVersion and
// id.PageVersion are the version numbers).
type (
	ObservationVersion struct {
		Observation id.ObservationID
		Version     id.ObsVersion
		Text        string
		EffectiveAt time.Time
		CommitKey   [32]byte
		Expected    id.ObsVersion // the base the compare-and-set checks
	}
	PageVersion struct {
		Page        id.PageID
		Version     id.PageVersion
		Text        string
		MarkdownKey string
		CommitKey   [32]byte
		Expected    id.PageVersion
	}
)

// Source is one evidence segment of a derived version (observation_version_sources, page_version_inputs).
type Source struct {
	Fact id.FactID
	Obs  id.ObservationID
}

// StaleKind says why a derived row is flagged stale.
type StaleKind uint8

// The stale kinds.
const (
	StaleSource StaleKind = iota + 1
	StaleWrite
)

// VerifyInput is what a derivation transaction re-verifies in a fresh statement before InsertVersion (N120).
type VerifyInput struct {
	Facts        []id.FactID
	Observations []ObservationVersionRef
	Base         id.ObsVersion
}

// ObservationVersionRef names one observation version that a new version was rendered from.
type ObservationVersionRef struct {
	Observation id.ObservationID
	Version     id.ObsVersion
}

// VerifyResult lists the inputs that are hidden at verification time; a non-empty list discards the proposal.
type VerifyResult struct {
	HiddenFacts []id.FactID
	BaseVisible bool
}

// ProposalRow and BatchState are the consolidation bookkeeping values (insert-only, N121).
type (
	ProposalRow struct {
		BatchKey [32]byte
		Attempt  int
		Payload  []byte
	}
	BatchState uint8
)

// The consolidation batch states.
const (
	BatchRouted BatchState = iota + 1
	BatchStored
	BatchApplied
	BatchDiscarded
	BatchCapacity
)

// ExpungeUnit is one resumable unit of expunge_progress.
type ExpungeUnit struct {
	Namespace id.NamespaceID
	Unit      string
	Table     string
	LastKey   []byte
	Done      bool
}

// PurgeTarget is the target of a purge batch: MARKERS, CHUNK_TOMBSTONES, REEXTRACTED_FACTS or OLD_EMBEDDING_MODEL
// (N119).
type PurgeTarget uint8

// The purge targets.
const (
	PurgeMarkers PurgeTarget = iota + 1
	PurgeChunkTombstones
	PurgeReextractedFacts
	PurgeOldEmbeddingModel
)

// MaterializeResult and PurgeResult report one Materialize or purge batch.
type (
	MaterializeResult struct {
		Hidden int
		Done   bool
	}
	PurgeResult struct {
		Rows int
		WAL  int64 // bytes written, for pacing
		Done bool
	}
)

// OperationRow is one operations row.
type OperationRow struct {
	Namespace   id.NamespaceID
	ID          id.OperationID
	Kind        string
	State       string
	RequestID   string
	TargetID    string
	WorkflowID  id.WorkflowID
	TaskQueue   string
	SubmittedAt time.Time
	Epoch       id.Epoch
}

// OperationQuery is the filter and paging of OperationReader.List.
type OperationQuery struct {
	Kinds     []string
	States    []string
	PageSize  int32
	PageToken string
}

// Result, Progress and DeferredInfo are the payloads of operation transitions.
type (
	Result struct {
		Message string
		Error   []byte // google.rpc.Status as JSON
	}
	Progress     map[string]int64
	DeferredInfo struct {
		Quota string
	}
)

// IdempotencyState is the answer of IdempotencyRepo.Begin.
type IdempotencyState struct {
	Replay   bool   // the key exists with the same hash: return Response
	Response []byte // the stored response bytes on a replay
	Conflict bool   // the key exists with a different hash: IDEMPOTENCY_KEY_REUSED
}

// LedgerEntry is one append-only ingest ledger row. Inline body up to 64 KiB, else the owner-keyed blob ledger/{id}.
type LedgerEntry struct {
	ID        id.LedgerID
	Document  id.DocumentID
	Version   id.DocVersion
	Inline    []byte
	BlobKey   string
	CreatedAt time.Time
}

// OutboxRow is one outbox row as the relay reads it.
type OutboxRow struct {
	Seq       int64
	Namespace id.NamespaceID
	Payload   []byte
}

// Cursor is one outbox_cursors row.
type Cursor struct {
	Consumer string
	Seq      int64
	Gaps     []int64
}

// OwnershipRow is one namespace_ownership row.
type OwnershipRow struct {
	Namespace     id.NamespaceID
	Tenant        id.TenantID
	State         string // incoming | ready | active | frozen | moved_out
	FreezeReason  string
	Epoch         id.Epoch
	Move          id.MoveID
	MoveEpoch     id.Epoch
	TargetShard   id.ShardID
	TargetEpoch   id.Epoch
	FloorLSN      id.LSN
	FloorTimeline int32
}

// TransitionParams are the column effects of an ownership edge (epoch, move_id, target hint).
type TransitionParams struct {
	Epoch       id.Epoch
	Move        id.MoveID
	MoveEpoch   id.Epoch
	TargetShard id.ShardID
	TargetEpoch id.Epoch
}

// ReplayIntent and ReplayOutcome are the input and result of RestoreRepo.Apply.
type (
	ReplayIntent struct {
		Subject     id.Subject
		Operation   id.OperationID
		Prev        id.OperationID
		DeletedAt   time.Time
		Epoch       id.Epoch
		UpToVersion id.DocVersion
		MemoryIDs   []id.FactID
	}
	ReplayOutcome uint8
)

// The replay outcomes (deletion_log.replay_outcome).
const (
	ReplayApplied ReplayOutcome = iota + 1
	ReplaySkipped
)

// IndexRequest and IndexState are a vector_indexes row and its state.
type (
	IndexRequest struct {
		Namespace id.NamespaceID
		Model     string
		State     IndexState
		Priority  bool
	}
	IndexState uint8
)

// The vector index states.
const (
	IndexRequested IndexState = iota + 1
	IndexBuilding
	IndexReady
	IndexFailed
)
