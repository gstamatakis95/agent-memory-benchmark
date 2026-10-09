package id

import (
	"fmt"

	"github.com/google/uuid"
)

// MemoryKind says which of the three things a recall result can be.
type MemoryKind uint8

// The memory kinds. The zero value is invalid on purpose.
const (
	KindFact MemoryKind = iota + 1
	KindObservation
	KindChunk
)

// String returns the lower-case kind name.
func (k MemoryKind) String() string {
	switch k {
	case KindFact:
		return "fact"
	case KindObservation:
		return "observation"
	case KindChunk:
		return "chunk"
	}
	return fmt.Sprintf("memory_kind(%d)", uint8(k))
}

// MemoryRef is the typed union of the three things a recall result can be. It replaces an untyped uuid.UUID in
// index.Hit, recall.Candidate and the citation verifier; build one with FactRef, ObservationRef or ChunkRef.
type MemoryRef struct {
	Kind MemoryKind
	ID   uuid.UUID
}

// FactRef wraps a fact id.
func FactRef(f FactID) MemoryRef { return MemoryRef{Kind: KindFact, ID: uuid.UUID(f)} }

// ObservationRef wraps an observation id.
func ObservationRef(o ObservationID) MemoryRef {
	return MemoryRef{Kind: KindObservation, ID: uuid.UUID(o)}
}

// ChunkRef wraps a chunk id.
func ChunkRef(c ChunkID) MemoryRef { return MemoryRef{Kind: KindChunk, ID: uuid.UUID(c)} }

// String renders `kind:uuid`.
func (r MemoryRef) String() string { return r.Kind.String() + ":" + r.ID.String() }

// Scope is the fencing token that travels with every request and every workflow input.
type Scope struct {
	Tenant    TenantID
	Namespace NamespaceID
	Shard     ShardID
	Epoch     Epoch
}

// Caller is the small value services take with a Scope instead of authz.RequestScope (N157, A-8).
type Caller struct {
	Subject   string
	RequestID string
}
