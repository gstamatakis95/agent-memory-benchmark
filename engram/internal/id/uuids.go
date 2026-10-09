// The nine UUID-backed entity ids share one shape (New, Parse, FromBytes, String, Bytes, IsZero) and are deliberately
// distinct types (N132): the compiler rejects a FactID where a ChunkID is wanted. Wire forms: the canonical
// 36-character string in JSON, logs and keys; the 16-byte form in events (N80).

package id

import "github.com/google/uuid"

// NamespaceID identifies a namespace (UUIDv7, server-assigned, D1).
type NamespaceID uuid.UUID

// NewNamespaceID mints a fresh UUIDv7.
func NewNamespaceID() NamespaceID { return NamespaceID(newV7()) }

// ParseNamespaceID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParseNamespaceID(s string) (NamespaceID, error) {
	u, err := parseUUID("namespace id", s, onlyV7)
	return NamespaceID(u), err
}

// NamespaceIDFromBytes converts the 16-byte form that events carry (N80).
func NamespaceIDFromBytes(b []byte) (NamespaceID, error) {
	u, err := bytesToUUID("namespace id", b, onlyV7)
	return NamespaceID(u), err
}

// String returns the canonical lower-case form.
func (x NamespaceID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x NamespaceID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x NamespaceID) IsZero() bool { return x == NamespaceID{} }

// FactID identifies a fact, the memory_id of D1 (UUIDv7).
type FactID uuid.UUID

// NewFactID mints a fresh UUIDv7.
func NewFactID() FactID { return FactID(newV7()) }

// ParseFactID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParseFactID(s string) (FactID, error) {
	u, err := parseUUID("fact id", s, onlyV7)
	return FactID(u), err
}

// FactIDFromBytes converts the 16-byte form that events carry (N80).
func FactIDFromBytes(b []byte) (FactID, error) {
	u, err := bytesToUUID("fact id", b, onlyV7)
	return FactID(u), err
}

// String returns the canonical lower-case form.
func (x FactID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x FactID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x FactID) IsZero() bool { return x == FactID{} }

// ChunkID identifies a chunk (UUIDv7).
type ChunkID uuid.UUID

// NewChunkID mints a fresh UUIDv7.
func NewChunkID() ChunkID { return ChunkID(newV7()) }

// ParseChunkID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParseChunkID(s string) (ChunkID, error) {
	u, err := parseUUID("chunk id", s, onlyV7)
	return ChunkID(u), err
}

// ChunkIDFromBytes converts the 16-byte form that events carry (N80).
func ChunkIDFromBytes(b []byte) (ChunkID, error) {
	u, err := bytesToUUID("chunk id", b, onlyV7)
	return ChunkID(u), err
}

// String returns the canonical lower-case form.
func (x ChunkID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x ChunkID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x ChunkID) IsZero() bool { return x == ChunkID{} }

// EntityID identifies an entity (UUIDv7).
type EntityID uuid.UUID

// NewEntityID mints a fresh UUIDv7.
func NewEntityID() EntityID { return EntityID(newV7()) }

// ParseEntityID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParseEntityID(s string) (EntityID, error) {
	u, err := parseUUID("entity id", s, onlyV7)
	return EntityID(u), err
}

// EntityIDFromBytes converts the 16-byte form that events carry (N80).
func EntityIDFromBytes(b []byte) (EntityID, error) {
	u, err := bytesToUUID("entity id", b, onlyV7)
	return EntityID(u), err
}

// String returns the canonical lower-case form.
func (x EntityID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x EntityID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x EntityID) IsZero() bool { return x == EntityID{} }

// ObservationID identifies an observation (UUIDv7).
type ObservationID uuid.UUID

// NewObservationID mints a fresh UUIDv7.
func NewObservationID() ObservationID { return ObservationID(newV7()) }

// ParseObservationID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParseObservationID(s string) (ObservationID, error) {
	u, err := parseUUID("observation id", s, onlyV7)
	return ObservationID(u), err
}

// ObservationIDFromBytes converts the 16-byte form that events carry (N80).
func ObservationIDFromBytes(b []byte) (ObservationID, error) {
	u, err := bytesToUUID("observation id", b, onlyV7)
	return ObservationID(u), err
}

// String returns the canonical lower-case form.
func (x ObservationID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x ObservationID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x ObservationID) IsZero() bool { return x == ObservationID{} }

// PageID identifies a page (UUIDv7).
type PageID uuid.UUID

// NewPageID mints a fresh UUIDv7.
func NewPageID() PageID { return PageID(newV7()) }

// ParsePageID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParsePageID(s string) (PageID, error) {
	u, err := parseUUID("page id", s, onlyV7)
	return PageID(u), err
}

// PageIDFromBytes converts the 16-byte form that events carry (N80).
func PageIDFromBytes(b []byte) (PageID, error) {
	u, err := bytesToUUID("page id", b, onlyV7)
	return PageID(u), err
}

// String returns the canonical lower-case form.
func (x PageID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x PageID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x PageID) IsZero() bool { return x == PageID{} }

// LedgerID identifies an ingest-ledger row, minted per attempt (UUIDv7, N7).
type LedgerID uuid.UUID

// NewLedgerID mints a fresh UUIDv7.
func NewLedgerID() LedgerID { return LedgerID(newV7()) }

// ParseLedgerID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParseLedgerID(s string) (LedgerID, error) {
	u, err := parseUUID("ledger id", s, onlyV7)
	return LedgerID(u), err
}

// LedgerIDFromBytes converts the 16-byte form that events carry (N80).
func LedgerIDFromBytes(b []byte) (LedgerID, error) {
	u, err := bytesToUUID("ledger id", b, onlyV7)
	return LedgerID(u), err
}

// String returns the canonical lower-case form.
func (x LedgerID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x LedgerID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x LedgerID) IsZero() bool { return x == LedgerID{} }

// OperationID identifies an operation; the client may supply any RFC 4122 UUID (D1, section 4.1.3).
type OperationID uuid.UUID

// NewOperationID mints a fresh UUIDv7.
func NewOperationID() OperationID { return OperationID(newV7()) }

// ParseOperationID parses the canonical 36-character form. Unlike every other id it accepts any version of the RFC 4122
// variant, not only UUIDv7: a client may supply its own operation_id and the server derives UUIDv5 ones (section 4.1.3,
// N72). The nil UUID is rejected.
func ParseOperationID(s string) (OperationID, error) {
	u, err := parseUUID("operation id", s, anyRFC4122)
	return OperationID(u), err
}

// OperationIDFromBytes converts the 16-byte form that events carry (N80); any RFC 4122 version, never the nil UUID.
func OperationIDFromBytes(b []byte) (OperationID, error) {
	u, err := bytesToUUID("operation id", b, anyRFC4122)
	return OperationID(u), err
}

// String returns the canonical lower-case form.
func (x OperationID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x OperationID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x OperationID) IsZero() bool { return x == OperationID{} }

// MoveID identifies a namespace move attempt (UUIDv7).
type MoveID uuid.UUID

// NewMoveID mints a fresh UUIDv7.
func NewMoveID() MoveID { return MoveID(newV7()) }

// ParseMoveID parses the canonical 36-character form. Only a UUIDv7 is accepted (D1); the nil UUID never is.
func ParseMoveID(s string) (MoveID, error) {
	u, err := parseUUID("move id", s, onlyV7)
	return MoveID(u), err
}

// MoveIDFromBytes converts the 16-byte form that events carry (N80).
func MoveIDFromBytes(b []byte) (MoveID, error) {
	u, err := bytesToUUID("move id", b, onlyV7)
	return MoveID(u), err
}

// String returns the canonical lower-case form.
func (x MoveID) String() string { return uuid.UUID(x).String() }

// Bytes returns the 16-byte form.
func (x MoveID) Bytes() []byte { return x[:] }

// IsZero reports whether x is the nil UUID.
func (x MoveID) IsZero() bool { return x == MoveID{} }
