// Package id holds the typed identifiers of Engram (PLAN.md section 2.1, "Shared vocabulary"; register D1, N132, N162).
// Every entity has its own type, so a FactID cannot be passed where a ChunkID is wanted. It is a leaf: the standard
// library and github.com/google/uuid are its only imports, and it performs no I/O.
package id

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// ErrInvalid is wrapped by every parse and validation failure of this package; callers render it as errs.Validation.
var ErrInvalid = errors.New("invalid identifier")

// TenantID is an opaque string [a-z0-9-]{1,64} minted by the control plane (D1).
type TenantID string

// DocumentID is client-chosen, at most 256 bytes, unique per namespace; it is the upsert key (D1).
type DocumentID string

// ShardID is a dense int32 assigned by the catalog; the metrics label is shard="7" (D1).
type ShardID int32

// Epoch is the per-namespace ownership epoch: it starts at 1 and grows at every move cutover and restore (D1).
type Epoch int64

// CellID names a cell (one Temporal cluster and its shards).
type CellID string

// WorkflowID is a Temporal workflow id such as "ns/{ns}/op/{op}"; only internal/workflows builds one.
type WorkflowID string

// MaxDocumentIDBytes is the length limit of a DocumentID (D1, section 4.1.8).
const MaxDocumentIDBytes = 256

var tenantRE = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)

// ParseTenantID validates s against [a-z0-9-]{1,64}.
func ParseTenantID(s string) (TenantID, error) {
	if !tenantRE.MatchString(s) {
		return "", fmt.Errorf("%w: tenant id %q must match [a-z0-9-]{1,64}", ErrInvalid, s)
	}
	return TenantID(s), nil
}

// String returns the tenant id.
func (t TenantID) String() string { return string(t) }

// ParseDocumentID validates s: non-empty and at most 256 bytes.
func ParseDocumentID(s string) (DocumentID, error) {
	if s == "" || len(s) > MaxDocumentIDBytes {
		return "", fmt.Errorf("%w: document id must be 1 to %d bytes, got %d", ErrInvalid, MaxDocumentIDBytes, len(s))
	}
	return DocumentID(s), nil
}

// String returns the document id.
func (d DocumentID) String() string { return string(d) }

// String returns the shard id in decimal, the form of the shard="7" metrics label.
func (s ShardID) String() string { return fmt.Sprintf("%d", int32(s)) }

// Versions are one type per entity (N140): a DocVersion cannot be passed where an ObsVersion is wanted.
type (
	// DocVersion is a document version number (starts at 1; 0 means "no version yet").
	DocVersion int64
	// ObsVersion is an observation version number.
	ObsVersion int64
	// PageVersion is a page version number.
	PageVersion int64
	// SnapshotVersion is an export snapshot version number.
	SnapshotVersion int64
)

// versionPolicy says which UUIDs a typed id accepts. D1 and section 2.1 define every entity id as a UUIDv7 (time
// ordered, index friendly). The one exception is OperationID: a client may supply its own, and section 4.1.3 validates
// it only "as a UUID, UUIDv7 recommended", because the per-document operation ids the server derives are UUIDv5 and
// "UUIDv7 only" would reject the server's own ids (N72, review F-44).
type versionPolicy uint8

const (
	onlyV7     versionPolicy = iota // UUIDv7 (RFC 9562)
	anyRFC4122                      // any version of the RFC 4122 variant
)

// checkUUID rejects the nil UUID (the zero value means "absent" and is never parsed) and enforces the policy.
func checkUUID(kind string, u uuid.UUID, p versionPolicy) (uuid.UUID, error) {
	switch {
	case u == uuid.Nil:
		return uuid.Nil, fmt.Errorf("%w: %s is the nil UUID", ErrInvalid, kind)
	case u.Variant() != uuid.RFC4122:
		return uuid.Nil, fmt.Errorf("%w: %s %s is not an RFC 4122 UUID", ErrInvalid, kind, u)
	case p == onlyV7 && u.Version() != 7:
		return uuid.Nil, fmt.Errorf("%w: %s %s is a version %d UUID, want version 7", ErrInvalid, kind, u, u.Version())
	}
	return u, nil
}

// parseUUID parses the canonical 8-4-4-4-12 form only (no braces, no urn: prefix, no bare hex) so that equal ids have
// one spelling in logs, keys and workflow ids.
func parseUUID(kind, s string, p versionPolicy) (uuid.UUID, error) {
	if len(s) != 36 {
		return uuid.Nil, fmt.Errorf("%w: %s %q is not a canonical UUID", ErrInvalid, kind, s)
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s %q: %v", ErrInvalid, kind, s, err)
	}
	return checkUUID(kind, u, p)
}

// bytesToUUID converts the 16-byte form carried by events (N80) under the same rules as the string form.
func bytesToUUID(kind string, b []byte, p versionPolicy) (uuid.UUID, error) {
	u, err := uuid.FromBytes(b)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s needs 16 bytes, got %d", ErrInvalid, kind, len(b))
	}
	return checkUUID(kind, u, p)
}

func newV7() uuid.UUID {
	u, err := uuid.NewV7()
	if err != nil {
		panic("id: uuid.NewV7: " + err.Error()) // only fails when the system entropy source fails
	}
	return u
}
