// Package blob is the blob store, prefix scoping and content addressing of PLAN.md section 2.2.6 (register N7, N23,
// N100, N104). Pattern: Decorator; Scoped wraps any Store and rejects keys outside a shard prefix. Two key families
// (N104): content-addressed caches (xcache, ecache, docsum, staging: a loss is a recompute) and owner-keyed blobs
// (ledger/{ledger_id}, ver/{document_id}/v{n}, page markdown, export parts), each with exactly one owner row that
// deletes it in the purge, so no reference check or adoption race exists. M0.1 declares the signatures only.
package blob

import (
	"context"
	"io"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// Store is the object store as the service sees it.
type Store interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, o PutOptions) (*ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, p ListPage) (*ListResult, error)
}

// PutOptions are the options of Store.Put.
type PutOptions struct {
	ContentType string
	// IfAbsent makes the put conditional (If-None-Match: *); intent objects use it (N122).
	IfAbsent bool
}

// ObjectInfo describes a stored object; LastModified supplies the xcache grace check (N100).
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// ListPage is the paging of Store.List.
type ListPage struct {
	Limit int
	After string
}

// ListResult is one page of keys.
type ListResult struct {
	Objects []ObjectInfo
	Next    string
}

// Prefix is the key prefix {shard}/{tenant}/{ns}/ of one namespace.
type Prefix struct {
	Shard     id.ShardID
	Tenant    id.TenantID
	Namespace id.NamespaceID
}

// Credential is the scoped credential a Scoped store presents (secrets only from /run/secrets).
type Credential struct {
	Name string
}

// Kind is a content-addressed key family.
type Kind string

// The content-addressed kinds (24 h grace on delete).
const (
	KindXCache  Kind = "xcache"
	KindECache  Kind = "ecache"
	KindDocSum  Kind = "docsum"
	KindStaging Kind = "staging"
)

// Scoped rejects any key not under {shard}/{tenant}/{ns}/ (../, absolute, other shard).
func Scoped(root Store, p Prefix, cred Credential) Store {
	panic("stub")
}

// ContentKey is "{prefix}{kind}/{hex sha256}[.ext]".
func ContentKey(p Prefix, k Kind, sum [32]byte, ext string) string {
	panic("stub")
}

// LedgerKey is "{prefix}ledger/{ledger_id}": owner = the ingest_ledger row, minted per attempt before the put (N7).
func LedgerKey(p Prefix, l id.LedgerID) string {
	panic("stub")
}

// VersionBodyKey is "{prefix}ver/{document_id}/v{n}": owner = the document_versions row (N104).
func VersionBodyKey(p Prefix, d id.DocumentID, v id.DocVersion) string {
	panic("stub")
}

// Tombstoner makes purges resumable: a blob_tombstones row is inserted first, the object deleted, then the row. It
// takes a store.Tx (blob imports store; store never imports blob).
type Tombstoner interface {
	MarkDeleted(ctx context.Context, tx store.Tx, key, reason string) error
	PurgeMarked(ctx context.Context, tx store.Tx, limit int) (int, error)
}
