// Package router maps a scope (or a workflow input) to the resources of its shard (PLAN.md section 2.2.4; register D4,
// N17, N52, N71, N93, N98, N140). It takes the plain id.Scope, never authz.RequestScope, so router does not import
// authz; store.Stores is built from the same list. Pattern: Registry of per-shard handles; Retry is the one place that
// encodes the move/freeze retry contract. M0.1 declares the signatures only.
package router

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/gstamatakis95/engram/internal/blob"
	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/index"
	"github.com/gstamatakis95/engram/internal/store"
	"github.com/gstamatakis95/engram/internal/telemetry"
)

// ShardHandle is the set of resources of one shard (router imports telemetry, a listed edge, N157).
type ShardHandle struct {
	Shard     id.ShardID
	Cell      id.CellID
	Store     store.Store
	Blob      blob.Store
	Index     index.Searcher
	TaskQueue string
	Metrics   telemetry.ShardLabels
}

// ShardRouter resolves scopes to handles.
type ShardRouter interface {
	// For returns the local handle or errs.ShardNotLocal(cell).
	For(ctx context.Context, sc id.Scope) (*ShardHandle, error)
	// ForShard serves workers, which carry shard_id in their inputs (D4).
	ForShard(s id.ShardID) (*ShardHandle, bool)
	// Forward is the cross-cell hop (phase 3), at most one (N17).
	Forward(ctx context.Context, cell id.CellID, method string, req, resp proto.Message) error
	Local() []id.ShardID
}

// Options configure the router built at process start for the cell's shard list.
type Options struct {
	Shards   []id.ShardID
	PoolSize int
}

// Retry runs fn under the retry contract: one re-resolve on WrongShardOrEpoch for writes; on MOVED_OUT it first follows
// the internal MovedOutHint to the new owner (no catalog read, N93); calls that meet MOVED_OUT, NamespaceNotReady or
// NamespaceFrozen re-resolve in a bounded loop (at most 5 s, jittered 50 to 500 ms; writes on NamespaceFrozen up to 30
// s). Only handlers that are safe to re-execute call it (all writes are idempotent).
func Retry(ctx context.Context, r catalog.Resolver, sr ShardRouter, sc id.Scope,
	fn func(ctx context.Context, h *ShardHandle, sc id.Scope) error) error {
	panic("stub")
}

// Forwarder proxies a request to another cell (phase 3, at most one hop, N17): unary calls with Invoke and the three
// server-streaming methods with a gRPC ClientStream. The external Envoy strips `engram-forward-*` (N17, N71).
type Forwarder struct{}
