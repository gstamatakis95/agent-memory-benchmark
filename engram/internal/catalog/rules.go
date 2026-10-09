package catalog

import (
	"context"
	"fmt"
	"regexp"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// The validation and error rules shared by the Postgres catalog and the MemoryCatalog double, so that both answer the
// same input with the same error (the T3 twin test holds them to it).

// AutoShard in CreateParams.Shard asks the catalog to place the namespace (pick_shard, PLAN.md section 3.2).
const AutoShard id.ShardID = -1

// Defaults of the vector space (N111): the namespace's embedding model and dimension are fixed at creation.
const (
	DefaultEmbeddingModel = "nomic-embed-text-v1.5"
	DefaultEmbeddingDims  = 768
)

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Pre-conditions types returned by the catalog CAS (PreconditionFailed.Type).
const (
	PreconditionStateMismatch = "NAMESPACE_STATE_MISMATCH"
	PreconditionEpochMismatch = "NAMESPACE_EPOCH_MISMATCH"
	PreconditionTenantDeleted = "TENANT_NOT_ACTIVE"
)

// ShardBootstrapper is the shard half of CreateNamespace (PLAN.md section 3.2): it inserts namespace_ownership
// (active, epoch 1), namespace_stats and namespace_models on the entry's shard. It is a seam, because the catalog does
// not import the store.
type ShardBootstrapper interface {
	Bootstrap(ctx context.Context, e *Entry) error
}

func validateCreate(p *CreateParams) error {
	if p.Tenant == "" {
		return errs.Validation("tenant_id", "required")
	}
	if !nameRE.MatchString(p.Name) {
		return errs.ValidationReason("namespace.name", "BAD_FORMAT", "must match "+nameRE.String())
	}
	if p.EmbeddingModel == "" {
		p.EmbeddingModel = DefaultEmbeddingModel
	}
	if p.EmbeddingDims == 0 {
		p.EmbeddingDims = DefaultEmbeddingDims
	}
	if len(p.EmbeddingModel) > 128 {
		return errs.ValidationReason("namespace.embedding_model", "TOO_LONG", "at most 128 bytes")
	}
	if p.EmbeddingDims < 8 || p.EmbeddingDims > 4000 {
		return errs.ValidationReason("namespace.embedding_dims", "OUT_OF_RANGE", "between 8 and 4000")
	}
	return nil
}

func validState(s NamespaceState) bool {
	switch s {
	case StateCreating, StateActive, StateMoving, StateFrozen, StateRestoring, StateDeleting, StateDeleted:
		return true
	}
	return false
}

func validateSetState(from, to NamespaceState) error {
	if !validState(from) || !validState(to) {
		return errs.ValidationReason("state", "UNKNOWN_ENUM_VALUE", fmt.Sprintf("%q -> %q", from, to))
	}
	if from == StateDeleted {
		return errs.PreconditionFailed(PreconditionStateMismatch, "namespace", "a deleted namespace is a tombstone")
	}
	return nil
}

func stateMismatch(ns id.NamespaceID, from, actual NamespaceState) error {
	return errs.PreconditionFailed(PreconditionStateMismatch, ns.String(),
		fmt.Sprintf("expected state %s, found %s", from, actual))
}

func epochMismatch(ns id.NamespaceID, expected, actual id.Epoch) error {
	return errs.PreconditionFailed(PreconditionEpochMismatch, ns.String(),
		fmt.Sprintf("expected epoch %d, found %d", expected, actual))
}

func nsNotFound(ns id.NamespaceID) error {
	return errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_NAMESPACE, ns)
}

func tenantNotActive(t id.TenantID, state string) error {
	if state == "deleting" {
		return errs.PreconditionFailed("TENANT_DELETING", t.String(), "the tenant is being deleted")
	}
	return errs.PreconditionFailed(PreconditionTenantDeleted, t.String(), "the tenant is "+state)
}

func nameTaken(name string) *errs.Error {
	e := errs.OperationConflict(id.OperationID{}, id.OperationID{},
		memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_IDEMPOTENCY_KEY_REUSED)
	e.Msg = "namespace name " + name + " is taken"
	return e
}

type stringer string

func (s stringer) String() string { return string(s) }
