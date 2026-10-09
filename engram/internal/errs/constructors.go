package errs

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	errorsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/errors/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/id"
)

// Retry hints that are part of the contract: the fence try-lock refusal (N82) and the document try-lock refusal (N83).
const (
	FenceBusyRetry    = 200 * time.Millisecond
	DocumentBusyRetry = 100 * time.Millisecond
)

// Reasons and domains of the ErrorInfo details (section 4.1.6).
const (
	ErrorDomain          = "engram"
	ReasonPermanentLLM   = "PERMANENT_LLM_ERROR"
	ReasonMissingScope   = "MISSING_SCOPE"
	ReasonNotAllowed     = "NAMESPACE_NOT_ALLOWED" // PERMISSION_DENIED: same tenant, outside ns / ns_group
	ReasonWrongCell      = "WRONG_CELL"            // PERMISSION_DENIED: an engram.worker token for another cell (N167)
	reasonDocumentDelete = "DOCUMENT_DELETED"
)

// The machine-readable reasons of ValidationError.violations[].reason that the cross-cutting code produces.
const (
	ReasonRequired        = "REQUIRED"
	ReasonBadFormat       = "BAD_FORMAT"
	ReasonOutOfRange      = "OUT_OF_RANGE"
	ReasonMissingDeadline = "MISSING_DEADLINE"
	ReasonDeadlineTooLong = "DEADLINE_TOO_LONG"
	ReasonRequestIDNeeded = "REQUEST_ID_REQUIRED"
)

// The Precondition.type values the authorization path produces (section 4.1.1).
const (
	PreconditionNamespaceDeleting = "NAMESPACE_DELETING"
	PreconditionTenantDeleting    = "TENANT_DELETING"
)

// DeadlineField is the pseudo-field a missing or out-of-range deadline is reported on, on both transports.
const DeadlineField = "grpc-timeout"

// Validation is INVALID_ARGUMENT with one violation.
func Validation(field, desc string) *Error { return ValidationReason(field, "", desc) }

// Violation is one field-level failure for ValidationErrors.
type Violation struct{ Field, Reason, Description string }

// ValidationErrors is INVALID_ARGUMENT with every violation the request has (at least one), in the order given.
func ValidationErrors(vs ...Violation) *Error {
	out := make([]*memoryv1.FieldViolation, len(vs))
	msg := "invalid request"
	for i, v := range vs {
		out[i] = &memoryv1.FieldViolation{Field: v.Field, Reason: v.Reason, Description: v.Description}
	}
	if len(vs) > 0 {
		msg = vs[0].Field + ": " + vs[0].Description
	}
	if len(vs) > 1 {
		msg += fmt.Sprintf(" (and %d more)", len(vs)-1)
	}
	return &Error{Kind: KindValidation, Msg: msg, Public: &memoryv1.ValidationError{Violations: out}}
}

// ValidationReason is Validation with a machine-readable reason such as MISSING_DEADLINE or DEADLINE_TOO_LONG.
func ValidationReason(field, reason, desc string) *Error {
	return &Error{Kind: KindValidation, Msg: field + ": " + desc, Public: &memoryv1.ValidationError{
		Violations: []*memoryv1.FieldViolation{{Field: field, Reason: reason, Description: desc}}}}
}

// NotFound is NOT_FOUND for a typed resource (N140).
func NotFound(kind memoryv1.ResourceKind, ref fmt.Stringer) *Error {
	return notFound(kind, ref, "")
}

// NotFoundNamespace is NOT_FOUND{NAMESPACE} with the NamespaceRef the caller addressed. It is the one answer for "no
// such namespace", "a namespace of another tenant" and "a deleted namespace", byte for byte, so that the response is no
// existence oracle (section 4.1.1, N5): the detail echoes the request, never the catalog.
func NotFoundNamespace(tenant id.TenantID, ns id.NamespaceID) *Error {
	e := notFound(memoryv1.ResourceKind_RESOURCE_KIND_NAMESPACE, ns, "")
	e.Public.(*memoryv1.NotFound).Namespace = &memoryv1.NamespaceRef{TenantId: tenant.String(),
		NamespaceId: ns.String()}
	return e
}

// NotFoundTenant is NOT_FOUND{TENANT}: an unknown tenant, or another tenant's id on a tenant-bound method.
func NotFoundTenant(t id.TenantID) *Error {
	return notFound(memoryv1.ResourceKind_RESOURCE_KIND_TENANT, t, "")
}

// NotFoundDeleted is NotFound with reason DOCUMENT_DELETED: a version a delete tombstone covers (N136), or a fact whose
// invalidate row was purged with its document (N162).
func NotFoundDeleted(kind memoryv1.ResourceKind, ref fmt.Stringer) *Error {
	return notFound(kind, ref, reasonDocumentDelete)
}

func notFound(kind memoryv1.ResourceKind, ref fmt.Stringer, reason string) *Error {
	idText := ""
	if ref != nil {
		idText = ref.String()
	}
	return &Error{Kind: KindNotFound, Msg: fmt.Sprintf("%s %s not found", kindWord(kind), idText),
		Public: &memoryv1.NotFound{Kind: kind, Id: idText, Reason: reason}}
}

// kindWord renders RESOURCE_KIND_DOCUMENT_VERSION as "document version".
func kindWord(k memoryv1.ResourceKind) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(k.String(), "RESOURCE_KIND_"), "_", " "))
}

// QuotaExceeded is RESOURCE_EXHAUSTED with RetryInfo{retryAfter}.
func QuotaExceeded(quota string, scope memoryv1.QuotaScope, limit, current int64, retryAfter time.Duration) *Error {
	return &Error{Kind: KindQuotaExceeded, Msg: "quota exceeded: " + quota, Retry: retryAfter,
		Public: &memoryv1.QuotaExceeded{Quota: quota, Scope: scope, Limit: limit, Current: current,
			RetryAfter: durationpb.New(retryAfter)}}
}

// WrongShardOrEpoch is FAILED_PRECONDITION: the shard's ownership row disagrees with the resolved (shard, epoch).
func WrongShardOrEpoch(ns id.NamespaceID, expected, actual id.Epoch, st memoryv1.NamespaceState) *Error {
	return &Error{Kind: KindWrongShardOrEpoch, Msg: fmt.Sprintf("namespace %s: expected epoch %d, found %d (%s)", ns,
		expected, actual, st), Public: wrongShard(ns, expected, actual, st)}
}

// MovedOut is WrongShardOrEpoch{MOVED_OUT} carrying the internal MovedOutHint, which the router follows to the new
// owner without a catalog read (N93). The hint is the Internal detail and never leaves the process.
func MovedOut(ns id.NamespaceID, expected, actual id.Epoch, hint *errorsv1.MovedOutHint) *Error {
	e := WrongShardOrEpoch(ns, expected, actual, memoryv1.NamespaceState_NAMESPACE_STATE_MOVED_OUT)
	e.Internal = hint
	return e
}

func wrongShard(ns id.NamespaceID, expected, actual id.Epoch, st memoryv1.NamespaceState) *memoryv1.WrongShardOrEpoch {
	return &memoryv1.WrongShardOrEpoch{NamespaceId: ns.String(), ExpectedEpoch: int64(expected),
		ActualEpoch: int64(actual), NamespaceState: st}
}

// NamespaceFrozen is FAILED_PRECONDITION for writes during a move or restore freeze.
func NamespaceFrozen(ns id.NamespaceID, r memoryv1.FreezeReason, retryAfter time.Duration) *Error {
	return &Error{Kind: KindNamespaceFrozen, Msg: "namespace " + ns.String() + " is frozen", Retry: retryAfter,
		Public: &memoryv1.NamespaceFrozen{NamespaceId: ns.String(), Reason: r, RetryAfter: durationpb.New(retryAfter)}}
}

// NamespaceFrozenUntil is NamespaceFrozen for a move freeze, carrying the freeze deadline clients back off to (N160).
func NamespaceFrozenUntil(ns id.NamespaceID, retryAfter time.Duration, until time.Time) *Error {
	e := NamespaceFrozen(ns, memoryv1.FreezeReason_FREEZE_REASON_MOVE, retryAfter)
	e.Public.(*memoryv1.NamespaceFrozen).FrozenUntilEstimate = timestamppb.New(until.UTC())
	return e
}

// NamespaceNotReady is UNAVAILABLE: the target shard of a move is `ready` but not yet `active` (N125).
func NamespaceNotReady(ns id.NamespaceID, retryAfter time.Duration) *Error {
	return &Error{Kind: KindNamespaceNotReady, Msg: "namespace " + ns.String() + " is not ready", Retry: retryAfter,
		Public: &memoryv1.NamespaceNotReady{NamespaceId: ns.String(), RetryAfter: durationpb.New(retryAfter)}}
}

// FenceBusy is the refusal of the shared fence try-lock (N82). It renders as NamespaceFrozen{UNSPECIFIED, 200 ms}; the
// internal FenceBusy detail is kept for the retry policy.
func FenceBusy(ns id.NamespaceID) *Error {
	return &Error{Kind: KindFenceBusy, Msg: "namespace " + ns.String() + " fence is busy", Retry: FenceBusyRetry,
		Public:   &memoryv1.NamespaceFrozen{NamespaceId: ns.String(), RetryAfter: durationpb.New(FenceBusyRetry)},
		Internal: &errorsv1.FenceBusy{NamespaceId: ns.String(), RetryAfter: durationpb.New(FenceBusyRetry)}}
}

// DocumentBusy is the refusal of the shared per-document try-lock by CommitChunk (N83). It is never rendered.
func DocumentBusy(doc id.DocumentID) *Error {
	return &Error{Kind: KindDocumentBusy, Msg: "document " + doc.String() + " is busy", Retry: DocumentBusyRetry,
		Internal: &errorsv1.DocumentBusy{DocumentId: doc.String(), RetryAfter: durationpb.New(DocumentBusyRetry)}}
}

// InputBlobMissing is the activity error of CommitChunk reading a purged cache or staging blob (N100).
func InputBlobMissing(blobKey, input string, contentHash [32]byte, attempt int) *Error {
	return &Error{Kind: KindInputBlobMissing, Msg: "input blob " + blobKey + " is missing",
		Internal: &errorsv1.InputBlobMissing{BlobKey: blobKey, Input: input,
			ContentHash: hex.EncodeToString(contentHash[:]), Attempt: int32(attempt)}} //nolint:gosec
}

// OperationConflict is ABORTED, or ALREADY_EXISTS for IDEMPOTENCY_KEY_REUSED.
func OperationConflict(op, existing id.OperationID, why memoryv1.OperationConflictReason) *Error {
	return &Error{Kind: KindOperationConflict, Msg: "operation conflict: " + why.String(),
		Public: &memoryv1.OperationConflict{OperationId: op.String(), ExistingOperationId: existing.String(),
			Reason: why}}
}

// PreconditionFailed is FAILED_PRECONDITION with one violation (typ is the machine-readable type, e.g. ETAG_MISMATCH).
func PreconditionFailed(typ, subject, desc string) *Error {
	return &Error{Kind: KindPreconditionFailed, Msg: typ + ": " + desc, Public: &memoryv1.PreconditionFailed{
		Violations: []*memoryv1.Precondition{{Type: typ, Subject: subject, Description: desc}}}}
}

// Unavailable is UNAVAILABLE with RetryInfo{retryAfter}.
func Unavailable(desc string, retryAfter time.Duration) *Error {
	return &Error{Kind: KindUnavailable, Msg: desc, Retry: retryAfter}
}

// shardNotLocal is the Cause that marks an Unavailable as "this shard is served by another cell".
type shardNotLocal struct{ cell string }

func (s shardNotLocal) Error() string { return "shard is not local; served by cell " + s.cell }

// ShardNotLocal is KindUnavailable; the router's Forwarder consumes it and forwards to cell (D4, N71).
func ShardNotLocal(cell string) *Error {
	return &Error{Kind: KindUnavailable, Msg: "shard not local", Retry: 0, Cause: shardNotLocal{cell: cell}}
}

// ShardNotLocalCell returns the cell named by a ShardNotLocal error anywhere in err's chain.
func ShardNotLocalCell(err error) (cell string, ok bool) {
	var s shardNotLocal
	if errors.As(err, &s) {
		return s.cell, true
	}
	return "", false
}

// PermanentLLM is INTERNAL and never retried: gateway 4xx, wrong embedding dims, schema-invalid output after one
// repair.
func PermanentLLM(model, status string, cause error) *Error {
	return &Error{Kind: KindPermanentLLM, Msg: "permanent LLM error (" + model + ", " + status + ")", Cause: cause,
		Public: &errdetails.ErrorInfo{Reason: ReasonPermanentLLM, Domain: ErrorDomain,
			Metadata: map[string]string{"model": model, "status": status}}}
}

// Unauthenticated is UNAUTHENTICATED: a missing, invalid or expired JWT.
func Unauthenticated(desc string) *Error { return &Error{Kind: KindUnauthenticated, Msg: desc} }

// PermissionDenied is PERMISSION_DENIED + ErrorInfo{MISSING_SCOPE} naming the scope the method needs.
func PermissionDenied(scope string) *Error {
	return &Error{Kind: KindPermissionDenied, Msg: "missing scope " + scope, Public: &errdetails.ErrorInfo{
		Reason: ReasonMissingScope, Domain: ErrorDomain, Metadata: map[string]string{"scope": scope}}}
}

// NamespaceNotAllowed is PERMISSION_DENIED for a namespace of the caller's own tenant that the token's `ns` and
// `ns_group` claims do not admit (within a tenant existence is not secret, so the answer differs from NOT_FOUND).
func NamespaceNotAllowed(ns id.NamespaceID) *Error {
	return &Error{Kind: KindPermissionDenied, Msg: "namespace " + ns.String() + " is not in the token's allowlist",
		Public: &errdetails.ErrorInfo{Reason: ReasonNotAllowed, Domain: ErrorDomain,
			Metadata: map[string]string{"namespace_id": ns.String()}}}
}

// WrongCell is PERMISSION_DENIED for an engram.worker token whose `cell` claim is not the cell of the shard (N167).
func WrongCell(cell string) *Error {
	return &Error{Kind: KindPermissionDenied, Msg: "the token is bound to cell " + cell, Public: &errdetails.ErrorInfo{
		Reason: ReasonWrongCell, Domain: ErrorDomain, Metadata: map[string]string{"cell": cell}}}
}

// DeadlineExceeded is DEADLINE_EXCEEDED.
func DeadlineExceeded(desc string, cause error) *Error {
	return &Error{Kind: KindDeadline, Msg: desc, Cause: cause}
}

// Internal is INTERNAL; the message is redacted when rendered (only the trace id reaches the caller).
func Internal(desc string, cause error) *Error {
	return &Error{Kind: KindInternal, Msg: desc, Cause: cause}
}

// Wrap builds an error of any kind around a cause, with no typed detail.
func Wrap(kind Kind, msg string, cause error) *Error {
	return &Error{Kind: kind, Msg: msg, Cause: cause}
}
