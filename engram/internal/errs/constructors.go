package errs

import (
	"encoding/hex"
	"errors"
	"fmt"
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
	reasonDocumentDelete = "DOCUMENT_DELETED"
)

// Validation is INVALID_ARGUMENT with one violation.
func Validation(field, desc string) *Error { return ValidationReason(field, "", desc) }

// ValidationReason is Validation with a machine-readable reason such as MISSING_DEADLINE or DEADLINE_TOO_LONG.
func ValidationReason(field, reason, desc string) *Error {
	return &Error{Kind: KindValidation, Msg: field + ": " + desc, Public: &memoryv1.ValidationError{
		Violations: []*memoryv1.FieldViolation{{Field: field, Reason: reason, Description: desc}}}}
}

// NotFound is NOT_FOUND for a typed resource (N140).
func NotFound(kind memoryv1.ResourceKind, ref fmt.Stringer) *Error {
	return notFound(kind, ref, "")
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
	return &Error{Kind: KindNotFound, Msg: fmt.Sprintf("%s %s not found", kind, idText),
		Public: &memoryv1.NotFound{Kind: kind, Id: idText, Reason: reason}}
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
