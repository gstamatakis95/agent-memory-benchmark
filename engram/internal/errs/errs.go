// Package errs holds the typed errors of Engram and their one mapping to gRPC, Connect and Temporal (PLAN.md section
// 2.4; register N1, N128, N139, N140). It is a leaf: store, services and workflows produce typed errors but may not
// import internal/api. It imports the generated detail messages of memory.v1 (public) and engram.internal.errors.v1
// (internal), the gRPC status packages and connect, and renders each error exactly once, in ToStatus and ToConnect.
package errs

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Kind is the stable classification. Public kinds are rendered to callers; the three internal kinds are consumed by the
// router or the retry policy and are dropped by the interceptor (N128).
type Kind int

// The error kinds, in the order of the section 2.4 table. The zero value is invalid on purpose.
const (
	KindValidation        Kind = iota + 1 // INVALID_ARGUMENT + memoryv1.ValidationError
	KindNotFound                          // NOT_FOUND + memoryv1.NotFound
	KindQuotaExceeded                     // RESOURCE_EXHAUSTED + memoryv1.QuotaExceeded + RetryInfo
	KindWrongShardOrEpoch                 // FAILED_PRECONDITION + memoryv1.WrongShardOrEpoch (epochs and state only)
	// KindNamespaceFrozen is FAILED_PRECONDITION + memoryv1.NamespaceFrozen{reason: MOVE|RESTORE|UNSPECIFIED}; a delete
	// freeze is KindPreconditionFailed{NAMESPACE_DELETING} (N139).
	KindNamespaceFrozen
	// KindNamespaceNotReady is UNAVAILABLE + memoryv1.NamespaceNotReady (target shard `ready`, N125).
	KindNamespaceNotReady
	KindOperationConflict  // ABORTED | ALREADY_EXISTS + memoryv1.OperationConflict{reason}
	KindPreconditionFailed // FAILED_PRECONDITION + memoryv1.PreconditionFailed
	KindUnavailable        // UNAVAILABLE + RetryInfo
	KindPermanentLLM       // INTERNAL (never retried) + ErrorInfo{PERMANENT_LLM_ERROR}
	KindUnauthenticated    // UNAUTHENTICATED
	KindPermissionDenied   // PERMISSION_DENIED + ErrorInfo{MISSING_SCOPE}
	KindDeadline           // DEADLINE_EXCEEDED
	KindInternal           // INTERNAL

	KindFenceBusy        // internal: the shared try-lock was refused; rendered as NamespaceFrozen{UNSPECIFIED, 200 ms}
	KindDocumentBusy     // internal, ABORTED on the write path only; retried by P-frozen at 100 ms (N83)
	KindInputBlobMissing // internal, activity error: the chunk sub-pipeline re-runs extract + embed at most 2x (N100)
)

var kindNames = [...]string{
	KindValidation: "Validation", KindNotFound: "NotFound", KindQuotaExceeded: "QuotaExceeded",
	KindWrongShardOrEpoch: "WrongShardOrEpoch", KindNamespaceFrozen: "NamespaceFrozen",
	KindNamespaceNotReady: "NamespaceNotReady", KindOperationConflict: "OperationConflict",
	KindPreconditionFailed: "PreconditionFailed", KindUnavailable: "Unavailable", KindPermanentLLM: "PermanentLLM",
	KindUnauthenticated: "Unauthenticated", KindPermissionDenied: "PermissionDenied", KindDeadline: "Deadline",
	KindInternal: "Internal", KindFenceBusy: "FenceBusy", KindDocumentBusy: "DocumentBusy",
	KindInputBlobMissing: "InputBlobMissing",
}

// String returns the kind name.
func (k Kind) String() string {
	if k <= 0 || int(k) >= len(kindNames) {
		return fmt.Sprintf("Kind(%d)", int(k))
	}
	return kindNames[k]
}

// IsInternal reports whether the kind never crosses the API boundary as its own detail (N128).
func (k Kind) IsInternal() bool {
	return k == KindFenceBusy || k == KindDocumentBusy || k == KindInputBlobMissing
}

// Code is the gRPC code of the kind (section 2.4 table). KindOperationConflict is ABORTED here; the error's own reason
// selects ALREADY_EXISTS for IDEMPOTENCY_KEY_REUSED (see (*Error).Code).
func (k Kind) Code() codes.Code {
	switch k {
	case KindValidation:
		return codes.InvalidArgument
	case KindNotFound:
		return codes.NotFound
	case KindQuotaExceeded:
		return codes.ResourceExhausted
	case KindWrongShardOrEpoch, KindNamespaceFrozen, KindPreconditionFailed, KindFenceBusy:
		return codes.FailedPrecondition
	case KindNamespaceNotReady, KindUnavailable:
		return codes.Unavailable
	case KindOperationConflict, KindDocumentBusy:
		return codes.Aborted
	case KindUnauthenticated:
		return codes.Unauthenticated
	case KindPermissionDenied:
		return codes.PermissionDenied
	case KindDeadline:
		return codes.DeadlineExceeded
	case KindPermanentLLM, KindInternal, KindInputBlobMissing:
		return codes.Internal
	}
	return codes.Unknown
}

// Error is the one error type of the system. Build it with the constructors of this package.
type Error struct {
	Kind     Kind
	Msg      string
	Public   proto.Message // typed detail from memory/v1/errors.proto, may be nil
	Internal proto.Message // typed detail from engram/internal/errors/v1 (MovedOutHint, FenceBusy, ...), never rendered
	Retry    time.Duration // 0 = no RetryInfo
	Cause    error
}

// Error implements error. The text is for logs; callers see only what ToStatus renders.
func (e *Error) Error() string {
	msg := e.Msg
	if msg == "" {
		msg = e.Kind.String()
	}
	if e.Cause != nil {
		return msg + ": " + e.Cause.Error()
	}
	return msg
}

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.Cause }

// Code is the gRPC code of this error: the kind's code, except that an OperationConflict with reason
// IDEMPOTENCY_KEY_REUSED is ALREADY_EXISTS (section 2.4, 4.1.6).
func (e *Error) Code() codes.Code {
	if e.Kind == KindOperationConflict && e.conflictReusedKey() {
		return codes.AlreadyExists
	}
	return e.Kind.Code()
}

// GRPCStatus renders the Public detail only (and RetryInfo or ErrorInfo): the Internal detail never leaves the process
// through this method. It makes *Error usable as a gRPC handler's return value.
func (e *Error) GRPCStatus() *status.Status { return e.status(false) }

// As finds the first *Error in err's chain.
func as(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// Is reports whether err's chain contains an *Error of kind k.
func Is(err error, k Kind) bool {
	e, ok := as(err)
	return ok && e.Kind == k
}

// KindOf returns the kind of the first *Error in err's chain, or 0 when there is none.
func KindOf(err error) Kind {
	if e, ok := as(err); ok {
		return e.Kind
	}
	return 0
}

// IsRetryable reports whether a retry of the same request may succeed: Unavailable, Deadline, NamespaceFrozen,
// NamespaceNotReady, FenceBusy and DocumentBusy (bounded by the caller: the API loop, P-frozen). An error that is not
// an *Error is not retryable.
func IsRetryable(err error) bool {
	switch KindOf(err) {
	case KindUnavailable, KindDeadline, KindNamespaceFrozen, KindNamespaceNotReady, KindFenceBusy, KindDocumentBusy:
		return true
	}
	return false
}

// TemporalType is the ApplicationError type of err, the key of the Temporal non-retryable list (never matched on
// message text): "WrongShardOrEpoch", "NamespaceFrozen", "NamespaceNotReady", "DocumentBusy", "InputBlobMissing",
// "ValidationError", "PermanentLLMError", "QuotaExceeded"; the other kinds use their kind name. A FenceBusy is a
// "NamespaceFrozen" (that is what it renders as). An error that is not an *Error is "Internal".
func TemporalType(err error) string {
	e, ok := as(err)
	if !ok {
		return KindInternal.String()
	}
	switch e.Kind {
	case KindValidation:
		return "ValidationError"
	case KindPermanentLLM:
		return "PermanentLLMError"
	case KindFenceBusy:
		return KindNamespaceFrozen.String()
	}
	return e.Kind.String()
}
