package errs

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	errorsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/errors/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
)

// RedactedMessage is what a caller sees for an error that is not a typed *Error: the cause stays in the logs, keyed by
// the trace id (section 4.1.6).
const RedactedMessage = "internal error"

func (e *Error) conflictReusedKey() bool {
	oc, ok := e.Public.(*memoryv1.OperationConflict)
	return ok && oc.GetReason() == memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_IDEMPOTENCY_KEY_REUSED
}

// message is what the status carries: the typed message for typed kinds, a redaction for INTERNAL.
func (e *Error) message() string {
	if e.Kind == KindInternal || e.Kind == KindInputBlobMissing {
		return RedactedMessage
	}
	return e.Msg
}

// details lists the detail messages of e in wire order: the Engram detail (at most one), then RetryInfo, then, when
// withInternal is set, the Internal detail. The same list feeds the gRPC status and the Connect error, so the two
// renderings carry byte-identical details.
func (e *Error) details(withInternal bool) []*anypb.Any {
	var ms []proto.Message
	if e.Public != nil {
		ms = append(ms, e.Public)
	}
	if e.Retry > 0 {
		ms = append(ms, &errdetails.RetryInfo{RetryDelay: durationpb.New(e.Retry)})
	}
	if withInternal && e.Internal != nil {
		ms = append(ms, e.Internal)
	}
	out := make([]*anypb.Any, 0, len(ms))
	for _, m := range ms {
		a, err := anypb.New(m)
		if err != nil {
			continue // a detail that cannot be marshalled is dropped; the code and message still carry the error
		}
		out = append(out, a)
	}
	return out
}

func (e *Error) status(withInternal bool) *status.Status {
	return status.FromProto(&spb.Status{Code: int32(e.Code()), Message: e.message(), Details: e.details(withInternal)})
}

// ToStatus renders err once for a gRPC response: the code of its kind, its Public detail and RetryInfo; the Internal
// detail is dropped (N128). Unknown errors become INTERNAL with a redacted message; a context deadline or cancellation
// keeps its own code. A nil err renders as nil.
func ToStatus(err error) *status.Status {
	if err == nil {
		return nil
	}
	if e, ok := as(err); ok {
		return e.status(false)
	}
	return foreign(err)
}

// ToInternalStatus is ToStatus for an internal hop (router.Forward between cells, activities): it also carries the
// Internal detail, so that FromStatus on the peer restores it. It must never be used for a response to a client.
func ToInternalStatus(err error) *status.Status {
	if err == nil {
		return nil
	}
	if e, ok := as(err); ok {
		return e.status(true)
	}
	return foreign(err)
}

func foreign(err error) *status.Status {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.New(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.New(codes.Canceled, "canceled")
	}
	return status.New(codes.Internal, RedactedMessage)
}

// ToConnect renders err once for a Connect response: the same code, message and details as ToStatus, attached with
// AddDetail, so a detail is byte-identical on both transports.
func ToConnect(err error) *connect.Error {
	if err == nil {
		return nil
	}
	st := ToStatus(err)
	// gRPC and Connect use the same numeric codes.
	ce := connect.NewError(connect.Code(st.Code()), errors.New(st.Message())) //nolint:gosec
	for _, a := range st.Proto().GetDetails() {
		if d, derr := connect.NewErrorDetail(a); derr == nil {
			ce.AddDetail(d)
		}
	}
	return ce
}

// FromConnect is the client-side inverse of ToConnect (adapters, tests).
func FromConnect(ce *connect.Error) *Error {
	if ce == nil {
		return nil
	}
	p := &spb.Status{Code: int32(ce.Code()), Message: ce.Message()} //nolint:gosec // codes coincide
	for _, d := range ce.Details() {
		if m, err := d.Value(); err == nil {
			if a, aerr := anypb.New(m); aerr == nil {
				p.Details = append(p.Details, a)
			}
		}
	}
	return FromStatus(status.FromProto(p))
}

// FromStatus is the client-side inverse of ToStatus (router.Forward, engram-mcp): it rebuilds the kind from the detail
// type (and the code), and keeps an Internal detail when the peer sent one (ToInternalStatus). A status without a known
// detail maps by code alone. A nil or OK status yields nil.
func FromStatus(st *status.Status) *Error {
	if st == nil || st.Code() == codes.OK {
		return nil
	}
	e := &Error{Msg: st.Message(), Kind: kindForCode(st.Code())}
	for _, a := range st.Proto().GetDetails() {
		m, err := a.UnmarshalNew()
		if err != nil {
			continue
		}
		switch d := m.(type) {
		case *memoryv1.ValidationError:
			e.Kind, e.Public = KindValidation, d
		case *memoryv1.NotFound:
			e.Kind, e.Public = KindNotFound, d
		case *memoryv1.QuotaExceeded:
			e.Kind, e.Public = KindQuotaExceeded, d
		case *memoryv1.WrongShardOrEpoch:
			e.Kind, e.Public = KindWrongShardOrEpoch, d
		case *memoryv1.NamespaceFrozen:
			e.Kind, e.Public = KindNamespaceFrozen, d
		case *memoryv1.NamespaceNotReady:
			e.Kind, e.Public = KindNamespaceNotReady, d
		case *memoryv1.OperationConflict:
			e.Kind, e.Public = KindOperationConflict, d
		case *memoryv1.PreconditionFailed:
			e.Kind, e.Public = KindPreconditionFailed, d
		case *errdetails.ErrorInfo:
			e.Public = d
			if d.GetReason() == ReasonPermanentLLM {
				e.Kind = KindPermanentLLM
			}
		case *errdetails.RetryInfo:
			e.Retry = d.GetRetryDelay().AsDuration()
		case *errorsv1.FenceBusy:
			e.Kind, e.Internal = KindFenceBusy, d
		case *errorsv1.DocumentBusy:
			e.Kind, e.Internal = KindDocumentBusy, d
		case *errorsv1.InputBlobMissing:
			e.Kind, e.Internal = KindInputBlobMissing, d
		case *errorsv1.MovedOutHint:
			e.Internal = d
		}
	}
	return e
}

func kindForCode(c codes.Code) Kind {
	switch c {
	case codes.InvalidArgument:
		return KindValidation
	case codes.NotFound:
		return KindNotFound
	case codes.ResourceExhausted:
		return KindQuotaExceeded
	case codes.FailedPrecondition:
		return KindPreconditionFailed
	case codes.Unavailable:
		return KindUnavailable
	case codes.Aborted, codes.AlreadyExists:
		return KindOperationConflict
	case codes.Unauthenticated:
		return KindUnauthenticated
	case codes.PermissionDenied:
		return KindPermissionDenied
	case codes.DeadlineExceeded:
		return KindDeadline
	}
	return KindInternal
}
