package errs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	errorsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/errors/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

const (
	freezeMove     = memoryv1.FreezeReason_FREEZE_REASON_MOVE
	rkDocument     = memoryv1.ResourceKind_RESOURCE_KIND_DOCUMENT
	scopeNamespace = memoryv1.QuotaScope_QUOTA_SCOPE_NAMESPACE
	reasonReused   = memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_IDEMPOTENCY_KEY_REUSED
)

func wantName(m proto.Message) string { return string(m.ProtoReflect().Descriptor().FullName()) }

var (
	ns  = id.NamespaceID{1}
	op1 = id.OperationID{2}
	op2 = id.OperationID{3}
)

// TestKindCodeTable pins the gRPC code, retryability and Temporal type of every kind to the table of section 2.4.
func TestKindCodeTable(t *testing.T) {
	tests := []struct {
		name      string
		err       *errs.Error
		kind      errs.Kind
		code      codes.Code
		retryable bool
		temporal  string
		detail    proto.Message // the Engram detail that must be on the wire, nil for none
	}{
		{"validation", errs.Validation("grpc-timeout", "missing"), errs.KindValidation, codes.InvalidArgument, false,
			"ValidationError", &memoryv1.ValidationError{}},
		{"not found", errs.NotFound(rkDocument, id.DocumentID("d")), errs.KindNotFound, codes.NotFound, false,
			"NotFound", &memoryv1.NotFound{}},
		{"quota", errs.QuotaExceeded("recalls_per_min", memoryv1.QuotaScope_QUOTA_SCOPE_TENANT, 10, 11, time.Second),
			errs.KindQuotaExceeded, codes.ResourceExhausted, false, "QuotaExceeded", &memoryv1.QuotaExceeded{}},
		{"wrong shard", errs.WrongShardOrEpoch(ns, 2, 3, memoryv1.NamespaceState_NAMESPACE_STATE_ACTIVE),
			errs.KindWrongShardOrEpoch, codes.FailedPrecondition, false, "WrongShardOrEpoch",
			&memoryv1.WrongShardOrEpoch{}},
		{"moved out", errs.MovedOut(ns, 2, 3, &errorsv1.MovedOutHint{NamespaceId: ns.String(), TargetShardId: 9}),
			errs.KindWrongShardOrEpoch, codes.FailedPrecondition, false, "WrongShardOrEpoch",
			&memoryv1.WrongShardOrEpoch{}},
		{"frozen", errs.NamespaceFrozen(ns, freezeMove, time.Second), errs.KindNamespaceFrozen,
			codes.FailedPrecondition, true, "NamespaceFrozen", &memoryv1.NamespaceFrozen{}},
		{"not ready", errs.NamespaceNotReady(ns, time.Second), errs.KindNamespaceNotReady, codes.Unavailable, true,
			"NamespaceNotReady", &memoryv1.NamespaceNotReady{}},
		{"conflict aborted", errs.OperationConflict(op1, op2,
			memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_NAMESPACE_BUSY), errs.KindOperationConflict,
			codes.Aborted, false, "OperationConflict", &memoryv1.OperationConflict{}},
		{"conflict reused key", errs.OperationConflict(op1, op2, reasonReused), errs.KindOperationConflict,
			codes.AlreadyExists, false, "OperationConflict", &memoryv1.OperationConflict{}},
		{"precondition", errs.PreconditionFailed("ETAG_MISMATCH", "namespace", "stale"), errs.KindPreconditionFailed,
			codes.FailedPrecondition, false, "PreconditionFailed", &memoryv1.PreconditionFailed{}},
		{"unavailable", errs.Unavailable("catalog down", 2*time.Second), errs.KindUnavailable, codes.Unavailable, true,
			"Unavailable", nil},
		{"permanent llm", errs.PermanentLLM("m", "400", errors.New("bad")), errs.KindPermanentLLM, codes.Internal,
			false, "PermanentLLMError", &errdetails.ErrorInfo{}},
		{"unauthenticated", errs.Unauthenticated("expired"), errs.KindUnauthenticated, codes.Unauthenticated, false,
			"Unauthenticated", nil},
		{"permission denied", errs.PermissionDenied("memory.write"), errs.KindPermissionDenied, codes.PermissionDenied,
			false, "PermissionDenied", &errdetails.ErrorInfo{}},
		{"deadline", errs.DeadlineExceeded("late", nil), errs.KindDeadline, codes.DeadlineExceeded, true, "Deadline",
			nil},
		{"internal", errs.Internal("bug", errors.New("x")), errs.KindInternal, codes.Internal, false, "Internal", nil},
		{"fence busy renders as frozen", errs.FenceBusy(ns), errs.KindFenceBusy, codes.FailedPrecondition, true,
			"NamespaceFrozen", &memoryv1.NamespaceFrozen{}},
		{"document busy", errs.DocumentBusy("d"), errs.KindDocumentBusy, codes.Aborted, true, "DocumentBusy", nil},
		{"input blob missing", errs.InputBlobMissing("k", "extract", [32]byte{1}, 1), errs.KindInputBlobMissing,
			codes.Internal, false, "InputBlobMissing", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("wrapped: %w", tc.err) // every helper must see through a wrap
			if !errs.Is(err, tc.kind) || errs.Is(err, tc.kind+100) {
				t.Errorf("Is(%v) mismatch", tc.kind)
			}
			if got := errs.IsRetryable(err); got != tc.retryable {
				t.Errorf("IsRetryable = %v, want %v", got, tc.retryable)
			}
			if got := errs.TemporalType(err); got != tc.temporal {
				t.Errorf("TemporalType = %q, want %q", got, tc.temporal)
			}
			st := errs.ToStatus(err)
			if st.Code() != tc.code {
				t.Errorf("ToStatus code = %v, want %v", st.Code(), tc.code)
			}
			if tc.err.GRPCStatus().Code() != tc.code || tc.err.Code() != tc.code {
				t.Error("GRPCStatus and Code must agree with ToStatus")
			}
			var engram []string
			for _, a := range st.Proto().GetDetails() {
				if a.MessageName() != "google.rpc.RetryInfo" {
					engram = append(engram, string(a.MessageName()))
				}
			}
			switch {
			case tc.detail == nil && len(engram) != 0:
				t.Errorf("unexpected details %v", engram)
			case tc.detail != nil && (len(engram) != 1 || engram[0] != wantName(tc.detail)):
				t.Errorf("details = %v, want exactly one %T", engram, tc.detail)
			}
		})
	}
}

func TestKindString(t *testing.T) {
	if errs.KindFenceBusy.String() != "FenceBusy" || errs.Kind(0).String() != "Kind(0)" ||
		errs.Kind(99).String() != "Kind(99)" {
		t.Error("Kind.String")
	}
	if !errs.KindFenceBusy.IsInternal() || !errs.KindDocumentBusy.IsInternal() ||
		!errs.KindInputBlobMissing.IsInternal() || errs.KindInternal.IsInternal() {
		t.Error("exactly the three N128 kinds are internal")
	}
}

func TestRetryInfoAndInternalDetailDropped(t *testing.T) {
	hint := &errorsv1.MovedOutHint{NamespaceId: ns.String(), TargetShardId: 9, NextEpoch: 4}
	e := errs.MovedOut(ns, 3, 3, hint)
	e.Retry = 50 * time.Millisecond

	st := errs.ToStatus(e)
	var sawRetry bool
	for _, a := range st.Proto().GetDetails() {
		if a.MessageName() == proto.MessageName(hint) {
			t.Fatal("the internal MovedOutHint must never be rendered (N128)")
		}
		if a.MessageName() == "google.rpc.RetryInfo" {
			sawRetry = true
		}
	}
	if !sawRetry {
		t.Error("a non-zero Retry must add RetryInfo")
	}
	w, ok := st.Details()[0].(*memoryv1.WrongShardOrEpoch)
	if !ok || w.GetNamespaceState() != memoryv1.NamespaceState_NAMESPACE_STATE_MOVED_OUT {
		t.Errorf("public detail = %#v, want WrongShardOrEpoch{MOVED_OUT}", st.Details()[0])
	}

	// The internal hop keeps it, and FromStatus restores it for the router.
	back := errs.FromStatus(errs.ToInternalStatus(e))
	got, ok := back.Internal.(*errorsv1.MovedOutHint)
	if !ok || !proto.Equal(got, hint) || back.Kind != errs.KindWrongShardOrEpoch || back.Retry != 50*time.Millisecond {
		t.Errorf("internal round trip = %+v", back)
	}
	// ...but the client-facing status does not.
	if lost := errs.FromStatus(st); lost.Internal != nil {
		t.Error("FromStatus of a client-facing status must not invent an Internal detail")
	}
}

func TestFenceBusyRendersAsNamespaceFrozen(t *testing.T) {
	st := errs.ToStatus(errs.FenceBusy(ns))
	var nf *memoryv1.NamespaceFrozen
	var retry *errdetails.RetryInfo
	for _, d := range st.Details() {
		switch v := d.(type) {
		case *memoryv1.NamespaceFrozen:
			nf = v
		case *errdetails.RetryInfo:
			retry = v
		case *errorsv1.FenceBusy:
			t.Fatal("FenceBusy detail must not be rendered")
		}
	}
	if nf == nil || nf.GetReason() != memoryv1.FreezeReason_FREEZE_REASON_UNSPECIFIED ||
		nf.GetRetryAfter().AsDuration() != 200*time.Millisecond ||
		retry.GetRetryDelay().AsDuration() != 200*time.Millisecond {
		t.Errorf("FenceBusy must render as NamespaceFrozen{UNSPECIFIED, 200 ms} + RetryInfo: %v", st.Details())
	}
	if round := errs.FromStatus(errs.ToInternalStatus(errs.FenceBusy(ns))); round.Kind != errs.KindFenceBusy {
		t.Errorf("internal hop must restore KindFenceBusy, got %v", round.Kind)
	}
}

func TestUnknownErrorIsRedacted(t *testing.T) {
	st := errs.ToStatus(errors.New("pq: password authentication failed for user engram_app"))
	if st.Code() != codes.Internal || st.Message() != errs.RedactedMessage || len(st.Details()) != 0 {
		t.Errorf("unknown error = %v %q %v; want INTERNAL, redacted, no details", st.Code(), st.Message(), st.Details())
	}
	typed := errs.ToStatus(errs.Internal("invariant broken: ns 42", errors.New("secret cause")))
	if typed.Message() != errs.RedactedMessage {
		t.Errorf("an Internal error's message must be redacted, got %q", typed.Message())
	}
	if errs.ToStatus(nil) != nil || errs.ToConnect(nil) != nil || errs.FromStatus(nil) != nil {
		t.Error("nil must map to nil")
	}
	if got := errs.ToStatus(fmt.Errorf("x: %w", context.DeadlineExceeded)).Code(); got != codes.DeadlineExceeded {
		t.Errorf("context deadline = %v", got)
	}
	if got := errs.ToStatus(context.Canceled).Code(); got != codes.Canceled {
		t.Errorf("context canceled = %v", got)
	}
	if errs.IsRetryable(errors.New("plain")) || errs.Is(errors.New("plain"), errs.KindInternal) ||
		errs.TemporalType(errors.New("plain")) != "Internal" {
		t.Error("a plain error is not an errs kind")
	}
}

// TestConnectRenderingIsByteIdentical is the transport-parity test: one error rendered by ToStatus (gRPC) and ToConnect
// carries the same code, message and, byte for byte, the same details in the same order.
func TestConnectRenderingIsByteIdentical(t *testing.T) {
	errsToRender := []error{
		errs.QuotaExceeded("recalls_per_min", scopeNamespace, 100, 101, 1500*time.Millisecond),
		errs.NamespaceFrozenUntil(ns, 30*time.Second, time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)),
		errs.NotFoundDeleted(memoryv1.ResourceKind_RESOURCE_KIND_DOCUMENT_VERSION, id.DocumentID("doc")),
		errs.OperationConflict(op1, op2, reasonReused),
		errs.PermissionDenied("memory.write"),
		errs.Unavailable("catalog down", 2*time.Second),
		errs.MovedOut(ns, 1, 1, &errorsv1.MovedOutHint{NamespaceId: ns.String()}),
	}
	for _, err := range errsToRender {
		t.Run(errs.TemporalType(err), func(t *testing.T) {
			st := errs.ToStatus(err)
			ce := errs.ToConnect(err)
			if uint32(ce.Code()) != uint32(st.Code()) || ce.Message() != st.Message() {
				t.Fatalf("connect %v %q vs grpc %v %q", ce.Code(), ce.Message(), st.Code(), st.Message())
			}
			grpcDetails := st.Proto().GetDetails()
			connectDetails := ce.Details()
			if len(grpcDetails) == 0 || len(grpcDetails) != len(connectDetails) {
				t.Fatalf("%d grpc details vs %d connect details", len(grpcDetails), len(connectDetails))
			}
			for i := range grpcDetails {
				if connectDetails[i].Type() != string(grpcDetails[i].MessageName()) {
					t.Errorf("detail %d type %q vs %q", i, connectDetails[i].Type(), grpcDetails[i].MessageName())
				}
				if !bytes.Equal(connectDetails[i].Bytes(), grpcDetails[i].GetValue()) {
					t.Errorf("detail %d bytes differ: %x vs %x", i, connectDetails[i].Bytes(),
						grpcDetails[i].GetValue())
				}
			}
			// The Connect side decodes back to an equal error.
			back := errs.FromConnect(ce)
			if back.Kind != errs.KindOf(err) || back.Code() != st.Code() {
				t.Errorf("FromConnect = kind %v code %v", back.Kind, back.Code())
			}
		})
	}
}

func TestFromStatusRoundTrip(t *testing.T) {
	for _, e := range []*errs.Error{
		errs.Validation("f", "bad"), errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_PAGE, id.PageID{9}),
		errs.NamespaceNotReady(ns, time.Second), errs.PreconditionFailed("T", "s", "d"),
		errs.PermanentLLM("m", "400", nil), errs.Unavailable("x", time.Second),
		errs.OperationConflict(op1, op2, memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_PAGE_REFRESHING),
	} {
		back := errs.FromStatus(errs.ToStatus(e))
		if back.Kind != e.Kind || back.Code() != e.Code() || back.Retry != e.Retry {
			t.Errorf("%v: round trip = kind %v code %v retry %v", e.Kind, back.Kind, back.Code(), back.Retry)
		}
		if e.Public != nil && !proto.Equal(back.Public, e.Public) {
			t.Errorf("%v: public detail changed: %v vs %v", e.Kind, back.Public, e.Public)
		}
	}
	if errs.FromStatus(errs.ToStatus(errs.Unauthenticated("x"))).Kind != errs.KindUnauthenticated {
		t.Error("a status without a known detail maps by code")
	}
}

func TestShardNotLocal(t *testing.T) {
	err := fmt.Errorf("route: %w", errs.ShardNotLocal("cell-b"))
	cell, ok := errs.ShardNotLocalCell(err)
	if !ok || cell != "cell-b" || !errs.Is(err, errs.KindUnavailable) || !errs.IsRetryable(err) {
		t.Errorf("ShardNotLocal = %q %v", cell, ok)
	}
	if _, ok := errs.ShardNotLocalCell(errs.Unavailable("x", 0)); ok {
		t.Error("a plain Unavailable is not ShardNotLocal")
	}
}

func TestErrorTextAndUnwrap(t *testing.T) {
	cause := errors.New("disk on fire")
	e := errs.Wrap(errs.KindUnavailable, "shard down", cause)
	if e.Error() != "shard down: disk on fire" || !errors.Is(e, cause) {
		t.Errorf("Error() = %q", e.Error())
	}
	if (&errs.Error{Kind: errs.KindNotFound}).Error() != "NotFound" {
		t.Error("empty message falls back to the kind name")
	}
}

func TestNotFoundDetail(t *testing.T) {
	d := errs.NotFoundDeleted(rkDocument, id.DocumentID("doc-1")).Public.(*memoryv1.NotFound)
	if d.GetId() != "doc-1" || d.GetReason() != "DOCUMENT_DELETED" || d.GetKind() != rkDocument {
		t.Errorf("detail = %v", d)
	}
	nilRef := errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_MEMORY, nil)
	if nilRef.Public.(*memoryv1.NotFound).GetId() != "" {
		t.Error("a nil ref must not panic")
	}
}

func TestConnectCodesAreGRPCCodes(t *testing.T) {
	for k := errs.KindValidation; k <= errs.KindInputBlobMissing; k++ {
		ce := errs.ToConnect(errs.Wrap(k, "x", nil))
		if uint32(ce.Code()) != uint32(k.Code()) {
			t.Errorf("%v: connect %v vs grpc %v", k, ce.Code(), k.Code())
		}
	}
}
