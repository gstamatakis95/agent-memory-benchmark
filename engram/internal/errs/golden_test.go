package errs_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	errorsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/errors/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

var update = flag.Bool("update", false, "rewrite testdata/errors/*.json")

// GoldenDir is where the error goldens live: the repository's testdata/errors, shared with the authz and API tests.
const goldenDir = "../../testdata/errors"

type goldenDetail struct {
	Type string          `json:"type"`
	JSON json.RawMessage `json:"json"`
	Hex  string          `json:"hex"`
}

type golden struct {
	Code    string         `json:"code"`
	Connect string         `json:"connect_code"`
	Message string         `json:"message"`
	Details []goldenDetail `json:"details"`
}

// Render is the golden form of an error: the gRPC status (code, message, every detail as protojson and as the exact
// wire bytes), and the Connect code. TestGolden_ConnectMatches asserts the Connect details equal the gRPC ones.
func render(t *testing.T, err error) []byte {
	t.Helper()
	st := errs.ToStatus(err)
	g := golden{Code: st.Code().String(), Connect: errs.ToConnect(err).Code().String(), Message: st.Message(),
		Details: []goldenDetail{}}
	for _, a := range st.Proto().GetDetails() {
		m, uerr := a.UnmarshalNew()
		if uerr != nil {
			t.Fatal(uerr)
		}
		js, merr := protojson.MarshalOptions{Multiline: false}.Marshal(m)
		if merr != nil {
			t.Fatal(merr)
		}
		var compact bytes.Buffer
		if cerr := json.Compact(&compact, js); cerr != nil {
			t.Fatal(cerr)
		}
		g.Details = append(g.Details, goldenDetail{Type: string(a.MessageName()), JSON: compact.Bytes(),
			Hex: hex.EncodeToString(a.GetValue())})
	}
	out, merr := json.MarshalIndent(g, "", "  ")
	if merr != nil {
		t.Fatal(merr)
	}
	return append(out, '\n')
}

// goldenCases is every error the system renders: one per Kind, per detail message of memory/v1/errors.proto and per
// authorization outcome of PLAN.md section 4.1.1. The authz and API table tests reuse the same files.
func goldenCases() map[string]error {
	frozenUntil := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
	ns := id.NamespaceID{0x01, 0x8f, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 0x0a}
	doc := id.DocumentID("doc-1")
	op1, op2 := id.OperationID{1}, id.OperationID{2}
	conflict := func(r memoryv1.OperationConflictReason) error { return errs.OperationConflict(op1, op2, r) }
	return map[string]error{
		"validation_missing_deadline": errs.ValidationReason(errs.DeadlineField, errs.ReasonMissingDeadline,
			"the call has no deadline"),
		"validation_deadline_too_long": errs.ValidationReason(errs.DeadlineField, errs.ReasonDeadlineTooLong,
			"over the 10s cap of Recall"),
		"validation_request_id": errs.ValidationReason("meta.request_id", errs.ReasonRequestIDNeeded, "required"),
		"validation_several": errs.ValidationErrors(
			errs.Violation{Field: "items[0].content", Reason: "TOO_LONG", Description: "over 1 MiB"},
			errs.Violation{Field: "tag_filter.mode", Reason: "INCONSISTENT", Description: "UNSPECIFIED with tags"}),
		"not_found_document":         errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_DOCUMENT, doc),
		"not_found_document_deleted": errs.NotFoundDeleted(memoryv1.ResourceKind_RESOURCE_KIND_DOCUMENT_VERSION, doc),
		"not_found_namespace":        errs.NotFoundNamespace("acme", ns),
		"not_found_tenant":           errs.NotFoundTenant("acme"),
		"quota_exceeded": errs.QuotaExceeded("recalls_per_min", memoryv1.QuotaScope_QUOTA_SCOPE_NAMESPACE, 100, 101,
			1500*time.Millisecond),
		"wrong_shard_or_epoch": errs.WrongShardOrEpoch(ns, 2, 3, memoryv1.NamespaceState_NAMESPACE_STATE_ACTIVE),
		"moved_out_hint_dropped": errs.MovedOut(ns, 2, 2,
			&errorsv1.MovedOutHint{NamespaceId: ns.String(), TargetShardId: 9, NextEpoch: 3}),
		"namespace_frozen_move": errs.NamespaceFrozenUntil(ns, 30*time.Second, frozenUntil),
		"namespace_frozen_restore": errs.NamespaceFrozen(ns, memoryv1.FreezeReason_FREEZE_REASON_RESTORE,
			time.Second),
		"fence_busy_as_frozen": errs.FenceBusy(ns),
		"namespace_not_ready":  errs.NamespaceNotReady(ns, 250*time.Millisecond),
		"operation_conflict_busy": conflict(
			memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_NAMESPACE_BUSY),
		"operation_conflict_reused": conflict(
			memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_IDEMPOTENCY_KEY_REUSED),
		"operation_conflict_page": conflict(
			memoryv1.OperationConflictReason_OPERATION_CONFLICT_REASON_PAGE_REFRESHING),
		"precondition_namespace_deleting": errs.PreconditionFailed(errs.PreconditionNamespaceDeleting, ns.String(),
			"the namespace is being deleted"),
		"precondition_tenant_deleting": errs.PreconditionFailed(errs.PreconditionTenantDeleting, "acme",
			"the tenant is being deleted"),
		"precondition_etag":           errs.PreconditionFailed("ETAG_MISMATCH", "namespace", "stale etag"),
		"unavailable_catalog":         errs.Unavailable("catalog unavailable", 2*time.Second),
		"unavailable_no_retry":        errs.Unavailable("shard down", 0),
		"permanent_llm":               errs.PermanentLLM("bge-reranker-base", "400", errors.New("bad request")),
		"unauthenticated_missing":     errs.Unauthenticated("missing bearer token"),
		"unauthenticated_expired":     errs.Unauthenticated("token expired"),
		"permission_denied_scope":     errs.PermissionDenied("memory.write"),
		"permission_denied_allowlist": errs.NamespaceNotAllowed(ns),
		"permission_denied_cell":      errs.WrongCell("cell-b"),
		"deadline_exceeded":           errs.DeadlineExceeded("deadline elapsed", nil),
		"internal_redacted":           errs.Internal("pq: password authentication failed", errors.New("secret")),
		"unknown_error_redacted":      errors.New("dial tcp 10.0.0.7:5432: connect: connection refused"),
		"document_busy_internal":      errs.DocumentBusy(doc),
		"input_blob_missing_internal": errs.InputBlobMissing("cache/ab/cd", "extract", [32]byte{7}, 2),
	}
}

func TestGolden(t *testing.T) {
	cases := goldenCases()
	if *update {
		if err := os.MkdirAll(goldenDir, 0o755); err != nil { //nolint:gosec // repository directory
			t.Fatal(err)
		}
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			got := render(t, err)
			path := filepath.Join(goldenDir, name+".json")
			if *update {
				if werr := os.WriteFile(path, got, 0o644); werr != nil { //nolint:gosec // checked-in golden
					t.Fatal(werr)
				}
				return
			}
			want, rerr := os.ReadFile(path) //nolint:gosec // fixed golden path
			if rerr != nil {
				t.Fatalf("%v (run `go test ./internal/errs -update`)", rerr)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("golden %s differs (run `go test ./internal/errs -update` and review it):\n got: %s\nwant: %s",
					path, got, want)
			}
		})
	}
	if !*update {
		files, _ := filepath.Glob(filepath.Join(goldenDir, "*.json"))
		if len(files) != len(cases) {
			t.Errorf("%d golden files for %d cases: a stale golden is a contract nobody tests", len(files), len(cases))
		}
	}
}

// TestGolden_ConnectMatches renders every case on both transports and asserts that the Connect error carries the same
// code, the same message and, byte for byte, the same details in the same order as the gRPC status.
func TestGolden_ConnectMatches(t *testing.T) {
	for name, err := range goldenCases() {
		t.Run(name, func(t *testing.T) {
			st, ce := errs.ToStatus(err), errs.ToConnect(err)
			if uint32(ce.Code()) != uint32(st.Code()) || ce.Message() != st.Message() {
				t.Fatalf("connect %v %q vs grpc %v %q", ce.Code(), ce.Message(), st.Code(), st.Message())
			}
			g := st.Proto().GetDetails()
			if len(ce.Details()) != len(g) {
				t.Fatalf("%d connect details vs %d grpc details", len(ce.Details()), len(g))
			}
			for i, d := range ce.Details() {
				if d.Type() != string(g[i].MessageName()) || !bytes.Equal(d.Bytes(), g[i].GetValue()) {
					t.Errorf("detail %d differs: %s %x vs %s %x", i, d.Type(), d.Bytes(), g[i].MessageName(),
						g[i].GetValue())
				}
			}
			// No internal detail ever leaves (N128).
			for _, a := range g {
				if len(a.MessageName()) > 16 && a.MessageName()[:16] == "engram.internal." {
					t.Errorf("internal detail %s rendered", a.MessageName())
				}
			}
		})
	}
}

// TestForeignStatusesLoseInternalDetails: a status that is not an *errs.Error but carries engram.internal.* details (a
// forwarded peer error returned raw by a handler) is rendered with its code, message and public details only.
func TestForeignStatusesLoseInternalDetails(t *testing.T) {
	ns := id.NamespaceID{1}
	withInternal := errs.ToInternalStatus(errs.MovedOut(ns, 1, 1, &errorsv1.MovedOutHint{NamespaceId: ns.String()}))
	if n := len(withInternal.Proto().GetDetails()); n != 2 {
		t.Fatalf("setup: internal status has %d details, want public + hint", n)
	}
	raw := connect.NewError(connect.CodeFailedPrecondition, errors.New(withInternal.Message()))
	for _, a := range withInternal.Proto().GetDetails() {
		if d, derr := connect.NewErrorDetail(a); derr == nil {
			raw.AddDetail(d)
		}
	}
	for _, err := range []error{withInternal.Err(), raw, errs.ToConnect(errs.FromStatus(withInternal))} {
		out := errs.ToStatus(err)
		if out.Code() != withInternal.Code() || out.Message() != withInternal.Message() {
			t.Errorf("code/message changed: %v %q", out.Code(), out.Message())
		}
		for _, d := range out.Details() {
			if _, bad := d.(*errorsv1.MovedOutHint); bad {
				t.Fatal("MovedOutHint leaked through a foreign status")
			}
		}
		if len(out.Details()) == 0 {
			t.Error("the public WrongShardOrEpoch detail must survive")
		}
	}
	// Rendering a raw errdetails status keeps it, too.
	st := errs.ToStatus(errs.PermissionDenied("memory.read"))
	if got := errs.ToStatus(st.Err()); got.Details()[0].(*errdetails.ErrorInfo).GetReason() != errs.ReasonMissingScope {
		t.Error("a rendered status passes through unchanged")
	}
}

// TestDetailBytesAreDeterministic pins the fix of a real defect: ErrorInfo.metadata is a map, the default proto
// encoding orders map entries per call, and the gRPC and the Connect rendering of one error then differed in bytes.
func TestDetailBytesAreDeterministic(t *testing.T) {
	err := errs.PermanentLLM("bge-reranker-base", "400", nil)
	want := errs.ToStatus(err).Proto().GetDetails()[0].GetValue()
	for range 200 {
		if got := errs.ToStatus(err).Proto().GetDetails()[0].GetValue(); !bytes.Equal(got, want) {
			t.Fatalf("ToStatus bytes vary between calls: %x vs %x", got, want)
		}
		if got := errs.ToConnect(err).Details()[0].Bytes(); !bytes.Equal(got, want) {
			t.Fatalf("ToConnect bytes differ from ToStatus: %x vs %x", got, want)
		}
	}
}

// TestInternalMessageInPublicIsNeverRendered is review F10: the N128 drop is structural, by the type of the message,
// not by which field a constructor put it in.
func TestInternalMessageInPublicIsNeverRendered(t *testing.T) {
	hint := &errorsv1.MovedOutHint{NamespaceId: "n", TargetShardId: 9, NextEpoch: 3}
	for _, e := range []*errs.Error{
		{Kind: errs.KindUnavailable, Msg: "x", Public: hint, Retry: time.Second},
		{Kind: errs.KindWrongShardOrEpoch, Msg: "x", Public: &errorsv1.FenceBusy{NamespaceId: "n"}},
		{Kind: errs.KindInternal, Msg: "x", Public: &errorsv1.DocumentBusy{DocumentId: "d"}},
	} {
		for name, details := range map[string][]string{
			"grpc":    detailTypes(errs.ToStatus(e)),
			"connect": connectTypes(errs.ToConnect(e)),
		} {
			for _, d := range details {
				if strings.HasPrefix(d, "engram.internal.") {
					t.Errorf("%s rendered the internal detail %s from Error.Public", name, d)
				}
			}
		}
	}
	// The internal hop still carries the Internal detail (router.Forward), and only that.
	e := errs.MovedOut(id.NamespaceID{1}, 1, 1, hint)
	if got := detailTypes(errs.ToInternalStatus(e)); len(got) != 2 {
		t.Errorf("internal hop details = %v; want the public detail and the hint", got)
	}
}

func detailTypes(st *status.Status) []string {
	var out []string
	for _, a := range st.Proto().GetDetails() {
		out = append(out, string(a.MessageName()))
	}
	return out
}

func connectTypes(ce *connect.Error) []string {
	var out []string
	for _, d := range ce.Details() {
		out = append(out, d.Type())
	}
	return out
}
