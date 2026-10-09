package api_test

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/gen/go/memory/v1/memoryv1connect"
	"github.com/gstamatakis95/engram/internal/api"
	"github.com/gstamatakis95/engram/internal/errs"
)

func TestDeadlineLimits_Table(t *testing.T) {
	ms, s := time.Millisecond, time.Second
	tests := []struct {
		method   string
		min, max time.Duration
	}{
		{"/memory.v1.MemoryService/Recall", 200 * ms, 10 * s},
		{"/memory.v1.MemoryService/Retain", 500 * ms, 30 * s},
		{"/memory.v1.MemoryService/Reflect", 5 * s, 330 * s},
		{"/memory.v1.OperationService/WaitOperation", s, 65 * s},
		{"/memory.v1.ExportService/StreamSnapshot", 5 * s, 600 * s},
		{"/memory.v1.DocumentService/DeleteDocument", s, 40 * s},
		{"/memory.v1.NamespaceService/DeleteNamespace", s, 40 * s},
		{"/memory.admin.v1.TenantService/DeleteTenant", s, 40 * s},
		{"/memory.v1.MemoryService/Restore", s, 40 * s},
		{"/memory.v1.MemoryService/GetMemory", 100 * ms, 30 * s},
		{"/memory.admin.v1.MoveService/StartMove", 100 * ms, 30 * s},
	}
	for _, tc := range tests {
		if l := api.DefaultDeadlineLimits(tc.method); l.Min != tc.min || l.Max != tc.max {
			t.Errorf("%s: %+v; want %v..%v", tc.method, l, tc.min, tc.max)
		}
	}
}

func TestDeadlineGuard_CheckRemaining(t *testing.T) {
	g := api.NewDeadlineGuard(api.Options{MaxDeadline: map[string]time.Duration{
		"/memory.v1.MemoryService/GetMemory": 2 * time.Second}})
	const recall, get = "/memory.v1.MemoryService/Recall", "/memory.v1.MemoryService/GetMemory"
	tests := []struct {
		name      string
		method    string
		remaining time.Duration
		want      string // reason, "" for accepted, "EXCEEDED" for DEADLINE_EXCEEDED
	}{
		{"at the cap", recall, 10 * time.Second, ""},
		{"a nanosecond over the cap", recall, 10*time.Second + 1, errs.ReasonDeadlineTooLong},
		{"far over the cap", recall, time.Hour, errs.ReasonDeadlineTooLong},
		{"at the minimum", recall, 200 * time.Millisecond, ""},
		{"lost a little on the wire", recall, 190 * time.Millisecond, ""},
		{"lost the whole allowance", recall, 180 * time.Millisecond, ""},
		{"lost more than the allowance", recall, 179 * time.Millisecond, errs.ReasonOutOfRange},
		{"below the minimum", recall, 50 * time.Millisecond, errs.ReasonOutOfRange},
		{"already elapsed", recall, 0, "EXCEEDED"},
		{"negative", recall, -time.Second, "EXCEEDED"},
		{"the override applies", get, 3 * time.Second, errs.ReasonDeadlineTooLong},
		{"the override leaves the minimum", get, 150 * time.Millisecond, ""},
		{"delete class takes 40 s", "/memory.v1.DocumentService/DeleteDocument", 40 * time.Second, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := g.CheckRemaining(tc.method, tc.remaining)
			switch tc.want {
			case "":
				if err != nil {
					t.Errorf("err = %v", err)
				}
			case "EXCEEDED":
				if !errs.Is(err, errs.KindDeadline) {
					t.Errorf("err = %v; want DEADLINE_EXCEEDED", err)
				}
			default:
				e, ok := err.(*errs.Error) //nolint:errorlint // exact type
				if !ok || e.Kind != errs.KindValidation {
					t.Fatalf("err = %v; want INVALID_ARGUMENT", err)
				}
				v := e.Public.(*memoryv1.ValidationError).GetViolations()
				if len(v) != 1 || v[0].GetField() != "grpc-timeout" || v[0].GetReason() != tc.want {
					t.Errorf("violations = %v; want grpc-timeout / %s", v, tc.want)
				}
			}
		})
	}
}

func TestDeadlineGuard_UnguardedServices(t *testing.T) {
	g := api.NewDeadlineGuard(api.Options{})
	if err := g.Check(context.Background(), "/grpc.health.v1.Health/Check"); err != nil {
		t.Errorf("health checks carry no deadline contract: %v", err)
	}
	if err := g.Check(context.Background(), "/memory.v1.MemoryService/Recall"); err == nil {
		t.Error("a served method without a deadline must be refused")
	}
}

// deadlineEnv serves GetMemory/Recall on both transports behind the guard, to prove that grpc-timeout and
// Connect-Timeout-Ms both reach it as the context deadline and that the details are byte-identical.
type deadlineEnv struct {
	conn    *grpc.ClientConn
	baseURL string
	http    *http.Client
}

type memStub struct {
	memoryv1connect.UnimplementedMemoryServiceHandler
}

func newDeadlineEnv(t *testing.T) *deadlineEnv {
	t.Helper()
	g := api.NewDeadlineGuard(api.Options{})
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.UnaryInterceptor(g.Unary()), grpc.StreamInterceptor(g.Stream()))
	memoryv1.RegisterMemoryServiceServer(gs, memoryv1.UnimplementedMemoryServiceServer{})
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(
		func(context.Context, string) (net.Conn, error) { return lis.DialContext(context.Background()) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	path, h := memoryv1connect.NewMemoryServiceHandler(memStub{}, connect.WithInterceptors(g.Connect()))
	mux.Handle(path, h)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() { _ = conn.Close(); gs.Stop(); ts.Close() })
	return &deadlineEnv{conn: conn, baseURL: ts.URL, http: ts.Client()}
}

func detailsOf(st *status.Status) []string {
	var out []string
	for _, a := range st.Proto().GetDetails() {
		out = append(out, string(a.MessageName())+":"+hex.EncodeToString(a.GetValue()))
	}
	return out
}

func connectDetails(err error) (connect.Code, []string) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return connect.CodeUnknown, nil
	}
	var out []string
	for _, d := range ce.Details() {
		out = append(out, d.Type()+":"+hex.EncodeToString(d.Bytes()))
	}
	return ce.Code(), out
}

// TestDeadlineGuard_Transports: a missing deadline, an over-long one and an in-range one, on gRPC unary, gRPC stream
// and Connect (unary and stream); the refusals carry the same details, byte for byte, on both transports.
func TestDeadlineGuard_Transports(t *testing.T) {
	e := newDeadlineEnv(t)
	getReq := &memoryv1.GetMemoryRequest{}
	recallReq := &memoryv1.RecallRequest{}

	type call struct {
		name     string
		deadline time.Duration // 0: none
		method   string
		stream   bool
		want     codes.Code
		wantType string
		wantSub  string // reason
	}
	calls := []call{
		{"unary, no deadline", 0, "/memory.v1.MemoryService/GetMemory", false, codes.InvalidArgument,
			"memory.v1.ValidationError", errs.ReasonMissingDeadline},
		{"unary, 31 s over the 30 s cap", 31 * time.Second, "/memory.v1.MemoryService/GetMemory", false,
			codes.InvalidArgument, "memory.v1.ValidationError", errs.ReasonDeadlineTooLong},
		{"stream, no deadline", 0, "/memory.v1.MemoryService/Recall", true, codes.InvalidArgument,
			"memory.v1.ValidationError", errs.ReasonMissingDeadline},
		{"stream, 11 s over the 10 s Recall cap", 11 * time.Second, "/memory.v1.MemoryService/Recall", true,
			codes.InvalidArgument, "memory.v1.ValidationError", errs.ReasonDeadlineTooLong},
		{"unary, in range reaches the handler", 5 * time.Second, "/memory.v1.MemoryService/GetMemory", false,
			codes.Unimplemented, "", ""},
		{"stream, in range reaches the handler", 5 * time.Second, "/memory.v1.MemoryService/Recall", true,
			codes.Unimplemented, "", ""},
	}
	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			if c.deadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, c.deadline)
				defer cancel()
			}
			// gRPC.
			var gerr error
			req := getReq
			if c.stream {
				var s grpc.ClientStream
				if s, gerr = e.conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, c.method); gerr == nil {
					if gerr = s.SendMsg(recallReq); gerr == nil {
						_ = s.CloseSend()
						gerr = s.RecvMsg(&emptypb.Empty{})
					}
				}
			} else {
				gerr = e.conn.Invoke(ctx, c.method, req, &emptypb.Empty{})
			}
			gst, _ := status.FromError(gerr)
			if gst.Code() != c.want {
				t.Fatalf("grpc code = %v (%v); want %v", gst.Code(), gerr, c.want)
			}
			// Connect.
			client := connect.NewClient[emptypb.Empty, emptypb.Empty](e.http, e.baseURL+c.method)
			var cerr error
			if c.stream {
				var s *connect.ServerStreamForClient[emptypb.Empty]
				if s, cerr = client.CallServerStream(ctx, connect.NewRequest(&emptypb.Empty{})); cerr == nil {
					for s.Receive() {
					}
					cerr = s.Err()
				}
			} else {
				_, cerr = client.CallUnary(ctx, connect.NewRequest(&emptypb.Empty{}))
			}
			ccode, cdetails := connectDetails(cerr)
			if codes.Code(ccode) != c.want { //nolint:gosec // equal code spaces
				t.Fatalf("connect code = %v (%v); want %v", ccode, cerr, c.want)
			}
			if c.wantType == "" {
				return
			}
			gd := detailsOf(gst)
			if len(gd) != 1 || len(cdetails) != 1 || gd[0] != cdetails[0] {
				t.Errorf("details differ across transports:\n grpc    %v\n connect %v", gd, cdetails)
			}
			v := gst.Details()[0].(*memoryv1.ValidationError).GetViolations()[0]
			if v.GetReason() != c.wantSub || v.GetField() != "grpc-timeout" {
				t.Errorf("detail = %v; want field grpc-timeout reason %s", v, c.wantSub)
			}
		})
	}
}
