package api_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/gen/go/memory/v1/memoryv1connect"
	"github.com/gstamatakis95/engram/internal/api"
)

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// TestPhase3Services_GRPCUnimplemented: PageService and ExportService are registered from day one and answer
// UNIMPLEMENTED until phase 3 (N14), so the contract and the adapters never change shape.
func TestPhase3Services_GRPCUnimplemented(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	(&api.Server{}).RegisterGRPC(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	pages := memoryv1.NewPageServiceClient(conn)
	exports := memoryv1.NewExportServiceClient(conn)
	calls := map[string]func() error{
		"PageService/GetPage": func() error {
			_, err := pages.GetPage(ctx(t), &memoryv1.GetPageRequest{})
			return err
		},
		"PageService/CreatePage": func() error {
			_, err := pages.CreatePage(ctx(t), &memoryv1.CreatePageRequest{})
			return err
		},
		"PageService/RefreshPage": func() error {
			_, err := pages.RefreshPage(ctx(t), &memoryv1.RefreshPageRequest{})
			return err
		},
		"ExportService/ListSnapshots": func() error {
			_, err := exports.ListSnapshots(ctx(t), &memoryv1.ListSnapshotsRequest{})
			return err
		},
		"ExportService/CreateSnapshot": func() error {
			_, err := exports.CreateSnapshot(ctx(t), &memoryv1.CreateSnapshotRequest{})
			return err
		},
		"ExportService/StreamSnapshot (server stream)": func() error {
			s, err := exports.StreamSnapshot(ctx(t), &memoryv1.StreamSnapshotRequest{})
			if err != nil {
				return err
			}
			_, err = s.Recv()
			return err
		},
	}
	for name, call := range calls {
		if got := status.Code(call()); got != codes.Unimplemented {
			t.Errorf("%s = %v, want Unimplemented", name, got)
		}
	}
}

// TestPhase3Services_ConnectUnimplemented is the same contract over Connect.
func TestPhase3Services_ConnectUnimplemented(t *testing.T) {
	mux := http.NewServeMux()
	(&api.Server{}).RegisterConnect(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	pages := memoryv1connect.NewPageServiceClient(ts.Client(), ts.URL)
	exports := memoryv1connect.NewExportServiceClient(ts.Client(), ts.URL)
	_, err1 := pages.GetPage(ctx(t), connect.NewRequest(&memoryv1.GetPageRequest{}))
	_, err2 := exports.ListSnapshots(ctx(t), connect.NewRequest(&memoryv1.ListSnapshotsRequest{}))
	s, err3 := exports.StreamSnapshot(ctx(t), connect.NewRequest(&memoryv1.StreamSnapshotRequest{}))
	if err3 == nil {
		s.Receive()
		err3 = s.Err()
		_ = s.Close()
	}
	for name, err := range map[string]error{"GetPage": err1, "ListSnapshots": err2, "StreamSnapshot": err3} {
		var ce *connect.Error
		if !errors.As(err, &ce) || ce.Code() != connect.CodeUnimplemented {
			t.Errorf("%s = %v, want CodeUnimplemented", name, err)
		}
	}
}

// phase3Method is one RPC of PageService or ExportService.
type phase3Method struct {
	path      string // "/memory.v1.PageService/GetPage"
	streaming bool   // server-streaming: Connect answers HTTP 200 with the error in the end-of-stream message
}

// phase3Methods lists the RPCs of PageService and ExportService from the generated descriptors.
func phase3Methods(t *testing.T) map[string][]phase3Method {
	t.Helper()
	out := map[string][]phase3Method{}
	for _, name := range []protoreflect.FullName{"memory.v1.PageService", "memory.v1.ExportService"} {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		if err != nil {
			t.Fatal(err)
		}
		svc := d.(protoreflect.ServiceDescriptor)
		for i := 0; i < svc.Methods().Len(); i++ {
			m := svc.Methods().Get(i)
			out[string(name)] = append(out[string(name)], phase3Method{
				path: "/" + string(name) + "/" + string(m.Name()), streaming: m.IsStreamingServer()})
		}
	}
	return out
}

// TestPhase3Services_AreRegistered asserts "registered from day one" (N14), not merely "answers Unimplemented": a gRPC
// server that has nothing registered also answers Unimplemented (unknown service), so the answer alone proves nothing.
func TestPhase3Services_AreRegistered(t *testing.T) {
	want := phase3Methods(t)
	if len(want["memory.v1.PageService"]) != 7 || len(want["memory.v1.ExportService"]) != 4 {
		t.Fatalf("descriptors list %v, want 7 PageService and 4 ExportService methods", want)
	}

	empty := grpc.NewServer()
	if info := empty.GetServiceInfo(); len(info) != 0 {
		t.Fatalf("an empty server lists %v", info)
	}

	srv := grpc.NewServer()
	(&api.Server{}).RegisterGRPC(srv) // must not panic and must register both services
	info := srv.GetServiceInfo()
	for svc, methods := range want {
		got, ok := info[svc]
		if !ok {
			t.Errorf("%s is not registered on the gRPC server (registered: %v)", svc, info)
			continue
		}
		var names, wantNames []string
		for _, m := range got.Methods {
			names = append(names, m.Name)
		}
		for _, m := range methods {
			wantNames = append(wantNames, m.path[strings.LastIndex(m.path, "/")+1:])
		}
		sort.Strings(names)
		sort.Strings(wantNames)
		methods := wantNames
		if !reflect.DeepEqual(names, methods) {
			t.Errorf("%s registers %v, want every method of the descriptor %v", svc, names, methods)
		}
	}
}

// rawPost sends a Connect request by hand, so that the HTTP status is visible (connect-go clients turn both a 404 and a
// 501 into CodeUnimplemented). A streaming method needs the enveloped `application/connect+json` form.
func rawPost(t *testing.T, base string, m phase3Method) (int, string) {
	t.Helper()
	ctype, body := "application/json", "{}"
	if m.streaming {
		ctype, body = "application/connect+json", "\x00\x00\x00\x00\x02{}"
	}
	req, err := http.NewRequestWithContext(ctx(t), http.MethodPost, base+m.path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Connect-Protocol-Version", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestPhase3Services_ConnectPathsAre501NotFound: mounted handlers answer HTTP 501 on every unary path (a streaming path
// answers 200 with an unimplemented end-of-stream message); an empty mux answers 404, which is what proves the 501
// comes from a registered handler.
func TestPhase3Services_ConnectPathsAre501NotFound(t *testing.T) {
	mounted := http.NewServeMux()
	(&api.Server{}).RegisterConnect(mounted)
	tsOn := httptest.NewServer(mounted)
	t.Cleanup(tsOn.Close)
	tsOff := httptest.NewServer(http.NewServeMux())
	t.Cleanup(tsOff.Close)

	n := 0
	for _, methods := range phase3Methods(t) {
		for _, m := range methods {
			n++
			status, body := rawPost(t, tsOn.URL, m)
			switch {
			case m.streaming && (status != http.StatusOK || !strings.Contains(body, `"code":"unimplemented"`)):
				t.Errorf("mounted stream POST %s = HTTP %d %q, want 200 + unimplemented end of stream", m.path, status,
					body)
			case !m.streaming && status != http.StatusNotImplemented:
				t.Errorf("mounted POST %s = HTTP %d, want 501", m.path, status)
			}
			if status, _ := rawPost(t, tsOff.URL, m); status != http.StatusNotFound {
				t.Errorf("unmounted POST %s = HTTP %d, want 404 (the negative case)", m.path, status)
			}
		}
	}
	if n != 11 {
		t.Errorf("checked %d paths, want 11", n)
	}
}
