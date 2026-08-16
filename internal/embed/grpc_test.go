package embed

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	embedv1 "example.com/agentmem/genproto/embed/v1"
)

// stubEmbedServer is an in-process embed.v1.Embedder.
type stubEmbedServer struct {
	embedv1.UnimplementedEmbedderServer

	mu           sync.Mutex
	calls        int
	failFirstN   int // return UNAVAILABLE for the first N calls
	dims         int // response dimensionality (default Dims)
	lastText     string
	lastTaskType string
}

func (s *stubEmbedServer) Embed(_ context.Context, req *embedv1.EmbedRequest) (*embedv1.EmbedResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.lastText = req.GetText()
	s.lastTaskType = req.GetTaskType()
	if s.calls <= s.failFirstN {
		return nil, status.Error(codes.Unavailable, "injected outage")
	}
	d := s.dims
	if d == 0 {
		d = Dims
	}
	vec := make([]float32, d)
	vec[0] = 3 // deliberately unnormalized
	return &embedv1.EmbedResponse{Vector: vec}, nil
}

// startBufconnEmbedder serves the stub over bufconn and dials it with the
// production DialOptions (retry service config + keepalive), returning a
// Client wired through the real GRPCEmbedder adapter.
func startBufconnEmbedder(t *testing.T, stub *stubEmbedServer) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	embedv1.RegisterEmbedderServer(srv, stub)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	opts := append(DialOptions(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return NewClient(NewGRPCEmbedder(conn, nil), nil)
}

// TestGRPCAdapterEndToEnd: the adapter carries the prefixed text and task
// type over the wire and the client returns a normalized 768-dim vector.
func TestGRPCAdapterEndToEnd(t *testing.T) {
	stub := &stubEmbedServer{}
	c := startBufconnEmbedder(t, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vec, err := c.EmbedDocument(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) != Dims {
		t.Fatalf("got %d dims", len(vec))
	}
	assertUnitNorm(t, vec)

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.lastText != DocumentPrefix+"hello world" {
		t.Fatalf("wire text = %q", stub.lastText)
	}
	if stub.lastTaskType != "search_document" {
		t.Fatalf("wire task_type = %q", stub.lastTaskType)
	}
}

// TestGRPCRetryOnUnavailable: the service config transparently retries
// UNAVAILABLE with backoff — one logical call, two RPC attempts.
func TestGRPCRetryOnUnavailable(t *testing.T) {
	stub := &stubEmbedServer{failFirstN: 1}
	c := startBufconnEmbedder(t, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.EmbedQuery(ctx, "retry me"); err != nil {
		t.Fatalf("call failed despite retry policy: %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.calls != 2 {
		t.Fatalf("server saw %d attempts, want 2 (1 UNAVAILABLE + 1 success)", stub.calls)
	}
	if stub.lastTaskType != "search_query" {
		t.Fatalf("wire task_type = %q", stub.lastTaskType)
	}
}

// TestGRPCWrongDimsPermanent: a wrong-dimension RPC response is permanent at
// the adapter level too (never retried as if transient).
func TestGRPCWrongDimsPermanent(t *testing.T) {
	stub := &stubEmbedServer{dims: 512}
	c := startBufconnEmbedder(t, stub)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.EmbedDocument(ctx, "x")
	if err == nil || !IsPermanent(err) {
		t.Fatalf("want permanent error for 512 dims, got %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.calls != 1 {
		t.Fatalf("wrong-dims response was retried: %d calls", stub.calls)
	}
}

// TestServiceConfig pins the docs/02-storage.md E.3 retry policy: valid
// JSON, UNAVAILABLE retryable, 5 attempts with exponential backoff.
func TestServiceConfig(t *testing.T) {
	var cfg struct {
		MethodConfig []struct {
			Name        []map[string]string `json:"name"`
			Timeout     string              `json:"timeout"`
			RetryPolicy struct {
				MaxAttempts          int      `json:"maxAttempts"`
				InitialBackoff       string   `json:"initialBackoff"`
				MaxBackoff           string   `json:"maxBackoff"`
				BackoffMultiplier    float64  `json:"backoffMultiplier"`
				RetryableStatusCodes []string `json:"retryableStatusCodes"`
			} `json:"retryPolicy"`
		} `json:"methodConfig"`
	}
	if err := json.Unmarshal([]byte(ServiceConfig), &cfg); err != nil {
		t.Fatalf("ServiceConfig is not valid JSON: %v", err)
	}
	mc := cfg.MethodConfig[0]
	if mc.Name[0]["service"] != "embed.v1.Embedder" {
		t.Fatalf("service = %q", mc.Name[0]["service"])
	}
	rp := mc.RetryPolicy
	if rp.MaxAttempts != 5 || rp.InitialBackoff != "0.1s" || rp.MaxBackoff != "3s" || rp.BackoffMultiplier != 2.0 {
		t.Fatalf("retry policy drifted from docs/02 E.3: %+v", rp)
	}
	if !strings.Contains(strings.Join(rp.RetryableStatusCodes, ","), "UNAVAILABLE") {
		t.Fatal("UNAVAILABLE not retryable")
	}
	if KeepaliveParams.Time != 30*time.Second || KeepaliveParams.Timeout != 10*time.Second || !KeepaliveParams.PermitWithoutStream {
		t.Fatalf("keepalive drifted from docs/02 E.3: %+v", KeepaliveParams)
	}
}
