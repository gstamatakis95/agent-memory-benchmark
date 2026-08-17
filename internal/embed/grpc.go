package embed

import (
	"context"
	"strings"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	embedv1 "example.com/agentmem/genproto/embed/v1"
)

// ServiceConfig is the grpc-go retry service config from docs/02-storage.md
// section E.3: UNAVAILABLE (and transient friends) retried with exponential
// backoff + jitter, 5 attempts. The per-attempt timeout is 30s (raised from
// the E.3 5s): the micro-batching bridge queues calls under saturation, so
// with hundreds of unary calls in flight the queue's tail latency legitimately
// exceeds 5s — a 5s deadline tail-kills healthy queued calls, and the retries
// requeue and amplify the pile-up.
const ServiceConfig = `{
  "methodConfig": [{
    "name": [{"service": "embed.v1.Embedder"}],
    "timeout": "30s",
    "retryPolicy": {
      "maxAttempts": 5,
      "initialBackoff": "0.1s",
      "maxBackoff": "3s",
      "backoffMultiplier": 2.0,
      "retryableStatusCodes": ["UNAVAILABLE","RESOURCE_EXHAUSTED","DEADLINE_EXCEEDED"]
    }
  }]
}`

// KeepaliveParams are the client keepalive settings from docs/02-storage.md
// section E.3. Client Time must stay >= the server's EnforcementPolicy
// MinTime or the server answers GOAWAY (ENHANCE_YOUR_CALM).
var KeepaliveParams = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

// DialOptions returns the standard dial options (retry service config +
// keepalive), exposed so tests can add a bufconn dialer.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(ServiceConfig),
		grpc.WithKeepaliveParams(KeepaliveParams),
		// grpc-go internally caps retry attempts at 5 unless raised here;
		// make the service-config maxAttempts effective explicitly.
		grpc.WithMaxCallAttempts(5),
	}
}

// Dial creates the ONE long-lived ClientConn to the embedder. HTTP/2
// multiplexes all concurrent unary calls over this single connection
// (docs/02-storage.md E.3: one ClientConn, not a pool); dial once at startup
// and share the conn across the bounded worker pool.
func Dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, DialOptions()...)
}

// GRPCEmbedder adapts the generated embed.v1.Embedder client to the Embedder
// interface: strictly one text per RPC. Throughput against the unary
// endpoint comes from bounded goroutine concurrency in the caller
// (errgroup.SetLimit), never from batching.
type GRPCEmbedder struct {
	cli embedv1.EmbedderClient
	lim *rate.Limiter // optional global QPS cap; nil = unlimited
}

// NewGRPCEmbedder wraps an already-dialed conn. lim may be nil.
func NewGRPCEmbedder(conn grpc.ClientConnInterface, lim *rate.Limiter) *GRPCEmbedder {
	return &GRPCEmbedder{cli: embedv1.NewEmbedderClient(conn), lim: lim}
}

// Embed sends one already-prefixed text. Transient failures are retried by
// the service config; a wrong-dimension response is returned as a
// PermanentError so enrichment dead-letters instead of retrying.
func (g *GRPCEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if g.lim != nil {
		if err := g.lim.Wait(ctx); err != nil {
			return nil, err
		}
	}
	task := "search_document"
	if strings.HasPrefix(text, QueryPrefix) {
		task = "search_query"
	}
	resp, err := g.cli.Embed(ctx, &embedv1.EmbedRequest{Text: text, TaskType: task})
	if err != nil {
		return nil, err
	}
	if len(resp.GetVector()) != Dims {
		return nil, Permanentf("embed: rpc returned %d dims, want %d", len(resp.GetVector()), Dims)
	}
	return resp.GetVector(), nil
}
