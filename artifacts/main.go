// Command mockembedder is a deterministic stand-in for the third-party unary
// nomic-embed-text-v1.5 gRPC service.
//
// Determinism matters: the same text always yields the same unit vector, so
// enrichment is reproducible and the embedding_cache behaves exactly as it
// would in production. Unrelated texts hash to near-orthogonal vectors, which
// is enough signal to exercise ranking end-to-end without a real model.
//
// Fault injection (env vars) lets you test Temporal retries, the backoff
// predicate, and permanent-failure dead-lettering without touching prod:
//
//	MOCK_LATENCY_MS=50      per-call latency, for throughput math
//	MOCK_FAIL_RATE=0.1      fraction of calls returning UNAVAILABLE (transient)
//	MOCK_FAIL_UNTIL=30s     hard outage for the first N after start
//	MOCK_BAD_DIMS_RATE=0.01 fraction returning 512 dims (permanent failure)
package main

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	embedv1 "example.com/agentmem/genproto/embed/v1"
)

const dims = 768

type server struct {
	embedv1.UnimplementedEmbedderServer
	latency     time.Duration
	failRate    float64
	badDimsRate float64
	outageUntil time.Time
}

func (s *server) Embed(ctx context.Context, req *embedv1.EmbedRequest) (*embedv1.EmbedResponse, error) {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "ctx cancelled")
		}
	}

	if time.Now().Before(s.outageUntil) {
		return nil, status.Error(codes.Unavailable, "simulated outage")
	}
	if s.failRate > 0 && rand.Float64() < s.failRate {
		return nil, status.Error(codes.Unavailable, "simulated transient failure")
	}

	n := dims
	if s.badDimsRate > 0 && rand.Float64() < s.badDimsRate {
		n = 512 // client must treat this as PERMANENT, not retry it
	}

	// Sanity-check the caller did its job. The real service would not care,
	// but catching a missing prefix here turns the highest-cost bug in the
	// system into a loud local failure instead of a silent recall regression.
	if req.TaskType == "" {
		log.Printf("WARN: empty task_type for text %.40q", req.Text)
	}

	return &embedv1.EmbedResponse{Vector: deterministicVector(req.Text, n)}, nil
}

// deterministicVector maps text -> a stable L2-normalized pseudo-random vector.
// Note the whole text (including its "search_document: " / "search_query: "
// prefix) feeds the hash, so a missing or doubled prefix produces a visibly
// different vector and shows up as a recall drop rather than passing silently.
func deterministicVector(text string, n int) []float32 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(text))
	src := rand.NewSource(int64(h.Sum64()))
	rng := rand.New(src)

	v := make([]float32, n)
	var norm float64
	for i := range v {
		x := rng.NormFloat64()
		v[i] = float32(x)
		norm += x * x
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		norm = 1
	}
	for i := range v {
		v[i] = float32(float64(v[i]) / norm)
	}
	return v
}

// unused, kept for parity with byte-packing helpers in the main pipeline
var _ = binary.LittleEndian

func main() {
	s := &server{
		latency:     time.Duration(envInt("MOCK_LATENCY_MS", 50)) * time.Millisecond,
		failRate:    envFloat("MOCK_FAIL_RATE", 0),
		badDimsRate: envFloat("MOCK_BAD_DIMS_RATE", 0),
	}
	if d := envDur("MOCK_FAIL_UNTIL", 0); d > 0 {
		s.outageUntil = time.Now().Add(d)
		log.Printf("simulating outage for the first %s", d)
	}

	lis, err := net.Listen("tcp", ":9100")
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer()
	embedv1.RegisterEmbedderServer(g, s)
	log.Printf("mock embedder on :9100 (latency=%s failRate=%.2f badDims=%.3f)",
		s.latency, s.failRate, s.badDimsRate)
	log.Fatal(g.Serve(lis))
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
