// Command nomicbridge is a unary gRPC embed.v1.Embedder server that proxies
// each Embed call to an OpenAI-compatible /embeddings endpoint (Docker Model
// Runner serving ai/nomic-embed-text-v1.5).
//
// The caller sends text ALREADY prefixed with "search_document: " /
// "search_query: " — exactly the prefixes the nomic model expects — so the
// input passes through verbatim, no extra prefixing here.
//
// Error mapping keeps the client's retry policy meaningful:
//   - connection errors / HTTP 5xx  -> UNAVAILABLE (transient, client retries)
//   - malformed or wrong-dim reply  -> INTERNAL   (permanent, no retry)
//
// Env:
//
//	OPENAI_BASE_URL  default http://model-runner.docker.internal/engines/v1
//	EMBED_MODEL      default ai/nomic-embed-text-v1.5
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	embedv1 "example.com/agentmem/genproto/embed/v1"
)

const (
	dims            = 768
	upstreamTimeout = 30 * time.Second // model can queue under load
)

type server struct {
	embedv1.UnimplementedEmbedderServer
	httpc *http.Client
	url   string // full .../embeddings URL
	model string
}

type embeddingsRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type embeddingsResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (s *server) Embed(ctx context.Context, req *embedv1.EmbedRequest) (*embedv1.EmbedResponse, error) {
	body, err := json.Marshal(embeddingsRequest{Model: s.model, Input: req.GetText()})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal request: %v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpc.Do(httpReq)
	if err != nil {
		// Connection refused, DNS failure, timeout, ... — all transient.
		return nil, status.Errorf(codes.Unavailable, "upstream request failed: %v", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read upstream response: %v", err)
	}

	if resp.StatusCode >= 500 {
		return nil, status.Errorf(codes.Unavailable, "upstream HTTP %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, status.Errorf(codes.Internal, "upstream HTTP %d: %s", resp.StatusCode, truncate(raw, 200))
	}

	var parsed embeddingsResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, status.Errorf(codes.Internal, "malformed upstream response: %v", err)
	}
	if len(parsed.Data) != 1 {
		return nil, status.Errorf(codes.Internal, "upstream returned %d embeddings, want 1", len(parsed.Data))
	}
	vec := parsed.Data[0].Embedding
	if len(vec) != dims {
		return nil, status.Errorf(codes.Internal, "upstream returned %d dims, want %d", len(vec), dims)
	}
	l2Normalize(vec) // defensive; idempotent on already-normalized vectors

	return &embedv1.EmbedResponse{Vector: vec}, nil
}

// l2Normalize scales v to unit length in place. A zero vector is left as-is.
func l2Normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		return
	}
	for i := range v {
		v[i] = float32(float64(v[i]) / norm)
	}
}

func truncate(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	baseURL := strings.TrimRight(envStr("OPENAI_BASE_URL", "http://model-runner.docker.internal/engines/v1"), "/")
	model := envStr("EMBED_MODEL", "ai/nomic-embed-text-v1.5")

	s := &server{
		httpc: &http.Client{
			Timeout: upstreamTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 64, // enrichment fan-out is 32-way concurrent
				IdleConnTimeout:     90 * time.Second,
			},
		},
		url:   fmt.Sprintf("%s/embeddings", baseURL),
		model: model,
	}

	lis, err := net.Listen("tcp", ":9100")
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer()
	embedv1.RegisterEmbedderServer(g, s)
	log.Printf("nomic bridge on :9100 (upstream=%s model=%s)", s.url, s.model)
	log.Fatal(g.Serve(lis))
}
